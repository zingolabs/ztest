//! The test environment.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::future::join_all;
use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, PostParams};
use tokio::sync::Mutex;

use std::net::{IpAddr, Ipv4Addr};

use crate::EnvError;
use crate::cluster::{self, Sentinel};
use crate::component::{ComponentOpts, Indexer, Validator, Wallet};
use crate::error::env_err;
use crate::topology::ActivationHeights;

use crate::handles::indexer::{IndexerBackend, IndexerConfig};
use crate::handles::validator::{ValidatorBackend, ValidatorConfig};
use crate::handles::wallet::WalletConfig;
use crate::handles::{Endpoint, ForwardRegistry, HandleInner};

/// Regtest materialization captured per validator at `add_validator`, applied once the
/// activation heights are known (so the concrete backend need not be retained)
type RegtestMaterializeFn = Box<
    dyn FnOnce(
            ComponentOpts,
            &ActivationHeights,
            &[(String, u16)],
        ) -> Result<ComponentOpts, EnvError>
        + Send,
>;

/// Indexer config materialization, captured at `add_indexer` (validator host resolved at
/// build time)
type IndexerMaterializeFn =
    Box<dyn FnOnce(ComponentOpts, Option<&str>) -> Result<ComponentOpts, EnvError> + Send>;
use crate::manifest::PodSpec;
use crate::mounts::{self, ResolvedMount};
use crate::naming::{self, RunCoords};
use crate::portforward::Forwarder;
use crate::qos;
use crate::seeds::{self, SeedBinding};

/// Live backend behind a materialized pod; the variant = the pod's category.
///
/// - Validators + indexers only (wallets run in-process)
/// - Drives the env's readiness (and, for validators, warm) probes during [`TestEnv::build`]
#[derive(Debug, Clone)]
pub(crate) enum ComponentHandle {
    Validator(Arc<dyn ValidatorBackend>),
    Indexer(Arc<dyn IndexerBackend>),
}

/// Per-component bookkeeping captured at `build` time.
#[derive(Debug, Clone)]
pub(crate) struct ComponentState {
    pub(crate) namespace: String,
    pub(crate) pod_name: String,
    pub(crate) label: &'static str,
    pub(crate) named_ports: Vec<(String, u16)>,
    pub(crate) handle: ComponentHandle,
}

impl ComponentState {
    fn new(spec: &PodSpec, namespace: String, handle: ComponentHandle) -> Self {
        ComponentState {
            namespace,
            pod_name: spec.pod_name.clone(),
            label: spec.label,
            named_ports: spec.ports.clone(),
            handle,
        }
    }
}

// ────────────────────────────── EnvInner ──────────────────────────────

pub(crate) struct EnvInner {
    pub(crate) client: OnceLock<Client>,
    pub(crate) namespace: std::sync::Mutex<Option<String>>,
    pub(crate) components: tokio::sync::RwLock<HashMap<u64, ComponentState>>,
    pub(crate) in_cluster: bool,
    pub(crate) forwards: ForwardRegistry,
    pub(crate) seed_bindings: std::sync::Mutex<Vec<SeedBinding>>,
    pub(crate) is_built: AtomicBool,
    /// One `kubectl logs -f` follow of every component pod, rendered into the teardown
    /// diagnostic on failure ([`crate::logstream`]); set only on a successfully-built env
    pub(crate) log_capture: std::sync::OnceLock<crate::logstream::LogCapture>,
    /// Detached syncs only: keeps the lease alive (`sync start` acquires then
    /// exits → driver renews for the pods' lifetime). Unset for an ordinary run
    pub(crate) sync_lease: std::sync::OnceLock<crate::qos::ledger::Reservation>,
}

impl std::fmt::Debug for EnvInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvInner")
            .field("namespace", &self.namespace.lock().ok().and_then(|g| g.clone()))
            .field("in_cluster", &self.in_cluster)
            .field("is_built", &self.is_built.load(Ordering::Relaxed))
            .finish()
    }
}

impl EnvInner {
    fn new() -> Self {
        EnvInner {
            client: OnceLock::new(),
            namespace: std::sync::Mutex::new(None),
            components: tokio::sync::RwLock::new(HashMap::new()),
            in_cluster: cluster::in_cluster(),
            forwards: Arc::new(Mutex::new(HashMap::new())),
            seed_bindings: std::sync::Mutex::new(Vec::new()),
            is_built: AtomicBool::new(false),
            log_capture: std::sync::OnceLock::new(),
            sync_lease: std::sync::OnceLock::new(),
        }
    }

    pub(crate) fn client_ref(&self) -> Result<&Client, EnvError> {
        self.client.get().ok_or(EnvError::NotBuilt)
    }

    pub(crate) async fn component_state(&self, id: u64) -> Result<ComponentState, EnvError> {
        let map = self.components.read().await;
        map.get(&id).cloned().ok_or(EnvError::UnknownComponent { id })
    }

    pub(crate) async fn resolve_named(
        &self,
        state: &ComponentState,
        name: &str,
    ) -> Result<Endpoint, EnvError> {
        let port =
            state.named_ports.iter().find_map(|(n, p)| (n == name).then_some(*p)).ok_or_else(
                || EnvError::UnknownEndpoint {
                    component: state.label.to_string(),
                    name: name.to_string(),
                },
            )?;
        self.resolve_port(state, port).await
    }

    pub(crate) async fn resolve_port(
        &self,
        state: &ComponentState,
        container_port: u16,
    ) -> Result<Endpoint, EnvError> {
        let client = self.client_ref()?;
        if self.in_cluster {
            let api: Api<Pod> = Api::namespaced(client.clone(), &state.namespace);
            let pod = api.get(&state.pod_name).await.map_err(env_err)?;
            let host: IpAddr = pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.as_deref())
                .ok_or_else(|| EnvError::NotReady {
                    component: state.pod_name.clone(),
                    elapsed: std::time::Duration::ZERO,
                })?
                .parse()
                .map_err(|e: std::net::AddrParseError| env_err(e))?;
            return Ok(Endpoint { host, port: container_port });
        }

        let key = (state.pod_name.clone(), container_port);
        let mut forwards = self.forwards.lock().await;
        if let Some(fw) = forwards.get(&key) {
            return Ok(Endpoint { host: IpAddr::V4(Ipv4Addr::LOCALHOST), port: fw.local_port });
        }
        let fw = Forwarder::start(
            client.clone(),
            state.namespace.clone(),
            state.pod_name.clone(),
            container_port,
        )
        .await
        .map_err(|e| EnvError::PortForwardFailed {
            component: state.pod_name.clone(),
            port: container_port,
            reason: e.to_string(),
        })?;
        let local_port = fw.local_port;
        forwards.insert(key, Arc::new(fw));
        Ok(Endpoint { host: IpAddr::V4(Ipv4Addr::LOCALHOST), port: local_port })
    }
}

// ──────────────────────── pending entries ─────────────────────────────

struct PendingValidator {
    id: u64,
    /// `take`n when applied
    materialize: Option<RegtestMaterializeFn>,
    handle: Arc<dyn ValidatorBackend>,
    opts: ComponentOpts,
}

struct PendingIndexer {
    id: u64,
    /// Retained so `build` renders the spec via [`IndexerBackend::pod_spec`], not a label match
    handle: Arc<dyn IndexerBackend>,
    /// `Some` iff the indexer opted into a network mode; `take`n when applied
    materialize: Option<IndexerMaterializeFn>,
    opts: ComponentOpts,
}

struct PendingWallet {
    opts: ComponentOpts,
}

// ──────────────────────────── shared volume ───────────────────────────

/// Env-scoped `ReadWriteOnce` PVC shared by two co-scheduled pods.
///
/// - Declared via [`TestEnv::shared_volume`], provisioned during [`TestEnv::build`]
/// - [`.mount(&vol)`](crate::ComponentBuilder::mount) on a zebrad + a
///   `.tuning(ZainoTuning::State)` zaino → one on-disk zebra-state DB
#[derive(Debug, Clone)]
pub struct SharedVolume {
    claim: String,
    mount_path: String,
}

impl SharedVolume {
    pub fn claim(&self) -> &str {
        &self.claim
    }
    /// In-pod mount path, identical in both sharing pods (zebra's `db_path` must resolve
    /// to one directory)
    pub fn mount_path(&self) -> &str {
        &self.mount_path
    }
    /// The [`Mount`](crate::Mount) attaching this volume at its canonical path, for
    /// [`ComponentBuilder::mount`](crate::ComponentBuilder::mount)
    pub fn as_mount(&self) -> crate::mount::Mount {
        crate::mount::Mount::shared(self.claim.clone(), self.mount_path.clone())
    }
}

impl From<&SharedVolume> for crate::mount::Mount {
    fn from(vol: &SharedVolume) -> Self {
        vol.as_mount()
    }
}

// ────────────────────────────── TestEnv ───────────────────────────────

pub struct TestEnv {
    inner: Arc<EnvInner>,
    pending_validators: Vec<PendingValidator>,
    pending_indexers: Vec<PendingIndexer>,
    pending_wallets: Vec<PendingWallet>,
    pending_shared_volumes: Vec<String>,
    next_id: u64,
    ready_timeout: Duration,
    /// Schedule of the *regtest* chain this env mines (`None` =
    /// [`ActivationHeights::regtest_default`]); unrelated to `chain_pin`, which records
    /// the schedule an archived chain already has
    activation_override: Option<ActivationHeights>,
    /// The one archive every restoring component names + what its manifest says it holds;
    /// resolved in [`build`](Self::build), `None` when nothing (or no chain) was restored
    chain_pin: Option<(crate::ArchiveHandle, crate::ChainInfo)>,
}

impl std::fmt::Debug for TestEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestEnv")
            .field("inner", &self.inner)
            .field("pending_validators", &self.pending_validators.len())
            .field("pending_indexers", &self.pending_indexers.len())
            .field("pending_wallets", &self.pending_wallets.len())
            .finish()
    }
}

impl TestEnv {
    pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(20);

    pub fn builder() -> Self {
        Self {
            inner: Arc::new(EnvInner::new()),
            pending_validators: Vec::new(),
            pending_indexers: Vec::new(),
            pending_wallets: Vec::new(),
            pending_shared_volumes: Vec::new(),
            next_id: 0,
            ready_timeout: Self::DEFAULT_READY_TIMEOUT,
            activation_override: None,
            chain_pin: None,
        }
    }

    /// Override the per-component readiness/RPC-probe budget for [`build`](Self::build).
    /// Order-independent relative to `add_*`
    pub fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// Pin an explicit regtest activation schedule, overriding
    /// [`ActivationHeights::regtest_default`]. Validated at [`build`](Self::build):
    /// activated upgrades = a contiguous prefix, heights non-decreasing
    pub fn activation_heights(mut self, heights: ActivationHeights) -> Self {
        self.activation_override = Some(heights);
        self
    }

    /// Declare an env-scoped shared volume to
    /// [`.mount(&vol)`](crate::ComponentBuilder::mount) on both a zebrad and a
    /// `.tuning(ZainoTuning::State)` zaino (PVC provisioned during [`TestEnv::build`])
    pub fn shared_volume(&mut self, name: &str) -> SharedVolume {
        let slug = short_kind(name);
        let claim = format!("shared-{slug}");
        self.pending_shared_volumes.push(claim.clone());
        SharedVolume { claim, mount_path: format!("/shared/{slug}") }
    }

    fn fresh_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Register a validator, returning its concrete handle (e.g. `ZebraValidator`).
    /// Backend RPCs are inherent methods → calling one on the wrong backend won't compile
    pub fn add_validator<B: ValidatorConfig>(&mut self, mut v: Validator<B>) -> B::Handle {
        let id = self.fresh_id();
        // Pin the resolved pool into `opts` (deferred regtest materialization renders the
        // matching miner address) and into the plumbing (`funded_faucet` picks its path)
        let coinbase_pool =
            v.opts.coinbase_pool.unwrap_or_else(|| v.backend.default_coinbase_pool());
        v.opts.coinbase_pool = Some(coinbase_pool);
        let plumbing = HandleInner {
            inner: Arc::downgrade(&self.inner),
            component_id: id,
            regtest: v.opts.regtest,
            coinbase_pool: Some(coinbase_pool),
        };
        // Backend not retained → capture regtest materialization as a closure, applied
        // once the activation heights are chosen
        let handle = v.backend.to_handle(plumbing);
        let dyn_handle: Arc<dyn ValidatorBackend> = Arc::new(handle.clone());
        let backend = v.backend;
        let materialize: RegtestMaterializeFn = Box::new(move |opts, activation, peers| {
            backend.materialize_regtest_opts(opts, activation, peers)
        });
        self.pending_validators.push(PendingValidator {
            id,
            materialize: Some(materialize),
            handle: dyn_handle,
            opts: v.opts,
        });
        handle
    }

    /// Register an indexer, returning its concrete handle (e.g. `ZainoIndexer`)
    pub fn add_indexer<B: IndexerConfig>(&mut self, i: Indexer<B>) -> B::Handle {
        let id = self.fresh_id();
        let plumbing = HandleInner {
            inner: Arc::downgrade(&self.inner),
            component_id: id,
            regtest: i.opts.regtest,
            coinbase_pool: None,
        };
        let handle = i.backend.to_handle(plumbing);
        let dyn_handle: Arc<dyn IndexerBackend> = Arc::new(handle.clone());
        // Any indexer in a network mode: renders backend + mode config once the validator
        // host resolves at build time
        let materialize: Option<IndexerMaterializeFn> =
            (i.mode != crate::component::IndexerMode::None).then(|| {
                let backend = i.backend;
                let tunings = i.tunings.clone();
                let mode = i.mode.clone();
                Box::new(move |opts, validator_host: Option<&str>| {
                    backend.materialize_opts(opts, &tunings, &mode, validator_host)
                }) as IndexerMaterializeFn
            });
        self.pending_indexers.push(PendingIndexer {
            id,
            handle: dyn_handle,
            materialize,
            opts: i.opts,
        });
        handle
    }

    /// Register an in-process wallet, returning its concrete handle (e.g. `ZingoWallet`)
    pub fn add_wallet<B: WalletConfig>(&mut self, w: Wallet<B>) -> B::Handle {
        let id = self.fresh_id();
        let plumbing = HandleInner {
            inner: Arc::downgrade(&self.inner),
            component_id: id,
            regtest: w.opts.regtest,
            coinbase_pool: None,
        };
        let handle = w.backend.to_handle(plumbing);
        self.pending_wallets.push(PendingWallet { opts: w.opts });
        handle
    }

    fn validate_topology(&self) -> Result<(), EnvError> {
        // v1 caps, not fundamental: build resolves one validator host
        // (`materialize_configs` takes `…next()`), handles assume ≤2 indexers + 1 wallet
        if self.pending_indexers.len() > 2 {
            return Err(EnvError::Config {
                reason: format!(
                    "v1 supports at most two indexers per env (found {})",
                    self.pending_indexers.len()
                ),
            });
        }
        if self.pending_wallets.len() > 1 {
            return Err(EnvError::Config {
                reason: format!(
                    "v1 supports at most one wallet per env (found {})",
                    self.pending_wallets.len()
                ),
            });
        }

        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let names = self
            .pending_validators
            .iter()
            .map(|p| pod_name_of(&p.opts))
            .chain(self.pending_indexers.iter().map(|p| pod_name_of(&p.opts)))
            .chain(self.pending_wallets.iter().map(|p| pod_name_of(&p.opts)));
        for name in names {
            if !seen.insert(name.clone()) {
                return Err(EnvError::Config {
                    reason: format!("duplicate component name `{name}`"),
                });
            }
        }

        Ok(())
    }

    /// Resolve the one archive this env restores; ≤1 chain in play is what makes
    /// [`chain`](Self::chain) — one answer for the whole env — well-defined.
    ///
    /// - Validator version ≠ producer version → in-place state-DB upgrade, or open failure
    /// - Components pinned to different artifacts → parity assertions compare two histories
    /// - Public archive at/without an activation → boundary assertions pass over empty data
    fn resolve_snapshot_pin(&mut self) -> Result<(), EnvError> {
        // Chain-metadata archives only (a bare tarball pins no history, names no producer)
        let pinned: Vec<(
            String,
            crate::ArchiveHandle,
            crate::ChainInfo,
            &str,
            Option<crate::ArchiveNetwork>,
        )> = self
            .pending_validators
            .iter()
            .map(|p| &p.opts)
            .chain(self.pending_indexers.iter().map(|p| &p.opts))
            .filter_map(|opts| match opts.restore.as_ref() {
                Some(crate::component::RestoreSource::Archive(h)) => Some((
                    pod_name_of(opts),
                    *h,
                    h.chain()?,
                    opts.version.as_str(),
                    opts.claimed_network,
                )),
                _ => None,
            })
            .collect();

        assert_claims_match_artifacts(&pinned)?;

        for (name, handle, chain, version, _) in &pinned {
            // An indexer's `version` is zaino's, no bearing on zebra's on-disk format
            let is_validator =
                self.pending_validators.iter().any(|p| &pod_name_of(&p.opts) == name);
            if is_validator && *version != chain.version() {
                return Err(EnvError::Config {
                    reason: format!(
                        "{name} runs version {version} but is pinned to snapshot {}, \
                         produced by {}; a validator reading a state DB written by a \
                         different release either upgrades it in place or fails to open it",
                        handle.name(),
                        chain.version(),
                    ),
                });
            }
        }

        // Identity = OID, not path/name → two declarations of one archive agree by
        // construction
        if let Some(((first_name, first, ..), (other_name, other, ..))) =
            pinned.first().zip(pinned.iter().find(|(_, h, ..)| h.oid() != pinned[0].1.oid()))
        {
            return Err(EnvError::Config {
                reason: format!(
                    "components in this env are pinned to different chain snapshots: \
                     {first_name} serves {} and {other_name} serves {}; they must name \
                     the same artifact or every comparison between them is measuring two \
                     different histories",
                    first.name(),
                    other.name(),
                ),
            });
        }

        let Some((_, handle, chain, _, _)) = pinned.first() else {
            return Ok(()); // nothing restored → no chain pin
        };

        // Public archive = immutable, height-pinned (empty peer set → tip never moves);
        // a regtest cache is mined onto, so none of the claims below hold for one
        if chain.network().is_public() {
            let reason = match chain.straddled_activation_opt() {
                None => Some("its manifest records no activation at all".to_owned()),
                Some(straddled) if straddled.upgrade_name().is_none() => Some(format!(
                    "its newest activation is `{}`, which no RPC reports as an upgrade",
                    straddled.key,
                )),
                Some(_) if chain.mature_height() <= chain.activation() => Some(format!(
                    "it is pinned at {}, which leaves no mature history above its {} \
                     activation at {}",
                    chain.tip_height(),
                    chain.upgrade_name(),
                    chain.activation(),
                )),
                Some(_) => None,
            };
            if let Some(reason) = reason {
                return Err(EnvError::Config {
                    reason: format!(
                        "{} cannot serve as a chain fixture: {reason}; a snapshot that \
                         does not straddle an upgrade with room to spare holds \
                         essentially none of the data it is named for, and every \
                         assertion drawn from it passes while proving nothing",
                        handle.name(),
                    ),
                });
            }
        }

        self.chain_pin = Some((*handle, *chain));
        Ok(())
    }

    /// What the restored chain contains — pinned tip, straddled upgrade, activation
    /// schedule, heights worth querying. Manifest-read at compile time, verified against
    /// the running validator in [`build`](Self::build)
    ///
    /// # Panics
    ///
    /// No component restored an archive, or the archive carries no chain metadata
    pub fn chain(&self) -> crate::ChainInfo {
        match self.chain_pin {
            Some((_, chain)) => chain,
            None => panic!(
                "this env names no chain archive, so it has no chain to describe; \
                 `TestEnv::chain` answers for the artifact a component was built with \
                 `.testnet(..)`",
            ),
        }
    }

    /// The archive [`chain`](Self::chain) describes, for diagnostics naming the artifact
    /// a failure came from. Panics on the same conditions
    pub fn chain_archive(&self) -> crate::ArchiveHandle {
        match self.chain_pin {
            Some((handle, _)) => handle,
            None => panic!(
                "this env names no chain archive; `TestEnv::chain_archive` names the \
                 artifact a component was built with `.testnet(..)`",
            ),
        }
    }

    fn materialize_configs(&mut self) -> Result<(), EnvError> {
        let activation = match &self.activation_override {
            None => ActivationHeights::regtest_default(),
            Some(heights) => {
                heights.validate_schedule().map_err(|reason| EnvError::Config { reason })?;
                *heights
            }
        };
        tracing::debug!(?activation, "regtest activation-height schedule chosen");

        let p2p_port = crate::handles::ports::ZEBRAD_P2P;
        let known_validators: std::collections::HashSet<String> =
            self.pending_validators.iter().map(|p| pod_name_of(&p.opts)).collect();
        let peer_tuples_for = |opts: &ComponentOpts| -> Result<Vec<(String, u16)>, EnvError> {
            opts.peers
                .iter()
                .map(|name| {
                    let host = short_kind(name);
                    if !known_validators.contains(&host) {
                        return Err(EnvError::Config {
                            reason: format!(
                                "validator peer {name:?} not found in this env's \
                                 topology (known: {known_validators:?})"
                            ),
                        });
                    }
                    Ok((host, p2p_port))
                })
                .collect()
        };

        // Validators: dispatch through backend trait method.
        let pending = std::mem::take(&mut self.pending_validators);
        let mut materialized = Vec::with_capacity(pending.len());
        for mut p in pending {
            if p.opts.regtest
                && let Some(materialize) = p.materialize.take()
            {
                let peers = peer_tuples_for(&p.opts)?;
                p.opts = materialize(p.opts, &activation, &peers)?;
            }
            materialized.push(p);
        }
        self.pending_validators = materialized;

        let validator_host = self.pending_validators.iter().map(|p| pod_name_of(&p.opts)).next();
        let pending = std::mem::take(&mut self.pending_indexers);
        let mut materialized = Vec::with_capacity(pending.len());
        for mut p in pending {
            if let Some(materialize) = p.materialize.take() {
                p.opts = materialize(p.opts, validator_host.as_deref())?;
            }
            materialized.push(p);
        }
        self.pending_indexers = materialized;

        Ok(())
    }

    pub async fn build(&mut self) -> Result<(), EnvError> {
        // Orchestrated → diagnostics to stdout, riding the pod-log capture path (the
        // reporter shows them per `--success-output`)
        crate::observ::init_in_pod();
        cluster::require_orchestrator()?;
        self.validate_topology()?;
        self.resolve_snapshot_pin()?;
        self.materialize_configs()?;

        let started = std::time::Instant::now();
        let coords = RunCoords::from_env().map_err(env_err)?;
        // Raw `module::test` for the namespace annotation, DNS slug for every label value
        // (`::` is illegal in a label)
        let test_raw = naming::current_test_name();
        let package = naming::current_package();
        let test_slug = naming::slug(&test_raw, naming::DNS_LABEL_MAX);
        // Pod path: parent `ztest run` created (and tears down) the namespace and injected
        // its name, so it can follow every pod over the kube API → reuse it, skip creation.
        // Unset ⇒ local path invents the name and owns the whole lifecycle in-process
        let laptop_owned = std::env::var(naming::TEST_NAMESPACE_ENV).ok().filter(|v| !v.is_empty());
        let namespace = laptop_owned
            .clone()
            .unwrap_or_else(|| naming::namespace_for(&package, &test_raw, &naming::test_suffix()));
        let client = cluster::client().await.map_err(env_err)?;

        let tier = qos::current();
        tracing::info!(
            test = %test_raw,
            tier = tier.as_label(),
            // Effective reserve (what this run holds, not its tier's default)
            reservation = %qos::current_profile().footprint,
            validators = self.pending_validators.len(),
            indexers = self.pending_indexers.len(),
            wallets = self.pending_wallets.len(),
            namespace = %namespace,
            "starting test run"
        );

        // Pre-pod (reservation must already cover them; CLI stopped renewing)
        self.hold_sync_reservation(&client, &coords);

        // Local path creates the namespace; the pod path already has one. The quota stays
        // ours either way (sized from a topology only known in-pod)
        if laptop_owned.is_none() {
            cluster::ensure_namespace(&client, &namespace, &coords, &package, &test_raw)
                .await
                .map_err(env_err)?;
        }
        // Cap at the tier component budget = what the scheduler reserved, so the
        // real bound on any `.resources()` override (apiserver enforces at create;
        // namespace delete cascades)
        let pod_count = self.pending_validators.len() + self.pending_indexers.len();
        if pod_count > 0 {
            let footprint = qos::current_profile().footprint;
            cluster::apply_resource_quota(&client, &namespace, footprint, pod_count)
                .await
                .map_err(env_err)?;
        }
        build_phase("namespace_quota", started);
        let sentinel = Sentinel::new(namespace.clone());
        let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
        let test_name = test_slug;

        let _ = self.inner.client.set(client.clone());
        *self.inner.namespace.lock().expect("namespace mutex poisoned") = Some(namespace.clone());

        // One read → placement + default split + deploy ceiling all describe the
        // same reserve the ledger holds
        let effective = qos::current_profile();
        let qos_placement = Some(effective.pool);
        // §7 default sizing: footprint split evenly across the pods as
        // requests==limits (Guaranteed); a test's `.resources()` overrides it per pod
        let qos_guaranteed = even_share(pod_count);
        // Charged per-pod as both phases build specs → bounds the *sum* that
        // deploys (not each pod alone, not the default split they may not use)
        let mut budget = DeployBudget::new(effective.footprint);

        // Before any pod references them (WaitForFirstConsumer → the claim stays Pending
        // until the Phase-1 validator schedules)
        for claim in std::mem::take(&mut self.pending_shared_volumes) {
            mounts::create_shared_pvc(&client, &sentinel, &claim).await?;
        }

        let ctx = MaterializeCtx {
            client: &client,
            pods: &pods,
            sentinel: &sentinel,
            coords: &coords,
            test_name: &test_name,
        };

        // Phase 1: validators.
        let validators: Vec<_> = self
            .pending_validators
            .drain(..)
            .map(|p| {
                let pod_name = pod_name_of(&p.opts);
                let mut spec = p.handle.pod_spec(&p.opts, pod_name)?;
                spec.placement = qos_placement;
                if spec.resources.is_none() {
                    spec.guaranteed = qos_guaranteed;
                }
                budget.admit(&spec);
                Ok::<_, EnvError>((p.id, spec, p.opts, ComponentHandle::Validator(p.handle)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let t_phase = std::time::Instant::now();
        self.materialize_phase(&ctx, &validators).await?;
        build_phase("validators_materialize", t_phase);
        // Probes resolve endpoints through the handles, which gate on `is_built` → on for
        // the probe window, off after (a Phase-2 failure must still report `NotBuilt`)
        self.inner.is_built.store(true, Ordering::Release);
        let t_phase = std::time::Instant::now();
        let warmup = async {
            self.wait_validators_rpc_ready().await?;
            self.warm_validators().await?;
            self.verify_restored_chain().await?;
            Ok::<(), EnvError>(())
        }
        .await;
        self.inner.is_built.store(false, Ordering::Release);
        build_phase("validators_ready_warm", t_phase);
        warmup?;

        // Phase 2: indexers. (Wallets run in-process; see below.)
        let dependents: Vec<_> = self
            .pending_indexers
            .drain(..)
            .map(|p| {
                let pod_name = pod_name_of(&p.opts);
                let mut spec = p.handle.pod_spec(&p.opts, pod_name)?;
                spec.placement = qos_placement;
                if spec.resources.is_none() {
                    spec.guaranteed = qos_guaranteed;
                }
                budget.admit(&spec);
                Ok::<_, EnvError>((p.id, spec, p.opts, ComponentHandle::Indexer(p.handle)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Wallets run in-process over gRPC → no pod; accounts built lazily via
        // `WalletHandle::account`
        self.pending_wallets.clear();
        let t_phase = std::time::Instant::now();
        self.materialize_phase(&ctx, &dependents).await?;
        build_phase("indexers_materialize", t_phase);

        // Local path only: one arrival-ordered, pod-prefixed stream for the rest of the
        // test (`--tail=-1` backfills Phase 1), rendered in `Drop` on failure and discarded
        // on success. Pod path: parent owns it over the kube API (runner image has no
        // `kubectl` to shell)
        if laptop_owned.is_none() {
            let component_pods = self.inner.components.read().await.len();
            let _ = self
                .inner
                .log_capture
                .set(crate::logstream::LogCapture::spawn(&namespace, component_pods));
        }

        // Indexer analogue of `wait_validators_rpc_ready`: pod-Ready ≠ gRPC listener bound
        // (zainod serves only after its initial chain-index build, which can lag Ready by
        // minutes under load), so gate on a live `GetLightdInfo`. `is_built` window as above
        self.inner.is_built.store(true, Ordering::Release);
        let t_phase = std::time::Instant::now();
        let ready = self.wait_indexers_rpc_ready().await;
        self.inner.is_built.store(false, Ordering::Release);
        build_phase("indexers_ready", t_phase);
        ready?;

        self.inner.is_built.store(true, Ordering::Release);
        crate::sync::note_setup("topology", None, "ready — starting the engine");

        tracing::debug!(
            target: "ztest::build",
            namespace = %namespace,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "TestEnv ready"
        );
        Ok(())
    }

    /// Prove the validator serves the chain its manifest claims: tip, the whole activation
    /// schedule, and the producer's boundary evidence re-checked against the *mounted*
    /// bytes (which diverge on a truncated extraction / partly-populated seed PVC).
    ///
    /// - Ordered before any indexer deploys (fails by name in seconds, not as a parity
    ///   mismatch hundreds of lines into a test)
    /// - Public networks only (a regtest cache's tip is *meant* to move, and its schedule
    ///   comes from [`activation_heights`](Self::activation_heights), not a manifest)
    async fn verify_restored_chain(&self) -> Result<(), EnvError> {
        let Some((handle, chain)) = self.chain_pin else {
            return Ok(());
        };
        if !chain.network().is_public() {
            return Ok(());
        }
        // Any validator: `resolve_snapshot_pin` already established they serve one artifact
        let validator = {
            let comps = self.inner.components.read().await;
            comps.values().find_map(|s| match &s.handle {
                ComponentHandle::Validator(h) => Some(Arc::clone(h)),
                ComponentHandle::Indexer(_) => None,
            })
        };
        let Some(validator) = validator else {
            return Ok(()); // indexer-only env, nothing authoritative to ask
        };

        crate::sync::note_setup("validator", None, "verifying the restored chain");
        let rpc = validator.json_rpc().await?;
        let mismatch = |reason: String| EnvError::ArchiveMismatch {
            archive: handle.name().to_owned(),
            reason,
        };
        let transport = |e: crate::RpcError| EnvError::Transient(Box::new(e));

        let tip = rpc.tip_height().await.map_err(transport)?;
        if tip != chain.tip_height() {
            return Err(mismatch(format!(
                "its manifest pins the tip at {}, but the validator serves {tip}",
                chain.tip_height(),
            )));
        }

        for activation in chain.activations() {
            // `before_overwinter` = the absence of an upgrade; no RPC reports it
            let Some(name) = activation.upgrade_name() else {
                continue;
            };
            let reported = rpc.activation_height(name).await.map_err(transport)?;
            if reported != activation.height {
                return Err(mismatch(format!(
                    "its manifest records {name} activating at {}, but the validator \
                     reports {reported}",
                    activation.height,
                )));
            }
        }

        if let Some(check) = chain.boundary_check() {
            // Below the activation the pool must be empty; at the pinned tip it holds the
            // producer's recorded value
            for (height, expected, position) in [
                (check.from_height - 1, check.value_before, "below"),
                (check.to_height, check.value_after, "at the tip above"),
            ] {
                let observed = rpc.pool_zats(height, check.pool).await.map_err(transport)?;
                if observed != expected {
                    return Err(mismatch(format!(
                        "the {} pool holds {observed} zats at height {height}, {position} \
                         the {} activation, where its producer recorded {expected}",
                        check.pool,
                        chain.upgrade_name(),
                    )));
                }
            }
        }

        tracing::debug!(
            target: "ztest::build",
            archive = handle.name(),
            tip,
            activations = chain.activations().len(),
            "restored chain verified against its manifest"
        );
        Ok(())
    }

    async fn wait_validators_rpc_ready(&self) -> Result<(), EnvError> {
        // Probes drive the handle: it resolves its own endpoint + picks the backend's
        // readiness RPC
        let validators: Vec<(String, Arc<dyn ValidatorBackend>)> = {
            let comps = self.inner.components.read().await;
            comps
                .values()
                .filter_map(|s| match &s.handle {
                    ComponentHandle::Validator(h) => Some((s.pod_name.clone(), Arc::clone(h))),
                    ComponentHandle::Indexer(_) => None,
                })
                .collect()
        };
        if validators.is_empty() {
            return Ok(());
        }
        crate::sync::note_setup("validator", None, "waiting for RPC readiness");

        let timeout = self.ready_timeout;
        let probes = validators.into_iter().map(|(pod_name, handle)| async move {
            handle.ready(timeout).await.map_err(|_| EnvError::RpcTimeout {
                component: pod_name,
                op: "wait_for_ready",
                elapsed: timeout,
            })
        });
        for res in join_all(probes).await {
            res?;
        }
        Ok(())
    }

    /// Indexer counterpart of [`Self::wait_validators_rpc_ready`]: block until every
    /// indexer answers gRPC `GetLightdInfo`, or `ready_timeout` elapses
    async fn wait_indexers_rpc_ready(&self) -> Result<(), EnvError> {
        let indexers: Vec<(String, Arc<dyn IndexerBackend>)> = {
            let comps = self.inner.components.read().await;
            comps
                .values()
                .filter_map(|s| match &s.handle {
                    ComponentHandle::Indexer(h) => Some((s.pod_name.clone(), Arc::clone(h))),
                    ComponentHandle::Validator(_) => None,
                })
                .collect()
        };
        if indexers.is_empty() {
            return Ok(());
        }
        crate::sync::note_setup("indexer", None, "waiting for gRPC GetLightdInfo");

        let timeout = self.ready_timeout;
        let probes = indexers.into_iter().map(|(pod_name, handle)| async move {
            handle.ready(timeout).await.map_err(|_| EnvError::RpcTimeout {
                component: pod_name,
                op: "wait_for_ready",
                elapsed: timeout,
            })
        });
        for res in join_all(probes).await {
            res?;
        }
        Ok(())
    }

    /// Kube client, once [`build`](Self::build) has connected. Used by the facade's run
    /// tail: the detach stop-watch and the mirrored durable report
    #[cfg_attr(not(feature = "zingo"), allow(dead_code))]
    pub(crate) fn kube_client(&self) -> Option<Client> {
        self.inner.client.get().cloned()
    }

    /// Namespace this env provisioned into, once [`build`](Self::build) has run. Locates
    /// this run's stop-watch and report ConfigMaps
    #[cfg_attr(not(feature = "zingo"), allow(dead_code))]
    pub(crate) fn namespace(&self) -> Option<String> {
        self.inner.namespace.lock().ok().and_then(|g| g.clone())
    }

    /// The one indexer in this topology, type-erased for a sync run's `SyncCtx` oracle.
    /// Errors on zero or >1 (a differential 2-indexer topology is a load-test shape)
    #[cfg_attr(not(feature = "zingo"), allow(dead_code))]
    pub(crate) async fn single_indexer(&self) -> Result<Arc<dyn IndexerBackend>, EnvError> {
        let comps = self.inner.components.read().await;
        let mut indexers = comps.values().filter_map(|s| match &s.handle {
            ComponentHandle::Indexer(h) => Some(Arc::clone(h)),
            ComponentHandle::Validator(_) => None,
        });
        match (indexers.next(), indexers.next()) {
            (Some(h), None) => Ok(h),
            (None, _) => Err(EnvError::Config {
                reason: "sync oracle needs an indexer, but the topology has none".into(),
            }),
            (Some(_), Some(_)) => Err(EnvError::Config {
                reason: "sync oracle is ambiguous: topology has more than one indexer".into(),
            }),
        }
    }

    async fn warm_validators(&self) -> Result<(), EnvError> {
        // Regtest only: mine one block per validator so indexers sync against a non-genesis
        // tip. A restored chain is already at its snapshot height; on a live network zebrad
        // refuses `generate` outright ("only supported on networks where PoW is disabled")
        let handles: Vec<Arc<dyn ValidatorBackend>> = {
            let comps = self.inner.components.read().await;
            comps
                .values()
                .filter_map(|s| match &s.handle {
                    ComponentHandle::Validator(h) if h.is_regtest() => Some(Arc::clone(h)),
                    ComponentHandle::Validator(_) | ComponentHandle::Indexer(_) => None,
                })
                .collect()
        };
        if !handles.is_empty() {
            crate::sync::note_setup("validator", None, "mining the warm-up block");
        }
        for handle in handles {
            handle.generate_blocks(1).await.map_err(|e| EnvError::Transient(Box::new(e)))?;
        }
        Ok(())
    }

    /// Detached syncs only: adopt the lease `ztest sync start` acquired
    ///
    /// - CLI must acquire (admission refusable while watched) then exits
    /// - Driver's lifetime = the pods' → it holds from here
    /// - Renewal idempotent → the CLI's overlapping heartbeat is harmless
    /// - Dropped with `TestEnv` → lease lapses at TTL
    fn hold_sync_reservation(&self, client: &Client, coords: &RunCoords) {
        if crate::sync::active_sync_id().is_none() {
            return;
        }
        let reserve = qos::current_profile().admitted();
        let held =
            crate::qos::ledger::Reservation::adopt(client, &coords.run_id, &coords.user, reserve);
        if self.inner.sync_lease.set(held).is_ok() {
            tracing::info!(lease = %coords.run_id, %reserve, "detached sync: holding reservation");
        }
    }

    async fn materialize_phase(
        &self,
        ctx: &MaterializeCtx<'_>,
        items: &[MaterializeItem],
    ) -> Result<(), EnvError> {
        deploy_ebpf_collector(ctx).await;
        for (id, spec, opts, handle) in items {
            // One INFO per provisioned pod; both build phases funnel here, so this is the
            // single place covering every pod
            let reservation = spec
                .resources
                .as_ref()
                .or(spec.guaranteed.as_ref())
                .map(|r| r.to_string())
                .unwrap_or_else(|| "unset".to_string());
            tracing::info!(
                component = spec.category.as_str(),
                name = %spec.pod_name,
                image = %image_summary(&spec.image),
                reservation = %reservation,
                "provisioning component"
            );
            crate::sync::note_setup(spec.category.as_str(), Some(&spec.pod_name), "creating pod");
            let state = ComponentState::new(spec, ctx.sentinel.namespace.clone(), handle.clone());
            cluster::create_pod_service(
                ctx.client,
                &ctx.sentinel.namespace,
                &spec.pod_name,
                &spec.ports,
            )
            .await
            .map_err(env_err)?;
            // Own step: resolving a seed mount can fetch an archive through the storage
            // backend (minutes of network, not a local call)
            if !opts.mounts.is_empty() {
                crate::sync::note_setup(
                    spec.category.as_str(),
                    Some(&spec.pod_name),
                    "resolving mounts and seeds",
                );
            }
            let resolved =
                mounts::resolve_all(ctx.client, ctx.sentinel, &spec.pod_name, &opts.mounts).await?;
            self.inner
                .seed_bindings
                .lock()
                .expect("seed_bindings mutex poisoned")
                .extend(resolved.seed_bindings);
            apply_pod(ctx, spec, &resolved.mounts).await?;
            self.inner.components.write().await.insert(*id, state);
        }

        // One event for the gate, not per pod (the waits run concurrently → "which pod are
        // we on" has no answer)
        if let Some((_, spec, _, _)) = items.first() {
            crate::sync::note_setup(
                spec.category.as_str(),
                None,
                &format!("waiting for {} pod(s) to reach Ready", items.len()),
            );
        }

        let timeout = self.ready_timeout;
        let waits = items.iter().map(|(_, spec, _, _)| {
            let pods = ctx.pods.clone();
            let name = spec.pod_name.clone();
            async move { await_pod_ready(&pods, &name, timeout).await }
        });
        for res in join_all(waits).await {
            res?;
        }
        Ok(())
    }
}

impl Drop for TestEnv {
    /// Local-path teardown. `Drop`, not a `teardown().await` an early `?` could skip —
    /// that leak filled the cluster's pod cap and timed out every later test.
    ///
    /// - Pod path (`ZTEST_TEST_NAMESPACE`) = no-op: parent owns logs + teardown over kube
    /// - Delete runs on its own OS thread + runtime, `join`ed (`Drop` can't await, and a
    ///   spawned task dies with the test runtime before its DELETE is sent), with a client
    ///   rebuilt inside it (the original is bound to the dying reactor)
    /// - `ZTEST_NO_CLEANUP` suppresses the delete; the 1h `janitor/ttl` still reaps it
    fn drop(&mut self) {
        // Deleting here would race the parent's still-draining collector
        if std::env::var(naming::TEST_NAMESPACE_ENV).is_ok_and(|v| !v.is_empty()) {
            return;
        }

        // Stop the follow child + snapshot the timeline, emitted below after the dead-pod
        // headers. Colour is the reporter's call, propagated by the engine as `ZTEST_COLOR`
        let color = std::env::var("ZTEST_COLOR").ok().as_deref() == Some("1");
        let timeline = self.inner.log_capture.get().and_then(|c| {
            c.stop();
            c.render(color)
        });

        let ns = self.inner.namespace.lock().ok().and_then(|mut g| g.take());
        let bindings: Vec<_> = self
            .inner
            .seed_bindings
            .lock()
            .ok()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default();
        if ns.is_none() && bindings.is_empty() {
            return;
        }

        // `--no-cleanup` preserves the namespace + seed bindings for inspection
        let cleanup = !cluster::no_cleanup_requested();
        let (ns_to_delete, bindings_to_delete) = if cleanup {
            (ns.clone(), bindings)
        } else {
            if let Some(ns) = &ns {
                // eprintln, not tracing: the hint must land in captured test output
                eprintln!(
                    "ztest: --no-cleanup — preserving namespace {ns} for inspection \
                     (janitor reaps it in ~1h).\n  \
                     inspect: kubectl get pods -n {ns}\n  \
                     logs:    kubectl logs -n {ns} <pod>\n  \
                     delete:  kubectl delete ns {ns}"
                );
            }
            tracing::warn!(
                namespace = ?ns,
                seed_bindings = bindings.len(),
                "ZTEST_NO_CLEANUP set — leaving TestEnv namespace for inspection"
            );
            (None, Vec::new())
        };

        tracing::debug!(
            namespace = ?ns_to_delete,
            seed_bindings = bindings_to_delete.len(),
            "tearing down TestEnv (Drop)"
        );
        let ns_for_diag = ns.clone();
        let outcome = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("teardown runtime: {e}"))?;
            rt.block_on(async move {
                let client =
                    cluster::client().await.map_err(|e| format!("teardown client: {e}"))?;
                // Before the namespace delete takes each dead pod's reason + logs: the
                // client-side error is only "connection refused", which can't tell an
                // Evicted/OOMKilled pod (contention) from a panicked one (component bug)
                if let Some(ns) = &ns_for_diag {
                    let headers = cluster::dead_pod_report(&client, ns).await;
                    if !headers.is_empty() {
                        eprint!("{headers}");
                    }
                }
                if let Some(timeline) = &timeline {
                    eprintln!("ztest: component logs (interleaved, by arrival):");
                    eprint!("{timeline}");
                }
                if let Some(ns) = ns_to_delete {
                    cluster::delete_namespace(&client, &ns)
                        .await
                        .map_err(|e| format!("delete namespace {ns}: {e}"))?;
                }
                for binding in bindings_to_delete {
                    if let Err(e) = seeds::delete_binding(&client, &binding).await {
                        tracing::warn!(
                            error = %e,
                            content = %binding.binding_content,
                            "seed binding content delete failed"
                        );
                    }
                }
                Ok::<(), String>(())
            })
        })
        .join();
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "TestEnv teardown failed"),
            Err(_) => tracing::error!("TestEnv teardown thread panicked"),
        }
    }
}

// ─────────────────────────────── helpers ──────────────────────────────

/// Emit one [`TestEnv::build`] phase's elapsed time on the `ztest::build` target,
/// isolating which provisioning step a slow build spent its time in
fn build_phase(phase: &str, since: std::time::Instant) {
    tracing::debug!(
        target: "ztest::build",
        phase,
        elapsed_ms = since.elapsed().as_millis() as u64,
        "build phase"
    );
}

/// Image reference → its final `repo:tag` segment, for the provisioning diagnostics:
/// `registry.example.com/lib/zebra:v6.2.0` → `zebra:v6.2.0`
fn image_summary(image: &str) -> &str {
    image.rsplit('/').next().unwrap_or(image)
}

/// Wait for a dependency pod to reach Ready. Three deadlines, in sequence:
///
/// - unscheduled → `PENDING_TIMEOUT` (contention clears inside it, a bad placement never does)
/// - `Running` → `ready_timeout` (time-to-ready = the app's problem)
/// - `CrashLoopBackOff` / `OOMKilled` / terminal pull error / `Failed` → fail fast
/// - transient kube-API `get` errors ignored, as in [`crate::engine`]'s runner-pod loop
async fn await_pod_ready(
    pods: &Api<Pod>,
    name: &str,
    ready_timeout: Duration,
) -> Result<(), EnvError> {
    use crate::pod_status as ps;

    let mut unscheduled_since: Option<Instant> = None;
    let mut running_since: Option<Instant> = None;
    let mut pull_error_since: Option<Instant> = None;
    loop {
        if let Ok(pod) = pods.get(name).await
            && let Some(status) = pod.status.as_ref()
        {
            if ps::is_ready(status) {
                return Ok(());
            }
            // Placement first (no node → every check below is vacuous)
            if !ps::is_scheduled(status) {
                let since = *unscheduled_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= ps::PENDING_TIMEOUT {
                    return Err(EnvError::PodUnschedulable {
                        component: name.to_string(),
                        reason: ps::schedule_blocker(status)
                            .unwrap_or_else(|| "no PodScheduled condition".to_string()),
                        elapsed: since.elapsed(),
                    });
                }
            }
            if let Some(reason) = ps::fault(status) {
                return Err(EnvError::PodFailed { component: name.to_string(), reason });
            }
            match ps::image_error(status) {
                Some(reason) => {
                    let first = *pull_error_since.get_or_insert_with(Instant::now);
                    if ps::pull_error_is_terminal(
                        &reason,
                        first,
                        Instant::now(),
                        ps::IMAGE_PULL_GRACE,
                    ) {
                        return Err(EnvError::PodFailed { component: name.to_string(), reason });
                    }
                }
                None => pull_error_since = None,
            }
            if ps::is_running(status) {
                let since = *running_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= ready_timeout {
                    return Err(EnvError::RpcTimeout {
                        component: name.to_string(),
                        op: "pod_ready",
                        elapsed: ready_timeout,
                    });
                }
            }
        }
        tokio::time::sleep(ps::POLL_INTERVAL).await;
    }
}

fn short_kind(s: &str) -> String {
    let s: String = s
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect();
    let s = s.trim_matches('-').to_string();
    if s.is_empty() { "x".into() } else { s.chars().take(20).collect() }
}

/// Per-run eBPF collector, applied before the run's pods exist — discovery is a watch, so
/// a target created later is still picked up.
///
/// - Server-side apply → re-running per phase re-applies, never duplicates
/// - Never fails the run: an absent profile costs a diagnostic (same contract as the
///   in-process pusher)
async fn deploy_ebpf_collector(ctx: &MaterializeCtx<'_>) {
    if !crate::profiling::ebpf::requested() {
        return;
    }
    let Some(url) = crate::profiling::push_url(ctx.client).await else {
        tracing::warn!("eBPF profiling requested but no Pyroscope found; running unprofiled");
        return;
    };
    // Sync id else run id, matching the in-process tenant exactly — `ztest cleanup` retires
    // one tenant per sync and must reach both pushers
    let id = crate::sync::active_sync_id().unwrap_or_else(|| ctx.coords.run_id.clone());
    let tenant = crate::profiling::tenant(&crate::naming::current_user(), &id);
    let collector = crate::profiling::ebpf::Collector {
        namespace: &ctx.sentinel.namespace,
        tenant: &tenant,
        push_url: &url,
        hz: crate::profiling::ebpf::hz(),
    };
    match crate::profiling::ebpf::deploy(ctx.client, &collector).await {
        Ok(()) => tracing::info!(namespace = %ctx.sentinel.namespace, %tenant, "eBPF collector up"),
        Err(e) => tracing::warn!(%e, "eBPF collector failed; running unprofiled"),
    }
}

async fn apply_pod(
    ctx: &MaterializeCtx<'_>,
    spec: &PodSpec,
    mounts: &[ResolvedMount],
) -> Result<(), EnvError> {
    let pod = spec.render(ctx.coords, ctx.test_name, mounts)?;
    ctx.pods.create(&PostParams::default(), &pod).await.map(|_| ()).map_err(env_err)
}

fn pod_name_of(opts: &ComponentOpts) -> String {
    short_kind(opts.name.as_deref().unwrap_or("x"))
}

/// Panic when a `.resources()` override tops the whole tier footprint: capacity the
/// parent scheduler never admitted, so the pod would wedge `Pending` behind a quota it
/// can't satisfy. No-op without an override (the tier-derived reserve fits by construction)
fn assert_override_within_tier(spec: &PodSpec, tier: crate::qos::Resources) {
    let Some(res) = spec.resources.as_ref() else {
        return;
    };
    let (cpu, mem) = (res.cpu.millicores(), res.memory.as_bytes());
    let requested = crate::qos::Resources::new(cpu, mem, 0, 0);
    assert!(
        requested.fits_within(&tier),
        "pod {} .resources() override ({}m cpu / {} B) exceeds its tier footprint \
         ({}m cpu / {} B) — raise the test's QoS tier or lower the override",
        spec.pod_name,
        cpu,
        mem,
        tier.cpu_milli,
        tier.mem_bytes,
    );
}

/// What a rendered pod requests: `.resources()` override, else tier share
/// (= the number the scheduler packs against)
fn spec_request(spec: &PodSpec) -> crate::qos::Resources {
    spec.resources
        .as_ref()
        .or(spec.guaranteed.as_ref())
        .map(|r| crate::qos::Resources::new(r.cpu.millicores(), r.memory.as_bytes(), 0, 0))
        .unwrap_or(crate::qos::Resources::ZERO)
}

/// Bounds the *sum* of what a test's pods request
///
/// - Per-pod checks miss it: 9c + 9c each fit a 15c tier, together they do not
/// - Surplus wedges `Pending`
/// - Ceiling = tier component footprint (what the scheduler reserved, what the quota caps)
struct DeployBudget {
    tier: crate::qos::Resources,
    committed: crate::qos::Resources,
    /// Charged pods → a violation names the whole topology, not just the last
    admitted: Vec<(String, crate::qos::Resources)>,
}

impl DeployBudget {
    /// `tier` = effective component footprint, same number the quota + lease were sized from
    fn new(tier: crate::qos::Resources) -> Self {
        DeployBudget { tier, committed: crate::qos::Resources::ZERO, admitted: Vec::new() }
    }

    /// Charge one pod, panicking on either bound (pre-create → assert fires
    /// instead of the deploy)
    fn admit(&mut self, spec: &PodSpec) {
        assert_override_within_tier(spec, self.tier);
        let want = spec_request(spec);
        self.committed = self.committed.saturating_add(&want);
        self.admitted.push((spec.pod_name.clone(), want));
        assert!(
            self.committed.fits_within(&self.tier),
            "QoS over-schedule: this test's component pods request {}m cpu / {} B in total, \
             exceeding the {}m cpu / {} B its tier reserved for them.\n  {}\n\
             Each pod fits the tier on its own — it is the sum that does not. Lower the \
             `.resources()` overrides so they fit together, or raise the test's QoS tier.",
            self.committed.cpu_milli,
            self.committed.mem_bytes,
            self.tier.cpu_milli,
            self.tier.mem_bytes,
            self.admitted
                .iter()
                .map(|(name, r)| format!("{name}: {}m / {} B", r.cpu_milli, r.mem_bytes))
                .collect::<Vec<_>>()
                .join("\n  "),
        );
    }
}

/// Reject a component whose `.testnet(_)` / `.mainnet(_)` verb disagrees with the network
/// its archive records.
///
/// - Verb is redundant by construction (the config generator reads the network off the
///   artifact), so a disagreement means the call site names a chain nobody booted
/// - `.mainnet(testnet::ORCHARD)` would otherwise run green against the wrong history
fn assert_claims_match_artifacts(
    pinned: &[(
        String,
        crate::ArchiveHandle,
        crate::ChainInfo,
        &str,
        Option<crate::ArchiveNetwork>,
    )],
) -> Result<(), EnvError> {
    for (name, handle, chain, _, claimed) in pinned {
        if let Some(claimed) = claimed
            && *claimed != chain.network()
        {
            return Err(EnvError::Config {
                reason: format!(
                    "{name} was built with .{}(…) but {} is a {} archive; the verb and the \
                     artifact must name the same network, or the test runs green against a \
                     chain it never asked for",
                    claimed.as_str(),
                    handle.name(),
                    chain.network().as_str(),
                ),
            });
        }
    }
    Ok(())
}

/// The active tier's per-pod share, in the pod spec's own units
fn even_share(pods: usize) -> Option<crate::component::Resources> {
    qos::current_profile().share(pods).map(|s| crate::component::Resources {
        cpu: crate::component::Cpu::millis(s.cpu_milli),
        memory: crate::component::Mem::bytes(s.mem_bytes),
    })
}

struct MaterializeCtx<'a> {
    client: &'a kube::Client,
    pods: &'a Api<Pod>,
    sentinel: &'a Sentinel,
    coords: &'a RunCoords,
    test_name: &'a str,
}

/// One pod to materialize: `(id, spec, opts, backend handle)`, shared by both phases —
/// the handle's variant distinguishes them
type MaterializeItem = (u64, PodSpec, ComponentOpts, ComponentHandle);

#[cfg(test)]
mod tests {
    use super::{DeployBudget, assert_claims_match_artifacts, spec_request};
    use crate::component::{Cpu, Mem};
    use crate::qos::{GIB, Resources};

    fn pin(
        handle: crate::ArchiveHandle,
        claimed: crate::ArchiveNetwork,
    ) -> (String, crate::ArchiveHandle, crate::ChainInfo, &'static str, Option<crate::ArchiveNetwork>)
    {
        let chain = handle.chain().expect("shipped snapshot carries chain info");
        ("zebrad".to_string(), handle, chain, "6.2.3", Some(claimed))
    }

    /// Both directions: a one-sided check leaves the other silently booting the wrong chain
    #[test]
    fn a_network_verb_that_contradicts_its_archive_is_rejected() {
        use crate::ArchiveNetwork::{Mainnet, Testnet};
        use crate::snapshots::{mainnet, testnet};

        let err = assert_claims_match_artifacts(&[pin(mainnet::SAPLING, Testnet)])
            .expect_err("mainnet archive named with .testnet must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains(".testnet"), "{msg}");
        assert!(msg.contains("is a mainnet archive"), "{msg}");

        assert!(
            assert_claims_match_artifacts(&[pin(testnet::ORCHARD, Mainnet)]).is_err(),
            "testnet archive named with .mainnet must be rejected"
        );
    }

    #[test]
    fn a_network_verb_matching_its_archive_is_accepted() {
        use crate::ArchiveNetwork::{Mainnet, Testnet};
        use crate::snapshots::{mainnet, testnet};

        assert_claims_match_artifacts(&[
            pin(mainnet::BLOSSOM, Mainnet),
            pin(testnet::BLOSSOM, Testnet),
        ])
        .expect("verbs agreeing with their artifacts must build");
    }

    /// `.regtest()` / `.mount(_)` record no claim → must pass untouched, never be compared
    /// against a network they did not name
    #[test]
    fn an_unclaimed_restore_is_not_network_checked() {
        let chain = crate::snapshots::mainnet::SAPLING.chain().expect("chain info");
        assert_claims_match_artifacts(&[(
            "zebrad".to_string(),
            crate::snapshots::mainnet::SAPLING,
            chain,
            "6.2.3",
            None,
        )])
        .expect("a restore with no verb claim is not checked");
    }

    // ── DeployBudget: the sum of what actually deploys ──────────────────────

    use crate::manifest::PodSpec;
    use crate::qos::QosClass;

    /// Component pod spec as an explicit `.resources()` override renders it
    fn overridden(name: &str, cpu: Cpu, memory: Mem) -> PodSpec {
        PodSpec {
            pod_name: name.to_string(),
            category: crate::component::ComponentCategory::Validator,
            label: "test",
            image: "img".into(),
            ports: Vec::new(),
            ready_port: 1,
            command: None,
            args: None,
            resources: Some(crate::component::Resources { cpu, memory }),
            guaranteed: None,
            env: Vec::new(),
            fs_group: None,
            run_as_user: None,
            supplemental_groups: Vec::new(),
            placement: None,
            image_pull_secret: None,
            termination_grace_period: None,
        }
    }

    #[test]
    fn a_budget_admits_overrides_that_fit_together() {
        // Ceiling = the sync tier footprint (15c/15Gi); 9+5 and 11+4 fit
        let mut b = DeployBudget::new(QosClass::Sync.profile().footprint);
        b.admit(&overridden("zainod", Cpu::cores(9), Mem::gib(11)));
        b.admit(&overridden("zebrad", Cpu::cores(5), Mem::gib(4)));
        assert_eq!(b.committed.cpu_milli, 14_000);
        assert_eq!(b.committed.mem_bytes, 15 * GIB);
    }

    #[test]
    fn a_budget_rejects_overrides_that_only_overflow_in_aggregate() {
        // The hole: each pod fits the tier alone (per-pod guard passes both),
        // only the sum overflows
        let tier = QosClass::Sync.profile().footprint;
        let each = overridden("p", Cpu::cores(9), Mem::gib(7));
        assert!(
            spec_request(&each).fits_within(&tier),
            "precondition: one such pod must pass the per-pod guard"
        );

        let mut b = DeployBudget::new(QosClass::Sync.profile().footprint);
        b.admit(&overridden("zainod", Cpu::cores(9), Mem::gib(7)));
        let over = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            b.admit(&overridden("zebrad", Cpu::cores(9), Mem::gib(7)))
        }))
        .expect_err("18 cores across a 14-core ceiling must panic");
        let msg = over.downcast_ref::<String>().map(String::as_str).unwrap_or_default();
        assert!(msg.contains("over-schedule"), "names the fault: {msg}");
        // Both pods named, not just the one that crossed
        assert!(msg.contains("zainod") && msg.contains("zebrad"), "{msg}");
    }

    #[test]
    fn a_budget_charges_the_default_share_when_a_test_sets_no_override() {
        // No override → the tier-derived even split, which fits its own ceiling by
        // construction; the guard must not fire on it
        let share = QosClass::Sync.profile().share(2).expect("two pods");
        let share = crate::component::Resources {
            cpu: Cpu::millis(share.cpu_milli),
            memory: Mem::bytes(share.mem_bytes),
        };
        let mut spec = overridden("zebrad", Cpu::cores(1), Mem::gib(1));
        spec.resources = None;
        spec.guaranteed = Some(share);

        let mut b = DeployBudget::new(QosClass::Sync.profile().footprint);
        b.admit(&spec);
        b.admit(&spec);
        assert!(b.committed.fits_within(&b.tier));
        assert_eq!(
            b.committed,
            {
                let s = QosClass::Sync.profile().share(2).unwrap();
                Resources::new(s.cpu_milli * 2, s.mem_bytes * 2, 0, 0)
            },
            "the default path must charge exactly what the quota is sized to"
        );
    }

    // ── same ceiling, moved by a `footprint = ".."` override ────────────

    #[test]
    fn an_override_raises_the_ceiling_the_budget_charges_against() {
        // zaino index-construction shape (tier default 15 GiB rejects 24; 29 GiB admits)
        let raised =
            QosClass::Sync.profile().with_footprint(Some(Resources::new(15_000, 29 * GIB, 0, 0)));
        let mut b = DeployBudget::new(raised.footprint);
        b.admit(&overridden("zainod", Cpu::cores(9), Mem::gib(24)));
        b.admit(&overridden("zebrad", Cpu::cores(5), Mem::gib(4)));
        assert_eq!(b.committed.mem_bytes, 28 * GIB);
        assert!(b.committed.fits_within(&b.tier));
    }

    #[test]
    fn an_override_is_still_a_ceiling_not_a_licence() {
        // Raising the reserve moves the bound, never removes it
        let raised =
            QosClass::Sync.profile().with_footprint(Some(Resources::new(15_000, 29 * GIB, 0, 0)));
        let mut b = DeployBudget::new(raised.footprint);
        b.admit(&overridden("zainod", Cpu::cores(9), Mem::gib(24)));
        let over = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            b.admit(&overridden("zebrad", Cpu::cores(5), Mem::gib(8)))
        }))
        .expect_err("32 GiB across a 29 GiB ceiling must panic");
        let msg = over.downcast_ref::<String>().map(String::as_str).unwrap_or_default();
        assert!(msg.contains("over-schedule"), "names the fault: {msg}");
    }

    #[test]
    fn a_pod_over_the_raised_ceiling_still_trips_the_per_pod_guard() {
        let raised =
            QosClass::Sync.profile().with_footprint(Some(Resources::new(15_000, 29 * GIB, 0, 0)));
        let mut b = DeployBudget::new(raised.footprint);
        let over = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            b.admit(&overridden("zainod", Cpu::cores(9), Mem::gib(30)))
        }))
        .expect_err("a single 30 GiB pod must not fit a 29 GiB reserve");
        let msg = over.downcast_ref::<String>().map(String::as_str).unwrap_or_default();
        assert!(msg.contains("exceeds its tier footprint"), "names the fault: {msg}");
    }

    // Flooring the per-pod share closes the whole-core-rounding over-admission gap:
    // `deployed ≤ footprint` for every admitted topology → a test's real placement
    // (components + runner) never tops `admitted()`, so a full wave still fits physically
    #[test]
    fn flooring_keeps_the_deployed_wave_within_what_admission_reserved() {
        use crate::qos::QosClass;
        use crate::qos::scheduler::{Admission, Request, Scheduler};

        let profile = QosClass::Integration.profile();
        let cores = (profile.footprint.cpu_milli / 1000) as usize;

        // Per topology: components deployed + runner ≤ admitted
        for pods in 1..=cores {
            let s = profile.share(pods).expect("pods > 0");
            let deployed =
                Resources::new(s.cpu_milli * pods as u64, s.mem_bytes * pods as u64, 0, 0);
            let real = deployed.cpu_milli + profile.runner.cpu_milli;
            assert!(
                real <= profile.admitted().cpu_milli,
                "{pods}-pod deploy is {real}m cpu, over the {}m admitted",
                profile.admitted().cpu_milli,
            );
        }

        // Fill a cluster sized to an exact multiple of `admitted()`, then check the
        // worst-case real deploy (max components + runner per test) still fits it
        let per_test = profile.admitted();
        let free = Resources::new(
            per_test.cpu_milli.saturating_mul(21),
            per_test.mem_bytes.saturating_mul(21),
            0,
            0,
        );
        let mut sched = Scheduler::new(free);
        let mut admitted = 0u64;
        for i in 0..64 {
            let req = Request {
                binary_id: "zaino".into(),
                test_name: format!("t{i}"),
                sa: "ci".into(),
                footprint: per_test,
                priority: profile.priority,
            };
            match sched.request(req) {
                Admission::Granted(_) => admitted += 1,
                _ => break,
            }
        }
        assert_eq!(admitted, 21);

        let max_real =
            profile.share(cores).unwrap().cpu_milli * cores as u64 + profile.runner.cpu_milli;
        let wave = max_real.saturating_mul(admitted);
        assert!(
            wave <= free.cpu_milli,
            "wave deploys {}c against {}c free",
            wave / 1000,
            free.cpu_milli / 1000,
        );
    }
}
