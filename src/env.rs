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
use crate::cluster;
use crate::component::{ComponentOpts, Disk, Indexer, Validator, Wallet};
use crate::error::env_err;
use crate::naming::Sentinel;
use crate::topology::ActivationHeights;

use crate::handles::indexer::{IndexerBackend, IndexerConfig};
use crate::handles::validator::{ValidatorBackend, ValidatorConfig};
use crate::handles::wallet::WalletConfig;
use crate::handles::{ForwardRegistry, HandleInner};
use crate::protocol::Endpoint;

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
pub enum ComponentHandle {
    Validator(Arc<dyn ValidatorBackend>),
    Indexer(Arc<dyn IndexerBackend>),
}

/// Per-component bookkeeping captured at `build` time.
#[derive(Debug, Clone)]
pub struct ComponentState {
    pub namespace: String,
    pub pod_name: String,
    pub label: &'static str,
    pub named_ports: Vec<(String, u16)>,
    pub handle: ComponentHandle,
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

pub struct EnvInner {
    pub client: OnceLock<Client>,
    pub namespace: std::sync::Mutex<Option<String>>,
    pub components: tokio::sync::RwLock<HashMap<u64, ComponentState>>,
    pub in_cluster: bool,
    pub forwards: ForwardRegistry,
    pub seed_bindings: std::sync::Mutex<Vec<SeedBinding>>,
    pub is_built: AtomicBool,
    /// Detached syncs only: keeps the lease alive (`sync start` acquires then
    /// exits → driver renews for the pods' lifetime). Unset for an ordinary run
    pub sync_lease: std::sync::OnceLock<crate::qos::ledger::Reservation>,
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
            in_cluster: crate::cluster_config::in_cluster(),
            forwards: Arc::new(Mutex::new(HashMap::new())),
            seed_bindings: std::sync::Mutex::new(Vec::new()),
            is_built: AtomicBool::new(false),
            sync_lease: std::sync::OnceLock::new(),
        }
    }

    pub fn client_ref(&self) -> Result<&Client, EnvError> {
        self.client.get().ok_or(EnvError::NotBuilt)
    }

    pub async fn component_state(&self, id: u64) -> Result<ComponentState, EnvError> {
        let map = self.components.read().await;
        map.get(&id).cloned().ok_or(EnvError::UnknownComponent { id })
    }

    pub async fn resolve_named(
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

    pub async fn resolve_port(
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
    pending_shared_volumes: Vec<(String, Disk)>,
    next_id: u64,
    ready_timeout: Duration,
    /// Schedule of the *regtest* chain this env mines (`None` =
    /// [`ActivationHeights::regtest_default`]); unrelated to `chain_pin`, which records
    /// the schedule an archived chain already has
    activation_override: Option<ActivationHeights>,
    /// The one archive every restoring component names + what its manifest says it holds;
    /// resolved in [`build`](Self::build), `None` when nothing (or no chain) was restored
    chain_pin: Option<crate::ChainSnapshot>,
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
        // Regtest chains are a handful of blocks; a public one wants `shared_volume_sized`
        self.shared_volume_sized(name, Disk::gib(2))
    }

    /// [`shared_volume`](Self::shared_volume) for a chain that outgrows the regtest
    /// default — a restored public network, or one syncing past its pin
    pub fn shared_volume_sized(&mut self, name: &str, disk: Disk) -> SharedVolume {
        let slug = short_kind(name);
        let claim = format!("shared-{slug}");
        self.pending_shared_volumes.push((claim.clone(), disk));
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

    /// Register an in-process wallet, returning its concrete handle (e.g. `LrzWallet`)
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
    ///
    /// Resolve the one snapshot this env restores; ≤1 chain in play is what makes
    /// [`chain`](Self::chain) — one answer for the whole env — well-defined.
    ///
    /// Components pinned to different artifacts would make every parity assertion compare
    /// two histories, so that is the only thing left to reject: the network, tip and
    /// backend now ride the snapshot, and cannot disagree with themselves.
    fn resolve_snapshot_pin(&mut self) -> Result<(), EnvError> {
        let pinned: Vec<(String, crate::ChainSnapshot)> = self
            .pending_validators
            .iter()
            .map(|p| &p.opts)
            .chain(self.pending_indexers.iter().map(|p| &p.opts))
            .filter_map(|opts| match opts.restore.as_ref() {
                Some(crate::component::RestoreSource::Archive(s)) => Some((pod_name_of(opts), *s)),
                _ => None,
            })
            .collect();

        let Some((first_name, first)) = pinned.first() else {
            return Ok(()); // nothing restored → no chain pin
        };
        if let Some((other_name, other)) =
            pinned.iter().find(|(_, s)| s.artifact.oid != first.artifact.oid)
        {
            return Err(EnvError::Config {
                reason: format!(
                    "snapshot mismatch: {first_name} serves {}, {other_name} serves {}",
                    first.artifact.name, other.artifact.name,
                ),
            });
        }

        self.chain_pin = Some(*first);
        Ok(())
    }

    /// The chain this env restored: its pin, its network, and the artifact it came from.
    ///
    /// Written at the declaration in [`ztest::snapshots`](crate::snapshots), and checked
    /// against the running validator during [`build`](Self::build).
    ///
    /// # Panics
    ///
    /// No component restored a snapshot.
    pub fn chain(&self) -> crate::ChainSnapshot {
        self.chain_pin.unwrap_or_else(|| panic!("no chain snapshot in this env",))
    }
    fn materialize_configs(&mut self) -> Result<(), EnvError> {
        let activation = match &self.activation_override {
            None => ActivationHeights::regtest_default(),
            Some(heights) => {
                heights
                    .validate_schedule()
                    .map_err(|e| EnvError::Config { reason: e.to_string() })?;
                *heights
            }
        };
        tracing::debug!(?activation, "regtest activation-height schedule chosen");

        let p2p_port = crate::ports::ZEBRAD_P2P;
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
                                "no validator peer {name:?}; known: {known_validators:?}"
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

        // One read → placement + deploy ceiling both describe the reserve the ledger holds
        let effective = qos::current_profile();
        let qos_placement = Some(effective.pool);
        // Charged per-pod as both phases build specs → bounds the *sum* that deploys.
        // Pods size themselves (`qos::pod` defaults, `.resources()` per pod); this is the
        // only thing the tier ceiling does to a topology
        let mut budget = DeployBudget::new(effective.footprint);

        // Before any pod references them (WaitForFirstConsumer → the claim stays Pending
        // until the Phase-1 validator schedules)
        for (claim, disk) in std::mem::take(&mut self.pending_shared_volumes) {
            mounts::create_shared_pvc(&client, &sentinel, &claim, disk).await?;
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
    ///
    /// Prove the validator serves the chain its declaration claims.
    ///
    /// The pinned tip is the whole check: it is written at the declaration rather than read
    /// off the bytes, and a truncated extraction or partly-populated seed PVC opens at a
    /// lower height. Ordered before any indexer deploys, so it fails by name in seconds
    /// rather than as a parity mismatch hundreds of lines into a test.
    ///
    /// Public networks only — a regtest chain is *meant* to grow.
    async fn verify_restored_chain(&self) -> Result<(), EnvError> {
        let Some(snapshot) = self.chain_pin else {
            return Ok(());
        };
        if !snapshot.network.is_public() {
            return Ok(());
        }
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
        let tip = rpc.tip_height().await.map_err(|e| EnvError::Transient(Box::new(e)))?;
        if tip != snapshot.tip_height {
            return Err(EnvError::ArchiveMismatch {
                archive: snapshot.artifact.name.to_owned(),
                reason: format!("pinned at {}, validator serves {tip}", snapshot.tip_height,),
            });
        }
        tracing::debug!(
            target: "ztest::build",
            archive = snapshot.artifact.name,
            tip,
            "restored chain verified against its declaration"
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
    #[cfg_attr(not(feature = "librustzcash"), allow(dead_code))]
    pub fn kube_client(&self) -> Option<Client> {
        self.inner.client.get().cloned()
    }

    /// Namespace this env provisioned into, once [`build`](Self::build) has run. Locates
    /// this run's stop-watch and report ConfigMaps
    #[cfg_attr(not(feature = "librustzcash"), allow(dead_code))]
    pub fn namespace(&self) -> Option<String> {
        self.inner.namespace.lock().ok().and_then(|g| g.clone())
    }

    /// The one indexer in this topology, type-erased for a sync run's `SyncCtx` oracle.
    /// Errors on zero or >1 (a differential 2-indexer topology is a load-test shape)
    #[cfg_attr(not(feature = "librustzcash"), allow(dead_code))]
    pub async fn single_indexer(&self) -> Result<Arc<dyn IndexerBackend>, EnvError> {
        let comps = self.inner.components.read().await;
        let mut indexers = comps.values().filter_map(|s| match &s.handle {
            ComponentHandle::Indexer(h) => Some(Arc::clone(h)),
            ComponentHandle::Validator(_) => None,
        });
        match (indexers.next(), indexers.next()) {
            (Some(h), None) => Ok(h),
            (None, _) => Err(EnvError::Config {
                reason: "sync oracle needs an indexer; topology has none".into(),
            }),
            (Some(_), Some(_)) => Err(EnvError::Config {
                reason: "sync oracle ambiguous: more than one indexer".into(),
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
        let held = crate::qos::ledger::Reservation::adopt(
            client,
            &coords.run_id,
            &coords.user,
            reserve,
            crate::qos::beacon::LeaseKind::Sync,
        );
        if self.inner.sync_lease.set(held).is_ok() {
            tracing::info!(lease = %coords.run_id, %reserve, "detached sync: holding reservation");
        }
    }

    async fn materialize_phase(
        &self,
        ctx: &MaterializeCtx<'_>,
        items: &[MaterializeItem],
    ) -> Result<(), EnvError> {
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
            let resolved = mounts::resolve_all(
                ctx.client,
                ctx.sentinel,
                &spec.pod_name,
                &opts.mounts,
                opts.disk,
            )
            .await?;
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
    /// - Detached-sync driver: seed bindings only. Namespace = the deletion unit it lives
    ///   inside, reaped on its TTL ([`mark_finished`](crate::sync::mark_finished))
    /// - Delete runs on its own OS thread + runtime, `join`ed (`Drop` can't await, and a
    ///   spawned task dies with the test runtime before its DELETE is sent), with a client
    ///   rebuilt inside it (the original is bound to the dying reactor)
    /// - `ZTEST_NO_CLEANUP` suppresses the delete; the 1h `janitor/ttl` still reaps it
    fn drop(&mut self) {
        // Deleting here would race the parent's still-draining collector
        let parented = std::env::var(naming::TEST_NAMESPACE_ENV).is_ok_and(|v| !v.is_empty());
        // Detached driver keeps its bindings (cluster-scoped, nothing cascades them) but
        // owns no namespace: it sits *inside* the deletion unit, and `mark_finished` hands
        // that to the reaper instead
        let detached = crate::sync::active_sync_id().is_some();
        if parented && !detached {
            return;
        }

        // Colour is the reporter's call, propagated by the engine as `ZTEST_COLOR`
        let color = std::env::var("ZTEST_COLOR").ok().as_deref() == Some("1");

        let ns = match detached {
            true => None,
            false => self.inner.namespace.lock().ok().and_then(|mut g| g.take()),
        };
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
                    let components = crate::logstream::fetch_component_lines(&client, ns).await;
                    if let Some(section) = crate::logstream::component_section(components, color) {
                        eprint!("{section}");
                    }
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

/// Wait for a dependency pod to reach Ready, under the shared
/// [`ReadyWatch`](crate::pod_status::ReadyWatch) deadlines.
///
/// - Transient kube-API `get` errors ignored, as in [`crate::engine`]'s runner-pod loop
async fn await_pod_ready(
    pods: &Api<Pod>,
    name: &str,
    ready_timeout: Duration,
) -> Result<(), EnvError> {
    use crate::pod_status as ps;

    let mut watch = ps::ReadyWatch::new(ready_timeout);
    loop {
        if let Ok(pod) = pods.get(name).await
            && let Some(status) = pod.status.as_ref()
        {
            let component = || name.to_string();
            match watch.observe(status, Instant::now()) {
                ps::Verdict::Ready => return Ok(()),
                ps::Verdict::Waiting => {}
                ps::Verdict::Unschedulable { reason, elapsed } => {
                    return Err(EnvError::PodUnschedulable {
                        component: component(),
                        reason,
                        elapsed,
                    });
                }
                ps::Verdict::Faulted(reason) | ps::Verdict::PullFailed(reason) => {
                    return Err(EnvError::PodFailed { component: component(), reason });
                }
                ps::Verdict::ReadyTimeout(elapsed) => {
                    return Err(EnvError::RpcTimeout {
                        component: component(),
                        op: "pod_ready",
                        elapsed,
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
    use super::{DeployBudget, spec_request};
    use crate::component::{Cpu, Mem};
    use crate::qos::{GIB, Resources};

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

    /// `sync` declares no default, so every ceiling in these cases is a test's own —
    /// the zaino index-construction shape
    fn sync_ceiling(mem_gib: u64) -> Resources {
        QosClass::Sync.profile_with(Some(Resources::new(15_000, mem_gib * GIB, 0, 0))).footprint
    }

    #[test]
    fn a_budget_admits_overrides_that_fit_together() {
        // Ceiling = a declared 15c/15Gi; 9+5 and 11+4 fit
        let mut b = DeployBudget::new(sync_ceiling(15));
        b.admit(&overridden("zainod", Cpu::cores(9), Mem::gib(11)));
        b.admit(&overridden("zebrad", Cpu::cores(5), Mem::gib(4)));
        assert_eq!(b.committed.cpu_milli, 14_000);
        assert_eq!(b.committed.mem_bytes, 15 * GIB);
    }

    #[test]
    fn a_budget_rejects_overrides_that_only_overflow_in_aggregate() {
        // The hole: each pod fits the ceiling alone (per-pod guard passes both),
        // only the sum overflows
        let tier = sync_ceiling(15);
        let each = overridden("p", Cpu::cores(9), Mem::gib(7));
        assert!(
            spec_request(&each).fits_within(&tier),
            "precondition: one such pod must pass the per-pod guard"
        );

        let mut b = DeployBudget::new(tier);
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

    /// The no-override path: pods carry the [`qos::pod`] defaults their backend rendered,
    /// and the tier ceiling is sized to hold exactly that topology
    #[test]
    fn a_budget_charges_the_per_pod_defaults_when_a_test_sets_no_override() {
        let defaulted = |name: &str, default: Resources| {
            let mut spec = overridden(name, Cpu::cores(1), Mem::gib(1));
            spec.resources = None;
            spec.guaranteed = Some(default.into());
            spec
        };

        let mut b = DeployBudget::new(QosClass::Integration.profile().footprint);
        b.admit(&defaulted("zebrad", crate::qos::pod::VALIDATOR));
        b.admit(&defaulted("zainod", crate::qos::pod::INDEXER));
        assert!(b.committed.fits_within(&b.tier));
        assert_eq!(
            b.committed,
            crate::qos::pod::VALIDATOR.saturating_add(&crate::qos::pod::INDEXER),
            "the default path must charge exactly what the quota is sized to"
        );
    }

    // ── same ceiling, moved by a `footprint = ".."` override ────────────

    #[test]
    fn an_override_raises_the_ceiling_the_budget_charges_against() {
        // zaino index-construction shape: 15 GiB rejects the indexer's 24, 29 GiB admits it
        let mut b = DeployBudget::new(sync_ceiling(29));
        b.admit(&overridden("zainod", Cpu::cores(9), Mem::gib(24)));
        b.admit(&overridden("zebrad", Cpu::cores(5), Mem::gib(4)));
        assert_eq!(b.committed.mem_bytes, 28 * GIB);
        assert!(b.committed.fits_within(&b.tier));
    }

    #[test]
    fn an_override_is_still_a_ceiling_not_a_licence() {
        // Raising the reserve moves the bound, never removes it
        let mut b = DeployBudget::new(sync_ceiling(29));
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
        let mut b = DeployBudget::new(sync_ceiling(29));
        let over = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            b.admit(&overridden("zainod", Cpu::cores(9), Mem::gib(30)))
        }))
        .expect_err("a single 30 GiB pod must not fit a 29 GiB reserve");
        let msg = over.downcast_ref::<String>().map(String::as_str).unwrap_or_default();
        assert!(msg.contains("exceeds its tier footprint"), "names the fault: {msg}");
    }

    /// `deployed ≤ footprint` for the topology the tier is priced for → a test's real
    /// placement (components + runner) never tops `admitted()`, so a full wave of them
    /// still fits the capacity admission handed out
    #[test]
    fn the_default_topology_deploys_inside_what_admission_reserved() {
        use crate::qos::QosClass;
        use crate::qos::scheduler::{Admission, Request, Scheduler};

        let profile = QosClass::Integration.profile();
        let deployed = crate::qos::pod::VALIDATOR.saturating_add(&crate::qos::pod::INDEXER);
        assert!(
            deployed.fits_within(&profile.footprint),
            "the default validator + indexer ({deployed}) overflows the tier ceiling ({})",
            profile.footprint,
        );
        assert!(deployed.saturating_add(&profile.runner).fits_within(&profile.admitted()));

        // Fill a cluster sized to an exact multiple of `admitted()`, then check the real
        // deploy (components + runner per test) still fits it
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

        let max_real = deployed.cpu_milli + profile.runner.cpu_milli;
        let wave = max_real.saturating_mul(admitted);
        assert!(
            wave <= free.cpu_milli,
            "wave deploys {}c against {}c free",
            wave / 1000,
            free.cpu_milli / 1000,
        );
    }
}
