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

/// Config-time regtest materialization, captured per validator at
/// `add_validator` so the topology resolver can apply it once the activation
/// heights are known, without retaining the concrete backend.
type RegtestMaterializeFn = Box<
    dyn FnOnce(
            ComponentOpts,
            &ActivationHeights,
            &[(String, u16)],
        ) -> Result<ComponentOpts, EnvError>
        + Send,
>;

/// Config-time config materialization for an indexer (takes the validator
/// host resolved at build time). Captured at `add_indexer`.
type IndexerMaterializeFn =
    Box<dyn FnOnce(ComponentOpts, Option<&str>) -> Result<ComponentOpts, EnvError> + Send>;
use crate::manifest::PodSpec;
use crate::mounts::{self, ResolvedMount};
use crate::naming::{self, RunCoords};
use crate::portforward::Forwarder;
use crate::qos;
use crate::seeds::{self, SeedBinding};

/// The live backend handle behind a materialized pod. Every pod that reaches
/// [`TestEnv::materialize_phase`] carries exactly one — validators and indexers
/// are the only categories that materialize (wallets run in-process), and the
/// variant is the pod's category. This is what the
/// env-level readiness (and, for validators, warm) probes drive during
/// [`TestEnv::build`].
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
    /// Live backend handle for this pod, driving the env's readiness (and, for
    /// validators, warm) probes during `build`. Its variant (validator vs
    /// indexer) is the pod's category — the single source of truth for it.
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
    /// Pod names of profiled components (`profile`-feature image), captured at
    /// materialize so a post-run [`TestEnv::collect_profiles`] can drain each and
    /// tar its flamegraph out.
    pub(crate) profiled_pods: std::sync::Mutex<Vec<String>>,
    pub(crate) is_built: AtomicBool,
    /// One `kubectl logs -f` follow of every component pod for this test's
    /// lifetime, buffered and rendered into the teardown diagnostic on failure
    /// (see [`crate::logstream`]). Spawned once at the end of `build`, so it is
    /// present only for a successfully-built env.
    pub(crate) log_capture: std::sync::OnceLock<crate::logstream::LogCapture>,
}

impl std::fmt::Debug for EnvInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvInner")
            .field(
                "namespace",
                &self.namespace.lock().ok().and_then(|g| g.clone()),
            )
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
            profiled_pods: std::sync::Mutex::new(Vec::new()),
            is_built: AtomicBool::new(false),
            log_capture: std::sync::OnceLock::new(),
        }
    }

    pub(crate) fn client_ref(&self) -> Result<&Client, EnvError> {
        self.client.get().ok_or(EnvError::NotBuilt)
    }

    pub(crate) async fn component_state(&self, id: u64) -> Result<ComponentState, EnvError> {
        let map = self.components.read().await;
        map.get(&id)
            .cloned()
            .ok_or(EnvError::UnknownComponent { id })
    }

    pub(crate) async fn resolve_named(
        &self,
        state: &ComponentState,
        name: &str,
    ) -> Result<Endpoint, EnvError> {
        let port = state
            .named_ports
            .iter()
            .find_map(|(n, p)| (n == name).then_some(*p))
            .ok_or_else(|| EnvError::UnknownEndpoint {
                component: state.label.to_string(),
                name: name.to_string(),
            })?;
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
            return Ok(Endpoint {
                host,
                port: container_port,
            });
        }

        let key = (state.pod_name.clone(), container_port);
        let mut forwards = self.forwards.lock().await;
        if let Some(fw) = forwards.get(&key) {
            return Ok(Endpoint {
                host: IpAddr::V4(Ipv4Addr::LOCALHOST),
                port: fw.local_port,
            });
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
        Ok(Endpoint {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: local_port,
        })
    }
}

// ──────────────────────── pending entries ─────────────────────────────

struct PendingValidator {
    id: u64,
    /// Regtest materialization, applied once the activation heights are chosen.
    /// `take`n when applied.
    materialize: Option<RegtestMaterializeFn>,
    /// Live handle, threaded into `ComponentState` for the env's probes.
    handle: Arc<dyn ValidatorBackend>,
    opts: ComponentOpts,
}

struct PendingIndexer {
    id: u64,
    /// Type-erased backend handle, retained so `env.build()` builds the pod spec
    /// via [`IndexerBackend::pod_spec`] rather than matching on a label string.
    handle: Arc<dyn IndexerBackend>,
    /// Config-materialization closure; `Some` for any indexer that opted into a
    /// network mode (regtest / testnet / mainnet), `take`n when applied.
    materialize: Option<IndexerMaterializeFn>,
    opts: ComponentOpts,
}

struct PendingWallet {
    opts: ComponentOpts,
}

// ──────────────────────────── shared volume ───────────────────────────

/// Handle to an env-scoped `ReadWriteOnce` PVC shared between two co-scheduled
/// pods. Created via [`TestEnv::shared_volume`], provisioned during
/// [`TestEnv::build`]. [`.mount(&vol)`](crate::ComponentBuilder::mount) it on
/// both a zebrad validator and a zaino indexer running
/// `.tuning(ZainoTuning::State)`, so both mount the same on-disk zebra-state DB.
#[derive(Debug, Clone)]
pub struct SharedVolume {
    claim: String,
    mount_path: String,
}

impl SharedVolume {
    /// PVC name in the test namespace.
    pub fn claim(&self) -> &str {
        &self.claim
    }
    /// In-pod path the shared volume is mounted at. Both sharing pods use this
    /// identical path so zebra's `db_path` resolves to the same directory.
    pub fn mount_path(&self) -> &str {
        &self.mount_path
    }
    /// The [`Mount`](crate::Mount) attaching this shared volume at its canonical
    /// path. Pass to [`ComponentBuilder::mount`](crate::ComponentBuilder::mount)
    /// — `zeb.mount(&vol)` and `zai.mount(&vol).tuning(ZainoTuning::State)` share
    /// one on-disk store.
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
    /// Per-component readiness/RPC-probe budget applied during
    /// [`build`](Self::build). Set any time before `build` via
    /// [`ready_timeout`](Self::ready_timeout).
    ready_timeout: Duration,
    /// An explicit regtest activation-height schedule for this env, if set via
    /// [`activation_heights`](Self::activation_heights). `None` uses the
    /// canonical default ([`ActivationHeights::regtest_default`]).
    ///
    /// Unrelated to [`chain_pin`](Self::chain_pin) below: this configures the
    /// schedule of a *regtest* chain this env is about to mine, while a pin
    /// records the schedule an archived chain already has.
    activation_override: Option<ActivationHeights>,
    /// The one chain archive every restoring component in this env names, and
    /// what its manifest says the archive contains. Resolved during
    /// [`build`](Self::build) by [`resolve_snapshot_pin`](Self::resolve_snapshot_pin);
    /// `None` when nothing was restored, or when what was restored is an
    /// opaque archive carrying no chain metadata.
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

    /// Override the per-component readiness/RPC-probe budget used during
    /// [`build`](Self::build). Order-independent relative to `add_*`.
    pub fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// Pin an explicit regtest activation-height schedule for this env,
    /// overriding the canonical default ([`ActivationHeights::regtest_default`]).
    /// The schedule is validated at [`build`](Self::build): activated upgrades
    /// must form a contiguous prefix with non-decreasing heights.
    /// Order-independent relative to `add_*`.
    pub fn activation_heights(mut self, heights: ActivationHeights) -> Self {
        self.activation_override = Some(heights);
        self
    }

    /// Declare an env-scoped shared volume, returning a [`SharedVolume`] handle
    /// to [`.mount(&vol)`](crate::ComponentBuilder::mount) on both a zebrad
    /// validator and a zaino indexer running `.tuning(ZainoTuning::State)`. The
    /// backing `ReadWriteOnce` PVC is
    /// provisioned during [`TestEnv::build`]; both consumers mount it at the same
    /// in-pod path.
    pub fn shared_volume(&mut self, name: &str) -> SharedVolume {
        let slug = short_kind(name);
        let claim = format!("shared-{slug}");
        self.pending_shared_volumes.push(claim.clone());
        SharedVolume {
            claim,
            mount_path: format!("/shared/{slug}"),
        }
    }

    fn fresh_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Register a validator and return its concrete, typed handle (e.g.
    /// `ZebraValidator`). Backend-specific RPCs are inherent methods on it, so
    /// calling one on the wrong backend is a compile error.
    pub fn add_validator<B: ValidatorConfig>(&mut self, mut v: Validator<B>) -> B::Handle {
        let id = self.fresh_id();
        // Resolve the coinbase pool once (builder choice, else backend
        // default) and pin it back into `opts` so the deferred regtest
        // materialization renders the matching miner address, and into the
        // handle's plumbing so `funded_faucet` can pick its funding path.
        let coinbase_pool = v
            .opts
            .coinbase_pool
            .unwrap_or_else(|| v.backend.default_coinbase_pool());
        v.opts.coinbase_pool = Some(coinbase_pool);
        let plumbing = HandleInner {
            inner: Arc::downgrade(&self.inner),
            component_id: id,
            regtest: v.opts.regtest,
            coinbase_pool: Some(coinbase_pool),
        };
        // Build the live handle (returned to the caller + stored for the
        // env's probes). The concrete backend isn't retained, so capture the
        // regtest materialization as a deferred closure applied once the
        // activation heights are chosen.
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

    /// Register an indexer and return its concrete, typed handle (e.g.
    /// `ZainoIndexer`).
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
        // Install the config-materialization closure for any indexer that
        // opted into a network mode; it renders the backend + mode config once
        // the validator host is resolved at build time.
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

    /// Register an in-process wallet and return its concrete, typed handle
    /// (e.g. `ZingoWallet`).
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
        // Deliberate v1 caps, not fundamental limits. The build wiring
        // resolves a single validator host (`materialize_configs`
        // takes `pending_validators…next()`) and the typed handles assume
        // at most a primary/secondary indexer pair and one in-process
        // wallet. Lift these alongside multi-validator topology support.
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

    /// Resolve the one chain archive this env restores, and reject an env
    /// whose components disagree about which chain they serve.
    ///
    /// Both halves at once, because they are the same question asked twice:
    /// the checks below establish that *at most one* chain is in play, which
    /// is exactly what makes [`chain`](Self::chain) — a single answer for the
    /// whole env — well-defined. The resolved pin is recorded here so that
    /// tests read chain facts from the env that is actually running, never
    /// from a handle that merely names an artifact.
    ///
    /// Three ways an env goes wrong, all silent without this check and all
    /// baffling when they surface:
    ///
    /// 1. **Builder version ≠ producer version.** A zebra state DB is tied to
    ///    the release that wrote it. Point a different zebrad at it and it
    ///    either upgrades the database in place — mutating the CoW clone, so it
    ///    stops being the artifact the name promises — or fails to open it. The
    ///    version on the builder is the only place that mismatch is visible
    ///    before a pod starts.
    /// 2. **Components pinned to different artifacts.** A state-backend zaino
    ///    reads the same chain the validator serves. Give them different pins
    ///    and every parity assertion in the test compares two different
    ///    histories, failing as an unrelated data mismatch hundreds of lines
    ///    from the cause.
    /// 3. **A testnet archive that cannot support the questions asked of it.**
    ///    A snapshot pinned *at* an activation, or with no activation recorded
    ///    at all, holds essentially none of the data it is named for, and every
    ///    boundary assertion drawn from it passes over empty results. Rejecting
    ///    it here is also what makes `ChainInfo::activation` and its
    ///    neighbours total for every chain a test can reach.
    fn resolve_snapshot_pin(&mut self) -> Result<(), EnvError> {
        // Only archives carrying chain metadata participate. A bare fixture
        // tarball pins no history and names no producer, so there is nothing
        // for its peers to disagree with.
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
            // Only validators write or read the state DB at a version of their
            // own; an indexer's `version` is zaino's, which has no bearing on
            // the zebra on-disk format.
            let is_validator = self
                .pending_validators
                .iter()
                .any(|p| &pod_name_of(&p.opts) == name);
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

        // Identity is the OID, so "the same artifact" is an exact comparison
        // rather than a path or name match — two declarations of one archive
        // from different call sites agree here by construction.
        if let Some(((first_name, first, ..), (other_name, other, ..))) = pinned.first().zip(
            pinned
                .iter()
                .find(|(_, h, ..)| h.oid() != pinned[0].1.oid()),
        ) {
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
            return Ok(()); // Nothing restored; this env has no chain pin.
        };

        // A public-network archive is an immutable, height-pinned artifact: the
        // restored validator runs with an empty peer set, so its tip never
        // moves and every derived height is a fact for the whole test. A
        // regtest cache is the opposite — a starting point tests mine onto —
        // so none of the claims below are meant to hold for one.
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

    /// What the chain this env restored actually contains.
    ///
    /// The tip it is pinned at, the upgrade it straddles, the activation
    /// schedule it carries, and the heights worth querying it at — read from
    /// the artifact's manifest at compile time, and verified against the
    /// running validator during [`build`](Self::build), so a test asserting on
    /// these is asserting about data that is really mounted.
    ///
    /// # Panics
    ///
    /// If no component in this env restored an archive, or the archive it
    /// restored is an opaque blob with no chain metadata. Both are statements
    /// about a chain that is not there, and neither is recoverable at the
    /// point a test asks the question.
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

    /// The archive [`chain`](Self::chain) describes — for diagnostics that
    /// want to name the artifact a failure came from.
    ///
    /// Panics on the same conditions as [`chain`](Self::chain).
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
                heights
                    .validate_schedule()
                    .map_err(|reason| EnvError::Config { reason })?;
                *heights
            }
        };
        tracing::debug!(?activation, "regtest activation-height schedule chosen");

        let p2p_port = crate::handles::ports::ZEBRAD_P2P;
        let known_validators: std::collections::HashSet<String> = self
            .pending_validators
            .iter()
            .map(|p| pod_name_of(&p.opts))
            .collect();
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

        let validator_host = self
            .pending_validators
            .iter()
            .map(|p| pod_name_of(&p.opts))
            .next();
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
        // Render this test process's diagnostics (the `ztest::build` phase timing
        // below) to stdout when orchestrated, so they ride the pod-log capture
        // path and the reporter can show them per `--success-output`.
        crate::observ::init_in_pod();
        cluster::require_orchestrator()?;
        self.validate_topology()?;
        self.resolve_snapshot_pin()?;
        self.materialize_configs()?;

        let started = std::time::Instant::now();
        let coords = RunCoords::from_env().map_err(env_err)?;
        // Raw `module::test` (for the namespace annotation + name) and its
        // DNS-safe slug (for every label value; `::` is illegal in labels).
        let test_raw = naming::current_test_name();
        let package = naming::current_package();
        let test_slug = naming::slug(&test_raw, naming::DNS_LABEL_MAX);
        // On the pod path the parent `ztest run` created (and will tear down) the
        // per-test namespace and injected its name here, so it can follow every
        // pod in it over the kube API — the single laptop-owned log path. We reuse
        // that name and skip creation + teardown. Unset ⇒ local path: invent the
        // name and own the namespace's whole lifecycle in-process (as before).
        let laptop_owned = std::env::var(naming::TEST_NAMESPACE_ENV)
            .ok()
            .filter(|v| !v.is_empty());
        let namespace = laptop_owned
            .clone()
            .unwrap_or_else(|| naming::namespace_for(&package, &test_raw, &naming::test_suffix()));
        let client = cluster::client().await.map_err(env_err)?;

        let tier = qos::current();
        tracing::info!(
            test = %test_raw,
            tier = tier.as_label(),
            reservation = %tier.profile().footprint,
            validators = self.pending_validators.len(),
            indexers = self.pending_indexers.len(),
            wallets = self.pending_wallets.len(),
            namespace = %namespace,
            "starting test run"
        );

        // Local path creates the namespace here; on the pod path the laptop
        // already created it (and waited for its default SA), so we go straight
        // to the quota, which we still own (it's sized from the topology only
        // known in-pod).
        if laptop_owned.is_none() {
            cluster::ensure_namespace(&client, &namespace, &coords, &package, &test_raw)
                .await
                .map_err(env_err)?;
        }
        // Cap the namespace at the tier's deployed footprint: a hard,
        // API-server-enforced backstop to the parent scheduler's soft admission
        // (§7). Sized to exactly what the pods below request, so it never
        // rejects a legitimately-admitted pod. The namespace delete cascades it.
        let pod_count = self.pending_validators.len() + self.pending_indexers.len();
        if pod_count > 0 {
            let footprint = deployed_footprint(qos::current().profile().footprint, pod_count);
            // The invariant that matters: the component pods this test is about
            // to deploy must never reserve more than the tier's component budget
            // the parent scheduler admitted it for. `deployed_footprint` sums the
            // whole-core-rounded per-pod share, which can exceed the raw tier
            // footprint when `pod_count` doesn't divide the tier's cores evenly —
            // that surplus is capacity the scheduler never reserved, so the pods
            // would silently wedge Pending. Fail loudly instead (hard panic, all
            // builds): a tripped guard means this topology is mispriced for its
            // tier — raise the tier so its cores divide across the topology.
            assert_deployed_within_tier(qos::current(), footprint, pod_count);
            // A detached sync's namespace outlives its components and hosts
            // ztest's own auxiliary pods — the profile collector
            // (`profiling::collect`) is created there after the run, and a quota
            // sized to exactly the components rejects any pod that declares no
            // requests at all. Every component pod is explicitly Guaranteed-sized
            // and admitted k8s-natively on the NVMe pool, so that is the real
            // bound; skip the backstop quota rather than price every auxiliary.
            //
            // Not about the driver pod, which lives in the run namespace with
            // every other runner and was never counted here.
            if crate::sync::active_sync_id().is_none() {
                cluster::apply_resource_quota(&client, &namespace, footprint, pod_count)
                    .await
                    .map_err(env_err)?;
            }
        }
        build_phase("namespace_quota", started);
        let sentinel = Sentinel::new(namespace.clone());
        let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
        let test_name = test_slug;

        let _ = self.inner.client.set(client.clone());
        *self
            .inner
            .namespace
            .lock()
            .expect("namespace mutex poisoned") = Some(namespace.clone());

        // The tier's node placement, stamped on every pod spec below.
        let qos_placement = Some(qos::current().profile().pool);
        // The whole-test tier footprint, the ceiling a single pod's explicit
        // `.resources()` override may not exceed (asserted per spec below).
        let tier_footprint = qos::current().profile().footprint;
        // QoS-default pod sizing (§7): split the tier footprint evenly across
        // the env's pods (validators + indexers; wallets are in-process) as
        // requests==limits, i.e. Guaranteed QoS. A test's explicit
        // `.resources()` overrides this per-pod. `None` when there are no pods.
        let qos_guaranteed = even_share(qos::current().profile().footprint, pod_count);

        // Provision shared PVCs before any pod references them. With the
        // default (WaitForFirstConsumer) binding the claim stays Pending until
        // the first consumer (the validator in Phase 1) schedules.
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
                    spec.guaranteed = qos_guaranteed.clone();
                }
                assert_override_within_tier(&spec, tier_footprint);
                Ok::<_, EnvError>((p.id, spec, p.opts, ComponentHandle::Validator(p.handle)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let t_phase = std::time::Instant::now();
        self.materialize_phase(&ctx, &validators).await?;
        build_phase("validators_materialize", t_phase);
        // The env's own readiness/warm probes drive the validators through
        // their handles, which gate endpoint resolution on `is_built`.
        // Flip it on for the probe window, then back off until the whole build
        // completes, so a Phase-2 failure still leaves test-side handle calls
        // reporting `NotBuilt`.
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
                    spec.guaranteed = qos_guaranteed.clone();
                }
                assert_override_within_tier(&spec, tier_footprint);
                Ok::<_, EnvError>((p.id, spec, p.opts, ComponentHandle::Indexer(p.handle)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Wallets run in-process in the test binary (libraries that connect to
        // the indexer over gRPC), so they get no pod; here we just drop the
        // pending entries. Account construction happens lazily, on demand, via
        // `WalletHandle::account`.
        self.pending_wallets.clear();
        let t_phase = std::time::Instant::now();
        self.materialize_phase(&ctx, &dependents).await?;
        build_phase("indexers_materialize", t_phase);

        // Local path only: follow every component pod as one arrival-ordered,
        // pod-prefixed stream in-process for the rest of the test (`--tail=-1`
        // backfills the Phase-1 validators' history). On failure it becomes the
        // interleaved timeline rendered in `Drop`; on success it is discarded.
        // On the pod path the parent `ztest run` owns this capture over the kube
        // API instead (see `logstream::Collector`), so we skip it here — the
        // runner image carries no `kubectl` for it to shell.
        if laptop_owned.is_none() {
            let component_pods = self.inner.components.read().await.len();
            let _ = self
                .inner
                .log_capture
                .set(crate::logstream::LogCapture::spawn(
                    &namespace,
                    component_pods,
                ));
        }

        // The indexer-phase analogue of `wait_validators_rpc_ready` above: a
        // pod reaching the Kubernetes Ready condition does not mean its gRPC
        // listener is bound — zainod only begins serving once its initial
        // chain-index build completes, which can lag pod-Ready by well over a
        // minute under load. Gate on a live `GetLightdInfo` so `build`
        // returning means every indexer is reachable, not merely scheduled.
        // `is_built` is flipped on for the probe window (endpoint resolution
        // gates on it) and back off, exactly as the validator phase does, so a
        // gate failure leaves test-side handle calls reporting `NotBuilt`.
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

    /// Prove the validator is serving the chain its archive's manifest claims.
    ///
    /// Runs in the validator phase of [`build`](Self::build) — after the
    /// validator answers RPCs, before a single indexer is deployed. A wrong
    /// archive therefore costs seconds and fails by name, instead of costing a
    /// multi-minute indexer sync and then failing as a parity mismatch
    /// hundreds of lines into a test.
    ///
    /// Three claims, each closing a way the manifest and the mounted data
    /// diverge:
    ///
    /// 1. **Tip.** The blunt check: a swapped or truncated archive fails here.
    /// 2. **The whole activation schedule**, not just the straddled upgrade. A
    ///    chain whose tip and boundary both match can still be the wrong
    ///    chain, and the manifest is the only place the full schedule is
    ///    written down.
    /// 3. **The producer's boundary evidence**, re-asserted against the
    ///    *mounted* data. The producer proved at production time that the
    ///    introduced pool holds value past the activation; this proves the
    ///    bytes now on disk still carry it. The two diverge on a truncated
    ///    extraction or a partially-populated seed PVC, and without this the
    ///    difference shows up only as a boundary test passing over empty data.
    ///
    /// Public networks only. A regtest cache is a starting point tests mine
    /// onto: its tip is *meant* to move, and its activation schedule comes from
    /// [`activation_heights`](Self::activation_heights) rather than from the
    /// manifest, so none of these claims are meant to hold for one.
    async fn verify_restored_chain(&self) -> Result<(), EnvError> {
        let Some((handle, chain)) = self.chain_pin else {
            return Ok(());
        };
        if !chain.network().is_public() {
            return Ok(());
        }
        // Any validator in the env will do: `resolve_snapshot_pin` has already
        // established they all serve the same artifact.
        let validator = {
            let comps = self.inner.components.read().await;
            comps.values().find_map(|s| match &s.handle {
                ComponentHandle::Validator(h) => Some(Arc::clone(h)),
                ComponentHandle::Indexer(_) => None,
            })
        };
        let Some(validator) = validator else {
            return Ok(()); // Indexer-only env; nothing authoritative to ask.
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
            // `before_overwinter` is the absence of an upgrade, so the RPC
            // never reports it and looking for it would fail every fixture.
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
            // One height below the activation and one at the pinned tip: the
            // pool must be empty before the upgrade that introduces it and
            // hold the producer's recorded value after.
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
        // (pod_name, handle) for each validator. Probes drive the handle,
        // which resolves its own endpoint and picks the backend-specific
        // readiness RPC.
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
            handle
                .ready(timeout)
                .await
                .map_err(|_| EnvError::RpcTimeout {
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

    /// The indexer-phase counterpart of [`Self::wait_validators_rpc_ready`]:
    /// block until every indexer answers its gRPC `GetLightdInfo`, or
    /// `ready_timeout` elapses. See the call site in `build` for why pod-Ready
    /// is not a sufficient signal for indexers.
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
            handle
                .ready(timeout)
                .await
                .map_err(|_| EnvError::RpcTimeout {
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

    /// The single indexer bound in this topology, type-erased for use as the
    /// independent correctness oracle in a sync run's `SyncCtx`. Errors if
    /// zero or more than one indexer is present — a sync oracle must be
    /// unambiguous (a differential ≤2-indexer topology is a load-test shape, not
    /// a sync-oracle one).
    /// The kube client, once [`build`](Self::build) has connected. Used by the
    /// facade's run tail: the detach stop-watch and the mirrored durable report.
    #[cfg_attr(not(feature = "zingo"), allow(dead_code))]
    pub(crate) fn kube_client(&self) -> Option<Client> {
        self.inner.client.get().cloned()
    }

    /// The test namespace this env provisioned into, once [`build`](Self::build)
    /// has run. Used to locate this run's stop-watch and report ConfigMaps.
    #[cfg_attr(not(feature = "zingo"), allow(dead_code))]
    pub(crate) fn namespace(&self) -> Option<String> {
        self.inner.namespace.lock().ok().and_then(|g| g.clone())
    }

    /// Drain each profiled component (graceful SIGTERM so its profiler flushes)
    /// and tar its profile artifacts into `dest_dir`, returning the collected
    /// paths. Best-effort per component; call after the run, before teardown.
    #[cfg_attr(not(feature = "zingo"), allow(dead_code))]
    pub(crate) async fn collect_profiles(
        &self,
        dest_dir: &std::path::Path,
    ) -> Vec<std::path::PathBuf> {
        let (Some(client), Some(ns)) = (self.inner.client.get().cloned(), self.namespace()) else {
            return Vec::new();
        };
        let pods = self
            .inner
            .profiled_pods
            .lock()
            .expect("profiled_pods mutex poisoned")
            .clone();
        let mut collected = Vec::new();
        for pod in pods {
            collected.extend(crate::profiling::collect(&client, &ns, &pod, dest_dir).await);
        }
        collected
    }

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
        // Mine one block per validator so dependents (indexers) sync
        // against a non-genesis tip. Drives each validator's handle.
        //
        // Regtest only. The warm-up exists because a freshly-created regtest
        // chain sits at genesis, which is not a useful tip to sync against; a
        // restored chain is already at its snapshot height and needs nothing.
        // On a real network mining is not merely redundant but impossible —
        // PoW is live, and zebrad answers `generate` with "generate is only
        // supported on networks where PoW is disabled", failing the whole build
        // before any indexer is provisioned.
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
            handle
                .generate_blocks(1)
                .await
                .map_err(|e| EnvError::Transient(Box::new(e)))?;
        }
        Ok(())
    }

    async fn materialize_phase(
        &self,
        ctx: &MaterializeCtx<'_>,
        items: &[MaterializeItem],
    ) -> Result<(), EnvError> {
        for (id, spec, opts, handle) in items {
            // The per-component companion to the run-level "starting test run"
            // line above: one INFO per pod as it is provisioned, naming its
            // category, identity, image, and the reservation it will run under.
            // Both build phases (validators, then indexers) funnel through here,
            // so this is the single place that covers every pod.
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
            // Its own step because resolving a seed mount can fetch an archive
            // through the storage backend — minutes of network, not a local call.
            if !opts.mounts.is_empty() {
                crate::sync::note_setup(
                    spec.category.as_str(),
                    Some(&spec.pod_name),
                    "resolving mounts and seeds",
                );
            }
            let mut resolved =
                mounts::resolve_all(ctx.client, ctx.sentinel, &spec.pod_name, &opts.mounts).await?;
            self.inner
                .seed_bindings
                .lock()
                .expect("seed_bindings mutex poisoned")
                .extend(resolved.seed_bindings);
            // Profiled component → give it a per-test artifact PVC at
            // `ZTEST_PROFILE_OUT` (which its pod spec set) so the flamegraph
            // written on graceful SIGTERM outlives the pod for collection.
            if opts.image.profile_enabled() {
                let claim = crate::profiling::artifact_pvc_name(&spec.pod_name);
                crate::profiling::ensure_artifact_pvc(ctx.client, ctx.sentinel, &claim).await?;
                resolved
                    .mounts
                    .push(crate::profiling::artifact_resolved_mount(&claim));
                self.inner
                    .profiled_pods
                    .lock()
                    .expect("profiled_pods mutex poisoned")
                    .push(spec.pod_name.clone());
            }
            apply_pod(ctx, spec, &resolved.mounts).await?;
            // Metrics-enabled component → emit a PodMonitor so OpenShift UWM
            // scrapes its `/metrics`. Non-fatal: a cluster without the
            // Prometheus-operator CRDs (or with UWM off) must not fail the test,
            // it just yields no server-side metrics for this run.
            if opts.image.metrics_enabled()
                && let Err(e) = crate::metrics::emit_pod_monitor(
                    ctx.client,
                    &ctx.sentinel.namespace,
                    &spec.pod_name,
                    &ctx.coords.run_id,
                )
                .await
            {
                tracing::warn!(
                    component = %spec.pod_name,
                    error = %e,
                    "PodMonitor emit failed; this component's metrics will not be scraped"
                );
            }
            self.inner.components.write().await.insert(*id, state);
        }

        // One event for the whole gate rather than one per pod: the waits below run
        // concurrently, so "which pod are we on" has no answer — the honest live
        // statement is which gate is open and how long it has been.
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
    /// Teardown for the **local path**, Drop-only and running to completion here,
    /// pass or fail. On the **pod path** (a laptop-created namespace, signalled by
    /// `ZTEST_TEST_NAMESPACE`) this is a no-op: the parent `ztest run` owns log
    /// collection, dead-pod reporting, and teardown over the kube API, precisely
    /// so that capture can't race an in-pod namespace delete. The guard below
    /// short-circuits before any teardown work in that case.
    ///
    /// There is deliberately no `teardown().await` method. An explicit call is
    /// skipped by any early `?`-return on a test's failure path, which leaks
    /// the namespace (and every pod in it): the exact cause of the cluster
    /// filling to its pod cap and every subsequent test timing out on
    /// `pod_ready`. Tying teardown to `Drop` makes it unconditional: the
    /// namespace is deleted whether the test returns `Ok`, returns `Err`, or
    /// panics.
    ///
    /// `Drop` cannot `.await`, and the test's own runtime is torn down the
    /// instant the test future resolves, so a `Handle::spawn`ed cleanup task
    /// would be cancelled before its DELETE was ever sent (that
    /// fire-and-forget shape is what leaked namespaces before). Instead we run
    /// the delete to completion on a dedicated OS thread with its own runtime
    /// and `join()` it, blocking the dropping thread until the API has accepted
    /// the deletion. This is runtime-flavour agnostic (works under both
    /// current-thread and multi-thread test runtimes). The kube client is
    /// rebuilt inside that runtime because the original is bound to the
    /// now-dying test runtime's reactor and is unsound to reuse across runtimes.
    ///
    /// `ztest run --no-cleanup` (via `ZTEST_NO_CLEANUP`) suppresses the delete
    /// so a developer can `kubectl` into the surviving pods for a post-mortem.
    /// The 1h `janitor/ttl` annotation still reaps the namespace afterwards, so
    /// this never leaks permanently. Capacity accounting lives in the parent
    /// `ztest run` scheduler and is released when this test process exits.
    fn drop(&mut self) {
        // Pod path: the parent `ztest run` owns the namespace's lifecycle, the
        // log capture, and the dead-pod report (all over the kube API). Nothing
        // to do here — and deleting the namespace would race the parent's
        // still-draining collector, the very bug this split exists to remove.
        if std::env::var(naming::TEST_NAMESPACE_ENV).is_ok_and(|v| !v.is_empty()) {
            return;
        }

        // Stop the follow child and snapshot the interleaved component-log
        // timeline; it's emitted in the teardown thread below, after the
        // dead-pod headers. Colour is the reporter's own decision, propagated by
        // the engine as `ZTEST_COLOR`.
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

        // `--no-cleanup` preserves the namespace + seed bindings for inspection.
        let cleanup = !cluster::no_cleanup_requested();
        let (ns_to_delete, bindings_to_delete) = if cleanup {
            (ns.clone(), bindings)
        } else {
            if let Some(ns) = &ns {
                // eprintln (not just tracing) so the hint shows in captured
                // test output, where a developer is looking.
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
                let client = cluster::client()
                    .await
                    .map_err(|e| format!("teardown client: {e}"))?;
                // Capture any component pod that died before the namespace (and
                // with it the pod's terminal reason + logs) is deleted — the
                // client-side error is only ever "connection refused", which
                // can't tell an Evicted/OOMKilled pod (contention) from a
                // panicked one (a real component bug).
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

/// Wait for a dependency pod to become ready on the harness's "no flaky tests"
/// terms.
///
/// The load-bearing rule: a pod parked `Pending` on scheduling capacity is the
/// broker's backlog to clear, not a test failure, so it is waited on
/// indefinitely — the outer per-test hard cap (enforced by the parent
/// `ztest run` scheduler, which SIGKILLs the runner pod) is the only bound.
/// Over-allocation therefore never reddens a test. The `ready_timeout` clock
/// starts only once the pod is confirmed `Running`; from that point time-to-ready
/// is the application's responsibility and a blown deadline is a real signal. A
/// pod that enters an unrecoverable state (`CrashLoopBackOff`, `OOMKilled`, a
/// terminal image-pull error, or a `Failed` phase) fails fast rather than
/// waiting out the deadline.
///
/// Transient kube-API `get` errors are ignored (retry on the next poll), like
/// [`crate::engine`]'s runner-pod loop — a single API blip must not fail a test.
/// Emit one `TestEnv::build` phase's elapsed time on the `ztest::build`
/// diagnostics target. In-pod this reaches stdout (captured as the test's
/// output, shown per `--success-output`); it isolates which provisioning
/// step — namespace/quota, validator pod materialization, validator
/// readiness+warm (block-gen), or indexer materialization — a slow build spent
/// its time in, most of which is waiting on the cluster (pod schedule, image
/// pull, readiness probes) rather than compute.
fn build_phase(phase: &str, since: std::time::Instant) {
    tracing::debug!(
        target: "ztest::build",
        phase,
        elapsed_ms = since.elapsed().as_millis() as u64,
        "build phase"
    );
}

/// Shorten a fully-qualified image reference to its human-readable tail for the
/// provisioning diagnostics: the final path segment (`repo:tag`), dropping the
/// registry host and any leading path. `registry.example.com/lib/zebra:v6.2.0`
/// → `zebra:v6.2.0`; a bare `zebra:v6.2.0` is returned unchanged.
fn image_summary(image: &str) -> &str {
    image.rsplit('/').next().unwrap_or(image)
}

async fn await_pod_ready(
    pods: &Api<Pod>,
    name: &str,
    ready_timeout: Duration,
) -> Result<(), EnvError> {
    use crate::pod_status as ps;

    let mut running_since: Option<Instant> = None;
    let mut pull_error_since: Option<Instant> = None;
    loop {
        if let Ok(pod) = pods.get(name).await
            && let Some(status) = pod.status.as_ref()
        {
            if ps::is_ready(status) {
                return Ok(());
            }
            if let Some(reason) = ps::fault(status) {
                return Err(EnvError::PodFailed {
                    component: name.to_string(),
                    reason,
                });
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
                        return Err(EnvError::PodFailed {
                            component: name.to_string(),
                            reason,
                        });
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
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let s = s.trim_matches('-').to_string();
    if s.is_empty() {
        "x".into()
    } else {
        s.chars().take(20).collect()
    }
}

async fn apply_pod(
    ctx: &MaterializeCtx<'_>,
    spec: &PodSpec,
    mounts: &[ResolvedMount],
) -> Result<(), EnvError> {
    let pod = spec.render(ctx.coords, ctx.test_name, mounts)?;
    ctx.pods
        .create(&PostParams::default(), &pod)
        .await
        .map(|_| ())
        .map_err(env_err)
}

fn pod_name_of(opts: &ComponentOpts) -> String {
    short_kind(opts.name.as_deref().unwrap_or("x"))
}

/// NASA-style guard on an explicit `.resources()` override: a single pod may
/// never request more than the test's whole tier footprint. That is capacity
/// the parent scheduler never admitted the test for, so such a pod would wedge
/// `Pending` forever behind a quota it can't satisfy. A hard `panic!` in every
/// build — a test author sizing a pod past its tier is an authoring error we
/// surface immediately, never a silent hang. No-op when the test set no
/// override (the QoS reserve is tier-derived and fits by construction).
fn assert_override_within_tier(spec: &PodSpec, tier: crate::qos::Resources) {
    let Some(res) = spec.resources.as_ref() else {
        return;
    };
    use crate::qos::units::{parse_cpu_milli_opt, parse_mem_bytes_opt};
    let cpu = parse_cpu_milli_opt(&res.cpu).unwrap_or_else(|| {
        panic!(
            "pod {} .resources() cpu {:?} is not a valid Kubernetes quantity",
            spec.pod_name, res.cpu
        )
    });
    let mem = parse_mem_bytes_opt(&res.memory).unwrap_or_else(|| {
        panic!(
            "pod {} .resources() memory {:?} is not a valid Kubernetes quantity",
            spec.pod_name, res.memory
        )
    });
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

/// The per-pod Guaranteed reserve when a tier's footprint is split evenly
/// across `pods` pods (§7), shaped for maximum performance.
///
/// Integer CPU cores: the kubelet CPU Manager `static` policy only pins
/// exclusive CPUs to a Guaranteed pod whose CPU is a whole number of cores
/// (fractional falls to the shared pool, no pinning). So the per-pod CPU is the
/// even share rounded *down* to whole cores (min 1) — see [`per_pod_share`] for
/// why down, not up — still pinnable, and never summing past the tier reserve.
/// Memory is the floored even share (no integer rule). Rendered by
/// [`PodSpec`] as `requests == limits`, i.e. Guaranteed.
///
/// Reject any component whose `.testnet(_)` / `.mainnet(_)` verb disagrees with
/// the network its archive records.
///
/// The verb is redundant by construction — the config generator reads the
/// network off the artifact, so the call site cannot actually steer it. That
/// redundancy is the point: `.mainnet(testnet::ORCHARD)` would otherwise boot
/// the testnet chain under a call site that says mainnet, and every number the
/// test reports would describe a chain nobody asked for. A green run against
/// the wrong history is the one failure a snapshot suite must never produce, so
/// the disagreement is surfaced here rather than tolerated.
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

/// `None` when there are no pods to size.
fn even_share(
    footprint: crate::qos::Resources,
    pods: usize,
) -> Option<crate::component::Resources> {
    if pods == 0 {
        return None;
    }
    let (cores, mem_bytes) = per_pod_share(footprint, pods as u64);
    Some(crate::component::Resources {
        cpu: cores.to_string(),
        memory: mem_bytes.to_string(),
    })
}

/// The load-bearing "no over-schedule" guard: the component pods a test deploys
/// (`deployed`, the floored per-pod share summed over `pod_count`) must fit
/// within the tier's component `footprint` — the budget the parent scheduler
/// admitted the test against ([`crate::qos::QosProfile::admitted`] = this
/// footprint plus the separately-reserved runner pod). If it doesn't, the test
/// would place more CPU/memory on the cluster than was ever reserved for it, and
/// the surplus pods wedge `Pending` behind capacity the ledger already handed
/// out.
///
/// With floored per-pod cores this holds for every `pod_count ≤ footprint_cores`
/// (the whole point of flooring). It can only trip when a topology needs *more*
/// component pods than the tier has whole cores — each pod is clamped to a 1-core
/// minimum, so `pod_count` cores exceed the footprint. That's a genuinely
/// mispriced tier: raise its footprint so its core count is at least the
/// topology's pod count. Hard panic in every build so it surfaces at deploy, not
/// as a silent `Pending` hang later.
fn assert_deployed_within_tier(
    class: crate::qos::QosClass,
    deployed: crate::qos::Resources,
    pod_count: usize,
) {
    let footprint = class.profile().footprint;
    assert!(
        deployed.fits_within(&footprint),
        "QoS over-schedule: tier {class:?} deploys {}m cpu / {} B across {pod_count} \
         component pods, exceeding the tier's component budget of {}m cpu / {} B that \
         the scheduler admitted — raise the tier footprint so its core count is at \
         least this topology's pod count",
        deployed.cpu_milli,
        deployed.mem_bytes,
        footprint.cpu_milli,
        footprint.mem_bytes,
    );
}

/// The per-pod `(whole CPU cores, memory bytes)` an even footprint split
/// yields. Shared by [`even_share`] (what each pod requests) and
/// [`deployed_footprint`] (what the namespace quota reserves) so the two agree
/// exactly: CPU is the even share rounded *down* to whole cores (min 1), memory
/// is the floored even share.
///
/// Rounding down (not up) is load-bearing: it keeps `pods × per-pod ≤ footprint`
/// for any `pods ≤ footprint_cores`, so the deploy never exceeds the tier
/// reserve the scheduler admitted the test against — no over-schedule, no wedged
/// `Pending`. A whole (floored) core still qualifies for CPU-Manager static
/// pinning. The floor loses at most a sub-core per pod versus the even share;
/// that slack stays inside the reserved footprint as headroom. The `.max(1)`
/// only bites when `pods > footprint_cores`, which then trips
/// [`assert_deployed_within_tier`] — the tier is too small for that topology.
fn per_pod_share(footprint: crate::qos::Resources, pods: u64) -> (u64, u64) {
    // Guard the divisor and the input: a zero pod count divides by zero, and a
    // degenerate tier footprint would size a BestEffort pod. Both are harness
    // bugs (callers gate `pods == 0` before reaching here, and every tier
    // footprint is positive), so panic loudly rather than emit a bad pod.
    assert!(pods > 0, "per_pod_share: pod count must be > 0");
    footprint.assert_pod_schedulable("per_pod_share tier footprint");
    let cores = (footprint.cpu_milli / pods / 1000).max(1);
    let mem_bytes = (footprint.mem_bytes / pods).max(1);
    (cores, mem_bytes)
}

/// What the rendered pods actually request (per-pod whole-core share × `pods`),
/// sizing the per-test namespace's `ResourceQuota` so the quota admits exactly
/// the pods that deploy. Because the per-pod share is *floored* to whole cores,
/// this never exceeds the tier `footprint` for `pods ≤ footprint_cores` (e.g. 8
/// cores over 3 pods is 2+2+2 = 6, well inside the 8 the scheduler reserved).
/// Falls back to the tier footprint when there are no QoS pods to size (e.g. a
/// wallet-only env).
fn deployed_footprint(footprint: crate::qos::Resources, pods: usize) -> crate::qos::Resources {
    if pods == 0 {
        return footprint;
    }
    let p = pods as u64;
    let (cores, mem_bytes) = per_pod_share(footprint, p);
    crate::qos::Resources::new(
        cores.saturating_mul(1000).saturating_mul(p),
        mem_bytes.saturating_mul(p),
        0,
        0,
    )
}

struct MaterializeCtx<'a> {
    client: &'a kube::Client,
    pods: &'a Api<Pod>,
    sentinel: &'a Sentinel,
    coords: &'a RunCoords,
    test_name: &'a str,
}

/// One pod to materialize: `(id, spec, opts, backend handle)`. Shared by both
/// materialization phases (validators, then dependents); the handle's variant
/// distinguishes them.
type MaterializeItem = (u64, PodSpec, ComponentOpts, ComponentHandle);

#[cfg(test)]
mod tests {
    use super::{
        assert_claims_match_artifacts, assert_deployed_within_tier, deployed_footprint, even_share,
    };
    use crate::qos::{GIB, MIB, Resources};

    fn pin(
        handle: crate::ArchiveHandle,
        claimed: crate::ArchiveNetwork,
    ) -> (
        String,
        crate::ArchiveHandle,
        crate::ChainInfo,
        &'static str,
        Option<crate::ArchiveNetwork>,
    ) {
        let chain = handle.chain().expect("shipped snapshot carries chain info");
        (
            "zebrad".to_string(),
            handle,
            chain,
            "6.2.3",
            Some(claimed),
        )
    }

    /// The verb and the artifact must name the same network. Both directions,
    /// because a check that only caught one would leave the other silently
    /// booting the wrong chain.
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

    /// A regtest cache or bare tarball is reached through `.regtest()` /
    /// `.mount(_)`, which record no claim. Those must pass untouched rather than
    /// be compared against a network they never named.
    #[test]
    fn an_unclaimed_restore_is_not_network_checked() {
        let chain = crate::snapshots::mainnet::SAPLING
            .chain()
            .expect("chain info");
        assert_claims_match_artifacts(&[(
            "zebrad".to_string(),
            crate::snapshots::mainnet::SAPLING,
            chain,
            "6.2.3",
            None,
        )])
        .expect("a restore with no verb claim is not checked");
    }

    #[test]
    fn deployed_within_tier_passes_for_every_topology_up_to_the_core_count() {
        use crate::qos::QosClass;
        // Integration (3c/3Gi) supports 1..=3 component pods. Each pod floors to
        // one whole core, so the deployed total (1c, 2c, 3c) never tops the 3c
        // budget — the 3-pod zaino topology that used to over-schedule now fits.
        for pods in 1..=3 {
            let deployed = deployed_footprint(QosClass::Integration.profile().footprint, pods);
            assert_deployed_within_tier(QosClass::Integration, deployed, pods);
        }
    }

    #[test]
    #[should_panic(expected = "over-schedule")]
    fn deployed_within_tier_panics_when_topology_needs_more_pods_than_cores() {
        use crate::qos::QosClass;
        // 4 component pods on a 3-core tier: each is clamped to the 1-core
        // minimum, so 4c deploys against a 3c budget — the tier is genuinely too
        // small for this topology, and the guard must catch it.
        let deployed = deployed_footprint(QosClass::Integration.profile().footprint, 4);
        assert_deployed_within_tier(QosClass::Integration, deployed, 4);
    }

    #[test]
    fn even_share_floors_cpu_to_whole_cores_and_splits_memory() {
        // sync 16c/32Gi across 2 pods → 8 cores / 16 GiB each (exact).
        let s = even_share(Resources::new(16_000, 32 * GIB, 0, 0), 2).unwrap();
        assert_eq!(s.cpu, "8");
        assert_eq!(s.memory, (16 * GIB).to_string());

        // basic 500m/512Mi on 1 pod → floors below a core, clamped to the 1-core
        // minimum (pinning needs an integer); memory is the exact share.
        let s = even_share(Resources::new(500, 512 * MIB, 0, 0), 1).unwrap();
        assert_eq!(s.cpu, "1");
        assert_eq!(s.memory, (512 * MIB).to_string());

        // testnet 8c/18Gi across 3 pods → 2667m/pod → floor to 2 cores (the 3rd
        // core of slack stays as reserved headroom, never over-deployed).
        let s = even_share(Resources::new(8_000, 18 * GIB, 0, 0), 3).unwrap();
        assert_eq!(s.cpu, "2");

        // No pods → nothing to size.
        assert!(even_share(Resources::new(8_000, 8 * GIB, 0, 0), 0).is_none());
    }

    #[test]
    fn deployed_footprint_matches_what_the_rendered_pods_request() {
        // testnet 8c/18Gi over 3 pods: pods request 2 cores each (floor), so the
        // namespace quota reserves 6 cores — inside the 8 the scheduler admitted.
        // Memory is the floored share × pods.
        let fp = deployed_footprint(Resources::new(8_000, 18 * GIB, 0, 0), 3);
        let mem_per_pod = (18 * GIB) / 3;
        assert_eq!(fp.cpu_milli, 6_000, "3 pods × floor(2667m)=2c → 6 cores");
        assert_eq!(fp.mem_bytes, mem_per_pod * 3);

        // Even split (16c/2 pods = 8 each) reserves exactly the footprint.
        let fp = deployed_footprint(Resources::new(16_000, 32 * GIB, 0, 0), 2);
        assert_eq!(fp.cpu_milli, 16_000);
        assert_eq!(fp.mem_bytes, 32 * GIB);

        // No QoS pods (wallet-only env) → reserve the raw tier footprint.
        let raw = Resources::new(8_000, 8 * GIB, 0, 0);
        assert_eq!(deployed_footprint(raw, 0), raw);
    }

    // Flooring the per-pod share closes the whole-core-rounding over-admission
    // gap: because `deployed ≤ footprint` for every topology the tier admits, a
    // test's real placement (components + runner) never exceeds the `admitted()`
    // total the scheduler reserves, so a full wave can't be granted more than
    // physically fits. (Ceil rounding had the opposite property — 2c over 3 pods
    // deployed 3c — which is why this used to be a deferred bug.)
    #[test]
    fn flooring_keeps_the_deployed_wave_within_what_admission_reserved() {
        use crate::qos::QosClass;
        use crate::qos::scheduler::{Admission, Request, Scheduler};

        let profile = QosClass::Integration.profile();
        let cores = (profile.footprint.cpu_milli / 1000) as usize;

        // The invariant, per topology: components deployed + runner ≤ admitted.
        for pods in 1..=cores {
            let deployed = deployed_footprint(profile.footprint, pods);
            assert_deployed_within_tier(QosClass::Integration, deployed, pods);
            let real = deployed.cpu_milli + profile.runner.cpu_milli;
            assert!(
                real <= profile.admitted().cpu_milli,
                "{pods}-pod deploy is {real}m cpu, over the {}m admitted",
                profile.admitted().cpu_milli,
            );
        }

        // End to end: fill a cluster sized to an exact multiple of `admitted()`,
        // then confirm the worst-case real deploy (max components + runner, per
        // admitted test) still fits that same free capacity.
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
            deployed_footprint(profile.footprint, cores).cpu_milli + profile.runner.cpu_milli;
        let wave = max_real.saturating_mul(admitted);
        assert!(
            wave <= free.cpu_milli,
            "wave deploys {}c against {}c free",
            wave / 1000,
            free.cpu_milli / 1000,
        );
    }
}
