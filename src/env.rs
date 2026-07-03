//! The test environment.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::future::join_all;
use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, PostParams};
use kube::runtime::wait::await_condition;
use tokio::sync::Mutex;

use std::net::{IpAddr, Ipv4Addr};

use crate::EnvError;
use crate::cluster::{self, Sentinel};
use crate::component::{ComponentCategory, ComponentOpts, Indexer, Validator, Wallet};
use crate::error::env_err;
use crate::topology::ActivationHeights;

use crate::handles::indexer::{IndexerBackend, IndexerConfig};
use crate::handles::validator::{ValidatorBackend, ValidatorConfig};
use crate::handles::wallet::WalletConfig;
use crate::handles::{Endpoint, ForwardRegistry, HandleInner};
use crate::topology::NetworkUpgrade;

/// Config-time regtest materialization, captured per validator at
/// `add_validator` so the build-time topology resolver can apply it once
/// the activation heights are known, without retaining the concrete
/// backend or a dyn-erased config trait.
type RegtestMaterializeFn = Box<
    dyn FnOnce(
            ComponentOpts,
            &ActivationHeights,
            &[(String, u16)],
        ) -> Result<ComponentOpts, EnvError>
        + Send,
>;

/// Config-time regtest materialization for an indexer (takes the validator
/// host resolved at build time). Captured at `add_indexer`.
type IndexerMaterializeFn =
    Box<dyn FnOnce(ComponentOpts, Option<&str>) -> Result<ComponentOpts, EnvError> + Send>;
use crate::manifest::{self, PodSpec};
use crate::mounts::{self, ResolvedMount};
use crate::naming::{self, RunCoords};
use crate::portforward::Forwarder;
use crate::qos::allocator::{Allocator, Outcome, ReservationRequest};
use crate::qos::kube_store::KubeStore;
use crate::qos::{self, AdmitStep};
use crate::seeds::{self, ShadowClone};

/// Per-component bookkeeping captured at `build` time.
#[derive(Debug, Clone)]
pub(crate) struct ComponentState {
    pub(crate) namespace: String,
    pub(crate) pod_name: String,
    pub(crate) category: ComponentCategory,
    pub(crate) label: &'static str,
    pub(crate) named_ports: Vec<(String, u16)>,
    /// Live handle for a validator component (used by the env's own
    /// readiness/warm probes during `build`). `None` for non-validators.
    pub(crate) validator_handle: Option<Arc<dyn ValidatorBackend>>,
}

impl ComponentState {
    fn new(
        spec: &PodSpec,
        namespace: String,
        validator_handle: Option<Arc<dyn ValidatorBackend>>,
    ) -> Self {
        ComponentState {
            namespace,
            pod_name: spec.pod_name.clone(),
            category: spec.category,
            label: spec.label,
            named_ports: spec.ports.clone(),
            validator_handle,
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
    pub(crate) shadow_clones: std::sync::Mutex<Vec<ShadowClone>>,
    pub(crate) is_built: AtomicBool,
    /// The QoS capacity reservation Lease held for this env's life, if QoS
    /// admission was enabled and granted. `Some(name)` means a `qos-*` Lease in
    /// the `zaino-qos` namespace must be released on teardown.
    pub(crate) qos_reservation: std::sync::Mutex<Option<String>>,
    /// Background task heartbeating [`Self::qos_reservation`]. Aborted on Drop
    /// (the reservation is then released explicitly, not left to expiry).
    pub(crate) qos_renew_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
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
            .field(
                "qos_reservation",
                &self.qos_reservation.lock().ok().and_then(|g| g.clone()),
            )
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
            shadow_clones: std::sync::Mutex::new(Vec::new()),
            is_built: AtomicBool::new(false),
            qos_reservation: std::sync::Mutex::new(None),
            qos_renew_task: std::sync::Mutex::new(None),
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
    /// This backend's NU ceiling (already dev-image-skipped), fed to the
    /// topology resolver. `None` opts out.
    nu_ceiling: Option<NetworkUpgrade>,
    /// This backend's regtest materialization, applied once the resolver
    /// has chosen the activation heights. `take`n when applied.
    materialize: Option<RegtestMaterializeFn>,
    /// Live handle, threaded into the component's `ComponentState` so the
    /// env can drive readiness/warm probes through it during `build`.
    handle: Arc<dyn ValidatorBackend>,
    opts: ComponentOpts,
}

struct PendingIndexer {
    id: u64,
    /// Pod label, captured from the handle at `add_indexer` (the concrete
    /// backend isn't retained).
    label: &'static str,
    nu_ceiling: Option<NetworkUpgrade>,
    /// Regtest materialization closure; `Some` only for regtest indexers,
    /// `take`n when applied.
    materialize: Option<IndexerMaterializeFn>,
    opts: ComponentOpts,
}

struct PendingWallet {
    nu_ceiling: Option<NetworkUpgrade>,
    opts: ComponentOpts,
}

// ──────────────────────────── shared volume ───────────────────────────

/// Handle to an env-scoped `ReadWriteOnce` PVC shared between two
/// co-scheduled pods. Created via [`TestEnv::shared_volume`]; the PVC is
/// provisioned during [`TestEnv::build`]. Hand the same handle to a validator's
/// [`Validator::persistent_state_in`](crate::Validator::persistent_state_in)
/// and a zaino indexer's
/// [`Indexer::regtest_state_in`](crate::Indexer::regtest_state_in) so both
/// mount the same on-disk zebra-state database.
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
    /// In-pod path the shared volume is mounted at. Both sharing pods use
    /// this identical path so zebra's `db_path` resolves to the same
    /// directory on each side.
    pub fn mount_path(&self) -> &str {
        &self.mount_path
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
    /// [`build`](Self::build). A plain build-time knob: set it any time before
    /// `build` via [`ready_timeout`](Self::ready_timeout); it never touches the
    /// shared [`EnvInner`], so issued handles are unaffected.
    ready_timeout: Duration,
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
        }
    }

    /// Override the per-component readiness/RPC-probe budget used during
    /// [`build`](Self::build). Order-independent: may be called before or after
    /// `add_*`, since it sets a plain field rather than rebuilding the shared
    /// env state.
    pub fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// Declare an env-scoped shared volume named `name`. Returns a
    /// [`SharedVolume`] handle to hand to a validator's
    /// [`Validator::persistent_state_in`](crate::Validator::persistent_state_in)
    /// and a zaino indexer's
    /// [`Indexer::regtest_state_in`](crate::Indexer::regtest_state_in).
    /// The backing `ReadWriteOnce` PVC is provisioned during
    /// [`TestEnv::build`]. Both consumers mount it at the same in-pod
    /// path so zebrad and a colocated zaino StateService address one
    /// on-disk database.
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
            regtest: v.opts.regtest_mode.is_some(),
            coinbase_pool: Some(coinbase_pool),
        };
        // Build the live handle (returned to the caller + stored for the
        // env's probes). The concrete backend isn't retained, so capture
        // the config-time behaviour the topology resolver needs: the NU
        // ceiling (dev images have no parseable version, so skip) and the
        // regtest materialization as a deferred closure.
        let handle = v.backend.to_handle(plumbing);
        let dyn_handle: Arc<dyn ValidatorBackend> = Arc::new(handle.clone());
        let nu_ceiling = match v.opts.image {
            crate::backends::image::ImageSpec::Dev { .. } => None,
            _ => v.backend.nu_ceiling(&v.opts.version),
        };
        let backend = v.backend;
        let materialize: RegtestMaterializeFn = Box::new(move |opts, activation, peers| {
            backend.materialize_regtest_opts(opts, activation, peers)
        });
        self.pending_validators.push(PendingValidator {
            id,
            nu_ceiling,
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
            regtest: i.opts.regtest_mode.is_some(),
            coinbase_pool: None,
        };
        let handle = i.backend.to_handle(plumbing);
        let label = handle.label();
        let nu_ceiling = match i.opts.image {
            crate::backends::image::ImageSpec::Dev { .. } => None,
            _ => i.backend.nu_ceiling(&i.opts.version),
        };
        // Capture the regtest materialization closure only for regtest
        // indexers; it gets the validator host resolved at build time.
        let materialize: Option<IndexerMaterializeFn> = i.regtest_backend.map(|regtest_backend| {
            let backend = i.backend;
            Box::new(move |opts, validator_host: Option<&str>| {
                backend.materialize_regtest_opts(opts, Some(regtest_backend), validator_host)
            }) as IndexerMaterializeFn
        });
        self.pending_indexers.push(PendingIndexer {
            id,
            label,
            nu_ceiling,
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
            regtest: w.opts.regtest_mode.is_some(),
            coinbase_pool: None,
        };
        let handle = w.backend.to_handle(plumbing);
        let nu_ceiling = match w.opts.image {
            crate::backends::image::ImageSpec::Dev { .. } => None,
            _ => w.backend.nu_ceiling(&w.opts.version),
        };
        self.pending_wallets.push(PendingWallet {
            nu_ceiling,
            opts: w.opts,
        });
        handle
    }

    fn validate_topology(&self) -> Result<(), EnvError> {
        // Deliberate v1 caps, not fundamental limits. The build wiring
        // resolves a single validator host (`materialize_regtest_configs`
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

    fn materialize_regtest_configs(&mut self) -> Result<(), EnvError> {
        use crate::component::RegtestMode;
        use crate::topology::{activation_heights_for_ceiling, resolve_ceiling};

        // Collect each component's reported NU ceiling. The per-component
        // `nu_ceiling` values were already dev-image-skipped at `add_*`.
        let mut ceilings: Vec<NetworkUpgrade> = Vec::new();
        for p in &self.pending_validators {
            // `nu_ceiling` was already dev-image-skipped at `add_validator`.
            if let Some(c) = p.nu_ceiling {
                ceilings.push(c);
            }
        }
        for p in &self.pending_indexers {
            // `nu_ceiling` was already dev-image-skipped at `add_indexer`.
            if let Some(c) = p.nu_ceiling {
                ceilings.push(c);
            }
        }
        for p in &self.pending_wallets {
            if let Some(c) = p.nu_ceiling {
                ceilings.push(c);
            }
        }

        let resolved = resolve_ceiling(&ceilings);
        let mut ceiling = resolved;
        for p in &self.pending_validators {
            if let Some(RegtestMode::ActivateThrough(requested)) = &p.opts.regtest_mode {
                if *requested > resolved {
                    return Err(EnvError::Config {
                        reason: format!(
                            "validator {:?} requested NU ceiling {:?}, but topology only \
                             supports up to {:?} (one or more pinned components is too old)",
                            p.opts.name, requested, resolved
                        ),
                    });
                }
                ceiling = ceiling.max(*requested);
            }
        }

        let activation = activation_heights_for_ceiling(ceiling);
        tracing::info!(
            ceiling = ?ceiling,
            "topology activation-height ceiling resolved"
        );

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
            if p.opts.regtest_mode.is_some()
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

        let _ = NetworkUpgrade::HIGHEST;
        Ok(())
    }

    /// QoS admission (`docs/qos-design.md` §5, §7).
    ///
    /// When QoS is enabled (`ztest run` set `ZTEST_QOS`), reserve this test's
    /// tier footprint against shared cluster capacity before any pod is
    /// created, via the decentralized in-process [`Allocator`] over the
    /// `zaino-qos` Lease ledger. Blocks while the request is queued for
    /// capacity (up to [`qos::ADMIT_BUDGET`]); fails fast if the footprint is
    /// unschedulable even on an empty cluster. On grant it stores the
    /// reservation name and spawns a heartbeat task that renews the Lease for
    /// the env's life; [`Drop`] aborts the heartbeat and releases the Lease.
    ///
    /// When QoS is disabled (a developer running `cargo nextest run`
    /// directly), this is a no-op: admission is skipped and the harness behaves
    /// exactly as before.
    async fn admit(
        &self,
        client: &Client,
        namespace: &str,
        coords: &RunCoords,
    ) -> Result<(), EnvError> {
        if !cluster::qos_enabled() {
            return Ok(());
        }
        let profile = qos::current().profile();

        // Per-test re-probe: the parent `ztest run` probe result doesn't cross
        // the nextest process boundary, so size the allocator's ceiling from a
        // live probe. Use the admission ceiling (allocatable minus non-ztest
        // baseline), not `free()`: the allocator must reject only footprints
        // that don't fit an empty-of-ztest cluster, and queue everything else.
        // ztest's own concurrent load is accounted exactly once, via the
        // ledger's committed reservations inside `decide()`; counting it again
        // here (as `free()` does, by subtracting every ztest pod's requests)
        // collapses the ceiling under load and turns queue-able tests into
        // spurious `ExceedsClusterCapacity` rejections.
        let ceiling = crate::pipeline::cluster::capacity(client)
            .await
            .map_err(env_err)?
            .admission_ceiling();

        // Sync-tier pods carry an NVMe nodeSelector (§7). The global ceiling
        // figure above can't see that this footprint only fits on NVMe nodes,
        // so check the NVMe pool directly: with none schedulable, fail fast
        // here instead of admitting and leaving the pod Pending on an
        // unsatisfiable selector until `ready_timeout`.
        if profile.pool == qos::Pool::Nvme {
            let nvme = crate::pipeline::cluster::nvme_node_count(client)
                .await
                .map_err(env_err)?;
            if nvme == 0 {
                return Err(EnvError::Config {
                    reason: format!(
                        "QoS tier {:?} requires an NVMe-pool node ({}={}) but none are \
                         schedulable; cannot place this test's pods",
                        qos::current(),
                        qos::NVME_NODE_LABEL_KEY,
                        qos::NVME_NODE_LABEL_VALUE,
                    ),
                });
            }
        }

        // The Lease ledger lives in a single ztest-managed namespace; create
        // it before the allocator issues any read/write against it (the
        // store has no other path to bootstrap it, and a missing namespace
        // is a 404 on every op, not a transient).
        let store = KubeStore::with_default_namespace(client.clone());
        store.ensure_namespace().await.map_err(env_err)?;

        let mut allocator = Allocator::new(
            store,
            ceiling,
            coords.run_id.clone(),
            qos::LOCK_TICKS,
            qos::RESERVATION_TICKS,
            qos::GRACE,
        );

        // ServiceAccount budget (§5.6): the run's reservations are charged to
        // its SA, identified by `ZTEST_SA` (the runner sets it, e.g. via the
        // downward API). When running in-cluster, read that SA's budget
        // annotations and enforce them; out-of-cluster (dev) or with no
        // `ZTEST_SA` the SA is unbudgeted (the allocator treats absent budget
        // as unlimited), and reservations are charged under the run id.
        let sa_id = std::env::var("ZTEST_SA").unwrap_or_else(|_| coords.run_id.clone());
        if cluster::in_cluster() && std::env::var_os("ZTEST_SA").is_some() {
            match read_sa_budget(client, client.default_namespace(), &sa_id).await {
                Ok(Some(budget)) => {
                    tracing::info!(
                        sa = %sa_id,
                        cpu_milli = budget.cpu_milli,
                        mem_bytes = budget.mem_bytes,
                        "QoS SA budget enforced"
                    );
                    allocator.set_budget(sa_id.clone(), budget);
                }
                Ok(None) => {}
                // A present-but-unparseable budget fails fast with a clear
                // message rather than silently admitting (or rejecting all).
                Err(detail) => {
                    return Err(EnvError::Config {
                        reason: format!("QoS ServiceAccount budget for {sa_id:?}: {detail}"),
                    });
                }
            }
        }

        // Opportunistically GC reservations left by crashed runs (the ledger
        // already excludes them from committed capacity; this just stops the
        // Lease objects accumulating). Best-effort; never block admission.
        let _ = allocator.reclaim_expired(qos::now_secs()).await;

        // Reserve what the pods will actually request (per-pod whole-core share
        // × pod count), not the raw tier footprint; see `deployed_footprint`.
        let pods = self.pending_validators.len() + self.pending_indexers.len();
        let req = ReservationRequest {
            unit: namespace.to_string(),
            sa: sa_id,
            footprint: deployed_footprint(profile.footprint, pods),
            class: qos::current(),
        };

        let started = std::time::Instant::now();
        let reservation = loop {
            let outcome = allocator
                .try_admit(&req, qos::now_secs())
                .await
                .map_err(env_err)?;
            match qos::classify(&outcome, started.elapsed(), qos::ADMIT_BUDGET) {
                AdmitStep::Proceed => {
                    let Outcome::Granted { reservation } = outcome else {
                        unreachable!("classify mapped a non-grant to Proceed")
                    };
                    break reservation;
                }
                AdmitStep::RetryNow => continue,
                AdmitStep::WaitThenRetry => tokio::time::sleep(qos::QUEUE_POLL).await,
                AdmitStep::Reject(reason) => {
                    return Err(EnvError::Config {
                        reason: format!(
                            "QoS admission rejected tier {:?} ({} mcpu / {} bytes): {reason:?} — \
                             unschedulable even on an empty cluster",
                            qos::current(),
                            profile.footprint.cpu_milli,
                            profile.footprint.mem_bytes,
                        ),
                    });
                }
                AdmitStep::Timedout => {
                    return Err(EnvError::RpcTimeout {
                        component: namespace.to_string(),
                        op: "qos_admission",
                        elapsed: started.elapsed(),
                    });
                }
            }
        };

        tracing::info!(
            reservation = %reservation,
            tier = ?qos::current(),
            "QoS reservation granted"
        );
        *self
            .inner
            .qos_reservation
            .lock()
            .expect("qos_reservation mutex poisoned") = Some(reservation.clone());

        // Heartbeat the reservation for the env's life on its own allocator
        // clone (the KubeStore is a cheap Arc-backed handle).
        let renew_alloc = Allocator::new(
            KubeStore::with_default_namespace(client.clone()),
            ceiling,
            coords.run_id.clone(),
            qos::LOCK_TICKS,
            qos::RESERVATION_TICKS,
            qos::GRACE,
        );
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(qos::RENEW_INTERVAL).await;
                if let Err(e) = renew_alloc.renew(&reservation, qos::now_secs()).await {
                    tracing::warn!(
                        error = %e,
                        reservation = %reservation,
                        "QoS reservation renew failed"
                    );
                }
            }
        });
        *self
            .inner
            .qos_renew_task
            .lock()
            .expect("qos_renew_task mutex poisoned") = Some(handle);

        Ok(())
    }

    pub async fn build(&mut self) -> Result<(), EnvError> {
        self.validate_topology()?;
        self.materialize_regtest_configs()?;

        let started = std::time::Instant::now();
        let coords = RunCoords::from_env().map_err(env_err)?;
        // Raw `module::test` (for the namespace annotation + name) and its
        // DNS-safe slug (for every label value; `::` is illegal in labels).
        let test_raw = naming::current_test_name();
        let package = naming::current_package();
        let test_slug = naming::slug(&test_raw, 63);
        let test_id = naming::test_suffix();
        let namespace = naming::namespace_for(&package, &test_raw, &test_id);
        let client = cluster::client().await.map_err(env_err)?;

        tracing::info!(
            namespace = %namespace,
            test = %test_raw,
            validators = self.pending_validators.len(),
            indexers = self.pending_indexers.len(),
            wallets = self.pending_wallets.len(),
            "building TestEnv"
        );

        cluster::ensure_namespace(&client, &namespace, &coords, &package, &test_raw)
            .await
            .map_err(env_err)?;
        let sentinel = Sentinel::new(namespace.clone());
        let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
        let test_name = test_slug;

        let _ = self.inner.client.set(client.clone());
        *self
            .inner
            .namespace
            .lock()
            .expect("namespace mutex poisoned") = Some(namespace.clone());

        // QoS admission (no-op unless `ztest run` enabled it): hold a capacity
        // reservation before creating any pod, and decide the tier's node
        // placement to stamp on every pod spec below.
        self.admit(&client, &namespace, &coords).await?;
        let qos_placement = cluster::qos_enabled().then(|| qos::current().profile().pool);
        // QoS-default pod sizing (§7): split the tier footprint evenly across
        // the env's pods (validators + indexers; wallets are in-process) as
        // requests==limits, i.e. Guaranteed QoS. A test's explicit
        // `.resources()` overrides this per-pod. `None` when QoS is off or
        // there are no pods.
        let qos_guaranteed = if cluster::qos_enabled() {
            let pods = self.pending_validators.len() + self.pending_indexers.len();
            even_share(qos::current().profile().footprint, pods)
        } else {
            None
        };

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
                let mut spec =
                    manifest::pod_spec_for_validator(p.handle.label(), &p.opts, pod_name);
                spec.placement = qos_placement;
                if spec.resources.is_none() {
                    spec.guaranteed = qos_guaranteed.clone();
                }
                (p.id, spec, p.opts, Some(p.handle))
            })
            .collect();
        self.materialize_phase(&ctx, &validators).await?;
        // The env's own readiness/warm probes drive the validators through
        // their handles, which gate endpoint resolution on `is_built`.
        // Flip it on for the probe window, then back off until the whole build
        // completes, so a Phase-2 failure still leaves test-side handle calls
        // reporting `NotBuilt`.
        self.inner.is_built.store(true, Ordering::Release);
        let warmup = async {
            self.wait_validators_rpc_ready().await?;
            self.warm_validators().await?;
            Ok::<(), EnvError>(())
        }
        .await;
        self.inner.is_built.store(false, Ordering::Release);
        warmup?;

        // Phase 2: indexers. (Wallets run in-process; see below.)
        let dependents: Vec<_> = self
            .pending_indexers
            .drain(..)
            .map(|p| {
                let pod_name = pod_name_of(&p.opts);
                let mut spec = manifest::pod_spec_for_indexer(p.label, &p.opts, pod_name)?;
                spec.placement = qos_placement;
                if spec.resources.is_none() {
                    spec.guaranteed = qos_guaranteed.clone();
                }
                Ok::<_, EnvError>((p.id, spec, p.opts, None))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Wallets run in-process in the test binary (libraries that connect to
        // the indexer over gRPC), so they get no pod. Their nu_ceiling was
        // already folded into the topology resolver in
        // `materialize_regtest_configs`; here we just drop the pending entries.
        // Account construction happens lazily, on demand, via
        // `WalletHandle::account`.
        self.pending_wallets.clear();
        self.materialize_phase(&ctx, &dependents).await?;

        self.inner.is_built.store(true, Ordering::Release);

        tracing::info!(
            namespace = %namespace,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "TestEnv ready"
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
                .filter(|s| matches!(s.category, ComponentCategory::Validator))
                .filter_map(|s| {
                    s.validator_handle
                        .as_ref()
                        .map(|h| (s.pod_name.clone(), Arc::clone(h)))
                })
                .collect()
        };
        if validators.is_empty() {
            return Ok(());
        }

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

    async fn warm_validators(&self) -> Result<(), EnvError> {
        // Mine one block per validator so dependents (indexers) sync
        // against a non-genesis tip. Drives each validator's handle.
        let handles: Vec<Arc<dyn ValidatorBackend>> = {
            let comps = self.inner.components.read().await;
            comps
                .values()
                .filter(|s| matches!(s.category, ComponentCategory::Validator))
                .filter_map(|s| s.validator_handle.as_ref().map(Arc::clone))
                .collect()
        };
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
        for (id, spec, opts, validator_handle) in items {
            let state = ComponentState::new(
                spec,
                ctx.sentinel.namespace.clone(),
                validator_handle.clone(),
            );
            cluster::create_pod_service(
                ctx.client,
                &ctx.sentinel.namespace,
                &spec.pod_name,
                &spec.ports,
            )
            .await
            .map_err(env_err)?;
            let resolved =
                mounts::resolve_all(ctx.client, ctx.sentinel, &spec.pod_name, &opts.mounts).await?;
            self.inner
                .shadow_clones
                .lock()
                .expect("shadow_clones mutex poisoned")
                .extend(resolved.shadow_clones);
            apply_pod(ctx, spec, &resolved.mounts).await?;
            self.inner.components.write().await.insert(*id, state);
        }

        let timeout = self.ready_timeout;
        let waits = items.iter().map(|(_, spec, _, _)| {
            let pods = ctx.pods.clone();
            let name = spec.pod_name.clone();
            async move {
                match tokio::time::timeout(timeout, await_condition(pods, &name, is_pod_ready()))
                    .await
                {
                    Ok(Ok(_)) => Ok::<(), EnvError>(()),
                    Ok(Err(e)) => Err(EnvError::Transient(Box::new(e))),
                    Err(_) => Err(EnvError::RpcTimeout {
                        component: name,
                        op: "pod_ready",
                        elapsed: timeout,
                    }),
                }
            }
        });
        for res in join_all(waits).await {
            res?;
        }
        Ok(())
    }
}

impl Drop for TestEnv {
    /// Teardown is Drop-only and runs to completion here, pass or fail.
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
    /// this never leaks permanently.
    ///
    /// The QoS capacity reservation is handled separately: the renew heartbeat
    /// is aborted and the reservation Lease is always released, even under
    /// `--no-cleanup`. Preserving pods for inspection must not leak the
    /// capacity accounting, or the reserve would linger until the Lease expired
    /// and starve concurrent runs.
    fn drop(&mut self) {
        // Stop heartbeating immediately so the reservation can't be renewed
        // out from under the release below.
        if let Some(task) = self
            .inner
            .qos_renew_task
            .lock()
            .ok()
            .and_then(|mut g| g.take())
        {
            task.abort();
        }
        let reservation = self
            .inner
            .qos_reservation
            .lock()
            .ok()
            .and_then(|mut g| g.take());

        let ns = self.inner.namespace.lock().ok().and_then(|mut g| g.take());
        let shadows: Vec<_> = self
            .inner
            .shadow_clones
            .lock()
            .ok()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default();
        if ns.is_none() && shadows.is_empty() && reservation.is_none() {
            return;
        }

        // `--no-cleanup` preserves the namespace + shadows for inspection, but
        // the reservation is still released (see the doc-comment).
        let cleanup = !cluster::no_cleanup_requested();
        let (ns_to_delete, shadows_to_delete) = if cleanup {
            (ns.clone(), shadows)
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
                shadow_clones = shadows.len(),
                "ZTEST_NO_CLEANUP set — leaving TestEnv namespace for inspection"
            );
            (None, Vec::new())
        };

        tracing::info!(
            namespace = ?ns_to_delete,
            shadow_clones = shadows_to_delete.len(),
            reservation = ?reservation,
            "tearing down TestEnv (Drop)"
        );
        let outcome = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("teardown runtime: {e}"))?;
            rt.block_on(async move {
                let client = cluster::client()
                    .await
                    .map_err(|e| format!("teardown client: {e}"))?;
                // Release the QoS reservation first (404-tolerant; the
                // throwaway allocator's config is irrelevant for a delete).
                if let Some(reservation) = reservation {
                    let allocator = Allocator::new(
                        KubeStore::with_default_namespace(client.clone()),
                        crate::qos::Resources::ZERO,
                        "",
                        0,
                        0,
                        0,
                    );
                    if let Err(e) = allocator.release(&reservation).await {
                        tracing::warn!(
                            error = %e,
                            reservation = %reservation,
                            "QoS reservation release failed (reclaimed on expiry)"
                        );
                    }
                }
                if let Some(ns) = ns_to_delete {
                    cluster::delete_namespace(&client, &ns)
                        .await
                        .map_err(|e| format!("delete namespace {ns}: {e}"))?;
                }
                for shadow in shadows_to_delete {
                    if let Err(e) = seeds::delete_shadow(&client, &shadow).await {
                        tracing::warn!(
                            error = %e,
                            vsc = %shadow.shadow_vsc_name,
                            "shadow VSC delete failed"
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

fn is_pod_ready() -> impl kube::runtime::wait::Condition<Pod> {
    |pod: Option<&Pod>| {
        pod.and_then(|p| p.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
            .unwrap_or(false)
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

/// The per-pod Guaranteed reserve when a tier's footprint is split evenly
/// across `pods` pods (§7), shaped for maximum performance.
///
/// Integer CPU cores: the kubelet CPU Manager `static` policy only pins
/// exclusive CPUs to a Guaranteed pod whose CPU is a whole number of cores
/// (fractional falls to the shared pool, no pinning). So the per-pod CPU is the
/// even share rounded up to whole cores (min 1), eligible for exclusive pinning
/// at the cost of slightly over-reserving small tiers vs the admission
/// footprint. Memory is the exact even share (no integer rule). Rendered by
/// [`manifest::PodSpec`] as `requests == limits`, i.e. Guaranteed.
///
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

/// The per-pod `(whole CPU cores, memory bytes)` an even footprint split
/// yields. Shared by [`even_share`] (what each pod requests) and
/// [`deployed_footprint`] (what admission reserves) so the two agree exactly:
/// CPU is the even share rounded up to whole cores (static-policy pinning; min
/// 1), memory is the exact even share.
fn per_pod_share(footprint: crate::qos::Resources, pods: u64) -> (u64, u64) {
    let cores = (footprint.cpu_milli / pods).div_ceil(1000).max(1);
    let mem_bytes = (footprint.mem_bytes / pods).max(1);
    (cores, mem_bytes)
}

/// The footprint admission must reserve: exactly what the rendered pods request
/// (per-pod whole-core share × `pods`), not the raw tier footprint.
///
/// Rounding the per-pod CPU up to whole cores (for static-policy pinning) means
/// `pods × per-pod` can exceed the tier footprint (e.g. 8 cores over 3 pods is
/// 3+3+3 = 9). If admission reserved only the raw 8, the ledger would
/// under-count and the cluster could grant capacity a pod then can't schedule
/// into (a silent Pending). Reserving the deployed total keeps the ledger and
/// the pods consistent. Falls back to the tier footprint when there are no QoS
/// pods to size (e.g. a wallet-only env).
fn deployed_footprint(footprint: crate::qos::Resources, pods: usize) -> crate::qos::Resources {
    if pods == 0 {
        return footprint;
    }
    let p = pods as u64;
    let (cores, mem_bytes) = per_pod_share(footprint, p);
    crate::qos::Resources::new(
        cores.saturating_mul(1000).saturating_mul(p),
        mem_bytes.saturating_mul(p),
    )
}

/// Read a ServiceAccount's QoS budget from its annotations (§5.6).
///
/// Infrastructure-best-effort: a missing SA or a transient API error yields
/// `Ok(None)` (unbudgeted/unlimited) so cluster hiccups never block admission.
/// But a budget annotation that is present and unparseable returns `Err`: a
/// typo'd budget must fail the run loudly, not silently become a zero
/// (rejecting every request) or be ignored. The parsing is the pure
/// [`qos::parse_sa_budget`].
async fn read_sa_budget(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<Option<crate::qos::Resources>, String> {
    use k8s_openapi::api::core::v1::ServiceAccount;
    let api: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);
    let Some(sa) = api.get_opt(name).await.ok().flatten() else {
        return Ok(None);
    };
    let Some(annotations) = sa.metadata.annotations.as_ref() else {
        return Ok(None);
    };
    qos::parse_sa_budget(annotations)
}

struct MaterializeCtx<'a> {
    client: &'a kube::Client,
    pods: &'a Api<Pod>,
    sentinel: &'a Sentinel,
    coords: &'a RunCoords,
    test_name: &'a str,
}

/// One pod to materialize: `(id, spec, opts, optional validator backend)`.
/// Shared by both materialization phases (validators, then dependents).
type MaterializeItem = (
    u64,
    PodSpec,
    ComponentOpts,
    Option<Arc<dyn ValidatorBackend>>,
);

#[cfg(test)]
mod tests {
    use super::{deployed_footprint, even_share};
    use crate::qos::{GIB, MIB, Resources};

    #[test]
    fn even_share_rounds_cpu_up_to_whole_cores_and_splits_memory() {
        // sync 16c/32Gi across 2 pods → 8 cores / 16 GiB each (exact).
        let s = even_share(Resources::new(16_000, 32 * GIB), 2).unwrap();
        assert_eq!(s.cpu, "8");
        assert_eq!(s.memory, (16 * GIB).to_string());

        // basic 500m/512Mi on 1 pod → rounds up to 1 whole core (pinning needs
        // an integer); memory is the exact share.
        let s = even_share(Resources::new(500, 512 * MIB), 1).unwrap();
        assert_eq!(s.cpu, "1");
        assert_eq!(s.memory, (512 * MIB).to_string());

        // testnet 8c/18Gi across 3 pods → 2667m/pod → ceil to 3 cores.
        let s = even_share(Resources::new(8_000, 18 * GIB), 3).unwrap();
        assert_eq!(s.cpu, "3");

        // No pods → nothing to size.
        assert!(even_share(Resources::new(8_000, 8 * GIB), 0).is_none());
    }

    #[test]
    fn deployed_footprint_matches_what_the_rendered_pods_request() {
        // testnet 8c/18Gi over 3 pods: pods request 3 cores each (ceil), so
        // admission must reserve 9 cores (not the raw 8) or the 3rd pod could
        // pend on capacity the ledger under-counted. Memory is the floored
        // share × pods.
        let fp = deployed_footprint(Resources::new(8_000, 18 * GIB), 3);
        let mem_per_pod = (18 * GIB) / 3;
        assert_eq!(fp.cpu_milli, 9_000, "3 pods × ceil(2667m)=3c → 9 cores");
        assert_eq!(fp.mem_bytes, mem_per_pod * 3);

        // Even split (16c/2 pods = 8 each) reserves exactly the footprint.
        let fp = deployed_footprint(Resources::new(16_000, 32 * GIB), 2);
        assert_eq!(fp.cpu_milli, 16_000);
        assert_eq!(fp.mem_bytes, 32 * GIB);

        // No QoS pods (wallet-only env) → reserve the raw tier footprint.
        let raw = Resources::new(8_000, 8 * GIB);
        assert_eq!(deployed_footprint(raw, 0), raw);
    }
}
