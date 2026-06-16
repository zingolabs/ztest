//! The test environment: builder methods, live env, and the
//! `Arc<EnvInner>` / `Weak<EnvInner>` plumbing that lets handles
//! borrow nothing from the env yet dispatch through it at call time.
//!
//! Lifecycle:
//! 1. `TestEnv::builder()` → empty `TestEnv` in the unbuilt state.
//! 2. `env.add_validator(Validator::zebrad("..").named("..").mount(..))`
//!    → returns an owned `ValidatorHandle`. The handle works after
//!    `build()` and returns `EnvError::NotBuilt` before it.
//! 3. `env.build().await` — applies Pods, waits for readiness, flips
//!    the `is_built` flag.
//! 4. Test code drives the handles directly.
//! 5. `env.teardown().await` (or `Drop`) deletes the per-test
//!    namespace; k8s cascade GC reaps every namespaced resource.

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

use crate::cluster::{self, Sentinel};
use crate::component::{ComponentKind, Indexer, Validator, Wallet};
use crate::error::env_err;
use crate::handles::{Endpoint, ForwardRegistry, IndexerHandle, ValidatorHandle, WalletHandle};
use crate::manifest::{self, PodSpec};
use crate::mounts::{self, ResolvedMount};
use crate::naming::{self, RunCoords};
use crate::portforward::Forwarder;
use crate::seeds::{self, ShadowClone};
use crate::EnvError;

/// Per-component bookkeeping captured at `build` time. Internal — only
/// the in-crate endpoint resolver reads this.
///
/// `pod_name` doubles as the in-cluster DNS short-name. A same-named
/// `ClusterIP` Service (see `cluster::create_pod_service`) makes
/// `{pod_name}.{namespace}.svc.cluster.local` resolve to the pod.
#[derive(Debug, Clone)]
pub(crate) struct ComponentState {
    pub(crate) namespace: String,
    pub(crate) pod_name: String,
    pub(crate) kind: ComponentKind,
    pub(crate) named_ports: Vec<(String, u16)>,
}

impl ComponentState {
    fn new(spec: &PodSpec, namespace: String) -> Self {
        ComponentState {
            namespace,
            pod_name: spec.pod_name.clone(),
            kind: spec.kind,
            named_ports: spec.ports.clone(),
        }
    }
}

// ────────────────────────────── EnvInner ──────────────────────────────

/// Shared state behind every `TestEnv` and every handle. Lives inside
/// an `Arc`; handles hold a `Weak<EnvInner>` so they can dispatch
/// through it without keeping the env alive past the test scope.
///
/// Constructed by `TestEnv::builder()` in an empty state, then filled
/// by `build()`: the kube `Client` and namespace are set, components
/// are inserted, and `is_built` flips to `true`. Handle methods
/// short-circuit with `EnvError::NotBuilt` before that point.
pub(crate) struct EnvInner {
    /// Set inside `build()`. Reads via `client_ref()` return `NotBuilt`
    /// if the env hasn't been built yet.
    pub(crate) client: OnceLock<Client>,
    /// Per-test namespace. `None` before build and after teardown.
    pub(crate) namespace: std::sync::Mutex<Option<String>>,
    /// Built up by `add_*`; frozen after `build()` returns.
    pub(crate) components: tokio::sync::RwLock<HashMap<u64, ComponentState>>,
    pub(crate) in_cluster: bool,
    pub(crate) forwards: ForwardRegistry,
    /// Cluster-scoped shadow VSCs minted during build — k8s GC can't
    /// cascade these across the namespace boundary, so `teardown`
    /// deletes them explicitly.
    pub(crate) shadow_clones: std::sync::Mutex<Vec<ShadowClone>>,
    /// `false` during the builder phase; flipped to `true` after
    /// `build()` returns. Every handle method checks this and returns
    /// `EnvError::NotBuilt` if false.
    pub(crate) is_built: AtomicBool,
    /// Per-component readiness budget used by `build()` when waiting
    /// for pods to become Ready and RPCs to come up. Set via
    /// [`TestEnv::ready_timeout`]; defaults to
    /// [`TestEnv::DEFAULT_READY_TIMEOUT`].
    pub(crate) ready_timeout: Duration,
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
    fn new(ready_timeout: Duration) -> Self {
        EnvInner {
            client: OnceLock::new(),
            namespace: std::sync::Mutex::new(None),
            components: tokio::sync::RwLock::new(HashMap::new()),
            in_cluster: cluster::in_cluster(),
            forwards: Arc::new(Mutex::new(HashMap::new())),
            shadow_clones: std::sync::Mutex::new(Vec::new()),
            is_built: AtomicBool::new(false),
            ready_timeout,
        }
    }

    /// Return the live kube `Client`. Errors with `NotBuilt` if
    /// `build()` hasn't run.
    pub(crate) fn client_ref(&self) -> Result<&Client, EnvError> {
        self.client.get().ok_or(EnvError::NotBuilt)
    }

    /// Component state by id. Returns `EnvDropped` if the id is
    /// unknown — which only happens if a handle was forged with a
    /// bad id (we don't expose that path).
    pub(crate) async fn component_state(&self, id: u64) -> Result<ComponentState, EnvError> {
        let map = self.components.read().await;
        map.get(&id).cloned().ok_or(EnvError::EnvDropped)
    }

    /// Resolve a named port (e.g. `"rpc"`, `"grpc"`) on the given
    /// component into a dialable `Endpoint`.
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
                component: state.kind.as_label().to_string(),
                name: name.to_string(),
            })?;
        self.resolve_port(state, port).await
    }

    /// Resolve a container-port number into a dialable `Endpoint`.
    /// In-cluster: returns the pod IP. Out-of-cluster: lazily starts a
    /// port-forwarder (cached in `self.forwards`).
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

// ────────────────────────────── TestEnv ───────────────────────────────

/// The unified test environment. `TestEnv::builder()` returns one in
/// the unbuilt state; `add_validator` / `add_indexer` / `add_wallet`
/// register components and return handles immediately; `build().await`
/// applies the manifests and flips the live flag.
///
/// Handle methods called before `build()` return `EnvError::NotBuilt`.
pub struct TestEnv {
    inner: Arc<EnvInner>,
    pending_validators: Vec<(u64, Validator)>,
    pending_indexers: Vec<(u64, Indexer)>,
    pending_wallets: Vec<(u64, Wallet)>,
    next_id: u64,
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
    /// Default per-component readiness budget. Applies to:
    ///   - the pod-Ready wait inside `build()` (`materialize_phase`),
    ///   - each validator's JSON-RPC `getblocktemplate` probe.
    ///
    /// Calibrated for a warm cluster with the test images already
    /// pulled. Cold-pull or unusually heavy tests should call
    /// [`Self::ready_timeout`] to extend it.
    pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(20);

    /// New, empty environment in the unbuilt state. Add components
    /// via `add_validator` / `add_indexer` / `add_wallet`; the handles
    /// returned from those methods become live after `build().await`.
    pub fn builder() -> Self {
        Self {
            inner: Arc::new(EnvInner::new(Self::DEFAULT_READY_TIMEOUT)),
            pending_validators: Vec::new(),
            pending_indexers: Vec::new(),
            pending_wallets: Vec::new(),
            next_id: 0,
        }
    }

    /// Override the per-component readiness budget used during
    /// `build()`. Default: [`Self::DEFAULT_READY_TIMEOUT`] (20s). Use
    /// this when the test pulls a cold image, restores a large chain
    /// archive, or otherwise needs longer than the default to come up.
    ///
    /// Panics if called after any component is registered with the
    /// env — the timeout has to be locked in before `EnvInner` is
    /// shared, since handles hold a `Weak<EnvInner>`.
    pub fn ready_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            self.pending_validators.is_empty()
                && self.pending_indexers.is_empty()
                && self.pending_wallets.is_empty(),
            "ready_timeout must be called before add_validator/add_indexer/add_wallet",
        );
        self.inner = Arc::new(EnvInner::new(timeout));
        self
    }

    fn fresh_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Register a validator and return its handle. The handle works
    /// after `build().await`; using it before returns
    /// [`EnvError::NotBuilt`].
    pub fn add_validator(&mut self, v: Validator) -> ValidatorHandle {
        let id = self.fresh_id();
        let kind = v.kind();
        self.pending_validators.push((id, v));
        ValidatorHandle::new(Arc::downgrade(&self.inner), id, kind)
    }

    /// Register an indexer and return its handle.
    pub fn add_indexer(&mut self, i: Indexer) -> IndexerHandle {
        let id = self.fresh_id();
        let kind = i.kind();
        self.pending_indexers.push((id, i));
        IndexerHandle::new(Arc::downgrade(&self.inner), id, kind)
    }

    /// Register a wallet and return its handle.
    pub fn add_wallet(&mut self, w: Wallet) -> WalletHandle {
        let id = self.fresh_id();
        let kind = w.kind();
        self.pending_wallets.push((id, w));
        WalletHandle::new(Arc::downgrade(&self.inner), id, kind)
    }

    /// Enforce v1 topology constraints before touching any cluster
    /// state: no duplicate hostnames, at most one validator/wallet
    /// per env, at most two indexers (fetch + state backends).
    fn validate_topology(&self) -> Result<(), EnvError> {
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
            .map(|(_, v)| pod_name_of(v.opts()))
            .chain(self.pending_indexers.iter().map(|(_, i)| pod_name_of(i.opts())))
            .chain(self.pending_wallets.iter().map(|(_, w)| pod_name_of(w.opts())));
        for name in names {
            if !seen.insert(name.clone()) {
                return Err(EnvError::Config {
                    reason: format!("duplicate component name `{name}`"),
                });
            }
        }
        Ok(())
    }

    /// Walk pending components, resolve the topology activation-height
    /// ceiling, and render the regtest config of every validator that
    /// opted in via `.regtest()`. Runs once at the start of
    /// [`build`](Self::build), after [`validate_topology`].
    ///
    /// Indexer / wallet regtest configs don't encode activation heights
    /// — they read them from the validator at runtime — so they're not
    /// touched here. Components without `regtest_mode` are left alone
    /// (they're not running regtest).
    fn materialize_regtest_configs(&mut self) {
        use crate::component::{ComponentOpts, RegtestMode, Validator};
        use crate::handles::validator::ValidatorKind;
        use crate::topology::{
            activation_heights_for_ceiling, resolve_ceiling, ComponentVersion, NetworkUpgrade,
        };

        let opts_versions = |opts: &ComponentOpts, family| -> Option<ComponentVersion> {
            // From-source components don't have a parseable version — their
            // `version` field holds a Dockerfile path. Skip them in the
            // topology resolver; HEAD is assumed to support the highest NU
            // the rest of the topology asks for, and if it doesn't, the
            // chain syncer will fail loudly at runtime.
            if matches!(
                opts.image,
                crate::handles::backends::image::ImageSpec::Dev { .. }
            ) {
                return None;
            }
            // The constructor parses the version string; if it can't,
            // the test author gave us garbage and we want a loud panic
            // here rather than a confused materialization error later.
            Some(ComponentVersion {
                family,
                version: opts
                    .version
                    .parse()
                    .expect("component version must be a valid Semver"),
            })
        };

        let mut topology: Vec<ComponentVersion> = Vec::new();
        for (_, v) in &self.pending_validators {
            let family = match v.kind() {
                ValidatorKind::Zebrad => crate::topology::ComponentFamily::Zebrad,
                ValidatorKind::Zcashd => crate::topology::ComponentFamily::Zcashd,
            };
            topology.extend(opts_versions(v.opts(), family));
        }
        for (_, i) in &self.pending_indexers {
            let family = match i.kind() {
                crate::handles::indexer::IndexerKind::Zainod => {
                    crate::topology::ComponentFamily::Zaino
                }
                crate::handles::indexer::IndexerKind::Lightwalletd => {
                    crate::topology::ComponentFamily::Lightwalletd
                }
            };
            topology.extend(opts_versions(i.opts(), family));
        }
        for (_, w) in &self.pending_wallets {
            let family = match w.kind() {
                crate::handles::wallet::WalletKind::Zingo => {
                    crate::topology::ComponentFamily::Zingo
                }
            };
            topology.extend(opts_versions(w.opts(), family));
        }

        // Resolve the ceiling. If any validator opted in to a higher NU
        // explicitly via `ActivateThrough`, override (and panic on
        // incompatibility — caller asked for an NU the topology can't
        // serve).
        let resolved = resolve_ceiling(&topology);
        let mut ceiling = resolved;
        for (_, v) in &self.pending_validators {
            if let Some(RegtestMode::ActivateThrough(requested)) = &v.opts().regtest_mode {
                if *requested > resolved {
                    panic!(
                        "validator {:?} requested NU ceiling {:?}, but topology only \
                         supports up to {:?} (one or more pinned components is too old)",
                        v.opts().name,
                        requested,
                        resolved
                    );
                }
                ceiling = ceiling.max(*requested);
            }
        }

        let activation = activation_heights_for_ceiling(ceiling);
        tracing::info!(
            ceiling = ?ceiling,
            "topology activation-height ceiling resolved"
        );

        // Resolve each opted-in validator's peer-name list into
        // `(host, port)` tuples once, up front — pod names are stable
        // by this point (validate_topology already ran). Unknown peer
        // names panic loudly: a typo in a `.peer("aliec")` is a
        // configuration bug, not a runtime concern.
        let p2p_port = crate::handles::ports::ZEBRAD_P2P;
        let known_validators: std::collections::HashSet<String> = self
            .pending_validators
            .iter()
            .map(|(_, v)| pod_name_of(v.opts()))
            .collect();
        let peer_tuples_for = |opts: &ComponentOpts| -> Vec<(String, u16)> {
            opts.peers
                .iter()
                .map(|name| {
                    let host = short_kind(name);
                    if !known_validators.contains(&host) {
                        panic!(
                            "validator peer {name:?} not found in this env's \
                             topology (known: {known_validators:?})"
                        );
                    }
                    (host, p2p_port)
                })
                .collect()
        };

        // Hand each opted-in validator over to its backend's
        // materializer. Indexers / wallets aren't height-dependent.
        let pending = std::mem::take(&mut self.pending_validators);
        self.pending_validators = pending
            .into_iter()
            .map(|(id, v)| {
                if v.opts().regtest_mode.is_some() {
                    let peers = peer_tuples_for(v.opts());
                    let v = match v {
                        Validator::Zebrad(_) => {
                            crate::handles::backends::zebra::materialize_regtest_config(
                                v, &activation, &peers,
                            )
                        }
                        Validator::Zcashd(_) => {
                            crate::handles::backends::zcashd::materialize_regtest_config(
                                v, &activation,
                            )
                        }
                    };
                    (id, v)
                } else {
                    (id, v)
                }
            })
            .collect();

        // Resolve the validator pod name for indexers that opted in to
        // regtest. v1 topology: one validator paired with one or more
        // indexers — pick the only validator's pod name. Indexers that
        // didn't call `.regtest()` / `.regtest_state()` (no
        // `regtest_backend` set) are left alone.
        let validator_host = self
            .pending_validators
            .iter()
            .map(|(_, v)| pod_name_of(v.opts()))
            .next();
        let pending = std::mem::take(&mut self.pending_indexers);
        self.pending_indexers = pending
            .into_iter()
            .map(|(id, i)| {
                let needs_regtest = match &i {
                    crate::component::Indexer::Zainod(o) => o.regtest_backend.is_some(),
                };
                if needs_regtest {
                    let host = validator_host.as_deref().expect(
                        "indexer opted in to regtest but no validator is registered in this env",
                    );
                    let i = crate::handles::backends::zainod::materialize_regtest_config(i, host);
                    (id, i)
                } else {
                    (id, i)
                }
            })
            .collect();

        // Suppress unused-variant warning on NetworkUpgrade until we
        // surface the ActivateThrough opt-in publicly.
        let _ = NetworkUpgrade::HIGHEST;
    }

    /// Apply manifests, wait for readiness. After this returns
    /// successfully, every handle method works.
    pub async fn build(&mut self) -> Result<(), EnvError> {
        self.validate_topology()?;
        self.materialize_regtest_configs();

        let started = std::time::Instant::now();
        let coords = RunCoords::from_env().map_err(env_err)?;
        let test_id = naming::test_suffix();
        let namespace = naming::namespace_for(&test_id);
        let client = cluster::client().await.map_err(env_err)?;

        tracing::info!(
            namespace = %namespace,
            validators = self.pending_validators.len(),
            indexers = self.pending_indexers.len(),
            wallets = self.pending_wallets.len(),
            "building TestEnv"
        );

        cluster::ensure_namespace(&client, &namespace, &coords)
            .await
            .map_err(env_err)?;
        let sentinel = Sentinel::new(namespace.clone());
        let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
        let test_name = current_test_name();

        // Late-bind the kube client + namespace into EnvInner. After
        // this point, EnvInner::resolve_port can dial the cluster.
        let _ = self.inner.client.set(client.clone());
        *self.inner.namespace.lock().expect("namespace mutex poisoned") = Some(namespace.clone());

        let ctx = MaterializeCtx {
            client: &client,
            pods: &pods,
            sentinel: &sentinel,
            coords: &coords,
            test_name: &test_name,
        };

        // Phase 1 — validators. Bring them up, wait for JSON-RPC, then
        // mine one block so indexers (notably zaino's ChainIndex sync
        // loop) see a real tip from first connect.
        let validators: Vec<_> = self
            .pending_validators
            .drain(..)
            .map(|(id, v)| {
                let spec = make_validator_spec(&v);
                let opts = v.opts().clone();
                (id, spec, opts)
            })
            .collect();
        self.materialize_phase(&ctx, &validators).await?;
        self.wait_validators_rpc_ready().await?;
        self.warm_validators().await?;

        // Phase 2 — indexers and wallets. They observe a non-genesis
        // chain from the moment they connect.
        let mut dependents: Vec<_> = self
            .pending_indexers
            .drain(..)
            .map(|(id, i)| {
                let spec = make_indexer_spec(&i)?;
                let opts = i.opts().clone();
                Ok::<_, EnvError>((id, spec, opts))
            })
            .collect::<Result<Vec<_>, _>>()?;
        dependents.extend(self.pending_wallets.drain(..).map(|(id, w)| {
            let spec = make_wallet_spec(&w);
            let opts = w.opts().clone();
            (id, spec, opts)
        }));
        self.materialize_phase(&ctx, &dependents).await?;

        // Flip the live flag last — readers see "either fully built or
        // not built" with no in-between state.
        self.inner.is_built.store(true, Ordering::Release);

        tracing::info!(
            namespace = %namespace,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "TestEnv ready"
        );
        Ok(())
    }

    /// Explicit teardown — preferred over relying on `Drop`. Deletes
    /// the per-test namespace (k8s cascades every Pod / Service / PVC
    /// / CM in it), then deletes the cluster-scoped shadow VSCs
    /// (which GC can't reach across namespaces).
    pub async fn teardown(self) -> Result<(), EnvError> {
        let ns = self
            .inner
            .namespace
            .lock()
            .expect("namespace mutex poisoned")
            .take();
        let shadows: Vec<_> = std::mem::take(
            &mut *self
                .inner
                .shadow_clones
                .lock()
                .expect("shadow_clones mutex poisoned"),
        );
        tracing::info!(
            namespace = ?ns,
            shadow_clones = shadows.len(),
            "tearing down TestEnv"
        );
        let client = self.inner.client.get().cloned();
        if let (Some(client_ref), Some(ns)) = (client.as_ref(), ns) {
            cluster::delete_namespace(client_ref, &ns).await.map_err(env_err)?;
        }
        if let Some(client) = client {
            for shadow in shadows {
                if let Err(e) = seeds::delete_shadow(&client, &shadow).await {
                    tracing::warn!(error = %e, vsc = %shadow.shadow_vsc_name, "shadow VSC delete failed");
                }
            }
        }
        Ok(())
    }

    // ── readiness ────────────────────────────────────────────────────

    /// Probe every validator's JSON-RPC port until `getblocktemplate`
    /// returns success. Runs all validators concurrently. The
    /// per-validator budget is the env's `ready_timeout` (default
    /// [`Self::DEFAULT_READY_TIMEOUT`]).
    async fn wait_validators_rpc_ready(&self) -> Result<(), EnvError> {
        let validators: Vec<ComponentState> = {
            let comps = self.inner.components.read().await;
            comps
                .values()
                .filter(|s| matches!(s.kind, ComponentKind::Validator(_)))
                .cloned()
                .collect()
        };
        if validators.is_empty() {
            return Ok(());
        }

        let timeout = self.inner.ready_timeout;
        let probes = validators.into_iter().map(|state| {
            let inner = Arc::clone(&self.inner);
            async move {
                let endpoint = inner.resolve_named(&state, "rpc").await?;
                // Route through the ValidatorBackend trait so the
                // auth scheme stays in one place — see
                // `handles::backends::validator_for_kind`.
                // Route through the ValidatorBackend trait so each
                // backend gets the readiness probe its RPC contract
                // actually expects: zebrad polls `getblocktemplate`;
                // zcashd polls `getinfo` (its `getblocktemplate` is
                // gated by IBD and never clears on regtest). Anything
                // non-validator (an indexer that slipped through the
                // filter above) keeps the legacy probe path.
                let vk = match state.kind {
                    ComponentKind::Validator(vk) => vk,
                    _ => return Ok(()),
                };
                let backend = crate::handles::backends::validator_for_kind(vk);
                let client = backend.build_authed_rpc(&endpoint);
                backend
                    .wait_for_ready(&client, endpoint.socket_addr(), timeout)
                    .await
                    .map_err(|e| EnvError::RpcTimeout {
                        component: state.pod_name.clone(),
                        op: "wait_for_ready",
                        elapsed: e.timeout,
                    })
            }
        });
        for res in join_all(probes).await {
            res?;
        }
        Ok(())
    }

    /// Mine one block on every validator. Called once during `build()`,
    /// after RPC is ready and before any indexer/wallet sees the chain.
    /// Zaino's `ChainIndex` sync loop trips its reorg detector on a
    /// genesis-only chain and would exit permanently otherwise.
    async fn warm_validators(&self) -> Result<(), EnvError> {
        let validators: Vec<(u64, crate::handles::validator::ValidatorKind)> = {
            let comps = self.inner.components.read().await;
            comps
                .iter()
                .filter_map(|(id, s)| match s.kind {
                    ComponentKind::Validator(k) => Some((*id, k)),
                    _ => None,
                })
                .collect()
        };
        // Temporarily flip is_built so the fabricated handles can drive
        // RPCs. `build()` is the unique flip-to-live point, so we reset
        // it on the way out.
        self.inner.is_built.store(true, Ordering::Release);
        let result = async {
            for (id, kind) in validators {
                let handle = ValidatorHandle::new(Arc::downgrade(&self.inner), id, kind);
                handle
                    .generate_blocks(1)
                    .await
                    .map_err(|e| EnvError::Transient(Box::new(e)))?;
            }
            Ok::<(), EnvError>(())
        }
        .await;
        self.inner.is_built.store(false, Ordering::Release);
        result
    }

    // ── materialize ─────────────────────────────────────────────────

    /// Apply one phase of components: create per-pod ClusterIP Services,
    /// resolve mounts, apply Pods, then wait for every pod's
    /// readinessProbe to pass.
    async fn materialize_phase(
        &self,
        ctx: &MaterializeCtx<'_>,
        items: &[(u64, PodSpec, crate::component::ComponentOpts)],
    ) -> Result<(), EnvError> {
        for (id, spec, opts) in items {
            let state = ComponentState::new(spec, ctx.sentinel.namespace.clone());
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

        let timeout = self.inner.ready_timeout;
        let waits = items.iter().map(|(_, spec, _)| {
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
    fn drop(&mut self) {
        // Best-effort async cleanup. If no runtime is current,
        // kube-janitor backstops.
        let ns = self
            .inner
            .namespace
            .lock()
            .ok()
            .and_then(|mut g| g.take());
        let shadows: Vec<_> = self
            .inner
            .shadow_clones
            .lock()
            .ok()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default();
        if ns.is_none() && shadows.is_empty() {
            return;
        }
        let Some(client) = self.inner.client.get().cloned() else {
            return;
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tracing::debug!(
                namespace = ?ns,
                shadow_clones = shadows.len(),
                "TestEnv dropped without explicit teardown; spawning best-effort cleanup"
            );
            handle.spawn(async move {
                if let Some(ns) = ns {
                    // Best-effort; kube-janitor backstops if this fails.
                    let _ = cluster::delete_namespace(&client, &ns).await;
                }
                for shadow in shadows {
                    // Best-effort; kube-janitor backstops if this fails.
                    let _ = seeds::delete_shadow(&client, &shadow).await;
                }
            });
        }
    }
}

// ─────────────────────────────── helpers ──────────────────────────────

/// `Condition<Pod>` matching pods whose `Ready` status condition is
/// `True`. `kube-runtime` ships `is_pod_running` but not this — the
/// "running" predicate fires as soon as containers start, which is
/// well before the readinessProbe binds the application port.
fn is_pod_ready() -> impl kube::runtime::wait::Condition<Pod> {
    |pod: Option<&Pod>| {
        pod.and_then(|p| p.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .map(|cs| {
                cs.iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True")
            })
            .unwrap_or(false)
    }
}

fn short_kind(s: &str) -> String {
    let s: String = s
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
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

/// DNS-1123 pod / service name for a component. Defaults to the
/// backend's kind label; `.named()` overrides.
fn pod_name_of(opts: &crate::component::ComponentOpts) -> String {
    short_kind(opts.name.as_deref().unwrap_or("x"))
}

fn make_validator_spec(v: &Validator) -> PodSpec {
    manifest::pod_spec_for_validator(v, pod_name_of(v.opts()))
}

fn make_indexer_spec(i: &Indexer) -> Result<PodSpec, EnvError> {
    manifest::pod_spec_for_indexer(i, pod_name_of(i.opts()))
}

fn make_wallet_spec(w: &Wallet) -> PodSpec {
    manifest::pod_spec_for_wallet(w, pod_name_of(w.opts()))
}

/// Shared inputs threaded into the materialize loop.
struct MaterializeCtx<'a> {
    client: &'a kube::Client,
    pods: &'a Api<Pod>,
    sentinel: &'a Sentinel,
    coords: &'a RunCoords,
    test_name: &'a str,
}

fn current_test_name() -> String {
    std::env::var("NEXTEST_TEST_NAME").unwrap_or_else(|_| "unknown".into())
}
