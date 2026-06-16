//! Component handles + per-category RPC dispatch.
//!
//! A `*Handle` is an **owned** view of one component inside a
//! `TestEnv`. It holds a `Weak<EnvInner>` plus the component id and
//! its kind. Method dispatch upgrades the `Weak` to an `Arc`, checks
//! the env's `is_built` flag, then resolves the endpoint and dials.
//!
//! Handles created before `env.build().await` are inert: calling any
//! handle method returns `EnvError::NotBuilt` (wrapped in `RpcError`
//! for RPC sugar methods). Handles outlive the env happily — the
//! `Weak` upgrade fails after `env.teardown()` / drop, surfacing as
//! `EnvError::EnvDropped`.
//!
//! Module layout:
//!  - [`validator`] / [`indexer`] / [`wallet`] — per-category typed
//!    methods, kind enums, and category-specific neutral types.
//!  - [`backends`] — per-binary free functions called by the handle
//!    dispatch methods.
//!  - [`client`] / [`jsonrpc`] — shared transport plumbing.

pub mod backends;
pub mod client;
pub mod indexer;
pub(crate) mod jsonrpc;
pub mod ready;
pub mod validator;
pub mod wallet;

/// Backend-specific extension traits — bring into scope to access RPCs
/// that only one binary serves (e.g. Zaino's nullifier endpoints).
pub use backends::zainod::ZainoIndexer;
pub use backends::zcashd::ZcashdValidator;
pub use backends::zebra::ZebraValidator;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Weak};

use tokio::sync::Mutex;

use crate::env::EnvInner;
use crate::portforward::Forwarder;
use crate::EnvError;

use self::indexer::IndexerKind;
use self::validator::ValidatorKind;
use self::wallet::WalletKind;

/// `(pod_name, container_port) → Forwarder`. One entry per active
/// local→pod tunnel; new tunnels are lazily added by
/// `EnvInner::resolve_port`.
pub(crate) type ForwardRegistry = Arc<Mutex<HashMap<(String, u16), Arc<Forwarder>>>>;

// ───────────────────────────── endpoint ────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct Endpoint {
    /// `127.0.0.1` when port-forwarded; pod IP when in-cluster.
    pub host: IpAddr,
    /// Local port (forwarded) or container port (direct).
    pub port: u16,
}

impl Endpoint {
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
    pub fn url(&self, scheme: &str) -> String {
        format!("{scheme}://{}", self.socket_addr())
    }
}

// ───────────────────────────── handles ─────────────────────────────────

/// Common plumbing behind every handle. Each method on a handle upgrades
/// the `Weak`, asserts `is_built`, then does its work.
#[derive(Debug, Clone)]
pub(crate) struct HandleInner {
    pub(crate) inner: Weak<EnvInner>,
    pub(crate) component_id: u64,
}

impl HandleInner {
    /// Upgrade to a live `Arc<EnvInner>` and require `is_built=true`.
    /// Errors with `NotBuilt` before `env.build()` and `EnvDropped`
    /// after `env.teardown()` / drop.
    pub(crate) fn ensure_built(&self) -> Result<Arc<EnvInner>, EnvError> {
        let inner = self.inner.upgrade().ok_or(EnvError::EnvDropped)?;
        if !inner
            .is_built
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(EnvError::NotBuilt);
        }
        Ok(inner)
    }

    pub(crate) async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError> {
        let inner = self.ensure_built()?;
        let state = inner.component_state(self.component_id).await?;
        inner.resolve_named(&state, name).await
    }

    pub(crate) async fn endpoint_for(&self, port: u16) -> Result<Endpoint, EnvError> {
        let inner = self.ensure_built()?;
        let state = inner.component_state(self.component_id).await?;
        inner.resolve_port(&state, port).await
    }
}

/// An owned view of a validator inside a `TestEnv`. Returned by
/// `env.add_validator(...)`. Methods short-circuit with
/// `EnvError::NotBuilt` before `env.build().await`.
///
/// All backend-varying behaviour goes through `backend`. There is no
/// `kind` field — `kind()` / `label()` / auth selection / block
/// generation all delegate to the trait object.
#[derive(Debug, Clone)]
pub struct ValidatorHandle {
    pub(crate) plumbing: HandleInner,
    pub(crate) backend: Arc<dyn backends::ValidatorBackend>,
}

#[derive(Debug, Clone)]
pub struct IndexerHandle {
    pub(crate) plumbing: HandleInner,
    pub(crate) backend: Arc<dyn backends::IndexerBackend>,
}

#[derive(Debug, Clone)]
pub struct WalletHandle {
    pub(crate) plumbing: HandleInner,
    pub(crate) backend: Arc<dyn backends::WalletBackend>,
}

impl ValidatorHandle {
    pub(crate) fn new(inner: Weak<EnvInner>, component_id: u64, kind: ValidatorKind) -> Self {
        Self {
            plumbing: HandleInner { inner, component_id },
            backend: backends::validator_for_kind(kind),
        }
    }
    pub async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint(name).await
    }
    pub async fn endpoint_for(&self, container_port: u16) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint_for(container_port).await
    }
}

impl IndexerHandle {
    pub(crate) fn new(inner: Weak<EnvInner>, component_id: u64, kind: IndexerKind) -> Self {
        Self {
            plumbing: HandleInner { inner, component_id },
            backend: backends::indexer_for_kind(kind),
        }
    }
    pub async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint(name).await
    }
    pub async fn endpoint_for(&self, container_port: u16) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint_for(container_port).await
    }
    /// The indexer's gRPC URI as a string (`http://host:port`).
    /// Callers parse to a typed `Uri` if needed.
    pub async fn grpc_uri(&self) -> Result<String, EnvError> {
        Ok(self.endpoint("grpc").await?.url("http"))
    }
}

impl WalletHandle {
    pub(crate) fn new(inner: Weak<EnvInner>, component_id: u64, kind: WalletKind) -> Self {
        Self {
            plumbing: HandleInner { inner, component_id },
            backend: backends::wallet_for_kind(kind),
        }
    }
    pub async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint(name).await
    }
    pub async fn endpoint_for(&self, container_port: u16) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint_for(container_port).await
    }
}

// ───────────────────────────── named-port table ────────────────────────

/// Static named-port table per the spec's component variants. The
/// orchestrator uses these when applying Pods and when resolving
/// `*Handle::endpoint(name)`.
pub mod ports {
    pub const ZEBRAD_RPC: u16 = 28232;
    pub const ZEBRAD_METRICS: u16 = 9999;
    /// zebrad peer-to-peer listen port. Used by `initial_testnet_peers`
    /// when running multi-validator regtest topologies — the rendered
    /// `[network] listen_addr` binds here so the same-named ClusterIP
    /// Service routes peer dials to the pod. Canonical testnet port.
    pub const ZEBRAD_P2P: u16 = 18233;
    pub const ZCASHD_RPC: u16 = 28232;
    pub const ZAINO_GRPC: u16 = 8137;
    pub const ZAINO_JSONRPC: u16 = 8232;
    pub const ZAINO_METRICS: u16 = 9998;
    pub const ZINGO_GRPC: u16 = 20000;
}
