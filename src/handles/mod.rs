//! Component handles and per-category RPC dispatch.
//!
//! - One concrete struct per backend (e.g. `ZebraValidator`) implementing its
//!   category's `*Backend` contract, holding a [`HandleInner`]
//! - Backend-specific RPCs stay inherent (wrong backend = compile error)

pub mod client;
pub mod indexer;
pub mod types;
pub mod validator;
pub mod wallet;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Weak};

use tokio::sync::Mutex;

use crate::EnvError;
use crate::env::EnvInner;
use crate::handles::wallet::Pool;
use crate::portforward::Forwarder;

pub use self::indexer::{IndexerBackend, IndexerConfig};
pub use self::validator::{ValidatorBackend, ValidatorConfig};
pub use self::wallet::{WalletBackend, WalletConfig};

pub(crate) type ForwardRegistry = Arc<Mutex<HashMap<(String, u16), Arc<Forwarder>>>>;

// ───────────────────────────── endpoint ────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct Endpoint {
    pub host: IpAddr,
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

/// Opaque plumbing a handle uses to reach its live component; backends move it in
/// and call [`endpoint`](Self::endpoint).
///
/// - `regtest` rides here because RPC cannot recover it (zebra reports regtest as
///   `chain: "test"`)
/// - `coinbase_pool` = `None` for indexers and wallets
#[derive(Debug, Clone)]
pub struct HandleInner {
    pub(crate) inner: Weak<EnvInner>,
    pub(crate) component_id: u64,
    pub(crate) regtest: bool,
    pub(crate) coinbase_pool: Option<Pool>,
}

impl HandleInner {
    pub(crate) fn ensure_built(&self) -> Result<Arc<EnvInner>, EnvError> {
        let inner = self.inner.upgrade().ok_or(EnvError::EnvDropped)?;
        if !inner.is_built.load(std::sync::atomic::Ordering::Acquire) {
            return Err(EnvError::NotBuilt);
        }
        Ok(inner)
    }

    /// Resolve a named endpoint (e.g. `"rpc"`) of this component
    pub async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError> {
        let inner = self.ensure_built()?;
        let state = inner.component_state(self.component_id).await?;
        inner.resolve_named(&state, name).await
    }

    pub async fn endpoint_for(&self, port: u16) -> Result<Endpoint, EnvError> {
        let inner = self.ensure_built()?;
        let state = inner.component_state(self.component_id).await?;
        inner.resolve_port(&state, port).await
    }
}

// ───────────────────────────── named-port table ────────────────────────

/// Sole source of every container port ztest assigns; backends reference these
/// rather than redeclaring drift-prone literals
pub mod ports {
    pub const ZEBRAD_RPC: u16 = 28232;
    /// zebrad JSON-RPC on any public network. Distinguishes which config
    /// generator rendered the node (public vs regtest [`ZEBRAD_RPC`]), not which
    /// chain — pods are namespace-isolated, and 8232 collides with [`ZAINO_JSONRPC`]
    pub const ZEBRAD_PUBLIC_RPC: u16 = 18232;
    pub const ZEBRAD_METRICS: u16 = 9999;
    pub const ZEBRAD_P2P: u16 = 18233;
    /// zebrad indexer gRPC (`rpc.indexer_listen_addr`). Served only on a shared
    /// state DB (`Shared`-volume `.mount(&vol)`); consumed by a colocated zaino
    /// StateService for non-finalized-state sync
    pub const ZEBRAD_INDEXER: u16 = 18230;
    pub const ZCASHD_RPC: u16 = 28232;
    pub const ZAINO_GRPC: u16 = 8137;
    pub const ZAINO_JSONRPC: u16 = 8232;
    pub const ZAINO_METRICS: u16 = 9998;
    pub const LIGHTWALLETD_GRPC: u16 = 9067;

    /// Mandatory listener bind address under pod-per-test: the client reaches a
    /// component at its pod IP, so an upstream loopback bind refuses every
    /// cross-pod call. Namespace isolation replaces loopback's protection
    pub const LISTEN_ALL: &str = "0.0.0.0";
}
