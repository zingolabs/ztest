//! Component handles and per-category RPC dispatch.
//!
//! Every handle is a concrete per-backend struct (e.g. `ZebraValidator`,
//! `ZainoIndexer`, `ZingoWallet`) implementing its category's `*Backend`
//! contract; backend-specific RPCs are inherent methods, so calling one on the
//! wrong backend is a compile error (no downcasts, no panics). Each handle
//! holds a [`HandleInner`] (a `Weak<EnvInner>` plus component id) to reach its
//! live component.

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

/// Opaque plumbing handed to a handle so it can reach its live component: a
/// back-reference to the env plus the component id used to resolve endpoints.
/// The env constructs one per component in `add_*`; backends receive it via
/// [`ValidatorSpec::to_handle`](crate::handles::validator::ValidatorSpec::to_handle)
/// and store it. Fields are crate-private: third-party backends move it into
/// their handle and call [`endpoint`](Self::endpoint), they don't construct or
/// inspect it.
#[derive(Debug, Clone)]
pub struct HandleInner {
    pub(crate) inner: Weak<EnvInner>,
    pub(crate) component_id: u64,
    /// Whether the env configured this component for regtest. Carried as
    /// plumbing because the network identity is otherwise unrecoverable at
    /// runtime: zebra models regtest as a Testnet-kind network, so its
    /// `getblockchaininfo.chain` reports `"test"`, indistinguishable from a
    /// real testnet by RPC alone. See `ValidatorBackend::chain_config`.
    pub(crate) regtest: bool,
    /// The value pool this validator mines its coinbase into, resolved at
    /// `add_validator` time from the builder's choice (or the backend
    /// default). `Some` for validators; `None` for indexers/wallets,
    /// which have no coinbase. Validator handles read it via
    /// [`validator::PoolSupport`].
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

    /// Resolve a named endpoint (e.g. `"rpc"`) of this component.
    pub async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError> {
        let inner = self.ensure_built()?;
        let state = inner.component_state(self.component_id).await?;
        inner.resolve_named(&state, name).await
    }

    /// Resolve an endpoint of this component by its container port.
    pub async fn endpoint_for(&self, port: u16) -> Result<Endpoint, EnvError> {
        let inner = self.ensure_built()?;
        let state = inner.component_state(self.component_id).await?;
        inner.resolve_port(&state, port).await
    }
}

// ───────────────────────────── named-port table ────────────────────────

/// The single source of truth for every container port ztest assigns.
/// Backends must reference these rather than redeclaring the literals: the
/// same number in two places is a drift bug waiting to happen (a renamed port
/// updated on only one side).
pub mod ports {
    pub const ZEBRAD_RPC: u16 = 28232;
    /// zebrad's JSON-RPC port on a testnet topology (distinct from the regtest
    /// [`ZEBRAD_RPC`]). Shared by the zebrad backend and any zaino indexer
    /// paired with it.
    pub const ZEBRAD_TESTNET_RPC: u16 = 18232;
    pub const ZEBRAD_METRICS: u16 = 9999;
    pub const ZEBRAD_P2P: u16 = 18233;
    /// zebrad's indexer gRPC (`rpc.indexer_listen_addr`). Only served when
    /// a validator is configured for shared-state via
    /// `Validator::persistent_state_in`; consumed by a colocated zaino
    /// StateService for non-finalized-state sync.
    pub const ZEBRAD_INDEXER: u16 = 18230;
    pub const ZCASHD_RPC: u16 = 28232;
    pub const ZAINO_GRPC: u16 = 8137;
    pub const ZAINO_JSONRPC: u16 = 8232;
    pub const ZAINO_METRICS: u16 = 9998;
    pub const LIGHTWALLETD_GRPC: u16 = 9067;
}
