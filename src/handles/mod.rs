//! Component handles and per-category RPC dispatch.
//!
//! - One concrete struct per backend (e.g. `ZebraValidator`) implementing its
//!   category's `*Backend` contract, holding a [`HandleInner`]
//! - Backend-specific RPCs stay inherent (wrong backend = compile error)

pub mod indexer;
pub mod validator;
pub mod wallet;

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::Mutex;

use crate::EnvError;
use crate::env::EnvInner;
use crate::handles::wallet::Pool;
use crate::portforward::Forwarder;
use crate::protocol::Endpoint;

pub use self::indexer::{IndexerBackend, IndexerConfig};
pub use self::validator::{ValidatorBackend, ValidatorConfig};
pub use self::wallet::{WalletBackend, WalletConfig};

pub type ForwardRegistry = Arc<Mutex<HashMap<(String, u16), Arc<Forwarder>>>>;

// ───────────────────────────── handles ─────────────────────────────────

/// Opaque plumbing a handle uses to reach its live component; backends move it in
/// and call [`endpoint`](Self::endpoint).
///
/// - `regtest` rides here because RPC cannot recover it (zebra reports regtest as
///   `chain: "test"`)
/// - `coinbase_pool` = `None` for indexers and wallets
#[derive(Debug, Clone)]
pub struct HandleInner {
    pub inner: Weak<EnvInner>,
    pub component_id: u64,
    pub regtest: bool,
    pub coinbase_pool: Option<Pool>,
}

impl HandleInner {
    pub fn ensure_built(&self) -> Result<Arc<EnvInner>, EnvError> {
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
