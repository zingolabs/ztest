//! `Ready` — uniform "is this handle responding to RPC yet?" check.
//!
//! Every component category implements `ready(timeout)` by polling a
//! cheap, side-effect-free RPC until it succeeds or the timeout
//! elapses. Tests write `handle.ready(Duration::from_secs(20)).await?`
//! to gate any subsequent work on the pod actually being live.
//!
//! Per-category readiness probes:
//!  - `ValidatorHandle` → backend-chosen JSON-RPC probe via
//!    [`crate::handles::backends::ValidatorBackend::wait_for_ready`]
//!    (zebrad: `getblocktemplate`; zcashd: `getinfo`).
//!  - `IndexerHandle`   → `GetLightdInfo` gRPC.
//!  - `WalletHandle`    → backend-specific liveness call.

use std::time::Duration;

use crate::handles::{IndexerHandle, ValidatorHandle, WalletHandle};
use crate::RpcError;

/// Polling cadence used by every `ready()` impl while waiting on the
/// readiness probe to succeed.
pub const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);

impl ValidatorHandle {
    /// Wait until the validator's JSON-RPC reports ready (probe is
    /// backend-specific; see
    /// [`crate::handles::backends::ValidatorBackend::wait_for_ready`]),
    /// or `timeout` elapses. Uses the same auth scheme as the rest of
    /// `ValidatorHandle` — unauthed for zebrad, Basic Auth for zcashd
    /// (which rejects every unauthed call with HTTP 401 and would
    /// otherwise burn the entire `timeout` budget here).
    pub async fn ready(&self, timeout: Duration) -> Result<(), RpcError> {
        let ep = self.endpoint("rpc").await?;
        let client = self.backend.build_authed_rpc(&ep);
        self.backend
            .wait_for_ready(&client, ep.socket_addr(), timeout)
            .await
            .map_err(|e| {
                RpcError::timeout(
                    self.backend.label(),
                    "ready",
                    timeout,
                    format!("validator JSON-RPC not ready: {}", e.last_error),
                )
            })
    }
}

impl IndexerHandle {
    /// Wait until the indexer's gRPC `GetLightdInfo` succeeds, or
    /// `timeout` elapses.
    pub async fn ready(&self, timeout: Duration) -> Result<(), RpcError> {
        let started = tokio::time::Instant::now();
        let deadline = started + timeout;
        loop {
            if self.indexer_info().await.is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RpcError::timeout(
                    self.backend.label(),
                    "ready",
                    timeout,
                    "indexer gRPC GetLightdInfo never succeeded".to_string(),
                ));
            }
            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }
}

impl WalletHandle {
    /// Wait until the wallet's gRPC responds to a sync-height query, or
    /// `timeout` elapses.
    pub async fn ready(&self, timeout: Duration) -> Result<(), RpcError> {
        let started = tokio::time::Instant::now();
        let deadline = started + timeout;
        loop {
            if self.synced_height().await.is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RpcError::timeout(
                    self.backend.label(),
                    "ready",
                    timeout,
                    "wallet gRPC never succeeded".to_string(),
                ));
            }
            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }
}
