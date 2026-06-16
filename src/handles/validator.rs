//! Validator handle: typed RPC sugar over `ValidatorHandle`.
//!
//! Every method delegates either to a shared JSON-RPC helper (with
//! the backend's own label string for attribution) or directly to the
//! [`backends::ValidatorBackend`] trait object set at construction.
//! No `match` on backend kind in this file.

use std::time::Duration;

use zingo_common_components::protocol::ActivationHeights;

use crate::handles::client::{AuthedRpc, JsonRpcClient};
use crate::handles::jsonrpc;
use crate::handles::ValidatorHandle;
use crate::mount::SnapshotRef;
use crate::{EnvError, RpcError};

/// Which validator backend a `ValidatorHandle` wraps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidatorKind {
    Zebrad,
    Zcashd,
}


/// Block-tip summary returned by `tip`.
#[derive(Debug, Clone, Copy)]
pub struct BlockTip {
    pub height: u32,
    pub hash: [u8; 32],
}

/// Full block returned by `block_at`.
#[derive(Debug, Clone)]
pub struct Block {
    pub height: u32,
    pub hash: [u8; 32],
}

/// Mempool statistics returned by [`ValidatorHandle::mempool_info`].
/// `usage` is reported by zcashd but not always by zebrad, so it's
/// optional.
#[derive(Debug, Clone, Copy)]
pub struct MempoolInfo {
    pub size: u64,
    pub bytes: u64,
    pub usage: Option<u64>,
}

/// Chain identity, tip, and difficulty summary — returned by
/// `blockchain_info()` on both [`ValidatorHandle`] and
/// [`crate::handles::IndexerHandle`]. Carries only fields shared by
/// every supported backend so parity comparisons are exact: backend-
/// specific extras (mining-info subfields, fee histograms, etc.) stay
/// reachable via the bare `json_rpc()` client.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockchainInfo {
    /// Chain identifier, e.g. `"regtest"`, `"testnet"`, `"main"`.
    pub chain: String,
    /// Current tip height.
    pub blocks: u32,
    /// Highest header seen (≥ `blocks` while a sync is in flight).
    pub headers: u32,
    /// Lowercase hex of the tip block hash (display byte order).
    pub best_block_hash: String,
    /// Difficulty target as reported by the RPC.
    pub difficulty: f64,
    /// Estimated final chain height. `None` on backends/networks that
    /// don't report this (e.g. fresh regtest pre-IBD).
    pub estimated_height: Option<u32>,
}

/// Peer-table snapshot returned by `peer_info()` on both
/// [`ValidatorHandle`] and [`crate::handles::IndexerHandle`]. Carries
/// the common subset across zebrad / zcashd / zaino — backends may
/// expose more fields, reachable via the bare `json_rpc()` client.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerInfo {
    pub peers: Vec<Peer>,
}

/// One row from [`PeerInfo`]. Field set is the intersection across
/// backends; extend conservatively.
#[derive(Debug, Clone, PartialEq)]
pub struct Peer {
    /// `host:port` of the remote.
    pub addr: String,
    /// `true` if the remote initiated the connection.
    pub inbound: bool,
    /// Peer-advertised protocol version.
    pub version: u32,
    /// Peer-advertised subversion / user agent string.
    pub subver: String,
}

/// Polling cadence for `poll_chain_height`. Matches
/// `zcash_local_net::validator::Validator::CHAIN_POLL_INTERVAL`.
pub const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Total budget for `poll_chain_height`. Matches
/// `zcash_local_net::validator::Validator::CHAIN_POLL_TIMEOUT`.
pub const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);
/// Per-block delay in `generate_blocks_with_delay`. Matches
/// `zcash_local_net::validator::Validator::BLOCK_GENERATION_DELAY`.
pub const BLOCK_GENERATION_DELAY: Duration = Duration::from_millis(1500);

impl ValidatorHandle {
    /// Which backend this handle wraps. Delegates to the trait object;
    /// kept as an inherent method so call sites read `handle.kind()`
    /// instead of `handle.backend.kind()`.
    pub fn kind(&self) -> ValidatorKind {
        self.backend.kind()
    }

    /// Build the internal `AuthedRpc` for this validator's `rpc`
    /// endpoint. Auth is the backend's responsibility — see
    /// [`backends::ValidatorBackend::build_authed_rpc`].
    async fn rpc_client(&self) -> Result<AuthedRpc, EnvError> {
        let ep = self.endpoint("rpc").await?;
        Ok(self.backend.build_authed_rpc(&ep))
    }

    /// Generate `n` blocks. Returns the new chain-tip height once the
    /// chain has advanced. Saves callers an extra `chain_height()`
    /// round-trip when they want to pair this with
    /// `IndexerHandle::wait_for_block_num(tip, ...)`.
    pub async fn generate_blocks(&self, n: u32) -> Result<u32, RpcError> {
        let client = self.rpc_client().await?;
        self.backend.generate_blocks(&client, n).await?;
        self.chain_height().await
    }

    /// `generate_blocks` with [`BLOCK_GENERATION_DELAY`] between mines.
    /// Returns the new chain-tip height.
    pub async fn generate_blocks_with_delay(&self, n: u32) -> Result<u32, RpcError> {
        let mut tip = self.chain_height().await?;
        for _ in 0..n {
            tip = self.generate_blocks(1).await?;
            tokio::time::sleep(BLOCK_GENERATION_DELAY).await;
        }
        Ok(tip)
    }

    /// Current chain-tip height.
    pub async fn chain_height(&self) -> Result<u32, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::chain_height(self.backend.label(), &client).await
    }

    /// Poll `chain_height` until it reaches `target` or
    /// [`CHAIN_POLL_TIMEOUT`] elapses. Thin wrapper over
    /// [`wait_for_block_num`] with the default timeout.
    pub async fn poll_chain_height(&self, target: u32) -> Result<(), RpcError> {
        self.wait_for_block_num(target, CHAIN_POLL_TIMEOUT).await
    }

    /// Wait until the validator's chain tip reaches `target`, or
    /// `timeout` elapses. The test-author-facing readiness primitive —
    /// pair with [`generate_blocks`] for "mine then wait" flows that
    /// previously lived in `TestManager::generate_blocks_and_wait_for_tip`.
    pub async fn wait_for_block_num(
        &self,
        target: u32,
        timeout: Duration,
    ) -> Result<(), RpcError> {
        let started = tokio::time::Instant::now();
        let deadline = started + timeout;
        loop {
            if self.chain_height().await? >= target {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RpcError::timeout(
                    self.backend.label(),
                    "wait_for_block_num",
                    started.elapsed(),
                    format!("chain did not reach height {target}"),
                ));
            }
            tokio::time::sleep(CHAIN_POLL_INTERVAL).await;
        }
    }

    /// Typed JSON-RPC client targeting this validator's RPC port.
    /// Use for any JSON-RPC call outside the small typed-sugar surface
    /// (`tip`, `chain_height`, …). Cheap; rebuild per call.
    ///
    /// Carries the backend's auth credentials when required — see
    /// [`backends::ValidatorBackend::build_json_rpc`].
    pub async fn json_rpc(&self) -> Result<JsonRpcClient, EnvError> {
        let ep = self.endpoint("rpc").await?;
        Ok(self.backend.build_json_rpc(&ep))
    }

    /// Configured network-upgrade activation heights, as reported by
    /// the running validator.
    pub async fn activation_heights(&self) -> Result<ActivationHeights, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::activation_heights(self.backend.label(), &client).await
    }

    /// `BlockTip { height, hash }` from `getblockchaininfo`.
    pub async fn tip(&self) -> Result<BlockTip, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::tip(self.backend.label(), &client).await
    }

    /// `getblock <height> 1` → `Block { height, hash }`.
    pub async fn get_block(&self, height: u32) -> Result<Block, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::get_block(self.backend.label(), &client, height).await
    }

    /// `getblock <hash> 1` → `Block`. The hash bytes must be in display
    /// (big-endian) order — i.e. exactly the bytes stored in
    /// `Block.hash`, so you can chain `get_block_by_hash(&b.hash)`.
    pub async fn get_block_by_hash(&self, hash: &[u8; 32]) -> Result<Block, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::get_block_by_hash(self.backend.label(), &client, hash).await
    }

    /// `getbestblockhash` → tip block hash as a lowercase hex string.
    pub async fn best_block_hash(&self) -> Result<String, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::best_block_hash(self.backend.label(), &client).await
    }

    /// `getblockcount` → current block count. Equivalent to
    /// [`chain_height`] but issued via the simpler RPC method; useful
    /// when a test wants parity against this specific RPC.
    pub async fn block_count(&self) -> Result<u32, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::block_count(self.backend.label(), &client).await
    }

    /// `getblocksubsidy <height>` — returned as raw JSON because the
    /// envelope shape varies across network upgrades (NU6 funding
    /// streams etc.). Project the fields you need at the call site.
    pub async fn block_subsidy(&self, height: u32) -> Result<serde_json::Value, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::block_subsidy(self.backend.label(), &client, height).await
    }

    /// `getmempoolinfo` → `MempoolInfo { size, bytes, usage }`.
    pub async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::mempool_info(self.backend.label(), &client).await
    }

    /// `getblockheader <hash> <verbose>` — returned as raw JSON. With
    /// `verbose=true` the result is the structured-object form; with
    /// `verbose=false` it's a hex-string-wrapped serialized header.
    pub async fn get_block_header(
        &self,
        hash: &str,
        verbose: bool,
    ) -> Result<serde_json::Value, RpcError> {
        let client = self.rpc_client().await?;
        jsonrpc::get_block_header(self.backend.label(), &client, hash, verbose).await
    }

    /// Mid-test snapshot of this validator's PVC.
    pub async fn snapshot(&self) -> Result<SnapshotRef, EnvError> {
        unimplemented!("ValidatorHandle::snapshot — mid-test snapshot wiring")
    }
}
