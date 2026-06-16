//! Per-binary RPC call sites.
//!
//! Each component category (validator / indexer / wallet) has one
//! `*Backend` trait here. The handle stores `Arc<dyn *Backend>` and
//! routes everything backend-varying through it — no `match self.kind`
//! anywhere in the dispatch path. Each module in this directory
//! contributes one ZST implementing its category's trait, plus any
//! backend-specific extension trait (e.g. `ZebraValidator`,
//! `ZainoIndexer`) for RPCs only that backend serves.
//!
//! The only `match` on a kind enum in this subsystem is the
//! construction-time factory ([`validator_for_kind`],
//! [`indexer_for_kind`], [`wallet_for_kind`]). Adding a new backend =
//! one module + one trait impl + one factory arm.
//!
//! Async methods box their future via `#[async_trait]` so the traits
//! are dyn-compatible. The per-call cost is one allocation; at "one
//! gRPC roundtrip per test op" frequency that's irrelevant, and the
//! readability vs hand-written `Pin<Box<dyn Future>>` is decisive at
//! the indexer trait's ~15-method surface.

use std::sync::Arc;

use async_trait::async_trait;

use crate::handles::client::{AuthedRpc, JsonRpcClient};
use crate::handles::indexer::{
    AddressUtxo, CompactBlock, IndexerInfo, IndexerKind, MempoolTx, SendResult, ShieldedProtocol,
    SubtreeRoot, TransactionBytes, TreeState,
};
use crate::handles::validator::ValidatorKind;
use crate::handles::wallet::WalletKind;
use crate::handles::Endpoint;
use crate::RpcError;

pub(crate) mod image;
pub(crate) mod lightwalletd;
pub mod zainod;
pub(crate) mod zcashd;
pub(crate) mod zebra;
pub(crate) mod zingo;

// ─────────────────────────── ValidatorBackend ─────────────────────────

/// Backend-varying behaviour for a validator. Object-safe; held by
/// `ValidatorHandle` as `Arc<dyn ValidatorBackend>`.
#[async_trait]
pub(crate) trait ValidatorBackend: Send + Sync + std::fmt::Debug {
    /// Backend discriminant. Used by backend-specific extension traits
    /// (e.g. `ZebraValidator`) to assert they're called on a matching
    /// handle.
    fn kind(&self) -> ValidatorKind;

    /// Stable label string for log / error attribution.
    fn label(&self) -> &'static str;

    /// Build the internal `AuthedRpc` transport for this backend's
    /// JSON-RPC port — auth scheme is the backend's responsibility.
    fn build_authed_rpc(&self, endpoint: &Endpoint) -> AuthedRpc;

    /// Build the public `JsonRpcClient` for this backend's JSON-RPC
    /// port. Same auth contract as [`build_authed_rpc`].
    fn build_json_rpc(&self, endpoint: &Endpoint) -> JsonRpcClient;

    /// Mine `n` blocks. Returns once the backend's submit path has
    /// accepted them; the caller polls chain height to confirm tip
    /// advance.
    async fn generate_blocks(&self, client: &AuthedRpc, n: u32) -> Result<(), RpcError>;

    /// Poll the validator's JSON-RPC port until it's ready to accept
    /// test traffic. The "right" probe is backend-specific:
    ///   - zebrad has no IBD guard on regtest, so `getblocktemplate`
    ///     working = chain ready to mine = test ready to drive.
    ///   - zcashd's `getblocktemplate` is gated by
    ///     `IsInitialBlockDownload`, which returns true forever on a
    ///     fresh peer-less regtest chain (error -10, "Zcash is
    ///     downloading blocks…"). It uses a different probe.
    /// Returning `Ok(())` means the backend is ready for whatever
    /// the next `TestEnv::build` step will throw at it.
    async fn wait_for_ready(
        &self,
        client: &crate::handles::client::AuthedRpc,
        address: std::net::SocketAddr,
        timeout: std::time::Duration,
    ) -> Result<(), crate::handles::client::RpcReadinessTimeout>;
}

/// Construction-time factory for a validator backend.
pub(crate) fn validator_for_kind(kind: ValidatorKind) -> Arc<dyn ValidatorBackend> {
    match kind {
        ValidatorKind::Zebrad => Arc::new(zebra::ZebraBackend),
        ValidatorKind::Zcashd => Arc::new(zcashd::ZcashdBackend),
    }
}

// ──────────────────────────── IndexerBackend ──────────────────────────

/// Backend-varying behaviour for an indexer. Object-safe; held by
/// `IndexerHandle` as `Arc<dyn IndexerBackend>`.
///
/// Methods that one backend doesn't implement return
/// `RpcError::decode` with an explanatory message — matching the
/// per-backend "lightwalletd doesn't ship this in ztest yet" shape
/// callers already expect.
#[async_trait]
pub(crate) trait IndexerBackend: Send + Sync + std::fmt::Debug {
    fn kind(&self) -> IndexerKind;
    fn label(&self) -> &'static str;

    async fn latest_block_height(&self, endpoint: &Endpoint) -> Result<u64, RpcError>;
    async fn indexer_info(&self, endpoint: &Endpoint) -> Result<IndexerInfo, RpcError>;
    async fn get_block(&self, endpoint: &Endpoint, height: u64) -> Result<CompactBlock, RpcError>;
    async fn get_block_by_hash(
        &self,
        endpoint: &Endpoint,
        hash: Vec<u8>,
    ) -> Result<CompactBlock, RpcError>;
    async fn get_taddress_balance(
        &self,
        endpoint: &Endpoint,
        addresses: Vec<String>,
    ) -> Result<i64, RpcError>;
    async fn get_block_range(
        &self,
        endpoint: &Endpoint,
        start: u64,
        end: u64,
        pool_types: Vec<i32>,
    ) -> Result<Vec<CompactBlock>, RpcError>;
    async fn drain_block_range(
        &self,
        endpoint: &Endpoint,
        start: u64,
        end: u64,
        pool_types: Vec<i32>,
    ) -> Result<(Vec<CompactBlock>, bool), RpcError>;
    async fn get_tree_state(
        &self,
        endpoint: &Endpoint,
        height: u64,
    ) -> Result<TreeState, RpcError>;
    async fn get_latest_tree_state(&self, endpoint: &Endpoint) -> Result<TreeState, RpcError>;
    async fn get_subtree_roots(
        &self,
        endpoint: &Endpoint,
        start_index: u32,
        protocol: ShieldedProtocol,
        max_entries: u32,
    ) -> Result<Vec<SubtreeRoot>, RpcError>;
    async fn get_taddress_txids(
        &self,
        endpoint: &Endpoint,
        address: String,
        start_height: u64,
        end_height: u64,
    ) -> Result<Vec<TransactionBytes>, RpcError>;
    async fn get_address_utxos(
        &self,
        endpoint: &Endpoint,
        addresses: Vec<String>,
        start_height: u64,
        max_entries: u32,
    ) -> Result<Vec<AddressUtxo>, RpcError>;
    async fn get_address_utxos_stream(
        &self,
        endpoint: &Endpoint,
        addresses: Vec<String>,
        start_height: u64,
        max_entries: u32,
    ) -> Result<Vec<AddressUtxo>, RpcError>;
    async fn get_mempool_tx(
        &self,
        endpoint: &Endpoint,
        exclude_txid_suffixes: Vec<Vec<u8>>,
    ) -> Result<Vec<MempoolTx>, RpcError>;
    async fn get_mempool_stream(
        &self,
        endpoint: &Endpoint,
    ) -> Result<Vec<TransactionBytes>, RpcError>;
    async fn send_transaction(
        &self,
        endpoint: &Endpoint,
        tx_bytes: Vec<u8>,
    ) -> Result<SendResult, RpcError>;
    async fn get_transaction(
        &self,
        endpoint: &Endpoint,
        txid: Vec<u8>,
    ) -> Result<TransactionBytes, RpcError>;
}

/// Construction-time factory for an indexer backend.
pub(crate) fn indexer_for_kind(kind: IndexerKind) -> Arc<dyn IndexerBackend> {
    match kind {
        IndexerKind::Zainod => Arc::new(zainod::ZainoBackend),
        IndexerKind::Lightwalletd => Arc::new(lightwalletd::LightwalletdBackend),
    }
}

// ──────────────────────────── WalletBackend ───────────────────────────

/// Backend-varying behaviour for a wallet. Object-safe; held by
/// `WalletHandle` as `Arc<dyn WalletBackend>`.
#[async_trait]
pub(crate) trait WalletBackend: Send + Sync + std::fmt::Debug {
    fn kind(&self) -> WalletKind;
    fn label(&self) -> &'static str;

    /// Last block height the wallet has synced to.
    async fn synced_height(&self, endpoint: &Endpoint) -> Result<u64, RpcError>;
}

/// Construction-time factory for a wallet backend.
pub(crate) fn wallet_for_kind(kind: WalletKind) -> Arc<dyn WalletBackend> {
    match kind {
        WalletKind::Zingo => Arc::new(zingo::ZingoBackend),
    }
}
