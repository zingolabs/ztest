//! Indexer backends: two traits.
//!
//!  - [`IndexerConfig`]: what a config ZST implements (e.g. `ZainoBackend`).
//!    Config-time behaviour (NU ceiling, regtest materialization) plus the
//!    factory that produces a live handle.
//!  - [`IndexerBackend`]: what a live handle implements (e.g. `ZainoIndexer`),
//!    the gRPC contract a test drives the indexer with. Backend-specific RPCs
//!    (zaino's `get_block_nullifiers`, JSON-RPC proxy) are inherent methods on
//!    the concrete handle, so calling one on the wrong backend is a compile
//!    error.

use std::time::Duration;

use async_trait::async_trait;

use crate::handles::client::JsonRpcClient;
use crate::handles::{Endpoint, HandleInner};
use crate::topology::NetworkUpgrade;
use crate::{EnvError, RpcError};

pub use crate::handles::types::BlockHash;
pub use crate::proto::{
    CompactBlock, CompactTx, GetAddressUtxosReply, LightdInfo, RawTransaction, SendResponse,
    SubtreeRoot, TreeState,
};
pub use zcash_protocol::ShieldedProtocol;
pub use zcash_protocol::TxId;
pub use zcash_protocol::consensus::BlockHeight;
pub use zcash_protocol::value::ZatBalance;

// ─────────────────────────── IndexerConfig ──────────────────────────

/// The config ZST handed to the [`Indexer`](crate::component::Indexer)
/// builder (e.g. `ZainoBackend`). Carries config-time behaviour (NU
/// ceiling, regtest materialization) and the factory that produces a
/// live [`IndexerBackend`].
pub trait IndexerConfig: Send + Sync + std::fmt::Debug + 'static {
    /// The live handle type this backend produces.
    type Handle: IndexerBackend + Clone;

    /// Build the runtime handle once the env has assigned `plumbing`.
    fn to_handle(&self, plumbing: HandleInner) -> Self::Handle;

    /// Highest network upgrade this backend's pinned version can decode.
    /// `None` opts out of the topology resolver.
    fn nu_ceiling(&self, version: &str) -> Option<NetworkUpgrade> {
        let _ = version;
        None
    }

    /// Apply regtest-time mounts / flags. Returns
    /// [`EnvError::Config`](crate::EnvError::Config) for invalid
    /// configuration (e.g. an unparseable pinned version). Default: no-op.
    fn materialize_regtest_opts(
        &self,
        opts: crate::component::ComponentOpts,
        regtest_backend: Option<crate::testnet_conf::ZainodBackend>,
        validator_host: Option<&str>,
    ) -> Result<crate::component::ComponentOpts, EnvError> {
        let _ = (regtest_backend, validator_host);
        Ok(opts)
    }
}

// ─────────────────────────── IndexerBackend ─────────────────────────

/// The live indexer: every gRPC call a test drives it with.
///
/// The gRPC methods are all required: each backend implements the whole
/// surface against its own endpoint. The convenience methods at the bottom are
/// pure composition over those calls, provided once.
#[async_trait]
pub trait IndexerBackend: Send + Sync + std::fmt::Debug + 'static {
    /// Stable label string for the backend behind this handle.
    fn label(&self) -> &'static str;

    /// Resolve a named endpoint (e.g. `"grpc"`, `"jsonrpc"`).
    async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError>;

    /// Resolve an endpoint by its container port.
    async fn endpoint_for(&self, container_port: u16) -> Result<Endpoint, EnvError>;

    async fn latest_block_height(&self) -> Result<BlockHeight, RpcError>;
    async fn indexer_info(&self) -> Result<LightdInfo, RpcError>;
    async fn get_block(&self, height: BlockHeight) -> Result<CompactBlock, RpcError>;
    async fn get_block_by_hash(&self, hash: BlockHash) -> Result<CompactBlock, RpcError>;
    async fn get_taddress_balance(&self, addresses: Vec<String>) -> Result<ZatBalance, RpcError>;
    async fn get_block_range_with_pools(
        &self,
        start: BlockHeight,
        end: BlockHeight,
        pool_types: Vec<i32>,
    ) -> Result<Vec<CompactBlock>, RpcError>;
    async fn drain_block_range(
        &self,
        start: BlockHeight,
        end: BlockHeight,
        pool_types: Vec<i32>,
    ) -> Result<(Vec<CompactBlock>, bool), RpcError>;
    async fn get_tree_state(&self, height: BlockHeight) -> Result<TreeState, RpcError>;
    async fn get_latest_tree_state(&self) -> Result<TreeState, RpcError>;
    async fn get_subtree_roots(
        &self,
        start_index: u32,
        protocol: ShieldedProtocol,
        max_entries: u32,
    ) -> Result<Vec<SubtreeRoot>, RpcError>;
    async fn get_taddress_txids(
        &self,
        address: String,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<Vec<RawTransaction>, RpcError>;
    async fn get_address_utxos(
        &self,
        addresses: Vec<String>,
        start_height: BlockHeight,
        max_entries: u32,
    ) -> Result<Vec<GetAddressUtxosReply>, RpcError>;
    async fn get_address_utxos_stream(
        &self,
        addresses: Vec<String>,
        start_height: BlockHeight,
        max_entries: u32,
    ) -> Result<Vec<GetAddressUtxosReply>, RpcError>;
    async fn get_mempool_tx(
        &self,
        exclude_txid_suffixes: Vec<Vec<u8>>,
    ) -> Result<Vec<CompactTx>, RpcError>;
    async fn get_mempool_stream(&self) -> Result<Vec<RawTransaction>, RpcError>;
    /// Submit a fully-serialized transaction (raw consensus bytes). ztest
    /// relays the bytes as-is to the indexer's `SendTransaction` RPC. Building
    /// and serializing a transaction is the wallet integration's job, so the
    /// harness needn't link a transaction type.
    async fn send_transaction(&self, raw_tx: &[u8]) -> Result<SendResponse, RpcError>;
    async fn get_transaction(&self, txid: TxId) -> Result<RawTransaction, RpcError>;

    // Conveniences: composition over the methods above, implemented per
    // backend (no default bodies, each handle spells its own).

    /// The indexer's gRPC URI as a string (`http://host:port`).
    async fn grpc_uri(&self) -> Result<String, EnvError>;

    /// Typed JSON-RPC client for this indexer's `jsonrpc` endpoint.
    async fn json_rpc(&self) -> Result<JsonRpcClient, EnvError>;

    /// `get_block_range` with the default (empty) pool-type filter.
    async fn get_block_range(
        &self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<Vec<CompactBlock>, RpcError>;

    /// Wait until the indexer's gRPC `GetLightdInfo` succeeds, or
    /// `timeout` elapses.
    async fn ready(&self, timeout: Duration) -> Result<(), RpcError>;

    /// Poll until the indexer indexes up to `target`, using the backend's
    /// default chain-poll timeout.
    async fn poll_block_height(&self, target: BlockHeight) -> Result<(), RpcError>;

    /// Poll the indexed height until it reaches `target` or `timeout`
    /// elapses.
    async fn wait_for_block_num(
        &self,
        target: BlockHeight,
        timeout: Duration,
    ) -> Result<(), RpcError>;
}
