//! Indexer backends.
//!
//! - [`IndexerConfig`] = config ZST: config-time behaviour + factory for a live handle
//! - [`IndexerBackend`] = the live handle's gRPC contract
//! - Backend-specific RPCs are inherent methods → wrong-backend call = compile error

use std::time::Duration;

use async_trait::async_trait;

use crate::handles::HandleInner;
use crate::protocol::Endpoint;
use crate::protocol::client::JsonRpcClient;
use crate::{EnvError, RpcError};

pub use crate::proto::{
    CompactBlock, CompactTx, GetAddressUtxosReply, LightdInfo, RawTransaction, SendResponse,
    SubtreeRoot, TreeState,
};
pub use crate::protocol::types::BlockHash;
// orchard-0.15 renamed `ShieldedProtocol` → `ShieldedPool`; re-export under the historical
// name (stable public API, no deprecation warning)
pub use zcash_protocol::ShieldedPool as ShieldedProtocol;
pub use zcash_protocol::TxId;
pub use zcash_protocol::consensus::BlockHeight;
pub use zcash_protocol::value::ZatBalance;

// ─────────────────────────── IndexerConfig ──────────────────────────

/// Config ZST for the [`Indexer`](crate::component::Indexer) builder (e.g. `ZainoBackend`):
/// config-time behaviour + factory for a live [`IndexerBackend`]
pub trait IndexerConfig: Send + Sync + std::fmt::Debug + 'static {
    type Handle: IndexerBackend + Clone;

    /// Tuning tokens for [`ComponentBuilder::tuning`](crate::ComponentBuilder::tuning).
    /// Knobless backends use [`NoTuning`](crate::component::NoTuning), uninhabited so
    /// `.tuning(..)` is uncallable at compile time
    type Tuning: Clone + std::fmt::Debug + Send + Sync + 'static;

    /// Build the runtime handle once the env has assigned `plumbing`
    fn to_handle(&self, plumbing: HandleInner) -> Self::Handle;

    /// Render the config at build time, once the validator host resolves. Skipped when
    /// `mode` is [`IndexerMode::None`](crate::component::IndexerMode)
    fn materialize_opts(
        &self,
        opts: crate::component::ComponentOpts,
        tunings: &[Self::Tuning],
        mode: &crate::component::IndexerMode,
        validator_host: Option<&str>,
    ) -> Result<crate::component::ComponentOpts, EnvError> {
        let _ = (tunings, mode, validator_host);
        Ok(opts)
    }
}

// ─────────────────────────── IndexerBackend ─────────────────────────

/// Live indexer: every gRPC call a test drives it with. gRPC methods required, the
/// conveniences below are composition over them
#[async_trait]
pub trait IndexerBackend: Send + Sync + std::fmt::Debug + 'static {
    fn label(&self) -> &'static str;

    /// Per-backend image, ports, ready port, security context
    fn pod_spec(
        &self,
        opts: &crate::component::ComponentOpts,
        pod_name: String,
    ) -> Result<crate::manifest::PodSpec, crate::EnvError>;

    /// Resolve a named endpoint (`"grpc"`, `"jsonrpc"`)
    async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError>;

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
    /// Raw consensus bytes relayed as-is to `SendTransaction`. Serializing is the wallet
    /// integration's job, so the harness links no transaction type
    async fn send_transaction(&self, raw_tx: &[u8]) -> Result<SendResponse, RpcError>;
    async fn get_transaction(&self, txid: TxId) -> Result<RawTransaction, RpcError>;

    // Conveniences: composition over the above, implemented per backend

    /// gRPC URI as `http://host:port`
    async fn grpc_uri(&self) -> Result<String, EnvError>;

    /// Persistent [`Channel`](tonic::transport::Channel) to `grpc`, opened once and
    /// HTTP/2-multiplexed. Per-RPC methods dial fresh per call — fine for a one-shot
    /// assertion, but that measures connection setup, so the load harness drives this
    async fn grpc_channel(&self) -> Result<tonic::transport::Channel, EnvError> {
        tonic::transport::Channel::from_shared(self.grpc_uri().await?)
            .map_err(crate::error::env_err)?
            .connect()
            .await
            .map_err(crate::error::env_err)
    }

    /// Cheap-clone [`LwdClient`](crate::loadtest::LwdClient) over a persistent channel =
    /// entry point for [`crate::loadtest`]
    async fn grpc_client(&self) -> Result<crate::loadtest::LwdClient, EnvError> {
        crate::loadtest::LwdClient::connect(self.grpc_uri().await?).await
    }

    /// Typed JSON-RPC client for the `jsonrpc` endpoint
    async fn json_rpc(&self) -> Result<JsonRpcClient, EnvError>;

    /// `get_block_range` with the default (empty) pool-type filter
    async fn get_block_range(
        &self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<Vec<CompactBlock>, RpcError>;

    /// Wait until `GetLightdInfo` succeeds, or `timeout` elapses
    async fn ready(&self, timeout: Duration) -> Result<(), RpcError>;

    /// Poll until indexed up to `target`, at the backend's default chain-poll timeout
    async fn poll_block_height(&self, target: BlockHeight) -> Result<(), RpcError>;

    async fn wait_for_block_num(
        &self,
        target: BlockHeight,
        timeout: Duration,
    ) -> Result<(), RpcError>;
}
