//! Validator backends.
//!
//! - [`ValidatorConfig`] = config ZST (config-time behaviour + live-handle factory)
//! - [`ValidatorBackend`] = the live handle's RPC contract
//! - Backend-specific RPCs stay inherent on the concrete handle (wrong backend = compile error)

use std::time::Duration;

use crate::topology::ActivationHeights;
use async_trait::async_trait;

use crate::component::ComponentOpts;
use crate::handles::HandleInner;
use crate::handles::wallet::Pool;
use crate::protocol::Endpoint;
use crate::protocol::client::JsonRpcClient;
use crate::{EnvError, RpcError};

pub use zcash_protocol::consensus::BlockHeight;

// Re-exported so a backend impl pulls trait + response types from one path
pub use crate::protocol::types::{
    BlockHash, BlockTip, BlockchainInfo, MempoolInfo, Peer, PeerInfo,
};

/// Static consensus params from ztest's pins, keyed on the node's network
/// identity. Not live chain state (that's [`BlockchainInfo`]).
///
/// - `network` = the node's own spelling (`regtest`/`test`/`main`)
/// - `first_halving_height` = `None` on zcashd (ztest sets no `nSubsidyHalvingInterval`)
#[derive(Debug, Clone, PartialEq)]
pub struct ChainConfig {
    pub network: String,
    pub first_halving_height: Option<BlockHeight>,
}

pub trait ValidatorConfig: Send + Sync + std::fmt::Debug + 'static {
    type Handle: ValidatorBackend + Clone;

    /// Backend tuning tokens ([`ComponentBuilder::tuning`](crate::ComponentBuilder::tuning));
    /// [`NoTuning`](crate::component::NoTuning) where there are no knobs
    type Tuning: Clone + std::fmt::Debug + Send + Sync + 'static;

    /// Build the runtime handle once the env has assigned `plumbing`
    fn to_handle(&self, plumbing: HandleInner) -> Self::Handle;

    /// Default coinbase pool, absent
    /// [`Validator::mine_to`](crate::component::Validator::mine_to). Both backends
    /// mine any pool; this is cost/convenience (zebrad [`Pool::Transparent`] =
    /// cheapest template, zcashd [`Pool::Sapling`])
    fn default_coinbase_pool(&self) -> Pool;

    /// Container port serving Prometheus, published as [`crate::metrics::PORT_NAME`].
    /// `None` = this backend exports nothing (no metrics plane, no `Exporter` impl)
    fn metrics_port(&self) -> Option<u16> {
        None
    }

    /// Stable backend label (`zcashd`/`zebrad`), readable pre-launch so a
    /// backend-generic test can branch on it
    fn label(&self) -> &'static str;

    /// Entrypoint argv, backend-local scratch mounts, regtest flag. Called from
    /// the blanket [`Regtest`](crate::regtest::Regtest) impl on
    /// [`Validator`](crate::component::Validator); height-dependent config lands
    /// later in [`Self::materialize_regtest_opts`]
    fn regtest_opts(&self, opts: ComponentOpts) -> ComponentOpts;

    /// Height-dependent regtest mounts/flags, applied from `env.build()` once the
    /// topology resolver has chosen `activation`. Default no-op
    fn materialize_regtest_opts(
        &self,
        opts: ComponentOpts,
        activation: &ActivationHeights,
        peers: &[(String, u16)],
    ) -> Result<ComponentOpts, EnvError> {
        let _ = (activation, peers);
        Ok(opts)
    }
}

/// Backend value-pool capabilities. `coinbase` follows the backend's miner
/// address, always in `supported`, overridable per-validator via
/// [`Validator::mine_to`](crate::component::Validator::mine_to)
#[derive(Debug, Clone)]
pub struct PoolSupport {
    pub supported: &'static [Pool],
    pub coinbase: Pool,
}

impl PoolSupport {
    /// Gate pool-specific work here, not deep inside the node
    pub fn supports(&self, pool: Pool) -> bool {
        self.supported.contains(&pool)
    }
}

#[async_trait]
pub trait ValidatorBackend: Send + Sync + std::fmt::Debug + 'static {
    fn label(&self) -> &'static str;

    /// Backend-owned image, ports, ready port, security context
    fn pod_spec(
        &self,
        opts: &crate::component::ComponentOpts,
        pod_name: String,
    ) -> Result<crate::manifest::PodSpec, crate::EnvError>;

    /// Resolve a named endpoint (e.g. `"rpc"`)
    async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError>;

    async fn endpoint_for(&self, container_port: u16) -> Result<Endpoint, EnvError>;

    /// Typed JSON-RPC client for this validator's `rpc` endpoint
    async fn json_rpc(&self) -> Result<JsonRpcClient, EnvError>;

    /// Block until JSON-RPC reports ready, or `timeout`. Probe is backend-specific
    /// (zebrad `getblocktemplate`, zcashd `getinfo`)
    async fn ready(&self, timeout: Duration) -> Result<(), RpcError>;

    /// Generate `n` blocks → new tip height once the chain advances. Coinbase pays
    /// into [`PoolSupport::coinbase`]
    ///
    /// - one `generate` RPC **per block**, never one call for `n` (a batched call is
    ///   held open for the whole mine and dies with the portforward)
    async fn generate_blocks(&self, n: u32) -> Result<BlockHeight, RpcError>;

    fn pool_support(&self) -> PoolSupport;

    async fn chain_height(&self) -> Result<BlockHeight, RpcError>;

    async fn tip(&self) -> Result<BlockTip, RpcError>;

    async fn get_block(&self, height: BlockHeight) -> Result<BlockTip, RpcError>;

    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<BlockTip, RpcError>;

    async fn best_block_hash(&self) -> Result<BlockHash, RpcError>;

    /// Mark `hash` invalid → node disconnects it + every descendant, settles on
    /// the best remaining chain.
    ///
    /// - Deterministic re-org primitive: capture a fork-point hash, invalidate,
    ///   mine a competing branch (no second miner, no partition)
    /// - May return before the chain settles; poll [`tip`](Self::tip)
    async fn invalidate_block(&self, hash: &BlockHash) -> Result<(), RpcError>;

    /// Clear a prior [`invalidate_block`](Self::invalidate_block); node
    /// reconsiders that block + descendants
    async fn reconsider_block(&self, hash: &BlockHash) -> Result<(), RpcError>;

    async fn block_count(&self) -> Result<BlockHeight, RpcError>;

    /// `getblocksubsidy <height>`: raw JSON (network/branch dependent)
    async fn block_subsidy(&self, height: BlockHeight) -> Result<serde_json::Value, RpcError>;

    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError>;

    /// `getblockheader <hash> <verbose>`: raw JSON
    async fn get_block_header(
        &self,
        hash: &str,
        verbose: bool,
    ) -> Result<serde_json::Value, RpcError>;

    async fn activation_heights(&self) -> Result<ActivationHeights, RpcError>;

    /// ztest's pinned constants ([`ChainConfig`]); not what the node enforces
    /// ([`Self::activation_heights`]), not live tip state ([`BlockchainInfo`])
    async fn chain_config(&self) -> Result<ChainConfig, RpcError>;

    /// Whether the env configured this validator for regtest.
    ///
    /// - Answered from env plumbing, not RPC (zebra reports regtest as
    ///   `chain: "test"`, so RPC cannot recover it)
    /// - Separates a created chain (genesis, mineable) from a restored one (at tip, read-only)
    fn is_regtest(&self) -> bool;

    // Conveniences: loops over the methods above, implemented per backend

    /// Poll until the chain reaches `target`, at the backend's default poll timeout
    async fn poll_chain_height(&self, target: BlockHeight) -> Result<(), RpcError>;

    async fn wait_for_block_num(
        &self,
        target: BlockHeight,
        timeout: Duration,
    ) -> Result<(), RpcError>;
}
