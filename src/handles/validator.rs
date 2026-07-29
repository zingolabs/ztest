//! Validator backends: [`ValidatorConfig`] is the config ZST (config-time
//! behaviour plus the factory for a live handle); [`ValidatorBackend`] is the
//! live handle's RPC contract. Backend-specific RPCs are inherent methods on
//! the concrete handle, so calling one on the wrong backend is a compile error.

use std::time::Duration;

use crate::topology::ActivationHeights;
use async_trait::async_trait;

use crate::component::ComponentOpts;
use crate::handles::client::JsonRpcClient;
use crate::handles::wallet::Pool;
use crate::handles::{Endpoint, HandleInner};
use crate::{EnvError, RpcError};

pub use zcash_protocol::consensus::BlockHeight;

// Re-exported from `handles::types` so a backend impl pulls the trait and its
// response types from one path, and the public surface stays stable.
pub use crate::handles::types::{BlockHash, BlockTip, BlockchainInfo, MempoolInfo, Peer, PeerInfo};

/// Static consensus parameters for a validator's network, sourced from ztest's
/// pinned view, not live chain state: the network identity is read from the
/// node, then the constants are resolved from ztest's pins. Distinct from
/// [`BlockchainInfo`] (runtime tip) and the node-enforced
/// [`ValidatorBackend::activation_heights`].
#[derive(Debug, Clone, PartialEq)]
pub struct ChainConfig {
    /// Network identifier as the node reports it (`"regtest"`, `"test"`,
    /// `"main"`).
    pub network: String,
    /// Height of the first block-subsidy halving. `Some` for zebrad (derived
    /// from the pinned `zebra-chain`); `None` for zcashd, where ztest sets no
    /// `nSubsidyHalvingInterval` and so doesn't track it.
    pub first_halving_height: Option<BlockHeight>,
}

pub trait ValidatorConfig: Send + Sync + std::fmt::Debug + 'static {
    /// The live handle type this backend produces.
    type Handle: ValidatorBackend + Clone;

    /// Backend-specific tuning tokens (see [`ComponentBuilder::tuning`]).
    /// [`NoTuning`](crate::component::NoTuning) for validators with no knobs.
    type Tuning: Clone + std::fmt::Debug + Send + Sync + 'static;

    /// Build the runtime handle once the env has assigned `plumbing`.
    fn to_handle(&self, plumbing: HandleInner) -> Self::Handle;

    /// The value pool this backend mines its coinbase into when a test doesn't
    /// override it via
    /// [`Validator::mine_to`](crate::component::Validator::mine_to). Both
    /// backends can mine any pool; the default is a cost/convenience choice
    /// (zebrad [`Pool::Transparent`] — cheapest block template; zcashd
    /// [`Pool::Sapling`]).
    fn default_coinbase_pool(&self) -> Pool;

    /// Stable label for this backend (`"zcashd"` / `"zebrad"`). Available on
    /// the spec before launch, so a backend-generic test can branch on it.
    fn label(&self) -> &'static str;

    /// Apply this backend's regtest launch configuration (entrypoint argv and
    /// any backend-local scratch mounts) to `opts`, and flag it regtest. Called
    /// once from the blanket [`Regtest`](crate::regtest::Regtest) impl on
    /// [`Validator`](crate::component::Validator), so a single generic
    /// `.regtest()` covers every backend — new backends get it for free.
    /// Height-dependent config is rendered later in
    /// [`Self::materialize_regtest_opts`].
    fn regtest_opts(&self, opts: ComponentOpts) -> ComponentOpts;

    /// Apply this backend's regtest-time, height-dependent mounts / flags to a
    /// `ComponentOpts`. Called from `env.build()` after the topology resolver
    /// has chosen `activation`. Returns
    /// [`EnvError::Config`](crate::EnvError::Config) on invalid config. Default:
    /// no-op.
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

/// A validator backend's value-pool capabilities: which pools it validates
/// (`supported`, a set) and the single pool its coinbase pays into
/// (`coinbase`).
#[derive(Debug, Clone)]
pub struct PoolSupport {
    /// Every value pool the node validates on its chain. `coinbase` is always
    /// a member.
    pub supported: &'static [Pool],

    /// The single pool the coinbase pays into: a fixed property of the
    /// backend's miner address, overridable per-validator via
    /// [`Validator::mine_to`](crate::component::Validator::mine_to). Always one
    /// of [`Self::supported`].
    pub coinbase: Pool,
}

impl PoolSupport {
    /// Whether the node validates `pool`. Tests gate pool-specific work on this
    /// rather than letting it fail deep in the node.
    pub fn supports(&self, pool: Pool) -> bool {
        self.supported.contains(&pool)
    }
}

#[async_trait]
pub trait ValidatorBackend: Send + Sync + std::fmt::Debug + 'static {
    /// Stable label string for the backend behind this handle.
    fn label(&self) -> &'static str;

    /// Build the Kubernetes [`PodSpec`](crate::manifest::PodSpec) for launching
    /// this backend from its resolved `opts` and assigned `pod_name`. Each
    /// backend owns its image, ports, ready port, and security context.
    fn pod_spec(
        &self,
        opts: &crate::component::ComponentOpts,
        pod_name: String,
    ) -> Result<crate::manifest::PodSpec, crate::EnvError>;

    /// Resolve a named endpoint (e.g. `"rpc"`).
    async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError>;

    /// Resolve an endpoint by its container port.
    async fn endpoint_for(&self, container_port: u16) -> Result<Endpoint, EnvError>;

    /// Typed JSON-RPC client for this validator's `rpc` endpoint.
    async fn json_rpc(&self) -> Result<JsonRpcClient, EnvError>;

    /// Block until the validator's JSON-RPC reports ready, or `timeout`
    /// elapses. The readiness probe (`getblocktemplate` for zebrad,
    /// `getinfo` for zcashd) is backend-specific.
    async fn ready(&self, timeout: Duration) -> Result<(), RpcError>;

    /// Generate `n` blocks. Returns the new chain-tip height once the chain has
    /// advanced. The coinbase pays into [`PoolSupport::coinbase`], the pool
    /// fixed for this backend.
    async fn generate_blocks(&self, n: u32) -> Result<BlockHeight, RpcError>;

    /// This backend's value-pool capabilities. See [`PoolSupport`].
    fn pool_support(&self) -> PoolSupport;

    /// Current chain-tip height.
    async fn chain_height(&self) -> Result<BlockHeight, RpcError>;

    /// Chain-tip `(height, hash)`.
    async fn tip(&self) -> Result<BlockTip, RpcError>;

    /// `(height, hash)` for the block at `height`.
    async fn get_block(&self, height: BlockHeight) -> Result<BlockTip, RpcError>;

    /// `(height, hash)` for the block with `hash`.
    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<BlockTip, RpcError>;

    /// Tip block hash.
    async fn best_block_hash(&self) -> Result<BlockHash, RpcError>;

    /// Current block count.
    async fn block_count(&self) -> Result<BlockHeight, RpcError>;

    /// `getblocksubsidy <height>`: raw JSON (network/branch dependent).
    async fn block_subsidy(&self, height: BlockHeight) -> Result<serde_json::Value, RpcError>;

    /// Mempool statistics.
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError>;

    /// `getblockheader <hash> <verbose>`: raw JSON.
    async fn get_block_header(
        &self,
        hash: &str,
        verbose: bool,
    ) -> Result<serde_json::Value, RpcError>;

    /// Regtest network-upgrade activation heights, read from the chain.
    async fn activation_heights(&self) -> Result<ActivationHeights, RpcError>;

    /// Static consensus parameters for this validator's network. See
    /// [`ChainConfig`]. Distinct from [`Self::activation_heights`] (what the
    /// node enforces) and [`BlockchainInfo`] (live tip state).
    async fn chain_config(&self) -> Result<ChainConfig, RpcError>;

    // Conveniences: loops over the methods above, implemented per backend.

    /// `generate_blocks` with a per-block delay between mines.
    async fn generate_blocks_with_delay(&self, n: u32) -> Result<BlockHeight, RpcError>;

    /// Poll until the chain reaches `target`, using the backend's default
    /// chain-poll timeout.
    async fn poll_chain_height(&self, target: BlockHeight) -> Result<(), RpcError>;

    /// Poll the chain height until it reaches `target` or `timeout`
    /// elapses.
    async fn wait_for_block_num(
        &self,
        target: BlockHeight,
        timeout: Duration,
    ) -> Result<(), RpcError>;
}
