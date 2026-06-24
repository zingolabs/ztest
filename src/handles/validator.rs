//! Validator backends — two traits, nothing else.
//!
//!  - [`ValidatorConfig`] — what your config ZST implements (e.g.
//!    `ZebraBackend`). Config-time behaviour (label, NU ceiling, regtest
//!    materialization) plus the factory that turns it into a live handle
//!    once the env assigns plumbing.
//!  - [`ValidatorBackend`] — what your live handle implements (e.g.
//!    `ZebraValidator`): the RPC contract a test drives the validator
//!    with. Backend-specific RPCs (zebrad's `getblockchaininfo`,
//!    zcashd's `getblockdeltas`) are *inherent* methods on the concrete
//!    handle, so calling one on the wrong backend is a compile error.

use std::time::Duration;

use async_trait::async_trait;
use zingo_common_components::protocol::ActivationHeights;

use crate::component::ComponentOpts;
use crate::handles::client::JsonRpcClient;
use crate::handles::wallet::Pool;
use crate::handles::{Endpoint, HandleInner};
use crate::topology::NetworkUpgrade;
use crate::{EnvError, RpcError};

pub use zcash_primitives::block::BlockHash;
pub use zcash_protocol::consensus::BlockHeight;

// JSON-RPC envelope types are owned by the protocol module — re-exported
// here so the public surface at `ztest::handles::validator::*` stays
// stable for existing consumers and `lib.rs` re-exports.
pub use crate::protocol::zcash_rpc::{BlockTip, BlockchainInfo, MempoolInfo, Peer, PeerInfo};

/// Static consensus parameters for a validator's network, sourced from
/// ztest's pinned view — NOT live chain state. The network identity is
/// read from the node; the constants are then resolved from ztest's pins
/// (the `zebra-chain` dependency for zebrad). Distinct from
/// [`BlockchainInfo`] (runtime tip) and the node-enforced
/// [`ValidatorBackend::activation_heights`].
#[derive(Debug, Clone, PartialEq)]
pub struct ChainConfig {
    /// Network identifier as the node reports it (`"regtest"`, `"test"`,
    /// `"main"`).
    pub network: String,
    /// Height of the first block-subsidy halving, when ztest models the
    /// backend's subsidy schedule. `Some` for zebrad (derived from the
    /// pinned `zebra-chain`); `None` for zcashd — ztest sets no
    /// `nSubsidyHalvingInterval`, so the binary's regtest default applies
    /// and ztest does not track it.
    pub first_halving_height: Option<BlockHeight>,
}

pub trait ValidatorConfig: Send + Sync + std::fmt::Debug + 'static {
    /// The live handle type this backend produces.
    type Handle: ValidatorBackend + Clone;

    /// Build the runtime handle once the env has assigned `plumbing`
    /// (the back-reference + component id used to resolve endpoints).
    fn into_handle(&self, plumbing: HandleInner) -> Self::Handle;

    /// The value pool this backend mines its coinbase into when a test
    /// doesn't override it via
    /// [`Validator::mine_to`](crate::component::Validator::mine_to).
    /// zebrad defaults to [`Pool::Transparent`] (the pool it can mine
    /// that avoids the Orchard-anchor and Sapling-scan problems); zcashd
    /// to [`Pool::Sapling`] (a scannable, instantly-spendable shielded
    /// coinbase).
    fn default_coinbase_pool(&self) -> Pool;

    /// Highest network upgrade this backend, at the given pinned
    /// version, can decode. Used by the topology resolver to compute the
    /// activation-height ceiling. `None` opts out of the resolver.
    fn nu_ceiling(&self, version: &str) -> Option<NetworkUpgrade> {
        let _ = version;
        None
    }

    /// Apply this backend's regtest-time, height-dependent mounts /
    /// flags to a `ComponentOpts`. Called from `env.build()` after the
    /// topology resolver has chosen `activation`. Returns
    /// [`EnvError::Config`](crate::EnvError::Config) for invalid
    /// configuration (e.g. an unparseable pinned version). Default: no-op.
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

/// A validator backend's value-pool capabilities.
///
/// Groups the two pool facts a test needs about a node: which pools it
/// validates at all, and the single pool its coinbase pays into. The two
/// have different cardinality — `supported` is a set, `coinbase` is one
/// distinguished member of it — so they're modelled as distinct fields
/// rather than a per-pool map.
#[derive(Debug, Clone)]
pub struct PoolSupport {
    /// Every value pool the node validates on its chain. zcashd is
    /// end-of-life and never gained Orchard support, so its set omits
    /// [`Pool::Orchard`]; zebrad lists all three. `coinbase` is always a
    /// member.
    pub supported: &'static [Pool],

    /// The single pool the coinbase pays into — a fixed property of the
    /// backend's miner address (baked into its regtest config), not a
    /// per-test choice. zebrad mines to [`Pool::Orchard`], zcashd to
    /// [`Pool::Sapling`]. Always one of [`Self::supported`].
    pub coinbase: Pool,
}

impl PoolSupport {
    /// Whether the node validates `pool`. Tests gate pool-specific work
    /// on this — e.g. skip an Orchard send where `supports(Pool::Orchard)`
    /// is `false`, rather than letting it fail deep in the node.
    pub fn supports(&self, pool: Pool) -> bool {
        self.supported.contains(&pool)
    }
}

#[async_trait]
pub trait ValidatorBackend: Send + Sync + std::fmt::Debug + 'static {
    /// Stable label string for the backend behind this handle.
    fn label(&self) -> &'static str;

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

    /// Generate `n` blocks. Returns the new chain-tip height once the
    /// chain has advanced. The coinbase pays into
    /// [`PoolSupport::coinbase`] — the pool fixed for this backend.
    async fn generate_blocks(&self, n: u32) -> Result<BlockHeight, RpcError>;

    /// This backend's value-pool capabilities: which pools it validates,
    /// and the single pool its coinbase pays into (chosen per-validator
    /// via [`Validator::mine_to`](crate::component::Validator::mine_to),
    /// defaulting to [`ValidatorConfig::default_coinbase_pool`]). See
    /// [`PoolSupport`].
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

    /// `getblocksubsidy <height>` — raw JSON (network/branch dependent).
    async fn block_subsidy(&self, height: BlockHeight) -> Result<serde_json::Value, RpcError>;

    /// Mempool statistics.
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError>;

    /// `getblockheader <hash> <verbose>` — raw JSON.
    async fn get_block_header(
        &self,
        hash: &str,
        verbose: bool,
    ) -> Result<serde_json::Value, RpcError>;

    /// Regtest network-upgrade activation heights, read from the chain.
    async fn activation_heights(&self) -> Result<ActivationHeights, RpcError>;

    /// Static consensus parameters for this validator's network. See
    /// [`ChainConfig`]. Reads the network identity from the node, then
    /// resolves ztest's pinned constants for it. Distinct from
    /// [`Self::activation_heights`] (what the node *enforces*) and from
    /// [`BlockchainInfo`] (live tip state).
    async fn chain_config(&self) -> Result<ChainConfig, RpcError>;

    // ── conveniences: loops over the methods above, implemented per
    //    backend (no default bodies — each handle spells its own out) ──

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
