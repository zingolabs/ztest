//! Dev-facing RPC domain types.
//!
//! The typed forms of validator/indexer RPC responses that test authors
//! interact with through the backend traits (`ValidatorBackend`,
//! `IndexerBackend`) and the per-backend inherent methods. They live in the
//! interface layer (here) and are produced by the transports
//! ([`ZcashRpc`](crate::protocol::zcash_rpc), the gRPC clients), so the
//! dependency points transport -> interface, not the reverse. Trait-specific
//! config/capability types (`ChainConfig`, `PoolSupport`) stay next to the
//! `ValidatorBackend` trait they describe.

use zcash_protocol::consensus::BlockHeight;

/// A 32-byte block hash, shared by the validator and indexer backends (both
/// expose `get_block_by_hash`). ztest defines its own rather than linking
/// `zcash_primitives::block::BlockHash` (which drags in the whole Orchard/Halo2
/// proving stack). The bytes are stored exactly as the node's RPC returns
/// them, so `hex::encode`/`hex::decode` round-trip directly and chaining
/// `get_block_by_hash(&tip_hash)` works without reordering.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct BlockHash(pub [u8; 32]);

impl std::fmt::Display for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

/// Block-tip summary: chain height + best block hash.
pub type BlockTip = (BlockHeight, BlockHash);

/// Mempool statistics returned by `getmempoolinfo`.
#[derive(Debug, Clone, Copy)]
pub struct MempoolInfo {
    pub size: u64,
    pub bytes: u64,
    pub usage: Option<u64>,
}

/// Chain identity, tip, and difficulty summary returned by `getblockchaininfo`.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockchainInfo {
    pub chain: String,
    pub blocks: BlockHeight,
    pub headers: BlockHeight,
    pub best_block_hash: BlockHash,
    pub difficulty: f64,
    pub estimated_height: Option<BlockHeight>,
}

/// Peer-table snapshot returned by `getpeerinfo`.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerInfo {
    pub peers: Vec<Peer>,
}

/// One row from [`PeerInfo`].
#[derive(Debug, Clone, PartialEq)]
pub struct Peer {
    pub addr: String,
    pub inbound: bool,
    pub version: u32,
    pub subver: String,
}
