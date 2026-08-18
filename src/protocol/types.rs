//! Dev-facing RPC domain types: typed validator/indexer responses, reached
//! through the backend traits.
//!
//! - Interface layer, produced by the transports (dependency points transport → interface)
//! - Trait-specific config/capability types (`ChainConfig`, `PoolSupport`) stay
//!   beside their trait

use zcash_protocol::consensus::BlockHeight;

/// 32-byte block hash, shared by validator and indexer backends.
///
/// - ztest-owned (`zcash_primitives::block::BlockHash` drags in Orchard/Halo2)
/// - Bytes stored exactly as the node's RPC returns them, so `hex` round-trips
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct BlockHash(pub [u8; 32]);

impl std::fmt::Display for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

pub type BlockTip = (BlockHeight, BlockHash);

/// `getmempoolinfo` statistics
#[derive(Debug, Clone, Copy)]
pub struct MempoolInfo {
    pub size: u64,
    pub bytes: u64,
    pub usage: Option<u64>,
}

/// `getblockchaininfo`: chain identity, tip, difficulty
#[derive(Debug, Clone, PartialEq)]
pub struct BlockchainInfo {
    pub chain: String,
    pub blocks: BlockHeight,
    pub headers: BlockHeight,
    pub best_block_hash: BlockHash,
    pub difficulty: f64,
    pub estimated_height: Option<BlockHeight>,
}

/// `getpeerinfo` peer-table snapshot
#[derive(Debug, Clone, PartialEq)]
pub struct PeerInfo {
    pub peers: Vec<Peer>,
}

/// One row from [`PeerInfo`]
#[derive(Debug, Clone, PartialEq)]
pub struct Peer {
    pub addr: String,
    pub inbound: bool,
    pub version: u32,
    pub subver: String,
}
