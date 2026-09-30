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
/// - Display order = JSON-RPC hex, so `hex` round-trips; gRPC carries the reverse
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct BlockHash(pub [u8; 32]);

impl BlockHash {
    /// gRPC `BlockID.hash` / `CompactBlock.hash` bytes → display order (`None` = not 32 bytes)
    pub fn from_wire(bytes: &[u8]) -> Option<Self> {
        let mut hash: [u8; 32] = bytes.try_into().ok()?;
        hash.reverse();
        Some(Self(hash))
    }

    pub fn to_wire(self) -> Vec<u8> {
        self.0.iter().rev().copied().collect()
    }
}

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

#[cfg(test)]
mod tests {
    use super::BlockHash;

    #[test]
    fn wire_bytes_are_the_display_hash_reversed() {
        // mainnet block 1: `getblockhash 1` vs its `BlockID.hash` bytes
        let display = "0007bc227e1c57a4a70e237cad00e7b7ce565155ab49166bc57397a26d339283";
        let wire = "8392336da29773c56b1649ab555156ceb7e700ad7c230ea7a4571c7e22bc0700";
        let wire = hex::decode(wire).expect("golden wire hex");

        let hash = BlockHash::from_wire(&wire).expect("32 bytes");
        assert_eq!(hash.to_string(), display);
        assert_eq!(hash.to_wire(), wire);
        assert_eq!(BlockHash::from_wire(&wire[..31]), None, "31 bytes refused");
    }
}
