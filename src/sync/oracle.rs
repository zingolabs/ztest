//! At-completion correctness-oracle helpers.
//!
//! The headline sync invariant checks the wallet's note-commitment-tree root
//! against the indexer's, an independent authority. The wallet side is exposed
//! on the terminal [`Snapshot::tree_root`](crate::sync::Snapshot::tree_root);
//! this module supplies the indexer side — deserializing a `TreeState` frontier
//! (as returned by `IndexerBackend::get_tree_state`) into the same 32-byte root
//! encoding so an `at_completion` probe is a plain `==`.

use crate::handles::wallet::Pool;

/// Parse a lightwalletd `TreeState` pool frontier — the hex-encoded legacy
/// serialized `CommitmentTree` in `TreeState.sapling_tree` / `orchard_tree` —
/// into its 32-byte Merkle root, matching
/// [`Snapshot::tree_root`](crate::sync::Snapshot::tree_root)'s encoding of the
/// wallet's own shard-tree root.
///
/// Returns `None` for an empty frontier (a pool with no commitments yet — the
/// wallet-side root is likewise empty, and an empty-vs-empty check is a no-op),
/// a pool with no wire representation (Ironwood/Transparent — the proto
/// `TreeState` carries only sapling and orchard), or an undecodable frontier.
pub fn commitment_tree_root(pool: Pool, hex_frontier: &str) -> Option<[u8; 32]> {
    if hex_frontier.is_empty() {
        return None;
    }
    let bytes = hex::decode(hex_frontier).ok()?;
    match pool {
        Pool::Sapling => {
            use sapling_crypto::{NOTE_COMMITMENT_TREE_DEPTH, Node};
            let tree = zcash_primitives::merkle_tree::read_commitment_tree::<
                Node,
                _,
                NOTE_COMMITMENT_TREE_DEPTH,
            >(&bytes[..])
            .ok()?;
            Some(tree.to_frontier().root().to_bytes())
        }
        Pool::Orchard => {
            use orchard::tree::MerkleHashOrchard;
            let tree = zcash_primitives::merkle_tree::read_commitment_tree::<
                MerkleHashOrchard,
                _,
                { orchard::NOTE_COMMITMENT_TREE_DEPTH as u8 },
            >(&bytes[..])
            .ok()?;
            Some(tree.to_frontier().root().to_bytes())
        }
        Pool::Ironwood | Pool::Transparent => None,
    }
}
