//! Note-commitment-tree roots — the harness's independent-authority oracle
//! (design §"End-state correctness").
//!
//! - Wallet builds its tree from the streamed compact blocks alone → matching the
//!   indexer's own root for that height proves in-order, no-skip, every-output service
//! - [`TreeRoots`] = the wallet side, folded per tick into
//!   [`Snapshot`](crate::sync::Snapshot); [`commitment_tree_root`] = the indexer side
//!   (hex frontier from `GetTreeState`)
//! - One `sapling_crypto`/`orchard` copy per graph (two give two `Node` types whose
//!   roots cannot be compared at all)

use crate::handles::wallet::Pool;

/// 32-byte note-commitment-tree root
pub type TreeRoot = [u8; 32];

/// Pools with a note commitment tree, in [`TreeRoots`] slot order
const TREE_POOLS: [Pool; 3] = [Pool::Orchard, Pool::Ironwood, Pool::Sapling];

fn slot(pool: Pool) -> Option<usize> {
    TREE_POOLS.iter().position(|&p| p == pool)
}

fn no_tree(pool: Pool) -> ! {
    panic!("{pool:?} has no note commitment tree; only {:?} do", TREE_POOLS)
}

/// One subject's note-commitment-tree roots at a tick, per shielded pool.
///
/// - Unreported = subject keeps no trees → [`require`](Self::require) panics rather
///   than answering `None` (same contract as [`Work::require`](crate::sync::Work::require))
/// - Reported-but-`None` = a real observation: empty pool, or a shard tree still
///   incomplete mid-scan
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TreeRoots {
    per_pool: Option<[Option<TreeRoot>; TREE_POOLS.len()]>,
}

impl TreeRoots {
    /// Subject maintaining no note commitment trees; the default
    pub const UNREPORTED: Self = Self { per_pool: None };

    /// Start a reading from a subject that *does* keep trees; pools begin absent, fill
    /// with [`with`](Self::with)
    pub fn reported() -> Self {
        Self { per_pool: Some([None; TREE_POOLS.len()]) }
    }

    /// Record `pool`'s root or its absence. Panics for a treeless pool (asking about a
    /// transparent "tree" is a bug, not a missing value)
    pub fn with(mut self, pool: Pool, root: Option<TreeRoot>) -> Self {
        let Some(i) = slot(pool) else { no_tree(pool) };
        let per_pool = self.per_pool.get_or_insert([None; TREE_POOLS.len()]);
        per_pool[i] = root;
        self
    }

    pub fn is_reported(&self) -> bool {
        self.per_pool.is_some()
    }

    /// `pool`'s root where an unreporting subject is a legitimate answer. `None` covers
    /// both "not reported" and "no root" — use only where those coincide for the caller
    pub fn get(&self, pool: Pool) -> Option<TreeRoot> {
        let i = slot(pool)?;
        self.per_pool.and_then(|roots| roots[i])
    }

    /// `pool`'s root, panicking when nothing reported trees → for a probe `Some`/`None`
    /// means has/hasn't a root at this height, never "nobody was looking"
    pub fn require(&self, pool: Pool) -> Option<TreeRoot> {
        let Some(i) = slot(pool) else { no_tree(pool) };
        match self.per_pool {
            Some(roots) => roots[i],
            None => panic!(
                "probe read the {pool:?} tree root, but this sync subject reports \
                 no note commitment trees (only a wallet subject does)"
            ),
        }
    }
}

/// Why an indexer's `GetTreeState` frontier yielded no root
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TreeRootError {
    NotHex(String),
    Malformed(String),
}

impl std::fmt::Display for TreeRootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TreeRootError::NotHex(e) => write!(f, "tree state field is not hex: {e}"),
            TreeRootError::Malformed(e) => {
                write!(f, "tree state field is not a commitment tree: {e}")
            }
        }
    }
}

impl std::error::Error for TreeRootError {}

/// Root of an indexer's `GetTreeState` frontier (`TreeState.sapling_tree`/`.orchard_tree`),
/// for comparison against a wallet's [`TreeRoots`].
///
/// - Field = hex-encoded *legacy* `CommitmentTree`; Ironwood shares Orchard's shape
/// - Empty → `Ok(None)` (no tree at that height); present-but-unparseable → error
///   (garbage and nothing are different findings)
#[cfg(feature = "librustzcash")]
pub fn commitment_tree_root(
    pool: Pool,
    frontier_hex: &str,
) -> Result<Option<TreeRoot>, TreeRootError> {
    use zcash_primitives::merkle_tree::read_commitment_tree;

    if frontier_hex.is_empty() {
        return Ok(None);
    }
    let bytes = hex::decode(frontier_hex).map_err(|e| TreeRootError::NotHex(e.to_string()))?;
    let malformed = |e: std::io::Error| TreeRootError::Malformed(e.to_string());
    let root = match pool {
        Pool::Sapling => read_commitment_tree::<
            sapling_crypto::Node,
            _,
            { sapling_crypto::NOTE_COMMITMENT_TREE_DEPTH },
        >(&bytes[..])
        .map_err(malformed)?
        .root()
        .to_bytes(),
        Pool::Orchard | Pool::Ironwood => read_commitment_tree::<
            orchard::tree::MerkleHashOrchard,
            _,
            { orchard::NOTE_COMMITMENT_TREE_DEPTH as u8 },
        >(&bytes[..])
        .map_err(malformed)?
        .root()
        .to_bytes(),
        Pool::Transparent => no_tree(pool),
    };
    Ok(Some(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unreported_is_the_default() {
        assert_eq!(TreeRoots::default(), TreeRoots::UNREPORTED);
        assert!(!TreeRoots::UNREPORTED.is_reported());
        assert_eq!(TreeRoots::UNREPORTED.get(Pool::Sapling), None);
    }

    #[test]
    #[should_panic(expected = "reports no note commitment trees")]
    fn require_panics_on_an_unreporting_subject() {
        // Point of the type: an observer must not answer a root probe with a quiet
        // `None` that reads as agreement
        TreeRoots::UNREPORTED.require(Pool::Orchard);
    }

    #[test]
    fn reported_but_empty_is_not_a_panic() {
        let roots = TreeRoots::reported();
        assert!(roots.is_reported());
        assert_eq!(roots.require(Pool::Orchard), None);
    }

    #[test]
    fn roots_round_trip_per_pool() {
        let roots = TreeRoots::reported()
            .with(Pool::Sapling, Some([1u8; 32]))
            .with(Pool::Orchard, Some([2u8; 32]));
        assert_eq!(roots.require(Pool::Sapling), Some([1u8; 32]));
        assert_eq!(roots.require(Pool::Orchard), Some([2u8; 32]));
        assert_eq!(roots.require(Pool::Ironwood), None);
    }

    #[test]
    #[should_panic(expected = "has no note commitment tree")]
    fn transparent_has_no_tree() {
        TreeRoots::reported().require(Pool::Transparent);
    }

    #[cfg(feature = "librustzcash")]
    #[test]
    fn an_empty_frontier_field_is_no_root_not_an_error() {
        assert_eq!(commitment_tree_root(Pool::Sapling, ""), Ok(None));
        assert_eq!(commitment_tree_root(Pool::Orchard, ""), Ok(None));
    }

    #[cfg(feature = "librustzcash")]
    #[test]
    fn garbage_is_an_error_not_a_silent_none() {
        assert!(matches!(commitment_tree_root(Pool::Sapling, "zz"), Err(TreeRootError::NotHex(_))));
        assert!(matches!(
            commitment_tree_root(Pool::Sapling, "00ff"),
            Err(TreeRootError::Malformed(_))
        ));
    }

    /// Whole wire path (hex + legacy `CommitmentTree` framing + root hash) against the
    /// one authority that cannot disagree with itself. Empty tree specifically = what
    /// both sides report before a pool's first output, so block one starts here
    #[cfg(feature = "librustzcash")]
    #[test]
    fn a_serialized_tree_round_trips_to_its_own_root() {
        use zcash_primitives::merkle_tree::write_commitment_tree;

        let empty = sapling_crypto::CommitmentTree::empty();
        let mut bytes = Vec::new();
        write_commitment_tree(&empty, &mut bytes).expect("serialize the empty sapling tree");
        assert_eq!(
            commitment_tree_root(Pool::Sapling, &hex::encode(&bytes)),
            Ok(Some(empty.root().to_bytes()))
        );
    }
}
