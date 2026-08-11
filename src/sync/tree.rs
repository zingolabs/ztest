//! Note-commitment-tree roots — the sync harness's independent-authority
//! oracle (design §"End-state correctness").
//!
//! A wallet builds its note-commitment tree from nothing but the compact blocks
//! the indexer streamed it. So if that tree hashes to the same root the indexer
//! independently serves for the same height, the indexer served a
//! self-consistent chain: in order, with no block skipped, and with every
//! shielded output in it. It is the strongest single statement the harness can
//! make about an indexer, and it costs one comparison.
//!
//! Two halves live here. [`TreeRoots`] is the per-tick reading a subject folds
//! into its [`Snapshot`](crate::sync::Snapshot) — the *wallet's* side.
//! [`commitment_tree_root`] parses the other side: the hex frontier an
//! indexer's `GetTreeState` returns.
//!
//! Both sides are parsed with the `sapling_crypto` / `orchard` versions pinned
//! in this crate's manifest, deliberately matched to zingolib `dev`'s lock. Two
//! copies of `sapling_crypto` in one graph would give two distinct `Node` types
//! whose roots could never be compared at all.

use crate::handles::wallet::Pool;

/// A 32-byte note-commitment-tree root.
pub type TreeRoot = [u8; 32];

/// The pools that have a note commitment tree, in [`TreeRoots`] slot order.
const TREE_POOLS: [Pool; 3] = [Pool::Orchard, Pool::Ironwood, Pool::Sapling];

/// The slot `pool` occupies, or `None` for a pool with no note commitment tree.
fn slot(pool: Pool) -> Option<usize> {
    TREE_POOLS.iter().position(|&p| p == pool)
}

/// Panic message for a pool that has no note commitment tree to ask about.
fn no_tree(pool: Pool) -> ! {
    panic!(
        "{pool:?} has no note commitment tree; only {:?} do",
        TREE_POOLS
    )
}

/// One subject's note-commitment-tree roots at a tick, per shielded pool.
///
/// Distinguishes *not reported* from *no root*, which are different facts and
/// must not collapse:
///
/// - **Unreported** — this subject does not maintain note commitment trees at
///   all (every observer subject: an indexer or validator being watched, which
///   has no wallet). Reading a root off one is a bug in the *test*, so
///   [`require`](Self::require) panics rather than answering `None`. Treating
///   unreported as "no root" would turn a root-comparison probe into a check
///   that can never fail — worse than no probe, because it reports green. Same
///   contract as [`Work::require`](crate::sync::Work::require).
/// - **Reported, but `None` for a pool** — a legitimate observation: the pool
///   is empty at this height, or the wallet's shard tree is still incomplete
///   there mid-scan. `None` on *both* sides is agreement, not absence of
///   evidence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TreeRoots {
    per_pool: Option<[Option<TreeRoot>; TREE_POOLS.len()]>,
}

impl TreeRoots {
    /// A subject that maintains no note commitment trees. The default.
    pub const UNREPORTED: Self = Self { per_pool: None };

    /// Start a reading from a subject that *does* maintain trees. Every pool
    /// begins absent; fill them in with [`with`](Self::with).
    pub fn reported() -> Self {
        Self {
            per_pool: Some([None; TREE_POOLS.len()]),
        }
    }

    /// Record `pool`'s root (or its absence). Panics for a pool with no note
    /// commitment tree — a caller asking about a transparent "tree" has a bug,
    /// not a missing value.
    pub fn with(mut self, pool: Pool, root: Option<TreeRoot>) -> Self {
        let Some(i) = slot(pool) else { no_tree(pool) };
        let per_pool = self.per_pool.get_or_insert([None; TREE_POOLS.len()]);
        per_pool[i] = root;
        self
    }

    /// Whether this subject reports tree roots at all.
    pub fn is_reported(&self) -> bool {
        self.per_pool.is_some()
    }

    /// `pool`'s root, where an unreporting subject is a legitimate answer.
    /// Returns `None` for both "not reported" and "no root" — use it only
    /// where those are genuinely the same to the caller.
    pub fn get(&self, pool: Pool) -> Option<TreeRoot> {
        let i = slot(pool)?;
        self.per_pool.and_then(|roots| roots[i])
    }

    /// `pool`'s root, panicking when nothing reported trees.
    ///
    /// For probe predicates: `Some`/`None` then means the pool has/hasn't a
    /// root at this height, and never "nobody was looking". See the type's
    /// docs.
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

/// Why an indexer's `GetTreeState` frontier could not be turned into a root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TreeRootError {
    /// The field was not valid hex.
    NotHex(String),
    /// The bytes were not a serialized commitment tree for this pool.
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

/// Compute the root of the commitment-tree frontier an indexer served in a
/// `GetTreeState` response (`TreeState.sapling_tree` / `.orchard_tree`), for
/// comparison against a wallet's own [`TreeRoots`].
///
/// The field is a hex-encoded *legacy* `CommitmentTree` — the same wire shape
/// lightwalletd has always served, and what zaino reproduces. An empty field is
/// `Ok(None)`: the indexer served no tree for that pool at that height (before
/// the pool's activation), which is a real answer, not a failure. A
/// present-but-unparseable field is an error, deliberately not folded into
/// `None`, because "the indexer served garbage" and "the indexer served
/// nothing" are different findings.
///
/// Ironwood shares Orchard's note-commitment-tree shape (same hash type, same
/// depth), so it parses identically.
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
        // The whole point of the type: an observer subject must not answer a
        // root probe with a quiet `None` that reads as agreement.
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
        assert!(matches!(
            commitment_tree_root(Pool::Sapling, "zz"),
            Err(TreeRootError::NotHex(_))
        ));
        assert!(matches!(
            commitment_tree_root(Pool::Sapling, "00ff"),
            Err(TreeRootError::Malformed(_))
        ));
    }

    /// Serialize a tree, parse it back through the oracle, and get the root the
    /// tree itself reports. Covers the whole wire path — hex, the legacy
    /// `CommitmentTree` framing, and the root hash — against the one authority
    /// that cannot disagree with itself.
    ///
    /// The empty tree specifically: that is what both an indexer and a
    /// freshly-synced wallet report for a pool before it has any output, so
    /// this is the case the comparison starts from at block one.
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
