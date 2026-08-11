//! Tier-A [`Work`] derived from the chain itself, via `CompactBlock`'s
//! `chainMetadata` commitment-tree sizes.
//!
//! Those three fields are cumulative as of the end of each block, so the
//! shielded work in any range is a subtraction of two of them — exact, two
//! RPCs, no scan, and no cache. The prefix-sum index this would otherwise
//! require is already on the wire, served by anything speaking the lightwallet
//! protocol, which is what makes the metric backend-agnostic without asking a
//! single component to expose a ztest-shaped counter.
//!
//! ## Why this is stateful
//!
//! A server predating these fields returns `0` for all three at every height,
//! and a flat zero is indistinguishable from a genuinely quiet pool if you look
//! at one number. Reading it as "no work" would paint a flat zero line through
//! a healthy sync — the most misleading thing this module could do.
//!
//! Guessing from an activation height does not work either: on regtest Sapling
//! activates at height 1 and a chain may legitimately carry no Sapling output
//! for its whole life, so "past activation and still zero" would condemn a
//! correct server.
//!
//! The sound test is a **contradiction inside a single block**: a block whose
//! own `vtx` carry N > 0 outputs of a pool must report a tree size ≥ N for that
//! pool. Seeing items alongside a zero tree size proves the field is
//! unpopulated, with no false positives and no knowledge of activation heights.
//! [`ChainWork`] latches that observation per pool, and the blocks it needs are
//! the ones the runner already fetches.

use crate::RpcError;
use crate::handles::indexer::{BlockHeight, CompactBlock, IndexerBackend};

use super::work::{Op, Work};

/// What is known about a server's `chainMetadata` support for one pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Support {
    /// No block has yet proven the field either way — every one seen so far
    /// reported a zero tree size and carried no items of this pool, which is
    /// exactly what a correct server says about a pool with no activity.
    #[default]
    Unproven,
    /// A nonzero tree size was reported: the field is populated.
    Populated,
    /// A block carried items of this pool while reporting a zero tree size.
    /// The field is not populated and its value must not be believed.
    Unpopulated,
}

/// Tracks `chainMetadata` support while turning fetched blocks into cumulative
/// [`Work`].
///
/// Long-lived: one per sync, folded across every block the runner reads, so a
/// server that only reveals itself at the first shielded block is still caught.
#[derive(Clone, Debug, Default)]
pub struct ChainWork {
    sapling: Support,
    orchard: Support,
    ironwood: Support,
}

/// One pool's reading out of a block: its tree size and how many items of that
/// pool the block itself carried.
struct Reading {
    tree_size: u32,
    items: usize,
}

impl ChainWork {
    /// A fresh tracker that has proven nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold `block` in, returning the cumulative work it reports.
    ///
    /// An op whose support has been disproven is left **unmeasured** rather
    /// than zero, so every consumer downstream renders it as `—`. An unproven
    /// op reports its value: the common case by far is a pool with no activity
    /// yet (regtest before any shielded transaction, Ironwood before NU6.3),
    /// and answering `—` there would be wrong in the ordinary case to guard
    /// against the rare one. If the server is in fact broken, the first block
    /// carrying items flips the latch and the row corrects itself.
    pub fn observe(&mut self, block: &CompactBlock) -> Work {
        let (sapling, orchard, ironwood) = readings(block);
        let mut work = Work::ZERO;
        for (support, reading, op) in [
            (&mut self.sapling, sapling, Op::SaplingOutput),
            (&mut self.orchard, orchard, Op::OrchardAction),
            (&mut self.ironwood, ironwood, Op::IronwoodAction),
        ] {
            match support.update(&reading) {
                Support::Unpopulated => {}
                _ => {
                    work.set(op, u64::from(reading.tree_size));
                }
            }
        }
        work
    }

    /// Fetch the block at `height` and fold it in.
    pub async fn observe_at<I>(&mut self, ix: &I, height: BlockHeight) -> Result<Work, RpcError>
    where
        I: IndexerBackend + ?Sized,
    {
        let block = ix.get_block(height).await?;
        Ok(self.observe(&block))
    }

    /// Support for `op`. Ops not carried by `chainMetadata` are always
    /// [`Support::Unpopulated`] here — this module does not derive them.
    pub fn support(&self, op: Op) -> Support {
        match op {
            Op::SaplingOutput => self.sapling,
            Op::OrchardAction => self.orchard,
            Op::IronwoodAction => self.ironwood,
            _ => Support::Unpopulated,
        }
    }

    /// Ops proven unpopulated, for the one-time diagnostic the runner logs when
    /// a row turns into `—` so the change is explained rather than mysterious.
    pub fn unpopulated(&self) -> Vec<Op> {
        [Op::SaplingOutput, Op::OrchardAction, Op::IronwoodAction]
            .into_iter()
            .filter(|&op| self.support(op) == Support::Unpopulated)
            .collect()
    }
}

impl Support {
    /// Advance on one block's evidence, returning the new state.
    ///
    /// Both transitions are one-way. `Populated` is proof the field works and
    /// cannot be undone by a later quiet block; `Unpopulated` is proof it does
    /// not and must not be undone by a later block that happens to be empty.
    fn update(&mut self, reading: &Reading) -> Support {
        *self = match (*self, reading.tree_size, reading.items) {
            (Support::Unpopulated, _, _) => Support::Unpopulated,
            (_, size, _) if size > 0 => Support::Populated,
            (Support::Populated, _, _) => Support::Populated,
            (_, 0, items) if items > 0 => Support::Unpopulated,
            _ => Support::Unproven,
        };
        *self
    }
}

/// The `(sapling, orchard, ironwood)` readings for a block: each pool's
/// reported tree size paired with the count of that pool's items actually
/// present in the block, which is what makes the contradiction detectable.
///
/// An absent `chainMetadata` reads as zero sizes, which is correct — a server
/// that omits the message entirely is exactly a server that does not populate
/// it, and it lands in the same latch.
fn readings(block: &CompactBlock) -> (Reading, Reading, Reading) {
    let meta = block.chain_metadata.as_ref();
    let size = |f: fn(&crate::proto::ChainMetadata) -> u32| meta.map_or(0, f);
    let count = |f: fn(&crate::proto::CompactTx) -> usize| block.vtx.iter().map(f).sum();
    (
        Reading {
            tree_size: size(|m| m.sapling_commitment_tree_size),
            items: count(|tx| tx.outputs.len()),
        },
        Reading {
            tree_size: size(|m| m.orchard_commitment_tree_size),
            items: count(|tx| tx.actions.len()),
        },
        Reading {
            tree_size: size(|m| m.ironwood_commitment_tree_size),
            items: count(|tx| tx.ironwood_actions.len()),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        ChainMetadata, CompactOrchardAction, CompactSaplingOutput, CompactTx as PbTx,
    };

    /// A block reporting `(sapling, orchard, ironwood)` tree sizes and carrying
    /// `(sapling_outputs, orchard_actions)` items of its own.
    fn block(sizes: (u32, u32, u32), items: (usize, usize)) -> CompactBlock {
        CompactBlock {
            chain_metadata: Some(ChainMetadata {
                sapling_commitment_tree_size: sizes.0,
                orchard_commitment_tree_size: sizes.1,
                ironwood_commitment_tree_size: sizes.2,
            }),
            vtx: vec![PbTx {
                outputs: vec![CompactSaplingOutput::default(); items.0],
                actions: vec![CompactOrchardAction::default(); items.1],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn a_populated_block_reports_its_tree_sizes_as_cumulative_work() {
        let mut cw = ChainWork::new();
        let w = cw.observe(&block((1_200, 340, 0), (2, 1)));
        assert_eq!(w.get(Op::SaplingOutput), Some(1_200));
        assert_eq!(w.get(Op::OrchardAction), Some(340));
        assert_eq!(w.get(Op::IronwoodAction), Some(0));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Populated);
    }

    /// The core detection. A block carrying outputs while reporting a zero tree
    /// size proves the field is not populated — the only sound signal available
    /// without knowing activation heights.
    #[test]
    fn items_alongside_a_zero_tree_size_prove_the_field_is_unpopulated() {
        let mut cw = ChainWork::new();
        let w = cw.observe(&block((0, 0, 0), (5, 3)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Unpopulated);
        assert_eq!(cw.support(Op::OrchardAction), Support::Unpopulated);
        assert_eq!(
            w.get(Op::SaplingOutput),
            None,
            "must not report a false zero"
        );
        assert_eq!(w.get(Op::OrchardAction), None);
    }

    /// The regression this module exists to prevent: a quiet pool on a correct
    /// server is a real zero, not a broken field. Regtest activates Sapling at
    /// height 1 and may never carry a shielded output, so any activation-height
    /// heuristic would condemn it.
    #[test]
    fn a_quiet_pool_on_a_correct_server_is_a_measured_zero() {
        let mut cw = ChainWork::new();
        let w = cw.observe(&block((0, 0, 0), (0, 0)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Unproven);
        assert_eq!(w.get(Op::SaplingOutput), Some(0));
    }

    /// A broken server is only revealed by the first block with items. Until
    /// then it is indistinguishable from a quiet chain, so the latch has to
    /// flip mid-run and the row has to correct itself.
    #[test]
    fn support_is_disproven_the_moment_the_first_item_arrives() {
        let mut cw = ChainWork::new();
        assert_eq!(
            cw.observe(&block((0, 0, 0), (0, 0))).get(Op::SaplingOutput),
            Some(0)
        );
        assert_eq!(
            cw.observe(&block((0, 0, 0), (4, 0))).get(Op::SaplingOutput),
            None
        );
        assert_eq!(cw.unpopulated(), vec![Op::SaplingOutput]);
    }

    /// Once proven populated, a later empty block must not undo the proof —
    /// most blocks carry no shielded activity at all, so a retractable latch
    /// would flap on every quiet block.
    #[test]
    fn a_later_empty_block_does_not_retract_proof_of_support() {
        let mut cw = ChainWork::new();
        cw.observe(&block((1_200, 0, 0), (2, 0)));
        let w = cw.observe(&block((1_200, 0, 0), (0, 0)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Populated);
        assert_eq!(w.get(Op::SaplingOutput), Some(1_200));
    }

    /// Nor may a later empty block rehabilitate a server already caught lying.
    #[test]
    fn a_later_empty_block_does_not_rehabilitate_a_disproven_field() {
        let mut cw = ChainWork::new();
        cw.observe(&block((0, 0, 0), (4, 0)));
        let w = cw.observe(&block((0, 0, 0), (0, 0)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Unpopulated);
        assert_eq!(w.get(Op::SaplingOutput), None);
    }

    /// Support is per pool: an Orchard-serving server that predates Ironwood is
    /// the ordinary case during the NU6.3 rollout and must not lose Orchard.
    #[test]
    fn support_is_tracked_independently_per_pool() {
        let mut cw = ChainWork::new();
        let w = cw.observe(&block((1_200, 340, 0), (0, 0)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Populated);
        assert_eq!(cw.support(Op::IronwoodAction), Support::Unproven);
        assert_eq!(w.get(Op::IronwoodAction), Some(0));
    }

    /// A server omitting the message entirely is exactly a server that does not
    /// populate it, and must land in the same latch rather than panicking or
    /// reporting nothing.
    #[test]
    fn an_absent_chain_metadata_is_treated_as_a_zero_reading() {
        let mut cw = ChainWork::new();
        let bare = CompactBlock {
            chain_metadata: None,
            vtx: vec![PbTx {
                outputs: vec![CompactSaplingOutput::default(); 2],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(cw.observe(&bare).get(Op::SaplingOutput), None);
        assert_eq!(cw.support(Op::SaplingOutput), Support::Unpopulated);
    }

    /// The whole point of cumulative tree sizes: work over a range is one
    /// subtraction, and it stays exact across ticks.
    #[test]
    fn work_over_a_range_is_the_difference_of_two_readings() {
        let mut cw = ChainWork::new();
        let base = cw.observe(&block((1_000, 200, 0), (1, 1)));
        let head = cw.observe(&block((1_450, 275, 12), (1, 1)));
        let done = head.delta(&base);
        assert_eq!(done.get(Op::SaplingOutput), Some(450));
        assert_eq!(done.get(Op::OrchardAction), Some(75));
        assert_eq!(done.get(Op::IronwoodAction), Some(12));
    }

    /// Tier-B ops are not derivable from a tree size, and must report as such
    /// rather than as a silent zero.
    #[test]
    fn tier_b_ops_are_not_derived_here() {
        let mut cw = ChainWork::new();
        let w = cw.observe(&block((1_200, 340, 0), (2, 1)));
        assert_eq!(w.get(Op::TransparentIn), None);
        assert_eq!(w.get(Op::SaplingSpend), None);
        assert_eq!(cw.support(Op::TransparentIn), Support::Unpopulated);
    }
}
