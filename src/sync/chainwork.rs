//! Tier-A [`Work`] from the chain itself: `CompactBlock`'s `chainMetadata`
//! commitment-tree sizes.
//!
//! - Cumulative per block → range work = one subtraction (2 RPCs, no scan, no cache);
//!   the prefix-sum index is already on the wire for anything speaking lightwallet
//! - Stateful because a pre-`chainMetadata` server reports flat `0`, indistinguishable
//!   from a quiet pool; activation heights can't decide it either (regtest activates
//!   Sapling at height 1 and may never carry an output)
//! - Sound signal = contradiction inside one block: N > 0 items of a pool ⟹ tree size
//!   ≥ N. [`ChainWork`] latches that per pool, over blocks the runner already fetches

use crate::RpcError;
use crate::handles::indexer::{BlockHeight, CompactBlock, IndexerBackend};

use super::work::{Op, Work};

/// A server's `chainMetadata` support for one pool.
///
/// - `Unproven` = zero tree size & no items so far (also what a correct server says
///   about a quiet pool)
/// - `Unpopulated` = the contradiction (items + zero tree size) → stop believing the field
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Support {
    #[default]
    Unproven,
    Populated,
    Unpopulated,
}

/// `chainMetadata` support latch + fetched blocks → cumulative [`Work`].
///
/// One per sync, folded over every block read (a server only reveals itself at the
/// first shielded block)
#[derive(Clone, Debug, Default)]
pub struct ChainWork {
    sapling: Support,
    orchard: Support,
    ironwood: Support,
}

/// One pool's reading out of a block: reported tree size vs items actually carried
struct Reading {
    tree_size: u32,
    items: usize,
}

impl ChainWork {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold `block` in → the cumulative work it reports.
    ///
    /// - Disproven op = **unmeasured**, never zero
    /// - Unproven op still reports (quiet pool = the ordinary case); the first block
    ///   with items flips the latch and the row corrects itself
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

    /// Fetch the block at `height` and fold it in
    pub async fn observe_at<I>(&mut self, ix: &I, height: BlockHeight) -> Result<Work, RpcError>
    where
        I: IndexerBackend + ?Sized,
    {
        let block = ix.get_block(height).await?;
        Ok(self.observe(&block))
    }

    /// Ops outside `chainMetadata` read [`Support::Unpopulated`] (not derived here)
    pub fn support(&self, op: Op) -> Support {
        match op {
            Op::SaplingOutput => self.sapling,
            Op::OrchardAction => self.orchard,
            Op::IronwoodAction => self.ironwood,
            _ => Support::Unpopulated,
        }
    }

    /// Ops proven unpopulated, for the runner's one-time `—` diagnostic
    pub fn unpopulated(&self) -> Vec<Op> {
        [Op::SaplingOutput, Op::OrchardAction, Op::IronwoodAction]
            .into_iter()
            .filter(|&op| self.support(op) == Support::Unpopulated)
            .collect()
    }
}

impl Support {
    /// Advance on one block's evidence. Both transitions one-way — a later empty block
    /// undoes neither `Populated` nor `Unpopulated`
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

/// `(sapling, orchard, ironwood)` readings: reported tree size + items present (the
/// pairing that makes the contradiction detectable). Absent `chainMetadata` reads as
/// zero sizes → an omitting server lands in the same latch
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

    /// `(sapling, orchard, ironwood)` tree sizes + `(sapling_outputs, orchard_actions)` items
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

    /// Core detection: outputs + zero tree size ⟹ unpopulated (the only sound signal
    /// available without activation heights)
    #[test]
    fn items_alongside_a_zero_tree_size_prove_the_field_is_unpopulated() {
        let mut cw = ChainWork::new();
        let w = cw.observe(&block((0, 0, 0), (5, 3)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Unpopulated);
        assert_eq!(cw.support(Op::OrchardAction), Support::Unpopulated);
        assert_eq!(w.get(Op::SaplingOutput), None, "must not report a false zero");
        assert_eq!(w.get(Op::OrchardAction), None);
    }

    /// The regression this module prevents: a quiet pool = a real zero, not a broken
    /// field (regtest activates Sapling at height 1 and may never carry an output)
    #[test]
    fn a_quiet_pool_on_a_correct_server_is_a_measured_zero() {
        let mut cw = ChainWork::new();
        let w = cw.observe(&block((0, 0, 0), (0, 0)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Unproven);
        assert_eq!(w.get(Op::SaplingOutput), Some(0));
    }

    /// Broken server indistinguishable from a quiet chain until the first block with
    /// items → the latch must flip mid-run and the row correct itself
    #[test]
    fn support_is_disproven_the_moment_the_first_item_arrives() {
        let mut cw = ChainWork::new();
        assert_eq!(cw.observe(&block((0, 0, 0), (0, 0))).get(Op::SaplingOutput), Some(0));
        assert_eq!(cw.observe(&block((0, 0, 0), (4, 0))).get(Op::SaplingOutput), None);
        assert_eq!(cw.unpopulated(), vec![Op::SaplingOutput]);
    }

    /// Retractable proof would flap on every quiet block (most carry no shielded activity)
    #[test]
    fn a_later_empty_block_does_not_retract_proof_of_support() {
        let mut cw = ChainWork::new();
        cw.observe(&block((1_200, 0, 0), (2, 0)));
        let w = cw.observe(&block((1_200, 0, 0), (0, 0)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Populated);
        assert_eq!(w.get(Op::SaplingOutput), Some(1_200));
    }

    /// Nor may an empty block rehabilitate a server already caught lying
    #[test]
    fn a_later_empty_block_does_not_rehabilitate_a_disproven_field() {
        let mut cw = ChainWork::new();
        cw.observe(&block((0, 0, 0), (4, 0)));
        let w = cw.observe(&block((0, 0, 0), (0, 0)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Unpopulated);
        assert_eq!(w.get(Op::SaplingOutput), None);
    }

    /// Orchard-serving but pre-Ironwood = ordinary during the NU6.3 rollout, and must
    /// not lose Orchard
    #[test]
    fn support_is_tracked_independently_per_pool() {
        let mut cw = ChainWork::new();
        let w = cw.observe(&block((1_200, 340, 0), (0, 0)));
        assert_eq!(cw.support(Op::SaplingOutput), Support::Populated);
        assert_eq!(cw.support(Op::IronwoodAction), Support::Unproven);
        assert_eq!(w.get(Op::IronwoodAction), Some(0));
    }

    /// Omitting the message == not populating it → same latch, no panic, not silence
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

    /// Point of cumulative tree sizes: range work = one subtraction, exact across ticks
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

    /// Tier-B ops aren't derivable from a tree size → must report as such, not zero
    #[test]
    fn tier_b_ops_are_not_derived_here() {
        let mut cw = ChainWork::new();
        let w = cw.observe(&block((1_200, 340, 0), (2, 1)));
        assert_eq!(w.get(Op::TransparentIn), None);
        assert_eq!(w.get(Op::SaplingSpend), None);
        assert_eq!(cw.support(Op::TransparentIn), Support::Unpopulated);
    }
}
