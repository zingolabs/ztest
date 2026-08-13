//! Immutable per-tick [`Snapshot`] + bounded [`History`].
//!
//! - One snapshot/tick from the subject's [`ProgressView`](crate::sync::ProgressView), with
//!   history-derived fields folded in (prev height/outputs, deepest reorg, last progress)
//! - Probe = pure predicate over one immutable value; probes due at a tick share it
//!   (design §"Snapshot-then-evaluate")

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::handles::wallet::{Pool, PoolBalances};

use super::subject::{Phase, ProgressView};
use super::tree::{TreeRoot, TreeRoots};
use super::work::{Rate, Work};

/// One immutable observation at a tick; probe predicates read this and nothing else
/// (RPC-backed probes also get a [`SyncCtx`](crate::sync::SyncCtx)).
///
/// - `work` cumulative → `(work - prev_work) / since_prev` = every throughput number
/// - `max_height_seen > height` = reorg rolled back
/// - Wallet extras `None`/[`TreeRoots::UNREPORTED`] on observers ([`ProgressView::balances`])
#[derive(Clone, Debug)]
pub struct Snapshot {
    seq: u64,
    at: Instant,
    height: u32,
    prev_height: u32,
    target: Option<u32>,
    pct: f32,
    phase: Phase,
    work: Work,
    prev_work: Work,
    since_prev: Duration,
    balances: Option<PoolBalances>,
    prev_balances: Option<PoolBalances>,
    tree_roots: TreeRoots,
    max_height_seen: u32,
    last_progress_at: Instant,
    observed_reorg: bool,
    observed_reconnect: bool,
    last_fault_at: Option<Instant>,
}

impl Snapshot {
    pub fn seq(&self) -> u64 {
        self.seq
    }
    pub fn height(&self) -> u32 {
        self.height
    }
    /// Height at the previous tick (`height` itself at `seq == 0`)
    pub fn prev_height(&self) -> u32 {
        self.prev_height
    }
    pub fn target(&self) -> Option<u32> {
        self.target
    }
    /// Fraction complete, `0.0..=100.0`
    pub fn pct(&self) -> f32 {
        self.pct
    }
    pub fn phase(&self) -> Phase {
        self.phase
    }
    /// Rollback from the deepest height seen, `0` = no reorg (probes compare against the
    /// Zcash rollback bound)
    pub fn reorg_depth(&self) -> u32 {
        self.max_height_seen.saturating_sub(self.height)
    }
    pub fn work(&self) -> Work {
        self.work
    }
    pub fn work_done(&self) -> Work {
        self.work.delta(&self.prev_work)
    }
    pub fn rate(&self) -> Rate {
        self.work_done().rate(self.since_prev)
    }
    /// Cumulative protocol work at the previous tick. Carries which ops were measured (with
    /// [`work`](Self::work)), so reading an uncounted op panics via [`Work::require`]
    /// instead of comparing two zeroes and passing
    pub fn prev_work(&self) -> Work {
        self.prev_work
    }
    /// Height increased within the last `window`?
    pub fn progressed_within(&self, window: Duration) -> bool {
        self.at.duration_since(self.last_progress_at) <= window
    }
    /// Height increased since the last injected fault? `true` with no fault yet
    pub fn progressed_since_fault(&self) -> bool {
        match self.last_fault_at {
            Some(fault_at) => self.last_progress_at >= fault_at,
            None => true,
        }
    }
    /// Coverage: reorg observed at or before this tick
    pub fn observed_reorg(&self) -> bool {
        self.observed_reorg
    }
    /// Coverage: reconnect after a dropped link observed
    pub fn observed_reconnect(&self) -> bool {
        self.observed_reconnect
    }

    /// Confirmed per-pool balances; panics when the subject reports none (balance probe on a
    /// non-wallet subject = test bug, and zeroed [`PoolBalances`] would be unfailable).
    /// [`try_balances`](Self::try_balances) where absence is legitimate
    pub fn balances(&self) -> PoolBalances {
        self.balances.unwrap_or_else(|| missing_balances("balances"))
    }
    /// Balances at the previous tick (this tick's at `seq == 0` → opening tick never reads
    /// as a change). Panics on [`balances`](Self::balances)' terms
    pub fn prev_balances(&self) -> PoolBalances {
        self.prev_balances.unwrap_or_else(|| missing_balances("prev_balances"))
    }
    /// Balances, or `None` when this subject holds no funds
    pub fn try_balances(&self) -> Option<PoolBalances> {
        self.balances
    }
    /// `pool`'s note-commitment-tree root; panics only when the subject maintains no trees.
    /// `None` = real observation (pool empty at this height, or shard tree incomplete mid-scan)
    pub fn tree_root(&self, pool: Pool) -> Option<TreeRoot> {
        self.tree_roots.require(pool)
    }
    pub fn tree_roots(&self) -> TreeRoots {
        self.tree_roots
    }
}

fn missing_balances(accessor: &str) -> ! {
    panic!(
        "probe read `Snapshot::{accessor}`, but this sync subject reports no \
         balances (only a wallet subject does)"
    )
}

/// Rolling state threaded across ticks to build each [`Snapshot`]. Separate so the snapshot
/// stays a plain immutable value
#[derive(Debug)]
pub(crate) struct SnapshotBuilder {
    seq: u64,
    prev_height: u32,
    prev_work: Work,
    prev_balances: Option<PoolBalances>,
    prev_at: Instant,
    max_height_seen: u32,
    last_progress_at: Instant,
    observed_reorg: bool,
    observed_reconnect: bool,
}

impl SnapshotBuilder {
    pub(crate) fn new(started_at: Instant) -> Self {
        Self {
            seq: 0,
            prev_height: 0,
            prev_work: Work::ZERO,
            prev_balances: None,
            prev_at: started_at,
            max_height_seen: 0,
            last_progress_at: started_at,
            observed_reorg: false,
            observed_reconnect: false,
        }
    }

    /// Fold one reading captured at `now` into a snapshot, advancing the rolling state.
    /// `last_fault_at` comes from the fault timeline
    pub(crate) fn build<P: ProgressView>(
        &mut self,
        p: &P,
        now: Instant,
        work: Work,
        last_fault_at: Option<Instant>,
    ) -> Snapshot {
        let height = p.height();
        let balances = p.balances();
        if self.seq == 0 {
            self.prev_height = height;
            // Opening readings are baselines, not this run's change: a subject resuming from
            // a seeded datadir opens with its whole balance & millions of outputs behind it
            self.prev_balances = balances;
            self.prev_work = work;
            self.prev_at = now;
            self.max_height_seen = height;
            self.last_progress_at = now;
        }
        if height > self.prev_height {
            self.last_progress_at = now;
        }
        // Below the deepest height seen = rollback (reorg)
        if height < self.max_height_seen {
            self.observed_reorg = true;
        }

        let snap = Snapshot {
            seq: self.seq,
            at: now,
            height,
            prev_height: self.prev_height,
            target: p.target(),
            pct: p.pct(),
            phase: p.phase(),
            work,
            prev_work: self.prev_work,
            since_prev: now.saturating_duration_since(self.prev_at),
            balances,
            prev_balances: self.prev_balances,
            tree_roots: p.tree_roots(),
            max_height_seen: self.max_height_seen.max(height),
            last_progress_at: self.last_progress_at,
            observed_reorg: self.observed_reorg,
            observed_reconnect: self.observed_reconnect,
            last_fault_at,
        };

        self.seq += 1;
        self.prev_height = height;
        self.prev_work = work;
        self.prev_balances = balances;
        self.prev_at = now;
        self.max_height_seen = self.max_height_seen.max(height);
        snap
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }
}

/// Bounded snapshot ring. `sometimes` probes replay the whole history at end of run,
/// `always`/`eventually` read the latest; cap bounds a runaway cadence
#[derive(Debug)]
pub struct History {
    buf: VecDeque<Arc<Snapshot>>,
    cap: usize,
    dropped: u64,
}

impl History {
    pub(crate) fn new(cap: usize) -> Self {
        Self { buf: VecDeque::new(), cap, dropped: 0 }
    }

    pub(crate) fn push(&mut self, snap: Arc<Snapshot>) {
        if self.buf.len() == self.cap {
            self.buf.pop_front();
            self.dropped += 1;
        }
        self.buf.push_back(snap);
    }

    pub fn latest(&self) -> Option<&Arc<Snapshot>> {
        self.buf.back()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<Snapshot>> {
        self.buf.iter()
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}
