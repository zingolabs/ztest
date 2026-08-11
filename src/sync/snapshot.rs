//! The immutable per-tick [`Snapshot`] and the bounded [`History`].
//!
//! The runner captures one `Snapshot` per tick from the subject's raw
//! [`ProgressView`](crate::sync::ProgressView), folding in history-derived
//! fields (previous height/outputs, deepest reorg, time of last progress) so a
//! probe is a pure predicate over a single immutable value. Probes due at the
//! same tick share the one snapshot (design §"Snapshot-then-evaluate").

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::handles::wallet::{Pool, PoolBalances};

use super::subject::{Phase, ProgressView};
use super::tree::{TreeRoot, TreeRoots};
use super::work::{Rate, Work};

/// One immutable observation of the sync at a tick. All probe predicates read
/// this and nothing else (RPC-backed probes additionally get a
/// [`SyncCtx`](crate::sync::SyncCtx)).
#[derive(Clone, Debug)]
pub struct Snapshot {
    seq: u64,
    at: Instant,
    height: u32,
    prev_height: u32,
    target: Option<u32>,
    pct: f32,
    phase: Phase,
    /// Cumulative protocol work at this tick, and at the previous one. Their
    /// difference is the work this tick actually did, which — divided by
    /// `since_prev` — is every throughput number the harness reports.
    work: Work,
    prev_work: Work,
    /// Interval since the previous snapshot, measured on the driver's clock.
    since_prev: Duration,
    /// Wallet extras: confirmed balances at this tick and the previous one, and
    /// the note-commitment-tree roots. `None`/[`TreeRoots::UNREPORTED`] for
    /// every observer subject; see [`ProgressView::balances`].
    balances: Option<PoolBalances>,
    prev_balances: Option<PoolBalances>,
    tree_roots: TreeRoots,
    /// Deepest height reached so far; `> height` means a reorg rolled back.
    max_height_seen: u32,
    /// When height last increased — the basis for [`progressed_within`].
    last_progress_at: Instant,
    observed_reorg: bool,
    observed_reconnect: bool,
    last_fault_at: Option<Instant>,
}

impl Snapshot {
    /// Sequence number (0-based tick index).
    pub fn seq(&self) -> u64 {
        self.seq
    }
    /// Current synced height.
    pub fn height(&self) -> u32 {
        self.height
    }
    /// Height at the previous tick (`height` at `seq == 0`).
    pub fn prev_height(&self) -> u32 {
        self.prev_height
    }
    /// Target tip, if known.
    pub fn target(&self) -> Option<u32> {
        self.target
    }
    /// Fraction complete, `0.0..=100.0`.
    pub fn pct(&self) -> f32 {
        self.pct
    }
    /// The live phase.
    pub fn phase(&self) -> Phase {
        self.phase
    }
    /// How far the tip rolled back from the deepest height seen (`0` = no
    /// reorg). A probe compares this against the Zcash rollback bound.
    pub fn reorg_depth(&self) -> u32 {
        self.max_height_seen.saturating_sub(self.height)
    }
    /// Cumulative protocol work as of this tick.
    pub fn work(&self) -> Work {
        self.work
    }
    /// Work completed since the previous tick.
    pub fn work_done(&self) -> Work {
        self.work.delta(&self.prev_work)
    }
    /// Per-second work rate over the interval since the previous tick.
    pub fn rate(&self) -> Rate {
        self.work_done().rate(self.since_prev)
    }
    /// Cumulative protocol work at the previous tick.
    ///
    /// Paired with [`work`](Self::work) for probes that compare across ticks.
    /// Both carry which ops were measured, so a predicate reading an op nobody
    /// counted panics via [`Work::require`] rather than silently comparing two
    /// zeroes and passing.
    pub fn prev_work(&self) -> Work {
        self.prev_work
    }
    /// Whether height increased within the last `window`.
    pub fn progressed_within(&self, window: Duration) -> bool {
        self.at.duration_since(self.last_progress_at) <= window
    }
    /// Whether height increased since the most recent injected fault. `true`
    /// when no fault has occurred (nothing to recover from).
    pub fn progressed_since_fault(&self) -> bool {
        match self.last_fault_at {
            Some(fault_at) => self.last_progress_at >= fault_at,
            None => true,
        }
    }
    /// Coverage: a reorg was observed at or before this tick.
    pub fn observed_reorg(&self) -> bool {
        self.observed_reorg
    }
    /// Coverage: a reconnect after a dropped link was observed.
    pub fn observed_reconnect(&self) -> bool {
        self.observed_reconnect
    }

    /// Confirmed per-pool balances at this tick, panicking when the subject
    /// reports none.
    ///
    /// Carries [`Work::require`](crate::sync::Work::require)'s contract: a
    /// balance probe pointed at a non-wallet subject is a bug in the *test*,
    /// and answering it with a zeroed [`PoolBalances`] would make that probe
    /// unfailable — green forever, checking nothing. Use
    /// [`try_balances`](Self::try_balances) where absence is a legitimate
    /// answer.
    pub fn balances(&self) -> PoolBalances {
        self.balances
            .unwrap_or_else(|| missing_balances("balances"))
    }
    /// Confirmed per-pool balances at the previous tick (this tick's at
    /// `seq == 0`, so the opening tick never reads as a change). Paired with
    /// [`balances`](Self::balances) for probes that compare across ticks, and
    /// panicking on the same terms.
    pub fn prev_balances(&self) -> PoolBalances {
        self.prev_balances
            .unwrap_or_else(|| missing_balances("prev_balances"))
    }
    /// Confirmed per-pool balances, or `None` when this subject holds no funds.
    pub fn try_balances(&self) -> Option<PoolBalances> {
        self.balances
    }
    /// `pool`'s note-commitment-tree root at this tick, panicking when the
    /// subject maintains no trees.
    ///
    /// `None` is a real observation — the pool is empty at this height, or the
    /// wallet's shard tree is still incomplete there mid-scan — which is why
    /// only the *unreported* case panics. See [`TreeRoots`].
    pub fn tree_root(&self, pool: Pool) -> Option<TreeRoot> {
        self.tree_roots.require(pool)
    }
    /// Every pool's root at this tick, including whether any were reported.
    pub fn tree_roots(&self) -> TreeRoots {
        self.tree_roots
    }
}

/// Panic message for a balance read on a subject that holds no funds.
fn missing_balances(accessor: &str) -> ! {
    panic!(
        "probe read `Snapshot::{accessor}`, but this sync subject reports no \
         balances (only a wallet subject does)"
    )
}

/// Rolling state the runner threads across ticks to build each [`Snapshot`].
/// Kept separate so the snapshot itself stays a plain immutable value.
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

    /// Fold one raw progress reading (captured at `now`) into a new snapshot and
    /// advance the rolling state. `last_fault_at` comes from the (step-5) fault
    /// timeline; `None` today.
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
            // Same reasoning as `prev_work`: the opening reading is a baseline,
            // not a change this run produced. A wallet resuming from a seeded
            // datadir opens with its whole balance already there, and calling
            // that a one-tick gain would misreport every delta probe.
            self.prev_balances = balances;
            // The first tick's cumulative reading is a baseline, not work this
            // run performed: a subject resuming from a seeded datadir starts
            // with millions of outputs already behind it, and counting them as
            // one tick's work would report an absurd opening rate.
            self.prev_work = work;
            self.prev_at = now;
            self.max_height_seen = height;
            self.last_progress_at = now;
        }
        if height > self.prev_height {
            self.last_progress_at = now;
        }
        // A drop below the deepest height seen is a rollback (reorg).
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

    /// Number of snapshots built so far (the tick count).
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }
}

/// A bounded ring buffer of snapshots. `sometimes` (coverage) probes replay
/// over the whole history at the end of the run; `always`/`eventually` read the
/// latest. ~11k snapshots at a 15 s cadence over 48 h is trivial; the cap keeps
/// a runaway cadence from growing unbounded.
#[derive(Debug)]
pub struct History {
    buf: VecDeque<Arc<Snapshot>>,
    cap: usize,
    dropped: u64,
}

impl History {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            buf: VecDeque::new(),
            cap,
            dropped: 0,
        }
    }

    pub(crate) fn push(&mut self, snap: Arc<Snapshot>) {
        if self.buf.len() == self.cap {
            self.buf.pop_front();
            self.dropped += 1;
        }
        self.buf.push_back(snap);
    }

    /// The most recent snapshot, if any.
    pub fn latest(&self) -> Option<&Arc<Snapshot>> {
        self.buf.back()
    }

    /// Iterate retained snapshots oldest-first.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<Snapshot>> {
        self.buf.iter()
    }

    /// Number of retained snapshots.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether no snapshots are retained.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// How many snapshots were evicted by the cap (a visible truncation, not
    /// silent — the reporter surfaces it).
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}
