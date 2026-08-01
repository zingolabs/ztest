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

use crate::handles::wallet::Pool;

use super::subject::{Phase, ProgressView};

/// `[sapling, orchard, ironwood]` output counts, indexed by [`pool_idx`].
type PoolOutputs = [u64; 3];

fn pool_idx(pool: Pool) -> Option<usize> {
    match pool {
        Pool::Sapling => Some(0),
        Pool::Orchard => Some(1),
        Pool::Ironwood => Some(2),
        Pool::Transparent => None,
    }
}

/// Total wallet balance, as the design's `s.balances().total()` surface.
#[derive(Clone, Copy, Debug)]
pub struct Balances {
    total: i64,
}

impl Balances {
    /// Total balance across pools, in zatoshis.
    pub fn total(&self) -> i64 {
        self.total
    }
}

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
    outputs: PoolOutputs,
    prev_outputs: PoolOutputs,
    balance_total: i64,
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
    /// Cumulative outputs scanned in `pool` (`0` for `Transparent`/observers).
    pub fn outputs(&self, pool: Pool) -> u64 {
        pool_idx(pool).map_or(0, |i| self.outputs[i])
    }
    /// `outputs(pool)` at the previous tick.
    pub fn prev_outputs(&self, pool: Pool) -> u64 {
        pool_idx(pool).map_or(0, |i| self.prev_outputs[i])
    }
    /// Wallet balances.
    pub fn balances(&self) -> Balances {
        Balances {
            total: self.balance_total,
        }
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
}

/// Rolling state the runner threads across ticks to build each [`Snapshot`].
/// Kept separate so the snapshot itself stays a plain immutable value.
#[derive(Debug)]
pub(crate) struct SnapshotBuilder {
    seq: u64,
    prev_height: u32,
    prev_outputs: PoolOutputs,
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
            prev_outputs: [0; 3],
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
        last_fault_at: Option<Instant>,
    ) -> Snapshot {
        let height = p.height();
        let outputs = [
            p.outputs(Pool::Sapling),
            p.outputs(Pool::Orchard),
            p.outputs(Pool::Ironwood),
        ];
        if self.seq == 0 {
            self.prev_height = height;
            self.prev_outputs = outputs;
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
            outputs,
            prev_outputs: self.prev_outputs,
            balance_total: p.balance_total(),
            max_height_seen: self.max_height_seen.max(height),
            last_progress_at: self.last_progress_at,
            observed_reorg: self.observed_reorg,
            observed_reconnect: self.observed_reconnect,
            last_fault_at,
        };

        self.seq += 1;
        self.prev_height = height;
        self.prev_outputs = outputs;
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
