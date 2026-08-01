//! The probe (invariant) taxonomy and per-probe scheduling state.
//!
//! A probe is a named predicate over a [`Snapshot`], registered in one of four
//! classes (design §"the invariant taxonomy"):
//!
//! - **always** — safety, true at *every* tick; a violation is a bug.
//! - **eventually** — liveness, must (re)satisfy within a `window`; else a stall.
//! - **sometimes** — coverage, true on ≥1 tick over the run; else a weak test.
//! - **at_completion** — a post-condition evaluated once at tip.
//!
//! Predicates decouple from class/cadence/severity: the same fn can register
//! `always/Fatal/5s` in one profile and `always/Recorded/30s` in another.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::handles::indexer::IndexerBackend;

use super::snapshot::Snapshot;

/// Outcome of one probe evaluation — three meanings, not a bool (Gomega's
/// discipline): a probe that returns `Violated` is "invariant broken", one that
/// returns `ProbeError` means the harness/RPC is broken (aborts the run), one
/// that returns `Pending` is "not yet, keep going".
#[derive(Clone, Debug)]
pub enum Verdict {
    /// The invariant holds at this tick.
    Satisfied,
    /// Not yet satisfied, but not a violation — retry next tick.
    Pending,
    /// The invariant is broken.
    Violated(Violation),
    /// The probe itself failed to evaluate (RPC/harness error) — not a verdict
    /// about the subject; aborts the run.
    ProbeError(String),
}

/// A recorded invariant violation. Mirrors `loadtest::oracle::Violation`.
#[derive(Clone, Debug)]
pub struct Violation {
    /// The probe name (auto-derived or `.named()`).
    pub probe: String,
    /// Height the violation was observed at, if applicable.
    pub height: Option<u32>,
    /// Human detail.
    pub detail: String,
}

/// Whether a violation ends the run or is only recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    /// A violation stops the sync (graceful) and fails the run.
    Fatal,
    /// A violation is recorded and surfaced, but the run continues.
    Recorded,
}

/// The invariant class — selects when/how a probe is evaluated and what a
/// failure means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Safety, per-tick.
    Always,
    /// Liveness, must (re)satisfy within its `window`.
    Eventually,
    /// Coverage, checked over the whole history at the end.
    Sometimes,
    /// Terminal post-condition.
    AtCompletion,
}

/// How often a probe is evaluated. `window` belongs to `eventually` (the
/// satisfy-deadline), the rest are evaluation cadences.
#[derive(Clone, Copy, Debug)]
pub enum Cadence {
    /// Every tick.
    EachTick,
    /// On a wall-clock interval.
    Every(Duration),
    /// Every `n` blocks of height progress.
    EveryBlocks(u32),
    /// `eventually`: must be satisfied at least once per rolling `Duration`.
    Window(Duration),
}

/// Context handed to RPC-backed probes: the independent oracle (indexer) the
/// wallet's state is checked against.
pub struct SyncCtx {
    indexer: Option<Arc<dyn IndexerBackend>>,
}

impl std::fmt::Debug for SyncCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncCtx")
            .field("has_indexer", &self.indexer.is_some())
            .finish()
    }
}

impl SyncCtx {
    pub(crate) fn new(indexer: Option<Arc<dyn IndexerBackend>>) -> Self {
        Self { indexer }
    }
    /// The oracle indexer handle, if the topology bound one. RPC-backed probes
    /// (`chain_continuity`, `tree_root_matches_indexer`) read it; `None` in a
    /// walletless/observer setup.
    pub fn indexer(&self) -> Option<&dyn IndexerBackend> {
        self.indexer.as_deref()
    }
}

/// A boxed async probe body: borrows the snapshot + ctx for the future's life.
type AsyncCheck = Box<
    dyn for<'a> Fn(&'a Snapshot, &'a SyncCtx) -> Pin<Box<dyn Future<Output = Verdict> + Send + 'a>>
        + Send
        + Sync,
>;

/// The evaluable body of a probe. A pure predicate stays a sync fn; an
/// oracle-backed one is an async closure. (These can't collapse into one
/// blanket-impl `check()` — two blanket `impl`s over `F` would violate
/// coherence — so the builder exposes `check` and `check_rpc`.)
pub(crate) enum Check {
    Sync(Box<dyn Fn(&Snapshot) -> Verdict + Send + Sync>),
    Async(AsyncCheck),
}

impl Check {
    pub(crate) async fn evaluate(&self, snap: &Snapshot, cx: &SyncCtx) -> Verdict {
        match self {
            Check::Sync(f) => f(snap),
            Check::Async(f) => f(snap, cx).await,
        }
    }
}

/// One registered probe: its identity/class/cadence/severity plus the rolling
/// scheduler state the runner threads across ticks.
pub(crate) struct ProbeSpec {
    pub(crate) name: String,
    pub(crate) class: Class,
    pub(crate) severity: Severity,
    pub(crate) cadence: Cadence,
    /// Arms an `eventually` window only after this named fault fires (step 5).
    pub(crate) after: Option<String>,
    /// Debounce: a violation must persist this long before firing (quantized to
    /// the cadence as a consecutive-violation count).
    pub(crate) hold_for: Option<Duration>,
    pub(crate) check: Check,
    // ── rolling scheduler state ──
    pub(crate) last_fired_seq: Option<u64>,
    pub(crate) last_fired_height: u32,
    pub(crate) next_due: Option<tokio::time::Instant>,
    /// Consecutive due-evaluations that returned `Violated` (for `hold_for`).
    pub(crate) violation_streak: u32,
    /// `eventually`: last time the probe was satisfied (or armed).
    pub(crate) last_satisfied: Option<tokio::time::Instant>,
    /// `sometimes`: satisfied on ≥1 tick.
    pub(crate) ever_satisfied: bool,
}

impl ProbeSpec {
    /// Whether this probe is due to evaluate at `(seq, height, now)`. `always`
    /// and `eventually` honor the cadence; `sometimes`/`at_completion` are not
    /// tick-driven (evaluated at end), so report not-due here.
    pub(crate) fn due(&self, height: u32, now: tokio::time::Instant) -> bool {
        match self.class {
            Class::Sometimes | Class::AtCompletion => false,
            Class::Always | Class::Eventually => match self.cadence {
                Cadence::EachTick | Cadence::Window(_) => true,
                Cadence::Every(_) => self.next_due.is_none_or(|due| now >= due),
                Cadence::EveryBlocks(n) => {
                    self.last_fired_seq.is_none()
                        || height.saturating_sub(self.last_fired_height) >= n
                }
            },
        }
    }

    /// Record that the probe fired at `(seq, height, now)`, advancing cadence.
    pub(crate) fn mark_fired(&mut self, seq: u64, height: u32, now: tokio::time::Instant) {
        self.last_fired_seq = Some(seq);
        self.last_fired_height = height;
        if let Cadence::Every(d) = self.cadence {
            self.next_due = Some(now + d);
        }
    }

    /// The debounce threshold in consecutive violations (≥1). A bare threshold
    /// flaps on noisy sync signals; `hold_for` quantized to the cadence smooths.
    pub(crate) fn violation_threshold(&self) -> u32 {
        match (self.hold_for, self.cadence) {
            (Some(hold), Cadence::Every(d)) if !d.is_zero() => {
                (hold.as_secs_f64() / d.as_secs_f64()).ceil() as u32
            }
            _ => 1,
        }
    }
}

/// Builder for one probe registration: `run.always(Fatal).every(secs(5)).check(fn)`.
/// Cadence/naming setters chain; `check`/`check_rpc` finalize and register.
#[must_use = "a probe builder does nothing until `.check(...)` is called"]
pub struct ProbeBuilder<'r> {
    pub(crate) sink: &'r mut Vec<ProbeSpec>,
    pub(crate) class: Class,
    pub(crate) severity: Severity,
    pub(crate) cadence: Cadence,
    pub(crate) after: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) hold_for: Option<Duration>,
}

impl std::fmt::Debug for ProbeBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeBuilder")
            .field("class", &self.class)
            .field("severity", &self.severity)
            .field("cadence", &self.cadence)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<'r> ProbeBuilder<'r> {
    /// Evaluate on a wall-clock interval.
    pub fn every(mut self, period: Duration) -> Self {
        self.cadence = Cadence::Every(period);
        self
    }
    /// Evaluate every `n` blocks of height progress.
    pub fn every_blocks(mut self, n: u32) -> Self {
        self.cadence = Cadence::EveryBlocks(n);
        self
    }
    /// Evaluate at every tick.
    pub fn each_tick(mut self) -> Self {
        self.cadence = Cadence::EachTick;
        self
    }
    /// (`eventually`) require satisfaction at least once per rolling `window`.
    pub fn window(mut self, window: Duration) -> Self {
        self.cadence = Cadence::Window(window);
        self
    }
    /// Arm this probe only after the named fault fires (step-5 nemesis).
    pub fn after(mut self, fault: impl Into<String>) -> Self {
        self.after = Some(fault.into());
        self
    }
    /// Override the auto-derived name.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    /// Require a violation to persist `dur` before firing (debounce).
    pub fn hold_for(mut self, dur: Duration) -> Self {
        self.hold_for = Some(dur);
        self
    }

    /// Register a pure predicate `fn(&Snapshot) -> Verdict`.
    pub fn check<F>(self, f: F)
    where
        F: Fn(&Snapshot) -> Verdict + Send + Sync + 'static,
    {
        self.finish(Check::Sync(Box::new(f)));
    }

    /// Register an RPC/oracle-backed probe. Write it as
    /// `|s, cx| Box::pin(async move { ... })`.
    pub fn check_rpc<F>(self, f: F)
    where
        F: for<'a> Fn(
                &'a Snapshot,
                &'a SyncCtx,
            ) -> Pin<Box<dyn Future<Output = Verdict> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        self.finish(Check::Async(Box::new(f)));
    }

    fn finish(self, check: Check) {
        let name = self.name.unwrap_or_else(|| format!("probe_{}", self.sink.len()));
        self.sink.push(ProbeSpec {
            name,
            class: self.class,
            severity: self.severity,
            cadence: self.cadence,
            after: self.after,
            hold_for: self.hold_for,
            check,
            last_fired_seq: None,
            last_fired_height: 0,
            next_due: None,
            violation_streak: 0,
            last_satisfied: None,
            ever_satisfied: false,
        });
    }
}

/// Convenience constructors for durations used in cadences.
pub fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}
/// Minutes.
pub fn mins(n: u64) -> Duration {
    Duration::from_secs(n * 60)
}
/// Hours.
pub fn hours(n: u64) -> Duration {
    Duration::from_secs(n * 3600)
}

/// `ensure!`-style helper for writing pure probes: return `Violated` unless the
/// condition holds.
#[macro_export]
macro_rules! sync_ensure {
    ($cond:expr, $($arg:tt)+) => {
        if !($cond) {
            return $crate::sync::Verdict::Violated($crate::sync::Violation {
                probe: String::new(),
                height: None,
                detail: format!($($arg)+),
            });
        }
    };
}
