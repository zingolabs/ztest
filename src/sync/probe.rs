//! Probe (invariant) taxonomy + per-probe scheduling state.
//!
//! Probe = named predicate over a [`Snapshot`] in one of four classes (design §"the
//! invariant taxonomy"):
//!
//! - **always** — safety, every tick; violation = bug
//! - **eventually** — liveness, (re)satisfy within a `window`; else stall
//! - **sometimes** — coverage, ≥1 tick over the run; else weak test
//! - **at_completion** — post-condition, once at tip
//!
//! Predicate decoupled from class/cadence/severity (same fn = `always/Fatal/5s` in one
//! profile, `always/Recorded/30s` in another)

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::handles::indexer::IndexerBackend;

use super::snapshot::Snapshot;

/// One evaluation's outcome — three meanings, not a bool: `Violated` = invariant broken,
/// `ProbeError` = harness/RPC broken (aborts the run), `Pending` = not yet, keep going
#[derive(Clone, Debug)]
pub enum Verdict {
    Satisfied,
    Pending,
    Violated(Violation),
    ProbeError(String),
}

/// Recorded invariant violation. Mirrors `loadtest::oracle::Violation`
#[derive(Clone, Debug)]
pub struct Violation {
    pub probe: String,
    pub height: Option<u32>,
    pub detail: String,
}

/// Violation ends the run, or is only recorded
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Fatal,
    Recorded,
}

/// Invariant class: selects when/how a probe evaluates and what a failure means
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Always,
    Eventually,
    Sometimes,
    AtCompletion,
}

/// Probe's live state at one tick = the board `ztest sync watch` renders.
///
/// - Derived from runner-private scheduler state → a draining liveness window is visible
///   *before* it fires
/// - `since_satisfied`/`window` are `eventually`-only; `window: None` = unbounded
#[derive(Clone, Debug)]
pub struct ProbeStatus {
    pub name: String,
    pub class: Class,
    pub severity: Severity,
    pub state: ProbeState,
    pub since_satisfied: Option<Duration>,
    pub window: Option<Duration>,
}

/// Standing across the run, vs [`Verdict`] = one evaluation. `Violating` = already failing
/// but not yet outlasting `hold_for`, so not fired
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeState {
    Ok,
    Pending,
    Violating,
    NotYet,
}

impl ProbeState {
    pub fn is_ok(&self) -> bool {
        matches!(self, ProbeState::Ok)
    }
}

/// Evaluation cadence. `Window` is `eventually`'s satisfy-deadline, not a cadence
#[derive(Clone, Copy, Debug)]
pub enum Cadence {
    EachTick,
    Every(Duration),
    EveryBlocks(u32),
    Window(Duration),
}

/// Context for RPC-backed probes: the independent oracle (indexer) wallet state is checked against
pub struct SyncCtx {
    indexer: Option<Arc<dyn IndexerBackend>>,
}

impl std::fmt::Debug for SyncCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncCtx").field("has_indexer", &self.indexer.is_some()).finish()
    }
}

impl SyncCtx {
    pub fn new(indexer: Option<Arc<dyn IndexerBackend>>) -> Self {
        Self { indexer }
    }
    /// `None` in a walletless/observer setup (no topology bound one)
    pub fn indexer(&self) -> Option<&dyn IndexerBackend> {
        self.indexer.as_deref()
    }
}

/// Boxed async probe body; borrows snapshot + ctx for the future's life
type AsyncCheck = Box<
    dyn for<'a> Fn(&'a Snapshot, &'a SyncCtx) -> Pin<Box<dyn Future<Output = Verdict> + Send + 'a>>
        + Send
        + Sync,
>;

/// Probe body: pure predicate = sync fn, oracle-backed = async closure. No single
/// blanket-impl `check()` (two blanket `impl`s over `F` break coherence) → `check`/`check_rpc`
pub enum Check {
    Sync(Box<dyn Fn(&Snapshot) -> Verdict + Send + Sync>),
    Async(AsyncCheck),
}

impl std::fmt::Debug for Check {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Check::Sync(_) => "Check::Sync(..)",
            Check::Async(_) => "Check::Async(..)",
        })
    }
}

impl Check {
    pub async fn evaluate(&self, snap: &Snapshot, cx: &SyncCtx) -> Verdict {
        match self {
            Check::Sync(f) => f(snap),
            Check::Async(f) => f(snap, cx).await,
        }
    }
}

/// Registered probe: identity/class/cadence/severity + rolling scheduler state the runner
/// threads across ticks.
///
/// - `after` = fault that must fire before an `eventually` window arms
/// - `hold_for` = debounce, quantized to the cadence as a consecutive-violation count
#[derive(Debug)]
pub struct ProbeSpec {
    pub name: String,
    pub class: Class,
    pub severity: Severity,
    pub cadence: Cadence,
    pub after: Option<String>,
    pub hold_for: Option<Duration>,
    pub check: Check,
    // ── rolling scheduler state ──
    pub last_fired_seq: Option<u64>,
    pub last_fired_height: u32,
    pub next_due: Option<tokio::time::Instant>,
    pub violation_streak: u32,
    pub last_satisfied: Option<tokio::time::Instant>,
    pub ever_satisfied: bool,
}

impl ProbeSpec {
    /// Due to evaluate at `(height, now)`? `always`/`eventually` honor the cadence;
    /// `sometimes`/`at_completion` evaluate at end → never due here
    pub fn due(&self, height: u32, now: tokio::time::Instant) -> bool {
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

    pub fn mark_fired(&mut self, seq: u64, height: u32, now: tokio::time::Instant) {
        self.last_fired_seq = Some(seq);
        self.last_fired_height = height;
        if let Cadence::Every(d) = self.cadence {
            self.next_due = Some(now + d);
        }
    }

    /// Debounce threshold in consecutive violations (≥1); a bare threshold flaps on noisy
    /// sync signals
    pub fn violation_threshold(&self) -> u32 {
        match (self.hold_for, self.cadence) {
            (Some(hold), Cadence::Every(d)) if !d.is_zero() => {
                (hold.as_secs_f64() / d.as_secs_f64()).ceil() as u32
            }
            _ => 1,
        }
    }

    /// Live board entry at `now`, given the engine's base `tick`.
    ///
    /// `eventually` stores only *when* last satisfied → state read off that age: ≤tick = Ok,
    /// >window = stall already fired, between = draining its allowance
    pub fn status(&self, now: tokio::time::Instant, tick: Duration) -> ProbeStatus {
        let window = match self.cadence {
            Cadence::Window(d) if d != Duration::MAX => Some(d),
            _ => None,
        };
        let since = self.last_satisfied.map(|last| now.saturating_duration_since(last));

        let state = match self.class {
            Class::Always => {
                if self.violation_streak > 0 {
                    ProbeState::Violating
                } else if self.last_fired_seq.is_some() {
                    ProbeState::Ok
                } else {
                    ProbeState::NotYet
                }
            }
            Class::Eventually => match (since, window) {
                (None, _) => ProbeState::NotYet,
                (Some(since), Some(w)) if since > w => ProbeState::Violating,
                (Some(since), _) if since > tick => ProbeState::Pending,
                (Some(_), _) => ProbeState::Ok,
            },
            Class::Sometimes => {
                if self.ever_satisfied {
                    ProbeState::Ok
                } else {
                    ProbeState::NotYet
                }
            }
            Class::AtCompletion => ProbeState::NotYet,
        };

        ProbeStatus {
            name: self.name.clone(),
            class: self.class,
            severity: self.severity,
            state,
            since_satisfied: since.filter(|_| self.class == Class::Eventually),
            window,
        }
    }
}

/// One probe registration: `run.always(Fatal).every(secs(5)).check(fn)`. Setters chain,
/// `check`/`check_rpc` finalize and register
#[must_use = "a probe builder does nothing until `.check(...)` is called"]
pub struct ProbeBuilder<'r> {
    pub sink: &'r mut Vec<ProbeSpec>,
    pub class: Class,
    pub severity: Severity,
    pub cadence: Cadence,
    pub after: Option<String>,
    pub name: Option<String>,
    pub hold_for: Option<Duration>,
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
    pub fn every(mut self, period: Duration) -> Self {
        self.cadence = Cadence::Every(period);
        self
    }
    /// Evaluate every `n` blocks of height progress
    pub fn every_blocks(mut self, n: u32) -> Self {
        self.cadence = Cadence::EveryBlocks(n);
        self
    }
    pub fn each_tick(mut self) -> Self {
        self.cadence = Cadence::EachTick;
        self
    }
    /// (`eventually`) satisfaction required ≥1× per rolling `window`
    pub fn window(mut self, window: Duration) -> Self {
        self.cadence = Cadence::Window(window);
        self
    }
    /// Arm only after the named fault fires
    pub fn after(mut self, fault: impl Into<String>) -> Self {
        self.after = Some(fault.into());
        self
    }
    /// Override the auto-derived name
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    /// Debounce: violation must persist `dur` before firing
    pub fn hold_for(mut self, dur: Duration) -> Self {
        self.hold_for = Some(dur);
        self
    }

    pub fn check<F>(self, f: F)
    where
        F: Fn(&Snapshot) -> Verdict + Send + Sync + 'static,
    {
        self.finish(Check::Sync(Box::new(f)));
    }

    /// Register an RPC/oracle-backed probe, written `|s, cx| Box::pin(async move { … })`
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

/// Cadence duration constructors; `const` so a profile can name cadences as `const` items
pub const fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}
pub const fn mins(n: u64) -> Duration {
    Duration::from_secs(n * 60)
}
pub const fn hours(n: u64) -> Duration {
    Duration::from_secs(n * 3600)
}

/// `ensure!`-style helper for pure probes: return `Violated` unless the condition holds
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
