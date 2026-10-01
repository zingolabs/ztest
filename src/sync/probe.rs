//! Probe (invariant) taxonomy + per-probe scheduling state.
//!
//! Probe = named async predicate over a [`Snapshot`] + the run's topology handles, in one of
//! four classes (design §"the invariant taxonomy"):
//!
//! - **always** — safety, every tick; violation = bug
//! - **eventually** — liveness, (re)satisfy within a `window`; else stall
//! - **sometimes** — coverage, ≥1 tick over the run; else weak test
//! - **at_completion** — post-condition, once at tip
//!
//! Body's answer typed per class ([`Kind::Answer`]): `()` = holds, `bool` = reached yet

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

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

/// Broken invariant; `probe` stamped by the runner. Raised in a body with
/// [`sync_ensure!`](crate::sync_ensure) / [`sync_fail!`](crate::sync_fail)
#[derive(Clone, Debug)]
pub struct Violation {
    pub probe: String,
    pub height: Option<u32>,
    pub detail: String,
}

impl Violation {
    pub fn new(height: Option<u32>, detail: impl Into<String>) -> Self {
        Self { probe: String::new(), height, detail: detail.into() }
    }
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.height {
            Some(height) => write!(f, "at {height}: {}", self.detail),
            None => f.write_str(&self.detail),
        }
    }
}

impl std::error::Error for Violation {}

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

/// Evaluation cadence. `Window` is `eventually`'s satisfy-deadline, not a cadence
#[derive(Clone, Copy, Debug)]
pub enum Cadence {
    EachTick,
    Every(Duration),
    EveryBlocks(u32),
    Window(Duration),
}

/// Body's answer → [`Verdict`]; `Err` = [`Violation`] (downcast) or harness error
pub trait Answer: Sized + 'static {
    fn verdict(answer: anyhow::Result<Self>) -> Verdict;
}

impl Answer for () {
    fn verdict(answer: anyhow::Result<()>) -> Verdict {
        answer.map_or_else(failed, |()| Verdict::Satisfied)
    }
}

impl Answer for bool {
    fn verdict(answer: anyhow::Result<bool>) -> Verdict {
        match answer {
            Ok(true) => Verdict::Satisfied,
            Ok(false) => Verdict::Pending,
            Err(e) => failed(e),
        }
    }
}

fn failed(e: anyhow::Error) -> Verdict {
    match e.downcast::<Violation>() {
        Ok(violation) => Verdict::Violated(violation),
        Err(e) => Verdict::ProbeError(format!("{e:#}")),
    }
}

/// Probe class at the type level: its [`Answer`] + the setters its builder offers
pub trait Kind: 'static {
    const CLASS: Class;
    type Answer: Answer;
}

#[derive(Debug)]
pub enum Always {}
#[derive(Debug)]
pub enum Eventually {}
#[derive(Debug)]
pub enum Sometimes {}
#[derive(Debug)]
pub enum AtCompletion {}

impl Kind for Always {
    const CLASS: Class = Class::Always;
    type Answer = ();
}
impl Kind for Eventually {
    const CLASS: Class = Class::Eventually;
    type Answer = bool;
}
impl Kind for Sometimes {
    const CLASS: Class = Class::Sometimes;
    type Answer = bool;
}
impl Kind for AtCompletion {
    const CLASS: Class = Class::AtCompletion;
    type Answer = ();
}

type Judging<'a> = Pin<Box<dyn Future<Output = Verdict> + 'a>>;

/// Object-safe `AsyncFn` (no `Send`: the runner awaits bodies inline, never spawns them)
trait Body<T> {
    fn judge<'a>(&'a self, snap: &'a Snapshot, topology: &'a T) -> Judging<'a>;
}

struct Typed<F, A>(F, PhantomData<fn() -> A>);

impl<T, A: Answer, F: AsyncFn(&Snapshot, &T) -> anyhow::Result<A>> Body<T> for Typed<F, A> {
    fn judge<'a>(&'a self, snap: &'a Snapshot, topology: &'a T) -> Judging<'a> {
        Box::pin(async move { A::verdict((self.0)(snap, topology).await) })
    }
}

/// Registered probe: identity/class/cadence/severity + rolling scheduler state the runner
/// threads across ticks.
///
/// - `after` = fault that must fire before an `eventually` window arms
/// - `hold_for` = debounce, quantized to the cadence as a consecutive-violation count
/// - `within` = `at_completion` retry span (a violation re-judged each tick until it ends)
pub struct ProbeSpec<T> {
    pub name: String,
    pub class: Class,
    pub severity: Severity,
    pub cadence: Cadence,
    pub after: Option<String>,
    pub hold_for: Option<Duration>,
    pub within: Option<Duration>,
    body: Rc<dyn Body<T>>,
    // ── rolling scheduler state ──
    pub last_fired_seq: Option<u64>,
    pub last_fired_height: u32,
    pub next_due: Option<tokio::time::Instant>,
    pub violation_streak: u32,
    pub last_satisfied: Option<tokio::time::Instant>,
    pub ever_satisfied: bool,
}

impl<T> std::fmt::Debug for ProbeSpec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeSpec")
            .field("name", &self.name)
            .field("class", &self.class)
            .field("severity", &self.severity)
            .field("cadence", &self.cadence)
            .finish_non_exhaustive()
    }
}

impl<T> ProbeSpec<T> {
    pub async fn judge(&self, snap: &Snapshot, topology: &T) -> Verdict {
        self.body.judge(snap, topology).await
    }

    /// Same body (its captured state included), fresh scheduler state: a run-wide probe's
    /// copy in each phase
    pub(super) fn fresh(&self) -> Self {
        Self {
            name: self.name.clone(),
            class: self.class,
            severity: self.severity,
            cadence: self.cadence,
            after: self.after.clone(),
            hold_for: self.hold_for,
            within: self.within,
            body: Rc::clone(&self.body),
            last_fired_seq: None,
            last_fired_height: 0,
            next_due: None,
            violation_streak: 0,
            last_satisfied: None,
            ever_satisfied: false,
        }
    }

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
}

/// One probe registration: `phase.always("name", Fatal).every(secs(5)).check(async |s, t| ..)`.
/// Setters per class ([`Kind`]), `check` finalizes and registers
#[must_use = "a probe builder does nothing until `.check(...)` is called"]
pub struct ProbeBuilder<'r, T, K> {
    sink: &'r mut Vec<ProbeSpec<T>>,
    name: String,
    severity: Severity,
    cadence: Cadence,
    after: Option<String>,
    hold_for: Option<Duration>,
    within: Option<Duration>,
    kind: PhantomData<K>,
}

impl<T, K> std::fmt::Debug for ProbeBuilder<'_, T, K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeBuilder")
            .field("name", &self.name)
            .field("severity", &self.severity)
            .field("cadence", &self.cadence)
            .finish_non_exhaustive()
    }
}

impl<'r, T, K: Kind> ProbeBuilder<'r, T, K> {
    pub(super) fn new(sink: &'r mut Vec<ProbeSpec<T>>, name: String, severity: Severity) -> Self {
        let cadence = match K::CLASS {
            Class::Eventually => Cadence::Window(Duration::MAX),
            _ => Cadence::EachTick,
        };
        Self {
            sink,
            name,
            severity,
            cadence,
            after: None,
            hold_for: None,
            within: None,
            kind: PhantomData,
        }
    }

    /// Register `body`: `async |snapshot, topology| ..`, `?` on any error (= harness broke)
    pub fn check<F>(self, body: F)
    where
        F: AsyncFn(&Snapshot, &T) -> anyhow::Result<K::Answer> + 'static,
    {
        self.sink.push(ProbeSpec {
            name: self.name,
            class: K::CLASS,
            severity: self.severity,
            cadence: self.cadence,
            after: self.after,
            hold_for: self.hold_for,
            within: self.within,
            body: Rc::new(Typed(body, PhantomData)),
            last_fired_seq: None,
            last_fired_height: 0,
            next_due: None,
            violation_streak: 0,
            last_satisfied: None,
            ever_satisfied: false,
        });
    }
}

impl<T> ProbeBuilder<'_, T, Always> {
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
    /// Debounce: violation must persist `dur` before firing
    pub fn hold_for(mut self, dur: Duration) -> Self {
        self.hold_for = Some(dur);
        self
    }
}

impl<T> ProbeBuilder<'_, T, Eventually> {
    /// Satisfaction required ≥1× per rolling `window`
    pub fn window(mut self, window: Duration) -> Self {
        self.cadence = Cadence::Window(window);
        self
    }
    /// Arm only after the named fault fires
    pub fn after(mut self, fault: impl Into<String>) -> Self {
        self.after = Some(fault.into());
        self
    }
}

impl<T> ProbeBuilder<'_, T, AtCompletion> {
    /// Violation re-judged each tick for up to `span` before it counts (cancellation ends it)
    pub fn within(mut self, span: Duration) -> Self {
        self.within = Some(span);
        self
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

/// `ensure!` for invariants: `Err(Violation)` unless `cond` (optional `at = height` first)
#[macro_export]
macro_rules! sync_ensure {
    (at = $height:expr, $cond:expr, $($arg:tt)+) => {
        if !($cond) {
            $crate::sync_fail!(at = $height, $($arg)+);
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !($cond) {
            $crate::sync_fail!($($arg)+);
        }
    };
}

/// `bail!` for invariants: return `Err(Violation)` (optional `at = height` first)
#[macro_export]
macro_rules! sync_fail {
    (at = $height:expr, $($arg:tt)+) => {
        return ::core::result::Result::Err(::core::convert::From::from(
            $crate::sync::Violation::new(::core::option::Option::Some($height), format!($($arg)+)),
        ))
    };
    ($($arg:tt)+) => {
        return ::core::result::Result::Err(::core::convert::From::from(
            $crate::sync::Violation::new(::core::option::Option::None, format!($($arg)+)),
        ))
    };
}
