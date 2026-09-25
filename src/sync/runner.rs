//! [`SyncEngine`] — continuous monitor with a completion predicate, one [`Phase`] at a time.
//!
//! - Per phase: launch its [`SyncSubject`], one immutable [`Snapshot`] per tick, bounded
//!   [`History`], its own probes at their cadences (all due at a tick share the one snapshot)
//! - `pass` = every phase reached completion, invariants intact; `fail` = fatal violation /
//!   coverage miss / subject failure / stall / cancellation / timeout
//! - Next phase starts only after one that completed with no fatal violation

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use tokio::time::{Instant, MissedTickBehavior, interval};

use crate::cancel::Cancel;
use crate::handles::ContainerSample;
use crate::handles::pod::Watched;
use crate::metrics::{Family, Row};

use super::chainwork::ChainWork;
use super::nemesis::{FaultKind, Nemesis, ScheduledFault};
use super::phase::{Phase, PhaseOutcome, Source};
use super::probe::{Cadence, Class, ProbeSpec, Severity, SyncCtx, Verdict, Violation};
use super::restart::RestartWatch;
use super::snapshot::{History, Snapshot, SnapshotBuilder, TickEvents};
use super::subject::{ProgressView, SyncSubject};
use super::work::{Op, OpSet, Segment, Work};
use crate::handles::indexer::BlockHeight;

/// Engine's sampling cadence when a profile names none. Also what a watcher assumes a
/// driver ticks at until its `Started` says otherwise
pub const DEFAULT_TICK: Duration = Duration::from_secs(5);

/// Readings the work preflight will take before calling the subject unreadable (exporter can
/// lag pod-Ready by a scrape)
const WORK_PREFLIGHT_ATTEMPTS: u32 = 3;

/// Terminal result of a run, sole vocabulary for one ([`SyncStatus`] wraps it with the
/// pre-terminal states rather than restating them).
///
/// - `Passed` = tip reached, every fatal invariant intact, every `sometimes` probe triggered
/// - `Errored` = probe or harness failure, not a verdict about the subject
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncVerdict {
    Passed,
    Failed,
    Cancelled,
    TimedOut,
    Errored,
}

impl SyncVerdict {
    pub fn is_pass(&self) -> bool {
        matches!(self, SyncVerdict::Passed)
    }
}

/// Variant name = the wire tag = the rendered word, one definition (serde derives the
/// same names) — a rename changes what a running driver publishes
impl std::fmt::Display for SyncVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

/// Recorded result of a run = every attempted phase folded.
///
/// - `verdict` = first non-passing phase's, else `Passed`
/// - `coverage_gaps` = `sometimes` probes never satisfied (green run → only a *weak* pass)
/// - `segment`/`target` = first phase's (the precondition `perf --base` checks); `None` segment
///   when no tick ever landed
#[derive(Debug)]
pub struct SyncOutcome {
    pub verdict: SyncVerdict,
    pub violations: Vec<Violation>,
    pub coverage_gaps: Vec<String>,
    pub error: Option<String>,
    pub ticks: u64,
    pub dropped_snapshots: u64,
    pub segment: Option<Segment>,
    /// `until_height` where a profile named one, else the last target seen. Recorded, not
    /// re-derived: a denominator read back off a series makes one bad scrape a shortfall
    pub target: Option<u32>,
    /// Declared rows the component never published (em-dashes read like a quiet run)
    pub unpublished: Vec<String>,
    pub phases: Vec<PhaseOutcome>,
}

/// One end of the span a phase covered ([`Segment`] = last mark - first).
///
/// `at` orders marks; `wall` places them in the TSDB — the report window's only source
#[derive(Clone, Copy)]
struct Mark {
    height: u32,
    work: Work,
    at: Instant,
    wall: SystemTime,
}

impl Mark {
    fn new(height: u32, work: Work, at: Instant) -> Mark {
        Mark { height, work, at, wall: SystemTime::now() }
    }
}

impl SyncOutcome {
    pub(crate) fn error_outcome(msg: String) -> Self {
        SyncOutcome {
            verdict: SyncVerdict::Errored,
            violations: Vec::new(),
            coverage_gaps: Vec::new(),
            error: Some(msg),
            ticks: 0,
            dropped_snapshots: 0,
            segment: None,
            target: None,
            unpublished: Vec::new(),
            phases: Vec::new(),
        }
    }
}

// Profile bodies return `SyncOutcome` but `?` on provisioning/RPC calls
// (`run.topology(..).await?`) → setup failure becomes an errored outcome
impl From<crate::EnvError> for SyncOutcome {
    fn from(e: crate::EnvError) -> Self {
        SyncOutcome::error_outcome(format!("env: {e}"))
    }
}
impl From<crate::RpcError> for SyncOutcome {
    fn from(e: crate::RpcError) -> Self {
        SyncOutcome::error_outcome(format!("rpc: {e}"))
    }
}

/// Sink for what a run produces as it goes. `origin` = the run's first reading on the wall
/// clock, every tick (a sink never has to remember it)
pub trait SyncReporter: Send {
    fn on_tick(&mut self, _snap: &Snapshot, _origin: SystemTime) {}
    /// Probe evaluated to a non-`Satisfied` verdict worth surfacing
    fn on_probe(&mut self, _name: &str, _verdict: &Verdict) {}
    fn on_phase_start(&mut self, _name: &str) {}
    fn on_phase(&mut self, _phase: &PhaseOutcome) {}
    fn on_finish(&mut self, _outcome: &SyncOutcome) {}
}

/// Discards everything
#[derive(Debug, Default)]
pub struct NullReporter;
impl SyncReporter for NullReporter {}

/// One human-readable line per interesting event, to stderr
#[derive(Debug, Default)]
pub struct StderrReporter;
impl SyncReporter for StderrReporter {
    fn on_probe(&mut self, name: &str, verdict: &Verdict) {
        match verdict {
            Verdict::Violated(v) => eprintln!("  ✗ {name}: {}", v.detail),
            Verdict::ProbeError(e) => eprintln!("  ! {name}: probe error: {e}"),
            _ => {}
        }
    }
    fn on_phase(&mut self, phase: &PhaseOutcome) {
        eprintln!("  phase {}", phase.describe());
    }
    fn on_finish(&mut self, o: &SyncOutcome) {
        eprintln!(
            "sync {:?}: {} ticks, {} violations, {} coverage gaps",
            o.verdict,
            o.ticks,
            o.violations.len(),
            o.coverage_gaps.len()
        );
    }
}

enum Flow {
    Continue,
    FailFast,
    Abort(String),
}

/// How the wait for a subject's gate families ended
enum Readiness {
    Ready,
    Cancelled,
    Late(String),
}

impl std::fmt::Debug for SyncEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncEngine")
            .field("first", &self.first)
            .field("rest", &self.rest)
            .finish_non_exhaustive()
    }
}

/// Subject is boxed, not a type parameter: every profile binds one dynamically, and a
/// generic engine would push that parameter into every caller for no gain.
///
/// Derefs to its first [`Phase`] → `engine.always(..)` registers there
pub struct SyncEngine {
    first: Phase,
    rest: Vec<Phase>,
    ctx: SyncCtx,
    cancel: Cancel,
    history_cap: usize,
    reporter: Box<dyn SyncReporter>,
    faults: Vec<ScheduledFault>,
    watched: Vec<Arc<dyn Watched>>,
}

impl std::ops::Deref for SyncEngine {
    type Target = Phase;
    fn deref(&self) -> &Phase {
        &self.first
    }
}
impl std::ops::DerefMut for SyncEngine {
    fn deref_mut(&mut self) -> &mut Phase {
        &mut self.first
    }
}

impl SyncEngine {
    /// Runner over `subject`: no oracle indexer, no cancellation, 5 s base tick,
    /// 20k-snapshot history, no timeout, `NullReporter`. Chain the setters below.
    pub fn new(subject: Box<dyn SyncSubject>) -> Self {
        Self::phased(Phase::new("sync", Source::Bound(subject)), Vec::new())
    }

    pub(crate) fn phased(first: Phase, rest: Vec<Phase>) -> Self {
        Self {
            first,
            rest,
            ctx: SyncCtx::new(None),
            cancel: Cancel::never(),
            history_cap: 20_000,
            reporter: Box::new(NullReporter),
            faults: Vec::new(),
            watched: Vec::new(),
        }
    }

    /// Append a phase, run after the previous one completed with no fatal violation
    pub fn then(mut self, phase: Phase) -> Self {
        self.rest.push(phase);
        self
    }

    /// Profile's override of one family's ready window (matched by name + selector)
    pub fn with_ready(mut self, family: Family) -> Self {
        self.first.ready_within(family, family.ready);
        self
    }

    pub fn with_ctx(mut self, ctx: SyncCtx) -> Self {
        self.ctx = ctx;
        self
    }
    pub fn with_cancel(mut self, cancel: Cancel) -> Self {
        self.cancel = cancel;
        self
    }
    /// Base sampling interval. `each_tick` probes fire every base tick, `every(d)` probes on
    /// their own `d` quantized to this. Coarse (seconds) to avoid the wallet write-lock.
    pub fn with_tick(mut self, tick: Duration) -> Self {
        self.first.tick(tick);
        self
    }
    /// First phase's time cap (QoS `sync` tier's 48 h, or a test bound)
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.first.timeout(timeout);
        self
    }
    /// Complete at `height` instead of at tip (what makes two runs comparable: a run to tip
    /// covers a different, growing span each time → its throughput measures the chain, not code)
    pub fn with_stop_height(mut self, height: u32) -> Self {
        self.first.until_height(height);
        self
    }
    /// Complete `blocks` past the first reading (see [`Phase::for_blocks`])
    pub fn with_stop_after(mut self, blocks: u32) -> Self {
        self.first.for_blocks(blocks);
        self
    }

    pub fn with_history_cap(mut self, cap: usize) -> Self {
        self.history_cap = cap;
        self
    }
    pub fn with_reporter(mut self, reporter: Box<dyn SyncReporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Ops the first phase's probes will [`Work::require`] — checked against one live reading
    /// before the phase, so a subject that does not publish them fails by name here rather
    /// than panicking a probe hours in
    pub fn requires_work(mut self, ops: OpSet) -> Self {
        self.first.requires_work(ops);
        self
    }

    pub fn with_probes(mut self, probes: Vec<ProbeSpec>) -> Self {
        self.first.probes = probes;
        self
    }

    /// Scheduled faults the runner applies (`kill`); other kinds stay recorded only
    pub fn with_nemesis(mut self, nemesis: &Nemesis) -> Self {
        self.faults = nemesis.scheduled.clone();
        self
    }

    /// Pods sampled each tick for restarts, and the targets `kill` faults resolve against
    pub(crate) fn with_watched(mut self, watched: Vec<Arc<dyn Watched>>) -> Self {
        self.watched = watched;
        self
    }

    pub async fn run(self) -> SyncOutcome {
        let SyncEngine { first, rest, ctx, cancel, history_cap, reporter, faults, watched } = self;
        let mut shared = Shared {
            chaos: Chaos::new(faults, Instant::now()),
            ctx,
            cancel,
            history_cap,
            reporter,
            watched,
            run_origin: None,
        };
        if let Err(e) = shared.preflight_faults().await {
            let outcome = SyncOutcome::error_outcome(e);
            shared.reporter.on_finish(&outcome);
            return outcome;
        }
        let mut results = Vec::new();
        for mut phase in std::iter::once(first).chain(rest) {
            shared.reporter.on_phase_start(&phase.name);
            let result = shared.run_phase(&mut phase).await;
            shared.reporter.on_phase(&result.summary);
            let proceed = result.proceed;
            results.push(result);
            if !proceed {
                break;
            }
        }
        let outcome = fold(results);
        shared.reporter.on_finish(&outcome);
        outcome
    }
}

/// Everything a phase leaves behind; [`fold`] turns the list into a [`SyncOutcome`]
struct PhaseResult {
    summary: PhaseOutcome,
    violations: Vec<Violation>,
    dropped: u64,
    segment: Option<Segment>,
    target: Option<u32>,
    unpublished: Vec<String>,
    proceed: bool,
}

fn fold(results: Vec<PhaseResult>) -> SyncOutcome {
    let verdict = results
        .iter()
        .map(|r| r.summary.verdict)
        .find(|v| !v.is_pass())
        .unwrap_or(SyncVerdict::Passed);
    let (segment, target) =
        results.first().map(|r| (r.segment.clone(), r.target)).unwrap_or((None, None));
    let mut outcome = SyncOutcome {
        verdict,
        violations: Vec::new(),
        coverage_gaps: Vec::new(),
        error: None,
        ticks: 0,
        dropped_snapshots: 0,
        segment,
        target,
        unpublished: Vec::new(),
        phases: Vec::new(),
    };
    for r in results {
        outcome.violations.extend(r.violations);
        outcome.coverage_gaps.extend(r.summary.coverage_gaps.iter().cloned());
        outcome.error = outcome.error.or_else(|| r.summary.error.clone());
        outcome.ticks += r.summary.ticks;
        outcome.dropped_snapshots += r.dropped;
        outcome.unpublished.extend(r.unpublished);
        outcome.phases.push(r.summary);
    }
    outcome
}

/// Scheduled faults on the run's clock (`at` = offset from run start, across phases)
struct Chaos {
    origin: Instant,
    pending: Vec<ScheduledFault>,
    fired: Vec<(String, Instant)>,
    last_fault_at: Option<Instant>,
}

impl Chaos {
    fn new(mut faults: Vec<ScheduledFault>, origin: Instant) -> Self {
        faults.retain(|f| matches!(f.kind, FaultKind::Kill { .. }));
        faults.sort_by_key(|f| f.at);
        Chaos { origin, pending: faults, fired: Vec::new(), last_fault_at: None }
    }

    fn take_due(&mut self, now: Instant) -> Vec<ScheduledFault> {
        let split = self.pending.iter().take_while(|f| self.origin + f.at <= now).count();
        self.pending.drain(..split).collect()
    }

    fn record(&mut self, fault: &ScheduledFault, at: Instant) {
        if let Some(name) = &fault.name {
            self.fired.push((name.clone(), at));
        }
        self.last_fault_at = Some(at);
    }

    fn fired(&self, name: &str) -> bool {
        self.fired.iter().any(|(n, _)| n == name)
    }
}

/// Run-scoped state every phase shares
struct Shared {
    ctx: SyncCtx,
    cancel: Cancel,
    history_cap: usize,
    reporter: Box<dyn SyncReporter>,
    watched: Vec<Arc<dyn Watched>>,
    chaos: Chaos,
    run_origin: Option<SystemTime>,
}

impl Shared {
    /// Every `kill` → exactly one restartable component (refused here, not at its offset hours in)
    async fn preflight_faults(&self) -> Result<(), String> {
        for fault in &self.chaos.pending {
            let FaultKind::Kill { component } = &fault.kind else { continue };
            let target = self.kill_target(component)?;
            target.ensure_restartable().await.map_err(|e| format!("nemesis kill: {e}"))?;
        }
        Ok(())
    }

    fn kill_target(&self, component: &str) -> Result<&Arc<dyn Watched>, String> {
        let mut hits = self.watched.iter().filter(|w| w.answers_to(component));
        match (hits.next(), hits.next()) {
            (Some(w), None) => Ok(w),
            (None, _) => Err(format!("nemesis kill: no component named {component:?}")),
            (Some(_), Some(_)) => Err(format!("nemesis kill: {component:?} names several pods")),
        }
    }

    async fn run_phase(&mut self, phase: &mut Phase) -> PhaseResult {
        let started = Instant::now();
        let started_wall = SystemTime::now();
        let deadline = phase.timeout.map(|t| started + t);
        let early = |phase: &Phase, verdict, error: Option<String>| {
            PhaseResult::early(phase, verdict, error, started, started_wall)
        };
        let subject = match std::mem::replace(&mut phase.source, Source::Unbound) {
            Source::Bound(subject) => subject,
            Source::Unbound => {
                return early(phase, SyncVerdict::Errored, Some("no subject bound".into()));
            }
            Source::Deferred(build) => {
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => return early(phase, SyncVerdict::Cancelled, None),
                    _ = sleep_until(deadline) => return early(phase, SyncVerdict::TimedOut, None),
                    built = build(self.ctx.clone()) => match built {
                        Ok(subject) => subject,
                        Err(e) => {
                            let error = Some(format!("build subject: {e}"));
                            return early(phase, SyncVerdict::Errored, error);
                        }
                    },
                }
            }
        };
        let run = PhaseRun {
            s: self,
            phase,
            subject,
            started,
            started_wall,
            deadline,
            restart: RestartWatch::default(),
        };
        run.run().await
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

impl PhaseResult {
    fn early(
        phase: &Phase,
        verdict: SyncVerdict,
        error: Option<String>,
        started: Instant,
        started_wall: SystemTime,
    ) -> Self {
        PhaseResult {
            summary: PhaseOutcome {
                name: phase.name.clone(),
                verdict,
                started_ms: super::detached::epoch_millis(started_wall),
                elapsed_ms: started.elapsed().as_millis() as u64,
                ticks: 0,
                violations: 0,
                coverage_gaps: Vec::new(),
                error,
                restarts: 0,
            },
            violations: Vec::new(),
            dropped: 0,
            segment: None,
            target: phase.stop_height,
            unpublished: Vec::new(),
            proceed: false,
        }
    }
}

/// One phase in flight: its subject, its probes, the run's shared state
struct PhaseRun<'a> {
    s: &'a mut Shared,
    phase: &'a mut Phase,
    subject: Box<dyn SyncSubject>,
    started: Instant,
    started_wall: SystemTime,
    deadline: Option<Instant>,
    restart: RestartWatch,
}

/// How the tick loop ended
struct Ending {
    verdict: SyncVerdict,
    error: Option<String>,
    completed: bool,
}

impl Ending {
    fn cut(verdict: SyncVerdict, error: Option<String>) -> Self {
        Ending { verdict, error, completed: false }
    }
}

impl PhaseRun<'_> {
    async fn run(mut self) -> PhaseResult {
        if let Err(e) = self.subject.launch().await {
            return self.early(SyncVerdict::Errored, Some(format!("launch: {e}")));
        }
        // Every ready window runs from here
        let launched = Instant::now();
        match self.await_ready(launched).await {
            Readiness::Ready => {}
            Readiness::Cancelled => {
                let _ = self.subject.stop().await;
                return self.early(SyncVerdict::Cancelled, None);
            }
            Readiness::Late(e) => {
                let _ = self.subject.stop().await;
                return self.early(SyncVerdict::Errored, Some(e));
            }
        }

        if let Err(e) = self.check_required_work().await {
            let _ = self.subject.stop().await;
            return self.early(SyncVerdict::Errored, Some(e.to_string()));
        }

        let mut pending: Vec<Row> = self.subject.rows().to_vec();
        let mut unpublished: Vec<String> = Vec::new();

        let mut builder = SnapshotBuilder::new(Instant::now());
        let mut history = History::new(self.s.history_cap);
        let mut violations: Vec<Violation> = Vec::new();
        let mut fatal = false;
        let mut chain_work = ChainWork::new();
        let mut last_work = Work::ZERO;
        let mut events = TickEvents::default();
        // Accumulated as the phase goes (origin = first tick, head = latest) → survives a
        // cancelled or timed-out phase and reports where it actually got to
        let mut origin: Option<Mark> = None;
        let mut head: Option<Mark> = None;
        let network = self.network().await;

        let mut ticker = interval(self.phase.tick);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Consecutive failed `progress()` reads, reset by any success
        let mut progress_errors: u32 = 0;
        let ending = loop {
            tokio::select! {
                biased;
                _ = self.s.cancel.cancelled() => {
                    let _ = self.subject.stop().await;
                    break Ending::cut(SyncVerdict::Cancelled, None);
                }
                _ = ticker.tick() => {}
            }

            let now = Instant::now();
            if self.deadline.is_some_and(|dl| now >= dl) {
                let _ = self.subject.stop().await;
                break Ending::cut(SyncVerdict::TimedOut, None);
            }
            if let Err(e) = self.apply_faults(now).await {
                let _ = self.subject.stop().await;
                break Ending::cut(SyncVerdict::Errored, Some(e));
            }
            self.settle_rows(&mut pending, &mut unpublished, launched).await;
            let samples = self.sample_watched().await;

            // Snapshot-then-evaluate. A progress-read error holds prior state and retries next
            // tick (the reservation loop's pattern); only a *probe* error aborts.
            //
            // Log at widening intervals: a never-answering subject produces no snapshot, which
            // every display reads as "not started yet" — indistinguishable from wedged.
            let progress = self.subject.progress().await;
            // Before `is_complete`: a task that died also reads complete
            if let Some(msg) = self.subject.failure().await {
                let _ = self.subject.stop().await;
                break Ending::cut(SyncVerdict::Failed, Some(msg));
            }
            let progress = match progress {
                Ok(p) => {
                    progress_errors = 0;
                    p
                }
                Err(e) => {
                    progress_errors += 1;
                    if progress_errors.is_power_of_two() {
                        tracing::warn!(
                            consecutive = progress_errors,
                            "progress read failed; no snapshot this tick: {e}"
                        );
                    }
                    if self.restart.observe(&samples, false).holding {
                        credit_liveness(&mut self.phase.probes, now);
                    }
                    continue;
                }
            };
            let view = self.restart.observe(&samples, true);
            events = TickEvents {
                last_fault_at: self.s.chaos.last_fault_at,
                restarts: view.restarts,
                restarting: view.holding,
            };
            last_work = self.read_work(&mut chain_work, progress.as_ref(), last_work).await;
            let snap = Arc::new(builder.build(progress.as_ref(), now, last_work, events));
            if let Some(blocks) = self.phase.stop_after.take() {
                self.phase.stop_height = Some(snap.height().saturating_add(blocks));
            }
            let mark = Mark::new(snap.height(), last_work, now);
            origin.get_or_insert(mark);
            head = Some(mark);
            history.push(snap.clone());
            let run_origin = *self.s.run_origin.get_or_insert(mark.wall);
            self.s.reporter.on_tick(&snap, run_origin);

            match self.eval_tick(&snap, now, &mut violations).await {
                Flow::Continue => {}
                Flow::FailFast => {
                    let _ = self.subject.stop().await;
                    fatal = true;
                    break Ending::cut(SyncVerdict::Failed, None);
                }
                Flow::Abort(msg) => {
                    let _ = self.subject.stop().await;
                    break Ending::cut(SyncVerdict::Errored, Some(msg));
                }
            }

            // Declared stop height completes ahead of the subject's own predicate (a segment
            // must end where it said it would, whether or not the chain has more)
            let reached_stop = self.phase.stop_height.is_some_and(|h| snap.height() >= h);
            if reached_stop || self.subject.is_complete().await {
                // at_completion probes over a final snapshot + the wallet's commitment-tree
                // roots (sync task done → wallet static, read cannot race the scan). A
                // roots-read failure degrades to empty roots, no abort at the finish line.
                let mut error = None;
                if let Ok(p) = self.subject.progress().await {
                    let work = self.read_work(&mut chain_work, p.as_ref(), last_work).await;
                    let at = Instant::now();
                    let final_snap = Arc::new(builder.build(p.as_ref(), at, work, events));
                    head = Some(Mark::new(final_snap.height(), work, at));
                    match self.eval_at_completion(&final_snap, &mut violations).await {
                        Ok(any_fatal) => fatal |= any_fatal,
                        Err(msg) => error = Some(msg),
                    }
                }
                let gaps = coverage_gaps(&self.phase.probes);
                fatal |= !gaps.is_empty();
                let verdict = if error.is_some() {
                    SyncVerdict::Errored
                } else if violations.is_empty() && gaps.is_empty() {
                    SyncVerdict::Passed
                } else {
                    SyncVerdict::Failed
                };
                break Ending { verdict, error, completed: true };
            }
        };

        // First reading → last: `work` is what this phase performed, not the cumulative totals
        // a seeded datadir brought, and `to` is where it truly reached, not where it was aimed.
        // A phase that traversed nothing has no comparable span and reports none.
        let segment = origin.zip(head).filter(|(origin, head)| head.height > origin.height).map(
            |(origin, head)| Segment {
                network,
                from: origin.height,
                to: head.height,
                work: head.work.delta(&origin.work),
                elapsed_ms: head.at.saturating_duration_since(origin.at).as_millis() as u64,
                started_ms: super::detached::epoch_millis(origin.wall),
            },
        );
        let target = history.latest().and_then(|s| s.target());
        let gaps = coverage_gaps(&self.phase.probes);
        PhaseResult {
            proceed: ending.completed && !fatal && ending.error.is_none(),
            summary: PhaseOutcome {
                name: self.phase.name.clone(),
                verdict: ending.verdict,
                started_ms: super::detached::epoch_millis(self.started_wall),
                elapsed_ms: self.started.elapsed().as_millis() as u64,
                ticks: builder.seq(),
                violations: violations.len(),
                coverage_gaps: gaps,
                error: ending.error,
                restarts: events.restarts,
            },
            violations,
            dropped: history.dropped(),
            segment,
            // Named stop height = the objective; absent one, the subject's own last target
            target: self.phase.stop_height.or(target),
            unpublished,
        }
    }

    /// Outcome of a phase that never reached its first tick
    fn early(&self, verdict: SyncVerdict, error: Option<String>) -> PhaseResult {
        PhaseResult::early(self.phase, verdict, error, self.started, self.started_wall)
    }

    /// Fire every `kill` whose offset passed; restart window opened at once (kubelet lags)
    async fn apply_faults(&mut self, now: Instant) -> Result<(), String> {
        for fault in self.s.chaos.take_due(now) {
            let FaultKind::Kill { component } = &fault.kind else { continue };
            let target = self.s.kill_target(component)?;
            target.kill().await.map_err(|e| format!("nemesis kill {component}: {e}"))?;
            tracing::info!(component = %component, fault = ?fault.name, "nemesis: killed");
            self.restart.open();
            self.s.chaos.record(&fault, now);
        }
        Ok(())
    }

    /// One container sample per watched pod; unreadable = `None`, never "down"
    async fn sample_watched(&self) -> Vec<Option<ContainerSample>> {
        let reads = self.s.watched.iter().map(|w| w.sample());
        futures::future::join_all(reads).await.into_iter().map(Result::ok).collect()
    }

    /// Chain this phase is against, as the indexer names it (`main`/`test`/`regtest`); `None`
    /// with no indexer to ask. Part of a segment's identity (block 840,000 differs per network)
    async fn network(&self) -> Option<String> {
        let indexer = self.s.ctx.indexer()?;
        let name = indexer.indexer_info().await.ok()?.chain_name;
        (!name.is_empty()).then_some(name)
    }

    /// Cumulative work behind this tick's reading.
    ///
    /// - Subject's own count preferred (a wallet scans non-linearly → its height understates)
    /// - Else derived from the chain at the subject's height
    /// - Failed read holds `last`, never zeroes (a zero prints a rate spike on recovery)
    async fn read_work(
        &self,
        chain_work: &mut ChainWork,
        progress: &dyn ProgressView,
        last: Work,
    ) -> Work {
        if let Some(own) = progress.work() {
            return own;
        }
        let Some(indexer) = self.s.ctx.indexer() else {
            return last;
        };
        let height = BlockHeight::from_u32(progress.height());
        chain_work.observe_at(indexer, height).await.unwrap_or(last)
    }

    /// `Ok(true)` = a fatal post-condition broke; `Err` = a probe errored → phase aborts
    async fn eval_at_completion(
        &mut self,
        snap: &Snapshot,
        violations: &mut Vec<Violation>,
    ) -> Result<bool, String> {
        let mut fatal = false;
        for spec in self.phase.probes.iter_mut().filter(|s| s.class == Class::AtCompletion) {
            match spec.check.evaluate(snap, &self.s.ctx).await {
                Verdict::Satisfied | Verdict::Pending => {}
                Verdict::Violated(mut v) => {
                    v.probe = spec.name.clone();
                    self.s.reporter.on_probe(&spec.name, &Verdict::Violated(v.clone()));
                    violations.push(v);
                    fatal |= spec.severity == Severity::Fatal;
                }
                Verdict::ProbeError(e) => return Err(format!("{}: {e}", spec.name)),
            }
        }
        Ok(fatal)
    }

    /// Every due `always`/`eventually` probe + every `sometimes` coverage probe against `snap`
    async fn eval_tick(
        &mut self,
        snap: &Snapshot,
        now: Instant,
        violations: &mut Vec<Violation>,
    ) -> Flow {
        for spec in self.phase.probes.iter_mut() {
            match spec.class {
                Class::AtCompletion => continue,
                Class::Sometimes => {
                    // Cheap coverage predicate → evaluate every tick, latch
                    if !spec.ever_satisfied {
                        match spec.check.evaluate(snap, &self.s.ctx).await {
                            Verdict::Satisfied => spec.ever_satisfied = true,
                            Verdict::ProbeError(e) => {
                                return Flow::Abort(format!("{}: {e}", spec.name));
                            }
                            _ => {}
                        }
                    }
                }
                Class::Always => {
                    if !spec.due(snap.height(), now) {
                        continue;
                    }
                    let verdict = spec.check.evaluate(snap, &self.s.ctx).await;
                    spec.mark_fired(snap.seq(), snap.height(), now);
                    match verdict {
                        Verdict::Satisfied | Verdict::Pending => spec.violation_streak = 0,
                        Verdict::Violated(mut v) => {
                            spec.violation_streak += 1;
                            if spec.violation_streak >= spec.violation_threshold() {
                                v.probe = spec.name.clone();
                                self.s.reporter.on_probe(&spec.name, &Verdict::Violated(v.clone()));
                                violations.push(v);
                                if spec.severity == Severity::Fatal {
                                    return Flow::FailFast;
                                }
                            }
                        }
                        Verdict::ProbeError(e) => {
                            return Flow::Abort(format!("{}: {e}", spec.name));
                        }
                    }
                }
                Class::Eventually => {
                    // Window paused while a restart is open or its `.after` fault has not fired
                    let armed = spec.after.as_deref().is_none_or(|f| self.s.chaos.fired(f));
                    if snap.restarting() || !armed || spec.last_satisfied.is_none() {
                        spec.last_satisfied = Some(now);
                        if snap.restarting() || !armed {
                            continue;
                        }
                    }
                    let window = match spec.cadence {
                        Cadence::Window(d) => d,
                        _ => Duration::MAX,
                    };
                    match spec.check.evaluate(snap, &self.s.ctx).await {
                        Verdict::Satisfied => spec.last_satisfied = Some(now),
                        Verdict::ProbeError(e) => {
                            return Flow::Abort(format!("{}: {e}", spec.name));
                        }
                        Verdict::Pending | Verdict::Violated(_) => {
                            let since = now.duration_since(spec.last_satisfied.unwrap_or(now));
                            if since > window {
                                let v = Violation {
                                    probe: spec.name.clone(),
                                    height: Some(snap.height()),
                                    detail: format!(
                                        "liveness stall: not satisfied for {since:?} (window {window:?})"
                                    ),
                                };
                                self.s.reporter.on_probe(&spec.name, &Verdict::Violated(v.clone()));
                                violations.push(v);
                                if spec.severity == Severity::Fatal {
                                    return Flow::FailFast;
                                }
                            }
                        }
                    }
                }
            }
        }
        Flow::Continue
    }

    /// One reading, before the tick loop: every [`requires_work`](Phase::requires_work) op
    /// must come back measured.
    ///
    /// - Probes read these with `Work::require`, which panics on an unmeasured op → a
    ///   missing series otherwise surfaces as a mid-run panic naming no series
    /// - Subject and harness agree on these names by string only (nothing cross-checks the
    ///   component's exporter against the families this backend reads)
    async fn check_required_work(&mut self) -> Result<(), crate::error::PipelineError> {
        let required = self.phase.required_work;
        if required.is_empty() {
            return Ok(());
        }
        let mut last_err = None;
        for attempt in 0..WORK_PREFLIGHT_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(self.phase.tick).await;
            }
            let progress = match self.subject.progress().await {
                Ok(p) => p,
                Err(e) => {
                    last_err = Some(e.to_string());
                    continue;
                }
            };
            // `None` = subject publishes no work vector at all → every required op is missing
            let measured = progress.work().map(|w| w.known()).unwrap_or(OpSet::NONE);
            let missing: Vec<Op> =
                Op::ALL.into_iter().filter(|&op| required.has(op) && !measured.has(op)).collect();
            if missing.is_empty() {
                return Ok(());
            }
            return Err(self.unmeasured_work_error(&missing, measured).into());
        }
        Err(format!(
            "work counters unreadable in {WORK_PREFLIGHT_ATTEMPTS} attempts: {}",
            last_err.unwrap_or_else(|| "no error reported".to_owned()),
        )
        .into())
    }

    /// Wait for every [`gates`](SyncSubject::gates) family, each within its ready window.
    ///
    /// - Inside the window a missing family = still starting (zaino's frontier exists only
    ///   after its first commit), never a read error
    /// - Past it = the phase errors naming the family and the window it missed
    async fn await_ready(&self, launched: Instant) -> Readiness {
        let gates = self.subject.gates();
        if gates.is_empty() {
            return Readiness::Ready;
        }
        let mut waits: u32 = 0;
        loop {
            let missing: Vec<Family> = match self.subject.exposition().await {
                Some(e) => gates.iter().copied().filter(|&f| !e.publishes(f)).collect(),
                None => gates.clone(),
            };
            if missing.is_empty() {
                return Readiness::Ready;
            }
            let elapsed = launched.elapsed();
            let late: Vec<String> = missing
                .iter()
                .filter(|&&f| elapsed >= self.phase.ready_for(f))
                .map(|&f| {
                    let window = crate::fmt::format_span(self.phase.ready_for(f));
                    format!("{f} (ready within {window})")
                })
                .collect();
            if !late.is_empty() {
                return Readiness::Late(format!(
                    "not published within its ready window: {} — widen one with \
                     `run.ready_within(family, d)`",
                    late.join(", ")
                ));
            }
            waits += 1;
            if waits.is_power_of_two() {
                let names: Vec<String> = missing.iter().map(ToString::to_string).collect();
                tracing::info!(missing = ?names, "waiting for the subject to publish");
            }
            tokio::select! {
                biased;
                _ = self.s.cancel.cancelled() => return Readiness::Cancelled,
                _ = tokio::time::sleep(self.phase.tick) => {}
            }
        }
    }

    /// Settle every row whose window has closed: resolved → dropped, absent → unpublished.
    ///
    /// - Checked at the deadline, not at launch (a histogram publishes after its first block)
    /// - Advisory — display rows must not fail a run (gRPC latency never appears on a sync
    ///   nobody queries)
    /// - Scrapes only once a row is due; an unreachable exporter defers to the next tick
    async fn settle_rows(
        &self,
        pending: &mut Vec<Row>,
        unpublished: &mut Vec<String>,
        launched: Instant,
    ) {
        let elapsed = launched.elapsed();
        if !pending.iter().any(|row| elapsed >= self.phase.ready_for(row.family())) {
            return;
        }
        let Some(exposition) = self.subject.exposition().await else {
            return;
        };
        pending.retain(|row| {
            if exposition.resolves(row) {
                return false;
            }
            if elapsed < self.phase.ready_for(row.family()) {
                return true;
            }
            unpublished.push(format!("{} <- {}", row.label, row.family()));
            false
        });
    }

    fn unmeasured_work_error(&self, missing: &[Op], measured: OpSet) -> String {
        let named = |op: Op| match self.subject.work_source(op) {
            Some(series) => format!("  {} <- {series}", op.label()),
            None => format!("  {} <- (no series declared by this subject)", op.label()),
        };
        let measured: Vec<&str> =
            Op::ALL.into_iter().filter(|&op| measured.has(op)).map(Op::label).collect();
        format!(
            "the subject does not measure {} op(s) this profile requires:\n{}\n\
             it measures: {}\n\
             A probe reading an unmeasured op panics mid-run, so the run is refused here \
             instead. Either the component does not publish the series (check its /metrics \
             against the name above), or the profile should not have required the op.",
            missing.len(),
            missing.iter().map(|&op| named(op)).collect::<Vec<_>>().join("\n"),
            if measured.is_empty() { "nothing".to_owned() } else { measured.join(", ") },
        )
    }
}

/// Restart window with no snapshot this tick → still pause every `eventually` window
fn credit_liveness(probes: &mut [ProbeSpec], now: Instant) {
    for spec in probes.iter_mut().filter(|s| s.class == Class::Eventually) {
        spec.last_satisfied = Some(now);
    }
}

fn coverage_gaps(probes: &[ProbeSpec]) -> Vec<String> {
    probes
        .iter()
        .filter(|s| s.class == Class::Sometimes && !s.ever_satisfied)
        .map(|s| s.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::cancel::CancelSource;
    use crate::handles::{ContainerSample, PodError};

    use super::super::nemesis::Nemesis;
    use super::super::probe::{Verdict, Violation};
    use super::super::snapshot::Snapshot;
    use super::super::subject::SyncSubject;
    use super::super::work::Op;
    use super::*;

    #[derive(Clone, Debug)]
    struct FakeProgress {
        height: u32,
        target: u32,
        sapling: u64,
        orchard: u64,
        measured: OpSet,
    }
    impl ProgressView for FakeProgress {
        fn height(&self) -> u32 {
            self.height
        }
        fn target(&self) -> Option<u32> {
            Some(self.target)
        }
        fn pct(&self) -> f32 {
            0.0
        }
        fn work(&self) -> Option<Work> {
            let mut w = Work::ZERO;
            for (op, n) in [(Op::SaplingOutput, self.sapling), (Op::OrchardAction, self.orchard)] {
                if self.measured.has(op) {
                    w.set(op, n);
                }
            }
            Some(w)
        }
    }

    /// Yields a scripted sequence, then reports complete. `never_complete` holds `is_complete`
    /// false forever (stall/fail-fast tests) and clamps at the last reading.
    struct FakeSubject {
        script: Vec<FakeProgress>,
        cursor: AtomicUsize,
        never_complete: bool,
        stopped: Arc<AtomicUsize>,
        // `(family, n)` = exporter publishes `family` from its n-th scrape on
        published: Option<(Family, usize)>,
        gated: bool,
        scrapes: AtomicUsize,
        rows: &'static [Row],
        failure: Option<String>,
        // Set = progress() errors (component down) and the cursor holds
        down: Option<Arc<AtomicBool>>,
    }
    impl FakeSubject {
        fn new(script: Vec<FakeProgress>) -> Self {
            Self {
                script,
                cursor: AtomicUsize::new(0),
                never_complete: false,
                stopped: Arc::new(AtomicUsize::new(0)),
                published: None,
                gated: false,
                scrapes: AtomicUsize::new(0),
                rows: &[],
                failure: None,
                down: None,
            }
        }
        fn never_complete(mut self) -> Self {
            self.never_complete = true;
            self
        }
        fn gated_on(mut self, family: Family, from_scrape: usize) -> Self {
            self.published = Some((family, from_scrape));
            self.gated = true;
            self
        }
        fn publishing(mut self, family: Family, from_scrape: usize, rows: &'static [Row]) -> Self {
            self.published = Some((family, from_scrape));
            self.rows = rows;
            self
        }
    }
    #[async_trait]
    impl SyncSubject for FakeSubject {
        async fn launch(&mut self) -> Result<(), crate::RpcError> {
            Ok(())
        }
        async fn progress(&self) -> Result<Box<dyn ProgressView>, crate::RpcError> {
            if self.down.as_ref().is_some_and(|d| d.load(Ordering::SeqCst)) {
                return Err(crate::RpcError::Decode {
                    component: "fake",
                    op: "progress",
                    reason: "component down".into(),
                });
            }
            let i = self.cursor.fetch_add(1, Ordering::SeqCst);
            let idx = i.min(self.script.len() - 1);
            Ok(Box::new(self.script[idx].clone()))
        }
        async fn is_complete(&self) -> bool {
            !self.never_complete && self.cursor.load(Ordering::SeqCst) >= self.script.len()
        }
        async fn failure(&self) -> Option<String> {
            self.failure.clone()
        }
        async fn stop(&mut self) -> Result<(), crate::RpcError> {
            self.stopped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn rows(&self) -> &'static [Row] {
            self.rows
        }
        async fn exposition(&self) -> Option<crate::metrics::Exposition> {
            let n = self.scrapes.fetch_add(1, Ordering::SeqCst) + 1;
            let mut e = crate::metrics::Exposition::default();
            if let Some((family, from)) = self.published
                && n >= from
            {
                e.absorb(&format!("# TYPE {0} gauge\n{0} 1\n", family.name));
            }
            Some(e)
        }
        fn gates(&self) -> Vec<Family> {
            self.published.filter(|_| self.gated).map(|(f, _)| f).into_iter().collect()
        }
        fn work_source(&self, op: Op) -> Option<crate::metrics::Counter> {
            match op {
                Op::SaplingOutput => Some(crate::metrics::counter(
                    "fake_sapling_outputs_total",
                    crate::metrics::Dimension::Count,
                )),
                _ => None,
            }
        }
    }

    fn p(height: u32, target: u32) -> FakeProgress {
        p_measuring(height, target, OpSet::of(&[Op::SaplingOutput, Op::OrchardAction]))
    }

    fn p_measuring(height: u32, target: u32, measured: OpSet) -> FakeProgress {
        FakeProgress { height, target, sapling: u64::from(height), orchard: 0, measured }
    }

    fn height_monotonic(s: &Snapshot) -> Verdict {
        if s.height() >= s.prev_height() {
            Verdict::Satisfied
        } else {
            Verdict::Violated(Violation {
                probe: String::new(),
                height: Some(s.height()),
                detail: format!("height {} < prev {}", s.height(), s.prev_height()),
            })
        }
    }

    fn fast_runner(subject: impl SyncSubject + 'static) -> SyncEngine {
        SyncEngine::new(Box::new(subject)).with_tick(Duration::from_millis(10))
    }

    /// The failure this guards is a *silent cross-repo rename*: the subject stops publishing
    /// a series, the harness still asks for it, and the only symptom is a `Work::require`
    /// panic hours in, naming an `Op` but never the series a reader must go grep for.
    #[tokio::test(start_paused = true)]
    async fn preflight_refuses_a_run_requiring_an_unmeasured_op() {
        let orchard_only = OpSet::of(&[Op::OrchardAction]);
        let run = fast_runner(FakeSubject::new(vec![p_measuring(1, 3, orchard_only)]))
            .requires_work(OpSet::of(&[Op::SaplingOutput, Op::OrchardAction]));

        let outcome = run.run().await;

        assert_eq!(outcome.verdict, SyncVerdict::Errored);
        let error = outcome.error.expect("a refused run reports why");
        assert!(error.contains("sapling-output"), "{error}");
        // The series name is the whole point — an Op label alone is not greppable
        assert!(error.contains("fake_sapling_outputs_total"), "{error}");
        assert!(error.contains("orchard-action"), "measured ops belong in the report: {error}");
    }

    #[tokio::test(start_paused = true)]
    async fn preflight_admits_a_run_whose_required_ops_are_all_measured() {
        let run = fast_runner(FakeSubject::new(vec![p(1, 2), p(2, 2)]))
            .requires_work(OpSet::of(&[Op::SaplingOutput]));

        assert_eq!(run.run().await.verdict, SyncVerdict::Passed);
    }

    const LATE: crate::metrics::Gauge =
        crate::metrics::gauge("fake_committed_height", crate::metrics::Dimension::Count);
    const NEVER: crate::metrics::Gauge =
        crate::metrics::gauge("fake_never_height", crate::metrics::Dimension::Count);

    /// A family still inside its window = the subject starting, not a failed read
    #[tokio::test(start_paused = true)]
    async fn the_run_waits_for_a_gate_family_inside_its_window() {
        let subject = FakeSubject::new(vec![p(1, 2), p(2, 2)]).gated_on(LATE.family(), 5);
        assert_eq!(fast_runner(subject).run().await.verdict, SyncVerdict::Passed);
    }

    #[tokio::test(start_paused = true)]
    async fn a_gate_family_missing_past_its_window_errors_by_name() {
        let late = LATE.ready_within(Duration::from_millis(30)).family();
        let subject = FakeSubject::new(vec![p(1, 2)]).gated_on(late, 100);
        let out = fast_runner(subject).run().await;
        assert_eq!(out.verdict, SyncVerdict::Errored);
        let error = out.error.expect("a late gate reports why");
        assert!(error.contains("fake_committed_height"), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_profile_override_widens_the_catalogues_window() {
        let late = LATE.ready_within(Duration::from_millis(30)).family();
        let subject = FakeSubject::new(vec![p(1, 2), p(2, 2)]).gated_on(late, 10);
        let run = fast_runner(subject).with_ready(Family { ready: Duration::from_secs(1), ..late });
        assert_eq!(run.run().await.verdict, SyncVerdict::Passed);
    }

    /// Nothing judged at launch: present by its deadline → settled, absent at it → unpublished
    #[tokio::test(start_paused = true)]
    async fn a_row_is_judged_at_its_deadline_not_at_launch() {
        static ROWS: [Row; 2] = [
            crate::metrics::row(
                "late",
                LATE.ready_within(Duration::from_millis(20)).level(),
                crate::metrics::Facet::Progress,
            ),
            crate::metrics::row(
                "never",
                NEVER.ready_within(Duration::from_millis(20)).level(),
                crate::metrics::Facet::Progress,
            ),
        ];
        let script = (1..=10).map(|h| p(h, 10)).collect();
        let subject = FakeSubject::new(script).publishing(LATE.family(), 1, &ROWS);
        let out = fast_runner(subject).run().await;
        assert_eq!(out.verdict, SyncVerdict::Passed, "{out:?}");
        assert_eq!(out.unpublished, ["never <- fake_never_height"]);
    }

    /// Declaring nothing must not start requiring everything
    #[tokio::test(start_paused = true)]
    async fn preflight_is_inert_when_no_work_is_required() {
        let run = fast_runner(FakeSubject::new(vec![
            p_measuring(1, 2, OpSet::NONE),
            p_measuring(2, 2, OpSet::NONE),
        ]));

        assert_eq!(run.run().await.verdict, SyncVerdict::Passed);
    }

    #[tokio::test(start_paused = true)]
    async fn passes_when_height_monotonic_to_tip() {
        let mut run = fast_runner(FakeSubject::new(vec![p(1, 3), p(2, 3), p(3, 3)]));
        run.always(Severity::Fatal).each_tick().check(height_monotonic);
        run.at_completion(Severity::Fatal).check(|s: &Snapshot| {
            if s.target() == Some(s.height()) {
                Verdict::Satisfied
            } else {
                Verdict::Violated(Violation {
                    probe: String::new(),
                    height: Some(s.height()),
                    detail: "did not reach target".into(),
                })
            }
        });
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Passed, "{out:?}");
        assert!(out.violations.is_empty());
    }

    /// Stop resolved off the first reading, not registration (live tip unknown until the phase opens)
    #[tokio::test(start_paused = true)]
    async fn stop_after_counts_from_the_first_reading_and_ignores_the_subjects_own_completion() {
        let script = (10..=20).map(|h| p(h, 20)).collect();
        let mut run = fast_runner(FakeSubject::new(script).never_complete()).with_stop_after(3);
        let finished_at = Arc::new(AtomicUsize::new(0));
        let seen = finished_at.clone();
        run.at_completion(Severity::Fatal).check(move |s: &Snapshot| {
            seen.store(s.height() as usize, Ordering::SeqCst);
            Verdict::Satisfied
        });

        let out = run.run().await;

        assert_eq!(out.verdict, SyncVerdict::Passed, "{out:?}");
        assert_eq!(
            finished_at.load(Ordering::SeqCst),
            14,
            "stop at 10 (first reading) + 3, then one fresh read for the final snapshot"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_violation_fails_fast_and_stops() {
        // Height regresses at the 3rd reading → monotonic violation, fatal
        let subject = FakeSubject::new(vec![p(1, 5), p(2, 5), p(1, 5), p(3, 5), p(5, 5)]);
        let stopped = subject.stopped.clone();
        let mut run = fast_runner(subject);
        run.always(Severity::Fatal).each_tick().check(height_monotonic);
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Failed, "{out:?}");
        assert_eq!(out.violations.len(), 1);
        assert!(out.ticks < 5, "should have stopped before the script end");
        assert_eq!(stopped.load(Ordering::SeqCst), 1, "stop() must be called");
    }

    #[tokio::test(start_paused = true)]
    async fn recorded_violation_does_not_stop() {
        let subject = FakeSubject::new(vec![p(1, 3), p(1, 3), p(3, 3)]);
        // Pool-output decrease = a Recorded (non-fatal) violation here
        let mut run = fast_runner(subject);
        run.always(Severity::Recorded).each_tick().check(|s: &Snapshot| {
            if s.work().require(Op::OrchardAction) >= s.prev_work().require(Op::OrchardAction) {
                Verdict::Satisfied
            } else {
                Verdict::Violated(Violation {
                    probe: String::new(),
                    height: None,
                    detail: "orchard outputs went backwards".into(),
                })
            }
        });
        // orchard always 0 → never violated; run passes and reaches tip
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Passed, "{out:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn sometimes_gap_fails_the_run() {
        let mut run = fast_runner(FakeSubject::new(vec![p(1, 2), p(2, 2)]));
        run.sometimes().named("saw_reorg").check(|s: &Snapshot| {
            if s.observed_reorg() { Verdict::Satisfied } else { Verdict::Pending }
        });
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Failed, "{out:?}");
        assert_eq!(out.coverage_gaps, vec!["saw_reorg".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn sometimes_satisfied_passes() {
        // Height dips → observed_reorg latches true → coverage satisfied
        let mut run = fast_runner(FakeSubject::new(vec![p(2, 4), p(1, 4), p(3, 4), p(4, 4)]));
        run.sometimes().named("saw_reorg").check(|s: &Snapshot| {
            if s.observed_reorg() { Verdict::Satisfied } else { Verdict::Pending }
        });
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Passed, "{out:?}");
        assert!(out.coverage_gaps.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn probe_error_aborts_distinctly() {
        let mut run = fast_runner(FakeSubject::new(vec![p(1, 2), p(2, 2)]));
        run.always(Severity::Fatal)
            .each_tick()
            .check(|_s: &Snapshot| Verdict::ProbeError("rpc broke".into()));
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Errored, "{out:?}");
        assert!(out.error.unwrap().contains("rpc broke"));
    }

    #[tokio::test(start_paused = true)]
    async fn every_blocks_cadence_fires_on_height_delta() {
        let fired = Arc::new(AtomicUsize::new(0));
        let f = fired.clone();
        // Heights 0,2,4,6,8,10 across ticks → every_blocks(5) fires at the first tick, again
        // once ≥5 blocks have passed
        let script: Vec<_> = (0..=5).map(|i| p(i * 2, 10)).collect();
        let mut run = fast_runner(FakeSubject::new(script));
        run.always(Severity::Recorded).every_blocks(5).check(move |_s: &Snapshot| {
            f.fetch_add(1, Ordering::SeqCst);
            Verdict::Satisfied
        });
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Passed, "{out:?}");
        // Height 0, then ≥5 (height 6); next would be ≥11, but the script stops at 10 → 2
        // fires, 2..=3 for boundary tolerance
        let n = fired.load(Ordering::SeqCst);
        assert!((2..=3).contains(&n), "every_blocks fired {n} times");
    }

    #[tokio::test(start_paused = true)]
    async fn eventually_stall_fails() {
        // Height never advances, subject never completes → no_stall fires once its window
        // elapses
        let subject = FakeSubject::new(vec![p(1, 9)]).never_complete();
        let mut run = fast_runner(subject);
        run.eventually(Severity::Fatal).window(Duration::from_millis(50)).check(|s: &Snapshot| {
            if s.progressed_within(Duration::from_millis(50)) {
                Verdict::Satisfied
            } else {
                Verdict::Pending
            }
        });
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Failed, "{out:?}");
        assert!(out.violations.iter().any(|v| v.detail.contains("stall")));
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_terminates() {
        let (src, cancel) = CancelSource::new();
        let subject = FakeSubject::new(vec![p(1, 9)]).never_complete();
        let mut run = fast_runner(subject).with_cancel(cancel);
        run.always(Severity::Fatal).each_tick().check(height_monotonic);
        // Cancellation from another task, shortly after start
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            src.cancel();
        });
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Cancelled, "{out:?}");
    }

    fn violated(detail: &str) -> Verdict {
        Verdict::Violated(Violation { probe: String::new(), height: None, detail: detail.into() })
    }

    /// Phase 2 ("wallet") built lazily; its factory flips `built`
    fn wallet_phase(built: Arc<AtomicBool>) -> Phase {
        let mut wallet = Phase::deferred("wallet", move |_cx: SyncCtx| async move {
            built.store(true, Ordering::SeqCst);
            Ok(FakeSubject::new(vec![p(10, 12), p(12, 12)]))
        });
        wallet.tick(Duration::from_millis(10));
        wallet
    }

    /// Every way phase 1 can end short of a clean completion → phase 2's subject never built.
    /// A recorded (non-fatal) violation is the one outcome that lets phase 2 run
    #[tokio::test(start_paused = true)]
    async fn phase_two_starts_only_after_a_fatal_free_completion() {
        type Register = fn(&mut SyncEngine);
        let cases: [(&str, Register, Option<&str>, SyncVerdict, bool); 5] = [
            (
                "fatal always",
                |r| r.always(Severity::Fatal).named("never").check(|_: &Snapshot| violated("x")),
                None,
                SyncVerdict::Failed,
                false,
            ),
            (
                "fatal at_completion",
                |r| r.at_completion(Severity::Fatal).check(|_: &Snapshot| violated("root")),
                None,
                SyncVerdict::Failed,
                false,
            ),
            (
                "coverage gap",
                |r| r.sometimes().named("saw_reorg").check(|_: &Snapshot| Verdict::Pending),
                None,
                SyncVerdict::Failed,
                false,
            ),
            (
                "subject failure",
                |_| {},
                Some("scan task died: disk full"),
                SyncVerdict::Failed,
                false,
            ),
            (
                "recorded violation",
                |r| r.always(Severity::Recorded).check(|_: &Snapshot| violated("noisy")),
                None,
                SyncVerdict::Failed,
                true,
            ),
        ];
        for (case, register, failure, verdict, wallet_runs) in cases {
            let built = Arc::new(AtomicBool::new(false));
            let mut subject = FakeSubject::new(vec![p(1, 3), p(3, 3)]);
            subject.failure = failure.map(Into::into);
            let stopped = subject.stopped.clone();
            let mut run = fast_runner(subject).then(wallet_phase(built.clone()));
            register(&mut run);

            let out = run.run().await;

            assert_eq!(out.verdict, verdict, "{case}: {out:?}");
            assert_eq!(built.load(Ordering::SeqCst), wallet_runs, "{case}: wallet factory");
            let names: Vec<_> = out.phases.iter().map(|p| p.name.as_str()).collect();
            let want: &[&str] = if wallet_runs { &["sync", "wallet"] } else { &["sync"] };
            assert_eq!(names, want, "{case}");
            assert_eq!(out.phases[0].verdict, verdict, "{case}");
            if let Some(msg) = failure {
                assert_eq!(out.error.as_deref(), Some(msg), "{case}: subject's own error text");
                assert_eq!(stopped.load(Ordering::SeqCst), 1, "{case}: failed subject stopped");
            }
        }
    }

    /// Probes belong to their phase; each phase clocks its own timeout; the factory sees ctx
    #[tokio::test(start_paused = true)]
    async fn phases_are_scoped_sequential_and_individually_timed() {
        let phase1_evals = Arc::new(AtomicUsize::new(0));
        let phase2_evals = Arc::new(AtomicUsize::new(0));
        let saw_ctx = Arc::new(AtomicBool::new(false));

        let mut run = fast_runner(FakeSubject::new(vec![p(1, 3), p(2, 3), p(3, 3)]))
            .with_timeout(Duration::from_secs(60));
        let n1 = phase1_evals.clone();
        run.always(Severity::Fatal).check(move |_: &Snapshot| {
            n1.fetch_add(1, Ordering::SeqCst);
            Verdict::Satisfied
        });
        let seen = saw_ctx.clone();
        let mut wallet = Phase::deferred("wallet", move |cx: SyncCtx| async move {
            seen.store(cx.indexer().is_none() && cx.pods().is_empty(), Ordering::SeqCst);
            Ok(FakeSubject::new(vec![p(5, 9)]).never_complete())
        });
        wallet.tick(Duration::from_millis(10)).timeout(Duration::from_millis(100));
        let n2 = phase2_evals.clone();
        wallet.always(Severity::Fatal).check(move |_: &Snapshot| {
            n2.fetch_add(1, Ordering::SeqCst);
            Verdict::Satisfied
        });

        let out = run.then(wallet).run().await;

        assert_eq!(out.verdict, SyncVerdict::TimedOut, "{out:?}");
        let verdicts: Vec<_> = out.phases.iter().map(|p| (p.name.as_str(), p.verdict)).collect();
        assert_eq!(verdicts, [("sync", SyncVerdict::Passed), ("wallet", SyncVerdict::TimedOut)]);
        assert_eq!(phase1_evals.load(Ordering::SeqCst), 3, "phase-1 probe ran only in phase 1");
        let wallet_evals = phase2_evals.load(Ordering::SeqCst);
        assert!((8..=11).contains(&wallet_evals), "phase-2 probe each tick: {wallet_evals}");
        assert!((100..=120).contains(&out.phases[1].elapsed_ms), "{:?}", out.phases[1]);
        assert!(out.phases[1].started_ms >= out.phases[0].started_ms);
        assert!(saw_ctx.load(Ordering::SeqCst), "factory receives the run's ctx");
        assert_eq!(out.ticks, out.phases[0].ticks + out.phases[1].ticks);
        assert_eq!(out.segment.as_ref().map(|s| (s.from, s.to)), Some((1, 3)), "phase-1 span");
    }

    /// Component killed + restarted in place: down for a few samples, then back one count up
    #[derive(Debug)]
    struct FakePod {
        down: Arc<AtomicBool>,
        down_samples: AtomicUsize,
        restarts: AtomicUsize,
        kills: AtomicUsize,
    }
    #[async_trait]
    impl Watched for FakePod {
        fn answers_to(&self, name: &str) -> bool {
            name == "zai"
        }
        async fn sample(&self) -> Result<ContainerSample, PodError> {
            if self.down.load(Ordering::SeqCst)
                && self.down_samples.fetch_add(1, Ordering::SeqCst) >= 5
            {
                self.down.store(false, Ordering::SeqCst);
                self.restarts.fetch_add(1, Ordering::SeqCst);
            }
            let restarts = self.restarts.load(Ordering::SeqCst) as u32;
            Ok(ContainerSample { restarts, ready: !self.down.load(Ordering::SeqCst) })
        }
        async fn kill(&self) -> Result<ContainerSample, PodError> {
            self.kills.fetch_add(1, Ordering::SeqCst);
            self.down.store(true, Ordering::SeqCst);
            Ok(ContainerSample {
                restarts: self.restarts.load(Ordering::SeqCst) as u32,
                ready: true,
            })
        }
        async fn ensure_restartable(&self) -> Result<(), PodError> {
            Ok(())
        }
    }

    /// Kill fault fires on the run clock, the outage never reads as a stall, `.after` probes
    /// arm at the fault, the restart lands in the snapshot + phase outcome
    #[tokio::test(start_paused = true)]
    async fn a_kill_fault_restarts_the_component_without_tripping_liveness() {
        let down = Arc::new(AtomicBool::new(false));
        let pod = Arc::new(FakePod {
            down: down.clone(),
            down_samples: AtomicUsize::new(0),
            restarts: AtomicUsize::new(2),
            kills: AtomicUsize::new(0),
        });
        let mut subject = FakeSubject::new((1..=12).map(|h| p(h, 12)).collect());
        subject.down = Some(down);
        let mut nemesis = Nemesis::default();
        nemesis.builder().named("crash").at(Duration::from_millis(35)).kill("zai");
        let watched: Vec<Arc<dyn Watched>> = vec![pod.clone()];
        let mut run = fast_runner(subject).with_nemesis(&nemesis).with_watched(watched);
        run.eventually(Severity::Fatal).named("no_stall").window(Duration::from_millis(25)).check(
            |s: &Snapshot| {
                if s.progressed_within(Duration::from_millis(15)) {
                    Verdict::Satisfied
                } else {
                    Verdict::Pending
                }
            },
        );
        let armed_at = Arc::new(AtomicUsize::new(usize::MAX));
        let armed = armed_at.clone();
        run.eventually(Severity::Fatal).after("crash").check(move |s: &Snapshot| {
            armed.fetch_min(s.seq() as usize, Ordering::SeqCst);
            if s.progressed_since_fault() { Verdict::Satisfied } else { Verdict::Pending }
        });
        run.sometimes().named("saw_restart").check(|s: &Snapshot| {
            if s.observed_restart() { Verdict::Satisfied } else { Verdict::Pending }
        });

        let out = run.run().await;

        assert_eq!(out.verdict, SyncVerdict::Passed, "{out:?}");
        assert_eq!(pod.kills.load(Ordering::SeqCst), 1);
        assert_eq!(out.phases[0].restarts, 1, "pre-existing restarts are the baseline");
        let armed = armed_at.load(Ordering::SeqCst);
        // seq 0..=3 land before the kill at 40 ms (35 ms offset, 10 ms ticks)
        assert!((4..usize::MAX).contains(&armed), ".after probe armed post-kill, seq {armed}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_kill_naming_no_watched_component_is_refused_before_the_run() {
        let mut nemesis = Nemesis::default();
        nemesis.builder().at(Duration::from_secs(60)).kill("zeb");
        let out = fast_runner(FakeSubject::new(vec![p(1, 2)])).with_nemesis(&nemesis).run().await;
        assert_eq!(out.verdict, SyncVerdict::Errored);
        assert!(out.phases.is_empty(), "{out:?}");
        let error = out.error.expect("refusal names the component");
        assert!(error.contains("\"zeb\""), "{error}");
    }
}
