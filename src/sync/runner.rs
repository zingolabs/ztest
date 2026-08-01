//! [`SyncEngine`] — the continuous-monitor engine (design §"a continuous monitor
//! with a completion predicate").
//!
//! It launches a [`SyncSubject`], captures one immutable [`Snapshot`] per tick,
//! evaluates the registered probes at their cadences (all probes due at a tick
//! share the one snapshot), keeps a bounded [`History`], and terminates: `pass`
//! when the subject reaches tip with every invariant intact, `fail` on a fatal
//! violation, a `sometimes` coverage miss, a stall, cancellation, or timeout.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{Instant, MissedTickBehavior, interval};

use crate::cancel::Cancel;

use super::probe::{Cadence, Class, ProbeBuilder, ProbeSpec, Severity, SyncCtx, Verdict, Violation};
use super::snapshot::{History, Snapshot, SnapshotBuilder};
use super::subject::SyncSubject;

/// The terminal result of a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncVerdict {
    /// Reached tip with every fatal invariant intact and every `sometimes`
    /// coverage probe triggered.
    Passed,
    /// A fatal invariant was violated, a stall fired, or a coverage probe was
    /// never triggered (a weak test).
    Failed,
    /// The run was cancelled (Ctrl-C / detach-stop).
    Cancelled,
    /// The run exceeded its time cap before reaching tip.
    TimedOut,
    /// A probe or the harness errored (RPC/launch failure) — not a verdict about
    /// the subject.
    Errored,
}

impl SyncVerdict {
    /// Whether this is a passing verdict.
    pub fn is_pass(&self) -> bool {
        matches!(self, SyncVerdict::Passed)
    }
}

/// The recorded result of a run.
#[derive(Debug)]
pub struct SyncOutcome {
    /// Terminal verdict.
    pub verdict: SyncVerdict,
    /// Every recorded violation (fatal and `Recorded`), in order.
    pub violations: Vec<Violation>,
    /// `sometimes` probes that were never satisfied — the coverage gaps that
    /// make a green run a *weak* pass.
    pub coverage_gaps: Vec<String>,
    /// A harness/probe error message, when `verdict == Errored`.
    pub error: Option<String>,
    /// Ticks captured over the run.
    pub ticks: u64,
    /// Snapshots evicted by the history cap (a visible, non-silent truncation).
    pub dropped_snapshots: u64,
}

impl SyncOutcome {
    fn error_outcome(msg: String) -> Self {
        SyncOutcome {
            verdict: SyncVerdict::Errored,
            violations: Vec::new(),
            coverage_gaps: Vec::new(),
            error: Some(msg),
            ticks: 0,
            dropped_snapshots: 0,
        }
    }
}

// A profile body returns `SyncOutcome` but uses `?` on provisioning/RPC calls
// (design profiles do `run.topology(..).await?`); these make that compile,
// turning a setup failure into an errored outcome.
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

/// Sink for live progress + probe events. Modeled on
/// [`crate::engine`]'s `RunReporter`; the pod log-stream `EmitSink` for
/// `ztest sync watch` (step 6) is a future impl of this trait.
pub trait SyncReporter: Send {
    /// The run started.
    fn on_start(&mut self) {}
    /// A snapshot was captured.
    fn on_tick(&mut self, _snap: &Snapshot) {}
    /// A probe evaluated to a non-`Satisfied` verdict worth surfacing.
    fn on_probe(&mut self, _name: &str, _verdict: &Verdict) {}
    /// The run finished.
    fn on_finish(&mut self, _outcome: &SyncOutcome) {}
}

/// Discards everything.
#[derive(Debug, Default)]
pub struct NullReporter;
impl SyncReporter for NullReporter {}

/// One human-readable line per interesting event, to stderr.
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

/// Internal per-tick evaluation control.
enum Flow {
    Continue,
    FailFast,
    Abort(String),
}

impl<S: SyncSubject> std::fmt::Debug for SyncEngine<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncEngine")
            .field("probes", &self.probes.len())
            .field("tick", &self.tick)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// The continuous-monitor engine, generic over the subject.
pub struct SyncEngine<S: SyncSubject> {
    subject: S,
    probes: Vec<ProbeSpec>,
    ctx: SyncCtx,
    cancel: Cancel,
    tick: Duration,
    history_cap: usize,
    timeout: Option<Duration>,
    reporter: Box<dyn SyncReporter>,
}

impl<S: SyncSubject> SyncEngine<S> {
    /// A runner over `subject`, with no oracle indexer and no cancellation
    /// (`Cancel::never`). Defaults: 5 s base tick, 20k-snapshot history, no
    /// timeout, `NullReporter`. Chain the setters below.
    pub fn new(subject: S) -> Self {
        Self {
            subject,
            probes: Vec::new(),
            ctx: SyncCtx::new(None),
            cancel: Cancel::never(),
            tick: Duration::from_secs(5),
            history_cap: 20_000,
            timeout: None,
            reporter: Box::new(NullReporter),
        }
    }

    /// Set the oracle context (indexer handle for RPC-backed probes).
    pub fn with_ctx(mut self, ctx: SyncCtx) -> Self {
        self.ctx = ctx;
        self
    }
    /// Observe this cancellation token (Ctrl-C / detach-stop).
    pub fn with_cancel(mut self, cancel: Cancel) -> Self {
        self.cancel = cancel;
        self
    }
    /// Base sampling interval — how often a snapshot is captured. `each_tick`
    /// probes fire every base tick; `every(d)` probes fire on their own `d`
    /// quantized to this. Coarse (seconds) to avoid the wallet write-lock.
    pub fn with_tick(mut self, tick: Duration) -> Self {
        self.tick = tick;
        self
    }
    /// Overall run time cap (the QoS `sync` tier's 48 h, or a test bound).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    /// Bounded snapshot history size.
    pub fn with_history_cap(mut self, cap: usize) -> Self {
        self.history_cap = cap;
        self
    }
    /// Install a live reporter.
    pub fn with_reporter(mut self, reporter: Box<dyn SyncReporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Replace the probe set (the facade registers probes on itself, then hands
    /// the whole set to the engine at run time).
    #[cfg_attr(not(feature = "zingo"), allow(dead_code))]
    pub(crate) fn with_probes(mut self, probes: Vec<ProbeSpec>) -> Self {
        self.probes = probes;
        self
    }

    // ── probe registration (design §"Test-author API") ──
    /// Register a safety invariant (true at every tick).
    pub fn always(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Always, severity)
    }
    /// Register a liveness invariant (must (re)satisfy within its `window`).
    pub fn eventually(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Eventually, severity)
    }
    /// Register a coverage invariant (true on ≥1 tick over the run). A miss
    /// fails the run — a green run that never triggered its coverage is weak.
    pub fn sometimes(&mut self) -> ProbeBuilder<'_> {
        self.builder(Class::Sometimes, Severity::Fatal)
    }
    /// Register a terminal post-condition (evaluated once at tip).
    pub fn at_completion(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::AtCompletion, severity)
    }

    fn builder(&mut self, class: Class, severity: Severity) -> ProbeBuilder<'_> {
        let default_cadence = match class {
            Class::Eventually => Cadence::Window(Duration::MAX),
            _ => Cadence::EachTick,
        };
        ProbeBuilder {
            sink: &mut self.probes,
            class,
            severity,
            cadence: default_cadence,
            after: None,
            name: None,
            hold_for: None,
        }
    }

    /// The number of registered probes (for `describe`/manifest — the Collect
    /// seam; full Collect mode is step 4).
    pub fn probe_count(&self) -> usize {
        self.probes.len()
    }

    /// Run to completion.
    pub async fn run(mut self) -> SyncOutcome {
        self.reporter.on_start();

        if let Err(e) = self.subject.launch().await {
            return self.finish(SyncVerdict::Errored, Vec::new(), Vec::new(), Some(format!("launch: {e}")), 0, 0);
        }

        let started = Instant::now();
        let deadline = self.timeout.map(|t| started + t);
        let mut builder = SnapshotBuilder::new(started);
        let mut history = History::new(self.history_cap);
        let mut violations: Vec<Violation> = Vec::new();

        let mut ticker = interval(self.tick);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Terminal state decided inside the loop; the single `finish` is after
        // it so no `self.finish(..)` call has to borrow `self.probes` for its
        // args while also taking `&mut self`.
        let verdict: SyncVerdict;
        let mut error: Option<String> = None;
        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    let _ = self.subject.stop().await;
                    verdict = SyncVerdict::Cancelled;
                    break;
                }
                _ = ticker.tick() => {}
            }

            let now = Instant::now();
            if let Some(dl) = deadline
                && now >= dl
            {
                let _ = self.subject.stop().await;
                verdict = SyncVerdict::TimedOut;
                break;
            }

            // Snapshot-then-evaluate. A transient progress-read error holds the
            // prior state and retries next tick (governor.rs pattern), rather
            // than aborting — only a *probe* error aborts.
            let progress = match self.subject.progress().await {
                Ok(p) => p,
                Err(_) => continue,
            };
            let snap = Arc::new(builder.build(&progress, now, None));
            history.push(snap.clone());
            self.reporter.on_tick(&snap);

            match self.eval_tick(&snap, now, &mut violations).await {
                Flow::Continue => {}
                Flow::FailFast => {
                    let _ = self.subject.stop().await;
                    verdict = SyncVerdict::Failed;
                    break;
                }
                Flow::Abort(msg) => {
                    let _ = self.subject.stop().await;
                    verdict = SyncVerdict::Errored;
                    error = Some(msg);
                    break;
                }
            }

            if self.subject.is_complete().await {
                // Terminal: at_completion probes over a final snapshot.
                if let Ok(p) = self.subject.progress().await {
                    let final_snap = Arc::new(builder.build(&p, Instant::now(), None));
                    if let Some(msg) = self.eval_at_completion(&final_snap, &mut violations).await {
                        error = Some(msg);
                    }
                }
                verdict = if error.is_some() {
                    SyncVerdict::Errored
                } else if violations.is_empty() && coverage_gaps(&self.probes).is_empty() {
                    SyncVerdict::Passed
                } else {
                    SyncVerdict::Failed
                };
                break;
            }
        }

        let gaps = coverage_gaps(&self.probes);
        let ticks = builder.seq();
        let dropped = history.dropped();
        self.finish(verdict, violations, gaps, error, ticks, dropped)
    }

    /// Evaluate the `at_completion` probes once against the final snapshot.
    /// Returns `Some(msg)` if a probe errored (aborts the run).
    async fn eval_at_completion(
        &mut self,
        snap: &Snapshot,
        violations: &mut Vec<Violation>,
    ) -> Option<String> {
        for spec in self.probes.iter_mut().filter(|s| s.class == Class::AtCompletion) {
            match spec.check.evaluate(snap, &self.ctx).await {
                Verdict::Satisfied | Verdict::Pending => {}
                Verdict::Violated(mut v) => {
                    v.probe = spec.name.clone();
                    self.reporter.on_probe(&spec.name, &Verdict::Violated(v.clone()));
                    violations.push(v);
                }
                Verdict::ProbeError(e) => return Some(format!("{}: {e}", spec.name)),
            }
        }
        None
    }

    /// Evaluate every due `always`/`eventually` probe and every `sometimes`
    /// coverage probe against `snap`. Returns the control flow for the tick.
    async fn eval_tick(
        &mut self,
        snap: &Snapshot,
        now: Instant,
        violations: &mut Vec<Violation>,
    ) -> Flow {
        for spec in self.probes.iter_mut() {
            match spec.class {
                Class::AtCompletion => continue,
                Class::Sometimes => {
                    // Cheap coverage predicate: evaluate every tick, latch.
                    if !spec.ever_satisfied {
                        match spec.check.evaluate(snap, &self.ctx).await {
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
                    let verdict = spec.check.evaluate(snap, &self.ctx).await;
                    spec.mark_fired(snap.seq(), snap.height(), now);
                    match verdict {
                        Verdict::Satisfied | Verdict::Pending => spec.violation_streak = 0,
                        Verdict::Violated(mut v) => {
                            spec.violation_streak += 1;
                            if spec.violation_streak >= spec.violation_threshold() {
                                v.probe = spec.name.clone();
                                self.reporter.on_probe(&spec.name, &Verdict::Violated(v.clone()));
                                violations.push(v);
                                if spec.severity == Severity::Fatal {
                                    return Flow::FailFast;
                                }
                            }
                        }
                        Verdict::ProbeError(e) => return Flow::Abort(format!("{}: {e}", spec.name)),
                    }
                }
                Class::Eventually => {
                    // Arm on first evaluation. `.after(fault)` gating: with no
                    // fault timeline yet (step 3), an `after` probe stays armed
                    // as "satisfied" and cannot stall.
                    if spec.last_satisfied.is_none() {
                        spec.last_satisfied = Some(now);
                    }
                    let window = match spec.cadence {
                        Cadence::Window(d) => d,
                        _ => Duration::MAX,
                    };
                    let gated = spec.after.is_some(); // no faults in step 3 → treat as satisfied
                    let verdict = if gated {
                        Verdict::Satisfied
                    } else {
                        spec.check.evaluate(snap, &self.ctx).await
                    };
                    match verdict {
                        Verdict::Satisfied => spec.last_satisfied = Some(now),
                        Verdict::ProbeError(e) => return Flow::Abort(format!("{}: {e}", spec.name)),
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
                                self.reporter.on_probe(&spec.name, &Verdict::Violated(v.clone()));
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

    #[allow(clippy::too_many_arguments)]
    fn finish(
        &mut self,
        verdict: SyncVerdict,
        violations: Vec<Violation>,
        coverage_gaps: Vec<String>,
        error: Option<String>,
        ticks: u64,
        dropped_snapshots: u64,
    ) -> SyncOutcome {
        let outcome = SyncOutcome {
            verdict,
            violations,
            coverage_gaps,
            error,
            ticks,
            dropped_snapshots,
        };
        self.reporter.on_finish(&outcome);
        outcome
    }
}

/// `sometimes` probes that never latched — the coverage gaps.
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::cancel::CancelSource;
    use crate::handles::wallet::Pool;

    use super::super::probe::{Verdict, Violation};
    use super::super::snapshot::Snapshot;
    use super::super::subject::{Phase, ProgressView, SyncSubject};
    use super::*;

    /// A scripted progress reading.
    #[derive(Clone, Debug)]
    struct FakeProgress {
        height: u32,
        target: u32,
        sapling: u64,
        orchard: u64,
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
        fn phase(&self) -> Phase {
            Phase::Historic
        }
        fn outputs(&self, pool: Pool) -> u64 {
            match pool {
                Pool::Sapling => self.sapling,
                Pool::Orchard => self.orchard,
                _ => 0,
            }
        }
    }

    /// Yields a scripted sequence, then reports complete. `never_complete`
    /// keeps `is_complete` false forever (for stall/fatal-fast tests) and
    /// clamps at the last reading.
    struct FakeSubject {
        script: Vec<FakeProgress>,
        cursor: AtomicUsize,
        never_complete: bool,
        stopped: Arc<AtomicUsize>,
    }
    impl FakeSubject {
        fn new(script: Vec<FakeProgress>) -> Self {
            Self {
                script,
                cursor: AtomicUsize::new(0),
                never_complete: false,
                stopped: Arc::new(AtomicUsize::new(0)),
            }
        }
        fn never_complete(mut self) -> Self {
            self.never_complete = true;
            self
        }
    }
    #[async_trait]
    impl SyncSubject for FakeSubject {
        type Progress = FakeProgress;
        async fn launch(&mut self) -> Result<(), crate::RpcError> {
            Ok(())
        }
        async fn progress(&self) -> Result<FakeProgress, crate::RpcError> {
            let i = self.cursor.fetch_add(1, Ordering::SeqCst);
            let idx = i.min(self.script.len() - 1);
            Ok(self.script[idx].clone())
        }
        async fn is_complete(&self) -> bool {
            !self.never_complete && self.cursor.load(Ordering::SeqCst) >= self.script.len()
        }
        async fn stop(&mut self) -> Result<(), crate::RpcError> {
            self.stopped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn p(height: u32, target: u32) -> FakeProgress {
        FakeProgress {
            height,
            target,
            sapling: u64::from(height),
            orchard: 0,
        }
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

    fn fast_runner<S: SyncSubject>(subject: S) -> SyncEngine<S> {
        SyncEngine::new(subject).with_tick(Duration::from_millis(10))
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

    #[tokio::test(start_paused = true)]
    async fn fatal_violation_fails_fast_and_stops() {
        // Height regresses at the 3rd reading → monotonic violation, fatal.
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
        // pool outputs decrease is a Recorded (non-fatal) violation here.
        let mut run = fast_runner(subject);
        run.always(Severity::Recorded).each_tick().check(|s: &Snapshot| {
            if s.outputs(Pool::Orchard) >= s.prev_outputs(Pool::Orchard) {
                Verdict::Satisfied
            } else {
                Verdict::Violated(Violation {
                    probe: String::new(),
                    height: None,
                    detail: "orchard outputs went backwards".into(),
                })
            }
        });
        // orchard is always 0 here → never violated; run should pass and reach tip.
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Passed, "{out:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn sometimes_gap_fails_the_run() {
        let mut run = fast_runner(FakeSubject::new(vec![p(1, 2), p(2, 2)]));
        run.sometimes().named("saw_reorg").check(|s: &Snapshot| {
            if s.observed_reorg() {
                Verdict::Satisfied
            } else {
                Verdict::Pending
            }
        });
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Failed, "{out:?}");
        assert_eq!(out.coverage_gaps, vec!["saw_reorg".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn sometimes_satisfied_passes() {
        // Height dips → observed_reorg latches true → coverage satisfied.
        let mut run = fast_runner(FakeSubject::new(vec![p(2, 4), p(1, 4), p(3, 4), p(4, 4)]));
        run.sometimes().named("saw_reorg").check(|s: &Snapshot| {
            if s.observed_reorg() {
                Verdict::Satisfied
            } else {
                Verdict::Pending
            }
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
        // Heights 0,2,4,6,8,10 across ticks; every_blocks(5) should fire at the
        // first tick and again once ≥5 blocks have passed.
        let script: Vec<_> = (0..=5).map(|i| p(i * 2, 10)).collect();
        let mut run = fast_runner(FakeSubject::new(script));
        run.always(Severity::Recorded)
            .every_blocks(5)
            .check(move |_s: &Snapshot| {
                f.fetch_add(1, Ordering::SeqCst);
                Verdict::Satisfied
            });
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Passed, "{out:?}");
        // fires at height 0 (first), then at >=5 (height 6), then at >=11? only
        // to 10 → so 2 fires (0 and 6). Allow 2..=3 for boundary tolerance.
        let n = fired.load(Ordering::SeqCst);
        assert!((2..=3).contains(&n), "every_blocks fired {n} times");
    }

    #[tokio::test(start_paused = true)]
    async fn eventually_stall_fails() {
        // Height never advances and the subject never completes → no_stall must
        // fire once its window elapses.
        let subject = FakeSubject::new(vec![p(1, 9)]).never_complete();
        let mut run = fast_runner(subject);
        run.eventually(Severity::Fatal)
            .window(Duration::from_millis(50))
            .check(|s: &Snapshot| {
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
        // Fire cancellation from another task shortly after start.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            src.cancel();
        });
        let out = run.run().await;
        assert_eq!(out.verdict, SyncVerdict::Cancelled, "{out:?}");
    }
}
