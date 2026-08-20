//! [`SyncEngine`] — continuous monitor with a completion predicate (per design).
//!
//! - Launches a [`SyncSubject`], one immutable [`Snapshot`] per tick, bounded [`History`]
//! - Probes evaluated at their cadences (all due at a tick share the one snapshot)
//! - `pass` = tip reached, invariants intact; `fail` = fatal violation / coverage miss / stall
//!   / cancellation / timeout

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::{Instant, MissedTickBehavior, interval};

use crate::cancel::Cancel;

use super::chainwork::ChainWork;
use super::probe::{
    Cadence, Class, ProbeBuilder, ProbeSpec, ProbeStatus, Severity, SyncCtx, Verdict, Violation,
};
use super::snapshot::{History, Snapshot, SnapshotBuilder};
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

/// Recorded result of a run.
///
/// - `coverage_gaps` = `sometimes` probes never satisfied (green run → only a *weak* pass)
/// - `segment` = the precondition `perf --base` checks before comparing two runs; `None` when
///   no tick ever landed
#[derive(Debug)]
pub struct SyncOutcome {
    pub verdict: SyncVerdict,
    pub violations: Vec<Violation>,
    pub coverage_gaps: Vec<String>,
    pub error: Option<String>,
    pub ticks: u64,
    pub dropped_snapshots: u64,
    pub segment: Option<Segment>,
}

/// One end of the span a run covered ([`Segment`] = last mark - first)
#[derive(Clone, Copy)]
struct Mark {
    height: u32,
    work: Work,
    at: Instant,
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
            segment: None,
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

/// Sink for live progress + probe events.
pub trait SyncReporter: Send {
    fn on_start(&mut self) {}
    fn on_tick(&mut self, _snap: &Snapshot) {}
    /// Probe evaluated to a non-`Satisfied` verdict worth surfacing
    fn on_probe(&mut self, _name: &str, _verdict: &Verdict) {}
    /// Standing board: every probe's live state once per tick, vs
    /// [`on_probe`](Self::on_probe)'s edge events
    fn on_probes(&mut self, _snap: &Snapshot, _board: &[ProbeStatus]) {}
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

impl std::fmt::Debug for SyncEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncEngine")
            .field("probes", &self.probes.len())
            .field("tick", &self.tick)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// Subject is boxed, not a type parameter: every profile binds one dynamically, and a
/// generic engine would push that parameter into every caller for no gain
pub struct SyncEngine {
    subject: Box<dyn SyncSubject>,
    probes: Vec<ProbeSpec>,
    ctx: SyncCtx,
    cancel: Cancel,
    tick: Duration,
    history_cap: usize,
    timeout: Option<Duration>,
    stop_height: Option<u32>,
    reporter: Box<dyn SyncReporter>,
    required_work: OpSet,
}

impl SyncEngine {
    /// Runner over `subject`: no oracle indexer, no cancellation, 5 s base tick,
    /// 20k-snapshot history, no timeout, `NullReporter`. Chain the setters below.
    pub fn new(subject: Box<dyn SyncSubject>) -> Self {
        Self {
            subject,
            probes: Vec::new(),
            ctx: SyncCtx::new(None),
            cancel: Cancel::never(),
            tick: DEFAULT_TICK,
            history_cap: 20_000,
            timeout: None,
            stop_height: None,
            reporter: Box::new(NullReporter),
            required_work: OpSet::NONE,
        }
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
        self.tick = tick;
        self
    }
    /// Overall run time cap (QoS `sync` tier's 48 h, or a test bound)
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    /// Complete at `height` instead of at tip (what makes two runs comparable: a run to tip
    /// covers a different, growing span each time → its throughput measures the chain, not code)
    pub fn with_stop_height(mut self, height: u32) -> Self {
        self.stop_height = Some(height);
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

    /// Ops this profile's probes will [`Work::require`] — checked against one live reading
    /// before the run, so a subject that does not publish them fails by name here rather
    /// than panicking a probe hours in
    pub fn requires_work(mut self, ops: OpSet) -> Self {
        self.required_work = ops;
        self
    }

    pub fn with_probes(mut self, probes: Vec<ProbeSpec>) -> Self {
        self.probes = probes;
        self
    }

    // ── probe registration (design §"Test-author API") ──
    /// Safety invariant (true at every tick)
    pub fn always(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Always, severity)
    }
    /// Liveness invariant (must (re)satisfy within its `window`)
    pub fn eventually(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Eventually, severity)
    }
    /// Coverage invariant, true on ≥1 tick. A miss fails the run (green without coverage = weak)
    pub fn sometimes(&mut self) -> ProbeBuilder<'_> {
        self.builder(Class::Sometimes, Severity::Fatal)
    }
    /// Terminal post-condition (evaluated once at tip)
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

    pub fn probe_count(&self) -> usize {
        self.probes.len()
    }

    pub async fn run(mut self) -> SyncOutcome {
        self.reporter.on_start();

        if let Err(e) = self.subject.launch().await {
            return self.finish(
                SyncVerdict::Errored,
                Vec::new(),
                Vec::new(),
                Some(format!("launch: {e}")),
                0,
                0,
                None,
            );
        }

        if let Err(e) = self.check_required_work().await {
            let _ = self.subject.stop().await;
            return self.finish(
                SyncVerdict::Errored,
                Vec::new(),
                Vec::new(),
                Some(e.to_string()),
                0,
                0,
                None,
            );
        }

        let started = Instant::now();
        let deadline = self.timeout.map(|t| started + t);
        let mut builder = SnapshotBuilder::new(started);
        let mut history = History::new(self.history_cap);
        let mut violations: Vec<Violation> = Vec::new();
        let mut chain_work = ChainWork::new();
        let mut last_work = Work::ZERO;
        // Accumulated as the run goes (origin = first tick, head = latest) → survives a
        // cancelled or timed-out run and reports where it actually got to
        let mut origin: Option<Mark> = None;
        let mut head: Option<Mark> = None;
        let network = self.network().await;

        let mut ticker = interval(self.tick);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Decided in the loop, single `finish` after it: no call borrows `self.probes` for
        // its args while also taking `&mut self`
        let verdict: SyncVerdict;
        let mut error: Option<String> = None;
        // Consecutive failed `progress()` reads, reset by any success
        let mut progress_errors: u32 = 0;
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

            // Snapshot-then-evaluate. A progress-read error holds prior state and retries next
            // tick (the reservation loop's pattern); only a *probe* error aborts.
            //
            // Log at widening intervals: a never-answering subject produces no snapshot, which
            // every display reads as "not started yet" — indistinguishable from wedged.
            let progress = match self.subject.progress().await {
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
                    continue;
                }
            };
            last_work = self.read_work(&mut chain_work, progress.as_ref(), last_work).await;
            let snap = Arc::new(builder.build(progress.as_ref(), now, last_work, None));
            let mark = Mark { height: snap.height(), work: last_work, at: now };
            origin.get_or_insert(mark);
            head = Some(mark);
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

            // Built before the reporter call → no borrow of `self.probes` while
            // `self.reporter` is borrowed mutably
            let board: Vec<ProbeStatus> =
                self.probes.iter().map(|p| p.status(now, self.tick)).collect();
            self.reporter.on_probes(&snap, &board);

            // Declared stop height completes ahead of the subject's own predicate (a segment
            // must end where it said it would, whether or not the chain has more)
            let reached_stop = self.stop_height.is_some_and(|h| snap.height() >= h);
            if reached_stop || self.subject.is_complete().await {
                // at_completion probes over a final snapshot + the wallet's commitment-tree
                // roots (sync task done → wallet static, read cannot race the scan). A
                // roots-read failure degrades to empty roots, no abort at the finish line.
                if let Ok(p) = self.subject.progress().await {
                    let work = self.read_work(&mut chain_work, p.as_ref(), last_work).await;
                    let at = Instant::now();
                    let final_snap = Arc::new(builder.build(p.as_ref(), at, work, None));
                    head = Some(Mark { height: final_snap.height(), work, at });
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
        // First reading → last: `work` is what this run performed, not the cumulative totals a
        // seeded datadir brought, and `to` is where it truly reached, not where it was aimed.
        // A run that traversed nothing has no comparable span and reports none.
        let segment = origin.zip(head).filter(|(origin, head)| head.height > origin.height).map(
            |(origin, head)| Segment {
                network,
                from: origin.height,
                to: head.height,
                work: head.work.delta(&origin.work),
                elapsed_ms: head.at.saturating_duration_since(origin.at).as_millis() as u64,
            },
        );
        self.finish(verdict, violations, gaps, error, ticks, dropped, segment)
    }

    /// Chain this run is against, as the indexer names it (`main`/`test`/`regtest`); `None`
    /// with no indexer to ask. Part of a segment's identity (block 840,000 differs per network)
    async fn network(&self) -> Option<String> {
        let indexer = self.ctx.indexer()?;
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
        let Some(indexer) = self.ctx.indexer() else {
            return last;
        };
        let height = BlockHeight::from_u32(progress.height());
        chain_work.observe_at(indexer, height).await.unwrap_or(last)
    }

    /// `Some(msg)` = a probe errored → run aborts
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

    /// Every due `always`/`eventually` probe + every `sometimes` coverage probe against `snap`
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
                    // Cheap coverage predicate → evaluate every tick, latch
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
                        Verdict::ProbeError(e) => {
                            return Flow::Abort(format!("{}: {e}", spec.name));
                        }
                    }
                }
                Class::Eventually => {
                    // Armed on first evaluation. No fault timeline yet (step 3) → an
                    // `.after(fault)`-gated probe stays "satisfied" and cannot stall.
                    if spec.last_satisfied.is_none() {
                        spec.last_satisfied = Some(now);
                    }
                    let window = match spec.cadence {
                        Cadence::Window(d) => d,
                        _ => Duration::MAX,
                    };
                    let gated = spec.after.is_some(); // no faults in step 3 → satisfied
                    let verdict = if gated {
                        Verdict::Satisfied
                    } else {
                        spec.check.evaluate(snap, &self.ctx).await
                    };
                    match verdict {
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

    /// One reading, before the engine loop: every [`requires_work`](Self::requires_work) op
    /// must come back measured.
    ///
    /// - Probes read these with `Work::require`, which panics on an unmeasured op → a
    ///   missing series otherwise surfaces as a mid-run panic naming no series
    /// - Subject and harness agree on these names by string only (nothing cross-checks the
    ///   component's exporter against the families this backend reads)
    async fn check_required_work(&mut self) -> Result<(), crate::error::PipelineError> {
        if self.required_work.is_empty() {
            return Ok(());
        }
        let mut last_err = None;
        for attempt in 0..WORK_PREFLIGHT_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(self.tick).await;
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
            let missing: Vec<Op> = Op::ALL
                .into_iter()
                .filter(|&op| self.required_work.has(op) && !measured.has(op))
                .collect();
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

    #[allow(clippy::too_many_arguments)]
    fn finish(
        &mut self,
        verdict: SyncVerdict,
        violations: Vec<Violation>,
        coverage_gaps: Vec<String>,
        error: Option<String>,
        ticks: u64,
        dropped_snapshots: u64,
        segment: Option<Segment>,
    ) -> SyncOutcome {
        let outcome = SyncOutcome {
            verdict,
            violations,
            coverage_gaps,
            error,
            ticks,
            dropped_snapshots,
            segment,
        };
        self.reporter.on_finish(&outcome);
        outcome
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::cancel::CancelSource;

    use super::super::probe::{Verdict, Violation};
    use super::super::snapshot::Snapshot;
    use super::super::subject::{Phase, SyncSubject};
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
        fn phase(&self) -> Phase {
            Phase::Syncing
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
        async fn launch(&mut self) -> Result<(), crate::RpcError> {
            Ok(())
        }
        async fn progress(&self) -> Result<Box<dyn ProgressView>, crate::RpcError> {
            let i = self.cursor.fetch_add(1, Ordering::SeqCst);
            let idx = i.min(self.script.len() - 1);
            Ok(Box::new(self.script[idx].clone()))
        }
        async fn is_complete(&self) -> bool {
            !self.never_complete && self.cursor.load(Ordering::SeqCst) >= self.script.len()
        }
        async fn stop(&mut self) -> Result<(), crate::RpcError> {
            self.stopped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn work_source(&self, op: Op) -> Option<&'static str> {
            match op {
                Op::SaplingOutput => Some("fake_sapling_outputs_total"),
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
}
