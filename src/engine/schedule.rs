//! Capacity-bounded run loop. 2D [`Scheduler`] = sole admission authority.
//!
//! - Submitted in priority order; what fits the live ceiling is granted, the rest queues
//! - Child exits → lease released → freed capacity backfills (no artificial thread cap)
//! - Spawn injected (admit/backfill/retry/fail-fast unit-tested with a fake: no procs, no cluster)

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};

use super::RunProgress;
use crate::cancel::Cancel;
use crate::engine::events::{
    CancelReason, RunReporter, RunStats, RunningView, SkipReason, TestEvent, Verdict,
};
use crate::engine::local_runner::TestOutcome;
use crate::engine::panel::{live_snapshot, run_progress};
use crate::engine::plan::WorkItem;
use crate::qos::Resources;
use crate::qos::beacon::{Progress, RunningTest};
use crate::qos::live::LiveSnapshot;
use crate::qos::scheduler::{Admission, RejectReason, Request, Scheduler, SlotId};
use crate::resource::{NodeId, NodeState};
use tokio::sync::watch;

/// - `cancel` → stop admitting, drop in-flight futures (`kill_on_drop` reaps the children)
/// - `resources`: dep `Failed`/`Blocked` → [`SkipReason::DependencyUnavailable`], empty = ungated
/// - `max_inflight` caps concurrency above capacity admission; `Some(1)` = `--no-capture` serial
/// - `reservation` = cross-run [`Reservation`](crate::qos::ledger::Reservation); `None` = local run
#[derive(Debug, Clone)]
pub struct LoopConfig {
    pub fail_fast: bool,
    pub slow_after: Option<Duration>,
    pub sa: String,
    pub redraw: Duration,
    pub run_id: String,
    pub cancel: Cancel,
    pub resources: HashMap<NodeId, NodeState>,
    pub max_inflight: Option<usize>,
    pub cap_rx: Option<watch::Receiver<Resources>>,
    pub reservation: Option<Arc<crate::qos::ledger::Reservation>>,
}

/// Live state handed to the per-tick render callback
#[derive(Debug)]
pub struct PanelFrame {
    pub snapshot: LiveSnapshot,
    pub progress: RunProgress,
    pub free: Resources,
    pub running: Vec<RunningView>,
}

/// Transient failure, not a setup error
fn retryable(v: &Verdict) -> bool {
    matches!(v, Verdict::Fail(_) | Verdict::Timeout)
}

type BoxedRun = Pin<Box<dyn Future<Output = (SlotId, TestOutcome)> + Send>>;

struct Running {
    item: WorkItem,
    attempt: u32,
    started: Instant,
    slow_emitted: bool,
}

/// Drive the run to completion.
///
/// - `spawn(item, attempt)` = future running one test; `on_tick` renders the panel per redraw
/// - Single-task ([`Scheduler`] sole owner → no locking)
pub async fn run_loop<S, F>(
    items: Vec<WorkItem>,
    ceiling: Resources,
    cfg: LoopConfig,
    reporter: &mut dyn RunReporter,
    spawn: S,
    mut on_tick: impl FnMut(&mut dyn RunReporter, &PanelFrame),
) -> RunStats
where
    S: Fn(WorkItem, u32) -> F,
    F: Future<Output = TestOutcome> + Send + 'static,
{
    let total = items.len();
    let mut stats = RunStats { total, ..RunStats::default() };
    let start = Instant::now();
    // Paired origin: per-test `Instant`s become wall clock for the lease beacon
    let start_wall = chrono::Utc::now();

    let mut sched = Scheduler::new(ceiling);
    let mut inflight: HashMap<SlotId, Running> = HashMap::new();
    // Queued in the scheduler, awaiting a grant. Attempt rides along so a backfilled retry
    // resumes at it (not reset to 1).
    let mut parked: HashMap<(String, String), (WorkItem, u32)> = HashMap::new();
    // Granted + lease held, not yet spawned (at `max_inflight`); ordered so a serialized
    // (`--no-capture`) run keeps priority/submission order.
    let mut ready: VecDeque<(SlotId, WorkItem, u32)> = VecDeque::new();
    let cap = cfg.max_inflight.unwrap_or(usize::MAX);
    let mut futs: FuturesUnordered<BoxedRun> = FuturesUnordered::new();
    let mut fail_fast_tripped = false;
    let mut cancelled: Option<CancelReason> = None;

    reporter.handle(&TestEvent::RunStarted { total, run_id: &cfg.run_id });

    let spawn_granted = |slot: SlotId,
                         item: WorkItem,
                         attempt: u32,
                         inflight: &mut HashMap<SlotId, Running>,
                         futs: &mut FuturesUnordered<BoxedRun>,
                         reporter: &mut dyn RunReporter| {
        reporter.handle(&TestEvent::TestStarted {
            binary_id: &item.binary_id,
            test_name: &item.test_name,
            class: item.class,
            attempt,
        });
        let fut = spawn(item.clone(), attempt);
        futs.push(Box::pin(async move { (slot, fut.await) }));
        inflight
            .insert(slot, Running { item, attempt, started: Instant::now(), slow_emitted: false });
    };

    // Spawn granted-but-queued tests FIFO up to `max_inflight` (sole gate for a serialized run)
    macro_rules! pump {
        () => {
            while inflight.len() < cap {
                match ready.pop_front() {
                    Some((lease, item, attempt)) => {
                        spawn_granted(lease, item, attempt, &mut inflight, &mut futs, reporter)
                    }
                    None => break,
                }
            }
        };
    }

    // Live appetite → an elastic reservation sizes itself; status rides the same tick onto
    // the lease, where `ztest status` reads it (no-op for local runs and tests)
    macro_rules! publish_demand {
        () => {
            if let Some(r) = &cfg.reservation {
                r.report_demand(sched.committed(), sched.demand());
                r.report_status(progress_of(&stats, &inflight, start, start_wall));
            }
        };
    }

    // Cloned so the loop awaits changes while `cfg` keeps its handle; `None` → ceiling arm idles
    let mut cap_rx = cfg.cap_rx.clone();

    // Initial admission sweep (priority order already baked into `items`).
    for item in items {
        // Skipped pre-submission (else admitted, spawned, then dead at `TestEnv::build()` with a
        // confusing "resource absent")
        if let Some(reason) = unmet_dep(&item, &cfg.resources) {
            reporter.handle(&TestEvent::TestSkipped {
                binary_id: &item.binary_id,
                test_name: &item.test_name,
                reason,
            });
            stats.skipped += 1;
            continue;
        }
        match sched.request(to_request(&item, &cfg.sa)) {
            Admission::Granted(lease) => ready.push_back((lease, item, 1)),
            Admission::Queued => {
                parked.insert(key(&item), (item, 1));
            }
            Admission::Rejected(reason) => {
                reporter.handle(&TestEvent::TestSkipped {
                    binary_id: &item.binary_id,
                    test_name: &item.test_name,
                    reason: skip_reason(reason),
                });
                stats.skipped += 1;
            }
        }
    }
    pump!();
    publish_demand!();

    let mut tick = tokio::time::interval(cfg.redraw);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    while !futs.is_empty() {
        // Ctrl-C: stop admitting, but do NOT drop the in-flight futures — each `spawn_test`
        // observes the same cancel, SIGKILLs its group, resolves `Verdict::Terminated` → every
        // in-flight test still lands a real result. Synchronous, not a `select!` arm: once
        // cancelled every future is ready, so the arm may never poll before `futs` drains
        // (loop-top check → the single `RunCancelling` precedes `RunFinished`).
        if cancelled.is_none() && cfg.cancel.is_cancelled() {
            cancelled = Some(CancelReason::Interrupt);
            reporter.handle(&TestEvent::RunCancelling {
                reason: CancelReason::Interrupt,
                running: inflight.len(),
            });
        }
        tokio::select! {
            // Cancel waker before `futs`: the loop bounces to the top-of-loop check with the
            // in-flight set intact, so `RunCancelling` reports every running test rather than
            // however many drained first (`Pending` when not cancelled → never starves `futs`)
            biased;

            // Pure waker → prompt loop-top recheck, no waiting for the next tick. Disabled once
            // noticed (else spins on the latched watch value).
            _ = cfg.cancel.cancelled(), if cancelled.is_none() => {}

            Some((lease, outcome)) = futs.next() => {
                let running = inflight.remove(&lease).expect("inflight entry for completed lease");
                let grants = sched.release(lease);

                if outcome.verdict.is_pass() {
                    emit_finished(reporter, &running, &outcome);
                    stats.passed += 1;
                } else if retryable(&outcome.verdict)
                    && running.attempt <= running.item.retries
                    && !fail_fast_tripped
                    && !cfg.cancel.is_cancelled()
                {
                    let next = running.attempt + 1;
                    reporter.handle(&TestEvent::TestRetrying {
                        binary_id: &running.item.binary_id,
                        test_name: &running.item.test_name,
                        next_attempt: next,
                        delay: Duration::ZERO,
                        verdict: outcome.verdict.clone(),
                        duration: outcome.duration,
                    });
                    match sched.request(to_request(&running.item, &cfg.sa)) {
                        Admission::Granted(l) => ready.push_back((l, running.item, next)),
                        Admission::Queued => {
                            // Carry the attempt (a reset to 1 lets a contended flaky test
                            // retry past `retries`)
                            parked.insert(key(&running.item), (running.item, next));
                        }
                        Admission::Rejected(reason) => {
                            reporter.handle(&TestEvent::TestSkipped {
                                binary_id: &running.item.binary_id,
                                test_name: &running.item.test_name,
                                reason: skip_reason(reason),
                            });
                            stats.skipped += 1;
                        }
                    }
                } else if matches!(outcome.verdict, Verdict::Terminated) {
                    // Killed by the cancellation, not by the test → tallied apart from
                    // `failed`, never trips fail-fast (the run is already stopping)
                    emit_finished(reporter, &running, &outcome);
                    stats.terminated += 1;
                } else {
                    emit_finished(reporter, &running, &outcome);
                    stats.failed += 1;
                    if cfg.fail_fast {
                        fail_fast_tripped = true;
                    }
                }

                // Backfill the freed capacity, unless fail-fast tripped or the run was
                // cancelled (then drain inflight, admit nothing). Gated on the authoritative
                // watch flag, not local `cancelled`: a terminated outcome processed in the
                // poll that cancel fired must not sneak a parked test in first.
                if !fail_fast_tripped && !cfg.cancel.is_cancelled() {
                    for g in grants {
                        if let Some((item, attempt)) = parked.remove(&(g.binary_id.clone(), g.test_name.clone())) {
                            ready.push_back((g.slot_id, item, attempt));
                        }
                    }
                    pump!();
                }
                publish_demand!();

                render_tick(reporter, &inflight, &sched, ceiling, stats, start, &mut on_tick);
            }
            // Reservation moved this run's live ceiling: a grow backfills parked tests into
            // the new headroom, a shrink stops admission without preempting running leases
            // (`Scheduler::reconcile`). Idles forever with no reservation.
            new_ceiling = next_ceiling(&mut cap_rx) => {
                let grants = sched.reconcile(new_ceiling);
                if !fail_fast_tripped && !cfg.cancel.is_cancelled() {
                    for g in grants {
                        if let Some((item, attempt)) = parked.remove(&(g.binary_id.clone(), g.test_name.clone())) {
                            ready.push_back((g.slot_id, item, attempt));
                        }
                    }
                    pump!();
                }
                publish_demand!();
                render_tick(reporter, &inflight, &sched, ceiling, stats, start, &mut on_tick);
            }
            _ = tick.tick() => {
                // Soft SLOW detection and spinner refresh.
                if let Some(after) = cfg.slow_after {
                    for r in inflight.values_mut() {
                        if !r.slow_emitted && r.started.elapsed() >= after {
                            r.slow_emitted = true;
                            // (event emitted below, after the borrow ends)
                        }
                    }
                    emit_slows(reporter, &inflight, after);
                }
                render_tick(reporter, &inflight, &sched, ceiling, stats, start, &mut on_tick);
            }
        }
    }

    reporter.handle(&TestEvent::RunFinished { stats, elapsed: start.elapsed() });
    stats
}

/// Next value from the reservation's live-ceiling channel. Pends forever with no reservation
/// or on a closed channel (arm never fires, rather than spinning)
async fn next_ceiling(rx: &mut Option<watch::Receiver<Resources>>) -> Resources {
    match rx {
        Some(r) => {
            if r.changed().await.is_ok() {
                *r.borrow()
            } else {
                std::future::pending().await
            }
        }
        None => std::future::pending().await,
    }
}

fn emit_finished(reporter: &mut dyn RunReporter, running: &Running, outcome: &TestOutcome) {
    reporter.handle(&TestEvent::TestFinished {
        binary_id: &running.item.binary_id,
        test_name: &running.item.test_name,
        verdict: outcome.verdict.clone(),
        duration: outcome.duration,
        attempt: running.attempt,
        output: &outcome.output,
    });
}

fn emit_slows(
    reporter: &mut dyn RunReporter,
    inflight: &HashMap<SlotId, Running>,
    after: Duration,
) {
    for r in inflight.values() {
        if r.slow_emitted && r.started.elapsed() >= after {
            reporter.handle(&TestEvent::TestSlow {
                binary_id: &r.item.binary_id,
                test_name: &r.item.test_name,
                elapsed: r.started.elapsed(),
                will_terminate: false,
                attempt: r.attempt,
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_tick(
    reporter: &mut dyn RunReporter,
    inflight: &HashMap<SlotId, Running>,
    sched: &Scheduler,
    _ceiling: Resources,
    stats: RunStats,
    start: Instant,
    on_tick: &mut impl FnMut(&mut dyn RunReporter, &PanelFrame),
) {
    let snapshot = live_snapshot(inflight.values().map(|r| &r.item), sched.committed());
    // Longest-running first, ordered by fixed `started` + identity tiebreak, NOT by a
    // re-snapshotted `elapsed`: HashMap iteration order varies and per-frame `elapsed()` is
    // measured at slightly different instants, so near-equal tests swapped rows (the flicker)
    let mut running: Vec<&Running> = inflight.values().collect();
    running.sort_by(|a, b| {
        a.started
            .cmp(&b.started)
            .then_with(|| a.item.binary_id.cmp(&b.item.binary_id))
            .then_with(|| a.item.test_name.cmp(&b.item.test_name))
    });
    let running: Vec<RunningView> = running
        .into_iter()
        .map(|r| RunningView {
            binary_id: r.item.binary_id.clone(),
            test_name: r.item.test_name.clone(),
            elapsed: r.started.elapsed(),
            slow: r.slow_emitted,
        })
        .collect();
    let frame = PanelFrame {
        snapshot,
        progress: run_progress(stats, start.elapsed()),
        free: sched.free(),
        running,
    };
    on_tick(reporter, &frame);
}

fn key(item: &WorkItem) -> (String, String) {
    (item.binary_id.clone(), item.test_name.clone())
}

/// Live run status for the lease beacon (`docs/design-status.md`).
///
/// - Newest-launched first: the display shows the head, and the latest test is the one a
///   watcher is looking for
/// - `queued` derived, not counted — `parked` + `ready` + the scheduler queue are three
///   places a test can sit, and their sum is exactly what is neither finished nor inflight
fn progress_of(
    stats: &RunStats,
    inflight: &HashMap<SlotId, Running>,
    origin: Instant,
    origin_wall: chrono::DateTime<chrono::Utc>,
) -> Progress {
    let mut running: Vec<RunningTest> = inflight
        .values()
        .map(|r| RunningTest {
            name: r.item.test_name.clone(),
            footprint: r.item.footprint,
            tier: r.item.class,
            started_at: origin_wall
                + chrono::Duration::from_std(r.started.saturating_duration_since(origin))
                    .unwrap_or_default(),
        })
        .collect();
    running.sort_by_key(|r| std::cmp::Reverse(r.started_at));
    let total = stats.total as u32;
    Progress {
        total,
        queued: total.saturating_sub(stats.finished()).saturating_sub(running.len() as u32),
        failed: stats.failed,
        running,
        eta_override: None,
    }
}

fn to_request(item: &WorkItem, sa: &str) -> Request {
    Request {
        binary_id: item.binary_id.clone(),
        test_name: item.test_name.clone(),
        sa: sa.to_string(),
        footprint: item.footprint,
        priority: item.priority,
    }
}

/// First declared dep that failed to provision, or is blocked behind one that did.
/// `Ready`/`Pending`/`Acquiring`/untracked all non-blocking (never skip on an unrecorded resource)
fn unmet_dep(item: &WorkItem, resources: &HashMap<NodeId, NodeState>) -> Option<SkipReason> {
    for dep in &item.deps {
        let detail = match resources.get(dep) {
            Some(NodeState::Failed(detail)) => format!("{dep:?}: {detail}"),
            Some(NodeState::Blocked) => {
                format!("{dep:?}: blocked by a failed dependency")
            }
            _ => continue,
        };
        return Some(SkipReason::DependencyUnavailable { resource: detail });
    }
    None
}

fn skip_reason(r: RejectReason) -> SkipReason {
    match r {
        RejectReason::ExceedsClusterCapacity => SkipReason::ExceedsClusterCapacity,
        RejectReason::ExceedsSaBudget => SkipReason::ExceedsSaBudget,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::events::NullReporter;
    use crate::qos::QosClass;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    /// `sync` carries no tier default, so an item on that tier is priced from a declared
    /// reserve — the same lowering `ztest sync` performs on a real profile
    fn item(name: &str, class: QosClass, retries: u32) -> WorkItem {
        let p = class.profile_with(match class {
            QosClass::Sync => Some(Resources::new(15_000, 15 * crate::qos::GIB, 0, 0)),
            _ => None,
        });
        WorkItem {
            binary_id: "pkg::b".into(),
            test_name: name.into(),
            binary_path: PathBuf::from("/t"),
            cwd: PathBuf::from("/t"),
            class,
            footprint: p.footprint,
            priority: p.priority,
            hard_cap: p.hard_cap,
            retries,
            deps: Vec::new(),
        }
    }

    fn cfg() -> LoopConfig {
        LoopConfig {
            fail_fast: false,
            slow_after: None,
            sa: "sa".into(),
            redraw: Duration::from_millis(5),
            run_id: "run".into(),
            cancel: Cancel::never(),
            resources: HashMap::new(),
            max_inflight: None,
            cap_rx: None,
            reservation: None,
        }
    }

    fn pass() -> TestOutcome {
        TestOutcome { verdict: Verdict::Pass, output: vec![], duration: Duration::from_millis(1) }
    }
    fn fail() -> TestOutcome {
        TestOutcome {
            verdict: Verdict::Fail(1),
            output: vec![],
            duration: Duration::from_millis(1),
        }
    }

    // Fits exactly two Integration tests (3000m each)
    fn ceiling_two_integration() -> Resources {
        Resources::new(6_000, 6 * crate::qos::GIB, 0, 0)
    }

    #[tokio::test]
    async fn all_pass_runs_every_test() {
        let items = vec![
            item("a", QosClass::Integration, 0),
            item("b", QosClass::Integration, 0),
            item("c", QosClass::Integration, 0),
        ];
        let mut rep = NullReporter;
        let stats = run_loop(
            items,
            ceiling_two_integration(),
            cfg(),
            &mut rep,
            |_item, _attempt| async { pass() },
            |_, _| {},
        )
        .await;
        assert_eq!(stats.passed, 3);
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.finished(), 3);
    }

    #[tokio::test]
    async fn never_overcommits_capacity() {
        // 6 Integration tests (3000m each), ceiling fits 2 at a time
        let items: Vec<_> =
            (0..6).map(|i| item(&format!("t{i}"), QosClass::Integration, 0)).collect();
        let peak = Arc::new(Mutex::new(0usize));
        let peak2 = peak.clone();
        let inflight_count = Arc::new(Mutex::new(0usize));
        let ic = inflight_count.clone();
        let mut rep = NullReporter;
        let stats = run_loop(
            items,
            ceiling_two_integration(),
            cfg(),
            &mut rep,
            move |_item, _attempt| {
                let ic = ic.clone();
                let peak2 = peak2.clone();
                async move {
                    {
                        let mut n = ic.lock().unwrap();
                        *n += 1;
                        let mut p = peak2.lock().unwrap();
                        *p = (*p).max(*n);
                    }
                    tokio::task::yield_now().await;
                    *ic.lock().unwrap() -= 1;
                    pass()
                }
            },
            |_, _| {},
        )
        .await;
        assert_eq!(stats.passed, 6);
        assert!(*peak.lock().unwrap() <= 2, "peak={}", peak.lock().unwrap());
    }

    #[tokio::test]
    async fn max_inflight_one_serializes_despite_capacity() {
        // Ceiling fits two, but `max_inflight: Some(1)` (`--no-capture`) must hold it to one
        let items: Vec<_> =
            (0..5).map(|i| item(&format!("t{i}"), QosClass::Integration, 0)).collect();
        let peak = Arc::new(Mutex::new(0usize));
        let live = Arc::new(Mutex::new(0usize));
        let (peak2, live2) = (peak.clone(), live.clone());
        let mut c = cfg();
        c.max_inflight = Some(1);
        let mut rep = NullReporter;
        let stats = run_loop(
            items,
            ceiling_two_integration(),
            c,
            &mut rep,
            move |_item, _attempt| {
                let (peak2, live2) = (peak2.clone(), live2.clone());
                async move {
                    {
                        let mut n = live2.lock().unwrap();
                        *n += 1;
                        let mut p = peak2.lock().unwrap();
                        *p = (*p).max(*n);
                    }
                    tokio::task::yield_now().await;
                    *live2.lock().unwrap() -= 1;
                    pass()
                }
            },
            |_, _| {},
        )
        .await;
        assert_eq!(stats.passed, 5, "every test still runs, just serially");
        assert_eq!(*peak.lock().unwrap(), 1, "max_inflight=1 must never run two at once");
    }

    #[tokio::test]
    async fn fail_fast_off_runs_whole_suite_despite_failures() {
        // Regression: 122 selected, only the first ~9-wide wave ran (first failure tripped
        // fail-fast, killing backfill). Fail-fast OFF = default → a failing test still
        // releases its lease and backfills.
        let n = 12;
        let items: Vec<_> =
            (0..n).map(|i| item(&format!("t{i}"), QosClass::Integration, 0)).collect();
        let mut rep = NullReporter;
        let stats = run_loop(
            items,
            ceiling_two_integration(), // only 2 in flight at a time → 6 backfill waves
            cfg(),                     // fail_fast: false
            &mut rep,
            |_item, _attempt| async { fail() },
            |_, _| {},
        )
        .await;
        assert_eq!(stats.finished() as usize, n, "every test must run");
        assert_eq!(stats.failed as usize, n);
        assert_eq!(stats.passed, 0);
    }

    #[tokio::test]
    async fn fail_fast_stops_admission() {
        // 6 tests, fail-fast on; first to finish fails → no further admits
        let items: Vec<_> =
            (0..6).map(|i| item(&format!("t{i}"), QosClass::Integration, 0)).collect();
        let mut c = cfg();
        c.fail_fast = true;
        let mut rep = NullReporter;
        let stats = run_loop(
            items,
            ceiling_two_integration(),
            c,
            &mut rep,
            |_item, _attempt| async { fail() },
            |_, _| {},
        )
        .await;
        // Inflight drains, no backfill → far fewer than 6 reach a verdict
        assert!(stats.failed >= 1);
        assert!(stats.finished() < 6, "finished={}", stats.finished());
    }

    #[tokio::test]
    async fn retry_reruns_failed_then_passes() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let mut rep = NullReporter;
        let stats = run_loop(
            vec![item("flaky", QosClass::Integration, 2)],
            ceiling_two_integration(),
            cfg(),
            &mut rep,
            move |_item, _attempt| {
                let a = a.clone();
                async move { if a.fetch_add(1, Ordering::SeqCst) == 0 { fail() } else { pass() } }
            },
            |_, _| {},
        )
        .await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2, "should have retried once");
        assert_eq!(stats.passed, 1);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn rejects_unschedulable_footprint() {
        // Sync needs 16000m; a 1000m ceiling cannot fit it even empty → Rejected
        let mut rep = NullReporter;
        let stats = run_loop(
            vec![item("huge", QosClass::Sync, 0)],
            Resources::new(1_000, crate::qos::GIB, 0, 0),
            cfg(),
            &mut rep,
            |_item, _attempt| async { pass() },
            |_, _| {},
        )
        .await;
        assert_eq!(stats.skipped, 1);
        assert_eq!(stats.passed, 0);
    }

    /// Spawn order (recorded synchronously = admission order), per-tier live/peak concurrency,
    /// and whether Σ(live footprints) ever exceeded the ceiling
    #[derive(Default)]
    struct ConcRec {
        order: Vec<QosClass>,
        live: std::collections::BTreeMap<QosClass, usize>,
        max_live: std::collections::BTreeMap<QosClass, usize>,
        overcommit: bool,
    }

    impl ConcRec {
        fn start(&mut self, class: QosClass, ceiling: Resources) {
            *self.live.entry(class).or_default() += 1;
            for (c, n) in &self.live {
                let e = self.max_live.entry(*c).or_default();
                *e = (*e).max(*n);
            }
            let mut sum = Resources::ZERO;
            for (c, n) in &self.live {
                for _ in 0..*n {
                    sum = sum.saturating_add(&c.profile().footprint);
                }
            }
            if !sum.fits_within(&ceiling) {
                self.overcommit = true;
            }
        }
        fn end(&mut self, class: QosClass) {
            if let Some(n) = self.live.get_mut(&class) {
                *n -= 1;
            }
        }
        fn peak(&self, class: QosClass) -> usize {
            self.max_live.get(&class).copied().unwrap_or(0)
        }
    }

    /// Injected spawn feeding a [`ConcRec`], always passing; dwells so co-admitted tests overlap
    fn recording_spawn(
        rec: Arc<Mutex<ConcRec>>,
        ceiling: Resources,
        dwell: Duration,
    ) -> impl Fn(WorkItem, u32) -> Pin<Box<dyn Future<Output = TestOutcome> + Send>> {
        move |it: WorkItem, _attempt| {
            rec.lock().unwrap().order.push(it.class);
            let rec = rec.clone();
            let class = it.class;
            Box::pin(async move {
                rec.lock().unwrap().start(class, ceiling);
                tokio::time::sleep(dwell).await;
                rec.lock().unwrap().end(class);
                pass()
            })
        }
    }

    /// Heavy (Testnet) tier runs first, capacity-capped; as it drains, light (Integration)
    /// concurrency ramps up — live: "2-3 testnet at a time, then 6-10 integration"
    #[tokio::test]
    async fn concurrency_ramps_up_as_heavy_tier_drains() {
        let t = QosClass::Testnet.profile().footprint;
        let i = QosClass::Integration.profile().footprint;
        // Room for 3 Testnet + 1 Integration backfilling the leftover
        let ceiling =
            Resources::new(3 * t.cpu_milli + i.cpu_milli, 3 * t.mem_bytes + i.mem_bytes, 0, 0);

        // Heavy-first, as `build_work_list` orders them in production
        let mut items = vec![
            item("net0", QosClass::Testnet, 0),
            item("net1", QosClass::Testnet, 0),
            item("net2", QosClass::Testnet, 0),
        ];
        items.extend((0..12).map(|n| item(&format!("int{n}"), QosClass::Integration, 0)));

        let rec = Arc::new(Mutex::new(ConcRec::default()));
        let dwell = Duration::from_millis(25); // overlap → true concurrency observed
        let mut rep = NullReporter;
        let stats = run_loop(
            items,
            ceiling,
            cfg(),
            &mut rep,
            recording_spawn(rec.clone(), ceiling, dwell),
            |_, _| {},
        )
        .await;

        assert_eq!(stats.passed, 15);
        let g = rec.lock().unwrap();
        assert!(!g.overcommit, "running footprint must never exceed the ceiling");
        // Testnet first, ≤3 at once (ignoring priority would pack 13 Integration in)
        assert_eq!(
            g.order.iter().take_while(|c| **c == QosClass::Testnet).count(),
            3,
            "heavy tier should start first; order={:?}",
            g.order
        );
        assert_eq!(g.peak(QosClass::Testnet), 3);
        // Light tier ramps far past the heavy cap = dynamic scaling
        let int_peak = g.peak(QosClass::Integration);
        assert!(
            int_peak >= 6,
            "integration concurrency should ramp up after heavy drains; peak={int_peak}"
        );
    }

    /// One-Sync ceiling → everything else queues, backfilled strictly highest-priority first.
    /// Lower tiers submitted *earlier* than Testnet (only priority can order the starts)
    #[tokio::test]
    async fn higher_tiers_backfill_before_lower_even_when_queued_earlier() {
        let ceiling = item("probe", QosClass::Sync, 0).footprint; // one Sync at a time
        let items = vec![
            item("sync", QosClass::Sync, 0), // grabs the only initial slot
            // Submitted lowest-priority-first on purpose:
            item("wal0", QosClass::Wallet, 0),
            item("wal1", QosClass::Wallet, 0),
            item("int0", QosClass::Integration, 0),
            item("int1", QosClass::Integration, 0),
            item("net0", QosClass::Testnet, 0),
            item("net1", QosClass::Testnet, 0),
        ];
        let rec = Arc::new(Mutex::new(ConcRec::default()));
        let dwell = Duration::from_millis(10);
        let mut rep = NullReporter;
        let stats = run_loop(
            items,
            ceiling,
            cfg(),
            &mut rep,
            recording_spawn(rec.clone(), ceiling, dwell),
            |_, _| {},
        )
        .await;

        assert_eq!(stats.passed, 7);
        let g = rec.lock().unwrap();
        assert!(!g.overcommit, "running footprint must never exceed the ceiling");
        let first = |c: QosClass| {
            g.order
                .iter()
                .position(|x| *x == c)
                .unwrap_or_else(|| panic!("tier {c:?} never ran; order={:?}", g.order))
        };
        assert!(
            first(QosClass::Sync) < first(QosClass::Testnet)
                && first(QosClass::Testnet) < first(QosClass::Wallet)
                && first(QosClass::Wallet) < first(QosClass::Integration),
            "tiers must start in descending-priority order; order={:?}",
            g.order
        );
    }

    /// One-Integration ceiling → the retry queues behind `hog`, taking the re-park path; the
    /// attempt must survive that wait (pre-fix reset to 1 gave an over-limit `[1, 1, 2]`)
    #[tokio::test]
    async fn retry_under_contention_preserves_attempt_count() {
        let ceiling = QosClass::Integration.profile().footprint; // one slot
        let runs = Arc::new(Mutex::new(Vec::<(String, u32)>::new()));
        let r = runs.clone();
        let mut rep = NullReporter;
        let stats = run_loop(
            vec![item("flaky", QosClass::Integration, 1), item("hog", QosClass::Integration, 0)],
            ceiling,
            cfg(),
            &mut rep,
            move |it: WorkItem, attempt| {
                r.lock().unwrap().push((it.test_name.clone(), attempt));
                let is_fail = it.test_name == "flaky" && attempt < 2;
                async move { if is_fail { fail() } else { pass() } }
            },
            |_, _| {},
        )
        .await;

        let runs = runs.lock().unwrap();
        let flaky: Vec<u32> = runs.iter().filter(|(n, _)| n == "flaky").map(|(_, a)| *a).collect();
        assert_eq!(flaky, vec![1, 2], "flaky must run exactly twice: attempts 1 then 2");
        assert_eq!(stats.passed, 2);
        assert_eq!(stats.failed, 0);
    }

    // ── Event-stream contract ──────────────────────────────────────────────
    //
    // `TestEvent` borrows its identity/output → a retaining reporter must copy.
    // `RecordingReporter` mirrors it owned, so tests assert the exact lifecycle
    // `StyledReporter` (and any future JUnit writer) consume and `NullReporter` drops.

    /// Owned, comparable mirror of one [`TestEvent`]
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Ev {
        RunStarted { total: usize },
        Started { test_name: String, class: QosClass, attempt: u32 },
        Slow { test_name: String, will_terminate: bool },
        Retrying { test_name: String, next_attempt: u32 },
        Finished { test_name: String, verdict: Verdict, output: Vec<u8> },
        Skipped { test_name: String, reason: SkipReason },
        Cancelling { reason: CancelReason, running: usize },
        RunFinished { stats: RunStats },
    }

    /// Test identity an event refers to (`None` = run-level)
    fn ev_name(e: &Ev) -> Option<&str> {
        match e {
            Ev::Started { test_name, .. }
            | Ev::Slow { test_name, .. }
            | Ev::Retrying { test_name, .. }
            | Ev::Finished { test_name, .. }
            | Ev::Skipped { test_name, .. } => Some(test_name),
            Ev::RunStarted { .. } | Ev::Cancelling { .. } | Ev::RunFinished { .. } => None,
        }
    }

    /// Records every event in emission order
    #[derive(Default)]
    struct RecordingReporter {
        events: Vec<Ev>,
    }

    impl RecordingReporter {
        fn of(&self, name: &str) -> Vec<&Ev> {
            self.events.iter().filter(|e| ev_name(e) == Some(name)).collect()
        }
    }

    impl RunReporter for RecordingReporter {
        fn handle(&mut self, ev: &TestEvent<'_>) {
            let owned = match ev {
                TestEvent::RunStarted { total, .. } => Ev::RunStarted { total: *total },
                TestEvent::TestStarted { test_name, class, attempt, .. } => Ev::Started {
                    test_name: test_name.to_string(),
                    class: *class,
                    attempt: *attempt,
                },
                TestEvent::TestSlow { test_name, will_terminate, .. } => {
                    Ev::Slow { test_name: test_name.to_string(), will_terminate: *will_terminate }
                }
                TestEvent::TestRetrying { test_name, next_attempt, .. } => {
                    Ev::Retrying { test_name: test_name.to_string(), next_attempt: *next_attempt }
                }
                TestEvent::TestFinished { test_name, verdict, output, .. } => Ev::Finished {
                    test_name: test_name.to_string(),
                    verdict: verdict.clone(),
                    output: output.to_vec(),
                },
                TestEvent::TestSkipped { test_name, reason, .. } => {
                    Ev::Skipped { test_name: test_name.to_string(), reason: reason.clone() }
                }
                TestEvent::RunCancelling { reason, running } => {
                    Ev::Cancelling { reason: *reason, running: *running }
                }
                TestEvent::RunFinished { stats, .. } => Ev::RunFinished { stats: *stats },
            };
            self.events.push(owned);
        }
        fn take_scrollback(&mut self) -> Vec<u8> {
            Vec::new()
        }
    }

    /// Exactly one `RunStarted` first + one `RunFinished` last; every test = one start paired
    /// with one terminal finish
    #[tokio::test]
    async fn event_stream_brackets_run_and_pairs_start_with_finish() {
        let items = vec![item("a", QosClass::Integration, 0), item("b", QosClass::Integration, 0)];
        let mut rep = RecordingReporter::default();
        let stats = run_loop(
            items,
            ceiling_two_integration(),
            cfg(),
            &mut rep,
            |_it, _a| async { pass() },
            |_, _| {},
        )
        .await;
        assert_eq!(stats.passed, 2);

        let ev = &rep.events;
        assert!(matches!(ev.first(), Some(Ev::RunStarted { total: 2 })));
        assert_eq!(ev.iter().filter(|e| matches!(e, Ev::RunStarted { .. })).count(), 1);
        assert_eq!(ev.iter().filter(|e| matches!(e, Ev::RunFinished { .. })).count(), 1);
        match ev.last() {
            Some(Ev::RunFinished { stats }) => {
                assert_eq!(stats.passed, 2);
                assert_eq!(stats.finished(), 2);
            }
            other => panic!("run must end with RunFinished, got {other:?}"),
        }
        for name in ["a", "b"] {
            let evs = rep.of(name);
            assert_eq!(evs.len(), 2, "{name}: {evs:?}");
            assert!(matches!(evs[0], Ev::Started { attempt: 1, class: QosClass::Integration, .. }));
            assert!(matches!(evs[1], Ev::Finished { verdict: Verdict::Pass, .. }));
        }
    }

    /// Retried attempt emits `Retrying`, not `Finished`; the rerun emits a fresh `Started` at
    /// the next attempt (only the terminal pass emits `Finished`)
    #[tokio::test]
    async fn retry_emits_retrying_then_restart_without_finishing_failed_attempt() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let n = Arc::new(AtomicU32::new(0));
        let a = n.clone();
        let mut rep = RecordingReporter::default();
        let stats = run_loop(
            vec![item("flaky", QosClass::Integration, 1)],
            ceiling_two_integration(),
            cfg(),
            &mut rep,
            move |_it, _a| {
                let a = a.clone();
                async move { if a.fetch_add(1, Ordering::SeqCst) == 0 { fail() } else { pass() } }
            },
            |_, _| {},
        )
        .await;
        assert_eq!(stats.passed, 1);

        let evs = rep.of("flaky");
        assert_eq!(evs.len(), 4, "{evs:?}");
        assert!(matches!(evs[0], Ev::Started { attempt: 1, .. }));
        assert!(matches!(evs[1], Ev::Retrying { next_attempt: 2, .. }));
        assert!(matches!(evs[2], Ev::Started { attempt: 2, .. }));
        assert!(matches!(evs[3], Ev::Finished { verdict: Verdict::Pass, .. }));
    }

    /// Unschedulable test = exactly one `Skipped` with reason, never a `Started`; run still
    /// closes with `RunFinished`
    #[tokio::test]
    async fn unschedulable_emits_skipped_with_reason_and_never_starts() {
        let mut rep = RecordingReporter::default();
        run_loop(
            vec![item("huge", QosClass::Sync, 0)],
            Resources::new(1_000, crate::qos::GIB, 0, 0),
            cfg(),
            &mut rep,
            |_it, _a| async { pass() },
            |_, _| {},
        )
        .await;

        let evs = rep.of("huge");
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], Ev::Skipped { reason: SkipReason::ExceedsClusterCapacity, .. }));
        assert!(
            !rep.events.iter().any(|e| matches!(e, Ev::Started { .. })),
            "a skipped test must never start"
        );
        assert!(matches!(
            rep.events.last(),
            Some(Ev::RunFinished { stats }) if stats.skipped == 1
        ));
    }

    /// Failed dep → `DependencyUnavailable` skip, never started; a test not needing it still
    /// runs ("a broken archive sidelines only its dependents", at loop level)
    #[tokio::test]
    async fn dependency_failure_skips_only_dependent_tests() {
        use crate::resource::{NodeId, NodeState};

        let dep = NodeId::Image("zebrad:dev-bad".into());
        let mut needs_img = item("needs_img", QosClass::Integration, 0);
        needs_img.deps = vec![dep.clone()];
        let free = item("free", QosClass::Integration, 0);

        let mut c = cfg();
        c.resources.insert(dep, NodeState::Failed("docker build failed".into()));

        let mut rep = RecordingReporter::default();
        let stats = run_loop(
            vec![needs_img, free],
            ceiling_two_integration(),
            c,
            &mut rep,
            |_it, _a| async { pass() },
            |_, _| {},
        )
        .await;

        assert_eq!(stats.passed, 1, "the dep-free test still runs");
        assert_eq!(stats.skipped, 1, "the dependent test is skipped, not run");
        assert_eq!(stats.failed, 0);

        let dependent = rep.of("needs_img");
        assert!(
            matches!(
                dependent.as_slice(),
                [Ev::Skipped { reason: SkipReason::DependencyUnavailable { .. }, .. }]
            ),
            "{dependent:?}"
        );
        assert!(
            !rep.events.iter().any(|e| matches!(
                e,
                Ev::Started { test_name, .. } if test_name == "needs_img"
            )),
            "a dependency-skipped test must never start"
        );
        assert!(
            rep.of("free").iter().any(|e| matches!(e, Ev::Finished { .. })),
            "the free test must finish"
        );
    }

    /// Terminal `Finished` carries the child's captured output (reporter replays it on failure)
    #[tokio::test]
    async fn finished_event_carries_captured_output_for_replay() {
        let mut rep = RecordingReporter::default();
        run_loop(
            vec![item("noisy", QosClass::Integration, 0)],
            ceiling_two_integration(),
            cfg(),
            &mut rep,
            |_it, _a| async {
                TestOutcome {
                    verdict: Verdict::Fail(2),
                    output: b"boom-output".to_vec(),
                    duration: Duration::from_millis(1),
                }
            },
            |_, _| {},
        )
        .await;

        let evs = rep.of("noisy");
        let fin = evs.iter().find(|e| matches!(e, Ev::Finished { .. })).expect("a Finished event");
        match fin {
            Ev::Finished { verdict, output, .. } => {
                assert_eq!(*verdict, Verdict::Fail(2));
                assert_eq!(String::from_utf8_lossy(output), "boom-output");
            }
            _ => unreachable!(),
        }
    }

    /// Ctrl-C mid-run: in-flight tests reported terminated + counted, never-started ones simply
    /// not run (short `finished/total`) — the old `break` vanished them from the summary
    #[tokio::test]
    async fn cancel_reports_inflight_as_terminated_and_leaves_rest_unrun() {
        use crate::cancel::CancelSource;

        let (src, cancel) = CancelSource::new();
        let mut c = cfg();
        c.cancel = cancel.clone();

        // Four Integration tests, ceiling fits two → two run, two park
        let items: Vec<_> =
            (0..4).map(|i| item(&format!("t{i}"), QosClass::Integration, 0)).collect();

        let mut rep = RecordingReporter::default();
        let mut fired = false;
        let stats = run_loop(
            items,
            ceiling_two_integration(),
            c,
            &mut rep,
            move |_it, _a| {
                // Ends only on cancel — mirrors `spawn_test` (SIGKILL group → `Terminated`)
                let cancel = cancel.clone();
                async move {
                    cancel.cancelled().await;
                    TestOutcome {
                        verdict: Verdict::Terminated,
                        output: vec![],
                        duration: Duration::from_millis(1),
                    }
                }
            },
            // Deterministic, not a racy wall-clock delay: tests complete only after cancel,
            // so the first tick showing 2 running is exactly both in flight
            move |_rep, frame| {
                if !fired && frame.running.len() == 2 {
                    fired = true;
                    src.cancel();
                }
            },
        )
        .await;

        assert_eq!(stats.terminated, 2, "in-flight tests must be reported");
        assert_eq!(stats.failed, 0, "a kill is not a failure");
        assert_eq!(stats.passed, 0);
        assert_eq!(stats.ran(), 2);
        assert_eq!(stats.total, 4);
        assert_eq!(stats.not_run(), 2, "the parked tests are accounted for");
        assert!(stats.any_failed(), "a cancelled run still exits non-zero");

        let cancels: Vec<_> =
            rep.events.iter().filter(|e| matches!(e, Ev::Cancelling { .. })).collect();
        assert_eq!(cancels.len(), 1, "exactly one RunCancelling");
        assert!(matches!(
            cancels[0],
            Ev::Cancelling { reason: CancelReason::Interrupt, running: 2 }
        ));

        let terminated = rep
            .events
            .iter()
            .filter(|e| matches!(e, Ev::Finished { verdict: Verdict::Terminated, .. }))
            .count();
        assert_eq!(terminated, 2, "each in-flight test reports terminated");
        assert!(matches!(rep.events.last(), Some(Ev::RunFinished { .. })));

        let started = rep.events.iter().filter(|e| matches!(e, Ev::Started { .. })).count();
        assert_eq!(started, 2, "cancellation stops admitting new tests");
    }
}
