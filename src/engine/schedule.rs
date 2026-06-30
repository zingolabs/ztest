//! The capacity-bounded run loop: ztest's 2D [`Scheduler`] is the sole admission
//! authority. Tests are submitted in priority order; the scheduler grants what
//! fits the live cluster ceiling and queues the rest. As each child exits, its
//! lease is released and the freed capacity backfills queued tests — so the
//! number of concurrently-running tests scales up and down with capacity, with
//! no artificial thread cap.
//!
//! The spawn function is injected so the loop's policy (admit / backfill /
//! retry / fail-fast) is unit-tested with a fake spawn — no processes, no
//! cluster.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};

use crate::engine::events::{RunReporter, RunStats, RunningView, SkipReason, TestEvent, Verdict};
use crate::engine::exec::TestOutcome;
use crate::engine::panel::{live_snapshot, run_progress};
use crate::engine::plan::WorkItem;
use crate::preflight::RunProgress;
use crate::qos::Resources;
use crate::qos::live::LiveSnapshot;
use crate::qos::scheduler::{Admission, LeaseId, RejectReason, Request, Scheduler};

/// Tunables for the run loop.
#[derive(Debug, Clone)]
pub struct LoopConfig {
    /// Stop admitting on the first terminal failure (nextest default).
    pub fail_fast: bool,
    /// Soft "slow" threshold; `None` disables the SLOW signal.
    pub slow_after: Option<Duration>,
    /// ServiceAccount the run charges against.
    pub sa: String,
    /// Render-tick / spinner cadence.
    pub redraw: Duration,
    /// Run id (for `RunStarted`).
    pub run_id: String,
}

/// The live state handed to the per-tick render callback.
#[derive(Debug)]
pub struct PanelFrame {
    /// Per-tier running counts + reserves.
    pub snapshot: LiveSnapshot,
    /// Pass/fail/elapsed for the QoS panel's progress line.
    pub progress: RunProgress,
    /// Full tally (incl. skipped) for the nextest-style top progress line.
    pub stats: RunStats,
    /// Free cluster capacity (ceiling − committed).
    pub free: Resources,
    /// In-flight tests, longest-running first — for the live running region.
    pub running: Vec<RunningView>,
}

/// Whether a verdict is worth retrying (transient failures, not setup errors).
fn retryable(v: &Verdict) -> bool {
    matches!(v, Verdict::Fail(_) | Verdict::Timeout)
}

type BoxedRun = Pin<Box<dyn Future<Output = (LeaseId, TestOutcome)> + Send>>;

struct Running {
    item: WorkItem,
    attempt: u32,
    started: Instant,
    slow_emitted: bool,
}

/// Drive the run to completion, returning the final stats.
///
/// `spawn(item, attempt)` produces the future that runs one test; `on_tick`
/// renders the live panel each redraw interval. The whole loop is single-task
/// (the `Scheduler` is its sole owner — no locking).
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
    let mut stats = RunStats {
        total,
        ..RunStats::default()
    };
    let start = Instant::now();

    let mut sched = Scheduler::new(ceiling);
    let mut inflight: HashMap<LeaseId, Running> = HashMap::new();
    // Identity → (WorkItem, attempt) currently queued in the scheduler, awaiting
    // a grant. The attempt rides along so a retry that has to wait for capacity
    // resumes at the right attempt when backfilled (not reset to 1).
    let mut parked: HashMap<(String, String), (WorkItem, u32)> = HashMap::new();
    let mut futs: FuturesUnordered<BoxedRun> = FuturesUnordered::new();
    let mut fail_fast_tripped = false;

    reporter.handle(&TestEvent::RunStarted {
        total,
        run_id: &cfg.run_id,
    });

    // Helper to spawn a granted test: push its future + record it inflight.
    let spawn_granted = |lease: LeaseId,
                         item: WorkItem,
                         attempt: u32,
                         inflight: &mut HashMap<LeaseId, Running>,
                         futs: &mut FuturesUnordered<BoxedRun>,
                         reporter: &mut dyn RunReporter| {
        reporter.handle(&TestEvent::TestStarted {
            binary_id: &item.binary_id,
            test_name: &item.test_name,
            class: item.class,
            attempt,
        });
        let fut = spawn(item.clone(), attempt);
        futs.push(Box::pin(async move { (lease, fut.await) }));
        inflight.insert(
            lease,
            Running {
                item,
                attempt,
                started: Instant::now(),
                slow_emitted: false,
            },
        );
    };

    // Initial admission sweep (priority order already baked into `items`).
    for item in items {
        match sched.request(to_request(&item, &cfg.sa)) {
            Admission::Granted(lease) => {
                spawn_granted(lease, item, 1, &mut inflight, &mut futs, reporter)
            }
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

    let mut tick = tokio::time::interval(cfg.redraw);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    while !futs.is_empty() {
        tokio::select! {
            Some((lease, outcome)) = futs.next() => {
                let running = inflight.remove(&lease).expect("inflight entry for completed lease");
                let grants = sched.release(lease);

                if outcome.verdict.is_pass() {
                    emit_finished(reporter, &running, &outcome);
                    stats.passed += 1;
                } else if retryable(&outcome.verdict)
                    && running.attempt <= running.item.retries
                    && !fail_fast_tripped
                {
                    // Retry: re-request a fresh lease at attempt+1.
                    let next = running.attempt + 1;
                    reporter.handle(&TestEvent::TestRetrying {
                        binary_id: &running.item.binary_id,
                        test_name: &running.item.test_name,
                        next_attempt: next,
                        delay: Duration::ZERO,
                    });
                    match sched.request(to_request(&running.item, &cfg.sa)) {
                        Admission::Granted(l) => {
                            spawn_granted(l, running.item, next, &mut inflight, &mut futs, reporter)
                        }
                        Admission::Queued => {
                            // Re-park carrying the attempt, so when capacity frees
                            // the retry resumes at `next` — not reset to 1, which
                            // would let a contended flaky test retry past `retries`.
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
                } else {
                    emit_finished(reporter, &running, &outcome);
                    stats.failed += 1;
                    if cfg.fail_fast {
                        fail_fast_tripped = true;
                    }
                }

                // Backfill the freed capacity with queued tests — unless fail-fast
                // tripped, in which case we drain inflight and admit nothing more.
                if !fail_fast_tripped {
                    for g in grants {
                        if let Some((item, attempt)) = parked.remove(&(g.binary_id.clone(), g.test_name.clone())) {
                            spawn_granted(g.lease_id, item, attempt, &mut inflight, &mut futs, reporter);
                        }
                    }
                }

                render_tick(reporter, &inflight, &sched, ceiling, stats, start, &cfg, &mut on_tick);
            }
            _ = tick.tick() => {
                // Soft SLOW detection + spinner refresh.
                if let Some(after) = cfg.slow_after {
                    for r in inflight.values_mut() {
                        if !r.slow_emitted && r.started.elapsed() >= after {
                            r.slow_emitted = true;
                            // (event emitted below, after the borrow ends)
                        }
                    }
                    emit_slows(reporter, &inflight, after);
                }
                render_tick(reporter, &inflight, &sched, ceiling, stats, start, &cfg, &mut on_tick);
            }
        }
    }

    reporter.handle(&TestEvent::RunFinished {
        stats,
        elapsed: start.elapsed(),
    });
    stats
}

fn emit_finished(reporter: &mut dyn RunReporter, running: &Running, outcome: &TestOutcome) {
    reporter.handle(&TestEvent::TestFinished {
        binary_id: &running.item.binary_id,
        test_name: &running.item.test_name,
        verdict: outcome.verdict.clone(),
        duration: outcome.duration,
        output: &outcome.output,
    });
}

fn emit_slows(
    reporter: &mut dyn RunReporter,
    inflight: &HashMap<LeaseId, Running>,
    after: Duration,
) {
    for r in inflight.values() {
        if r.slow_emitted && r.started.elapsed() >= after {
            reporter.handle(&TestEvent::TestSlow {
                binary_id: &r.item.binary_id,
                test_name: &r.item.test_name,
                elapsed: r.started.elapsed(),
                will_terminate: false,
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_tick(
    reporter: &mut dyn RunReporter,
    inflight: &HashMap<LeaseId, Running>,
    sched: &Scheduler,
    _ceiling: Resources,
    stats: RunStats,
    start: Instant,
    cfg: &LoopConfig,
    on_tick: &mut impl FnMut(&mut dyn RunReporter, &PanelFrame),
) {
    let snapshot = live_snapshot(
        inflight.values().map(|r| &r.item),
        sched.committed(),
        &cfg.sa,
    );
    // In-flight tests for the live region, longest-running first. Order by the
    // fixed `started` instant (oldest first) with an identity tiebreak — NOT by
    // a re-snapshotted `elapsed`: `inflight` is a HashMap (varying iteration
    // order) and per-test `elapsed()` is measured at slightly different instants
    // each frame, so sorting on it let near-equal tests swap rows frame-to-frame
    // (the flicker). `started`/identity never change, so the order is stable.
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
        stats,
        free: sched.free(),
        running,
    };
    on_tick(reporter, &frame);
}

fn key(item: &WorkItem) -> (String, String) {
    (item.binary_id.clone(), item.test_name.clone())
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

    fn item(name: &str, class: QosClass, retries: u32) -> WorkItem {
        let p = class.profile();
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
        }
    }

    fn cfg() -> LoopConfig {
        LoopConfig {
            fail_fast: false,
            slow_after: None,
            sa: "sa".into(),
            redraw: Duration::from_millis(5),
            run_id: "run".into(),
        }
    }

    fn pass() -> TestOutcome {
        TestOutcome {
            verdict: Verdict::Pass,
            output: vec![],
            duration: Duration::from_millis(1),
        }
    }
    fn fail() -> TestOutcome {
        TestOutcome {
            verdict: Verdict::Fail(1),
            output: vec![],
            duration: Duration::from_millis(1),
        }
    }

    // A ceiling that fits exactly two Integration tests (2000m each).
    fn ceiling_two_integration() -> Resources {
        Resources::new(4_000, 4 * crate::qos::GIB)
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
        // 6 Integration tests (2000m each), ceiling fits 2 at a time.
        let items: Vec<_> = (0..6)
            .map(|i| item(&format!("t{i}"), QosClass::Integration, 0))
            .collect();
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
        // Ceiling fits exactly 2 → never more than 2 running concurrently.
        assert!(*peak.lock().unwrap() <= 2, "peak={}", peak.lock().unwrap());
    }

    #[tokio::test]
    async fn fail_fast_off_runs_whole_suite_despite_failures() {
        // Reproduces the scheduling incident (122 selected, only the first
        // ~9-wide capacity wave ran because the first failure tripped fail-fast
        // and killed backfill). With fail-fast OFF — now the default — a failing
        // test still releases its lease and backfills the queue, so EVERY
        // schedulable test runs even though all of them fail.
        let n = 12;
        let items: Vec<_> = (0..n)
            .map(|i| item(&format!("t{i}"), QosClass::Integration, 0))
            .collect();
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
        // The whole suite ran (no early halt); all failed; nothing abandoned.
        assert_eq!(stats.finished() as usize, n, "every test must run");
        assert_eq!(stats.failed as usize, n);
        assert_eq!(stats.passed, 0);
    }

    #[tokio::test]
    async fn fail_fast_stops_admission() {
        // 6 tests, fail-fast on; the first to finish fails → no further admits.
        let items: Vec<_> = (0..6)
            .map(|i| item(&format!("t{i}"), QosClass::Integration, 0))
            .collect();
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
        // With fail-fast, far fewer than 6 reach a verdict (inflight drains, no backfill).
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
                async move {
                    // Fail the first attempt, pass the second.
                    if a.fetch_add(1, Ordering::SeqCst) == 0 {
                        fail()
                    } else {
                        pass()
                    }
                }
            },
            |_, _| {},
        )
        .await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "should have retried once"
        );
        assert_eq!(stats.passed, 1);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn rejects_unschedulable_footprint() {
        // Sync needs 16000m; a 1000m ceiling can't fit it even empty → Rejected.
        let mut rep = NullReporter;
        let stats = run_loop(
            vec![item("huge", QosClass::Sync, 0)],
            Resources::new(1_000, crate::qos::GIB),
            cfg(),
            &mut rep,
            |_item, _attempt| async { pass() },
            |_, _| {},
        )
        .await;
        assert_eq!(stats.skipped, 1);
        assert_eq!(stats.passed, 0);
    }

    /// Records, from the injected spawn, the exact spawn order (synchronously, so
    /// it reflects the scheduler's admission order) and the live/peak concurrency
    /// per tier (across the future's lifetime), plus whether the live footprint
    /// ever exceeded the ceiling.
    #[derive(Default)]
    struct ConcRec {
        /// Class of each test in the order the loop spawned it.
        order: Vec<QosClass>,
        /// Currently-running count per tier.
        live: std::collections::BTreeMap<QosClass, usize>,
        /// Peak concurrent count per tier over the whole run.
        max_live: std::collections::BTreeMap<QosClass, usize>,
        /// Set if Σ(live footprints) ever exceeded the ceiling.
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

    /// An injected spawn that feeds a [`ConcRec`]: records spawn order
    /// synchronously (so it mirrors the scheduler's admission order), tracks
    /// live/peak concurrency, flags any overcommit, and dwells `dwell` so
    /// concurrently-admitted tests genuinely overlap in time. Always passes.
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

    /// Dynamic scale-up: with heavy (Testnet) and light (Integration) tests both
    /// queued, the scheduler runs the heavy tier first (a few at a time, capped by
    /// capacity); as the heavy tests drain, concurrency automatically ramps up
    /// because each light test reserves far less — exactly the live behaviour
    /// ("2-3 testnet at a time, then 6-10 integration once the heavy ones finish").
    #[tokio::test]
    async fn concurrency_ramps_up_as_heavy_tier_drains() {
        let t = QosClass::Testnet.profile().footprint;
        let i = QosClass::Integration.profile().footprint;
        // Room for 3 Testnet + 1 Integration: a few heavy tests, with one light
        // test backfilling the leftover capacity.
        let ceiling = Resources::new(3 * t.cpu_milli + i.cpu_milli, 3 * t.mem_bytes + i.mem_bytes);

        // Submitted heavy-first (as `build_work_list` orders them in production).
        let mut items = vec![
            item("net0", QosClass::Testnet, 0),
            item("net1", QosClass::Testnet, 0),
            item("net2", QosClass::Testnet, 0),
        ];
        items.extend((0..12).map(|n| item(&format!("int{n}"), QosClass::Integration, 0)));

        let rec = Arc::new(Mutex::new(ConcRec::default()));
        let dwell = Duration::from_millis(25); // overlap so true concurrency is observed
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
        assert!(
            !g.overcommit,
            "running footprint must never exceed the ceiling"
        );
        // Heavy tier first AND capped by capacity: the first three to start are
        // Testnet (not 13 Integration packed in, which is what ignoring priority
        // would do), and never more than 3 Testnet run at once.
        assert_eq!(
            g.order
                .iter()
                .take_while(|c| **c == QosClass::Testnet)
                .count(),
            3,
            "heavy tier should start first; order={:?}",
            g.order
        );
        assert_eq!(g.peak(QosClass::Testnet), 3);
        // Once the heavy tier drains, light-tier concurrency ramps far past the
        // heavy cap (down-scaling on heavy, up-scaling on light = dynamic).
        let int_peak = g.peak(QosClass::Integration);
        assert!(
            int_peak >= 6,
            "integration concurrency should ramp up after heavy drains; peak={int_peak}"
        );
    }

    /// Tier prioritisation through the loop: with a ceiling that fits one Sync at
    /// a time, everything else queues; as capacity frees, the scheduler backfills
    /// strictly highest-priority first. The lower tiers are *submitted earlier*
    /// than Testnet, so only the scheduler's priority — not submission order — can
    /// produce the descending-priority start order.
    #[tokio::test]
    async fn higher_tiers_backfill_before_lower_even_when_queued_earlier() {
        let ceiling = QosClass::Sync.profile().footprint; // one Sync at a time
        let items = vec![
            item("sync", QosClass::Sync, 0), // grabs the only initial slot
            // Submitted lowest-priority-first on purpose:
            item("basic0", QosClass::Basic, 0),
            item("basic1", QosClass::Basic, 0),
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
        assert!(
            !g.overcommit,
            "running footprint must never exceed the ceiling"
        );
        let first = |c: QosClass| {
            g.order
                .iter()
                .position(|x| *x == c)
                .unwrap_or_else(|| panic!("tier {c:?} never ran; order={:?}", g.order))
        };
        // Sync (it took the initial slot) → Testnet → Integration → Basic, despite
        // Basic/Integration being submitted before Testnet.
        assert!(
            first(QosClass::Sync) < first(QosClass::Testnet)
                && first(QosClass::Testnet) < first(QosClass::Integration)
                && first(QosClass::Integration) < first(QosClass::Basic),
            "tiers must start in descending-priority order; order={:?}",
            g.order
        );
    }

    /// Retry under contention: with a one-Integration ceiling, a flaky test's
    /// retry must queue behind `hog`, so it goes through the re-park path. The
    /// attempt must be preserved across that wait — a flaky test with `retries=1`
    /// runs at most attempts 1 then 2 (the pre-fix bug reset the attempt to 1 on
    /// backfill, producing an extra, over-limit `[1, 1, 2]` run).
    #[tokio::test]
    async fn retry_under_contention_preserves_attempt_count() {
        let ceiling = QosClass::Integration.profile().footprint; // one slot
        let runs = Arc::new(Mutex::new(Vec::<(String, u32)>::new()));
        let r = runs.clone();
        let mut rep = NullReporter;
        let stats = run_loop(
            vec![
                item("flaky", QosClass::Integration, 1),
                item("hog", QosClass::Integration, 0),
            ],
            ceiling,
            cfg(),
            &mut rep,
            move |it: WorkItem, attempt| {
                r.lock().unwrap().push((it.test_name.clone(), attempt));
                // `flaky` fails its first attempt, passes from the second on.
                let is_fail = it.test_name == "flaky" && attempt < 2;
                async move { if is_fail { fail() } else { pass() } }
            },
            |_, _| {},
        )
        .await;

        let runs = runs.lock().unwrap();
        let flaky: Vec<u32> = runs
            .iter()
            .filter(|(n, _)| n == "flaky")
            .map(|(_, a)| *a)
            .collect();
        assert_eq!(
            flaky,
            vec![1, 2],
            "flaky must run exactly twice: attempts 1 then 2"
        );
        assert_eq!(stats.passed, 2);
        assert_eq!(stats.failed, 0);
    }

    // ── Event-stream contract ──────────────────────────────────────────────
    //
    // `TestEvent` borrows its identity/output, so a reporter that retains events
    // must copy them. `RecordingReporter` keeps an owned, comparable mirror so
    // tests can assert the exact lifecycle the loop emits — the contract the real
    // `StyledReporter` (and any future JUnit writer) consume but which
    // `NullReporter` silently drops.

    /// An owned, comparable mirror of one [`TestEvent`].
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Ev {
        RunStarted {
            total: usize,
        },
        Started {
            test_name: String,
            class: QosClass,
            attempt: u32,
        },
        Slow {
            test_name: String,
            will_terminate: bool,
        },
        Retrying {
            test_name: String,
            next_attempt: u32,
        },
        Finished {
            test_name: String,
            verdict: Verdict,
            output: Vec<u8>,
        },
        Skipped {
            test_name: String,
            reason: SkipReason,
        },
        RunFinished {
            stats: RunStats,
        },
    }

    /// The test identity an event refers to (`None` for run-level events).
    fn ev_name(e: &Ev) -> Option<&str> {
        match e {
            Ev::Started { test_name, .. }
            | Ev::Slow { test_name, .. }
            | Ev::Retrying { test_name, .. }
            | Ev::Finished { test_name, .. }
            | Ev::Skipped { test_name, .. } => Some(test_name),
            Ev::RunStarted { .. } | Ev::RunFinished { .. } => None,
        }
    }

    /// A reporter that records every event in emission order for assertion.
    #[derive(Default)]
    struct RecordingReporter {
        events: Vec<Ev>,
    }

    impl RecordingReporter {
        /// Events for one test, in emission order.
        fn of(&self, name: &str) -> Vec<&Ev> {
            self.events
                .iter()
                .filter(|e| ev_name(e) == Some(name))
                .collect()
        }
    }

    impl RunReporter for RecordingReporter {
        fn handle(&mut self, ev: &TestEvent<'_>) {
            let owned = match ev {
                TestEvent::RunStarted { total, .. } => Ev::RunStarted { total: *total },
                TestEvent::TestStarted {
                    test_name,
                    class,
                    attempt,
                    ..
                } => Ev::Started {
                    test_name: test_name.to_string(),
                    class: *class,
                    attempt: *attempt,
                },
                TestEvent::TestSlow {
                    test_name,
                    will_terminate,
                    ..
                } => Ev::Slow {
                    test_name: test_name.to_string(),
                    will_terminate: *will_terminate,
                },
                TestEvent::TestRetrying {
                    test_name,
                    next_attempt,
                    ..
                } => Ev::Retrying {
                    test_name: test_name.to_string(),
                    next_attempt: *next_attempt,
                },
                TestEvent::TestFinished {
                    test_name,
                    verdict,
                    output,
                    ..
                } => Ev::Finished {
                    test_name: test_name.to_string(),
                    verdict: verdict.clone(),
                    output: output.to_vec(),
                },
                TestEvent::TestSkipped {
                    test_name, reason, ..
                } => Ev::Skipped {
                    test_name: test_name.to_string(),
                    reason: reason.clone(),
                },
                TestEvent::RunFinished { stats, .. } => Ev::RunFinished { stats: *stats },
            };
            self.events.push(owned);
        }
        fn take_scrollback(&mut self) -> Vec<u8> {
            Vec::new()
        }
    }

    /// The stream brackets the run with exactly one `RunStarted` (first) and one
    /// `RunFinished` (last), and every test contributes exactly one start paired
    /// with one terminal finish.
    #[tokio::test]
    async fn event_stream_brackets_run_and_pairs_start_with_finish() {
        let items = vec![
            item("a", QosClass::Integration, 0),
            item("b", QosClass::Integration, 0),
        ];
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
        assert_eq!(
            ev.iter()
                .filter(|e| matches!(e, Ev::RunStarted { .. }))
                .count(),
            1
        );
        assert_eq!(
            ev.iter()
                .filter(|e| matches!(e, Ev::RunFinished { .. }))
                .count(),
            1
        );
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
            assert!(matches!(
                evs[0],
                Ev::Started {
                    attempt: 1,
                    class: QosClass::Integration,
                    ..
                }
            ));
            assert!(matches!(
                evs[1],
                Ev::Finished {
                    verdict: Verdict::Pass,
                    ..
                }
            ));
        }
    }

    /// A retried attempt emits `Retrying` (not `Finished`) and the rerun emits a
    /// fresh `Started` at the next attempt; only the terminal pass emits
    /// `Finished`. This is the subtle contract `NullReporter` hid.
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
                async move {
                    if a.fetch_add(1, Ordering::SeqCst) == 0 {
                        fail()
                    } else {
                        pass()
                    }
                }
            },
            |_, _| {},
        )
        .await;
        assert_eq!(stats.passed, 1);

        let evs = rep.of("flaky");
        assert_eq!(evs.len(), 4, "{evs:?}");
        assert!(matches!(evs[0], Ev::Started { attempt: 1, .. }));
        assert!(matches!(
            evs[1],
            Ev::Retrying {
                next_attempt: 2,
                ..
            }
        ));
        assert!(matches!(evs[2], Ev::Started { attempt: 2, .. }));
        assert!(matches!(
            evs[3],
            Ev::Finished {
                verdict: Verdict::Pass,
                ..
            }
        ));
    }

    /// An unschedulable test emits exactly one `Skipped` (with the reason) and
    /// never a `Started`; the run still closes with `RunFinished`.
    #[tokio::test]
    async fn unschedulable_emits_skipped_with_reason_and_never_starts() {
        let mut rep = RecordingReporter::default();
        run_loop(
            vec![item("huge", QosClass::Sync, 0)],
            Resources::new(1_000, crate::qos::GIB),
            cfg(),
            &mut rep,
            |_it, _a| async { pass() },
            |_, _| {},
        )
        .await;

        let evs = rep.of("huge");
        assert_eq!(evs.len(), 1);
        assert!(matches!(
            evs[0],
            Ev::Skipped {
                reason: SkipReason::ExceedsClusterCapacity,
                ..
            }
        ));
        assert!(
            !rep.events.iter().any(|e| matches!(e, Ev::Started { .. })),
            "a skipped test must never start"
        );
        assert!(matches!(
            rep.events.last(),
            Some(Ev::RunFinished { stats }) if stats.skipped == 1
        ));
    }

    /// The terminal `Finished` event carries the child's captured output, so the
    /// reporter can replay it on failure.
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
        let fin = evs
            .iter()
            .find(|e| matches!(e, Ev::Finished { .. }))
            .expect("a Finished event");
        match fin {
            Ev::Finished {
                verdict, output, ..
            } => {
                assert_eq!(*verdict, Verdict::Fail(2));
                assert_eq!(String::from_utf8_lossy(output), "boom-output");
            }
            _ => unreachable!(),
        }
    }
}
