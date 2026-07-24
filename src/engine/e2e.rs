//! End-to-end engine stories: wire the real plan → scheduler → `spawn_test` →
//! reporter → panel layers together and assert across their seams at once (the
//! per-module tests each fake out a neighbour, so nothing else covers the seams).
//! Hermetic and fast — the "tests" are tiny `#!/bin/sh` scripts run as
//! short-lived child processes, no cluster.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::engine::events::RunReporter as _;
use crate::engine::local_runner::{EngineEnv, spawn_test};
use crate::engine::plan::{WorkItem, build_work_list};
use crate::engine::reporter::StyledReporter;
use crate::engine::schedule::{LoopConfig, PanelFrame, run_loop};
use crate::inventory::QosEntry;
use crate::pipeline::SelectedBinary;
use crate::qos::{QosClass, Resources};

// ── Fixture scaffolding ────────────────────────────────────────────────────

/// A throwaway directory of executable `#!/bin/sh` "test binaries", removed on
/// drop. Scripts are all written before the run starts, so no write-fd is open
/// across a concurrent `fork`+`exec` (the `ETXTBSY` race).
struct Fixture {
    dir: PathBuf,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("ztest-e2e-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    /// Write `body` as an executable script and return its path.
    fn script(&self, name: &str, body: &str) -> PathBuf {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;
        let path = self.dir.join(format!("{name}.sh"));
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, "#!/bin/sh\n{body}\n").unwrap();
        let mut perms = f.metadata().unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();
        path
    }

    /// A path inside the fixture for cross-process scratch (e.g. a rerun counter).
    fn scratch(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// One selected binary = one script exposing a single libtest test `test_name`.
fn binary(binary_id: &str, script: &Path, test_name: &str) -> SelectedBinary {
    SelectedBinary {
        binary_path: script.to_path_buf(),
        cwd: PathBuf::from("/"),
        binary_id: binary_id.to_string(),
        selected_tests: vec![test_name.to_string()],
    }
}

/// A per-binary QoS dump entry. `test_id` is crate-rooted (`<crate>::<name>`),
/// so the dummy crate prefix exercises the real strip/join in `build_work_list`.
fn qos(binary_id: &str, test_name: &str, class: QosClass) -> (String, Vec<QosEntry>) {
    (
        binary_id.to_string(),
        vec![QosEntry {
            test_id: format!("somecrate::{test_name}"),
            class,
        }],
    )
}

fn env() -> EngineEnv {
    EngineEnv {
        dylib_path: std::ffi::OsString::new(),
        run_id: "e2e-run".into(),
        sa: "ztest-local".into(),
        no_cleanup: false,
        capture: true,
        ztest_log: None,
    }
}

fn cfg(fail_fast: bool, slow_after: Option<Duration>) -> LoopConfig {
    LoopConfig {
        fail_fast,
        slow_after,
        sa: "ztest-local".into(),
        redraw: Duration::from_millis(10),
        run_id: "e2e-run".into(),
        cancel: crate::cancel::Cancel::never(),
        resources: std::collections::HashMap::new(),
        max_inflight: None,
        cap_rx: None,
        demand_tx: None,
    }
}

/// Live concurrency witness, bumped by the real spawn closure as each child
/// starts and finishes: records peak concurrency and flags any moment the
/// running footprint exceeded the ceiling (the no-overload guarantee).
#[derive(Debug, Default)]
struct Concurrency {
    live: usize,
    peak: usize,
    committed: Resources,
    overcommit: bool,
}

impl Concurrency {
    fn enter(&mut self, fp: Resources, ceiling: Resources) {
        self.live += 1;
        self.peak = self.peak.max(self.live);
        self.committed = self.committed.saturating_add(&fp);
        if !self.committed.fits_within(&ceiling) {
            self.overcommit = true;
        }
    }
    fn exit(&mut self, fp: Resources) {
        self.live -= 1;
        self.committed = self.committed.saturating_sub(&fp);
    }
}

/// What an `on_tick` observer saw of the live panel over the run.
#[derive(Debug, Default)]
struct PanelWitness {
    frames: usize,
    max_running: usize,
    free_ever_exceeded_ceiling: bool,
    saw_running: bool,
}

/// Drive a real run and return (stats, scrollback, concurrency, panel). `tweak`
/// adjusts the work-list after planning (e.g. shorten a hard cap) while still
/// exercising the real planner.
async fn drive_real(
    binaries: &[SelectedBinary],
    qos_by_binary: &[(String, Vec<QosEntry>)],
    retries: u32,
    ceiling: Resources,
    cfg: LoopConfig,
    tweak: impl FnOnce(Vec<WorkItem>) -> Vec<WorkItem>,
) -> (
    crate::engine::events::RunStats,
    String,
    Concurrency,
    PanelWitness,
) {
    let items = tweak(build_work_list(
        binaries,
        qos_by_binary,
        retries,
        &crate::engine::plan::ResourceDeps::default(),
    ));
    let env = env();

    let conc = Arc::new(Mutex::new(Concurrency::default()));
    let conc_spawn = conc.clone();

    let mut reporter =
        StyledReporter::new(false, true, crate::engine::output::OutputConfig::default());
    let panel = Arc::new(Mutex::new(PanelWitness::default()));
    let panel_tick = panel.clone();

    let stats = run_loop(
        items,
        ceiling,
        cfg,
        &mut reporter,
        move |item: WorkItem, _attempt| {
            let env = env.clone();
            let conc = conc_spawn.clone();
            let fp = item.footprint;
            async move {
                conc.lock().unwrap().enter(fp, ceiling);
                let cap = item.hard_cap;
                let out = spawn_test(&item, &env, cap, &crate::cancel::Cancel::never()).await;
                conc.lock().unwrap().exit(fp);
                out
            }
        },
        // Never drain the reporter here, so the full scrollback survives for one
        // post-run assertion; only record what the panel would have shown.
        move |_rep, frame: &PanelFrame| {
            let mut w = panel_tick.lock().unwrap();
            w.frames += 1;
            w.max_running = w.max_running.max(frame.running.len());
            if !frame.running.is_empty() {
                w.saw_running = true;
            }
            if !frame.free.fits_within(&ceiling) {
                w.free_ever_exceeded_ceiling = true;
            }
        },
    )
    .await;

    let scrollback = String::from_utf8(reporter.take_scrollback()).unwrap();
    let conc = Arc::try_unwrap(conc).unwrap().into_inner().unwrap();
    let panel = Arc::try_unwrap(panel).unwrap().into_inner().unwrap();
    (stats, scrollback, conc, panel)
}

// ── Story 1 ─────────────────────────────────────────────────────────────────

/// A mixed suite packs the cluster and runs to completion: three Integration
/// tests share a ceiling fitting two at a time; two pass, one fails. Asserts
/// every test reaches a verdict, concurrency never exceeds two, the failure's
/// output replays, and the summary tallies correctly — the engine's spine in
/// one path.
#[tokio::test]
async fn mixed_suite_packs_runs_to_completion_and_reports_each_verdict() {
    let fx = Fixture::new("mixed");
    // Short dwell so two children genuinely overlap without slowing the suite.
    let pass_a = fx.script("alpha", "sleep 0.06; echo alpha-ok; exit 0");
    let pass_b = fx.script("beta", "sleep 0.06; echo beta-ok; exit 0");
    let fail_c = fx.script("gamma", "echo kaboom-from-gamma 1>&2; exit 1");

    let binaries = [
        binary("pkg::alpha", &pass_a, "runs_fine"),
        binary("pkg::beta", &pass_b, "also_fine"),
        binary("pkg::gamma", &fail_c, "blows_up"),
    ];
    let qos_dump = [
        qos("pkg::alpha", "runs_fine", QosClass::Integration),
        qos("pkg::beta", "also_fine", QosClass::Integration),
        qos("pkg::gamma", "blows_up", QosClass::Integration),
    ];
    // The scheduler charges each test its admitted footprint (tier footprint +
    // per-test runner reserve), so the ceiling must be sized from `admitted()`,
    // not the bare tier footprint, or fewer than two tests are admitted.
    let i = QosClass::Integration.profile().admitted();
    let ceiling = Resources::new(2 * i.cpu_milli, 2 * i.mem_bytes, 0, 0); // fits exactly 2

    let (stats, out, conc, panel) =
        drive_real(&binaries, &qos_dump, 0, ceiling, cfg(false, None), |w| w).await;

    // Whole suite reached a verdict — nothing left parked.
    assert_eq!(stats.total, 3);
    assert_eq!(stats.finished(), 3, "every test must finish; {out}");
    assert_eq!(stats.passed, 2);
    assert_eq!(stats.failed, 1);

    // No overload: real processes never exceeded the two-slot ceiling.
    assert!(!conc.overcommit, "running footprint exceeded the ceiling");
    assert!(conc.peak <= 2, "peak real concurrency {} > 2", conc.peak);
    assert!(
        conc.peak >= 2,
        "two slots should actually be used; got {}",
        conc.peak
    );

    // The report carries each verdict, replays the failure, and tallies up.
    assert!(out.contains("PASS"), "{out}");
    assert!(out.contains("FAIL"), "{out}");
    assert!(out.contains("pkg::alpha"), "{out}");
    assert!(out.contains("pkg::gamma"), "{out}");
    assert!(
        out.contains("kaboom-from-gamma"),
        "failure output must replay; {out}"
    );
    assert!(out.contains("3 tests run"), "{out}");
    assert!(out.contains("2 passed"), "{out}");
    assert!(out.contains("1 failed"), "{out}");

    // The live panel saw the run and never reported more concurrency than the
    // scheduler allowed.
    assert!(panel.frames > 0, "the panel must have ticked");
    assert!(
        panel.saw_running,
        "the panel should have shown running tests"
    );
    assert!(
        panel.max_running <= 2,
        "panel showed {} running > 2",
        panel.max_running
    );
    assert!(!panel.free_ever_exceeded_ceiling);
}

// ── Story 2 ─────────────────────────────────────────────────────────────────

/// A flaky test recovers on retry: a child fails its first process then passes
/// its second (counting attempts through a file, so the retry is a genuinely
/// separate process). With `retries = 1` the run finishes green: the scrollback
/// shows the failed first attempt as `TRY 1 FAIL` (nextest labels the retry line
/// with the attempt that just failed) and the final `PASS`, and the summary
/// reports no failure (a superseded attempt isn't counted).
#[tokio::test]
async fn flaky_test_recovers_on_retry_and_retry_is_logged() {
    let fx = Fixture::new("flaky");
    let counter = fx.scratch("attempts");
    let body = format!(
        "n=$(cat '{c}' 2>/dev/null || echo 0); n=$((n+1)); echo \"$n\" > '{c}'; \
         if [ \"$n\" -lt 2 ]; then echo flaked-on-$n 1>&2; exit 1; fi; echo recovered; exit 0",
        c = counter.display()
    );
    let flaky = fx.script("flaky", &body);

    let binaries = [binary("pkg::flaky", &flaky, "sometimes_fails")];
    let qos_dump = [qos("pkg::flaky", "sometimes_fails", QosClass::Integration)];
    let i = QosClass::Integration.profile().admitted();
    let ceiling = Resources::new(i.cpu_milli, i.mem_bytes, 0, 0);

    let (stats, out, _conc, _panel) =
        drive_real(&binaries, &qos_dump, 1, ceiling, cfg(false, None), |w| w).await;

    assert_eq!(stats.passed, 1, "the retry must pass; {out}");
    assert_eq!(stats.failed, 0, "a recovered flake is not a failure; {out}");
    let attempts = std::fs::read_to_string(&counter).unwrap();
    assert_eq!(attempts.trim(), "2", "should have run exactly twice");
    assert!(
        out.contains("TRY 1 FAIL"),
        "the failed first attempt must show as TRY 1 FAIL; {out}"
    );
    assert!(out.contains("PASS"), "{out}");
    // The inline retry line legitimately carries the uppercase verdict word
    // `FAIL`; a recovered flake must simply not be counted as a failure in the
    // summary (lowercase `failed`, present only when a test ends failed).
    assert!(
        !out.contains("failed"),
        "a recovered flake must not be tallied as failed; {out}"
    );
}

// ── Story 3 ─────────────────────────────────────────────────────────────────

/// An oversized (Sync-tier) test that can't fit a small ceiling even on an empty
/// cluster is skipped with a visible reason and recorded in stats + summary,
/// while the Basic test alongside it still runs. Guards against silently
/// dropping a selected test.
#[tokio::test]
async fn oversized_test_is_skipped_with_reason_while_the_rest_runs() {
    let fx = Fixture::new("skip");
    let ok = fx.script("ok", "echo small-ok; exit 0");
    let huge = fx.script("huge", "echo should-never-run; exit 0");

    let binaries = [
        binary("pkg::ok", &ok, "fits"),
        binary("pkg::huge", &huge, "too_big"),
    ];
    let qos_dump = [
        qos("pkg::ok", "fits", QosClass::Basic),
        qos("pkg::huge", "too_big", QosClass::Sync),
    ];
    // Ceiling fits Basic but is far below a Sync footprint → Sync is rejected.
    let b = QosClass::Basic.profile().footprint;
    let ceiling = Resources::new(b.cpu_milli * 2, b.mem_bytes * 2, 0, 0);

    let (stats, out, _conc, _panel) =
        drive_real(&binaries, &qos_dump, 0, ceiling, cfg(false, None), |w| w).await;

    assert_eq!(stats.passed, 1, "the schedulable test still runs; {out}");
    assert_eq!(
        stats.skipped, 1,
        "the oversized test is skipped, not dropped; {out}"
    );
    assert_eq!(stats.failed, 0);
    assert!(out.contains("SKIP"), "{out}");
    assert!(
        out.contains("exceeds cluster capacity"),
        "skip reason must show; {out}"
    );
    assert!(out.contains("pkg::huge"), "{out}");
    assert!(
        out.contains("PASS"),
        "the basic test must report PASS; {out}"
    );
    assert!(
        out.contains("1 skipped"),
        "summary must tally the skip; {out}"
    );
    // The huge script must never have executed.
    assert!(
        !out.contains("should-never-run"),
        "skipped test must not run; {out}"
    );
}

// ── Story 4 ─────────────────────────────────────────────────────────────────

/// A hung test goes SLOW, is killed at its hard cap as TIMEOUT, and frees its
/// one-slot ceiling to backfill the queued test, which runs and passes. The only
/// path exercising the soft-slow signal, the hard-cap kill through the loop, and
/// capacity-driven backfill after a non-clean exit.
#[tokio::test]
async fn hung_test_goes_slow_then_times_out_and_frees_the_slot() {
    let fx = Fixture::new("timeout");
    let hang = fx.script("hang", "echo entering-hang; sleep 30");
    let ok = fx.script("ok", "echo queued-ran; exit 0");

    let binaries = [
        binary("pkg::hang", &hang, "never_returns"),
        binary("pkg::ok", &ok, "waits_its_turn"),
    ];
    let qos_dump = [
        qos("pkg::hang", "never_returns", QosClass::Integration),
        qos("pkg::ok", "waits_its_turn", QosClass::Integration),
    ];
    let i = QosClass::Integration.profile().admitted();
    let ceiling = Resources::new(i.cpu_milli, i.mem_bytes, 0, 0); // one slot → ok queues

    // Shorten the hung test's hard cap so the story runs fast; the slow threshold
    // sits below it so SLOW fires first.
    let tweak = |items: Vec<WorkItem>| {
        items
            .into_iter()
            .map(|mut w| {
                if w.test_name == "never_returns" {
                    w.hard_cap = Duration::from_millis(350);
                }
                w
            })
            .collect()
    };

    let (stats, out, _conc, _panel) = drive_real(
        &binaries,
        &qos_dump,
        0,
        ceiling,
        cfg(false, Some(Duration::from_millis(80))),
        tweak,
    )
    .await;

    // The hung test timed out; the queued test backfilled and passed.
    assert_eq!(stats.total, 2);
    assert_eq!(
        stats.finished(),
        2,
        "both tests must reach a verdict; {out}"
    );
    assert_eq!(
        stats.passed, 1,
        "the queued test must run after the slot frees; {out}"
    );
    assert_eq!(stats.failed, 1, "the timeout counts as a failure; {out}");
    assert!(
        out.contains("TIMEOUT"),
        "the hung test must report TIMEOUT; {out}"
    );
    assert!(
        out.contains("SLOW"),
        "the hung test must cross the slow threshold; {out}"
    );
    // The backfilled test reaching a PASS verdict (not SKIP) proves the freed
    // slot admitted it — a passing test's stdout is not replayed to scrollback.
    assert!(
        out.contains("PASS") && out.contains("pkg::ok"),
        "the backfilled test must run and pass after the slot frees; {out}"
    );
}
