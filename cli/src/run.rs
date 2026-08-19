//! `ztest run` — preflight orchestration, then the test engine.
//!
//! - TTY: render thread (`console`) pins a status panel, phase output scrolls above
//! - Non-TTY: linear, no panel

use std::borrow::Cow;
use std::io::{IsTerminal, Write, stdout};
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use nextest_metadata::NextestExitCode;

use ztest::api::engine;
use ztest::api::engine::RunSelector;
use ztest::api::inventory::QosEntry;
use ztest::api::pipeline::{self, ArchivesOutcome, BuildOutcome, ProbeOutcome};
use ztest_ui::console::{CapRx, Console, SceneFrame, provision_with_tracker};

/// [`NextestExitCode`] constant → process [`ExitCode`] (codes mirror nextest's, CI
/// cross-references them)
fn exit(code: i32) -> ExitCode {
    ExitCode::from(code as u8)
}

/// Sole fatal-exit path: emit `msg` durably, return `code`.
///
/// - Console → [`Console::fatal`] (raw `eprintln!` is clobbered by the footer repaint)
/// - Non-TTY → stderr already durable
fn fatal(console: Option<&Console>, msg: impl std::fmt::Display, code: i32) -> ExitCode {
    match console {
        Some(c) => c.fatal(msg.to_string()),
        None => eprintln!("{msg}"),
    }
    exit(code)
}

/// Route panics through [`Console::fatal`] (default hook's stderr is clobbered by the
/// footer repaint). Console only — non-TTY keeps the default hook + its backtrace
fn install_panic_surface(console: &Console) {
    let sink = console.clone();
    std::panic::set_hook(Box::new(move |info| {
        sink.fatal(format_panic(info));
    }));
}

/// Panic → fatal-surface text: message, location, backtrace (when `RUST_BACKTRACE` set)
fn format_panic(info: &std::panic::PanicHookInfo<'_>) -> String {
    let payload = info
        .payload()
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string panic payload>");
    let loc = info.location().map(|l| format!(" at {}:{}", l.file(), l.line())).unwrap_or_default();
    let mut out = format!("ztest run: internal error (panic){loc}: {payload}");
    let bt = std::backtrace::Backtrace::capture();
    if matches!(bt.status(), std::backtrace::BacktraceStatus::Captured) {
        out.push_str(&format!("\nbacktrace:\n{bt}"));
    }
    out
}

/// Ctrl-C exit code (shell convention `128 + SIGINT`)
const CANCELLED: i32 = 130;
use ztest::api::resource::NodeState;
use ztest_ui::{self, ArchiveRow, ArchiveStatus, BannerState, ClusterState, Theme, Transfers};

/// `ztest run` arguments.
#[derive(Debug, Parser)]
pub struct Args {
    /// Arguments accepted exactly as by `cargo nextest run`: any flag, filter
    /// expression, or positional. Run `cargo nextest run --help` for the full
    /// reference. Migration is a literal `s/cargo nextest/ztest/`.
    ///
    /// Selection / filter / build flags are forwarded to `cargo nextest list`
    /// unchanged. The ztest engine consumes the run-behavior flags directly:
    /// `--retries`, `--fail-fast` / `--no-fail-fast`, `--no-capture`,
    /// `--profile` / `-P`, `--message-format`, and `-j` / `--test-threads`
    /// (advisory; the engine auto-scales concurrency to QoS capacity).
    ///
    /// Plus one ztest-only flag, recognized here and not forwarded:
    ///
    ///   --no-cleanup   Leave each test's Kubernetes namespace (pods, logs,
    ///                  volumes) in place instead of tearing it down, so you can
    ///                  `kubectl` into a failure for a post-mortem. A 1h janitor
    ///                  backstop still reaps them, so nothing leaks permanently.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, value_name = "NEXTEST_ARGS")]
    pub nextest_args: Vec<String>,

    /// Select a named cluster profile (`ztest cluster list`): binds the
    /// kube-context, cluster class, and image distribution in one shot,
    /// overriding the persisted default and the ambient `ZTEST_IMAGE_REGISTRY`
    /// / `KIND_CLUSTER` / kube-context. Must appear before the nextest args.
    #[arg(long, value_name = "NAME")]
    pub cluster: Option<String>,

    /// Rerun only the tests that did not pass in a previous recorded run:
    /// `latest`, a run-id or unambiguous prefix, or a recording path. Tests new
    /// since that run are included too. Mirrors `cargo nextest run --rerun`.
    /// Must appear before the nextest args.
    #[arg(short = 'R', long = "rerun", value_name = "RUN_ID_OR_RECORDING")]
    pub rerun: Option<RunSelector>,
}

/// Engine-owned flags pulled from the `cargo nextest run`-style argv.
///
/// - `list_args` = the rest, verbatim, for `cargo nextest list`
/// - `no_tests` = `pass`/`warn`/`fail` (nextest default `fail`)
/// - `slow_after` = soft SLOW threshold (`None` disables; hard kill = the tier's `hard_cap`)
/// - `unsupported` = dropped run-only flags, warned about once
#[derive(Debug, Default)]
struct RunOptions {
    list_args: Vec<String>,
    test_threads: Option<u32>,
    no_tests: Option<String>,
    no_cleanup: bool,
    retries: u32,
    fail_fast: bool,
    slow_after: Option<std::time::Duration>,
    success_output: Option<String>,
    failure_output: Option<String>,
    no_capture: bool,
    unsupported: Vec<String>,
    rerun: Option<RunSelector>,
}

impl RunOptions {
    /// Classify the argv in one pass: engine-owned flags out, everything else forwarded
    fn parse(args: &[String]) -> Self {
        // Forward by default (a missed run-only flag fails loudly; a dropped selection
        // flag would silently mis-select)

        // Run-only value flags `nextest list` rejects
        const RUN_VALUE: &[&str] = &[
            "-j",
            "--test-threads",
            "--jobs",
            "--retries",
            "--message-format",
            "--no-tests",
            "--slow-timeout",
            "--success-output",
            "--failure-output",
        ];
        // Run-only booleans `list` rejects (`--no-cleanup` = ztest-only)
        const RUN_BOOL: &[&str] = &[
            "--no-cleanup",
            "--no-capture",
            "--nocapture",
            "--fail-fast",
            "--ff",
            "--no-fail-fast",
            "--nff",
        ];
        // Run-only value flags the engine ignores (stripped so `list` doesn't choke)
        const IGNORED_VALUE: &[&str] = &[
            "--max-fail",
            "--debugger",
            "--tracer",
            "--stress-count",
            "--stress-duration",
            "--status-level",
            "--final-status-level",
            "--show-progress",
            "--max-progress-running",
            "--message-format-version",
            "--archive-file",
            "--archive-format",
            "--extract-to",
            "--cargo-metadata",
            "--workspace-remap",
            "--binaries-metadata",
            "--target-dir-remap",
        ];
        // Run-only booleans the engine ignores
        const IGNORED_BOOL: &[&str] = &[
            "--no-run",
            "--hide-progress-bar",
            "--no-output-indent",
            "--no-input-handler",
            "--extract-overwrite",
            "--persist-extract-tempdir",
        ];
        // Ignored flags that change behavior if dropped (display-only ones drop silently)
        const WARN_UNSUPPORTED: &[&str] = &[
            "--max-fail",
            "--debugger",
            "--tracer",
            "--stress-count",
            "--stress-duration",
            "--archive-file",
            "--extract-to",
            "--cargo-metadata",
            "--binaries-metadata",
            "--no-run",
        ];

        let mut o = RunOptions::default();
        let mut it = args.iter().peekable();
        while let Some(arg) = it.next() {
            // Post-`--` = filter positionals, forwarded verbatim
            if arg == "--" {
                o.list_args.push(arg.clone());
                o.list_args.extend(it.cloned());
                break;
            }
            let (flag, inline) = split_eq(arg);

            if RUN_BOOL.contains(&flag) {
                match flag {
                    "--no-cleanup" => o.no_cleanup = true,
                    "--no-fail-fast" | "--nff" => o.fail_fast = false,
                    "--fail-fast" | "--ff" => o.fail_fast = true,
                    "--no-capture" | "--nocapture" => o.no_capture = true,
                    _ => {}
                }
                continue;
            }
            if IGNORED_BOOL.contains(&flag) {
                if WARN_UNSUPPORTED.contains(&flag) {
                    o.unsupported.push(flag.to_string());
                }
                continue;
            }

            if RUN_VALUE.contains(&flag) || IGNORED_VALUE.contains(&flag) {
                let value = inline.map(str::to_owned).or_else(|| it.next().cloned());
                match flag {
                    "-j" | "--test-threads" | "--jobs" => {
                        o.test_threads = value.as_deref().and_then(|v| v.parse().ok());
                    }
                    "--no-tests" => o.no_tests = value,
                    "--retries" => {
                        o.retries = value.as_deref().and_then(|v| v.parse().ok()).unwrap_or(0);
                    }
                    "--slow-timeout" => {
                        o.slow_after = value.as_deref().and_then(parse_duration_secs);
                    }
                    "--success-output" => o.success_output = value,
                    "--failure-output" => o.failure_output = value,
                    _ => {
                        if WARN_UNSUPPORTED.contains(&flag) {
                            o.unsupported.push(flag.to_string());
                        }
                    }
                }
                continue;
            }

            // Selection / filter / build / `--profile` → `cargo nextest list`, verbatim
            o.list_args.push(arg.clone());
        }
        o
    }

    /// Zero-tests-selected policy from `--no-tests`
    fn no_tests_is_error(&self) -> bool {
        !matches!(self.no_tests.as_deref(), Some("pass") | Some("warn"))
    }

    /// Captured-output policy. Precedence CLI > `ZTEST_*` > `NEXTEST_*` > default
    ///
    /// - Invalid value warns + falls back (never aborts)
    /// - `--no-capture` forces `immediate` only on a stream not otherwise pinned
    fn output_config(&self) -> ztest::api::engine::OutputConfig {
        use ztest::api::engine::output::{CaptureStrategy, OutputConfig, TestOutputDisplay};

        fn env_first(names: &[&str]) -> Option<String> {
            names.iter().find_map(|n| std::env::var(n).ok().filter(|s| !s.trim().is_empty()))
        }
        fn resolve(
            cli: &Option<String>,
            envs: &[&str],
            default: TestOutputDisplay,
        ) -> TestOutputDisplay {
            match cli.clone().or_else(|| env_first(envs)) {
                Some(v) => v.parse().unwrap_or_else(|e| {
                    eprintln!("ztest run: {e}; using default");
                    default
                }),
                None => default,
            }
        }

        let success_env = ["ZTEST_SUCCESS_OUTPUT", "NEXTEST_SUCCESS_OUTPUT"];
        let failure_env = ["ZTEST_FAILURE_OUTPUT", "NEXTEST_FAILURE_OUTPUT"];
        let success_set = self.success_output.is_some() || env_first(&success_env).is_some();
        let failure_set = self.failure_output.is_some() || env_first(&failure_env).is_some();

        let default = OutputConfig::default();
        let mut cfg = OutputConfig {
            success: resolve(&self.success_output, &success_env, default.success),
            failure: resolve(&self.failure_output, &failure_env, default.failure),
            capture: default.capture,
        };
        if self.no_capture {
            cfg.capture = CaptureStrategy::None;
            if !success_set {
                cfg.success = TestOutputDisplay::Immediate;
            }
            if !failure_set {
                cfg.failure = TestOutputDisplay::Immediate;
            }
        }
        cfg
    }
}

/// `--slow-timeout` seconds (`"60"` / `"60s"`) → [`Duration`](std::time::Duration);
/// `None` on malformed
fn parse_duration_secs(s: &str) -> Option<std::time::Duration> {
    let s = s.strip_suffix('s').unwrap_or(s);
    s.parse::<u64>().ok().map(std::time::Duration::from_secs)
}

/// `--flag=value` → `("--flag", Some("value"))`; bare token → `(token, None)`
fn split_eq(arg: &str) -> (&str, Option<&str>) {
    match arg.split_once('=') {
        Some((flag, value)) => (flag, Some(value)),
        None => (arg, None),
    }
}

/// `--rerun` selector → `(binary_id, test_name)` that passed then = excluded now
fn resolve_rerun(sel: &RunSelector) -> Result<std::collections::HashSet<(String, String)>, String> {
    let workspace = engine::locate::current_workspace().map_err(|e| e.to_string())?;
    let dir = engine::locate::resolve(&workspace, sel).map_err(|e| e.to_string())?;
    engine::passed_tests(&dir).map_err(|e| e.to_string())
}

/// `ztest run describe [filter]` — the plan `run` would provision, rendered, cluster-free
async fn describe(list_args: Vec<String>) -> Result<(), String> {
    let build =
        ztest::api::pipeline::index(&list_args).await.map_err(|e| format!("build/list: {e}"))?;
    let BuildOutcome::Ok { selected_binaries, .. } = &build else {
        return Err("build produced no test selection".into());
    };
    let (dump, qos_by_binary) = ztest::api::pipeline::discover(selected_binaries).await;
    let ztest::api::pipeline::DumpOutcome::Discovered {
        seeds,
        images_by_binary,
        deps_by_binary,
        ..
    } = &dump
    else {
        return Err("inventory dump failed".into());
    };
    let plan = ztest::api::plan::for_run(
        selected_binaries,
        images_by_binary,
        deps_by_binary,
        &qos_by_binary,
        seeds,
    );
    print!("{}", ztest_ui::render_plan(&plan, &Theme::detect()));
    Ok(())
}

pub fn execute(args: Args) -> ExitCode {
    // Bail pre-UI (cargo's own error would land in the scroll region as an
    // unexplained "build failed")
    if let Err(detail) = locate_cargo_workspace() {
        eprintln!("ztest run: {detail}");
        eprintln!(
            "       cd into a cargo workspace (one containing a Cargo.toml in this dir or any ancestor) and retry."
        );
        return exit(NextestExitCode::SETUP_ERROR);
    }

    // `describe` recognised only as the *first* token — `nextest_args` is `trailing_var_arg`,
    // so a real subcommand cannot be declared here. Filter for a test *named* `describe`
    // with `-E 'test(describe)'`
    if args.nextest_args.first().is_some_and(|a| a == "describe") {
        let filter = RunOptions::parse(&args.nextest_args[1..]).list_args;
        return crate::block_on("run describe", crate::Rt::Multi, describe(filter));
    }

    let mut opts = RunOptions::parse(&args.nextest_args);
    opts.rerun = args.rerun.clone();
    if !opts.unsupported.is_empty() {
        eprintln!(
            "ztest run: ignoring flag(s) the ztest engine doesn't support: {}",
            opts.unsupported.join(", ")
        );
    }
    let theme = Theme::detect();

    // Bind the target cluster before any thread reads the env.
    // Precedence: --cluster > ambient env > persisted default.
    //
    // SAFETY: set_var must precede thread creation; still single-threaded here.
    match unsafe { ztest::api::cluster_config::activate(args.cluster.as_deref()) } {
        Ok(_) => {}
        Err(detail) => {
            eprintln!("ztest run: {detail}");
            return exit(NextestExitCode::SETUP_ERROR);
        }
    }

    let mut state = build_initial_state(&opts);
    let session_start = Instant::now();

    // Shared run id up front → parent reaper + every (env-inheriting) test child agree on
    // `ztest.io/run-id`; else each derives its own `{user}-{ppid}` and label-reap misses.
    //
    // SAFETY: `set_var` must precede thread creation; still single-threaded here.
    if std::env::var_os("ZTEST_RUN_ID").is_none() && std::env::var_os("GITHUB_RUN_ID").is_none() {
        let user = std::env::var("USER").unwrap_or_else(|_| "anon".into());
        unsafe {
            std::env::set_var("ZTEST_RUN_ID", format!("ztest-{user}-{}", std::process::id()));
        }
    }
    let run_coords = ztest::api::naming::RunCoords::from_env().unwrap_or_else(|_| {
        ztest::api::naming::RunCoords {
            run_id: format!("ztest-{}", std::process::id()),
            user: "anon".to_string(),
        }
    });

    let tty = stdout().is_terminal();

    // One multi-thread runtime for every work-side phase (render thread = its own OS
    // thread, so the panel animates independently)
    let work_rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ztest run: tokio runtime: {e}");
            return exit(NextestExitCode::SETUP_ERROR);
        }
    };

    // Persistent bottom panel (TTY only): render thread owns the terminal, stays live
    // while a phase blocks on a silent subprocess. Non-TTY = linear, banner at the end.
    let (console, guard) = if tty {
        // Painted the instant Ctrl-C arrives, before the work side can react
        let cancel_theme = theme.clone();
        let cancel_panel =
            Box::new(move |elapsed| ztest_ui::render_cancel_panel(elapsed, &cancel_theme));
        match Console::start(session_start, cancel_panel) {
            Ok((c, g)) => (Some(c), Some(g)),
            Err(_) => (None, None),
        }
    } else {
        (None, None)
    };
    if let Some(c) = &console {
        // Panics → render thread (crash surfaces on teardown, not wiped by the footer)
        install_panic_surface(c);
        push_preflight_scene(c, &state, &Transfers::default(), "Preflight", &theme, None);
    }

    let code = run_inner(
        &work_rt,
        &opts,
        &theme,
        &mut state,
        console.as_ref(),
        session_start,
        &run_coords,
    );

    // Teardown only once every phase has stopped producing output (commits the final
    // frame, restores the cursor)
    if let Some(g) = guard {
        g.finish();
    }
    code
}

/// Ctrl-C teardown: label-reap this run's resources, return 130.
///
/// - Reap by `ztest.io/run-id` (a SIGKILLed test never ran its `Drop`)
/// - Deadline-bounded (stuck apiserver must not hang the exit; janitor = backstop)
/// - Skipped under `--no-cleanup`
fn cancel_exit(work_rt: &tokio::runtime::Runtime, run_id: &str, no_cleanup: bool) -> ExitCode {
    if !no_cleanup {
        work_rt.block_on(async {
            match ztest::api::cluster::client().await {
                Ok(client) => {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        ztest::api::resource::reap_run(&client, run_id),
                    )
                    .await
                    {
                        Ok(errors) => {
                            for e in errors {
                                eprintln!("ztest run: cleanup: {e}");
                            }
                        }
                        Err(_) => eprintln!(
                            "ztest run: cleanup timed out; the namespace janitor will reap the rest"
                        ),
                    }
                }
                Err(e) => eprintln!("ztest run: cleanup: no cluster client: {e}"),
            }
        });
    }
    exit(CANCELLED)
}

#[allow(clippy::too_many_arguments)]
fn run_inner(
    work_rt: &tokio::runtime::Runtime,
    opts: &RunOptions,
    theme: &Theme,
    state: &mut BannerState,
    console: Option<&Console>,
    session_start: Instant,
    run: &ztest::api::naming::RunCoords,
) -> ExitCode {
    // Checked after every phase → an interrupted phase exits 130, not "failed"
    let cancelled = || console.is_some_and(Console::cancelled);
    let cancel_exit = || cancel_exit(work_rt, &run.run_id, opts.no_cleanup);

    // Remote clusters ship source, not artifacts (test binaries compile in the builder
    // pod); every other topology takes the local-compile path below
    if ztest::backends::image::builds_on_cluster() {
        return run_inner_on_cluster(work_rt, opts, theme, state, console, run);
    }

    let outcome =
        match pipeline_phase(work_rt, &opts.list_args, theme, state, console, session_start) {
            Ok(o) => o,
            Err(err) => {
                // A subprocess killed by our own Ctrl-C surfaces as an error → cancellation
                if cancelled() {
                    return cancel_exit();
                }
                // unexpected: nextest maps unknown failures to 1
                return fatal(console, format!("ztest run: pipeline phase crashed: {err}"), 1);
            }
        };
    if cancelled() {
        return cancel_exit();
    }

    // Abort before an image build+push onto a cluster we can't run on. Detail surfaced,
    // else it dies with the banner and the run looks like it exited for no reason.
    if let ProbeOutcome::Failed { detail } = &outcome.probe {
        return fatal(
            console,
            format!("ztest run: cluster probe failed — {detail}"),
            NextestExitCode::SETUP_ERROR,
        );
    }

    let image_phase = if let BuildOutcome::Ok { selected_binaries, .. } = &outcome.build {
        run_image_phases(work_rt, selected_binaries, console, state, theme, session_start)
    } else {
        ImagePhaseOutcome::default()
    };
    launch_engine(
        work_rt,
        opts,
        theme,
        state,
        console,
        run,
        Preflight { probe: &outcome.probe, build: &outcome.build, images: image_phase },
    )
}

#[allow(clippy::too_many_arguments)]
/// ServiceAccount this run charges its reservations to (build lease + run lease bill
/// the same identity)
fn service_account() -> String {
    std::env::var("ZTEST_SA").unwrap_or_else(|_| "ztest-local".to_string())
}

/// BuildKit pod + its reservation, as one object (they must die as one)
///
/// - Largest footprint ztest places
/// - Leaked Lease → capacity sterilised until TTL
/// - Leaked pod → unbudgeted memory under a node admission thinks is free
struct ReservedBuilder {
    pod: String,
    reservation: ztest::qos::ledger::Reservation,
}

impl ReservedBuilder {
    /// Reserve, then create the pod it covers (reservation named for the run-id the pod
    /// carries → ledger attributes it, peers subtract it for the pod's whole life)
    async fn acquire(
        client: &kube::Client,
        run: &ztest::api::naming::RunCoords,
        capacity: ztest::qos::ClusterCapacity,
    ) -> Result<Self, String> {
        let reservation = ztest::qos::ledger::acquire(
            client,
            &run.run_id,
            &service_account(),
            &run.user,
            capacity,
            ztest::qos::ledger::Reserve::Fixed(ztest::qos::build::BUILDKIT_BUILD),
            ztest::qos::beacon::LeaseKind::Build,
        )
        .await
        .map_err(|e| e.to_string())?;
        // `?` drops `reservation` → released (the point of the RAII shape)
        let pod = ztest::api::resource::create_build_pod(client, &run.run_id, &run.user)
            .await
            .map_err(|e| e.to_string())?;
        if let Err(e) = ztest::api::resource::wait_build_pod_ready(client, &pod).await {
            ztest::api::resource::delete_build_pod(client, &pod).await;
            return Err(e.to_string());
        }
        Ok(ReservedBuilder { pod, reservation })
    }

    /// Delete the pod, then release its reservation (capacity must not read free while
    /// the pod still terminates on it)
    fn teardown(self, work_rt: &tokio::runtime::Runtime, client: &kube::Client) {
        work_rt.block_on(async {
            ztest::api::resource::delete_build_pod(client, &self.pod).await;
            self.reservation.release().await;
        });
    }
}

/// What the preflight phases resolved, as one value into the engine launch.
struct Preflight<'a> {
    probe: &'a ProbeOutcome,
    build: &'a BuildOutcome,
    images: ImagePhaseOutcome,
}

/// Shared tail: fold the resolved build + image phases into the engine and execute
/// (both compile paths converge here → one [`engine::EngineInput`] assembly)
fn launch_engine(
    work_rt: &tokio::runtime::Runtime,
    opts: &RunOptions,
    theme: &Theme,
    state: &mut BannerState,
    console: Option<&Console>,
    run: &ztest::api::naming::RunCoords,
    preflight: Preflight<'_>,
) -> ExitCode {
    let Preflight { probe, build, images: image_phase } = preflight;
    let cancelled = || console.is_some_and(Console::cancelled);
    let cancel_exit = || cancel_exit(work_rt, &run.run_id, opts.no_cleanup);

    if cancelled() {
        return cancel_exit();
    }

    // Only the Phase-C dump distinguishes a `#[ztest::sync_test]` profile from an ordinary
    // test → prune here, before anything downstream sizes a wave or admits against it
    // (QoS plan + work-list then share one pruned selection)
    let mut selected_binaries = match build {
        BuildOutcome::Ok { selected_binaries, .. } => selected_binaries.clone(),
        BuildOutcome::Failed { .. } => Vec::new(),
    };
    let excluded_sync = engine::drop_sync_tests(
        &mut selected_binaries,
        &image_phase.sync_by_binary,
        &image_phase.qos_by_binary,
    );
    let qos_by_binary = prune_qos(&image_phase.qos_by_binary, &excluded_sync);
    let selected_count: usize = selected_binaries.iter().map(|b| b.selected_tests.len()).sum();

    state.qos_plan = qos_plan_from(&qos_by_binary, probe);

    // Phase B counted the sync profiles it could not recognise → correct the banner
    // pre-render so it states what will actually run
    if !excluded_sync.is_empty()
        && let ztest_ui::BuildState::Ok { test_count, binary_count, .. } = &mut state.build
    {
        *test_count = selected_count;
        *binary_count = selected_binaries.len();
    }

    if let Some(c) = console {
        push_preflight_scene(c, state, &Transfers::default(), "Preflight", theme, None);
    }

    // Non-TTY prints the full resolved banner (CI logs keep a record)
    if console.is_none() {
        let _ = stdout().write_all(ztest_ui::render(state, theme).as_bytes());
    }

    // Child = `cargo nextest`, so its code is already a nextest code (101 build, 94 bad
    // filterset, 2 usage, ...). Collapsing to BUILD_FAILED would lie about the last two
    if let BuildOutcome::Failed { exit_code, .. } = build {
        return exit(*exit_code);
    }
    if let Some(detail) = &image_phase.failure {
        return fatal(
            console,
            format!("ztest run: image preflight failed: {detail}"),
            NextestExitCode::SETUP_ERROR,
        );
    }
    // Name what the filter matched but the engine will not run (no silent absences)
    if !excluded_sync.is_empty() {
        let note = sync_exclusion_notice(&excluded_sync);
        match console {
            Some(c) => c.scrollback(note),
            None => print!("{note}"),
        }
    }
    // No tests selected → honor `--no-tests` (nextest default `fail` ⇒ exit 4). Counted
    // post-prune, so a sync-only filter lands here instead of starting an empty run.
    if selected_count == 0 {
        let only_sync = !excluded_sync.is_empty();
        let (msg, code) = match (only_sync, opts.no_tests_is_error()) {
            (true, _) => (
                "ztest run: no tests to run — the filter matched only sync-tier tests, \
                 which run detached (`ztest sync start <name>`)"
                    .to_string(),
                NextestExitCode::OK,
            ),
            (false, true) => (
                "ztest run: no tests to run (--no-tests=fail)".to_string(),
                NextestExitCode::NO_TESTS_RUN,
            ),
            (false, false) => ("ztest run: no tests to run".to_string(), NextestExitCode::OK),
        };
        return fatal(console, msg, code);
    }

    // Scheduler seed = free capacity (`allocatable - Σ requested` over every pod, peers'
    // Guaranteed included) → runs coexist: first to probe packs, next queues on the rest
    let capacity = match probe {
        ProbeOutcome::Ok { capacity, .. } => capacity,
        _ => {
            return fatal(
                console,
                "ztest run: requires a probed cluster (no kubeconfig / probe unavailable)",
                NextestExitCode::SETUP_ERROR,
            );
        }
    };
    let summary = match build {
        BuildOutcome::Ok { summary, .. } => summary.as_ref(),
        // Build failure already returned `BUILD_FAILED` above
        BuildOutcome::Failed { .. } => unreachable!("build failure handled above"),
    };
    let selected_binaries = selected_binaries.as_slice();

    let sa = service_account();
    // Shared with the reservation lease → ledger's per-run invariant groups the runner pods
    let run_id = format!("ztest-run-{}", std::process::id());

    // Scheduler ceiling = the granted slice, never a raw `free()` snapshot, so concurrent
    // runs carve the node up instead of all claiming the same headroom (`docs/design-qos.md`)
    let client = match work_rt.block_on(ztest::api::cluster::client()) {
        Ok(c) => c,
        Err(e) => {
            return fatal(console, format!("cluster client: {e}"), NextestExitCode::SETUP_ERROR);
        }
    };
    let reservation = match work_rt.block_on(ztest::qos::ledger::acquire(
        &client,
        &run_id,
        &sa,
        &run.user,
        *capacity,
        ztest::qos::ledger::Reserve::Elastic,
        ztest::qos::beacon::LeaseKind::Run,
    )) {
        Ok(g) => g,
        Err(e) => {
            return fatal(console, format!("ztest run: {e}"), NextestExitCode::SETUP_ERROR);
        }
    };
    let ceiling = reservation.reserved();
    // Engine reconciles the scheduler against the live ceiling + reports demand
    // back (→ elastic resize)
    let reservation = std::sync::Arc::new(reservation);

    // `--rerun`: prior run's passed set → excluded from this work-list (bad selector = setup error)
    let rerun_passed = match &opts.rerun {
        Some(sel) => match resolve_rerun(sel) {
            Ok(set) => set,
            Err(e) => {
                return fatal(
                    console,
                    format!("ztest run: --rerun: {e}"),
                    NextestExitCode::SETUP_ERROR,
                );
            }
        },
        None => std::collections::HashSet::new(),
    };

    let input = engine::EngineInput {
        summary,
        selected_binaries,
        qos_by_binary: &qos_by_binary,
        ceiling,
        resource_deps: image_phase.resource_deps,
        resource_states: image_phase.resource_states,
        runner_image: image_phase.runner_image,
        image_refs: image_phase.image_refs,
        rerun_passed,
        opts: engine::EngineOpts {
            retries: opts.retries,
            fail_fast: opts.fail_fast,
            slow_after: opts.slow_after,
            sa,
            no_cleanup: opts.no_cleanup,
            run_id,
            output: opts.output_config(),
        },
        reservation: Some(reservation.clone()),
    };
    let view = console.map(|c| ztest_ui::ConsoleView::new(c, theme));
    let code = engine::run(
        work_rt,
        input,
        view.as_ref().map(|v| v as &dyn ztest::api::engine::RunView),
        state.qos_plan.clone(),
    );
    // Release on every exit → freed capacity reaches the next run now (TTL = crash backstop)
    if let Some(r) = std::sync::Arc::into_inner(reservation) {
        work_rt.block_on(r.release());
    }
    if cancelled() {
        return cancel_exit();
    }
    code
}

/// On-cluster-compile path: probe → ephemeral BuildKit pod → ship source and let it
/// produce the binaries ([`pipeline::remote_compile::compile_on_cluster`]).
///
/// - Probe / builder startup / compile = three distinct phases
/// - Provisioning + the engine tail are the shared paths
fn run_inner_on_cluster(
    work_rt: &tokio::runtime::Runtime,
    opts: &RunOptions,
    theme: &Theme,
    state: &mut BannerState,
    console: Option<&Console>,
    run: &ztest::api::naming::RunCoords,
) -> ExitCode {
    use ztest::api::pipeline::images;
    use ztest_ui::BuildState;

    let cancelled = || console.is_some_and(Console::cancelled);
    let cancel_exit = || cancel_exit(work_rt, &run.run_id, opts.no_cleanup);

    // Phase A: probe (+ archives) — client drives the builder pod, probe's free-capacity
    // ceiling feeds the engine
    let (probe, client) = work_rt.block_on(async {
        let ev_tx = pipeline::channel().0;
        let (probe, client) = pipeline::cluster_run(&ev_tx).await;
        if let Some(c) = &client {
            let archives = pipeline::archives_discover(c, &ev_tx).await;
            apply_update(state, Update::Archives(archives));
        } else {
            apply_update(state, Update::ArchivesSkipped);
        }
        apply_update(state, Update::Probe(probe.clone()));
        (probe, client)
    });
    if cancelled() {
        return cancel_exit();
    }

    // Abort before driving the builder. `Missing` (unresolved kubeconfig/context) instead
    // falls through to the no-client guard below.
    if let ProbeOutcome::Failed { detail } = &probe {
        return fatal(
            console,
            format!("ztest run: cluster probe failed — {detail}"),
            NextestExitCode::SETUP_ERROR,
        );
    }
    let Some(client) = client else {
        return fatal(
            console,
            "ztest run: on-cluster build requires a reachable cluster (no kubeconfig / probe unavailable)",
            NextestExitCode::SETUP_ERROR,
        );
    };

    // Registry repo the runner build pushes to = in-cluster pull address (`ZTEST_IMAGE_REGISTRY`)
    let Some(runner_repo_ref) = ztest::backends::image::runner_repo_ref() else {
        return fatal(
            console,
            "ztest run: on-cluster build requires ZTEST_IMAGE_REGISTRY (the in-cluster pull address)",
            NextestExitCode::SETUP_ERROR,
        );
    };
    let refs = pipeline::remote_compile::BakeRefs { runner_repo_ref };

    // Probe = pre-build snapshot → watch capacity so the panel tracks build-pod and test
    // churn rather than stale headroom
    let initial_cap = match &probe {
        ProbeOutcome::Ok { capacity, .. } => *capacity,
        _ => Default::default(),
    };
    // `spawn` uses `tokio::spawn` → needs a runtime context (no guard = no reactor =
    // panic). `work_rt` keeps the watch tasks live while this thread blocks on the build.
    let cap_watch = {
        let _enter = work_rt.enter();
        pipeline::capacity_watch_spawn(client.clone(), initial_cap)
    };
    let cap_rx = cap_watch.receiver();

    // Phase B/C on the cluster. No local child streams output → the panel's one live row
    // names the current remote sub-phase, timer reset per transition by `on_phase`.
    // Defined pre-build-pod so builder startup reports as the first sub-phase (else an
    // invisible wait that reads as a hung probe).
    let started = Instant::now();

    // Console → builder compiles under a PTY, raw bytes into the emulator (as the local
    // reader does); no console → tty-free, line per line to stderr
    let byte_sink = |bytes: &[u8]| {
        if let Some(c) = console {
            c.output(bytes.to_vec());
        }
    };
    let line_sink = |line: &str| eprintln!("{line}");
    let compile_out = match console {
        Some(c) => pipeline::remote_compile::CompileOut::Pty {
            size: (c.size().cols, c.live_rows()),
            sink: &byte_sink,
        },
        None => pipeline::remote_compile::CompileOut::Lines { sink: &line_sink },
    };

    // Shared driver colours each boundary line; here (where the banner state lives) each
    // new sub-phase drives the live panel row
    let mut on_phase = |ev: pipeline::remote_compile::Phase<'_>| {
        if let Some(phase) = ztest_ui::console::commit_phase(console, theme, ev) {
            state.build = BuildState::Compiling { started_at: Instant::now(), phase: Some(phase) };
            if let Some(c) = console {
                push_preflight_scene(
                    c,
                    state,
                    &Transfers::default(),
                    "Compiling",
                    theme,
                    Some(cap_rx.clone()),
                );
            }
        }
    };

    // Startup builder: reserve the build footprint, then create + await the pod it covers.
    //
    // - Reserve first (pod is Guaranteed and the largest ztest places → a peer must
    //   subtract it for its whole life, else it hands the same memory to tests)
    // - Run-id-labelled → `reap_run` takes it on Ctrl-C / cleanup
    // - Own phase + live timer (a builder waiting on capacity else reads as a hung probe)
    use pipeline::remote_compile::Phase;
    on_phase(Phase::Start("startup builder"));
    let t_builder = Instant::now();
    let builder = match work_rt.block_on(ReservedBuilder::acquire(&client, run, initial_cap)) {
        Ok(b) => b,
        Err(e) => {
            if let Some(c) = console {
                c.flush_live();
            }
            return fatal(
                console,
                format!("ztest run: startup builder failed — {e}"),
                NextestExitCode::SETUP_ERROR,
            );
        }
    };
    on_phase(Phase::Done { label: "builder pod ready", dur: t_builder.elapsed() });
    if cancelled() {
        builder.teardown(work_rt, &client);
        return cancel_exit();
    }

    let remote = work_rt.block_on(pipeline::remote_compile::compile_on_cluster(
        &client,
        &builder.pod,
        &opts.list_args,
        &refs,
        &run.run_id,
        Some(compile_out),
        Some(&mut on_phase),
    ));
    if cancelled() {
        builder.teardown(work_rt, &client);
        return cancel_exit();
    }
    let remote = match remote {
        Ok(r) => r,
        Err(e) => {
            // Drop the builder before bailing (else its Guaranteed footprint + reservation
            // leak until the janitor)
            builder.teardown(work_rt, &client);
            // Commit the failing compile's output first → the error lands after, not above,
            // the output explaining it
            if let Some(c) = console {
                c.flush_live();
            }
            return fatal(
                console,
                format!("ztest run: on-cluster compile failed — {e}"),
                NextestExitCode::BUILD_FAILED,
            );
        }
    };

    let build = remote.build;
    state.build = match &build {
        BuildOutcome::Ok { test_count, binary_count, .. } => BuildState::Ok {
            test_count: *test_count,
            binary_count: *binary_count,
            elapsed: started.elapsed(),
        },
        BuildOutcome::Failed { exit_code, stage } => {
            BuildState::Failed { exit_code: *exit_code, stage: *stage, elapsed: started.elapsed() }
        }
    };

    // Component dev images + seeds still provision through the resource graph; only the
    // runner image is already baked, so it rides in `Prebaked`
    let (images, seeds, images_by_binary, deps_by_binary, sync_by_binary) = match remote.dump {
        images::DumpOutcome::Discovered {
            images,
            seeds,
            images_by_binary,
            deps_by_binary,
            sync_tests: _,
            sync_by_binary,
        } => (images, seeds, images_by_binary, deps_by_binary, sync_by_binary),
        // `compile_on_cluster` only returns `Discovered`; handled rather than unwrapped
        images::DumpOutcome::Failed { detail } => {
            builder.teardown(work_rt, &client);
            if let Some(c) = console {
                c.flush_live();
            }
            return fatal(
                console,
                format!("ztest run: on-cluster inventory dump failed — {detail}"),
                NextestExitCode::SETUP_ERROR,
            );
        }
    };
    let mut image_phase = provision_and_resolve(
        work_rt,
        images,
        seeds,
        images_by_binary,
        deps_by_binary,
        remote.qos_by_binary,
        RunnerSource::Prebaked(remote.runner_image_ref),
        Some(builder.pod.clone()),
        console,
        state,
        theme,
        Some(cap_rx.clone()),
    );
    // Set here, not through `provision_and_resolve` (no use for it, several early
    // returns) → the edge rides every path out
    image_phase.sync_by_binary = sync_by_binary;
    // Build done → drop the builder so neither footprint nor reservation is held through
    // the test run (this release is what lets `launch_engine`'s acquire see it free;
    // `reap_run` backstops the error/cancel paths)
    builder.teardown(work_rt, &client);
    if cancelled() {
        return cancel_exit();
    }

    launch_engine(
        work_rt,
        opts,
        theme,
        state,
        console,
        run,
        Preflight { probe: &probe, build: &build, images: image_phase },
    )
}

/// Clones only on the live path (static path stays borrow-only)
fn with_live_capacity<'a>(snap: &'a BannerState, cap: Option<&CapRx>) -> Cow<'a, BannerState> {
    match cap {
        Some(rx) => {
            let mut s = snap.clone();
            s.cluster.capacity = *rx.borrow();
            Cow::Owned(s)
        }
        None => Cow::Borrowed(snap),
    }
}

fn push_preflight_scene(
    con: &Console,
    state: &BannerState,
    transfers: &Transfers,
    label: &'static str,
    theme: &Theme,
    cap: Option<CapRx>,
) {
    let snap = state.clone();
    let tx = transfers.clone();
    let theme = theme.clone();
    con.scene(move |elapsed| SceneFrame {
        left: ztest_ui::render_preflight_panel(
            &with_live_capacity(&snap, cap.as_ref()),
            label,
            elapsed,
            &theme,
        ),
        mid: None,
        right: ztest_ui::render_transfers(&tx, elapsed, &theme),
        // `None` → live region derives from the avt grid (the child's output)
        live: None,
    });
}

/// `Building`-phase scene. `live: None` → region derives from the avt grid, where every
/// build streams its native output
fn push_building_scene(
    con: &Console,
    state: &BannerState,
    transfers: &Transfers,
    theme: &Theme,
    cap: Option<CapRx>,
) {
    let snap = state.clone();
    let tx = transfers.clone();
    let theme = theme.clone();
    con.scene(move |elapsed| SceneFrame {
        left: ztest_ui::render_preflight_panel(
            &with_live_capacity(&snap, cap.as_ref()),
            "Building",
            elapsed,
            &theme,
        ),
        mid: None,
        right: ztest_ui::render_transfers(&tx, elapsed, &theme),
        live: None,
    });
}

/// Drop excluded tests' QoS declarations → the wave estimate covers what will actually
/// run (sync profiles wear the top-priority `sync` tier the engine never admits)
fn prune_qos(
    qos_by_binary: &[(String, Vec<QosEntry>)],
    excluded: &[engine::ExcludedSync],
) -> Vec<(String, Vec<QosEntry>)> {
    if excluded.is_empty() {
        return qos_by_binary.to_vec();
    }
    let dropped: std::collections::HashSet<(&str, &str)> =
        excluded.iter().map(|e| (e.binary_id.as_str(), e.test_name.as_str())).collect();
    qos_by_binary
        .iter()
        .map(|(binary_id, entries)| {
            let kept: Vec<QosEntry> = entries
                .iter()
                .filter(|e| {
                    !dropped.contains(&(binary_id.as_str(), engine::libtest_name(&e.test_id)))
                })
                .cloned()
                .collect();
            (binary_id.clone(), kept)
        })
        .filter(|(_, entries)| !entries.is_empty())
        .collect()
}

/// Scrollback note naming each declined sync-tier test + (for a profile) the command
/// that does run it
fn sync_exclusion_notice(excluded: &[engine::ExcludedSync]) -> String {
    use engine::SyncExclusion;

    let mut note = format!(
        "Excluded {} sync-tier test(s) from this run — the `sync` tier is owned by \
         `ztest sync start`, not the engine:\n",
        excluded.len()
    );
    for e in excluded {
        let tail = match &e.reason {
            SyncExclusion::Profile(profile) => format!("→ ztest sync start {profile}"),
            SyncExclusion::TierOnly => "(declares the sync tier, no sync profile)".to_string(),
        };
        note.push_str(&format!("  {} ({}) {tail}\n", e.test_name, e.binary_id));
    }
    note
}

/// Per-binary QoS dump → per-tier counts + a wave estimate against probed capacity;
/// `None` when no QoS tests were declared
fn qos_plan_from(
    qos_by_binary: &[(String, Vec<QosEntry>)],
    probe: &ProbeOutcome,
) -> Option<ztest::qos::schedule::QosPlan> {
    // One entry per declared test at its real submitted reserve
    // (count-by-tier × footprint mis-states any run holding an override)
    let tests: Vec<ztest::qos::schedule::PlannedTest> = qos_by_binary
        .iter()
        .flat_map(|(_binary_id, entries)| entries.iter())
        .map(|e| ztest::qos::schedule::PlannedTest {
            class: e.class,
            admitted: e.profile().admitted(),
        })
        .collect();
    if tests.is_empty() {
        return None;
    }
    // Same free-capacity figure the engine scheduler is seeded from
    let ceiling = match probe {
        ProbeOutcome::Ok { capacity, .. } => Some(capacity.free()),
        _ => None,
    };
    Some(ztest::qos::schedule::plan(&tests, ceiling))
}

/// Phase A (probe) + Phase B (build) outcomes from the pipeline.
#[derive(Debug)]
struct PipelineOutcome {
    build: BuildOutcome,
    probe: ProbeOutcome,
}

/// Incremental Phase A (probe / archive discovery) update, mpsc → [`BannerState`]
/// (see [`apply_update`]).
#[derive(Debug)]
enum Update {
    Probe(ProbeOutcome),
    Archives(ArchivesOutcome),
    ArchivesSkipped,
}

/// Phase A (probe + archive discovery) concurrent with Phase B (`cargo nextest list`).
///
/// - TTY: compile under a PTY, probe feeding the panel
/// - Non-TTY: linear, inherited stderr
fn pipeline_phase(
    work_rt: &tokio::runtime::Runtime,
    list_args: &[String],
    theme: &Theme,
    state: &mut BannerState,
    console: Option<&Console>,
    session_start: Instant,
) -> std::io::Result<PipelineOutcome> {
    match console {
        Some(con) => pipeline_console(work_rt, list_args, theme, state, con, session_start),
        None => pipeline_inherited(list_args, state),
    }
}

/// TTY pipeline: pass 1 emulated under a PTY (probe concurrent), then pass 2 (JSON
/// index) captured. Update drain folds a mid-step probe result in as a fresh scene.
fn pipeline_console(
    work_rt: &tokio::runtime::Runtime,
    list_args: &[String],
    theme: &Theme,
    state: &mut BannerState,
    con: &Console,
    _session_start: Instant,
) -> std::io::Result<PipelineOutcome> {
    use ztest::api::pipeline::BuildStage;
    use ztest_ui::BuildState;

    work_rt.block_on(async {
        // Probe + archives feed the panel via `Update`; the throwaway event channel only
        // satisfies the pipeline fns' signature
        let (upd_tx, mut upd_rx) = tokio::sync::mpsc::unbounded_channel::<Update>();
        let probe_handle = {
            let upd = upd_tx.clone();
            let ev_tx = pipeline::channel().0;
            tokio::spawn(async move {
                let (probe, client) = pipeline::cluster_run(&ev_tx).await;
                let _ = upd.send(Update::Probe(probe.clone()));
                match client {
                    Some(c) => {
                        let outcome = pipeline::archives_discover(&c, &ev_tx).await;
                        let _ = upd.send(Update::Archives(outcome));
                    }
                    None => {
                        let _ = upd.send(Update::ArchivesSkipped);
                    }
                }
                probe
            })
        };
        drop(upd_tx);

        let started_at = Instant::now();
        state.build = BuildState::Compiling { started_at, phase: None };
        push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme, None);

        let args = pipeline::compile_argv(list_args);
        let code = run_child_draining(con, "cargo", &args, &[], &mut upd_rx, state, theme).await?;

        let build = if code != 0 {
            // Terminal for the run → commit the child's diagnostics to scrollback now.
            // Teardown would otherwise re-flush the grid *under* the panel = printed twice
            con.flush_live();
            BuildOutcome::Failed { exit_code: code, stage: BuildStage::Compile }
        } else {
            // Pass 2, JSON index: cargo's metadata/freshness pass is multi-second even warm
            // and silent; the drain only exists so a late probe result still lands
            state.build = BuildState::Indexing { started_at: Instant::now() };
            push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme, None);
            drive_draining(pipeline::build_index(list_args), &mut upd_rx, con, state, theme).await?
        };

        state.build = match &build {
            BuildOutcome::Ok { test_count, binary_count, .. } => BuildState::Ok {
                test_count: *test_count,
                binary_count: *binary_count,
                elapsed: started_at.elapsed(),
            },
            BuildOutcome::Failed { exit_code, stage } => BuildState::Failed {
                exit_code: *exit_code,
                stage: *stage,
                elapsed: started_at.elapsed(),
            },
        };

        // Fold late probe/archive updates, then refresh. Do NOT flush the live region:
        // cargo's final frame left in place lets the next child continue on the same grid.
        while let Ok(u) = upd_rx.try_recv() {
            apply_update(state, u);
        }
        push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme, None);

        let probe =
            probe_handle.await.map_err(|e| std::io::Error::other(format!("Phase A: {e}")))?;
        Ok(PipelineOutcome { build, probe })
    })
}

/// Drive a PTY child to completion, folding concurrent probe/archive updates into
/// fresh panel scenes
async fn run_child_draining(
    con: &Console,
    program: &str,
    args: &[String],
    envs: &[(&str, String)],
    upd_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Update>,
    state: &mut BannerState,
    theme: &Theme,
) -> std::io::Result<i32> {
    let child = ztest_ui::console::run_child(con, program, args, envs);
    tokio::pin!(child);
    let mut upd_open = true;
    loop {
        tokio::select! {
            code = &mut child => return code,
            u = upd_rx.recv(), if upd_open => match u {
                Some(u) => {
                    apply_update(state, u);
                    push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme, None);
                }
                None => upd_open = false,
            },
        }
    }
}

/// Drive a future to completion, folding concurrent probe/archive updates into fresh
/// panel scenes
async fn drive_draining<F: std::future::Future>(
    fut: F,
    upd_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Update>,
    con: &Console,
    state: &mut BannerState,
    theme: &Theme,
) -> F::Output {
    tokio::pin!(fut);
    let mut upd_open = true;
    loop {
        tokio::select! {
            out = &mut fut => return out,
            u = upd_rx.recv(), if upd_open => match u {
                Some(u) => {
                    apply_update(state, u);
                    push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme, None);
                }
                None => upd_open = false,
            },
        }
    }
}

/// Non-TTY pipeline: probe + two-pass build, concurrent, inherited stderr (cargo's
/// plain output straight to the log), no panel
fn pipeline_inherited(
    list_args: &[String],
    state: &mut BannerState,
) -> std::io::Result<PipelineOutcome> {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().worker_threads(3).build()?;
    let list_args = list_args.to_vec();

    rt.block_on(async move {
        let (event_tx, mut event_rx) = pipeline::channel();
        let (upd_tx, mut upd_rx) = tokio::sync::mpsc::unbounded_channel::<Update>();

        let cluster_upd = upd_tx.clone();
        let cluster_tx = event_tx.clone();
        let cluster_handle = tokio::spawn(async move {
            let (probe, client) = pipeline::cluster_run(&cluster_tx).await;
            let _ = cluster_upd.send(Update::Probe(probe.clone()));
            match client {
                Some(c) => {
                    let outcome = pipeline::archives_discover(&c, &cluster_tx).await;
                    let _ = cluster_upd.send(Update::Archives(outcome));
                }
                None => {
                    let _ = cluster_upd.send(Update::ArchivesSkipped);
                }
            }
            probe
        });

        let build_tx = event_tx.clone();
        let build_handle =
            tokio::spawn(async move { pipeline::build_run(&list_args, &build_tx, None).await });

        drop(event_tx);
        drop(upd_tx);

        let mut upd_open = true;
        let mut event_open = true;
        while upd_open || event_open {
            tokio::select! {
                upd = upd_rx.recv(), if upd_open => match upd {
                    Some(u) => apply_update(state, u),
                    None => upd_open = false,
                },
                evt = event_rx.recv(), if event_open => match evt {
                    Some(e) => apply_event(state, e),
                    None => event_open = false,
                },
            }
        }

        let probe =
            cluster_handle.await.map_err(|e| std::io::Error::other(format!("Phase A: {e}")))?;
        let build = build_handle
            .await
            .map_err(|e| std::io::Error::other(format!("Phase B join: {e}")))?
            .map_err(|e| std::io::Error::other(format!("Phase B: {e}")))?;

        Ok(PipelineOutcome { build, probe })
    })
}

/// Build-lifecycle [`pipeline::Event`] → mutation on `state.build`, reusing the
/// `BuildStarted` `started_at` for elapsed
fn apply_event(state: &mut BannerState, event: pipeline::Event) {
    use pipeline::Event;
    use ztest_ui::BuildState;

    match event {
        Event::BuildStarted => {
            state.build =
                BuildState::Compiling { started_at: std::time::Instant::now(), phase: None };
        }
        Event::BuildIndexing => {
            // Original `started_at` preserved → clock measures all of Phase B, not pass 2
            let started_at = match &state.build {
                BuildState::Compiling { started_at, .. } => *started_at,
                _ => std::time::Instant::now(),
            };
            state.build = BuildState::Indexing { started_at };
        }
        Event::BuildComplete { test_count, binary_count } => {
            let elapsed = phase_b_elapsed(&state.build);
            state.build = BuildState::Ok { test_count, binary_count, elapsed };
        }
        Event::BuildFailed { exit_code, stage } => {
            let elapsed = phase_b_elapsed(&state.build);
            state.build = BuildState::Failed { exit_code, stage, elapsed };
        }
        // Phase A events arrive on the `Update` channel; duplicates here ignored
        Event::ProbeStarted | Event::ProbeComplete | Event::ProbeFailed => {}
    }
}

fn phase_b_elapsed(build: &ztest_ui::BuildState) -> std::time::Duration {
    use ztest_ui::BuildState;
    match build {
        BuildState::Compiling { started_at, .. } | BuildState::Indexing { started_at } => {
            started_at.elapsed()
        }
        _ => std::time::Duration::ZERO,
    }
}

fn apply_update(state: &mut BannerState, upd: Update) {
    match upd {
        Update::Probe(ProbeOutcome::Ok {
            context,
            nodes_ready,
            nodes_cordoned,
            capacity,
            slots_used,
        }) => {
            state.cluster.context = context;
            state.cluster.nodes_ready = nodes_ready;
            state.cluster.nodes_cordoned = nodes_cordoned;
            state.cluster.capacity = capacity;
            state.cluster.slots_used = slots_used;
        }
        Update::Probe(ProbeOutcome::Missing { detail }) => {
            state.cluster.context = format!("(no kubeconfig: {detail})");
        }
        Update::Probe(ProbeOutcome::Failed { detail }) => {
            state.cluster.context = format!("(probe failed: {detail})");
        }
        Update::Archives(ArchivesOutcome::Discovered { entries }) => {
            state.archives = entries
                .into_iter()
                .map(|e| ArchiveRow {
                    name: e.name,
                    status: if e.ready {
                        ArchiveStatus::Cached { size_bytes: e.size_bytes }
                    } else {
                        ArchiveStatus::Missing { detail: "not yet ready".to_string() }
                    },
                })
                .collect();
        }
        Update::Archives(ArchivesOutcome::NamespaceMissing) => {
            state.archives.clear();
        }
        Update::Archives(ArchivesOutcome::Failed { detail }) => {
            state.archives = vec![ArchiveRow {
                name: "(discovery failed)".to_string(),
                status: ArchiveStatus::Missing { detail },
            }];
        }
        Update::ArchivesSkipped => {
            state.archives.clear();
        }
    }
}

/// What the image/resource phase hands the run phase: QoS dump + the dependency edges
/// and provisioned states the engine gates admission on.
///
/// - `sync_by_binary` = subtracted from the selection (owned by `ztest sync start`)
/// - `runner_image` / `image_refs` = remote runs only; `None` runner → in-process
#[derive(Debug, Default)]
struct ImagePhaseOutcome {
    failure: Option<String>,
    qos_by_binary: Vec<(String, Vec<QosEntry>)>,
    sync_by_binary: Vec<(String, Vec<ztest::api::inventory::SyncTestEntry>)>,
    resource_deps: ztest::api::engine::ResourceDeps,
    resource_states:
        std::collections::HashMap<ztest::api::resource::NodeId, ztest::api::resource::NodeState>,
    runner_image: Option<String>,
    image_refs: std::collections::BTreeMap<String, String>,
}

/// Inventory-driven image phase: Phase C dump names the dev images + seeds the selection
/// needs, resource graph provisions each (skipping what is present).
///
/// - Returns per-binary QoS + the dependency edges and states the engine gates on
/// - Provisioning is serial (see [`provision_and_resolve`])
fn run_image_phases(
    work_rt: &tokio::runtime::Runtime,
    binaries: &[pipeline::SelectedBinary],
    console: Option<&Console>,
    state: &mut BannerState,
    theme: &Theme,
    _session_start: Instant,
) -> ImagePhaseOutcome {
    use ztest::api::pipeline::images;

    // Phase C, inventory dump: every test binary spawned with `ZTEST_DUMP_INVENTORY=1` →
    // deduped dev images + seeds the selection declares, plus the per-binary/per-test edges
    let (outcome, qos_by_binary) = work_rt.block_on(images::discover(binaries));
    let (images, seeds, images_by_binary, deps_by_binary, sync_by_binary) = match outcome {
        images::DumpOutcome::Discovered {
            images,
            seeds,
            images_by_binary,
            deps_by_binary,
            sync_tests: _,
            sync_by_binary,
        } => (images, seeds, images_by_binary, deps_by_binary, sync_by_binary),
        images::DumpOutcome::Failed { detail } => {
            return ImagePhaseOutcome {
                failure: Some(detail),
                qos_by_binary,
                ..Default::default()
            };
        }
    };

    // Local kind only: compute is on this machine → in-process, no runner image (a remote
    // cluster bakes the runner on a separate path)
    let mut phase = provision_and_resolve(
        work_rt,
        images,
        seeds,
        images_by_binary,
        deps_by_binary,
        qos_by_binary,
        RunnerSource::None,
        None,
        console,
        state,
        theme,
        None,
    );
    phase.sync_by_binary = sync_by_binary;
    phase
}

/// Where the runner image (pod-per-test image carrying the compiled binaries) comes from.
///
/// - `None` = local, in-process
/// - `Prebaked` = pull ref the on-cluster builder already pushed
enum RunnerSource {
    None,
    Prebaked(String),
}

/// Provision the selection's dev images + seeds, resolve the engine's admission inputs
/// (dependency edges, states, image refs, runner ref).
///
/// - Shared by both compile paths, which differ only in [`RunnerSource`]
#[allow(clippy::too_many_arguments)]
fn provision_and_resolve(
    work_rt: &tokio::runtime::Runtime,
    images: Vec<ztest::api::inventory::DevImageEntry>,
    seeds: Vec<ztest::api::inventory::SeedEntry>,
    images_by_binary: Vec<(String, Vec<ztest::api::inventory::DevImageEntry>)>,
    deps_by_binary: Vec<(String, Vec<ztest::api::inventory::TestDepEntry>)>,
    qos_by_binary: Vec<(String, Vec<QosEntry>)>,
    runner: RunnerSource,
    // BuildKit pod component-image builds exec against; `None` on the local/kind path
    build_pod: Option<String>,
    console: Option<&Console>,
    state: &mut BannerState,
    theme: &Theme,
    // `Some` on the on-cluster path, where the build pod is still scheduled here
    cap_rx: Option<CapRx>,
) -> ImagePhaseOutcome {
    use std::collections::HashMap;
    use ztest::api::resource;

    let cancelled = || console.is_some_and(Console::cancelled);

    // Survives the no-resources short-circuit below (an on-cluster run with no components
    // still has a runner to run)
    let prebaked = match &runner {
        RunnerSource::Prebaked(r) => Some(r.clone()),
        RunnerSource::None => None,
    };

    if (images.is_empty() && seeds.is_empty()) || cancelled() {
        return ImagePhaseOutcome { qos_by_binary, runner_image: prebaked, ..Default::default() };
    }

    // Commit the compile's final frame + blank the live region → provisioning output lands
    // on a clean grid
    if let Some(c) = console {
        c.flush_live();
        push_building_scene(c, state, &Transfers::default(), theme, cap_rx.clone());
    }

    // Plan the resource graph (images + seeds), then provision.
    //
    // - `probe` skips what is already present
    // - cap 1 keeps the single live region coherent across serial `docker`/`kind` children
    let graph = match resource::plan_runtime(&images, &seeds) {
        Ok(g) => g,
        Err(e) => {
            return ImagePhaseOutcome { failure: Some(e), qos_by_binary, ..Default::default() };
        }
    };
    // Seeds provider talks to the API server → live client (cheap, pooled). Unreachable
    // cluster yields Failed states per seed node; the caller renders them and moves on.
    let client = match work_rt.block_on(ztest::api::cluster::client()) {
        Ok(c) => c,
        Err(e) => {
            return ImagePhaseOutcome {
                failure: Some(format!("connect to cluster for resource provisioning: {e}")),
                qos_by_binary,
                ..Default::default()
            };
        }
    };

    // Shared driver: sequential (cap 1) topological walk, each build's native output through
    // the emulator grid, lifecycle + sub-phase events into the right-column tracker (every
    // transfer change repaints `Building` from this run's banner state)
    let resource_states = work_rt.block_on(provision_with_tracker(
        &graph,
        client,
        build_pod,
        console,
        |transfers: &Transfers| {
            if let Some(c) = console {
                push_building_scene(c, state, transfers, theme, cap_rx.clone());
            }
        },
    ));

    // Dependency edges → the node ids the graph provisioned, so the engine gates each test
    // on the states above. Image ids key the binary edge; seed ids (content-addressed by
    // source) key the per-test edge from `#[ztest::archive]`/`#[needs]`.
    let mut resource_deps = ztest::api::engine::ResourceDeps::default();
    for (binary_id, entries) in &images_by_binary {
        let ids: Vec<_> = entries.iter().filter_map(|e| resource::image_node_id(e).ok()).collect();
        if !ids.is_empty() {
            resource_deps.images_by_binary.insert(binary_id.clone(), ids);
        }
    }
    // OID → seed node id. A test edge names its resource by OID (same as the `SeedDecl` the
    // macro submits) → resolved to the provisioned node with no re-derivation.
    let seed_id_by_source: HashMap<&str, ztest::api::resource::NodeId> =
        seeds.iter().map(|e| (e.oid.as_str(), resource::seed_node_id(e))).collect();
    for (binary_id, deps) in &deps_by_binary {
        for dep in deps {
            if let Some(id) = seed_id_by_source.get(dep.resource.as_str()) {
                let key =
                    (binary_id.clone(), ztest::api::engine::libtest_name(&dep.test_id).to_string());
                resource_deps.seeds_by_test.entry(key).or_default().push(id.clone());
            }
        }
    }

    // Failure isolation (the graph's point): a failed resource skips only its declarants
    // (`SkipReason::DependencyUnavailable`), never the run; cause into scrollback.
    //
    // Dev-image failures are called out distinctly: they do NOT fall back to a published
    // image (`resolve` → `DevImageMissing`, `probe` never substitutes a tag), which is how
    // a `dev!(Validator::Zebrad, …)` failure used to masquerade as a consensus error.
    // `{detail}` carries the underlying `ImageError` (build stderr tail / fetch failure).
    let image_node_ids: std::collections::BTreeSet<ztest::api::resource::NodeId> = images_by_binary
        .iter()
        .flat_map(|(_, entries)| entries.iter().filter_map(|e| resource::image_node_id(e).ok()))
        .collect();
    for (id, st) in &resource_states {
        if let NodeState::Failed(detail) = st {
            let msg = if image_node_ids.contains(id) {
                format!(
                    "dev image {id:?} FAILED TO BUILD/LOAD:\n{detail}\n\
                     → tests that declare this image are SKIPPED; they do NOT fall back to a \
                     published image. Fix the build failure above and re-run `ztest run`."
                )
            } else {
                format!(
                    "resource {id:?} failed to provision ({detail}); tests needing it will be skipped"
                )
            };
            // Durable scrollback, never a raw `eprintln` (render thread owns the terminal;
            // a direct write smears across the panel and teardown can wipe it). Off a TTY
            // there is no render thread → `eprintln`.
            match console {
                Some(c) => c.scrollback(format!("ztest: {msg}\n")),
                None => eprintln!("ztest run: {msg}"),
            }
        }
    }

    // Build manifest (`DevImageId → pull ref`) for every provisioned dev image: seeded
    // process-globally (local kind shares a process with the tests) and forwarded to each
    // remote runner pod via `ZTEST_IMAGE_REFS`
    let image_refs = resource::dev_image_refs(&images_by_binary, &resource_states);
    ztest::backends::image::seed_dev_images(&image_refs);

    // Prebaked ref passes straight through (already built + pushed on-cluster); local kind
    // has no runner at all
    let runner_image = match runner {
        RunnerSource::Prebaked(r) => Some(r),
        RunnerSource::None => None,
    };

    ImagePhaseOutcome {
        failure: None,
        qos_by_binary,
        sync_by_binary: Vec::new(),
        resource_deps,
        resource_states,
        runner_image,
        image_refs,
    }
}

/// Walk up from cwd for a `Cargo.toml`; `Err(detail)` is user-facing. In-process rather
/// than `cargo locate-project` (the common in-workspace path costs nothing at startup)
fn locate_cargo_workspace() -> Result<(), String> {
    let cwd = std::env::current_dir()
        .map_err(|e| format!("could not read current working directory: {e}"))?;
    for dir in cwd.ancestors() {
        if dir.join("Cargo.toml").is_file() {
            return Ok(());
        }
    }
    Err(format!("no Cargo.toml found in {} or any ancestor directory", cwd.display()))
}

fn build_initial_state(opts: &RunOptions) -> BannerState {
    BannerState {
        cluster: ClusterState {
            context: "probing…".to_string(),
            slots_used: 0,
            slots_total: 16,
            slots_configured: opts.test_threads.unwrap_or(0),
            nodes_ready: 0,
            nodes_cordoned: 0,
            capacity: ztest::qos::ClusterCapacity::default(),
        },
        build: ztest_ui::BuildState::Pending,
        archives: Vec::<ArchiveRow>::new(),
        qos_plan: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    fn parse(args: &[&str]) -> RunOptions {
        RunOptions::parse(&v(args))
    }

    #[test]
    fn forwards_selection_and_filter_flags_verbatim() {
        let o = parse(&["-p", "wallet-tests", "--lib", "-E", "test(reorg)"]);
        assert_eq!(o.list_args, v(&["-p", "wallet-tests", "--lib", "-E", "test(reorg)"]));
    }

    #[test]
    fn strips_engine_owned_flags_from_list_args() {
        // Engine-owned run-behavior flags must not reach `cargo nextest list`
        let o = parse(&[
            "-p",
            "wt",
            "--retries",
            "3",
            "--no-fail-fast",
            "--no-capture",
            "-j",
            "8",
            "--message-format",
            "libtest-json",
            "--no-cleanup",
        ]);
        assert_eq!(o.list_args, v(&["-p", "wt"]), "only selection survives");
        assert!(o.no_cleanup);
        assert_eq!(o.test_threads, Some(8));
    }

    #[test]
    fn output_flags_parse_into_display_policy() {
        use ztest::api::engine::TestOutputDisplay;
        let o = parse(&["-p", "wt", "--success-output", "final", "--failure-output", "never"]);
        // Stripped from the list argv (`list` rejects run-only flags)
        assert_eq!(o.list_args, v(&["-p", "wt"]));
        let cfg = o.output_config();
        assert_eq!(cfg.success, TestOutputDisplay::Final);
        assert_eq!(cfg.failure, TestOutputDisplay::Never);
        assert!(cfg.captures() && !cfg.is_serial());
    }

    #[test]
    fn no_capture_is_serial_and_non_capturing() {
        let cfg = parse(&["-p", "wt", "--no-capture"]).output_config();
        assert!(cfg.is_serial(), "--no-capture must serialize the run");
        assert!(!cfg.captures(), "--no-capture must not capture output");
    }

    #[test]
    fn explicit_success_output_survives_no_capture() {
        use ztest::api::engine::TestOutputDisplay;
        // Explicit `--success-output` beats `--no-capture`'s implied `immediate`
        let cfg = parse(&["--no-capture", "--success-output", "final"]).output_config();
        assert_eq!(cfg.success, TestOutputDisplay::Final);
        assert!(cfg.is_serial());
    }

    #[test]
    fn fail_fast_defaults_off_and_opts_in() {
        // Fail-fast ON abandoned the queued majority on the first failure (9 of 122 ran)
        assert!(!parse(&["-p", "wt"]).fail_fast, "default must be OFF");
        assert!(parse(&["-p", "wt", "--fail-fast"]).fail_fast);
        assert!(parse(&["-p", "wt", "--ff"]).fail_fast);
        // Explicit `--no-fail-fast` matches the default (and still strips)
        assert!(!parse(&["-p", "wt", "--no-fail-fast"]).fail_fast);
    }

    #[test]
    fn no_cleanup_anywhere_is_extracted() {
        assert!(parse(&["--no-cleanup", "-E", "test(foo)"]).no_cleanup);
        assert!(parse(&["-p", "x", "--no-cleanup"]).no_cleanup);
        assert!(!parse(&["-p", "x"]).no_cleanup);
    }

    #[test]
    fn no_cleanup_after_double_dash_is_a_filter_positional() {
        // Post-`--` tokens = nextest filter positionals, forwarded verbatim
        let o = parse(&["--", "--no-cleanup"]);
        assert!(!o.no_cleanup);
        assert_eq!(o.list_args, v(&["--", "--no-cleanup"]));
    }

    #[test]
    fn profile_forwards_verbatim() {
        // `--profile`/`-P` uninterpreted → forwarded to both `list` and `run`, so nextest
        // resolves it; both spelling forms
        assert_eq!(parse(&["-P", "ci", "-p", "x"]).list_args, v(&["-P", "ci", "-p", "x"]));
        assert_eq!(parse(&["--profile=ci"]).list_args, v(&["--profile=ci"]));
    }

    #[test]
    fn equals_form_value_flags_are_stripped() {
        let o = parse(&["--retries=2", "-p", "x", "--test-threads=4"]);
        assert_eq!(o.list_args, v(&["-p", "x"]));
        assert_eq!(o.test_threads, Some(4));
    }

    #[test]
    fn strips_run_only_flags_that_list_rejects() {
        // `cargo nextest list` rejects these; reaching it breaks Phase B. Selection survives.
        let o = parse(&[
            "-p",
            "wt",
            "--max-fail",
            "3",
            "--no-tests",
            "warn",
            "--status-level",
            "all",
            "--stress-count",
            "5",
            "--no-run",
            "--final-status-level=none",
            "--archive-file",
            "/tmp/a.tar.zst",
        ]);
        assert_eq!(o.list_args, v(&["-p", "wt"]), "only selection reaches `nextest list`");
        assert_eq!(o.no_tests.as_deref(), Some("warn"));
    }

    #[test]
    fn no_tests_policy() {
        assert!(parse(&[]).no_tests_is_error(), "nextest default is fail");
        assert!(parse(&["--no-tests", "fail"]).no_tests_is_error());
        assert!(!parse(&["--no-tests", "pass"]).no_tests_is_error());
        assert!(!parse(&["--no-tests", "warn"]).no_tests_is_error());
    }

    #[test]
    fn unsupported_flags_are_recorded_for_warning() {
        // Behavior-changing run-only flags surface; display-only ones do not
        let o = parse(&["--archive-file", "/a", "--debugger", "gdb", "--status-level", "all"]);
        assert!(o.unsupported.contains(&"--archive-file".to_string()));
        assert!(o.unsupported.contains(&"--debugger".to_string()));
        assert!(
            !o.unsupported.contains(&"--status-level".to_string()),
            "display-only flags are ignored silently"
        );
    }
}
