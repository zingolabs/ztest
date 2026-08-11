//! `ztest run`: preflight orchestration then the test engine. On a TTY a
//! persistent render thread (`cli::console`) pins a status panel to the bottom
//! while each phase's output scrolls above it; non-TTY runs linearly, no panel.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::{IsTerminal, Write, stdout};
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use nextest_metadata::NextestExitCode;

use crate::cli::console::{CapRx, Console, SceneFrame, provision_with_tracker};
use crate::engine;
use crate::engine::record::RunSelector;
use crate::inventory::QosEntry;
use crate::pipeline::{self, ArchivesOutcome, BuildOutcome, ProbeOutcome};

/// A [`NextestExitCode`] constant as a process [`ExitCode`]. `ztest run` mirrors
/// nextest's documented exit codes so CI can cross-reference them.
fn exit(code: i32) -> ExitCode {
    ExitCode::from(code as u8)
}

/// The single fatal-exit path: emit `msg` durably and return `code`.
///
/// With a console the message goes through [`Console::fatal`], which the render
/// thread reprints on a clean line once the pinned panel is torn down. A raw
/// `eprintln!` here would instead be overwritten by the footer's in-place
/// repaint and vanish behind a frozen frame — the failure mode that made setup
/// errors look like silent stalls. Without a console (non-TTY) stderr is already
/// durable, so it prints directly.
fn fatal(console: Option<&Console>, msg: impl std::fmt::Display, code: i32) -> ExitCode {
    match console {
        Some(c) => c.fatal(msg.to_string()),
        None => eprintln!("{msg}"),
    }
    exit(code)
}

/// Route panics through the durable fatal surface. The default panic hook writes
/// to stderr while the render thread is still repainting the footer, so its
/// message is clobbered; instead capture the panic and hand it to the render
/// thread, which reprints it on teardown (see [`Console::fatal`]). Installed only
/// with a console — non-TTY keeps the default hook, whose stderr is already
/// durable and carries the standard backtrace.
fn install_panic_surface(console: &Console) {
    let sink = console.clone();
    std::panic::set_hook(Box::new(move |info| {
        sink.fatal(format_panic(info));
    }));
}

/// Format a panic for the fatal surface: message, source location, and — when
/// `RUST_BACKTRACE` is set — the captured backtrace.
fn format_panic(info: &std::panic::PanicHookInfo<'_>) -> String {
    let payload = info
        .payload()
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string panic payload>");
    let loc = info
        .location()
        .map(|l| format!(" at {}:{}", l.file(), l.line()))
        .unwrap_or_default();
    let mut out = format!("ztest run: internal error (panic){loc}: {payload}");
    let bt = std::backtrace::Backtrace::capture();
    if matches!(bt.status(), std::backtrace::BacktraceStatus::Captured) {
        out.push_str(&format!("\nbacktrace:\n{bt}"));
    }
    out
}

/// Exit code for a Ctrl-C-interrupted run: the shell convention `128 + SIGINT`.
const CANCELLED: i32 = 130;
use crate::resource::NodeState;
use crate::ui::{self, ArchiveRow, ArchiveStatus, BannerState, ClusterState, Theme, Transfers};

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
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "NEXTEST_ARGS"
    )]
    pub nextest_args: Vec<String>,

    /// Select a named cluster profile (`ztest cluster list`): binds the
    /// kube-context, image distribution, and OpenShift flag in one shot,
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

/// The flags `ztest run` pulls out of its `cargo nextest run`-style argv: the
/// few the engine acts on, plus the verbatim vector forwarded to
/// `cargo nextest list`.
#[derive(Debug, Default)]
struct RunOptions {
    /// Args for `cargo nextest list` (Phase B inventory): the verbatim argv with
    /// the run-only flags stripped, since `list` rejects them.
    list_args: Vec<String>,
    /// `-j` / `--test-threads`: surfaced in the preflight banner only.
    test_threads: Option<u32>,
    /// `--no-tests <ACTION>`: behavior when zero tests are selected
    /// (`pass` / `warn` / `fail`; nextest default `fail`).
    no_tests: Option<String>,
    /// `--no-cleanup` (ztest-only): leave each test's k8s namespace standing
    /// (passed to the run as the `ZTEST_NO_CLEANUP` env, not a nextest flag).
    no_cleanup: bool,
    /// `--retries N`: max retry attempts per failed test (engine-owned; default 0).
    retries: u32,
    /// `--fail-fast` / `--no-fail-fast`: stop admitting on the first terminal
    /// failure. Default off — pack the whole suite and report every result;
    /// `--fail-fast` opts back in.
    fail_fast: bool,
    /// `--slow-timeout <DUR>` (ztest-only): soft "slow" threshold; hard kill is
    /// the tier `hard_cap`. `None` disables the SLOW signal.
    slow_after: Option<std::time::Duration>,
    /// `--success-output <WHEN>`: when a passing test's captured output is shown
    /// (`immediate`/`immediate-final`/`final`/`never`). `None` → env/default.
    success_output: Option<String>,
    /// `--failure-output <WHEN>`: as above, for failing tests.
    failure_output: Option<String>,
    /// `--no-capture`: stream output live and run serially (nextest coupling).
    no_capture: bool,
    /// Run-only flags the user passed that ztest drops (e.g. `--archive-file`,
    /// `--debugger`), surfaced as a warning. Display-only flags aren't listed.
    unsupported: Vec<String>,
    /// `-R/--rerun`: the prior run whose non-passing tests to re-execute. Not
    /// parsed from the passthrough argv — it's a real `ztest run` flag copied in
    /// from [`Args`] after classification.
    rerun: Option<RunSelector>,
}

impl RunOptions {
    /// Classify the verbatim `ztest run` argv in one pass: extract the
    /// engine-owned flags, forward everything else to `cargo nextest list`.
    fn parse(args: &[String]) -> Self {
        // Forward by default, strip the run-only flags here: forward-by-default
        // fails loudly if we miss one, whereas a dropped selection flag would
        // silently mis-select.

        // Run-only value-taking flags `cargo nextest list` rejects: stripped.
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
        // Run-only booleans `list` rejects. `--no-cleanup` is ztest-only.
        const RUN_BOOL: &[&str] = &[
            "--no-cleanup",
            "--no-capture",
            "--nocapture",
            "--fail-fast",
            "--ff",
            "--no-fail-fast",
            "--nff",
        ];
        // Run-only value-taking flags the engine ignores; stripped so `list`
        // doesn't choke on them.
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
        // Run-only booleans the engine ignores.
        const IGNORED_BOOL: &[&str] = &[
            "--no-run",
            "--hide-progress-bar",
            "--no-output-indent",
            "--no-input-handler",
            "--extract-overwrite",
            "--persist-extract-tempdir",
        ];
        // Ignored flags that change behavior if dropped (unlike display-only
        // flags, which ztest owns and drops silently) — worth warning about.
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
            // After `--`, the rest are filter positionals: forward verbatim.
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

            // Everything else (selection / filter / build / `--profile`) forwards
            // verbatim to `cargo nextest list`.
            o.list_args.push(arg.clone());
        }
        o
    }

    /// The zero-tests-selected policy from `--no-tests` (`pass`/`warn`/`fail`;
    /// nextest default `fail`).
    fn no_tests_is_error(&self) -> bool {
        !matches!(self.no_tests.as_deref(), Some("pass") | Some("warn"))
    }

    /// Resolve the captured-output policy: CLI flag > `ZTEST_*` > `NEXTEST_*` >
    /// default. An invalid value warns and falls back rather than aborting.
    /// `--no-capture` forces `immediate` only for a stream not otherwise pinned.
    fn output_config(&self) -> crate::engine::output::OutputConfig {
        use crate::engine::output::{CaptureStrategy, OutputConfig, TestOutputDisplay};

        fn env_first(names: &[&str]) -> Option<String> {
            names
                .iter()
                .find_map(|n| std::env::var(n).ok().filter(|s| !s.trim().is_empty()))
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

/// Parse a `--slow-timeout` value as seconds (`"60"` or `"60s"`) into a
/// [`Duration`](std::time::Duration). `None` on a malformed value.
fn parse_duration_secs(s: &str) -> Option<std::time::Duration> {
    let s = s.strip_suffix('s').unwrap_or(s);
    s.parse::<u64>().ok().map(std::time::Duration::from_secs)
}

/// Split `--flag=value` into `("--flag", Some("value"))`; a bare token yields
/// `(token, None)`.
fn split_eq(arg: &str) -> (&str, Option<&str>) {
    match arg.split_once('=') {
        Some((flag, value)) => (flag, Some(value)),
        None => (arg, None),
    }
}

/// Resolve a `--rerun` selector to the set of `(binary_id, test_name)` that
/// passed in that run — the tests this run will exclude.
fn resolve_rerun(sel: &RunSelector) -> Result<std::collections::HashSet<(String, String)>, String> {
    let workspace = engine::record::locate::current_workspace().map_err(|e| e.to_string())?;
    let dir = engine::record::locate::resolve(&workspace, sel).map_err(|e| e.to_string())?;
    engine::record::passed_tests(&dir).map_err(|e| e.to_string())
}

pub fn execute(args: Args) -> ExitCode {
    // Bail before any UI is drawn if we're not in a cargo workspace: cargo's
    // own error would otherwise land in the pinned-banner scroll region and be
    // squashed into an unexplained "build failed".
    if let Err(detail) = locate_cargo_workspace() {
        eprintln!("ztest run: {detail}");
        eprintln!(
            "       cd into a cargo workspace (one containing a Cargo.toml in this dir or any ancestor) and retry."
        );
        return exit(NextestExitCode::SETUP_ERROR);
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

    // Bind the target cluster from the selected profile before any thread reads
    // the env. Precedence: --cluster > ambient env > persisted default.
    //
    // SAFETY: set_var must precede thread creation; still single-threaded here.
    match unsafe { crate::cluster_config::activate(args.cluster.as_deref()) } {
        Ok(_) => {}
        Err(detail) => {
            eprintln!("ztest run: {detail}");
            return exit(NextestExitCode::SETUP_ERROR);
        }
    }

    let mut state = build_initial_state(&opts);
    let session_start = Instant::now();

    // Force a shared run id before any thread starts so the parent's reaper and
    // every test child (env-inherited) agree on the `ztest.io/run-id` label;
    // otherwise each derives a different `{user}-{ppid}` id and label-reap can't
    // target the children's resources.
    //
    // SAFETY: `set_var` must precede thread creation; still single-threaded here.
    if std::env::var_os("ZTEST_RUN_ID").is_none() && std::env::var_os("GITHUB_RUN_ID").is_none() {
        let user = std::env::var("USER").unwrap_or_else(|_| "anon".into());
        unsafe {
            std::env::set_var(
                "ZTEST_RUN_ID",
                format!("ztest-{user}-{}", std::process::id()),
            );
        }
    }
    let run_coords =
        crate::naming::RunCoords::from_env().unwrap_or_else(|_| crate::naming::RunCoords {
            run_id: format!("ztest-{}", std::process::id()),
            user: "anon".to_string(),
        });

    let tty = stdout().is_terminal();

    // One multi-thread runtime drives every work-side phase; the render thread
    // is a separate OS thread so the panel animates independently.
    let work_rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ztest run: tokio runtime: {e}");
            return exit(NextestExitCode::SETUP_ERROR);
        }
    };

    // The persistent bottom panel (TTY only): a dedicated render thread owns the
    // terminal and stays live even while a phase blocks on a silent subprocess.
    // Non-TTY runs linearly with inherited stdio; the banner prints at the end.
    let (console, guard) = if tty {
        // The render thread paints this the instant Ctrl-C arrives, before the
        // work side has a chance to react.
        let cancel_theme = theme.clone();
        let cancel_panel = Box::new(move |elapsed| ui::render_cancel_panel(elapsed, &cancel_theme));
        match Console::start(session_start, cancel_panel) {
            Ok((c, g)) => (Some(c), Some(g)),
            Err(_) => (None, None),
        }
    } else {
        (None, None)
    };
    if let Some(c) = &console {
        // Route panics to the render thread so a crash surfaces on teardown
        // instead of being wiped by the live footer.
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

    // Tear the render thread down only after every phase has finished producing
    // output (commits the final frame, restores the cursor).
    if let Some(g) = guard {
        g.finish();
    }
    code
}

/// Ctrl-C teardown: reap the current run's resources by `ztest.io/run-id` label
/// (a SIGKILLed test never ran its `Drop`) and return the conventional 130.
/// Deadline-bounded so a stuck apiserver can't hang the exit — the namespace
/// janitor is the backstop. Skipped under `--no-cleanup`.
fn cancel_exit(work_rt: &tokio::runtime::Runtime, run_id: &str, no_cleanup: bool) -> ExitCode {
    if !no_cleanup {
        work_rt.block_on(async {
            match crate::cluster::client().await {
                Ok(client) => {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        crate::resource::reap_run(&client, run_id),
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
    run: &crate::naming::RunCoords,
) -> ExitCode {
    // Cancellation (Ctrl-C) is checked after every phase, so an interrupted
    // phase exits 130 via `cancel_exit` rather than misreporting as a failure.
    let cancelled = || console.is_some_and(Console::cancelled);
    let cancel_exit = || cancel_exit(work_rt, &run.run_id, opts.no_cleanup);

    // On-cluster-build targets (OpenShift) ship source, not artifacts: the test
    // binaries compile in the builder pod. Every other topology keeps the
    // local-compile path below.
    if crate::backends::image::builds_on_cluster() {
        return run_inner_on_cluster(work_rt, opts, theme, state, console, run);
    }

    let outcome = match pipeline_phase(
        work_rt,
        &opts.list_args,
        theme,
        state,
        console,
        session_start,
    ) {
        Ok(o) => o,
        Err(err) => {
            // A subprocess killed by our own Ctrl-C surfaces here as an error;
            // report it as cancellation, not a crash.
            if cancelled() {
                return cancel_exit();
            }
            // unexpected: nextest maps unknown failures to 1
            return fatal(
                console,
                format!("ztest run: pipeline phase crashed: {err}"),
                1,
            );
        }
    };
    if cancelled() {
        return cancel_exit();
    }

    // A hard probe failure aborts before we waste an image build+push on a
    // cluster we can't run on. Surface the detail, or it dies with the
    // torn-down banner and the run appears to exit for no reason.
    if let ProbeOutcome::Failed { detail } = &outcome.probe {
        return fatal(
            console,
            format!("ztest run: cluster probe failed — {detail}"),
            NextestExitCode::SETUP_ERROR,
        );
    }

    // Image phases, only when Phase B succeeded.
    let image_phase = if let BuildOutcome::Ok {
        selected_binaries, ..
    } = &outcome.build
    {
        run_image_phases(
            work_rt,
            selected_binaries,
            console,
            state,
            theme,
            session_start,
        )
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
        Preflight {
            probe: &outcome.probe,
            build: &outcome.build,
            images: image_phase,
        },
    )
}

/// The shared tail of a run: fold the resolved build + image phases into the
/// engine and execute. Both the local-compile and on-cluster-compile paths
/// converge here so the [`engine::EngineInput`] assembly lives in one place.
#[allow(clippy::too_many_arguments)]
/// The ServiceAccount this run charges its reservations to. Both the build lease
/// and the run lease bill the same identity, so they read it from one place.
fn service_account() -> String {
    std::env::var("ZTEST_SA").unwrap_or_else(|_| "ztest-local".to_string())
}

/// The build phase's BuildKit pod together with the ledger reservation that
/// covers it: the pod name, the `Renewer` holding its Lease, and the heartbeat
/// keeping that Lease alive across a long compile.
///
/// They travel as one object because they must die as one. The pod's footprint
/// ([`BUILDKIT_BUILD`](crate::qos::build::BUILDKIT_BUILD)) is the largest ztest
/// places, so a path that dropped the pod but leaked the Lease would sterilise
/// that capacity until the TTL, and one that dropped the Lease but leaked the pod
/// would put unbudgeted memory back under a node admission believes is free —
/// precisely the failure this reservation exists to prevent. With a single
/// [`teardown`](Self::teardown) there is no exit path that can do either.
struct ReservedBuilder {
    pod: String,
    renewer: crate::qos::ledger::Renewer,
    /// Aborted on drop, which stops the renewals; the Lease is deleted explicitly
    /// in `teardown` rather than left to expire.
    _heartbeat: crate::qos::ledger::Heartbeat,
}

impl ReservedBuilder {
    /// Reserve the build footprint, then create the pod it covers.
    ///
    /// The reservation is taken *first* and named after the run-id the pod
    /// carries, so the ledger attributes the pod to it (`usage_by_leased_run`)
    /// and peers subtract it (`sum_reservations`) for the pod's whole life. The
    /// wait inside `acquire` is what replaces the old behaviour of creating the
    /// pod unconditionally and failing the run outright when no node could place
    /// it.
    async fn acquire(
        client: &kube::Client,
        run: &crate::naming::RunCoords,
        allocatable: crate::qos::Resources,
    ) -> Result<Self, String> {
        let grant = crate::qos::ledger::acquire(
            client,
            &run.run_id,
            &service_account(),
            &run.user,
            allocatable,
            crate::qos::ledger::Reserve::Fixed(crate::qos::build::BUILDKIT_BUILD),
        )
        .await
        .map_err(|e| e.to_string())?;
        let heartbeat = grant.renewer.spawn_heartbeat();
        let pod = match crate::resource::impls::buildkit::create_build_pod(
            client,
            &run.run_id,
            &run.user,
        )
        .await
        {
            Ok(p) => p,
            Err(e) => {
                grant.renewer.release().await;
                return Err(e.to_string());
            }
        };
        if let Err(e) = crate::resource::impls::buildkit::wait_build_pod_ready(client, &pod).await {
            crate::resource::impls::buildkit::delete_build_pod(client, &pod).await;
            grant.renewer.release().await;
            return Err(e.to_string());
        }
        Ok(ReservedBuilder {
            pod,
            renewer: grant.renewer,
            _heartbeat: heartbeat,
        })
    }

    /// Delete the pod, then release its reservation. In that order: the capacity
    /// must not be advertised as free while the pod is still terminating on it.
    fn teardown(self, work_rt: &tokio::runtime::Runtime, client: &kube::Client) {
        work_rt.block_on(async {
            crate::resource::impls::buildkit::delete_build_pod(client, &self.pod).await;
            self.renewer.release().await;
        });
    }
}

/// Everything the preflight phases resolved, travelling as one value into the
/// engine launch. Grouped because the three are only ever produced together and
/// consumed together, and both call sites forward all of them unchanged.
struct Preflight<'a> {
    probe: &'a ProbeOutcome,
    build: &'a BuildOutcome,
    images: ImagePhaseOutcome,
}

fn launch_engine(
    work_rt: &tokio::runtime::Runtime,
    opts: &RunOptions,
    theme: &Theme,
    state: &mut BannerState,
    console: Option<&Console>,
    run: &crate::naming::RunCoords,
    preflight: Preflight<'_>,
) -> ExitCode {
    let Preflight {
        probe,
        build,
        images: image_phase,
    } = preflight;
    let cancelled = || console.is_some_and(Console::cancelled);
    let cancel_exit = || cancel_exit(work_rt, &run.run_id, opts.no_cleanup);

    if cancelled() {
        return cancel_exit();
    }

    // `cargo nextest list` cannot tell a `#[ztest::sync_test]` profile from an
    // ordinary test — only the Phase-C dump can — so the selection is pruned
    // here, before anything downstream sizes a wave or admits an item against
    // it. Both the QoS plan and the work-list are then built from the same
    // pruned selection.
    let mut selected_binaries = match build {
        BuildOutcome::Ok {
            selected_binaries, ..
        } => selected_binaries.clone(),
        BuildOutcome::Failed { .. } => Vec::new(),
    };
    let excluded_sync = engine::plan::drop_sync_tests(
        &mut selected_binaries,
        &image_phase.sync_by_binary,
        &image_phase.qos_by_binary,
    );
    let qos_by_binary = prune_qos(&image_phase.qos_by_binary, &excluded_sync);
    let selected_count: usize = selected_binaries
        .iter()
        .map(|b| b.selected_tests.len())
        .sum();

    // Group the dumped tiers and estimate the wave structure against capacity.
    state.qos_plan = qos_plan_from(&qos_by_binary, probe);

    // Phase B counted the sync profiles it could not recognise; correct the
    // banner before it is rendered so it states what will actually run.
    if !excluded_sync.is_empty()
        && let crate::ui::BuildState::Ok {
            test_count,
            binary_count,
            ..
        } = &mut state.build
    {
        *test_count = selected_count;
        *binary_count = selected_binaries.len();
    }

    // Final panel refresh with all phases resolved.
    if let Some(c) = console {
        push_preflight_scene(c, state, &Transfers::default(), "Preflight", theme, None);
    }

    // In non-TTY mode print the full resolved banner so CI logs have a record.
    if console.is_none() {
        let _ = stdout().write_all(ui::render(state, theme).as_bytes());
    }

    if let BuildOutcome::Failed { .. } = build {
        // Phase B is `cargo nextest list`; a non-zero exit is a build failure.
        return exit(NextestExitCode::BUILD_FAILED);
    }
    if let Some(detail) = &image_phase.failure {
        return fatal(
            console,
            format!("ztest run: image preflight failed: {detail}"),
            NextestExitCode::SETUP_ERROR,
        );
    }
    // Name what the filter matched but the engine will not run, so an absent
    // test is never a silent absence.
    if !excluded_sync.is_empty() {
        let note = sync_exclusion_notice(&excluded_sync);
        match console {
            Some(c) => c.scrollback(note),
            None => print!("{note}"),
        }
    }
    // No tests selected: honor `--no-tests` (nextest default `fail` ⇒ exit 4).
    // Counted after the sync prune, so a filter that matched only sync profiles
    // lands here rather than starting an empty run.
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
            (false, false) => (
                "ztest run: no tests to run".to_string(),
                NextestExitCode::OK,
            ),
        };
        return fatal(console, msg, code);
    }

    // The engine's 2D scheduler is seeded from the cluster's free capacity
    // (`allocatable - Σ requested` across every pod, including other runs'
    // Guaranteed pods), so independent runs coexist: whoever probes first packs,
    // the next queues on the remainder. A probed cluster is required.
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
        // A build failure already returned `BUILD_FAILED` above.
        BuildOutcome::Failed { .. } => unreachable!("build failure handled above"),
    };
    let selected_binaries = selected_binaries.as_slice();

    let sa = service_account();
    // Shared with the reservation lease so the ledger's per-run invariant groups
    // the runner pods together.
    let run_id = format!("ztest-run-{}", std::process::id());

    // Acquire this run's cross-run capacity reservation. The granted slice — not
    // a raw `free()` snapshot — seeds the scheduler ceiling, so concurrent runs
    // carve the node up instead of all claiming the same headroom
    // (`docs/design-qos.md`).
    let client = match work_rt.block_on(crate::cluster::client()) {
        Ok(c) => c,
        Err(e) => {
            return fatal(
                console,
                format!("cluster client: {e}"),
                NextestExitCode::SETUP_ERROR,
            );
        }
    };
    let grant = match work_rt.block_on(crate::qos::ledger::acquire(
        &client,
        &run_id,
        &sa,
        &run.user,
        capacity.allocatable,
        crate::qos::ledger::Reserve::Elastic,
    )) {
        Ok(g) => g,
        Err(e) => {
            return fatal(
                console,
                format!("ztest run: {e}"),
                NextestExitCode::SETUP_ERROR,
            );
        }
    };
    let ceiling = grant.slice;

    // The cross-run governor drives this run's reservation and the scheduler
    // ceiling live for the whole run — a lone run expands to fill the cluster, a
    // busy one cedes its fair share to peers — and its reconcile heartbeat renews
    // the Lease, so there is no separate renewal task. The demand channel carries
    // the scheduler's live appetite back to it. Dropped (aborted) before the
    // Lease is released so it cannot re-create the Lease after release.
    let (demand_tx, demand_rx) =
        tokio::sync::watch::channel(crate::qos::governor::RunDemand::default());
    let (governor, ceiling_rx) = work_rt.block_on(async {
        crate::qos::governor::spawn(
            crate::qos::governor::GovernorConfig {
                client: client.clone(),
                run_id: run_id.clone(),
                allocatable: capacity.allocatable,
                budget: grant.budget,
                initial: grant.slice,
                renewer: grant.renewer.clone(),
            },
            demand_rx,
        )
    });

    // `--rerun`: resolve the prior run to the set of tests that passed, which the
    // engine excludes from this run's work-list. A bad selector is a setup error.
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
        capacity: Some(engine::schedule::CapacityChannels {
            ceiling_rx,
            demand_tx,
        }),
    };
    let code = engine::run(work_rt, input, console, theme, state.qos_plan.clone());
    // Stop the governor before releasing, then release the reservation on every
    // exit path so freed capacity is available to the next run immediately (the
    // TTL is only the crash backstop).
    drop(governor);
    work_rt.block_on(grant.renewer.release());
    if cancelled() {
        return cancel_exit();
    }
    code
}

/// The on-cluster-compile path: probe the cluster, start the ephemeral BuildKit
/// builder pod, then ship source and have that pod produce the binaries via
/// [`pipeline::remote_compile::compile_on_cluster`] (source stream → BuildKit
/// runner build → inventory export). Provisioning and the engine tail are the
/// shared paths. The probe, builder startup, and compile are distinct phases.
fn run_inner_on_cluster(
    work_rt: &tokio::runtime::Runtime,
    opts: &RunOptions,
    theme: &Theme,
    state: &mut BannerState,
    console: Option<&Console>,
    run: &crate::naming::RunCoords,
) -> ExitCode {
    use crate::pipeline::images;
    use crate::ui::BuildState;

    let cancelled = || console.is_some_and(Console::cancelled);
    let cancel_exit = || cancel_exit(work_rt, &run.run_id, opts.no_cleanup);

    // Phase A: probe (+ archives) — the client drives the builder pod and the
    // probe's free-capacity ceiling feeds the engine.
    let (probe, client) = work_rt.block_on(async {
        let ev_tx = pipeline::channel().0;
        let (probe, client) = pipeline::cluster::run(&ev_tx).await;
        if let Some(c) = &client {
            let archives = pipeline::archives::discover(c, &ev_tx).await;
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

    // A hard probe failure aborts before we drive the builder. A `Missing` probe
    // (unresolved kubeconfig/context) falls through to the no-client guard below.
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

    // Registry repo the runner build pushes to (the in-cluster pull address,
    // i.e. `ZTEST_IMAGE_REGISTRY`).
    let Some(runner_repo_ref) = crate::backends::image::runner_repo_ref() else {
        return fatal(
            console,
            "ztest run: on-cluster build requires ZTEST_IMAGE_REGISTRY (the in-cluster pull address)",
            NextestExitCode::SETUP_ERROR,
        );
    };
    let refs = pipeline::remote_compile::BakeRefs { runner_repo_ref };

    // The probe is a pre-build snapshot; watch capacity so the panel reflects the
    // build pod and test churn instead of stale pre-build headroom.
    let initial_cap = match &probe {
        ProbeOutcome::Ok { capacity, .. } => *capacity,
        _ => Default::default(),
    };
    // `spawn` schedules its watch tasks with `tokio::spawn`, so it must be called
    // within a runtime context. Enter `work_rt` (multi-thread) so those tasks run
    // on its workers and keep capacity current while this thread blocks on the
    // build; without the guard `tokio::spawn` finds no reactor and panics.
    let cap_watch = {
        let _enter = work_rt.enter();
        pipeline::capacity_watch::spawn(client.clone(), initial_cap)
    };
    let cap_rx = cap_watch.receiver();

    // Phase B/C on the cluster. No local child streams output, so the panel's
    // one live row names the current remote sub-phase, its timer reset at each
    // transition by the `on_phase` callback below. Defined before the build pod
    // so its startup (below) reports as the first sub-phase, rather than an
    // invisible wait that looks like the probe hanging.
    let started = Instant::now();

    // With a console the builder compiles under a PTY and its raw bytes feed the
    // terminal emulator, as the local reader does; CI (no console) runs tty-free
    // with each line to stderr.
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

    // `remote_compile` reports phase transitions; the shared driver colours each
    // boundary line and here — where the banner state lives — we drive the live
    // panel row on each new sub-phase.
    let mut on_phase = |ev: pipeline::remote_compile::Phase<'_>| {
        if let Some(phase) = crate::cli::console::commit_phase(console, theme, ev) {
            state.build = BuildState::Compiling {
                started_at: Instant::now(),
                phase: Some(phase),
            };
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

    // Startup builder: reserve the build footprint in the cross-run ledger, then
    // create the ephemeral BuildKit pod it covers and wait it Ready. Reserving
    // first is what keeps admission honest — the pod is Guaranteed and the largest
    // ztest places, so a peer run must subtract it for its whole life or it will
    // hand the same memory to tests. It is torn down after the build phase and
    // must not linger into the test run; labelled with the run id so `reap_run`
    // removes it on Ctrl-C / cleanup. Reported as its own phase (with a live
    // timer) rather than a silent wait — a build pod waiting on capacity is
    // otherwise indistinguishable from a hung probe.
    use pipeline::remote_compile::Phase;
    on_phase(Phase::Start("startup builder"));
    let t_builder = Instant::now();
    let builder = match work_rt.block_on(ReservedBuilder::acquire(
        &client,
        run,
        initial_cap.allocatable,
    )) {
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
    on_phase(Phase::Done {
        label: "builder pod ready",
        dur: t_builder.elapsed(),
    });
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
            // Drop the ephemeral builder before bailing — a failed build must not
            // leak its Guaranteed footprint, nor the reservation covering it,
            // until the janitor.
            builder.teardown(work_rt, &client);
            // Commit the failing compile's output before the error line, so the
            // error lands after (not above) the output that explains it.
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

    // Reflect the resolved build into the banner (counts + elapsed).
    let build = remote.build;
    state.build = match &build {
        BuildOutcome::Ok {
            test_count,
            binary_count,
            ..
        } => BuildState::Ok {
            test_count: *test_count,
            binary_count: *binary_count,
            elapsed: started.elapsed(),
        },
        BuildOutcome::Failed { exit_code, stage } => BuildState::Failed {
            exit_code: *exit_code,
            stage: *stage,
            elapsed: started.elapsed(),
        },
    };

    // Component dev images + data seeds still provision through the resource
    // graph; only the runner image is already baked, so it rides in Prebaked.
    let (images, seeds, images_by_binary, deps_by_binary, sync_by_binary) = match remote.dump {
        images::DumpOutcome::Discovered {
            images,
            seeds,
            images_by_binary,
            deps_by_binary,
            sync_tests: _,
            sync_by_binary,
        } => (
            images,
            seeds,
            images_by_binary,
            deps_by_binary,
            sync_by_binary,
        ),
        // `compile_on_cluster` only ever returns `Discovered`, but handle the
        // failure variant rather than unwrap.
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
    // Set here rather than passed through `provision_and_resolve` (which has no
    // use for it and several early returns): the edge then rides every path out.
    image_phase.sync_by_binary = sync_by_binary;
    // Build phase done — drop the builder so neither its footprint nor the
    // reservation covering it is held through the test run. Releasing here is what
    // lets `launch_engine`'s own acquire see that capacity as free (reap_run is the
    // backstop on the error/cancel paths).
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
        Preflight {
            probe: &probe,
            build: &build,
            images: image_phase,
        },
    )
}

/// Clones only on the live path, so the static path stays borrow-only.
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
        left: ui::render_preflight_panel(
            &with_live_capacity(&snap, cap.as_ref()),
            label,
            elapsed,
            &theme,
        ),
        right: ui::render_transfers(&tx, elapsed, &theme),
        // `None` → the live region derives from the avt grid (the child's output).
        live: None,
    });
}

/// The `Building`-phase scene; `live: None` so the region derives from the avt
/// grid where every build streams its native output.
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
        left: ui::render_preflight_panel(
            &with_live_capacity(&snap, cap.as_ref()),
            "Building",
            elapsed,
            &theme,
        ),
        right: ui::render_transfers(&tx, elapsed, &theme),
        live: None,
    });
}

/// Build the scheduling plan for the preflight banner: fold the per-binary QoS
/// dump into per-tier counts and estimate the wave structure against probed
/// capacity. `None` when no QoS tests were declared.
/// Drop the QoS declarations of tests that were excluded from the selection, so
/// the wave estimate is a function of what will actually run. Sync profiles wear
/// the top-priority `sync` tier, so leaving them in would shape the whole plan
/// around a test the engine never admits.
fn prune_qos(
    qos_by_binary: &[(String, Vec<QosEntry>)],
    excluded: &[engine::plan::ExcludedSync],
) -> Vec<(String, Vec<QosEntry>)> {
    if excluded.is_empty() {
        return qos_by_binary.to_vec();
    }
    let dropped: std::collections::HashSet<(&str, &str)> = excluded
        .iter()
        .map(|e| (e.binary_id.as_str(), e.test_name.as_str()))
        .collect();
    qos_by_binary
        .iter()
        .map(|(binary_id, entries)| {
            let kept: Vec<QosEntry> = entries
                .iter()
                .filter(|e| {
                    !dropped.contains(&(binary_id.as_str(), engine::plan::libtest_name(&e.test_id)))
                })
                .cloned()
                .collect();
            (binary_id.clone(), kept)
        })
        .filter(|(_, entries)| !entries.is_empty())
        .collect()
}

/// The scrollback note naming each sync-tier test the run declined to execute
/// and, for a profile, the command that does run it.
fn sync_exclusion_notice(excluded: &[engine::plan::ExcludedSync]) -> String {
    use engine::plan::SyncExclusion;

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

fn qos_plan_from(
    qos_by_binary: &[(String, Vec<QosEntry>)],
    probe: &ProbeOutcome,
) -> Option<crate::qos::schedule::QosPlan> {
    let mut counts: BTreeMap<crate::qos::QosClass, u32> = BTreeMap::new();
    for (_binary_id, entries) in qos_by_binary {
        for e in entries {
            *counts.entry(e.class).or_insert(0) += 1;
        }
    }
    if counts.is_empty() {
        return None;
    }
    // Same free-capacity figure the engine scheduler is seeded from.
    let ceiling = match probe {
        ProbeOutcome::Ok { capacity, .. } => Some(capacity.free()),
        _ => None,
    };
    Some(crate::qos::schedule::plan(&counts, ceiling))
}

/// Combined outcome of Phase A (probe) and Phase B (build) from the pipeline.
#[derive(Debug)]
struct PipelineOutcome {
    build: BuildOutcome,
    probe: ProbeOutcome,
}

/// Incremental update from the concurrent Phase A probe / archive discovery,
/// pushed onto an mpsc and folded into [`BannerState`] (see [`apply_update`]).
#[derive(Debug)]
enum Update {
    Probe(ProbeOutcome),
    Archives(ArchivesOutcome),
    ArchivesSkipped,
}

/// Run Phase A (cluster probe + archive discovery) and Phase B
/// (`cargo nextest list`) concurrently. TTY runs the compile under a PTY with
/// the probe feeding the panel; non-TTY is linear with inherited stderr.
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

/// TTY pipeline: pass 1 emulated under a PTY with the probe running
/// concurrently, then pass 2 (JSON index) captured. The concurrent update
/// drain folds a mid-step probe result into the panel as a fresh scene.
fn pipeline_console(
    work_rt: &tokio::runtime::Runtime,
    list_args: &[String],
    theme: &Theme,
    state: &mut BannerState,
    con: &Console,
    _session_start: Instant,
) -> std::io::Result<PipelineOutcome> {
    use crate::ui::{BuildStage, BuildState};

    work_rt.block_on(async {
        // Probe + archives feed the panel via the `Update` channel; the throwaway
        // event channel just satisfies the pipeline fns' signature.
        let (upd_tx, mut upd_rx) = tokio::sync::mpsc::unbounded_channel::<Update>();
        let probe_handle = {
            let upd = upd_tx.clone();
            let ev_tx = pipeline::channel().0;
            tokio::spawn(async move {
                let (probe, client) = pipeline::cluster::run(&ev_tx).await;
                let _ = upd.send(Update::Probe(probe.clone()));
                match client {
                    Some(c) => {
                        let outcome = pipeline::archives::discover(&c, &ev_tx).await;
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

        // Pass 1 uses `run --no-run` rather than `list` because under a PTY the
        // merged stdout/stderr would dump `list`'s full test listing into the
        // view; `run --no-run` emits only compile output. Pass 2 does the index.
        let started_at = Instant::now();
        state.build = BuildState::Compiling {
            started_at,
            phase: None,
        };
        push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme, None);

        let mut args = vec![
            "nextest".to_string(),
            "run".to_string(),
            "--no-run".to_string(),
        ];
        args.extend(list_args.iter().cloned());
        let code = run_child_draining(con, "cargo", &args, &[], &mut upd_rx, state, theme).await?;

        let build = if code != 0 {
            BuildOutcome::Failed {
                exit_code: code,
                stage: BuildStage::Compile,
            }
        } else {
            // Pass 2, JSON index: cargo's metadata/freshness pass is multi-second
            // even warm and emits no output; the drain is only so a late probe
            // result still lands.
            state.build = BuildState::Indexing {
                started_at: Instant::now(),
            };
            push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme, None);
            drive_draining(
                pipeline::build::index(list_args),
                &mut upd_rx,
                con,
                state,
                theme,
            )
            .await?
        };

        // Reflect the build outcome into the panel state.
        state.build = match &build {
            BuildOutcome::Ok {
                test_count,
                binary_count,
                ..
            } => BuildState::Ok {
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

        // Fold any late probe/archive updates, then refresh. Deliberately do NOT
        // flush the live region: leaving cargo's final frame lets the next child's
        // output continue on the same grid, a seamless handoff.
        while let Ok(u) = upd_rx.try_recv() {
            apply_update(state, u);
        }
        push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme, None);

        let probe = probe_handle
            .await
            .map_err(|e| std::io::Error::other(format!("Phase A: {e}")))?;
        Ok(PipelineOutcome { build, probe })
    })
}

/// Run a PTY child to completion while folding concurrent probe/archive updates
/// into fresh panel scenes.
async fn run_child_draining(
    con: &Console,
    program: &str,
    args: &[String],
    envs: &[(&str, String)],
    upd_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Update>,
    state: &mut BannerState,
    theme: &Theme,
) -> std::io::Result<i32> {
    let child = crate::cli::console::run_child(Some(con), program, args, envs);
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

/// Drive a future to completion while folding concurrent probe/archive updates
/// into fresh panel scenes.
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

/// Non-TTY pipeline: probe + the two-pass build run concurrently with inherited
/// stderr (cargo's plain output goes straight to the log); no panel.
fn pipeline_inherited(
    list_args: &[String],
    state: &mut BannerState,
) -> std::io::Result<PipelineOutcome> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(3)
        .build()?;
    let list_args = list_args.to_vec();

    rt.block_on(async move {
        let (event_tx, mut event_rx) = pipeline::channel();
        let (upd_tx, mut upd_rx) = tokio::sync::mpsc::unbounded_channel::<Update>();

        let cluster_upd = upd_tx.clone();
        let cluster_tx = event_tx.clone();
        let cluster_handle = tokio::spawn(async move {
            let (probe, client) = pipeline::cluster::run(&cluster_tx).await;
            let _ = cluster_upd.send(Update::Probe(probe.clone()));
            match client {
                Some(c) => {
                    let outcome = pipeline::archives::discover(&c, &cluster_tx).await;
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
            tokio::spawn(async move { pipeline::build::run(&list_args, &build_tx, None).await });

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

        let probe = cluster_handle
            .await
            .map_err(|e| std::io::Error::other(format!("Phase A: {e}")))?;
        let build = build_handle
            .await
            .map_err(|e| std::io::Error::other(format!("Phase B join: {e}")))?
            .map_err(|e| std::io::Error::other(format!("Phase B: {e}")))?;

        Ok(PipelineOutcome { build, probe })
    })
}

/// Translate a build-lifecycle [`pipeline::events::Event`] into a mutation on
/// `state.build`, reusing the `BuildStarted` `started_at` for elapsed time.
fn apply_event(state: &mut BannerState, event: pipeline::events::Event) {
    use crate::ui::BuildState;
    use pipeline::events::Event;

    match event {
        Event::BuildStarted => {
            state.build = BuildState::Compiling {
                started_at: std::time::Instant::now(),
                phase: None,
            };
        }
        Event::BuildIndexing => {
            // Preserve the original `started_at` so the clock measures the whole
            // Phase B, not just pass 2.
            let started_at = match &state.build {
                BuildState::Compiling { started_at, .. } => *started_at,
                _ => std::time::Instant::now(),
            };
            state.build = BuildState::Indexing { started_at };
        }
        Event::BuildComplete {
            test_count,
            binary_count,
        } => {
            let elapsed = phase_b_elapsed(&state.build);
            state.build = BuildState::Ok {
                test_count,
                binary_count,
                elapsed,
            };
        }
        Event::BuildFailed { exit_code, stage } => {
            let elapsed = phase_b_elapsed(&state.build);
            state.build = BuildState::Failed {
                exit_code,
                stage,
                elapsed,
            };
        }
        // Phase A events flow through the `Update` channel; duplicates here are
        // ignored.
        Event::ProbeStarted | Event::ProbeComplete | Event::ProbeFailed => {}
    }
}

/// Phase B elapsed time from whichever ticking variant the state is in.
fn phase_b_elapsed(build: &crate::ui::BuildState) -> std::time::Duration {
    use crate::ui::BuildState;
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
                        ArchiveStatus::Cached {
                            size_bytes: e.size_bytes,
                        }
                    } else {
                        ArchiveStatus::Missing {
                            detail: "not yet ready".to_string(),
                        }
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

/// Everything the image/resource phase hands the run phase: the QoS dump plus
/// the dependency edges and provisioned states the engine gates admission on.
#[derive(Debug, Default)]
struct ImagePhaseOutcome {
    /// Fatal setup failure detail (dump/plan failure), if any — the run aborts.
    failure: Option<String>,
    /// Per-binary QoS tier declarations harvested from the same dump.
    qos_by_binary: Vec<(String, Vec<QosEntry>)>,
    /// Per-binary `#[ztest::sync_test]` profiles from the same dump. `ztest run`
    /// subtracts these — and everything else wearing the `sync` tier — from the
    /// selection: their lifecycle owner is `ztest sync start`, never the engine.
    sync_by_binary: Vec<(String, Vec<crate::inventory::SyncTestEntry>)>,
    /// Resource dependency edges (binary images + per-test seeds) for the engine.
    resource_deps: crate::engine::plan::ResourceDeps,
    /// Provisioned resource states (node → state) the engine gates admission on.
    resource_states: std::collections::HashMap<crate::resource::NodeId, crate::resource::NodeState>,
    /// Pull reference of the built runner image, when this is a remote run — the
    /// engine runs each test in a pod from it. `None` → local-process execution.
    runner_image: Option<String>,
    /// Resolved `spec_key → pull reference` for dev component images, forwarded
    /// into each runner pod so an in-pod test resolves them without rebuilding.
    /// Populated only for remote runs.
    image_refs: std::collections::BTreeMap<String, String>,
}

/// Run the inventory-driven image phase: the Phase C dump learns which dev
/// images and seeds the selected tests need, then the resource graph provisions
/// each (skipping anything already present). Returns the per-binary QoS
/// declarations plus the dependency edges and provisioned states the engine
/// gates on. Provisioning is serial (see [`provision_and_resolve`]).
fn run_image_phases(
    work_rt: &tokio::runtime::Runtime,
    binaries: &[pipeline::SelectedBinary],
    console: Option<&Console>,
    state: &mut BannerState,
    theme: &Theme,
    _session_start: Instant,
) -> ImagePhaseOutcome {
    use crate::pipeline::images;

    // Phase C, inventory dump: spawns every test binary with
    // `ZTEST_DUMP_INVENTORY=1`, yielding the deduped dev images and seeds the
    // selection declares plus the per-binary/per-test edges.
    let (outcome, qos_by_binary) = work_rt.block_on(images::discover(binaries));
    let (images, seeds, images_by_binary, deps_by_binary, sync_by_binary) = match outcome {
        images::DumpOutcome::Discovered {
            images,
            seeds,
            images_by_binary,
            deps_by_binary,
            sync_tests: _,
            sync_by_binary,
        } => (
            images,
            seeds,
            images_by_binary,
            deps_by_binary,
            sync_by_binary,
        ),
        images::DumpOutcome::Failed { detail } => {
            return ImagePhaseOutcome {
                failure: Some(detail),
                qos_by_binary,
                ..Default::default()
            };
        }
    };

    // Local kind only: compute is on this machine, so tests run in-process —
    // no runner image. (OpenShift bakes the runner in a separate path.)
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

/// Where the runner image (the pod-per-test image carrying the compiled test
/// binaries) comes from.
enum RunnerSource {
    /// No runner image — local kind runs tests in-process on this machine.
    None,
    /// Already pushed to the registry by the on-cluster builder; carries the pull
    /// reference the runner pods use.
    Prebaked(String),
}

/// Provision the component dev images + data seeds a selection declares and
/// resolve the engine's admission inputs (dependency edges, provisioned states,
/// image refs, runner image ref). Shared by both compile paths, which differ
/// only in where the runner image came from ([`RunnerSource`]).
#[allow(clippy::too_many_arguments)]
fn provision_and_resolve(
    work_rt: &tokio::runtime::Runtime,
    images: Vec<crate::inventory::DevImageEntry>,
    seeds: Vec<crate::inventory::SeedEntry>,
    images_by_binary: Vec<(String, Vec<crate::inventory::DevImageEntry>)>,
    deps_by_binary: Vec<(String, Vec<crate::inventory::TestDepEntry>)>,
    qos_by_binary: Vec<(String, Vec<QosEntry>)>,
    runner: RunnerSource,
    // The ephemeral BuildKit pod component-image builds exec against (on-cluster
    // path); `None` on the local/kind path, which builds no images on a pod.
    build_pod: Option<String>,
    console: Option<&Console>,
    state: &mut BannerState,
    theme: &Theme,
    // `Some` on the on-cluster path, where the build pod is still scheduled here.
    cap_rx: Option<CapRx>,
) -> ImagePhaseOutcome {
    use crate::resource;
    use std::collections::HashMap;

    let cancelled = || console.is_some_and(Console::cancelled);

    // Survives the no-resources short-circuit below: an on-cluster run with no
    // component images / seeds still has a runner to run.
    let prebaked = match &runner {
        RunnerSource::Prebaked(r) => Some(r.clone()),
        RunnerSource::None => None,
    };

    if (images.is_empty() && seeds.is_empty()) || cancelled() {
        return ImagePhaseOutcome {
            qos_by_binary,
            runner_image: prebaked,
            ..Default::default()
        };
    }

    // Commit the compile's final frame and blank the live region so
    // provisioning's build output lands on a clean grid.
    if let Some(c) = console {
        c.flush_live();
        push_building_scene(c, state, &Transfers::default(), theme, cap_rx.clone());
    }

    // Plan the resource graph (images + seeds) and provision it. `probe` skips
    // resources already present; cap 1 keeps the console's single live region
    // coherent across serial `docker`/`kind` children. Seeds need a cluster
    // client, built here (cheap, pooled) — the probe already validated the
    // cluster exists.
    let graph = match resource::plan_runtime(&images, &seeds) {
        Ok(g) => g,
        Err(e) => {
            return ImagePhaseOutcome {
                failure: Some(e),
                qos_by_binary,
                ..Default::default()
            };
        }
    };
    // Runtime provisioning needs a live client (the seeds provider talks to
    // the API server). If we can't reach the cluster the graph produces
    // structured Failed states for every seed node — the caller renders
    // and moves on rather than crashing here.
    let client = match work_rt.block_on(crate::cluster::client()) {
        Ok(c) => c,
        Err(e) => {
            return ImagePhaseOutcome {
                failure: Some(format!("connect to cluster for resource provisioning: {e}")),
                qos_by_binary,
                ..Default::default()
            };
        }
    };

    // Provision through the shared driver: a sequential (cap 1) topological walk
    // that streams each build's native output through the emulator grid and folds
    // lifecycle + sub-phase events into the right-column tracker. Each transfer
    // change repaints the `Building` panel from this run's banner state.
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

    // Resolve the dependency edges to their node ids (the same ids the graph
    // provisioned), so the engine can gate each test on the states above. Image
    // ids key the binary-level edge; seed ids (content-addressed by source) key
    // the per-test edge from `#[ztest::archive]`/`#[needs]`.
    let mut resource_deps = crate::engine::plan::ResourceDeps::default();
    for (binary_id, entries) in &images_by_binary {
        let ids: Vec<_> = entries
            .iter()
            .filter_map(|e| resource::image_node_id(e).ok())
            .collect();
        if !ids.is_empty() {
            resource_deps
                .images_by_binary
                .insert(binary_id.clone(), ids);
        }
    }
    // OID → seed node id, from the seeds we planned. A test edge names its
    // resource by OID (identical to the `SeedDecl` the same macro submits), so
    // this map resolves it to the provisioned node with no re-derivation.
    let seed_id_by_source: HashMap<&str, crate::resource::NodeId> = seeds
        .iter()
        .map(|e| (e.oid.as_str(), resource::seed_node_id(e)))
        .collect();
    for (binary_id, deps) in &deps_by_binary {
        for dep in deps {
            if let Some(id) = seed_id_by_source.get(dep.resource.as_str()) {
                let key = (
                    binary_id.clone(),
                    crate::engine::plan::libtest_name(&dep.test_id).to_string(),
                );
                resource_deps
                    .seeds_by_test
                    .entry(key)
                    .or_default()
                    .push(id.clone());
            }
        }
    }

    // Failure isolation (the graph's whole point): a resource that fails to
    // provision does NOT abort the run — the engine now SKIPS only the tests that
    // declared it (`SkipReason::DependencyUnavailable`), and every unaffected test
    // still runs. We surface each failure into scrollback so the cause is visible
    // above the panel.
    //
    // Dev-image build failures are called out distinctly and ALWAYS printed to
    // stderr (not just the live scrollback, which can scroll off): a failed dev
    // image means the tests that declared it are skipped, and — critically —
    // they do NOT silently fall back to a published image (`resolve` returns
    // `DevImageMissing`, and `probe` never substitutes a different tag). A silent
    // fallback is exactly how a `dev!(Validator::Zebrad, …)` build failure used
    // to masquerade as a downstream consensus error (wrong branch id) instead of
    // the real cause. The `{detail}` carries the underlying `ImageError`
    // (docker-build stderr tail / git-fetch failure), so the reason is visible.
    let image_node_ids: std::collections::BTreeSet<crate::resource::NodeId> = images_by_binary
        .iter()
        .flat_map(|(_, entries)| {
            entries
                .iter()
                .filter_map(|e| resource::image_node_id(e).ok())
        })
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
            // Route through the console's durable scrollback, never a raw
            // `eprintln` — the render thread owns the terminal, so a direct
            // stderr write smears across the live panel (and its teardown can
            // wipe it). Off a TTY there's no render thread, so `eprintln` is right.
            match console {
                Some(c) => c.scrollback(format!("ztest: {msg}\n")),
                None => eprintln!("ztest run: {msg}"),
            }
        }
    }

    // The build manifest (`DevImageId → pull reference`) for every dev image
    // present after provisioning, seeded process-globally here (for local kind,
    // where preflight and tests share a process) and forwarded into each remote
    // runner pod via `ZTEST_IMAGE_REFS`. The `ztest sync` controller derives the
    // same map from the same helper.
    let image_refs = resource::dev_image_refs(&images_by_binary, &resource_states);
    crate::backends::image::seed_dev_images(&image_refs);

    // Resolve the runner image's pull reference. A prebaked (on-cluster) runner
    // was already built + pushed by the on-cluster buildkit build, so its ref
    // passes straight through; local kind has no runner (tests run in-process).
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

/// Walk up from the current working directory looking for a `Cargo.toml`.
/// Returns `Ok(())` as soon as one is found; `Err(detail)` describes the failure
/// in user-facing language.
///
/// Done ourselves rather than shelling out to `cargo locate-project` so the
/// check costs microseconds and adds nothing to startup on the common
/// (in-workspace) path.
fn locate_cargo_workspace() -> Result<(), String> {
    let cwd = std::env::current_dir()
        .map_err(|e| format!("could not read current working directory: {e}"))?;
    for dir in cwd.ancestors() {
        if dir.join("Cargo.toml").is_file() {
            return Ok(());
        }
    }
    Err(format!(
        "no Cargo.toml found in {} or any ancestor directory",
        cwd.display()
    ))
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
            capacity: crate::qos::ClusterCapacity::default(),
        },
        build: crate::ui::BuildState::Pending,
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
        // Selection / filter flags are not interpreted, just passed through.
        let o = parse(&["-p", "wallet-tests", "--lib", "-E", "test(reorg)"]);
        assert_eq!(
            o.list_args,
            v(&["-p", "wallet-tests", "--lib", "-E", "test(reorg)"])
        );
    }

    #[test]
    fn strips_engine_owned_flags_from_list_args() {
        // Engine-owned run-behavior flags must not reach `cargo nextest list`.
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
        use crate::engine::output::TestOutputDisplay;
        let o = parse(&[
            "-p",
            "wt",
            "--success-output",
            "final",
            "--failure-output",
            "never",
        ]);
        // Stripped from the list argv (nextest `list` rejects run-only flags).
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
        use crate::engine::output::TestOutputDisplay;
        // An explicit `--success-output` wins over `--no-capture`'s implied
        // `immediate` (nextest only forces the ones not otherwise set).
        let cfg = parse(&["--no-capture", "--success-output", "final"]).output_config();
        assert_eq!(cfg.success, TestOutputDisplay::Final);
        assert!(cfg.is_serial());
    }

    #[test]
    fn fail_fast_defaults_off_and_opts_in() {
        // Regression for the scheduling incident: with fail-fast ON, the first
        // failure abandoned the queued majority (only the initial ~9-wide
        // capacity wave ran out of 122 selected). ztest defaults fail-fast OFF:
        // the whole suite is packed and every result reported.
        assert!(!parse(&["-p", "wt"]).fail_fast, "default must be OFF");
        // `--fail-fast` / `--ff` opt back into stop-on-first-failure.
        assert!(parse(&["-p", "wt", "--fail-fast"]).fail_fast);
        assert!(parse(&["-p", "wt", "--ff"]).fail_fast);
        // Explicit `--no-fail-fast` matches the default (and still strips).
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
        // After `--`, tokens are nextest filter positionals: forwarded verbatim.
        let o = parse(&["--", "--no-cleanup"]);
        assert!(!o.no_cleanup);
        assert_eq!(o.list_args, v(&["--", "--no-cleanup"]));
    }

    #[test]
    fn profile_forwards_verbatim() {
        // `--profile`/`-P` is not interpreted by ztest; it forwards to both
        // `cargo nextest list` (here) and `cargo nextest run` (verbatim args), so
        // nextest resolves the profile. Both spelling forms.
        assert_eq!(
            parse(&["-P", "ci", "-p", "x"]).list_args,
            v(&["-P", "ci", "-p", "x"])
        );
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
        // `cargo nextest list` rejects these run-only flags; they must never
        // reach it (they did before M4.5, breaking Phase B). Selection survives.
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
        assert_eq!(
            o.list_args,
            v(&["-p", "wt"]),
            "only selection reaches `nextest list`"
        );
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
        // Behavior-changing run-only flags are surfaced; display-only ones aren't.
        let o = parse(&[
            "--archive-file",
            "/a",
            "--debugger",
            "gdb",
            "--status-level",
            "all",
        ]);
        assert!(o.unsupported.contains(&"--archive-file".to_string()));
        assert!(o.unsupported.contains(&"--debugger".to_string()));
        assert!(
            !o.unsupported.contains(&"--status-level".to_string()),
            "display-only flags are ignored silently"
        );
    }
}
