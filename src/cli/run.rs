//! `ztest run`: preflight orchestration + `cargo nextest run`.
//!
//! A single compact status panel is pinned to the bottom of the terminal for
//! the whole session by a persistent render thread (`cli::console`); every
//! phase's subprocess output scrolls above it into native scrollback (TTY only;
//! non-TTY runs linearly with inherited stdio and no panel). The work side
//! mutates [`BannerState`] with plain calls and pushes an immutable scene
//! snapshot ([`push_preflight_scene`]) after each change; the render thread
//! animates it independently, so the panel stays live even while a phase blocks.
//!
//! 1. Start the `Console` render thread (the bottom panel).
//! 2. Run Phase A (cluster probe -> archive discovery) and Phase B
//!    (`cargo nextest list`) concurrently in `pipeline_phase`. Cargo's
//!    `Compiling foo` is emulated under a PTY into scrollback above the panel,
//!    which refreshes as probe/archive/build state lands.
//! 3. Run the image phases (`run_image_phases`), `docker build` / `kind load`,
//!    as PTY children through the same console.
//! 4. Hand off to the run phase: the engine (`crate::engine`) produces scenes +
//!    scrollback through the same console (no viewport rebuild); a plain
//!    inherited-stdio run off a TTY.
//!
//! Arg handling: [`Args::nextest_args`] captures everything after `ztest run`
//! verbatim (clap's `trailing_var_arg + allow_hyphen_values`).
//! [`RunOptions::parse`] makes one pass over it: the few run-behavior flags the
//! engine acts on (`--no-cleanup`, `-j`, `--profile`, ...) are extracted, and
//! everything else (selection, filter, build flags) is forwarded unmodified to
//! `cargo nextest list`, which resolves the build and test selection exactly as
//! `cargo nextest run` would. We never re-parse the selection grammar, so
//! filtering stays identical to nextest.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write, stdout};
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use nextest_metadata::NextestExitCode;

use crate::cli::console::{Console, SceneFrame};
use crate::engine;
use crate::inventory::QosEntry;
use crate::pipeline::{self, ArchivesOutcome, BuildOutcome, ProbeOutcome};

/// A [`NextestExitCode`] constant as a process [`ExitCode`]. `ztest run` mirrors
/// nextest's documented exit codes so CI can cross-reference them.
fn exit(code: i32) -> ExitCode {
    ExitCode::from(code as u8)
}

/// Exit code for a Ctrl-C-interrupted run: the shell convention `128 + SIGINT`.
const CANCELLED: i32 = 130;
use crate::preflight::{
    self, ArchiveRow, ArchiveStatus, BannerState, ClusterState, SnapshotRow, Theme, TransferKind,
    TransferProgress, TransferRow, Transfers,
};
use crate::resource::NodeId;
use crate::resource::NodeState;

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
}

/// The flags `ztest run` pulls out of its `cargo nextest run`-style argv: the
/// few the engine acts on, plus the verbatim vector forwarded to
/// `cargo nextest list` (which resolves the build + test selection exactly as
/// nextest would; we never re-parse that grammar).
#[derive(Debug, Default)]
struct RunOptions {
    /// Args for `cargo nextest list` (Phase B inventory): the verbatim argv with
    /// the run-only flags stripped, since `list` rejects them. Run behavior
    /// (retries / fail-fast / slow-timeout) is parsed into the fields below and
    /// owned by the engine, not forwarded to a `cargo nextest run`.
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
    /// failure. Default off: a ztest run packs the whole (often flaky,
    /// cluster-dependent) suite and reports every result, rather than abandoning
    /// the queued majority on the first failure. `--fail-fast` opts back in.
    fail_fast: bool,
    /// `--slow-timeout <DUR>` (ztest-only): soft "slow" threshold; hard kill is
    /// the tier `hard_cap`. `None` disables the SLOW signal.
    slow_after: Option<std::time::Duration>,
    /// Run-only flags the user passed that ztest drops (e.g. `--archive-file`,
    /// `--debugger`), surfaced as a warning. Display-only flags aren't listed.
    unsupported: Vec<String>,
}

impl RunOptions {
    /// Classify the verbatim `ztest run` argv in one pass: extract the
    /// engine-owned flags, forward everything else to `cargo nextest list`.
    fn parse(args: &[String]) -> Self {
        // `cargo nextest list` accepts the selection / filter / build / config
        // flags but rejects every run-only flag. We forward by default and strip
        // the run-only flags here: forward-by-default fails loudly if we miss
        // one, whereas a dropped selection flag would silently mis-select. The
        // tables below mirror `cargo nextest run --help`'s run-only sections.

        // Run-only, value-taking, that `cargo nextest list` rejects: strip from
        // `list_args`. A couple we also read (for the banner / zero-test policy);
        // the rest are stripped only (the run still gets them verbatim).
        const RUN_VALUE: &[&str] = &[
            "-j",
            "--test-threads",
            "--jobs",
            "--retries",
            "--message-format",
            "--no-tests",
            "--slow-timeout",
        ];
        // Run-only booleans `list` rejects: strip. `--no-cleanup` is ztest-only.
        const RUN_BOOL: &[&str] = &[
            "--no-cleanup",
            "--no-capture",
            "--fail-fast",
            "--ff",
            "--no-fail-fast",
            "--nff",
        ];
        // Run-only flags the engine does not implement: stripped so `list`
        // doesn't choke, then ignored. Value-taking (consume their argument):
        // Runner + Stress + Reporter-display (superseded by ztest's reporter) +
        // Reuse-build (ztest has its own archive phase).
        const IGNORED_VALUE: &[&str] = &[
            "--max-fail",
            "--debugger",
            "--tracer",
            "--stress-count",
            "--stress-duration",
            "--failure-output",
            "--success-output",
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
        // The subset of ignored flags that meaningfully change behavior if
        // dropped (vs. display-only); worth warning about. Display flags
        // (`--status-level`, `--show-progress`, ...) are ignored silently since
        // ztest owns the console rendering.
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

        // `fail_fast` defaults OFF (ztest runs the whole suite and reports every
        // result); `--fail-fast`/`--ff` opts into nextest's stop-on-first-failure.
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
                    _ => {}
                }
                continue; // stripped from list_args
            }
            if IGNORED_BOOL.contains(&flag) {
                if WARN_UNSUPPORTED.contains(&flag) {
                    o.unsupported.push(flag.to_string());
                }
                continue; // stripped + ignored
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
                    _ => {
                        if WARN_UNSUPPORTED.contains(&flag) {
                            o.unsupported.push(flag.to_string());
                        }
                    }
                }
                continue; // stripped from list_args
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

pub fn execute(args: Args) -> ExitCode {
    // Workspace preflight: bail before any UI is drawn if we're not inside a
    // cargo workspace. Otherwise cargo's own "could not find Cargo.toml" stderr
    // lands inside the pinned-banner scroll region and gets squashed, leaving a
    // banner that says "build failed" with no usable signal about why.
    if let Err(detail) = locate_cargo_workspace() {
        eprintln!("ztest run: {detail}");
        eprintln!(
            "       cd into a cargo workspace (one containing a Cargo.toml in this dir or any ancestor) and retry."
        );
        return exit(NextestExitCode::SETUP_ERROR);
    }

    // One pass over the verbatim argv: extract the engine-owned flags
    // (`--no-cleanup`, `--profile`, `-j`, ...); everything else becomes the
    // `cargo nextest list` argv.
    let opts = RunOptions::parse(&args.nextest_args);
    if !opts.unsupported.is_empty() {
        eprintln!(
            "ztest run: ignoring flag(s) the ztest engine doesn't support: {}",
            opts.unsupported.join(", ")
        );
    }
    let theme = Theme::detect();

    let mut state = build_initial_state(&opts);
    let session_start = Instant::now();

    // Establish a shared run id BEFORE any thread starts, so the parent's reaper
    // and every test child (which inherit this process's env) agree on the
    // `zaino.io/run-id` label all resources are stamped with. Without a forced
    // value the parent and its children derive *different* `{user}-{ppid}` ids
    // (a child's ppid is us, ours is the shell), and label-reap can't target the
    // children's resources.
    //
    // SAFETY: `set_var` must precede thread creation; we are still single-threaded
    // here (before the work runtime and the render thread are built below).
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

    // One multi-thread runtime drives every work-side phase (probe, build, image,
    // run). The console's render thread is a separate dedicated OS thread, so the
    // panel animates independently of whatever this runtime is doing.
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
    // terminal for the whole session and stays live even while a phase blocks on
    // a silent subprocess. Non-TTY (CI, pipes) runs linearly with inherited
    // stdio and no panel; the full banner is printed at the end for the log.
    let (console, guard) = if tty {
        // The render thread paints this the instant Ctrl-C arrives, before the
        // work side has a chance to react.
        let cancel_theme = theme.clone();
        let cancel_panel =
            Box::new(move |elapsed| preflight::render_cancel_panel(elapsed, &cancel_theme));
        match Console::start(session_start, cancel_panel) {
            Ok((c, g)) => (Some(c), Some(g)),
            Err(_) => (None, None),
        }
    } else {
        (None, None)
    };
    if let Some(c) = &console {
        push_preflight_scene(c, &state, &Transfers::default(), "Preflight", &theme);
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

    // Tear the render thread down (commits the final frame, restores the cursor)
    // only after every phase, including the run, has finished producing output.
    if let Some(g) = guard {
        g.finish();
    }
    code
}

/// Run every phase and return the process exit code. Split from [`execute`] so
/// the render thread's teardown is one unconditional step after it returns, no
/// matter which path produced the code.
/// Reap the current run's resources by `zaino.io/run-id` label and return the
/// conventional 130. This is the Ctrl-C teardown: a SIGKILLed test never ran its
/// `Drop`, so the surviving parent reaps by label instead. Bounded by a deadline
/// so a stuck apiserver can't hang the exit — the namespace janitor is the
/// backstop past it. Skipped under `--no-cleanup`, which asks to leave everything
/// standing for post-mortem inspection.
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
    // Cancellation (Ctrl-C) is checked after every phase: the render thread has
    // already signalled the running subprocess, so the phase returns promptly.
    // `cancel_exit` reaps this run's resources by label (Drop can't run after a
    // SIGKILL) and returns the conventional 130 — rather than misreporting the
    // interrupted phase as a build/setup failure.
    let cancelled = || console.is_some_and(Console::cancelled);
    let cancel_exit = || cancel_exit(work_rt, &run.run_id, opts.no_cleanup);

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
            eprintln!("ztest run: pipeline phase crashed: {err}");
            return ExitCode::FAILURE; // unexpected: nextest maps unknown failures to 1
        }
    };
    if cancelled() {
        return cancel_exit();
    }

    // Image phases, only when Phase B succeeded. Their docker/kind output is
    // relayed through the same console, beneath the panel.
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
    let qos_by_binary = &image_phase.qos_by_binary;
    if cancelled() {
        return cancel_exit();
    }

    // QoS scheduling plan (§8 planning pass): group the dumped tiers and estimate
    // the wave structure against probed capacity.
    state.qos_plan = qos_plan_from(qos_by_binary, &outcome.probe);

    // Final panel refresh with all phases resolved.
    if let Some(c) = console {
        push_preflight_scene(c, state, &Transfers::default(), "Preflight", theme);
    }

    // In non-TTY mode print the full resolved banner so CI logs have a record.
    if console.is_none() {
        let _ = stdout().write_all(preflight::render(state, theme).as_bytes());
    }

    if let BuildOutcome::Failed { .. } = outcome.build {
        // Phase B is `cargo nextest list`; a non-zero exit is a build failure.
        return exit(NextestExitCode::BUILD_FAILED);
    }
    if let Some(detail) = &image_phase.failure {
        eprintln!("ztest run: image preflight failed: {detail}");
        return exit(NextestExitCode::SETUP_ERROR);
    }
    if matches!(outcome.probe, ProbeOutcome::Failed { .. }) {
        return exit(NextestExitCode::SETUP_ERROR);
    }

    // No tests selected: honor `--no-tests` (nextest default `fail` ⇒ exit 4).
    if let BuildOutcome::Ok { test_count: 0, .. } = outcome.build {
        if opts.no_tests_is_error() {
            eprintln!("ztest run: no tests to run (--no-tests=fail)");
            return exit(NextestExitCode::NO_TESTS_RUN);
        }
        eprintln!("ztest run: no tests to run");
        return exit(NextestExitCode::OK);
    }

    // The run phase: the native engine (`crate::engine`) owns process-per-test
    // execution and produces scenes/scrollback through the same console. Verdict
    // lines scroll into native scrollback while the QoS panel stays pinned
    // beneath. The engine's 2D scheduler is seeded from the probed cluster
    // ceiling, so a probed cluster is required.
    let ceiling = match &outcome.probe {
        ProbeOutcome::Ok { capacity, .. } => capacity.admission_ceiling(),
        _ => {
            eprintln!("ztest run: requires a probed cluster (no kubeconfig / probe unavailable)");
            return exit(NextestExitCode::SETUP_ERROR);
        }
    };
    let (summary, selected_binaries) = match &outcome.build {
        BuildOutcome::Ok {
            summary,
            selected_binaries,
            ..
        } => (summary.as_ref(), selected_binaries.as_slice()),
        // A build failure already returned `BUILD_FAILED` above.
        BuildOutcome::Failed { .. } => unreachable!("build failure handled above"),
    };

    let sa = std::env::var("ZTEST_SA").unwrap_or_else(|_| "ztest-local".to_string());
    let input = engine::EngineInput {
        summary,
        selected_binaries,
        qos_by_binary,
        ceiling,
        resource_deps: image_phase.resource_deps,
        resource_states: image_phase.resource_states,
        opts: engine::EngineOpts {
            retries: opts.retries,
            fail_fast: opts.fail_fast,
            slow_after: opts.slow_after,
            sa,
            no_cleanup: opts.no_cleanup,
            run_id: format!("ztest-run-{}", std::process::id()),
        },
    };
    let code = engine::run(work_rt, input, console, theme, state.qos_plan.clone());
    if cancelled() {
        return cancel_exit();
    }
    code
}

/// Push a fresh preflight/build/image panel recipe to the render thread. Called
/// after every `BannerState` / `Transfers` mutation; the closure captures an
/// immutable snapshot of both columns and re-renders them with the render
/// thread's advancing clock so spinners animate between updates. `label` is the
/// left column's right-aligned action word (`Preflight`, `Building`); `transfers`
/// is the right column's live acquisition set.
fn push_preflight_scene(
    con: &Console,
    state: &BannerState,
    transfers: &Transfers,
    label: &'static str,
    theme: &Theme,
) {
    let snap = state.clone();
    let tx = transfers.clone();
    let theme = theme.clone();
    con.scene(move |elapsed| SceneFrame {
        left: preflight::render_preflight_panel(&snap, label, elapsed, &theme),
        right: preflight::render_transfers(&tx, elapsed, &theme),
        // Live region mirrors the child's own output (cargo compile) via the avt
        // grid during these phases.
        live: None,
    });
}

/// A background-transfer state change destined for the right column. Merges the
/// resource graph's coarse lifecycle (`on_change`) with each provider's finer
/// sub-phase notes ([`ProgressSink`](crate::resource::ProgressSink)) onto one
/// channel, so the work side folds both into the [`TransferRegistry`] in order.
enum TransferEvent {
    /// A node's lifecycle transition from the graph executor.
    State(NodeId, NodeState),
    /// A provider's sub-phase note (`building`, `load→kind`).
    Note(NodeId, String),
}

/// The work-side model behind the right column: the in-flight (and failed)
/// background acquisitions, keyed by resource node. [`snapshot`](Self::snapshot)
/// renders it into the render-facing [`Transfers`].
#[derive(Default)]
struct TransferRegistry {
    rows: BTreeMap<NodeId, TransferRow>,
}

impl TransferRegistry {
    /// Fold one event into the registry: a node starts (Acquiring) as an active
    /// row, updates its note, drops out on Ready, or is marked failed. Pending /
    /// Blocked never surface (nothing to show or the dep's own failure is shown).
    fn apply(&mut self, ev: TransferEvent) {
        match ev {
            TransferEvent::State(id, NodeState::Acquiring) => {
                let (label, kind) = describe_node(&id);
                self.rows.entry(id).or_insert_with(|| TransferRow {
                    label,
                    kind,
                    progress: TransferProgress::Active {
                        note: "acquiring".to_string(),
                        bytes: None,
                    },
                });
            }
            TransferEvent::State(id, NodeState::Ready) => {
                self.rows.remove(&id);
            }
            TransferEvent::State(id, NodeState::Failed(detail)) => {
                if let Some(row) = self.rows.get_mut(&id) {
                    row.progress = TransferProgress::Failed { detail };
                }
            }
            // Pending: not yet started. Blocked: a dependency failed and this node
            // was never attempted — its dependency's own Failed row is the signal.
            TransferEvent::State(_, NodeState::Pending | NodeState::Blocked) => {}
            TransferEvent::Note(id, note) => {
                if let Some(TransferRow {
                    progress: TransferProgress::Active { note: n, .. },
                    ..
                }) = self.rows.get_mut(&id)
                {
                    *n = note;
                }
            }
        }
    }

    /// An immutable snapshot for the render thread.
    fn snapshot(&self) -> Transfers {
        Transfers {
            rows: self.rows.values().cloned().collect(),
        }
    }
}

/// A short label + kind for a resource node's right-column row. Image tags
/// (`<repo>:dev-<hash>`) collapse to `dev-<repo-leaf>`; seeds keep their
/// `seed-<sha8>` id.
///
/// The runtime graph (`resource::plan_runtime`) emits only [`NodeId::Image`]
/// and [`NodeId::Seed`]; every other variant is cluster infrastructure
/// belonging to `ztest setup`. If one somehow shows up here we fall back
/// to the node's canonical `display_label()` and treat it as an image row
/// — better than panicking, and unambiguous in the UI (the label carries
/// its own kind tag: `qos-sa/basic`, `csi-driver`, ...).
fn describe_node(id: &NodeId) -> (String, TransferKind) {
    match id {
        NodeId::Image(tag) => {
            let repo = tag.split(':').next().unwrap_or(tag);
            let leaf = repo.rsplit('/').next().unwrap_or(repo);
            (format!("dev-{leaf}"), TransferKind::Image)
        }
        NodeId::Seed(name) => (name.clone(), TransferKind::Seed),
        other => (other.display_label(), TransferKind::Image),
    }
}

/// Build the §8 scheduling plan for the preflight banner: fold the per-binary
/// QoS dump into per-tier counts and estimate the wave structure against
/// probed capacity. `None` when no QoS tests were declared (no block shown).
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
    // Plan the wave structure against the admission ceiling (ztest-available
    // capacity), the same figure the engine scheduler and in-test admission key
    // on; not live `free()`, which would shrink the estimate by ztest's own
    // load and overstate the wave count.
    let ceiling = match probe {
        ProbeOutcome::Ok { capacity, .. } => Some(capacity.admission_ceiling()),
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

/// Incremental update from the concurrently-running cluster probe / archive
/// discovery (Phase A).
///
/// The probe task pushes these onto an mpsc; the run loop drains them, folds
/// them into the shared [`BannerState`] (see [`apply_update`]), and the next
/// frame reflects them in the bottom panel, all while the compile's output
/// scrolls above it.
#[derive(Debug)]
enum Update {
    Probe(ProbeOutcome),
    Archives(ArchivesOutcome),
    ArchivesSkipped,
}

/// Run Phase A (cluster probe + archive discovery) and Phase B
/// (`cargo nextest list`) concurrently.
///
/// TTY: Phase B's pass-1 compile runs under the console's PTY (so cargo keeps
/// its colour + live progress, emulated through `avt`), while the probe runs
/// concurrently and feeds the compact panel. Non-TTY: linear, inherited stderr,
/// no panel (the CI path, unchanged).
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

/// TTY pipeline: pass 1 (`cargo nextest list`) emulated under a PTY with the
/// probe running concurrently, then pass 2 (JSON index) captured. The render
/// thread keeps the panel live throughout on its own clock; we run each step
/// concurrently with an update drain only so a probe result that lands mid-step
/// is folded into the panel as a fresh scene.
fn pipeline_console(
    work_rt: &tokio::runtime::Runtime,
    list_args: &[String],
    theme: &Theme,
    state: &mut BannerState,
    con: &Console,
    _session_start: Instant,
) -> std::io::Result<PipelineOutcome> {
    use crate::preflight::{BuildStage, BuildState};

    work_rt.block_on(async {
        // Probe + archives run concurrently, feeding the panel via the `Update`
        // channel. The throwaway event channel satisfies the pipeline fns'
        // signature (their events are unused here).
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

        // Pass 1 compiles the test binaries. We use `run --no-run` rather than
        // `list` because, under a PTY, stdout and stderr merge onto one stream:
        // `list` would dump its full human-readable test listing into the view,
        // whereas `run --no-run` emits only cargo's compile output. Pass 2
        // (`index`) does the JSON inventory.
        let started_at = Instant::now();
        state.build = BuildState::Compiling { started_at };
        push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme);

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
            // Pass 2, JSON index. Re-running cargo's metadata/freshness pass over
            // the whole workspace is multi-second even on a warm cache and emits no
            // output (stderr to null), but the render thread keeps the panel live
            // regardless; the drain is only so a late probe result still lands.
            state.build = BuildState::Indexing {
                started_at: Instant::now(),
            };
            push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme);
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

        // Fold any probe/archive updates that arrived after the last step, then
        // refresh the panel to its resolved state. We deliberately do not flush
        // the live region: leaving cargo's final frame in place lets the next
        // child's output continue scrolling the same grid, a seamless handoff.
        while let Ok(u) = upd_rx.try_recv() {
            apply_update(state, u);
        }
        push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme);

        let probe = probe_handle
            .await
            .map_err(|e| std::io::Error::other(format!("Phase A: {e}")))?;
        Ok(PipelineOutcome { build, probe })
    })
}

/// Run a PTY child to completion while folding any concurrent probe/archive
/// updates into fresh panel scenes. Liveness is the render thread's job; this
/// only keeps the panel's data current during the child's run.
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
                    push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme);
                }
                None => upd_open = false,
            },
        }
    }
}

/// Drive an arbitrary future to completion while folding concurrent
/// probe/archive updates into fresh panel scenes. The render thread animates the
/// panel independently; this only services data updates.
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
                    push_preflight_scene(con, state, &Transfers::default(), "Preflight", theme);
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

/// Translate a build-lifecycle [`pipeline::events::Event`] into a
/// mutation on `state.build`. Phase B's `started_at` is recorded at
/// `BuildStarted` and reused for elapsed-time computation through the
/// final `BuildComplete` / `BuildFailed`.
fn apply_event(state: &mut BannerState, event: pipeline::events::Event) {
    use crate::preflight::BuildState;
    use pipeline::events::Event;

    match event {
        Event::BuildStarted => {
            state.build = BuildState::Compiling {
                started_at: std::time::Instant::now(),
            };
        }
        Event::BuildIndexing => {
            // Preserve the original `started_at` so the running clock
            // measures the whole Phase B, not just pass 2.
            let started_at = match &state.build {
                BuildState::Compiling { started_at } => *started_at,
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
        // Phase A events flow through the `Update` channel instead;
        // the ones that arrive here are duplicates we just ignore.
        Event::ProbeStarted | Event::ProbeComplete { .. } | Event::ProbeFailed { .. } => {}
    }
}

/// Compute Phase B elapsed time from whichever ticking variant the
/// state is currently in. Used when transitioning to a terminal state.
fn phase_b_elapsed(build: &crate::preflight::BuildState) -> std::time::Duration {
    use crate::preflight::BuildState;
    match build {
        BuildState::Compiling { started_at } | BuildState::Indexing { started_at } => {
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
            nvme_nodes: _,
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

/// Everything the image/resource phase hands the run phase. Beyond the QoS dump,
/// it carries the resolved dependency edges and the provisioned resource states
/// so the engine can gate admission and cleanly SKIP a test whose resource failed.
#[derive(Debug, Default)]
struct ImagePhaseOutcome {
    /// Fatal setup failure detail (dump/plan failure), if any — the run aborts.
    failure: Option<String>,
    /// Per-binary QoS tier declarations harvested from the same dump.
    qos_by_binary: Vec<(String, Vec<QosEntry>)>,
    /// Resource dependency edges (binary images + per-test seeds) for the engine.
    resource_deps: crate::engine::plan::ResourceDeps,
    /// Provisioned resource states (node → state) the engine gates admission on.
    resource_states: std::collections::HashMap<crate::resource::NodeId, crate::resource::NodeState>,
}

/// Run the inventory-driven image phase. Discovery (Phase C, the dump) learns
/// which dev images and archives the selected tests need; provisioning drives the
/// resource graph ([`crate::resource`]) to ensure each is present
/// (`docker build` + `kind load` / seed materialization, skipping anything already
/// present). Returns the per-binary QoS declarations plus the resolved dependency
/// edges and provisioned states the engine uses to gate/skip tests on resource
/// readiness.
///
/// Provisioning runs serially (cap 1): each `docker build` / `kind load` / seed
/// materialization streams its native output live through the console's single
/// emulator grid, one at a time, with its sub-phase shown as a row in the right
/// column. Image builds are already disk/CPU/network bound, so serial costs
/// little and keeps the live output coherent (no interleaving, no lock). Off a
/// TTY children inherit stdio. The graph's `probe` skips images already in the
/// cluster's containerd.
fn run_image_phases(
    work_rt: &tokio::runtime::Runtime,
    binaries: &[pipeline::SelectedBinary],
    console: Option<&Console>,
    state: &mut BannerState,
    theme: &Theme,
    _session_start: Instant,
) -> ImagePhaseOutcome {
    use crate::pipeline::images;
    use crate::resource;
    use std::collections::HashMap;

    let cancelled = || console.is_some_and(Console::cancelled);

    // Phase C, inventory dump (discovery). Spawns every test binary with
    // `ZTEST_DUMP_INVENTORY=1`; the render thread keeps the panel live on its own
    // clock, so a plain `block_on` suffices. Yields the deduped dev images and
    // data seeds the selection declares, plus the per-binary image and per-test
    // seed edges.
    let (outcome, qos_by_binary) = work_rt.block_on(images::discover(binaries));
    let (images, seeds, images_by_binary, deps_by_binary) = match outcome {
        images::DumpOutcome::Discovered {
            images,
            seeds,
            images_by_binary,
            deps_by_binary,
        } => (images, seeds, images_by_binary, deps_by_binary),
        images::DumpOutcome::Failed { detail } => {
            return ImagePhaseOutcome {
                failure: Some(detail),
                qos_by_binary,
                ..Default::default()
            };
        }
    };
    if (images.is_empty() && seeds.is_empty()) || cancelled() {
        return ImagePhaseOutcome {
            qos_by_binary,
            ..Default::default()
        };
    }

    // The right-column tracker for this phase's background acquisitions.
    let mut registry = TransferRegistry::default();

    // Commit the compile's final frame to scrollback and blank the live region so
    // provisioning's live build output lands on a clean grid.
    if let Some(c) = console {
        c.flush_live();
        push_preflight_scene(c, state, &registry.snapshot(), "Building", theme);
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

    // Provision sequentially (cap 1): a topological walk, `docker build && kind
    // load && …`. Image builds are already disk/CPU/network bound, so building two
    // at once mostly contends rather than helps — and serial order lets each stream
    // its native BuildKit/kind output live through the single emulator grid with no
    // interleaving and no lock. Each in-flight build/load is still a right-column
    // transfer row, fed by the graph's lifecycle transitions (`on_change`) plus the
    // provider's sub-phase notes on the `TransferEvent` channel. On a TTY the child
    // streams to the grid; off it, it inherits stdio. `probe` skips warm resources.
    let cap = 1;
    let resource_states = work_rt.block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<TransferEvent>();
        let progress = console.map(|_| {
            let tx = tx.clone();
            resource::ProgressSink::new(move |id, note| {
                let _ = tx.send(TransferEvent::Note(id, note));
            })
        });
        let mut builder = resource::Cx::builder(client);
        if let Some(c) = console.cloned() {
            builder = builder.console(c);
        }
        if let Some(p) = progress {
            builder = builder.progress(p);
        }
        let cx = builder.build();
        let on_change = {
            let tx = tx.clone();
            move |id: &NodeId, st: &NodeState| {
                let _ = tx.send(TransferEvent::State(id.clone(), st.clone()));
            }
        };
        // Only the `on_change` + sink clones keep the channel open, so it closes
        // when provisioning finishes.
        drop(tx);

        let prov = graph.provision(&cx, cap, on_change);
        tokio::pin!(prov);
        loop {
            tokio::select! {
                states = &mut prov => {
                    // Drain any notes queued after the last state transition, then
                    // paint the final (usually empty) right column.
                    while let Ok(ev) = rx.try_recv() {
                        registry.apply(ev);
                    }
                    if let Some(c) = console {
                        push_preflight_scene(c, state, &registry.snapshot(), "Building", theme);
                    }
                    break states;
                }
                Some(ev) = rx.recv() => {
                    registry.apply(ev);
                    if let Some(c) = console {
                        push_preflight_scene(c, state, &registry.snapshot(), "Building", theme);
                    }
                }
            }
        }
    });

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
    // Source path → seed node id, from the seeds we planned. A test edge names its
    // resource by source path (identical to the `SeedDecl` the same macro submits),
    // so this map resolves it to the provisioned node with no re-derivation.
    let seed_id_by_source: HashMap<&str, crate::resource::NodeId> = seeds
        .iter()
        .filter_map(|e| {
            resource::seed_node_id(e)
                .ok()
                .map(|id| (e.source.as_str(), id))
        })
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
    for (id, st) in &resource_states {
        if let NodeState::Failed(detail) = st {
            let msg = format!(
                "resource {id:?} failed to provision ({detail}); tests needing it will be skipped"
            );
            match console {
                Some(c) => c.scrollback(format!("ztest: {msg}\n")),
                None => eprintln!("ztest run: {msg}"),
            }
        }
    }

    ImagePhaseOutcome {
        failure: None,
        qos_by_binary,
        resource_deps,
        resource_states,
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
        build: crate::preflight::BuildState::Pending,
        archives: Vec::<ArchiveRow>::new(),
        snapshots: Vec::<SnapshotRow>::new(),
        // QoS tier/queue/reservation rows render as the `Scheduling` block from
        // `qos_plan` once the inventory dump + probe land. (The live during-run
        // reservation view is a deferred §8 half, noted inside that block.)
        future: vec![],
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
