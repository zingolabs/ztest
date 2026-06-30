//! `ztest run` — preflight orchestration + `cargo nextest run`.
//!
//! ## Architecture
//!
//! A single compact status panel is pinned to the bottom of the terminal for
//! the whole session via `cli::console`; every phase's subprocess output
//! scrolls *above* it into native scrollback (TTY only — non-TTY runs linearly
//! with inherited stdio and no panel).
//!
//! 1. Open a `Console` (the bottom panel).
//! 2. Run Phase A (cluster probe → archive discovery) and Phase B
//!    (`cargo nextest list`) concurrently in `pipeline_phase`. Cargo's
//!    `Compiling foo…` is piped and relayed line-by-line into scrollback above
//!    the panel, which refreshes as probe/archive/build state lands.
//! 3. Run the image phases (`run_image_phases`) — `docker build` / `kind load`
//!    — relayed through the same console.
//! 4. Finish the relay console, then hand off to the run phase: when there are
//!    QoS tests, `cli::console::run` emulates `cargo nextest run` under a PTY so
//!    its cursor-addressed per-test UI renders faithfully, with the live QoS
//!    panel pinned beneath; otherwise a plain inherited-stdio run.
//!
//! ## Arg handling
//!
//! [`Args::nextest_args`] captures everything after `ztest run` verbatim
//! (clap's `trailing_var_arg + allow_hyphen_values`). [`RunOptions::parse`]
//! makes one pass over it: the few run-behavior flags the engine *acts on*
//! (`--no-cleanup`, `-j`, `--profile`, …) are extracted, and everything else —
//! selection, filter, and build flags — is forwarded **unmodified** to
//! `cargo nextest list`, which resolves the build and test selection exactly as
//! `cargo nextest run` would. We never re-parse the selection grammar ourselves,
//! so filtering is identical to nextest by construction.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write, stdout};
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use nextest_metadata::NextestExitCode;

use crate::cli::console::Console;
use crate::engine;
use crate::inventory::QosEntry;
use crate::pipeline::{self, ArchivesOutcome, BuildOutcome, ProbeOutcome};

/// A [`NextestExitCode`] constant as a process [`ExitCode`] — `ztest run` mirrors
/// nextest's documented exit codes so CI can cross-reference them.
fn exit(code: i32) -> ExitCode {
    ExitCode::from(code as u8)
}
use crate::preflight::{
    self, ArchiveRow, ArchiveStatus, BannerState, ClusterState, SnapshotRow, Theme,
};

/// `ztest run` arguments.
#[derive(Debug, Parser)]
pub struct Args {
    /// Arguments accepted exactly as by `cargo nextest run` — any flag, filter
    /// expression, or positional. Run `cargo nextest run --help` for the full
    /// reference. Migration is a literal `s/cargo nextest/ztest/`.
    ///
    /// Selection / filter / build flags are forwarded to `cargo nextest list`
    /// unchanged. The ztest engine consumes the run-behavior flags directly:
    /// `--retries`, `--fail-fast` / `--no-fail-fast`, `--no-capture`,
    /// `--profile` / `-P`, `--message-format`, and `-j` / `--test-threads`
    /// (advisory — the engine auto-scales concurrency to QoS capacity).
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
/// nextest would — we never re-parse that grammar).
#[derive(Debug, Default)]
struct RunOptions {
    /// Args for `cargo nextest list` (Phase B inventory): the verbatim argv with
    /// the run-only flags stripped, since `list` rejects them. Run behavior
    /// (retries / fail-fast / slow-timeout) is parsed into the fields below and
    /// owned by the engine — it is not forwarded to a `cargo nextest run`.
    list_args: Vec<String>,
    /// `-j` / `--test-threads` — surfaced in the preflight banner only.
    test_threads: Option<u32>,
    /// `--no-tests <ACTION>` — behavior when zero tests are selected
    /// (`pass` / `warn` / `fail`; nextest default `fail`).
    no_tests: Option<String>,
    /// `--no-cleanup` (ztest-only) — leave each test's k8s namespace standing
    /// (passed to the run as the `ZTEST_NO_CLEANUP` env, not a nextest flag).
    no_cleanup: bool,
    /// `--retries N` — max retry attempts per failed test (engine-owned; default 0).
    retries: u32,
    /// `--fail-fast` / `--no-fail-fast` — stop admitting on the first terminal
    /// failure. Default **off**: a ztest run packs the whole (often flaky,
    /// cluster-dependent) suite and reports every result, rather than abandoning
    /// the queued majority on the first failure. `--fail-fast` opts back in.
    fail_fast: bool,
    /// `--slow-timeout <DUR>` (ztest-only) — soft "slow" threshold; hard kill is
    /// the tier `hard_cap`. `None` disables the SLOW signal.
    slow_after: Option<std::time::Duration>,
    /// Run-only flags the user passed that ztest drops (e.g. `--archive-file`,
    /// `--debugger`) — surfaced as a warning. Display-only flags aren't listed.
    unsupported: Vec<String>,
}

impl RunOptions {
    /// Classify the verbatim `ztest run` argv in one pass: extract the
    /// engine-owned flags, forward everything else to `cargo nextest list`.
    fn parse(args: &[String]) -> Self {
        // `cargo nextest list` accepts the selection / filter / build / config
        // flags but REJECTS every run-only flag. We forward by default and strip
        // the run-only flags here (forward-by-default fails loudly if we miss
        // one — a dropped *selection* flag would silently mis-select). The tables
        // below mirror `cargo nextest run --help`'s run-only sections.

        // Run-only, value-taking, that `cargo nextest list` rejects → strip from
        // `list_args`. A couple we also read (for the banner / zero-test policy);
        // the rest are stripped only (the *run* still gets them verbatim).
        const RUN_VALUE: &[&str] = &[
            "-j", "--test-threads", "--jobs", "--retries", "--message-format", "--no-tests",
            "--slow-timeout",
        ];
        // Run-only booleans `list` rejects → strip. `--no-cleanup` is ztest-only.
        const RUN_BOOL: &[&str] = &[
            "--no-cleanup", "--no-capture", "--fail-fast", "--ff", "--no-fail-fast", "--nff",
        ];
        // Run-only flags the engine does NOT implement — stripped so `list`
        // doesn't choke, then ignored. Value-taking (consume their argument):
        // Runner + Stress + Reporter-display (superseded by ztest's reporter) +
        // Reuse-build (ztest has its own archive phase).
        const IGNORED_VALUE: &[&str] = &[
            "--max-fail", "--debugger", "--tracer",
            "--stress-count", "--stress-duration",
            "--failure-output", "--success-output", "--status-level", "--final-status-level",
            "--show-progress", "--max-progress-running", "--message-format-version",
            "--archive-file", "--archive-format", "--extract-to", "--cargo-metadata",
            "--workspace-remap", "--binaries-metadata", "--target-dir-remap",
        ];
        // Run-only booleans the engine ignores.
        const IGNORED_BOOL: &[&str] = &[
            "--no-run",
            "--hide-progress-bar", "--no-output-indent", "--no-input-handler",
            "--extract-overwrite", "--persist-extract-tempdir",
        ];
        // The subset of ignored flags that meaningfully change behavior if
        // dropped (vs. display-only) — worth warning the user about. Display
        // flags (`--status-level`, `--show-progress`, …) are ignored silently
        // since ztest owns the console rendering.
        const WARN_UNSUPPORTED: &[&str] = &[
            "--max-fail", "--debugger", "--tracer", "--stress-count", "--stress-duration",
            "--archive-file", "--extract-to", "--cargo-metadata", "--binaries-metadata",
            "--no-run",
        ];

        // `fail_fast` defaults OFF (ztest runs the whole suite and reports every
        // result); `--fail-fast`/`--ff` opts into nextest's stop-on-first-failure.
        let mut o = RunOptions::default();
        let mut it = args.iter().peekable();
        while let Some(arg) = it.next() {
            // After `--`, the rest are filter positionals — forward verbatim.
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
    // Workspace preflight — bail BEFORE any UI is drawn if we're not
    // inside a cargo workspace. Without this, cargo's own
    // "could not find Cargo.toml" stderr lands inside the pinned-banner
    // scroll region and gets visually squashed, leaving the user with
    // a banner that says "build failed" and no usable signal about why.
    if let Err(detail) = locate_cargo_workspace() {
        eprintln!("ztest run: {detail}");
        eprintln!(
            "       cd into a cargo workspace (one containing a Cargo.toml in this dir or any ancestor) and retry."
        );
        return exit(NextestExitCode::SETUP_ERROR);
    }

    // One pass over the verbatim argv: extract the engine-owned flags
    // (`--no-cleanup`, `--profile`, `-j`, …); everything else becomes the
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

    // One compact status panel pinned at the bottom of the terminal for the
    // whole session (TTY only). Preflight / build / image output is relayed
    // above it into native scrollback; the panel summarizes the live state.
    // Non-TTY (CI, pipes) runs linearly with no panel — output is inherited and
    // the full banner is printed at the end for the log.
    let mut console = if stdout().is_terminal() {
        match Console::new() {
            Ok(mut c) => {
                let _ = c.paint_panel(&phase_panel(&state, &theme, session_start, "Preflight"));
                Some(c)
            }
            Err(_) => None,
        }
    } else {
        None
    };

    let outcome = match pipeline_phase(
        &opts.list_args,
        &theme,
        &mut state,
        console.as_mut(),
        session_start,
    ) {
        Ok(o) => o,
        Err(err) => {
            if let Some(c) = console.take() {
                let _ = c.finish();
            }
            eprintln!("ztest run: pipeline phase crashed: {err}");
            return ExitCode::FAILURE; // unexpected — nextest maps unknown failures to 1
        }
    };

    // Image phases — only when Phase B succeeded. Their docker/kind output is
    // relayed through the same console, beneath the panel.
    let (image_phase_failed, qos_by_binary) = if let BuildOutcome::Ok {
        selected_binaries, ..
    } = &outcome.build
    {
        run_image_phases(selected_binaries, console.as_mut(), &mut state, &theme, session_start)
    } else {
        (None, Vec::new())
    };

    // QoS scheduling plan (§8 planning pass): group the dumped tiers and
    // estimate the wave structure against probed capacity.
    state.qos_plan = qos_plan_from(&qos_by_binary, &outcome.probe);

    // Final panel refresh with all phases resolved.
    if let Some(c) = console.as_mut() {
        let _ = c.paint_panel(&phase_panel(&state, &theme, session_start, "Preflight"));
    }

    // In non-TTY mode print the full resolved banner so CI logs have a record.
    if !stdout().is_terminal() {
        let _ = stdout().write_all(preflight::render(&state, &theme).as_bytes());
    }

    // Failure paths: finish the console (clean line) before the error.
    if let BuildOutcome::Failed { .. } = outcome.build {
        if let Some(c) = console.take() {
            let _ = c.finish();
        }
        // Phase B is `cargo nextest list`; a non-zero exit is a build failure.
        return exit(NextestExitCode::BUILD_FAILED);
    }
    if let Some(detail) = image_phase_failed {
        if let Some(c) = console.take() {
            let _ = c.finish();
        }
        eprintln!("ztest run: image preflight failed: {detail}");
        return exit(NextestExitCode::SETUP_ERROR);
    }
    if matches!(outcome.probe, ProbeOutcome::Failed { .. }) {
        if let Some(c) = console.take() {
            let _ = c.finish();
        }
        return exit(NextestExitCode::SETUP_ERROR);
    }

    // No tests selected: honor `--no-tests` (nextest default `fail` ⇒ exit 4).
    if let BuildOutcome::Ok { test_count: 0, .. } = outcome.build {
        if let Some(c) = console.take() {
            let _ = c.finish();
        }
        if opts.no_tests_is_error() {
            eprintln!("ztest run: no tests to run (--no-tests=fail)");
            return exit(NextestExitCode::NO_TESTS_RUN);
        }
        eprintln!("ztest run: no tests to run");
        return exit(NextestExitCode::OK);
    }

    // The run phase: the native engine (`crate::engine`) owns process-per-test
    // execution, driving the reused `Surface` directly — nextest-style verdict
    // lines scroll into native scrollback while the QoS panel stays pinned
    // beneath (one viewport, no flash). The engine's 2D scheduler is seeded from
    // the probed cluster ceiling, so a probed cluster is required.
    let ceiling = match &outcome.probe {
        ProbeOutcome::Ok { capacity, .. } => capacity.admission_ceiling(),
        _ => {
            if let Some(c) = console.take() {
                let _ = c.finish();
            }
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
        qos_by_binary: &qos_by_binary,
        ceiling,
        opts: engine::EngineOpts {
            retries: opts.retries,
            fail_fast: opts.fail_fast,
            slow_after: opts.slow_after,
            sa,
            no_cleanup: opts.no_cleanup,
            run_id: format!("ztest-run-{}", std::process::id()),
        },
    };
    engine::run(input, console.take(), &theme, state.qos_plan.clone())
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
    // on — not live `free()`, which would shrink the estimate by ztest's own
    // load and overstate the wave count.
    let ceiling = match probe {
        ProbeOutcome::Ok { capacity, .. } => Some(capacity.admission_ceiling()),
        _ => None,
    };
    Some(crate::qos::schedule::plan(&counts, ceiling))
}

/// Render the compact bottom panel for the preflight/build/image phases —
/// `render_preflight_panel` with the session-wide spinner clock. `phase` is the
/// right-aligned action label (`Preflight` during the probe + compile,
/// `Building` during the image phase).
fn phase_panel(state: &BannerState, theme: &Theme, session_start: Instant, phase: &str) -> String {
    preflight::render_preflight_panel(state, phase, session_start.elapsed(), theme)
}

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
/// frame reflects them in the bottom panel — all while the compile's output
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
/// no panel — the CI path, unchanged.
fn pipeline_phase(
    list_args: &[String],
    theme: &Theme,
    state: &mut BannerState,
    console: Option<&mut Console>,
    session_start: Instant,
) -> std::io::Result<PipelineOutcome> {
    match console {
        Some(con) => pipeline_console(list_args, theme, state, con, session_start),
        None => pipeline_inherited(list_args, state),
    }
}

/// TTY pipeline: pass 1 (`cargo nextest list`) under the console PTY with the
/// probe running concurrently, then pass 2 (JSON index) captured.
fn pipeline_console(
    list_args: &[String],
    theme: &Theme,
    state: &mut BannerState,
    con: &mut Console,
    session_start: Instant,
) -> std::io::Result<PipelineOutcome> {
    use crate::preflight::{BuildStage, BuildState};

    let started_at = Instant::now();
    state.build = BuildState::Compiling { started_at };

    // Pass 1 compiles the test binaries. We use `run --no-run` rather than
    // `list` because, under a PTY, stdout and stderr are merged onto one stream
    // — `list` would dump its full human-readable test listing (stdout) into the
    // view, whereas `run --no-run` emits only cargo's compile output. The list
    // args (selection / filter / build) are all that compilation needs. Pass 2
    // (`index`) does the JSON inventory.
    let mut args = vec![
        "nextest".to_string(),
        "run".to_string(),
        "--no-run".to_string(),
    ];
    args.extend(list_args.iter().cloned());

    // Probe + archives run concurrently on the console's runtime, feeding the
    // panel via the `Update` channel. The throwaway event channel satisfies the
    // pipeline fns' signature (their events are unused here).
    let (upd_tx, mut upd_rx) = tokio::sync::mpsc::unbounded_channel::<Update>();
    let ev_tx = pipeline::channel().0;
    let probe_handle = {
        let upd = upd_tx;
        con.runtime().spawn(async move {
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

    // Pass 1 — the chatty compile, under the PTY.
    let code = con.run_child(
        "cargo",
        &args,
        &[],
        session_start,
        state,
        &mut upd_rx,
        apply_update,
        |_s, _line| {},
        |s, elapsed| preflight::render_preflight_panel(s, "Preflight", elapsed, theme),
    )?;

    let build = if code != 0 {
        BuildOutcome::Failed {
            exit_code: code,
            stage: BuildStage::Compile,
        }
    } else {
        // Pass 2 — JSON index (captured, fast on the warm cache).
        state.build = BuildState::Indexing { started_at };
        let _ = con.paint_panel(&phase_panel(state, theme, session_start, "Preflight"));
        con.runtime().block_on(pipeline::build::index(list_args))?
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

    let probe = con
        .runtime()
        .block_on(probe_handle)
        .map_err(|e| std::io::Error::other(format!("Phase A: {e}")))?;

    // Apply any probe/archive updates that arrived after pass 1 exited (the
    // run-loop already applied those seen during the compile), then refresh the
    // panel to its resolved state. We deliberately do NOT flush the live region:
    // leaving cargo's final frame in place lets the next child's output continue
    // scrolling the same grid — a seamless handoff, with the compile output
    // flowing into scrollback naturally as the run fills the screen.
    while let Ok(u) = upd_rx.try_recv() {
        apply_update(state, u);
    }
    let _ = con.paint_panel(&phase_panel(state, theme, session_start, "Preflight"));

    Ok(PipelineOutcome { build, probe })
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

/// Run one preflight subprocess (cargo compile / docker build / kind load)
/// through the **same** console PTY as the cargo compile, so it gets native
/// colour + live in-place progress emulated into the live region with the panel
/// pinned beneath. Off a TTY (`console` is `None`) it inherits stdio for the CI
/// log. Returns the child's exit code. `label` is the panel's action word.
#[allow(clippy::too_many_arguments)]
fn run_phase_child(
    console: Option<&mut Console>,
    state: &mut BannerState,
    theme: &Theme,
    session_start: Instant,
    label: &'static str,
    program: &str,
    argv: &[String],
    envs: &[(&str, String)],
) -> std::io::Result<i32> {
    match console {
        Some(con) => {
            // No concurrent background updates during these phases; a dropped
            // sender just leaves the updates arm inert.
            let (_tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            con.run_child(
                program,
                argv,
                envs,
                session_start,
                state,
                &mut rx,
                |_: &mut BannerState, _: ()| {},
                |_: &mut BannerState, _: &str| {},
                move |s: &BannerState, elapsed| {
                    preflight::render_preflight_panel(s, label, elapsed, theme)
                },
            )
        }
        None => {
            let mut cmd = std::process::Command::new(program);
            cmd.args(argv);
            for (k, v) in envs {
                cmd.env(k, v);
            }
            Ok(cmd.status()?.code().unwrap_or(1))
        }
    }
}

/// Run the inventory-driven image phases (C: dump, D: docker build, E: kind
/// load). Returns the failure detail (if any) **and** the per-binary QoS tier
/// declarations harvested from the same dump (consumed by the config-lowering
/// step). The QoS data is returned even when there are no dev images to build.
///
/// `docker build` and `kind load` run through the console PTY
/// ([`run_phase_child`]), so they get the same live-output polish as the cargo
/// compile (BuildKit's in-place layer progress, kind's progress) rather than a
/// flat line relay.
fn run_image_phases(
    binaries: &[pipeline::SelectedBinary],
    mut console: Option<&mut Console>,
    state: &mut BannerState,
    theme: &Theme,
    session_start: Instant,
) -> (Option<String>, Vec<(String, Vec<QosEntry>)>) {
    use crate::backends::image;
    use crate::pipeline::images;

    // Phase C — inventory dump (scoped so its runtime is gone before we drive
    // the console's own runtime in the build/load passes below).
    let (outcome, qos_by_binary) = {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(r) => r,
            Err(e) => return (Some(format!("tokio runtime: {e}")), Vec::new()),
        };
        rt.block_on(images::discover(binaries))
    };
    let decls = match outcome {
        images::ImagesOutcome::Discovered { images } => images,
        images::ImagesOutcome::Failed { detail } => return (Some(detail), qos_by_binary),
    };
    if decls.is_empty() {
        return (None, qos_by_binary);
    }

    // Commit the compile's final frame to scrollback and blank the live region
    // so each build/load starts its emulated output on a clean grid.
    if let Some(c) = console.as_mut() {
        let _ = c.flush_live();
        let _ = c.paint_panel(&phase_panel(state, theme, session_start, "Building"));
    }

    // Phase D — docker build each declared image (skipping ones already in the
    // cluster's containerd), then Phase E — kind load each freshly-built tag.
    let mut to_load: Vec<String> = Vec::new();
    for decl in &decls {
        let dockerfile = std::path::Path::new(&decl.dockerfile);
        let context = std::path::Path::new(&decl.context);
        let tag = match image::dev_tag(dockerfile, context, &decl.features, &decl.repo) {
            Ok(t) => t,
            Err(e) => return (Some(format!("hash context for {}: {e}", decl.dockerfile)), qos_by_binary),
        };
        match image::exists_in_kind(&tag) {
            Ok(true) => continue, // cached: skip both build and load
            Ok(false) => {}
            Err(e) => return (Some(format!("cluster image query for {tag}: {e}")), qos_by_binary),
        }
        let argv = image::docker_build_argv(dockerfile, context, &decl.features, &tag);
        let code = match run_phase_child(
            console.as_deref_mut(),
            state,
            theme,
            session_start,
            "Building",
            "docker",
            &argv,
            &[("DOCKER_BUILDKIT", "1".to_string())],
        ) {
            Ok(c) => c,
            Err(e) => return (Some(format!("docker build {tag}: {e}")), qos_by_binary),
        };
        if code != 0 {
            return (Some(format!("docker build {tag} exited {code}")), qos_by_binary);
        }
        to_load.push(tag);
    }

    for tag in &to_load {
        let argv = image::kind_load_argv(tag);
        let code = match run_phase_child(
            console.as_deref_mut(),
            state,
            theme,
            session_start,
            "Loading",
            "kind",
            &argv,
            &[],
        ) {
            Ok(c) => c,
            Err(e) => return (Some(format!("kind load {tag}: {e}")), qos_by_binary),
        };
        if code != 0 {
            return (Some(format!("kind load {tag} exited {code}")), qos_by_binary);
        }
    }
    (None, qos_by_binary)
}

/// Walk up from the current working directory looking for a `Cargo.toml`.
/// Returns `Ok(())` as soon as one is found; `Err(detail)` describes
/// the failure in user-facing language.
///
/// We do this ourselves rather than shelling out to `cargo
/// locate-project` so the check costs ~microseconds and contributes
/// nothing to startup latency on the common (in-workspace) path.
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
        // The QoS tier/queue/reservation rows are real now — rendered as the
        // `Scheduling` block from `qos_plan` once the inventory dump + probe
        // land. (The live during-run reservation view is a deferred §8 half,
        // noted inside that block.)
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
        assert_eq!(o.list_args, v(&["-p", "wallet-tests", "--lib", "-E", "test(reorg)"]));
    }

    #[test]
    fn strips_engine_owned_flags_from_list_args() {
        // Engine-owned run-behavior flags must not reach `cargo nextest list`.
        let o = parse(&[
            "-p", "wt", "--retries", "3", "--no-fail-fast", "--no-capture", "-j", "8",
            "--message-format", "libtest-json", "--no-cleanup",
        ]);
        assert_eq!(o.list_args, v(&["-p", "wt"]), "only selection survives");
        assert!(o.no_cleanup);
        assert_eq!(o.test_threads, Some(8));
    }

    #[test]
    fn fail_fast_defaults_off_and_opts_in() {
        // Regression for the scheduling incident: with fail-fast ON, the first
        // failure abandoned the queued majority (only the initial ~9-wide
        // capacity wave ran out of 122 selected). ztest defaults fail-fast OFF —
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
        // After `--`, tokens are nextest filter positionals — forwarded verbatim.
        let o = parse(&["--", "--no-cleanup"]);
        assert!(!o.no_cleanup);
        assert_eq!(o.list_args, v(&["--", "--no-cleanup"]));
    }

    #[test]
    fn profile_forwards_verbatim() {
        // `--profile`/`-P` is not interpreted by ztest — it forwards to both
        // `cargo nextest list` (here) and `cargo nextest run` (verbatim args), so
        // nextest resolves the profile. Both spelling forms.
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
        // `cargo nextest list` rejects these run-only flags; they must never
        // reach it (they did before M4.5, breaking Phase B). Selection survives.
        let o = parse(&[
            "-p", "wt",
            "--max-fail", "3",
            "--no-tests", "warn",
            "--status-level", "all",
            "--stress-count", "5",
            "--no-run",
            "--final-status-level=none",
            "--archive-file", "/tmp/a.tar.zst",
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
        // Behavior-changing run-only flags are surfaced; display-only ones aren't.
        let o = parse(&["--archive-file", "/a", "--debugger", "gdb", "--status-level", "all"]);
        assert!(o.unsupported.contains(&"--archive-file".to_string()));
        assert!(o.unsupported.contains(&"--debugger".to_string()));
        assert!(
            !o.unsupported.contains(&"--status-level".to_string()),
            "display-only flags are ignored silently"
        );
    }
}
