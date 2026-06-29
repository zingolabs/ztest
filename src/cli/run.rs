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
//! ## Arg-forwarding contract
//!
//! [`Args::nextest_args`] is a `Vec<String>` populated by clap's
//! `trailing_var_arg + allow_hyphen_values` mode — it captures
//! everything after `ztest run` literally. The vec is handed unmodified
//! to `cargo nextest run`.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write, stdout};
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::time::Instant;

use clap::Parser;

use crate::cli::args_peek;
use crate::cli::console::Console;
use crate::inventory::QosEntry;
use crate::pipeline::{self, ArchivesOutcome, BuildOutcome, ProbeOutcome};
use crate::qos::lower::{self, TestRef};
use crate::preflight::{
    self, ArchiveRow, ArchiveStatus, BannerState, ClusterState, SnapshotRow, Theme,
};

/// `ztest run` arguments.
#[derive(Debug, Parser)]
pub struct Args {
    /// Arguments forwarded verbatim to `cargo nextest run`.
    ///
    /// Any flag, filter expression, or positional substring accepted
    /// by `cargo nextest run` works here. Run
    /// `cargo nextest run --help` for the full reference.
    ///
    /// One ztest-only flag is recognized anywhere in this list and is
    /// NOT forwarded to nextest:
    ///
    ///   --no-cleanup   Leave each test's Kubernetes namespace (pods,
    ///                  logs, volumes) in place instead of tearing it
    ///                  down, so you can `kubectl` into a failure for a
    ///                  post-mortem. A 1h janitor backstop still reaps
    ///                  them, so nothing leaks permanently.
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "NEXTEST_ARGS"
    )]
    pub nextest_args: Vec<String>,
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
        return ExitCode::from(2);
    }

    // `--no-cleanup` is a ztest-only flag, not a nextest one — pull it out
    // of the forwarded argv (nextest would reject it) and remember it.
    let (no_cleanup, nextest_args) = extract_no_cleanup(args.nextest_args);

    let peek = args_peek::peek(&nextest_args);
    let theme = Theme::detect();

    let mut state = build_initial_state(&peek);
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
        &nextest_args,
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
            return ExitCode::from(127);
        }
    };

    // Image phases — only when Phase B succeeded. Their docker/kind output is
    // relayed through the same console, beneath the panel.
    let (image_phase_failed, qos_by_binary) = if let BuildOutcome::Ok {
        selected_binaries, ..
    } = &outcome.build
    {
        run_image_phases(selected_binaries, console.as_mut(), &state, &theme, session_start)
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
    if let BuildOutcome::Failed { exit_code, .. } = outcome.build {
        if let Some(c) = console.take() {
            let _ = c.finish();
        }
        return ExitCode::from(exit_code.clamp(1, 255) as u8);
    }
    if let Some(detail) = image_phase_failed {
        if let Some(c) = console.take() {
            let _ = c.finish();
        }
        eprintln!("ztest run: image preflight failed: {detail}");
        return ExitCode::from(3);
    }
    if matches!(outcome.probe, ProbeOutcome::Failed { .. }) {
        if let Some(c) = console.take() {
            let _ = c.finish();
        }
        return ExitCode::from(2);
    }

    // QoS config lowering (§6): turn the dumped tiers into a generated nextest
    // tool-config and a capacity-bounded thread pool. Cheap, infallible
    // (write errors degrade to "no tool-config"), and a no-op when no QoS
    // tests were declared.
    let lowered = build_qos_lowering(&qos_by_binary, &peek, &outcome.probe);

    // The run phase, on the same console: emulate `cargo nextest run` under a
    // PTY so its cursor-addressed per-test UI renders faithfully, with the live
    // QoS panel pinned beneath. See `cli::console`.
    if let (Some(mut con), Some(plan)) = (console.take(), state.qos_plan.clone()) {
        let free = probe_free(&outcome.probe);
        let total_tests = build_test_count(&state.build);
        // `quiet_progress = false`: we run nextest under a PTY and emulate it, so
        // its native live progress (spinner, running count, per-test stream) is
        // exactly the output we want to show — don't suppress it. The QoS panel
        // sits beneath as added cluster context.
        let (effective_args, envs) = nextest_invocation(&nextest_args, no_cleanup, &lowered, false);
        let exit = con.run_tests(effective_args, envs, &plan, &free, total_tests, &theme);
        let _ = con.finish();
        if let Some(path) = &lowered.tool_config {
            let _ = std::fs::remove_file(path);
        }
        return exit;
    }

    // No QoS plan (or non-TTY): finish the console, then a plain blocking run.
    if let Some(c) = console.take() {
        let _ = c.finish();
    }
    print_launching_line(&theme);
    exec_nextest_run_inherited(&nextest_args, no_cleanup, lowered)
}

/// Probed free capacity, or `ZERO` when the probe was unavailable.
fn probe_free(probe: &ProbeOutcome) -> crate::qos::Resources {
    match probe {
        ProbeOutcome::Ok { capacity, .. } => capacity.free(),
        _ => crate::qos::Resources::ZERO,
    }
}

/// The QoS-derived nextest run knobs: an optional generated tool-config file
/// and the effective `--test-threads` pool.
#[derive(Debug, Default)]
struct LoweredRun {
    /// Absolute path to the generated tool-config TOML, if any QoS tests were
    /// declared. Passed as `--tool-config-file ztest:<path>`.
    tool_config: Option<PathBuf>,
    /// Effective `--test-threads` pool = `min(user ceiling, cluster units)`.
    test_threads: u32,
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
    // capacity), consistent with the `--test-threads` pool and in-process
    // admission — not live `free()`, which would shrink the estimate by
    // ztest's own load and overstate the wave count.
    let ceiling = match probe {
        ProbeOutcome::Ok { capacity, .. } => Some(capacity.admission_ceiling()),
        _ => None,
    };
    Some(crate::qos::schedule::plan(&counts, ceiling))
}

/// Group the per-binary QoS dump by tier, render the tool-config TOML, write
/// it to a temp file, and compute the capacity-bounded thread pool.
fn build_qos_lowering(
    qos_by_binary: &[(String, Vec<QosEntry>)],
    peek: &args_peek::NextestArgs,
    probe: &ProbeOutcome,
) -> LoweredRun {
    // Size the nextest `--test-threads` pool from the *admission ceiling*
    // (`allocatable − non-ztest baseline`), NOT live `free()`. The pool only
    // bounds how many test processes nextest spawns; QoS admission is the
    // authoritative gate that queues them to fit live capacity. Sizing the
    // pool on `free()` double-counts ztest's own load — every running ztest
    // pod (and any zombie left by `--no-cleanup` or a concurrent run) shrinks
    // the pool, so the suite serializes to one-at-a-time and admission never
    // sees the contention it exists to manage. The ceiling excludes ztest
    // namespaces, so the pool reflects how densely the cluster *can* pack.
    let (ceiling, nvme_nodes) = match probe {
        ProbeOutcome::Ok {
            capacity,
            nvme_nodes,
            ..
        } => (Some(capacity.admission_ceiling()), *nvme_nodes),
        _ => (None, 0),
    };
    let test_threads = lower::pool(peek.test_threads, ceiling, lower::DEFAULT_POOL);

    let mut by_tier: BTreeMap<crate::qos::QosClass, Vec<TestRef>> = BTreeMap::new();
    for (binary_id, entries) in qos_by_binary {
        for e in entries {
            by_tier.entry(e.class).or_default().push(TestRef {
                binary_id: binary_id.clone(),
                name: lower::nextest_test_name(&e.test_id).to_string(),
            });
        }
    }
    let profile = peek.profile.as_deref().unwrap_or("default");
    let tool_config = lower::render_tool_config(&by_tier, profile, nvme_nodes).and_then(|toml| {
        let path = std::env::temp_dir().join(format!("ztest-qos-{}.toml", std::process::id()));
        match std::fs::write(&path, toml) {
            Ok(()) => Some(path),
            Err(e) => {
                // Degrade gracefully: the broker is still authoritative (§6),
                // so a missing tool-config only loses coarse backpressure.
                eprintln!("ztest: could not write QoS tool-config ({e}); continuing without it");
                None
            }
        }
    });

    LoweredRun {
        tool_config,
        test_threads,
    }
}

/// Render the compact bottom panel for the preflight/build/image phases —
/// `render_preflight_panel` with the session-wide spinner clock. `phase` is the
/// right-aligned action label (`Preflight` during the probe + compile,
/// `Building` during the image phase).
fn phase_panel(state: &BannerState, theme: &Theme, session_start: Instant, phase: &str) -> String {
    preflight::render_preflight_panel(state, phase, session_start.elapsed(), theme)
}

/// `   Launching nextest run` in nextest's `{:>12} {}` vocabulary.
fn print_launching_line(theme: &Theme) {
    use owo_colors::OwoColorize;
    println!(
        "{:>12} {} run",
        "Launching".style(theme.styles.pass),
        "nextest".style(theme.styles.script_id),
    );
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
    nextest_args: &[String],
    theme: &Theme,
    state: &mut BannerState,
    console: Option<&mut Console>,
    session_start: Instant,
) -> std::io::Result<PipelineOutcome> {
    match console {
        Some(con) => pipeline_console(nextest_args, theme, state, con, session_start),
        None => pipeline_inherited(nextest_args, state),
    }
}

/// TTY pipeline: pass 1 (`cargo nextest list`) under the console PTY with the
/// probe running concurrently, then pass 2 (JSON index) captured.
fn pipeline_console(
    nextest_args: &[String],
    theme: &Theme,
    state: &mut BannerState,
    con: &mut Console,
    session_start: Instant,
) -> std::io::Result<PipelineOutcome> {
    use crate::preflight::{BuildStage, BuildState};

    let list_args = pipeline::build::list_args(nextest_args);
    let started_at = Instant::now();
    state.build = BuildState::Compiling { started_at };

    // Pass 1 compiles the test binaries. We use `run --no-run` rather than
    // `list` because, under a PTY, stdout and stderr are merged onto one stream
    // — `list` would dump its full human-readable test listing (stdout) into the
    // view, whereas `run --no-run` emits only cargo's compile output. The args
    // are the user's run args verbatim (filters etc. don't affect compilation;
    // run-only flags are valid here). Pass 2 (`index`) does the JSON inventory.
    let mut args = vec![
        "nextest".to_string(),
        "run".to_string(),
        "--no-run".to_string(),
    ];
    args.extend(nextest_args.iter().cloned());

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
        con.runtime().block_on(pipeline::build::index(&list_args))?
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
    nextest_args: &[String],
    state: &mut BannerState,
) -> std::io::Result<PipelineOutcome> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(3)
        .build()?;
    let nextest_args = nextest_args.to_vec();

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
            tokio::spawn(async move { pipeline::build::run(&nextest_args, &build_tx, None).await });

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

/// Run the inventory-driven image phases (C: dump, D: docker build, E: kind
/// load). Returns the failure detail (if any) **and** the per-binary QoS tier
/// declarations harvested from the same dump (consumed by the config-lowering
/// step). The QoS data is returned even when there are no dev images to build.
///
/// docker/kind output is relayed through `console` into native scrollback
/// beneath the panel (or, in non-TTY mode, printed to stderr for the CI log).
fn run_image_phases(
    binaries: &[pipeline::SelectedBinary],
    console: Option<&mut Console>,
    state: &BannerState,
    theme: &Theme,
    session_start: Instant,
) -> (Option<String>, Vec<(String, Vec<QosEntry>)>) {
    use crate::pipeline::{docker, images, kind_load};

    // Phase C — inventory dump. Async because it spawns subprocesses
    // concurrently in the future; for now serial-await is fine.
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => return (Some(format!("tokio runtime: {e}")), Vec::new()),
    };
    let (outcome, qos_by_binary) = rt.block_on(images::discover(binaries));
    let images = match outcome {
        images::ImagesOutcome::Discovered { images } => images,
        images::ImagesOutcome::Failed { detail } => return (Some(detail), qos_by_binary),
    };
    if images.is_empty() {
        return (None, qos_by_binary);
    }

    // We're about to relay plain docker/kind lines via `log_line` (insert_before),
    // which would land *above* whatever the live region currently shows (the
    // compile's final frame). Commit that frame to scrollback and blank the live
    // region first so the image output reads in order.
    let mut console = console;
    if let Some(c) = console.as_mut() {
        let _ = c.flush_live();
        let _ = c.paint_panel(&phase_panel(state, theme, session_start, "Building"));
    }

    // Sink for docker/kind output: relay each line into the console's
    // scrollback and refresh the panel (or print to stderr when there's no
    // console, preserving CI-log behaviour).
    let mut sink = |line: &str| match console.as_mut() {
        Some(c) => {
            let _ = c.log_line(line);
            let _ = c.paint_panel(&phase_panel(state, theme, session_start, "Building"));
        }
        None => eprintln!("{line}"),
    };

    sink(&format!("ztest: {} dev image(s) to preflight", images.len()));

    // Phase D — docker build (blocking; runs synchronously here).
    let built = match docker::build_all(&images, &mut sink) {
        docker::DockerOutcome::Built { images } => images,
        docker::DockerOutcome::Failed { detail } => return (Some(detail), qos_by_binary),
    };

    // Phase E — kind load (blocking).
    let failure = match kind_load::load_all(&built, &mut sink) {
        kind_load::KindLoadOutcome::Loaded { count, skipped } => {
            sink(&format!("ztest: kind load: {count} loaded, {skipped} cached"));
            None
        }
        kind_load::KindLoadOutcome::Failed { detail } => Some(detail),
    };
    (failure, qos_by_binary)
}

/// `cargo nextest run` inherits this process's stdio.
///
/// We force `--test-threads=<pool>` where `pool = min(user ceiling, cluster
/// units)` (§6) — replacing any user-supplied value, which acts only as the
/// *local* ceiling. The current single-node kind cluster gets I/O-bound when
/// more than ~6 tests spin up their pods concurrently, so [`lower::DEFAULT_POOL`]
/// (6) is the fallback when capacity is unknown. When QoS tests were declared
/// we also prepend `--tool-config-file ztest:<generated>` carrying the
/// per-tier slow-timeout/priority/threads-required overrides (Layer 1, §5.2).
fn exec_nextest_run_inherited(
    nextest_args: &[String],
    no_cleanup: bool,
    lowered: LoweredRun,
) -> ExitCode {
    let mut cmd = build_nextest_command(nextest_args, no_cleanup, &lowered, false);
    let status = cmd.status();

    // Best-effort temp-file cleanup (the run is over; nextest has read it).
    if let Some(path) = &lowered.tool_config {
        let _ = std::fs::remove_file(path);
    }

    match status {
        Ok(status) => exit_code_of(&status),
        Err(err) => {
            eprintln!("ztest run: failed to spawn `cargo nextest run`: {err}");
            ExitCode::from(127)
        }
    }
}

/// Assemble the `cargo nextest run` command shared by the blocking and
/// live-panel paths: force `--test-threads=<pool>` (§6), prepend the generated
/// `--tool-config-file ztest:<path>` if any, set `ZTEST_QOS`/`ZTEST_NO_CLEANUP`,
/// and — when `quiet_progress` — `--show-progress=none` so nextest's own bar
/// doesn't fight the pinned QOS panel (skipped if the user set `--show-progress`).
fn build_nextest_command(
    nextest_args: &[String],
    no_cleanup: bool,
    lowered: &LoweredRun,
    quiet_progress: bool,
) -> Command {
    let (effective_args, envs) =
        nextest_invocation(nextest_args, no_cleanup, lowered, quiet_progress);

    let mut cmd = Command::new("cargo");
    cmd.arg("nextest").arg("run").args(&effective_args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd
}

/// Compute the `cargo nextest run` argument tail (everything after `run`) and
/// the extra environment variables, shared by the blocking [`Command`] path
/// ([`build_nextest_command`]) and the PTY live-panel path ([`crate::cli::console`]).
///
/// - Drops any user `--test-threads`/`-j` (already folded into the pool as the
///   local ceiling) and forces the computed capacity-bounded pool (§6).
/// - When `quiet_progress`, suppresses nextest's own progress bar with
///   `--show-progress=none` (unless the user set `--show-progress`) so it
///   doesn't fight a pinned panel — the panel becomes the progress authority.
/// - Prepends the generated `--tool-config-file ztest:<path>` if any.
/// - `ZTEST_NO_CLEANUP` (when requested) and `ZTEST_QOS=1` enable post-mortem
///   retention and broker admission in the spawned test binaries.
fn nextest_invocation(
    nextest_args: &[String],
    no_cleanup: bool,
    lowered: &LoweredRun,
    quiet_progress: bool,
) -> (Vec<String>, Vec<(&'static str, String)>) {
    let mut effective_args = strip_test_threads_flag(nextest_args);
    effective_args.insert(0, format!("--test-threads={}", lowered.test_threads));
    if quiet_progress && !nextest_args.iter().any(|a| a.starts_with("--show-progress")) {
        effective_args.insert(0, "--show-progress=none".to_string());
    }
    if let Some(path) = &lowered.tool_config {
        // `--tool-config-file TOOL:ABS_PATH`. The `ztest` tool name namespaces
        // our overrides; the path must be absolute (it is — `temp_dir()`).
        effective_args.insert(0, format!("ztest:{}", path.display()));
        effective_args.insert(0, "--tool-config-file".to_string());
    }

    let mut envs: Vec<(&'static str, String)> = Vec::with_capacity(2);
    // Propagate `--no-cleanup` to the test binaries: `TestEnv::drop` reads
    // `ZTEST_NO_CLEANUP` and skips namespace teardown for post-mortem.
    if no_cleanup {
        envs.push(("ZTEST_NO_CLEANUP", "1".to_string()));
    }
    // Enable QoS admission in the test binaries. `ztest run` has probed the
    // cluster and opts every spawned test into broker admission + tier
    // placement via `ZTEST_QOS`. A developer invoking `cargo nextest run`
    // directly never sets this, and `TestEnv::build()` degrades gracefully.
    // See `cluster::qos_enabled`.
    envs.push(("ZTEST_QOS", "1".to_string()));
    (effective_args, envs)
}

/// Map a finished child status to a process exit code. A normal exit
/// propagates its code; a signal death uses the conventional shell encoding
/// `128 + signum` (so SIGINT→130, SIGKILL→137, SIGSEGV→139) rather than a flat
/// 130 that would mask which signal fired.
fn exit_code_of(status: &std::process::ExitStatus) -> ExitCode {
    if let Some(code) = status.code() {
        return ExitCode::from(code.clamp(0, 255) as u8);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return ExitCode::from((128 + sig).clamp(0, 255) as u8);
        }
    }
    ExitCode::from(1)
}

/// Total test count discovered by Phase B, or `0` if the build state never
/// reached a successful list (degrades the panel's progress line to a bare
/// "done" count with no denominator).
fn build_test_count(build: &crate::preflight::BuildState) -> u32 {
    match build {
        crate::preflight::BuildState::Ok { test_count, .. } => *test_count as u32,
        _ => 0,
    }
}

/// Split the ztest-only `--no-cleanup` flag out of the forwarded argv.
///
/// `--no-cleanup` is not a `cargo nextest` flag, so it must be removed
/// before the rest is handed to nextest (which would otherwise reject it).
/// Returns whether it was present and the argv with every occurrence
/// removed. Only tokens *before* a `--` separator are considered — anything
/// after `--` is a nextest filter positional and is left untouched, matching
/// the convention in [`crate::cli::args_peek`].
fn extract_no_cleanup(args: Vec<String>) -> (bool, Vec<String>) {
    const FLAG: &str = "--no-cleanup";
    let mut found = false;
    let mut past_separator = false;
    let mut out = Vec::with_capacity(args.len());
    for arg in args {
        if !past_separator && arg == "--" {
            past_separator = true;
        } else if !past_separator && arg == FLAG {
            found = true;
            continue;
        }
        out.push(arg);
    }
    (found, out)
}

/// Remove any `--test-threads`/`-j` flag (and its value) from the forwarded
/// argv, so `exec_nextest_run_inherited` can inject the single
/// capacity-bounded pool without a duplicate flag. The user's value was
/// already folded in as the local ceiling (see [`lower::pool`]); here we just
/// strip it. Handles `--test-threads N`, `--test-threads=N`, `-j N`, `-jN`.
/// Only tokens before a `--` separator are considered (positionals after `--`
/// are filter expressions), matching [`extract_no_cleanup`].
fn strip_test_threads_flag(args: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(args.len());
    let mut past_separator = false;
    let mut skip_next = false;
    for arg in args {
        if skip_next {
            // This token is the value of a space-separated `--test-threads`/`-j`.
            skip_next = false;
            continue;
        }
        if !past_separator && arg == "--" {
            past_separator = true;
            out.push(arg.clone());
            continue;
        }
        if !past_separator {
            if arg == "--test-threads" || arg == "-j" {
                skip_next = true; // drop the following value token too
                continue;
            }
            if arg.starts_with("--test-threads=") || (arg.starts_with("-j") && arg.len() > 2) {
                continue; // attached value form
            }
        }
        out.push(arg.clone());
    }
    out
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

fn build_initial_state(peek: &args_peek::NextestArgs) -> BannerState {
    BannerState {
        cluster: ClusterState {
            context: "probing…".to_string(),
            slots_used: 0,
            slots_total: 16,
            slots_configured: peek.test_threads.unwrap_or(0),
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

    #[test]
    fn extract_no_cleanup_strips_flag_anywhere() {
        let (found, rest) = extract_no_cleanup(v(&["-p", "wallet-tests", "--no-cleanup"]));
        assert!(found);
        assert_eq!(rest, v(&["-p", "wallet-tests"]));

        let (found, rest) = extract_no_cleanup(v(&["--no-cleanup", "-E", "test(foo)"]));
        assert!(found);
        assert_eq!(rest, v(&["-E", "test(foo)"]));
    }

    #[test]
    fn extract_no_cleanup_absent_is_identity() {
        let (found, rest) = extract_no_cleanup(v(&["-p", "wallet-tests"]));
        assert!(!found);
        assert_eq!(rest, v(&["-p", "wallet-tests"]));
    }

    #[test]
    fn extract_no_cleanup_ignores_after_double_dash() {
        // After `--`, tokens are nextest filter positionals — left verbatim.
        let (found, rest) = extract_no_cleanup(v(&["--", "--no-cleanup"]));
        assert!(!found);
        assert_eq!(rest, v(&["--", "--no-cleanup"]));
    }

    #[test]
    fn strip_test_threads_handles_every_form() {
        // Space-separated long flag (drops flag + value).
        assert_eq!(
            strip_test_threads_flag(&v(&["-p", "wt", "--test-threads", "8", "--no-fail-fast"])),
            v(&["-p", "wt", "--no-fail-fast"])
        );
        // Attached long flag.
        assert_eq!(
            strip_test_threads_flag(&v(&["--test-threads=8", "-E", "test(x)"])),
            v(&["-E", "test(x)"])
        );
        // Short forms: `-j 8` and `-j8`.
        assert_eq!(strip_test_threads_flag(&v(&["-j", "8", "x"])), v(&["x"]));
        assert_eq!(strip_test_threads_flag(&v(&["-j8", "x"])), v(&["x"]));
        // Absent → identity.
        assert_eq!(strip_test_threads_flag(&v(&["-p", "wt"])), v(&["-p", "wt"]));
    }

    #[test]
    fn strip_test_threads_ignores_after_double_dash() {
        // A filter positional after `--` that happens to contain the text is
        // left alone.
        assert_eq!(
            strip_test_threads_flag(&v(&["--", "--test-threads", "8"])),
            v(&["--", "--test-threads", "8"])
        );
    }
}
