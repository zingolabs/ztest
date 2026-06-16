//! `ztest run` — preflight orchestration + `cargo nextest run`.
//!
//! ## Architecture
//!
//! 1. **Pin the banner to the top of main screen** via [`PinnedHeader`]
//!    (DECSTBM scroll region in main screen — banner stays anchored at
//!    rows 1..H while everything below scrolls within rows H+1..bottom).
//! 2. Run Phase A (cluster probe → archive discovery) and Phase B
//!    (`cargo nextest list`) concurrently. Cargo's `Compiling foo...`
//!    stderr is inherited and lands in the scroll region beneath the
//!    banner. As each Phase-A sub-step completes, the banner is
//!    redrawn in place (cursor save → home → write → restore) so the
//!    cluster context, archive rows, etc. fill in live without
//!    disturbing cargo's output.
//! 3. Release DECSTBM with `\x1b[r`. Banner + cargo output stay on
//!    screen; subsequent writes scroll naturally so everything ends
//!    up in the terminal's native scrollback.
//! 4. Hand off to `cargo nextest run` with **inherited stdio**.
//!    Nextest sees the real TTY, renders its own live progress bar,
//!    and writes from where cargo's stderr left off.
//!
//! ## Arg-forwarding contract
//!
//! [`Args::nextest_args`] is a `Vec<String>` populated by clap's
//! `trailing_var_arg + allow_hyphen_values` mode — it captures
//! everything after `ztest run` literally. The vec is handed unmodified
//! to `cargo nextest run`.

use std::io::{IsTerminal, Stdout, Write, stdout};
use std::process::{Command, ExitCode};

use clap::Parser;

use crate::cli::args_peek;
use crate::pipeline::{self, ArchivesOutcome, BuildOutcome, ProbeOutcome};
use crate::preflight::{
    self, ArchiveRow, ArchiveStatus, BannerState, ClusterState, FutureRow, PinnedHeader,
    SnapshotRow, Theme,
};

/// `ztest run` arguments.
#[derive(Debug, Parser)]
pub struct Args {
    /// Arguments forwarded verbatim to `cargo nextest run`.
    ///
    /// Any flag, filter expression, or positional substring accepted
    /// by `cargo nextest run` works here. Run
    /// `cargo nextest run --help` for the full reference.
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "NEXTEST_ARGS",
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

    let peek = args_peek::peek(&args.nextest_args);
    let theme = Theme::detect();

    let mut state = build_initial_state(&peek);

    // Pin the banner below the user's command row if stdout is a
    // real terminal. We query the cursor position via crossterm
    // (handles the DSR raw-mode dance) so the banner appears
    // directly below the `ztest run` prompt line instead of
    // overwriting the top of the screen. In non-TTY mode (CI,
    // pipes) we skip the pinning entirely — DECSTBM on a non-
    // terminal writer is meaningless; the banner is printed at the
    // end like any other linear output.
    let mut pinned = if stdout().is_terminal() {
        let rows = terminal_size::terminal_size()
            .map(|(_w, h)| h.0)
            .unwrap_or(40);
        // crossterm reports (col, row) 0-indexed; ANSI is 1-indexed.
        // On failure (no TTY for DSR), fall back to row 1.
        let start_row = crossterm::cursor::position()
            .map(|(_col, row)| row + 1)
            .unwrap_or(1);
        let banner = preflight::render(&state, &theme);
        PinnedHeader::enter(stdout(), rows, &banner, start_row).ok()
    } else {
        None
    };

    let outcome = match pipeline_phase(&args.nextest_args, &theme, &mut state, pinned.as_mut()) {
        Ok(o) => o,
        Err(err) => {
            eprintln!("ztest run: pipeline phase crashed: {err}");
            return ExitCode::from(127);
        }
    };

    // Image phases — only when Phase B succeeded. The pinned banner
    // is still active so docker/kind stderr lands in the scroll
    // region beneath it, same way cargo's `Compiling foo` did.
    let image_phase_failed = if let BuildOutcome::Ok {
        selected_binaries, ..
    } = &outcome.build
    {
        run_image_phases(selected_binaries)
    } else {
        None
    };

    // Final banner redraw with all phases resolved.
    if let Some(pinned) = pinned.as_mut() {
        let _ = pinned.redraw(&preflight::render(&state, &theme));
    }

    // Drop releases DECSTBM. Banner + cargo output stay on screen;
    // subsequent writes scroll naturally into the terminal's native
    // scrollback ring.
    drop(pinned);

    // In non-TTY mode we still want to print the resolved banner so
    // CI logs have a record of what preflight saw.
    if !stdout().is_terminal() {
        let _ = stdout().write_all(preflight::render(&state, &theme).as_bytes());
    }

    if let BuildOutcome::Failed { exit_code, .. } = outcome.build {
        return ExitCode::from(exit_code.clamp(1, 255) as u8);
    }
    if let Some(detail) = image_phase_failed {
        eprintln!("ztest run: image preflight failed: {detail}");
        return ExitCode::from(3);
    }
    if matches!(outcome.probe, ProbeOutcome::Failed { .. }) {
        return ExitCode::from(2);
    }

    // Scroll the preflight content (banner + cargo output) up into
    // the terminal's native scrollback ring via SU (Scroll Up),
    // leaving nextest a clean canvas. `\x1b[<rows>S` pushes exactly
    // the visible content into scrollback regardless of where the
    // cursor is — way cleaner than emitting linefeeds and counting
    // on cursor-at-bottom behavior. `\x1b[H` parks the cursor at
    // (1,1) for nextest to write from.
    if stdout().is_terminal() {
        let rows = terminal_size::terminal_size()
            .map(|(_w, h)| h.0)
            .unwrap_or(40);
        let mut out = stdout().lock();
        let _ = write!(out, "\x1b[{rows}S\x1b[H");
        let _ = out.flush();
    }

    // Transition line — nextest-style right-aligned action label so
    // the handoff reads as just another step in the run, not a
    // separate UI. Lives at row 1 of the now-clean screen; nextest's
    // own `Nextest run ID …` line follows immediately on row 2.
    print_launching_line(&theme);

    exec_nextest_run_inherited(&args.nextest_args)
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

/// Incremental update from one of the concurrently-running phases.
///
/// Phases push these onto an mpsc; the main task drains them, mutates
/// the shared [`BannerState`], and redraws the pinned banner. The
/// pinned-header redraw uses save/home/write/restore — cargo's
/// inherited-stderr output continues uninterrupted beneath the banner
/// throughout.
#[derive(Debug)]
enum Update {
    Probe(ProbeOutcome),
    Archives(ArchivesOutcome),
    ArchivesSkipped,
}

/// Run Phase A (cluster probe + archive discovery) and Phase B
/// (cargo nextest list) on a fresh tokio runtime. Phase B's stderr
/// flows directly to the user's terminal (`cargo Compiling foo...`)
/// so the ~20s of test-binary compile time isn't a silent stall, and
/// because DECSTBM pins the banner at the top, that stderr scrolls
/// beneath without disturbing the banner.
fn pipeline_phase(
    nextest_args: &[String],
    theme: &Theme,
    state: &mut BannerState,
    pinned: Option<&mut PinnedHeader<Stdout>>,
) -> std::io::Result<PipelineOutcome> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(3)
        .build()?;

    let nextest_args = nextest_args.to_vec();

    rt.block_on(async move {
        let (event_tx, mut event_rx) = pipeline::channel();
        let (upd_tx, mut upd_rx) = tokio::sync::mpsc::unbounded_channel::<Update>();

        // Phase A — A1 (probe) then A3 (archives) gated on A1's client.
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

        // Phase B — two-step `cargo nextest list`. Lifecycle (started,
        // indexing, complete/failed) flows through the event channel
        // so the main loop can mutate `state.build` between passes.
        let build_tx = event_tx.clone();
        let build_handle = tokio::spawn(async move {
            pipeline::build::run(&nextest_args, &build_tx).await
        });

        drop(event_tx);
        drop(upd_tx);

        // Multiplex three sources:
        // - `upd_rx`: probe / archive completion (state mutations)
        // - `event_rx`: build lifecycle events (state mutations on
        //   `state.build`)
        // - 200ms ticker: redraw so the spinner / elapsed-time
        //   fields in `state.build` animate while compile is running
        //
        // Loop exits when both producer channels are closed.
        let mut pinned = pinned;
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(200));
        // Skip the initial immediate tick — we draw on real changes.
        tick.tick().await;

        let mut upd_open = true;
        let mut event_open = true;
        loop {
            tokio::select! {
                biased;
                upd = upd_rx.recv(), if upd_open => match upd {
                    Some(u) => {
                        apply_update(state, u);
                        if let Some(p) = pinned.as_mut() {
                            let _ = p.redraw(&preflight::render(state, theme));
                        }
                    }
                    None => upd_open = false,
                },
                evt = event_rx.recv(), if event_open => match evt {
                    Some(e) => {
                        apply_event(state, e);
                        if let Some(p) = pinned.as_mut() {
                            let _ = p.redraw(&preflight::render(state, theme));
                        }
                    }
                    None => event_open = false,
                },
                _ = tick.tick() => {
                    // No state changes — but if build is in a
                    // ticking phase (Compiling/Indexing), the spinner
                    // glyph + elapsed seconds advance on each redraw.
                    if matches!(
                        state.build,
                        crate::preflight::BuildState::Compiling { .. }
                            | crate::preflight::BuildState::Indexing { .. }
                    ) && let Some(p) = pinned.as_mut() {
                        let _ = p.redraw(&preflight::render(state, theme));
                    }
                }
            }
            if !upd_open && !event_open {
                break;
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
            cores,
            memory_gib,
            slots_used,
        }) => {
            state.cluster.context = context;
            state.cluster.nodes_ready = nodes_ready;
            state.cluster.nodes_cordoned = nodes_cordoned;
            state.cluster.cores = cores;
            state.cluster.memory_gib = memory_gib;
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

/// Run Phases C/D/E — image inventory dump, `docker build` per unique
/// image, `kind load docker-image` for the freshly-built tags. Returns
/// `Some(detail)` on failure and `None` on success (including the
/// empty-inventory case where there's nothing to do).
///
/// Output lands in the pinned-banner scroll region: the dump phase is
/// silent on success; docker streams its own progress through
/// `image::docker_build`; kind emits one stderr line per tag. The
/// banner state itself isn't touched here — image-phase rows are
/// future work.
fn run_image_phases(binaries: &[pipeline::SelectedBinary]) -> Option<String> {
    use crate::pipeline::{docker, images, kind_load};

    // Phase C — inventory dump. Async because it spawns subprocesses
    // concurrently in the future; for now serial-await is fine.
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => return Some(format!("tokio runtime: {e}")),
    };
    let images = match rt.block_on(images::discover(binaries)) {
        images::ImagesOutcome::Discovered { images } => images,
        images::ImagesOutcome::Failed { detail } => return Some(detail),
    };
    if images.is_empty() {
        return None;
    }
    eprintln!("ztest: {} dev image(s) to preflight", images.len());

    // Phase D — docker build (blocking; runs synchronously here).
    let built = match docker::build_all(&images) {
        docker::DockerOutcome::Built { images } => images,
        docker::DockerOutcome::Failed { detail } => return Some(detail),
    };

    // Phase E — kind load (blocking).
    match kind_load::load_all(&built) {
        kind_load::KindLoadOutcome::Loaded { count, skipped } => {
            eprintln!("ztest: kind load: {count} loaded, {skipped} cached");
            None
        }
        kind_load::KindLoadOutcome::Failed { detail } => Some(detail),
    }
}

/// `cargo nextest run` inherits this process's stdio.
///
/// If the user didn't supply `--test-threads`, we inject
/// `--test-threads=6`. The current single-node kind cluster gets I/O-
/// bound when more than ~6 tests spin up their pods concurrently —
/// individual pod startup that takes ~10s on an idle cluster stretches
/// past the 20s `pod_ready` timeout under stampede. 6 is the empirical
/// sweet spot; users can override by passing `--test-threads N`
/// explicitly.
fn exec_nextest_run_inherited(nextest_args: &[String]) -> ExitCode {
    const DEFAULT_TEST_THREADS: &str = "6";

    let user_set_threads = args_peek::peek(nextest_args).test_threads.is_some();
    let mut effective_args: Vec<String> = nextest_args.to_vec();
    if !user_set_threads {
        effective_args.insert(0, format!("--test-threads={DEFAULT_TEST_THREADS}"));
    }

    let mut cmd = Command::new("cargo");
    cmd.arg("nextest").arg("run").args(&effective_args);

    match cmd.status() {
        Ok(status) => match status.code() {
            Some(code) => ExitCode::from(code.clamp(0, 255) as u8),
            None => ExitCode::from(130),
        },
        Err(err) => {
            eprintln!("ztest run: failed to spawn `cargo nextest run`: {err}");
            ExitCode::from(127)
        }
    }
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
            cores: 0,
            memory_gib: 0,
        },
        build: crate::preflight::BuildState::Pending,
        archives: Vec::<ArchiveRow>::new(),
        snapshots: Vec::<SnapshotRow>::new(),
        future: vec![
            FutureRow {
                label: "cluster probe",
            },
            FutureRow {
                label: "mount inventory",
            },
            FutureRow {
                label: "archive fetch",
            },
            FutureRow {
                label: "snapshot bind",
            },
            FutureRow { label: "tier" },
            FutureRow { label: "queue" },
            FutureRow {
                label: "reservation",
            },
        ],
    }
}
