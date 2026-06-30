//! The native test-execution engine: ztest's 2D QoS [`Scheduler`] owns test
//! execution, spawning each test as its own process and packing them into live
//! cluster capacity. Replaces the old `cargo nextest run` subprocess.
//!
//! - [`plan`] — the work-list (test × footprint × tier).
//! - [`dylib`] — the `LD_LIBRARY_PATH` the children inherit.
//! - [`exec`] — spawning one test process.
//! - [`schedule`] — the capacity-bounded run loop.
//! - [`events`] — the lifecycle events + the [`RunReporter`](events::RunReporter) trait.
//! - [`panel`] — the live QoS panel inputs.
//!
//! [`Scheduler`]: crate::qos::scheduler::Scheduler

pub mod dylib;
pub mod events;
pub mod exec;
pub mod panel;
pub mod plan;
pub mod reporter;
pub mod schedule;

use std::process::ExitCode;
use std::time::Duration;

use nextest_metadata::{NextestExitCode, TestListSummary};

use crate::cli::console::Console;
use crate::engine::events::RunReporter as _;
use crate::engine::exec::EngineEnv;
use crate::engine::reporter::StyledReporter;
use crate::engine::schedule::{run_loop, LoopConfig, PanelFrame};
use crate::inventory::QosEntry;
use crate::pipeline::SelectedBinary;
use crate::preflight::{render_live_panel, Theme};
use crate::qos::schedule::QosPlan;
use crate::qos::Resources;

/// Run-behavior options (parsed from `ztest run` flags).
#[derive(Debug, Clone)]
pub struct EngineOpts {
    /// Max retry attempts per test on failure (0 = run once).
    pub retries: u32,
    /// Stop admitting on the first terminal failure (default false — ztest runs
    /// the whole suite; `--fail-fast` opts in).
    pub fail_fast: bool,
    /// Soft "slow" threshold; `None` disables the SLOW signal.
    pub slow_after: Option<Duration>,
    /// ServiceAccount the run charges against.
    pub sa: String,
    /// Pass `ZTEST_NO_CLEANUP=1` to children.
    pub no_cleanup: bool,
    /// Shared `NEXTEST_RUN_ID`.
    pub run_id: String,
}

/// Everything the engine needs, already extracted from preflight outcomes by
/// the caller (`cli::run`).
#[derive(Debug)]
pub struct EngineInput<'a> {
    /// The `cargo nextest list` summary (for the dylib build-meta).
    pub summary: &'a TestListSummary,
    /// Test binaries with their selected tests.
    pub selected_binaries: &'a [SelectedBinary],
    /// Per-binary QoS tier dump.
    pub qos_by_binary: &'a [(String, Vec<QosEntry>)],
    /// Cluster admission ceiling (seeds the scheduler).
    pub ceiling: Resources,
    /// Run-behavior options.
    pub opts: EngineOpts,
}

/// Run the engine to completion and map the result to a process exit code
/// (mirroring `NextestExitCode`).
///
/// With a [`Console`] (TTY), the run drives the reused [`Surface`] directly —
/// nextest-style verdict lines scroll into native scrollback while the QoS
/// panel ([`render_live_panel`]) stays pinned beneath (one viewport, no flash).
/// Without one (CI / piped), it renders plain lines to stdout.
pub(crate) fn run(
    input: EngineInput<'_>,
    console: Option<Console>,
    theme: &Theme,
    qos_plan: Option<QosPlan>,
) -> ExitCode {
    let items = plan::build_work_list(
        input.selected_binaries,
        input.qos_by_binary,
        input.opts.retries,
    );

    let env = EngineEnv {
        dylib_path: dylib::dylib_path_value(&input.summary.rust_build_meta),
        run_id: input.opts.run_id.clone(),
        sa: input.opts.sa.clone(),
        no_cleanup: input.opts.no_cleanup,
    };
    let cfg = LoopConfig {
        fail_fast: input.opts.fail_fast,
        slow_after: input.opts.slow_after,
        sa: input.opts.sa.clone(),
        redraw: Duration::from_millis(33),
        run_id: input.opts.run_id.clone(),
    };
    let ceiling = input.ceiling;

    let stats = match console {
        Some(c) => run_tty(items, ceiling, cfg, env, c, theme, qos_plan),
        None => run_inherited(items, ceiling, cfg, env),
    };

    let stats = match stats {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ztest engine: {e}");
            return ExitCode::from(NextestExitCode::SETUP_ERROR as u8);
        }
    };

    if stats.any_failed() {
        ExitCode::from(NextestExitCode::TEST_RUN_FAILED as u8)
    } else if stats.skipped > 0 {
        // Unschedulable tests are a setup-level problem.
        ExitCode::from(NextestExitCode::SETUP_ERROR as u8)
    } else {
        ExitCode::from(NextestExitCode::OK as u8)
    }
}

/// The TTY path: drive the reused `Surface` — scroll-lines to native scrollback,
/// the QoS panel pinned beneath — using the console's runtime.
fn run_tty(
    items: Vec<plan::WorkItem>,
    ceiling: Resources,
    cfg: LoopConfig,
    env: EngineEnv,
    console: Console,
    theme: &Theme,
    qos_plan: Option<QosPlan>,
) -> std::io::Result<events::RunStats> {
    let color = supports_color::on(supports_color::Stream::Stdout).is_some();
    // The live region: one top progress line (nextest's `progress_str`) + the
    // running-tests list beneath it.
    let run_rows = live_region_rows();
    let live_rows = run_rows + 1;
    let (mut surface, rt) = console.into_surface(live_rows)?;
    let mut reporter = StyledReporter::new(color);
    let plan = qos_plan.unwrap_or_else(empty_plan);

    let stats = {
        let surf = &mut surface;
        drive(items, ceiling, cfg, env, &rt, &mut reporter, |rep, frame| {
            let bytes = rep.take_scrollback();
            let sb = if bytes.is_empty() {
                Vec::new()
            } else {
                surf.scrollback_from_ansi(&String::from_utf8_lossy(&bytes))
            };
            let to_line = |s: &str| {
                if s.is_empty() {
                    ratatui::text::Line::default()
                } else {
                    surf.scrollback_from_ansi(s).into_iter().next().unwrap_or_default()
                }
            };
            // Live region = the nextest-style progress line on top, then one
            // line per in-flight test (with its elapsed clock), padded to a
            // stable height beneath it.
            let mut live: Vec<ratatui::text::Line> = Vec::with_capacity(live_rows as usize);
            live.push(to_line(&reporter::progress_line(
                &frame.stats,
                frame.running.len(),
                frame.progress.elapsed,
                color,
            )));
            live.extend(
                reporter::render_running(&frame.running, run_rows as usize, color)
                    .iter()
                    .map(|s| to_line(s)),
            );
            let panel =
                render_live_panel(&frame.snapshot, &plan, &frame.free, &frame.progress, theme);
            surf.present(&live, &sb, &panel);
        })
    };

    // Commit any leftover scroll-lines (incl. the final summary, emitted after
    // the last tick) and tear down the viewport.
    let leftover = reporter.take_scrollback();
    let final_lines = surface.scrollback_from_ansi(&String::from_utf8_lossy(&leftover));
    surface.finish(&final_lines)?;
    Ok(stats)
}

/// The non-TTY path: plain scroll-lines to stdout on a fresh runtime.
fn run_inherited(
    items: Vec<plan::WorkItem>,
    ceiling: Resources,
    cfg: LoopConfig,
    env: EngineEnv,
) -> std::io::Result<events::RunStats> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let mut reporter = StyledReporter::new(false);
    let stats = drive(items, ceiling, cfg, env, &rt, &mut reporter, |rep, _frame| {
        flush_stdout(rep)
    });
    flush_stdout(&mut reporter);
    Ok(stats)
}

/// Build the spawn closure and drive [`run_loop`] on `rt`.
fn drive(
    items: Vec<plan::WorkItem>,
    ceiling: Resources,
    cfg: LoopConfig,
    env: EngineEnv,
    rt: &tokio::runtime::Runtime,
    reporter: &mut dyn events::RunReporter,
    on_tick: impl FnMut(&mut dyn events::RunReporter, &PanelFrame),
) -> events::RunStats {
    rt.block_on(run_loop(
        items,
        ceiling,
        cfg,
        reporter,
        move |item, _attempt| {
            let env = env.clone();
            async move {
                let cap = item.hard_cap;
                exec::spawn_test(&item, &env, cap).await
            }
        },
        on_tick,
    ))
}

/// Maximum rows the live running region may occupy. Keeps the reserved viewport
/// bounded (the last slot is the overflow `… and K more running` summary).
const MAX_LIVE_ROWS: u16 = 10;

/// Height of the live running region: [`MAX_LIVE_ROWS`], but never more than
/// half the terminal so the scrollback above it stays usable on short windows.
fn live_region_rows() -> u16 {
    let rows = terminal_size::terminal_size()
        .map(|(_, h)| h.0)
        .unwrap_or(40);
    MAX_LIVE_ROWS.min((rows / 2).max(1))
}

/// An empty plan for runs with no `#[qos]` declarations — the panel then shows
/// running/progress counts without per-tier lines.
fn empty_plan() -> QosPlan {
    QosPlan {
        tiers: Vec::new(),
        total: Resources::ZERO,
        free: None,
        waves: 0,
        peak: Resources::ZERO,
        unschedulable: Vec::new(),
    }
}

/// Drain a reporter's scroll-line bytes to stdout (non-TTY rendering).
fn flush_stdout(rep: &mut dyn events::RunReporter) {
    use std::io::Write as _;
    let bytes = rep.take_scrollback();
    if !bytes.is_empty() {
        let _ = std::io::stdout().write_all(&bytes);
        let _ = std::io::stdout().flush();
    }
}
