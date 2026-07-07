//! The native test-execution engine: ztest's 2D QoS [`Scheduler`] owns test
//! execution, spawning each test as its own process and packing them into live
//! cluster capacity. Replaces the old `cargo nextest run` subprocess.
//!
//! [`plan`] is the work-list (test x footprint x tier). [`dylib`] is the
//! `LD_LIBRARY_PATH` the children inherit. [`exec`] spawns one test process.
//! [`schedule`] is the capacity-bounded run loop. [`events`] holds the lifecycle
//! events and the [`RunReporter`](events::RunReporter) trait. [`panel`] builds
//! the live QoS panel inputs.
//!
//! [`Scheduler`]: crate::qos::scheduler::Scheduler

pub mod dylib;
pub mod events;
pub mod exec;
pub mod panel;
pub mod plan;
pub mod reporter;
pub mod schedule;

#[cfg(test)]
mod e2e;

use std::process::ExitCode;
use std::time::Duration;

use nextest_metadata::{NextestExitCode, TestListSummary};

use crate::cancel::Cancel;
use crate::cli::console::{Console, SceneFrame};
use crate::engine::events::RunReporter as _;
use crate::engine::exec::EngineEnv;
use crate::engine::reporter::StyledReporter;
use crate::engine::schedule::{LoopConfig, PanelFrame, run_loop};
use crate::inventory::QosEntry;
use crate::pipeline::SelectedBinary;
use crate::preflight::{Theme, render_live_panel};
use crate::qos::Resources;
use crate::qos::schedule::QosPlan;

/// Run-behavior options (parsed from `ztest run` flags).
#[derive(Debug, Clone)]
pub struct EngineOpts {
    /// Max retry attempts per test on failure (0 = run once).
    pub retries: u32,
    /// Stop admitting on the first terminal failure (default false: ztest runs
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
    /// Resource dependency edges (binary images + per-test seeds), attached to
    /// each work item so the run loop can gate/skip on readiness.
    pub resource_deps: crate::engine::plan::ResourceDeps,
    /// Provisioned resource states (node → state) the run loop gates admission
    /// on. Empty when the run declared no resources.
    pub resource_states:
        std::collections::HashMap<crate::resource::NodeId, crate::resource::NodeState>,
    /// Run-behavior options.
    pub opts: EngineOpts,
}

/// Run the engine to completion and map the result to a process exit code
/// (mirroring `NextestExitCode`).
///
/// With a [`Console`] (TTY), the run produces scenes plus scrollback through the
/// session's render thread: nextest-style verdict lines scroll into native
/// scrollback while the QoS panel ([`render_live_panel`]) stays pinned beneath.
/// Without one (CI / piped), it renders plain lines to stdout. Either way it runs
/// on the caller's `work_rt`; the render thread is entirely separate.
pub(crate) fn run(
    work_rt: &tokio::runtime::Runtime,
    input: EngineInput<'_>,
    console: Option<&Console>,
    theme: &Theme,
    qos_plan: Option<QosPlan>,
) -> ExitCode {
    let items = plan::build_work_list(
        input.selected_binaries,
        input.qos_by_binary,
        input.opts.retries,
        &input.resource_deps,
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
        // On a TTY the render thread fires this on Ctrl-C; off a TTY there's no
        // render thread, so the process dies on the default SIGINT disposition.
        cancel: console.map(Console::cancel).unwrap_or_else(Cancel::never),
        resources: input.resource_states,
    };
    let ceiling = input.ceiling;

    let stats = match console {
        Some(c) => run_tty(work_rt, items, ceiling, cfg, env, c, theme, qos_plan),
        None => run_inherited(work_rt, items, ceiling, cfg, env),
    };

    let stats = match stats {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ztest engine: {e}");
            return ExitCode::from(NextestExitCode::SETUP_ERROR as u8);
        }
    };

    // A cancelled run (Ctrl-C) is never a success: in-flight tests were killed
    // and counted as failed, and tests past the cancellation point never ran, so
    // the run is incomplete. Mirror nextest's non-zero cancel exit even in the
    // (near-impossible) case where nothing had failed yet when cancel landed.
    let cancelled = console.map(|c| c.cancel().is_cancelled()).unwrap_or(false);

    if stats.any_failed() || cancelled {
        ExitCode::from(NextestExitCode::TEST_RUN_FAILED as u8)
    } else if stats.skipped > 0 {
        // Unschedulable tests are a setup-level problem.
        ExitCode::from(NextestExitCode::SETUP_ERROR as u8)
    } else {
        ExitCode::from(NextestExitCode::OK as u8)
    }
}

/// The TTY path: produce scenes plus scrollback through the session's render
/// thread. Verdict lines go to native scrollback; each tick ships a fresh scene
/// whose live region is the nextest-style progress line plus running-tests list
/// and whose panel is the QoS [`render_live_panel`]. The render thread owns all
/// painting.
#[allow(clippy::too_many_arguments)]
fn run_tty(
    rt: &tokio::runtime::Runtime,
    items: Vec<plan::WorkItem>,
    ceiling: Resources,
    cfg: LoopConfig,
    env: EngineEnv,
    console: &Console,
    theme: &Theme,
    qos_plan: Option<QosPlan>,
) -> std::io::Result<events::RunStats> {
    let color = supports_color::on(supports_color::Stream::Stdout).is_some();
    let mut reporter = StyledReporter::new(
        color,
        supports_unicode::on(supports_unicode::Stream::Stdout),
    );
    let plan = qos_plan.unwrap_or_else(empty_plan);
    // The live region's height (the same rows the compile phase used for cargo);
    // the run fills them with the running-tests block.
    let live_rows = console.live_rows() as usize;

    // Commit the preflight/image phase's final emulated grid into native
    // scrollback before we switch the live region from the child PTY to our own
    // engine-rendered lines.
    console.flush_live();

    let stats = drive(items, ceiling, cfg, env, rt, &mut reporter, |rep, frame| {
        let bytes = rep.take_scrollback();
        if !bytes.is_empty() {
            console.scrollback(String::from_utf8_lossy(&bytes).into_owned());
        }
        // The reporter's per-test verdict lines scroll into native scrollback
        // (above), following nextest conventions just like the build/image
        // subprocess output. The pinned panel is the QoS/progress summary,
        // snapshotted into an immutable scene the render thread re-paints (and
        // animates the spinner) until the next tick.
        let left = render_live_panel(&frame.snapshot, &plan, &frame.free, &frame.progress, theme);
        // The live region above the panel shows the running-tests block (the
        // nextest-style live status), filling the same rows the compile phase used
        // for cargo's output. The `avt` grid is idle during the run, so we drive
        // the live region explicitly via the scene.
        let live = reporter::render_running(&frame.running, live_rows, color).join("\n");
        // By the run phase every background transfer has completed (provisioning
        // is a pre-run barrier), so the right column is blank. The width-driven
        // two-column split still holds, so the left column keeps the same width it
        // had during preflight — the panel doesn't reflow at the handoff.
        console.scene(move |_elapsed| SceneFrame {
            left: left.clone(),
            right: String::new(),
            live: Some(live.clone()),
        });
    });

    // Commit any leftover scroll-lines (including the final summary, emitted
    // after the last tick). The render thread's teardown restores the terminal.
    let leftover = reporter.take_scrollback();
    if !leftover.is_empty() {
        console.scrollback(String::from_utf8_lossy(&leftover).into_owned());
    }
    Ok(stats)
}

/// The non-TTY path: plain scroll-lines to stdout on the work runtime.
fn run_inherited(
    rt: &tokio::runtime::Runtime,
    items: Vec<plan::WorkItem>,
    ceiling: Resources,
    cfg: LoopConfig,
    env: EngineEnv,
) -> std::io::Result<events::RunStats> {
    let mut reporter = StyledReporter::new(
        false,
        supports_unicode::on(supports_unicode::Stream::Stdout),
    );
    let stats = drive(
        items,
        ceiling,
        cfg,
        env,
        rt,
        &mut reporter,
        |rep, _frame| flush_stdout(rep),
    );
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
    let cancel = cfg.cancel.clone();
    rt.block_on(run_loop(
        items,
        ceiling,
        cfg,
        reporter,
        move |item, _attempt| {
            let env = env.clone();
            let cancel = cancel.clone();
            async move {
                let cap = item.hard_cap;
                exec::spawn_test(&item, &env, cap, &cancel).await
            }
        },
        on_tick,
    ))
}

/// An empty plan for runs with no `#[qos]` declarations: the panel then shows
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
