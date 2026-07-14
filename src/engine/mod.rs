//! The native test-execution engine: ztest's 2D QoS [`Scheduler`] owns test
//! execution, spawning each test as its own process and packing them into live
//! cluster capacity. Replaces the old `cargo nextest run` subprocess.
//!
//! [`plan`] is the work-list (test x footprint x tier). [`dylib`] is the
//! `LD_LIBRARY_PATH` the children inherit. [`local_runner`] spawns one test process.
//! [`schedule`] is the capacity-bounded run loop. [`events`] holds the lifecycle
//! events and the [`RunReporter`](events::RunReporter) trait. [`panel`] builds
//! the live QoS panel inputs.
//!
//! [`Scheduler`]: crate::qos::scheduler::Scheduler

pub mod dylib;
pub mod events;
pub mod local_runner;
pub mod panel;
pub mod plan;
pub mod pod_runner;
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
use crate::engine::local_runner::EngineEnv;
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
    /// Pull reference of the preflight-built runner image. `Some` → run each test
    /// in a pod from it (remote clusters); `None` → local child processes.
    pub runner_image: Option<String>,
    /// Resolved `spec_key → pull reference` for the run's dev component images,
    /// forwarded into each runner pod as [`crate::backends::image::IMAGE_REFS_ENV`]
    /// so an in-pod test resolves them without the Dockerfile it lacks. Empty for
    /// local runs.
    pub image_refs: std::collections::BTreeMap<String, String>,
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
    let executor = match select_executor(work_rt, &input, env) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("ztest engine: {e}");
            return ExitCode::from(NextestExitCode::SETUP_ERROR as u8);
        }
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
        Some(c) => run_tty(work_rt, items, ceiling, cfg, executor, c, theme, qos_plan),
        None => run_inherited(work_rt, items, ceiling, cfg, executor),
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
    executor: std::sync::Arc<dyn local_runner::Executor>,
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

    let stats = drive(
        items,
        ceiling,
        cfg,
        executor,
        rt,
        &mut reporter,
        |rep, frame| {
            let bytes = rep.take_scrollback();
            if !bytes.is_empty() {
                console.scrollback(String::from_utf8_lossy(&bytes).into_owned());
            }
            // The reporter's per-test verdict lines scroll into native scrollback
            // (above), following nextest conventions just like the build/image
            // subprocess output. The pinned panel is the QoS/progress summary,
            // snapshotted into an immutable scene the render thread re-paints (and
            // animates the spinner) until the next tick.
            let left =
                render_live_panel(&frame.snapshot, &plan, &frame.free, &frame.progress, theme);
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
        },
    );

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
    executor: std::sync::Arc<dyn local_runner::Executor>,
) -> std::io::Result<events::RunStats> {
    let mut reporter = StyledReporter::new(
        false,
        supports_unicode::on(supports_unicode::Stream::Stdout),
    );
    let stats = drive(
        items,
        ceiling,
        cfg,
        executor,
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
    executor: std::sync::Arc<dyn local_runner::Executor>,
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
        move |item, _attempt| executor.run(item, cancel.clone()),
        on_tick,
    ))
}

/// Choose how tests execute. Default is the local child-process
/// [`LocalExecutor`](local_runner::LocalExecutor). Setting `ZTEST_RUNNER_IMAGE` selects
/// the pod-per-test [`PodExecutor`](pod_runner::PodExecutor): each test runs in a
/// runner pod built from that image (the nix `ztest-runner`), with the workspace
/// delivered via a hostPath mount (kind), so compute leaves the laptop and the
/// test is hermetic. Env knobs (kind demo; a cluster profile will drive these
/// later):
///   ZTEST_RUNNER_IMAGE      runner image ref; presence enables pod execution
///   ZTEST_RUNNER_NAMESPACE  namespace for runner pods (default "default")
///   ZTEST_RUNNER_HOSTPATH   node path holding the workspace (default: the
///                           workspace path itself, i.e. an identical mount)
///   ZTEST_RUNNER_SA         ServiceAccount for the runner pod (default: none)
fn select_executor(
    work_rt: &tokio::runtime::Runtime,
    input: &EngineInput<'_>,
    env: EngineEnv,
) -> Result<std::sync::Arc<dyn local_runner::Executor>, String> {
    // The preflight-built runner image (remote runs) takes precedence; the env var
    // is a manual override (e.g. local kind hostPath testing). Neither → local.
    let from_preflight = input.runner_image.clone();
    let image = match from_preflight.clone().or_else(|| {
        std::env::var("ZTEST_RUNNER_IMAGE")
            .ok()
            .filter(|s| !s.is_empty())
    }) {
        Some(img) => img,
        None => return Ok(std::sync::Arc::new(local_runner::LocalExecutor { env })),
    };

    // `ztest setup` provisions the `ztest` namespace + `ztest` SA (bound to the
    // `ztest-remote` ClusterRole: namespaces/pods/pvcs/… create, + the nonroot-v2
    // SCC on OpenShift) on every target. Running the runner pod as that identity
    // means a component-spawning test in-pod can create its per-test namespace and
    // pods with no extra RBAC. Overridable, but this is the right default.
    let namespace = std::env::var("ZTEST_RUNNER_NAMESPACE").unwrap_or_else(|_| "ztest".into());
    let service_account = Some(
        std::env::var("ZTEST_RUNNER_SA")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "ztest".into()),
    );

    let client = work_rt
        .block_on(crate::cluster::client())
        .map_err(|e| format!("pod executor: connect to cluster: {e}"))?;

    // A preflight-built image is baked (outputs inside it). Otherwise honor the
    // manual delivery knob: `baked`, or `hostpath` (local kind) mounting the
    // workspace from the node at its laptop path.
    let baked = from_preflight.is_some()
        || std::env::var("ZTEST_RUNNER_DELIVERY").as_deref() == Ok("baked");
    let image_refs = input.image_refs.clone();
    let cfg = if baked {
        pod_runner::PodRunConfig::baked(env, image, namespace, service_account, image_refs)
    } else {
        let target_dir = input.summary.rust_build_meta.target_directory.as_str();
        let workspace = std::path::Path::new(target_dir)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| target_dir.to_string());
        let node_workspace =
            std::env::var("ZTEST_RUNNER_HOSTPATH").unwrap_or_else(|_| workspace.clone());
        pod_runner::PodRunConfig::hostpath(
            env,
            image,
            namespace,
            workspace,
            node_workspace,
            service_account,
            image_refs,
        )
    };
    Ok(std::sync::Arc::new(pod_runner::PodExecutor::new(
        client, cfg,
    )))
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
