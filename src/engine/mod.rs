//! The native test-execution engine: ztest's 2D QoS [`Scheduler`] spawns each
//! test as its own process and packs them into live cluster capacity, replacing
//! the old `cargo nextest run` subprocess.
//!
//! [`Scheduler`]: crate::qos::scheduler::Scheduler

pub mod dylib;
pub mod events;
pub mod local_runner;
pub mod output;
pub mod panel;
pub mod plan;
pub mod pod_runner;
pub mod record;
pub mod reporter;
pub mod schedule;

#[cfg(test)]
mod e2e;

use std::process::ExitCode;
use std::time::Duration;

use nextest_metadata::{NextestExitCode, TestListSummary};

use crate::cancel::Cancel;
use crate::cli::console::{Console, SceneFrame};
use crate::engine::local_runner::EngineEnv;
use crate::engine::reporter::StyledReporter;
use crate::engine::schedule::{LoopConfig, PanelFrame, run_loop};
use crate::inventory::QosEntry;
use crate::pipeline::SelectedBinary;
use crate::preflight::{Theme, render_live_panel};
use crate::qos::Resources;
use crate::qos::schedule::QosPlan;
use crate::resource::impls::policy::{RUN_NAMESPACE, RUN_SERVICE_ACCOUNT};

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
    /// Captured-output display policy (`--success-output`/`--failure-output`)
    /// and capture strategy (`--no-capture`). See [`output`](crate::engine::output).
    pub output: crate::engine::output::OutputConfig,
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
    /// Test identities that passed in a prior run selected by `--rerun`. They are
    /// excluded from this run's work-list, so only tests that did not pass (and
    /// tests new since that run) execute. Empty on a normal run.
    pub rerun_passed: std::collections::HashSet<(String, String)>,
    /// Live cross-run capacity channels from the
    /// [`CapacityGovernor`](crate::qos::governor::CapacityGovernor): the ceiling
    /// the scheduler reconciles from and the demand sink it feeds. `None` for
    /// local runs and tests, where the ceiling stays at its seed.
    pub capacity: Option<crate::engine::schedule::CapacityChannels>,
}

/// Run the engine to completion and map the result to a `NextestExitCode`. With
/// a [`Console`] (TTY) it ships scenes through the render thread — verdict lines
/// scroll into native scrollback, the QoS panel stays pinned beneath; without one
/// (CI / piped) it renders plain lines to stdout. Either way it runs on `work_rt`.
pub(crate) fn run(
    work_rt: &tokio::runtime::Runtime,
    input: EngineInput<'_>,
    console: Option<&Console>,
    theme: &Theme,
    qos_plan: Option<QosPlan>,
) -> ExitCode {
    let mut items = plan::build_work_list(
        input.selected_binaries,
        input.qos_by_binary,
        input.opts.retries,
        &input.resource_deps,
    );

    // `--rerun`: drop the tests that already passed in the selected run, so only
    // not-passed (and new) tests run. The note lands in scrollback before the
    // first verdict.
    if !input.rerun_passed.is_empty() {
        let before = items.len();
        items.retain(|it| {
            !input
                .rerun_passed
                .contains(&(it.binary_id.clone(), it.test_name.clone()))
        });
        let skipped = before - items.len();
        let note = format!(
            "Rerunning {} test(s); {skipped} passed previously and were skipped\n",
            items.len()
        );
        match console {
            Some(c) => c.scrollback(note),
            None => print!("{note}"),
        }
    }

    let env = EngineEnv {
        dylib_path: dylib::dylib_path_value(&input.summary.rust_build_meta),
        run_id: input.opts.run_id.clone(),
        sa: input.opts.sa.clone(),
        no_cleanup: input.opts.no_cleanup,
        capture: input.opts.output.captures(),
        color: supports_color::on(supports_color::Stream::Stdout).is_some(),
        ztest_log: std::env::var("ZTEST_LOG")
            .ok()
            .filter(|v| !v.trim().is_empty()),
    };
    let executor = match select_executor(work_rt, &input, env) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("ztest engine: {e}");
            return ExitCode::from(NextestExitCode::SETUP_ERROR as u8);
        }
    };
    let (cap_rx, demand_tx) = match input.capacity {
        Some(c) => (Some(c.ceiling_rx), Some(c.demand_tx)),
        None => (None, None),
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
        // `--no-capture` serializes the run (nextest's `test_threads = 1`
        // coupling) so streamed output doesn't interleave.
        max_inflight: input.opts.output.is_serial().then_some(1),
        cap_rx,
        demand_tx,
    };
    let ceiling = input.ceiling;
    let output = input.opts.output;

    // A capturing TTY run hands the terminal to the render thread, so diagnostics
    // go to a per-run file (surfaced for `tail -f`) rather than tearing the panel;
    // otherwise they write to stderr like nextest.
    //
    // The file MUST be a local, writable path. The build summary's
    // `target_directory` is unusable here: on the on-cluster path it is the
    // builder pod's `CARGO_TARGET_DIR=/cache/target`, absent on the laptop, so
    // opening the log there fails and the subscriber falls back to stderr —
    // which then spews `ztest::pod` events onto the terminal and desyncs the
    // pinned footer. The local temp dir always exists and is writable.
    let log_path = std::env::temp_dir()
        .join("ztest-logs")
        .join(format!("{}.log", input.opts.run_id));
    match console {
        Some(c) if output.captures() => {
            crate::observ::init(crate::observ::Sink::File(log_path.clone()));
            // Through the console, never a raw `eprintln!`: a direct stderr write
            // here would corrupt the very panel this File sink exists to protect.
            c.scrollback(format!(
                "ztest: diagnostics → {} (filter via ZTEST_LOG)\n",
                log_path.display()
            ));
        }
        _ => crate::observ::init(crate::observ::Sink::Stderr),
    }

    // The recording sink (on by default; `ZTEST_NO_RECORD=1` opts out). Best
    // effort: a setup failure disables recording with a warning rather than
    // aborting the run — the replay feature is auxiliary to running tests.
    let recorder = if record::recording_enabled() {
        build_recorder(&input.opts.run_id, output.captures())
    } else {
        None
    };

    // `--no-capture` streams output live and serially; the pinned TTY panel can't
    // coexist, so even on a terminal it takes the plain inherited path.
    let stats = match console {
        Some(c) if output.captures() => run_tty(
            work_rt, items, ceiling, cfg, executor, c, theme, qos_plan, output, recorder,
        ),
        _ => run_inherited(work_rt, items, ceiling, cfg, executor, output, recorder),
    };

    let stats = match stats {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ztest engine: {e}");
            return ExitCode::from(NextestExitCode::SETUP_ERROR as u8);
        }
    };

    // A cancelled run is never a success: it's incomplete even in the
    // near-impossible case where nothing had failed yet when cancel landed.
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

/// The TTY path: verdict lines go to native scrollback while each tick ships a
/// fresh scene (running-tests live region + QoS [`render_live_panel`]) to the
/// render thread, which owns all painting.
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
    output: crate::engine::output::OutputConfig,
    recorder: Option<record::RunRecorder>,
) -> std::io::Result<events::RunStats> {
    let color = supports_color::on(supports_color::Stream::Stdout).is_some();
    let styled = StyledReporter::new(
        color,
        supports_unicode::on(supports_unicode::Stream::Stdout),
        output,
    );
    let mut reporter = wrap_reporter(styled, recorder);
    let plan = qos_plan.unwrap_or_else(empty_plan);
    let live_rows = console.live_rows() as usize;

    // Commit the preflight/image phase's final grid into scrollback before we
    // switch the live region from the child PTY to our engine-rendered lines.
    console.flush_live();

    let stats = drive(
        items,
        ceiling,
        cfg,
        executor,
        rt,
        reporter.as_mut(),
        |rep, frame| {
            let bytes = rep.take_scrollback();
            if !bytes.is_empty() {
                console.scrollback(String::from_utf8_lossy(&bytes).into_owned());
            }
            // The panel is snapshotted into an immutable scene the render thread
            // re-paints (and animates the spinner) until the next tick.
            let left =
                render_live_panel(&frame.snapshot, &plan, &frame.free, &frame.progress, theme);
            // The `avt` grid is idle during the run, so drive the live region
            // explicitly via the scene.
            let live = reporter::render_running(&frame.running, live_rows, color).join("\n");
            // Provisioning is a pre-run barrier, so the right column is blank; the
            // width-driven split still holds, so the panel doesn't reflow here.
            console.scene(move |_elapsed| SceneFrame {
                left: left.clone(),
                right: String::new(),
                live: Some(live.clone()),
            });
        },
    );

    // Commit any leftover scroll-lines (including the final summary, emitted after
    // the last tick).
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
    output: crate::engine::output::OutputConfig,
    recorder: Option<record::RunRecorder>,
) -> std::io::Result<events::RunStats> {
    let styled = StyledReporter::new(
        false,
        supports_unicode::on(supports_unicode::Stream::Stdout),
        output,
    );
    let mut reporter = wrap_reporter(styled, recorder);
    let stats = drive(
        items,
        ceiling,
        cfg,
        executor,
        rt,
        reporter.as_mut(),
        |rep, _frame| flush_stdout(rep),
    );
    flush_stdout(reporter.as_mut());
    Ok(stats)
}

/// Wrap the styled reporter in a [`RecordingReporter`] when recording is active,
/// so the run loop records every event before it's rendered. Without a recorder
/// the styled reporter is used directly. The returned box owns the recorder, so
/// dropping it (at the end of the render path) finalizes the event log.
fn wrap_reporter(
    styled: StyledReporter,
    recorder: Option<record::RunRecorder>,
) -> Box<dyn events::RunReporter> {
    match recorder {
        Some(rec) => Box::new(record::RecordingReporter::new(Box::new(styled), rec)),
        None => Box::new(styled),
    }
}

/// Build the recording sink for a run, keyed by workspace and `run_id` under the
/// per-user cache dir. Returns `None` (with a `tracing` warning) if the cache dir
/// or log can't be created — recording is best-effort.
fn build_recorder(run_id: &str, captured: bool) -> Option<record::RunRecorder> {
    let workspace = record::locate::current_workspace().ok()?;
    let dir = record::locate::run_dir(&workspace, run_id).ok()?;
    let meta = record::RunMeta {
        format_version: record::FORMAT_VERSION,
        run_id: run_id.to_string(),
        started_at: chrono::Utc::now().to_rfc3339(),
        args: std::env::args().collect(),
        captured,
    };
    let recorder = match record::RunRecorder::create(&dir, &meta) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("ztest: recording disabled: {e}");
            return None;
        }
    };
    // Prune old recordings for this workspace best-effort, so the cache stays
    // bounded without a separate maintenance command.
    record::retention::gc(&workspace, record::retention::RetentionPolicy::default());
    Some(recorder)
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

/// Choose how tests execute: the local child-process
/// [`LocalExecutor`](local_runner::LocalExecutor) by default, or the pod-per-test
/// [`PodExecutor`](pod_runner::PodExecutor) when a runner image is present (from
/// preflight, or the `ZTEST_RUNNER_IMAGE` override). The `ZTEST_RUNNER_*` env
/// knobs (namespace, hostpath, SA, delivery) are a kind demo shim until a cluster
/// profile drives them.
fn select_executor(
    work_rt: &tokio::runtime::Runtime,
    input: &EngineInput<'_>,
    env: EngineEnv,
) -> Result<std::sync::Arc<dyn local_runner::Executor>, String> {
    // Preflight image (remote runs) takes precedence; the env var is a manual
    // override. Neither → local.
    let from_preflight = input.runner_image.clone();
    let image = match from_preflight.clone().or_else(|| {
        std::env::var("ZTEST_RUNNER_IMAGE")
            .ok()
            .filter(|s| !s.is_empty())
    }) {
        Some(img) => img,
        None => return Ok(std::sync::Arc::new(local_runner::LocalExecutor { env })),
    };

    // `ztest setup` provisions the `ztest` namespace + SA with the RBAC a
    // component-spawning in-pod test needs, so running the runner pod as that
    // identity requires no extra grants. The right default; overridable.
    let namespace =
        std::env::var("ZTEST_RUNNER_NAMESPACE").unwrap_or_else(|_| RUN_NAMESPACE.to_string());
    let service_account = Some(
        std::env::var("ZTEST_RUNNER_SA")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| RUN_SERVICE_ACCOUNT.to_string()),
    );

    let client = work_rt
        .block_on(crate::cluster::client())
        .map_err(|e| format!("pod executor: connect to cluster: {e}"))?;

    // A preflight image is baked (outputs inside it); otherwise honor the manual
    // delivery knob (`baked`, or `hostpath` mounting the workspace from the node).
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
