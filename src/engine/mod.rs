//! Native test-execution engine: 2D QoS [`Scheduler`] spawns each test as its own process,
//! packed into live cluster capacity (replaced the `cargo nextest run` subprocess).
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

/// During-run progress, tallied by the run loop (`console`) from relayed
/// per-test result lines. `total` = preflight test count, `0` = unknown
#[derive(Debug, Clone, Default)]
pub struct RunProgress {
    pub elapsed: std::time::Duration,
    pub passed: u32,
    pub failed: u32,
    pub total: u32,
}

impl RunProgress {
    pub fn done(&self) -> u32 {
        self.passed + self.failed
    }
}

use std::process::ExitCode;
use std::time::Duration;

use nextest_metadata::{NextestExitCode, TestListSummary};

use crate::cancel::Cancel;
use crate::engine::local_runner::EngineEnv;
use crate::engine::reporter::StyledReporter;
use crate::engine::schedule::{LoopConfig, PanelFrame, run_loop};
use crate::inventory::QosEntry;
use crate::naming::{RUN_NAMESPACE, RUN_SERVICE_ACCOUNT};
use crate::pipeline::SelectedBinary;
use crate::qos::Resources;
use crate::qos::schedule::QosPlan;

/// Live-terminal seam: engine ships state, the presentation layer paints it.
///
/// - Engine holds no `Console`/`Theme` (→ no core → ui edge)
/// - `None` = non-TTY path (verdicts straight to stdout)
pub trait RunView: Send + Sync {
    fn cancel(&self) -> Cancel;
    fn live_rows(&self) -> usize;
    fn flush_live(&self);
    fn scrollback(&self, text: String);
    fn tick(&self, frame: &PanelFrame, plan: &QosPlan, live: String);
}

/// Run-behavior options, parsed from `ztest run` flags
#[derive(Debug, Clone)]
pub struct EngineOpts {
    pub retries: u32,
    pub fail_fast: bool,
    pub slow_after: Option<Duration>,
    pub sa: String,
    pub no_cleanup: bool,
    pub run_id: String,
    pub output: crate::engine::output::OutputConfig,
}

/// Everything the engine needs, extracted from preflight outcomes by `cli::run`.
///
/// - `runner_image`: `Some` = pod-per-test from that image, `None` = local child process
/// - `image_refs` = `spec_key → pull ref`, reaching each pod as
///   [`IMAGE_REFS_ENV`](crate::backends::image::IMAGE_REFS_ENV) (in-pod test has no Dockerfile)
/// - `rerun_passed` = `(binary_id, test_name)` dropped from this work-list
#[derive(Debug)]
pub struct EngineInput<'a> {
    pub summary: &'a TestListSummary,
    pub selected_binaries: &'a [SelectedBinary],
    pub qos_by_binary: &'a [(String, Vec<QosEntry>)],
    pub ceiling: Resources,
    pub resource_deps: crate::engine::plan::ResourceDeps,
    pub resource_states:
        std::collections::HashMap<crate::resource::NodeId, crate::resource::NodeState>,
    pub runner_image: Option<String>,
    pub image_refs: std::collections::BTreeMap<String, String>,
    pub opts: EngineOpts,
    pub rerun_passed: std::collections::HashSet<(String, String)>,
    pub reservation: Option<std::sync::Arc<crate::qos::ledger::Reservation>>,
}

/// Run the engine to completion, mapped to a `NextestExitCode`. Runs on `work_rt` either way.
///
/// - [`Console`] (TTY) → scenes through the render thread (verdicts scroll, QoS panel pinned)
/// - None (CI / piped) → plain lines to stdout
pub fn run(
    work_rt: &tokio::runtime::Runtime,
    input: EngineInput<'_>,
    view: Option<&dyn RunView>,
    qos_plan: Option<QosPlan>,
) -> ExitCode {
    let mut items = plan::build_work_list(
        input.selected_binaries,
        input.qos_by_binary,
        input.opts.retries,
        &input.resource_deps,
    );

    // `--rerun`: drop what already passed → only not-passed (and new) tests run
    if !input.rerun_passed.is_empty() {
        let before = items.len();
        items.retain(|it| {
            !input.rerun_passed.contains(&(it.binary_id.clone(), it.test_name.clone()))
        });
        let skipped = before - items.len();
        let note = format!(
            "Rerunning {} test(s); {skipped} passed previously and were skipped\n",
            items.len()
        );
        match view {
            Some(v) => v.scrollback(note),
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
        ztest_log: std::env::var("ZTEST_LOG").ok().filter(|v| !v.trim().is_empty()),
        image_refs: input.image_refs.clone(),
    };
    let executor = match select_executor(work_rt, &input, env) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("ztest engine: {e}");
            return ExitCode::from(NextestExitCode::SETUP_ERROR as u8);
        }
    };
    let cap_rx = input.reservation.as_ref().map(|r| r.ceiling());
    let cfg = LoopConfig {
        fail_fast: input.opts.fail_fast,
        slow_after: input.opts.slow_after,
        sa: input.opts.sa.clone(),
        redraw: Duration::from_millis(33),
        run_id: input.opts.run_id.clone(),
        // Fired by the render thread on Ctrl-C; off a TTY there is none, so the process
        // dies on the default SIGINT disposition
        cancel: view.map(RunView::cancel).unwrap_or_else(Cancel::never),
        resources: input.resource_states,
        // `--no-capture` serializes (nextest's `test_threads = 1` coupling) → no interleaving
        max_inflight: input.opts.output.is_serial().then_some(1),
        cap_rx,
        reservation: input.reservation,
    };
    let ceiling = input.ceiling;
    let output = input.opts.output;

    // Capturing TTY run → diagnostics to a per-run file (`tail -f`-able), else stderr like
    // nextest. Path MUST be local + writable: the summary's `target_directory` is the builder
    // pod's `CARGO_TARGET_DIR=/cache/target` on the on-cluster path, absent on the laptop, so
    // the subscriber falls back to stderr and desyncs the pinned footer with `ztest::pod` events.
    let log_path =
        std::env::temp_dir().join("ztest-logs").join(format!("{}.log", input.opts.run_id));
    match view {
        Some(v) if output.captures() => {
            crate::observ::init(crate::observ::Sink::File(log_path.clone()));
            // Through the console, never `eprintln!` (a direct stderr write corrupts the
            // very panel this File sink exists to protect)
            v.scrollback(format!(
                "ztest: diagnostics → {} (filter via ZTEST_LOG)\n",
                log_path.display()
            ));
        }
        _ => crate::observ::init(crate::observ::Sink::Stderr),
    }

    // Recording sink, on by default (`ZTEST_NO_RECORD=1` opts out). Best effort: a setup
    // failure warns and disables rather than aborting (replay is auxiliary to running tests).
    let recorder = if record::recording_enabled() {
        build_recorder(&input.opts.run_id, output.captures())
    } else {
        None
    };

    // `--no-capture` streams live + serially; the pinned TTY panel cannot coexist → even on a
    // terminal it takes the plain inherited path
    let stats = match view {
        Some(v) if output.captures() => {
            run_tty(work_rt, items, ceiling, cfg, executor, v, qos_plan, output, recorder)
        }
        _ => run_inherited(work_rt, items, ceiling, cfg, executor, output, recorder),
    };

    let stats = match stats {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ztest engine: {e}");
            return ExitCode::from(NextestExitCode::SETUP_ERROR as u8);
        }
    };

    // Never a success: incomplete even if nothing had failed when cancel landed
    let cancelled = view.map(|v| v.cancel().is_cancelled()).unwrap_or(false);

    if stats.any_failed() || cancelled {
        ExitCode::from(NextestExitCode::TEST_RUN_FAILED as u8)
    } else if stats.skipped > 0 {
        // Unschedulable = a setup-level problem
        ExitCode::from(NextestExitCode::SETUP_ERROR as u8)
    } else {
        ExitCode::from(NextestExitCode::OK as u8)
    }
}

/// TTY path: verdicts into native scrollback, each tick shipping a fresh scene (live region +
/// QoS [`render_live_panel`]) to the render thread, which owns all painting
#[allow(clippy::too_many_arguments)]
fn run_tty(
    rt: &tokio::runtime::Runtime,
    items: Vec<plan::WorkItem>,
    ceiling: Resources,
    cfg: LoopConfig,
    executor: std::sync::Arc<dyn local_runner::Executor>,
    view: &dyn RunView,
    qos_plan: Option<QosPlan>,
    output: crate::engine::output::OutputConfig,
    recorder: Option<record::RunRecorder>,
) -> std::io::Result<events::RunStats> {
    let color = supports_color::on(supports_color::Stream::Stdout).is_some();
    let styled =
        StyledReporter::new(color, supports_unicode::on(supports_unicode::Stream::Stdout), output);
    let mut reporter = wrap_reporter(styled, recorder);
    let plan = qos_plan.unwrap_or_else(empty_plan);
    let live_rows = view.live_rows();

    // Commit the preflight/image grid before the live region switches from the child PTY to
    // engine-rendered lines
    view.flush_live();

    let stats = drive(items, ceiling, cfg, executor, rt, reporter.as_mut(), |rep, frame| {
        let bytes = rep.take_scrollback();
        if !bytes.is_empty() {
            view.scrollback(String::from_utf8_lossy(&bytes).into_owned());
        }
        // `avt` grid idle during the run → drive the live region explicitly via the scene
        let live = reporter::render_running(&frame.running, live_rows, color).join("\n");
        view.tick(frame, &plan, live);
    });

    // Leftover scroll-lines, incl. the final summary (emitted after the last tick)
    let leftover = reporter.take_scrollback();
    if !leftover.is_empty() {
        view.scrollback(String::from_utf8_lossy(&leftover).into_owned());
    }
    Ok(stats)
}

/// Non-TTY path: plain scroll-lines to stdout on the work runtime
fn run_inherited(
    rt: &tokio::runtime::Runtime,
    items: Vec<plan::WorkItem>,
    ceiling: Resources,
    cfg: LoopConfig,
    executor: std::sync::Arc<dyn local_runner::Executor>,
    output: crate::engine::output::OutputConfig,
    recorder: Option<record::RunRecorder>,
) -> std::io::Result<events::RunStats> {
    let styled =
        StyledReporter::new(false, supports_unicode::on(supports_unicode::Stream::Stdout), output);
    let mut reporter = wrap_reporter(styled, recorder);
    let stats = drive(items, ceiling, cfg, executor, rt, reporter.as_mut(), |rep, _frame| {
        flush_stdout(rep)
    });
    flush_stdout(reporter.as_mut());
    Ok(stats)
}

/// Wrap the styled reporter so every event is recorded before rendering. The box owns the
/// recorder → dropping it finalizes the event log
fn wrap_reporter(
    styled: StyledReporter,
    recorder: Option<record::RunRecorder>,
) -> Box<dyn events::RunReporter> {
    match recorder {
        Some(rec) => Box::new(record::RecordingReporter::new(Box::new(styled), rec)),
        None => Box::new(styled),
    }
}

/// Recording sink for a run, keyed by workspace + `run_id` under the per-user cache dir.
/// Best-effort: an uncreatable dir or log yields `None` + a `tracing` warning, not an error
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
    // Best-effort prune → cache stays bounded without a maintenance command
    record::retention::gc(&workspace, record::retention::RetentionPolicy::default());
    Some(recorder)
}

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

/// [`LocalExecutor`](local_runner::LocalExecutor) child process by default,
/// [`PodExecutor`](pod_runner::PodExecutor) pod-per-test given a runner image (preflight or the
/// `ZTEST_RUNNER_IMAGE` override). The `ZTEST_RUNNER_*` knobs are a kind demo shim until a
/// cluster profile drives them.
fn select_executor(
    work_rt: &tokio::runtime::Runtime,
    input: &EngineInput<'_>,
    env: EngineEnv,
) -> Result<std::sync::Arc<dyn local_runner::Executor>, crate::error::PipelineError> {
    // Preflight image (remote runs) wins over the manual env override; neither → local
    let from_preflight = input.runner_image.clone();
    let image = match from_preflight
        .clone()
        .or_else(|| std::env::var("ZTEST_RUNNER_IMAGE").ok().filter(|s| !s.is_empty()))
    {
        Some(img) => img,
        None => return Ok(std::sync::Arc::new(local_runner::LocalExecutor { env })),
    };

    // `ztest cluster setup` provisions the `ztest` ns + SA with the RBAC a component-spawning
    // in-pod test needs → running as that identity needs no extra grants
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

    // Preflight image = baked (outputs inside it); else the manual delivery knob (`baked`, or
    // `hostpath` mounting the workspace from the node)
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
    Ok(std::sync::Arc::new(pod_runner::PodExecutor::new(client, cfg)))
}

/// Plan for runs with no `#[qos]` declarations (panel shows running/progress, no per-tier lines)
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

fn flush_stdout(rep: &mut dyn events::RunReporter) {
    use std::io::Write as _;
    let bytes = rep.take_scrollback();
    if !bytes.is_empty() {
        let _ = std::io::stdout().write_all(&bytes);
        let _ = std::io::stdout().flush();
    }
}
