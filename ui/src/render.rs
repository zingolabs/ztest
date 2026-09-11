use std::borrow::Cow;
use std::time::Duration;

use super::layout::*;
use super::theme::Theme;
use super::{
    ArchiveRow, ArchiveStatus, BannerState, BuildState, QosPlan, SyncVitals, SyncWatchState,
    TransferKind, TransferProgress, TransferRow, Transfers,
};
use crate::template::{Fields, Row, Template};
use ztest::api::BuildStage;
use ztest::api::LiveSnapshot;
use ztest::api::RunProgress;
use ztest::api::{column_width, format_elapsed, thousands};

pub fn render(state: &BannerState, theme: &Theme) -> String {
    let mut out = String::with_capacity(2048);

    render_top_rule(&mut out, theme);
    render_header_line(&mut out, state, theme);
    blank_line(&mut out);
    render_cluster_block(&mut out, state, theme);
    blank_line(&mut out);
    render_inventory_block(&mut out, state, theme);
    blank_line(&mut out);
    render_archive_block(&mut out, state, theme);
    if let Some(plan) = &state.qos_plan {
        blank_line(&mut out);
        render_qos_block(&mut out, plan, theme);
    }
    render_bottom_rule(&mut out, theme);

    out
}

// ─────────────────────────── row binder ───────────────────────────────

/// Star budget is `0`: no template here carries a `*` cell (every column is either fixed
/// or pre-measured by the caller)
fn draw(out: &mut String, f: Fields<'_>, src: &str, elapsed: Duration, theme: &Theme) {
    out.push_str(&Template::parse(src).render_str(&f, 0, elapsed, theme));
    out.push('\n');
}

/// Action-label column, padded here rather than by a template width: the column is a
/// minimum, and a fixed cell would clip the phase that names the row
fn label(text: &str) -> String {
    format!("{text:>width$}", width = LABEL_WIDTH)
}

fn side_label(text: &str) -> String {
    format!("{text:>width$}", width = METRIC_LABEL_WIDTH)
}

// ─────────────────────────── banner ───────────────────────────────────

/// Preflight banner row shapes
mod banner_row {
    pub(super) const HEADER: &str = "{label|pass} {name}";
    pub(super) const CLUSTER: &str = concat!(
        "{label|pass} context {context} {@dot|dim} {used|bold} / {total|bold} slots used",
        " {@dot|dim} configured {configured|bold} via --test-threads",
    );
    pub(super) const NODES: &str = "{label} {ready|bold} ready {@dot|dim} {cordoned|bold} cordoned";
    pub(super) const CAPACITY: &str =
        "{label} capacity {@dot|dim} {cpu|bold} {@dot|dim} {mem|bold}";
    pub(super) const INVENTORY_QUEUED: &str = "{label|dim} {state|dim}";
    pub(super) const INVENTORY_WORKING: &str =
        "{label|pass} {@spin|bold} {phase}{@ellipsis} {@dot|dim} {elapsed|bold}";
    pub(super) const INVENTORY_OK: &str = concat!(
        "{label|pass} {@ok|pass} {tests|bold} tests across {bins|bold} binaries",
        " {@dot|dim} {elapsed|bold}",
    );
    pub(super) const INVENTORY_FAILED: &str =
        "{label|fail} {@warn|fail} {stage} failed (exit {code}) {@dot|dim} {elapsed|bold}";
    pub(super) const ARCHIVES: &str = "{label|pass} {count|bold} selected";
    pub(super) const ARCHIVE_CACHED: &str =
        "{label} {@ok|pass} {name} {@dot|dim} {state|pass} {@dot|dim} {size|bytes.bold}";
    pub(super) const ARCHIVE_MISSING: &str =
        "{label} {@warn|skip} {name} {@dot|dim} missing {@dot|dim} {detail|dim}";
    pub(super) const SCHEDULING: &str =
        "{label|pass} {tests|bold} tests {@dot|dim} {total|bold} reserved total";
    pub(super) const SCHEDULING_BLIND: &str = concat!(
        "{label|pass} {tests|bold} tests {@dot|dim} {total|bold} reserved total",
        " {@dot|dim} capacity unknown (probe unavailable)",
    );
    pub(super) const UNSCHEDULABLE: &str = concat!(
        "{label} {@warn|skip} {test|skip} needs {admitted} {@dot|dim}",
        " exceeds cluster capacity — will be rejected",
    );
}

fn render_header_line(out: &mut String, _state: &BannerState, theme: &Theme) {
    let f = Fields::new().text("label", label("Preflight")).text("name", "ztest");
    draw(out, f, banner_row::HEADER, Duration::ZERO, theme);
}

fn render_cluster_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let c = &state.cluster;

    let f = Fields::new()
        .text("label", label("Cluster"))
        .text("context", c.context.as_str())
        .text("used", c.slots_used.to_string())
        .text("total", c.slots_total.to_string())
        .text("configured", c.slots_configured.to_string());
    draw(out, f, banner_row::CLUSTER, Duration::ZERO, theme);

    let f = Fields::new()
        .text("label", label(""))
        .text("ready", c.nodes_ready.to_string())
        .text("cordoned", c.nodes_cordoned.to_string());
    draw(out, f, banner_row::NODES, Duration::ZERO, theme);

    // One global figure: requested of allocatable, per dimension
    let (cpu, mem) = used_of(&c.capacity.reserved, &c.capacity.allocatable);
    let f = Fields::new().text("label", label("")).text("cpu", cpu).text("mem", mem);
    draw(out, f, banner_row::CAPACITY, Duration::ZERO, theme);
}

fn render_inventory_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let f = Fields::new().text("label", label("Inventory"));
    match &state.build {
        BuildState::Pending => draw(
            out,
            f.text("state", "queued"),
            banner_row::INVENTORY_QUEUED,
            Duration::ZERO,
            theme,
        ),
        BuildState::Compiling { started_at, phase } => draw(
            out,
            f.text("phase", phase.as_deref().unwrap_or("compiling test binaries"))
                .text("elapsed", format_elapsed(started_at.elapsed())),
            banner_row::INVENTORY_WORKING,
            started_at.elapsed(),
            theme,
        ),
        BuildState::Indexing { started_at } => draw(
            out,
            f.text("phase", "indexing test selection")
                .text("elapsed", format_elapsed(started_at.elapsed())),
            banner_row::INVENTORY_WORKING,
            started_at.elapsed(),
            theme,
        ),
        BuildState::Ok { test_count, binary_count, elapsed } => draw(
            out,
            f.text("tests", test_count.to_string())
                .text("bins", binary_count.to_string())
                .text("elapsed", format_elapsed(*elapsed)),
            banner_row::INVENTORY_OK,
            Duration::ZERO,
            theme,
        ),
        BuildState::Failed { exit_code, stage, elapsed } => draw(
            out,
            f.text("stage", stage_label(*stage))
                .text("code", exit_code.to_string())
                .text("elapsed", format_elapsed(*elapsed)),
            banner_row::INVENTORY_FAILED,
            Duration::ZERO,
            theme,
        ),
    }
}

fn stage_label(stage: BuildStage) -> &'static str {
    match stage {
        BuildStage::Compile => "compile",
        BuildStage::Index => "index",
    }
}

fn render_archive_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let archives = &state.archives;
    let f =
        Fields::new().text("label", label("Archives")).text("count", archives.len().to_string());
    draw(out, f, banner_row::ARCHIVES, Duration::ZERO, theme);

    let name_col = column_width(archives.iter().map(|r| r.name.as_str()), 18, 28);
    for row in archives {
        write_archive_row(out, row, name_col, theme);
    }
}

/// `name_col` = caller-measured across sibling rows, so the padding is data rather than
/// a template width
fn write_archive_row(out: &mut String, row: &ArchiveRow, name_col: usize, theme: &Theme) {
    let f = Fields::new().text("label", label("")).text("name", pad(&row.name, name_col));
    match &row.status {
        ArchiveStatus::Cached { size_bytes } => draw(
            out,
            f.text("state", "cached").value("size", *size_bytes as f64),
            banner_row::ARCHIVE_CACHED,
            Duration::ZERO,
            theme,
        ),
        ArchiveStatus::Missing { detail } => draw(
            out,
            f.text("detail", detail.as_str()),
            banner_row::ARCHIVE_MISSING,
            Duration::ZERO,
            theme,
        ),
    }
}

fn render_qos_block(out: &mut String, plan: &QosPlan, theme: &Theme) {
    let header = Fields::new()
        .text("label", label("Scheduling"))
        .text("tests", plan.tests.to_string())
        .text("total", plan.total.to_string());
    match plan.free {
        Some(_) => draw(out, header, banner_row::SCHEDULING, Duration::ZERO, theme),
        None => draw(out, header, banner_row::SCHEDULING_BLIND, Duration::ZERO, theme),
    }

    // Fail-fast on a test admission will reject, named with its own reserve
    for u in &plan.unschedulable {
        let f = Fields::new()
            .text("label", label(""))
            .text("test", u.name.as_str())
            .text("admitted", u.admitted.to_string());
        draw(out, f, banner_row::UNSCHEDULABLE, Duration::ZERO, theme);
    }
}

// ─────────────────────────── pinned panels ────────────────────────────

/// Pinned-panel row shapes, shared where two panels draw the same row
mod panel_row {
    pub(super) const RUNNING: &str = concat!(
        "{label|pass} {@spin|bold} {running|bold} running {@dot|dim} {cpu|bold} {@dot|dim}",
        " {mem|bold}",
    );
    pub(super) const PROGRESS: &str = concat!(
        "{label} {done|bold}[/{total|bold}] done {@dot|dim} {passed|pass} passed",
        "[ {@dot|dim} {failed|count.fail} failed] {@dot|dim} {elapsed|dim}",
    );
    pub(super) const QUEUE: &str = "{label} {queued|bold} queued";
    pub(super) const CLUSTER: &str = concat!(
        "{label|pass} {@spin|bold} {context} {@dot|dim} {ready|bold} ready {@dot|dim}",
        " {used|bold}/{total|bold} slots",
    );
    pub(super) const CAPACITY: &str = "{label|dim} {cpu|bold} {@dot|dim} {mem|bold}";
    pub(super) const BUILD_QUEUED: &str = "{label|pass} {@dot|dim} queued";
    pub(super) const BUILD_WORKING: &str =
        "{label|pass} {@spin|bold} {phase}{@ellipsis} {@dot|dim} {elapsed}";
    pub(super) const BUILD_OK: &str =
        "{label|pass} {@ok|pass} {tests} tests / {bins} bins {@dot|dim} {elapsed}";
    pub(super) const BUILD_FAILED: &str = "{label|pass} {@warn|fail} build failed (exit {code})";
    pub(super) const SCHEDULING: &str =
        "{label|pass} {tests|bold} tests {@dot|dim} {total|bold} reserved";
    pub(super) const SCHEDULING_BLIND: &str =
        "{label|pass} {tests|bold} tests {@dot|dim} capacity unknown";
    pub(super) const SUBJECT: &str =
        "{label|pass} {@spin|bold} {profile|bold} {@dot|dim} {sync_id|dim}";
    pub(super) const CONTEXT: &str = "{label|dim} {context}";
    pub(super) const CANCEL: &str =
        "{label|skip} {@spin|skip} terminating subprocesses{@ellipsis} {@dot|dim} {hint|dim}";
}

/// Left column during the run: [`render_preflight_panel`]'s counterpart, same
/// [`PANEL_LINES`] height
pub fn render_live_panel(snapshot: &LiveSnapshot, progress: &RunProgress, theme: &Theme) -> String {
    let mut out = String::with_capacity(320);

    render_label_rule(&mut out, theme);

    let (cpu, mem) = used_of(&snapshot.committed, &snapshot.limit);
    let running = Fields::new()
        .text("label", label("Running"))
        .text("running", snapshot.running.to_string())
        .text("cpu", cpu)
        .text("mem", mem);
    draw(&mut out, running, panel_row::RUNNING, progress.elapsed, theme);

    // Bare `done` when total unknown; `failed` only once one has
    let mut progress_row = Fields::new()
        .text("label", label(""))
        .text("done", progress.done().to_string())
        .text("passed", progress.passed.to_string())
        .text("elapsed", format_elapsed(progress.elapsed));
    if progress.total > 0 {
        progress_row = progress_row.text("total", progress.total.to_string());
    }
    if progress.failed > 0 {
        progress_row = progress_row.value("failed", f64::from(progress.failed));
    }
    draw(&mut out, progress_row, panel_row::PROGRESS, Duration::ZERO, theme);

    let f = Fields::new().text("label", label("")).text("queued", snapshot.queued.to_string());
    draw(&mut out, f, panel_row::QUEUE, Duration::ZERO, theme);

    pad_to_panel(&mut out);
    out
}

/// Left column during preflight/build/image; [`render_live_panel`]'s counterpart
/// at the same [`PANEL_LINES`] height (panel never reflows between phases).
/// `phase` = action label, `elapsed` drives the spinner
pub fn render_preflight_panel(
    state: &BannerState,
    phase: &str,
    elapsed: std::time::Duration,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(320);
    let c = &state.cluster;

    render_label_rule(&mut out, theme);

    let f = Fields::new()
        .text("label", label(phase))
        .text("context", c.context.as_str())
        .text("ready", c.nodes_ready.to_string())
        .text("used", c.slots_used.to_string())
        .text("total", c.slots_total.to_string());
    draw(&mut out, f, panel_row::CLUSTER, elapsed, theme);

    let (cpu, mem) = used_of(&c.capacity.reserved, &c.capacity.allocatable);
    let f = Fields::new().text("label", label("capacity")).text("cpu", cpu).text("mem", mem);
    draw(&mut out, f, panel_row::CAPACITY, Duration::ZERO, theme);

    render_build_line(&mut out, &state.build, elapsed, theme);

    // Scheduling row absent without a QoS plan
    if let Some(plan) = &state.qos_plan {
        let f =
            Fields::new().text("label", label("Scheduling")).text("tests", plan.tests.to_string());
        match plan.free {
            Some(_) => draw(
                &mut out,
                f.text("total", plan.total.to_string()),
                panel_row::SCHEDULING,
                Duration::ZERO,
                theme,
            ),
            None => draw(&mut out, f, panel_row::SCHEDULING_BLIND, Duration::ZERO, theme),
        }
    }

    pad_to_panel(&mut out);
    out
}

/// Shared `Inventory` line: `ztest sync start`'s panel and the run banner cannot
/// disagree about build state. `elapsed` drives the spinner
fn render_build_line(
    out: &mut String,
    build: &BuildState,
    elapsed: std::time::Duration,
    theme: &Theme,
) {
    let f = Fields::new().text("label", label("Inventory"));
    match build {
        BuildState::Pending => draw(out, f, panel_row::BUILD_QUEUED, elapsed, theme),
        BuildState::Compiling { started_at, phase } => draw(
            out,
            f.text("phase", phase.as_deref().unwrap_or("compiling test binaries"))
                .text("elapsed", format_elapsed(started_at.elapsed())),
            panel_row::BUILD_WORKING,
            elapsed,
            theme,
        ),
        BuildState::Indexing { started_at } => draw(
            out,
            f.text("phase", "indexing test selection")
                .text("elapsed", format_elapsed(started_at.elapsed())),
            panel_row::BUILD_WORKING,
            elapsed,
            theme,
        ),
        BuildState::Ok { test_count, binary_count, elapsed: took } => draw(
            out,
            f.text("tests", test_count.to_string())
                .text("bins", binary_count.to_string())
                .text("elapsed", format_elapsed(*took)),
            panel_row::BUILD_OK,
            elapsed,
            theme,
        ),
        BuildState::Failed { exit_code, .. } => draw(
            out,
            f.text("code", exit_code.to_string()),
            panel_row::BUILD_FAILED,
            elapsed,
            theme,
        ),
    }
}

/// Left column during `ztest sync start`'s build+provision: [`render_preflight_panel`]'s
/// frame, the detached sync's context in place of the run's probe/scheduling rows
/// (no equivalent for a detached sync). [`PANEL_LINES`] like every panel
pub fn render_sync_build_panel(
    profile: &str,
    sync_id: &str,
    context: &str,
    build: &BuildState,
    phase: &str,
    elapsed: std::time::Duration,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(320);

    render_label_rule(&mut out, theme);

    let f =
        Fields::new().text("label", label(phase)).text("profile", profile).text("sync_id", sync_id);
    draw(&mut out, f, panel_row::SUBJECT, elapsed, theme);

    let f = Fields::new().text("label", label("cluster")).text("context", context);
    draw(&mut out, f, panel_row::CONTEXT, Duration::ZERO, theme);

    render_build_line(&mut out, build, elapsed, theme);

    pad_to_panel(&mut out);
    out
}

// ─────────────────────────── transfers ────────────────────────────────

/// Left column while `ztest sync watch` follows a detached sync: live vitals, or before the first
/// commit the driver pod's phase explaining the silence. Same frame as the build that launched it
pub fn render_sync_watch_panel(state: &SyncWatchState, elapsed: Duration, theme: &Theme) -> String {
    let mut out = String::with_capacity(384);

    render_label_rule(&mut out, theme);

    let f = Fields::new()
        .text("label", label("Watching"))
        .text("profile", state.profile.as_str())
        .text("sync_id", state.sync_id.as_str());
    draw(&mut out, f, panel_row::SUBJECT, elapsed, theme);

    match &state.vitals {
        Some(v) => render_sync_vitals(&mut out, v, elapsed, theme),
        None => render_sync_waiting(&mut out, state, elapsed, theme),
    }

    pad_to_panel(&mut out);
    out
}

/// Metric row shapes. First three sit in the watch panel's [`LABEL_WIDTH`] column, the rest in
/// the narrower [`METRIC_LABEL_WIDTH`] side columns
mod metric_row {
    pub(super) const HEIGHT: &str =
        "{label|dim} {height|bold} / {target|bold} {@dot|dim} {pct|fraction.bold} {gauge:12#}";
    pub(super) const PACE: &str = concat!(
        "{label|dim} [{blk:.1} blk/s][{blk_na}] {@dot|dim} [{tx|per_sec.bold} tx/s][{tx_na|bold}]",
        "[ {@dot|dim} eta {eta}]",
    );
    pub(super) const TREND: &str =
        "{label|dim} {blocks:12~} {span|dim}[ {@dot|dim} peak {peak:.0|dim} blk/s]";
    pub(super) const DRIVER: &str = "{label|dim} {phase|bold} {@dot|dim} {age|dim}";
    pub(super) const NOTE: &str = "{label|dim} {note|dim}";
    pub(super) const POOL: &str = "{label|dim} [{rate:>8|per_sec.bold}][{rate_na:>8|bold}] {spark}";
    pub(super) const TOTAL: &str =
        "{label|dim} [{rate:>8|per_sec.bold}][{rate_na:>8|bold}] {span|dim}";
    // Absent limit != zero limit (a Burstable pod gets no denominator, never an invented one)
    pub(super) const LOAD: &str =
        "{label|dim} {cpu:.1|bold}c[/{cpu_limit:.0|bold}c] [{mem%|bold}][{mem|bytes.bold}]";
    pub(super) const SIDE_NOTE: &str = "{label|dim} {note|dim}";
    pub(super) const MORE: &str = "{label|dim} +{count|count.dim} more";
}

/// Believability = 3 scrape intervals (one missed scrape must not blink the panel). Past it rates
/// blank, never hold (a frozen rate drawn as healthy = the one unacceptable failure)
const STALE_AFTER: Duration =
    Duration::from_secs(ztest::api::metrics::SCRAPE_INTERVAL.as_secs() * 3);

fn stale(v: &SyncVitals, elapsed: Duration) -> bool {
    elapsed.saturating_sub(v.received_at) > STALE_AFTER
}

/// Measured → the `{key}` cell; unmeasured or stale → `{na}` = `—` (one statement: not known now)
fn rate<'a>(f: Fields<'a>, key: &'static str, na: &'static str, r: Option<f64>) -> Fields<'a> {
    match r {
        Some(r) => f.value(key, r),
        None => f.text(na, "—"),
    }
}

fn render_sync_vitals(out: &mut String, v: &SyncVitals, elapsed: Duration, theme: &Theme) {
    let pct = v.pct();
    let f = Fields::new()
        .text("label", label("height"))
        .text("height", thousands(u64::from(v.height)))
        .text("target", thousands(u64::from(v.target)))
        .value("pct", pct / 100.0)
        .percent("gauge", pct.clamp(0.0, 100.0) as u8);
    draw(out, f, metric_row::HEIGHT, Duration::ZERO, theme);

    // tx/s beside blk/s: one read, one staleness → never disagree about whether the subject moves
    let stale = stale(v, elapsed);
    let fresh = |r: Option<f64>| r.filter(|_| !stale);
    let mut pace = Fields::new().text("label", label("pace"));
    pace = rate(pace, "blk", "blk_na", fresh(v.pace.map(|p| p.per_sec)));
    pace = rate(pace, "tx", "tx_na", fresh(v.tx_rate));
    // Blanked with its rate (a countdown off a frozen rate counts to a finish that is not coming)
    if let Some(eta) = v.pace.and_then(|p| p.eta).filter(|_| !stale) {
        pace = pace.text("eta", format_elapsed(eta));
    }
    draw(out, pace, metric_row::PACE, Duration::ZERO, theme);

    render_scan_trend(out, v, theme);
}

/// Scan rate over the run + its best (a scan holding at half its demonstrated peak = a regression
/// nothing else on the panel states)
fn render_scan_trend(out: &mut String, v: &SyncVitals, theme: &Theme) {
    let Some(blocks) = v.blocks.as_ref().filter(|b| !b.points.is_empty()) else {
        let f = Fields::new().text("label", label("blocks")).text("note", "gathering");
        draw(out, f, metric_row::NOTE, Duration::ZERO, theme);
        return;
    };
    let trend = Fields::new()
        .text("label", label("blocks"))
        .bands("blocks", super::report::bands(blocks))
        .text("span", format_elapsed(v.span))
        .maybe_value("peak", blocks.peak());
    draw(out, trend, metric_row::TREND, Duration::ZERO, theme);
}

/// Pre-first-commit rows. Provisioning + image pulls = most of a sync's early wall-clock → the only
/// progress display for minutes
fn render_sync_waiting(out: &mut String, state: &SyncWatchState, elapsed: Duration, theme: &Theme) {
    let f = Fields::new().text("label", label("cluster")).text("context", state.context.as_str());
    draw(out, f, panel_row::CONTEXT, Duration::ZERO, theme);

    let f = Fields::new()
        .text("label", label("driver"))
        .text("phase", state.pod_phase.as_str())
        .text("age", format_elapsed(elapsed));
    draw(out, f, metric_row::DRIVER, Duration::ZERO, theme);

    let f = Fields::new()
        .text("label", label("metrics"))
        .text("note", state.metrics_note.as_deref().unwrap_or("awaiting first read"));
    draw(out, f, metric_row::NOTE, Duration::ZERO, theme);
}

/// Middle column of `ztest sync watch`: total, then one row per measured pool with its sparkline.
///
/// - Total heads the column (four pools + a total fill every row the panel has)
/// - Pool never published = no series = no row (an empty sparkline claims idle)
pub fn render_sync_work(state: &SyncWatchState, elapsed: Duration, theme: &Theme) -> String {
    use super::plot::{Palette, PlotOpts, plot_stacked};

    let mut out = String::with_capacity(320);
    let Some(vitals) = state.vitals.as_ref() else {
        out.push('\n');
        let f = Fields::new().text("label", side_label("work")).text("note", "awaiting first read");
        draw(&mut out, f, metric_row::SIDE_NOTE, Duration::ZERO, theme);
        pad_to_panel(&mut out);
        return out;
    };
    let stale = stale(vitals, elapsed);
    let fresh = |r: Option<f64>| r.filter(|_| !stale);

    // Carries the span (else ten minutes and two days of history look alike)
    let f =
        Fields::new().text("label", side_label("total")).text("span", format_elapsed(vitals.span));
    draw(
        &mut out,
        rate(f, "rate", "rate_na", fresh(vitals.work_rate())),
        metric_row::TOTAL,
        Duration::ZERO,
        theme,
    );

    let palette = Palette::pools(theme.is_colorized());
    let opts = PlotOpts::new(SPARK_WIDTH, 1, theme.chars.graph);
    for pool in vitals.pools.iter().take(MAX_TRANSFER_ROWS) {
        // Sparkline drawn here, not via `{key:N~}` (palette keys on the pool; a template key is literal)
        let (name, tag) = pool
            .channel
            .map_or((pool.label.as_str(), pool.label.as_str()), |c| (c.name(), c.tag()));
        let spark = plot_stacked(&[(name, super::report::bands(pool))], &opts, &palette)
            .pop()
            .unwrap_or_default();
        let f = Fields::new().text("label", side_label(tag)).text("spark", spark);
        draw(
            &mut out,
            rate(f, "rate", "rate_na", fresh(pool.last())),
            metric_row::POOL,
            Duration::ZERO,
            theme,
        );
    }
    if vitals.pools.is_empty() {
        let f = Fields::new().text("label", side_label("work")).text("note", "no pool measured");
        draw(&mut out, f, metric_row::SIDE_NOTE, Duration::ZERO, theme);
    }

    pad_to_panel(&mut out);
    out
}

/// Right column of `ztest sync watch`: each container's newest cpu + memory against its limit.
///
/// - Kubelet's reading off the same TSDB read (no exporter sees its own cgroup)
/// - Empty column names its cause (blank rows read as idle containers)
pub fn render_sync_load(state: &SyncWatchState, theme: &Theme) -> String {
    let mut out = String::with_capacity(320);
    out.push('\n');

    if state.loads.is_empty() {
        let f = Fields::new()
            .text("label", side_label("load"))
            .text("note", state.loads_note.as_deref().unwrap_or("awaiting first sample"));
        draw(&mut out, f, metric_row::SIDE_NOTE, Duration::ZERO, theme);
        pad_to_panel(&mut out);
        return out;
    }

    // Blank top row aligns with the left column's rule; a longer topology collapses its tail
    let budget = PANEL_LINES - 1;
    let (shown, hidden) = match state.loads.len() > budget {
        true => (&state.loads[..budget - 1], state.loads.len() - (budget - 1)),
        false => (&state.loads[..], 0),
    };
    for load in shown {
        let limit = load.limit.as_ref();
        let mut f = Fields::new()
            .text("label", side_label(&clip_name(&load.container)))
            .value("cpu", load.usage.cpu_milli as f64 / 1000.0)
            .maybe_value(
                "cpu_limit",
                limit.map(|l| l.cpu_milli).filter(|&c| c > 0).map(|c| c as f64 / 1000.0),
            );
        // Pair when a denominator exists (`10.0/24.0 GiB` shares one magnitude); bare otherwise
        f = match limit.map(|l| l.mem_bytes).filter(|&m| m > 0) {
            Some(m) => f.pair("mem", load.usage.mem_bytes, m),
            None => f.value("mem", load.usage.mem_bytes as f64),
        };
        draw(&mut out, f, metric_row::LOAD, Duration::ZERO, theme);
    }
    if hidden > 0 {
        let f = Fields::new().text("label", side_label("")).value("count", hidden as f64);
        draw(&mut out, f, metric_row::MORE, Duration::ZERO, theme);
    }

    pad_to_panel(&mut out);
    out
}

/// Long name loses its tail rather than push the numbers off the column
fn clip_name(name: &str) -> String {
    name.chars().take(METRIC_LABEL_WIDTH).collect()
}

/// Right column of the pinned console: live background acquisitions, independent
/// of the scrolling main output. [`PANEL_LINES`] = blank top row + up to
/// [`MAX_TRANSFER_ROWS`] rows, tail collapsing to `+N more`
pub fn render_transfers(
    transfers: &Transfers,
    elapsed: std::time::Duration,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(320);
    // Blank top row aligns the first transfer with the left column's cluster line
    out.push('\n');

    let rows = &transfers.rows;
    let show = rows.len().min(MAX_TRANSFER_ROWS);
    // Last slot reserved for `+N more` on overflow
    let (visible, overflow) = if rows.len() > MAX_TRANSFER_ROWS {
        (MAX_TRANSFER_ROWS - 1, rows.len() - (MAX_TRANSFER_ROWS - 1))
    } else {
        (show, 0)
    };

    let name_col = column_width(rows.iter().take(visible).map(|r| r.label.as_str()), 12, 18);
    for row in rows.iter().take(visible) {
        write_transfer_row(&mut out, row, name_col, elapsed, theme);
    }
    if overflow > 0 {
        let f = Fields::new().value("count", overflow as f64);
        draw(&mut out, f, transfer_row::OVERFLOW, Duration::ZERO, theme);
    }

    pad_to_panel(&mut out);
    out
}

/// One transfer row, standalone — no panel padding, no row cap, no trailing newline.
///
/// - [`render_transfers`] pads to `PANEL_ROWS` (paints into the console `run` owns)
/// - Cluster-free subcommands have no console + one row → repaint discipline is theirs
pub fn render_transfer_line(
    row: &TransferRow,
    elapsed: std::time::Duration,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(160);
    let name_col = row.label.chars().count();
    write_transfer_row(&mut out, row, name_col, elapsed, theme);
    out.truncate(out.trim_end_matches('\n').len());
    out
}

/// Row shapes. Three, because the variants differ in what they *are*, not in width:
/// a stage has no bar, a failure has no spinner
mod transfer_row {
    pub(super) const STAGE: &str =
        "{glyph|dim}{@spin|bold} {label:<*} {@dot|dim} {note|bold}{@ellipsis|dim}";
    pub(super) const BYTES: &str = concat!(
        "{glyph|dim}{@spin|bold} {label:<*} {@dot|dim} {pct:12#} {pct|count}% {@dot|dim} {done%|bold}",
        "[ {@dot|dim} {rate|bytes_per_sec.bold}]",
        "[ {@dot|dim} {eta|dim} left]",
    );
    pub(super) const FAILED: &str = "{glyph|dim}{@warn|fail} {label:<*} {@dot|dim} {detail|dim}";
    pub(super) const OVERFLOW: &str = "+{count|count.dim} more transferring";
}

/// [`TransferRow`] + the theme it draws under, bound to a template.
///
/// Theme rides along because the glyphs are theme-chosen (ASCII fallback), which a bare
/// view-model cannot answer
struct TransferData<'a> {
    row: &'a TransferRow,
    theme: &'a Theme,
}

impl Row for TransferData<'_> {
    fn text(&self, key: &str) -> Option<Cow<'_, str>> {
        match key {
            "glyph" => Some(Cow::Borrowed(transfer_glyph(self.row.kind, self.theme))),
            "label" => Some(Cow::Borrowed(self.row.label.as_str())),
            "note" => match &self.row.progress {
                TransferProgress::Stage(n) => Some(Cow::Borrowed(n.as_str())),
                _ => None,
            },
            "detail" => match &self.row.progress {
                TransferProgress::Failed { detail } => Some(Cow::Borrowed(detail.as_str())),
                _ => None,
            },
            "eta" => match &self.row.progress {
                TransferProgress::Bytes { pace: Some(p), .. } => {
                    p.eta.map(|e| Cow::Owned(format_elapsed(e)))
                }
                _ => None,
            },
            // Markers resolve as `{@name}` cells; this row answers data only
            _ => None,
        }
    }

    fn value(&self, key: &str) -> Option<f64> {
        match (key, &self.row.progress) {
            ("pct", TransferProgress::Bytes { done, total, .. }) => {
                Some(f64::from(percent_of(*done, *total)))
            }
            ("rate", TransferProgress::Bytes { pace: Some(p), .. }) => Some(p.per_sec),
            _ => None,
        }
    }

    fn pair(&self, key: &str) -> Option<(u64, u64)> {
        match (key, &self.row.progress) {
            ("done", TransferProgress::Bytes { done, total, .. }) => Some((*done, *total)),
            _ => None,
        }
    }

    fn percent(&self, key: &str) -> Option<u8> {
        match (key, &self.row.progress) {
            ("pct", TransferProgress::Bytes { done, total, .. }) => Some(percent_of(*done, *total)),
            _ => None,
        }
    }
}

/// Saturating, so a zero-length transfer reads 0% rather than dividing by it
fn percent_of(done: u64, total: u64) -> u8 {
    match total {
        0 => 0,
        t => ((done as u128 * 100) / t as u128).min(100) as u8,
    }
}

/// One transfer line: marker, label, then a `%` bar (bytes known) or the note
fn write_transfer_row(
    out: &mut String,
    row: &TransferRow,
    name_col: usize,
    elapsed: std::time::Duration,
    theme: &Theme,
) {
    let src = match &row.progress {
        TransferProgress::Stage(_) => transfer_row::STAGE,
        TransferProgress::Bytes { .. } => transfer_row::BYTES,
        TransferProgress::Failed { .. } => transfer_row::FAILED,
    };
    // `name_col` is the label column the caller measured across sibling rows; the rest of
    // the row is fixed, so handing it as the star budget reproduces the old padding
    let data = TransferData { row, theme };
    let line = Template::parse(src);
    out.push_str(&line.render_str(&data, name_col, elapsed, theme));
    out.push('\n');
}

fn transfer_glyph(kind: TransferKind, theme: &Theme) -> &'static str {
    match kind {
        TransferKind::Image | TransferKind::Upload => theme.chars.up,
        TransferKind::Download | TransferKind::Seed => theme.chars.progress,
    }
}

/// Pinned panel while a Ctrl-C is honoured. Stands alone — the console's render
/// thread has no [`BannerState`]
pub fn render_cancel_panel(elapsed: std::time::Duration, theme: &Theme) -> String {
    let mut out = String::with_capacity(128);
    render_label_rule(&mut out, theme);
    let f =
        Fields::new().text("label", label("Cancelling")).text("hint", "Ctrl-C again to force quit");
    draw(&mut out, f, panel_row::CANCEL, elapsed, theme);
    out
}

// ─────────────────────────── tests ────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use ztest::api::GIB;
    use ztest::api::QosClass;
    use ztest::api::Resources;

    // ─────────────────────── sync watch panel ─────────────────────────

    fn watching(vitals: Option<SyncVitals>) -> SyncWatchState {
        SyncWatchState {
            profile: "zaino_state_sync".into(),
            sync_id: "zaino-state-sync-a52f9ec9".into(),
            context: "zingo-infra".into(),
            pod_phase: "Running".into(),
            vitals,
            ..SyncWatchState::default()
        }
    }

    fn per_minute(
        label: &str,
        channel: Option<ztest::api::Channel>,
        values: &[f64],
    ) -> ztest::api::Series {
        ztest::api::Series {
            reading: None,
            label: label.into(),
            unit: ztest::api::Unit::PerSec,
            facet: None,
            channel,
            points: values.iter().enumerate().map(|(i, v)| (i as f64 * 60.0, *v)).collect(),
            total: None,
            coverage: None,
        }
    }

    /// Session-elapsed frame these render at: 2s after [`sample_vitals`]'s read = fresh
    const FRAME: Duration = Duration::from_secs(212);

    const POOLS: [ztest::api::Channel; 4] = [
        ztest::api::Channel::Transparent,
        ztest::api::Channel::Sapling,
        ztest::api::Channel::Orchard,
        ztest::api::Channel::Ironwood,
    ];

    fn sample_vitals() -> SyncVitals {
        SyncVitals {
            height: 901,
            target: 1024,
            pace: Some(ztest::api::Pace { per_sec: 12.4, eta: Some(Duration::from_secs(10)) }),
            tx_rate: Some(48.0),
            blocks: Some(per_minute("blocks", None, &[400.0, 440.0, 480.0, 520.0])),
            // Ironwood counted & idle: a row at zero, never dropped
            pools: POOLS
                .iter()
                .zip([900.0, 19_400.0, 4_200.0, 0.0])
                .map(|(c, r)| per_minute(c.name(), Some(*c), &[r, r]))
                .collect(),
            span: Duration::from_secs(3 * 3600),
            received_at: Duration::from_secs(210),
        }
    }

    fn load(container: &str, cpu_milli: u64, mem_gib: u64) -> ContainerLoad {
        ContainerLoad {
            container: container.to_string(),
            usage: Resources::new(cpu_milli, mem_gib * GIB, 0, 0),
            limit: Some(Resources::new(9_000, 24 * GIB, 0, 0)),
        }
    }

    /// A column whose line count differs from its neighbours' shears the pinned block
    #[test]
    fn every_watch_column_is_panel_height_in_every_state() {
        let theme = plain_unicode_theme();
        let mut loaded = watching(Some(sample_vitals()));
        loaded.loads = (0..8).map(|i| load(&format!("c{i}"), 100, 1)).collect();
        for state in [watching(None), watching(Some(sample_vitals())), loaded] {
            for (column, out) in [
                ("left", render_sync_watch_panel(&state, FRAME, &theme)),
                ("work", render_sync_work(&state, FRAME, &theme)),
                ("load", render_sync_load(&state, &theme)),
            ] {
                assert_eq!(out.lines().count(), PANEL_LINES, "{column} column:\n{out}");
            }
        }
    }

    #[test]
    fn the_left_panel_shows_height_pace_and_trend() {
        let out = render_sync_watch_panel(
            &watching(Some(sample_vitals())),
            FRAME,
            &plain_unicode_theme(),
        );
        for expected in ["901", "1,024", "12.4 blk/s", "eta", "peak 520"] {
            assert!(out.contains(expected), "`{expected}` missing:\n{out}");
        }
    }

    /// Before the first commit the panel explains the wait (driver phase + why no vitals)
    #[test]
    fn the_left_panel_waiting_names_the_driver_phase() {
        let mut state = watching(None);
        state.metrics_note = Some("no height scraped yet".into());
        let out = render_sync_watch_panel(&state, FRAME, &plain_unicode_theme());
        assert!(out.contains("Running"), "driver phase:\n{out}");
        assert!(out.contains("no height scraped yet"), "cause:\n{out}");
    }

    #[test]
    fn a_stale_read_blanks_every_rate_and_the_countdown() {
        let theme = plain_unicode_theme();
        let late = FRAME + Duration::from_secs(600);
        let state = watching(Some(sample_vitals()));
        let left = render_sync_watch_panel(&state, late, &theme);
        assert!(left.contains('—') && !left.contains("eta"), "stale pace must blank:\n{left}");
        let work = render_sync_work(&state, late, &theme);
        assert!(work.contains('—'), "stale pool rates must blank:\n{work}");
    }

    #[test]
    fn the_work_column_heads_every_measured_pool_with_the_total() {
        let out = render_sync_work(&watching(Some(sample_vitals())), FRAME, &plain_unicode_theme());
        assert!(out.lines().next().is_some_and(|l| l.contains("total")), "total first:\n{out}");
        for pool in POOLS {
            assert!(out.contains(pool.tag()), "`{}` row missing:\n{out}", pool.name());
        }
    }

    #[test]
    fn the_work_column_names_why_it_is_empty() {
        let out = render_sync_work(&watching(None), FRAME, &plain_unicode_theme());
        assert!(out.contains("awaiting first read"), "{out}");
    }

    #[test]
    fn the_load_column_names_why_it_is_empty() {
        let theme = plain_unicode_theme();
        let mut state = watching(Some(sample_vitals()));
        assert!(render_sync_load(&state, &theme).contains("awaiting first sample"));
        state.loads_note = Some("prometheus unreadable".into());
        assert!(render_sync_load(&state, &theme).contains("prometheus unreadable"));
    }

    #[test]
    fn the_load_column_shows_usage_against_each_containers_limit() {
        let mut state = watching(Some(sample_vitals()));
        state.loads = vec![load("zainod", 593, 10), load("zebrad", 6, 1)];
        let s = render_sync_load(&state, &plain_unicode_theme());
        assert!(s.contains("zainod") && s.contains("zebrad"), "names each container:\n{s}");
        assert!(s.contains("0.6c/9c"), "cpu against its limit:\n{s}");
        // Byte pair shares one magnitude → the denominator costs 6 columns, not 9
        assert!(s.contains("10.0/24.0 GiB"), "memory against its limit:\n{s}");
    }

    /// Burstable containers have no denominator; inventing one misreports headroom
    #[test]
    fn a_container_without_limits_shows_bare_usage() {
        let mut state = watching(Some(sample_vitals()));
        state.loads = vec![ContainerLoad { limit: None, ..load("zainod", 1_500, 2) }];
        let s = render_sync_load(&state, &plain_unicode_theme());
        assert!(s.contains("1.5c"), "bare cpu:\n{s}");
        assert!(s.contains("2.0 GiB"), "bare memory:\n{s}");
        assert!(!s.contains('/'), "no invented denominator:\n{s}");
    }

    #[test]
    fn a_deep_topology_collapses_its_tail() {
        let mut state = watching(Some(sample_vitals()));
        state.loads = (0..8).map(|i| load(&format!("c{i}"), 100, 1)).collect();
        let s = render_sync_load(&state, &plain_unicode_theme());
        assert!(s.contains("+5 more"), "tail collapses:\n{s}");
    }

    /// `(tag, count)` → one `PlannedTest` per test, each at its tag's default reserve
    fn planned(sets: &[(QosClass, u32)]) -> Vec<ztest::api::PlannedTest> {
        sets.iter()
            .flat_map(|&(class, n)| {
                (0..n).map(move |i| ztest::api::PlannedTest {
                    name: format!("{}_{i}", class.as_label()),
                    admitted: class.profile().admitted(),
                })
            })
            .collect()
    }

    /// One staged row + one byte row with a pace: between them they exercise every cell
    /// kind a transfers column can draw (spinner, bar, rate, eta, kind glyph)
    fn transfers_fixture() -> Transfers {
        Transfers {
            rows: vec![
                TransferRow {
                    label: "dev-zainod".to_string(),
                    kind: TransferKind::Image,
                    progress: TransferProgress::Stage("building".to_string()),
                },
                TransferRow {
                    label: "testnet-3.1m".to_string(),
                    kind: TransferKind::Download,
                    progress: TransferProgress::Bytes {
                        done: 17_900_000_000,
                        total: 28_000_000_000,
                        pace: Some(ztest::api::Pace {
                            per_sec: 94.0 * 1024.0 * 1024.0,
                            eta: Some(std::time::Duration::from_secs(102)),
                        }),
                    },
                },
            ],
        }
    }

    fn sample_state() -> BannerState {
        BannerState {
            cluster: ClusterState {
                context: "kind-zaino-local".to_string(),
                slots_used: 12,
                slots_total: 16,
                slots_configured: 6,
                nodes_ready: 3,
                nodes_cordoned: 0,
                capacity: ztest::api::ClusterCapacity {
                    allocatable: Resources::new(12_000, 48 * GIB, 0, 0),
                    reserved: Resources::new(6_000, 20 * GIB, 0, 0),
                },
            },
            build: BuildState::Ok {
                test_count: 47,
                binary_count: 8,
                elapsed: std::time::Duration::from_secs(18),
            },
            archives: vec![
                ArchiveRow {
                    name: "regtest-nu5-h128".to_string(),
                    status: ArchiveStatus::Cached { size_bytes: 432_013_312 },
                },
                ArchiveRow {
                    name: "testnet-2.6m".to_string(),
                    status: ArchiveStatus::Cached { size_bytes: 19_754_106_880 },
                },
                ArchiveRow {
                    name: "mainnet-snapshot-9.0".to_string(),
                    status: ArchiveStatus::Missing {
                        detail: "manifest committed, blob absent".to_string(),
                    },
                },
            ],
            qos_plan: None,
        }
    }

    #[test]
    fn preflight_panel_is_constant_height_and_summarizes_phase() {
        let mut state = sample_state();
        state.qos_plan = Some(ztest::api::qos_plan(
            &planned(&[(QosClass::Integration, 6)]),
            Some(Resources::new(12_000, 48 * GIB, 0, 0)),
        ));
        let s = render_preflight_panel(
            &state,
            "Preflight",
            std::time::Duration::from_secs(3),
            &plain_unicode_theme(),
        );
        // rule + cluster + capacity + inventory + scheduling = PANEL_LINES, no
        // bottom rule
        assert_eq!(s.lines().count(), PANEL_LINES, "fixed-height panel:\n{s}");
        assert!(!s.trim_end().ends_with("────────────"), "no bottom rule:\n{s}");
        assert!(s.contains("Preflight"), "phase label:\n{s}");
        assert!(s.contains("kind-zaino-local"), "cluster context:\n{s}");
        assert!(s.contains("capacity"), "capacity gauge:\n{s}");
        assert!(s.contains("47 tests / 8 bins"), "build summary:\n{s}");
        assert!(s.contains("6 tests") && s.contains("reserved"), "scheduling summary:\n{s}");
    }

    #[test]
    fn preflight_panel_is_constant_height_even_when_empty() {
        // Pre-probe/build must still be PANEL_LINES (viewport never reflows)
        let mut state = sample_state();
        state.build = BuildState::Pending;
        state.qos_plan = None;
        let s = render_preflight_panel(
            &state,
            "Preflight",
            std::time::Duration::ZERO,
            &plain_unicode_theme(),
        );
        assert_eq!(s.lines().count(), PANEL_LINES, "fixed-height panel:\n{s}");
    }

    #[test]
    fn transfers_column_is_constant_height_and_shows_rows() {
        let theme = plain_unicode_theme();
        // Idle still PANEL_LINES (blank rows reserve the space)
        let idle = render_transfers(&Transfers::default(), std::time::Duration::ZERO, &theme);
        assert_eq!(idle.lines().count(), PANEL_LINES, "idle height:\n{idle}");

        let transfers = transfers_fixture();
        let s = render_transfers(&transfers, std::time::Duration::from_secs(1), &theme);
        assert_eq!(s.lines().count(), PANEL_LINES, "active height:\n{s}");
        assert!(s.contains("dev-zainod"), "image row:\n{s}");
        assert!(s.contains("building"), "image note:\n{s}");
        assert!(s.contains("testnet-3.1m"), "download row:\n{s}");
        assert!(s.contains('%'), "byte bar percent:\n{s}");
        assert!(s.contains("94.0 MiB/s"), "transfer rate:\n{s}");
        assert!(s.contains("left"), "eta:\n{s}");
        // Upload vs download glyphs
        assert!(s.contains(theme.chars.up), "upload glyph:\n{s}");
        assert!(s.contains(theme.chars.progress), "download glyph:\n{s}");
    }

    #[test]
    fn transfers_column_collapses_overflow_tail() {
        let theme = plain_unicode_theme();
        let rows: Vec<TransferRow> = (0..8)
            .map(|i| TransferRow {
                label: format!("dev-img{i}"),
                kind: TransferKind::Image,
                progress: TransferProgress::Stage("building".to_string()),
            })
            .collect();
        let s = render_transfers(&Transfers { rows }, std::time::Duration::ZERO, &theme);
        assert_eq!(s.lines().count(), PANEL_LINES, "overflow height:\n{s}");
        assert!(s.contains("more transferring"), "overflow marker:\n{s}");
    }

    /// Regression: `render_transfers` pads one row to five (repainting caller then
    /// scrolls four lines per frame)
    #[test]
    fn a_standalone_transfer_line_is_one_line_and_unterminated() {
        let row = TransferRow {
            label: "archive.tar.zst".to_string(),
            kind: TransferKind::Upload,
            progress: TransferProgress::Bytes { done: 512, total: 1024, pace: None },
        };
        let line = render_transfer_line(&row, std::time::Duration::ZERO, &plain_unicode_theme());
        assert!(!line.contains('\n'), "must be one unterminated line:\n{line:?}");
        assert!(line.contains("50%"), "percent:\n{line}");
        assert!(line.contains("512 B / 1.0 KiB"), "byte pair:\n{line}");
    }

    #[test]
    fn an_upload_row_carries_the_up_glyph() {
        let theme = plain_unicode_theme();
        let row = |kind| TransferRow {
            label: "a".to_string(),
            kind,
            progress: TransferProgress::Stage("hashing".to_string()),
        };
        let up =
            render_transfer_line(&row(TransferKind::Upload), std::time::Duration::ZERO, &theme);
        let down =
            render_transfer_line(&row(TransferKind::Download), std::time::Duration::ZERO, &theme);
        assert!(up.contains(theme.chars.up), "upload glyph:\n{up}");
        assert_ne!(up, down, "upload and download must not render identically");
    }

    /// No colours + Unicode glyphs = `Theme::detect()` under UTF-8 + `NO_COLOR=1`
    /// (lets these snapshot byte-exact output)
    fn plain_unicode_theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn plain_ascii_theme() -> Theme {
        Theme::for_capabilities(false, false)
    }

    fn colorized_unicode_theme() -> Theme {
        Theme::for_capabilities(true, true)
    }

    #[test]
    fn plain_unicode_golden() {
        let s = render(&sample_state(), &plain_unicode_theme());
        let expected = "\
────────────
   Preflight ztest

     Cluster context kind-zaino-local · 12 / 16 slots used · configured 6 via --test-threads
             3 ready · 0 cordoned
             capacity · 6 / 12 cores · 20 / 48 GiB

   Inventory ✓ 47 tests across 8 binaries · 18s

    Archives 3 selected
             ✓ regtest-nu5-h128     · cached · 412.0 MiB
             ✓ testnet-2.6m         · cached · 18.4 GiB
             ! mainnet-snapshot-9.0 · missing · manifest committed, blob absent
────────────
";
        assert_eq!(s, expected, "golden mismatch.\n--- got ---\n{s}\n--- want ---\n{expected}");
    }

    /// Regression: this asserted three named glyphs were absent and passed while braille,
    /// box-drawing and arrows leaked past it. The gate now rejects *any* non-ascii
    /// character, so a new hardcoded glyph fails on the surface that introduced it
    #[test]
    fn ascii_fallback_strips_unicode_glyphs() {
        let s = render(&sample_state(), &plain_ascii_theme());
        crate::testing::assert_ascii_clean("render", &s);
        assert!(s.contains("------------"), "ascii hbar missing:\n{s}");
        assert!(s.contains("OK regtest-nu5-h128"), "ascii ok marker:\n{s}");
        assert!(s.contains("WARN mainnet-snapshot-9.0"), "ascii warn marker:\n{s}");
    }

    /// Every panel this module exports, in ascii, at a couple of frame times so a spinner
    /// cannot hide a Unicode frame behind an ascii one
    #[test]
    fn every_panel_falls_back_to_ascii() {
        use std::time::Duration;
        let t = plain_ascii_theme();
        let check = crate::testing::assert_ascii_clean;
        for ms in [0u64, 250, 700] {
            let at = Duration::from_millis(ms);
            check("render_cancel_panel", &render_cancel_panel(at, &t));
            check("render_transfers", &render_transfers(&transfers_fixture(), at, &t));
            check(
                "render_preflight_panel",
                &render_preflight_panel(&sample_state(), "Preflight", at, &t),
            );
        }
    }

    #[test]
    fn colorized_render_contains_ansi_escapes() {
        let s = render(&sample_state(), &colorized_unicode_theme());
        assert!(s.contains("\x1b["), "colorized output missing ESC:\n{s}");
        // ANSI must not affect visible text: "Preflight" survives as a substring
        assert!(s.contains("Preflight"), "Preflight label missing:\n{s}");
    }

    #[test]
    fn empty_lists_render_zero_count() {
        let mut state = sample_state();
        state.archives.clear();
        let s = render(&state, &plain_unicode_theme());
        assert!(s.contains("Archives 0 selected"), "got:\n{s}");
    }

    #[test]
    fn qos_plan_renders_the_total_and_names_unschedulable_tests() {
        let mut state = sample_state();
        let mut tests = planned(&[(QosClass::Integration, 3), (QosClass::Wallet, 1)]);
        // 15c/15Gi components + 1c/1Gi runner = 16c/16Gi > 4c/8Gi
        let sync = QosClass::Sync.profile_with(Some(Resources::new(15_000, 15 * GIB, 0, 0)));
        tests
            .push(ztest::api::PlannedTest { name: "zaino_sync".into(), admitted: sync.admitted() });
        state.qos_plan =
            Some(ztest::api::qos_plan(&tests, Some(Resources::new(4_000, 8 * GIB, 0, 0))));
        let s = render(&state, &plain_unicode_theme());
        assert!(s.contains("Scheduling 5 tests"), "missing header:\n{s}");
        assert!(s.contains("reserved total"), "missing total:\n{s}");
        assert!(!s.contains("wave") && !s.contains("integration"), "no waves, no tiers:\n{s}");
        assert!(
            s.contains("zaino_sync needs 16c / 16 GiB") && s.contains("will be rejected"),
            "missing unschedulable warning:\n{s}"
        );
    }

    #[test]
    fn live_panel_shows_running_queued_and_used_of_limit() {
        use ztest::api::LiveSnapshot;

        let snapshot = LiveSnapshot {
            running: 3,
            queued: 12,
            committed: Resources::new(9_000, 17 * GIB, 0, 0),
            limit: Resources::new(12_000, 48 * GIB, 0, 0),
        };
        let progress = RunProgress {
            elapsed: std::time::Duration::from_secs(42),
            passed: 7,
            failed: 1,
            total: 20,
        };
        let s = render_live_panel(&snapshot, &progress, &plain_unicode_theme());
        assert!(s.contains("3 running"), "header:\n{s}");
        assert!(
            s.contains("3 running · 9 / 12 cores · 17 / 48 GiB"),
            "used of limit per dimension:\n{s}"
        );
        assert!(!s.contains('%') && !s.contains("free"), "no bar, percent or free:\n{s}");
        // done/total, passed, failed, elapsed
        assert!(s.contains("8/20 done"), "done count:\n{s}");
        assert!(s.contains("7 passed"), "passed count:\n{s}");
        assert!(s.contains("1 failed"), "failed count:\n{s}");
        assert!(s.contains("12 queued"), "queue depth:\n{s}");
        assert!(!s.contains("sync") && !s.contains("integration"), "no tier rows:\n{s}");
        // Rule at the top only — a bottom rule would leave a trailing blank when the
        // console sizes the panel region
        assert!(s.starts_with("───── Ztest ─────"), "top rule present:\n{s}");
        assert!(!s.trim_end().ends_with("────────────"), "no bottom rule:\n{s}");
    }

    #[test]
    /// Zero free = full, a measurement — never "unknown" (the two once shared a sentinel)
    fn a_full_run_reads_as_full_per_dimension() {
        use ztest::api::LiveSnapshot;

        let snapshot = LiveSnapshot {
            running: 23,
            queued: 115,
            committed: Resources::new(92_000, 122 * GIB, 0, 0),
            limit: Resources::new(92_000, 198 * GIB, 0, 0),
        };
        let s = render_live_panel(&snapshot, &RunProgress::default(), &plain_unicode_theme());
        assert!(s.contains("92 / 92 cores · 122 / 198 GiB"), "used of limit:\n{s}");
        assert!(!s.contains("unknown") && !s.contains("unavailable"), "never unknown:\n{s}");
    }

    #[test]
    fn no_qos_plan_renders_no_scheduling_block() {
        let mut state = sample_state();
        state.qos_plan = None;
        let s = render(&state, &plain_unicode_theme());
        assert!(!s.contains("Scheduling"), "unexpected scheduling block:\n{s}");
    }

    #[test]
    fn theme_detect_for_capabilities_truth_table() {
        assert!(!Theme::for_capabilities(false, false).is_colorized());
        assert!(!Theme::for_capabilities(false, true).is_colorized());
        assert!(Theme::for_capabilities(true, false).is_colorized());
        assert!(Theme::for_capabilities(true, true).is_colorized());
    }

    #[test]
    fn capacity_line_shows_reserved_of_allocatable_per_dimension() {
        let s = render(&sample_state(), &plain_unicode_theme());
        assert!(s.contains("capacity · 6 / 12 cores · 20 / 48 GiB"), "capacity line wrong:\n{s}");
    }

    #[test]
    fn capacity_line_degrades_to_zero_before_the_probe_lands() {
        let mut state = sample_state();
        state.cluster.capacity = ztest::api::ClusterCapacity::default();
        let s = render(&state, &plain_unicode_theme());
        assert!(s.contains("capacity · 0 / 0 cores · 0 / 0 GiB"), "zero-capacity line wrong:\n{s}");
    }

    #[test]
    fn a_fraction_keeps_one_decimal_and_a_whole_number_drops_it() {
        let (cpu, mem) = used_of(
            &Resources::new(500, 512 * 1024 * 1024, 0, 0),
            &Resources::new(8_000, 16 * GIB, 0, 0),
        );
        assert_eq!((cpu.as_str(), mem.as_str()), ("0.5 / 8 cores", "0.5 / 16 GiB"));
    }
}
