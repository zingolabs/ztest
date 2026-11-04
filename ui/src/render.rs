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
    pub(super) const CAPACITY_PROBING: &str = "{label} capacity {@dot|dim} probing{@ellipsis}";
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
    let f = Fields::new().text("label", label(""));
    match &c.capacity {
        Some(cap) => {
            let (cpu, mem) = used_of(&cap.reserved, &cap.allocatable);
            draw(
                out,
                f.text("cpu", cpu).text("mem", mem),
                banner_row::CAPACITY,
                Duration::ZERO,
                theme,
            )
        }
        None => draw(out, f, banner_row::CAPACITY_PROBING, Duration::ZERO, theme),
    }
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
    pub(super) const CAPACITY_PROBING: &str = "{label|dim} probing{@ellipsis}";
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

    let f = Fields::new().text("label", label("capacity"));
    match &c.capacity {
        Some(cap) => {
            let (cpu, mem) = used_of(&cap.reserved, &cap.allocatable);
            draw(
                &mut out,
                f.text("cpu", cpu).text("mem", mem),
                panel_row::CAPACITY,
                Duration::ZERO,
                theme,
            )
        }
        None => draw(&mut out, f, panel_row::CAPACITY_PROBING, Duration::ZERO, theme),
    }

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

/// Believability = 3 live reads (one missed read must not blink the panel). Past it rates blank,
/// never hold (a frozen rate drawn as healthy = the one unacceptable failure)
const STALE_AFTER: Duration = Duration::from_secs(ztest::api::metrics::LIVE_INTERVAL.as_secs() * 3);

fn stale(v: &SyncVitals, elapsed: Duration) -> bool {
    elapsed.saturating_sub(v.received_at) > STALE_AFTER
}

/// Segment span as of this frame: last read's span + local time since (ticks between reads); `—`
/// until the driver's segment start is read
fn uptime(v: &SyncVitals, elapsed: Duration) -> String {
    let span = v.span.map(|span| span + elapsed.saturating_sub(v.received_at));
    span.map_or_else(|| "—".to_owned(), format_elapsed)
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

    render_scan_trend(out, v, elapsed, theme);
}

/// Scan rate over the run + its best (a scan holding at half its demonstrated peak = a regression
/// nothing else on the panel states)
fn render_scan_trend(out: &mut String, v: &SyncVitals, elapsed: Duration, theme: &Theme) {
    let Some(blocks) = v.blocks.as_ref().filter(|b| !b.points.is_empty()) else {
        let f = Fields::new().text("label", label("blocks")).text("note", "gathering");
        draw(out, f, metric_row::NOTE, Duration::ZERO, theme);
        return;
    };
    let trend = Fields::new()
        .text("label", label("blocks"))
        .bands("blocks", super::report::bands(blocks))
        .text("span", uptime(v, elapsed))
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
    let f = Fields::new().text("label", side_label("total")).text("span", uptime(vitals, elapsed));
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
        TransferKind::Image => theme.chars.up,
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
            span: Some(Duration::from_secs(3 * 3600)),
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

    /// No colours + Unicode glyphs = `Theme::detect()` under UTF-8 + `NO_COLOR=1`
    /// (lets these snapshot byte-exact output)
    fn plain_unicode_theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    #[test]
    fn an_unprobed_cluster_says_probing_never_zero() {
        let state = BannerState {
            cluster: ClusterState {
                context: "kind-zaino-local".to_string(),
                slots_used: 12,
                slots_total: 16,
                slots_configured: 6,
                nodes_ready: 3,
                nodes_cordoned: 0,
                capacity: None,
            },
            build: BuildState::Ok {
                test_count: 47,
                binary_count: 8,
                elapsed: std::time::Duration::from_secs(18),
            },
            archives: vec![ArchiveRow {
                name: "regtest-nu5-h128".to_string(),
                status: ArchiveStatus::Cached { size_bytes: 432_013_312 },
            }],
            qos_plan: None,
        };
        let banner = render(&state, &plain_unicode_theme());
        let panel = render_preflight_panel(
            &state,
            "Preflight",
            std::time::Duration::ZERO,
            &plain_unicode_theme(),
        );
        for s in [banner, panel] {
            assert!(s.contains("probing…"), "unprobed must say so:\n{s}");
            assert!(!s.contains("0 / 0"), "unprobed is not a zero reading:\n{s}");
        }
    }
}
