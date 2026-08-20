use std::borrow::Cow;
use std::time::Duration;

use super::layout::*;
use super::theme::Theme;
use super::{
    ArchiveRow, ArchiveStatus, BannerState, BuildState, QosPlan, SyncVitals, SyncWatchState,
    TierPlan, TransferKind, TransferProgress, TransferRow, Transfers,
};
use crate::template::{Fields, Row, Template};
use ztest::api::BuildStage;
use ztest::api::LiveSnapshot;
use ztest::api::Resources;
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

/// [`label`] in the narrower side columns
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
    pub(super) const CAPACITY: &str = concat!(
        "{label} capacity {@dot|dim} {free_cores|bold} / {alloc_cores|bold} cores",
        " {@dot|dim} {free_gib|bold} / {alloc_gib|bold} GiB free {gauge:12#} {pct|bold}%",
    );
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
    pub(super) const SCHEDULING: &str = concat!(
        "{label|pass} {tests|bold} tests {@dot|dim} {waves|bold} waves {@dot|dim} peak",
        " {peak|bold} {@dot|dim} {total|bold} reserved total",
    );
    pub(super) const SCHEDULING_BLIND: &str = concat!(
        "{label|pass} {tests|bold} tests {@dot|dim} {total|bold} reserved total",
        " {@dot|dim} capacity unknown (probe unavailable)",
    );
    pub(super) const TIER: &str = "{label} {class|dim} {count|bold} {@dot|dim} {each} each";
    pub(super) const TIER_MIXED: &str =
        "{label} {class|dim} {count|bold} {@dot|dim} {subtotal} total {@dot|dim} mixed footprints";
    pub(super) const UNSCHEDULABLE: &str = concat!(
        "{label} {@warn|skip} {class|skip} needs {admitted} {@dot|dim}",
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

    // One global figure (allocatable − requested); gauge = free headroom, driven
    // by the tighter dimension
    let alloc = c.capacity.allocatable;
    let free = c.capacity.free();
    let pct = free_percent(&free, &alloc);
    let f = Fields::new()
        .text("label", label(""))
        .text("free_cores", cores_of(&free).to_string())
        .text("alloc_cores", cores_of(&alloc).to_string())
        .text("free_gib", gib_of(&free).to_string())
        .text("alloc_gib", gib_of(&alloc).to_string())
        .percent("gauge", pct)
        .text("pct", pct.to_string());
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
    let total_tests: u32 = plan.tiers.iter().map(|t| t.count).sum();
    let header = Fields::new()
        .text("label", label("Scheduling"))
        .text("tests", total_tests.to_string())
        .text("total", plan.total.to_string());
    match plan.free {
        Some(_) => draw(
            out,
            header.text("waves", plan.waves.to_string()).text("peak", plan.peak.to_string()),
            banner_row::SCHEDULING,
            Duration::ZERO,
            theme,
        ),
        None => draw(out, header, banner_row::SCHEDULING_BLIND, Duration::ZERO, theme),
    }

    let name_col = column_width(plan.tiers.iter().map(|t| t.class.as_label()), 12, 16);
    for TierPlan { class, count, per_test, subtotal } in &plan.tiers {
        let f = Fields::new()
            .text("label", label(""))
            .text("class", pad(class.as_label(), name_col))
            .text("count", count.to_string());
        // "X each" only when uniform; mixed (an override in the tier) → subtotal
        match per_test {
            Some(each) => {
                draw(out, f.text("each", each.to_string()), banner_row::TIER, Duration::ZERO, theme)
            }
            None => draw(
                out,
                f.text("subtotal", subtotal.to_string()),
                banner_row::TIER_MIXED,
                Duration::ZERO,
                theme,
            ),
        }
    }

    // Fail-fast on a test admission will reject; reserve carried on the rejection
    // (override → tier no longer determines it)
    for u in &plan.unschedulable {
        let f = Fields::new()
            .text("label", label(""))
            .text("class", u.class.as_label())
            .text("admitted", u.admitted.to_string());
        draw(out, f, banner_row::UNSCHEDULABLE, Duration::ZERO, theme);
    }
}

// ─────────────────────────── pinned panels ────────────────────────────

/// Pinned-panel row shapes, shared where two panels draw the same row
mod panel_row {
    pub(super) const RUNNING: &str = concat!(
        "{label|pass} {@spin|bold} {running|bold} running {@dot|dim} {committed|bold}",
        " committed {@dot|dim}[ {gauge:12#} of {free|bold} free][ {blind}]",
    );
    pub(super) const PROGRESS: &str = concat!(
        "{label} {done|bold}[/{total|bold}] done {@dot|dim} {passed|pass} passed",
        "[ {@dot|dim} {failed|count.fail} failed] {@dot|dim} {elapsed|dim}",
    );
    pub(super) const TIERS: &str = "{label} {tiers} {@dot|dim} running / planned";
    pub(super) const CLUSTER: &str = concat!(
        "{label|pass} {@spin|bold} {context} {@dot|dim} {ready|bold} ready {@dot|dim}",
        " {used|bold}/{total|bold} slots",
    );
    pub(super) const CAPACITY: &str = concat!(
        "{label|dim} {gauge:12#} {pct|bold}% {@dot|dim} {free_cores|bold}/{alloc_cores|bold}c",
        " {@dot|dim} {free_gib|bold}/{alloc_gib|bold}Gi free",
    );
    pub(super) const BUILD_QUEUED: &str = "{label|pass} {@dot|dim} queued";
    pub(super) const BUILD_WORKING: &str =
        "{label|pass} {@spin|bold} {phase}{@ellipsis} {@dot|dim} {elapsed}";
    pub(super) const BUILD_OK: &str =
        "{label|pass} {@ok|pass} {tests} tests / {bins} bins {@dot|dim} {elapsed}";
    pub(super) const BUILD_FAILED: &str = "{label|pass} {@warn|fail} build failed (exit {code})";
    pub(super) const SCHEDULING: &str =
        "{label|pass} {tests|bold} tests {@dot|dim} {waves|bold} waves {@dot|dim} peak {peak|bold}";
    pub(super) const SCHEDULING_BLIND: &str =
        "{label|pass} {tests|bold} tests {@dot|dim} capacity unknown";
    pub(super) const SUBJECT: &str =
        "{label|pass} {@spin|bold} {profile|bold} {@dot|dim} {sync_id|dim}";
    pub(super) const CONTEXT: &str = "{label|dim} {context}";
    pub(super) const CANCEL: &str =
        "{label|skip} {@spin|skip} terminating subprocesses{@ellipsis} {@dot|dim} {hint|dim}";
}

/// Left column during the run: [`render_preflight_panel`]'s counterpart, same
/// [`PANEL_LINES`] height. Ledger-only, so per-tier `n/m` = running / planned,
/// not queue depth
pub fn render_live_panel(
    snapshot: &LiveSnapshot,
    plan: &QosPlan,
    free: &Resources,
    progress: &RunProgress,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(320);

    render_label_rule(&mut out, theme);

    let running = Fields::new()
        .text("label", label("Running"))
        .text("running", snapshot.total_running().to_string())
        .text("committed", snapshot.committed.to_string());
    // `free` == 0 means the re-probe was unavailable; say so, never draw an empty gauge
    let running = match free.cpu_milli == 0 && free.mem_bytes == 0 {
        true => running.text("blind", "capacity unknown (probe unavailable)"),
        false => running
            .percent("gauge", used_percent(&snapshot.committed, free))
            .text("free", free.to_string()),
    };
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

    if !plan.tiers.is_empty() {
        let parts: Vec<String> = plan
            .tiers
            .iter()
            .map(|t| {
                let run = snapshot.running.get(&t.class).map(|x| x.count).unwrap_or(0);
                format!("{} {}/{}", t.class.as_label(), run, t.count)
            })
            .collect();
        let f = Fields::new()
            .text("label", label(""))
            .text("tiers", parts.join(&format!(" {} ", theme.chars.dot)));
        draw(&mut out, f, panel_row::TIERS, Duration::ZERO, theme);
    }

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

    // Gauge on the tighter of cpu/mem; own label + compact units keep the line unclipped
    let alloc = c.capacity.allocatable;
    let free = c.capacity.free();
    let pct = free_percent(&free, &alloc);
    let f = Fields::new()
        .text("label", label("capacity"))
        .percent("gauge", pct)
        .text("pct", pct.to_string())
        .text("free_cores", cores_of(&free).to_string())
        .text("alloc_cores", cores_of(&alloc).to_string())
        .text("free_gib", gib_of(&free).to_string())
        .text("alloc_gib", gib_of(&alloc).to_string());
    draw(&mut out, f, panel_row::CAPACITY, Duration::ZERO, theme);

    render_build_line(&mut out, &state.build, elapsed, theme);

    // Scheduling row absent without a QoS plan
    if let Some(plan) = &state.qos_plan {
        let total_tests: u32 = plan.tiers.iter().map(|t| t.count).sum();
        let f =
            Fields::new().text("label", label("Scheduling")).text("tests", total_tests.to_string());
        match plan.free {
            Some(_) => draw(
                &mut out,
                f.text("waves", plan.waves.to_string()).text("peak", plan.peak.to_string()),
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

/// Left column while `ztest sync watch` tails a detached sync: live vitals, or
/// pre-first-tick the pod phase explaining the silence. Shared panel frame, only
/// the rows differ from the build that launched it
pub fn render_sync_watch_panel(
    state: &SyncWatchState,
    elapsed: std::time::Duration,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(384);

    render_label_rule(&mut out, theme);

    let f = Fields::new()
        .text("label", label("Watching"))
        .text("profile", state.profile.as_str())
        .text("sync_id", state.sync_id.as_str());
    draw(&mut out, f, panel_row::SUBJECT, elapsed, theme);

    match &state.vitals {
        Some(v) => render_sync_vitals(&mut out, state, v, elapsed, theme),
        None => render_sync_waiting(&mut out, state, elapsed, theme),
    }

    pad_to_panel(&mut out);
    out
}

// ─────────────────────────── sync metrics ─────────────────────────────

/// Metric row shapes. First four sit in the watch panel's [`LABEL_WIDTH`] column, the
/// rest in the narrower [`METRIC_LABEL_WIDTH`] side columns
mod metric_row {
    pub(super) const HEIGHT: &str =
        "{label|dim} {height|bold}[ / {target|bold}] {@dot|dim} {pct|fraction.bold} {gauge:12#}";
    pub(super) const PACE: &str = concat!(
        "{label|dim} [{blk:.1} blk/s][{blk_na}] {@dot|dim} [{tx|per_sec.bold} tx/s][{tx_na|bold}]",
        "[ {@dot|dim} eta {eta}][ {@dot|dim} reorg -{reorg|count.skip}]",
    );
    pub(super) const TREND: &str =
        "{label|dim} {blocks:12~} {span|dim}[ {@dot|dim} peak {peak:.0|dim} blk/s]";
    pub(super) const DRIVER: &str = "{label|dim} {phase|bold} {@dot|dim} {age|dim}";
    pub(super) const SETUP: &str =
        "{label|dim} {subject|bold} {@dot|dim} {detail} {@dot|dim} {age|dim}";
    pub(super) const NOTE: &str = "{label|dim} {note|dim}";
    pub(super) const POOL: &str = "{label|dim} [{rate:>8|per_sec.bold}][{rate_na:>8|bold}] {spark}";
    pub(super) const TOTAL: &str =
        "{label|dim} [{rate:>8|per_sec.bold}][{rate_na:>8|bold}] {span|dim}";
    // Absent limit != zero limit: a Burstable pod gets no denominator rather than an
    // invented one
    pub(super) const LOAD: &str =
        "{label|dim} {cpu:.1|bold}c[/{cpu_limit:.0|bold}c] [{mem%|bold}][{mem|bytes.bold}]";
    pub(super) const SIDE_NOTE: &str = "{label|dim} {note|dim}";
    pub(super) const MORE: &str = "{label|dim} +{count|count.dim} more";
}

/// Scrape believability window = 3 periods (one dropped scrape must not blink the
/// panel; a wedged exporter still resolves in seconds). Past it, derived numbers
/// blank rather than hold — a frozen rate rendered as healthy is the one
/// unacceptable failure
const STALE_AFTER: std::time::Duration =
    std::time::Duration::from_secs(ztest::api::LIVE_PERIOD.as_secs() * 3);

fn stale(v: &SyncVitals, elapsed: std::time::Duration) -> bool {
    elapsed.saturating_sub(v.received_at) > STALE_AFTER
}

/// Measured → the `{key}` rate cell, unmeasured or stale → `{na}` = `—` (one statement
/// to the reader: not known now)
fn rate<'a>(f: Fields<'a>, key: &'static str, na: &'static str, r: Option<f64>) -> Fields<'a> {
    match r {
        Some(r) => f.value(key, r),
        None => f.text(na, "—"),
    }
}

fn render_sync_vitals(
    out: &mut String,
    state: &SyncWatchState,
    v: &SyncVitals,
    elapsed: std::time::Duration,
    theme: &Theme,
) {
    let f = Fields::new()
        .text("label", label("height"))
        .text("height", thousands(v.height as u64))
        .maybe_text("target", v.target.map(|t| thousands(t as u64)))
        .value("pct", f64::from(v.pct) / 100.0)
        .percent("gauge", v.pct.clamp(0.0, 100.0) as u8);
    draw(out, f, metric_row::HEIGHT, Duration::ZERO, theme);

    let stale = stale(v, elapsed);
    // tx/s beside blk/s: same window, same staleness → the two never disagree about
    // whether the subject is moving (phase rides the `Watching` row instead)
    let fresh = |r: Option<f64>| r.filter(|_| !stale);
    let mut pace = Fields::new().text("label", label("pace"));
    pace = rate(pace, "blk", "blk_na", fresh(v.pace.map(|p| p.per_sec)));
    pace = rate(pace, "tx", "tx_na", fresh(v.tx_rate));
    // Suppressed with the stale rate it derives from (a projection off a frozen
    // rate counts down to a finish that is not happening)
    if let Some(eta) = v.pace.and_then(|p| p.eta).filter(|_| !stale) {
        pace = pace.text("eta", format_elapsed(eta));
    }
    if v.reorg_depth > 0 {
        pace = pace.value("reorg", f64::from(v.reorg_depth));
    }
    draw(out, pace, metric_row::PACE, Duration::ZERO, theme);

    render_scan_trend(out, state, theme);
}

/// Scan rate over the run: sparkline + span + best rate. The peak makes the trend
/// actionable (a scan holding at half its demonstrated best = a regression nothing
/// else on the panel can state)
fn render_scan_trend(out: &mut String, state: &SyncWatchState, theme: &Theme) {
    let Some(timeline) = &state.timeline else {
        let f = Fields::new().text("label", label("blocks")).text("note", "gathering");
        draw(out, f, metric_row::NOTE, Duration::ZERO, theme);
        return;
    };
    let trend = Fields::new()
        .text("label", label("blocks"))
        .bands(ztest::api::BLOCKS, timeline.bands(ztest::api::BLOCKS))
        .text("span", format_elapsed(timeline.span()))
        .maybe_value("peak", timeline.peak(&[ztest::api::BLOCKS]));
    draw(out, trend, metric_row::TREND, Duration::ZERO, theme);
}

/// Pre-first-tick rows: cluster, driver-pod phase, provisioning gate.
///
/// - Most of a sync's wall-clock sits here = the only progress display for minutes
/// - Setup row age = time in *that* gate, not since launch (separates slow from hung)
fn render_sync_waiting(
    out: &mut String,
    state: &SyncWatchState,
    elapsed: std::time::Duration,
    theme: &Theme,
) {
    let f = Fields::new().text("label", label("cluster")).text("context", state.context.as_str());
    draw(out, f, panel_row::CONTEXT, Duration::ZERO, theme);

    let f = Fields::new()
        .text("label", label("driver"))
        .text("phase", state.pod_phase.as_str())
        .text("age", format_elapsed(elapsed));
    draw(out, f, metric_row::DRIVER, Duration::ZERO, theme);

    let f = Fields::new().text("label", label("setup"));
    match &state.setup {
        Some(s) => draw(
            out,
            f.text("subject", s.subject.as_str())
                .text("detail", s.detail.as_str())
                .text("age", format_elapsed(elapsed.saturating_sub(s.received_at))),
            metric_row::SETUP,
            Duration::ZERO,
            theme,
        ),
        // Say "no report yet" rather than leave a row that reads as a finished step
        None => draw(
            out,
            f.text("note", "waiting for the driver's first report"),
            metric_row::NOTE,
            Duration::ZERO,
            theme,
        ),
    }
}

/// Middle column of `ztest sync watch`: one row per measured pool + `total`, each
/// with rate and sparkline.
///
/// - Per-pool, not `ztest sync status`'s stacked plot (one row per channel leaves
///   a stack nowhere to stack)
/// - Unmeasured channels dropped, never drawn flat (an empty sparkline claims idle)
pub fn render_sync_work(
    state: &SyncWatchState,
    elapsed: std::time::Duration,
    theme: &Theme,
) -> String {
    use super::plot::{Palette, PlotOpts, plot_stacked};

    let mut out = String::with_capacity(320);
    out.push('\n');

    let (Some(timeline), Some(vitals)) = (&state.timeline, state.vitals.as_ref()) else {
        let f =
            Fields::new().text("label", side_label("work")).text("note", "awaiting first scrape");
        draw(&mut out, f, metric_row::SIDE_NOTE, Duration::ZERO, theme);
        pad_to_panel(&mut out);
        return out;
    };
    let stale = stale(vitals, elapsed);

    let palette = Palette::pools(theme.is_colorized());
    let opts = PlotOpts::new(SPARK_WIDTH, 1, theme.chars.graph);
    // Measured channels only, in `CHANNELS` (oldest-pool-first) order, leaving a
    // row for the total.
    // Filtered *before* the row budget is applied: taking first would spend the
    // budget on tier-B channels that are then skipped, and the pools that were
    // actually measured would fall off the bottom.
    let measured = vitals
        .pool_rates
        .iter()
        .map(|(name, rate)| (*name, rate, timeline.bands(name)))
        .filter(|(_, _, bands)| bands.iter().any(Option::is_some));

    let mut drawn = 0;
    // Sparkline drawn here, not through a `{key:N~}` cell: the palette keys on the
    // channel name, which a template's literal key cannot carry per pool
    let fresh = |r: Option<f64>| r.filter(|_| !stale);
    for (name, r, bands) in measured.take(MAX_TRANSFER_ROWS.saturating_sub(1)) {
        let spark = plot_stacked(&[(name, bands)], &opts, &palette).pop().unwrap_or_default();
        let f = Fields::new().text("label", side_label(name)).text("spark", spark);
        draw(
            &mut out,
            rate(f, "rate", "rate_na", fresh(*r)),
            metric_row::POOL,
            Duration::ZERO,
            theme,
        );
        drawn += 1;
    }
    if drawn == 0 {
        let f = Fields::new().text("label", side_label("work")).text("note", "no pool measured");
        draw(&mut out, f, metric_row::SIDE_NOTE, Duration::ZERO, theme);
    }

    // Total last, under its pools, carrying the sparkline span (else a reader
    // cannot tell ten minutes from two days of history)
    let f = Fields::new()
        .text("label", side_label("total"))
        .text("span", format_elapsed(timeline.span()));
    draw(
        &mut out,
        rate(f, "rate", "rate_na", fresh(vitals.work_rate)),
        metric_row::TOTAL,
        Duration::ZERO,
        theme,
    );

    pad_to_panel(&mut out);
    out
}

/// Right column of `ztest sync watch`: where the subject's per-block time goes.
///
/// - Latencies, not throughput: `fetch` = upstream validator, `parse` = subject
///   → the ratio says which to fix
/// - [`PANEL_LINES`], top row blank (matches [`render_transfers`])
/// - Empty column names its cause (blank rows read as "everything is zero")
pub fn render_sync_load(state: &SyncWatchState, theme: &Theme) -> String {
    let mut out = String::with_capacity(320);
    out.push('\n');

    // Name the cause: a blank column reads as "the pods are idle", the one thing it
    // never means (no metrics API, or the first 15s sample not yet landed)
    if state.pods.is_empty() {
        let f = Fields::new()
            .text("label", side_label("load"))
            .text("note", state.pods_note.as_deref().unwrap_or("awaiting first sample"));
        draw(&mut out, f, metric_row::SIDE_NOTE, Duration::ZERO, theme);
        pad_to_panel(&mut out);
        return out;
    }

    // One row per pod, `PANEL_LINES - 1` of them (blank top row aligns with the left
    // column's branded rule); a longer topology collapses its tail
    let budget = PANEL_LINES - 1;
    let (shown, hidden) = if state.pods.len() > budget {
        (&state.pods[..budget - 1], state.pods.len() - (budget - 1))
    } else {
        (&state.pods[..], 0)
    };

    for load in shown {
        let limit = load.limit.as_ref();
        let mut f = Fields::new()
            .text("label", side_label(&clip_pod(&load.pod)))
            .value("cpu", load.usage.cpu_milli as f64 / 1000.0)
            .maybe_value(
                "cpu_limit",
                limit.map(|l| l.cpu_milli).filter(|&c| c > 0).map(|c| c as f64 / 1000.0),
            );
        // Pair when a denominator exists (`10.0/24.0 GiB` shares one magnitude); bare
        // usage otherwise
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

/// Component names are short (`zainod`, `zebrad`); a long one loses its tail rather
/// than pushing the numbers off the column
fn clip_pod(name: &str) -> String {
    if name.chars().count() <= METRIC_LABEL_WIDTH {
        return name.to_string();
    }
    name.chars().take(METRIC_LABEL_WIDTH).collect()
}

// ─────────────────────────── transfers ────────────────────────────────

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

    /// `(tier, count)` → one `PlannedTest` per test, each at its tier default
    fn at_tiers(sets: &[(QosClass, u32)]) -> Vec<ztest::api::PlannedTest> {
        sets.iter()
            .flat_map(|&(class, n)| {
                std::iter::repeat_n(
                    ztest::api::PlannedTest { class, admitted: class.profile().admitted() },
                    n as usize,
                )
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
            &at_tiers(&[(QosClass::Basic, 6)]),
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
        assert!(s.contains("waves"), "scheduling summary:\n{s}");
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
             capacity · 6 / 12 cores · 28 / 48 GiB free [██████░░░░░░] 50%

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
            let w = watching(Some(sample_vitals()));
            check("render_sync_work", &render_sync_work(&w, FRAME, &t));
            check("render_sync_load", &render_sync_load(&w, &t));
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
    fn qos_plan_renders_tiers_waves_and_unschedulable_warning() {
        let mut state = sample_state();
        // sync (17c/18Gi admitted) cannot fit 4c/8Gi → unschedulable; the rest schedule
        state.qos_plan = Some(ztest::api::qos_plan(
            &at_tiers(&[(QosClass::Basic, 3), (QosClass::Integration, 1), (QosClass::Sync, 2)]),
            Some(Resources::new(4000, 8 * GIB, 0, 0)),
        ));
        let s = render(&state, &plain_unicode_theme());
        // Header: test count + wave estimate
        assert!(s.contains("Scheduling 6 tests"), "missing header:\n{s}");
        assert!(s.contains("waves"), "missing wave estimate:\n{s}");
        // Per-tier rows in priority order. CPU always whole cores (integer
        // allocations) → basic renders `1c`, never `500m`
        assert!(s.contains("integration"), "got:\n{s}");
        // Admitted = components + runner
        assert!(s.contains("2c / 1 GiB"), "missing basic footprint:\n{s}");
        // sync admitted 15c/15Gi components + 1c/1Gi runner = 16c/16Gi > 4c/8Gi
        assert!(
            s.contains("sync needs 16c / 16 GiB") && s.contains("will be rejected"),
            "missing unschedulable warning:\n{s}"
        );
    }

    #[test]
    fn live_panel_shows_running_over_planned_and_a_gauge() {
        use std::collections::BTreeMap;
        use ztest::api::{LiveSnapshot, TierLive};

        let plan = ztest::api::qos_plan(
            &at_tiers(&[(QosClass::Sync, 2), (QosClass::Basic, 3)]),
            Some(Resources::new(12_000, 48 * GIB, 0, 0)),
        );
        let snapshot = LiveSnapshot {
            running: BTreeMap::from([
                (
                    QosClass::Sync,
                    TierLive { count: 1, reserve: Resources::new(8_000, 16 * GIB, 0, 0) },
                ),
                (QosClass::Basic, TierLive { count: 2, reserve: Resources::new(1_000, GIB, 0, 0) }),
            ]),
            committed: Resources::new(9_000, 17 * GIB, 0, 0),
        };
        let progress = RunProgress {
            elapsed: std::time::Duration::from_secs(42),
            passed: 7,
            failed: 1,
            total: 20,
        };
        let s = render_live_panel(
            &snapshot,
            &plan,
            &Resources::new(12_000, 48 * GIB, 0, 0),
            &progress,
            &plain_unicode_theme(),
        );
        assert!(s.contains("3 running"), "header:\n{s}");
        assert!(s.contains("9c / 17 GiB committed"), "committed:\n{s}");
        // done/total, passed, failed, elapsed
        assert!(s.contains("8/20 done"), "done count:\n{s}");
        assert!(s.contains("7 passed"), "passed count:\n{s}");
        assert!(s.contains("1 failed"), "failed count:\n{s}");
        // Per-tier running/planned, priority order
        assert!(s.contains("sync 1/2"), "got:\n{s}");
        assert!(s.contains("basic 2/3"), "got:\n{s}");
        let sync_at = s.find("sync 1/2").unwrap();
        let basic_at = s.find("basic 2/3").unwrap();
        assert!(sync_at < basic_at, "priority order:\n{s}");
        assert!(s.contains("running / planned"), "legend:\n{s}");
        // Rule at the top only — a bottom rule would leave a trailing blank when the
        // console sizes the panel region
        assert!(s.starts_with("───── Ztest ─────"), "top rule present:\n{s}");
        assert!(!s.trim_end().ends_with("────────────"), "no bottom rule:\n{s}");
    }

    #[test]
    fn live_panel_with_unknown_capacity_says_so_instead_of_a_zero_gauge() {
        use ztest::api::LiveSnapshot;

        let plan = ztest::api::qos_plan(&at_tiers(&[(QosClass::Basic, 2)]), None);
        let snapshot =
            LiveSnapshot { committed: Resources::new(1_000, GIB, 0, 0), ..LiveSnapshot::default() };
        // free == ZERO → probe unavailable
        let s = render_live_panel(
            &snapshot,
            &plan,
            &Resources::ZERO,
            &RunProgress::default(),
            &plain_unicode_theme(),
        );
        assert!(s.contains("capacity unknown (probe unavailable)"), "got:\n{s}");
        assert!(!s.contains("of 0c"), "should not show a zero-free gauge:\n{s}");
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
    fn capacity_line_shows_free_over_allocatable_and_a_gauge() {
        let s = render(&sample_state(), &plain_unicode_theme());
        // free = 12-6 cores / 48-20 GiB; gauge on the tighter dim
        assert!(
            s.contains("capacity · 6 / 12 cores · 28 / 48 GiB free [██████░░░░░░] 50%"),
            "capacity line wrong:\n{s}"
        );
    }

    #[test]
    fn capacity_line_degrades_to_zero_before_the_probe_lands() {
        let mut state = sample_state();
        state.cluster.capacity = ztest::api::ClusterCapacity::default();
        let s = render(&state, &plain_unicode_theme());
        // All-zero capacity renders 0/0 and a 0% gauge, no panic / div-by-zero.
        assert!(
            s.contains("capacity · 0 / 0 cores · 0 / 0 GiB free [░░░░░░░░░░░░] 0%"),
            "zero-capacity line wrong:\n{s}"
        );
    }

    #[test]
    fn free_percent_uses_the_tighter_dimension() {
        // CPU 50% free, memory 25% free → 25
        let free = Resources::new(2_000, GIB, 0, 0);
        let alloc = Resources::new(4_000, 4 * GIB, 0, 0);
        assert_eq!(free_percent(&free, &alloc), 25);
        // Zero allocatable → 0, no div-by-zero
        assert_eq!(free_percent(&Resources::ZERO, &Resources::ZERO), 0);
    }

    // ─────────────────────── sync watch panel ─────────────────────────

    fn watching(vitals: Option<SyncVitals>) -> SyncWatchState {
        SyncWatchState {
            profile: "zaino_state_sync".into(),
            sync_id: "zaino-state-sync-a52f9ec9".into(),
            context: "crc-remote".into(),
            pod_phase: "Running".into(),
            setup: None,
            vitals,
            metrics_note: None,
            probes: Vec::new(),
            violations: 0,
            timeline: None,
            pods: Vec::new(),
            pods_note: None,
        }
    }

    fn pod_load(name: &str, cpu_milli: u64, mem_gib: u64) -> ztest::api::PodLoad {
        ztest::api::PodLoad {
            pod: name.to_string(),
            usage: Resources::new(cpu_milli, mem_gib * ztest::api::GIB, 0, 0),
            limit: Some(Resources::new(9_000, 24 * ztest::api::GIB, 0, 0)),
        }
    }

    /// Session-elapsed frame these render at: 2s after [`sample_vitals`]'s scrape
    /// = comfortably fresh
    const FRAME: std::time::Duration = std::time::Duration::from_secs(212);

    fn sample_vitals() -> SyncVitals {
        SyncVitals {
            height: 901,
            target: Some(1024),
            pct: 88.1,
            phase: Some(ztest::api::Phase::Syncing),
            phase_detail: Some("scanning".into()),
            reorg_depth: 0,
            pace: Some(ztest::api::Pace {
                per_sec: 12.4,
                eta: Some(std::time::Duration::from_secs(10)),
            }),
            tx_rate: Some(48.0),
            // transparent/sprout = tier B, uncounted; ironwood counted & genuinely
            // idle — the panel must render that difference
            work_rate: Some(23_600.0),
            pool_rates: vec![
                ("transparent", None),
                ("sprout", None),
                ("sapling", Some(19_400.0)),
                ("orchard", Some(4_200.0)),
                ("ironwood", Some(0.0)),
            ],
            cost: ztest::api::Cost {
                fetch_ms: Some(41.2),
                treestate_ms: Some(2.1),
                parse_ms: Some(6.8),
                grpc_ms: Some(4.13),
            },
            received_at: std::time::Duration::from_secs(210),
        }
    }

    /// Scan rate + every channel `sample_vitals` rates, so trend row and work
    /// column both have something to draw. Over
    /// [`plot_channels`](ztest::api::plot_channels) = the watcher's real channel set
    fn sample_timeline() -> ztest::api::Timeline {
        let names: Vec<&str> = ztest::api::plot_channels().collect();
        let mut timeline =
            ztest::api::Timeline::new(names.clone(), std::time::Duration::from_secs(60));
        for step in 0..6u64 {
            let samples: Vec<Option<f64>> = names
                .iter()
                .map(|n| match *n {
                    "blocks" => Some(400.0 + step as f64 * 40.0),
                    "sapling" => Some(19_400.0 + step as f64 * 100.0),
                    "orchard" => Some(4_200.0),
                    "ironwood" => Some(0.0),
                    // Tier B: uncounted
                    _ => None,
                })
                .collect();
            timeline.push(std::time::Duration::from_secs(step * 60), &samples);
        }
        timeline
    }

    /// Unmeasured pool gets no row, never a flat line at zero (a flat sparkline
    /// claims idle — the one thing an unmeasured channel does not say)
    #[test]
    fn the_work_column_draws_only_measured_pools() {
        let mut state = watching(Some(sample_vitals()));
        state.timeline = Some(sample_timeline());
        let out = render_sync_work(&state, FRAME, &plain_unicode_theme());

        for measured in ["sapling", "orchard", "ironwood"] {
            assert!(out.contains(measured), "`{measured}` row missing:\n{out}");
        }
        for unmeasured in ["transparent", "sprout"] {
            assert!(
                !out.contains(unmeasured),
                "`{unmeasured}` was never counted and must not get a row:\n{out}"
            );
        }
        assert!(out.contains("total"), "total row missing:\n{out}");
    }

    /// Height-critical: a column whose line count differs from its neighbours'
    /// shears the fixed panel block
    #[test]
    fn the_work_column_is_always_panel_height() {
        let theme = plain_unicode_theme();
        let mut full = watching(Some(sample_vitals()));
        full.timeline = Some(sample_timeline());
        for state in [watching(None), watching(Some(sample_vitals())), full] {
            let out = render_sync_work(&state, FRAME, &theme);
            assert_eq!(
                out.lines().count(),
                PANEL_LINES,
                "work column must be exactly {PANEL_LINES} lines:\n{out}"
            );
        }
    }

    /// Pre-first-series there is nothing to plot, and an empty column reads as
    /// "no work happened" — the one thing it does not mean
    #[test]
    fn the_work_column_names_why_it_is_empty() {
        let theme = plain_unicode_theme();
        assert!(
            render_sync_work(&watching(None), FRAME, &theme).contains("awaiting first scrape"),
            "a pre-scrape column must say so"
        );
    }

    /// An empty load column names its cause (blank rows read as "the pods are idle")
    #[test]
    fn the_load_column_distinguishes_why_it_is_empty() {
        let theme = plain_unicode_theme();
        let render = |note: Option<&str>| {
            let mut state = watching(Some(sample_vitals()));
            state.pods_note = note.map(str::to_string);
            let s = render_sync_load(&state, &theme);
            assert_eq!(s.lines().count(), PANEL_LINES, "fixed height:\n{s}");
            s
        };

        assert!(render(None).contains("awaiting first sample"), "pre-sample cause");
        let s = render(Some("no metrics API (`ztest cluster setup`)"));
        assert!(s.contains("no metrics API"), "missing-API cause:\n{s}");
    }

    #[test]
    fn the_load_column_shows_usage_against_each_pods_limit() {
        let mut state = watching(Some(sample_vitals()));
        state.pods = vec![pod_load("zainod", 593, 10), pod_load("zebrad", 6, 1)];
        let s = render_sync_load(&state, &plain_unicode_theme());

        assert_eq!(s.lines().count(), PANEL_LINES, "fixed height:\n{s}");
        assert!(s.contains("zainod"), "names the pod:\n{s}");
        assert!(s.contains("0.6c/9c"), "cpu against its limit:\n{s}");
        // Byte pair shares one magnitude, so the denominator costs 6 columns, not 9
        assert!(s.contains("10.0/24.0 GiB"), "memory against its limit:\n{s}");
        assert!(s.contains("zebrad"), "second pod:\n{s}");
    }

    /// Burstable pods have no denominator; inventing one would misreport headroom
    #[test]
    fn a_pod_without_limits_shows_bare_usage() {
        let mut state = watching(Some(sample_vitals()));
        let mut load = pod_load("zainod", 1_500, 2);
        load.limit = None;
        state.pods = vec![load];
        let s = render_sync_load(&state, &plain_unicode_theme());
        // Regression: routing cpu through `Unit::Cores` reduced 1.5 to `2c` (`compact` is
        // integer past 1.0); a `{cpu:.1}` cell keeps the tenth a load reading needs
        assert!(s.contains("1.5c"), "bare cpu:\n{s}");
        assert!(s.contains("2.0 GiB"), "bare memory:\n{s}");
        assert!(!s.contains('/'), "no invented denominator:\n{s}");
    }

    /// Height-critical: more pods than rows must collapse, never shear the panel
    #[test]
    fn a_deep_topology_collapses_its_tail() {
        let mut state = watching(Some(sample_vitals()));
        state.pods = (0..8).map(|i| pod_load(&format!("pod{i}"), 100, 1)).collect::<Vec<_>>();
        let s = render_sync_load(&state, &plain_unicode_theme());
        assert_eq!(s.lines().count(), PANEL_LINES, "fixed height:\n{s}");
        assert!(s.contains("+5 more"), "tail collapses:\n{s}");
    }
}
