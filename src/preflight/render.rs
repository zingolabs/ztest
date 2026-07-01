//! Pure formatter for the preflight banner.
//!
//! Takes a [`BannerState`] and a [`Theme`], returns a `String`. No I/O, no async.
//! Obeys nextest's reporter conventions:
//! - a 12-column right-aligned action label plus a space on every primary line
//!   (`"{:>12} "`), matching nextest's `Nextest run` / `Starting` / `Running` /
//!   `Skipped` cadence;
//! - a 12-char horizontal rule (`theme.chars.hbar(12)`) opening and closing the
//!   block, the width and glyph nextest uses for `RunStarted` / `RunFinished`;
//! - `·` between metadata fields, styled `dim`;
//! - markers (`✓ ⇣ !`) styled `pass` / `dim` / `skip`; numeric counts `count`.
//!
//! The result is that the preflight reads as another group of action lines in
//! nextest's banner, not a separate UI.

use std::fmt::Write as _;

use bytesize::ByteSize;
use owo_colors::OwoColorize;

use super::theme::Theme;
use super::{
    ArchiveRow, ArchiveStatus, BannerState, BuildStage, BuildState, DownloadSource, QosPlan,
    SnapshotRow, SnapshotStatus, TierPlan,
};
use crate::qos::live::LiveSnapshot;
use crate::qos::{GIB, MIB, QosClass, Resources};

/// Width of the action-label column, matching nextest's `{:>12}`.
const LABEL_WIDTH: usize = 12;

/// Width of the bracketed progress bar in [`render_progress_bar`].
const PROGRESS_BAR_WIDTH: usize = 12;

/// Render one frame of the banner to a `String`.
///
/// The result ends in `\n`. ANSI escape codes are present iff
/// `theme.is_colorized()`. Callers doing in-place refresh must emit cursor-up
/// sequences between successive `render` calls; this only produces one frame.
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
    blank_line(&mut out);
    render_snapshot_block(&mut out, state, theme);
    if let Some(plan) = &state.qos_plan {
        blank_line(&mut out);
        render_qos_block(&mut out, plan, theme);
    }
    if !state.future.is_empty() {
        blank_line(&mut out);
        render_future_block(&mut out, state, theme);
    }
    render_bottom_rule(&mut out, theme);

    out
}

/// One blank line, the section separator. A single `\n` so the live-renderer's
/// line counter doesn't double-count.
fn blank_line(out: &mut String) {
    out.push('\n');
}

// ─────────────────────────── line writers ─────────────────────────────

/// Indent for `{:>12} ` action labels, used on lines that continue the previous
/// label (e.g. the cluster block's node line).
const INDENT: &str = "             "; // 12 spaces + 1 separator = label column width + 1

fn render_top_rule(out: &mut String, theme: &Theme) {
    writeln!(out, "{}", theme.chars.hbar(LABEL_WIDTH)).expect("write to string");
}

fn render_bottom_rule(out: &mut String, theme: &Theme) {
    writeln!(out, "{}", theme.chars.hbar(LABEL_WIDTH)).expect("write to string");
}

/// The branded divider between scrolled output above and a pinned status panel
/// below: `───── Ztest ─────`. Reuses `hbar` so it follows the theme's glyph
/// (`-----` under the ASCII fallback).
fn render_label_rule(out: &mut String, theme: &Theme) {
    let side = theme.chars.hbar(5);
    writeln!(
        out,
        "{side} {} {side}",
        "Ztest".style(theme.styles.script_id)
    )
    .expect("write to string");
}

fn render_header_line(out: &mut String, _state: &BannerState, theme: &Theme) {
    let label = "Preflight";
    writeln!(
        out,
        "{:>width$} {}",
        label.style(theme.styles.pass),
        "ztest".style(theme.styles.script_id),
        width = LABEL_WIDTH,
    )
    .expect("write to string");
}

fn render_cluster_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let c = &state.cluster;
    let dot = theme.chars.dot.style(theme.styles.dim);

    writeln!(
        out,
        "{:>width$} context {} {dot} {} / {} slots used {dot} configured {} via --test-threads",
        "Cluster".style(theme.styles.pass),
        c.context,
        c.slots_used.style(theme.styles.count),
        c.slots_total.style(theme.styles.count),
        c.slots_configured.style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    writeln!(
        out,
        "{INDENT}{} ready {dot} {} cordoned",
        c.nodes_ready.style(theme.styles.count),
        c.nodes_cordoned.style(theme.styles.count),
    )
    .expect("write to string");

    // Capacity: one global figure (allocatable minus sum of requested). The
    // gauge shows free headroom, driven by the tighter of the two dimensions.
    let alloc = c.capacity.allocatable;
    let free = c.capacity.free();
    let pct = free_percent(&free, &alloc);
    let bar = render_progress_bar(pct, theme);
    writeln!(
        out,
        "{INDENT}capacity {dot} {} / {} cores {dot} {} / {} GiB free {bar} {}",
        cores_of(&free).style(theme.styles.count),
        cores_of(&alloc).style(theme.styles.count),
        gib_of(&free).style(theme.styles.count),
        gib_of(&alloc).style(theme.styles.count),
        format_args!("{pct}%").style(theme.styles.count),
    )
    .expect("write to string");
}

/// Whole CPU cores in a [`Resources`] (millicpu / 1000, rounded down).
fn cores_of(r: &Resources) -> u64 {
    r.cpu_milli / 1000
}

/// Whole GiB in a [`Resources`].
fn gib_of(r: &Resources) -> u64 {
    r.mem_bytes / GIB
}

/// Percent of capacity free, the tighter (min) of the CPU and memory free
/// fractions: the binding constraint for packing more work. Zero allocatable
/// (e.g. before the probe lands) gives 0%.
fn free_percent(free: &Resources, alloc: &Resources) -> u8 {
    let frac = |f: u64, a: u64| -> u64 {
        if a == 0 {
            0
        } else {
            ((f as u128 * 100) / a as u128).min(100) as u64
        }
    };
    frac(free.cpu_milli, alloc.cpu_milli).min(frac(free.mem_bytes, alloc.mem_bytes)) as u8
}

/// Spinner glyph table: the braille frames `indicatif` uses for its default
/// spinner. The frame index is derived from elapsed time so a ~200ms redraw
/// cadence cycles smoothly.
const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Pick a spinner frame from `elapsed`. 100ms per frame, so a 200ms redraw
/// cadence advances ~2 frames per tick.
fn spinner_glyph(elapsed: std::time::Duration) -> &'static str {
    let idx = (elapsed.as_millis() / 100) as usize % SPINNER_FRAMES.len();
    SPINNER_FRAMES[idx]
}

/// Format an elapsed duration as `12s` / `1m23s`, matching nextest's `[NNs]` /
/// `[Nm NNs]` timestamp vocabulary.
fn format_elapsed(d: std::time::Duration) -> String {
    let total = d.as_secs();
    if total < 60 {
        format!("{total}s")
    } else {
        format!("{}m{:02}s", total / 60, total % 60)
    }
}

fn render_inventory_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let dot = theme.chars.dot.style(theme.styles.dim);
    match &state.build {
        BuildState::Pending => {
            writeln!(
                out,
                "{:>width$} {}",
                "Inventory".style(theme.styles.dim),
                "queued".style(theme.styles.dim),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
        BuildState::Compiling { started_at } => {
            let elapsed = started_at.elapsed();
            writeln!(
                out,
                "{:>width$} {} compiling test binaries… {dot} {}",
                "Inventory".style(theme.styles.pass),
                spinner_glyph(elapsed).style(theme.styles.count),
                format_elapsed(elapsed).style(theme.styles.count),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
        BuildState::Indexing { started_at } => {
            let elapsed = started_at.elapsed();
            writeln!(
                out,
                "{:>width$} {} indexing test selection… {dot} {}",
                "Inventory".style(theme.styles.pass),
                spinner_glyph(elapsed).style(theme.styles.count),
                format_elapsed(elapsed).style(theme.styles.count),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
        BuildState::Ok {
            test_count,
            binary_count,
            elapsed,
        } => {
            writeln!(
                out,
                "{:>width$} {} {} tests across {} binaries {dot} {}",
                "Inventory".style(theme.styles.pass),
                theme.chars.ok.style(theme.styles.pass),
                test_count.style(theme.styles.count),
                binary_count.style(theme.styles.count),
                format_elapsed(*elapsed).style(theme.styles.count),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
        BuildState::Failed {
            exit_code,
            stage,
            elapsed,
        } => {
            let stage_label = match stage {
                BuildStage::Compile => "compile",
                BuildStage::Index => "index",
            };
            writeln!(
                out,
                "{:>width$} {} {} failed (exit {exit_code}) {dot} {}",
                "Inventory".style(theme.styles.fail),
                theme.chars.warn.style(theme.styles.fail),
                stage_label,
                format_elapsed(*elapsed).style(theme.styles.count),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
    }
}

fn render_archive_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let archives = &state.archives;
    writeln!(
        out,
        "{:>width$} {} selected",
        "Archives".style(theme.styles.pass),
        archives.len().style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    let name_col = column_width(archives.iter().map(|r| r.name.as_str()), 18, 28);
    let dot = theme.chars.dot.style(theme.styles.dim);
    for row in archives {
        write_archive_row(out, row, name_col, &dot, theme);
    }
}

fn render_snapshot_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let snapshots = &state.snapshots;
    writeln!(
        out,
        "{:>width$} {} selected",
        "Snapshots".style(theme.styles.pass),
        snapshots.len().style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    let name_col = column_width(snapshots.iter().map(|r| r.pvc.as_str()), 24, 36);
    let dot = theme.chars.dot.style(theme.styles.dim);
    for row in snapshots {
        write_snapshot_row(out, row, name_col, &dot, theme);
    }
}

fn render_future_block(out: &mut String, state: &BannerState, theme: &Theme) {
    if state.future.is_empty() {
        return;
    }
    writeln!(
        out,
        "{:>width$} {} planned",
        "Scheduling".style(theme.styles.dim),
        state.future.len().style(theme.styles.dim),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    let name_col = column_width(state.future.iter().map(|r| r.label), 12, 16);
    let dot = theme.chars.dot.style(theme.styles.dim);
    for row in &state.future {
        writeln!(
            out,
            "{INDENT}{:<width$} {dot} {}",
            row.label.style(theme.styles.dim),
            "planned (allocator pending)".style(theme.styles.dim),
            width = name_col,
        )
        .expect("write to string");
    }
}

/// The lowercase tier name shown in the scheduling block.
fn tier_label(c: QosClass) -> &'static str {
    match c {
        QosClass::Basic => "basic",
        QosClass::Integration => "integration",
        QosClass::Testnet => "testnet",
        QosClass::Sync => "sync",
    }
}

/// A footprint's CPU as `Nc` (whole cores) or `Nm` (millicpu), so a half-core
/// `basic` reads `500m`, not `0 cores`.
fn cpu_str(milli: u64) -> String {
    if milli != 0 && milli.is_multiple_of(1000) {
        format!("{}c", milli / 1000)
    } else {
        format!("{milli}m")
    }
}

/// A footprint's memory as a clean `N GiB` / `N MiB` (exact for the
/// power-of-two tier reserves).
fn mem_str(bytes: u64) -> String {
    if bytes != 0 && bytes.is_multiple_of(GIB) {
        format!("{} GiB", bytes / GIB)
    } else {
        format!("{} MiB", bytes / MIB)
    }
}

/// `<cpu> / <mem>` for a single-test footprint (exact units: `500m`, `16 GiB`).
fn footprint_str(r: &Resources) -> String {
    format!("{} / {}", cpu_str(r.cpu_milli), mem_str(r.mem_bytes))
}

/// `<cpu> / <mem>` for an aggregate (peak / total) reserve, in decimal cores and
/// GiB: `11.5c / 19.5 GiB` reads better than `11500m / 19968 MiB` for summed
/// figures.
fn agg_str(r: &Resources) -> String {
    let cpu = if r.cpu_milli.is_multiple_of(1000) {
        format!("{}c", r.cpu_milli / 1000)
    } else {
        format!("{:.1}c", r.cpu_milli as f64 / 1000.0)
    };
    let mem = if r.mem_bytes.is_multiple_of(GIB) {
        format!("{} GiB", r.mem_bytes / GIB)
    } else {
        format!("{:.1} GiB", r.mem_bytes as f64 / GIB as f64)
    };
    format!("{cpu} / {mem}")
}

/// The QoS scheduling plan (`docs/qos-design.md` §8 planning pass): per-tier
/// selected counts and footprints, the wave/peak estimate against probed
/// capacity, and any unschedulable-tier warnings. The live during-run
/// reservation view is a deferred follow-up, noted as the final dim line.
fn render_qos_block(out: &mut String, plan: &QosPlan, theme: &Theme) {
    let dot = theme.chars.dot.style(theme.styles.dim);
    let total_tests: u32 = plan.tiers.iter().map(|t| t.count).sum();

    match plan.free {
        Some(_) => writeln!(
            out,
            "{:>width$} {} tests {dot} {} waves {dot} peak {} {dot} {} reserved total",
            "Scheduling".style(theme.styles.pass),
            total_tests.style(theme.styles.count),
            plan.waves.style(theme.styles.count),
            agg_str(&plan.peak).style(theme.styles.count),
            agg_str(&plan.total).style(theme.styles.count),
            width = LABEL_WIDTH,
        )
        .expect("write to string"),
        None => writeln!(
            out,
            "{:>width$} {} tests {dot} {} reserved total {dot} capacity unknown (probe unavailable)",
            "Scheduling".style(theme.styles.pass),
            total_tests.style(theme.styles.count),
            agg_str(&plan.total).style(theme.styles.count),
            width = LABEL_WIDTH,
        )
        .expect("write to string"),
    }

    let name_col = column_width(plan.tiers.iter().map(|t| tier_label(t.class)), 12, 16);
    for TierPlan {
        class,
        count,
        footprint,
    } in &plan.tiers
    {
        writeln!(
            out,
            "{INDENT}{:<width$} {} {dot} {} each",
            tier_label(*class).style(theme.styles.dim),
            count.style(theme.styles.count),
            footprint_str(footprint),
            width = name_col,
        )
        .expect("write to string");
    }

    // Fail-fast: a tier whose footprint exceeds the whole cluster will be
    // rejected at admission, so surface it now.
    let warn = theme.chars.warn.style(theme.styles.skip);
    for class in &plan.unschedulable {
        writeln!(
            out,
            "{INDENT}{warn} {} needs {} {dot} exceeds cluster capacity — will be rejected",
            tier_label(*class).style(theme.styles.skip),
            footprint_str(&class.profile().footprint),
        )
        .expect("write to string");
    }

    // The live reservation view is the deferred §8 half: it would poll the
    // ledger during the run.
    writeln!(
        out,
        "{INDENT}{:<width$} {dot} {}",
        "reservation".style(theme.styles.dim),
        "live view during run (pending)".style(theme.styles.dim),
        width = name_col,
    )
    .expect("write to string");
}

/// Utilization percent of `part` within `whole`, the tighter (max) of the CPU
/// and memory fractions: how full the binding dimension is. Zero `whole` gives 0%.
fn used_percent(part: &Resources, whole: &Resources) -> u8 {
    let frac = |p: u64, w: u64| -> u64 {
        if w == 0 {
            0
        } else {
            ((p as u128 * 100) / w as u128).min(100) as u64
        }
    };
    frac(part.cpu_milli, whole.cpu_milli).max(frac(part.mem_bytes, whole.mem_bytes)) as u8
}

/// Live test-run progress for the during-run panel.
///
/// Populated by the run loop (`cli::console`): `elapsed` drives the spinner and
/// wall clock (the proof-of-life heartbeat), and the counts are tallied from the
/// relayed per-test result lines. `total` is the test count discovered during
/// preflight (Phase B); `0` means unknown, and the done-of-total fraction
/// renders without a denominator.
#[derive(Debug, Clone, Default)]
pub struct RunProgress {
    /// Wall time since the run started; drives the spinner and the clock.
    pub elapsed: std::time::Duration,
    /// Tests finished with a passing verdict.
    pub passed: u32,
    /// Tests finished with a failing verdict (FAIL/TIMEOUT/LEAK/…).
    pub failed: u32,
    /// Total tests to run, from preflight; `0` = unknown.
    pub total: u32,
}

impl RunProgress {
    /// Tests that have reached a terminal verdict.
    fn done(&self) -> u32 {
        self.passed + self.failed
    }
}

/// The compact live QoS panel pinned beneath nextest's output during the run
/// (`docs/qos-design.md` §8). Three lines under a top rule: per-tier running
/// (from live reservation Leases) against the planning total; the committed
/// reserve and a utilization gauge vs probed free capacity; and the test
/// pass/fail progress plus wall clock. Reports only what the ledger knows, so the
/// `n/m` is running / planned, not a queue depth. No bottom rule; the console
/// ([`crate::cli::console`]) sizes the panel region to the returned line count.
pub fn render_live_panel(
    snapshot: &LiveSnapshot,
    plan: &QosPlan,
    free: &Resources,
    progress: &RunProgress,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(320);
    let dot = theme.chars.dot.style(theme.styles.dim);
    // The spinner advances on every redraw the run loop triggers (sub-second
    // cadence, independent of cluster polling), so the panel animates even when
    // no reservation or test verdict has changed: the "is the cluster still
    // alive?" heartbeat.
    let spin = spinner_glyph(progress.elapsed);

    render_label_rule(&mut out, theme);

    // When the per-test capacity re-probe was unavailable, `free` is ZERO. Don't
    // render a misleading empty gauge "of 0c / 0 GiB free"; say so, as the
    // preflight block does.
    let capacity = if free.cpu_milli == 0 && free.mem_bytes == 0 {
        "capacity unknown (probe unavailable)".to_string()
    } else {
        let bar = render_progress_bar(used_percent(&snapshot.committed, free), theme);
        format!("{bar} of {} free", agg_str(free).style(theme.styles.count))
    };
    writeln!(
        out,
        "{:>width$} {} {} running {dot} {} committed {dot} {capacity}",
        "Running".style(theme.styles.pass),
        spin.style(theme.styles.count),
        snapshot.total_running().style(theme.styles.count),
        agg_str(&snapshot.committed).style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    // Test progress + wall clock: `done/total` (or bare `done` when total is
    // unknown), pass/fail tallies, and elapsed time. Answers "are tests actually
    // completing?" as the per-test stream scrolls past beneath the panel.
    let done = match progress.total {
        0 => format!("{} done", progress.done().style(theme.styles.count)),
        total => format!(
            "{}/{} done",
            progress.done().style(theme.styles.count),
            total.style(theme.styles.count),
        ),
    };
    let failed = if progress.failed > 0 {
        format!(
            " {dot} {} {}",
            progress.failed.style(theme.styles.fail),
            "failed".style(theme.styles.fail),
        )
    } else {
        String::new()
    };
    writeln!(
        out,
        "{INDENT}{done} {dot} {} passed{failed} {dot} {}",
        progress.passed.style(theme.styles.pass),
        format_elapsed(progress.elapsed).style(theme.styles.dim),
    )
    .expect("write to string");

    if !plan.tiers.is_empty() {
        let parts: Vec<String> = plan
            .tiers
            .iter()
            .map(|t| {
                let run = snapshot.running.get(&t.class).map(|x| x.count).unwrap_or(0);
                format!("{} {}/{}", tier_label(t.class), run, t.count)
            })
            .collect();
        writeln!(
            out,
            "{INDENT}{} {dot} running / planned",
            parts.join(&format!(" {} ", theme.chars.dot)),
        )
        .expect("write to string");
    }

    out
}

/// Compact status panel for the unified bottom console (`cli::console`) during
/// the preflight, build, and image phases.
///
/// Where [`render`] is the tall multi-section banner (now used only for the
/// non-TTY CI log), this is the few-line summary pinned at the bottom while
/// `cargo nextest list` and `docker build` output scrolls above into native
/// scrollback. It is the preflight counterpart of [`render_live_panel`] (the
/// run-phase panel), so the two share one visual language across the session.
///
/// Three rows: a top rule, a cluster line, and a build/archives/scheduling line.
/// No bottom rule; the console sizes its panel region to the returned line count.
/// `elapsed` drives the spinner heartbeat; `phase` is the right-aligned action
/// label (`Preflight`, `Building`).
pub fn render_preflight_panel(
    state: &BannerState,
    phase: &str,
    elapsed: std::time::Duration,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(256);
    let dot = theme.chars.dot.style(theme.styles.dim);
    let spin = spinner_glyph(elapsed);

    render_label_rule(&mut out, theme);

    // Cluster line: context, ready nodes, slot usage.
    let c = &state.cluster;
    writeln!(
        out,
        "{:>width$} {} {} {dot} {} ready {dot} {} / {} slots",
        phase.style(theme.styles.pass),
        spin.style(theme.styles.count),
        c.context,
        c.nodes_ready.style(theme.styles.count),
        c.slots_used.style(theme.styles.count),
        c.slots_total.style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    // Build · archives · scheduling line.
    let build = match &state.build {
        BuildState::Pending => "inventory queued".to_string(),
        BuildState::Compiling { started_at } => {
            format!("compiling {}", format_elapsed(started_at.elapsed()))
        }
        BuildState::Indexing { started_at } => {
            format!("indexing {}", format_elapsed(started_at.elapsed()))
        }
        BuildState::Ok {
            test_count,
            binary_count,
            ..
        } => format!("{test_count} tests / {binary_count} bins"),
        BuildState::Failed { .. } => "build failed".to_string(),
    };
    let cached = state
        .archives
        .iter()
        .filter(|a| matches!(a.status, ArchiveStatus::Cached { .. }))
        .count();
    write!(
        out,
        "{INDENT}{} {dot} {} / {} archives",
        build.style(theme.styles.count),
        cached.style(theme.styles.count),
        state.archives.len().style(theme.styles.count),
    )
    .expect("write to string");
    if let Some(plan) = &state.qos_plan {
        write!(out, " {dot} {} waves", plan.waves.style(theme.styles.count),)
            .expect("write to string");
    }
    out.push('\n');

    out
}

/// The pinned panel shown while a Ctrl-C is being honoured. Rendered by the
/// console's render thread (which has no [`BannerState`]), so it stands alone:
/// the branded rule plus a single spinner line. `elapsed` keeps the spinner
/// animating while subprocesses are torn down.
pub fn render_cancel_panel(elapsed: std::time::Duration, theme: &Theme) -> String {
    let mut out = String::with_capacity(128);
    let dot = theme.chars.dot.style(theme.styles.dim);
    render_label_rule(&mut out, theme);
    write!(
        out,
        "{:>width$} {} terminating subprocesses… {dot} {}",
        "Cancelling".style(theme.styles.skip),
        spinner_glyph(elapsed).style(theme.styles.skip),
        "Ctrl-C again to force quit".style(theme.styles.dim),
        width = LABEL_WIDTH,
    )
    .expect("write to string");
    out.push('\n');
    out
}

// ─────────────────────────── per-row writers ──────────────────────────

fn write_archive_row(
    out: &mut String,
    row: &ArchiveRow,
    name_col: usize,
    dot: &impl std::fmt::Display,
    theme: &Theme,
) {
    let (marker, marker_style) = match &row.status {
        ArchiveStatus::Cached { .. } => (theme.chars.ok, theme.styles.pass),
        ArchiveStatus::Downloading { .. } => (theme.chars.progress, theme.styles.count),
        ArchiveStatus::Missing { .. } => (theme.chars.warn, theme.styles.skip),
    };
    write!(
        out,
        "{INDENT}{} {:<width$} {dot} ",
        marker.style(marker_style),
        row.name,
        width = name_col,
    )
    .expect("write to string");
    write_archive_detail(out, &row.status, theme);
    out.push('\n');
}

fn write_archive_detail(out: &mut String, status: &ArchiveStatus, theme: &Theme) {
    let dot = theme.chars.dot.style(theme.styles.dim);
    match status {
        ArchiveStatus::Cached { size_bytes } => {
            write!(
                out,
                "{} {dot} {}",
                "cached".style(theme.styles.pass),
                ByteSize::b(*size_bytes)
                    .display()
                    .iec()
                    .style(theme.styles.count),
            )
            .expect("write to string");
        }
        ArchiveStatus::Downloading {
            source,
            bytes_done,
            bytes_total,
        } => {
            let source_label = match source {
                DownloadSource::Lfs => "LFS",
                DownloadSource::ClusterCache => "cluster cache",
            };
            let percent = status.download_progress().map(|(p, _, _)| p).unwrap_or(0);
            let bar = render_progress_bar(percent, theme);
            write!(
                out,
                "downloading from {source_label} {bar} {} {dot} {} / {}",
                format_args!("{percent}%").style(theme.styles.count),
                ByteSize::b(*bytes_done)
                    .display()
                    .iec()
                    .style(theme.styles.count),
                ByteSize::b(*bytes_total)
                    .display()
                    .iec()
                    .style(theme.styles.count),
            )
            .expect("write to string");
        }
        ArchiveStatus::Missing { detail } => {
            write!(out, "missing {dot} {}", detail.style(theme.styles.dim),)
                .expect("write to string");
        }
    }
}

fn write_snapshot_row(
    out: &mut String,
    row: &SnapshotRow,
    name_col: usize,
    dot: &impl std::fmt::Display,
    theme: &Theme,
) {
    let (marker, marker_style) = match &row.status {
        SnapshotStatus::BoundReady => (theme.chars.ok, theme.styles.pass),
        SnapshotStatus::Provisioning { .. } => (theme.chars.progress, theme.styles.count),
    };
    write!(
        out,
        "{INDENT}{} {:<width$} {dot} ",
        marker.style(marker_style),
        row.pvc,
        width = name_col,
    )
    .expect("write to string");
    match &row.status {
        SnapshotStatus::BoundReady => {
            write!(
                out,
                "{} {dot} {}",
                "bound".style(theme.styles.pass),
                "ready".style(theme.styles.pass),
            )
            .expect("write to string");
        }
        SnapshotStatus::Provisioning { from_archive } => {
            write!(out, "provisioning from {from_archive}").expect("write to string");
        }
    }
    out.push('\n');
}

// ─────────────────────────── helpers ──────────────────────────────────

fn render_progress_bar(percent: u8, theme: &Theme) -> String {
    let pct = percent.min(100) as usize;
    let filled = pct * PROGRESS_BAR_WIDTH / 100;
    let empty = PROGRESS_BAR_WIDTH - filled;
    format!(
        "{}{}{}{}",
        "[".style(theme.styles.dim),
        theme
            .chars
            .bar_fill
            .repeat(filled)
            .style(theme.styles.count),
        theme.chars.bar_empty.repeat(empty).style(theme.styles.dim),
        "]".style(theme.styles.dim),
    )
}

/// Column width for a name column: max(items) clamped to a sane range so one
/// very-long name can't push detail off the right.
fn column_width<'a>(names: impl IntoIterator<Item = &'a str>, min: usize, max: usize) -> usize {
    names
        .into_iter()
        .map(|n| n.len())
        .max()
        .unwrap_or(0)
        .clamp(min, max)
}

// ─────────────────────────── tests ────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;

    fn sample_state() -> BannerState {
        BannerState {
            cluster: ClusterState {
                context: "kind-zaino-local".to_string(),
                slots_used: 12,
                slots_total: 16,
                slots_configured: 6,
                nodes_ready: 3,
                nodes_cordoned: 0,
                capacity: crate::qos::ClusterCapacity {
                    allocatable: Resources::new(12_000, 48 * GIB),
                    requested: Resources::new(6_000, 20 * GIB),
                    baseline: Resources::new(2_000, 8 * GIB),
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
                    status: ArchiveStatus::Cached {
                        size_bytes: 432_013_312,
                    },
                },
                ArchiveRow {
                    name: "testnet-2.6m".to_string(),
                    status: ArchiveStatus::Cached {
                        size_bytes: 19_754_106_880,
                    },
                },
                ArchiveRow {
                    name: "testnet-3.1m".to_string(),
                    status: ArchiveStatus::Downloading {
                        source: DownloadSource::Lfs,
                        bytes_done: 19_241_454_485,
                        bytes_total: 30_064_771_072,
                    },
                },
                ArchiveRow {
                    name: "mainnet-snapshot-9.0".to_string(),
                    status: ArchiveStatus::Missing {
                        detail: "LFS pointer present, blob absent".to_string(),
                    },
                },
            ],
            snapshots: vec![
                SnapshotRow {
                    pvc: "pvc/zebra-testnet-cache".to_string(),
                    status: SnapshotStatus::BoundReady,
                },
                SnapshotRow {
                    pvc: "pvc/zebra-mainnet-cache".to_string(),
                    status: SnapshotStatus::Provisioning {
                        from_archive: "testnet-3.1m".to_string(),
                    },
                },
            ],
            future: vec![],
            qos_plan: None,
        }
    }

    #[test]
    fn preflight_panel_is_three_lines_and_summarizes_phase() {
        let mut state = sample_state();
        state.qos_plan = Some(crate::qos::schedule::plan(
            &std::collections::BTreeMap::from([(QosClass::Basic, 6)]),
            Some(Resources::new(12_000, 48 * GIB)),
        ));
        let s = render_preflight_panel(
            &state,
            "Preflight",
            std::time::Duration::from_secs(3),
            &plain_unicode_theme(),
        );
        // Top rule + cluster line + build/archives/scheduling line, no bottom
        // rule, so the only separator is at the top.
        assert_eq!(s.lines().count(), 3, "fixed 3-row panel:\n{s}");
        assert!(
            !s.trim_end().ends_with("────────────"),
            "no bottom rule:\n{s}"
        );
        assert!(s.contains("Preflight"), "phase label:\n{s}");
        assert!(s.contains("kind-zaino-local"), "cluster context:\n{s}");
        assert!(s.contains("47 tests / 8 bins"), "build summary:\n{s}");
        assert!(s.contains("2 / 4 archives"), "archives cached/total:\n{s}");
        assert!(s.contains("waves"), "scheduling summary:\n{s}");
    }

    /// Theme with no colours and Unicode glyphs: what `Theme::detect()` returns
    /// under a UTF-8 locale with `NO_COLOR=1`, letting us snapshot byte-exact
    /// output.
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

    Archives 4 selected
             ✓ regtest-nu5-h128     · cached · 412.0 MiB
             ✓ testnet-2.6m         · cached · 18.4 GiB
             ⇣ testnet-3.1m         · downloading from LFS [███████░░░░░] 64% · 17.9 GiB / 28.0 GiB
             ! mainnet-snapshot-9.0 · missing · LFS pointer present, blob absent

   Snapshots 2 selected
             ✓ pvc/zebra-testnet-cache  · bound · ready
             ⇣ pvc/zebra-mainnet-cache  · provisioning from testnet-3.1m
────────────
";
        assert_eq!(
            s, expected,
            "golden mismatch.\n--- got ---\n{s}\n--- want ---\n{expected}"
        );
    }

    #[test]
    fn ascii_fallback_strips_unicode_glyphs() {
        let s = render(&sample_state(), &plain_ascii_theme());
        assert!(!s.contains('─'), "ascii leaked hbar:\n{s}");
        assert!(!s.contains('│'), "ascii leaked vert rule:\n{s}");
        assert!(!s.contains('·'), "ascii leaked dot:\n{s}");
        assert!(s.contains("------------"), "ascii hbar missing:\n{s}");
        assert!(s.contains("OK regtest-nu5-h128"), "ascii ok marker:\n{s}");
        assert!(s.contains(".. testnet-3.1m"), "ascii progress marker:\n{s}");
        assert!(
            s.contains("WARN mainnet-snapshot-9.0"),
            "ascii warn marker:\n{s}"
        );
    }

    #[test]
    fn colorized_render_contains_ansi_escapes() {
        let s = render(&sample_state(), &colorized_unicode_theme());
        assert!(s.contains("\x1b["), "colorized output missing ESC:\n{s}");
        // ANSI sequences must not affect the visible text: "Preflight" is still
        // present as a substring.
        assert!(s.contains("Preflight"), "Preflight label missing:\n{s}");
    }

    #[test]
    fn empty_lists_render_zero_count() {
        let mut state = sample_state();
        state.archives.clear();
        state.snapshots.clear();
        let s = render(&state, &plain_unicode_theme());
        assert!(s.contains("Archives 0 selected"), "got:\n{s}");
        assert!(s.contains("Snapshots 0 selected"), "got:\n{s}");
    }

    #[test]
    fn future_rows_render_as_a_labeled_section() {
        let mut state = sample_state();
        state.future = vec![
            FutureRow { label: "tier" },
            FutureRow { label: "queue" },
            FutureRow {
                label: "reservation",
            },
        ];
        let s = render(&state, &plain_unicode_theme());
        // Section header.
        assert!(
            s.contains("Scheduling 3 planned"),
            "missing scheduling header:\n{s}"
        );
        // Rows are dot-separated and tagged planned.
        assert!(s.contains("tier"), "got:\n{s}");
        assert!(s.contains("queue"), "got:\n{s}");
        assert!(s.contains("reservation"), "got:\n{s}");
        assert!(s.contains("planned (allocator pending)"), "got:\n{s}");
        // Blank line separator landed before the scheduling block.
        assert!(
            s.contains("\n\n  Scheduling"),
            "missing blank separator:\n{s}"
        );
    }

    #[test]
    fn qos_plan_renders_tiers_waves_and_unschedulable_warning() {
        use crate::qos::schedule;
        use std::collections::BTreeMap;
        let mut state = sample_state();
        // sync (8c/16Gi) can't fit a 4-core/8-GiB cluster, so it's
        // unschedulable; basic + integration schedule normally.
        state.qos_plan = Some(schedule::plan(
            &BTreeMap::from([
                (QosClass::Basic, 3),
                (QosClass::Integration, 1),
                (QosClass::Sync, 2),
            ]),
            Some(Resources::new(4000, 8 * GIB)),
        ));
        let s = render(&state, &plain_unicode_theme());
        // Header: total test count + a wave estimate.
        assert!(s.contains("Scheduling 6 tests"), "missing header:\n{s}");
        assert!(s.contains("waves"), "missing wave estimate:\n{s}");
        // Per-tier rows (priority order: sync, integration, basic) with
        // footprints (basic's half-core renders as 500m, not 0c).
        assert!(s.contains("integration"), "got:\n{s}");
        assert!(
            s.contains("500m / 512 MiB"),
            "missing basic footprint:\n{s}"
        );
        // Unschedulable warning for sync (16c/32Gi can't fit a 4c/8Gi cluster).
        assert!(
            s.contains("sync needs 16c / 32 GiB") && s.contains("will be rejected"),
            "missing unschedulable warning:\n{s}"
        );
        // Deferred live view note.
        assert!(s.contains("live view during run (pending)"), "got:\n{s}");
    }

    #[test]
    fn live_panel_shows_running_over_planned_and_a_gauge() {
        use crate::qos::live::{LiveSnapshot, TierLive};
        use crate::qos::schedule;
        use std::collections::BTreeMap;

        let plan = schedule::plan(
            &BTreeMap::from([(QosClass::Sync, 2), (QosClass::Basic, 3)]),
            Some(Resources::new(12_000, 48 * GIB)),
        );
        let snapshot = LiveSnapshot {
            running: BTreeMap::from([
                (
                    QosClass::Sync,
                    TierLive {
                        count: 1,
                        reserve: Resources::new(8_000, 16 * GIB),
                    },
                ),
                (
                    QosClass::Basic,
                    TierLive {
                        count: 2,
                        reserve: Resources::new(1_000, GIB),
                    },
                ),
            ]),
            committed: Resources::new(9_000, 17 * GIB),
            by_sa: BTreeMap::new(),
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
            &Resources::new(12_000, 48 * GIB),
            &progress,
            &plain_unicode_theme(),
        );
        assert!(s.contains("3 running"), "header:\n{s}");
        assert!(s.contains("9c / 17 GiB committed"), "committed:\n{s}");
        // Test progress line: done/total, passed, failed, elapsed.
        assert!(s.contains("8/20 done"), "done count:\n{s}");
        assert!(s.contains("7 passed"), "passed count:\n{s}");
        assert!(s.contains("1 failed"), "failed count:\n{s}");
        // Per-tier running/planned in priority order (sync before basic).
        assert!(s.contains("sync 1/2"), "got:\n{s}");
        assert!(s.contains("basic 2/3"), "got:\n{s}");
        let sync_at = s.find("sync 1/2").unwrap();
        let basic_at = s.find("basic 2/3").unwrap();
        assert!(sync_at < basic_at, "priority order:\n{s}");
        assert!(s.contains("running / planned"), "legend:\n{s}");
        // The branded separator rule appears only at the top; no bottom rule
        // (and so no trailing blank when the console sizes the panel region).
        assert!(s.starts_with("───── Ztest ─────"), "top rule present:\n{s}");
        assert!(
            !s.trim_end().ends_with("────────────"),
            "no bottom rule:\n{s}"
        );
    }

    #[test]
    fn live_panel_with_unknown_capacity_says_so_instead_of_a_zero_gauge() {
        use crate::qos::live::LiveSnapshot;
        use crate::qos::schedule;
        use std::collections::BTreeMap;

        let plan = schedule::plan(&BTreeMap::from([(QosClass::Basic, 2)]), None);
        let snapshot = LiveSnapshot {
            committed: Resources::new(1_000, GIB),
            ..LiveSnapshot::default()
        };
        // free == ZERO ⇒ the per-test probe was unavailable.
        let s = render_live_panel(
            &snapshot,
            &plan,
            &Resources::ZERO,
            &RunProgress::default(),
            &plain_unicode_theme(),
        );
        assert!(
            s.contains("capacity unknown (probe unavailable)"),
            "got:\n{s}"
        );
        assert!(
            !s.contains("of 0c"),
            "should not show a zero-free gauge:\n{s}"
        );
    }

    #[test]
    fn no_qos_plan_renders_no_scheduling_block() {
        let mut state = sample_state();
        state.qos_plan = None;
        state.future = vec![]; // also no placeholder rows
        let s = render(&state, &plain_unicode_theme());
        assert!(
            !s.contains("Scheduling"),
            "unexpected scheduling block:\n{s}"
        );
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
        // free = 12-6 cores / 48-20 GiB; gauge driven by the tighter dim.
        assert!(
            s.contains("capacity · 6 / 12 cores · 28 / 48 GiB free [██████░░░░░░] 50%"),
            "capacity line wrong:\n{s}"
        );
    }

    #[test]
    fn capacity_line_degrades_to_zero_before_the_probe_lands() {
        let mut state = sample_state();
        state.cluster.capacity = crate::qos::ClusterCapacity::default();
        let s = render(&state, &plain_unicode_theme());
        // All-zero capacity renders 0/0 and a 0% gauge, no panic / div-by-zero.
        assert!(
            s.contains("capacity · 0 / 0 cores · 0 / 0 GiB free [░░░░░░░░░░░░] 0%"),
            "zero-capacity line wrong:\n{s}"
        );
    }

    #[test]
    fn free_percent_uses_the_tighter_dimension() {
        // CPU 50% free, memory 25% free → min = 25.
        let free = Resources::new(2_000, GIB);
        let alloc = Resources::new(4_000, 4 * GIB);
        assert_eq!(free_percent(&free, &alloc), 25);
        // Zero allocatable → 0, no panic.
        assert_eq!(free_percent(&Resources::ZERO, &Resources::ZERO), 0);
    }

    #[test]
    fn progress_bar_clamps_overflow() {
        let theme = plain_unicode_theme();
        let bar0 = render_progress_bar(0, &theme);
        assert_eq!(bar0, "[░░░░░░░░░░░░]");
        let bar100 = render_progress_bar(100, &theme);
        assert_eq!(bar100, "[████████████]");
        let bar250 = render_progress_bar(250, &theme);
        assert_eq!(bar250, "[████████████]", "should clamp at 100%");
    }
}
