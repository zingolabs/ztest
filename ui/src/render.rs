use std::fmt::Write as _;

use bytesize::ByteSize;
use owo_colors::OwoColorize;

use super::layout::*;
use super::text::meter;
use super::theme::Theme;
use super::{
    ArchiveRow, ArchiveStatus, BannerState, BuildState, QosPlan, SyncVitals, SyncWatchState,
    TierPlan, TransferKind, TransferProgress, TransferRow, Transfers,
};
use ztest::api::BuildStage;
use ztest::api::LiveSnapshot;
use ztest::api::Resources;
use ztest::api::RunProgress;
use ztest::api::{byte_pair, byte_rate, column_width, compact, format_elapsed, thousands};

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

    // One global figure (allocatable − requested); gauge = free headroom, driven
    // by the tighter dimension
    let alloc = c.capacity.allocatable;
    let free = c.capacity.free();
    let pct = free_percent(&free, &alloc);
    let bar = meter(pct, theme);
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
        BuildState::Compiling { started_at, phase } => {
            let elapsed = started_at.elapsed();
            let label = phase.as_deref().unwrap_or("compiling test binaries");
            writeln!(
                out,
                "{:>width$} {} {label}… {dot} {}",
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
        BuildState::Ok { test_count, binary_count, elapsed } => {
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
        BuildState::Failed { exit_code, stage, elapsed } => {
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
            plan.peak.style(theme.styles.count),
            plan.total.style(theme.styles.count),
            width = LABEL_WIDTH,
        )
        .expect("write to string"),
        None => writeln!(
            out,
            "{:>width$} {} tests {dot} {} reserved total {dot} capacity unknown (probe unavailable)",
            "Scheduling".style(theme.styles.pass),
            total_tests.style(theme.styles.count),
            plan.total.style(theme.styles.count),
            width = LABEL_WIDTH,
        )
        .expect("write to string"),
    }

    let name_col = column_width(plan.tiers.iter().map(|t| t.class.as_label()), 12, 16);
    for TierPlan { class, count, per_test, subtotal } in &plan.tiers {
        // "X each" only when uniform; mixed (an override in the tier) → subtotal
        let amount = match per_test {
            Some(each) => format!("{each} each"),
            None => format!("{subtotal} total {dot} mixed footprints"),
        };
        writeln!(
            out,
            "{INDENT}{:<width$} {} {dot} {amount}",
            class.as_label().style(theme.styles.dim),
            count.style(theme.styles.count),
            width = name_col,
        )
        .expect("write to string");
    }

    // Fail-fast on a test admission will reject; reserve carried on the rejection
    // (override → tier no longer determines it)
    let warn = theme.chars.warn.style(theme.styles.skip);
    for u in &plan.unschedulable {
        writeln!(
            out,
            "{INDENT}{warn} {} needs {} {dot} exceeds cluster capacity — will be rejected",
            u.class.as_label().style(theme.styles.skip),
            u.admitted,
        )
        .expect("write to string");
    }
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
    let dot = theme.chars.dot.style(theme.styles.dim);
    // Advances per redraw, independent of cluster polling = "still alive?" heartbeat
    let spin = spinner_glyph(progress.elapsed);

    render_label_rule(&mut out, theme);

    // `free` == 0 means the re-probe was unavailable; say so, never draw an empty gauge
    let capacity = if free.cpu_milli == 0 && free.mem_bytes == 0 {
        "capacity unknown (probe unavailable)".to_string()
    } else {
        let bar = meter(used_percent(&snapshot.committed, free), theme);
        format!("{bar} of {} free", free.style(theme.styles.count))
    };
    writeln!(
        out,
        "{:>width$} {} {} running {dot} {} committed {dot} {capacity}",
        "Running".style(theme.styles.pass),
        spin.style(theme.styles.count),
        snapshot.total_running().style(theme.styles.count),
        snapshot.committed.style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    // Bare `done` when total unknown
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
                format!("{} {}/{}", t.class.as_label(), run, t.count)
            })
            .collect();
        writeln!(
            out,
            "{INDENT}{} {dot} running / planned",
            parts.join(&format!(" {} ", theme.chars.dot)),
        )
        .expect("write to string");
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
    let dot = theme.chars.dot.style(theme.styles.dim);
    let spin = spinner_glyph(elapsed);
    let c = &state.cluster;

    render_label_rule(&mut out, theme);

    // Line 1 — cluster
    writeln!(
        out,
        "{:>width$} {} {} {dot} {} ready {dot} {}/{} slots",
        phase.style(theme.styles.pass),
        spin.style(theme.styles.count),
        c.context,
        c.nodes_ready.style(theme.styles.count),
        c.slots_used.style(theme.styles.count),
        c.slots_total.style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    // Line 2 — capacity gauge (tighter of cpu/mem). Own label + compact units keep
    // the line unclipped
    let alloc = c.capacity.allocatable;
    let free = c.capacity.free();
    let pct = free_percent(&free, &alloc);
    let bar = meter(pct, theme);
    writeln!(
        out,
        "{:>width$} {bar} {} {dot} {}/{}c {dot} {}/{}Gi free",
        "capacity".style(theme.styles.dim),
        format_args!("{pct}%").style(theme.styles.count),
        cores_of(&free).style(theme.styles.count),
        cores_of(&alloc).style(theme.styles.count),
        gib_of(&free).style(theme.styles.count),
        gib_of(&alloc).style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    // Line 3 — inventory / build state
    render_build_line(&mut out, &state.build, spin, theme);

    // Line 4 — scheduling (blank without a QoS plan)
    if let Some(plan) = &state.qos_plan {
        let total_tests: u32 = plan.tiers.iter().map(|t| t.count).sum();
        match plan.free {
            Some(_) => writeln!(
                out,
                "{:>width$} {} tests {dot} {} waves {dot} peak {}",
                "Scheduling".style(theme.styles.pass),
                total_tests.style(theme.styles.count),
                plan.waves.style(theme.styles.count),
                plan.peak.style(theme.styles.count),
                width = LABEL_WIDTH,
            ),
            None => writeln!(
                out,
                "{:>width$} {} tests {dot} capacity unknown",
                "Scheduling".style(theme.styles.pass),
                total_tests.style(theme.styles.count),
                width = LABEL_WIDTH,
            ),
        }
        .expect("write to string");
    }

    pad_to_panel(&mut out);
    out
}

/// Shared `Inventory` line: `ztest sync start`'s panel and the run banner cannot
/// disagree about build state. `spin` = caller's per-frame glyph
fn render_build_line(out: &mut String, build: &BuildState, spin: &str, theme: &Theme) {
    let dot = theme.chars.dot.style(theme.styles.dim);
    let (build_marker, build_style, build_text) = match build {
        BuildState::Pending => (theme.chars.dot, theme.styles.dim, "queued".to_string()),
        BuildState::Compiling { started_at, phase } => (
            spin,
            theme.styles.count,
            format!(
                "{}… {dot} {}",
                phase.as_deref().unwrap_or("compiling test binaries"),
                format_elapsed(started_at.elapsed())
            ),
        ),
        BuildState::Indexing { started_at } => (
            spin,
            theme.styles.count,
            format!("indexing test selection… {dot} {}", format_elapsed(started_at.elapsed())),
        ),
        BuildState::Ok { test_count, binary_count, elapsed } => (
            theme.chars.ok,
            theme.styles.pass,
            format!("{test_count} tests / {binary_count} bins {dot} {}", format_elapsed(*elapsed)),
        ),
        BuildState::Failed { exit_code, .. } => {
            (theme.chars.warn, theme.styles.fail, format!("build failed (exit {exit_code})"))
        }
    };
    writeln!(
        out,
        "{:>width$} {} {build_text}",
        "Inventory".style(theme.styles.pass),
        build_marker.style(build_style),
        width = LABEL_WIDTH,
    )
    .expect("write to string");
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
    let dot = theme.chars.dot.style(theme.styles.dim);
    let spin = spinner_glyph(elapsed);

    render_label_rule(&mut out, theme);

    // Line 1 — phase + profile + sync id
    writeln!(
        out,
        "{:>width$} {} {} {dot} {}",
        phase.style(theme.styles.pass),
        spin.style(theme.styles.count),
        profile.style(theme.styles.count),
        sync_id.style(theme.styles.dim),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    // Line 2 — target cluster context
    writeln!(
        out,
        "{:>width$} {}",
        "cluster".style(theme.styles.dim),
        context,
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    // Line 3 — inventory / build state (shared with the run banner)
    render_build_line(&mut out, build, spin, theme);

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
    let dot = theme.chars.dot.style(theme.styles.dim);
    let spin = spinner_glyph(elapsed);

    render_label_rule(&mut out, theme);

    writeln!(
        out,
        "{:>width$} {} {} {dot} {}",
        "Watching".style(theme.styles.pass),
        spin.style(theme.styles.count),
        state.profile.style(theme.styles.count),
        state.sync_id.style(theme.styles.dim),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    match &state.vitals {
        Some(v) => render_sync_vitals(&mut out, state, v, elapsed, theme),
        None => render_sync_waiting(&mut out, state, elapsed, theme),
    }

    pad_to_panel(&mut out);
    out
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

/// `—` for both unmeasured and stale (one statement to the reader: not known now)
fn rate_text(rate: Option<f64>, stale: bool, unit: &str) -> String {
    match rate.filter(|_| !stale) {
        Some(r) if unit == "blk/s" => format!("{r:.1} {unit}"),
        Some(r) => format!("{}{unit}", compact(r)),
        None => "—".to_string(),
    }
}

fn render_sync_vitals(
    out: &mut String,
    state: &SyncWatchState,
    v: &SyncVitals,
    elapsed: std::time::Duration,
    theme: &Theme,
) {
    let dot = theme.chars.dot.style(theme.styles.dim);

    let target = match v.target {
        Some(t) => format!("{} / {}", thousands(v.height as u64), thousands(t as u64)),
        None => thousands(v.height as u64),
    };
    writeln!(
        out,
        "{:>width$} {} {dot} {} {}",
        "height".style(theme.styles.dim),
        target.style(theme.styles.count),
        format_args!("{:.1}%", v.pct).style(theme.styles.count),
        meter(v.pct.clamp(0.0, 100.0) as u8, theme),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    let stale = stale(v, elapsed);
    // tx/s beside blk/s: same window, same staleness → the two never disagree about
    // whether the subject is moving (phase rides the `Watching` row instead)
    let mut pace = format!(
        "{} {dot} {}",
        rate_text(v.pace.map(|p| p.per_sec), stale, "blk/s"),
        rate_text(v.tx_rate, stale, " tx/s").style(theme.styles.count),
    );
    // Suppressed with the stale rate it derives from (a projection off a frozen
    // rate counts down to a finish that is not happening)
    if let Some(eta) = v.pace.and_then(|p| p.eta).filter(|_| !stale) {
        pace.push_str(&format!(" {dot} eta {}", format_elapsed(eta)));
    }
    if v.reorg_depth > 0 {
        pace.push_str(&format!(
            " {dot} {}",
            format_args!("reorg -{}", v.reorg_depth).style(theme.styles.skip),
        ));
    }
    writeln!(out, "{:>width$} {pace}", "pace".style(theme.styles.dim), width = LABEL_WIDTH,)
        .expect("write to string");

    render_scan_trend(out, state, theme);
}

/// Scan rate over the run: sparkline + span + best rate. The peak makes the trend
/// actionable (a scan holding at half its demonstrated best = a regression nothing
/// else on the panel can state)
fn render_scan_trend(out: &mut String, state: &SyncWatchState, theme: &Theme) {
    use super::plot::{Palette, PlotOpts, plot_stacked};
    let dot = theme.chars.dot.style(theme.styles.dim);

    let body = match &state.timeline {
        Some(timeline) => {
            let bands = timeline.bands(ztest::api::BLOCKS);
            let opts = PlotOpts::new(SPARK_WIDTH, 1, theme.chars.graph);
            let spark = plot_stacked(
                &[(ztest::api::BLOCKS, bands)],
                &opts,
                &Palette::pools(theme.is_colorized()),
            )
            .pop()
            .unwrap_or_default();
            let peak = match timeline.peak(&[ztest::api::BLOCKS]) {
                Some(p) => format!(" {dot} peak {p:.0} blk/s"),
                None => String::new(),
            };
            format!(
                "{spark} {}{}",
                format_elapsed(timeline.span()).style(theme.styles.dim),
                peak.style(theme.styles.dim),
            )
        }
        None => "gathering".style(theme.styles.dim).to_string(),
    };
    writeln!(out, "{:>width$} {body}", "blocks".style(theme.styles.dim), width = LABEL_WIDTH,)
        .expect("write to string");
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
    let dot = theme.chars.dot.style(theme.styles.dim);
    writeln!(
        out,
        "{:>width$} {}",
        "cluster".style(theme.styles.dim),
        state.context,
        width = LABEL_WIDTH,
    )
    .expect("write to string");
    writeln!(
        out,
        "{:>width$} {} {dot} {}",
        "driver".style(theme.styles.dim),
        state.pod_phase.style(theme.styles.count),
        format_elapsed(elapsed).style(theme.styles.dim),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    let step = match &state.setup {
        Some(s) => format!(
            "{} {dot} {} {dot} {}",
            s.subject.style(theme.styles.count),
            s.detail,
            format_elapsed(elapsed.saturating_sub(s.received_at)).style(theme.styles.dim),
        ),
        // Say "no report yet" rather than leave a row that reads as a finished step
        None => format!("{}", "waiting for the driver's first report".style(theme.styles.dim)),
    };
    writeln!(out, "{:>width$} {step}", "setup".style(theme.styles.dim), width = LABEL_WIDTH,)
        .expect("write to string");
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
        writeln!(
            out,
            "{:>width$} {}",
            "work".style(theme.styles.dim),
            "awaiting first scrape".style(theme.styles.dim),
            width = METRIC_LABEL_WIDTH,
        )
        .expect("write to string");
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
    for (name, rate, bands) in measured.take(MAX_TRANSFER_ROWS.saturating_sub(1)) {
        let spark = plot_stacked(&[(name, bands)], &opts, &palette).pop().unwrap_or_default();
        writeln!(
            out,
            "{:>width$} {:>8} {spark}",
            name.style(theme.styles.dim),
            rate_text(*rate, stale, "/s").style(theme.styles.count),
            width = METRIC_LABEL_WIDTH,
        )
        .expect("write to string");
        drawn += 1;
    }
    if drawn == 0 {
        writeln!(
            out,
            "{:>width$} {}",
            "work".style(theme.styles.dim),
            "no pool measured".style(theme.styles.dim),
            width = METRIC_LABEL_WIDTH,
        )
        .expect("write to string");
    }

    // Total last, under its pools, carrying the sparkline span (else a reader
    // cannot tell ten minutes from two days of history)
    writeln!(
        out,
        "{:>width$} {:>8} {}",
        "total".style(theme.styles.dim),
        rate_text(vitals.work_rate, stale, "/s").style(theme.styles.count),
        format_elapsed(timeline.span()).style(theme.styles.dim),
        width = METRIC_LABEL_WIDTH,
    )
    .expect("write to string");

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
        writeln!(
            out,
            "{:>width$} {}",
            "load".style(theme.styles.dim),
            state.pods_note.as_deref().unwrap_or("awaiting first sample").style(theme.styles.dim),
            width = METRIC_LABEL_WIDTH,
        )
        .expect("write to string");
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
        writeln!(
            out,
            "{:>width$} {} {}",
            clip_pod(&load.pod).style(theme.styles.dim),
            cpu_text(load).style(theme.styles.count),
            mem_text(load).style(theme.styles.count),
            width = METRIC_LABEL_WIDTH,
        )
        .expect("write to string");
    }
    if hidden > 0 {
        writeln!(
            out,
            "{:>width$} {}",
            "".style(theme.styles.dim),
            format_args!("+{hidden} more").style(theme.styles.dim),
            width = METRIC_LABEL_WIDTH,
        )
        .expect("write to string");
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

/// `0.6/9c` against a limit, bare `0.6c` without one (no invented denominator)
fn cpu_text(load: &ztest::api::PodLoad) -> String {
    let used = load.usage.cpu_milli as f64 / 1000.0;
    match load.limit.as_ref().map(|l| l.cpu_milli).filter(|&c| c > 0) {
        Some(limit) => format!("{used:.1}/{}c", limit / 1000),
        None => format!("{used:.1}c"),
    }
}

fn mem_text(load: &ztest::api::PodLoad) -> String {
    let used = load.usage.mem_bytes as f64 / ztest::api::GIB as f64;
    match load.limit.as_ref().map(|l| l.mem_bytes).filter(|&m| m > 0) {
        Some(limit) => format!("{used:.1}/{}Gi", limit / ztest::api::GIB),
        None => format!("{used:.1}Gi"),
    }
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
    let dot = theme.chars.dot.style(theme.styles.dim);
    for row in rows.iter().take(visible) {
        write_transfer_row(&mut out, row, name_col, elapsed, &dot, theme);
    }
    if overflow > 0 {
        writeln!(out, "{} more transferring", format_args!("+{overflow}").style(theme.styles.dim),)
            .expect("write to string");
    }

    pad_to_panel(&mut out);
    out
}

/// One transfer line: marker, label, then a `%` bar (bytes known) or the note
fn write_transfer_row(
    out: &mut String,
    row: &TransferRow,
    name_col: usize,
    elapsed: std::time::Duration,
    dot: &impl std::fmt::Display,
    theme: &Theme,
) {
    // Direction glyph = kind; the spinner beside it is the heartbeat for rows
    // with no byte bar
    let glyph = transfer_glyph(row.kind, theme);
    let mut head = |marker: &str, style| {
        write!(
            out,
            "{}{} {:<name_col$} {dot} ",
            glyph.style(theme.styles.dim),
            marker.style(style),
            row.label,
        )
        .expect("write to string");
    };
    match &row.progress {
        TransferProgress::Stage(note) => {
            head(spinner_glyph(elapsed), theme.styles.count);
            writeln!(out, "{}", note.style(theme.styles.count)).expect("write to string");
        }
        TransferProgress::Bytes { done, total, pace } => {
            head(spinner_glyph(elapsed), theme.styles.count);
            let percent = if *total == 0 {
                0
            } else {
                ((*done as u128 * 100) / *total as u128).min(100) as u8
            };
            let bar = meter(percent, theme);
            write!(
                out,
                "{bar} {} {dot} {}",
                format_args!("{percent}%").style(theme.styles.count),
                byte_pair(*done, *total).style(theme.styles.count),
            )
            .expect("write to string");
            if let Some(pace) = pace {
                write!(out, " {dot} {}", byte_rate(pace.per_sec).style(theme.styles.count))
                    .expect("write to string");
                if let Some(eta) = pace.eta {
                    write!(
                        out,
                        " {dot} {}",
                        format_args!("{} left", format_elapsed(eta)).style(theme.styles.dim),
                    )
                    .expect("write to string");
                }
            }
            out.push('\n');
        }
        TransferProgress::Failed { detail } => {
            writeln!(
                out,
                "{}{} {:<name_col$} {dot} {}",
                glyph.style(theme.styles.dim),
                theme.chars.warn.style(theme.styles.fail),
                row.label,
                detail.style(theme.styles.dim),
            )
            .expect("write to string");
        }
    }
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
                ByteSize::b(*size_bytes).display().iec().style(theme.styles.count),
            )
            .expect("write to string");
        }
        ArchiveStatus::Missing { detail } => {
            write!(out, "missing {dot} {}", detail.style(theme.styles.dim),)
                .expect("write to string");
        }
    }
}

// ─────────────────────────── helpers ──────────────────────────────────

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

        let transfers = Transfers {
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
        };
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

    #[test]
    fn ascii_fallback_strips_unicode_glyphs() {
        let s = render(&sample_state(), &plain_ascii_theme());
        assert!(!s.contains('─'), "ascii leaked hbar:\n{s}");
        assert!(!s.contains('│'), "ascii leaked vert rule:\n{s}");
        assert!(!s.contains('·'), "ascii leaked dot:\n{s}");
        assert!(s.contains("------------"), "ascii hbar missing:\n{s}");
        assert!(s.contains("OK regtest-nu5-h128"), "ascii ok marker:\n{s}");
        assert!(s.contains("WARN mainnet-snapshot-9.0"), "ascii warn marker:\n{s}");
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
            phase: Some(ztest::api::Phase::Historic),
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
        assert!(s.contains("0.6/9c"), "cpu against its limit:\n{s}");
        assert!(s.contains("10.0/24Gi"), "memory against its limit:\n{s}");
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
        assert!(s.contains("1.5c"), "bare cpu:\n{s}");
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
