use std::fmt::Write as _;

use bytesize::ByteSize;
use owo_colors::OwoColorize;

use super::layout::*;
use super::text::{column_width, compact, format_elapsed, meter, thousands};
use super::theme::Theme;
use super::{
    ArchiveRow, ArchiveStatus, BannerState, BuildStage, BuildState, QosPlan, SyncVitals,
    SyncWatchState, TierPlan, TransferKind, TransferProgress, TransferRow, Transfers,
};
use crate::qos::Resources;
use crate::qos::live::LiveSnapshot;

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

/// One blank line, the section separator. A single `\n` so the live-renderer's
/// line counter doesn't double-count.
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

/// Whole CPU cores in a [`Resources`] (millicpu / 1000, rounded down).
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

/// `<cpu> / <mem>` for an aggregate (peak / total) reserve, in decimal cores and
/// GiB: `11.5c / 19.5 GiB` reads better than `11500m / 19968 MiB` for summed
/// figures.
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

    let name_col = column_width(plan.tiers.iter().map(|t| t.class.as_label()), 12, 16);
    for TierPlan {
        class,
        count,
        footprint,
    } in &plan.tiers
    {
        writeln!(
            out,
            "{INDENT}{:<width$} {} {dot} {} each",
            class.as_label().style(theme.styles.dim),
            count.style(theme.styles.count),
            footprint,
            width = name_col,
        )
        .expect("write to string");
    }

    // Fail-fast: a tier whose admitted total exceeds the whole cluster will be
    // rejected at admission, so surface it now. Uses `admitted` (components +
    // runner) to match the per-tier rows and what the scheduler actually checks.
    let warn = theme.chars.warn.style(theme.styles.skip);
    for class in &plan.unschedulable {
        writeln!(
            out,
            "{INDENT}{warn} {} needs {} {dot} exceeds cluster capacity — will be rejected",
            class.as_label().style(theme.styles.skip),
            class.profile().admitted(),
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
/// Live test-run progress for the during-run panel, populated by the run loop
/// (`cli::console`): `elapsed` drives the spinner/clock heartbeat, the counts
/// are tallied from relayed per-test result lines, and `total` (`0` = unknown)
/// is the preflight test count.
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

/// The **left column** during the run phase, the counterpart of
/// [`render_preflight_panel`]. Exactly [`PANEL_LINES`] lines under a branded
/// rule: committed reserve + running count + a utilization gauge, test
/// pass/fail progress + wall clock, and per-tier running against the planning
/// total. Reports only what the ledger knows, so `n/m` is running / planned,
/// not a queue depth.
pub fn render_live_panel(
    snapshot: &LiveSnapshot,
    plan: &QosPlan,
    free: &Resources,
    progress: &RunProgress,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(320);
    let dot = theme.chars.dot.style(theme.styles.dim);
    // Advances on every redraw (independent of cluster polling), so the panel
    // animates even when nothing has changed: the "still alive?" heartbeat.
    let spin = spinner_glyph(progress.elapsed);

    render_label_rule(&mut out, theme);

    // `free` is ZERO when the per-test capacity re-probe was unavailable; say so
    // rather than rendering a misleading empty gauge.
    let capacity = if free.cpu_milli == 0 && free.mem_bytes == 0 {
        "capacity unknown (probe unavailable)".to_string()
    } else {
        let bar = meter(used_percent(&snapshot.committed, free), theme);
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
    // unknown), pass/fail tallies, and elapsed time.
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

/// The **left column** of the pinned bottom console during the preflight,
/// build, and image phases; the counterpart of [`render_live_panel`], sharing
/// its constant [`PANEL_LINES`] height so the panel never reflows between
/// phases. Exactly [`PANEL_LINES`] lines: a branded rule, then cluster,
/// capacity, inventory, and scheduling lines (blank-padded when empty).
/// `elapsed` drives the spinner heartbeat; `phase` is the action label.
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

    // Line 1 — cluster: phase label + context, ready nodes, slot usage.
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

    // Line 2 — capacity gauge (free headroom, tighter of cpu/mem). Its own label
    // (rather than a wide indent) and compact units keep the line unclipped.
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

    // Line 3 — inventory / build state.
    render_build_line(&mut out, &state.build, spin, theme);

    // Line 4 — scheduling summary (blank when no QoS plan yet).
    if let Some(plan) = &state.qos_plan {
        let total_tests: u32 = plan.tiers.iter().map(|t| t.count).sum();
        match plan.free {
            Some(_) => writeln!(
                out,
                "{:>width$} {} tests {dot} {} waves {dot} peak {}",
                "Scheduling".style(theme.styles.pass),
                total_tests.style(theme.styles.count),
                plan.waves.style(theme.styles.count),
                agg_str(&plan.peak).style(theme.styles.count),
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

/// The shared `Inventory` line: build-phase marker + status text. Factored out
/// of [`render_preflight_panel`] so `ztest sync start`'s panel
/// ([`render_sync_build_panel`]) shows the identical build state. `spin` is the
/// caller's per-frame spinner glyph (used for the in-progress markers).
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
            format!(
                "indexing test selection… {dot} {}",
                format_elapsed(started_at.elapsed())
            ),
        ),
        BuildState::Ok {
            test_count,
            binary_count,
            elapsed,
        } => (
            theme.chars.ok,
            theme.styles.pass,
            format!(
                "{test_count} tests / {binary_count} bins {dot} {}",
                format_elapsed(*elapsed)
            ),
        ),
        BuildState::Failed { exit_code, .. } => (
            theme.chars.warn,
            theme.styles.fail,
            format!("build failed (exit {exit_code})"),
        ),
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

/// The **left column** during `ztest sync start`'s on-cluster build+provision.
/// Mirrors [`render_preflight_panel`]'s frame — the branded rule and the shared
/// `Inventory` build line — but shows the detached sync's own context (profile,
/// id, target cluster) in place of the run's probe/scheduling rows, which a
/// detached sync has no equivalent of. Held to [`PANEL_LINES`] like every panel.
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

    // Line 1 — phase + profile + sync id.
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

    // Line 2 — target cluster context.
    writeln!(
        out,
        "{:>width$} {}",
        "cluster".style(theme.styles.dim),
        context,
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    // Line 3 — inventory / build state (shared with the run banner).
    render_build_line(&mut out, build, spin, theme);

    pad_to_panel(&mut out);
    out
}

/// The left column while `ztest sync watch` tails a detached sync: the sync's
/// live vitals, or — before the driver's first tick — the pod phase that explains
/// the silence. Reuses the shared panel frame so a watched sync looks like the
/// build that launched it; only the rows differ.
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

/// The three vitals rows: chain position, pace, and the probe board summary.
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

    // Tick age, not wall-clock elapsed: a driver that has stopped publishing is
    // the failure this row exists to make visible.
    let age = elapsed.saturating_sub(v.received_at);
    let rate = match v.blocks_per_sec {
        Some(r) => format!("{r:.1} blk/s"),
        None => "—".to_string(),
    };
    let mut pace = format!("{rate} {dot} {}", v.phase.style(theme.styles.count));
    if let Some(eta) = v.eta {
        pace.push_str(&format!(" {dot} eta {}", format_elapsed(eta)));
    }
    if v.reorg_depth > 0 {
        pace.push_str(&format!(
            " {dot} {}",
            format_args!("reorg -{}", v.reorg_depth).style(theme.styles.skip),
        ));
    }
    writeln!(
        out,
        "{:>width$} {pace} {dot} {}",
        "pace".style(theme.styles.dim),
        format_args!("tick {}", format_elapsed(age)).style(theme.styles.dim),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    render_probe_summary(out, state, theme);
}

/// The probe board on one line: the satisfied tally, then whichever probe is
/// nearest to failing, with its window countdown when it has one.
fn render_probe_summary(out: &mut String, state: &SyncWatchState, theme: &Theme) {
    let dot = theme.chars.dot.style(theme.styles.dim);
    let (ok, total) = state.probe_tally();
    let tally_style = if ok == total {
        theme.styles.pass
    } else {
        theme.styles.count
    };

    let detail = match state.worst_probe() {
        Some(row) => {
            let marker = match row.state {
                crate::sync::ProbeState::Violating => theme.chars.fail,
                _ => theme.chars.warn,
            };
            let style = match row.state {
                crate::sync::ProbeState::Violating => theme.styles.fail,
                _ => theme.styles.skip,
            };
            let countdown = match (row.since_satisfied, row.window) {
                (Some(since), Some(window)) => {
                    format!(" {}/{}", format_elapsed(since), format_elapsed(window))
                }
                _ => String::new(),
            };
            format!(
                "{dot} {} {}{}",
                marker.style(style),
                row.name.style(style),
                countdown.style(theme.styles.dim),
            )
        }
        None => String::new(),
    };
    let violations = if state.violations > 0 {
        format!(
            "{dot} {}",
            format_args!("{} violation(s)", state.violations).style(theme.styles.fail),
        )
    } else {
        String::new()
    };

    writeln!(
        out,
        "{:>width$} {} {detail}{violations}",
        "probes".style(theme.styles.dim),
        format_args!("{ok}/{total} ok").style(tally_style),
        width = LABEL_WIDTH,
    )
    .expect("write to string");
}

/// The pre-first-tick rows: cluster, driver-pod phase, and the provisioning gate
/// the driver is currently in.
///
/// A sync spends the great majority of its wall-clock here — image pull, chain
/// provisioning, indexer index-build — so these rows are not a placeholder for the
/// vitals, they are the run's only progress display for minutes at a time. The
/// setup row in particular is what separates a slow gate from a hung one: its age
/// is time in *that* gate, not time since launch.
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
        // Before the driver's first report there is genuinely nothing to say about
        // provisioning; say that, rather than leaving a row that looks like a
        // finished step.
        None => format!(
            "{}",
            "waiting for the driver's first report".style(theme.styles.dim)
        ),
    };
    writeln!(
        out,
        "{:>width$} {step}",
        "setup".style(theme.styles.dim),
        width = LABEL_WIDTH,
    )
    .expect("write to string");
}

/// The **right column** of the `ztest sync watch` panel: the newest metrics
/// reading, one label per row. Held to [`PANEL_LINES`] with the top row blank,
/// matching [`render_transfers`].
///
/// Fed by a direct exporter scrape, not by a monitoring stack — a column that
/// repaints once per scrape interval is not a live display. Four blank rows read
/// as "everything is zero", so when there are no values the *reason* is named.
pub fn render_sync_metrics(reading: &crate::metrics::Reading, theme: &Theme) -> String {
    let mut out = String::with_capacity(320);
    out.push('\n');
    let dot = theme.chars.dot.style(theme.styles.dim);

    // Naming the cause is the point: "no samples" sent a reader hunting a broken
    // exporter when nothing was broken at all.
    if let Some(note) = &reading.note {
        writeln!(
            out,
            "{:>width$} {}",
            "metrics".style(theme.styles.dim),
            note.style(theme.styles.dim),
            width = METRIC_LABEL_WIDTH,
        )
        .expect("write to string");
        pad_to_panel(&mut out);
        return out;
    }

    let row = |out: &mut String, label: &str, body: String| {
        writeln!(
            out,
            "{:>width$} {body}",
            label.style(theme.styles.dim),
            width = METRIC_LABEL_WIDTH,
        )
        .expect("write to string");
    };
    let num = |label: &str| {
        reading
            .get(label)
            .map(compact)
            .unwrap_or_else(|| "—".to_string())
    };

    row(
        &mut out,
        "lag",
        format!(
            "{} blk {dot} reorgs {}",
            num("sync lag (blocks)").style(theme.styles.count),
            num("reorgs").style(theme.styles.count),
        ),
    );
    // Grouped in full, not abbreviated: the tip is read against the `height` row
    // opposite it, and `1.5M` can't be compared with `1,284,901`.
    let tip = reading
        .get("chain tip height")
        .map(|h| thousands(h.max(0.0) as u64))
        .unwrap_or_else(|| "—".to_string());
    row(&mut out, "tip", tip.style(theme.styles.count).to_string());
    row(
        &mut out,
        "indexed",
        format!(
            "{} tx",
            num("transactions indexed").style(theme.styles.count)
        ),
    );
    let latency = reading
        .get("gRPC mean latency (ms)")
        .map(|ms| format!("{ms:.1} ms"))
        .unwrap_or_else(|| "—".to_string());
    row(
        &mut out,
        "gRPC",
        format!(
            "{} req {dot} {}",
            num("gRPC requests").style(theme.styles.count),
            latency.style(theme.styles.count),
        ),
    );

    pad_to_panel(&mut out);
    out
}

/// Group a count with thin separators: heights and totals in the millions are
/// unreadable as a bare digit run at a glance.
/// The **right column** of the pinned bottom console: the live set of background
/// acquisitions (dev-image build+load, archive/seed downloads) tracked
/// independently of the scrolling main output. Exactly [`PANEL_LINES`] lines:
/// the top row is blank (aligning with the left column's branded rule), then up
/// to [`MAX_TRANSFER_ROWS`] transfer rows, the tail collapsing into a `+N more`
/// line. Blank-padded when idle so the panel height is constant across phases.
///
/// `elapsed` drives each active row's spinner (the "still moving" heartbeat).
pub fn render_transfers(
    transfers: &Transfers,
    elapsed: std::time::Duration,
    theme: &Theme,
) -> String {
    let mut out = String::with_capacity(320);
    // Top row blank: aligns the first transfer with the left column's cluster
    // line, leaving the branded rule to stand alone on its row.
    out.push('\n');

    let rows = &transfers.rows;
    let show = rows.len().min(MAX_TRANSFER_ROWS);
    // Reserve the last slot for `+N more` when the list overflows.
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
        writeln!(
            out,
            "{} more transferring",
            format_args!("+{overflow}").style(theme.styles.dim),
        )
        .expect("write to string");
    }

    pad_to_panel(&mut out);
    out
}

/// One right-column transfer line: an animated marker, the label, and either a
/// `%` bar (when bytes are known) or the sub-phase note.
fn write_transfer_row(
    out: &mut String,
    row: &TransferRow,
    name_col: usize,
    elapsed: std::time::Duration,
    dot: &impl std::fmt::Display,
    theme: &Theme,
) {
    // Direction glyph (⇡ upload / ⇣ download) conveys the kind; the spinner sits
    // beside it as the "still moving" heartbeat for rows without a byte bar.
    let glyph = transfer_glyph(row.kind, theme);
    match &row.progress {
        TransferProgress::Active { note, bytes } => {
            let marker = spinner_glyph(elapsed);
            write!(
                out,
                "{}{} {:<name_col$} {dot} ",
                glyph.style(theme.styles.dim),
                marker.style(theme.styles.count),
                row.label,
            )
            .expect("write to string");
            match bytes {
                Some((done, total)) => {
                    let percent = if *total == 0 {
                        0
                    } else {
                        ((*done as u128 * 100) / *total as u128).min(100) as u8
                    };
                    let bar = meter(percent, theme);
                    write!(
                        out,
                        "{bar} {} {dot} {} / {}",
                        format_args!("{percent}%").style(theme.styles.count),
                        ByteSize::b(*done).display().iec().style(theme.styles.count),
                        ByteSize::b(*total)
                            .display()
                            .iec()
                            .style(theme.styles.count),
                    )
                    .expect("write to string");
                    // A short qualifier after the counts (`layer 5/7`); the byte
                    // bar already carries the primary signal, so this is dimmed.
                    if !note.is_empty() {
                        write!(out, " {dot} {}", note.style(theme.styles.dim))
                            .expect("write to string");
                    }
                    out.push('\n');
                }
                None => {
                    writeln!(out, "{}", note.style(theme.styles.count)).expect("write to string");
                }
            }
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

/// The direction glyph for a transfer kind: `⇡` for an outgoing dev-image
/// build+load, `⇣` for an incoming archive/seed download.
fn transfer_glyph(kind: TransferKind, theme: &Theme) -> &'static str {
    match kind {
        TransferKind::Image => theme.chars.up,
        TransferKind::Download | TransferKind::Seed => theme.chars.progress,
    }
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
    use crate::qos::GIB;
    use crate::qos::QosClass;

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
                    name: "mainnet-snapshot-9.0".to_string(),
                    status: ArchiveStatus::Missing {
                        detail: "LFS pointer present, blob absent".to_string(),
                    },
                },
            ],
            qos_plan: None,
        }
    }

    #[test]
    fn preflight_panel_is_constant_height_and_summarizes_phase() {
        let mut state = sample_state();
        state.qos_plan = Some(crate::qos::schedule::plan(
            &std::collections::BTreeMap::from([(QosClass::Basic, 6)]),
            Some(Resources::new(12_000, 48 * GIB, 0, 0)),
        ));
        let s = render_preflight_panel(
            &state,
            "Preflight",
            std::time::Duration::from_secs(3),
            &plain_unicode_theme(),
        );
        // Constant-height left column: branded rule + cluster + capacity +
        // inventory + scheduling = PANEL_LINES rows, no bottom rule.
        assert_eq!(s.lines().count(), PANEL_LINES, "fixed-height panel:\n{s}");
        assert!(
            !s.trim_end().ends_with("────────────"),
            "no bottom rule:\n{s}"
        );
        assert!(s.contains("Preflight"), "phase label:\n{s}");
        assert!(s.contains("kind-zaino-local"), "cluster context:\n{s}");
        assert!(s.contains("capacity"), "capacity gauge:\n{s}");
        assert!(s.contains("47 tests / 8 bins"), "build summary:\n{s}");
        assert!(s.contains("waves"), "scheduling summary:\n{s}");
    }

    #[test]
    fn preflight_panel_is_constant_height_even_when_empty() {
        // Before any probe/build lands, the panel must still be exactly
        // PANEL_LINES so the viewport never reflows between phases.
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
        // Idle: still exactly PANEL_LINES (blank rows), so the column reserves its
        // space even with nothing in flight.
        let idle = render_transfers(&Transfers::default(), std::time::Duration::ZERO, &theme);
        assert_eq!(idle.lines().count(), PANEL_LINES, "idle height:\n{idle}");

        let transfers = Transfers {
            rows: vec![
                TransferRow {
                    label: "dev-zainod".to_string(),
                    kind: TransferKind::Image,
                    progress: TransferProgress::Active {
                        note: "building".to_string(),
                        bytes: None,
                    },
                },
                TransferRow {
                    label: "testnet-3.1m".to_string(),
                    kind: TransferKind::Download,
                    progress: TransferProgress::Active {
                        note: "downloading".to_string(),
                        bytes: Some((17_900_000_000, 28_000_000_000)),
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
        // A byte row still shows its note (e.g. `layer 5/7`) after the counts.
        assert!(s.contains("downloading"), "byte-row note:\n{s}");
        // Upload vs download direction glyphs.
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
                progress: TransferProgress::Active {
                    note: "building".to_string(),
                    bytes: None,
                },
            })
            .collect();
        let s = render_transfers(&Transfers { rows }, std::time::Duration::ZERO, &theme);
        assert_eq!(s.lines().count(), PANEL_LINES, "overflow height:\n{s}");
        assert!(s.contains("more transferring"), "overflow marker:\n{s}");
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

    Archives 3 selected
             ✓ regtest-nu5-h128     · cached · 412.0 MiB
             ✓ testnet-2.6m         · cached · 18.4 GiB
             ! mainnet-snapshot-9.0 · missing · LFS pointer present, blob absent
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
        let s = render(&state, &plain_unicode_theme());
        assert!(s.contains("Archives 0 selected"), "got:\n{s}");
    }

    #[test]
    fn qos_plan_renders_tiers_waves_and_unschedulable_warning() {
        use crate::qos::schedule;
        use std::collections::BTreeMap;
        let mut state = sample_state();
        // sync (17c/18Gi admitted) can't fit a 4-core/8-GiB cluster, so it's
        // unschedulable; basic + integration schedule normally.
        state.qos_plan = Some(schedule::plan(
            &BTreeMap::from([
                (QosClass::Basic, 3),
                (QosClass::Integration, 1),
                (QosClass::Sync, 2),
            ]),
            Some(Resources::new(4000, 8 * GIB, 0, 0)),
        ));
        let s = render(&state, &plain_unicode_theme());
        // Header: total test count + a wave estimate.
        assert!(s.contains("Scheduling 6 tests"), "missing header:\n{s}");
        assert!(s.contains("waves"), "missing wave estimate:\n{s}");
        // Per-tier rows (priority order: sync, integration, basic) with
        // footprints. CPU is always whole cores (integer allocations only), so
        // basic renders as `1c`, never a fractional `500m`.
        assert!(s.contains("integration"), "got:\n{s}");
        // Admitted total (components + runner): basic is 2c / 1 GiB.
        assert!(s.contains("2c / 1 GiB"), "missing basic footprint:\n{s}");
        // Unschedulable warning for sync (17c/18Gi admitted can't fit a 4c/8Gi cluster).
        assert!(
            s.contains("sync needs 17c / 18 GiB") && s.contains("will be rejected"),
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
            Some(Resources::new(12_000, 48 * GIB, 0, 0)),
        );
        let snapshot = LiveSnapshot {
            running: BTreeMap::from([
                (
                    QosClass::Sync,
                    TierLive {
                        count: 1,
                        reserve: Resources::new(8_000, 16 * GIB, 0, 0),
                    },
                ),
                (
                    QosClass::Basic,
                    TierLive {
                        count: 2,
                        reserve: Resources::new(1_000, GIB, 0, 0),
                    },
                ),
            ]),
            committed: Resources::new(9_000, 17 * GIB, 0, 0),
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
            &Resources::new(12_000, 48 * GIB, 0, 0),
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
            committed: Resources::new(1_000, GIB, 0, 0),
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
        let free = Resources::new(2_000, GIB, 0, 0);
        let alloc = Resources::new(4_000, 4 * GIB, 0, 0);
        assert_eq!(free_percent(&free, &alloc), 25);
        // Zero allocatable → 0, no panic.
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
            probes: Vec::new(),
            violations: 0,
            timeline: None,
        }
    }

    fn sample_vitals() -> SyncVitals {
        SyncVitals {
            height: 901,
            target: Some(1024),
            pct: 88.1,
            phase: "Historic".into(),
            reorg_depth: 0,
            blocks_per_sec: Some(12.4),
            eta: Some(std::time::Duration::from_secs(10)),
            // Transparent and sprout are tier B and nobody counted them;
            // Ironwood was counted and is genuinely idle. The panel has to
            // render that difference.
            work_rate: Some(23_600.0),
            pool_rates: vec![
                ("transparent", None),
                ("sprout", None),
                ("sapling", Some(19_400.0)),
                ("orchard", Some(4_200.0)),
                ("ironwood", Some(0.0)),
            ],
            received_at: std::time::Duration::from_secs(210),
        }
    }

    fn probe(name: &str, state: crate::sync::ProbeState) -> ProbeRow {
        ProbeRow {
            name: name.into(),
            state,
            since_satisfied: None,
            window: None,
        }
    }

    #[test]
    fn sync_watch_panel_shows_vitals_at_constant_height() {
        let mut state = watching(Some(sample_vitals()));
        state.probes = vec![
            probe("height_monotonic", crate::sync::ProbeState::Ok),
            probe("reached_tip", crate::sync::ProbeState::NotYet),
        ];
        let s = render_sync_watch_panel(
            &state,
            std::time::Duration::from_secs(212),
            &plain_unicode_theme(),
        );
        assert_eq!(s.lines().count(), PANEL_LINES, "fixed-height panel:\n{s}");
        assert!(s.contains("zaino_state_sync"), "profile:\n{s}");
        assert!(s.contains("901 / 1,024"), "height vs target:\n{s}");
        assert!(s.contains("88.1%"), "percentage:\n{s}");
        assert!(s.contains("12.4 blk/s"), "scan rate:\n{s}");
        assert!(s.contains("Historic"), "phase:\n{s}");
        assert!(s.contains("eta 10s"), "eta:\n{s}");
        // 212s frame - 210s tick = 2s of tick age.
        assert!(s.contains("tick 2s"), "tick age:\n{s}");
        assert!(s.contains("1/2 ok"), "probe tally:\n{s}");
        assert!(s.contains("reached_tip"), "worst probe named:\n{s}");
    }

    #[test]
    fn sync_watch_panel_falls_back_to_pod_phase_before_the_first_tick() {
        let s = render_sync_watch_panel(
            &watching(None),
            std::time::Duration::from_secs(45),
            &plain_unicode_theme(),
        );
        assert_eq!(s.lines().count(), PANEL_LINES, "fixed-height panel:\n{s}");
        assert!(s.contains("Running"), "pod phase:\n{s}");
        assert!(s.contains("crc-remote"), "cluster context:\n{s}");
        assert!(
            s.contains("waiting for the driver's first report"),
            "explains silence:\n{s}"
        );
    }

    /// The whole point of the setup row: a reader must be able to see which gate
    /// the minutes are going into, and how long *that gate* has been open —
    /// otherwise a slow provisioning step is indistinguishable from a hang.
    #[test]
    fn the_setup_row_names_the_current_gate_and_its_own_age() {
        let mut state = watching(None);
        state.setup = Some(SetupStep {
            subject: "zainod".into(),
            detail: "waiting for gRPC GetLightdInfo".into(),
            received_at: std::time::Duration::from_secs(60),
        });
        let s = render_sync_watch_panel(
            &state,
            std::time::Duration::from_secs(9 * 60),
            &plain_unicode_theme(),
        );
        assert_eq!(s.lines().count(), PANEL_LINES, "fixed-height panel:\n{s}");
        assert!(s.contains("zainod"), "names the component:\n{s}");
        assert!(s.contains("GetLightdInfo"), "names the gate:\n{s}");
        assert!(
            s.contains("8m"),
            "ages the gate, not the session (9m elapsed, gate opened at 1m):\n{s}"
        );
    }

    /// Once ticks arrive the vitals own those rows; a stale provisioning step must
    /// not linger beside live chain progress.
    #[test]
    fn the_setup_row_gives_way_to_the_vitals() {
        let mut state = watching(Some(sample_vitals()));
        state.setup = Some(SetupStep {
            subject: "zainod".into(),
            detail: "waiting for gRPC GetLightdInfo".into(),
            received_at: std::time::Duration::from_secs(60),
        });
        let s = render_sync_watch_panel(
            &state,
            std::time::Duration::from_secs(9 * 60),
            &plain_unicode_theme(),
        );
        assert!(!s.contains("GetLightdInfo"), "setup row persisted:\n{s}");
        assert!(s.contains("height"), "vitals absent:\n{s}");
    }

    #[test]
    fn a_reorg_and_violations_surface_on_the_panel() {
        let mut vitals = sample_vitals();
        vitals.reorg_depth = 7;
        let mut state = watching(Some(vitals));
        state.violations = 2;
        state.probes = vec![probe(
            "chain_continuity",
            crate::sync::ProbeState::Violating,
        )];
        let s = render_sync_watch_panel(
            &state,
            std::time::Duration::from_secs(212),
            &plain_unicode_theme(),
        );
        assert_eq!(s.lines().count(), PANEL_LINES, "fixed-height panel:\n{s}");
        assert!(s.contains("reorg -7"), "reorg depth:\n{s}");
        assert!(s.contains("2 violation(s)"), "violation count:\n{s}");
        assert!(s.contains("chain_continuity"), "violating probe:\n{s}");
    }

    #[test]
    fn a_draining_liveness_window_is_shown_as_a_countdown() {
        let mut state = watching(Some(sample_vitals()));
        state.probes = vec![ProbeRow {
            name: "no_stall".into(),
            state: crate::sync::ProbeState::Pending,
            since_satisfied: Some(std::time::Duration::from_secs(25)),
            window: Some(std::time::Duration::from_secs(30)),
        }];
        let s = render_sync_watch_panel(
            &state,
            std::time::Duration::from_secs(212),
            &plain_unicode_theme(),
        );
        assert!(s.contains("no_stall 25s/30s"), "window countdown:\n{s}");
    }

    /// Each empty-column cause must name itself. A single "no samples yet" for all
    /// of them sends the reader hunting a broken exporter when the truth may be
    /// that the engine simply has not started.
    #[test]
    fn the_metrics_column_distinguishes_why_it_is_empty() {
        let theme = plain_unicode_theme();
        let render = |reading: &crate::metrics::Reading| {
            let s = render_sync_metrics(reading, &theme);
            assert_eq!(s.lines().count(), PANEL_LINES, "fixed height:\n{s}");
            s
        };

        // Whatever the cause, the column states it instead of showing blank rows.
        // The causes themselves are derived where the reading is taken; this is
        // the renderer's half of that contract.
        let s = render(&reading_note("no metrics-exposing pod yet".into()));
        assert!(
            s.contains("no metrics-exposing pod"),
            "pre-target cause:\n{s}"
        );

        let s = render(&reading_note(format!(
            "scraping every {} · no series published yet",
            format_elapsed(crate::metrics::LIVE_PERIOD)
        )));
        assert!(
            s.contains("no series published yet"),
            "unscraped cause:\n{s}"
        );

        let s = render(&reading_note("unavailable · connection refused".into()));
        assert!(s.contains("unavailable"), "unreachable cause:\n{s}");
        assert!(s.contains("connection refused"), "carries the reason:\n{s}");
    }

    #[test]
    fn sync_metrics_column_shows_values_at_constant_height() {
        let theme = plain_unicode_theme();
        let s = render_sync_metrics(
            &reading(&[
                ("sync lag (blocks)", 0.0),
                ("reorgs", 1.0),
                ("chain tip height", 1024.0),
                ("transactions indexed", 18_204.0),
                ("gRPC requests", 12_400.0),
                ("gRPC mean latency (ms)", 4.13),
            ]),
            &theme,
        );
        assert_eq!(s.lines().count(), PANEL_LINES, "sampled height:\n{s}");
        assert!(s.contains("18.2k tx"), "compact totals:\n{s}");
        assert!(s.contains("4.1 ms"), "latency:\n{s}");
        assert!(s.contains("1,024"), "tip height grouped in full:\n{s}");
    }

    #[test]
    fn a_missing_metric_reads_as_absent_not_zero() {
        let mut r = reading(&[("reorgs", 0.0)]);
        r.values.push(crate::metrics::Value {
            label: "sync lag (blocks)",
            value: None,
        });
        let s = render_sync_metrics(&r, &plain_unicode_theme());
        assert!(s.contains("\u{2014}"), "absent value is an em dash:\n{s}");
    }

    /// A reading carrying values, as the poller projects one.
    fn reading(values: &[(&'static str, f64)]) -> crate::metrics::Reading {
        crate::metrics::Reading {
            values: values
                .iter()
                .map(|(label, v)| crate::metrics::Value {
                    label,
                    value: Some(*v),
                })
                .collect(),
            note: None,
        }
    }

    /// A reading with nothing to show and a stated reason.
    fn reading_note(note: String) -> crate::metrics::Reading {
        crate::metrics::Reading {
            values: Vec::new(),
            note: Some(note),
        }
    }
}
