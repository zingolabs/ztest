//! `ztest sync status`: the whole run in one screen, running or finished.
//!
//! - One view for both, not two that drift — a sync mid-flight and the same sync an hour
//!   dead differ in their header, never in what the panels mean
//! - Three rows over one clock: work by pool, pace, draw — one panel per component in
//!   the last, since a stacked namespace total hides which one grew
//! - Every series comes from the TSDB ([`query`](ztest::metrics::query)), which scrapes a
//!   live sync exactly as it scraped a finished one — hence the single view
//! - [`SyncStatus`] is the whole reason a verdict is not a `bool`: a run still going has
//!   none, and must not be drawn as a failure
//! - Chain progress (`phase`) sits beside the status, never in it — a subject at tip in a
//!   run still probing is `Running · Done`

use std::fmt::Write as _;
use std::time::Duration;

use owo_colors::OwoColorize as _;

use super::Theme;
use super::boxes::{beside, boxed, dim, interior, row};
use super::layout::{display_width, truncate};
use super::plot::{self, Band, Palette, PlotOpts};
use super::text::y_axis;
use ztest::api::Series;
use ztest::api::Unit;
use ztest::api::{Phase, SyncStatus};
use ztest::api::{compact, format_elapsed, thousands, unit_value};

/// Full terminal, capped. Past this the plots stop gaining resolution the eye can use
/// and the panels drift apart on a wide monitor
const MAX_WIDTH: usize = 160;

/// Below this a single column is wider than two cramped ones
const MIN_TWO_COLUMN: usize = 100;

const PLOT_ROWS: usize = 5;
/// cpu over mem in one panel: two stacks, each shorter than a lone plot
const HALF_ROWS: usize = 3;
const GUTTER: usize = 6;

/// One component's draw on the cluster, kubelet's reading. Split per component rather
/// than stacked into a namespace total — which container grew is the only question
/// this panel is asked, and a stack answers it by hiding it
#[derive(Debug, Clone, Default)]
pub struct ComponentResources {
    pub component: String,
    pub cpu: Vec<Series>,
    pub mem: Vec<Series>,
    pub disk: Vec<Series>,
    pub io_stall: Vec<Series>,
}

/// Everything the run view draws, live or finished. Header comes from the report mirror
/// once there is one and from the driver's tick stream before that; every panel below it
/// comes from the TSDB either way, so the two are the same view at different times
#[derive(Debug, Clone, Default)]
pub struct ReportView {
    pub sync_id: String,
    pub profile: String,
    pub status: SyncStatus,
    pub phase: Option<Phase>,
    /// Subject's own stage word, rendered after the lifecycle one
    pub phase_detail: Option<String>,
    pub elapsed: Duration,
    pub segment: Option<String>,
    pub height: Option<(u32, u32)>,
    pub eta: Option<Duration>,
    pub probes: Option<(usize, usize)>,
    pub tip: Option<u32>,
    pub violations: Vec<(String, String)>,
    pub coverage_gaps: Vec<String>,
    pub error: Option<String>,
    pub ticks: u64,
    pub dropped_snapshots: u64,
    pub transparent: Vec<Series>,
    pub shielded: Vec<Series>,
    pub blocks: Vec<Series>,
    pub throughput: Vec<Series>,
    pub resources: Vec<ComponentResources>,
    pub note: Option<String>,
    pub grafana: Option<String>,
}

pub fn render_sync_report(view: &ReportView, theme: &Theme, width: usize) -> String {
    let width = width.min(MAX_WIDTH);
    let mut out = String::with_capacity(4096);
    header(&mut out, view, theme, width);

    let two_up = width >= MIN_TWO_COLUMN;
    let (left_w, right_w) = match two_up {
        true => (width / 2, width - width / 2 - 1),
        false => (width, width),
    };

    let mut panels = vec![
        (
            pool_box("transparent ops/sec", &view.transparent, view, theme, left_w),
            pool_box("shielded ops/sec", &fold_sapling(&view.shielded), view, theme, right_w),
        ),
        (
            rate_box("blocks/sec", &view.blocks, view, theme, left_w),
            rate_box("transactions/sec", &view.throughput, view, theme, right_w),
        ),
    ];
    match view.resources.is_empty() {
        true => panels.push((empty(theme, "resources", view, left_w), Vec::new())),
        // Pairwise: a third component earns its own row rather than a narrower one
        false => panels.extend(view.resources.chunks(2).map(|pair| {
            (
                resources_box(&pair[0], theme, left_w),
                pair.get(1).map(|r| resources_box(r, theme, right_w)).unwrap_or_default(),
            )
        })),
    }

    for (a, b) in panels {
        match two_up {
            true => beside(&mut out, &a, &b, left_w),
            false => {
                for line in a.iter().chain(b.iter()) {
                    let _ = writeln!(out, "{line}");
                }
            }
        }
    }
    footer(&mut out, view, theme, width);
    out
}

// ──────────────────────────────── header ────────────────────────────────

/// Sole status → glyph + ink mapping (report header + `watch` headline both draw here).
/// - Live / unresolved never red (no verdict = nothing to have failed)
pub fn status_mark(status: SyncStatus, theme: &Theme) -> (&'static str, owo_colors::Style) {
    match status {
        SyncStatus::Finished(v) if v.is_pass() => (theme.chars.ok, theme.styles.pass),
        SyncStatus::Finished(_) => (theme.chars.fail, theme.styles.fail),
        SyncStatus::Pending | SyncStatus::Running => (theme.chars.dot, theme.styles.count),
        SyncStatus::Unresolved => (theme.chars.warn, theme.styles.fail),
    }
}

fn header(out: &mut String, v: &ReportView, theme: &Theme, width: usize) {
    let (mark, style) = status_mark(v.status, theme);
    let dot = theme.chars.dot.style(theme.styles.dim);

    let _ = writeln!(
        out,
        "{}",
        truncate(
            &format!(
                "{} {} {}",
                mark.style(style),
                v.sync_id.style(theme.styles.count),
                format!("[{}]", v.profile).style(theme.styles.dim),
            ),
            width,
        )
    );
    // A run still going has no segment *yet*, which is not a finding — only a finished
    // one that recorded none has something to answer for
    let segment = match (v.segment.as_deref(), v.status.is_live()) {
        (Some(s), _) => format!(" {dot} {}", s.style(theme.styles.dim)),
        (None, true) => String::new(),
        (None, false) => format!(" {dot} {}", "no segment recorded".style(theme.styles.dim)),
    };
    // Phase = chain-walk position, live only (a finished run's last phase adds nothing to
    // its verdict)
    let phase = match v.phase.filter(|_| v.status.is_live()) {
        Some(p) => {
            let label = match v.phase_detail.as_deref() {
                Some(d) => format!("{p} {dot} {d}"),
                None => p.to_string(),
            };
            format!(" {dot} {}", label.style(theme.styles.dim))
        }
        None => String::new(),
    };
    let _ = writeln!(
        out,
        "{}",
        truncate(
            &format!(
                "  {} {dot} {}{phase}{segment}",
                v.status.to_string().style(style),
                format_elapsed(v.elapsed).style(theme.styles.count),
            ),
            width,
        )
    );

    // Shortfall is the verdict for an index run, so it leads rather than sitting in a
    // metrics row: `short 1,001` is the finding, `99.8%` alone reads as success.
    // Measured against the height *asked for* — the tip only annotates, since a run
    // that met its target is not short because the network kept going
    if let Some((reached, target)) = v.height {
        let bar_w = width.saturating_sub(46).clamp(10, 40);
        let pct = match target {
            0 => 0.0,
            t => (f64::from(reached) / f64::from(t) * 100.0).clamp(0.0, 100.0),
        };
        let filled = (pct / 100.0 * bar_w as f64).round() as usize;
        // Nothing is short until the run stops trying — a live sync gets its countdown
        // in the slot a finished one spends on the shortfall
        let standing = match (v.status.is_live(), target.saturating_sub(reached)) {
            (true, _) => match v.eta {
                None => String::new(),
                Some(eta) => {
                    format!("eta {}", format_elapsed(eta)).style(theme.styles.count).to_string()
                }
            },
            (_, 0) => "complete".style(theme.styles.pass).to_string(),
            (_, n) => {
                format!("short {}", thousands(u64::from(n))).style(theme.styles.fail).to_string()
            }
        };
        let standing = match standing.is_empty() {
            true => String::new(),
            false => format!(" {dot} {standing}"),
        };
        // Chain moved on while the run worked: context, never a failure
        let behind = match v.tip.map(|tip| tip.saturating_sub(target)) {
            Some(0) | None => String::new(),
            Some(n) => format!(" {dot} tip +{}", thousands(u64::from(n)))
                .style(theme.styles.dim)
                .to_string(),
        };
        let line = format!(
            "  {} / {}{standing} {dot} {}{} {:>5.1}%{behind}",
            thousands(u64::from(reached)).style(theme.styles.count),
            thousands(u64::from(target)).style(theme.styles.dim),
            theme.chars.bar_fill.repeat(filled),
            theme.chars.bar_empty.repeat(bar_w.saturating_sub(filled)).style(theme.styles.dim),
            pct,
        );
        let _ = writeln!(out, "{}", truncate(&line, width));
    }
    out.push('\n');
}

// ──────────────────────────────── panels ────────────────────────────────

/// Ops by pool: plot at full width, totals on one chip row beneath it
fn pool_box(
    title: &str,
    pools: &[Series],
    v: &ReportView,
    theme: &Theme,
    width: usize,
) -> Vec<String> {
    let inner = interior(width);
    if pools.is_empty() {
        return empty(theme, title, v, width);
    }
    let mut body: Vec<String> = stacked(pools, PLOT_ROWS, None, inner, 0, theme)
        .into_iter()
        .map(|(label, plotted)| format!("{} {plotted}", label.style(theme.styles.dim)))
        .collect();

    // Chips and rates are the same reading, so they share a line where both fit. Narrow
    // → stack rather than truncate (a cut chip drops a pool's total with no sign of it)
    // Aggregate dropped for a lone pool: its chip already is that number
    let total = pools.iter().filter_map(Series::integral).sum::<f64>();
    let summary = rate_text(pools, Some(total).filter(|_| pools.len() > 1));
    let chips = pool_chips(pools, theme, inner);
    match display_width(&chips) + display_width(&summary) < inner {
        true => {
            let pad = inner - display_width(&chips) - display_width(&summary);
            body.push(format!("{chips}{:pad$}{}", "", summary.style(theme.styles.count)));
        }
        false => {
            body.push(chips);
            body.push(row("", &summary, inner, theme));
        }
    }
    boxed(title, "", &body, width, theme)
}

/// `tsp 67.9M * sap 1.58M * orc 152` — whole-run total per pool, largest first.
///
/// - Tag takes its band's colour ([`Palette::pools`]) → chip maps to layer by eye
/// - Unpadded: this view prints once into scrollback, so no width to hold steady
fn pool_chips(pools: &[Series], theme: &Theme, budget: usize) -> String {
    let palette = Palette::pools(theme.is_colorized());
    let mut ranked: Vec<&Series> = pools.iter().collect();
    ranked.sort_by(|a, b| b.integral().unwrap_or(0.0).total_cmp(&a.integral().unwrap_or(0.0)));

    let chips: Vec<String> = ranked
        .iter()
        .map(|s| {
            let total = s.integral().map(compact).unwrap_or_else(|| "—".into());
            format!(
                "{} {}",
                pool_tag(&s.label).style(palette.style_of(&s.label)),
                total.style(theme.styles.count),
            )
        })
        .collect();
    truncate(&chips.join(&format!(" {} ", "*".style(palette.separator()))), budget)
}

/// Sapling ships split (only the output side is checkable against the note-commitment trees,
/// see [`chainwork`](ztest::sync::chainwork)); the panel reads pools, so display folds them
const SAPLING_PARTS: [&str; 2] = ["sapling spends", "sapling outputs"];

/// Sapling spends + outputs → one `sapling` band, in the split's place
fn fold_sapling(pools: &[Series]) -> Vec<Series> {
    let is_part = |s: &Series| SAPLING_PARTS.contains(&s.label.as_str());
    let (parts, mut rest): (Vec<Series>, Vec<Series>) = pools.iter().cloned().partition(&is_part);
    let Some(sapling) = Series::folded("sapling", &parts) else {
        return rest;
    };
    rest.insert(pools.iter().take_while(|s| !is_part(s)).count(), sapling);
    rest
}

/// Three-letter pool tag. First three characters, overridden where they collide or name
/// nothing a reader recognises (`tra`; both transparent directions; both sapling directions)
fn pool_tag(label: &str) -> String {
    match label {
        "transparent" => "tsp".to_string(),
        "transparent in" => "tsp-in".to_string(),
        "transparent out" => "tsp-out".to_string(),
        "sapling spends" => "sap-sp".to_string(),
        "sapling outputs" => "sap-out".to_string(),
        other => other.chars().take(3).flat_map(char::to_lowercase).collect(),
    }
}

/// A single rate, plotted full-width. No legend: one series names itself in the title,
/// and the summary already carries its total
fn rate_box(
    title: &str,
    series: &[Series],
    v: &ReportView,
    theme: &Theme,
    width: usize,
) -> Vec<String> {
    let inner = interior(width);
    if series.is_empty() {
        return empty(theme, title, v, width);
    }
    let mut body: Vec<String> = stacked(series, PLOT_ROWS, None, inner, 0, theme)
        .into_iter()
        .map(|(label, plotted)| format!("{} {plotted}", label.style(theme.styles.dim)))
        .collect();
    let total = series.iter().filter_map(Series::integral).sum::<f64>();
    body.push(row("", &rate_text(series, Some(total)), inner, theme));
    boxed(title, "", &body, width, theme)
}

/// One component's draw and its disk. The title carries the component, so each summary
/// names its quantity instead of repeating it.
///
/// - Disk is per-cgroup, so a PVC shared by two pods still attributes: the charge follows
///   the task that moved the bytes
/// - Stall says who *waited*, never who caused it — on a shared device both containers
///   stalling in one window is the contention signature, which is why these stay per
///   component and are never summed (overlapping stalls would total past 100%)
fn resources_box(r: &ComponentResources, theme: &Theme, width: usize) -> Vec<String> {
    let inner = interior(width);
    let mut body = Vec::new();
    // Disk above the stall it explains: throughput then the cost of waiting for it
    for (series, unit, name) in [
        (&r.cpu, Unit::Cores, "cpu"),
        (&r.mem, Unit::Bytes, "memory"),
        (&r.disk, Unit::BytesPerSec, "disk"),
        (&r.io_stall, Unit::Fraction, "io stall"),
    ] {
        if series.is_empty() {
            continue;
        }
        let peak = stacked_peak(series);
        // Flat zero = the expected stall reading; the summary line carries it, and three
        // rows of zero axis would evict the quantities that vary
        if peak > 0.0 {
            // Axis in the plotted unit, not `compact`'s bare number: `16.0 GiB` and `16.0`
            // in the same gutter are two different claims
            body.extend(
                stacked(series, HALF_ROWS, Some(unit), inner, 0, theme)
                    .into_iter()
                    .map(|(label, plotted)| format!("{} {plotted}", label.style(theme.styles.dim))),
            );
        }
        body.push(row(name, &format!("peak {}", unit_value(unit, peak)), inner, theme));
    }
    boxed(&format!("resources · {}", r.component), "", &body, width, theme)
}

// ──────────────────────────────── footer ────────────────────────────────

/// Continuation indent for a wrapped violation, past the `✗ ` marker
const VIOLATION_INDENT: &str = "    ";

/// Break `text` on whitespace to `width` columns, keeping every character.
///
/// - Word longer than `width` still emits (a URL or a 90-char type name is exactly
///   what a reader needs; a hard split would corrupt it)
/// - Plain text only — callers style *after*, so no escape lands inside a measurement
fn wrap(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        let pending = line.chars().count() + 1 + word.chars().count();
        if !line.is_empty() && pending > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

fn footer(out: &mut String, v: &ReportView, theme: &Theme, width: usize) {
    let dot = theme.chars.dot.style(theme.styles.dim);
    // Wrapped, never truncated: this is the one line stating *why* the run failed, and
    // a backend error carries its cause last (`{component} {op}: {source}`) — clipping
    // to the terminal keeps the half that names nothing
    for (probe, detail) in &v.violations {
        let _ = writeln!(
            out,
            "{} {}",
            theme.chars.fail.style(theme.styles.fail),
            probe.style(theme.styles.fail),
        );
        for line in wrap(detail, width.saturating_sub(VIOLATION_INDENT.len())) {
            let _ = writeln!(out, "{VIOLATION_INDENT}{}", line.style(theme.styles.dim));
        }
    }
    for gap in &v.coverage_gaps {
        let _ = writeln!(out, "{} coverage gap {dot} {}", theme.chars.warn, gap);
    }
    if let Some(error) = &v.error {
        let _ = writeln!(out, "{} {}", theme.chars.fail.style(theme.styles.fail), error);
    }
    if let Some(note) = &v.note {
        let _ = writeln!(out, "  {}", note.style(theme.styles.dim));
    }
    let dropped = match v.dropped_snapshots {
        0 => String::new(),
        n => format!(" {dot} {} snapshots dropped", n.style(theme.styles.count)),
    };
    if let Some(url) = &v.grafana {
        let _ = writeln!(out, "  {}", url.style(theme.styles.dim));
    }
    // Probe tally is the live counterpart of a verdict: before one exists, how many
    // probes are holding is the only standing there is
    let probes = match v.probes {
        None => String::new(),
        Some((ok, total)) => {
            format!(" {dot} {}/{total} probes ok", ok.style(theme.styles.count))
        }
    };
    // Pre-first-tick a `0 ticks` reads as a stalled run rather than one still starting
    let ticks = match v.ticks {
        0 => String::new(),
        n => format!("{} ticks {dot} ", n.style(theme.styles.count)),
    };
    let _ = writeln!(
        out,
        "  {ticks}{} violations{probes}{dropped}",
        v.violations.len().style(theme.styles.count),
    );
}

// ──────────────────────────────── helpers ────────────────────────────────

/// One band per sample. [`plot_stacked`](plot::plot_stacked) resamples to its own
/// width, so no decimation is needed here — and doing it twice would flatten extremes
fn bands(series: &Series) -> Vec<Band> {
    series.points.iter().map(|(_, v)| Some((*v, *v))).collect()
}

/// A stacked plot paired with its axis, as `(label, row)` per line.
///
/// - Gutter is sized to the labels it will hold, not fixed: `11.2 GiB` and `228k` do
///   not share a width, and a fixed one either wastes columns or shears the frame
/// - Two passes because the gutter's width depends on the ceiling, which depends on
///   the plot width the gutter leaves
/// - `reserve` = columns the caller keeps to the right (the pool legend)
fn stacked(
    series: &[Series],
    rows: usize,
    unit: Option<Unit>,
    inner: usize,
    reserve: usize,
    theme: &Theme,
) -> Vec<(String, String)> {
    let channels: Vec<plot::Channel<'_>> =
        series.iter().map(|s| (s.label.as_str(), bands(s))).collect();
    let palette = Palette::pools(theme.is_colorized());
    let width_for = |gutter: usize| inner.saturating_sub(gutter + 1 + reserve).max(1);

    let probe = PlotOpts::new(width_for(GUTTER), rows, theme.chars.graph);
    let gutter = axis(plot::ceiling(&channels, &probe), rows, unit).1;

    let opts = PlotOpts::new(width_for(gutter), rows, theme.chars.graph);
    let (labels, _) = axis(plot::ceiling(&channels, &opts), rows, unit);
    labels.into_iter().zip(plot::plot_stacked(&channels, &opts, &palette)).collect()
}

/// Axis labels top-down, right-aligned, plus the width they needed
fn axis(ceiling: f64, rows: usize, unit: Option<Unit>) -> (Vec<String>, usize) {
    let raw: Vec<String> = match unit {
        None => y_axis(ceiling, rows, 0),
        Some(unit) => (0..rows)
            .map(|r| {
                let frac = match rows {
                    0 | 1 => 1.0,
                    _ => 1.0 - (r as f64 / (rows - 1) as f64),
                };
                unit_value(unit, ceiling * frac)
            })
            .collect(),
    };
    let width = raw.iter().map(|l| display_width(l)).max().unwrap_or(0);
    (raw.into_iter().map(|l| format!("{:>width$}", l)).collect(), width)
}

/// `avg 44.3k/s · peak 88.0k/s · 78.3M total`. Unstyled — callers that pair it with
/// pre-styled text lay it out themselves
fn rate_text(series: &[Series], total: Option<f64>) -> String {
    let mean: f64 = series.iter().filter_map(Series::mean).sum();
    let peak: f64 = series.iter().filter_map(Series::peak).sum();
    let total =
        total.filter(|t| *t > 0.0).map(|t| format!(" · {} total", compact(t))).unwrap_or_default();
    format!("avg {}/s · peak {}/s{total}", compact(mean), compact(peak))
}

/// Peak of the *sum*, not the sum of peaks: series do not peak together, and adding
/// their maxima invents a moment the run never had
fn stacked_peak(series: &[Series]) -> f64 {
    let len = series.iter().map(|s| s.points.len()).max().unwrap_or(0);
    (0..len)
        .map(|i| series.iter().filter_map(|s| s.points.get(i)).map(|(_, v)| v).sum::<f64>())
        .fold(0.0, f64::max)
}

/// An empty frame reads as "nothing happened"; the real cause is the record's reach,
/// which is a fact about retention and the cluster, not about the run
fn empty(theme: &Theme, title: &str, v: &ReportView, width: usize) -> Vec<String> {
    let why = v.note.as_deref().unwrap_or("no series recorded for this run");
    boxed(title, "", &[dim(why, theme)], width, theme)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn series(label: &str, unit: Unit, values: &[f64]) -> Series {
        Series {
            label: label.to_string(),
            unit,
            facet: None,
            points: values.iter().enumerate().map(|(i, v)| (i as f64 * 30.0, *v)).collect(),
        }
    }

    fn wave(scale: f64, n: usize) -> Vec<f64> {
        (0..n).map(|i| scale * (1.0 + (i as f64 / 9.0).sin())).collect()
    }

    pub(super) fn view() -> ReportView {
        ReportView {
            sync_id: "zaino-index-construction-c3115f50".into(),
            profile: "zaino_index_construction".into(),
            status: SyncStatus::Finished(ztest::api::SyncVerdict::Failed),
            phase: None,
            phase_detail: None,
            eta: None,
            probes: None,
            elapsed: Duration::from_secs(1_842),
            segment: Some("main 3,013..658,599".into()),
            height: Some((658_599, 659_600)),
            tip: Some(659_640),
            violations: vec![
                (
                    "reached_the_pinned_tip".into(),
                    "index frontier finished at 658599 but the snapshot pins its tip at 659600"
                        .into(),
                ),
                (
                    "serves_from_its_own_index".into(),
                    "at frontier 658599 zaino still forwards getaddressdeltas to the validator"
                        .into(),
                ),
            ],
            coverage_gaps: Vec::new(),
            error: None,
            ticks: 118,
            dropped_snapshots: 0,
            transparent: vec![series("transparent", Unit::PerSec, &wave(70_000.0, 60))],
            shielded: vec![
                series("sapling", Unit::PerSec, &wave(30_000.0, 60)),
                series("orchard", Unit::PerSec, &wave(14_000.0, 60)),
                series("ironwood", Unit::PerSec, &wave(0.0, 60)),
            ],
            blocks: vec![series("blocks", Unit::PerSec, &wave(400.0, 60))],
            throughput: vec![series("transactions", Unit::PerSec, &wave(900.0, 60))],
            resources: vec![
                ComponentResources {
                    component: "zainod".into(),
                    cpu: vec![series("zainod", Unit::Cores, &wave(3.0, 60))],
                    mem: vec![series("zainod", Unit::Bytes, &wave(6e9, 60))],
                    disk: vec![
                        series("read", Unit::BytesPerSec, &wave(4.1e8, 60)),
                        series("write", Unit::BytesPerSec, &wave(9e7, 60)),
                    ],
                    io_stall: vec![series("zainod", Unit::Fraction, &wave(0.004, 60))],
                },
                ComponentResources {
                    component: "zebrad".into(),
                    cpu: vec![series("zebrad", Unit::Cores, &wave(1.0, 60))],
                    mem: vec![series("zebrad", Unit::Bytes, &wave(2e9, 60))],
                    disk: vec![series("write", Unit::BytesPerSec, &wave(2.2e8, 60))],
                    io_stall: vec![series("zebrad", Unit::Fraction, &[0.0; 60])],
                },
            ],
            note: None,
            grafana: None,
        }
    }

    fn lines(width: usize) -> Vec<String> {
        render_sync_report(&view(), &theme(), width).lines().map(str::to_string).collect()
    }

    /// Founding invariant: one over-long line wraps and shears every box below it
    #[test]
    fn no_line_ever_exceeds_the_requested_width() {
        for width in [60, 80, 100, 120, 160, 220] {
            for line in lines(width) {
                assert!(
                    display_width(&line) <= width.min(MAX_WIDTH),
                    "width {width}: {line:?} is {} wide",
                    display_width(&line)
                );
            }
        }
    }

    #[test]
    fn a_wide_terminal_gets_two_panels_per_row() {
        let rendered = lines(120);
        for (left, right) in [
            ("transparent ops/sec", "shielded ops/sec"),
            ("blocks/sec", "transactions/sec"),
            ("resources · zainod", "resources · zebrad"),
        ] {
            assert!(
                rendered.iter().any(|l| l.contains(left) && l.contains(right)),
                "{left} | {right} not on one row:\n{rendered:#?}"
            );
        }
    }

    /// Stack, never truncate — half a panel is worse than a taller view
    #[test]
    fn a_narrow_terminal_stacks_the_panels() {
        let rendered = lines(80);
        for title in [
            "transparent ops/sec",
            "shielded ops/sec",
            "blocks/sec",
            "transactions/sec",
            "resources · zainod",
            "resources · zebrad",
        ] {
            assert!(rendered.iter().any(|l| l.contains(title)), "{title} missing");
        }
        assert!(
            !rendered
                .iter()
                .any(|l| l.contains("resources · zainod") && l.contains("resources · zebrad"))
        );
    }

    /// A stacked namespace total hides which container grew — the one question this
    /// panel is asked
    #[test]
    fn each_component_draws_in_its_own_resource_panel() {
        let rendered = render_sync_report(&view(), &theme(), 160);
        assert!(rendered.contains("resources · zainod"), "{rendered}");
        assert!(rendered.contains("resources · zebrad"), "{rendered}");
    }

    /// An odd component count must not squeeze three panels onto a two-column row
    #[test]
    fn a_third_component_gets_its_own_row() {
        let mut v = view();
        v.resources.push(ComponentResources {
            component: "lightwalletd".into(),
            cpu: vec![series("lightwalletd", Unit::Cores, &wave(0.5, 60))],
            mem: Vec::new(),
            disk: Vec::new(),
            io_stall: Vec::new(),
        });
        let rendered = render_sync_report(&v, &theme(), 120);
        assert!(rendered.contains("resources · lightwalletd"), "{rendered}");
        for line in rendered.lines() {
            assert!(display_width(line) <= 120, "{line:?}");
        }
    }

    /// The finding, not the percentage: `99.8%` alone reads as success
    #[test]
    fn the_header_leads_with_the_shortfall_rather_than_the_percentage() {
        let rendered = render_sync_report(&view(), &theme(), 120);
        assert!(rendered.contains("short 1,001"), "{rendered}");
        assert!(rendered.contains("658,599 / 659,600"));
    }

    #[test]
    fn a_run_that_met_its_target_says_so_instead_of_a_shortfall() {
        let mut v = view();
        v.height = Some((659_600, 659_600));
        let rendered = render_sync_report(&v, &theme(), 120);
        assert!(rendered.contains("complete"), "{rendered}");
        assert!(!rendered.contains("short"));
    }

    /// The bug this replaced: measured against the tip, a run that indexed every block
    /// it was asked for reported `short 1,001` purely because the chain kept moving
    #[test]
    fn a_chain_that_advanced_past_the_target_annotates_but_never_shortens() {
        let mut v = view();
        v.height = Some((659_600, 659_600));
        v.tip = Some(660_601);
        let rendered = render_sync_report(&v, &theme(), 120);
        assert!(rendered.contains("complete"), "{rendered}");
        assert!(rendered.contains("tip +1,001"), "{rendered}");
        assert!(!rendered.contains("short"), "{rendered}");
    }

    /// Every violation must reach the screen — this view is the only place a finished
    /// run explains itself
    #[test]
    fn every_violation_is_printed() {
        let rendered = render_sync_report(&view(), &theme(), 160);
        assert!(rendered.contains("reached_the_pinned_tip"));
        assert!(rendered.contains("serves_from_its_own_index"));
    }

    /// The bug that made a real failure undiagnosable: the detail was clipped to one
    /// line, and `RpcError`'s cause is *last* (`{component} {op}: {source}`), so the
    /// clip kept "zaino" and dropped everything that named the fault
    #[test]
    fn a_violation_keeps_its_cause_however_long_the_detail() {
        let mut v = view();
        let cause = "connection refused after 3 attempts to 10.244.0.19:8137";
        v.violations = vec![(
            "index_serves_the_pinned_tip".into(),
            format!(
                "1800s after the finalised writer committed its last batch at frontier \
                 1692103, `gettxoutsetinfo` did not answer: zaino json_rpc: {cause}"
            ),
        )];
        let rendered = render_sync_report(&v, &theme(), 100);
        assert!(rendered.contains(cause), "the cause was clipped:\n{rendered}");
        assert!(rendered.lines().all(|l| display_width(l) <= 100), "{rendered}");
    }

    /// A byte axis labelled `16.0` and one labelled `16.0 GiB` are different claims
    #[test]
    fn a_resource_axis_carries_its_unit() {
        let rendered = render_sync_report(&view(), &theme(), 120);
        assert!(rendered.contains("GiB") || rendered.contains("GB"), "{rendered}");
        assert!(rendered.lines().any(|l| l.contains('c') && l.contains("peak")), "{rendered}");
    }

    /// Containers do not peak together; summing their maxima invents a moment
    #[test]
    fn a_stacked_peak_is_the_peak_of_the_sum_not_the_sum_of_the_peaks() {
        let a = series("a", Unit::Cores, &[10.0, 0.0]);
        let b = series("b", Unit::Cores, &[0.0, 10.0]);
        assert_eq!(stacked_peak(&[a, b]), 10.0, "sum of peaks would be 20");
    }

    /// Prometheus gone or the run past retention: state it, never draw an empty frame
    #[test]
    fn a_run_with_no_record_states_why_rather_than_drawing_empty_plots() {
        let mut v = view();
        v.transparent.clear();
        v.shielded.clear();
        v.blocks.clear();
        v.throughput.clear();
        v.resources.clear();
        v.note = Some("no metrics recorded (run is older than the 30d retention)".into());
        let rendered = render_sync_report(&v, &theme(), 120);
        assert!(rendered.contains("older than the 30d retention"), "{rendered}");
        for line in rendered.lines() {
            assert!(display_width(line) <= 120, "{line:?}");
        }
    }

    /// Each half of the top row states its own absence — one pool going unpublished
    /// must not blank the other
    #[test]
    fn a_missing_pool_leaves_the_other_panel_drawn() {
        let mut v = view();
        v.shielded.clear();
        v.note = Some("zaino published no shielded counters".into());
        let rendered = render_sync_report(&v, &theme(), 120);
        assert!(rendered.contains("published no shielded counters"), "{rendered}");
        assert!(rendered.contains("tsp "), "transparent chip missing:\n{rendered}");
    }

    /// One band per pool: the spend/output split serves `chainwork`, not the reader
    #[test]
    fn the_sapling_split_renders_as_one_pool() {
        let mut v = view();
        v.shielded = vec![
            series("sapling spends", Unit::PerSec, &wave(20_000.0, 60)),
            series("sapling outputs", Unit::PerSec, &wave(10_000.0, 60)),
            series("orchard", Unit::PerSec, &wave(14_000.0, 60)),
        ];
        let rendered = render_sync_report(&v, &theme(), 120);
        assert!(!rendered.contains("sap-sp"), "{rendered}");
        assert!(!rendered.contains("sap-out"), "{rendered}");
        assert!(rendered.contains("sap "), "folded chip missing:\n{rendered}");

        let folded = fold_sapling(&v.shielded);
        assert_eq!(
            folded.iter().map(|s| s.label.as_str()).collect::<Vec<_>>(),
            ["sapling", "orchard"]
        );
        let split_total: f64 = v.shielded[..2].iter().filter_map(Series::integral).sum();
        assert!((folded[0].integral().expect("rate") - split_total).abs() < 1.0);
    }

    /// Position is the split's, not the front — a pool ordered before sapling keeps its place
    #[test]
    fn a_folded_sapling_holds_the_splits_place() {
        let pools = vec![
            series("sprout", Unit::PerSec, &[1.0, 1.0]),
            series("sapling spends", Unit::PerSec, &[2.0, 2.0]),
            series("sapling outputs", Unit::PerSec, &[3.0, 3.0]),
            series("orchard", Unit::PerSec, &[4.0, 4.0]),
        ];
        let folded = fold_sapling(&pools);
        assert_eq!(
            folded.iter().map(|s| s.label.as_str()).collect::<Vec<_>>(),
            ["sprout", "sapling", "orchard"]
        );
        assert_eq!(folded[1].points, vec![(0.0, 5.0), (30.0, 5.0)]);
    }

    #[test]
    fn a_pool_tag_is_three_characters() {
        for (label, tag) in [
            ("transparent", "tsp"),
            ("sapling", "sap"),
            ("orchard", "orc"),
            ("ironwood", "iro"),
            ("sprout", "spr"),
        ] {
            assert_eq!(pool_tag(label), tag);
        }
    }

    /// Printed once into scrollback, so nothing re-flows: no padding to reserve
    #[test]
    fn a_chip_row_separates_pools_and_leads_with_the_largest() {
        let row = pool_chips(&view().shielded, &theme(), 60);
        assert_eq!(row, "sap 53.4M * orc 24.9M * iro 0", "{row:?}");
    }

    /// Per-pool totals and the run's rates are the same reading; two lines wasted one
    #[test]
    fn the_chips_share_a_line_with_the_rate_summary() {
        let rendered = render_sync_report(&view(), &theme(), 160);
        let line = rendered.lines().find(|l| l.contains("sap 53.4M")).expect("a chip row");
        assert!(line.contains("avg 44.3k/s"), "{line:?}");
        assert!(line.contains("peak 88.0k/s"), "{line:?}");
    }

    /// Truncating would drop a pool's total with nothing to show it happened
    #[test]
    fn a_narrow_panel_stacks_the_chips_rather_than_cutting_one_off() {
        for width in [60, 80, 100, 120, 160] {
            let rendered = render_sync_report(&view(), &theme(), width);
            for pool in ["sap", "orc", "iro"] {
                assert!(
                    rendered.contains(&format!("{pool} ")),
                    "width {width}: {pool} chip missing\n{rendered}"
                );
            }
        }
    }

    /// A lone pool's chip already is the aggregate; printing it twice says nothing.
    /// Stacked width, so the line holds one panel rather than two
    #[test]
    fn a_single_pool_panel_does_not_repeat_its_total_as_an_aggregate() {
        let rendered = render_sync_report(&view(), &theme(), 80);
        let line = rendered.lines().find(|l| l.contains("tsp ")).expect("a chip row");
        assert!(!line.contains("total"), "{line:?}");
        assert!(line.contains("avg 70.5k/s"), "{line:?}");
    }

    /// `B` is billions here — every `compact` caller is a count, rate or core figure
    #[test]
    fn a_billion_scale_total_reads_as_b_not_g() {
        let s = series("orchard", Unit::PerSec, &[9_876_543_210.0, 9_876_543_210.0]);
        assert!(pool_chips(&[s], &theme(), 60).contains('B'));
    }

    /// Pre-first-tick and empty-report shapes must render, not unwrap-panic
    #[test]
    fn an_empty_report_still_renders() {
        let rendered = render_sync_report(&ReportView::default(), &theme(), 120);
        assert!(!rendered.is_empty());
        for line in rendered.lines() {
            assert!(display_width(line) <= 120, "{line:?}");
        }
    }

    // ── live ──────────────────────────────────────────────────────────────

    /// Header source differs; the panels do not. This is the whole point of the view
    #[test]
    fn a_running_sync_draws_the_same_panels_as_a_finished_one() {
        let rendered = render_sync_report(&running(), &theme(), 160);
        for title in [
            "transparent ops/sec",
            "shielded ops/sec",
            "blocks/sec",
            "transactions/sec",
            "resources · zainod",
            "resources · zebrad",
        ] {
            assert!(rendered.contains(title), "{title} missing from a live run:\n{rendered}");
        }
    }

    /// A run still going has not failed — the mark a `bool` forced is the reason
    /// [`SyncStatus`] is not one
    #[test]
    fn a_running_sync_is_never_marked_failed() {
        let theme = theme();
        let rendered = render_sync_report(&running(), &theme, 160);
        let first = rendered.lines().next().expect("a header");
        assert!(!first.starts_with(theme.chars.fail), "{first:?}");
        assert!(!rendered.contains("complete"), "no verdict is due yet:\n{rendered}");
        assert!(!rendered.contains("short "), "a run mid-flight is not short:\n{rendered}");
    }

    /// The countdown a finished run spends on its shortfall
    #[test]
    fn a_running_sync_shows_its_eta_where_a_finished_one_shows_the_shortfall() {
        let rendered = render_sync_report(&running(), &theme(), 160);
        assert!(rendered.contains("eta 72m00s"), "{rendered}");
    }

    /// Before a verdict exists, how many probes are holding is the only standing there is
    #[test]
    fn a_running_sync_reports_its_probe_tally() {
        let rendered = render_sync_report(&running(), &theme(), 160);
        assert!(rendered.contains("3/4 probes ok"), "{rendered}");
    }

    /// `0 ticks` reads as a stalled run rather than one still starting
    #[test]
    fn a_run_before_its_first_tick_omits_the_tick_count() {
        let mut v = running();
        v.ticks = 0;
        let rendered = render_sync_report(&v, &theme(), 160);
        assert!(!rendered.contains("ticks"), "{rendered}");
        assert!(rendered.contains("violations"), "{rendered}");
    }

    /// A live run has no segment *yet*; scolding it for that is a finished run's job
    #[test]
    fn a_running_sync_omits_the_missing_segment_placeholder() {
        let rendered = render_sync_report(&running(), &theme(), 160);
        assert!(!rendered.contains("no segment recorded"), "{rendered}");

        let mut finished = running();
        finished.status = SyncStatus::Finished(ztest::api::SyncVerdict::Failed);
        assert!(render_sync_report(&finished, &theme(), 160).contains("no segment recorded"));
    }

    /// Subject at tip = chain fact, not a verdict (the confusion the split
    /// `outcome`/`verdict` pair invited)
    #[test]
    fn a_subject_at_tip_still_reads_as_running() {
        let mut v = running();
        v.phase = Some(Phase::Done);
        v.height = Some((1_700_000, 1_700_000));
        let rendered = render_sync_report(&v, &theme(), 160);
        let header = rendered.lines().nth(1).expect("the status line");
        assert!(header.contains("Running"), "the run has no verdict yet: {header:?}");
        assert!(header.contains("Done"), "the chain walk is at tip: {header:?}");
        assert!(!rendered.contains("Passed"), "no verdict was mirrored:\n{rendered}");
    }

    pub(super) fn running() -> ReportView {
        ReportView {
            status: SyncStatus::Running,
            phase: Some(Phase::Syncing),
            segment: None,
            height: Some((1_204_551, 1_700_000)),
            eta: Some(Duration::from_secs(4_320)),
            probes: Some((3, 4)),
            violations: vec![(
                "index_serves_the_pinned_tip".into(),
                "unsatisfied 12m03s of 30m00s".into(),
            )],
            ticks: 118,
            ..view()
        }
    }
}

#[cfg(test)]
mod preview {
    #[test]
    #[ignore = "visual check: cargo test -- --ignored --nocapture preview"]
    fn show() {
        let theme = super::Theme::for_capabilities(true, true);
        println!("\n{}", super::render_sync_report(&super::tests::view(), &theme, 120));
        println!("\n{}", super::render_sync_report(&super::tests::running(), &theme, 120));
    }
}
