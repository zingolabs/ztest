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
use super::boxes::{beside, boxed, interior};
use super::layout::{display_width, truncate};
use super::plot::{self, Band, Palette, PlotOpts};
use super::template::{Fields, Template};
use super::text::y_axis;
use ztest::api::Series;
use ztest::api::Unit;
use ztest::api::{Phase, SyncStatus};
use ztest::api::{format_elapsed, thousands, unit_value};

/// Full terminal, capped. Past this the plots stop gaining resolution the eye can use
/// and the panels drift apart on a wide monitor
const MAX_WIDTH: usize = 160;

/// Below this a single column is wider than two cramped ones
const MIN_TWO_COLUMN: usize = 100;

const PLOT_ROWS: usize = 5;
/// cpu over mem in one panel: two stacks, each shorter than a lone plot
const HALF_ROWS: usize = 3;
const GUTTER: usize = 6;

// ──────────────────────────────── rows ────────────────────────────────

/// Row shapes, one per thing the report draws.
///
/// - Ink that tracks the verdict is spliced (`{tone}`), never duplicated per verdict
/// - Meter stays two text cells, not `{k:N#}` — the header bar carries no `[..]` frame
mod row {
    pub(super) fn ident(tone: &str) -> String {
        format!("{{mark|{tone}}} {{id|bold}} {{profile|dim}}")
    }

    pub(super) fn status(tone: &str) -> String {
        format!(
            "  {{status|{tone}}} {{@dot|dim}} {{elapsed|bold}}\
             [ {{@dot|dim}} {{phase|dim}}[ {{@dot|dim}} {{detail|dim}}]]\
             [ {{@dot|dim}} {{segment|dim}}]"
        )
    }

    pub(super) fn height(tone: &str) -> String {
        format!(
            "  {{reached|bold}} / {{target|dim}}[ {{@dot|dim}} {{standing|{tone}}}] \
             {{@dot|dim}} {{fill}}{{empty|dim}} {{pct:>6|fraction}}[ {{@dot|dim}} {{behind|dim}}]"
        )
    }

    pub(super) fn violation_detail() -> String {
        format!("{}{{line|dim}}", super::VIOLATION_INDENT)
    }

    /// Dim — the number above still stands; this says only that the plot ends elsewhere
    pub(super) const HEIGHT_CHECK: &str = "  {@warn|dim} prometheus last read {observed|dim} here \
         ({gap|dim} {side|dim} the recorded finish)";

    pub(super) const UNPUBLISHED: &str =
        "{@warn} {n|bold} declared metric(s) this build does not publish";

    pub(super) const CHIP: &str = "{tag} {total|bold}";
    pub(super) const CHIPS_AND_RATE: &str = "{chips}{summary:>*|bold}";
    pub(super) const LABEL_VALUE: &str = "{label|dim}{value:>*|bold}";
    pub(super) const EMPTY: &str = "{why|dim}";
    pub(super) const VIOLATION: &str = "{@fail|fail} {probe|fail}";
    pub(super) const COVERAGE_GAP: &str = "{@warn} coverage gap {@dot|dim} {gap}";
    pub(super) const ERROR: &str = "{@fail|fail} {error}";
    pub(super) const INDENTED: &str = "  {text|dim}";
    pub(super) const TALLY: &str = "  [{ticks|bold} ticks {@dot|dim} ]\
        {violations|bold} violations[ {@dot|dim} {ok|bold}/{total} probes ok]\
        [ {@dot|dim} {dropped|bold} snapshots dropped]";
}

/// Bind and draw. Zero elapsed: a report prints once, so no template here holds a spinner
fn draw(t: &Template, data: &Fields<'_>, width: usize, theme: &Theme) -> String {
    t.render_str(data, width, Duration::ZERO, theme)
}

/// `label   value` row, value right-aligned to the box interior
fn label_value(label: &str, value: &str, inner: usize, theme: &Theme) -> String {
    let data = Fields::new().text("label", label).text("value", value);
    draw(&Template::parse(row::LABEL_VALUE), &data, inner, theme)
}

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
    /// `(recorded, observed)` past scrape lag. Rendered, never resolved — the record keeps
    /// the verdict's number and the divergence is itself the finding
    pub height_check: Option<(u32, u32)>,
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
    /// Per-stage cost + its tail. Paired by label: `fetch` carries `fetch p99`
    pub write_path: Vec<Series>,
    pub store: Vec<Series>,
    pub resources: Vec<ComponentResources>,
    /// `label <- family{sel}` the build never published (em-dashes read like a quiet run)
    pub unpublished: Vec<String>,
    pub note: Option<String>,
    pub grafana: Option<String>,
}

/// [`render_sync_report`] without its panels: the same header + footer over the same
/// view, for a surface that already scrolled the series past (`watch`'s settled verdict,
/// which never queries the TSDB and would draw every panel as "no series recorded")
pub fn render_sync_verdict(view: &ReportView, theme: &Theme, width: usize) -> String {
    let width = width.min(MAX_WIDTH);
    let mut out = String::with_capacity(1024);
    header(&mut out, view, theme, width);
    footer(&mut out, view, theme, width);
    out
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
        (
            latency_box("write path", &view.write_path, view, theme, left_w),
            gauge_box("store", &view.store, view, theme, right_w),
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
pub fn status_mark(status: SyncStatus, theme: &Theme) -> (&'static str, &'static str) {
    let glyph = match status {
        SyncStatus::Finished(v) if v.is_pass() => theme.chars.ok,
        SyncStatus::Finished(_) => theme.chars.fail,
        SyncStatus::Pending | SyncStatus::Running => theme.chars.dot,
        SyncStatus::Unresolved => theme.chars.warn,
    };
    (glyph, status_tone(status))
}

/// Status ink as a template tone, paired with [`status_mark`]'s glyph
pub(super) fn status_tone(status: SyncStatus) -> &'static str {
    match status {
        SyncStatus::Finished(v) if v.is_pass() => "pass",
        SyncStatus::Finished(_) | SyncStatus::Unresolved => "fail",
        SyncStatus::Pending | SyncStatus::Running => "bold",
    }
}

fn header(out: &mut String, v: &ReportView, theme: &Theme, width: usize) {
    let (mark, _) = status_mark(v.status, theme);
    let tone = status_tone(v.status);
    let live = v.status.is_live();
    let mut line = |src: &str, data: &Fields<'_>| {
        let _ =
            writeln!(out, "{}", truncate(&draw(&Template::parse(src), data, width, theme), width));
    };

    let ident = Fields::new()
        .text("mark", mark)
        .text("id", &*v.sync_id)
        .text("profile", format!("[{}]", v.profile));
    line(&row::ident(tone), &ident);

    // - Phase = chain-walk position, live only (a finished run's last phase adds nothing)
    // - A live run has no segment *yet*; only a finished one that recorded none answers for it
    let segment = match (v.segment.as_deref(), live) {
        (Some(s), _) => Some(s),
        (None, true) => None,
        (None, false) => Some("no segment recorded"),
    };
    let status = Fields::new()
        .text("status", v.status.to_string())
        .text("elapsed", format_elapsed(v.elapsed))
        .maybe_text("phase", v.phase.filter(|_| live).map(|p| p.to_string()))
        .maybe_text("detail", v.phase_detail.as_deref())
        .maybe_text("segment", segment);
    line(&row::status(tone), &status);

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
        let (standing, tone) = match (live, target.saturating_sub(reached)) {
            (true, _) => (v.eta.map(|e| format!("eta {}", format_elapsed(e))), "bold"),
            (_, 0) => (Some("complete".to_string()), "pass"),
            (_, n) => (Some(format!("short {}", thousands(u64::from(n)))), "fail"),
        };
        // Chain moved on while the run worked: context, never a failure
        let behind = v
            .tip
            .map(|tip| tip.saturating_sub(target))
            .filter(|n| *n > 0)
            .map(|n| format!("tip +{}", thousands(u64::from(n))));
        let height = Fields::new()
            .text("reached", thousands(u64::from(reached)))
            .text("target", thousands(u64::from(target)))
            .maybe_text("standing", standing)
            .text("fill", theme.chars.bar_fill.repeat(filled))
            .text("empty", theme.chars.bar_empty.repeat(bar_w.saturating_sub(filled)))
            .value("pct", pct / 100.0)
            .maybe_text("behind", behind);
        line(&row::height(tone), &height);
    }
    // Two observers of one run disagreeing = the finding; the record's number stands above
    if let Some((recorded, observed)) = v.height_check {
        let (side, gap) = match observed < recorded {
            true => ("below", recorded - observed),
            false => ("above", observed - recorded),
        };
        let f = Fields::new()
            .text("observed", thousands(u64::from(observed)))
            .text("side", side.to_string())
            .text("gap", thousands(u64::from(gap)));
        line(row::HEIGHT_CHECK, &f);
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
    let summary = summary_text(pools, (pools.len() > 1).then_some(total), theme);
    let chips = pool_chips(pools, theme, inner);
    match display_width(&chips) + display_width(&summary) < inner {
        true => {
            let data = Fields::new().text("chips", chips).text("summary", summary);
            body.push(draw(&Template::parse(row::CHIPS_AND_RATE), &data, inner, theme));
        }
        false => {
            body.push(chips);
            body.push(label_value("", &summary, inner, theme));
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

    // Tag pre-styled: a pool's ink is its band's, which no named tone can name
    let chip = Template::parse(row::CHIP);
    let chips: Vec<String> = ranked
        .iter()
        .map(|s| {
            let data = Fields::new()
                .text("tag", pool_tag(&s.label).style(palette.style_of(&s.label)).to_string())
                .text(
                    "total",
                    s.integral()
                        .map(|t| unit_value(Unit::Count, t))
                        .unwrap_or_else(|| theme.chars.na.to_string()),
                );
            draw(&chip, &data, budget, theme)
        })
        .collect();
    // Themed, not a literal `*`: the ASCII spelling happened to match, so a Unicode
    // report drew a separator no other surface uses
    let sep = format!(" {} ", theme.chars.dot.style(palette.separator()));
    truncate(&chips.join(&sep), budget)
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
    body.push(label_value("", &summary_text(series, Some(total), theme), inner, theme));
    boxed(title, "", &body, width, theme)
}

/// Tail series' label = its stage's, suffixed. Paired here rather than plotted apart,
/// since a mean and its p99 are one reading
const P99_SUFFIX: &str = " p99";

/// Per-stage cost, one line each.
///
/// - No plot: two stages' latencies never add to a quantity, so a stack would draw one
/// - `peak` off the tail series where published (worst window, at the sharpest resolution)
/// - Counters riding this facet (errors) report their whole-run total, not a duration
fn latency_box(
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
    let tail =
        |stage: &str| series.iter().find(|s| s.label.strip_suffix(P99_SUFFIX) == Some(stage));

    let mut body = Vec::new();
    for s in series.iter().filter(|s| !s.label.ends_with(P99_SUFFIX)) {
        let tail = tail(&s.label);
        let mut parts = Vec::new();
        if s.unit == Unit::Millis {
            if let Some(mean) = s.mean() {
                parts.push(format!("avg {}", unit_value(s.unit, mean)));
            }
            if let Some(p99) = tail.and_then(Series::mean) {
                parts.push(format!("p99 {}", unit_value(s.unit, p99)));
            }
            // Tail's peak where there is one: the mean series' worst window still averages
            if let Some(peak) = tail.or(Some(s)).and_then(Series::peak) {
                parts.push(format!("peak {}", unit_value(s.unit, peak)));
            }
        } else if let Some(last) = s.last() {
            parts.push(format!("{} total", unit_value(s.unit, last)));
        }
        if parts.is_empty() {
            continue;
        }
        let sep = format!(" {} ", theme.chars.dot);
        body.push(label_value(&s.label, &parts.join(&sep), inner, theme));
    }
    if body.is_empty() {
        return empty(theme, title, v, width);
    }
    boxed(title, "", &body, width, theme)
}

/// A held quantity rather than a flow: plotted as published, summarised by where it
/// finished. No total — integrating a size yields byte-seconds
fn gauge_box(
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
    let unit = series[0].unit;
    let mut body: Vec<String> = stacked(series, PLOT_ROWS, Some(unit), inner, 0, theme)
        .into_iter()
        .map(|(label, plotted)| format!("{} {plotted}", label.style(theme.styles.dim)))
        .collect();
    for s in series {
        let Some(peak) = s.peak() else { continue };
        let mut parts = vec![format!("peak {}", unit_value(unit, peak))];
        if let Some(last) = s.last() {
            parts.push(format!("final {}", unit_value(unit, last)));
        }
        let sep = format!(" {} ", theme.chars.dot);
        body.push(label_value(&s.label, &parts.join(&sep), inner, theme));
    }
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
        body.push(label_value(name, &format!("peak {}", unit_value(unit, peak)), inner, theme));
    }
    boxed(&format!("resources {} {}", theme.chars.dot, r.component), "", &body, width, theme)
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
    let mut line = |src: &str, data: &Fields<'_>| {
        let _ = writeln!(out, "{}", draw(&Template::parse(src), data, width, theme));
    };

    // Wrapped, never truncated: this is the one line stating *why* the run failed, and
    // a backend error carries its cause last (`{component} {op}: {source}`) — clipping
    // to the terminal keeps the half that names nothing
    for (probe, detail) in &v.violations {
        line(row::VIOLATION, &Fields::new().text("probe", &**probe));
        for text in wrap(detail, width.saturating_sub(VIOLATION_INDENT.len())) {
            line(&row::violation_detail(), &Fields::new().text("line", text));
        }
    }
    for gap in &v.coverage_gaps {
        let data = Fields::new().text("gap", &**gap);
        line(row::COVERAGE_GAP, &data);
    }
    // Not a verdict (run unaffected), but every panel these rows feed is blank for a reason
    if !v.unpublished.is_empty() {
        line(row::UNPUBLISHED, &Fields::new().text("n", v.unpublished.len().to_string()));
        for row in &v.unpublished {
            line(row::INDENTED, &Fields::new().text("text", &**row));
        }
    }
    if let Some(error) = &v.error {
        line(row::ERROR, &Fields::new().text("error", &**error));
    }
    for text in v.note.iter().chain(v.grafana.iter()) {
        line(row::INDENTED, &Fields::new().text("text", &**text));
    }
    // - Probe tally = the live counterpart of a verdict (before one exists, the only standing)
    // - `0 ticks` reads as a stalled run rather than one still starting
    // - Counts through `thousands`, never `Unit::Count` (`1.50k ticks` points at no tick)
    let tally = Fields::new()
        .text("violations", thousands(v.violations.len() as u64))
        .maybe_text("ticks", Some(v.ticks).filter(|n| *n > 0).map(thousands))
        .maybe_text("ok", v.probes.map(|(ok, _)| thousands(ok as u64)))
        .maybe_text("total", v.probes.map(|(_, total)| thousands(total as u64)))
        .maybe_text("dropped", Some(v.dropped_snapshots).filter(|n| *n > 0).map(thousands));
    line(row::TALLY, &tally);
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

/// Axis labels top-down, right-aligned, plus the width they needed.
/// Unitless caller = [`Unit::Count`] (bare magnitude, keeping [`y_axis`] the only path)
fn axis(ceiling: f64, rows: usize, unit: Option<Unit>) -> (Vec<String>, usize) {
    let raw = y_axis(ceiling, rows, unit.unwrap_or(Unit::Count), 0);
    let width = raw.iter().map(|l| display_width(l)).max().unwrap_or(0);
    (raw.into_iter().map(|l| format!("{:>width$}", l)).collect(), width)
}

/// `avg 44.3k/s · peak 88.0k/s · 78.3M total`. Unstyled — callers that pair it with
/// pre-styled text lay it out themselves.
///
/// - avg/peak in the series' declared [`Unit`] (hardcoded `/s` mislabels a byte rate)
/// - total = an [`integral`](Series::integral), i.e. a count → no `/s`
fn summary_text(series: &[Series], total: Option<f64>, theme: &Theme) -> String {
    let unit = series.first().map_or(Unit::Count, |s| s.unit);
    let mean: f64 = series.iter().filter_map(Series::mean).sum();
    // Peak of the sum — summing per-series maxima invents a moment the run never had
    let peak = stacked_peak(series);
    let total = total
        .filter(|t| *t > 0.0)
        .map(|t| format!(" {} {} total", theme.chars.dot, unit_value(Unit::Count, t)))
        .unwrap_or_default();
    format!(
        "avg {} {} peak {}{total}",
        unit_value(unit, mean),
        theme.chars.dot,
        unit_value(unit, peak)
    )
}

/// Peak of the *sum*, not the sum of peaks: series do not peak together, and adding
/// their maxima invents a moment the run never had.
///
/// Summed by timestamp, never by position — a dropped `NaN` shortens one series and
/// slides every later sample against its neighbours
fn stacked_peak(series: &[Series]) -> f64 {
    let mut at: std::collections::BTreeMap<u64, f64> = std::collections::BTreeMap::new();
    for (t, v) in series.iter().flat_map(|s| &s.points) {
        *at.entry((t * 1000.0).round() as u64).or_default() += v;
    }
    at.into_values().fold(0.0, f64::max)
}

/// An empty frame reads as "nothing happened"; the real cause is the record's reach,
/// which is a fact about retention and the cluster, not about the run
fn empty(theme: &Theme, title: &str, v: &ReportView, width: usize) -> Vec<String> {
    let why = v.note.as_deref().unwrap_or("no series recorded for this run");
    let data = Fields::new().text("why", why);
    boxed(
        title,
        "",
        &[draw(&Template::parse(row::EMPTY), &data, interior(width), theme)],
        width,
        theme,
    )
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

    /// `shift` in samples. Stacked channels get different shifts — in phase, sum-of-peaks
    /// and peak-of-sum agree, and the fixture cannot tell them apart
    fn wave(scale: f64, n: usize, shift: f64) -> Vec<f64> {
        (0..n).map(|i| scale * (1.0 + ((i as f64 + shift) / 9.0).sin())).collect()
    }

    pub(super) fn view() -> ReportView {
        ReportView {
            sync_id: "zaino-index-construction-c3115f50".into(),
            profile: "zaino_index_construction".into(),
            status: SyncStatus::Finished(ztest::api::SyncVerdict::Failed),
            phase: None,
            phase_detail: None,
            height_check: None,
            unpublished: Vec::new(),
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
            transparent: vec![series("transparent", Unit::PerSec, &wave(70_000.0, 60, 0.0))],
            // Half a period apart: these two never peak in the same slot
            shielded: vec![
                series("sapling", Unit::PerSec, &wave(30_000.0, 60, 0.0)),
                series("orchard", Unit::PerSec, &wave(14_000.0, 60, 14.0)),
                series("ironwood", Unit::PerSec, &wave(0.0, 60, 0.0)),
            ],
            blocks: vec![series("blocks", Unit::PerSec, &wave(400.0, 60, 0.0))],
            throughput: vec![series("transactions", Unit::PerSec, &wave(900.0, 60, 0.0))],
            write_path: vec![
                series("fetch", Unit::Millis, &wave(4.0, 60, 0.0)),
                series("fetch p99", Unit::Millis, &wave(30.0, 60, 14.0)),
                series("treestate", Unit::Millis, &wave(1.2, 60, 0.0)),
                series("batch write", Unit::Millis, &wave(880.0, 60, 7.0)),
                series("errors", Unit::Count, &[0.0; 60]),
            ],
            store: vec![series("db used", Unit::Bytes, &wave(4e9, 60, 0.0))],
            resources: vec![
                ComponentResources {
                    component: "zainod".into(),
                    cpu: vec![series("zainod", Unit::Cores, &wave(3.0, 60, 0.0))],
                    mem: vec![series("zainod", Unit::Bytes, &wave(6e9, 60, 0.0))],
                    disk: vec![
                        series("read", Unit::BytesPerSec, &wave(4.1e8, 60, 0.0)),
                        series("write", Unit::BytesPerSec, &wave(9e7, 60, 14.0)),
                    ],
                    io_stall: vec![series("zainod", Unit::Fraction, &wave(0.004, 60, 0.0))],
                },
                ComponentResources {
                    component: "zebrad".into(),
                    cpu: vec![series("zebrad", Unit::Cores, &wave(1.0, 60, 0.0))],
                    mem: vec![series("zebrad", Unit::Bytes, &wave(2e9, 60, 0.0))],
                    disk: vec![series("write", Unit::BytesPerSec, &wave(2.2e8, 60, 0.0))],
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

    /// Verdict carries every header/footer reading of the full report, and no panel
    #[test]
    fn the_verdict_drops_the_panels_and_keeps_the_findings() {
        let rendered = render_sync_verdict(&view(), &theme(), 120);
        for kept in ["zaino-index-construction-c3115f50", "short 1,001", "2 violations"] {
            assert!(rendered.contains(kept), "{kept} missing:\n{rendered}");
        }
        for dropped in ["ops/sec", "resources ·", "no series recorded"] {
            assert!(!rendered.contains(dropped), "{dropped} drawn:\n{rendered}");
        }
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
            cpu: vec![series("lightwalletd", Unit::Cores, &wave(0.5, 60, 0.0))],
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

    /// Reading the verdict off the series printed `short 4,479` in fail ink on a finished run
    #[test]
    fn a_stale_final_scrape_annotates_instead_of_inventing_a_shortfall() {
        let mut v = view();
        v.height = Some((659_600, 659_600));
        v.height_check = Some((659_600, 655_121));
        let rendered = render_sync_report(&v, &theme(), 120);
        assert!(rendered.contains("complete"), "the record's verdict stands: {rendered}");
        assert!(!rendered.contains("short "), "{rendered}");
        assert!(rendered.contains("655,121"), "the disagreement is stated: {rendered}");
        assert!(rendered.contains("below"), "{rendered}");
    }

    /// Silence is the normal case and must cost no ink
    #[test]
    fn agreement_between_the_record_and_the_series_draws_nothing() {
        let rendered = render_sync_report(&view(), &theme(), 120);
        assert!(!rendered.contains("prometheus last read"), "{rendered}");
        assert!(!rendered.contains("does not publish"), "{rendered}");
    }

    /// Renamed family → empty panel, indistinguishable from a quiet run
    #[test]
    fn rows_the_build_never_published_are_named_in_the_footer() {
        let mut v = view();
        v.unpublished = vec![
            "db used <- zaino_db_used_bytes".into(),
            "fetch <- zaino_sync_block_fetch_seconds{stage=\"finalised\"}".into(),
        ];
        let rendered = render_sync_report(&v, &theme(), 120);
        assert!(rendered.contains("2 declared metric(s)"), "{rendered}");
        assert!(rendered.contains("zaino_db_used_bytes"), "{rendered}");
        assert!(rendered.contains("stage=\"finalised\""), "the selector too: {rendered}");
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

    /// A dropped `NaN` shortens one series; summing by position then adds each later
    /// sample to a neighbour's *earlier* one and reports a peak from two different instants
    #[test]
    fn a_stack_sums_by_timestamp_rather_than_by_position() {
        let at = |points: &[(f64, f64)]| Series {
            label: "x".into(),
            unit: Unit::Cores,
            facet: None,
            points: points.to_vec(),
        };
        // Second series lost its t=0 sample: by position its 4.0 would land against 1.0
        let full = at(&[(0.0, 1.0), (30.0, 2.0), (60.0, 9.0)]);
        let gapped = at(&[(30.0, 4.0), (60.0, 1.0)]);
        assert_eq!(stacked_peak(&[full, gapped]), 10.0, "9+1 at t=60, never 1+4 across slots");
    }

    /// Tail belongs to its stage's line — surfaced as its own row it reads as a fifth
    /// stage, and the stage it describes loses the number that makes it actionable
    #[test]
    fn a_tail_row_joins_its_stage_rather_than_becoming_one() {
        let rendered = latency_box("write path", &view().write_path, &view(), &theme(), 60);
        let body: Vec<&String> =
            rendered.iter().filter(|l| l.contains("avg") || l.contains("total")).collect();
        assert_eq!(body.len(), 4, "fetch, treestate, batch write, errors: {rendered:#?}");
        let fetch = body.iter().find(|l| l.contains("fetch")).expect("the fetch line");
        assert!(fetch.contains("p99"), "{fetch}");
        assert!(!rendered.iter().any(|l| l.contains("fetch p99 ")), "no row of its own");
    }

    /// Integrating a held size yields byte-seconds; only a flow has a total
    #[test]
    fn a_store_gauge_reports_where_it_finished_and_never_a_total() {
        let rendered = gauge_box("store", &view().store, &view(), &theme(), 60);
        let summary = rendered.iter().find(|l| l.contains("peak")).expect("a summary");
        assert!(summary.contains("final"), "{summary}");
        assert!(!rendered.iter().any(|l| l.contains("total")), "{rendered:#?}");
    }

    /// Queried and dropped, these were 10 of 23 rows: the whole per-block cost breakdown
    /// paid for on every report and shown on none
    #[test]
    fn the_write_path_and_store_facets_reach_the_report() {
        let rendered = render_sync_report(&view(), &theme(), 120);
        assert!(rendered.contains("write path"), "{rendered}");
        assert!(rendered.contains("db used"), "{rendered}");
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
            series("sapling spends", Unit::PerSec, &wave(20_000.0, 60, 0.0)),
            series("sapling outputs", Unit::PerSec, &wave(10_000.0, 60, 0.0)),
            series("orchard", Unit::PerSec, &wave(14_000.0, 60, 14.0)),
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

    /// Printed once into scrollback, so nothing re-flows: no padding to reserve.
    ///
    /// Regression: the separator was a literal `*` — the ASCII spelling of `dot` — so the
    /// Unicode report drew a glyph no other surface uses, and the ASCII one looked correct
    #[test]
    fn a_chip_row_separates_pools_and_leads_with_the_largest() {
        let row = pool_chips(&view().shielded, &theme(), 60);
        assert_eq!(row, "sap 53.4M · orc 25.8M · iro 0", "{row:?}");
        let ascii = pool_chips(&view().shielded, &Theme::for_capabilities(false, false), 60);
        assert_eq!(ascii, "sap 53.4M * orc 25.8M * iro 0", "{ascii:?}");
    }

    /// Per-pool totals and the run's rates are the same reading; two lines wasted one
    #[test]
    fn the_chips_share_a_line_with_the_rate_summary() {
        let rendered = render_sync_report(&view(), &theme(), 160);
        let line = rendered.lines().find(|l| l.contains("sap 53.4M")).expect("a chip row");
        assert!(line.contains("avg 45.0k/s"), "{line:?}");
        assert!(line.contains("peak 77.3k/s"), "{line:?}");
    }

    /// Sapling and orchard peak half a period apart → maxima sum to 88.0k/s, a moment
    /// the run never had
    #[test]
    fn a_panel_peak_is_the_peak_of_the_stack_not_the_sum_of_its_channels_peaks() {
        let shielded = fold_sapling(&view().shielded);
        let sum_of_peaks: f64 = shielded.iter().filter_map(Series::peak).sum();
        let peak_of_sum = stacked_peak(&shielded);
        assert!(
            peak_of_sum < sum_of_peaks * 0.95,
            "fixture must not have its channels in phase: {peak_of_sum} vs {sum_of_peaks}"
        );
        let rendered = render_sync_report(&view(), &theme(), 160);
        assert!(rendered.contains("peak 77.3k/s"), "{rendered}");
        assert!(!rendered.contains("peak 88.0k/s"), "sum of peaks leaked back in");
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
mod ascii {
    use super::tests::{running, view};
    use super::{Theme, render_sync_report, render_sync_verdict};

    /// Both report surfaces, both fixtures, at the widths a piped report actually uses
    #[test]
    fn the_sync_report_falls_back_to_ascii() {
        let ascii = Theme::for_capabilities(false, false);
        for (name, v) in [("finished", view()), ("running", running())] {
            for width in [80, 120] {
                crate::testing::assert_ascii_clean(
                    &format!("render_sync_report({name}, {width})"),
                    &render_sync_report(&v, &ascii, width),
                );
                crate::testing::assert_ascii_clean(
                    &format!("render_sync_verdict({name}, {width})"),
                    &render_sync_verdict(&v, &ascii, width),
                );
            }
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
