//! `ztest sync status`: a run's shape and vitals, in boxes.
//!
//! - Two narrow columns by default (repeat-run command; full width would evict the
//!   developer's own scrollback each check), still carrying the graph
//! - One width per column, one time axis per graph — work/pace/probes are shown
//!   together to be read against each other, which misalignment would falsify

use std::fmt::Write as _;

use owo_colors::OwoColorize as _;

use super::plot::{self, Palette, PlotOpts};
use super::text::{compact, format_elapsed, thousands, y_axis};
use super::{SyncWatchState, Theme};

/// One short of 80 (an exactly-80 terminal would wrap the last cell, shearing
/// every box)
const WIDTH: usize = 79;

/// Left (graph) column, the larger share (time series needs horizontal room;
/// the right column holds short rows)
const LEFT: usize = 46;

/// Below this the layout stacks — neither column would hold readable content
const MIN_TWO_COLUMN: usize = 72;

const PLOT_ROWS: usize = 5;

pub fn render_sync_status(state: &SyncWatchState, theme: &Theme, width: usize) -> String {
    let width = width.min(WIDTH);
    let mut out = String::with_capacity(1024);
    header(&mut out, state, theme);

    let two_up = width >= MIN_TWO_COLUMN;
    let (left_w, right_w) = match two_up {
        true => (LEFT, width - LEFT - 1),
        false => (width, width),
    };

    let work = work_box(state, theme, left_w);
    let pools = pools_box(state, theme, right_w);
    let progress = progress_box(state, theme, left_w);
    let probes = probes_box(state, theme, right_w);

    for (a, b) in [(work, pools), (progress, probes)] {
        match two_up {
            true => beside(&mut out, &a, &b, left_w),
            false => {
                for line in a.iter().chain(b.iter()) {
                    let _ = writeln!(out, "{line}");
                }
            }
        }
    }
    out
}

fn header(out: &mut String, state: &SyncWatchState, theme: &Theme) {
    let dot = theme.chars.dot.style(theme.styles.dim);
    let _ = writeln!(
        out,
        "  {} {dot} {} {dot} {}",
        state.sync_id.style(theme.styles.count),
        state.profile.style(theme.styles.dim),
        state.pod_phase.style(theme.styles.dim),
    );
}

/// Two boxes side by side, shorter padded with blank lines so both start level
fn beside(out: &mut String, left: &[String], right: &[String], left_w: usize) {
    let rows = left.len().max(right.len());
    for i in 0..rows {
        let blank = String::new();
        let l = left.get(i).unwrap_or(&blank);
        let r = right.get(i).unwrap_or(&blank);
        let pad = left_w.saturating_sub(display_width(l));
        let line = format!("{l}{:pad$} {r}", "");
        let _ = writeln!(out, "{}", line.trim_end());
    }
}

fn work_box(state: &SyncWatchState, theme: &Theme, width: usize) -> Vec<String> {
    let inner = interior(width);
    let gutter = 6;
    let plot_w = inner.saturating_sub(gutter + 1);

    let Some(timeline) = &state.timeline else {
        // Stated, not blank — an empty frame reads as "no work happened", when
        // the fact is the run's age or build, not its throughput.
        return boxed("work", "", &[dim("no series published yet", theme)], width, theme);
    };

    // Unmeasured pool = absent, not a layer of unknown height. Left in, it floors
    // the stack with no placeable base and hatches the whole graph unplaceable
    // whenever tier B goes uncollected. Absence is stated in the `pools` box, as `—`.
    let channels: Vec<plot::Channel<'_>> = crate::sync::CHANNELS
        .iter()
        .map(|(name, _)| (*name, timeline.bands(name)))
        .filter(|(_, bands)| bands.iter().any(Option::is_some))
        .collect();
    let palette = Palette::pools(theme.is_colorized());
    let opts = PlotOpts::new(plot_w, PLOT_ROWS, theme.chars.graph);
    let ceiling = plot::ceiling(&channels, &opts);
    let rows = plot::plot_stacked(&channels, &opts, &palette);
    let axis = y_axis(ceiling, PLOT_ROWS, gutter);

    let mut body: Vec<String> = axis
        .iter()
        .zip(rows.iter())
        .map(|(label, row)| format!("{} {row}", label.style(theme.styles.dim)))
        .collect();
    // Span covered — ten minutes vs two days, which the graph itself cannot say.
    let span = format_elapsed(timeline.span());
    let gap = plot_w.saturating_sub(span.len() + 7);
    body.push(format!("{:>gutter$} {}", "", dim(&format!("{span} ago{:>gap$}now", ""), theme),));

    let now = state
        .vitals
        .as_ref()
        .and_then(|v| v.work_rate)
        .map(|r| format!("{}/s", compact(r)))
        .unwrap_or_default();
    boxed("work · ops/s", &now, &body, width, theme)
}

/// Per-pool rate breakdown, always beside the total, never instead of it (a total
/// alone can't separate a throughput change from a change in range contents)
fn pools_box(state: &SyncWatchState, theme: &Theme, width: usize) -> Vec<String> {
    let inner = interior(width);
    let Some(vitals) = &state.vitals else {
        return boxed("pools", "", &[dim("awaiting first tick", theme)], width, theme);
    };

    let body: Vec<String> = vitals
        .pool_rates
        .iter()
        .map(|(name, rate)| {
            let value = match rate {
                // Unmeasured, not idle — `0` here would be the most misleading
                // cell on the screen.
                None => "—".to_string(),
                Some(r) => format!("{}/s", compact(*r)),
            };
            let pad = inner.saturating_sub(name.len() + display_width(&value));
            let styled = match rate {
                None => value.style(theme.styles.dim).to_string(),
                Some(_) => value.style(theme.styles.count).to_string(),
            };
            format!("{}{:pad$}{styled}", name.style(theme.styles.dim), "")
        })
        .collect();
    boxed("pools", "", &body, width, theme)
}

fn progress_box(state: &SyncWatchState, theme: &Theme, width: usize) -> Vec<String> {
    let inner = interior(width);
    let dot = theme.chars.dot.style(theme.styles.dim);
    let Some(v) = &state.vitals else {
        return boxed("progress", "", &[dim("awaiting first tick", theme)], width, theme);
    };

    let target = match v.target {
        Some(t) => format!("{} / {}", thousands(u64::from(v.height)), thousands(u64::from(t))),
        None => thousands(u64::from(v.height)),
    };
    let bar_w = inner.saturating_sub(8);
    let filled = (f64::from(v.pct.clamp(0.0, 100.0)) / 100.0 * bar_w as f64).round() as usize;
    let bar = format!(
        "{}{}",
        theme.chars.bar_fill.repeat(filled),
        theme.chars.bar_empty.repeat(bar_w.saturating_sub(filled)),
    );

    let mut body = vec![
        row("height", &target, inner, theme),
        format!("{bar} {:>5.1}%", v.pct),
        row(
            "pace",
            &format!(
                "{} {dot} {}",
                v.pace.map(|p| format!("{:.1} blk/s", p.per_sec)).unwrap_or_else(|| "—".into()),
                v.phase
            ),
            inner,
            theme,
        ),
    ];
    if let Some(eta) = v.pace.and_then(|p| p.eta) {
        body.push(row("eta", &format_elapsed(eta), inner, theme));
    }
    if v.reorg_depth > 0 {
        body.push(row("reorg", &format!("-{}", v.reorg_depth), inner, theme));
    }
    boxed("progress", "", &body, width, theme)
}

fn probes_box(state: &SyncWatchState, theme: &Theme, width: usize) -> Vec<String> {
    let inner = interior(width);
    let (ok, total) = state.probe_tally();
    let mut body: Vec<String> = state
        .probes
        .iter()
        .take(4)
        .map(|p| {
            let (mark, style) = match p.state {
                crate::sync::ProbeState::Violating => (theme.chars.fail, theme.styles.fail),
                crate::sync::ProbeState::Ok => (theme.chars.ok, theme.styles.pass),
                _ => (theme.chars.warn, theme.styles.skip),
            };
            // Countdown = the whole value of a live `eventually` probe (a name
            // says unsatisfied; `18s/30s` says about-to-fail).
            let countdown = match (p.since_satisfied, p.window) {
                (Some(since), Some(window)) => {
                    format!(" {}/{}", format_elapsed(since), format_elapsed(window))
                }
                _ => String::new(),
            };
            let room = inner.saturating_sub(3 + countdown.chars().count());
            let name: String = p.name.chars().take(room).collect();
            format!("{} {name}{}", mark.style(style), dim(&countdown, theme))
        })
        .collect();
    // State what was dropped — silent truncation reads as a shorter board.
    if state.probes.len() > 4 {
        body.push(dim(&format!("… {} more", state.probes.len() - 4), theme));
    }
    if body.is_empty() {
        body.push(dim("no probes registered", theme));
    }
    if state.violations > 0 {
        body.push(
            format!("{} violation(s)", state.violations).style(theme.styles.fail).to_string(),
        );
    }
    boxed(&format!("probes {ok}/{total} ok"), "", &body, width, theme)
}

// ───────────────────────────── box drawing ─────────────────────────────

/// Content width inside a `width`-column box: less two borders, two pad spaces
fn interior(width: usize) -> usize {
    width.saturating_sub(4)
}

/// Frame `body` in a titled box of exactly `width` columns. Enforced by padding +
/// truncation here, never trusted from callers (one long line shears every box below)
fn boxed(title: &str, right: &str, body: &[String], width: usize, theme: &Theme) -> Vec<String> {
    let [tl, tr, bl, br, h, v] = theme.chars.frame;
    let inner = interior(width);
    let dim = theme.styles.dim;

    let head = format!("{h} {title} ");
    let tail = match right.is_empty() {
        true => String::new(),
        false => format!(" {right} "),
    };
    // Display width, not bytes — frame glyphs and the title's separator dot are
    // multibyte, and byte-measuring shortens the rule per glyph.
    let rule = width.saturating_sub(2 + display_width(&head) + display_width(&tail));

    let mut out = vec![format!(
        "{}{}{}{}{}",
        tl.style(dim),
        head.style(theme.styles.count),
        h.repeat(rule).style(dim),
        tail.style(dim),
        tr.style(dim),
    )];
    for line in body {
        let shown = truncate(line, inner);
        let pad = inner.saturating_sub(display_width(&shown));
        out.push(format!("{} {shown}{:pad$} {}", v.style(dim), "", v.style(dim)));
    }
    out.push(format!(
        "{}{}{}",
        bl.style(dim),
        h.repeat(width.saturating_sub(2)).style(dim),
        br.style(dim)
    ));
    out
}

/// `label   value` row, right-aligned to the box interior
fn row(label: &str, value: &str, inner: usize, theme: &Theme) -> String {
    let pad = inner.saturating_sub(label.len() + display_width(value));
    format!("{}{:pad$}{}", label.style(theme.styles.dim), "", value.style(theme.styles.count))
}

fn dim(text: &str, theme: &Theme) -> String {
    text.style(theme.styles.dim).to_string()
}

/// Printable width, ANSI escapes ignored (padding against byte length leaves
/// every coloured box ragged)
fn display_width(s: &str) -> usize {
    console::measure_text_width(s)
}

/// Cut `s` to `width` printable columns, escapes preserved
fn truncate(s: &str, width: usize) -> String {
    match display_width(s) <= width {
        true => s.to_string(),
        false => console::truncate_str(s, width, "").into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::SyncVitals;
    use super::*;
    use crate::sync::Timeline;
    use std::time::Duration;

    pub(super) fn theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn vitals() -> SyncVitals {
        SyncVitals {
            height: 1_204_551,
            target: Some(1_700_000),
            pct: 70.9,
            phase: "Historic".into(),
            reorg_depth: 0,
            pace: Some(crate::rate::Pace { per_sec: 412.0, eta: Some(Duration::from_secs(4_320)) }),
            work_rate: Some(23_600.0),
            pool_rates: vec![
                ("transparent", None),
                ("sprout", None),
                ("sapling", Some(19_400.0)),
                ("orchard", Some(4_200.0)),
                ("ironwood", Some(0.0)),
            ],
            cost: crate::sync::Cost::default(),
            received_at: Duration::from_secs(0),
        }
    }

    fn timeline() -> Timeline {
        let mut t =
            Timeline::new(crate::sync::CHANNELS.map(|(name, _)| name), Duration::from_secs(5));
        for i in 0..200u64 {
            // Wave + a dropped-in stall, so a visual check exercises shape and
            // band, not a flat block.
            let phase = (i as f64 / 12.0).sin();
            let sapling = match i {
                90..=96 => 400.0,
                _ => 14_000.0 + 6_000.0 * phase,
            };
            t.push(
                Duration::from_secs(i * 5),
                &[None, None, Some(sapling), Some(4_000.0 + 1_500.0 * phase), Some(0.0)],
            );
        }
        t
    }

    pub(super) fn state() -> SyncWatchState {
        SyncWatchState {
            profile: "zaino_ingest".into(),
            sync_id: "sync-7f3a".into(),
            context: "crc".into(),
            pod_phase: "Running".into(),
            setup: None,
            vitals: Some(vitals()),
            metrics_note: None,
            probes: Vec::new(),
            violations: 0,
            timeline: Some(timeline()),
        }
    }

    fn lines(width: usize) -> Vec<String> {
        render_sync_status(&state(), &theme(), width).lines().map(str::to_string).collect()
    }

    /// Layout's founding invariant: one over-long line wraps and shears every box
    /// below, at any terminal size incl. narrower than the layout wants
    #[test]
    fn no_line_ever_exceeds_the_requested_width() {
        for width in [40, 60, 72, 79, 100, 200] {
            for line in lines(width) {
                assert!(
                    display_width(&line) <= width.min(WIDTH),
                    "width {width}: {:?} is {} wide",
                    line,
                    display_width(&line)
                );
            }
        }
    }

    /// Point of the two-column default: no whole-screen takeover, columns really adjacent
    #[test]
    fn a_normal_terminal_gets_two_columns() {
        let rendered = lines(79);
        assert!(
            rendered.iter().any(|l| l.contains("work") && l.contains("pools")),
            "work and pools must share a line: {rendered:#?}"
        );
        assert!(
            rendered.iter().any(|l| l.contains("progress") && l.contains("probes")),
            "progress and probes must share a line"
        );
    }

    /// Stack, never truncate (half a box is worse than a taller view)
    #[test]
    fn a_narrow_terminal_stacks_the_columns() {
        let rendered = lines(50);
        assert!(rendered.iter().any(|l| l.contains("work")));
        assert!(rendered.iter().any(|l| l.contains("pools")));
        assert!(
            !rendered.iter().any(|l| l.contains("work") && l.contains("pools")),
            "a narrow terminal must not try to fit both: {rendered:#?}"
        );
    }

    /// Unmeasured-vs-idle, carried wire → screen
    #[test]
    fn an_unmeasured_pool_renders_as_absent_and_an_idle_one_as_zero() {
        let rendered = lines(79).join("\n");
        let line_with = |name: &str| {
            rendered
                .lines()
                .find(|l| l.contains(name))
                .unwrap_or_else(|| panic!("no {name} row in:\n{rendered}"))
                .to_string()
        };
        assert!(
            line_with("transparent").contains('—'),
            "unmeasured must be an em dash, not a zero: {}",
            line_with("transparent")
        );
        assert!(line_with("sprout").contains('—'));
        assert!(line_with("ironwood").contains('0'));
        assert!(line_with("sapling").contains("19.4k"));
    }

    /// Empty frame reads as "no work happened"; real cause = no series yet, a fact
    /// about the run's age
    #[test]
    fn a_run_with_no_series_says_so_rather_than_drawing_an_empty_graph() {
        let mut s = state();
        s.timeline = None;
        let rendered = render_sync_status(&s, &theme(), 79);
        assert!(rendered.contains("no series published yet"), "{rendered}");
    }

    /// Pre-first-tick = no vitals at all; every box must cope, not unwrap-panic
    #[test]
    fn a_sync_before_its_first_tick_still_renders() {
        let mut s = state();
        s.vitals = None;
        s.timeline = None;
        let rendered = render_sync_status(&s, &theme(), 79);
        assert!(rendered.contains("awaiting first tick"), "{rendered}");
        for line in rendered.lines() {
            assert!(display_width(line) <= 79, "{line:?}");
        }
    }

    /// Misalignment within a column shears the layout
    #[test]
    fn every_box_in_a_column_is_the_same_width() {
        let widths: Vec<usize> = boxed("t", "", &["a".into(), "bb".into()], 40, &theme())
            .iter()
            .map(|l| display_width(l))
            .collect();
        assert!(widths.iter().all(|&w| w == 40), "{widths:?}");
    }

    /// Over-long body line truncates; it never breaks the frame
    #[test]
    fn an_over_long_body_line_is_cut_to_fit() {
        let long = "x".repeat(200);
        for line in boxed("t", "", &[long], 40, &theme()) {
            assert_eq!(display_width(&line), 40);
        }
    }

    /// `status` runs against several syncs → an unlabelled answer is ambiguous
    #[test]
    fn the_header_names_the_sync_and_its_profile() {
        let rendered = render_sync_status(&state(), &theme(), 79);
        let first = rendered.lines().next().expect("a header");
        assert!(first.contains("sync-7f3a") && first.contains("zaino_ingest"), "{first}");
    }
}

#[cfg(test)]
mod preview {
    #[test]
    #[ignore = "visual check: cargo test -- --ignored --nocapture preview"]
    fn show() {
        println!(
            "\n{}",
            super::render_sync_status(&super::tests::state(), &super::tests::theme(), 79)
        );
    }
}
