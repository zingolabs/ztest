//! The `ztest sync status` view: a run's shape and vitals, in boxes.
//!
//! Two columns by default, deliberately. `status` is a command a developer runs
//! repeatedly in a terminal they are also using for other things, so a view that
//! claimed the whole width would push their scrollback away every time they
//! checked on a sync. Two narrow columns fit an 80-column terminal, leave the
//! rest of the screen alone, and still carry the graph — which is the part that
//! makes the answer glanceable rather than a wall of figures.
//!
//! Every box in a column shares one width and every graph one time axis, because
//! the whole reason to show work beside pace beside probes is to read them
//! against each other. A misaligned column would make that correlation an
//! artefact of the layout.

use std::fmt::Write as _;

use owo_colors::OwoColorize as _;

use super::plot::{self, Palette, PlotOpts};
use super::text::{compact, format_elapsed, thousands, y_axis};
use super::{SyncWatchState, Theme};

/// Total width the view occupies.
///
/// One column short of the classic 80 so a terminal at exactly 80 does not wrap
/// the last cell onto its own line, which would break every box.
const WIDTH: usize = 79;

/// Width of the left (graph) column. The graph gets the larger share because a
/// time series needs horizontal room to say anything, whereas the right column
/// holds short labelled rows.
const LEFT: usize = 46;

/// Below this the two columns cannot both hold readable content, so the layout
/// stacks instead of truncating.
const MIN_TWO_COLUMN: usize = 72;

/// Rows of plot inside the work box.
const PLOT_ROWS: usize = 5;

/// Render the status view for `state` at `width` columns.
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

/// The identifying line above the boxes: which sync, which profile, how long.
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

/// Lay two boxes side by side, padding the shorter with blank lines so the
/// second column starts level however tall each happens to be.
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

/// The work graph: the run's whole shape, stacked by pool.
fn work_box(state: &SyncWatchState, theme: &Theme, width: usize) -> Vec<String> {
    let inner = interior(width);
    let gutter = 6;
    let plot_w = inner.saturating_sub(gutter + 1);

    let Some(timeline) = &state.timeline else {
        // An empty frame would read as "no work happened". The cause is that
        // this driver has not published a series yet, which is a fact about the
        // run's age or its build, not about its throughput.
        return boxed(
            "work",
            "",
            &[dim("no series published yet", theme)],
            width,
            theme,
        );
    };

    // A pool nobody measured is absent from the decomposition, not a layer of
    // unknown height. Left in, it would sit at the bottom of the stack with no
    // placeable floor and invalidate every pool above it — hatching the entire
    // graph as unplaceable in the ordinary case where tier B is simply not
    // collected. The `pools` box is where its absence is stated, as `—`.
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
    // The span the graph covers, so a reader knows whether they are looking at
    // ten minutes or two days — which the graph itself cannot say.
    let span = format_elapsed(timeline.span());
    let gap = plot_w.saturating_sub(span.len() + 7);
    body.push(format!(
        "{:>gutter$} {}",
        "",
        dim(&format!("{span} ago{:>gap$}now", ""), theme),
    ));

    let now = state
        .vitals
        .as_ref()
        .and_then(|v| v.work_rate)
        .map(|r| format!("{}/s", compact(r)))
        .unwrap_or_default();
    boxed("work · ops/s", &now, &body, width, theme)
}

/// The per-pool rate breakdown.
///
/// Always shown beside the total, never instead of it: a total alone cannot
/// distinguish a throughput change from a change in what the range contained,
/// and the breakdown is what makes that visible.
fn pools_box(state: &SyncWatchState, theme: &Theme, width: usize) -> Vec<String> {
    let inner = interior(width);
    let Some(vitals) = &state.vitals else {
        return boxed(
            "pools",
            "",
            &[dim("awaiting first tick", theme)],
            width,
            theme,
        );
    };

    let body: Vec<String> = vitals
        .pool_rates
        .iter()
        .map(|(name, rate)| {
            let value = match rate {
                // Unmeasured, not idle. Rendering `0` here would state that the
                // range holds no transparent activity when in truth nothing
                // counted it — the single most misleading cell on the screen.
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

/// Chain position, pace, and projection.
fn progress_box(state: &SyncWatchState, theme: &Theme, width: usize) -> Vec<String> {
    let inner = interior(width);
    let dot = theme.chars.dot.style(theme.styles.dim);
    let Some(v) = &state.vitals else {
        return boxed(
            "progress",
            "",
            &[dim("awaiting first tick", theme)],
            width,
            theme,
        );
    };

    let target = match v.target {
        Some(t) => format!(
            "{} / {}",
            thousands(u64::from(v.height)),
            thousands(u64::from(t))
        ),
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
                v.blocks_per_sec
                    .map(|r| format!("{r:.1} blk/s"))
                    .unwrap_or_else(|| "—".into()),
                v.phase
            ),
            inner,
            theme,
        ),
    ];
    if let Some(eta) = v.eta {
        body.push(row("eta", &format_elapsed(eta), inner, theme));
    }
    if v.reorg_depth > 0 {
        body.push(row("reorg", &format!("-{}", v.reorg_depth), inner, theme));
    }
    boxed("progress", "", &body, width, theme)
}

/// The probe board, summarised.
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
            let name: String = p.name.chars().take(inner.saturating_sub(3)).collect();
            format!("{} {}", mark.style(style), name)
        })
        .collect();
    // Silent truncation would read as a shorter board than the run actually
    // has, so what was dropped is stated rather than merely omitted.
    if state.probes.len() > 4 {
        body.push(dim(&format!("… {} more", state.probes.len() - 4), theme));
    }
    if body.is_empty() {
        body.push(dim("no probes registered", theme));
    }
    if state.violations > 0 {
        body.push(
            format!("{} violation(s)", state.violations)
                .style(theme.styles.fail)
                .to_string(),
        );
    }
    boxed(&format!("probes {ok}/{total} ok"), "", &body, width, theme)
}

// ───────────────────────────── box drawing ─────────────────────────────

/// Content width inside a box of `width` columns: two borders and two spaces of
/// padding.
fn interior(width: usize) -> usize {
    width.saturating_sub(4)
}

/// Frame `body` in a titled box exactly `width` columns wide.
///
/// Width is enforced by padding and truncation here rather than trusted from
/// callers: a single over-long line wraps in the terminal and shears every box
/// below it, so the invariant has to hold at the one place that draws them.
fn boxed(title: &str, right: &str, body: &[String], width: usize, theme: &Theme) -> Vec<String> {
    let [tl, tr, bl, br, h, v] = theme.chars.frame;
    let inner = interior(width);
    let dim = theme.styles.dim;

    let head = format!("{h} {title} ");
    let tail = match right.is_empty() {
        true => String::new(),
        false => format!(" {right} "),
    };
    // Display width, not byte length: the frame glyphs and the separator dot in
    // a title are multibyte, and measuring bytes would shorten every rule by
    // however many of them the title happened to contain.
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
        out.push(format!(
            "{} {shown}{:pad$} {}",
            v.style(dim),
            "",
            v.style(dim)
        ));
    }
    out.push(format!(
        "{}{}{}",
        bl.style(dim),
        h.repeat(width.saturating_sub(2)).style(dim),
        br.style(dim)
    ));
    out
}

/// A `label   value` row, right-aligned to the box interior.
fn row(label: &str, value: &str, inner: usize, theme: &Theme) -> String {
    let pad = inner.saturating_sub(label.len() + display_width(value));
    format!(
        "{}{:pad$}{}",
        label.style(theme.styles.dim),
        "",
        value.style(theme.styles.count)
    )
}

fn dim(text: &str, theme: &Theme) -> String {
    text.style(theme.styles.dim).to_string()
}

/// Printable width, ignoring ANSI escapes — the styled string is longer than
/// what the terminal shows, and padding against its byte length would leave
/// every coloured box ragged.
fn display_width(s: &str) -> usize {
    console::measure_text_width(s)
}

/// Cut `s` to `width` printable columns, preserving escapes.
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
            blocks_per_sec: Some(412.0),
            eta: Some(Duration::from_secs(4_320)),
            work_rate: Some(23_600.0),
            pool_rates: vec![
                ("transparent", None),
                ("sprout", None),
                ("sapling", Some(19_400.0)),
                ("orchard", Some(4_200.0)),
                ("ironwood", Some(0.0)),
            ],
            received_at: Duration::from_secs(0),
        }
    }

    fn timeline() -> Timeline {
        let mut t = Timeline::new(
            crate::sync::CHANNELS.map(|(name, _)| name),
            Duration::from_secs(5),
        );
        for i in 0..200u64 {
            // A wave with a stall dropped into it, so a visual check exercises
            // both the shape and the band rather than a flat block.
            let phase = (i as f64 / 12.0).sin();
            let sapling = match i {
                90..=96 => 400.0,
                _ => 14_000.0 + 6_000.0 * phase,
            };
            t.push(
                Duration::from_secs(i * 5),
                &[
                    None,
                    None,
                    Some(sapling),
                    Some(4_000.0 + 1_500.0 * phase),
                    Some(0.0),
                ],
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
            probes: Vec::new(),
            violations: 0,
            timeline: Some(timeline()),
        }
    }

    fn lines(width: usize) -> Vec<String> {
        render_sync_status(&state(), &theme(), width)
            .lines()
            .map(str::to_string)
            .collect()
    }

    /// The invariant the whole layout rests on. One over-long line wraps and
    /// shears every box below it, so no rendering may exceed its width — at any
    /// terminal size, including ones narrower than the layout wants.
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

    /// The point of the two-column default: `status` must not take the whole
    /// screen, and both columns have to actually appear side by side.
    #[test]
    fn a_normal_terminal_gets_two_columns() {
        let rendered = lines(79);
        assert!(
            rendered
                .iter()
                .any(|l| l.contains("work") && l.contains("pools")),
            "work and pools must share a line: {rendered:#?}"
        );
        assert!(
            rendered
                .iter()
                .any(|l| l.contains("progress") && l.contains("probes")),
            "progress and probes must share a line"
        );
    }

    /// Narrow terminals stack rather than truncate: half a box is worse than a
    /// taller view.
    #[test]
    fn a_narrow_terminal_stacks_the_columns() {
        let rendered = lines(50);
        assert!(rendered.iter().any(|l| l.contains("work")));
        assert!(rendered.iter().any(|l| l.contains("pools")));
        assert!(
            !rendered
                .iter()
                .any(|l| l.contains("work") && l.contains("pools")),
            "a narrow terminal must not try to fit both: {rendered:#?}"
        );
    }

    /// The distinction carried all the way from the wire to the screen.
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

    /// An empty frame reads as "no work happened". The real cause is that the
    /// driver has published no series, which is a fact about the run's age.
    #[test]
    fn a_run_with_no_series_says_so_rather_than_drawing_an_empty_graph() {
        let mut s = state();
        s.timeline = None;
        let rendered = render_sync_status(&s, &theme(), 79);
        assert!(rendered.contains("no series published yet"), "{rendered}");
    }

    /// Before the first tick there are no vitals at all, and every box has to
    /// cope rather than panicking on an unwrap.
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

    /// Boxes in a column must align, or the layout shears.
    #[test]
    fn every_box_in_a_column_is_the_same_width() {
        let widths: Vec<usize> = boxed("t", "", &["a".into(), "bb".into()], 40, &theme())
            .iter()
            .map(|l| display_width(l))
            .collect();
        assert!(widths.iter().all(|&w| w == 40), "{widths:?}");
    }

    /// A body line longer than the box is truncated rather than allowed to
    /// break the frame.
    #[test]
    fn an_over_long_body_line_is_cut_to_fit() {
        let long = "x".repeat(200);
        for line in boxed("t", "", &[long], 40, &theme()) {
            assert_eq!(display_width(&line), 40);
        }
    }

    /// The header names which sync is being reported on — `status` is run
    /// against several and an unlabelled answer is ambiguous.
    #[test]
    fn the_header_names_the_sync_and_its_profile() {
        let rendered = render_sync_status(&state(), &theme(), 79);
        let first = rendered.lines().next().expect("a header");
        assert!(
            first.contains("sync-7f3a") && first.contains("zaino_ingest"),
            "{first}"
        );
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
