//! Panel geometry and the primitives every surface shares: the fixed line
//! budget, the label column, the rules, the spinner, and the resource
//! formatters.
//!
//! These live apart from the surfaces that use them because the *constants* are
//! a contract between surfaces, not an implementation detail of any one. The
//! pinned console composes a left and a right column side by side; if they
//! disagreed about [`PANEL_LINES`] the frame would tear. Keeping the number in
//! one place is what makes "constant height" checkable rather than a convention
//! four modules each re-assert.

use std::fmt::Write as _;

use owo_colors::OwoColorize;

use super::theme::Theme;
use crate::qos::{GIB, Resources};

/// Width of the action-label column, matching nextest's `{:>12}`.
pub(super) const LABEL_WIDTH: usize = 12;

/// Label column for the right-hand metrics block. Narrower than [`LABEL_WIDTH`]
/// because the right column is the one starved for width (it gets whatever the
/// terminal has past the left column's fixed 80).
pub(super) const METRIC_LABEL_WIDTH: usize = 7;

/// The pinned panel's fixed line count (must equal `cli::console::PANEL_ROWS`).
/// Every left/right block formatter returns exactly this many lines so the
/// two-column panel is a constant, non-reflowing block for the whole session.
pub(super) const PANEL_LINES: usize = 5;

/// The most transfer rows the right column can show at once; a longer list
/// collapses its tail into a `+N more` line. `PANEL_LINES - 1` because the right
/// column's top row is left blank to align with the left column's branded rule.
pub(super) const MAX_TRANSFER_ROWS: usize = PANEL_LINES - 1;

/// Character width of a per-pool sparkline in the panel's work column.
///
/// Fixed rather than derived: a panel block is rendered without knowing the
/// width it will be clipped to (the console splits columns at present time), and
/// a sparkline whose length tracked the terminal would make two runs
/// un-comparable at a glance. Twelve characters is 24 braille sub-columns.
pub(super) const SPARK_WIDTH: usize = 12;

/// Force `out` to exactly [`PANEL_LINES`] lines: pad short blocks with blank
/// lines, truncate an over-long one, so the fixed-height panel viewport doesn't
pub(super) fn pad_to_panel(out: &mut String) {
    let n = out.lines().count();
    match n.cmp(&PANEL_LINES) {
        std::cmp::Ordering::Less => {
            for _ in n..PANEL_LINES {
                out.push('\n');
            }
        }
        std::cmp::Ordering::Greater => {
            let kept: String = out.lines().take(PANEL_LINES).collect::<Vec<_>>().join("\n");
            *out = kept;
        }
        std::cmp::Ordering::Equal => {}
    }
}

/// Render one frame of the banner to a `String`. The result ends in `\n`; ANSI
/// escape codes are present iff `theme.is_colorized()`. Produces one frame, so
/// a caller doing in-place refresh must emit its own cursor-up sequences.
pub(super) fn blank_line(out: &mut String) {
    out.push('\n');
}

/// label (e.g. the cluster block's node line).
pub(super) const INDENT: &str = "             "; // 12 spaces + 1 separator = label column width + 1

pub(super) fn render_top_rule(out: &mut String, theme: &Theme) {
    writeln!(out, "{}", theme.chars.hbar(LABEL_WIDTH)).expect("write to string");
}

pub(super) fn render_bottom_rule(out: &mut String, theme: &Theme) {
    writeln!(out, "{}", theme.chars.hbar(LABEL_WIDTH)).expect("write to string");
}

/// The branded divider between scrolled output above and a pinned status panel
/// below: `───── Ztest ─────`. Reuses `hbar` so it follows the theme's glyph
/// (`-----` under the ASCII fallback).
pub(super) fn render_label_rule(out: &mut String, theme: &Theme) {
    let side = theme.chars.hbar(5);
    writeln!(
        out,
        "{side} {} {side}",
        "Ztest".style(theme.styles.script_id)
    )
    .expect("write to string");
}

pub(super) fn cores_of(r: &Resources) -> u64 {
    r.cpu_milli / 1000
}

/// Whole GiB in a [`Resources`].
pub(super) fn gib_of(r: &Resources) -> u64 {
    r.mem_bytes / GIB
}

/// Percent of capacity free, the tighter (min) of the CPU and memory free
/// fractions: the binding constraint for packing more work. Zero allocatable
/// (e.g. before the probe lands) gives 0%.
pub(super) fn free_percent(free: &Resources, alloc: &Resources) -> u8 {
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
pub(super) const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Milliseconds per spinner frame. The single source of truth for the animation
/// cadence: the console's render thread gates its redraw on this same step (so it
/// repaints exactly when the frame advances) — see `cli::console`'s `FrameClock`.
pub(crate) const SPINNER_STEP_MS: u128 = 100;

/// Pick a spinner frame from `elapsed`. One frame per [`SPINNER_STEP_MS`], so a
/// 200ms redraw cadence advances ~2 frames per tick.
pub(super) fn spinner_glyph(elapsed: std::time::Duration) -> &'static str {
    let idx = (elapsed.as_millis() / SPINNER_STEP_MS) as usize % SPINNER_FRAMES.len();
    SPINNER_FRAMES[idx]
}

pub(super) fn agg_str(r: &Resources) -> String {
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

/// The QoS scheduling plan (`docs/design-qos.md` §8 planning pass): per-tier
/// selected counts and footprints, the wave/peak estimate against probed
/// capacity, and any unschedulable-tier warnings. The live during-run
/// reservation view is a deferred follow-up, noted as the final dim line.
pub(super) fn used_percent(part: &Resources, whole: &Resources) -> u8 {
    let frac = |p: u64, w: u64| -> u64 {
        if w == 0 {
            0
        } else {
            ((p as u128 * 100) / w as u128).min(100) as u64
        }
    };
    frac(part.cpu_milli, whole.cpu_milli).max(frac(part.mem_bytes, whole.mem_bytes)) as u8
}
