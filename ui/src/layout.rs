//! Panel geometry + shared surface primitives: line budget, label column, rules,
//! spinner, resource formatters.
//!
//! Constants live here as a contract *between* surfaces: the pinned console
//! composes two columns side by side, and disagreement on [`PANEL_LINES`] tears
//! the frame

use std::fmt::Write as _;

use owo_colors::OwoColorize;

use super::theme::Theme;
use ztest::api::{GIB, Resources};

/// Action-label column width, matching nextest's `{:>12}`
pub(super) const LABEL_WIDTH: usize = 12;

/// Right-hand metrics label column. Under [`LABEL_WIDTH`] — the right column gets
/// only what the terminal has past the left column's fixed 80
pub(super) const METRIC_LABEL_WIDTH: usize = 7;

/// Pinned panel's fixed line count; must equal `cli::console::PANEL_ROWS`. Every
/// block formatter returns exactly this many (session-constant, non-reflowing)
pub(super) const PANEL_LINES: usize = 5;

/// Max transfer rows at once; a longer list collapses its tail to `+N more`.
/// `-1` = the right column's blank top row aligning with the left's branded rule
pub(super) const MAX_TRANSFER_ROWS: usize = PANEL_LINES - 1;

/// Per-pool sparkline width. Fixed, not derived (a block renders without knowing
/// its clip width, and a terminal-tracking sparkline breaks run-to-run comparison)
pub(super) const SPARK_WIDTH: usize = 12;

/// Pad/truncate `out` to exactly [`PANEL_LINES`] lines (viewport never reflows)
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

pub(super) fn blank_line(out: &mut String) {
    out.push('\n');
}

pub(super) const INDENT: &str = "             "; // 12 spaces + 1 separator = label column width + 1

pub(super) fn render_top_rule(out: &mut String, theme: &Theme) {
    writeln!(out, "{}", theme.chars.hbar(LABEL_WIDTH)).expect("write to string");
}

pub(super) fn render_bottom_rule(out: &mut String, theme: &Theme) {
    writeln!(out, "{}", theme.chars.hbar(LABEL_WIDTH)).expect("write to string");
}

/// Branded divider (`───── Ztest ─────`) between scrolled output and the pinned
/// panel. Via `hbar`, so it follows the theme's glyph
pub(super) fn render_label_rule(out: &mut String, theme: &Theme) {
    let side = theme.chars.hbar(5);
    writeln!(out, "{side} {} {side}", "Ztest".style(theme.styles.script_id))
        .expect("write to string");
}

/// Printable width, ANSI escapes ignored (padding against byte length leaves every
/// coloured cell ragged)
pub(super) fn display_width(s: &str) -> usize {
    ::console::measure_text_width(s)
}

/// Cut to `width` printable columns, escapes preserved
pub(super) fn truncate(s: &str, width: usize) -> String {
    match display_width(s) <= width {
        true => s.to_string(),
        false => ::console::truncate_str(s, width, "").into_owned(),
    }
}

/// Pad to `width` printable columns; a styled cell's escapes must not count toward it
pub(super) fn pad(s: &str, width: usize) -> String {
    format!("{s}{}", " ".repeat(width.saturating_sub(display_width(s))))
}

pub(super) fn cores_of(r: &Resources) -> u64 {
    r.cpu_milli / 1000
}

pub(super) fn gib_of(r: &Resources) -> u64 {
    r.mem_bytes / GIB
}

/// Percent capacity free = min(cpu, mem) free fraction, the binding constraint
/// for packing work. Zero allocatable → 0%
pub(super) fn free_percent(free: &Resources, alloc: &Resources) -> u8 {
    let (cpu, mem) = free.ratio_pct(alloc);
    cpu.min(mem)
}

/// Braille frames of `indicatif`'s default spinner
pub(super) const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Ms per spinner frame = sole animation cadence; `console`'s `FrameClock`
/// gates redraw on the same step, repainting exactly when the frame advances
pub const SPINNER_STEP_MS: u128 = 100;

pub(super) fn spinner_glyph(elapsed: std::time::Duration) -> &'static str {
    let idx = (elapsed.as_millis() / SPINNER_STEP_MS) as usize % SPINNER_FRAMES.len();
    SPINNER_FRAMES[idx]
}

/// Binding-dimension fullness = max(cpu, mem) fraction of `part` in `whole`.
/// Zero `whole` → 0%
pub(super) fn used_percent(part: &Resources, whole: &Resources) -> u8 {
    let (cpu, mem) = part.ratio_pct(whole);
    cpu.max(mem)
}
