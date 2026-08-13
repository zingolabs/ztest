//! Panel geometry + shared surface primitives: line budget, label column, rules,
//! spinner, resource formatters.
//!
//! Constants live here as a contract *between* surfaces: the pinned console
//! composes two columns side by side, and disagreement on [`PANEL_LINES`] tears
//! the frame

use std::fmt::Write as _;

use owo_colors::OwoColorize;

use super::theme::Theme;
use crate::qos::{GIB, Resources};

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

pub(super) fn cores_of(r: &Resources) -> u64 {
    r.cpu_milli / 1000
}

pub(super) fn gib_of(r: &Resources) -> u64 {
    r.mem_bytes / GIB
}

/// Percent capacity free = min(cpu, mem) free fraction, the binding constraint
/// for packing work. Zero allocatable → 0%
pub(super) fn free_percent(free: &Resources, alloc: &Resources) -> u8 {
    let frac = |f: u64, a: u64| -> u64 {
        if a == 0 { 0 } else { ((f as u128 * 100) / a as u128).min(100) as u64 }
    };
    frac(free.cpu_milli, alloc.cpu_milli).min(frac(free.mem_bytes, alloc.mem_bytes)) as u8
}

/// Braille frames of `indicatif`'s default spinner
pub(super) const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Ms per spinner frame = sole animation cadence; `cli::console`'s `FrameClock`
/// gates redraw on the same step, repainting exactly when the frame advances
pub(crate) const SPINNER_STEP_MS: u128 = 100;

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

/// Binding-dimension fullness = max(cpu, mem) fraction of `part` in `whole`.
/// Zero `whole` → 0%
pub(super) fn used_percent(part: &Resources, whole: &Resources) -> u8 {
    let frac = |p: u64, w: u64| -> u64 {
        if w == 0 { 0 } else { ((p as u128 * 100) / w as u128).min(100) as u64 }
    };
    frac(part.cpu_milli, whole.cpu_milli).max(frac(part.mem_bytes, whole.mem_bytes)) as u8
}
