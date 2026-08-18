//! Themed gauge / axis vocabulary for every [`ui`](super) surface.

use owo_colors::OwoColorize as _;

use super::Theme;
use ztest::api::compact;

pub const METER_WIDTH: usize = 12;

/// Bracketed percentage gauge `[██████░░░░░░]`
pub fn meter(percent: u8, theme: &Theme) -> String {
    let pct = percent.min(100) as usize;
    let filled = pct * METER_WIDTH / 100;
    format!(
        "{}{}{}{}",
        "[".style(theme.styles.dim),
        theme.chars.bar_fill.repeat(filled).style(theme.styles.count),
        theme.chars.bar_empty.repeat(METER_WIDTH - filled).style(theme.styles.dim),
        "]".style(theme.styles.dim),
    )
}

/// Evenly spaced y-axis labels, topmost first, right-aligned to `gutter`
pub fn y_axis(max: f64, rows: usize, gutter: usize) -> Vec<String> {
    (0..rows)
        .map(|r| {
            let frac = match rows {
                0 | 1 => 1.0,
                _ => 1.0 - (r as f64 / (rows - 1) as f64),
            };
            format!("{:>gutter$}", compact(max * frac))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_meter_clamps_overflow() {
        let theme = Theme::for_capabilities(false, true);
        assert_eq!(meter(0, &theme), "[░░░░░░░░░░░░]");
        assert_eq!(meter(100, &theme), "[████████████]");
        assert_eq!(meter(250, &theme), "[████████████]", "clamps at 100%");
    }

    /// Labels descend ceiling → zero, so the gutter reads top-down against the plot
    #[test]
    fn the_y_axis_runs_from_the_ceiling_down_to_zero() {
        let axis = y_axis(100.0, 5, 6);
        assert_eq!(axis.len(), 5);
        assert_eq!(axis[0].trim(), "100");
        assert_eq!(axis[4].trim(), "0");
        assert!(axis.iter().all(|l| l.chars().count() == 6));
    }

    #[test]
    fn a_single_row_axis_is_labelled_with_the_ceiling() {
        assert_eq!(y_axis(42.0, 1, 4)[0].trim(), "42");
        assert!(y_axis(42.0, 0, 4).is_empty());
    }
}
