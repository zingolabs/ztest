//! Axis-label vocabulary. Gauges are the template `{k:N#}` cell (`text::meter` deleted)

use ztest::api::{Unit, unit_value};

/// Evenly spaced y-axis labels in `unit`, topmost first, right-aligned to `gutter`.
/// Sole axis-labelling path (`16.0` vs `16.0 GiB` in one gutter = two claims)
pub fn y_axis(max: f64, rows: usize, unit: Unit, gutter: usize) -> Vec<String> {
    (0..rows)
        .map(|r| {
            let frac = match rows {
                0 | 1 => 1.0,
                _ => 1.0 - (r as f64 / (rows - 1) as f64),
            };
            format!("{:>gutter$}", unit_value(unit, max * frac))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Labels descend ceiling → zero, so the gutter reads top-down against the plot
    #[test]
    fn the_y_axis_runs_from_the_ceiling_down_to_zero() {
        let axis = y_axis(100.0, 5, Unit::Count, 6);
        assert_eq!(axis.len(), 5);
        assert_eq!(axis[0].trim(), "100");
        assert_eq!(axis[4].trim(), "0");
        assert!(axis.iter().all(|l| l.chars().count() == 6));
    }

    #[test]
    fn a_single_row_axis_is_labelled_with_the_ceiling() {
        assert_eq!(y_axis(42.0, 1, Unit::Count, 4)[0].trim(), "42");
        assert!(y_axis(42.0, 0, Unit::Count, 4).is_empty());
    }

    /// Unit-blind labelling was the duplicate this parameter deleted
    #[test]
    fn a_byte_axis_is_labelled_in_bytes() {
        assert_eq!(y_axis(16.0 * 1024.0 * 1024.0 * 1024.0, 2, Unit::Bytes, 0)[0], "16.0 GiB");
    }
}
