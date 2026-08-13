//! Shared number / duration / gauge vocabulary for every [`ui`](super) surface.
//!
//! Together because mutual consistency *is* the requirement: `1.2k` in a metrics row
//! beside `1.23k` on an axis reads as two numbers with no way to tell it is one

use owo_colors::OwoColorize as _;

use super::Theme;

/// Narrow-column value: `0`, `<0.01`, `0.04`, `912`, `1.23k`, `2.40M`.
///
/// - ≤ 3 significant figures → a label never outgrows its gutter as the scale changes
/// - Below one, decimals grow; under `0.01` becomes a bound (a fractional rate shown as
///   `0` reports a stall that is not happening)
pub fn compact(v: f64) -> String {
    if v == 0.0 {
        return "0".to_string();
    }
    if v.abs() < 0.01 {
        let sign = match v < 0.0 {
            true => "-",
            false => "",
        };
        return format!("{sign}<0.01");
    }
    let (scaled, suffix) = match v.abs() {
        n if n >= 1e9 => (v / 1e9, "G"),
        n if n >= 1e6 => (v / 1e6, "M"),
        n if n >= 1e3 => (v / 1e3, "k"),
        _ => (v, ""),
    };
    let decimals = match (suffix.is_empty(), scaled.abs()) {
        (true, n) if n < 0.1 => 2,
        (true, n) if n < 1.0 => 1,
        (true, _) => 0,
        (false, n) if n >= 100.0 => 0,
        (false, n) if n >= 10.0 => 1,
        (false, _) => 2,
    };
    format!("{scaled:.decimals$}{suffix}")
}

/// Thousands-separated count `3,120,455` — [`compact`]'s counterpart for a figure read
/// as identity, not magnitude (a block height, where `1.02M` loses what the reader came for)
pub fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Elapsed as `12s` / `1m23s`, matching nextest's `[NNs]` / `[Nm NNs]` vocabulary
pub fn format_elapsed(d: std::time::Duration) -> String {
    let total = d.as_secs();
    match total < 60 {
        true => format!("{total}s"),
        false => format!("{}m{:02}s", total / 60, total % 60),
    }
}

/// Transfer rate `18.1 MiB/s`, IEC to match the byte counts beside it
pub fn byte_rate(bytes_per_sec: f64) -> String {
    let clamped = bytes_per_sec.max(0.0) as u64;
    format!("{}/s", bytesize::ByteSize::b(clamped).display().iec())
}

/// `done / total` as one figure when both land in the same magnitude — `2.6/3.9 GiB`,
/// else `986 MiB / 3.9 GiB`. Shared unit is the common case, and the ~6 columns it
/// frees are what the right column has to spend
pub fn byte_pair(done: u64, total: u64) -> String {
    let iec = |b: u64| bytesize::ByteSize::b(b).display().iec().to_string();
    let (d, t) = (iec(done), iec(total));
    match (d.rsplit_once(' '), t.rsplit_once(' ')) {
        (Some((dn, du)), Some((tn, tu))) if du == tu => format!("{dn}/{tn} {tu}"),
        _ => format!("{d} / {t}"),
    }
}

/// Longest name, clamped → one very long name cannot push detail off the right edge
pub fn column_width<'a>(names: impl IntoIterator<Item = &'a str>, min: usize, max: usize) -> usize {
    names.into_iter().map(|n| n.len()).max().unwrap_or(0).clamp(min, max)
}

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
    fn compact_labels_stay_short_and_carry_a_unit() {
        assert_eq!(compact(0.0), "0");
        assert_eq!(compact(912.0), "912");
        assert_eq!(compact(1_234.0), "1.23k");
        assert_eq!(compact(18_204.0), "18.2k");
        assert_eq!(compact(138_200.0), "138k");
        assert_eq!(compact(2_400_000.0), "2.40M");
        assert_eq!(compact(987_654_321.0), "988M");
        assert!(compact(9_876_543_210.0).ends_with('G'));
        assert!(compact(1_234.0).len() <= 6);
    }

    /// A sub-1 rate is still a rate: `0` (and `0.00`) reports a stall that is not
    /// happening → the small end is a bound, not a rounding
    #[test]
    fn a_sub_unit_value_never_rounds_down_to_zero() {
        assert_eq!(compact(0.4), "0.4");
        assert_eq!(compact(0.04), "0.04");
        assert_eq!(compact(0.004), "<0.01");
        assert_eq!(compact(0.0), "0", "a real zero stays bare");
        assert_eq!(compact(-0.004), "-<0.01");
        for v in [0.4, 0.04, 0.004, 0.000_04] {
            assert_ne!(compact(v), "0", "{v} rendered as a stall");
            assert!(compact(v).len() <= 6, "{v} outgrew the gutter");
        }
    }

    #[test]
    fn a_byte_rate_carries_its_unit_per_second() {
        assert_eq!(byte_rate(94.0 * 1024.0 * 1024.0), "94.0 MiB/s");
        assert_eq!(byte_rate(0.0), "0 B/s");
        assert_eq!(byte_rate(-5.0), "0 B/s", "a negative rate floors, never prints");
    }

    /// Shared magnitude collapses to one unit; a crossed boundary keeps both, so the
    /// pair never reads as `986/3.9 GiB`
    #[test]
    fn a_byte_pair_shares_a_unit_only_when_both_ends_do() {
        const GIB: u64 = 1024 * 1024 * 1024;
        assert_eq!(byte_pair(2 * GIB + GIB / 2, 4 * GIB), "2.5/4.0 GiB");
        assert_eq!(byte_pair(986 * 1024 * 1024, 4 * GIB), "986.0 MiB / 4.0 GiB");
        assert_eq!(byte_pair(0, 0), "0/0 B");
    }

    #[test]
    fn thousands_groups_digits() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(901), "901");
        assert_eq!(thousands(1_024), "1,024");
        assert_eq!(thousands(3_120_455), "3,120,455");
    }

    #[test]
    fn elapsed_switches_to_minutes_past_a_minute() {
        use std::time::Duration;
        assert_eq!(format_elapsed(Duration::from_secs(12)), "12s");
        assert_eq!(format_elapsed(Duration::from_secs(59)), "59s");
        assert_eq!(format_elapsed(Duration::from_secs(83)), "1m23s");
        assert_eq!(format_elapsed(Duration::from_secs(3_600)), "60m00s");
    }

    #[test]
    fn the_meter_clamps_overflow() {
        let theme = Theme::for_capabilities(false, true);
        assert_eq!(meter(0, &theme), "[░░░░░░░░░░░░]");
        assert_eq!(meter(100, &theme), "[████████████]");
        assert_eq!(meter(250, &theme), "[████████████]", "clamps at 100%");
    }

    #[test]
    fn column_width_clamps_to_its_bounds() {
        assert_eq!(column_width(["ab", "abcd"], 1, 10), 4);
        assert_eq!(column_width(["ab", "abcd"], 6, 10), 6, "floors at min");
        assert_eq!(column_width(["a".repeat(40).as_str()], 1, 10), 10);
        assert_eq!(column_width([], 3, 10), 3);
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
