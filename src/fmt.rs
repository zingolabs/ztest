//! Number / duration formatting vocabulary.
//!
//! - Pure: no `Theme`, no colour (presentation lives in `ztest_ui::text`)
//! - Together because mutual consistency *is* the requirement (`1.2k` beside `1.23k`
//!   reads as two numbers with no way to tell it is one)

/// What a reduced value *is*, so a reader renders it and a range query derives it
/// without a per-row special case at either end.
///
/// - `PerSec` = a flow, derived by [`Reading`](crate::metrics::Reading) rather than declared
/// - `Fraction` = seconds-per-second, i.e. dimensionless in 0..1 (PSI stall); rendered
///   `%`, never `/s` — the ratio's denominator is the same clock as its numerator
/// - `BytesPerSec` = throughput; IEC magnitude like `Bytes` (`412.0 MiB/s`), since
///   `compact`'s decimal `412M/s` names a different quantity than the byte axis above it
/// - Rest are already the quantity named; the query is the reduction alone
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Count,
    PerSec,
    Fraction,
    Bytes,
    BytesPerSec,
    Millis,
    Cores,
}

/// Narrow-column value: `0`, `<0.01`, `0.04`, `912`, `1.23k`, `2.40M`, `9.88B`.
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
        // `B` = billion, never bytes: every caller here is a count, rate or core figure
        // (`Unit::Bytes` takes its own arm in `unit_value`)
        n if n >= 1e9 => (v / 1e9, "B"),
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

/// A [`Unit`]'d value in the narrowest form that keeps its meaning.
///
/// - `Millis` crosses to `1m56s` past a second (`116111ms` is a number a reader has to
///   divide before it says anything)
/// - Sub-millisecond keeps a decimal: a `0.4ms` latency shown as `0ms` reports a
///   measurement that did not happen
pub fn unit_value(unit: Unit, v: f64) -> String {
    match unit {
        Unit::Count => compact(v),
        Unit::PerSec => format!("{}/s", compact(v)),
        // Sub-tenth keeps a decimal: a 0.04% stall shown as `0%` reports no measurement
        Unit::Fraction if v > 0.0 && v < 0.001 => format!("{:.2}%", v * 100.0),
        Unit::Fraction => format!("{:.1}%", v * 100.0),
        Unit::Cores => format!("{}c", compact(v)),
        Unit::Bytes => bytesize::ByteSize::b(v.max(0.0) as u64).display().iec().to_string(),
        Unit::BytesPerSec => {
            format!("{}/s", bytesize::ByteSize::b(v.max(0.0) as u64).display().iec())
        }
        Unit::Millis if v < 1.0 => format!("{v:.1}ms"),
        Unit::Millis if v < 1_000.0 => format!("{v:.0}ms"),
        Unit::Millis => format_elapsed(std::time::Duration::from_millis(v as u64)),
    }
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

/// Age `47s` / `2m05s` / `3h07m` — [`format_elapsed`] carried past the hour.
///
/// The third member of this vocabulary, and the reason all three live here: each is
/// pinned to a different contract, so none can be collapsed into another.
///
/// - [`format_elapsed`] mirrors nextest's `NNs`/`NmNNs` exactly and *never* reaches
///   hours; a 3-hour test would read `187m12s`, which is what nextest prints
/// - [`format_span`] drops seconds past a minute and stops at hours, so a declared cap
///   echoes its own spelling (`48h`)
/// - this one keeps seconds *and* rolls to hours, which is what a listing needs: a pod
///   that has been terminating for three hours must not read as `187m`
pub fn format_age(d: std::time::Duration) -> String {
    let s = d.as_secs();
    match s {
        s if s < 60 => format!("{s}s"),
        s if s < 3_600 => format!("{}m{:02}s", s / 60, s % 60),
        s => format!("{}h{:02}m", s / 3_600, (s % 3_600) / 60),
    }
}

/// Human span `45s` / `12m` / `2h30m` / `48h`.
///
/// - Distinct from [`format_elapsed`] on purpose: that one tracks nextest's `NNs`/`NmNNs`
///   and never reaches hours, which a 48h cap needs
/// - Exact hours collapse (`48h`, not `48h00m`)
/// - Hours never roll to days: a hard cap echoes its declaration (`timeout = "48h"`), and
///   `2d` breaks that correspondence
pub fn format_span(d: std::time::Duration) -> String {
    let s = d.as_secs();
    match s {
        s if s < 60 => format!("{s}s"),
        s if s < 3_600 => format!("{}m", s / 60),
        s if s % 3_600 == 0 => format!("{}h", s / 3_600),
        s => format!("{}h{:02}m", s / 3_600, (s % 3_600) / 60),
    }
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
