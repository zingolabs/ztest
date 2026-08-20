//! `footprint = "<cpu>/<mem>[/<disk>]"` — per-test QoS reserve override.
//!
//! - One parser, three readers: proc-macro (expands), CLI source scan, `qos::units`
//! - Lives here (proc-macro crate unlinkable from CLI, both must agree byte-for-byte)
//! - CPU × memory schedulable; disk declared-but-inert (bytes, io_bps, io_iops)

/// Component reserve replacing a tier's own, `footprint = ".."`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footprint {
    pub cpu_milli: u64,
    pub mem_bytes: u64,
    pub disk_bytes: Option<u64>,
    pub disk_bps: Option<u64>,
    pub disk_iops: Option<u64>,
}

/// `"15c/29Gi"` / `"15c/29Gi/400Gi"` → [`Footprint`]
///
/// - Units mandatory every segment (ledger holds this for hours; misread = silent
///   under-reserve)
/// - Whole cores only (`guaranteed_cpu_mem` rounds up → pod could top its own reserve)
pub fn parse(s: &str) -> Result<Footprint, String> {
    let mut parts = s.split('/');
    let (Some(cpu), Some(mem)) = (parts.next(), parts.next()) else {
        return Err(format!(
            "footprint `{s}` must be `<cpu>/<mem>` or `<cpu>/<mem>/<disk>`, e.g. \"15c/29Gi\""
        ));
    };
    let disk = parts.next();
    if parts.next().is_some() {
        return Err(format!(
            "footprint `{s}` has more than three segments; grammar is `<cpu>/<mem>[/<disk>]`"
        ));
    }
    Ok(Footprint {
        cpu_milli: parse_cpu(cpu.trim())?,
        mem_bytes: parse_mem(mem.trim())?,
        disk_bytes: disk.map(|d| parse_disk(d.trim())).transpose()?,
        // No grammar segment yet; declared here so one type carries every disk dimension
        disk_bps: None,
        disk_iops: None,
    })
}

/// k8s storage quantity → bytes. Same grammar as memory, own diagnostic + zero rule
///
/// - Zero rejected: `"…/0Gi"` reads as "declared none", which `None` already says
fn parse_disk(s: &str) -> Result<u64, String> {
    if !s.ends_with(|c: char| c.is_ascii_alphabetic()) {
        return Err(format!(
            "footprint disk `{s}` needs a unit, e.g. \"400Gi\" (a bare number would mean bytes)"
        ));
    }
    let bytes = parse_mem_bytes(s)
        .ok_or_else(|| format!("footprint disk `{s}` is not a k8s quantity, e.g. \"400Gi\""))?;
    if bytes == 0 {
        return Err("footprint disk is zero — omit the segment to declare no reserve".to_string());
    }
    Ok(bytes)
}

/// `"15c"` / `"15000m"` → millicores
fn parse_cpu(s: &str) -> Result<u64, String> {
    let milli = if let Some(n) = s.strip_suffix('c') {
        n.trim()
            .parse::<u64>()
            .map_err(|_| format!("footprint cpu `{s}`: `{}` is not a whole number", n.trim()))?
            .checked_mul(1000)
            .ok_or_else(|| format!("footprint cpu `{s}` overflows"))?
    } else if let Some(n) = s.strip_suffix('m') {
        n.trim()
            .parse::<u64>()
            .map_err(|_| format!("footprint cpu `{s}`: `{}` is not a whole number", n.trim()))?
    } else {
        return Err(format!(
            "footprint cpu `{s}` needs a unit: `15c` (cores) or `15000m` (millicores)"
        ));
    };
    if milli == 0 {
        return Err("footprint cpu is zero — a pod sized from it would be BestEffort".to_string());
    }
    if !milli.is_multiple_of(1000) {
        return Err(format!(
            "footprint cpu `{s}` is {milli}m, not a whole number of cores; pods render \
             whole-core Guaranteed requests, so a fractional reserve is one a deploy can exceed"
        ));
    }
    Ok(milli)
}

/// k8s memory quantity → bytes, apiserver suffix set
fn parse_mem(s: &str) -> Result<u64, String> {
    // Shared parser reads bare number as bytes (right for node allocatable, wrong here:
    // `29` meant `29Gi`, would become a 29-byte reserve)
    if !s.ends_with(|c: char| c.is_ascii_alphabetic()) {
        return Err(format!(
            "footprint memory `{s}` needs a unit, e.g. \"29Gi\" or \"512Mi\" \
             (a bare number would mean bytes)"
        ));
    }
    let bytes = parse_mem_bytes(s).ok_or_else(|| {
        format!("footprint memory `{s}` is not a k8s quantity, e.g. \"29Gi\" or \"512Mi\"")
    })?;
    if bytes == 0 {
        return Err(
            "footprint memory is zero — a pod sized from it would be BestEffort".to_string()
        );
    }
    Ok(bytes)
}

/// k8s memory/byte quantity → bytes (binary, decimal SI, exponent, raw); overflow saturates
///
/// - Shared with `qos::units` (node `allocatable` + pod `requests`) → one accepted syntax
pub fn parse_mem_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    // Two-char binary suffixes first (single-char decimal ones would shadow them)
    let (num, mult) = if let Some(n) = s.strip_suffix("Ki") {
        (n, 1u64 << 10)
    } else if let Some(n) = s.strip_suffix("Mi") {
        (n, 1u64 << 20)
    } else if let Some(n) = s.strip_suffix("Gi") {
        (n, 1u64 << 30)
    } else if let Some(n) = s.strip_suffix("Ti") {
        (n, 1u64 << 40)
    } else if let Some(n) = s.strip_suffix("Pi") {
        (n, 1u64 << 50)
    } else if let Some(n) = s.strip_suffix("Ei") {
        (n, 1u64 << 60)
    } else if let Some(n) = s.strip_suffix('k').or_else(|| s.strip_suffix('K')) {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix('M') {
        (n, 1_000_000)
    } else if let Some(n) = s.strip_suffix('G') {
        (n, 1_000_000_000)
    } else if let Some(n) = s.strip_suffix('T') {
        (n, 1_000_000_000_000)
    } else if let Some(n) = s.strip_suffix('P') {
        (n, 1_000_000_000_000_000)
    } else if let Some(n) = s.strip_suffix('E') {
        (n, 1_000_000_000_000_000_000)
    } else {
        (s, 1)
    };
    let num = num.trim();
    if let Ok(v) = num.parse::<u64>() {
        Some(v.saturating_mul(mult))
    } else if let Ok(f) = num.parse::<f64>() {
        // Fractional/exponent ("1.5Gi", "129e6")
        Some((f.max(0.0) * mult as f64).round() as u64)
    } else {
        None
    }
}

/// k8s CPU quantity → millicores, `"500m"`/`"2"`/`"1.5"`/`"2500000n"` (rounds up)
///
/// - Permissive apiserver form (fractional/nanocore); footprint grammar takes neither
pub fn parse_cpu_milli(s: &str) -> Option<u64> {
    let s = s.trim();
    // Float→int casts saturate (NaN/negative → 0, huge → u64::MAX), never panic
    let scaled = |body: &str, per_milli: f64| -> Option<u64> {
        body.trim().parse::<f64>().ok().map(|v| (v / per_milli).round() as u64)
    };
    if let Some(n) = s.strip_suffix('m') {
        n.trim().parse::<u64>().ok()
    } else if let Some(n) = s.strip_suffix('u') {
        scaled(n, 1_000.0) // microcores → millicores
    } else if let Some(n) = s.strip_suffix('n') {
        scaled(n, 1_000_000.0) // nanocores → millicores
    } else {
        // Bare cores, maybe fractional/exponent ("1.5", "2e0")
        s.parse::<f64>().ok().map(|cores| (cores * 1000.0).round() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    /// Every disk dimension undeclared — the shape `<cpu>/<mem>` yields
    fn cpu_mem(cpu_milli: u64, mem_bytes: u64) -> Footprint {
        Footprint { cpu_milli, mem_bytes, disk_bytes: None, disk_bps: None, disk_iops: None }
    }

    #[test]
    fn parses_the_canonical_form() {
        assert_eq!(parse("15c/29Gi"), Ok(cpu_mem(15_000, 29 * GIB)));
        assert_eq!(parse("15000m/29Gi"), Ok(cpu_mem(15_000, 29 * GIB)));
        assert_eq!(parse(" 4c / 512Mi "), Ok(cpu_mem(4_000, 512 << 20)));
    }

    #[test]
    fn requires_both_halves() {
        assert!(parse("29Gi").is_err());
        assert!(parse("15c").is_err());
    }

    #[test]
    fn parses_the_optional_disk_segment() {
        assert_eq!(
            parse("15c/29Gi/400Gi"),
            Ok(Footprint { disk_bytes: Some(400 * GIB), ..cpu_mem(15_000, 29 * GIB) })
        );
        assert_eq!(parse(" 15c / 29Gi / 400Gi ").map(|f| f.disk_bytes), Ok(Some(400 * GIB)));
    }

    /// Bare number would read as bytes, and a 400-byte reserve passes every check it faces
    #[test]
    fn disk_needs_a_unit_and_may_not_be_zero() {
        assert!(parse("15c/29Gi/400").is_err());
        assert!(parse("15c/29Gi/0Gi").is_err());
        assert!(parse("15c/29Gi/lots").is_err());
    }

    #[test]
    fn a_fourth_segment_is_rejected() {
        assert!(parse("15c/29Gi/400Gi/9").is_err());
    }

    // Rejections stopping a silent misread → bad reserve

    #[test]
    fn rejects_a_unitless_half() {
        assert!(parse("15/29Gi").is_err(), "bare cpu must not be read as millicores");
        assert!(parse("15c/29").is_err(), "bare memory must not be read as 29 bytes");
    }

    #[test]
    fn rejects_zero_in_either_dimension() {
        assert!(parse("0c/29Gi").is_err());
        assert!(parse("15c/0Gi").is_err());
    }

    #[test]
    fn rejects_fractional_cores() {
        // Renders 2 whole cores via `guaranteed_cpu_mem` (> the 1500m reserved)
        assert!(parse("1500m/2Gi").is_err());
    }

    #[test]
    fn memory_accepts_binary_and_decimal_suffixes() {
        assert_eq!(parse_mem_bytes("1Gi"), Some(GIB));
        assert_eq!(parse_mem_bytes("1G"), Some(1_000_000_000));
        assert_eq!(parse_mem_bytes("1.5Gi"), Some(GIB + GIB / 2));
        assert_eq!(parse_mem_bytes("nonsense"), None);
    }

    #[test]
    fn cpu_quantity_parser_accepts_what_a_node_reports() {
        assert_eq!(parse_cpu_milli("500m"), Some(500));
        assert_eq!(parse_cpu_milli("2"), Some(2_000));
        assert_eq!(parse_cpu_milli("1.5"), Some(1_500));
        assert_eq!(parse_cpu_milli("2500000n"), Some(3));
        assert_eq!(parse_cpu_milli("nonsense"), None);
    }
}
