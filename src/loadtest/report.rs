//! L4 — the result. [`LoadReport`] carries each backend's latency percentiles
//! (true p50/p90/p99/p99.9 from an `hdrhistogram`, the actionable upgrade over
//! `zaino-admin`'s min/mean/max), throughput, error and violation counts — and,
//! for a differential run, the heights at which the two backends disagreed.
//!
//! Two gates:
//! - [`assert_correct`](LoadReport::assert_correct) — zero violations, zero
//!   errors, on **every** backend driven. Always safe: a broken chain link or a
//!   failed RPC is a defect, not a timing artifact.
//! - [`assert_parity`](LoadReport::assert_parity) — A ≡ B, byte-identical.
//! - [`assert_relative`](LoadReport::assert_relative) — A/B **ratio**, robust
//!   even on an uncalibrated cluster because both backends share the node.
//!
//! There is deliberately no *absolute* latency gate. It would need a calibrated
//! cluster (static CPU policy, fio-calibrated I/O) to mean anything, and until
//! that exists a number here would be false confidence.

use std::collections::BTreeMap;
use std::time::Duration;

use hdrhistogram::Histogram;

use crate::loadtest::oracle::Violation;

/// The operation a latency sample is attributed to, so a report can point at the
/// slow RPC rather than reporting one blended number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OpKind {
    /// Dialing this connection's channel. Separated from the RPC it precedes
    /// because the accept path degrades independently — a server that stalls on
    /// a spawn burst shows it here and nowhere else. Connect samples never count
    /// toward throughput.
    Connect,
    GetLatestBlock,
    GetBlock,
    GetBlockRange,
}

impl std::fmt::Display for OpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            OpKind::Connect => "Connect",
            OpKind::GetLatestBlock => "GetLatestBlock",
            OpKind::GetBlock => "GetBlock",
            OpKind::GetBlockRange => "GetBlockRange",
        };
        f.write_str(s)
    }
}

/// Latency distribution for one op kind. Percentiles come straight from an
/// `hdrhistogram`, so p99 is a real tail measurement, not `max`.
#[derive(Debug, Clone, Copy)]
pub struct LatencyStats {
    pub p50: Duration,
    pub p90: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub max: Duration,
    pub count: u64,
}

impl LatencyStats {
    /// Summarize a histogram of **microsecond** samples.
    pub(crate) fn from_hist(h: &Histogram<u64>) -> Self {
        let us = |q: f64| Duration::from_micros(h.value_at_quantile(q));
        Self {
            p50: us(0.50),
            p90: us(0.90),
            p99: us(0.99),
            p999: us(0.999),
            max: Duration::from_micros(h.max()),
            count: h.len(),
        }
    }
}

/// One backend's results.
///
/// A differential run fills two of these rather than prefixing a second set of
/// fields onto the report, so a failure always names the backend that produced
/// it — parity alone cannot, since two backends broken identically agree
/// perfectly.
#[derive(Debug, Clone)]
pub struct Side {
    pub by_op: BTreeMap<OpKind, LatencyStats>,
    /// Successful ops per second across the whole run (wall-clock).
    pub throughput: f64,
    pub total_ops: u64,
    pub errors: u64,
    /// Correctness failures the oracle raised while the run was in flight.
    pub violations: Vec<Violation>,
}

/// A height at which the two backends disagreed.
///
/// Byte inequality is the whole gate: `CompactBlock` derives `PartialEq`, so it
/// is exhaustive by construction and cannot miss a field the way a hand-written
/// field-by-field differ drifts as the proto evolves. The height is the
/// fingerprint; reproduce it with a single `GetBlock` against each backend.
#[derive(Debug, Clone)]
pub struct ParityRecord {
    pub height: u64,
    pub detail: &'static str,
}

impl ParityRecord {
    pub(crate) const DIFFERS: &'static str = "blocks differ";
    pub(crate) const MISSING_FROM_B: &'static str = "present in A, missing from B";
    pub(crate) const MISSING_FROM_A: &'static str = "missing from A, present in B";
}

impl std::fmt::Display for ParityRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "height {}: {}", self.height, self.detail)
    }
}

/// The verdict of a load run.
#[derive(Debug, Clone)]
pub struct LoadReport {
    pub label: String,
    pub wall: Duration,
    pub connections: usize,
    /// The endpoint driven by [`LoadDriver::new`](super::LoadDriver::new), or
    /// the first of the pair.
    pub a: Side,
    /// The second endpoint, present only for a differential run
    /// ([`LoadDriver::pair`](super::LoadDriver::pair)).
    pub b: Option<Side>,
    /// Heights where A and B disagreed. Empty for a single-endpoint run — which
    /// is why [`assert_parity`](Self::assert_parity) rejects one outright rather
    /// than reading emptiness as success.
    pub parity_diffs: Vec<ParityRecord>,
}

/// A/B-relative budget: ratios of B against A. Robust on any cluster.
#[derive(Debug, Clone, Copy)]
pub struct Rel {
    /// Max allowed `B.p99 / A.p99` (worst op kind).
    pub p99_ratio_max: f64,
    /// Min allowed `B.throughput / A.throughput`.
    pub throughput_ratio_min: f64,
}

/// The always-on gate: a correctness failure that no amount of load excuses.
#[derive(Debug, thiserror::Error)]
pub enum CorrectnessError {
    #[error("{label}: {count} oracle violation(s); first: {first}")]
    Violations {
        label: String,
        count: usize,
        first: String,
    },
    #[error("{label}: {errors} request error(s) across {attempts} attempt(s)")]
    Errors {
        label: String,
        errors: u64,
        attempts: u64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ParityError {
    #[error("parity failed ({label}): {count} height(s) diverged; first: {first}")]
    Diverged {
        label: String,
        count: usize,
        first: String,
    },
    /// Guards against a vacuous pass: a single-endpoint run has nothing to
    /// compare, so an empty diff list is not evidence of agreement.
    #[error("{label}: assert_parity needs a differential run (LoadDriver::pair)")]
    NotDifferential { label: String },
}

#[derive(Debug, thiserror::Error)]
pub enum RelError {
    #[error("relative budget breached ({label}): {reasons:?}")]
    Breached { label: String, reasons: Vec<String> },
    #[error("{label}: assert_relative needs a differential run (LoadDriver::pair)")]
    NotDifferential { label: String },
}

impl LoadReport {
    /// Each backend paired with a label naming it, so every message is
    /// attributable.
    fn sides(&self) -> Vec<(String, &Side)> {
        let mut sides = vec![(self.label.clone(), &self.a)];
        if let Some(b) = &self.b {
            sides.push((format!("{} [backend B]", self.label), b));
        }
        sides
    }

    /// Human-readable summary. Printed unconditionally; the engine's reporter
    /// captures it and surfaces it on failure.
    pub fn print(&self) {
        eprintln!("── load report: {} ──", self.label);
        for (name, side) in self.sides() {
            eprintln!(
                "  {name}: {} conns · {} ops · {} errors · {:.0} ops/s · {:.1}s wall",
                self.connections,
                side.total_ops,
                side.errors,
                side.throughput,
                self.wall.as_secs_f64(),
            );
            for (op, s) in &side.by_op {
                eprintln!(
                    "    {op:<16} n={:<8} p50={:>8.2}ms p90={:>8.2}ms p99={:>8.2}ms \
                     p99.9={:>8.2}ms max={:>8.2}ms",
                    s.count,
                    ms(s.p50),
                    ms(s.p90),
                    ms(s.p99),
                    ms(s.p999),
                    ms(s.max),
                );
            }
            if !side.violations.is_empty() {
                eprintln!("    ⚠ {} oracle violation(s):", side.violations.len());
                for v in side.violations.iter().take(10) {
                    eprintln!("        {v}");
                }
            }
        }
        if let Some(b) = &self.b {
            eprintln!("  B/A ratios:");
            for (op, a) in &self.a.by_op {
                if let Some(bs) = b.by_op.get(op) {
                    eprintln!("    {op:<16} p99 ×{:.2}", ratio(bs.p99, a.p99));
                }
            }
            eprintln!(
                "    {:<16} ×{:.2}",
                "throughput",
                if self.a.throughput > 0.0 {
                    b.throughput / self.a.throughput
                } else {
                    0.0
                },
            );
        }
        if !self.parity_diffs.is_empty() {
            eprintln!("  ⚠ {} height(s) diverged:", self.parity_diffs.len());
            for p in self.parity_diffs.iter().take(10) {
                eprintln!("      {p}");
            }
        }
    }

    /// The correctness gate: zero oracle violations and zero request errors, on
    /// every backend driven. A differential run that checked only A would miss a
    /// defect B alone exhibits.
    pub fn assert_correct(&self) -> Result<(), CorrectnessError> {
        // Violations first across both sides, then errors: a chain-link failure
        // is a more specific diagnosis than "the RPC failed".
        for (name, side) in self.sides() {
            if let Some(first) = side.violations.first() {
                return Err(CorrectnessError::Violations {
                    label: name,
                    count: side.violations.len(),
                    first: first.to_string(),
                });
            }
        }
        for (name, side) in self.sides() {
            if side.errors > 0 {
                return Err(CorrectnessError::Errors {
                    label: name,
                    errors: side.errors,
                    attempts: side.total_ops + side.errors,
                });
            }
        }
        Ok(())
    }

    /// Gate on A ≡ B — no height at which the two backends disagreed.
    pub fn assert_parity(&self) -> Result<(), ParityError> {
        if self.b.is_none() {
            return Err(ParityError::NotDifferential {
                label: self.label.clone(),
            });
        }
        match self.parity_diffs.first() {
            None => Ok(()),
            Some(first) => Err(ParityError::Diverged {
                label: self.label.clone(),
                count: self.parity_diffs.len(),
                first: first.to_string(),
            }),
        }
    }

    /// Gate on the A/B ratio budget.
    pub fn assert_relative(&self, rel: Rel) -> Result<(), RelError> {
        let Some(b) = &self.b else {
            return Err(RelError::NotDifferential {
                label: self.label.clone(),
            });
        };
        let mut reasons = Vec::new();
        for (op, a) in &self.a.by_op {
            if let Some(bs) = b.by_op.get(op) {
                let r = ratio(bs.p99, a.p99);
                if r > rel.p99_ratio_max {
                    reasons.push(format!("{op} p99 ratio {r:.2} > {:.2}", rel.p99_ratio_max));
                }
            }
        }
        if self.a.throughput > 0.0 {
            let r = b.throughput / self.a.throughput;
            if r < rel.throughput_ratio_min {
                reasons.push(format!(
                    "throughput ratio {r:.2} < {:.2}",
                    rel.throughput_ratio_min
                ));
            }
        }
        if reasons.is_empty() {
            Ok(())
        } else {
            Err(RelError::Breached {
                label: self.label.clone(),
                reasons,
            })
        }
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// `b / a`, or `0.0` when `a` is zero (no samples — nothing to compare).
fn ratio(b: Duration, a: Duration) -> f64 {
    let a = a.as_secs_f64();
    if a > 0.0 { b.as_secs_f64() / a } else { 0.0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn side() -> Side {
        Side {
            by_op: BTreeMap::new(),
            throughput: 100.0,
            total_ops: 10,
            errors: 0,
            violations: Vec::new(),
        }
    }

    fn report(a: Side, b: Option<Side>) -> LoadReport {
        LoadReport {
            label: "t".into(),
            wall: Duration::from_secs(1),
            connections: 1,
            a,
            b,
            parity_diffs: Vec::new(),
        }
    }

    /// The regression this shape exists to prevent: B's errors used to be summed
    /// into A's count, so a failure could not name the backend that produced it.
    #[test]
    fn errors_are_attributed_to_the_backend_that_produced_them() {
        let mut b = side();
        b.errors = 3;
        let err = report(side(), Some(b)).assert_correct().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("[backend B]"), "{msg}");
        assert!(msg.contains("3 request error"), "{msg}");
    }

    #[test]
    fn violations_are_attributed_to_the_backend_that_produced_them() {
        let mut b = side();
        b.violations.push(Violation {
            height: 7,
            field: "hash".into(),
            detail: "boom".into(),
        });
        let err = report(side(), Some(b)).assert_correct().unwrap_err();
        assert!(err.to_string().contains("[backend B]"));
    }

    /// A single-endpoint run has an empty diff list; reading that as agreement
    /// would let a mis-authored test pass while comparing nothing.
    #[test]
    fn parity_and_relative_reject_a_single_endpoint_run() {
        let r = report(side(), None);
        assert!(matches!(
            r.assert_parity(),
            Err(ParityError::NotDifferential { .. })
        ));
        assert!(matches!(
            r.assert_relative(Rel {
                p99_ratio_max: 5.0,
                throughput_ratio_min: 0.1,
            }),
            Err(RelError::NotDifferential { .. })
        ));
    }

    #[test]
    fn a_differential_run_with_no_diffs_passes_parity() {
        assert!(report(side(), Some(side())).assert_parity().is_ok());
    }
}
