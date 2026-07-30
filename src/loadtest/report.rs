//! L4 — the result. [`LoadReport`] carries per-op latency percentiles (true
//! p50/p90/p99/p99.9 from an `hdrhistogram`, the actionable upgrade over
//! `zaino-admin`'s min/mean/max), throughput, error and oracle-violation counts,
//! and — for a differential run — the second backend's numbers plus the exact
//! field-level parity diffs.
//!
//! Three gates, matching the design doc's measurement model:
//! - [`assert_slo`](LoadReport::assert_slo) — **absolute** p99 / throughput.
//!   Trust only on a calibrated cluster (static CPU policy + fio-calibrated I/O).
//! - [`assert_parity`](LoadReport::assert_parity) — A ≡ B, field-identical.
//! - [`assert_relative`](LoadReport::assert_relative) — A/B **ratio**, robust
//!   even on an uncalibrated cluster because both backends share the node.

use std::collections::BTreeMap;
use std::time::Duration;

use hdrhistogram::Histogram;

use crate::loadtest::oracle::{FieldDiff, Violation};

/// The operation a latency sample is attributed to, so a report can point at the
/// slow RPC rather than reporting one blended number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OpKind {
    GetLatestBlock,
    GetBlock,
    GetBlockRange,
}

impl std::fmt::Display for OpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
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

/// A field-level divergence between backend A and B at a given height.
#[derive(Debug, Clone)]
pub struct ParityRecord {
    pub height: u64,
    pub diff: FieldDiff,
}

impl std::fmt::Display for ParityRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "height {} [{}]: A={} B={}",
            self.height, self.diff.field, self.diff.value_a, self.diff.value_b
        )
    }
}

/// The verdict of a load run. Single-backend runs leave the `b_*` fields empty;
/// [`DiffLoadDriver`](super::DiffLoadDriver) populates them.
#[derive(Debug, Clone)]
pub struct LoadReport {
    pub label: String,
    pub by_op: BTreeMap<OpKind, LatencyStats>,
    /// Successful ops per second across the whole run (wall-clock).
    pub throughput: f64,
    pub total_ops: u64,
    pub errors: u64,
    pub wall: Duration,
    pub connections: usize,
    /// Correctness failures the oracle raised while the run was in flight.
    pub violations: Vec<Violation>,
    /// Backend B's latency, present only for a differential run.
    pub b_by_op: BTreeMap<OpKind, LatencyStats>,
    pub b_throughput: f64,
    /// Field-level A-vs-B divergences, present only for a differential run.
    pub parity_diffs: Vec<ParityRecord>,
}

/// Absolute service-level objective. Trustworthy only on a calibrated cluster.
#[derive(Debug, Clone, Copy)]
pub struct Slo {
    pub max_p99: Duration,
    pub min_throughput: f64,
    pub max_error_rate: f64,
    pub zero_violations: bool,
}

/// A/B-relative budget: ratios of B against A. Robust on any cluster.
#[derive(Debug, Clone, Copy)]
pub struct Rel {
    /// Max allowed `B.p99 / A.p99` (worst op kind).
    pub p99_ratio_max: f64,
    /// Min allowed `B.throughput / A.throughput`.
    pub throughput_ratio_min: f64,
}

/// The always-on gate: a correctness failure that no amount of load should
/// excuse. Kept separate from [`SloError`] so perf and correctness gate
/// independently.
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
#[error("SLO breached ({label}): {reasons:?}")]
pub struct SloError {
    pub label: String,
    pub reasons: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("parity failed ({label}): {} field di(s) across {} height(s); first: {first}", count, heights)]
pub struct ParityError {
    pub label: String,
    pub count: usize,
    pub heights: usize,
    pub first: String,
}

#[derive(Debug, thiserror::Error)]
#[error("relative budget breached ({label}): {reasons:?}")]
pub struct RelError {
    pub label: String,
    pub reasons: Vec<String>,
}

impl LoadReport {
    fn error_rate(&self) -> f64 {
        let attempts = self.total_ops + self.errors;
        if attempts == 0 {
            0.0
        } else {
            self.errors as f64 / attempts as f64
        }
    }

    fn worst_p99(stats: &BTreeMap<OpKind, LatencyStats>) -> Option<(OpKind, Duration)> {
        stats
            .iter()
            .map(|(k, s)| (*k, s.p99))
            .max_by_key(|(_, d)| *d)
    }

    /// Human-readable summary. Printed unconditionally; the engine's reporter
    /// captures it and surfaces it on failure.
    pub fn print(&self) {
        eprintln!("── load report: {} ──", self.label);
        eprintln!(
            "  {} conns · {} ops · {} errors · {:.0} ops/s · {:.1}s wall",
            self.connections,
            self.total_ops,
            self.errors,
            self.throughput,
            self.wall.as_secs_f64(),
        );
        for (op, s) in &self.by_op {
            eprint!(
                "  {op:<16} n={:<8} p50={:>8.2}ms p90={:>8.2}ms p99={:>8.2}ms p99.9={:>8.2}ms max={:>8.2}ms",
                s.count,
                ms(s.p50),
                ms(s.p90),
                ms(s.p99),
                ms(s.p999),
                ms(s.max),
            );
            if let Some(b) = self.b_by_op.get(op) {
                eprint!("  | B p99={:>8.2}ms", ms(b.p99));
            }
            eprintln!();
        }
        if !self.violations.is_empty() {
            eprintln!("  ⚠ {} oracle violation(s):", self.violations.len());
            for v in self.violations.iter().take(10) {
                eprintln!("      {v}");
            }
        }
        if !self.parity_diffs.is_empty() {
            eprintln!("  ⚠ {} parity diff(s):", self.parity_diffs.len());
            for p in self.parity_diffs.iter().take(10) {
                eprintln!("      {p}");
            }
        }
    }

    /// The correctness gate: zero oracle violations and zero request errors.
    /// This holds regardless of cluster calibration — a broken chain link or a
    /// failed RPC is a defect, not a timing artifact — so it is always safe to
    /// gate on, unlike [`assert_slo`](Self::assert_slo).
    pub fn assert_correct(&self) -> Result<(), CorrectnessError> {
        if let Some(first) = self.violations.first() {
            return Err(CorrectnessError::Violations {
                label: self.label.clone(),
                count: self.violations.len(),
                first: first.to_string(),
            });
        }
        if self.errors > 0 {
            return Err(CorrectnessError::Errors {
                label: self.label.clone(),
                errors: self.errors,
                attempts: self.total_ops + self.errors,
            });
        }
        Ok(())
    }

    /// Gate on an absolute SLO. See the caveat on cluster calibration.
    pub fn assert_slo(&self, slo: Slo) -> Result<(), SloError> {
        let mut reasons = Vec::new();
        if let Some((op, p99)) = Self::worst_p99(&self.by_op) {
            if p99 > slo.max_p99 {
                reasons.push(format!(
                    "{op} p99 {:.2}ms > {:.2}ms",
                    ms(p99),
                    ms(slo.max_p99)
                ));
            }
        }
        if self.throughput < slo.min_throughput {
            reasons.push(format!(
                "throughput {:.0} < {:.0} ops/s",
                self.throughput, slo.min_throughput
            ));
        }
        let rate = self.error_rate();
        if rate > slo.max_error_rate {
            reasons.push(format!(
                "error rate {:.4} > {:.4}",
                rate, slo.max_error_rate
            ));
        }
        if slo.zero_violations && !self.violations.is_empty() {
            reasons.push(format!("{} oracle violation(s)", self.violations.len()));
        }
        if reasons.is_empty() {
            Ok(())
        } else {
            Err(SloError {
                label: self.label.clone(),
                reasons,
            })
        }
    }

    /// Gate on A ≡ B — no field-level divergence between the two backends.
    pub fn assert_parity(&self) -> Result<(), ParityError> {
        if self.parity_diffs.is_empty() {
            return Ok(());
        }
        let mut heights: Vec<u64> = self.parity_diffs.iter().map(|p| p.height).collect();
        heights.sort_unstable();
        heights.dedup();
        Err(ParityError {
            label: self.label.clone(),
            count: self.parity_diffs.len(),
            heights: heights.len(),
            first: self.parity_diffs[0].to_string(),
        })
    }

    /// Gate on the A/B ratio budget.
    pub fn assert_relative(&self, rel: Rel) -> Result<(), RelError> {
        let mut reasons = Vec::new();
        for (op, a) in &self.by_op {
            if let Some(b) = self.b_by_op.get(op) {
                let a_us = a.p99.as_secs_f64();
                if a_us > 0.0 {
                    let ratio = b.p99.as_secs_f64() / a_us;
                    if ratio > rel.p99_ratio_max {
                        reasons.push(format!(
                            "{op} p99 ratio {ratio:.2} > {:.2}",
                            rel.p99_ratio_max
                        ));
                    }
                }
            }
        }
        if self.throughput > 0.0 {
            let ratio = self.b_throughput / self.throughput;
            if ratio < rel.throughput_ratio_min {
                reasons.push(format!(
                    "throughput ratio {ratio:.2} < {:.2}",
                    rel.throughput_ratio_min
                ));
            }
        }
        if reasons.is_empty() {
            Ok(())
        } else {
            Err(RelError {
                label: self.label.clone(),
                reasons,
            })
        }
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
