//! Component metrics: one vocabulary, two planes.
//!
//! - Whole exporter contract = a container port named [`PORT_NAME`] serving Prometheus
//!   text at `/metrics` (a new component joins by implementing [`Exporter`], never by
//!   entering a table here)
//! - [`query`] = durable plane, ztest's Prometheus
//!   ([`observability`](crate::resource::impls::observability)) discovers off pod labels
//!   + that port and keeps 30d of history; read back long after the pods are gone
//! - [`live`] = now plane, scrapes an [`Exporter`] direct at ~1 s (a display on the
//!   scrape interval lags what it describes)
//! - [`Row`] is shared by both on purpose: it is what stops a metric meaning one number
//!   live and another in the report
//! - Knows nothing of syncs/ticks/probes/verdicts — consumers call in

pub use crate::fmt::Unit;

use std::collections::HashMap;
use std::time::Duration;

use prometheus_parse::Value as Scraped;

pub mod live;
pub mod query;

pub use self::live::{DEFAULT_SAMPLE_RATE, Exporter, LIVE_PERIOD, PodExporter, Poller, Sample};

/// Container-port name serving `/metrics` = the entire contract. Prometheus SD
/// keeps a pod by it, [`PodExporter`] discovers by it, every `pod_spec` declares it
pub const PORT_NAME: &str = "metrics";

// ──────────────────────────────── the rows ────────────────────────────────

/// Family → the one scalar a reader shows.
///
/// - `MeanMs` = `Σ_sum / Σ_count × 1000` over a summary's aggregate families
/// - Doubles as the counter/gauge distinction: `Sum` = additive across targets
///   (counter), `Max` = a level (gauge). A [`Unit::PerSec`] series differentiates by
///   whichever it is, and picking wrong reports a reorg rollback as a counter reset
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reduce {
    Sum,
    Max,
    MeanMs,
}

/// Prometheus exposition type to read a family as.
///
/// - [`Exposition::absorb`] keeps only scalar samples; histogram/summary composites are
///   dropped and their `_sum`/`_count` arrive as families of their own. So `Sum`/`Count`/
///   `Mean` are the readable parts of a histogram — **buckets and quantiles are not kept**
/// - `Gauge` takes the max and `Counter` the sum across label sets, matching how their
///   PromQL counterparts fold a namespace
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricKind {
    Gauge,
    Counter,
    /// `{name}_sum`
    Sum,
    /// `{name}_count`
    Count,
    /// `{name}_sum / {name}_count`, in the observation's own unit
    Mean,
}

/// Which reading a row belongs to, so a renderer groups by meaning rather than by
/// matching family names it would have to hardcode per backend.
///
/// - `Transparent`/`Shielded` rows are per-pool and stack: `label` = the pool, keyed by
///   `ztest_ui`'s pool palette. Split because the two answer
///   different questions (utxo churn vs note-commitment work) and share no scale
/// - Container cpu/mem carries no facet (kubelet's, not a component's)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Facet {
    Transparent,
    Shielded,
    Blocks,
    Throughput,
    WritePath,
    Progress,
    Store,
}

/// One published metric, owned by its publishing backend (never a global table here).
///
/// - `label` names the quantity, `unit` carries what it is measured in (a label
///   spelling its own unit renders it twice)
/// - `live` = meaningful as a mid-run instant, not only as a whole-run total
#[derive(Debug, Clone, Copy)]
pub struct Row {
    pub label: &'static str,
    pub family: &'static str,
    pub reduce: Reduce,
    pub live: bool,
    pub unit: Unit,
    pub facet: Facet,
}

/// Shows on a live reader
pub const LIVE: bool = true;
/// Whole-run figure only, never a mid-run instant
pub const AT_REST: bool = false;

pub const fn row(
    label: &'static str,
    family: &'static str,
    reduce: Reduce,
    live: bool,
    unit: Unit,
    facet: Facet,
) -> Row {
    Row { label, family, reduce, live, unit, facet }
}

/// What one component publishes, declared beside the family constants it owns.
///
/// The single origin for that component's metric layout: [`crate::backends`]'s
/// label→backend table and its own [`Exporter::rows`] both read this, so a row added
/// here reaches every reader without a second edit. Components whose *sync* is
/// observable extend it with [`Observe`](crate::sync::Observe)
pub trait MetricLayout {
    /// Report order — a reader renders them top to bottom
    const ROWS: &'static [Row];
}

// ───────────────────────────── the exposition ─────────────────────────────

/// Exporter's scalar samples, bucketed by family name. All absorbed targets share
/// a bucket, so a reduction spans them as its PromQL counterpart spans a namespace
#[derive(Debug, Default)]
pub struct Exposition {
    by_name: HashMap<String, Vec<f64>>,
}

impl Exposition {
    /// Fold one exporter's exposition text in.
    ///
    /// - Unparseable absorbs as nothing, never an error (mid-write scrape = transient,
    ///   and the next read beats an error indistinguishable from a broken component)
    pub fn absorb(&mut self, text: &str) {
        let lines = text.lines().map(|l| Ok(l.to_string()));
        let Ok(scrape) = prometheus_parse::Scrape::parse(lines) else {
            return;
        };
        for sample in scrape.samples {
            // Histogram/summary aggregates arrive as their own `_sum`/`_count`
            // families (what `MeanMs` reads); the composites hold nothing scalar
            let value = match sample.value {
                Scraped::Counter(v) | Scraped::Gauge(v) | Scraped::Untyped(v) => v,
                Scraped::Histogram(_) | Scraped::Summary(_) => continue,
            };
            self.by_name.entry(sample.metric).or_default().push(value);
        }
    }

    fn values(&self, family: &str) -> Option<&[f64]> {
        self.by_name.get(family).map(Vec::as_slice)
    }

    /// Apply one reduction. Absent family → `None`, a real answer ("nothing published
    /// of that kind yet") and never a zero
    pub fn reduce(&self, family: &str, reduce: Reduce) -> Option<f64> {
        match reduce {
            Reduce::Sum => Some(self.values(family)?.iter().sum()),
            Reduce::Max => self
                .values(family)?
                .iter()
                .copied()
                .fold(None, |acc: Option<f64>, v| Some(acc.map_or(v, |a| a.max(v)))),
            Reduce::MeanMs => {
                let sum: f64 = self.values(&format!("{family}_sum"))?.iter().sum();
                let count: f64 = self.values(&format!("{family}_count"))?.iter().sum();
                // No observations = 0/0: no mean latency yet, and NaN would render as one
                (count > 0.0).then(|| sum / count * 1000.0)
            }
        }
    }

    /// One family as `kind`. Absent → `None`, never a zero
    pub fn read(&self, name: &str, kind: MetricKind) -> Option<f64> {
        match kind {
            MetricKind::Gauge => self.reduce(name, Reduce::Max),
            MetricKind::Counter => self.reduce(name, Reduce::Sum),
            MetricKind::Sum => self.reduce(&format!("{name}_sum"), Reduce::Sum),
            MetricKind::Count => self.reduce(&format!("{name}_count"), Reduce::Sum),
            MetricKind::Mean => {
                let sum = self.reduce(&format!("{name}_sum"), Reduce::Sum)?;
                let count = self.reduce(&format!("{name}_count"), Reduce::Sum)?;
                // No observations = 0/0; NaN would render as a real mean
                (count > 0.0).then(|| sum / count)
            }
        }
    }

    /// Gauge → whole number.
    ///
    /// - Every gauge here = a block height the exporter widened to `f64`; narrow at
    ///   the read so that leaks into no probe arithmetic
    /// - Negative/non-finite → `None`, never a wrapped `u32` (broken exporter, not a low height)
    pub fn height_gauge(&self, family: &str) -> Option<u32> {
        let v = self.reduce(family, Reduce::Max)?;
        (v.is_finite() && v >= 0.0).then_some(v as u32)
    }

    /// Counter total → whole number, under [`height_gauge`](Self::height_gauge)'s
    /// narrowing contract
    pub fn counter_total(&self, family: &str) -> Option<u64> {
        let v = self.reduce(family, Reduce::Sum)?;
        (v.is_finite() && v >= 0.0).then_some(v as u64)
    }
}

/// `base` = `http://host:port` root, `/metrics` appended here.
/// `timeout` = when the caller calls the target wedged, not a budget (hence no default)
pub async fn scrape(
    http: &reqwest::Client,
    base: &str,
    timeout: Duration,
) -> Result<Exposition, crate::error::PipelineError> {
    let text = http
        .get(format!("{}/metrics", base.trim_end_matches('/')))
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;
    let mut exposition = Exposition::default();
    exposition.absorb(&text);
    Ok(exposition)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// zaino-shaped exposition: counter across two label sets, gauge, summary, as
    /// `metrics-exporter-prometheus` emits
    pub const EXPOSITION: &str = "\
# HELP zaino_grpc_requests_total Total gRPC requests
# TYPE zaino_grpc_requests_total counter
zaino_grpc_requests_total{method=\"GetBlock\"} 12
zaino_grpc_requests_total{method=\"GetLightdInfo\"} 5
# TYPE zaino_chain_tip_height gauge
zaino_chain_tip_height 304
# TYPE zaino_grpc_request_duration_seconds summary
zaino_grpc_request_duration_seconds{quantile=\"0.5\"} 0.004
zaino_grpc_request_duration_seconds_sum 0.85
zaino_grpc_request_duration_seconds_count 17
";

    /// Histogram composites are dropped by `absorb`; only the `_sum`/`_count` families
    /// survive, so those are what `Sum`/`Count`/`Mean` read
    #[test]
    fn metric_kinds_read_their_own_families() {
        let e = exposition(&[EXPOSITION]);
        assert_eq!(e.read("zaino_chain_tip_height", MetricKind::Gauge), Some(304.0));
        // Counter folds both label sets
        assert_eq!(e.read("zaino_grpc_requests_total", MetricKind::Counter), Some(17.0));
        assert_eq!(e.read("zaino_grpc_request_duration_seconds", MetricKind::Sum), Some(0.85));
        assert_eq!(e.read("zaino_grpc_request_duration_seconds", MetricKind::Count), Some(17.0));
        let mean = e.read("zaino_grpc_request_duration_seconds", MetricKind::Mean).unwrap();
        assert!((mean - 0.05).abs() < 1e-9, "mean {mean}");
        assert_eq!(e.read("never_published", MetricKind::Gauge), None);
    }

    pub fn exposition(texts: &[&str]) -> Exposition {
        let mut e = Exposition::default();
        for t in texts {
            e.absorb(t);
        }
        e
    }

    #[test]
    fn a_counters_label_sets_sum_into_one_family_total() {
        let e = exposition(&[EXPOSITION]);
        assert_eq!(e.reduce("zaino_grpc_requests_total", Reduce::Sum), Some(17.0));
    }

    #[test]
    fn a_gauge_reduces_by_max_across_targets() {
        let e = exposition(&[
            EXPOSITION,
            "# TYPE zaino_chain_tip_height gauge\nzaino_chain_tip_height 309\n",
        ]);
        assert_eq!(e.reduce("zaino_chain_tip_height", Reduce::Max), Some(309.0));
    }

    /// The reduction with arithmetic in it
    #[test]
    fn mean_latency_is_the_summarys_sum_over_its_count_in_milliseconds() {
        let e = exposition(&[EXPOSITION]);
        let ms = e.reduce("zaino_grpc_request_duration_seconds", Reduce::MeanMs).expect("a mean");
        assert!((ms - 50.0).abs() < 1e-9, "0.85s / 17 = 50ms, got {ms}");
    }

    /// Unpublished family reads absent, never as a zero a probe accepts as an observation
    #[test]
    fn an_absent_family_is_none_rather_than_zero() {
        let e = exposition(&[EXPOSITION]);
        assert_eq!(e.height_gauge("zaino_sync_finalized_height"), None);
        assert_eq!(e.counter_total("zaino_sync_orchard_actions_total"), None);
    }

    #[test]
    fn a_height_gauge_narrows_back_to_a_whole_height() {
        let e = exposition(&[EXPOSITION]);
        assert_eq!(e.height_gauge("zaino_chain_tip_height"), Some(304));
    }
}
