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

pub use self::live::{Exporter, LIVE_PERIOD, PodExporter, Poller, Sample};

/// Container-port name serving `/metrics` = the entire contract. Prometheus SD
/// keeps a pod by it, [`PodExporter`] discovers by it, every `pod_spec` declares it
pub const PORT_NAME: &str = "metrics";

/// Cadence the collect plane samples at. Stated once — the Prometheus ConfigMap is rendered
/// from it, and a plot `step` under it invents points Prometheus fills by repeating the last
/// sample, a flat stretch that never happened
pub const SCRAPE_INTERVAL: Duration = Duration::from_secs(5);

// ──────────────────────────────── the catalogue ────────────────────────────────

/// Wire dimension of a published value, base units only.
///
/// - Renderer scales (ms, MiB, `/s`); a catalogue that stores a display unit drifts from the wire
/// - `Seconds` vs `Ratio` = what the seconds are seconds *of*: cpu time is unbounded-parallel
///   (rated → cores), a PSI stall is bounded by wall clock (rated → a fraction)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dimension {
    Seconds,
    Bytes,
    Ratio,
    Count,
}

/// Quantile a bucketed histogram answers (interpolated in-bucket → only as good as the ladder)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phi {
    P50,
    P99,
}

impl Phi {
    pub fn value(self) -> f64 {
        match self {
            Phi::P50 => 0.5,
            Phi::P99 => 0.99,
        }
    }
}

/// Histogram's cumulative parts, undivided — the reader owns the span.
///
/// `Δsum/Δcount` over two scrapes = that window's mean; `sum/count` off one = lifetime
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Tally {
    pub sum: f64,
    pub count: f64,
}

impl Tally {
    /// `None` = nothing observed across the span (`0 ms` would read as an instant one)
    pub fn mean_ms(before: Tally, after: Tally) -> Option<f64> {
        let (sum, count) = (after.sum - before.sum, after.count - before.count);
        (count > 0.0 && sum >= 0.0).then(|| sum / count * 1000.0)
    }
}

/// Narrows a split family to one label value (folding a producer's dimension changes the quantity).
///
/// zaino stages every ingest counter (`finalised`/`non-finalised`/`migration`) → a fold counts 3×
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Select {
    pub label: &'static str,
    pub value: &'static str,
}

/// Family name + the selector making it one quantity + what it is measured in.
///
/// Reached through a shape witness ([`Counter`]/[`Gauge`]/[`Hist`]), never declared bare — the
/// shape is what makes a [`Reading`] legal
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Family {
    pub name: &'static str,
    pub select: Option<Select>,
    pub dim: Dimension,
}

/// Selector included — "nothing published" != "that label value never published"
impl std::fmt::Display for Family {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.select {
            None => write!(f, "{}", self.name),
            Some(s) => write!(f, "{}{{{}=\"{}\"}}", self.name, s.label, s.value),
        }
    }
}

/// Monotonic cumulative count. Resets to 0 on producer restart
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counter(Family);
/// Level sampled as published. A decrease is a real move, never a restart
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gauge(Family);
/// Cumulative `_sum`/`_count`/`_bucket` ladder.
///
/// No `Summary` peer: producer-windowed quantiles are not re-aggregatable, so nothing can read one
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hist(Family);

pub const fn counter(name: &'static str, dim: Dimension) -> Counter {
    Counter(Family { name, select: None, dim })
}

pub const fn gauge(name: &'static str, dim: Dimension) -> Gauge {
    Gauge(Family { name, select: None, dim })
}

pub const fn hist(name: &'static str, dim: Dimension) -> Hist {
    Hist(Family { name, select: None, dim })
}

/// Split family, narrowed to one label value
pub const fn counter_where(
    name: &'static str,
    dim: Dimension,
    label: &'static str,
    value: &'static str,
) -> Counter {
    Counter(Family { name, select: Some(Select { label, value }), dim })
}

macro_rules! shape {
    ($($t:ty),*) => {$(
        impl $t {
            pub const fn family(self) -> Family {
                self.0
            }
        }
        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    )*};
}
shape!(Counter, Gauge, Hist);

impl Counter {
    /// Per-second plot + the exact count it accumulated
    pub const fn rate(self) -> Reading {
        Reading::Rate(self.0)
    }
}

impl Gauge {
    pub const fn level(self) -> Reading {
        Reading::Level(self.0)
    }

    /// Per-second plot + net progress across the run
    pub const fn slope(self) -> Reading {
        Reading::Slope(self.0)
    }

    /// Net change only, no plot
    pub const fn progress(self) -> Reading {
        Reading::Progress(self.0)
    }
}

impl Hist {
    pub const fn mean(self) -> Reading {
        Reading::Mean(self.0)
    }

    pub const fn p(self, phi: Phi) -> Reading {
        Reading::Quantile(self.0, phi)
    }
}

/// The projections that mean anything, closed.
///
/// Both planes are a total match over these arms — a seventh fails to compile in both at once.
/// Constructed only through a shape witness, so `Gauge::rate` (a reorg read as a counter reset,
/// adding back the whole height) does not exist to be written
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reading {
    Rate(Family),
    Slope(Family),
    Level(Family),
    Progress(Family),
    Mean(Family),
    Quantile(Family, Phi),
}

impl Reading {
    pub const fn family(self) -> Family {
        match self {
            Reading::Rate(f)
            | Reading::Slope(f)
            | Reading::Level(f)
            | Reading::Progress(f)
            | Reading::Mean(f) => f,
            Reading::Quantile(f, _) => f,
        }
    }

    /// Cumulative on the wire → differenced before it means anything
    pub const fn cumulative(self) -> bool {
        matches!(self, Reading::Rate(_) | Reading::Mean(_) | Reading::Quantile(_, _))
    }

    /// Carries a whole-run count (a level and a latency do not)
    pub const fn totalled(self) -> bool {
        matches!(self, Reading::Rate(_) | Reading::Slope(_) | Reading::Progress(_))
    }

    /// Plotted over time (`Progress` is one number, and a latency plots per stage, not stacked)
    pub const fn plotted(self) -> bool {
        matches!(self, Reading::Rate(_) | Reading::Slope(_) | Reading::Level(_))
    }

    /// Unit of the *result*, derived — a declared one drifts from the wire
    pub const fn unit(self) -> Unit {
        match self {
            Reading::Rate(f) | Reading::Slope(f) => match f.dim {
                Dimension::Bytes => Unit::BytesPerSec,
                Dimension::Seconds => Unit::Cores,
                Dimension::Ratio => Unit::Fraction,
                Dimension::Count => Unit::PerSec,
            },
            Reading::Mean(_) | Reading::Quantile(_, _) => Unit::Millis,
            Reading::Level(f) | Reading::Progress(f) => match f.dim {
                Dimension::Bytes => Unit::Bytes,
                Dimension::Seconds => Unit::Millis,
                Dimension::Ratio => Unit::Fraction,
                Dimension::Count => Unit::Count,
            },
        }
    }
}

/// Which panel a row belongs to, so a renderer groups by meaning rather than by matching
/// family names it would have to hardcode per backend.
///
/// - `Transparent`/`Shielded` rows are per-pool and stack, keyed by [`Channel`](crate::sync::Channel)
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
/// `channel` set on the per-pool rows only — it is what folds a split (spends + outputs) and
/// keys the stack's colour, in place of matching the display label
#[derive(Debug, Clone, Copy)]
pub struct Row {
    pub label: &'static str,
    pub reading: Reading,
    pub facet: Facet,
    pub channel: Option<crate::sync::Channel>,
}

pub const fn row(label: &'static str, reading: Reading, facet: Facet) -> Row {
    Row { label, reading, facet, channel: None }
}

impl Row {
    /// Pool this row stacks into
    pub const fn pool(mut self, channel: crate::sync::Channel) -> Row {
        self.channel = Some(channel);
        self
    }

    pub const fn family(&self) -> Family {
        self.reading.family()
    }

    pub const fn unit(&self) -> Unit {
        self.reading.unit()
    }
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
    by_name: HashMap<String, Vec<Point>>,
    by_histogram: HashMap<String, Vec<Buckets>>,
}

/// Per label set: `(upper bound, observations ≤ it)` ascending to `+Inf`.
/// Kept — a quantile is unrecoverable from `_sum`/`_count`
#[derive(Debug, Clone)]
struct Buckets {
    labels: prometheus_parse::Labels,
    le: Vec<(f64, f64)>,
}

/// Scalar sample + the labels a [`Select`] narrows on (a fold cannot be undone later)
#[derive(Debug, Clone)]
struct Point {
    value: f64,
    labels: prometheus_parse::Labels,
}

/// Live counterpart of `histogram_quantile(φ, rate(f_bucket[w]))`, ms.
///
/// - Differenced first → describes the window, not all history
/// - `None` = nothing observed, or no `+Inf` bucket to total against
pub fn windowed_quantile(before: &[(f64, f64)], after: &[(f64, f64)], phi: Phi) -> Option<f64> {
    let (&(inf, hi), &(_, lo)) = (after.last()?, before.last()?);
    if before.len() != after.len() || !inf.is_infinite() {
        return None;
    }
    let rank = phi.value() * (hi - lo);
    if rank <= 0.0 {
        return None;
    }
    // Bucket below the one holding `rank`, as (upper bound, count)
    let mut under = (0.0, 0.0);
    for (&(le, b), &(le_after, a)) in before.iter().zip(after) {
        // Re-bucketed mid-run (redeploy): equal lengths would difference unlike bounds
        if le != le_after {
            return None;
        }
        let count = (a - b).max(0.0);
        if count < rank {
            under = (le, count);
            continue;
        }
        let span = count - under.1;
        let frac = if span > 0.0 { (rank - under.1) / span } else { 0.0 };
        let at = if le.is_infinite() { under.0 } else { under.0 + (le - under.0) * frac };
        return Some(at * 1000.0);
    }
    None
}

/// Unselected admits every series; a series missing the label is not that value
fn admits(select: Option<Select>, labels: &prometheus_parse::Labels) -> bool {
    match select {
        None => true,
        Some(s) => labels.get(s.label) == Some(s.value),
    }
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
            // `_sum`/`_count` arrive as scalar families of their own, either way
            let value = match sample.value {
                Scraped::Counter(v) | Scraped::Gauge(v) | Scraped::Untyped(v) => v,
                Scraped::Histogram(counts) => {
                    let le = counts.iter().map(|c| (c.less_than, c.count)).collect();
                    let labels = sample.labels.clone();
                    self.by_histogram
                        .entry(sample.metric)
                        .or_default()
                        .push(Buckets { labels, le });
                    continue;
                }
                // Producer-windowed → cannot be re-scoped or aggregated
                Scraped::Summary(_) => continue,
            };
            let labels = sample.labels.clone();
            self.by_name.entry(sample.metric).or_default().push(Point { value, labels });
        }
    }

    /// `None` = family absent
    pub fn tally(&self, family: Family) -> Option<Tally> {
        let sum: f64 = self.part(family, "_sum")?.sum();
        let count: f64 = self.part(family, "_count")?.sum();
        Some(Tally { sum, count })
    }

    /// Whether `row` resolves in the *shape* its reading needs.
    ///
    /// Shape, not name: a gauge turned histogram upstream keeps its name and stops answering a level
    pub fn resolves(&self, row: &Row) -> bool {
        let family = row.family();
        match row.reading {
            Reading::Rate(_) | Reading::Slope(_) | Reading::Level(_) | Reading::Progress(_) => {
                self.values(family).is_some()
            }
            Reading::Mean(_) => self.tally(family).is_some(),
            Reading::Quantile(_, _) => self.buckets(family).is_some_and(|b| !b.is_empty()),
        }
    }

    /// Cumulative parts + the ladder a quantile needs, where the producer bucketed it
    pub fn timing(&self, hist: Hist) -> Option<crate::sync::Timing> {
        let family = hist.family();
        Some(crate::sync::Timing {
            tally: self.tally(family)?,
            buckets: self.buckets(family).unwrap_or_default().into(),
        })
    }

    /// Folded across every admitted label set, ascending. `None` = not bucketed here
    pub fn buckets(&self, family: Family) -> Option<Vec<(f64, f64)>> {
        let sets = self.by_histogram.get(family.name)?;
        let mut folded: std::collections::BTreeMap<u64, f64> = std::collections::BTreeMap::new();
        let mut matched = false;
        for set in sets.iter().filter(|s| admits(family.select, &s.labels)) {
            matched = true;
            for &(le, count) in &set.le {
                *folded.entry(le.to_bits()).or_default() += count;
            }
        }
        matched.then(|| folded.into_iter().map(|(le, c)| (f64::from_bits(le), c)).collect())
    }

    fn values(&self, family: Family) -> Option<impl Iterator<Item = f64> + '_> {
        self.part(family, "")
    }

    /// Selector applies under every suffix (`_sum`/`_count` carry the producer's labels too).
    ///
    /// Absent family → `None`; selector matching nothing → empty (present, that value never seen)
    fn part(&self, family: Family, suffix: &str) -> Option<impl Iterator<Item = f64> + '_> {
        let points = match suffix.is_empty() {
            true => self.by_name.get(family.name)?,
            false => self.by_name.get(&format!("{}{suffix}", family.name))?,
        };
        let select = family.select;
        Some(points.iter().filter(move |p| admits(select, &p.labels)).map(|p| p.value))
    }

    /// Cumulative total across every admitted label set. Absent family → `None`, never a zero
    pub fn total(&self, counter: Counter) -> Option<f64> {
        Some(self.values(counter.family())?.sum())
    }

    /// Largest across admitted label sets — a level is one quantity however many targets
    /// report it, and summing two would invent a height neither reached
    pub fn level(&self, gauge: Gauge) -> Option<f64> {
        self.values(gauge.family())?
            .fold(None, |acc: Option<f64>, v| Some(acc.map_or(v, |a| a.max(v))))
    }

    /// Height gauge → whole number.
    ///
    /// - Every gauge here = a block height the exporter widened to `f64`; narrow at
    ///   the read so that leaks into no probe arithmetic
    /// - Negative/non-finite → `None`, never a wrapped `u32` (broken exporter, not a low height)
    pub fn height(&self, gauge: Gauge) -> Option<u32> {
        let v = self.level(gauge)?;
        (v.is_finite() && v >= 0.0).then_some(v as u32)
    }

    /// Counter total → whole number, under [`height`](Self::height)'s narrowing contract
    pub fn counter_total(&self, counter: Counter) -> Option<u64> {
        let v = self.total(counter)?;
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

    /// Unselected folds every label set; a selector reads one and an unpublished value
    /// reads as absent, never as the fold
    #[test]
    fn a_selector_narrows_a_split_family_instead_of_folding_it() {
        let e = exposition(&[EXPOSITION]);
        let named = |l, v| counter_where("zaino_grpc_requests_total", Dimension::Count, l, v);
        let all = counter("zaino_grpc_requests_total", Dimension::Count);

        assert_eq!(e.total(all), Some(17.0), "unselected folds both methods");
        assert_eq!(e.total(named("method", "GetBlock")), Some(12.0), "selector reads its own");
        assert_eq!(
            e.total(named("method", "GetTreeState")),
            Some(0.0),
            "family present, value never published"
        );
        assert_eq!(e.total(counter("never_published", Dimension::Count)), None);
    }

    /// One scrape carries only the lifetime figure — converges, hiding the decay
    #[test]
    fn a_windowed_reduction_refuses_to_answer_from_one_scrape() {
        let e = exposition(&[EXPOSITION]);
        let latency = hist("zaino_grpc_request_duration_seconds", Dimension::Seconds);
        assert_eq!(
            e.tally(latency.family()),
            Some(Tally { sum: 0.85, count: 17.0 }),
            "the cumulative parts are what a window differences"
        );
    }

    /// `_sum`/`_count` carry the producer's labels too, so a selected mean must not
    /// divide one stage's sum by every stage's count
    #[test]
    fn a_selected_tally_keeps_its_selector_on_both_parts() {
        let mut e = Exposition::default();
        e.absorb(
            "# TYPE zaino_sync_block_fetch_seconds histogram\n\
             zaino_sync_block_fetch_seconds_sum{stage=\"finalised\"} 0.6\n\
             zaino_sync_block_fetch_seconds_count{stage=\"finalised\"} 100\n\
             zaino_sync_block_fetch_seconds_sum{stage=\"non-finalised\"} 9.0\n\
             zaino_sync_block_fetch_seconds_count{stage=\"non-finalised\"} 50\n",
        );
        let one = Family {
            name: "zaino_sync_block_fetch_seconds",
            select: Some(Select { label: "stage", value: "finalised" }),
            dim: Dimension::Seconds,
        };
        assert_eq!(e.tally(one), Some(Tally { sum: 0.6, count: 100.0 }));

        let before = Tally { sum: 0.0, count: 0.0 };
        assert_eq!(
            Tally::mean_ms(before, e.tally(one).unwrap()),
            Some(6.0),
            "9.6s over 150 would be the fold — a different quantity under the same label"
        );
    }

    /// Absent != nothing happened; neither is a latency of zero
    #[test]
    fn an_unobserved_window_has_no_mean_rather_than_a_zero_one() {
        let flat = Tally { sum: 1.0, count: 10.0 };
        assert_eq!(Tally::mean_ms(flat, flat), None);
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
        assert_eq!(e.total(counter("zaino_grpc_requests_total", Dimension::Count)), Some(17.0));
    }

    #[test]
    fn a_level_is_the_max_across_targets_never_their_sum() {
        let e = exposition(&[
            EXPOSITION,
            "# TYPE zaino_chain_tip_height gauge\nzaino_chain_tip_height 309\n",
        ]);
        assert_eq!(e.level(gauge("zaino_chain_tip_height", Dimension::Count)), Some(309.0));
    }

    /// Only path to a quantile — `_sum`/`_count` bound the mean, saying nothing of the tail
    #[test]
    fn a_quantile_comes_from_bucket_deltas_not_from_all_history() {
        let ladder = |le_5ms: u32, le_50ms: u32, inf: u32| {
            let mut e = Exposition::default();
            e.absorb(&format!(
                "# TYPE fetch_seconds histogram\n\
                 fetch_seconds_bucket{{le=\"0.005\"}} {le_5ms}\n\
                 fetch_seconds_bucket{{le=\"0.05\"}} {le_50ms}\n\
                 fetch_seconds_bucket{{le=\"+Inf\"}} {inf}\n"
            ));
            e.buckets(hist("fetch_seconds", Dimension::Seconds).family()).expect("bucketed")
        };
        // History: 100 fast. Window: 100 more, all in the 5–50ms bucket
        let before = ladder(100, 100, 100);
        let after = ladder(100, 200, 200);
        let p50 = windowed_quantile(&before, &after, Phi::P50).expect("observed");
        assert!((5.0..=50.0).contains(&p50), "p50 {p50}ms sits in the window's own bucket");

        assert_eq!(
            windowed_quantile(&before, &before, Phi::P50),
            None,
            "nothing observed in the window is not a latency of zero"
        );

        // Same bucket count, different bounds: differencing these interpolates one
        // ladder's counts across another's edges
        let rebucketed = vec![(0.001, 100.0), (0.5, 200.0), (f64::INFINITY, 200.0)];
        assert_eq!(windowed_quantile(&before, &rebucketed, Phi::P50), None);
    }

    /// Both planes read one catalogue, so every arm must be answerable by both. A reading
    /// the live plane cannot resolve renders `—` where the report shows a number, and the
    /// two disagreeing is exactly the class of defect this catalogue exists to stop
    #[test]
    fn every_reading_a_backend_declares_resolves_against_a_live_exposition() {
        let e = exposition(&[
            EXPOSITION,
            "# TYPE zaino_sync_orchard_actions_total counter\n\
             zaino_sync_orchard_actions_total 4200\n\
             # TYPE zaino_sync_fetched_height gauge\n\
             zaino_sync_fetched_height 658599\n\
             # TYPE zaino_sync_block_fetch_seconds histogram\n\
             zaino_sync_block_fetch_seconds_sum 0.6\n\
             zaino_sync_block_fetch_seconds_count 100\n\
             zaino_sync_block_fetch_seconds_bucket{le=\"0.005\"} 90\n\
             zaino_sync_block_fetch_seconds_bucket{le=\"+Inf\"} 100\n",
        ]);
        let counter = counter("zaino_sync_orchard_actions_total", Dimension::Count);
        let gauge = gauge("zaino_sync_fetched_height", Dimension::Count);
        let hist = hist("zaino_sync_block_fetch_seconds", Dimension::Seconds);

        for reading in [
            counter.rate(),
            gauge.level(),
            gauge.slope(),
            gauge.progress(),
            hist.mean(),
            hist.p(Phi::P99),
        ] {
            let row = row("probe", reading, Facet::Progress);
            assert!(e.resolves(&row), "{reading:?} published but unresolvable live");
        }
    }

    /// The unit is derived from the wire dimension, so a renderer cannot be handed a
    /// per-second figure labelled as a level, or seconds labelled as milliseconds
    #[test]
    fn a_readings_unit_follows_its_wire_dimension() {
        let cpu = counter("container_cpu_usage_seconds_total", Dimension::Seconds);
        let stall = counter("container_pressure_io_stalled_seconds_total", Dimension::Ratio);
        let bytes = counter("container_blkio_device_usage_total", Dimension::Bytes);
        let ops = counter("zaino_sync_orchard_actions_total", Dimension::Count);

        assert_eq!(cpu.rate().unit(), Unit::Cores, "cpu-seconds per second = parallelism");
        assert_eq!(stall.rate().unit(), Unit::Fraction, "stall is bounded by wall clock");
        assert_eq!(bytes.rate().unit(), Unit::BytesPerSec);
        assert_eq!(ops.rate().unit(), Unit::PerSec);

        let height = gauge("zaino_sync_fetched_height", Dimension::Count);
        assert_eq!(height.level().unit(), Unit::Count);
        assert_eq!(height.slope().unit(), Unit::PerSec);
        assert_eq!(
            hist("zaino_sync_block_fetch_seconds", Dimension::Seconds).mean().unit(),
            Unit::Millis,
            "a seconds histogram renders as ms"
        );
    }

    /// A level has no whole-run count and a latency has no plot; asking either for one is
    /// how a rate came to be integrated into a total in the first place
    #[test]
    fn only_a_flow_carries_a_total_and_only_a_level_or_flow_is_plotted() {
        let c = counter("c_total", Dimension::Count);
        let g = gauge("g", Dimension::Count);
        let h = hist("h_seconds", Dimension::Seconds);

        assert!(c.rate().totalled() && c.rate().plotted());
        assert!(g.slope().totalled() && g.slope().plotted());
        assert!(g.progress().totalled() && !g.progress().plotted());
        assert!(!g.level().totalled() && g.level().plotted());
        assert!(!h.mean().totalled() && !h.mean().plotted());
        assert!(!h.p(Phi::P99).totalled());
    }

    /// Unpublished family reads absent, never as a zero a probe accepts as an observation
    #[test]
    fn an_absent_family_is_none_rather_than_zero() {
        let e = exposition(&[EXPOSITION]);
        assert_eq!(e.height(gauge("zaino_sync_finalized_height", Dimension::Count)), None);
        assert_eq!(
            e.counter_total(counter("zaino_sync_orchard_actions_total", Dimension::Count)),
            None
        );
    }

    #[test]
    fn a_height_narrows_back_to_a_whole_height() {
        let e = exposition(&[EXPOSITION]);
        assert_eq!(e.height(gauge("zaino_chain_tip_height", Dimension::Count)), Some(304));
    }
}
