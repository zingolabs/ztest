//! Component metrics: exporter contract, reads, live polling.
//!
//! - Whole contract = a container port named [`PORT_NAME`] serving Prometheus text
//!   at `/metrics` (a new component joins by implementing [`Exporter`], never by
//!   entering a table here)
//! - Durable plane: ztest's Prometheus
//!   ([`observability`](crate::resource::impls::observability)) discovers off pod
//!   labels + that port, keeps full-fidelity history, needs nothing per component
//! - Live plane: [`Poller`] scrapes an [`Exporter`] direct, ~1 s (a display on the
//!   scrape interval lags what it describes)
//! - Knows nothing of syncs/ticks/probes/verdicts — consumers call in

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, ListParams};
use prometheus_parse::Value as Scraped;
use tokio::sync::{Mutex, watch};

use crate::error::EnvError;
use crate::handles::Endpoint;
use crate::portforward::Forwarder;

/// Container-port name serving `/metrics` = the entire contract. Prometheus SD
/// keeps a pod by it, [`PodExporter`] discovers by it, every `pod_spec` declares it
pub const PORT_NAME: &str = "metrics";

// ──────────────────────────────── the rows ────────────────────────────────

/// Family → the one scalar a reader shows.
/// `MeanMs` = `Σ_sum / Σ_count × 1000` over a summary's aggregate families
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reduce {
    Sum,
    Max,
    MeanMs,
}

/// One published metric, owned by its publishing backend (never a global table here).
///
/// - `label` carries the unit, and the reduction yields the value already in it
/// - `live` = meaningful as a mid-run instant, not only as a whole-run total
#[derive(Debug, Clone, Copy)]
pub struct Row {
    pub label: &'static str,
    pub family: &'static str,
    pub reduce: Reduce,
    pub live: bool,
}

/// Shows on a live reader
pub const LIVE: bool = true;
/// Whole-run figure only, never a mid-run instant
pub const AT_REST: bool = false;

pub const fn row(label: &'static str, family: &'static str, reduce: Reduce, live: bool) -> Row {
    Row { label, family, reduce, live }
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
) -> Result<Exposition, String> {
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

// ──────────────────────────────── Exporter ────────────────────────────────

/// Scrapable right now. This impl + a [`PORT_NAME`] port in `pod_spec` = joining
/// the metrics plane (nothing here names a component)
#[async_trait::async_trait]
pub trait Exporter: Send + Sync + 'static {
    /// `/metrics` location, resolved per scrape (pods get replaced mid-run).
    /// Caching is the implementation's call
    async fn endpoint(&self) -> Result<Endpoint, EnvError>;

    fn rows(&self) -> &'static [Row] {
        &[]
    }

    /// Scrapee's `ztest.io/component` label, `None` until a target resolves
    fn component(&self) -> Option<String> {
        None
    }

    /// One scrape, now. Live readers want [`Poller`] (holds its client across scrapes)
    async fn read(&self, timeout: Duration) -> Result<Exposition, String> {
        let endpoint = self.endpoint().await.map_err(|e| e.to_string())?;
        let http = reqwest::Client::new();
        scrape(&http, &endpoint.url("http"), timeout).await
    }
}

/// Exporter reached from **outside** the cluster: pod by `ztest.io/component-category`,
/// forwarded to its [`PORT_NAME`] port, `rows_for` keyed on `ztest.io/component`.
///
/// - `latched` filled at first resolve: per-frame reads are sync, so they cannot
///   await the target lock (and a category's component is fixed for a run)
pub struct PodExporter {
    client: Client,
    namespace: String,
    category: String,
    rows_for: fn(&str) -> &'static [Row],
    target: Mutex<Option<Target>>,
    latched: std::sync::RwLock<(Option<String>, &'static [Row])>,
}

struct Target {
    pod: String,
    forwarder: Forwarder,
}

impl std::fmt::Debug for PodExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PodExporter")
            .field("namespace", &self.namespace)
            .field("category", &self.category)
            .finish_non_exhaustive()
    }
}

impl PodExporter {
    pub fn new(
        client: Client,
        namespace: String,
        category: impl Into<String>,
        rows_for: fn(&str) -> &'static [Row],
    ) -> PodExporter {
        PodExporter {
            client,
            namespace,
            category: category.into(),
            rows_for,
            target: Mutex::new(None),
            latched: std::sync::RwLock::new((None, &[])),
        }
    }

    /// Newest scrape-capable pod of this category + its port and component label
    async fn resolve(&self) -> Option<(String, u16, String)> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let params =
            ListParams::default().labels(&format!("ztest.io/component-category={}", self.category));
        let pods = api.list(&params).await.ok()?;
        pods.items.iter().find_map(|p| {
            if !ready_to_scrape(p) {
                return None;
            }
            let label = p
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("ztest.io/component"))
                .cloned()
                .unwrap_or_default();
            Some((p.metadata.name.clone()?, metrics_port(p)?, label))
        })
    }
}

#[async_trait::async_trait]
impl Exporter for PodExporter {
    async fn endpoint(&self) -> Result<Endpoint, EnvError> {
        let (pod, port, component) = self.resolve().await.ok_or_else(|| EnvError::Config {
            reason: format!(
                "no ready {} pod in {} exposes a `{PORT_NAME}` port",
                self.category, self.namespace
            ),
        })?;

        let mut target = self.target.lock().await;
        // Keep the forwarder while it still points here (a fresh one per scrape
        // rebinds a local port every period and loses the reuse making 1 s free)
        if !matches!(&*target, Some(t) if t.pod == pod) {
            let forwarder =
                Forwarder::start(self.client.clone(), self.namespace.clone(), pod.clone(), port)
                    .await
                    .map_err(|e| EnvError::Config {
                        reason: format!("portforward to {pod}:{port}: {e}"),
                    })?;
            *target = Some(Target { pod, forwarder });
            *self.latched.write().expect("latch poisoned") =
                (Some(component.clone()), (self.rows_for)(&component));
        }
        let local = target.as_ref().expect("target set immediately above").forwarder.local_port;
        Ok(Endpoint { host: std::net::Ipv4Addr::LOCALHOST.into(), port: local })
    }

    fn rows(&self) -> &'static [Row] {
        self.latched.read().expect("latch poisoned").1
    }

    fn component(&self) -> Option<String> {
        self.latched.read().expect("latch poisoned").0.clone()
    }
}

/// Scrapable = Running (dialing a `Pending`/terminated pod only yields errors a
/// reader shows as a broken exporter)
fn ready_to_scrape(pod: &Pod) -> bool {
    pod.status.as_ref().and_then(|s| s.phase.as_deref()).is_some_and(|phase| phase == "Running")
}

fn metrics_port(pod: &Pod) -> Option<u16> {
    pod.spec
        .as_ref()?
        .containers
        .iter()
        .flat_map(|c| c.ports.iter().flatten())
        .find(|p| p.name.as_deref() == Some(PORT_NAME))
        .map(|p| p.container_port as u16)
}

// ───────────────────────────────── Poller ─────────────────────────────────

/// Default live cadence: a counter climbing during a scan must look like it.
/// No load concern (an exporter is an in-memory registry)
pub const LIVE_PERIOD: Duration = Duration::from_secs(1);

/// Per-scrape bound, under [`LIVE_PERIOD`] (a wedged target must not stall the cadence)
const LIVE_TIMEOUT: Duration = Duration::from_millis(700);

/// One reading. No status enum: never-read (`at.is_none()`), unreachable
/// (`error.is_some()`) and silent (all values `None`) all derive, a second encoding drifts
#[derive(Debug, Clone, Default)]
pub struct Sample {
    pub at: Option<Instant>,
    pub exposition: Arc<Exposition>,
    pub error: Option<String>,
}

impl Sample {
    /// Why this sample shows nothing, when it shows nothing.
    ///
    /// - Only this plane separates "never resolved" / "unreachable" / "silent", so a
    ///   renderer of blank rows never has to guess
    /// - `published` = the caller's own verdict on the families it asked for
    pub fn note(&self, published: bool) -> Option<String> {
        match (&self.error, self.at) {
            (None, None) => Some("no metrics-exposing pod yet".to_string()),
            (Some(why), None) => Some(format!("resolving target · {why}")),
            (Some(why), Some(_)) => Some(format!("unavailable · {why}")),
            (None, Some(_)) if !published => {
                Some(format!("scraping every {}s · no series published yet", LIVE_PERIOD.as_secs()))
            }
            (None, Some(_)) => None,
        }
    }
}

/// Running live scrape of one [`Exporter`]. Drop = stop + release the exporter's
/// hold on its target (a departed reader must not keep dialing)
pub struct Poller {
    rx: watch::Receiver<Sample>,
    task: tokio::task::JoinHandle<()>,
    exporter: Arc<dyn Exporter>,
}

impl std::fmt::Debug for Poller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Poller").field("rows", &self.rows().len()).finish_non_exhaustive()
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Poller {
    pub fn spawn(exporter: impl Exporter, period: Duration) -> Poller {
        let exporter: Arc<dyn Exporter> = Arc::new(exporter);
        let (tx, rx) = watch::channel(Sample::default());
        let task = tokio::spawn(poll_loop(exporter.clone(), period, tx));
        Poller { rx, task, exporter }
    }

    pub fn rows(&self) -> &'static [Row] {
        self.exporter.rows()
    }

    /// See [`Exporter::component`]
    pub fn component(&self) -> Option<String> {
        self.exporter.component()
    }

    /// Next sample, always the newest (a lagging caller skips, never drains a backlog).
    ///
    /// - Cancel-safe, fit for a `select!` arm
    /// - Pends forever once the poll task is gone (an instantly-resolving arm spins its loop)
    pub async fn changed(&mut self) -> Sample {
        match self.rx.changed().await {
            Ok(()) => self.rx.borrow_and_update().clone(),
            Err(_) => std::future::pending().await,
        }
    }
}

async fn poll_loop(exporter: Arc<dyn Exporter>, period: Duration, tx: watch::Sender<Sample>) {
    let http = match reqwest::Client::builder().timeout(LIVE_TIMEOUT).build() {
        Ok(http) => http,
        Err(e) => {
            let _ = tx
                .send(Sample { error: Some(format!("no HTTP client: {e}")), ..Sample::default() });
            return;
        }
    };
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let sample = match exporter.endpoint().await {
            Ok(endpoint) => match scrape(&http, &endpoint.url("http"), LIVE_TIMEOUT).await {
                Ok(exposition) => Sample {
                    at: Some(Instant::now()),
                    exposition: Arc::new(exposition),
                    error: None,
                },
                Err(e) => failed(&tx, e),
            },
            Err(e) => failed(&tx, e.to_string()),
        };
        // `send` fails only with every reader gone = what this feeds is down
        if tx.send(sample).is_err() {
            return;
        }
    }
}

/// Failure keeps the last good exposition (one refused scrape must not blank a
/// truthful reading), with `error` marking it stale
fn failed(tx: &watch::Sender<Sample>, error: String) -> Sample {
    Sample { error: Some(error), ..tx.borrow().clone() }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// zaino-shaped exposition: counter across two label sets, gauge, summary, as
    /// `metrics-exporter-prometheus` emits
    const EXPOSITION: &str = "\
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

    fn exposition(texts: &[&str]) -> Exposition {
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

    #[test]
    fn a_pod_without_a_metrics_port_is_not_a_target() {
        let with: Pod = serde_json::from_value(json!({
            "metadata": { "name": "zainod" },
            "spec": { "containers": [{
                "name": "zainod",
                "ports": [{ "name": "grpc", "containerPort": 8137 },
                          { "name": PORT_NAME, "containerPort": 9998 }],
            }] },
            "status": { "phase": "Running" },
        }))
        .expect("a pod");
        let without: Pod = serde_json::from_value(json!({
            "metadata": { "name": "wallet" },
            "spec": { "containers": [{ "name": "wallet" }] },
            "status": { "phase": "Running" },
        }))
        .expect("a pod");

        assert_eq!(metrics_port(&with), Some(9998));
        assert_eq!(metrics_port(&without), None);
        assert!(ready_to_scrape(&with));
    }

    /// Dialing an unstarted pod yields connection errors shown as a broken exporter
    #[test]
    fn a_pending_pod_is_not_scraped_yet() {
        let pending: Pod = serde_json::from_value(json!({
            "metadata": { "name": "zainod" },
            "status": { "phase": "Pending" },
        }))
        .expect("a pod");
        assert!(!ready_to_scrape(&pending));
    }

    /// Every cause a reader must distinguish, derived from the sample itself
    /// (why no status enum, and why no consumer classifies a blank column)
    #[test]
    fn a_sample_states_why_it_has_no_values() {
        let at = || Some(Instant::now());
        let tip = |s: &Sample| s.exposition.reduce("zaino_chain_tip_height", Reduce::Max);

        // never read → no target yet
        let never = Sample::default();
        assert_eq!(never.note(false).as_deref(), Some("no metrics-exposing pod yet"));
        assert_eq!(tip(&never), None);

        // unresolved → target lookup failed, nothing scraped
        let unresolved = Sample { error: Some("no ready indexer pod".into()), ..Sample::default() };
        assert!(
            unresolved.note(false).is_some_and(|n| n.starts_with("resolving target")),
            "{unresolved:?}"
        );

        // unreachable → scraped once, target now down
        let unreachable =
            Sample { at: at(), error: Some("connection refused".into()), ..Sample::default() };
        assert!(
            unreachable.note(false).is_some_and(|n| n.contains("connection refused")),
            "{unreachable:?}"
        );

        // silent → scraped fine, asked-for families absent
        let silent = Sample {
            at: at(),
            exposition: Arc::new(exposition(&["# TYPE other gauge\nother 1\n"])),
            error: None,
        };
        assert!(
            silent.note(false).is_some_and(|n| n.contains("no series published yet")),
            "{silent:?}"
        );
        assert_eq!(tip(&silent), None);

        // published → no note
        let sampled =
            Sample { at: at(), exposition: Arc::new(exposition(&[EXPOSITION])), error: None };
        assert_eq!(sampled.note(true), None);
        assert_eq!(tip(&sampled), Some(304.0));
    }

    /// Failure after a good scrape keeps the numbers, adds the reason (blanking on
    /// one refused connection loses a reading true a second ago)
    #[test]
    fn a_failure_keeps_the_last_good_reading() {
        let (tx, _rx) = watch::channel(Sample {
            at: Some(Instant::now()),
            exposition: Arc::new(exposition(&[EXPOSITION])),
            error: None,
        });
        let after = failed(&tx, "connection refused".into());
        assert_eq!(after.error.as_deref(), Some("connection refused"));
        assert_eq!(after.exposition.reduce("zaino_chain_tip_height", Reduce::Max), Some(304.0));
    }
}
