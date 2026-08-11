//! Component metrics: the exporter contract, reading it, and polling it live.
//!
//! One contract, stated once: a component declares a container port **named
//! [`PORT_NAME`]** and serves Prometheus text exposition at `/metrics` on it.
//! Everything here is built on that and nothing else, which is what lets a new
//! ecosystem component join by implementing [`Exporter`] rather than by being
//! added to a table somewhere in this file.
//!
//! Two planes, deliberately separate:
//!
//! - **Durable record.** ztest emits a `PodMonitor` per metrics-enabled
//!   component ([`emit_pod_monitor`]). OpenShift User Workload Monitoring
//!   discovers it and scrapes the component into the user-workload Prometheus,
//!   where the full-fidelity history lives for as long as the cluster keeps it.
//!   ztest writes that record and never reads it back — a time-series database
//!   is a better home for it than a field on a run report, and anything can
//!   query it.
//! - **Live read.** [`Poller`] scrapes an [`Exporter`] directly on its own
//!   cadence. Direct because UWM enforces a ~15 s scrape floor, so no amount of
//!   querying it more often yields a fresher number.
//!
//! This module knows nothing about syncs, ticks, probes or verdicts. Consumers
//! call in; no type here is owned by another subsystem.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use prometheus_parse::Value as Scraped;
use serde_json::json;
use tokio::sync::{Mutex, watch};

use crate::error::{EnvError, env_err};
use crate::handles::Endpoint;
use crate::portforward::Forwarder;

/// The container-port name a component serves `/metrics` on. The whole contract:
/// the `PodMonitor` selects this name, [`PodExporter`] discovers by it, and each
/// backend's `pod_spec` declares it.
pub const PORT_NAME: &str = "metrics";

/// Field-manager for server-side applies of ztest-owned PodMonitors.
const FIELD_MANAGER: &str = "ztest";
/// UWM enforces a scrape-interval floor (~5–15 s); 15 s is fine for the
/// minutes-to-hours sync/testnet tests this record serves. It is also why
/// nothing reads back through it live — see [`Poller`].
const SCRAPE_INTERVAL: Duration = Duration::from_secs(15);

// ─────────────────────────── the durable record ───────────────────────────

fn podmonitor_api(client: &Client, namespace: &str) -> Api<DynamicObject> {
    let ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "monitoring.coreos.com",
        "v1",
        "PodMonitor",
    ));
    Api::namespaced_with(client.clone(), namespace, &ar)
}

/// Server-side-apply a `PodMonitor` selecting one component's pod by its
/// `ztest.io/component-name` label and scraping its [`PORT_NAME`] port.
/// Idempotent; torn down with the run namespace.
///
/// The per-run namespace is the natural query scope (one namespace per run), so
/// no run-id metric relabeling is needed to isolate a run.
///
/// A no-op-safe swallow of the missing-CRD case: on a cluster without the
/// Prometheus-operator CRDs, emitting a PodMonitor is meaningless and must not
/// fail a test — the caller logs and continues.
pub(crate) async fn emit_pod_monitor(
    client: &Client,
    namespace: &str,
    pod_name: &str,
    run_id: &str,
) -> Result<(), EnvError> {
    let name = format!("ztest-{pod_name}");
    let obj: DynamicObject = serde_json::from_value(json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PodMonitor",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": { "ztest.io/run-id": run_id },
        },
        "spec": {
            "selector": { "matchLabels": { "ztest.io/component-name": pod_name } },
            "podMetricsEndpoints": [{
                "port": PORT_NAME,
                "path": "/metrics",
                "interval": format!("{}s", SCRAPE_INTERVAL.as_secs()),
            }],
        },
    }))
    .expect("static PodMonitor manifest is valid");

    podmonitor_api(client, namespace)
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&obj),
        )
        .await
        .map(|_| ())
        .map_err(env_err)
}

// ──────────────────────────────── the rows ────────────────────────────────

/// How one metric family collapses to the single scalar a reader shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reduce {
    /// Total across every label set — for cumulative counters.
    Sum,
    /// The largest value — for a gauge reporting a view of something (a height,
    /// a lag) where summing would be meaningless.
    Max,
    /// Mean of a summary, in milliseconds: `Σ_sum / Σ_count × 1000`.
    ///
    /// Not a quantile: an exporter that sets no histogram buckets scrapes as a
    /// summary with no `_bucket{le}` to quantile over, and the summary quantiles
    /// it does expose cover only a short rolling window.
    MeanMs,
}

/// One metric a component publishes: the unit-bearing label a reader sees, the
/// exposed Prometheus family behind it, and how that family reduces to one
/// number.
///
/// Owned by the backend that publishes it, not by this module. A global table
/// here would render every component's labels on every run, including runs that
/// provisioned none of them.
#[derive(Debug, Clone, Copy)]
pub struct Row {
    /// Human-readable, unit-bearing label. The unit lives in the name
    /// (Prometheus convention) and the reduction yields the value already in
    /// that unit, so no renderer needs per-metric unit knowledge.
    pub label: &'static str,
    /// The exposed Prometheus family name — the exporter's own, after scrape.
    pub family: &'static str,
    pub reduce: Reduce,
    /// Whether a live reader shows it. The mid-run-meaningful subset: an instant
    /// read of a counter is a running total, not a rate.
    pub live: bool,
}

/// Shows on a live reader.
pub const LIVE: bool = true;
/// Meaningful as a whole-run figure, not as a mid-run instant.
pub const AT_REST: bool = false;

/// Row constructor, so a backend's table stays one line per metric and reads as
/// the table it is.
pub const fn row(label: &'static str, family: &'static str, reduce: Reduce, live: bool) -> Row {
    Row {
        label,
        family,
        reduce,
        live,
    }
}

// ───────────────────────────── the exposition ─────────────────────────────

/// Every scalar sample from an exporter, grouped by exposed family name.
///
/// Samples from all absorbed targets land in one bucket per family, so a
/// reduction spans them exactly as its PromQL counterpart spans a namespace.
#[derive(Debug, Default)]
pub struct Exposition {
    by_name: HashMap<String, Vec<f64>>,
}

impl Exposition {
    /// Fold one exporter's exposition text into the bucket set.
    ///
    /// Unparseable text is absorbed as nothing rather than reported: a scrape
    /// that arrives mid-write is a transient, and the caller's next read is a
    /// better answer than an error that would have to be distinguished from a
    /// component that is genuinely broken.
    pub fn absorb(&mut self, text: &str) {
        let lines = text.lines().map(|l| Ok(l.to_string()));
        let Ok(scrape) = prometheus_parse::Scrape::parse(lines) else {
            return;
        };
        for sample in scrape.samples {
            // Histogram/summary aggregates arrive as their own `_sum`/`_count`
            // families (the parser only folds `le`/`quantile` series into the
            // composite value), which is exactly what `MeanMs` reads — so the
            // composites themselves carry nothing scalar and are skipped.
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

    /// Apply one reduction. `None` when the family was absent — which is a real
    /// answer ("this component has published nothing of that kind yet"),
    /// distinct from a zero.
    pub fn reduce(&self, family: &str, reduce: Reduce) -> Option<f64> {
        match reduce {
            Reduce::Sum => Some(self.values(family)?.iter().sum()),
            Reduce::Max => self
                .values(family)?
                .iter()
                .copied()
                .fold(None, |acc: Option<f64>, v| {
                    Some(acc.map_or(v, |a| a.max(v)))
                }),
            Reduce::MeanMs => {
                let sum: f64 = self.values(&format!("{family}_sum"))?.iter().sum();
                let count: f64 = self.values(&format!("{family}_count"))?.iter().sum();
                // A summary with no observations divides zero by zero; there is
                // no mean latency yet, and NaN would render as one.
                (count > 0.0).then(|| sum / count * 1000.0)
            }
        }
    }

    /// A gauge's value as a whole number.
    ///
    /// Every gauge this reads is a block height, which is an integer the
    /// exporter widened to `f64` on the way out; narrowing it back at the read
    /// keeps that representation detail from leaking into probe arithmetic. A
    /// negative or non-finite reading is `None` rather than a wrapped `u32` — no
    /// height is either, so it is a broken exporter, not a low height.
    pub fn height_gauge(&self, family: &str) -> Option<u32> {
        let v = self.reduce(family, Reduce::Max)?;
        (v.is_finite() && v >= 0.0).then_some(v as u32)
    }

    /// A cumulative counter's total as a whole number. Carries
    /// [`height_gauge`](Self::height_gauge)'s narrowing contract.
    pub fn counter_total(&self, family: &str) -> Option<u64> {
        let v = self.reduce(family, Reduce::Sum)?;
        (v.is_finite() && v >= 0.0).then_some(v as u64)
    }
}

/// Scrape one exporter's `/metrics` and parse it.
///
/// `base` is the target's `http://host:port` root; the well-known `/metrics`
/// path is appended here so no caller has to restate it and none can spell it
/// differently.
///
/// `timeout` is the caller's, deliberately: an exporter is an in-memory registry
/// and a scrape is sub-millisecond in practice, so this bound is never a budget
/// — it is the point at which the caller declares the target wedged. A live
/// panel must beat its own refresh period; a probe must not outlive the tick
/// that asked for it. Those are different numbers for different reasons, and
/// neither is a default the other should inherit.
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

/// A component that can be scraped right now.
///
/// Two implementations, and the split is the point: a live handle inside a run
/// resolves its own address through the env, while [`PodExporter`] reaches a pod
/// from outside the cluster. Both feed the same [`Poller`], so a value a probe
/// gates on and a value a panel shows are produced by one reader.
///
/// A new ecosystem component joins the metrics plane by implementing this — two
/// methods — plus declaring the [`PORT_NAME`] port in its `pod_spec`. Nothing in
/// this module names a component.
#[async_trait::async_trait]
pub trait Exporter: Send + Sync + 'static {
    /// Where this component serves `/metrics`.
    ///
    /// Resolved per scrape rather than cached by the caller: a pod can be
    /// replaced mid-run, and a stale address would then be scraped until
    /// whatever holds it was restarted. Implementations that *can* cache (a
    /// forwarder, say) own that decision, because only they know when it is
    /// still valid.
    async fn endpoint(&self) -> Result<Endpoint, EnvError>;

    /// The families this component publishes and how each reduces.
    fn rows(&self) -> &'static [Row] {
        &[]
    }

    /// Scrape this component once, now.
    ///
    /// For a caller that needs a reading synchronously rather than a stream of
    /// them — a probe reading its own subject on the tick that asked for it. A
    /// live reader wants [`Poller`] instead, which holds its client across
    /// scrapes.
    async fn read(&self, timeout: Duration) -> Result<Exposition, String> {
        let endpoint = self.endpoint().await.map_err(|e| e.to_string())?;
        let http = reqwest::Client::new();
        scrape(&http, &endpoint.url("http"), timeout).await
    }
}

/// Reaches a component's exporter from **outside** the cluster: resolves a pod
/// by its backend-independent `ztest.io/component-category` label and forwards
/// to its [`PORT_NAME`] port.
///
/// Category rather than component name so a reader selects "the indexer" without
/// knowing whether the profile runs zainod or something else — the same
/// selection `ztest sync watch` already follows for logs.
///
/// The rows come from a caller-supplied resolver keyed on the pod's
/// `ztest.io/component` label, so this module still names no component: which
/// backend publishes which families is the backends' knowledge, and the
/// composition root wires the two together.
pub struct PodExporter {
    client: Client,
    namespace: String,
    category: String,
    rows_for: fn(&str) -> &'static [Row],
    target: Mutex<Option<Target>>,
    /// Latched at first resolve. `rows()` is sync (a renderer calls it per
    /// frame) so it cannot await the lock; the component behind a category does
    /// not change within a run, which is what makes latching correct.
    latched: std::sync::RwLock<&'static [Row]>,
}

/// The pod currently being scraped and the forwarder that reaches it.
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
            latched: std::sync::RwLock::new(&[]),
        }
    }

    /// The newest pod of this category that can serve a scrape, with its
    /// metrics port and component label.
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
        // Keep the forwarder while it still points at this pod: a fresh one per
        // scrape would rebind a local port every period and drop the connection
        // reuse that makes a 1 s cadence free.
        if !matches!(&*target, Some(t) if t.pod == pod) {
            let forwarder = Forwarder::start(
                self.client.clone(),
                self.namespace.clone(),
                pod.clone(),
                port,
            )
            .await
            .map_err(|e| EnvError::Config {
                reason: format!("portforward to {pod}:{port}: {e}"),
            })?;
            *target = Some(Target { pod, forwarder });
            *self.latched.write().expect("rows lock poisoned") = (self.rows_for)(&component);
        }
        let local = target
            .as_ref()
            .expect("target set immediately above")
            .forwarder
            .local_port;
        Ok(Endpoint {
            host: std::net::Ipv4Addr::LOCALHOST.into(),
            port: local,
        })
    }

    fn rows(&self) -> &'static [Row] {
        *self.latched.read().expect("rows lock poisoned")
    }
}

/// Whether a pod can serve a scrape: its exporter listens once the container is
/// running, and dialing a `Pending` or terminated pod only produces errors a
/// reader would show as a broken exporter.
fn ready_to_scrape(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .is_some_and(|phase| phase == "Running")
}

/// The pod's [`PORT_NAME`] container port, if it declares one.
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

/// Default live cadence. Fast enough that a counter climbing during a scan is
/// visibly climbing, which is the whole point of a live read; an exporter is an
/// in-memory registry, so this is not a load concern.
pub const LIVE_PERIOD: Duration = Duration::from_secs(1);

/// Per-scrape bound for a live poll, comfortably under [`LIVE_PERIOD`] so a
/// wedged target cannot stall the cadence.
const LIVE_TIMEOUT: Duration = Duration::from_millis(700);

/// One reading, and everything a reader needs to say why it holds what it does.
///
/// No status enum: the causes a reader must distinguish are all derivable —
/// never read (`at.is_none()`), unreachable (`error.is_some()`), reachable but
/// publishing none of the rows (every [`value`](Self::value) `None`). An enum
/// would be a second encoding of the same three facts, and the two would drift.
#[derive(Debug, Clone, Default)]
pub struct Sample {
    /// When this reading was taken. `None` before the first completed scrape.
    pub at: Option<Instant>,
    pub exposition: Arc<Exposition>,
    /// Why the last attempt produced nothing, when it failed.
    pub error: Option<String>,
}

impl Sample {
    /// One row's reduced value, or `None` when the family was absent.
    pub fn value(&self, row: &Row) -> Option<f64> {
        self.exposition.reduce(row.family, row.reduce)
    }

    /// Project this sample against `rows` into the live view of it.
    pub fn reading(&self, rows: &[Row]) -> Reading {
        let values: Vec<Value> = rows
            .iter()
            .filter(|r| r.live)
            .map(|r| Value {
                label: r.label,
                value: self.value(r),
            })
            .collect();
        // Why the values are missing is this plane's own knowledge — only the
        // reader knows whether it never resolved a target, could not reach one,
        // or reached one that publishes nothing. A renderer handed blank rows
        // would have to guess, and a caller asked to classify it would be
        // re-deriving what is already known here.
        let note = match (&self.error, self.at) {
            (None, None) => Some("no metrics-exposing pod yet".to_string()),
            (Some(why), None) => Some(format!("resolving target · {why}")),
            (Some(why), Some(_)) => Some(format!("unavailable · {why}")),
            (None, Some(_)) if values.iter().all(|v| v.value.is_none()) => Some(format!(
                "scraping every {}s · no series published yet",
                LIVE_PERIOD.as_secs()
            )),
            (None, Some(_)) => None,
        };
        Reading { values, note }
    }
}

/// One row's label and what it currently reads.
#[derive(Debug, Clone)]
pub struct Value {
    /// The unit-bearing label from the [`Row`] this came from.
    pub label: &'static str,
    pub value: Option<f64>,
}

/// A [`Sample`] projected against a component's rows: the live values, and why
/// they are missing when they are.
///
/// The unit a reader consumes. Carries no exposition and no transport detail, so
/// a renderer takes this and never learns how the reading was obtained.
#[derive(Debug, Clone, Default)]
pub struct Reading {
    /// The live rows, in table order.
    pub values: Vec<Value>,
    /// Why [`values`](Self::values) are empty, already human-readable. `None`
    /// when at least one value is present.
    pub note: Option<String>,
}

impl Reading {
    /// One value by its label.
    pub fn get(&self, label: &str) -> Option<f64> {
        self.values
            .iter()
            .find(|v| v.label == label)
            .and_then(|v| v.value)
    }
}

/// A running live scrape of one [`Exporter`].
///
/// Dropping it stops the scrape and releases whatever the exporter held to reach
/// its target — a reader that has walked away should not keep dialing.
pub struct Poller {
    rx: watch::Receiver<Sample>,
    task: tokio::task::JoinHandle<()>,
    exporter: Arc<dyn Exporter>,
}

impl std::fmt::Debug for Poller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Poller")
            .field("rows", &self.rows().len())
            .finish_non_exhaustive()
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Poller {
    /// Start polling `exporter` every `period`.
    pub fn spawn(exporter: impl Exporter, period: Duration) -> Poller {
        let exporter: Arc<dyn Exporter> = Arc::new(exporter);
        let (tx, rx) = watch::channel(Sample::default());
        let task = tokio::spawn(poll_loop(exporter.clone(), period, tx));
        Poller { rx, task, exporter }
    }

    /// What this poller is reading, so a caller can label the values it gets
    /// back. Held here rather than passed alongside: a reading and the rows that
    /// name it come from one exporter, and two parameters could disagree.
    pub fn rows(&self) -> &'static [Row] {
        self.exporter.rows()
    }

    /// Wait for the next reading — always the newest: a caller that falls behind
    /// skips stale readings rather than working through a backlog, which is what
    /// a live reader wants.
    ///
    /// Cancel-safe, so it can sit in a `select!` arm. Once the poll task is gone
    /// this never completes, deliberately: an arm that resolved instantly
    /// forever would spin its loop.
    pub async fn changed(&mut self) -> Reading {
        match self.rx.changed().await {
            Ok(()) => {
                let sample = self.rx.borrow_and_update().clone();
                sample.reading(self.exporter.rows())
            }
            Err(_) => std::future::pending().await,
        }
    }
}

async fn poll_loop(exporter: Arc<dyn Exporter>, period: Duration, tx: watch::Sender<Sample>) {
    let http = match reqwest::Client::builder().timeout(LIVE_TIMEOUT).build() {
        Ok(http) => http,
        Err(e) => {
            let _ = tx.send(Sample {
                error: Some(format!("no HTTP client: {e}")),
                ..Sample::default()
            });
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
        // `send` fails only once every reader is gone, which means whatever this
        // exists to feed is down.
        if tx.send(sample).is_err() {
            return;
        }
    }
}

/// A failed attempt keeps the last good exposition: one unreachable scrape must
/// not blank values a reader was truthfully shown a second ago, and the `error`
/// alongside them says the reading is no longer fresh.
fn failed(tx: &watch::Sender<Sample>, error: String) -> Sample {
    Sample {
        error: Some(error),
        ..tx.borrow().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A zaino-shaped exposition: a counter across two label sets, a gauge, and
    /// a summary in the form `metrics-exporter-prometheus` emits.
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
        assert_eq!(
            e.reduce("zaino_grpc_requests_total", Reduce::Sum),
            Some(17.0)
        );
    }

    #[test]
    fn a_gauge_reduces_by_max_across_targets() {
        let e = exposition(&[
            EXPOSITION,
            "# TYPE zaino_chain_tip_height gauge\nzaino_chain_tip_height 309\n",
        ]);
        assert_eq!(e.reduce("zaino_chain_tip_height", Reduce::Max), Some(309.0));
    }

    /// The reduction with arithmetic in it: `Σ_sum / Σ_count × 1000`.
    #[test]
    fn mean_latency_is_the_summarys_sum_over_its_count_in_milliseconds() {
        let e = exposition(&[EXPOSITION]);
        let ms = e
            .reduce("zaino_grpc_request_duration_seconds", Reduce::MeanMs)
            .expect("a mean");
        assert!((ms - 50.0).abs() < 1e-9, "0.85s / 17 = 50ms, got {ms}");
    }

    /// The distinction the whole reader rests on: a family nobody published
    /// reads as absent, never as a zero a probe would accept as an observation.
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

    /// Dialing a pod that has not started only produces connection errors, which
    /// would be shown as a broken exporter rather than a pod still coming up.
    #[test]
    fn a_pending_pod_is_not_scraped_yet() {
        let pending: Pod = serde_json::from_value(json!({
            "metadata": { "name": "zainod" },
            "status": { "phase": "Pending" },
        }))
        .expect("a pod");
        assert!(!ready_to_scrape(&pending));
    }

    /// The causes a reader must distinguish, each derived from the reading
    /// itself — the reason there is no status enum to keep in step with it, and
    /// the reason no consumer has to classify a blank column for itself.
    #[test]
    fn a_reading_states_why_it_has_no_values() {
        const ROWS: [Row; 1] = [row("tip", "zaino_chain_tip_height", Reduce::Max, LIVE)];
        let at = || Some(Instant::now());

        let never = Sample::default().reading(&ROWS);
        assert_eq!(never.note.as_deref(), Some("no metrics-exposing pod yet"));
        assert_eq!(never.get("tip"), None);

        let unresolved = Sample {
            error: Some("no ready indexer pod".into()),
            ..Sample::default()
        }
        .reading(&ROWS);
        assert!(
            unresolved
                .note
                .as_deref()
                .is_some_and(|n| n.starts_with("resolving target")),
            "{unresolved:?}"
        );

        let unreachable = Sample {
            at: at(),
            error: Some("connection refused".into()),
            ..Sample::default()
        }
        .reading(&ROWS);
        assert!(
            unreachable
                .note
                .as_deref()
                .is_some_and(|n| n.contains("connection refused")),
            "{unreachable:?}"
        );

        let silent = Sample {
            at: at(),
            exposition: Arc::new(exposition(&["# TYPE other gauge\nother 1\n"])),
            error: None,
        }
        .reading(&ROWS);
        assert!(
            silent
                .note
                .as_deref()
                .is_some_and(|n| n.contains("no series published yet")),
            "{silent:?}"
        );

        let sampled = Sample {
            at: at(),
            exposition: Arc::new(exposition(&[EXPOSITION])),
            error: None,
        }
        .reading(&ROWS);
        assert_eq!(sampled.note, None, "values present means no note");
        assert_eq!(sampled.get("tip"), Some(304.0));
    }

    /// A row the component does not publish is a row with no value, not a row
    /// that vanishes: the panel keeps its shape and shows the gap.
    #[test]
    fn an_unpublished_row_is_kept_with_no_value() {
        const ROWS: [Row; 2] = [
            row("tip", "zaino_chain_tip_height", Reduce::Max, LIVE),
            row("lag", "zaino_sync_lag_blocks", Reduce::Max, LIVE),
        ];
        let r = Sample {
            at: Some(Instant::now()),
            exposition: Arc::new(exposition(&[EXPOSITION])),
            error: None,
        }
        .reading(&ROWS);
        assert_eq!(r.values.len(), 2);
        assert_eq!(r.get("lag"), None);
    }

    /// Only the live subset reaches a live reader; the rest are whole-run
    /// figures that an instant read would misreport.
    #[test]
    fn a_reading_carries_only_the_live_rows() {
        const ROWS: [Row; 2] = [
            row("tip", "zaino_chain_tip_height", Reduce::Max, LIVE),
            row("errors", "zaino_grpc_errors_total", Reduce::Sum, AT_REST),
        ];
        let r = Sample::default().reading(&ROWS);
        assert_eq!(r.values.len(), 1);
        assert_eq!(r.values[0].label, "tip");
    }

    /// A scrape that fails after a good one keeps the numbers and adds the
    /// reason: blanking a column on one refused connection loses a reading that
    /// was true a second ago.
    #[test]
    fn a_failure_keeps_the_last_good_reading() {
        let (tx, _rx) = watch::channel(Sample {
            at: Some(Instant::now()),
            exposition: Arc::new(exposition(&[EXPOSITION])),
            error: None,
        });
        let after = failed(&tx, "connection refused".into());
        assert_eq!(after.error.as_deref(), Some("connection refused"));
        assert_eq!(
            after
                .exposition
                .reduce("zaino_chain_tip_height", Reduce::Max),
            Some(304.0)
        );
    }
}
