//! The **live** reader of the metrics plane: a direct scrape of each component's
//! exporter, at panel refresh rate.
//!
//! The other reader ([`super::query_run_metrics`]) goes through thanos, which is
//! right for the report — it is the durable, cluster-aggregated record — and
//! useless for a live display: user workload monitoring scrapes on a 15 s
//! interval, so no amount of querying it more often produces a fresher number.
//! Reading the exporter itself is the only way to get per-second resolution, and
//! it is also cheaper, since it skips the whole storage layer.
//!
//! This runs **controller-side**, in `ztest sync watch`, not in the driver: the
//! driver's only channel to a watcher is its pod log, and a 48-hour sync cannot
//! afford a log line per second — the kubelet would rotate away the run's early
//! history to carry numbers nobody is reading. Scraping from the side that is
//! actually displaying them costs nothing durable and stops when the watcher
//! detaches. Pod ports are reached with [`Forwarder`], the same
//! `pods/portforward` path every out-of-cluster run already uses, so this needs no
//! privilege ztest does not already hold.

use std::collections::HashMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use prometheus_parse::Value;
use tokio::sync::watch;

use super::{MetricSample, MetricsSummary, Reduce, live_metrics};
use crate::portforward::Forwarder;

/// How often the live panel's metrics are re-read. Fast enough that a counter
/// climbing during a scan is visibly climbing, which is the whole point of a live
/// display; the exporters are in-memory registries, so a scrape is a cheap local
/// read and this cadence is not a load concern.
pub const LIVE_SCRAPE_PERIOD: Duration = Duration::from_secs(1);

/// How often the target set is re-resolved. Components arrive over the course of
/// provisioning (the indexer minutes after the validator) and can be replaced, so
/// the target list cannot be resolved once at startup.
const DISCOVER_PERIOD: Duration = Duration::from_secs(5);

/// The container-port name a scrape target exposes `/metrics` on — the same name
/// the `PodMonitor` selects, so the live reader and UWM can never disagree about
/// which port is the metrics port.
const METRICS_PORT_NAME: &str = super::METRICS_PORT_NAME;

/// Per-scrape HTTP timeout. Comfortably under [`LIVE_SCRAPE_PERIOD`] so a wedged
/// target cannot stall the reading of the others behind it.
const SCRAPE_TIMEOUT: Duration = Duration::from_millis(700);

/// The newest live reading, as the panel needs it.
#[derive(Debug, Clone, Default)]
pub struct Reading {
    /// The reduced values, in [`super::METRICS`] order, live subset only.
    pub samples: MetricsSummary,
    /// Why [`samples`](Self::samples) holds what it does.
    pub state: State,
}

/// What the live reader is able to say right now. Distinguished because an empty
/// column has causes a reader must be able to tell apart: nothing to scrape yet is
/// normal for the first minutes of a run, whereas a target that refuses the scrape
/// is a broken exporter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum State {
    /// No pod in the namespace exposes a metrics port yet.
    #[default]
    NoTargets,
    /// Targets were scraped, but none of the live families were present — normal
    /// briefly at startup, and the symptom of a component that publishes no
    /// metrics after that.
    NoSeries,
    /// At least one value was read.
    Sampled,
    /// Every target failed, with the first reason.
    Failing(String),
}

/// A running live scrape. Dropping it stops the scrape and tears down the
/// forwarders — a watcher that has detached should not keep dialing the cluster.
#[derive(Debug)]
pub struct LiveMetrics {
    rx: watch::Receiver<Reading>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for LiveMetrics {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl LiveMetrics {
    /// Start scraping every metrics-exposing pod in `namespace`.
    pub fn spawn(client: kube::Client, namespace: String) -> LiveMetrics {
        let (tx, rx) = watch::channel(Reading::default());
        let task = tokio::spawn(scrape_loop(client, namespace, tx));
        LiveMetrics { rx, task }
    }

    /// Wait for the next published reading — one per [`LIVE_SCRAPE_PERIOD`], and
    /// always the newest: a caller that falls behind skips stale readings rather
    /// than working through a backlog, which is what a live display wants.
    ///
    /// Cancel-safe, so it can sit in a `select!` arm. Once the scrape task is gone
    /// this never completes, deliberately: an arm that resolves instantly forever
    /// would spin its loop.
    pub async fn changed(&mut self) -> Reading {
        match self.rx.changed().await {
            Ok(()) => self.rx.borrow_and_update().clone(),
            Err(_) => std::future::pending().await,
        }
    }
}

async fn scrape_loop(client: kube::Client, namespace: String, tx: watch::Sender<Reading>) {
    let api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let http = match reqwest::Client::builder().timeout(SCRAPE_TIMEOUT).build() {
        Ok(http) => http,
        Err(e) => {
            let _ = tx.send(Reading {
                samples: MetricsSummary::default(),
                state: State::Failing(format!("no HTTP client: {e}")),
            });
            return;
        }
    };

    let mut targets: HashMap<String, Target> = HashMap::new();
    let mut discover = tokio::time::interval(DISCOVER_PERIOD);
    let mut scrape = tokio::time::interval(LIVE_SCRAPE_PERIOD);
    scrape.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = discover.tick() => {
                reconcile_targets(&client, &api, &namespace, &mut targets).await;
            }
            _ = scrape.tick() => {
                let reading = read_all(&http, &targets).await;
                // `send` fails only once every watcher is gone, which means the
                // panel this exists to feed is down.
                if tx.send(reading).is_err() {
                    return;
                }
            }
        }
    }
}

/// One pod being scraped, and the forwarder that reaches it.
#[derive(Debug)]
struct Target {
    forwarder: Forwarder,
}

/// Bring `targets` in line with the namespace: add a forwarder for each new
/// metrics-exposing pod, drop the ones whose pods are gone.
///
/// Failure to start a forwarder is left for the next pass rather than reported: a
/// pod accepted by the API server is routinely not yet routable, and that is the
/// normal case here, not an error worth showing.
async fn reconcile_targets(
    client: &kube::Client,
    api: &Api<Pod>,
    namespace: &str,
    targets: &mut HashMap<String, Target>,
) {
    let Ok(pods) = api.list(&ListParams::default()).await else {
        return;
    };
    let found: Vec<(String, u16)> = pods
        .items
        .iter()
        .filter(|p| ready_to_scrape(p))
        .filter_map(|p| Some((p.metadata.name.clone()?, metrics_port(p)?)))
        .collect();

    targets.retain(|name, _| found.iter().any(|(n, _)| n == name));
    for (name, port) in found {
        if targets.contains_key(&name) {
            continue;
        }
        if let Ok(forwarder) =
            Forwarder::start(client.clone(), namespace.to_string(), name.clone(), port).await
        {
            tracing::debug!(pod = %name, port, "live metrics target added");
            targets.insert(name, Target { forwarder });
        }
    }
}

/// Whether a pod can serve a scrape: its exporter listens once the container is
/// running, and dialing a `Pending` or terminated pod only produces errors that
/// would be shown as a broken plane.
fn ready_to_scrape(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .is_some_and(|phase| phase == "Running")
}

/// The pod's `metrics` container port, if it declares one.
fn metrics_port(pod: &Pod) -> Option<u16> {
    pod.spec
        .as_ref()?
        .containers
        .iter()
        .flat_map(|c| c.ports.iter().flatten())
        .find(|p| p.name.as_deref() == Some(METRICS_PORT_NAME))
        .map(|p| p.container_port as u16)
}

/// Scrape every target and reduce the results into one reading.
async fn read_all(http: &reqwest::Client, targets: &HashMap<String, Target>) -> Reading {
    if targets.is_empty() {
        return Reading {
            samples: MetricsSummary::default(),
            state: State::NoTargets,
        };
    }

    // Concurrently, so the pass costs one scrape's latency rather than the sum:
    // serialized, a handful of targets each allowed [`SCRAPE_TIMEOUT`] would
    // overrun the period and the display would slip behind its own cadence.
    let reads = futures::future::join_all(targets.iter().map(|(name, target)| async move {
        (name, scrape_one(http, target.forwarder.local_port).await)
    }))
    .await;

    let mut families = Families::default();
    let mut failure = None;
    let mut scraped = 0usize;
    for (name, read) in reads {
        match read {
            Ok(text) => {
                families.absorb(&text);
                scraped += 1;
            }
            Err(e) => {
                tracing::debug!(pod = %name, error = %e, "live metrics scrape failed");
                failure.get_or_insert(e);
            }
        }
    }

    // A partial read is a reading: one unreachable pod must not blank values that
    // another is still reporting truthfully.
    if scraped == 0 {
        return Reading {
            samples: MetricsSummary::default(),
            state: State::Failing(failure.unwrap_or_else(|| "no target answered".into())),
        };
    }

    let samples = MetricsSummary {
        samples: live_metrics()
            .map(|def| MetricSample {
                name: def.label.to_string(),
                // The live reader evaluates the reduction directly; there is no
                // query, and inventing one to fill the field would misreport where
                // the value came from.
                query: String::new(),
                value: families.reduce(def.family, def.reduce),
            })
            .collect(),
    };
    let state = match samples.is_empty() {
        true => State::NoSeries,
        false => State::Sampled,
    };
    Reading { samples, state }
}

async fn scrape_one(http: &reqwest::Client, local_port: u16) -> Result<String, String> {
    http.get(format!("http://127.0.0.1:{local_port}/metrics"))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())
}

/// Every scalar sample read this pass, grouped by exposed family name.
///
/// Values from all targets land in one bucket per family so a reduction spans the
/// namespace exactly as its PromQL counterpart does — `sum(f{namespace=…})` over
/// two pods and `Sum` over both pods' samples are the same number by construction.
#[derive(Debug, Default)]
struct Families {
    by_name: HashMap<String, Vec<f64>>,
}

impl Families {
    /// Fold one exporter's exposition into the bucket set.
    fn absorb(&mut self, text: &str) {
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
                Value::Counter(v) | Value::Gauge(v) | Value::Untyped(v) => v,
                Value::Histogram(_) | Value::Summary(_) => continue,
            };
            self.by_name.entry(sample.metric).or_default().push(value);
        }
    }

    fn values(&self, family: &str) -> Option<&[f64]> {
        self.by_name.get(family).map(Vec::as_slice)
    }

    /// Apply one reduction. `None` when the family was absent — which is a real
    /// answer ("this component has published nothing of that kind yet"), distinct
    /// from a zero.
    fn reduce(&self, family: &str, reduce: Reduce) -> Option<f64> {
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
                // A summary with no observations divides zero by zero; there is no
                // mean latency yet, and NaN would render as one.
                (count > 0.0).then(|| sum / count * 1000.0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A zaino-shaped exposition: a counter across two label sets, a gauge, and a
    /// summary in the form `metrics-exporter-prometheus` emits.
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

    fn families(texts: &[&str]) -> Families {
        let mut f = Families::default();
        for t in texts {
            f.absorb(t);
        }
        f
    }

    #[test]
    fn a_counters_label_sets_sum_into_one_family_total() {
        let f = families(&[EXPOSITION]);
        assert_eq!(
            f.reduce("zaino_grpc_requests_total", Reduce::Sum),
            Some(17.0)
        );
    }

    #[test]
    fn a_gauge_reduces_by_max_across_pods() {
        let f = families(&[
            EXPOSITION,
            "# TYPE zaino_chain_tip_height gauge\nzaino_chain_tip_height 309\n",
        ]);
        assert_eq!(f.reduce("zaino_chain_tip_height", Reduce::Max), Some(309.0));
    }

    /// The reduction that has to match its PromQL exactly, because it is the one
    /// with arithmetic in it: `Σ_sum / Σ_count × 1000`.
    #[test]
    fn mean_latency_is_the_summarys_sum_over_its_count_in_milliseconds() {
        let f = families(&[EXPOSITION]);
        let ms = f
            .reduce("zaino_grpc_request_duration_seconds", Reduce::MeanMs)
            .expect("a mean");
        assert!((ms - 50.0).abs() < 1e-9, "0.85s / 17 = 50ms, got {ms}");
    }

    /// Absent is not zero: a family nobody has published must read as "no value",
    /// or the panel asserts a measurement that was never taken.
    #[test]
    fn an_absent_family_reduces_to_no_value() {
        let f = families(&[EXPOSITION]);
        assert_eq!(f.reduce("zaino_sync_reorg_total", Reduce::Sum), None);
        assert_eq!(f.reduce("zaino_nothing_here", Reduce::MeanMs), None);
    }

    /// A registered-but-unobserved summary divides 0 by 0; NaN would render as a
    /// latency, so it must read as no value.
    #[test]
    fn an_unobserved_summary_has_no_mean_rather_than_nan() {
        let f = families(&["# TYPE zaino_grpc_request_duration_seconds summary\n\
             zaino_grpc_request_duration_seconds_sum 0\n\
             zaino_grpc_request_duration_seconds_count 0\n"]);
        assert_eq!(
            f.reduce("zaino_grpc_request_duration_seconds", Reduce::MeanMs),
            None
        );
    }

    /// Garbage must not panic the watcher: an exporter mid-restart can serve a
    /// truncated body, and the reading simply has nothing in it.
    #[test]
    fn an_unparseable_body_yields_no_values() {
        let f = families(&["<html>502 Bad Gateway</html>"]);
        assert_eq!(f.reduce("zaino_grpc_requests_total", Reduce::Sum), None);
    }

    #[test]
    fn a_pod_without_a_metrics_port_is_not_a_target() {
        let with: Pod = serde_json::from_value(serde_json::json!({
            "metadata": { "name": "zainod" },
            "spec": { "containers": [{
                "name": "zainod",
                "ports": [{ "name": "grpc", "containerPort": 8137 },
                          { "name": METRICS_PORT_NAME, "containerPort": 9998 }],
            }] },
            "status": { "phase": "Running" },
        }))
        .expect("a pod");
        let without: Pod = serde_json::from_value(serde_json::json!({
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
    /// would be shown as a broken metrics plane rather than a pod still coming up.
    #[test]
    fn a_pending_pod_is_not_scraped_yet() {
        let pending: Pod = serde_json::from_value(serde_json::json!({
            "metadata": { "name": "zainod" },
            "status": { "phase": "Pending" },
        }))
        .expect("a pod");
        assert!(!ready_to_scrape(&pending));
    }
}
