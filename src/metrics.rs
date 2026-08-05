//! The server-side metrics plane (`docs/design-observability.md`).
//!
//! For each component whose image was built with a Prometheus-metrics feature
//! (`ImageSpec::metrics_enabled`), ztest emits a `PodMonitor` into the test
//! namespace. OpenShift User Workload Monitoring — enabled cluster-side by the
//! `ztest setup` monitoring provider — discovers `PodMonitor`s in user
//! namespaces and scrapes the component's `/metrics`, so the per-run series land
//! in the user-workload Prometheus, queryable through thanos-querier. The
//! per-run namespace is the natural query scope (one namespace per run), so no
//! run-id metric relabeling is needed to isolate a run's metrics.

use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::Client;
use kube::api::{Api, Patch, PatchParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use serde_json::json;

use crate::error::{EnvError, env_err};

/// The live reader — a direct scrape of the exporters, for the watch panel. The
/// thanos path in this module is the *report's* reader; see the module docs for why
/// a live display cannot use it.
pub mod live;

/// In-cluster thanos-querier endpoint (the UWM aggregation layer). Reachable on
/// the pod network from any test/runner pod; the run SA is granted read access
/// by the setup monitoring provider.
#[cfg_attr(not(feature = "zingo"), allow(dead_code))]
const THANOS_URL: &str = "https://thanos-querier.openshift-monitoring.svc:9091";
/// Standard in-pod ServiceAccount token path.
#[cfg_attr(not(feature = "zingo"), allow(dead_code))]
const SA_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
/// Per-namespace ConfigMap the OpenShift service-ca operator injects, carrying
/// the CA that signs thanos-querier's serving cert. Used to validate the TLS
/// connection so the SA token is never sent over an unverified channel.
#[cfg_attr(not(feature = "zingo"), allow(dead_code))]
const SERVICE_CA_CM: &str = "openshift-service-ca.crt";
#[cfg_attr(not(feature = "zingo"), allow(dead_code))]
const SERVICE_CA_KEY: &str = "service-ca.crt";

/// Field-manager for server-side applies of ztest-owned PodMonitors.
const FIELD_MANAGER: &str = "ztest";
/// The container port name both zaino and zebrad expose `/metrics` on
/// (`PodSpec` names it `"metrics"`; see `handles::ports`).
const METRICS_PORT_NAME: &str = "metrics";
/// UWM enforces a scrape-interval floor (~5–15 s); 15 s is fine for the
/// minutes-to-hours sync/testnet tests this plane serves.
///
/// This is the floor on how fresh anything read back *through thanos* can be, and
/// the reason the live panel does not read through thanos at all — see
/// [`live`](live::LIVE_SCRAPE_PERIOD).
const SCRAPE_INTERVAL: Duration = Duration::from_secs(15);

/// The Prometheus duration spelling of [`SCRAPE_INTERVAL`], which is what a
/// `PodMonitor` endpoint takes.
fn scrape_interval_spec() -> String {
    format!("{}s", SCRAPE_INTERVAL.as_secs())
}

fn podmonitor_api(client: &Client, namespace: &str) -> Api<DynamicObject> {
    let ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "monitoring.coreos.com",
        "v1",
        "PodMonitor",
    ));
    Api::namespaced_with(client.clone(), namespace, &ar)
}

/// Server-side-apply a `PodMonitor` selecting one component's pod by its
/// `ztest.io/component-name` label and scraping its `metrics` port. Idempotent
/// (re-applying identical content is a no-op); torn down with the run namespace.
///
/// A no-op-safe swallow of the missing-CRD case: on a cluster without the
/// Prometheus-operator CRDs (e.g. plain kind before its own Prometheus is
/// wired), emitting a PodMonitor is meaningless and must not fail a test — the
/// caller logs and continues.
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
                "port": METRICS_PORT_NAME,
                "path": "/metrics",
                "interval": scrape_interval_spec(),
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
/// One server-side metric read back for the report: a human label that carries
/// its own unit (`sync lag (blocks)`, `gRPC mean latency (ms)`), the PromQL that
/// produced it, and the scalar value (`None` if the series was absent — e.g. no
/// traffic of that kind yet, or the component didn't emit it). The unit lives in
/// the name (Prometheus convention) rather than a typed field, so the report DTO
/// stays a flat name/value pair; each query already yields the value in the
/// unit its name states.
#[derive(Debug, Clone)]
pub struct MetricSample {
    /// Human-readable, unit-bearing label for the report line.
    pub name: String,
    /// The PromQL query evaluated against thanos-querier.
    pub query: String,
    /// The scalar result, or `None` when the series was absent.
    pub value: Option<f64>,
}

/// A run's server-side metrics summary, read from thanos-querier and surfaced in
/// the [`SyncOutcome`](crate::sync::SyncOutcome). Empty when metrics were
/// unavailable (not in-cluster, UWM off, or thanos unreachable) — this plane is
/// an observability aid, never a gate.
#[derive(Debug, Clone, Default)]
pub struct MetricsSummary {
    /// The read-back samples, in query order.
    pub samples: Vec<MetricSample>,
}

impl MetricsSummary {
    /// Whether any series returned a value.
    pub fn is_empty(&self) -> bool {
        self.samples.iter().all(|s| s.value.is_none())
    }
}

/// An authenticated instant-query channel to thanos-querier.
///
/// Connecting is the expensive, failure-prone half (a ConfigMap read, a PEM
/// parse, a token read, a TLS client build); evaluating a query is cheap. They're
/// split so a live poller can connect once and query on a cadence instead of
/// repeating the handshake per sample.
#[cfg_attr(not(feature = "zingo"), allow(dead_code))]
pub(crate) struct Thanos {
    client: reqwest::Client,
    token: String,
}

#[cfg_attr(not(feature = "zingo"), allow(dead_code))]
impl Thanos {
    /// Establish the channel, or `None` if this plane is unavailable — not
    /// in-cluster, no service CA (a non-OpenShift cluster), or no TLS client.
    /// Never an error: metrics are an observability aid, never a gate.
    pub(crate) async fn connect(kube: &Client, namespace: &str) -> Option<Thanos> {
        // Establish trust in thanos's serving cert *before* sending the SA token —
        // a bearer credential must never ride an unverified TLS channel, internal
        // or not. The service CA is auto-injected per-namespace; if it's absent,
        // skip metrics rather than leak the token.
        let cms: Api<ConfigMap> = Api::namespaced(kube.clone(), namespace);
        let ca_pem = cms
            .get_opt(SERVICE_CA_CM)
            .await
            .ok()
            .flatten()
            .and_then(|cm| cm.data.and_then(|d| d.get(SERVICE_CA_KEY).cloned()))?;
        let ca_certs = reqwest::Certificate::from_pem_bundle(ca_pem.as_bytes()).ok()?;
        let token = std::fs::read_to_string(SA_TOKEN_PATH).ok()?.trim().to_string();
        // Full cert verification against the service CA (thanos-querier's cert has
        // a `thanos-querier.openshift-monitoring.svc` SAN, matching THANOS_URL).
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        for cert in ca_certs {
            builder = builder.add_root_certificate(cert);
        }
        Some(Thanos {
            client: builder.build().ok()?,
            token,
        })
    }

    /// Evaluate each `(label, query)` pair as one instant query, in order.
    pub(crate) async fn sample(&self, probes: impl IntoIterator<Item = (String, String)>) -> MetricsSummary {
        let mut samples = Vec::new();
        for (name, query) in probes {
            let value = query_scalar(&self.client, &self.token, &query).await;
            samples.push(MetricSample { name, query, value });
        }
        MetricsSummary { samples }
    }
}

/// Query thanos-querier for a run's key component metrics, scoped to the test
/// `namespace` (one namespace per run, so the namespace *is* the run filter).
///
/// Best-effort by contract: a missing in-pod token (not running in-cluster),
/// an unreachable thanos, or UWM being off all yield an empty summary rather
/// than an error — the caller attaches whatever came back.
#[cfg_attr(not(feature = "zingo"), allow(dead_code))]
pub(crate) async fn query_run_metrics(kube: &Client, namespace: &str) -> MetricsSummary {
    let Some(thanos) = Thanos::connect(kube, namespace).await else {
        return MetricsSummary::default();
    };
    thanos.sample(metric_queries(namespace)).await
}

/// How one metric family collapses to the single scalar a report line or panel
/// row shows.
///
/// Named as a reduction rather than written out as PromQL because there are now
/// two evaluators — thanos, which compiles it to a query, and the live scraper,
/// which applies it to a pod's raw exposition. A reduction expressed once cannot
/// disagree with itself; two hand-written implementations of the same aggregate
/// would drift the moment either is edited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reduce {
    /// Total across every emitting pod — for cumulative counters.
    Sum,
    /// The largest value across pods — for a gauge every pod reports its own view
    /// of (a height, a lag), where summing would be meaningless.
    Max,
    /// Whole-run mean of a summary, in milliseconds: `Σ_sum / Σ_count × 1000`.
    ///
    /// Not a quantile: zaino sets no histogram buckets, so its latency histograms
    /// scrape as summaries with no `_bucket{le}` to quantile over, and the summary
    /// quantiles it does expose cover only a ~60 s rolling window.
    MeanMs,
}

/// One metric this plane reads: the unit-bearing label a reader sees, the
/// Prometheus family behind it, and how that family reduces to one number.
///
/// Series names are zaino's own (`metric_names` modules) *after scrape*:
/// `metrics-exporter-prometheus` sanitizes the dotted source names to the
/// Prometheus charset, so `zaino.grpc.requests_total` is exposed as
/// `zaino_grpc_requests_total`. The unit lives in the label (Prometheus
/// convention) and the reduction yields the value already in that unit, so no
/// renderer needs per-metric unit knowledge.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MetricDef {
    /// Human-readable, unit-bearing label for the report line or panel row.
    pub(crate) label: &'static str,
    /// The exposed Prometheus family name.
    pub(crate) family: &'static str,
    pub(crate) reduce: Reduce,
    /// Whether the live panel shows it. The mid-run-meaningful subset: cumulative
    /// counters only where the panel prints them as running totals, since an
    /// instant read of a counter is a total and not a rate.
    pub(crate) live: bool,
}

/// Shows on the live watch panel.
const LIVE: bool = true;
/// Report-only — meaningful as a whole-run figure, not as a mid-run instant.
const REPORT_ONLY: bool = false;

/// Row constructor for [`METRICS`], so the table below stays one line per metric
/// and reads as the table it is.
const fn def(label: &'static str, family: &'static str, reduce: Reduce, live: bool) -> MetricDef {
    MetricDef {
        label,
        family,
        reduce,
        live,
    }
}

/// Every metric this plane reads, in report order — `(label, family, reduction,
/// visibility)`. The single definition both readers derive from.
///
/// Kept as a table on purpose: rustfmt's argument-list width would give each row
/// six lines, and eighty lines of vertical `def(` calls is unreadable for the one
/// thing a reader comes here to do — scan the columns.
#[rustfmt::skip]
pub(crate) const METRICS: [MetricDef; 13] = [
    // Inbound gRPC — zaino's serving surface.
    def("gRPC requests", "zaino_grpc_requests_total", Reduce::Sum, LIVE),
    def("gRPC errors", "zaino_grpc_errors_total", Reduce::Sum, REPORT_ONLY),
    def("gRPC mean latency (ms)", "zaino_grpc_request_duration_seconds", Reduce::MeanMs, LIVE),
    // Outbound JSON-RPC — zaino → validator.
    def("outbound RPC requests", "zaino_rpc_outbound_requests_total", Reduce::Sum, REPORT_ONLY),
    def("outbound RPC retries", "zaino_rpc_outbound_retries_total", Reduce::Sum, REPORT_ONLY),
    // Sync health.
    def("sync lag (blocks)", "zaino_sync_lag_blocks", Reduce::Max, LIVE),
    def("reorgs", "zaino_sync_reorg_total", Reduce::Sum, LIVE),
    def("sync errors", "zaino_sync_errors_total", Reduce::Sum, REPORT_ONLY),
    def("reached tip (1=yes)", "zaino_sync_has_reached_tip", Reduce::Max, REPORT_ONLY),
    // Indexed work — throughput evidence.
    def("transactions indexed", "zaino_sync_transactions_total", Reduce::Sum, LIVE),
    def("orchard actions", "zaino_sync_orchard_actions_total", Reduce::Sum, REPORT_ONLY),
    // Chain position.
    def("finalized height", "zaino_sync_finalized_height", Reduce::Max, REPORT_ONLY),
    def("chain tip height", "zaino_chain_tip_height", Reduce::Max, LIVE),
];

/// The live-panel subset of [`METRICS`], in report order.
pub(crate) fn live_metrics() -> impl Iterator<Item = &'static MetricDef> {
    METRICS.iter().filter(|m| m.live)
}

/// Compile every [`METRICS`] entry to `(label, PromQL)`, scoped to `namespace`
/// (one namespace per run, so the namespace *is* the run filter).
///
/// Each is one instant query reducing to a single scalar — no range queries. Only
/// cumulative counters and final gauges are read, both of which are meaningful as
/// an end-of-run instant.
#[cfg_attr(not(feature = "zingo"), allow(dead_code))]
fn metric_queries(namespace: &str) -> Vec<(String, String)> {
    let sel = format!("{{namespace=\"{namespace}\"}}");
    METRICS
        .iter()
        .map(|m| {
            let query = match m.reduce {
                Reduce::Sum => format!("sum({}{sel})", m.family),
                Reduce::Max => format!("max({}{sel})", m.family),
                Reduce::MeanMs => format!(
                    "sum({f}_sum{sel}) / sum({f}_count{sel}) * 1000",
                    f = m.family
                ),
            };
            (m.label.to_string(), query)
        })
        .collect()
}

/// Evaluate one instant PromQL query and pull the first vector/scalar value.
#[cfg_attr(not(feature = "zingo"), allow(dead_code))]
async fn query_scalar(client: &reqwest::Client, token: &str, query: &str) -> Option<f64> {
    let resp = client
        .get(format!("{THANOS_URL}/api/v1/query"))
        .bearer_auth(token)
        .query(&[("query", query)])
        .send()
        .await
        .ok()?;
    let body: serde_json::Value = resp.json().await.ok()?;
    // Instant-vector shape: data.result[0].value == [<ts>, "<value>"].
    body.get("data")?
        .get("result")?
        .as_array()?
        .first()?
        .get("value")?
        .as_array()?
        .get(1)?
        .as_str()?
        .parse::<f64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two readers of this plane must agree, so the PromQL a reduction
    /// compiles to has to be the reduction the live scraper applies — pinned here
    /// per variant, since a silent divergence would show two different numbers for
    /// one metric depending on which reader produced it.
    #[test]
    fn each_reduction_compiles_to_the_aggregate_it_names() {
        let by_label: std::collections::HashMap<_, _> = metric_queries("ns").into_iter().collect();
        for def in METRICS {
            let query = &by_label[def.label];
            match def.reduce {
                Reduce::Sum => {
                    assert_eq!(query, &format!("sum({}{{namespace=\"ns\"}})", def.family))
                }
                Reduce::Max => {
                    assert_eq!(query, &format!("max({}{{namespace=\"ns\"}})", def.family))
                }
                Reduce::MeanMs => assert_eq!(
                    query,
                    &format!(
                        "sum({f}_sum{{namespace=\"ns\"}}) / sum({f}_count{{namespace=\"ns\"}}) * 1000",
                        f = def.family
                    )
                ),
            }
        }
    }

    /// An empty live subset would leave the panel permanently blank with nothing
    /// broken to find.
    #[test]
    fn the_live_subset_is_not_empty_and_is_a_subset() {
        let live: Vec<_> = live_metrics().map(|m| m.label).collect();
        assert!(!live.is_empty());
        for label in live {
            assert!(METRICS.iter().any(|m| m.label == label));
        }
    }

    #[test]
    fn queries_are_scoped_to_the_run_namespace() {
        for (label, query) in metric_queries("ztest-sync-abc") {
            assert!(
                query.contains("namespace=\"ztest-sync-abc\""),
                "{label} is not namespace-scoped: {query}"
            );
        }
    }
}
