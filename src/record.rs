//! Reading the durable record back out of Prometheus.
//!
//! - Live plane ([`crate::metrics::Poller`]) dies with its components; this re-reads the
//!   same [`Row`]s from the TSDB after the pods and namespace are gone
//! - **Never load-bearing**: every failure → `None`, caller omits the section (a verdict
//!   here would hang on scrape cadence, retention, and an optional Deployment)

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kube::Client;
use serde::Deserialize;

use crate::metrics::{Reduce, Row};
use crate::portforward::Forwarder;

/// Range-query resolution, coarser than the 5 s scrape (a twelve-hour sync at scrape
/// resolution is ~8600 points to reduce to one number)
const STEP: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq)]
pub struct Recorded {
    pub label: &'static str,
    pub value: f64,
}

/// PromQL for a row, scoped to one run's namespace.
///
/// Mirrors [`Exposition::reduce`](crate::metrics::Exposition::reduce) server-side; one
/// enum for both keeps a metric from meaning one thing live and another in the report
fn promql(row: &Row, namespace: &str) -> String {
    let scope = format!("{{namespace=\"{namespace}\"}}");
    match row.reduce {
        Reduce::Sum => format!("sum({}{scope})", row.family),
        Reduce::Max => format!("max({}{scope})", row.family),
        // Σ_sum / Σ_count × 1000, guarded: an unobserved summary has count 0, and
        // PromQL divides to NaN rather than erroring → a plausible-looking `NaN ms`
        Reduce::MeanMs => format!(
            "sum({family}_sum{scope}) / clamp_min(sum({family}_count{scope}), 1) * 1000",
            family = row.family
        ),
    }
}

/// Query every row over `window`; `None` when the record is unreachable. Rows with
/// nothing recorded are omitted, not zeroed (never-published != published-zero)
pub async fn summarize(
    client: &Client,
    namespace: &str,
    rows: &[Row],
    window: (SystemTime, SystemTime),
) -> Option<Vec<Recorded>> {
    let reader = Reader::open(client).await.ok()?;
    let mut out = Vec::new();
    for row in rows {
        if let Ok(Some(value)) = reader.last_value(&promql(row, namespace), window).await {
            out.push(Recorded { label: row.label, value });
        }
    }
    (!out.is_empty()).then_some(out)
}

/// Port-forward to ztest's Prometheus, held open across a batch of queries
struct Reader {
    forwarder: Forwarder,
    http: reqwest::Client,
}

impl Reader {
    async fn open(client: &Client) -> Result<Reader, String> {
        let (namespace, pod, port) = prometheus_backend(client).await?;
        let forwarder = Forwarder::start(client.clone(), namespace, pod, port)
            .await
            .map_err(|e| format!("port-forward to Prometheus: {e}"))?;
        Ok(Reader { forwarder, http: reqwest::Client::new() })
    }

    /// Last sample of the reduced series over `window`. `query_range`, not an instant
    /// `query` (by report time the pods are gone → an instant read lands past the final
    /// scrape and returns nothing)
    async fn last_value(
        &self,
        query: &str,
        window: (SystemTime, SystemTime),
    ) -> Result<Option<f64>, String> {
        let url = format!("http://127.0.0.1:{}/api/v1/query_range", self.forwarder.local_port);
        let response = self
            .http
            .get(&url)
            .query(&[
                ("query", query),
                ("start", &epoch_secs(window.0).to_string()),
                ("end", &epoch_secs(window.1).to_string()),
                ("step", &STEP.as_secs().to_string()),
            ])
            .send()
            .await
            .map_err(|e| format!("querying Prometheus: {e}"))?;

        if !response.status().is_success() {
            return Err(format!("Prometheus returned {}", response.status()));
        }
        let body: RangeResponse =
            response.json().await.map_err(|e| format!("decoding Prometheus response: {e}"))?;
        Ok(body.last_value())
    }
}

/// Pod backing the Prometheus Service, via the Service's own selector — same reasoning
/// as [`profiling::pyroscope_backend`](crate::profiling)
async fn prometheus_backend(client: &Client) -> Result<(String, String, u16), String> {
    use k8s_openapi::api::core::v1::{Pod, Service};
    use kube::api::{Api, ListParams};

    let services: Api<Service> = Api::namespaced(client.clone(), crate::resource::OBS_NAMESPACE);
    let svc = services
        .get(crate::resource::PROMETHEUS_SERVICE)
        .await
        .map_err(|e| format!("no ztest Prometheus: {e}"))?;

    let port = svc
        .spec
        .as_ref()
        .and_then(|s| s.ports.as_ref())
        .and_then(|p| p.first())
        .map(|p| p.port as u16)
        .unwrap_or(crate::resource::PROMETHEUS_PORT);
    let selector = svc
        .spec
        .as_ref()
        .and_then(|s| s.selector.as_ref())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Prometheus Service selects no pods".to_string())?
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");

    let pods: Api<Pod> = Api::namespaced(client.clone(), crate::resource::OBS_NAMESPACE);
    pods.list(&ListParams::default().labels(&selector))
        .await
        .map_err(|e| format!("listing Prometheus pods: {e}"))?
        .items
        .into_iter()
        .find(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        })
        .and_then(|p| p.metadata.name)
        .map(|name| (crate::resource::OBS_NAMESPACE.to_string(), name, port))
        .ok_or_else(|| "no ready Prometheus pod".to_string())
}

// ── Response shape ────────────────────────────────────────────────────
//
// Only what a reduced series needs. Sample values arrive as JSON *strings* (preserving
// `NaN`/`Inf` and float precision) → `[timestamp, "value"]`, second element parsed

#[derive(Deserialize)]
struct RangeResponse {
    data: RangeData,
}

#[derive(Deserialize)]
struct RangeData {
    #[serde(default)]
    result: Vec<RangeSeries>,
}

#[derive(Deserialize)]
struct RangeSeries {
    #[serde(default)]
    values: Vec<(f64, String)>,
}

impl RangeResponse {
    /// Final sample of the first (after `sum`/`max`, only) series. Non-finite dropped —
    /// `NaN` is what an empty division yields, and printing it beats nothing only in noise
    fn last_value(&self) -> Option<f64> {
        self.data.result.first()?.values.last()?.1.parse::<f64>().ok().filter(|v| v.is_finite())
    }
}

fn epoch_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{AT_REST, row};

    #[test]
    fn a_sum_row_scopes_to_the_run_namespace() {
        let r = row("blocks", "zebrad_chain_verified_block_total", Reduce::Sum, AT_REST);
        assert_eq!(
            promql(&r, "ztest-sync-abc"),
            r#"sum(zebrad_chain_verified_block_total{namespace="ztest-sync-abc"})"#
        );
    }

    #[test]
    fn a_max_row_uses_max_not_sum() {
        let r = row("height", "zebrad_chain_verified_block_height", Reduce::Max, AT_REST);
        assert!(promql(&r, "ns").starts_with("max("));
    }

    /// Without the guard an un-observed summary divides by zero and reports `NaN ms`
    /// as though measured
    #[test]
    fn a_mean_row_guards_against_a_zero_count() {
        let r = row("latency ms", "zaino_grpc_duration", Reduce::MeanMs, AT_REST);
        let q = promql(&r, "ns");
        assert!(q.contains("clamp_min(sum(zaino_grpc_duration_count{namespace=\"ns\"}), 1)"));
        assert!(q.ends_with("* 1000"));
    }

    /// Values arrive as JSON strings; the *last* is the answer
    #[test]
    fn the_last_sample_of_a_series_is_the_reported_value() {
        let body: RangeResponse = serde_json::from_str(
            r#"{"status":"success","data":{"resultType":"matrix","result":[
                 {"metric":{},"values":[[1700000000,"1"],[1700000030,"42.5"]]}]}}"#,
        )
        .unwrap();
        assert_eq!(body.last_value(), Some(42.5));
    }

    #[test]
    fn an_empty_result_is_no_value_rather_than_zero() {
        let body: RangeResponse =
            serde_json::from_str(r#"{"status":"success","data":{"result":[]}}"#).unwrap();
        assert_eq!(body.last_value(), None);
    }

    #[test]
    fn a_nan_sample_is_dropped() {
        let body: RangeResponse = serde_json::from_str(
            r#"{"status":"success","data":{"result":[{"values":[[1700000000,"NaN"]]}]}}"#,
        )
        .unwrap();
        assert_eq!(body.last_value(), None);
    }
}
