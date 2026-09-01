//! Reading the durable record back out of Prometheus.
//!
//! - Live plane ([`crate::metrics::Poller`]) dies with its components; this re-reads the
//!   same [`Row`]s from the TSDB after the pods and namespace are gone
//! - **Never load-bearing**: every failure → `None`, caller omits the section (a verdict
//!   here would hang on scrape cadence, retention, and an optional Deployment)
//! - [`purge`] is the one exception — it deletes, so it reports

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kube::Client;
use serde::Deserialize;

use crate::metrics::{Facet, Family, Reduce, Row, Unit};
use crate::portforward::Forwarder;

/// [`SCRAPE_CONFIG`](crate::resource::impls::observability)'s interval. A `step` under
/// it invents points, which Prometheus fills by repeating the last sample — a flat
/// stretch that never happened
pub const SCRAPE_INTERVAL: Duration = Duration::from_secs(5);

/// Container cpu, cores. Cumulative seconds-per-second → a rate
const CONTAINER_CPU: &str = "container_cpu_usage_seconds_total";
/// Resident set the kernel cannot reclaim under pressure = what OOM-kills a container
/// (`container_memory_usage_bytes` counts reclaimable page cache and reads high)
const CONTAINER_MEM: &str = "container_memory_working_set_bytes";

/// Kernel PSI, cgroup `io.pressure` `full`: seconds no task in the container could
/// progress for want of IO.
///
/// - The one off-CPU reading available here: a CPU profile samples on `ITIMER_PROF`, which
///   does not advance while a thread is blocked, so stalls are invisible to it by construction
/// - `full`, not `some`: one blocked thread among sixteen costs nothing, and `some`
///   reports it identically to a whole-container stall
/// - `some` is scraped too but read only from Grafana (brackets this from above:
///   exposure, not loss) — see `SCRAPE_CONFIG`
const CONTAINER_IO_STALL: &str = "container_pressure_io_stalled_seconds_total";

/// Device bytes charged to a cgroup, by `operation` (`Read`/`Write`) and device.
///
/// The only per-cgroup disk figure cAdvisor fills under cgroup v2 + containerd — the
/// whole `container_fs_*` family reads 0. Bytes only: `io.stat`'s `rios`/`wios` are
/// dropped upstream, so IOPS is not derivable from this exposition
const CONTAINER_DISK: &str = "container_blkio_device_usage_total";

/// PromQL for a row, scoped to one run's namespace.
///
/// Mirrors [`Exposition::reduce`](crate::metrics::Exposition::reduce) server-side; one
/// enum for both keeps a metric from meaning one thing live and another in the report
fn promql(row: &Row, namespace: &str, grid: Grid) -> String {
    let scope = scope_of(row.family, namespace);
    let family = row.family.name;
    let w = grid.rate_window.as_secs();
    match row.reduce {
        Reduce::Sum => format!("sum({family}{scope})"),
        Reduce::Max => format!("max({family}{scope})"),
        // Unguarded: unobserved → 0/0 → NaN → dropped as a gap, matching live's `None`
        // (a `clamp_min` would print a fabricated `0 ms`)
        Reduce::Mean => format!(
            "sum(rate({family}_sum{scope}[{w}s])) / sum(rate({family}_count{scope}[{w}s])) * 1000"
        ),
        // `rate` before the quantile, `le` kept through the sum (histogram_quantile needs one
        // series per bound; un-rated answers for all history)
        Reduce::Quantile(phi) => format!(
            "histogram_quantile({p}, sum by (le) (rate({family}_bucket{scope}[{w}s]))) * 1000",
            p = phi.value(),
        ),
    }
}

/// Run namespace + the row's own selector, so both planes narrow a split family alike
fn scope_of(family: Family, namespace: &str) -> String {
    match family.select {
        None => format!("{{namespace=\"{namespace}\"}}"),
        Some(s) => format!("{{namespace=\"{namespace}\",{}=\"{}\"}}", s.label, s.value),
    }
}

// ─────────────────────────────── history ───────────────────────────────

/// One metric over time, already in `unit`.
///
/// - Gaps are absent points, never interpolated (a partition's absence *is* the reading)
/// - `label` is dynamic for container series (the container's name), fixed for a [`Row`]
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub label: String,
    pub unit: Unit,
    pub facet: Option<Facet>,
    pub points: Vec<(f64, f64)>,
}

impl Series {
    pub fn peak(&self) -> Option<f64> {
        self.points
            .iter()
            .map(|(_, v)| *v)
            .fold(None, |a: Option<f64>, v| Some(a.map_or(v, |a| a.max(v))))
    }

    pub fn mean(&self) -> Option<f64> {
        match self.points.len() {
            0 => None,
            n => Some(self.points.iter().map(|(_, v)| v).sum::<f64>() / n as f64),
        }
    }

    pub fn last(&self) -> Option<f64> {
        self.points.last().map(|(_, v)| *v)
    }

    /// Area under a per-second series = the whole-run count it accumulated. `None`
    /// for anything not a rate (summing latencies yields a number with no meaning)
    pub fn integral(&self) -> Option<f64> {
        (self.unit == Unit::PerSec).then(|| {
            self.points.windows(2).map(|w| (w[1].0 - w[0].0) * (w[0].1 + w[1].1) / 2.0).sum()
        })
    }

    /// Pointwise sum of like-for-like series onto one label.
    ///
    /// - Timestamp intersection (part missing a scrape → total unknown, never smaller)
    /// - Unit & facet from the first part (caller folds like with like)
    pub fn folded(label: &str, parts: &[Series]) -> Option<Series> {
        let first = parts.first()?;
        // ms key: samples must compare exactly & f64 is not Ord
        let mut acc: BTreeMap<u64, (f64, usize)> = BTreeMap::new();
        for (at, v) in parts.iter().flat_map(|s| &s.points) {
            let slot = acc.entry((at * 1000.0).round() as u64).or_insert((0.0, 0));
            slot.0 += v;
            slot.1 += 1;
        }
        Some(Series {
            label: label.to_string(),
            unit: first.unit,
            facet: first.facet,
            points: acc
                .into_iter()
                .filter(|(_, (_, seen))| *seen == parts.len())
                .map(|(ms, (sum, _))| (ms as f64 / 1000.0, sum))
                .collect(),
        })
    }
}

/// Analysis resolution + the span counters are differenced over. Viewport-independent —
/// derived from terminal width, `rate_window` made every plotted peak a function of it.
///
/// - `step` floors at the scrape (finer → Prometheus repeats the last sample)
/// - `rate_window` ≥ `step` + scrape, always: a window narrower than the step samples
///   only part of each interval, and the rest reaches no plotted point
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Grid {
    pub step: Duration,
    pub rate_window: Duration,
}

const ANALYSIS_STEP: Duration = Duration::from_secs(30);

/// Payload ceiling per series (48 h at 30 s = 5,760)
const MAX_SLOTS: u32 = 6_000;

impl Grid {
    pub fn for_span(span: Duration) -> Grid {
        let step = ANALYSIS_STEP.max(span / MAX_SLOTS).max(SCRAPE_INTERVAL);
        Grid { step, rate_window: (step + SCRAPE_INTERVAL).max(SCRAPE_INTERVAL * 4) }
    }
}

/// As [`promql`], but for a plotted series: a cumulative family is differenced
/// server-side rather than shown climbing to its total.
///
/// [`Reduce`] carries which kind it is (`Sum` = counter, `Max` = gauge), and the two
/// take different differentiators
fn promql_series(row: &Row, namespace: &str, grid: Grid) -> String {
    let w = grid.rate_window.as_secs();
    let scope = scope_of(row.family, namespace);
    let family = row.family.name;
    match (row.unit, row.reduce) {
        // `rate` not `irate` (5 s scrape → instantaneous slope is mostly jitter);
        // `rate` before `sum` (summing first hides a restart from the reset correction)
        (Unit::PerSec, Reduce::Sum) => format!("sum(rate({family}{scope}[{w}s]))"),
        // Gauge slope, the TSDB counterpart of
        // [`Window::block_pace`](crate::sync::Window::block_pace).
        //
        // - `deriv` not `rate` (`rate` assumes counter semantics → reads a reorg
        //   rollback as a reset and inflates the slope)
        // - `clamp_min` at 0, as `block_pace` drops a retreating frontier rather than
        //   reporting a negative scan rate
        // - Subquery resolution = the scrape, never `step` (a resolution at or above the
        //   range yields one sample, and `deriv` needs two → the row vanishes silently)
        (Unit::PerSec, Reduce::Max) => format!(
            "clamp_min(deriv(max({family}{scope})[{w}s:{res}s]), 0)",
            res = SCRAPE_INTERVAL.as_secs(),
        ),
        _ => promql(row, namespace, grid),
    }
}

/// Every row as a series over `window`, at the analysis resolution.
///
/// Rows with nothing recorded are omitted rather than emitted empty (never-published
/// != published-zero)
pub async fn history(
    client: &Client,
    namespace: &str,
    rows: &[Row],
    window: (SystemTime, SystemTime),
) -> Option<Vec<Series>> {
    let reader = Reader::open(client).await.ok()?;
    let grid = Grid::for_span(span(window));
    let mut out = Vec::new();
    for row in rows {
        let query = promql_series(row, namespace, grid);
        let Ok(series) = reader.series(&query, window, grid.step).await else { continue };
        // `sum`/`max` collapse to one series; anything else here is a query bug
        let Some(first) = series.into_iter().next() else { continue };
        if first.points.is_empty() {
            continue;
        }
        out.push(Series {
            label: row.label.to_string(),
            unit: row.unit,
            facet: Some(row.facet),
            points: first.points,
        });
    }
    (!out.is_empty()).then_some(out)
}

/// What the kubelet saw of a run's containers, each split per container.
///
/// Readings of one cgroup, carried together because they are only meaningful side by
/// side: `cpu` spent, `mem` held, `disk` moved, `io_stall` lost waiting for it
#[derive(Debug, Clone, Default)]
pub struct ContainerHistory {
    pub cpu: Vec<Series>,
    pub mem: Vec<Series>,
    pub disk_read: Vec<Series>,
    pub disk_write: Vec<Series>,
    pub io_stall: Vec<Series>,
}

/// Per-container cpu, memory and IO stall over `window`, one [`Series`] per container.
///
/// Kubelet's reading, not a component's: no exporter can see its own cgroup. Split by
/// container because a namespace total hides which one grew
pub async fn container_history(
    client: &Client,
    namespace: &str,
    window: (SystemTime, SystemTime),
) -> Option<ContainerHistory> {
    let reader = Reader::open(client).await.ok()?;
    let grid = Grid::for_span(span(window));
    let scope = format!("{{namespace=\"{namespace}\",container!=\"\"}}");

    let cpu_q = format!(
        "sum by (container) (rate({CONTAINER_CPU}{scope}[{w}s]))",
        w = grid.rate_window.as_secs()
    );
    let mem_q = format!("sum by (container) ({CONTAINER_MEM}{scope})");
    // `max`, not `sum`: PSI is already a whole-cgroup ratio, and summing two containers'
    // stall fractions yields a share of time >1, which is not a quantity
    let stall_q = format!(
        "max by (container) (rate({CONTAINER_IO_STALL}{scope}[{w}s]))",
        w = grid.rate_window.as_secs()
    );

    let by_container = |labelled: Vec<Labelled>, unit: Unit| -> Vec<Series> {
        let mut rows: Vec<Series> = labelled
            .into_iter()
            .filter(|l| !l.points.is_empty())
            .map(|l| Series {
                label: l.labels.get("container").cloned().unwrap_or_default(),
                unit,
                facet: None,
                points: l.points,
            })
            .collect();
        // Largest first: the stack's base is the container that dominates it
        rows.sort_by(|a, b| {
            b.peak().unwrap_or(0.0).total_cmp(&a.peak().unwrap_or(0.0)).then(a.label.cmp(&b.label))
        });
        rows
    };

    let cpu = by_container(reader.series(&cpu_q, window, grid.step).await.ok()?, Unit::Cores);
    let mem = by_container(reader.series(&mem_q, window, grid.step).await.ok()?, Unit::Bytes);
    // Not `?`: PSI and blkio each need a kubelet that fills them, so a cluster without
    // either must still get cpu/mem rather than an absent resources panel
    let optional =
        async |query: &str, unit: Unit| match reader.series(query, window, grid.step).await {
            Ok(series) => by_container(series, unit),
            Err(_) => Vec::new(),
        };
    let io_stall = optional(&stall_q, Unit::Fraction).await;
    let disk_read = optional(&disk_q(namespace, "Read", grid), Unit::BytesPerSec).await;
    let disk_write = optional(&disk_q(namespace, "Write", grid), Unit::BytesPerSec).await;

    let history = ContainerHistory { cpu, mem, disk_read, disk_write, io_stall };
    let empty = history.cpu.is_empty()
        && history.mem.is_empty()
        && history.disk_read.is_empty()
        && history.disk_write.is_empty()
        && history.io_stall.is_empty();
    (!empty).then_some(history)
}

/// Device bytes `operation` moved on this cgroup's behalf, per second.
///
/// - `max`, never `sum`, over devices: a stacked block layer charges one write to every
///   layer, so TopoLVM's thin LV → tpool → tdata → partition reports it 4× (measured).
///   The layers are the same bytes, so the largest *is* the total
/// - Cost of that: a container writing two independent devices reports only the larger.
///   Under-reads a CSI driver, exact for a component holding one claim
/// - Per-cgroup, so a PVC shared by two pods still attributes — the charge follows the
///   task that moved the bytes, not the file it touched
/// - Device traffic only: a block left warm by the writing pod is a page-cache hit for
///   the reader and costs 0 here, which is the disk's truth, not the reader's
fn disk_q(namespace: &str, operation: &str, grid: Grid) -> String {
    format!(
        "max by (container) (rate({CONTAINER_DISK}\
         {{namespace=\"{namespace}\",container!=\"\",operation=\"{operation}\"}}[{w}s]))",
        w = grid.rate_window.as_secs()
    )
}

/// CPU-seconds the kernel charged `container` over `window`.
///
/// - Ground truth for a profile's own total: cgroup accounting counts every scheduled
///   nanosecond, where a sampling profiler counts only the signals it was delivered
/// - Sound because a component container runs one process — the cgroup total *is* the
///   process total, agent threads included
/// - `increase`, not a first/last subtraction: it handles the counter reset a restart
///   leaves behind
pub async fn container_cpu_seconds(
    client: &Client,
    namespace: &str,
    container: &str,
    window: (SystemTime, SystemTime),
) -> Option<f64> {
    let reader = Reader::open(client).await.ok()?;
    let span = span(window).as_secs().max(1);
    let query = format!(
        "sum(increase({CONTAINER_CPU}{{namespace=\"{namespace}\",container=\"{container}\"}}[{span}s]))"
    );
    let series = reader.series(&query, window, Duration::from_secs(span)).await.ok()?;
    // Last point: `increase` over the whole span only covers it at the window's end
    series.first()?.points.last().map(|(_, v)| *v)
}

fn span(window: (SystemTime, SystemTime)) -> Duration {
    window.1.duration_since(window.0).unwrap_or_default()
}

// ──────────────────────────────── purge ────────────────────────────────

/// Prometheus' own words when `--web.enable-admin-api` is absent, delivered as a `500`
const ADMIN_DISABLED: &str = "admin APIs disabled";

/// Failing API response. Prometheus reports the cause here, never in the status
#[derive(Debug, Deserialize)]
struct ApiError {
    error: String,
}

/// ztest's Prometheus exists at all.
///
/// Absent = a cluster set up with `--no-observability`, which records nothing — a
/// caller must not read that as a purge that failed
pub async fn is_deployed(client: &Client) -> bool {
    use k8s_openapi::api::core::v1::Service;
    use kube::api::Api;

    let api: Api<Service> = Api::namespaced(client.clone(), crate::naming::OBS_NAMESPACE);
    api.get_opt(crate::naming::PROMETHEUS_SERVICE).await.ok().flatten().is_some()
}

/// Drop every series matching `selectors`, permanently.
///
/// - Selectors are label matchers (`{sync_id="…"}`), *not* PromQL — the admin API takes
///   no functions and no aggregation
/// - No `start`/`end` = whole history (a partially-reclaimed sync is worse than either end)
/// - `delete_series` only writes tombstones; the bytes come back at `clean_tombstones`
/// - Returns `Err`, unlike the rest of this module: a silent failure here reads as
///   "reclaimed" while the data is still queryable
pub async fn purge(
    client: &Client,
    selectors: &[String],
) -> Result<(), crate::error::PipelineError> {
    if selectors.is_empty() {
        return Ok(());
    }
    let reader = Reader::open(client).await?;
    let matchers: Vec<(&str, &str)> = selectors.iter().map(|s| ("match[]", s.as_str())).collect();
    reader.admin("delete_series", &matchers).await?;
    reader.admin("clean_tombstones", &[]).await
}

/// Port-forward to ztest's Prometheus, held open across a batch of queries
struct Reader {
    forwarder: Forwarder,
    http: reqwest::Client,
}

impl Reader {
    async fn open(client: &Client) -> Result<Reader, crate::error::PipelineError> {
        let (namespace, pod, port) = prometheus_backend(client).await?;
        let forwarder = Forwarder::start(client.clone(), namespace, pod, port)
            .await
            .map_err(|e| format!("port-forward to Prometheus: {e}"))?;
        Ok(Reader { forwarder, http: reqwest::Client::new() })
    }

    /// Every sample of every series `query` matches, at `step` resolution.
    ///
    /// `query_range`, never an instant `query`: by report time the pods are gone, and
    /// an instant read lands past the final scrape and returns nothing
    async fn series(
        &self,
        query: &str,
        window: (SystemTime, SystemTime),
        step: Duration,
    ) -> Result<Vec<Labelled>, crate::error::PipelineError> {
        let body: RangeResponse = self.range(query, window, step).await?;
        Ok(body.data.result.into_iter().map(Labelled::from).collect())
    }

    /// One TSDB admin endpoint. Success is `204`, carrying no body
    async fn admin(
        &self,
        endpoint: &str,
        params: &[(&str, &str)],
    ) -> Result<(), crate::error::PipelineError> {
        let url =
            format!("http://127.0.0.1:{}/api/v1/admin/tsdb/{endpoint}", self.forwarder.local_port);
        let response = self
            .http
            .post(&url)
            .query(params)
            .send()
            .await
            .map_err(|e| format!("{endpoint}: {e}"))?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        // Status alone cannot diagnose this API: a disabled admin API is a `500` whose
        // body carries the only distinguishing text
        let body = response.text().await.unwrap_or_default();
        let detail = serde_json::from_str::<ApiError>(&body)
            .ok()
            .map(|e| e.error)
            .unwrap_or_else(|| body.trim().to_string());

        match detail.contains(ADMIN_DISABLED) {
            true => Err(format!("{endpoint}: prometheus admin API disabled").into()),
            false => Err(format!("{endpoint}: Prometheus returned {status}: {detail}").into()),
        }
    }

    async fn range(
        &self,
        query: &str,
        window: (SystemTime, SystemTime),
        step: Duration,
    ) -> Result<RangeResponse, crate::error::PipelineError> {
        let url = format!("http://127.0.0.1:{}/api/v1/query_range", self.forwarder.local_port);
        let response = self
            .http
            .get(&url)
            .query(&[
                ("query", query),
                ("start", &epoch_secs(window.0).to_string()),
                ("end", &epoch_secs(window.1).to_string()),
                ("step", &step.as_secs().max(1).to_string()),
            ])
            .send()
            .await
            .map_err(|e| format!("querying Prometheus: {e}"))?;

        if !response.status().is_success() {
            return Err(format!("Prometheus returned {}", response.status()).into());
        }
        response.json().await.map_err(|e| format!("decoding Prometheus response: {e}").into())
    }
}

/// One matrix series: its identifying labels, and its finite samples
#[derive(Debug, Clone, PartialEq)]
struct Labelled {
    labels: BTreeMap<String, String>,
    points: Vec<(f64, f64)>,
}

impl From<RangeSeries> for Labelled {
    /// Non-finite samples are dropped, not zeroed — `NaN` is what an empty division
    /// yields, and a zero there is a stall the run never had
    fn from(s: RangeSeries) -> Labelled {
        Labelled {
            labels: s.metric,
            points: s
                .values
                .into_iter()
                .filter_map(|(at, v)| Some((at, v.parse::<f64>().ok().filter(|v| v.is_finite())?)))
                .collect(),
        }
    }
}

/// Pod backing the Prometheus Service, via the Service's own selector — same reasoning
/// as [`profiling::pyroscope_backend`](crate::profiling)
async fn prometheus_backend(
    client: &Client,
) -> Result<(String, String, u16), crate::error::PipelineError> {
    use k8s_openapi::api::core::v1::{Pod, Service};
    use kube::api::{Api, ListParams};

    let services: Api<Service> = Api::namespaced(client.clone(), crate::naming::OBS_NAMESPACE);
    let svc = services
        .get(crate::naming::PROMETHEUS_SERVICE)
        .await
        .map_err(|e| format!("no ztest Prometheus: {e}"))?;

    let port = svc
        .spec
        .as_ref()
        .and_then(|s| s.ports.as_ref())
        .and_then(|p| p.first())
        .map(|p| p.port as u16)
        .unwrap_or(crate::ports::PROMETHEUS_PORT);
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

    let pods: Api<Pod> = Api::namespaced(client.clone(), crate::naming::OBS_NAMESPACE);
    pods.list(&ListParams::default().labels(&selector))
        .await
        .map_err(|e| crate::error::PipelineError(format!("list prometheus pods: {e}")))?
        .items
        .into_iter()
        .find(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        })
        .and_then(|p| p.metadata.name)
        .map(|name| (crate::naming::OBS_NAMESPACE.to_string(), name, port))
        .ok_or_else(|| "no ready prometheus pod".into())
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
    metric: BTreeMap<String, String>,
    #[serde(default)]
    values: Vec<(f64, String)>,
}

fn epoch_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{Facet, Unit, family, family_where, row};

    /// Verbatim from a live v3.13.2 with the flag absent. `500` + an `unavailable`
    /// errorType: neither the status nor a status→cause table can tell this from a
    /// genuine TSDB failure, so only the body may be trusted
    #[test]
    fn a_disabled_admin_api_is_diagnosed_from_the_body_not_the_status() {
        let body = r#"{"status":"error","errorType":"unavailable","error":"admin APIs disabled"}"#;
        let parsed: ApiError = serde_json::from_str(body).expect("parses");
        assert!(parsed.error.contains(ADMIN_DISABLED));
    }

    #[test]
    fn a_sum_row_scopes_to_the_run_namespace() {
        let r = row(
            "blocks",
            family("zebrad_chain_verified_block_total"),
            Reduce::Sum,
            Unit::Count,
            Facet::Progress,
        );
        assert_eq!(
            promql(&r, "ztest-sync-abc", grid()),
            r#"sum(zebrad_chain_verified_block_total{namespace="ztest-sync-abc"})"#
        );
    }

    /// A real grid, never a hand-built one: a query test on invented values asserts a
    /// shape production never emits
    fn grid() -> Grid {
        Grid::for_span(Duration::from_secs(600))
    }

    #[test]
    fn a_max_row_uses_max_not_sum() {
        let r = row(
            "height",
            family("zebrad_chain_verified_block_height"),
            Reduce::Max,
            Unit::Count,
            Facet::Progress,
        );
        assert!(promql(&r, "ns", grid()).starts_with("max("));
    }

    /// Lifetime `sum/count` converges, hiding late decay; a guard would print `0 ms` for
    /// a path that never ran, where live answers `None`
    #[test]
    fn a_mean_is_windowed_and_unguarded() {
        let r = row(
            "latency",
            family("zaino_grpc_duration"),
            Reduce::Mean,
            Unit::Millis,
            Facet::Throughput,
        );
        let q = promql(&r, "ns", grid());
        assert!(!q.contains("clamp_min"), "{q}");
        assert_eq!(
            q,
            r#"sum(rate(zaino_grpc_duration_sum{namespace="ns"}[35s])) / sum(rate(zaino_grpc_duration_count{namespace="ns"}[35s])) * 1000"#
        );
    }

    /// `histogram_quantile` needs one series per `le`, and the buckets must be rated
    /// before the quantile or it answers for all history rather than this window
    #[test]
    fn a_quantile_rates_the_buckets_and_keeps_le_through_the_sum() {
        let r = row(
            "fetch p99",
            family_where("zaino_sync_block_fetch_seconds", "stage", "finalised"),
            Reduce::Quantile(crate::metrics::Phi::P99),
            Unit::Millis,
            Facet::WritePath,
        );
        assert_eq!(
            promql(&r, "ns", grid()),
            r#"histogram_quantile(0.99, sum by (le) (rate(zaino_sync_block_fetch_seconds_bucket{namespace="ns",stage="finalised"}[35s]))) * 1000"#
        );
    }

    /// zaino splits every per-block family on `stage`; folding it counts one block
    /// once per ingest pass, so the selector must reach the query
    #[test]
    fn a_split_family_carries_its_selector_into_the_query() {
        let r = row(
            "orchard",
            family_where("zaino_sync_orchard_actions_total", "stage", "finalised"),
            Reduce::Sum,
            Unit::PerSec,
            Facet::Shielded,
        );
        assert_eq!(
            promql_series(&r, "ns", grid()),
            r#"sum(rate(zaino_sync_orchard_actions_total{namespace="ns",stage="finalised"}[35s]))"#
        );
    }

    /// A counter plotted raw climbs to its total and says nothing about throughput;
    /// the rate is the reading, and it must be taken server-side
    #[test]
    fn a_cumulative_row_is_differentiated_for_a_plot_but_not_for_a_total() {
        let r = row(
            "orchard",
            family("zaino_sync_orchard_actions_total"),
            Reduce::Sum,
            Unit::PerSec,
            Facet::Shielded,
        );
        assert_eq!(
            promql_series(&r, "ns", grid()),
            r#"sum(rate(zaino_sync_orchard_actions_total{namespace="ns"}[35s]))"#
        );
        assert_eq!(
            promql(&r, "ns", grid()),
            r#"sum(zaino_sync_orchard_actions_total{namespace="ns"})"#
        );
    }

    /// Zaino counts no blocks; the scan rate is the frontier gauge's slope. `rate`
    /// here would read a reorg rollback as a counter reset and inflate it, and the
    /// negative arm of a real rollback is no scan rate at all
    #[test]
    fn a_per_second_gauge_row_takes_a_clamped_derivative_not_a_rate() {
        let r = row(
            "blocks",
            family("zaino_sync_fetched_height"),
            Reduce::Max,
            Unit::PerSec,
            Facet::Blocks,
        );
        assert_eq!(
            promql_series(&r, "ns", grid()),
            r#"clamp_min(deriv(max(zaino_sync_fetched_height{namespace="ns"})[35s:5s]), 0)"#
        );
    }

    /// Subquery resolution is the scrape, so the range holds samples however coarse the
    /// display step gets. At `step` it held one, and `deriv` under two samples returns
    /// nothing — the scan-rate row vanished with no gap to show for it
    #[test]
    fn a_gauge_slope_keeps_enough_samples_to_have_a_slope() {
        let r = row(
            "blocks",
            family("zaino_sync_fetched_height"),
            Reduce::Max,
            Unit::PerSec,
            Facet::Blocks,
        );
        for secs in [60, 600, 7_200, 172_800, 30 * 24 * 3_600] {
            let grid = Grid::for_span(Duration::from_secs(secs));
            let q = promql_series(&r, "ns", grid);
            let (range, res) = subquery_of(&q);
            assert!(range / res >= 2, "{secs}s span: {q} carries no slope");
        }
    }

    /// `[<range>:<resolution>]` from a rendered subquery, in seconds
    fn subquery_of(query: &str) -> (u64, u64) {
        let inner = query.rsplit_once('[').expect("a subquery").1;
        let (range, rest) = inner.split_once(':').expect("a resolution");
        let res = rest.split_once("s]").expect("a close").0;
        (range.trim_end_matches('s').parse().unwrap(), res.parse().unwrap())
    }

    /// Everything else already *is* the quantity; wrapping it in `rate` would report
    /// the speed a latency was changing
    #[test]
    fn a_gauge_row_is_plotted_as_published() {
        let r =
            row("db used", family("zaino_db_used_bytes"), Reduce::Max, Unit::Bytes, Facet::Store);
        let grid = Grid::for_span(Duration::from_secs(600));
        assert_eq!(promql_series(&r, "ns", grid), promql(&r, "ns", grid));
    }

    /// Smoothing was `4 × step`, `step` from terminal width → one run at 80 and 200 columns
    /// attenuated its peaks differently, neither plot saying so
    #[test]
    fn the_rate_window_is_the_same_however_long_the_run() {
        let baseline = Grid::for_span(Duration::from_secs(60)).rate_window;
        for secs in [60, 600, 7_200, 172_800] {
            let grid = Grid::for_span(Duration::from_secs(secs));
            assert_eq!(grid.rate_window, baseline, "{secs}s span must smooth like every other");
            assert!(grid.step >= SCRAPE_INTERVAL, "never finer than the scrape: {grid:?}");
        }
    }

    /// Grafana's `$__rate_interval`: a window under the step leaves every interval partly
    /// unread, and nothing in the plot says which third of the run is missing
    #[test]
    fn a_rate_window_always_covers_its_step() {
        for secs in [60, 600, 7_200, 172_800, 30 * 24 * 3_600] {
            let grid = Grid::for_span(Duration::from_secs(secs));
            assert!(
                grid.rate_window >= grid.step + SCRAPE_INTERVAL,
                "{secs}s span samples only {:?} of each {:?}",
                grid.rate_window,
                grid.step
            );
            assert!(grid.rate_window >= SCRAPE_INTERVAL * 4, "{grid:?}");
        }
    }

    /// Finer → Prometheus repeats the last sample; coarser only caps a long run's payload
    #[test]
    fn the_step_holds_at_the_analysis_resolution_until_a_run_is_very_long() {
        assert_eq!(Grid::for_span(Duration::from_secs(60)).step, ANALYSIS_STEP);
        assert_eq!(Grid::for_span(Duration::from_secs(7_200)).step, ANALYSIS_STEP);
        let long = Grid::for_span(Duration::from_secs(48 * 3_600));
        assert!(long.step >= ANALYSIS_STEP, "{long:?}");
        assert!(
            48 * 3_600 / long.step.as_secs() <= u64::from(MAX_SLOTS),
            "a 48h run must stay under the payload cap: {long:?}"
        );
    }

    /// `sum by (container)` returns one series per container, and which one it is
    /// lives in `metric`, not in the values
    #[test]
    fn a_grouped_query_keeps_the_label_that_names_each_series() {
        let body: RangeResponse = serde_json::from_str(
            r#"{"data":{"result":[
                 {"metric":{"container":"zainod"},"values":[[0,"1.5"],[30,"2.5"]]},
                 {"metric":{"container":"zebrad"},"values":[[0,"0.25"]]}]}}"#,
        )
        .unwrap();
        let series: Vec<Labelled> = body.data.result.into_iter().map(Labelled::from).collect();
        assert_eq!(series[0].labels.get("container").map(String::as_str), Some("zainod"));
        assert_eq!(series[0].points, vec![(0.0, 1.5), (30.0, 2.5)]);
        assert_eq!(series[1].points.len(), 1);
    }

    /// A part missing a scrape leaves the total unknown; carrying the survivors alone
    /// would draw a dip the run never had
    #[test]
    fn a_fold_sums_where_every_part_sampled_and_gaps_elsewhere() {
        let part = |label: &str, points: Vec<(f64, f64)>| Series {
            label: label.into(),
            unit: Unit::PerSec,
            facet: Some(Facet::Shielded),
            points,
        };
        let folded = Series::folded(
            "sapling",
            &[
                part("sapling spends", vec![(0.0, 1.0), (30.0, 2.0), (60.0, 4.0)]),
                part("sapling outputs", vec![(0.0, 10.0), (60.0, 40.0)]),
            ],
        )
        .expect("two parts fold");

        assert_eq!(folded.label, "sapling");
        assert_eq!(folded.unit, Unit::PerSec);
        assert_eq!(folded.facet, Some(Facet::Shielded));
        assert_eq!(folded.points, vec![(0.0, 11.0), (60.0, 44.0)]);
    }

    #[test]
    fn folding_nothing_yields_no_series() {
        assert_eq!(Series::folded("sapling", &[]), None);
    }

    /// A dropped sample is a gap, not a zero — a stall the run never had
    #[test]
    fn a_non_finite_sample_leaves_a_gap_rather_than_a_zero() {
        let body: RangeResponse = serde_json::from_str(
            r#"{"data":{"result":[{"metric":{},"values":[[0,"1"],[30,"NaN"],[60,"3"]]}]}}"#,
        )
        .unwrap();
        let s = Labelled::from(body.data.result.into_iter().next().unwrap());
        assert_eq!(s.points, vec![(0.0, 1.0), (60.0, 3.0)]);
    }

    /// Area under a rate = the count it accumulated, which is what the legend shows
    /// beside each pool
    #[test]
    fn integrating_a_rate_recovers_its_total() {
        let rate = |points: Vec<(f64, f64)>| Series {
            label: "orchard".into(),
            unit: Unit::PerSec,
            facet: None,
            points,
        };
        // 10/s held across 60s
        let steady = rate(vec![(0.0, 10.0), (30.0, 10.0), (60.0, 10.0)]);
        assert_eq!(steady.integral(), Some(600.0));
        assert_eq!(steady.mean(), Some(10.0));
        assert_eq!(steady.peak(), Some(10.0));

        // Trapezoid, not a left-hand sum: 0→10 over 60s is 300, not 0
        assert_eq!(rate(vec![(0.0, 0.0), (60.0, 10.0)]).integral(), Some(300.0));
    }

    /// Summing latencies yields a number with no meaning; only a rate has an integral
    #[test]
    fn a_latency_series_has_no_total() {
        let latency = Series {
            label: "batch write".into(),
            unit: Unit::Millis,
            facet: None,
            points: vec![(0.0, 12.0), (30.0, 900.0)],
        };
        assert_eq!(latency.integral(), None);
        assert_eq!(latency.peak(), Some(900.0));
    }

    #[test]
    fn an_empty_series_reports_no_statistics() {
        let empty = Series { label: "x".into(), unit: Unit::PerSec, facet: None, points: vec![] };
        assert_eq!(empty.mean(), None);
        assert_eq!(empty.peak(), None);
        assert_eq!(empty.last(), None);
    }
}
