//! Now plane: scrape an [`Exporter`] directly, on the reader's cadence.
//!
//! - Bypasses Prometheus by design — a panel refreshing on the scrape interval shows a
//!   number already stale by up to that interval
//! - Same [`Row`]/[`Exposition`] vocabulary as [`query`](super::query), so a figure
//!   cannot mean one thing here and another in the report
//! - Reduces across whatever targets it resolves, as its PromQL counterpart reduces
//!   across a namespace

use std::sync::Arc;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, ListParams};
use tokio::sync::{Mutex, watch};

use super::{Exposition, MetricKind, PORT_NAME, Row, scrape};
use crate::error::EnvError;
use crate::portforward::Forwarder;
use crate::protocol::Endpoint;

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

    /// One metric by its **exposition family name** (`zaino_db_tip_height`, …), read as
    /// `kind`. The caller names the wire family, so nothing here can drift from what the
    /// component publishes.
    ///
    /// Waits for the family to appear: a gauge exists only once first set, so a probe
    /// racing the first write sees nothing rather than a wrong value. Re-scrapes every
    /// `sample_rate` ([`DEFAULT_SAMPLE_RATE`] when `None`) until it does, giving up after
    /// [`METRIC_WAIT_BUDGET`] — a misspelled family would otherwise hang the test.
    async fn metric(
        &self,
        name: &str,
        kind: MetricKind,
        sample_rate: Option<Duration>,
    ) -> Result<f64, String> {
        let rate = sample_rate.unwrap_or(DEFAULT_SAMPLE_RATE);
        let deadline = tokio::time::Instant::now() + METRIC_WAIT_BUDGET;
        loop {
            if let Some(v) = self.read(SCRAPE_TIMEOUT).await?.read(name, kind) {
                return Ok(v);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "{name} ({kind:?}) absent from /metrics after {METRIC_WAIT_BUDGET:?}                      — wrong family name, or this build publishes no metrics"
                ));
            }
            tokio::time::sleep(rate).await;
        }
    }
}

/// `metric`'s re-scrape cadence when the caller names none
pub const DEFAULT_SAMPLE_RATE: Duration = Duration::from_secs(5);
/// How long `metric` waits for a family to appear before calling it absent. At the
/// default sample rate that is two scrapes — enough to clear a scrape landing between
/// two writes, not enough to hide a name that is simply wrong
const METRIC_WAIT_BUDGET: Duration = Duration::from_secs(5);
/// One `/metrics` HTTP read
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(5);

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

    use super::super::Reduce;
    use super::super::tests::{EXPOSITION, exposition};
    use super::*;

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
