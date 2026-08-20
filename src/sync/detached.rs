//! Detached-sync runtime + the vocabulary the stateless `ztest sync` controller
//! and the in-pod runner share (design §"Execution model: ztest-owned pods").
//!
//! - Driver pod runs the ordinary `#[ztest::sync_test]` body, detached, marked by
//!   [`SYNC_ID_ENV`]
//! - Body then watches [`STOP_ANNOTATION`] → engine cancel (checkpoint, not kill),
//!   and mirrors its report to a ConfigMap in [`OBS_NAMESPACE`](crate::naming::OBS_NAMESPACE),
//!   beside the metrics + profiles of the same run (one record, one lifetime)
//! - Every locator (labels, annotation, namespace, CM name) lives here, so the two
//!   sides cannot drift
//! - Compiled unconditionally; in-pod runtime runs only in the consumer's test binary

use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use serde::{Deserialize, Serialize};

/// Sync id into the detached pod; presence = the switch (set ⇒ wire the stop-watch
/// + report mirror, absent ⇒ ordinary engine / `cargo test` run)
pub const SYNC_ID_ENV: &str = "ZTEST_SYNC_ID";
/// Profile *name* (`#[sync_test(name = ..)]`), for the report
pub const SYNC_PROFILE_ENV: &str = "ZTEST_SYNC_PROFILE";
/// Downward-API pod name, so the in-pod runner watches itself without guessing
pub const POD_NAME_ENV: &str = "ZTEST_POD_NAME";
/// Downward-API pod namespace, other half of [`POD_NAME_ENV`]. Driver runs in the
/// *run* namespace, its topology in the *sync* one — stop-watch polls the former only
pub const POD_NAMESPACE_ENV: &str = "ZTEST_POD_NAMESPACE";

/// Marks a detached-sync driver pod ([`KIND_LABEL_VALUE`]); also on the per-sync
/// namespace + report CM. Ownership stays on
/// [`LABEL_USER`](crate::qos::LABEL_USER), as for every ztest resource
pub const KIND_LABEL_KEY: &str = "ztest.io/kind";
/// [`KIND_LABEL_KEY`] value a detached sync carries
pub const KIND_LABEL_VALUE: &str = "sync";
/// Per-sync id key, on the driver pod, its namespace and its report CM
pub const SYNC_ID_KEY: &str = "ztest.io/sync-id";
/// Controller → driver pod graceful-stop request. In-pod runner drives
/// `sync_mode = Shutdown` via engine cancel: checkpoint, not kill (design §stop)
pub const STOP_ANNOTATION: &str = "ztest.io/sync-stop";

/// `ztest.io/kind=sync` selector the controller lists by
pub fn kind_selector() -> String {
    format!("{KIND_LABEL_KEY}={KIND_LABEL_VALUE}")
}

/// Hermetic per-sync namespace, isolating component-pod names + `TestEnv`'s ResourceQuota.
///
/// - Holds the topology only — the report lives in [`report_cm_namespace`], so reclaiming
///   this namespace never costs a verdict
/// - Excludes the driver pod itself, see [`driver_pod_for`]
pub fn namespace_for(sync_id: &str) -> String {
    format!("ztest-sync-{sync_id}")
}

/// Driver pod name. Sync id must be in it — the driver lives in the *shared* run
/// namespace, so nothing cascades and `ztest cleanup` reaps it beside the namespace
pub fn driver_pod_for(sync_id: &str) -> String {
    format!("ztest-sync-{sync_id}")
}

/// Profiler ConfigMap for a sync — one name, derived twice (collector builds it, unwind deletes it)
pub fn profiler_config_name(sync_id: &str) -> String {
    format!("{}-profiler", driver_pod_for(sync_id))
}

/// Driver pod still running (neither `Succeeded` nor `Failed`); absent = run over, which is what
/// a host-placed collector's lifetime is pinned to (nothing resident survives to stop it)
pub async fn driver_is_live(client: &kube::Client, id: &str) -> bool {
    use k8s_openapi::api::core::v1::Pod;
    use kube::Api;
    let ns = crate::naming::RUN_NAMESPACE;
    let Ok(Some(pod)) =
        Api::<Pod>::namespaced(client.clone(), ns).get_opt(&driver_pod_for(id)).await
    else {
        return false;
    };
    !matches!(pod.status.and_then(|s| s.phase).as_deref(), Some("Succeeded") | Some("Failed"))
}

/// ConfigMap key the report JSON is stored under, read and written through this one name
pub const REPORT_KEY: &str = "report.json";

pub fn report_cm_name(sync_id: &str) -> String {
    format!("ztest-sync-report-{sync_id}")
}

/// Where a report is written and read. Cluster-lifetime, beside the Prometheus series and
/// Pyroscope profiles of the same run (reclaimed with them by `ztest cleanup`, never with
/// the sync's own namespace)
pub fn report_cm_namespace() -> &'static str {
    crate::naming::OBS_NAMESPACE
}

/// Lookups against a detached sync's cluster objects
#[derive(Debug, thiserror::Error)]
pub enum SyncLookupError {
    #[error("get pod {pod}: {source}")]
    GetPod {
        pod: String,
        #[source]
        source: kube::Error,
    },

    #[error("no sync `{0}`")]
    NoSync(String),

    #[error("read report: {0}")]
    ReadReport(#[source] kube::Error),

    #[error("parse report: {0}")]
    ParseReport(#[source] serde_json::Error),
}

/// Driver pod, by sync id. Reads the *run* namespace, never the sync's own — the driver
/// is a runner pod, and a sync-scoped lookup silently finds nothing
pub async fn find_driver(client: &kube::Client, sync_id: &str) -> Result<Pod, SyncLookupError> {
    let run_ns = crate::naming::RUN_NAMESPACE;
    let pod = driver_pod_for(sync_id);
    kube::Api::<Pod>::namespaced(client.clone(), run_ns)
        .get_opt(&pod)
        .await
        .map_err(|source| SyncLookupError::GetPod { pod, source })?
        .ok_or_else(|| SyncLookupError::NoSync(sync_id.to_string()))
}

/// Mirrored report, `None` while the run has yet to finish. Counterpart of
/// [`write_report`] — same namespace, same key, stated once
pub async fn read_report(
    client: &kube::Client,
    sync_id: &str,
) -> Result<Option<SyncReportMirror>, SyncLookupError> {
    let api: kube::Api<ConfigMap> = kube::Api::namespaced(client.clone(), report_cm_namespace());
    let Some(cm) =
        api.get_opt(&report_cm_name(sync_id)).await.map_err(SyncLookupError::ReadReport)?
    else {
        return Ok(None);
    };
    let Some(body) = cm.data.and_then(|d| d.get(REPORT_KEY).cloned()) else {
        return Ok(None);
    };
    serde_json::from_str(&body).map(Some).map_err(SyncLookupError::ParseReport)
}

/// Active sync id, `None` unless this process is a detached sync
pub fn active_sync_id() -> Option<String> {
    std::env::var(SYNC_ID_ENV).ok().filter(|s| !s.is_empty())
}

/// Publish a provisioning milestone; no-op off a detached sync. Not a
/// [`SyncReporter`](crate::sync::SyncReporter) hook — these minutes are spent
/// inside `TestEnv::build`, before any engine or reporter exists
pub fn note_setup(phase: &str, component: Option<&str>, detail: &str) {
    if active_sync_id().is_none() {
        return;
    }
    super::event::publish(&super::event::SyncEvent::Setup {
        phase: phase.to_string(),
        detail: detail.to_string(),
        component: component.map(str::to_string),
    });
}

/// Where a sync stands = the one answer `list`/`status`/`watch` all render.
///
/// - [`Self::observe`] = sole constructor (mirror outranks pod phase, outliving the pod)
/// - `Unresolved` = pod terminal/unreachable & no mirror → no verdict coming
/// - Chain progress ([`Phase`](crate::sync::Phase)) = the other axis, never this one (subject
///   at tip mid-probe = `Running`)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncStatus {
    Pending,
    Running,
    Finished(crate::sync::SyncVerdict),
    /// Also the `Default`: a status nothing was observed for renders loud, never green
    #[default]
    Unresolved,
}

impl SyncStatus {
    /// Pod phase as kubelet reports it (`None` = pod gone), mirror if one landed
    pub fn observe(pod_phase: Option<&str>, mirror: Option<&SyncReportMirror>) -> Self {
        if let Some(m) = mirror {
            return SyncStatus::Finished(m.verdict);
        }
        match pod_phase {
            Some("Pending") => SyncStatus::Pending,
            Some("Running") => SyncStatus::Running,
            _ => SyncStatus::Unresolved,
        }
    }

    /// No verdict yet, none due — guard on every "is this a failure?" branch
    pub fn is_live(&self) -> bool {
        matches!(self, SyncStatus::Pending | SyncStatus::Running)
    }

    /// Finished green (live / unresolved = not a pass)
    pub fn is_pass(&self) -> bool {
        matches!(self, SyncStatus::Finished(v) if v.is_pass())
    }
}

impl std::fmt::Display for SyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncStatus::Finished(v) => v.fmt(f),
            _ => std::fmt::Debug::fmt(self, f),
        }
    }
}

/// Durable projection of [`SyncOutcome`](crate::sync::SyncOutcome), stored in the
/// report ConfigMap so it survives the pod.
///
/// - Plain DTO (the live types carry handles and must not be `Serialize`)
/// - `segment` absent from a pre-segment driver → `perf --base` calls it incomparable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncReportMirror {
    pub sync_id: String,
    pub profile: String,
    pub verdict: crate::sync::SyncVerdict,
    pub ticks: u64,
    pub dropped_snapshots: u64,
    pub violations: Vec<ReportViolation>,
    pub coverage_gaps: Vec<String>,
    pub error: Option<String>,
    #[serde(default)]
    pub segment: Option<crate::sync::Segment>,
}

/// Recorded violation, projected for the durable report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportViolation {
    pub probe: String,
    pub height: Option<u32>,
    pub detail: String,
}

impl SyncReportMirror {
    pub fn from_outcome(sync_id: &str, profile: &str, outcome: &crate::sync::SyncOutcome) -> Self {
        SyncReportMirror {
            sync_id: sync_id.to_string(),
            profile: profile.to_string(),
            verdict: outcome.verdict,
            segment: outcome.segment.clone(),
            ticks: outcome.ticks,
            dropped_snapshots: outcome.dropped_snapshots,
            violations: outcome
                .violations
                .iter()
                .map(|v| ReportViolation {
                    probe: v.probe.clone(),
                    height: v.height,
                    detail: v.detail.clone(),
                })
                .collect(),
            coverage_gaps: outcome.coverage_gaps.clone(),
            error: outcome.error.clone(),
        }
    }

    pub fn passed(&self) -> bool {
        self.verdict.is_pass()
    }

    /// Compact human line, for `status`/`report` non-JSON output
    pub fn summary(&self) -> String {
        format!(
            "{} [{}]: {} — {} ticks, {} violations, {} coverage gaps",
            self.sync_id,
            self.profile,
            self.verdict,
            self.ticks,
            self.violations.len(),
            self.coverage_gaps.len(),
        )
    }
}

// ── in-pod runtime (only reachable inside the sync test binary) ──────────

pub use runtime::{watch_stop, write_report};

mod runtime {
    use std::time::Duration;

    use k8s_openapi::api::core::v1::{ConfigMap, Pod};
    use kube::Client;
    use kube::api::{Api, Patch, PatchParams};
    use serde_json::json;

    use crate::cancel::{Cancel, CancelSource};

    use super::{
        POD_NAME_ENV, POD_NAMESPACE_ENV, REPORT_KEY, STOP_ANNOTATION, SYNC_ID_KEY, SyncReportMirror,
    };

    /// Stop-watch re-read interval. Coarse — a poll beats a watch stream needing
    /// re-establishment across API hiccups, and a stop is not latency-critical
    const STOP_POLL: Duration = Duration::from_secs(5);

    /// Spawn the in-pod stop-watch → a [`Cancel`] the engine observes.
    ///
    /// - Fires on [`STOP_ANNOTATION`] or `SIGTERM`; both = checkpoint, not kill
    /// - Pod address from the downward API, not the caller (the driver is not in
    ///   the sync namespace its `TestEnv` points at)
    pub async fn watch_stop(client: &Client) -> Cancel {
        let (source, cancel) = CancelSource::new();
        let pod_name = std::env::var(POD_NAME_ENV).unwrap_or_default();
        let namespace = std::env::var(POD_NAMESPACE_ENV).unwrap_or_default();
        let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
        // Either half missing ⇒ unaddressable pod, annotation path dead, SIGTERM
        // only. Said loudly: a silent stop-watch makes `ztest sync stop` look like
        // it worked while the sync runs on for hours.
        let addressable = !pod_name.is_empty() && !namespace.is_empty();
        if !addressable {
            tracing::warn!(
                pod = %pod_name,
                namespace = %namespace,
                "sync stop-watch: incomplete pod address ({POD_NAME_ENV} / \
                 {POD_NAMESPACE_ENV}) — `ztest sync stop` cannot be observed; \
                 SIGTERM only"
            );
        }

        tokio::spawn(async move {
            let mut sigterm = match tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            ) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "sync stop-watch: cannot install SIGTERM handler");
                    poll_forever(&pods, &pod_name, addressable, &source).await;
                    return;
                }
            };
            let mut ticker = tokio::time::interval(STOP_POLL);
            loop {
                tokio::select! {
                    _ = sigterm.recv() => {
                        tracing::info!("sync stop-watch: SIGTERM → graceful shutdown");
                        source.cancel();
                        return;
                    }
                    _ = ticker.tick() => {
                        if !addressable {
                            continue;
                        }
                        if stop_requested(&pods, &pod_name).await {
                            tracing::info!("sync stop-watch: stop annotation set → graceful shutdown");
                            source.cancel();
                            return;
                        }
                    }
                }
            }
        });

        cancel
    }

    /// Annotation-only fallback, for when no SIGTERM handler could be installed
    async fn poll_forever(
        pods: &Api<Pod>,
        pod_name: &str,
        addressable: bool,
        source: &CancelSource,
    ) {
        if !addressable {
            return;
        }
        let mut ticker = tokio::time::interval(STOP_POLL);
        loop {
            ticker.tick().await;
            if stop_requested(pods, pod_name).await {
                source.cancel();
                return;
            }
        }
    }

    /// Truthy [`STOP_ANNOTATION`] on this pod? Read error = "not yet" (no stopping
    /// on a blip)
    async fn stop_requested(pods: &Api<Pod>, pod_name: &str) -> bool {
        match pods.get_opt(pod_name).await {
            Ok(Some(p)) => p
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(STOP_ANNOTATION))
                .map(|v| v == "true")
                .unwrap_or(false),
            _ => false,
        }
    }

    /// Mirror the final report to its ConfigMap (SSA, idempotent) in
    /// [`report_cm_namespace`](super::report_cm_namespace). Best-effort — the run already
    /// finished; a lost mirror costs only `report`
    pub async fn write_report(client: &Client, report: &SyncReportMirror) {
        let namespace = super::report_cm_namespace();
        let name = super::report_cm_name(&report.sync_id);
        let body = match serde_json::to_string_pretty(report) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "sync report: serialize failed");
                return;
            }
        };
        let cm: ConfigMap = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    super::KIND_LABEL_KEY: super::KIND_LABEL_VALUE,
                    SYNC_ID_KEY: report.sync_id,
                },
            },
            "data": { REPORT_KEY: body },
        }))
        .expect("static ConfigMap manifest is valid");
        let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        if let Err(e) =
            api.patch(&name, &PatchParams::apply("ztest-sync").force(), &Patch::Apply(&cm)).await
        {
            tracing::warn!(error = %e, cm = %name, "sync report: ConfigMap mirror failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::SyncVerdict;

    fn mirror(verdict: SyncVerdict) -> SyncReportMirror {
        SyncReportMirror {
            sync_id: "sync-1".into(),
            profile: "p".into(),
            verdict,
            ticks: 1,
            dropped_snapshots: 0,
            violations: Vec::new(),
            coverage_gaps: Vec::new(),
            error: None,
            segment: None,
        }
    }

    /// Disagreement `list` & `status` used to render: driver in teardown = `Running` to
    /// kubelet, verdict already durable
    #[test]
    fn a_mirrored_verdict_outranks_a_pod_still_running() {
        let status = SyncStatus::observe(Some("Running"), Some(&mirror(SyncVerdict::Passed)));
        assert_eq!(status, SyncStatus::Finished(SyncVerdict::Passed));
    }

    #[test]
    fn the_pod_answers_until_a_report_lands() {
        assert_eq!(SyncStatus::observe(Some("Pending"), None), SyncStatus::Pending);
        assert_eq!(SyncStatus::observe(Some("Running"), None), SyncStatus::Running);
    }

    /// Terminal pod + no mirror = verdict never coming (pass *or* fail would invent one)
    #[test]
    fn a_driver_gone_without_a_report_is_unresolved() {
        for phase in [Some("Succeeded"), Some("Failed"), Some("Unknown"), None] {
            let status = SyncStatus::observe(phase, None);
            assert_eq!(status, SyncStatus::Unresolved, "{phase:?}");
            assert!(!status.is_pass());
            assert!(!status.is_live());
        }
    }

    /// Word on the wire = word on screen, for every status
    #[test]
    fn a_status_renders_its_verdict_not_its_wrapper() {
        assert_eq!(SyncStatus::Finished(SyncVerdict::TimedOut).to_string(), "TimedOut");
        assert_eq!(SyncStatus::Running.to_string(), "Running");
        assert_eq!(SyncStatus::Unresolved.to_string(), "Unresolved");
    }

    /// Mirror JSON written by an older driver (verdict as a bare debug name) still reads
    #[test]
    fn a_report_round_trips_through_its_configmap_json() {
        let json = serde_json::to_string(&mirror(SyncVerdict::Failed)).expect("serialize");
        assert!(json.contains(r#""verdict":"Failed""#), "{json}");
        let back: SyncReportMirror = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.verdict, SyncVerdict::Failed);
        assert!(!back.passed());
    }
}
