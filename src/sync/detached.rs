//! Detached-sync runtime + the vocabulary the stateless `ztest sync` controller
//! and the in-pod runner share (design §"Execution model: ztest-owned pods").
//!
//! - Driver pod runs the ordinary `#[ztest::sync_test]` body, detached, marked by
//!   [`SYNC_ID_ENV`]
//! - Body then watches [`STOP_ANNOTATION`] → engine cancel (checkpoint, not kill),
//!   and mirrors its report to a ConfigMap in [`OBS_NAMESPACE`](crate::naming::OBS_NAMESPACE),
//!   beside the metrics + profiles of the same run (one record, one lifetime)
//! - Two writers, two keys, two field managers: controller stamps [`LAUNCH_KEY`] at start,
//!   driver [`REPORT_KEY`] at end — together self-contained, so no reader of a finished run
//!   touches a live object (namespace, pod) to interpret it
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

/// ConfigMap key the launch record sits under, beside [`REPORT_KEY`] in the same object.
///
/// - Separate key + separate field manager ([`LAUNCH_FIELD_MANAGER`]) → driver's apply of
///   the report cannot evict what the controller wrote, and neither needs read-modify-write
pub const LAUNCH_KEY: &str = "launch.json";

/// Server-side-apply manager owning [`LAUNCH_KEY`]. Distinct from the driver's, else one
/// apply drops the other's key (SSA prunes fields its own manager no longer sends)
pub const LAUNCH_FIELD_MANAGER: &str = "ztest-sync-launch";

/// Manager owning [`REPORT_KEY`]; see [`LAUNCH_FIELD_MANAGER`]
pub const REPORT_FIELD_MANAGER: &str = "ztest-sync-report";

/// Namespace TTL a sync is born with = 2x the tier's hard cap.
///
/// - Set at creation, never at completion: a driver OOM-killed, evicted or SIGKILLed past
///   its grace annotates nothing, and that is exactly the run that would leak
/// - Sized off the tier as the most generous bound ztest states anywhere; NOT a guarantee
///   the run fits inside it — `hard_cap` is enforced by the engine's runners, and a
///   detached driver goes through neither (its only clock is the profile's own `timeout`)
/// - Slack is why nothing breaks today: `ztest cleanup` reaps on driver phase, not on this,
///   and no janitor ships with ztest. Deploying one makes this load-bearing — a sync
///   outliving it would be reaped mid-run, so pair that with an enforced cap or a renewal
pub fn birth_ttl(profile: &crate::qos::QosProfile) -> std::time::Duration {
    profile.hard_cap * 2
}

/// `--no-cleanup` doubles the window rather than removing it (bounded, so nothing is
/// permanent; long enough that another dev notices and runs `ztest cleanup`)
pub fn held_ttl(profile: &crate::qos::QosProfile) -> std::time::Duration {
    birth_ttl(profile) * 2
}

/// Finished and unheld → already expired (TTL runs from creation, so anything under the
/// run's own age reaps on the next sweep)
pub const FINISHED_TTL: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// Wall clock → epoch millis: the one time encoding a durable sync record uses
pub fn epoch_millis(t: std::time::SystemTime) -> u64 {
    t.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

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

    #[error("record launch: {0}")]
    WriteLaunch(#[source] kube::Error),

    #[error("encode launch: {0}")]
    EncodeLaunch(#[source] serde_json::Error),
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

/// One key of a sync's record ConfigMap. `None` = no object, or the other writer's half
/// has yet to land
async fn read_record<T: serde::de::DeserializeOwned>(
    client: &kube::Client,
    sync_id: &str,
    key: &str,
) -> Result<Option<T>, SyncLookupError> {
    let api: kube::Api<ConfigMap> = kube::Api::namespaced(client.clone(), report_cm_namespace());
    let Some(cm) =
        api.get_opt(&report_cm_name(sync_id)).await.map_err(SyncLookupError::ReadReport)?
    else {
        return Ok(None);
    };
    let Some(body) = cm.data.and_then(|d| d.get(key).cloned()) else {
        return Ok(None);
    };
    serde_json::from_str(&body).map(Some).map_err(SyncLookupError::ParseReport)
}

/// Mirrored report, `None` while the run has yet to finish. Counterpart of
/// [`write_report`] — same namespace, same key, stated once
pub async fn read_report(
    client: &kube::Client,
    sync_id: &str,
) -> Result<Option<SyncReportMirror>, SyncLookupError> {
    read_record(client, sync_id, REPORT_KEY).await
}

/// Launch record, `None` for a sync started before it was written
pub async fn read_launch(
    client: &kube::Client,
    sync_id: &str,
) -> Result<Option<SyncLaunch>, SyncLookupError> {
    read_record(client, sync_id, LAUNCH_KEY).await
}

/// Stamp the launch record. Controller-side, before the driver can write its half.
///
/// - Apply under [`LAUNCH_FIELD_MANAGER`] → co-exists with the driver's [`REPORT_KEY`]
/// - Fatal to the caller: an unrecorded launch is a run whose profiles become
///   unreadable the moment its namespace goes
pub async fn write_launch(
    client: &kube::Client,
    launch: &SyncLaunch,
) -> Result<(), SyncLookupError> {
    use kube::api::{Patch, PatchParams};

    let name = report_cm_name(&launch.sync_id);
    let body = serde_json::to_string_pretty(launch).map_err(SyncLookupError::EncodeLaunch)?;
    let cm: ConfigMap = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": report_cm_namespace(),
            // Owner rides the record, not just the pod: `list` outlives both
            "labels": {
                KIND_LABEL_KEY: KIND_LABEL_VALUE,
                SYNC_ID_KEY: launch.sync_id,
                crate::qos::LABEL_USER: crate::naming::current_user(),
            },
        },
        "data": { LAUNCH_KEY: body },
    }))
    .expect("static ConfigMap manifest is valid");
    let api: kube::Api<ConfigMap> = kube::Api::namespaced(client.clone(), report_cm_namespace());
    api.patch(&name, &PatchParams::apply(LAUNCH_FIELD_MANAGER).force(), &Patch::Apply(&cm))
        .await
        .map_err(SyncLookupError::WriteLaunch)?;
    Ok(())
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

/// What the controller knows at launch and no later reader can rederive.
///
/// - Written by `ztest sync start`, never the driver (tenant + placement exist only there)
/// - Exists from t=0 → `perf` reads a mid-run, a finished and a *crashed* sync alike
/// - Holds no derivable value: namespace is [`namespace_for`], elapsed is the segment's
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncLaunch {
    pub sync_id: String,
    pub started_ms: u64,
    #[serde(default)]
    pub profiling: Option<LaunchProfiling>,
}

/// `None` on [`SyncLaunch::profiling`] = `--profile false`, or no Pyroscope to push to
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchProfiling {
    pub tenant: String,
    pub placement: crate::profiling::ebpf::Placement,
    pub hz: u32,
    pub off_cpu: f64,
}

impl SyncLaunch {
    pub fn started(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(self.started_ms)
    }
}

/// Durable projection of [`SyncOutcome`](crate::sync::SyncOutcome), stored in the
/// report ConfigMap so it survives the pod.
///
/// - Plain DTO (the live types carry handles and must not be `Serialize`)
/// - `segment` absent from a pre-segment driver → `perf --base` calls it incomparable
/// - `ended_ms` = engine finish, ahead of the pod's `terminated_at` (teardown deletes a
///   namespace in between); `None` from a pre-`ended_ms` driver
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
    #[serde(default)]
    pub ended_ms: Option<u64>,
    /// Recorded, not re-derived: a denominator read off a series makes one bad scrape a shortfall
    #[serde(default)]
    pub target: Option<u32>,
    #[serde(default)]
    pub unpublished: Vec<String>,
}

/// Recorded violation, projected for the durable report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportViolation {
    pub probe: String,
    pub height: Option<u32>,
    pub detail: String,
}

impl SyncReportMirror {
    pub fn ended(&self) -> Option<std::time::SystemTime> {
        Some(std::time::UNIX_EPOCH + std::time::Duration::from_millis(self.ended_ms?))
    }

    pub fn from_outcome(sync_id: &str, profile: &str, outcome: &crate::sync::SyncOutcome) -> Self {
        SyncReportMirror {
            sync_id: sync_id.to_string(),
            profile: profile.to_string(),
            verdict: outcome.verdict,
            segment: outcome.segment.clone(),
            target: outcome.target,
            unpublished: outcome.unpublished.clone(),
            ended_ms: Some(epoch_millis(std::time::SystemTime::now())),
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

pub use runtime::{mark_finished, watch_stop, write_report};

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

    /// Shorten the sync namespace's TTL to [`FINISHED_TTL`](super::FINISHED_TTL), so the
    /// reaper takes it on the next sweep.
    ///
    /// - Shortens a bound set at creation; never establishes one (see
    ///   [`birth_ttl`](super::birth_ttl))
    /// - No-op under `--no-cleanup`: that window is the point of the flag
    /// - Deletes nothing — driver owns no namespace, and the record it just wrote lives
    ///   in [`OBS_NAMESPACE`](crate::naming::OBS_NAMESPACE) either way
    /// - Best-effort: the birth TTL still bounds a namespace this fails to shorten
    pub async fn mark_finished(client: &Client, sync_id: &str) {
        if crate::cluster::no_cleanup_requested() {
            tracing::info!(sync_id, "--no-cleanup: sync namespace held for its full TTL");
            return;
        }
        let ns = super::namespace_for(sync_id);
        let ttl = crate::naming::ttl_value(super::FINISHED_TTL);
        let patch = json!({
            "metadata": { "annotations": { crate::naming::TTL_ANNOTATION: ttl } },
        });
        let api: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(client.clone());
        match api.patch(&ns, &PatchParams::default(), &Patch::Merge(&patch)).await {
            Ok(_) => tracing::info!(sync_id, ttl = %ttl, "sync namespace marked for reclaim"),
            Err(e) => tracing::warn!(error = %e, ns = %ns, "sync namespace TTL not shortened"),
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
        if let Err(e) = api
            .patch(
                &name,
                &PatchParams::apply(super::REPORT_FIELD_MANAGER).force(),
                &Patch::Apply(&cm),
            )
            .await
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
            ended_ms: None,
            segment: None,
            target: None,
            unpublished: Vec::new(),
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

    /// Every field `perf`/`status` need, present with the run namespace deleted
    #[test]
    fn a_launch_record_carries_the_tenant_and_the_origin() {
        let launch = SyncLaunch {
            sync_id: "sync-1".into(),
            started_ms: 1_700_000_000_000,
            profiling: Some(LaunchProfiling {
                tenant: "ztest.elicb.sync-1".into(),
                placement: crate::profiling::ebpf::Placement::Sidecar,
                hz: 19,
                off_cpu: 0.05,
            }),
        };
        let json = serde_json::to_string(&launch).expect("serialize");
        let back: SyncLaunch = serde_json::from_str(&json).expect("deserialize");
        let want = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_000);
        assert_eq!(back.started(), want);
        assert_eq!(back.profiling.expect("profiling").tenant, "ztest.elicb.sync-1");
    }

    /// Unprofiled run is a recorded fact, not an absent record — `perf` must distinguish
    /// "never collected" from "cannot tell"
    #[test]
    fn an_unprofiled_launch_records_that_it_was_unprofiled() {
        let json = r#"{"sync_id":"sync-1","started_ms":1}"#;
        let back: SyncLaunch = serde_json::from_str(json).expect("deserialize");
        assert!(back.profiling.is_none());
    }

    /// Reports written before `ended_ms` existed still read — the window falls back to the pod
    #[test]
    fn a_pre_ended_ms_report_still_deserialises() {
        let mut json: serde_json::Value =
            serde_json::to_value(mirror(SyncVerdict::Passed)).expect("serialize");
        json.as_object_mut().expect("object").remove("ended_ms");
        let back: SyncReportMirror = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back.ended(), None);
    }

    /// Widest bound ztest states, so a reaper honouring it clears any run that respects
    /// its tier (nothing enforces that on the detached path — see [`birth_ttl`])
    #[test]
    fn a_birth_ttl_outlives_the_tier_it_bounds() {
        for class in [crate::qos::QosClass::Sync, crate::qos::QosClass::Testnet] {
            let profile = class.profile();
            assert!(birth_ttl(&profile) > profile.hard_cap, "{class:?}");
            assert!(held_ttl(&profile) > birth_ttl(&profile), "{class:?}");
        }
    }

    /// `--no-cleanup` widens the window; it never removes the bound
    #[test]
    fn a_held_namespace_is_still_bounded() {
        let profile = crate::qos::QosClass::Sync.profile();
        assert_eq!(held_ttl(&profile), birth_ttl(&profile) * 2);
        assert_eq!(crate::naming::ttl_value(birth_ttl(&profile)), "96h");
        assert_eq!(crate::naming::ttl_value(held_ttl(&profile)), "192h");
    }

    /// TTL runs from creation, so a finished run's shortened value is already in the past
    #[test]
    fn a_finished_ttl_expires_immediately_on_any_real_run() {
        assert_eq!(crate::naming::ttl_value(FINISHED_TTL), "5m");
        assert!(FINISHED_TTL < crate::qos::QosClass::Sync.profile().hard_cap);
    }

    /// Both halves under one name, each under its own field manager — an apply of one
    /// must never prune the other
    #[test]
    fn the_two_writers_share_a_configmap_but_not_a_manager() {
        assert_ne!(LAUNCH_KEY, REPORT_KEY);
        assert_ne!(LAUNCH_FIELD_MANAGER, REPORT_FIELD_MANAGER);
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
