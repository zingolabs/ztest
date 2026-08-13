//! Detached-sync runtime + the vocabulary the stateless `ztest sync` controller
//! and the in-pod runner share (design §"Execution model: ztest-owned pods").
//!
//! - Driver pod runs the ordinary `#[ztest::sync_test]` body, detached, marked by
//!   [`SYNC_ID_ENV`]
//! - Body then watches [`STOP_ANNOTATION`] → engine cancel (checkpoint, not kill),
//!   and mirrors its report to a ConfigMap outliving the pod
//! - Every locator (labels, annotation, namespace, CM name) lives here, so the two
//!   sides cannot drift
//! - Compiled unconditionally; the in-pod runtime runs only in the `zingo` binary

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

/// Hermetic per-sync namespace, isolating component-pod names + `TestEnv`'s
/// ResourceQuota.
///
/// - Outlives the driver pod so `report`/`status` still answer; `ztest cleanup`
///   deletes it once that pod stops Running
/// - Excludes the driver pod itself, see [`driver_pod_for`]
pub fn namespace_for(sync_id: &str) -> String {
    format!("ztest-sync-{sync_id}")
}

/// Driver pod name. Sync id must be in it — the driver lives in the *shared* run
/// namespace, so nothing cascades and `ztest cleanup` reaps it beside the namespace
pub fn driver_pod_for(sync_id: &str) -> String {
    format!("ztest-sync-{sync_id}")
}

pub fn report_cm_name(sync_id: &str) -> String {
    format!("ztest-sync-report-{sync_id}")
}

/// Active sync id, `None` unless this process is a detached sync
pub fn active_sync_id() -> Option<String> {
    std::env::var(SYNC_ID_ENV).ok().filter(|s| !s.is_empty())
}

/// Publish a provisioning milestone; no-op off a detached sync. Not a
/// [`SyncReporter`](crate::sync::SyncReporter) hook — these minutes are spent
/// inside `TestEnv::build`, before any engine or reporter exists
pub(crate) fn note_setup(phase: &str, component: Option<&str>, detail: &str) {
    if active_sync_id().is_none() {
        return;
    }
    super::event::publish(&super::event::SyncEvent::Setup {
        phase: phase.to_string(),
        detail: detail.to_string(),
        component: component.map(str::to_string),
    });
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
    pub verdict: String,
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
            verdict: format!("{:?}", outcome.verdict),
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

    /// Every invariant met. Verdict travels as its debug name → sole place that
    /// knows which name means success
    pub fn passed(&self) -> bool {
        self.verdict == format!("{:?}", crate::sync::SyncVerdict::Passed)
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

#[cfg(feature = "librustzcash")]
pub use runtime::{watch_stop, write_report};

#[cfg(feature = "librustzcash")]
mod runtime {
    use std::time::Duration;

    use k8s_openapi::api::core::v1::{ConfigMap, Pod};
    use kube::Client;
    use kube::api::{Api, Patch, PatchParams};
    use serde_json::json;

    use crate::cancel::{Cancel, CancelSource};

    use super::{POD_NAME_ENV, POD_NAMESPACE_ENV, STOP_ANNOTATION, SYNC_ID_KEY, SyncReportMirror};

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

    /// Mirror the final report to its ConfigMap (SSA, idempotent) so it outlives
    /// the pod. Best-effort — the run already finished; a lost mirror costs only `report`
    pub async fn write_report(client: &Client, namespace: &str, report: &SyncReportMirror) {
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
            "data": { "report.json": body },
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
