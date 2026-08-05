//! Detached-sync runtime + the vocabulary the stateless `ztest sync` controller
//! and the in-pod runner agree on (design §"Execution model: ztest-owned pods").
//!
//! A `ztest sync start` pod runs the *ordinary* sync-test body — the same
//! compiled `#[ztest::sync_test]` — but as a detached, ztest-owned sync rather
//! than a CI-gating engine run. The pod carries [`SYNC_ID_ENV`]; the body, on
//! seeing it, (1) watches its own pod for the [`STOP_ANNOTATION`] and turns a
//! set annotation into engine cancellation (`sync_mode = Shutdown` →
//! checkpoint), and (2) mirrors the final report to a ConfigMap that outlives
//! the pod so `ztest sync report`/`status` work after it is gone.
//!
//! Everything the controller needs to *find* and *read* a detached sync — the
//! labels, the stop annotation, the per-sync namespace, the report ConfigMap
//! name — is defined here, so the two sides never drift. This module is compiled
//! unconditionally (the CLI controller uses the constants; the in-pod runtime
//! functions are exercised only in the `zingo` test binary).

use serde::{Deserialize, Serialize};

/// Env var carrying the sync id into the detached pod. Its presence is the
/// switch: set ⇒ "you are a detached sync, wire the stop-watch + report mirror";
/// absent ⇒ an ordinary engine / `cargo test` run.
pub const SYNC_ID_ENV: &str = "ZTEST_SYNC_ID";
/// Env var carrying the profile *name* (`#[sync_test(name = ..)]`) for the report.
pub const SYNC_PROFILE_ENV: &str = "ZTEST_SYNC_PROFILE";
/// Downward-API env carrying the pod's own name (set in the detached pod spec),
/// so the in-pod runner can watch itself without guessing its generated name.
pub const POD_NAME_ENV: &str = "ZTEST_POD_NAME";

/// Label key marking a ztest-owned detached-sync driver pod (value
/// [`KIND_LABEL_VALUE`]); also stamped on the per-sync namespace + report CM.
pub const KIND_LABEL_KEY: &str = "ztest.io/kind";
/// The [`KIND_LABEL_KEY`] value a detached sync carries.
pub const KIND_LABEL_VALUE: &str = "sync";
/// Per-sync id label key (on the driver pod, its namespace, and its report CM).
pub const SYNC_ID_KEY: &str = "ztest.io/sync-id";
/// Launching-user label key (for `list --all-users` filtering).
pub const OWNER_KEY: &str = "ztest.io/owner";
/// Annotation the controller sets on the driver pod to request a graceful stop.
/// The in-pod runner watches it and drives `sync_mode = Shutdown` via engine
/// cancellation — this is a checkpoint, not a kill (design §stop).
pub const STOP_ANNOTATION: &str = "ztest.io/sync-stop";

/// The `ztest.io/kind=sync` label selector the controller lists by.
pub fn kind_selector() -> String {
    format!("{KIND_LABEL_KEY}={KIND_LABEL_VALUE}")
}

/// The hermetic per-sync namespace for a sync id. Each detached sync gets its
/// own namespace: it isolates the component-pod names and the ResourceQuota
/// `TestEnv` applies, exactly as an ephemeral `ztest run` isolates each test,
/// while the namespace's persistence + `kind=sync` label keep it out of
/// `ztest cleanup`'s reach. `rm` deletes the namespace and cascades everything.
pub fn namespace_for(sync_id: &str) -> String {
    format!("ztest-sync-{sync_id}")
}

/// The report-mirror ConfigMap name for a sync id.
pub fn report_cm_name(sync_id: &str) -> String {
    format!("ztest-sync-report-{sync_id}")
}

/// The active sync id if this process is a detached sync, else `None`.
pub fn active_sync_id() -> Option<String> {
    std::env::var(SYNC_ID_ENV).ok().filter(|s| !s.is_empty())
}

/// Publish a provisioning milestone to whoever is watching this sync — a no-op
/// unless this process *is* a detached sync, since a local `cargo test` run has
/// nobody tailing it and its `tracing` output already says the same thing.
///
/// Deliberately not a [`SyncReporter`](crate::sync::SyncReporter) hook: the
/// minutes these report on are spent inside `TestEnv::build`, before an engine or
/// a reporter exists — which is precisely why that window used to be a blank
/// panel indistinguishable from a hang.
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

/// A serialized, self-describing final report, stored in the report ConfigMap so
/// it survives the pod (`ztest sync report`/`status` read it back). A plain DTO
/// — the live [`SyncOutcome`](crate::sync::SyncOutcome) types aren't `Serialize`
/// and shouldn't be (they carry live handles); this is their durable projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncReportMirror {
    pub sync_id: String,
    pub profile: String,
    /// `SyncVerdict` rendered as its debug name (`Passed`/`Failed`/…).
    pub verdict: String,
    pub ticks: u64,
    pub dropped_snapshots: u64,
    pub violations: Vec<ReportViolation>,
    pub coverage_gaps: Vec<String>,
    pub metrics: Vec<ReportMetric>,
    pub error: Option<String>,
}

/// A recorded violation, projected for the durable report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportViolation {
    pub probe: String,
    pub height: Option<u32>,
    pub detail: String,
}

/// One server-side metric sample, projected for the durable report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportMetric {
    pub name: String,
    pub value: Option<f64>,
}

impl SyncReportMirror {
    /// Project a finished run into its durable report.
    pub fn from_outcome(
        sync_id: &str,
        profile: &str,
        outcome: &crate::sync::SyncOutcome,
    ) -> Self {
        SyncReportMirror {
            sync_id: sync_id.to_string(),
            profile: profile.to_string(),
            verdict: format!("{:?}", outcome.verdict),
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
            metrics: outcome
                .metrics
                .samples
                .iter()
                .map(|s| ReportMetric {
                    name: s.name.clone(),
                    value: s.value,
                })
                .collect(),
            error: outcome.error.clone(),
        }
    }

    /// Whether the run met every invariant. The verdict is carried as its debug
    /// name, so the one place that knows which name means success is here.
    pub fn passed(&self) -> bool {
        self.verdict == format!("{:?}", crate::sync::SyncVerdict::Passed)
    }

    /// A compact human summary line (for `status`/`report` non-JSON output).
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
    use kube::api::{Api, Patch, PatchParams};
    use kube::Client;
    use serde_json::json;

    use crate::cancel::{Cancel, CancelSource};

    use super::{SyncReportMirror, POD_NAME_ENV, STOP_ANNOTATION, SYNC_ID_KEY};

    /// How often the stop-watch re-reads its own pod's annotations. Coarse on
    /// purpose — a detached sync is a minutes-to-days job and a stop is not
    /// latency-critical; a light poll is simpler and more robust than a watch
    /// stream that must be re-established across API hiccups.
    const STOP_POLL: Duration = Duration::from_secs(5);

    /// Spawn the in-pod stop-watch and return a [`Cancel`] the engine observes.
    /// It fires on either the controller setting [`STOP_ANNOTATION`] on this pod
    /// (`ztest sync stop`) or a `SIGTERM` (node loss / `rm`) — both mean "wind
    /// down gracefully now", which the engine turns into `subject.stop()` →
    /// checkpoint. The pod name comes from the downward-API [`POD_NAME_ENV`].
    pub async fn watch_stop(client: &Client, namespace: &str) -> Cancel {
        let (source, cancel) = CancelSource::new();
        let pod_name = std::env::var(POD_NAME_ENV).unwrap_or_default();
        let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);

        tokio::spawn(async move {
            let mut sigterm = match tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            ) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "sync stop-watch: cannot install SIGTERM handler");
                    // Fall back to annotation-only watching.
                    poll_forever(&pods, &pod_name, &source).await;
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
                        if pod_name.is_empty() {
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

    /// Annotation-only fallback loop (no SIGTERM handler available).
    async fn poll_forever(pods: &Api<Pod>, pod_name: &str, source: &CancelSource) {
        if pod_name.is_empty() {
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

    /// Whether this pod carries a truthy [`STOP_ANNOTATION`]. A transient read
    /// error is treated as "not yet" — the sync keeps running rather than
    /// stopping on a blip.
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

    /// Mirror the final report to its ConfigMap (server-side apply, idempotent),
    /// so it outlives the pod. Best-effort: a write failure is logged, never
    /// fatal — the run already finished; losing the mirror only costs `report`.
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
        if let Err(e) = api
            .patch(&name, &PatchParams::apply("ztest-sync").force(), &Patch::Apply(&cm))
            .await
        {
            tracing::warn!(error = %e, cm = %name, "sync report: ConfigMap mirror failed");
        }
    }
}
