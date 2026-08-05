//! The CPU-profiling artifact plane (`docs/design-observability.md`,
//! `docs/how-to-profile.md`).
//!
//! A component built `--features profile` runs profiled when `ZTEST_PROFILE` is
//! set (wired into its pod spec) and writes `flamegraph.svg` + `profile.pb` to
//! `ZTEST_PROFILE_OUT` on *graceful* SIGTERM. Because the write happens at pod
//! teardown, the out dir cannot be an `emptyDir` (destroyed with the pod) — it
//! is a ztest-owned per-test PVC that outlives the component pod. Collection is
//! therefore a two-step teardown: delete the component pod with a grace period
//! (so the profiler flushes to the PVC), then run a short collector pod that
//! mounts the same PVC and streams its contents out.

use std::path::{Path, PathBuf};

use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, DeleteParams, LogParams, PostParams};
use serde_json::json;

use crate::cluster::Sentinel;
use crate::error::{EnvError, env_err};
use crate::mounts::ResolvedMount;

/// In-pod mount path for the profiling artifact volume; also the value of
/// `ZTEST_PROFILE_OUT` set on a profiled component.
pub(crate) const ARTIFACT_DIR: &str = "/var/lib/ztest/profile";
/// `terminationGracePeriodSeconds` for a profiled pod: SIGTERM → graceful
/// shutdown → build + write the pprof report → exit, before the kubelet
/// SIGKILL. Generous enough for report building on a large process.
pub(crate) const GRACE_SECS: i64 = 90;
/// Pod-spec volume name for the artifact mount.
const ARTIFACT_VOLUME: &str = "ztest-profile";
/// Minimal image the collector pod runs `tar` from (already mirrored for tests).
const COLLECTOR_IMAGE: &str = "docker.io/library/busybox:1.36";

/// The per-component artifact PVC name (one profiled pod → one RWO PVC).
pub(crate) fn artifact_pvc_name(pod_name: &str) -> String {
    format!("ztest-profile-{pod_name}")
}

/// The artifact PVC as a resolved pod mount at [`ARTIFACT_DIR`]. Injected into a
/// profiled component's mount set in `materialize_phase`.
pub(crate) fn artifact_resolved_mount(claim: &str) -> ResolvedMount {
    ResolvedMount {
        volume: json!({
            "name": ARTIFACT_VOLUME,
            "persistentVolumeClaim": { "claimName": claim },
        }),
        volume_mount: json!({
            "name": ARTIFACT_VOLUME,
            "mountPath": ARTIFACT_DIR,
        }),
    }
}

/// Create the RWO artifact PVC for a profiled component (reuses the shared-PVC
/// provisioner; the claim is namespace-scoped and reclaimed at teardown).
pub(crate) async fn ensure_artifact_pvc(
    client: &Client,
    sentinel: &Sentinel,
    claim: &str,
) -> Result<(), EnvError> {
    crate::mounts::create_shared_pvc(client, sentinel, claim).await
}

/// Collect one profiled component's artifacts (`flamegraph.svg` +
/// `profile.pb` — the raw pprof profile is the re-renderable, diffable source
/// the SVG is only one view of) to `dest_dir`, returning the written paths.
///
/// Steps: (1) delete the component pod with [`GRACE_SECS`] so its SIGTERM
/// handler flushes the profile to the PVC; (2) run a collector pod that tars
/// the (now pod-free, RWO) PVC to stdout as base64; (3) decode + unpack into
/// `dest_dir`. Best-effort — a failure at any step logs and returns an empty
/// vec rather than failing the run (a profile is a diagnostic, not a gate).
pub(crate) async fn collect(
    client: &Client,
    namespace: &str,
    pod_name: &str,
    dest_dir: &Path,
) -> Vec<PathBuf> {
    if let Err(e) = drain_pod(client, namespace, pod_name).await {
        tracing::warn!(pod = %pod_name, error = %e, "profiling: graceful pod drain failed");
        return Vec::new();
    }
    let claim = artifact_pvc_name(pod_name);
    let tarball = match run_collector(client, namespace, pod_name, &claim).await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!(pod = %pod_name, error = %e, "profiling: collector pod failed");
            return Vec::new();
        }
    };
    match unpack_artifacts(&tarball, dest_dir, pod_name) {
        Ok(paths) => {
            for path in &paths {
                tracing::info!(pod = %pod_name, artifact = %path.display(), "collected profile artifact");
            }
            paths
        }
        Err(e) => {
            tracing::warn!(pod = %pod_name, error = %e, "profiling: unpack failed");
            Vec::new()
        }
    }
}

/// Delete the component pod with the profiling grace period and wait for it to
/// disappear, so the RWO artifact PVC is free for the collector to mount.
async fn drain_pod(client: &Client, namespace: &str, pod_name: &str) -> Result<(), EnvError> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let dp = DeleteParams {
        grace_period_seconds: Some(GRACE_SECS as u32),
        ..DeleteParams::default()
    };
    if let Err(e) = pods.delete(pod_name, &dp).await {
        // Already gone is fine.
        if !matches!(&e, kube::Error::Api(r) if r.code == 404) {
            return Err(env_err(e));
        }
    }
    // Poll until the pod object is fully removed (RWO detach completes with it).
    let deadline = GRACE_SECS + 30;
    for _ in 0..deadline {
        match pods.get_opt(pod_name).await {
            Ok(None) => return Ok(()),
            Ok(Some(_)) | Err(_) => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
        }
    }
    Err(EnvError::Config {
        reason: format!("profiled pod {pod_name} did not terminate within {deadline}s"),
    })
}

/// Run a one-shot collector pod that tars the artifact PVC to stdout (base64, so
/// it survives the log channel), wait for completion, and return the raw
/// (uncompressed) tar bytes.
async fn run_collector(
    client: &Client,
    namespace: &str,
    pod_name: &str,
    claim: &str,
) -> Result<Vec<u8>, EnvError> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let collector = format!("ztest-profile-collect-{pod_name}");
    let manifest: Pod = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": collector },
        "spec": {
            "restartPolicy": "Never",
            "securityContext": { "fsGroup": 1000, "runAsUser": 1000 },
            "containers": [{
                "name": "collect",
                "image": COLLECTOR_IMAGE,
                "command": ["sh", "-c",
                    format!("tar cf - -C {ARTIFACT_DIR} . | base64 -w0")],
                "volumeMounts": [{ "name": "artifact", "mountPath": ARTIFACT_DIR, "readOnly": true }],
            }],
            "volumes": [{
                "name": "artifact",
                "persistentVolumeClaim": { "claimName": claim, "readOnly": true },
            }],
        },
    }))
    .expect("static collector pod manifest is valid");

    // Best-effort clean of a stale collector from a prior attempt.
    let _ = pods.delete(&collector, &DeleteParams::default()).await;
    pods.create(&PostParams::default(), &manifest)
        .await
        .map_err(env_err)?;

    // Wait for it to reach a terminal phase.
    let mut b64 = String::new();
    for _ in 0..120 {
        if let Ok(Some(p)) = pods.get_opt(&collector).await {
            let phase = p
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("");
            if phase == "Succeeded" {
                b64 = pods
                    .logs(&collector, &LogParams::default())
                    .await
                    .map_err(env_err)?;
                break;
            }
            if phase == "Failed" {
                let _ = pods.delete(&collector, &DeleteParams::default()).await;
                return Err(EnvError::Config {
                    reason: format!("collector pod for {pod_name} failed"),
                });
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let _ = pods.delete(&collector, &DeleteParams::default()).await;

    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| EnvError::Config {
            reason: format!("decode collector output for {pod_name}: {e}"),
        })
}

/// Unpack every profile artifact (`flamegraph.svg`, `profile.pb`, …) from the
/// collected tar into `dest_dir`, each written as `<pod_name>-<basename>` so
/// several profiled components in one run never collide. Returns the written
/// paths; errors only if the tar is unreadable or carried no files.
fn unpack_artifacts(
    tarball: &[u8],
    dest_dir: &Path,
    pod_name: &str,
) -> Result<Vec<PathBuf>, EnvError> {
    let mut ar = tar::Archive::new(tarball);
    std::fs::create_dir_all(dest_dir).map_err(|e| EnvError::Config {
        reason: format!("create artifact dir {}: {e}", dest_dir.display()),
    })?;
    let mut written = Vec::new();
    for entry in ar.entries().map_err(|e| EnvError::Config {
        reason: format!("read collected tar: {e}"),
    })? {
        let mut entry = entry.map_err(|e| EnvError::Config {
            reason: format!("read tar entry: {e}"),
        })?;
        // Only regular files are artifacts; skip the `.` directory entry the
        // `tar cf -C dir .` invocation carries.
        if !entry.header().entry_type().is_file() {
            continue;
        }
        // Flatten to the archived basename — never trust the tar's path for the
        // output location, so a crafted entry can't escape `dest_dir`.
        let path = entry.path().map_err(|e| EnvError::Config {
            reason: format!("tar entry path: {e}"),
        })?;
        let Some(file) = path.file_name().and_then(|n| n.to_str()).map(str::to_owned) else {
            continue;
        };
        let out = dest_dir.join(format!("{pod_name}-{file}"));
        entry.unpack(&out).map_err(|e| EnvError::Config {
            reason: format!("unpack {file}: {e}"),
        })?;
        written.push(out);
    }
    if written.is_empty() {
        return Err(EnvError::Config {
            reason: format!("no profile artifacts in {pod_name}'s collected tar"),
        });
    }
    Ok(written)
}
