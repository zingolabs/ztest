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
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, AttachParams, DeleteParams, PostParams};
use kube::runtime::wait::await_condition;
use serde_json::json;
use tokio::io::AsyncReadExt as _;

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

/// Every profiled component in `namespace`, recovered from its artifact PVC.
///
/// The driver holds the profiled-pod list in memory and takes it to the grave, so
/// an after-the-fact reader (`ztest sync perf`, run from a laptop long after the
/// driver exited) has to rediscover it. The PVC naming convention is the record
/// that survives: one claim per profiled pod, so the claims *are* the list.
pub(crate) async fn profiled_components(
    client: &Client,
    namespace: &str,
) -> Result<Vec<String>, EnvError> {
    use k8s_openapi::api::core::v1::PersistentVolumeClaim;

    let claims: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    let list = claims
        .list(&kube::api::ListParams::default())
        .await
        .map_err(env_err)?;
    let mut pods: Vec<String> = list
        .items
        .iter()
        .filter_map(|c| c.metadata.name.as_deref())
        .filter_map(|name| name.strip_prefix("ztest-profile-"))
        .map(str::to_string)
        .collect();
    pods.sort();
    Ok(pods)
}

/// Retrieve one component's profile artifacts **without disturbing the run**.
///
/// Prefers the live component pod, which is both faster (no collector pod, no PVC
/// remount) and the only option while the pod holds the RWO claim. Falls back to
/// the PVC once the component is gone, which is the state a finished sync is left
/// in. Neither path stops, drains, or signals anything: a diagnostic command that
/// can end a multi-hour run is a footgun, so `perf` is strictly a reader.
///
/// The live path yields whatever the component has flushed *so far*. Under the
/// current write-on-SIGTERM contract that is nothing until shutdown — see the
/// cadence discussion in `docs/how-to-profile.md`.
pub(crate) async fn retrieve(
    client: &Client,
    namespace: &str,
    pod_name: &str,
    dest_dir: &Path,
) -> Result<Vec<PathBuf>, EnvError> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let live = matches!(pods.get_opt(pod_name).await, Ok(Some(p))
        if p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"));

    let tarball = match live {
        true => exec_tar(&pods, pod_name).await?,
        false => run_collector(client, namespace, pod_name, &artifact_pvc_name(pod_name)).await?,
    };
    unpack_artifacts(&tarball, dest_dir, pod_name)
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

/// How long the collector pod idles waiting to be exec'd into. Bounds the mess a
/// crashed `ztest` leaves behind: the pod deletes itself once this elapses, so a
/// lost collector cannot pin the RWO artifact PVC indefinitely.
const COLLECTOR_IDLE_SECS: u32 = 300;
/// How long to wait for a previous collector to leave the API before forcing it.
const COLLECTOR_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(60);
/// How long to wait for a fresh collector to reach `Running`.
const COLLECTOR_START_TIMEOUT: Duration = Duration::from_secs(120);

/// Stream `ARTIFACT_DIR` out of a **running** pod as tar bytes, over `exec`.
///
/// The transport is deliberately `exec` rather than the pod log. Tarring to
/// stdout and reading it back as a log line requires base64 (+33%) and rides a
/// channel the kubelet *rotates*: a profile larger than the container log cap
/// comes back silently truncated, and profile size grows with a process's unique
/// stack count — precisely the artifact that outgrows it. The exec websocket has
/// no rotation and needs no re-encoding, and `pods/exec` is already granted to
/// the run role alongside `pods/log`.
async fn exec_tar(pods: &Api<Pod>, pod_name: &str) -> Result<Vec<u8>, EnvError> {
    let mut proc = pods
        .exec(
            pod_name,
            ["tar", "cf", "-", "-C", ARTIFACT_DIR, "."],
            &AttachParams::default().stdout(true).stderr(false),
        )
        .await
        .map_err(env_err)?;
    // Taken before the read, because `join` consumes the process. `join` only
    // reports whether the *websocket* survived; the command's own exit code
    // arrives on this channel. Ignoring it would let a `tar` that died partway
    // — "file changed as we read it" is reachable here, since the component
    // renames snapshots into this directory while we read it — return a
    // truncated archive that unpacks without complaint into a short profile.
    let status = proc.take_status().ok_or_else(|| EnvError::Config {
        reason: format!("exec into {pod_name} returned no status channel"),
    })?;
    let mut stdout = proc.stdout().ok_or_else(|| EnvError::Config {
        reason: format!("exec into {pod_name} returned no stdout stream"),
    })?;
    let mut tarball = Vec::new();
    stdout
        .read_to_end(&mut tarball)
        .await
        .map_err(|e| EnvError::Config {
            reason: format!("read tar stream from {pod_name}: {e}"),
        })?;
    // Awaited after the read: the far end only closes once `tar` has exited, so
    // waiting first would deadlock against the bytes still in flight.
    let exit = status.await;
    proc.join().await.map_err(env_err)?;

    if let Some(failure) = exit.filter(|s| s.status.as_deref() != Some("Success")) {
        return Err(EnvError::Config {
            reason: format!(
                "tar in {pod_name} failed: {}",
                failure.message.as_deref().unwrap_or("no detail")
            ),
        });
    }
    if tarball.is_empty() {
        return Err(EnvError::Config {
            reason: format!("{pod_name} produced no profile artifacts at {ARTIFACT_DIR}"),
        });
    }
    Ok(tarball)
}

/// Mount the artifact PVC in a short-lived collector pod and stream it out.
///
/// Used when the component that owns the profile is gone — the artifact outlives
/// it on the PVC, but nothing is left to `exec` into. The pod idles rather than
/// running `tar` as its entrypoint so the tar can be an `exec` (see
/// [`exec_tar`]); `Running`, not `Succeeded`, is therefore the state to wait for.
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
                "command": ["sleep", COLLECTOR_IDLE_SECS.to_string()],
                "volumeMounts": [{ "name": "artifact", "mountPath": ARTIFACT_DIR, "readOnly": true }],
            }],
            "volumes": [{
                "name": "artifact",
                "persistentVolumeClaim": { "claimName": claim, "readOnly": true },
            }],
        },
    }))
    .expect("static collector pod manifest is valid");

    remove_collector(&pods, &collector).await;
    pods.create(&PostParams::default(), &manifest)
        .await
        .map_err(env_err)?;

    let ready = wait_running(&pods, &collector).await;
    let result = match ready {
        Ok(()) => exec_tar(&pods, &collector).await,
        Err(e) => Err(e),
    };
    remove_collector(&pods, &collector).await;
    result
}

/// Delete the collector and wait for it to actually leave the API.
///
/// `delete` returns once the deletion is *accepted*; the object keeps its name
/// until the kubelet has stopped the container and unmounted the PVC. Creating
/// the replacement inside that window fails with a 409 that no retry of the
/// create can clear (`object is being deleted: … already exists`), so waiting
/// for the name to free is what makes `ztest sync perf` re-runnable.
async fn remove_collector(pods: &Api<Pod>, name: &str) {
    if pods.delete(name, &DeleteParams::default()).await.is_err() {
        return;
    }
    let gone = await_condition(pods.clone(), name, |p: Option<&Pod>| p.is_none());
    if !matches!(
        tokio::time::timeout(COLLECTOR_TEARDOWN_TIMEOUT, gone).await,
        Ok(Ok(_))
    ) {
        let _ = pods
            .delete(name, &DeleteParams::default().grace_period(0))
            .await;
    }
}

/// Wait for a pod to reach `Running`, the state in which it can be `exec`'d.
///
/// Settles on `Failed` as well: under `restartPolicy: Never` a collector that
/// cannot start never recovers — usually because the RWO artifact volume is
/// still attached to a component pod that has not finished terminating — so
/// waking for it reports the cause instead of spinning to the timeout.
async fn wait_running(pods: &Api<Pod>, name: &str) -> Result<(), EnvError> {
    let settled = await_condition(pods.clone(), name, |p: Option<&Pod>| {
        p.is_some_and(|p| {
            matches!(
                p.status.as_ref().and_then(|s| s.phase.as_deref()),
                Some("Running" | "Failed")
            )
        })
    });
    match tokio::time::timeout(COLLECTOR_START_TIMEOUT, settled).await {
        Err(_) => Err(EnvError::Config {
            reason: format!(
                "collector pod {name} was not Running within {}s — its artifact \
                 volume is likely still attached to a terminating pod",
                COLLECTOR_START_TIMEOUT.as_secs()
            ),
        }),
        Ok(Err(e)) => Err(EnvError::Config {
            reason: format!("watching collector pod {name}: {e}"),
        }),
        Ok(Ok(pod)) => match pod
            .as_ref()
            .and_then(|p| p.status.as_ref())
            .and_then(|s| s.phase.as_deref())
        {
            Some("Failed") => Err(EnvError::Config {
                reason: format!("collector pod {name} failed to start"),
            }),
            _ => Ok(()),
        },
    }
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
        // A leading dot is a snapshot mid-write: the contract writes to `.<name>.tmp`
        // and renames, so anything still dotted was caught in flight and is a
        // partial protobuf. Unpacking it would litter the output with files that
        // look like profiles and parse as nothing.
        if file.starts_with('.') {
            continue;
        }
        let out = dest_dir.join(format!("{pod_name}-{file}"));
        entry.unpack(&out).map_err(|e| EnvError::Config {
            reason: format!("unpack {file}: {e}"),
        })?;
        written.push(out);
    }
    if written.is_empty() {
        return Err(EnvError::Config {
            reason: format!(
                "{pod_name}'s artifact volume holds no profile — without \
                 ZTEST_PROFILE_INTERVAL one is written only on graceful shutdown, \
                 so a component still running has none yet"
            ),
        });
    }
    Ok(written)
}
