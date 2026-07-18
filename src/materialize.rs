//! Bring a `seed-{sha8}` PVC into existence and populate it from the
//! local file system on first use.
//!
//! Single entry point: [`ensure_seed`]. Idempotent and race-safe; the
//! happy path on a warm cluster is two `GET`s.
//!
//! The heavy lifting is done by k8s and a one-shot uploader pod, not by us:
//!
//! 1. Get-or-create the seed PVC. A 409 means somebody else won the race,
//!    so fall through to wait-for-ready.
//! 2. If we created it (or it exists but isn't `ready=true`) and there's no
//!    in-flight uploader, launch the uploader pod. The pod runs
//!    `tar -I zstd -xf - -C /seed` (or `cat > /seed/blob` for files) with
//!    `stdin: true`, mounting the seed PVC.
//! 3. Attach to the pod's stdin via `Api::<Pod>::attach` and stream the
//!    local file. When stdin EOFs, the command finishes and the pod
//!    transitions to `Succeeded`.
//! 4. Label the PVC `seeds.ztest.io/ready=true` and create the paired
//!    `VolumeSnapshot`. From here `seeds::read_seed_handle` can resolve the
//!    CSI snapshot handle.
//!
//! Race losers (PVC already exists, not ours) poll the PVC for the
//! `ready=true` label, then poll the snapshot for `status.readyToUse`. No
//! leader election; `kubectl get pod` is the lock.

use std::path::Path;
use std::time::Duration;

use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::Client;
use kube::api::{Api, AttachParams, DynamicObject, Patch, PatchParams, PostParams};
use kube::runtime::wait::{Condition, await_condition, conditions};
use serde_json::json;
use tokio::io::AsyncWriteExt;

use crate::EnvError;
use crate::error::env_err;
use crate::seeds::{self, SEEDS_NAMESPACE, SeedHandle, volume_snapshot_gvk};
use crate::storage::{self, StorageBackend};

const WAIT_INTERVAL: Duration = Duration::from_secs(2);
const WAIT_BUDGET: Duration = Duration::from_secs(300);

/// What we're loading. The choice drives the uploader command: archives
/// are extracted, files are copied byte-for-byte.
#[derive(Debug, Clone, Copy)]
pub enum Payload {
    Archive,
    File,
}

/// Get-or-create the seed PVC and ensure it's populated and snapshotted.
/// Returns a fully-formed `SeedHandle` ready to be consumed by
/// `mint_shadow_clone`.
pub async fn ensure_seed(
    client: &Client,
    source: &Path,
    payload: Payload,
) -> Result<SeedHandle, EnvError> {
    let sha8 = seeds::sha8(source).map_err(|e| EnvError::ArchiveMaterializeFailed {
        archive: source.to_path_buf(),
        reason: format!("hashing source failed: {e}"),
    })?;
    let pvc_name = format!("seed-{sha8}");

    // Fail fast on a cluster that can't back seeds at all, rather than
    // creating an unschedulable PVC and polling out the full `WAIT_BUDGET`.
    // The classic case is a stock kind cluster missing CSI snapshot
    // support; the fix is a single `ztest setup`.
    check_seed_support(client, source).await?;

    ensure_seeds_namespace(client).await?;

    let we_created = create_seed_pvc(client, &pvc_name).await?;
    if we_created || !pvc_is_ready(client, &pvc_name).await? {
        tracing::info!(pvc = %pvc_name, source = ?source, "materializing seed PVC");
        match try_materialize(client, &pvc_name, source, payload).await {
            Ok(Ok(())) => {
                mark_ready(client, &pvc_name).await?;
                create_volume_snapshot(client, &pvc_name).await?;
            }
            Ok(Err(e)) => return Err(e),
            // Another process is materializing; wait it out in the poll
            // loops below.
            Err(InFlight) => {
                tracing::debug!(pvc = %pvc_name, "seed materialization in flight elsewhere; waiting");
            }
        }
    }

    wait_pvc_ready(client, &pvc_name).await?;
    wait_snapshot_ready(client, &pvc_name).await?;
    seeds::read_seed_handle(client, source, &sha8).await
}

// ─────────────────────────── capability preflight ───────────────────

/// Verify the cluster can actually back a seed before we create one.
/// Without this, a stock kind cluster (no CSI snapshot support) accepts an
/// unschedulable PVC and we burn the whole [`WAIT_BUDGET`] before failing
/// with an opaque timeout. Here we surface the real cause (a missing
/// StorageClass or VolumeSnapshotClass) in milliseconds, with a pointer to
/// the fix.
async fn check_seed_support(client: &Client, source: &Path) -> Result<(), EnvError> {
    use k8s_openapi::api::storage::v1::StorageClass;

    let sc_name = detect_storage_class();
    let sc_api: Api<StorageClass> = Api::all(client.clone());
    let sc_missing = match sc_api.get_opt(&sc_name).await {
        Ok(opt) => opt.is_none(),
        Err(_) => true,
    };
    if sc_missing {
        return Err(unsupported(
            source,
            format!("StorageClass `{sc_name}` not found"),
        ));
    }

    let vsc_name = detect_snapshot_class();
    let gvk = kube::api::GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshotClass".into(),
    };
    let vsc_api: Api<DynamicObject> =
        Api::all_with(client.clone(), &kube::api::ApiResource::from_gvk(&gvk));
    // An `Err` here usually means the snapshot CRDs aren't installed at
    // all (discovery fails); `Ok(None)` means the class is absent. Both
    // mean "this cluster can't snapshot".
    let vsc_missing = !matches!(vsc_api.get_opt(&vsc_name).await, Ok(Some(_)));
    if vsc_missing {
        return Err(unsupported(
            source,
            format!("VolumeSnapshotClass `{vsc_name}` / CSI snapshot support not found"),
        ));
    }
    Ok(())
}

/// Build the "this cluster can't back seeds" error with a fix pointer.
fn unsupported(source: &Path, what: String) -> EnvError {
    EnvError::ArchiveMaterializeFailed {
        archive: source.to_path_buf(),
        reason: format!(
            "{what} — this archive-backed test needs CSI snapshot support. \
             On a local kind cluster run `ztest setup`; on a shared cluster \
             check that the seed StorageClass / VolumeSnapshotClass are installed."
        ),
    }
}

// ─────────────────────────── namespace + PVC ────────────────────────

async fn ensure_seeds_namespace(client: &Client) -> Result<(), EnvError> {
    use k8s_openapi::api::core::v1::Namespace;
    let api: Api<Namespace> = Api::all(client.clone());
    if api
        .get_opt(SEEDS_NAMESPACE)
        .await
        .map_err(env_err)?
        .is_some()
    {
        return Ok(());
    }
    let ns: Namespace = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": SEEDS_NAMESPACE },
    }))
    .expect("static manifest");
    match api.create(&PostParams::default(), &ns).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(env_err(e)),
    }
}

async fn create_seed_pvc(client: &Client, pvc_name: &str) -> Result<bool, EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    if api.get_opt(pvc_name).await.map_err(env_err)?.is_some() {
        return Ok(false);
    }
    let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": pvc_name,
            "labels": {
                "seeds.ztest.io/sha": pvc_name.trim_start_matches("seed-"),
                "seeds.ztest.io/ready": "false",
            },
            "annotations": { "seeds.ztest.io/last_accessed_at": "now" },
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": detect_seed_size() } },
            "storageClassName": detect_storage_class(),
        }
    }))
    .expect("static manifest");
    match api.create(&PostParams::default(), &pvc).await {
        Ok(_) => Ok(true),
        // Lost the race: someone else got there first.
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
        Err(e) => Err(env_err(e)),
    }
}

async fn pvc_is_ready(client: &Client, pvc_name: &str) -> Result<bool, EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let pvc = api.get(pvc_name).await.map_err(env_err)?;
    Ok(pvc
        .metadata
        .labels
        .as_ref()
        .and_then(|m| m.get("seeds.ztest.io/ready"))
        .map(|s| s == "true")
        .unwrap_or(false))
}

async fn mark_ready(client: &Client, pvc_name: &str) -> Result<(), EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let patch = json!({
        "metadata": { "labels": { "seeds.ztest.io/ready": "true" } }
    });
    api.patch(pvc_name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .map_err(env_err)?;
    Ok(())
}

// ─────────────────────────── uploader pod ───────────────────────────

/// Sentinel error returned by `materialize` when the uploader pod already
/// exists (another actor is uploading right now). Collapses to a "wait"
/// branch in `ensure_seed`.
struct InFlight;

async fn try_materialize(
    client: &Client,
    pvc_name: &str,
    source: &Path,
    payload: Payload,
) -> Result<Result<(), EnvError>, InFlight> {
    match materialize(client, pvc_name, source, payload).await {
        Ok(()) => Ok(Ok(())),
        Err(MaterializeErr::InFlight) => Err(InFlight),
        Err(MaterializeErr::Fatal(e)) => Ok(Err(e)),
    }
}

enum MaterializeErr {
    InFlight,
    Fatal(EnvError),
}
impl From<EnvError> for MaterializeErr {
    fn from(e: EnvError) -> Self {
        MaterializeErr::Fatal(e)
    }
}

async fn materialize(
    client: &Client,
    pvc_name: &str,
    source: &Path,
    payload: Payload,
) -> Result<(), MaterializeErr> {
    let pod_name = format!("uploader-{}", pvc_name.trim_start_matches("seed-"));
    let pods: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);

    // Resolve the byte source (local file or LFS pointer) and fix the uploader
    // command up front. Compression is derived without transferring the bytes,
    // so the pod is created before `open()` starts any download.
    let backend = storage::for_source(source).map_err(|e| storage_fatal(source, e))?;
    let cmd = uploader_cmd(payload, backend.as_ref()).map_err(|e| storage_fatal(source, e))?;
    let pod_body = uploader_pod(&pod_name, pvc_name, &cmd);
    match pods.create(&PostParams::default(), &pod_body).await {
        Ok(_) => {}
        // Another process beat us to launching the uploader; fall back to
        // wait-for-ready.
        Err(kube::Error::Api(e)) if e.code == 409 => return Err(MaterializeErr::InFlight),
        Err(e) => return Err(MaterializeErr::Fatal(env_err(e))),
    }

    // 1. Wait for the container to be Running so the stdin attach works.
    //    Bounded: a wedged image pull would otherwise hang the whole test
    //    until nextest's slow-timeout kills it.
    tokio::time::timeout(
        WAIT_BUDGET,
        await_condition(pods.clone(), &pod_name, conditions::is_pod_running()),
    )
    .await
    .map_err(|_| uploader_stuck(source, &pod_name))?
    .map_err(env_err)?;

    // 2. Stream the source bytes into the pod's stdin. When we drop stdin,
    //    the command sees EOF and exits.
    let mut attached = pods
        .attach(
            &pod_name,
            &AttachParams::default()
                .stdin(true)
                .stderr(true)
                .stdout(false),
        )
        .await
        .map_err(env_err)?;
    let mut stdin = attached
        .stdin()
        .ok_or_else(|| env_err(std::io::Error::other("uploader pod did not expose stdin")))?;
    // Open the bytes only now the pod can receive them: for LFS this is when the
    // batch API resolves and the blob GET begins, so nothing is held open across
    // pod scheduling. When stdin EOFs, the uploader command finishes.
    let mut src = backend.open().await.map_err(|e| storage_fatal(source, e))?;
    tokio::io::copy(&mut src, &mut stdin)
        .await
        .map_err(env_err)?;
    stdin.shutdown().await.ok();
    drop(stdin);

    // 3. Wait for the pod to terminate (bounded), then distinguish success
    //    from failure. A failed uploader (e.g. the image lacks the
    //    decompressor `tar` needs) must surface its logs, not hang.
    tokio::time::timeout(
        WAIT_BUDGET,
        await_condition(pods.clone(), &pod_name, is_pod_finished()),
    )
    .await
    .map_err(|_| uploader_stuck(source, &pod_name))?
    .map_err(env_err)?;
    let _ = attached.join().await;
    if pod_phase(&pods, &pod_name).await.as_deref() == Some("Failed") {
        let logs = pods
            .logs(&pod_name, &Default::default())
            .await
            .unwrap_or_else(|e| format!("<logs unavailable: {e}>"));
        return Err(MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
            archive: source.to_path_buf(),
            reason: format!("uploader pod failed: {}", logs.trim()),
        }));
    }
    // Best-effort cleanup; janitor backstops.
    let _ = pods.delete(&pod_name, &Default::default()).await;
    Ok(())
}

/// Error for an uploader that never reached a terminal state within the
/// budget (typically a wedged image pull).
fn uploader_stuck(source: &Path, pod: &str) -> MaterializeErr {
    MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
        archive: source.to_path_buf(),
        reason: format!(
            "uploader pod {pod} did not finish within {WAIT_BUDGET:?} \
             (check `kubectl -n {SEEDS_NAMESPACE} describe pod {pod}` for image-pull/scheduling errors)"
        ),
    })
}

/// The shell command the uploader runs to populate `/seed` from the streamed
/// bytes. Archives are extracted with an explicit decompression flag (GNU tar
/// can't auto-detect on a non-seekable stdin pipe); files are copied verbatim.
/// The flag's decompressor must exist in the uploader image — see
/// [`detect_uploader_image`].
fn uploader_cmd(
    payload: Payload,
    backend: &dyn StorageBackend,
) -> Result<String, storage::StorageError> {
    match payload {
        Payload::Archive => Ok(format!(
            "tar {}-xf - -C /seed",
            backend.compression()?.tar_flag()
        )),
        // File path inside the PVC is always `/seed/blob`; `read_seed_handle`
        // doesn't care, only the consumer's volumeMount path does.
        Payload::File => Ok("cat > /seed/blob".to_string()),
    }
}

/// A storage failure (bad pointer, unreachable LFS blob, unreadable file) as a
/// fatal materialize error tagged with the source archive.
fn storage_fatal(source: &Path, err: storage::StorageError) -> MaterializeErr {
    MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
        archive: source.to_path_buf(),
        reason: err.to_string(),
    })
}

fn uploader_pod(name: &str, pvc_name: &str, cmd: &str) -> Pod {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "labels": { "seeds.ztest.io/uploader-for": pvc_name },
        },
        "spec": {
            "restartPolicy": "Never",
            "volumes": [{
                "name": "seed",
                "persistentVolumeClaim": { "claimName": pvc_name }
            }],
            "containers": [{
                "name": "uploader",
                "image": detect_uploader_image(),
                "command": ["sh", "-c", cmd],
                "stdin": true,
                "stdinOnce": true,
                "volumeMounts": [{ "name": "seed", "mountPath": "/seed" }],
            }],
        }
    });
    serde_json::from_value(body).expect("static manifest")
}

// ─────────────────────────── snapshot + waits ───────────────────────

async fn create_volume_snapshot(client: &Client, pvc_name: &str) -> Result<(), EnvError> {
    let snap_gvk = volume_snapshot_gvk();
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &snap_gvk);
    let body = json!({
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": { "name": pvc_name },
        "spec": {
            "source": { "persistentVolumeClaimName": pvc_name },
            "volumeSnapshotClassName": detect_snapshot_class(),
        }
    });
    let snap: DynamicObject = serde_json::from_value(body).expect("static manifest");
    match api.create(&PostParams::default(), &snap).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(env_err(e)),
    }
}

async fn wait_pvc_ready(client: &Client, pvc_name: &str) -> Result<(), EnvError> {
    poll(WAIT_BUDGET, || async {
        pvc_is_ready(client, pvc_name).await
    })
    .await
}

async fn wait_snapshot_ready(client: &Client, snap_name: &str) -> Result<(), EnvError> {
    let snap_gvk = volume_snapshot_gvk();
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &snap_gvk);
    poll(WAIT_BUDGET, || async {
        let snap = match api.get_opt(snap_name).await.map_err(env_err)? {
            Some(s) => s,
            None => return Ok::<bool, EnvError>(false),
        };
        Ok(snap.data["status"]["readyToUse"].as_bool().unwrap_or(false))
    })
    .await
}

/// Tiny polling helper. Returns once the predicate yields `Ok(true)` or
/// budgets out. Errors from the predicate propagate immediately.
async fn poll<F, Fut>(budget: Duration, mut f: F) -> Result<(), EnvError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool, EnvError>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if f().await? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(EnvError::NotReady {
                component: "seed materialize".into(),
                elapsed: budget,
            });
        }
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
}

/// True once a terminal phase is reached, success or failure. We then read
/// the phase via [`pod_phase`] to tell the two apart, so a failed uploader
/// surfaces its logs instead of waiting out the budget.
fn is_pod_finished() -> impl Condition<Pod> {
    |p: Option<&Pod>| {
        matches!(
            p.and_then(|p| p.status.as_ref())
                .and_then(|s| s.phase.as_deref()),
            Some("Succeeded") | Some("Failed")
        )
    }
}

/// Current `status.phase` of a pod, if it can be read.
async fn pod_phase(pods: &Api<Pod>, name: &str) -> Option<String> {
    pods.get_opt(name)
        .await
        .ok()
        .flatten()
        .and_then(|p| p.status)
        .and_then(|s| s.phase)
}

// ─────────────────────────── config knobs ───────────────────────────

fn detect_uploader_image() -> String {
    // Needs `sh`, `tar`, and the decompressor matching the archive's
    // compression (see `uploader_cmd`). The default ships `sh`/`tar`/`gzip`
    // but not `xz`/`zstd`, so gzip archives work out of the box; for xz or
    // zstd archives set `ZAINO_UPLOADER_IMAGE` to an image bundling those.
    std::env::var("ZAINO_UPLOADER_IMAGE").unwrap_or_else(|_| "ubuntu:24.04".into())
}
fn detect_storage_class() -> String {
    std::env::var("ZAINO_SEED_STORAGECLASS").unwrap_or_else(|_| "rook-ceph-block-archive".into())
}
fn detect_snapshot_class() -> String {
    std::env::var("ZAINO_VOLUMESNAPSHOTCLASS").unwrap_or_else(|_| "ceph-rbd-snapclass".into())
}
fn detect_seed_size() -> String {
    std::env::var("ZAINO_SEED_SIZE").unwrap_or_else(|_| "32Gi".into())
}
