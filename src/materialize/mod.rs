//! Bring a `seed-{sha8}` PVC into existence and populate it from the snapshot
//! bucket — the master copy every test's mount is cloned from.
//!
//! Two entry points, and which one you may call is the whole design:
//!
//! - [`provision_seed`] — the *parent* side (`ztest run`'s preflight). Creates
//!   the PVC, runs a puller Job to fill it, snapshots it. Needs bucket
//!   credentials, and is the only thing here that does.
//! - [`await_seed`] — the *test* side (`TestEnv::build`). Waits for a seed that
//!   preflight already published and resolves its snapshot handle. Needs
//!   nothing but the OID baked into the handle.
//!
//! # Why the split exists
//!
//! There used to be one entry point that did both, and it re-derived the seed's
//! identity by hashing the source file. That works on a laptop and nowhere
//! else. Under on-cluster compile the caller is a runner pod: no checkout, no
//! archive, no credentials — so it failed on every seeded test with
//! "does not exist", naming a path that had only ever existed inside a build
//! container. Identity now travels as the OID, and only the side that holds
//! credentials moves bytes.
//!
//! # The pull
//!
//! 1. Get-or-create the seed PVC. A 409 means somebody else won the race, so
//!    fall through to wait-for-ready.
//! 2. If we created it (or it exists but isn't `ready=true`), launch a puller
//!    Job: a one-shot pod that `curl`s a presigned URL for `lfs/<oid>` and
//!    pipes it into `tar -x -C /seed` (or `cat > /seed/blob` for a file seed).
//!    The bytes go R2 → node. Nothing streams through this process — which is
//!    why the puller has to *say* how far it has got for the console to draw a
//!    bar, and why [`progress`] exists.
//! 3. Label the PVC `seeds.ztest.io/ready=true` and create the paired
//!    `VolumeSnapshot`. From here `seeds::read_seed_handle` resolves the CSI
//!    snapshot handle, and `bind_seed` gives each pod a writable clone
//!    off the master.
//!
//! Race losers poll the PVC for the `ready=true` label, then poll the snapshot
//! for `status.readyToUse`. No leader election; the Job's name is the lock.

use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::Client;
use kube::api::{Api, DynamicObject, Patch, PatchParams, PostParams};
use kube::runtime::wait::{Condition, await_condition};
use serde_json::json;

use crate::EnvError;
use crate::error::env_err;
use crate::inventory::SeedEntry;
use crate::seeds::{self, SEEDS_NAMESPACE, SeedHandle, volume_snapshot_gvk};
use crate::storage::{self, r2::Bucket};

pub(crate) mod progress;

pub(crate) use progress::{SeedProgress, Silent};

const WAIT_INTERVAL: Duration = Duration::from_secs(2);
const WAIT_BUDGET: Duration = Duration::from_secs(300);

/// Floor for the puller's own budget: scheduling, image pull, and the fixed
/// costs that don't scale with the payload.
const PULL_BUDGET_FLOOR: Duration = Duration::from_secs(300);

/// Pessimistic sustained throughput for transfer-plus-extract, in bytes per
/// second. Deliberately low — this sets a *deadline*, not an expectation, and
/// a budget that tracks the fast path turns a slow cluster into a spurious
/// failure. Override with `ZTEST_SEED_THROUGHPUT_MIB_S` when a cluster is
/// known to be slower (a loaded local kind node) or faster (CI with local SSD).
///
/// Now that the transfer is R2 → node rather than R2 → laptop → apiserver →
/// node, the real rate is an order of magnitude higher; this stays pessimistic
/// because a deadline that tracks the fast path is how a slow cluster turns
/// into a spurious failure.
const DEFAULT_THROUGHPUT_BYTES_PER_SEC: u64 = 8 * 1024 * 1024;

/// How long a puller's presigned URL stays valid. Long enough to cover
/// scheduling, image pull and the transfer itself for the largest artifact;
/// short enough that a leaked manifest is not a standing grant.
const PRESIGN_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Wall-clock budget for a puller handling `bytes`.
///
/// A flat constant is wrong in both directions at once: 300 s is generous for
/// a 100 MB regtest cache and nowhere near enough for a 9.7 GB chain snapshot
/// to finish extracting, where it surfaces as an opaque "did not finish"
/// rather than "this payload is large". Deriving it from the size the manifest
/// already recorded makes both cases correct without a per-call-site knob.
fn pull_budget(bytes: u64) -> Duration {
    let throughput = std::env::var("ZTEST_SEED_THROUGHPUT_MIB_S")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|mib| mib * 1024 * 1024)
        .unwrap_or(DEFAULT_THROUGHPUT_BYTES_PER_SEC)
        .max(1);
    PULL_BUDGET_FLOOR + Duration::from_secs(bytes / throughput)
}

/// Publish a seed: get-or-create its PVC, fill it from the bucket, snapshot it.
///
/// The parent-side entry point, driven from the preflight resource graph
/// (`resource::impls::seed`). Idempotent and race-safe; the happy path on a
/// warm cluster is two `GET`s and no Job at all.
///
/// This is the only function in ztest that needs bucket credentials.
pub async fn provision_seed(
    client: &Client,
    seed: &SeedEntry,
    progress: &dyn SeedProgress,
) -> Result<SeedHandle, EnvError> {
    let pvc_name = format!("seed-{}", storage::seed_sha8(&seed.oid));

    // Fail fast on a cluster that can't back seeds at all, rather than
    // creating an unschedulable PVC and polling out the full `WAIT_BUDGET`.
    // The classic case is a stock kind cluster missing CSI snapshot
    // support; the fix is a single `ztest setup`.
    progress.stage("checking seed support");
    check_seed_support(client, &seed.name).await?;

    ensure_seeds_namespace(client).await?;

    progress.stage("creating seed volume");
    let we_created = create_seed_pvc(client, &pvc_name, &seed.name, progress).await?;
    if we_created || !pvc_is_ready(client, &pvc_name).await? {
        tracing::info!(pvc = %pvc_name, archive = %seed.name, "materializing seed PVC");
        match try_materialize(client, &pvc_name, seed, progress).await {
            Ok(Ok(())) => mark_ready(client, &pvc_name).await?,
            Ok(Err(e)) => return Err(e),
            // Another process is materializing; wait it out in the poll
            // loops below.
            Err(InFlight) => {
                progress.stage("waiting on another run's pull");
                tracing::debug!(pvc = %pvc_name, "seed materialization in flight elsewhere; waiting");
            }
        }
    }

    wait_pvc_ready(client, &pvc_name).await?;

    // Unconditional, and deliberately *after* the readiness wait rather than
    // inside the branch that filled the PVC.
    //
    // After, because `ready=true` is what tells us the bytes are all in — on the
    // `InFlight` path another process is still pulling, and snapshotting before
    // its wait clears would capture a half-filled volume.
    //
    // Unconditional, because a seed is published only when its PVC *and* its
    // snapshot exist, while `ready=true` records just the first. A PVC that
    // survives its snapshot — pruned, or deleted by hand — otherwise parks every
    // future run on `wait_snapshot_ready` for the full budget and then fails,
    // with nothing in the flow able to create the thing being waited for. The
    // create is 409-tolerant, so re-running it on the warm path costs one GET.
    progress.stage("snapshotting");
    create_volume_snapshot(client, &pvc_name).await?;
    progress.stage("waiting for snapshot");
    wait_snapshot_ready(client, &pvc_name).await?;
    seeds::read_seed_handle(client, &seed.name, storage::seed_sha8(&seed.oid)).await
}

/// Resolve a seed preflight already published.
///
/// The test-side entry point. It waits and reads; it never creates a PVC, runs
/// a Job, or touches the bucket — a runner pod has no credentials and no
/// checkout, and this is the function that makes that a design property rather
/// than a latent failure.
///
/// A seed that is absent here is a *preflight* bug, not a transient condition,
/// so the wait is bounded and the error says which declaration is missing
/// rather than timing out anonymously.
pub async fn await_seed(
    client: &Client,
    handle: crate::ArchiveHandle,
) -> Result<SeedHandle, EnvError> {
    let sha8 = storage::seed_sha8(handle.oid());
    let pvc_name = format!("seed-{sha8}");
    if !pvc_exists(client, &pvc_name).await? {
        return Err(EnvError::ArchiveMaterializeFailed {
            archive: handle.name().to_string(),
            reason: format!(
                "seed {pvc_name} was never provisioned. A test may only mount an archive it \
                 declares: add `#[ztest::needs({})]` to this test so preflight provisions \
                 the seed before the run starts.",
                handle.name()
            ),
        });
    }
    wait_pvc_ready(client, &pvc_name).await?;
    wait_snapshot_ready(client, &pvc_name).await?;
    seeds::read_seed_handle(client, handle.name(), sha8).await
}

// ─────────────────────────── capability preflight ───────────────────

/// Verify the cluster can actually back a seed before we create one.
/// Without this, a stock kind cluster (no CSI snapshot support) accepts an
/// unschedulable PVC and we burn the whole [`WAIT_BUDGET`] before failing
/// with an opaque timeout. Here we surface the real cause (a missing
/// StorageClass or VolumeSnapshotClass) in milliseconds, with a pointer to
/// the fix.
async fn check_seed_support(client: &Client, archive: &str) -> Result<(), EnvError> {
    use k8s_openapi::api::storage::v1::StorageClass;

    let sc_name = detect_storage_class();
    let sc_api: Api<StorageClass> = Api::all(client.clone());
    let sc_missing = match sc_api.get_opt(&sc_name).await {
        Ok(opt) => opt.is_none(),
        Err(_) => true,
    };
    if sc_missing {
        return Err(unsupported(
            archive,
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
            archive,
            format!("VolumeSnapshotClass `{vsc_name}` / CSI snapshot support not found"),
        ));
    }
    Ok(())
}

/// Build the "this cluster can't back seeds" error with a fix pointer.
fn unsupported(archive: &str, what: String) -> EnvError {
    EnvError::ArchiveMaterializeFailed {
        archive: archive.to_string(),
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

async fn create_seed_pvc(
    client: &Client,
    pvc_name: &str,
    archive: &str,
    progress: &dyn SeedProgress,
) -> Result<bool, EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    if let Some(existing) = api.get_opt(pvc_name).await.map_err(env_err)? {
        if existing.metadata.deletion_timestamp.is_none() {
            return Ok(false);
        }
        // A PVC under deletion is not a seed to adopt: its data is on the way
        // out, and it still carries whatever `ready=true` it was labelled with,
        // so believing it sends the run on to wait for a snapshot of a volume
        // that is being destroyed. Its name is also not reusable until it is
        // actually gone, so the only correct move is to wait it out.
        progress.stage("clearing terminating volume");
        await_pvc_gone(client, pvc_name, archive).await?;
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
            "resources": { "requests": { "storage": seed_size() } },
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

/// Wait for a `Terminating` seed PVC to actually disappear, so its name is free.
///
/// The error is the whole point of the bounded wait. A seed PVC that will not
/// finalize is essentially always pinned by `kubernetes.io/pvc-protection` on
/// behalf of a pod that still mounts it, and the pods that outlive their own
/// Job are exactly the ones nothing else will reap. A bare timeout sends the
/// reader looking at CSI and the storage class; naming the holders points at the
/// one `kubectl delete` that fixes it.
async fn await_pvc_gone(client: &Client, pvc_name: &str, archive: &str) -> Result<(), EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let deadline = tokio::time::Instant::now() + WAIT_BUDGET;
    loop {
        if api.get_opt(pvc_name).await.map_err(env_err)?.is_none() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let holders = pvc_holders(client, pvc_name).await;
            let blame = if holders.is_empty() {
                "no pod in the namespace still references it, so the block is its \
                 CSI driver rather than a reference"
                    .to_string()
            } else {
                format!(
                    "still referenced by {} — delete them to release the finalizer: \
                     `kubectl -n {SEEDS_NAMESPACE} delete pod {}`",
                    holders.join(", "),
                    holders.join(" "),
                )
            };
            return Err(EnvError::ArchiveMaterializeFailed {
                archive: archive.to_string(),
                reason: format!(
                    "seed volume {pvc_name} has been Terminating for over {}s and its name \
                     cannot be reused until it is gone; {blame}",
                    WAIT_BUDGET.as_secs()
                ),
            });
        }
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
}

/// Pods in the seeds namespace still mounting `pvc_name` — the holders of its
/// `pvc-protection` finalizer.
async fn pvc_holders(client: &Client, pvc_name: &str) -> Vec<String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let Ok(list) = pods.list(&Default::default()).await else {
        return Vec::new();
    };
    list.items
        .into_iter()
        .filter(|p| {
            p.spec
                .iter()
                .flat_map(|s| s.volumes.iter().flatten())
                .any(|v| {
                    v.persistent_volume_claim
                        .as_ref()
                        .is_some_and(|c| c.claim_name == pvc_name)
                })
        })
        .filter_map(|p| p.metadata.name)
        .collect()
}

/// Whether the seed PVC exists at all.
///
/// Distinct from [`pvc_is_ready`]: absent means *nobody ever provisioned this
/// seed*, which is a preflight bug worth naming, while present-but-not-ready
/// means a puller is running and waiting is correct.
async fn pvc_exists(client: &Client, pvc_name: &str) -> Result<bool, EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    api.get_opt(pvc_name)
        .await
        .map(|o| o.is_some())
        .map_err(env_err)
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

// ─────────────────────────── puller Job ─────────────────────────────

/// Sentinel error returned by `materialize` when the puller Job already exists
/// (another actor is filling this seed right now). Collapses to a "wait" branch
/// in [`provision_seed`].
struct InFlight;

async fn try_materialize(
    client: &Client,
    pvc_name: &str,
    seed: &SeedEntry,
    progress: &dyn SeedProgress,
) -> Result<Result<(), EnvError>, InFlight> {
    match materialize(client, pvc_name, seed, progress).await {
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

/// Fill `pvc_name` from the bucket with a one-shot puller Job.
///
/// The Job — rather than a bare Pod — is what makes a transient bucket or
/// network error survivable: `backoffLimit` retries the pod, and the Job's own
/// name is the concurrency lock that a 409 reports. Nothing is streamed from
/// here; the pod holds a presigned URL and does its own transfer.
async fn materialize(
    client: &Client,
    pvc_name: &str,
    seed: &SeedEntry,
    progress: &dyn SeedProgress,
) -> Result<(), MaterializeErr> {
    let job_name = format!("puller-{}", pvc_name.trim_start_matches("seed-"));
    let jobs: Api<Job> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let pods: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);

    // Presign before creating the Job: a bucket that isn't configured, or an
    // object that isn't there, should fail here — named, in milliseconds —
    // rather than as a puller that retries a 403 until its budget expires.
    progress.stage("presigning blob");
    let bucket = Bucket::resolve().map_err(|e| storage_fatal(&seed.name, e))?;
    if !bucket
        .has(&seed.oid, seed.size)
        .await
        .map_err(|e| storage_fatal(&seed.name, e))?
    {
        return Err(MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
            archive: seed.name.clone(),
            reason: format!(
                "the bucket has no blob for oid {} at the manifest's size ({} bytes). The \
                 archive is committed but was never uploaded: run `git lfs push` for it.",
                seed.oid, seed.size
            ),
        }));
    }
    let url = bucket
        .presigned_get(&seed.oid, PRESIGN_TTL)
        .await
        .map_err(|e| storage_fatal(&seed.name, e))?;

    let cmd = puller_cmd(seed).map_err(|e| storage_fatal(&seed.name, e))?;
    let body = puller_job(&job_name, pvc_name, &cmd, &url);
    match jobs.create(&PostParams::default(), &body).await {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 409 => {
            // A Job of this name already exists, and the two reasons are
            // opposite. Either another actor is pulling right now — wait it out
            // — or a *previous* run's puller finished and was left behind. We
            // only delete on success, so a leftover is specifically a failed
            // one: it will never make the PVC ready, and treating it as
            // in-flight means every subsequent run burns its whole budget
            // waiting on a corpse, then reports a timeout that says nothing
            // about the real failure. Reap it and try once more.
            if !reap_finished_job(&jobs, &job_name).await {
                return Err(MaterializeErr::InFlight);
            }
            jobs.create(&PostParams::default(), &body)
                .await
                .map_err(|e| MaterializeErr::Fatal(env_err(e)))?;
        }
        Err(e) => return Err(MaterializeErr::Fatal(env_err(e))),
    }

    // Wait for the Job to reach a terminal condition. This is the wait that has
    // to scale: a multi-GB snapshot does not transfer *and* extract inside a
    // flat 300 s, and sizing the budget from the manifest's `size_bytes` is what
    // lets a deep fixture land at all rather than surfacing as an opaque
    // "did not finish".
    //
    // The watcher rides alongside rather than in front: it only ever *reports*,
    // so the Job's terminal condition stays the sole verdict on the pull. It
    // never resolves, so this `select!` is decided by the wait, not the report.
    let budget = pull_budget(seed.size);
    let finished = tokio::time::timeout(
        budget,
        await_condition(jobs.clone(), &job_name, is_job_finished()),
    );
    let outcome = tokio::select! {
        r = finished => r,
        () = progress::watch_puller(&pods, &job_name, seed.size, progress) => {
            unreachable!("watch_puller parks rather than resolving")
        }
    };
    outcome
        .map_err(|_| puller_stuck(&seed.name, &job_name, budget))?
        .map_err(env_err)?;
    progress.finalizing();

    if !job_succeeded(&jobs, &job_name).await {
        let logs = job_logs(&pods, &job_name).await;
        return Err(MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
            archive: seed.name.clone(),
            reason: format!("puller job failed: {}", logs.trim()),
        }));
    }
    // Best-effort cleanup; janitor backstops. Background propagation so the
    // Job's pods go with it: the server's default for a Job is `Orphan`, and an
    // orphaned puller holds the seed PVC's `pvc-protection` finalizer for as
    // long as it exists — which turns any later delete of that PVC into a
    // permanent `Terminating`, with no owner left to reap the pod that pins it.
    let _ = jobs
        .delete(&job_name, &kube::api::DeleteParams::background())
        .await;
    Ok(())
}

/// Error for an uploader that never reached a terminal state within the
/// budget (typically a wedged image pull).
fn puller_stuck(archive: &str, pod: &str, budget: Duration) -> MaterializeErr {
    MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
        archive: archive.to_string(),
        reason: format!(
            "puller job {pod} did not finish within {budget:?} \
             (check `kubectl -n {SEEDS_NAMESPACE} describe job {pod}` for image-pull/scheduling \
             errors; if the payload is simply large and the cluster slow, raise \
             ZTEST_SEED_THROUGHPUT_MIB_S)"
        ),
    })
}

/// The shell command the puller runs to populate `/seed` from the bucket.
///
/// `curl --fail` so an HTTP error is a non-zero exit rather than an error page
/// piped into `tar`; `pipefail` so a mid-transfer failure fails the pod even
/// though `tar` exits 0 on a truncated-but-well-formed prefix. Archives get an
/// explicit decompression flag (GNU tar can't auto-detect on a non-seekable
/// pipe), whose decompressor must exist in the puller image — see
/// [`detect_puller_image`].
///
/// The URL arrives via the `SEED_URL` env var, never on the command line: a
/// presigned URL is a bearer credential, and argv is world-readable to anything
/// that can read the pod spec or `/proc`.
///
/// The payload is followed by [`NORMALIZE_MODES`], chained with `&&` so it never
/// runs over a failed transfer, and metered by [`METER`] so the parent can draw
/// a byte bar — see [`progress`] for why the meter is `dd` and what the
/// [`LINE_DELIMIT`] wrapper is for.
fn puller_cmd(seed: &SeedEntry) -> Result<String, storage::StorageError> {
    let prelude = "set -o pipefail";
    let fetch = "curl --fail --silent --show-error --location \"$SEED_URL\"";
    let payload = match seed.payload {
        crate::inventory::SeedPayload::Archive => {
            let compression = storage::compression_from_name(&seed.name).ok_or_else(|| {
                storage::StorageError::UnknownCompression {
                    name: seed.name.clone(),
                }
            })?;
            format!(
                "{fetch} | {METER} | tar {}-xf - -C /seed && {NORMALIZE_MODES}",
                compression.tar_flag()
            )
        }
        // File path inside the PVC is always `/seed/blob`; `read_seed_handle`
        // doesn't care, only the consumer's volumeMount path does.
        crate::inventory::SeedPayload::File => {
            format!("{fetch} | {METER} > /seed/blob && {NORMALIZE_MODES}")
        }
    };
    // Group-then-pipe rather than appending to the payload's own pipeline: the
    // meter writes to *stderr*, and only a group can carry every command's
    // stderr (curl's and tar's diagnostics included) into one delimiter pass.
    // `pipefail` spans both pipelines, so the payload's exit status still
    // decides the pod's — `tr` cannot mask a failed transfer.
    Ok(format!(
        "{prelude}; {{ {payload} ; }} 2>&1 | {LINE_DELIMIT}"
    ))
}

/// Meters the transfer mid-pipe, announcing a running byte total to stderr once
/// a second, which [`progress::watch_puller`] turns into the row's `%` bar.
/// Pure pass-through: it never changes the payload and its own exit status is
/// always 0, so it cannot mask `curl`'s or `tar`'s.
const METER: &str = "dd bs=1M status=progress";

/// Rewrite [`METER`]'s `\r` record separators to `\n`, line-buffered.
///
/// Both halves are load-bearing. A CRI log writer only flushes a partial record
/// when its buffer fills, so `\r`-separated output would reach the parent in
/// ~16 KiB batches — minutes of apparent silence on a slow pull. And `tr`'s own
/// stdout is a pipe, hence block-buffered by stdio, which would reintroduce the
/// same stall one stage later; `stdbuf -oL` is what makes it per-record.
const LINE_DELIMIT: &str = r#"stdbuf -oL tr '\r' '\n'"#;

/// Mirror each entry's owner bits onto its group, so a seed is usable by a
/// consumer running under any uid.
///
/// The puller runs under `restricted-v2` with `capabilities: drop ALL`, so it
/// holds no `CAP_CHOWN` and `tar` cannot restore the archive's ownership: every
/// entry lands owned by the puller's namespace uid. Consumers run in their own
/// namespaces, which OpenShift gives their own uid ranges, so ownership can
/// never match. Nor would pinning a uid help — one seed is mounted at once by
/// pods that must run as *different* uids (zebrad's image is 10001, zainod's is
/// 1000), so no single owner serves them all.
///
/// The group is the way through, and the volume hands us one for free: the CSI
/// volume root is `2777` setgid gid 0, so every entry written under it inherits
/// [`SEED_GID`]. Granting the group what the owner has therefore grants any
/// consumer that carries `SEED_GID` exactly the access the puller had — which is
/// a property a consumer must *declare*, not one it gets by default. See
/// [`SEED_GID`].
///
/// Read access alone would not be enough. Zebra writes its `version` file
/// through `atomic_write`, which is tempfile-backed and so lands at mode `0600`
/// — the one non-`0644` file in the tree, and the one that made this surface, as
/// a panic on `.expect("unable to read database format version file")` (`EACCES`
/// is not `NotFound`, so it does not take the "no database yet" path). But the
/// db *directories* arrive `2755`, and RocksDB must create `LOCK`, `MANIFEST`
/// and new SSTs inside them, so the fix has to cover write on directories too.
///
/// `fsGroup` cannot do this job: on this storage class kubelet's ownership pass
/// never runs — the volume root stays gid 0 even for a pod that requests an
/// fsGroup, which is the observable signature of a CSIDriver declaring
/// `fsGroupPolicy: None`.
///
/// Runs once per seed, before the snapshot is taken, so every clone inherits it.
/// `/seed` itself is skipped (`-mindepth 1`): it is root-owned, so chmod'ing it
/// would fail and take the Job down with it. `lost+found` is pruned for the same
/// reason.
const NORMALIZE_MODES: &str =
    "find /seed -mindepth 1 -name lost+found -prune -o -exec chmod g=u {} +";

/// The group every entry in a materialized seed belongs to, and therefore the
/// group a pod must hold to make [`NORMALIZE_MODES`] pay off.
///
/// Gid 0 is not a choice: it is what the setgid CSI volume root stamps on
/// everything written beneath it, and the puller (`capabilities: drop ALL`, no
/// `CAP_CHOWN`) cannot change it.
///
/// A pod does *not* acquire this group by default. Kubernetes takes a
/// container's primary gid from the image's `USER` when nothing pins
/// `runAsGroup`, so an image whose user has a non-zero primary group — zainod's
/// does — runs outside the seed's group no matter what `runAsUser` or `fsGroup`
/// say, and every mode bit `NORMALIZE_MODES` set is unreachable. That is why
/// mounting a seed and reading it are separate declarations: any pod that mounts
/// one must also list this gid in
/// [`PodSpec::supplemental_groups`](crate::manifest::PodSpec::supplemental_groups),
/// which [`seed_groups`](crate::backends::seed_groups) derives from the
/// component's own restore source.
pub(crate) const SEED_GID: i64 = 0;

/// A storage failure (unconfigured or unreachable bucket) as a fatal
/// materialize error tagged with the archive it was resolving.
fn storage_fatal(archive: &str, err: storage::StorageError) -> MaterializeErr {
    MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
        archive: archive.to_string(),
        reason: err.to_string(),
    })
}

/// The one-shot Job that fills a seed PVC from the bucket.
///
/// `backoffLimit: 2` — a presign is valid for [`PRESIGN_TTL`], so a retry after
/// a transient network or R2 error reuses the same URL and simply starts over.
/// Retrying is safe because the target is a fresh PVC and `tar -x` over a
/// partial tree is idempotent for our purposes: the Job is not marked complete
/// until one pod exits 0 having written the whole stream.
fn puller_job(name: &str, pvc_name: &str, cmd: &str, url: &str) -> Job {
    // Guaranteed QoS (requests == limits) at the fixed puller footprint, via the
    // single QoS lowering — this pod moves seed bytes and must never be
    // BestEffort.
    let (cpu, mem) = crate::qos::build::UPLOADER.guaranteed_cpu_mem("seed puller pod");
    let body = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "labels": { "seeds.ztest.io/puller-for": pvc_name },
        },
        "spec": {
            "backoffLimit": 2,
            "template": {
                "metadata": {
                    "labels": { "seeds.ztest.io/puller-for": pvc_name },
                },
                "spec": {
                    "restartPolicy": "Never",
                    "volumes": [{
                        "name": "seed",
                        "persistentVolumeClaim": { "claimName": pvc_name }
                    }],
                    "containers": [{
                        "name": "puller",
                        "image": detect_puller_image(),
                        "command": ["sh", "-c", cmd],
                        "env": [{ "name": "SEED_URL", "value": url }],
                        "volumeMounts": [{ "name": "seed", "mountPath": "/seed" }],
                        "resources": {
                            "requests": { "cpu": cpu, "memory": mem },
                            "limits": { "cpu": cpu, "memory": mem },
                        },
                    }],
                }
            }
        }
    });
    serde_json::from_value(body).expect("static manifest")
}

/// Delete a puller Job that has already reached a terminal state, so its name
/// can be reused. Returns whether one was reaped.
///
/// `false` means the Job is still running and the caller should wait for it
/// rather than interfering with another actor's pull.
async fn reap_finished_job(jobs: &Api<Job>, name: &str) -> bool {
    let Ok(Some(job)) = jobs.get_opt(name).await else {
        // Vanished between the 409 and now — whoever owned it cleaned up, so
        // the name is free.
        return true;
    };
    if !is_job_finished().matches_object(Some(&job)) {
        return false;
    }
    // Background propagation so the Job's pods go with it instead of being
    // orphaned into the namespace.
    let dp = kube::api::DeleteParams::background();
    if jobs.delete(name, &dp).await.is_err() {
        return false;
    }
    // The name is not reusable until the object is actually gone; a create
    // racing the delete just 409s again.
    let deadline = tokio::time::Instant::now() + WAIT_BUDGET;
    while tokio::time::Instant::now() < deadline {
        match jobs.get_opt(name).await {
            Ok(None) => return true,
            Ok(Some(_)) => tokio::time::sleep(WAIT_INTERVAL).await,
            Err(_) => return false,
        }
    }
    false
}

/// Terminal-state condition for a puller Job: `Complete` or `Failed`.
fn is_job_finished() -> impl Condition<Job> {
    |obj: Option<&Job>| {
        obj.and_then(|j| j.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .is_some_and(|cs| {
                cs.iter().any(|c| {
                    matches!(c.type_.as_str(), "Complete" | "Failed") && c.status == "True"
                })
            })
    }
}

/// Whether the Job reached `Complete` (as opposed to `Failed`).
async fn job_succeeded(jobs: &Api<Job>, name: &str) -> bool {
    jobs.get_opt(name)
        .await
        .ok()
        .flatten()
        .and_then(|j| j.status)
        .and_then(|s| s.succeeded)
        .is_some_and(|n| n > 0)
}

/// Logs of the Job's most recent pod, for a failure message. A Job owns its
/// pods indirectly, so they're found by the label the template stamps.
async fn job_logs(pods: &Api<Pod>, job_name: &str) -> String {
    let lp = kube::api::ListParams::default().labels(&format!("job-name={job_name}"));
    let Ok(list) = pods.list(&lp).await else {
        return "<pod list unavailable>".to_string();
    };
    let Some(pod) = list.items.into_iter().next_back() else {
        return "<no puller pod found>".to_string();
    };
    let Some(name) = pod.metadata.name else {
        return "<puller pod has no name>".to_string();
    };
    pods.logs(&name, &Default::default())
        .await
        .unwrap_or_else(|e| format!("<logs unavailable: {e}>"))
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

// ─────────────────────────── config knobs ───────────────────────────

fn detect_puller_image() -> String {
    // `puller_cmd` needs `curl`, GNU `tar`, and the decompressor matching the
    // archive — `zstd` for every chain fixture we ship. fedora is the smallest
    // widely-available base that carries all of them already.
    //
    // Installing them at pod start instead was tried and cannot work: under
    // OpenShift's `restricted-v2` SCC the pod runs as an arbitrary non-root UID,
    // so the package manager fails with "Unable to lock database: Permission
    // denied" — and then `curl` is missing and busybox `tar` rejects `--zstd`,
    // which is what the failure actually looked like. The tools have to be in
    // the image.
    std::env::var("ZTEST_PULLER_IMAGE").unwrap_or_else(|_| "fedora:40".into())
}
fn detect_storage_class() -> String {
    std::env::var("ZAINO_SEED_STORAGECLASS").unwrap_or_else(|_| "rook-ceph-block-archive".into())
}
fn detect_snapshot_class() -> String {
    std::env::var("ZAINO_VOLUMESNAPSHOTCLASS").unwrap_or_else(|_| "ceph-rbd-snapclass".into())
}
/// Requested size of a seed PVC, and so the floor for every PVC cloned from
/// its snapshot. `seeds::read_seed_handle` reuses it as the fallback when a
/// driver reports no `restoreSize`, which is why it isn't private to this file.
pub(crate) fn seed_size() -> String {
    std::env::var("ZAINO_SEED_SIZE").unwrap_or_else(|_| "32Gi".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::inventory::SeedPayload;

    fn seed(name: &str, payload: SeedPayload) -> SeedEntry {
        SeedEntry {
            name: name.to_string(),
            oid: "a".repeat(64),
            size: 4096,
            payload,
        }
    }

    /// Every property the parent's byte bar depends on, pinned together: the
    /// meter must sit *between* the fetch and the consumer (so it counts the
    /// bytes that actually arrive), and the delimiter must wrap the whole group
    /// (so the meter's stderr, not just stdout, reaches the pod log per-record).
    #[test]
    fn the_archive_command_meters_the_transfer_and_delimits_its_records() {
        let cmd = puller_cmd(&seed("chain.tar.zst", SeedPayload::Archive)).expect("known suffix");
        assert!(cmd.starts_with("set -o pipefail; { curl "), "{cmd}");
        assert!(
            cmd.contains(&format!("| {METER} | tar --zstd -xf - -C /seed")),
            "{cmd}"
        );
        assert!(
            cmd.ends_with(&format!("; }} 2>&1 | {LINE_DELIMIT}")),
            "{cmd}"
        );
    }

    #[test]
    fn a_file_seed_is_metered_the_same_way_before_landing_at_the_blob_path() {
        let cmd = puller_cmd(&seed("wallet.dat", SeedPayload::File)).expect("file needs no suffix");
        assert!(cmd.contains(&format!("| {METER} > /seed/blob")), "{cmd}");
        assert!(
            cmd.ends_with(&format!("; }} 2>&1 | {LINE_DELIMIT}")),
            "{cmd}"
        );
    }

    /// The meter added a second pipeline, and `tr` exits 0 over a failed
    /// transfer. `pipefail` is the only thing keeping the payload's status the
    /// pod's status, so it must precede both.
    #[test]
    fn pipefail_still_spans_the_command() {
        for payload in [SeedPayload::Archive, SeedPayload::File] {
            let name = match payload {
                SeedPayload::Archive => "chain.tar.zst",
                SeedPayload::File => "wallet.dat",
            };
            let cmd = puller_cmd(&seed(name, payload)).expect("valid seed");
            assert_eq!(cmd.find("set -o pipefail"), Some(0), "{cmd}");
        }
    }

    /// Mode normalization runs on the extracted tree, so it must stay chained
    /// behind the payload with `&&` rather than after the delimiter.
    #[test]
    fn normalization_stays_inside_the_metered_group() {
        let cmd = puller_cmd(&seed("chain.tar.zst", SeedPayload::Archive)).expect("known suffix");
        let normalize = cmd.find(NORMALIZE_MODES).expect("normalization is chained");
        assert!(
            normalize < cmd.find(LINE_DELIMIT).expect("delimiter"),
            "{cmd}"
        );
        assert!(cmd[..normalize].ends_with("-C /seed && "), "{cmd}");
    }
}
