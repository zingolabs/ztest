//! Bring a `seed-{sha8}-{driver}` PVC into existence and fill it from the snapshot bucket
//! — the master copy every test's mount clones from.
//!
//! - [`provision_seed`] = parent side (`ztest run` preflight): create PVC, run the
//!   puller Job, snapshot. Only thing here needing bucket credentials
//! - [`await_seed`] = test side (`TestEnv::build`): waits + resolves the snapshot
//!   handle from the baked-in OID, nothing else
//! - Identity travels as the OID, never a re-hashed source path (a runner pod has
//!   no checkout, no archive, no credentials)
//!
//! # The pull
//!
//! 1. Get-or-create the PVC (409 = lost the race → wait-for-ready)
//! 2. If created, or not `ready=true`, launch a puller Job: `curl` a presigned
//!    `lfs/<oid>` into `tar -x -C /seed` (`cat > /seed/blob` for a file seed).
//!    R2 → node, nothing through this process, hence [`progress`]
//! 3. Label `seeds.ztest.io/ready=true` + create the paired `VolumeSnapshot`, from
//!    which `seeds::read_seed_handle` resolves the handle and `bind_seed` clones per pod
//!
//! Race losers poll `ready=true`, then `status.readyToUse`. No leader election
//! (the Job's name is the lock)
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
use crate::progress::{Silent, StepProgress};
use crate::seeds::{self, SEEDS_NAMESPACE, SeedHandle, volume_snapshot_gvk};
use crate::storage::{self, r2::Bucket};

pub mod progress;

const WAIT_INTERVAL: Duration = Duration::from_secs(2);
const WAIT_BUDGET: Duration = Duration::from_secs(300);

/// Budget floor: scheduling, image pull, the costs not scaling with the payload
const PULL_BUDGET_FLOOR: Duration = Duration::from_secs(300);

/// Sustained transfer+extract throughput, B/s. Far below the real rate — a
/// *deadline*, not an expectation (a fast-path budget makes a slow cluster fail
/// spuriously). Override: `ZTEST_SEED_THROUGHPUT_MIB_S`
const DEFAULT_THROUGHPUT_BYTES_PER_SEC: u64 = 8 * 1024 * 1024;

/// Presigned-URL lifetime: covers schedule + image pull + the largest transfer,
/// short enough that a leaked manifest is no standing grant
const PRESIGN_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Wall-clock budget for `bytes`. Sized, not flat (a constant is at once generous
/// for a 100 MB cache and short for a 9.7 GB snapshot, which surfaces as an opaque
/// "did not finish")
fn pull_budget(bytes: u64) -> Duration {
    let throughput = std::env::var("ZTEST_SEED_THROUGHPUT_MIB_S")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|mib| mib * 1024 * 1024)
        .unwrap_or(DEFAULT_THROUGHPUT_BYTES_PER_SEC)
        .max(1);
    PULL_BUDGET_FLOOR + Duration::from_secs(bytes / throughput)
}

/// Publish a seed: get-or-create the PVC, fill from the bucket, snapshot.
///
/// - Parent-side, driven from the preflight graph (`resource::impls::seed`)
/// - Idempotent + race-safe, warm path = two `GET`s and no Job
/// - Sole function in ztest needing bucket credentials
pub async fn provision_seed(
    client: &Client,
    seed: &SeedEntry,
    progress: &dyn StepProgress,
) -> Result<SeedHandle, EnvError> {
    // Fail fast, not via an unschedulable PVC polled out to `WAIT_BUDGET`
    // (classic: a stock kind cluster with no CSI snapshot support)
    progress.note("checking seed support");
    check_seed_support(client, &seed.name).await?;

    let driver = selected_driver(client).await?;
    let pvc_name = storage::seed_pvc_name(&seed.oid, &driver);

    ensure_seeds_namespace(client).await?;

    progress.note("creating seed volume");
    let we_created = create_seed_pvc(client, &pvc_name, seed, progress).await?;
    if we_created || !pvc_is_ready(client, &pvc_name).await? {
        tracing::info!(pvc = %pvc_name, archive = %seed.name, "materializing seed PVC");
        match try_materialize(client, &pvc_name, seed, progress).await {
            Ok(Ok(())) => mark_ready(client, &pvc_name).await?,
            Ok(Err(e)) => return Err(e),
            // Another process materializing → wait it out in the polls below
            Err(InFlight) => {
                progress.note("waiting on another run's pull");
                tracing::debug!(pvc = %pvc_name, "seed materialization in flight elsewhere; waiting");
            }
        }
    }

    wait_pvc_ready(client, &pvc_name).await?;

    // After the readiness wait: `ready=true` is what says every byte is in, and on
    // the `InFlight` path snapshotting early captures a half-filled volume
    //
    // Unconditional: published = PVC *and* snapshot, but `ready=true` records only
    // the first. A PVC outliving its snapshot otherwise parks every future run on
    // `wait_snapshot_ready` with nothing able to create what it waits for
    // 409-tolerant, so the warm path costs one GET
    progress.note("snapshotting");
    create_volume_snapshot(client, &pvc_name).await?;
    wait_snapshot_ready(client, &pvc_name, progress).await?;
    seeds::read_seed_handle(client, &seed.name, &seed.oid, &driver).await
}

/// CSI driver this run's storage resolves to — the other half of a seed's identity
async fn selected_driver(client: &Client) -> Result<String, EnvError> {
    crate::storage_class::selected(client)
        .await
        .map(|s| s.provisioner.clone())
        .map_err(|reason| EnvError::Manifest { reason })
}

/// Resolve a preflight-published seed, test side.
///
/// - Waits and reads only: no PVC create, no Job, no bucket (runner pods hold nothing)
/// - Absent seed = a *preflight* bug, not a transient → bounded wait, error names
///   the missing declaration
pub async fn await_seed(client: &Client, handle: crate::Artifact) -> Result<SeedHandle, EnvError> {
    let driver = selected_driver(client).await?;
    let pvc_name = storage::seed_pvc_name(handle.oid, &driver);
    if !pvc_exists(client, &pvc_name).await? {
        return Err(EnvError::ArchiveMaterializeFailed {
            archive: handle.name.to_string(),
            reason: format!(
                "seed {pvc_name} was never provisioned. A test may only mount an archive it \
                 declares: add `#[ztest::needs({})]` to this test so preflight provisions \
                 the seed before the run starts.",
                handle.name
            ),
        });
    }
    wait_pvc_ready(client, &pvc_name).await?;
    // Test side: no console row (`NodeProgress::default` → nowhere)
    wait_snapshot_ready(client, &pvc_name, &Silent).await?;
    seeds::read_seed_handle(client, handle.name, handle.oid, &driver).await
}

// ─────────────────────────── capability preflight ───────────────────

/// Absent this, a snapshot-less cluster accepts an unschedulable PVC and burns
/// [`WAIT_BUDGET`] on an opaque timeout instead of naming the missing class
async fn check_seed_support(client: &Client, archive: &str) -> Result<(), EnvError> {
    // Same join as `ztest cluster check` (StorageClass whose provisioner a
    // VolumeSnapshotClass backs) → passing `check` cannot fail here
    crate::storage_class::selected(client)
        .await
        .map(|_| ())
        .map_err(|why| unsupported(archive, why))
}

fn unsupported(archive: &str, what: String) -> EnvError {
    EnvError::ArchiveMaterializeFailed {
        archive: archive.to_string(),
        reason: format!(
            // `cluster setup` installs no storage → naming it here loops the reader
            "{what} — this archive-backed test needs CSI snapshot support. \
             On a local kind cluster run `scripts/kind-storage.sh`; on a shared cluster \
             check that the seed StorageClass / VolumeSnapshotClass are installed."
        ),
    }
}

// ─────────────────────────── namespace + PVC ────────────────────────

async fn ensure_seeds_namespace(client: &Client) -> Result<(), EnvError> {
    use k8s_openapi::api::core::v1::Namespace;
    let api: Api<Namespace> = Api::all(client.clone());
    if api.get_opt(SEEDS_NAMESPACE).await.map_err(env_err)?.is_some() {
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
    seed: &SeedEntry,
    progress: &dyn StepProgress,
) -> Result<bool, EnvError> {
    let storage = crate::storage_class::selected(client)
        .await
        .map_err(|e| EnvError::Manifest { reason: e })?;
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    if let Some(existing) = api.get_opt(pvc_name).await.map_err(env_err)? {
        if existing.metadata.deletion_timestamp.is_none() {
            return Ok(false);
        }
        // Never adopt a deleting PVC: it still carries its `ready=true`, so believing
        // it means waiting on a snapshot of a volume being destroyed
        // Its name is unusable until it's gone → wait it out
        progress.note("clearing terminating volume");
        await_pvc_gone(client, pvc_name, &seed.name).await?;
    }
    // Both halves of the identity as labels: the name encodes them, but `snapshot
    // list` reads a driver without parsing it back out of a slug
    let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": pvc_name,
            "labels": {
                "seeds.ztest.io/sha": storage::seed_sha8(&seed.oid),
                "seeds.ztest.io/driver": crate::naming::slug(
                    &storage.provisioner, crate::naming::DNS_LABEL_MAX),
                "seeds.ztest.io/ready": "false",
            },
            "annotations": { "seeds.ztest.io/last_accessed_at": "now" },
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": crate::cluster_config::seed_size() } },
            "storageClassName": storage.class_name,
        }
    }))
    .expect("static manifest");
    match api.create(&PostParams::default(), &pvc).await {
        Ok(_) => Ok(true),
        // Lost the race
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
        Err(e) => Err(env_err(e)),
    }
}

/// Wait out a `Terminating` seed PVC, freeing its name.
///
/// - Error = the point of the bound: a stuck PVC is near-always pinned by
///   `pvc-protection` for a mounting pod, so naming holders points at the fix
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

/// Pods still mounting `pvc_name` = holders of its `pvc-protection` finalizer
async fn pvc_holders(client: &Client, pvc_name: &str) -> Vec<String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let Ok(list) = pods.list(&Default::default()).await else {
        return Vec::new();
    };
    list.items
        .into_iter()
        .filter(|p| {
            p.spec.iter().flat_map(|s| s.volumes.iter().flatten()).any(|v| {
                v.persistent_volume_claim.as_ref().is_some_and(|c| c.claim_name == pvc_name)
            })
        })
        .filter_map(|p| p.metadata.name)
        .collect()
}

/// Distinct from [`pvc_is_ready`]: absent = never provisioned (a preflight bug
/// worth naming), present-but-unready = a puller is running and waiting is right
async fn pvc_exists(client: &Client, pvc_name: &str) -> Result<bool, EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    api.get_opt(pvc_name).await.map(|o| o.is_some()).map_err(env_err)
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
    api.patch(pvc_name, &PatchParams::default(), &Patch::Merge(&patch)).await.map_err(env_err)?;
    Ok(())
}

// ─────────────────────────── puller Job ─────────────────────────────

/// Puller Job already exists (another actor filling this seed) → the "wait" branch
/// of [`provision_seed`]
struct InFlight;

async fn try_materialize(
    client: &Client,
    pvc_name: &str,
    seed: &SeedEntry,
    progress: &dyn StepProgress,
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
/// - Job, not a bare Pod: `backoffLimit` survives a transient bucket/network error
/// - Job name = the concurrency lock a 409 reports
/// - Nothing streams from here (the pod holds a presigned URL, transfers itself)
async fn materialize(
    client: &Client,
    pvc_name: &str,
    seed: &SeedEntry,
    progress: &dyn StepProgress,
) -> Result<(), MaterializeErr> {
    let job_name = format!("puller-{}", pvc_name.trim_start_matches("seed-"));
    let jobs: Api<Job> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let pods: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);

    // Presign before the Job: an unconfigured bucket or missing object fails here,
    // named, in ms, not as a puller retrying a 403 to its budget
    progress.note("presigning blob");
    let bucket = Bucket::resolve().map_err(|e| storage_fatal(&seed.name, e))?;
    if !bucket.has(&seed.oid, seed.size).await.map_err(|e| storage_fatal(&seed.name, e))? {
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
            // Two opposite causes: a live pull (wait) or a leftover from a previous
            // run. Deletes happen only on success, so a leftover is a *failed* puller
            // that will never make the PVC ready — treated as in-flight it burns every
            // later run's budget on a corpse. Reap and retry once
            if !reap_finished_job(&jobs, &job_name).await {
                return Err(MaterializeErr::InFlight);
            }
            jobs.create(&PostParams::default(), &body)
                .await
                .map_err(|e| MaterializeErr::Fatal(env_err(e)))?;
        }
        Err(e) => return Err(MaterializeErr::Fatal(env_err(e))),
    }

    // The wait that must scale: a multi-GB snapshot transfers *and* extracts, so the
    // budget comes from the manifest's `size_bytes`, not a flat 300 s
    //
    // Watcher rides alongside, never in front: it only reports, and never resolves,
    // so this `select!` is decided by the wait
    let budget = pull_budget(seed.size);
    let finished =
        tokio::time::timeout(budget, await_condition(jobs.clone(), &job_name, is_job_finished()));
    let outcome = tokio::select! {
        r = finished => r,
        () = progress::watch_puller(&pods, &job_name, seed.size, progress) => {
            unreachable!("watch_puller parks rather than resolving")
        }
    };
    outcome.map_err(|_| puller_stuck(&seed.name, &job_name, budget))?.map_err(env_err)?;
    progress.finalizing();

    if !job_succeeded(&jobs, &job_name).await {
        let logs = job_logs(&pods, &job_name).await;
        return Err(MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
            archive: seed.name.clone(),
            reason: format!("puller job failed: {}", logs.trim()),
        }));
    }
    // Best-effort, janitor backstops. Background propagation (server default for a
    // Job = `Orphan`, and an orphaned puller pins the PVC's `pvc-protection`
    // finalizer forever, with no owner left to reap it)
    let _ = jobs.delete(&job_name, &kube::api::DeleteParams::background()).await;
    Ok(())
}

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

/// Shell command populating `/seed` from the bucket.
///
/// - `curl --fail` (else an error page pipes into `tar`)
/// - `pipefail` (`tar` exits 0 on a truncated-but-well-formed prefix)
/// - Explicit decompression flag, present in the image (GNU tar can't sniff a
///   non-seekable pipe) — see [`detect_puller_image`]
/// - URL via `SEED_URL`, never argv (bearer credential vs world-readable `/proc`)
/// - Digest checked in flight: a seed is *named* by its content, so bytes hashing to
///   anything else are the one corruption no other check here catches
fn puller_cmd(seed: &SeedEntry) -> Result<String, storage::StorageError> {
    let prelude = "set -o pipefail";
    let fetch = "curl --fail --silent --show-error --location \"$SEED_URL\"";
    let payload = match seed.payload {
        crate::inventory::SeedPayload::Archive => {
            let compression = storage::compression_from_name(&seed.name).ok_or_else(|| {
                storage::StorageError::UnknownCompression { name: seed.name.clone() }
            })?;
            format!(
                "{fetch} | tee {VERIFY_FIFO} | {METER} | tar {}-xf - -C /seed && \
                 {NORMALIZE_MODES}",
                compression.tar_flag()
            )
        }
        // Always `/seed/blob` — only the consumer's volumeMount path cares
        crate::inventory::SeedPayload::File => {
            format!("{fetch} | tee {VERIFY_FIFO} | {METER} > /seed/blob && {NORMALIZE_MODES}")
        }
    };
    let verified = format!("{VERIFY_OPEN}; {payload} && {}", verify_close(&seed.oid));
    // Group-then-pipe, not appended to the payload's pipeline: the meter writes to
    // *stderr*, and only a group carries every command's stderr through one delimiter
    // `pipefail` spans both pipelines → `tr` cannot mask a failed transfer
    Ok(format!("{prelude}; {{ {verified} ; }} 2>&1 | {LINE_DELIMIT}"))
}

/// Where `tee` forks the stream to the hasher. A FIFO, so the bytes are hashed as they
/// stream past — nothing is staged, and a 21 GB archive needs no second copy
const VERIFY_FIFO: &str = "/tmp/verify.fifo";

/// Start hashing before the transfer opens the write end.
///
/// - Backgrounded against a real pid, not a `>(…)` substitution: the shell never waits
///   for those, so the compare could read a half-written digest
/// - `mkfifo` stays in the *foreground* — `mkfifo … && { … } &` backgrounds the whole
///   list, letting `tee` reach the path before it is a FIFO
const VERIFY_OPEN: &str = "mkfifo /tmp/verify.fifo || exit 1; \
                           { sha256sum < /tmp/verify.fifo | cut -d' ' -f1 > /tmp/verify.sum ; } & \
                           VERIFY_PID=$!";

/// Join the hasher and compare. Runs only after the transfer succeeded, so a mismatch
/// here means the bucket served bytes that are not what the seed is named for
fn verify_close(oid: &str) -> String {
    format!(
        "wait $VERIFY_PID; ACTUAL=$(cat /tmp/verify.sum); \
         [ \"$ACTUAL\" = \"{oid}\" ] || {{ \
         echo \"seed digest mismatch: expected {oid}, got $ACTUAL\" >&2; exit 1; }}"
    )
}

/// Mid-pipe meter: running byte total to stderr once a second, drawn by
/// [`progress::watch_puller`]. Pass-through, always exit 0 (masks nothing)
const METER: &str = "dd bs=1M status=progress";

/// [`METER`]'s `\r` separators → `\n`, line-buffered. Both halves load-bearing:
///
/// - CRI flushes a partial record only on a full buffer → `\r`-only = ~16 KiB batches
/// - `tr`'s stdout is a pipe, block-buffered by stdio → `stdbuf -oL` makes it per-record
const LINE_DELIMIT: &str = r#"stdbuf -oL tr '\r' '\n'"#;

/// Owner bits mirrored onto the group, so any uid can consume a seed.
///
/// - No `CAP_CHOWN` under `restricted-v2` (and consumers run as different uids:
///   zebrad 10001, zainod 1000), so the group is the only route
/// - CSI volume root is `2777` setgid gid 0 → every entry inherits [`SEED_GID`]
/// - Write, not just read: RocksDB writes `LOCK`/`MANIFEST`/SSTs, zebra's `version`
///   lands `0600`; `fsGroup` can't help (volume root stays gid 0 under
///   `fsGroupPolicy: None`)
/// - Once per seed, pre-snapshot, so every clone inherits it; `/seed` + `lost+found`
///   skipped (root-owned, chmod would fail the Job)
const NORMALIZE_MODES: &str =
    "find /seed -mindepth 1 -name lost+found -prune -o -exec chmod g=u {} +";

/// Group of every entry in a materialized seed. Not a choice — the setgid CSI
/// volume root stamps it. Not acquired by default either (k8s takes the primary gid
/// from the image's `USER`), so a mounting pod must list it in
/// [`PodSpec::supplemental_groups`](crate::manifest::PodSpec::supplemental_groups)
pub const SEED_GID: i64 = 0;

fn storage_fatal(archive: &str, err: storage::StorageError) -> MaterializeErr {
    MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
        archive: archive.to_string(),
        reason: err.to_string(),
    })
}

/// One-shot Job filling a seed PVC from the bucket.
///
/// - `backoffLimit: 2`: a retry reuses the same URL (valid for [`PRESIGN_TTL`])
/// - Safe to retry — fresh PVC, and complete only once a pod exits 0 on the whole stream
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

/// Delete a terminal puller Job, freeing its name. `false` = still running, so the
/// caller waits instead of disturbing another actor's pull
async fn reap_finished_job(jobs: &Api<Job>, name: &str) -> bool {
    let Ok(Some(job)) = jobs.get_opt(name).await else {
        // Vanished since the 409 — its owner cleaned up, name free
        return true;
    };
    if !is_job_finished().matches_object(Some(&job)) {
        return false;
    }
    // Background propagation so the Job's pods go with it, not orphaned
    let dp = kube::api::DeleteParams::background();
    if jobs.delete(name, &dp).await.is_err() {
        return false;
    }
    // Name unusable until the object is gone (a create racing the delete 409s again)
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

/// Terminal = `Complete` or `Failed`
fn is_job_finished() -> impl Condition<Job> {
    |obj: Option<&Job>| {
        obj.and_then(|j| j.status.as_ref()).and_then(|s| s.conditions.as_ref()).is_some_and(|cs| {
            cs.iter()
                .any(|c| matches!(c.type_.as_str(), "Complete" | "Failed") && c.status == "True")
        })
    }
}

/// `Complete`, as against `Failed`
async fn job_succeeded(jobs: &Api<Job>, name: &str) -> bool {
    jobs.get_opt(name)
        .await
        .ok()
        .flatten()
        .and_then(|j| j.status)
        .and_then(|s| s.succeeded)
        .is_some_and(|n| n > 0)
}

/// Newest pod's logs, for a failure message. Found by the template's stamped label
/// (a Job owns its pods indirectly)
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
    let storage = crate::storage_class::selected(client)
        .await
        .map_err(|e| EnvError::Manifest { reason: e })?;
    let snap_gvk = volume_snapshot_gvk();
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &snap_gvk);
    let body = json!({
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": { "name": pvc_name },
        "spec": {
            "source": { "persistentVolumeClaimName": pvc_name },
            "volumeSnapshotClassName": storage.snapshot_class,
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
    poll(WAIT_BUDGET, || async { pvc_is_ready(client, pvc_name).await }).await
}

async fn wait_snapshot_ready(
    client: &Client,
    snap_name: &str,
    progress: &dyn StepProgress,
) -> Result<(), EnvError> {
    let snap_gvk = volume_snapshot_gvk();
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &snap_gvk);
    let started = std::time::Instant::now();
    poll(WAIT_BUDGET, || async {
        // Elapsed on the row (copying drivers sit here minutes; bare spinner = hang)
        progress.note(&format!("waiting for snapshot ({}s)", started.elapsed().as_secs()));
        let snap = match api.get_opt(snap_name).await.map_err(env_err)? {
            Some(s) => s,
            None => return Ok::<bool, EnvError>(false),
        };
        Ok(snap.data["status"]["readyToUse"].as_bool().unwrap_or(false))
    })
    .await
}

/// Returns on `Ok(true)` or budget expiry. Predicate errors propagate at once
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
    // `puller_cmd` needs `curl` + GNU `tar` + `zstd`; fedora = smallest common base
    // carrying all three
    // Installing at pod start is impossible: restricted admission runs an arbitrary
    // non-root UID → "Unable to lock database: Permission denied"
    std::env::var("ZTEST_PULLER_IMAGE").unwrap_or_else(|_| "fedora:40".into())
}
#[cfg(test)]
mod tests {
    use super::*;

    use crate::inventory::SeedPayload;

    fn seed(name: &str, payload: SeedPayload) -> SeedEntry {
        SeedEntry { name: name.to_string(), oid: "a".repeat(64), size: 4096, payload }
    }

    /// Everything the parent's byte bar rests on:
    /// - Meter *between* fetch and consumer (counts the bytes that arrive)
    /// - Delimiter wraps the whole group (meter's stderr reaches the log per-record)
    #[test]
    fn the_archive_command_meters_the_transfer_and_delimits_its_records() {
        let cmd = puller_cmd(&seed("chain.tar.zst", SeedPayload::Archive)).expect("known suffix");
        assert!(cmd.starts_with("set -o pipefail; { mkfifo "), "{cmd}");
        assert!(cmd.contains("curl --fail"), "{cmd}");
        assert!(cmd.contains(&format!("| {METER} | tar --zstd -xf - -C /seed")), "{cmd}");
        assert!(cmd.ends_with(&format!("; }} 2>&1 | {LINE_DELIMIT}")), "{cmd}");
    }

    /// A seed is *named* by its content, so bytes hashing to anything else are the one
    /// corruption nothing downstream would catch — the PVC would just serve them.
    ///
    /// Three properties, each load-bearing: the hasher is joined on a real pid (a
    /// `>(…)` substitution is never waited for, so the compare could read a partial
    /// digest); the compare runs only on a successful transfer, so a mismatch means the
    /// bucket lied rather than the network dropped; and the expected oid is in the
    /// message, because "digest mismatch" alone names no artifact to go look at.
    #[test]
    fn a_seed_whose_bytes_do_not_hash_to_its_name_fails_the_pull() {
        for payload in [SeedPayload::Archive, SeedPayload::File] {
            let s = seed("chain.tar.zst", payload);
            let cmd = puller_cmd(&s).expect("valid seed");
            assert!(cmd.contains(&format!("tee {VERIFY_FIFO}")), "{cmd}");
            assert!(cmd.contains("wait $VERIFY_PID"), "not joined on a real pid: {cmd}");
            assert!(cmd.contains("&& wait $VERIFY_PID"), "compared unconditionally: {cmd}");
            assert!(cmd.contains(&format!("expected {}", s.oid)), "{cmd}");
            assert!(cmd.contains("exit 1"), "mismatch does not fail the pod: {cmd}");
        }
    }

    #[test]
    fn a_file_seed_is_metered_the_same_way_before_landing_at_the_blob_path() {
        let cmd = puller_cmd(&seed("wallet.dat", SeedPayload::File)).expect("file needs no suffix");
        assert!(cmd.contains(&format!("| {METER} > /seed/blob")), "{cmd}");
        assert!(cmd.ends_with(&format!("; }} 2>&1 | {LINE_DELIMIT}")), "{cmd}");
    }

    /// Meter adds a second pipeline and `tr` exits 0 over a failed transfer, so
    /// `pipefail` must precede both to keep the payload's status the pod's
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

    /// Normalization runs on the extracted tree → chained behind the payload with
    /// `&&`, never after the delimiter
    #[test]
    fn normalization_stays_inside_the_metered_group() {
        let cmd = puller_cmd(&seed("chain.tar.zst", SeedPayload::Archive)).expect("known suffix");
        let normalize = cmd.find(NORMALIZE_MODES).expect("normalization is chained");
        assert!(normalize < cmd.find(LINE_DELIMIT).expect("delimiter"), "{cmd}");
        assert!(cmd[..normalize].ends_with("-C /seed && "), "{cmd}");
    }
}
