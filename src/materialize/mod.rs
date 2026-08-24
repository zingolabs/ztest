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
//! 2. If created, or not `ready=true`, launch a puller Job: `curl` `lfs/<oid>` as byte
//!    ranges into `tar -x -C /seed` (`cat > /seed/blob` for a file seed). R2 → node,
//!    nothing through here, hence [`progress`]
//!    - Object's frame table (`storage::seek_table`) decides the shape: segments the pod
//!      extracts and records one at a time, resuming off `/seed/.ztest-resume` after a dead
//!      pod — or, with no table, the single stream every pre-segmentation blob is
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
use crate::storage;
use crate::storage::seekable::Segment;

pub mod progress;

const WAIT_INTERVAL: Duration = Duration::from_secs(2);
const WAIT_BUDGET: Duration = Duration::from_secs(300);

/// Bounded: a wrong base_uri hangs on connect, and this sits in front of every seed
const BLOB_PROBE_TIMEOUT: Duration = Duration::from_secs(20);

/// Cluster GC's hold on a finished puller Job — long enough to read a failure, short enough
/// that an interrupted run leaks nothing lasting
const JOB_TTL: Duration = Duration::from_secs(60 * 60);

/// Puller log lines quoted in an error. The meter writes one record a second, so the whole
/// log is thousands of them and the failure is always at the end
const LOG_TAIL_LINES: i64 = 50;

/// Refuse to fill a volume that cannot hold the artifact.
///
/// - Name is content+driver, never capacity → a PVC created under an older sizing policy
///   is adopted silently and caps the extraction
/// - Failure surfaces hours in as `tar: Cannot write: No space left on device`, naming an
///   `.sst` file rather than the volume (observed: a 48 GiB PVC adopted for a 258 GiB tree)
/// - Never deletes: the volume may hold another run's seed. Names the remedy instead
fn adopted_volume_fits(
    pvc: &PersistentVolumeClaim,
    seed: &SeedEntry,
    pvc_name: &str,
) -> Result<(), EnvError> {
    // `status.capacity` = what the CSI driver actually gave; `spec.resources` is only what
    // was asked for, and a bound volume can be either
    let have = pvc
        .status
        .as_ref()
        .and_then(|s| s.capacity.as_ref())
        .and_then(|c| c.get("storage"))
        .or_else(|| pvc.spec.as_ref()?.resources.as_ref()?.requests.as_ref()?.get("storage"))
        .map(|q| q.0.as_str());
    let want = crate::cluster_config::seed_size_for(seed.uncompressed_bytes);
    match volume_shortfall(have, &want) {
        None => Ok(()),
        Some((have, want)) => Err(EnvError::ArchiveMaterializeFailed {
            archive: seed.name.clone(),
            reason: format!(
                "seed volume {pvc_name} is {have}, but this artifact extracts to {want}. \
                 It predates the current sizing; delete it and re-run: \
                 kubectl -n {SEEDS_NAMESPACE} delete pvc {pvc_name}"
            ),
        }),
    }
}

/// `(have, want)` when the volume is too small, `None` when it fits or either quantity is
/// unreadable — an unparseable capacity is not evidence of a problem
fn volume_shortfall<'a>(have: Option<&'a str>, want: &'a str) -> Option<(&'a str, &'a str)> {
    use crate::qos::units::parse_mem_bytes_opt;
    let (h, w) = (parse_mem_bytes_opt(have?)?, parse_mem_bytes_opt(want)?);
    (h < w).then_some((have?, want))
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
        .map_err(|e| EnvError::Manifest { reason: e.to_string() })
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
            reason: format!("seed {pvc_name}: no #[ztest::needs({})]", handle.name),
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
        .map_err(|why| unsupported(archive, why.to_string()))
}

fn unsupported(archive: &str, what: String) -> EnvError {
    EnvError::ArchiveMaterializeFailed {
        archive: archive.to_string(),
        reason: format!(
            "{what} — this archive-backed test needs CSI snapshot support. \
             On a local kind cluster run `ztest cluster setup --install-storage`; on a \
             shared cluster check that the seed StorageClass / VolumeSnapshotClass are \
             installed."
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
        .map_err(|e| EnvError::Manifest { reason: e.to_string() })?;
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    if let Some(existing) = api.get_opt(pvc_name).await.map_err(env_err)? {
        if existing.metadata.deletion_timestamp.is_none() {
            adopted_volume_fits(&existing, seed, pvc_name)?;
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
            "resources": { "requests": {
                "storage": crate::cluster_config::seed_size_for(seed.uncompressed_bytes) } },
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
                    "seed volume {pvc_name}: Terminating for {}s; {blame}",
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
/// - Job, not a bare Pod: its terminal condition is the verdict, and its name the lock
/// - Job name = the concurrency lock a 409 reports
/// - Nothing streams from here (the pod holds the public URL, transfers itself)
async fn materialize(
    client: &Client,
    pvc_name: &str,
    seed: &SeedEntry,
    progress: &dyn StepProgress,
) -> Result<(), MaterializeErr> {
    let job_name = format!("puller-{}", pvc_name.trim_start_matches("seed-"));
    let jobs: Api<Job> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let pods: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);

    // Check before the Job: a missing object fails here, named, in ms, not as a puller
    // retrying a 404 to its budget. Unauthenticated, like every read ztest makes
    let url = seed.blob_url();
    progress.note("locating blob");
    let present = crate::storage::blob_present(&url, seed.size, BLOB_PROBE_TIMEOUT)
        .await
        .map_err(|e| storage_fatal(&seed.name, e))?;
    if !present {
        return Err(MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
            archive: seed.name.clone(),
            reason: format!("no blob at {url} sized {} bytes", seed.size),
        }));
    }

    // Frame table decides the shape of the pull: present = resumable segments, absent =
    // one stream (every object published before segmentation)
    let segments = crate::storage::seek_table(&url, seed.size, BLOB_PROBE_TIMEOUT)
        .await
        .map_err(|e| storage_fatal(&seed.name, e))?;
    let resumable = segments.is_some();
    let cmd = puller_cmd(seed, segments.as_deref()).map_err(|e| storage_fatal(&seed.name, e))?;
    let body = puller_job(&job_name, pvc_name, &cmd, &url, backoff_limit(resumable));
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

    // No duration is predicted, because none can be: transfer+extract time spans orders of
    // magnitude across link, CPU and CSI write path, so any budget is at once too tight for
    // the slowest honest cluster and too loose to catch a hang. The Job's condition decides
    // the pull; the watcher only ends states no condition would ever arrive to settle
    let stalled = tokio::select! {
        r = await_condition(jobs.clone(), &job_name, is_job_finished()) => {
            r.map_err(env_err)?;
            None
        }
        stall = progress::watch_puller(&pods, &job_name, seed.size, resumable, progress) => {
            Some(stall)
        }
    };
    // Wedged pull is deleted, not left to inspect: it holds a Guaranteed pod and the PVC's
    // `pvc-protection` finalizer, and `reap_finished_job` reads a non-terminal Job as another
    // run's live pull — so leaving it wedges every later run too. Diagnostic rides the error
    if let Some(stall) = stalled {
        let tail = match stall.ran() {
            true => job_logs(&pods, &job_name).await,
            false => String::new(),
        };
        let _ = jobs.delete(&job_name, &kube::api::DeleteParams::background()).await;
        return Err(MaterializeErr::Fatal(puller_stuck(&seed.name, stall, &tail)));
    }
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

/// Last thing the puller said, appended to the verdict. One line, not the tail: over a
/// wedged pull the rest is meter records, and the count they carry is already in `stall`
fn puller_stuck(archive: &str, stall: progress::Stall, tail: &str) -> EnvError {
    let last = tail.lines().rev().map(str::trim).find(|l| !l.is_empty());
    EnvError::ArchiveMaterializeFailed {
        archive: archive.to_string(),
        reason: match last {
            None => stall.to_string(),
            Some(last) => format!("{stall}; puller last said: {last}"),
        },
    }
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
fn puller_cmd(
    seed: &SeedEntry,
    segments: Option<&[Segment]>,
) -> Result<String, storage::StorageError> {
    puller_cmd_chunked(seed, CHUNK_BYTES, segments)
}

/// [`puller_cmd`] at an explicit chunk size, so a test can drive the same command over a
/// fixture small enough to run in-process
fn puller_cmd_chunked(
    seed: &SeedEntry,
    chunk: u64,
    segments: Option<&[Segment]>,
) -> Result<String, storage::StorageError> {
    let payload = match (seed.payload, segments) {
        (crate::inventory::SeedPayload::Archive, Some(segments)) => segmented(segments, seed.size),
        (crate::inventory::SeedPayload::Archive, None) => {
            let compression = storage::compression_from_name(&seed.name).ok_or_else(|| {
                storage::StorageError::UnknownCompression { name: seed.name.clone() }
            })?;
            format!(
                "stream 0 $(({} - 1)) | tee {VERIFY_FIFO} | {METER} | tar {}-ixf - -C /seed && \
                 {NORMALIZE_MODES}",
                seed.size,
                compression.tar_flag()
            )
        }
        // Always `/seed/blob` — only the consumer's volumeMount path cares
        (crate::inventory::SeedPayload::File, _) => format!(
            "stream 0 $(({} - 1)) | tee {VERIFY_FIFO} | {METER} > /seed/blob && {NORMALIZE_MODES}",
            seed.size
        ),
    };
    // Verify grouped, not spliced: it is a `;`-separated list, so a bare `&&` would gate
    // only its first command and leave the group's status set by the digest test —
    // masking a failed `tar` whenever the bytes still hashed correctly
    let verified = format!("{VERIFY_OPEN}; {payload} && {{ {} ; }}", verify_close(&seed.oid));
    // Group-then-pipe, not appended to the payload's pipeline: the meter writes to
    // *stderr*, and only a group carries every command's stderr through one delimiter
    // `pipefail` spans both pipelines → `tr` cannot mask a failed transfer
    // `RESUME` is read by [`verify_close`] on every path; only [`segmented`] ever moves it
    // off zero, so a single-stream pull verifies exactly as it always did
    Ok(format!(
        "set -o pipefail; RESUME=0; {}; {{ {verified} ; }} 2>&1 | {LINE_DELIMIT}",
        stream_fns(chunk)
    ))
}

/// Segment-at-a-time pull, resumable across a dead pod.
///
/// - Marker written only *after* `tar` returns → a segment interrupted mid-extract is
///   redone, and redoing one is harmless (`tar` overwrites what it already wrote)
/// - Marker lives on the PVC (the one thing that outlives the pod) and is removed before
///   the seed is published, or every clone would carry it
/// - `-i`: each segment ends with its own end-of-archive blocks, and without this `tar`
///   stops at the first pair and exits **0** over a fraction of the tree
/// - Meter restarts at every frame, so each segment announces the absolute offset its
///   counts are relative to ([`BASE_MARK`])
/// - Digest compared only on a pull that ran the whole object; a resumed one never held
///   the earlier bytes, and each frame carries its own checksum for zstd to enforce
/// - Seek table trails the last frame and no segment covers it, so a whole pull draws it
///   through the hasher too — which is what binds the table the parent read off the network
///   to the oid committed in the tree
fn segmented(segments: &[Segment], size: u64) -> String {
    let lens = segments.iter().map(|s| s.compressed.to_string()).collect::<Vec<_>>().join(" ");
    format!(
        "SEGS=\"{lens}\"; \
         RESUME=$(cat {RESUME_FILE} 2>/dev/null || echo 0); \
         case \"$RESUME\" in \'\'|*[!0-9]*) RESUME=0;; esac; \
         exec 9>{VERIFY_FIFO}; \
         k=0; off=0; \
         for len in $SEGS; do \
         if [ $k -ge $RESUME ]; then \
         echo \"{BASE_MARK} $off\"; \
         stream $off $((off + len - 1)) | tee {VERIFY_FIFO} | {METER} \
         | zstd -dc | tar -ixf - -C /seed || exit 1; \
         echo $((k + 1)) > {RESUME_FILE}; \
         fi; \
         off=$((off + len)); k=$((k + 1)); \
         done; \
         [ $RESUME -eq 0 ] && {{ stream $off $(({size} - 1)) | tee {VERIFY_FIFO} > /dev/null \
         || exit 1; }}; \
         exec 9>&-; \
         rm -f {CHUNK_FILE}.a {CHUNK_FILE}.b {RESUME_FILE} && {NORMALIZE_MODES}"
    )
}

/// `fetch` + `stream`: the transfer, as two shell functions the payloads call.
///
/// - One connection for the whole object = the failure mode (no endpoint holds a
///   multi-hour transfer open; a 245 GiB seed died at 8/33/34 GB, restarting from zero)
/// - Chunk staged, emitted whole (a partial range already on stdout would be re-sent by
///   its retry → duplicate bytes past the hasher)
/// - Staging alone would serialise transfer and extraction, so chunk *n+1* downloads
///   while *n* feeds `tar`: measured 17.1 → 20.3 MB/s mean over 6 pairs, and the floor
///   11.8 → 18.4 (the win is the consumer's time, so it grows with link speed)
/// - Two buffers, never more: a deeper queue would stage more disk for a transfer
///   already at the link's ceiling
/// - `return`, never `exit`: `stream` is a pipeline element, so it runs in a subshell and
///   only its status reaches `pipefail`
fn stream_fns(chunk: u64) -> String {
    // `fetch <off> <end> <file>`, retried; run backgrounded, so its status reaches the
    // loop through `wait`
    let fetch = format!(
        "fetch() {{ want=$(($2 - $1 + 1)); try=1; while :; do \
         curl --fail --silent --show-error --location --range \"$1-$2\" \
         --speed-limit {STALL_FLOOR_BPS} --speed-time {STALL_WINDOW_SECS} \
         --output \"$3\" \"$SEED_URL\" && [ \"$(wc -c < \"$3\")\" -eq $want ] && return 0; \
         [ $try -ge {CHUNK_ATTEMPTS} ] && return 1; \
         sleep $((try * ${{{BACKOFF_ENV}:-{CHUNK_BACKOFF_SECS}}})); try=$((try + 1)); \
         done; }}"
    );
    // `stream <first> <last>`, both inclusive — a byte range on stdout, one chunk ahead
    format!(
        "{fetch}; \
         stream() {{ a={CHUNK_FILE}.a; b={CHUNK_FILE}.b; off=$1; last=$2; \
         end=$((off + {chunk} - 1)); [ $end -gt $last ] && end=$last; \
         fetch $off $end $a & p=$!; \
         while [ $off -le $last ]; do \
         wait $p || return 1; \
         nxt=$((end + 1)); \
         [ $nxt -le $last ] && {{ nend=$((nxt + {chunk} - 1)); \
         [ $nend -gt $last ] && nend=$last; \
         fetch $nxt $nend $b & p=$!; end=$nend; }}; \
         cat $a || return 1; t=$a; a=$b; b=$t; off=$nxt; \
         done; }}"
    )
}

/// Absolute offset the meter's following counts are relative to. `dd` restarts at every
/// frame, so without this the bar would saw back to zero once a segment
const BASE_MARK: &str = "ZTEST_BASE";

/// Segments already extracted, on the volume that outlives the pod
const RESUME_FILE: &str = "/seed/.ztest-resume";

/// Bytes per ranged GET. Short enough that a dropped connection costs one chunk, long
/// enough that a 245 GiB seed stays under 1k requests. Two of these are staged at once
const CHUNK_BYTES: u64 = 256 * 1024 * 1024;

/// Redial floor: a range crawling under this for [`STALL_WINDOW_SECS`] is abandoned to the
/// retry rather than held open. A throttled connection does not recover on its own, and a
/// stalled one costs the whole run's clock
const STALL_FLOOR_BPS: u64 = 1024 * 1024;

/// Long enough to ride out a pause, far under the run budget
const STALL_WINDOW_SECS: u64 = 60;

/// Attempts per chunk before the pull fails
const CHUNK_ATTEMPTS: u32 = 5;

/// Backoff between attempts, × the attempt number. Overridable in the pod's environment so a
/// test can drive the exhaustion path without waiting out the real ladder
const CHUNK_BACKOFF_SECS: u32 = 5;
const BACKOFF_ENV: &str = "ZTEST_CHUNK_BACKOFF_SECS";

/// Where [`ranged_fetch`] stages the in-flight chunk
const CHUNK_FILE: &str = "/tmp/chunk";

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
/// here means the bucket served bytes that are not what the seed is named for.
///
/// Skipped on a resumed pull alone (`RESUME` != 0): those bytes were drawn by a pod that is
/// gone, and no digest over the remainder means anything. What covers them instead is the
/// per-frame checksum zstd enforces at decompression
fn verify_close(oid: &str) -> String {
    format!(
        "wait $VERIFY_PID; [ $RESUME -eq 0 ] || exit 0; ACTUAL=$(cat /tmp/verify.sum); \
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
/// - `backoffLimit` tracks what a restart costs: a segmented object resumes off its marker,
///   so a fresh pod is cheap; an unsegmented one would re-fetch from byte 0, and there the
///   only retry that can resume anything is `fetch()`'s, per range
/// - Complete only once a pod exits 0 on the whole stream
/// - `ttlSecondsAfterFinished` backstops the delete-on-success (a Ctrl-C between create and
///   delete leaks the Job otherwise), long enough that a failure is still there to read
/// - No `activeDeadlineSeconds`: server-side wall clock, the same unmodelable duration
fn puller_job(name: &str, pvc_name: &str, cmd: &str, url: &str, backoff_limit: u32) -> Job {
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
            "backoffLimit": backoff_limit,
            "ttlSecondsAfterFinished": JOB_TTL.as_secs(),
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

/// Attempts a Job is worth. A resumed pod picks up at its marker, so a restart costs one
/// segment; without a marker it costs the object, and failing loudly beats redoing it twice
fn backoff_limit(resumable: bool) -> u32 {
    match resumable {
        true => 2,
        false => 0,
    }
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
    let lp = kube::api::LogParams { tail_lines: Some(LOG_TAIL_LINES), ..Default::default() };
    pods.logs(&name, &lp).await.unwrap_or_else(|e| format!("<logs unavailable: {e}>"))
}

// ─────────────────────────── snapshot + waits ───────────────────────

async fn create_volume_snapshot(client: &Client, pvc_name: &str) -> Result<(), EnvError> {
    let storage = crate::storage_class::selected(client)
        .await
        .map_err(|e| EnvError::Manifest { reason: e.to_string() })?;
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
        SeedEntry {
            name: name.to_string(),
            oid: "a".repeat(64),
            size: 4096,
            uncompressed_bytes: 0,
            payload,
            base_uri: crate::storage::BASE_URI.to_string(),
            key_prefix: crate::storage::KEY_PREFIX.to_string(),
        }
    }

    /// Everything the parent's byte bar rests on:
    /// - Meter *between* fetch and consumer (counts the bytes that arrive)
    /// - Delimiter wraps the whole group (meter's stderr reaches the log per-record)
    #[test]
    fn the_archive_command_meters_the_transfer_and_delimits_its_records() {
        let cmd =
            puller_cmd(&seed("chain.tar.zst", SeedPayload::Archive), None).expect("known suffix");
        assert!(cmd.starts_with("set -o pipefail; RESUME=0; fetch() "), "{cmd}");
        assert!(cmd.contains("curl --fail"), "{cmd}");
        assert!(cmd.contains(&format!("| {METER} | tar --zstd -ixf - -C /seed")), "{cmd}");
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
            let cmd = puller_cmd(&s, None).expect("valid seed");
            assert!(cmd.contains(&format!("tee {VERIFY_FIFO}")), "{cmd}");
            assert!(cmd.contains("wait $VERIFY_PID"), "not joined on a real pid: {cmd}");
            // Grouped: a bare `&&` would gate only the `wait`, leaving the pod's status
            // set by the digest test — a failed `tar` would pass on matching bytes
            assert!(cmd.contains("&& { wait $VERIFY_PID"), "compared unconditionally: {cmd}");
            assert!(cmd.contains(&format!("expected {}", s.oid)), "{cmd}");
            assert!(cmd.contains("exit 1"), "mismatch does not fail the pod: {cmd}");
        }
    }

    #[test]
    fn a_file_seed_is_metered_the_same_way_before_landing_at_the_blob_path() {
        let cmd =
            puller_cmd(&seed("wallet.dat", SeedPayload::File), None).expect("file needs no suffix");
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
            let cmd = puller_cmd(&seed(name, payload), None).expect("valid seed");
            assert_eq!(cmd.find("set -o pipefail"), Some(0), "{cmd}");
        }
    }

    /// Multi-hour transfers get dropped, so the blob arrives as ranges: a chunk reaches
    /// the consumer only once it is whole, and one that never is fails the pod
    #[test]
    fn the_transfer_is_ranged_and_a_short_chunk_never_reaches_the_consumer() {
        let mut s = seed("chain.tar.zst", SeedPayload::Archive);
        s.size = 3 * CHUNK_BYTES;
        let cmd = puller_cmd(&s, None).expect("known suffix");
        assert!(cmd.contains(&format!("stream 0 $(({} - 1))", s.size)), "{cmd}");
        assert!(cmd.contains(r#"--range "$1-$2""#), "{cmd}");
        assert!(cmd.contains(r#"--output "$3""#), "not staged: {cmd}");
        let whole = r#"[ "$(wc -c < "$3")" -eq $want ]"#;
        assert!(cmd.contains(whole), "short chunk not caught: {cmd}");
        assert!(cmd.find(whole) < cmd.find("cat $a"), "emitted before it was whole: {cmd}");
    }

    /// Transfer and extraction overlap, or the link idles for every byte `tar` spends
    /// decompressing (measured 17.1 → 20.3 MB/s). Two buffers, swapped
    #[test]
    fn the_next_chunk_downloads_while_the_current_one_feeds_the_consumer() {
        let cmd =
            puller_cmd(&seed("chain.tar.zst", SeedPayload::Archive), None).expect("known suffix");
        let prefetch = cmd.find("fetch $nxt $nend $b & p=$!").expect("no prefetch");
        let emit = cmd.find("cat $a").expect("nothing emitted");
        assert!(prefetch < emit, "prefetch does not precede the emit it overlaps: {cmd}");
        assert!(cmd.contains("t=$a; a=$b; b=$t"), "buffers never swap: {cmd}");
        // `return`, not `exit`: `stream` is a pipeline element, so it runs in a subshell
        assert!(cmd.contains("wait $p || return 1"), "prefetch failure is not awaited: {cmd}");
    }

    /// A volume too small for the artifact must be refused at adoption, in milliseconds —
    /// not discovered three hours in as `tar: No space left on device`
    #[test]
    fn a_volume_smaller_than_the_extracted_tree_is_refused() {
        assert_eq!(volume_shortfall(Some("48Gi"), "297Gi"), Some(("48Gi", "297Gi")));
        assert_eq!(volume_shortfall(Some("297Gi"), "297Gi"), None, "exact fit refused");
        assert_eq!(volume_shortfall(Some("400Gi"), "297Gi"), None, "larger volume refused");
    }

    /// Nothing readable to compare is not evidence of a problem: a PVC whose capacity this
    /// cannot parse still gets its pull, exactly as before the check existed
    #[test]
    fn an_unreadable_capacity_does_not_block_the_pull() {
        assert_eq!(volume_shortfall(None, "297Gi"), None);
        assert_eq!(volume_shortfall(Some("what"), "297Gi"), None);
        assert_eq!(volume_shortfall(Some("48Gi"), "nonsense"), None);
    }

    /// A throttled range does not recover — it is redialed. Without this the pod holds a
    /// crawling connection open against the run's clock and the retry never fires
    #[test]
    fn a_stalled_range_is_abandoned_to_the_retry() {
        let cmd =
            puller_cmd(&seed("chain.tar.zst", SeedPayload::Archive), None).expect("known suffix");
        let guard = format!("--speed-limit {STALL_FLOOR_BPS} --speed-time {STALL_WINDOW_SECS}");
        assert!(cmd.contains(&guard), "{cmd}");
    }

    /// Everything the shell does, run for real: `curl` serves `file://` ranges, so the
    /// generated command can pull a fixture archive end to end with no network and no
    /// cluster. Covers what `sh -n` cannot — chunk boundaries, ordering across the
    /// double buffer, the `wait` status path, and the digest gate
    struct PullerRun {
        dir: std::path::PathBuf,
        seed: std::path::PathBuf,
        status: std::process::ExitStatus,
        /// Captured, not inherited: the meter writes a progress line per second, and a
        /// passing test should say nothing. Surfaces in the assertion that fails
        log: String,
    }

    /// Absent tooling skips rather than fails: this asserts ztest's command, not the
    /// image the developer happens to be sitting in
    fn run_puller(
        name: &str,
        oid: Option<&str>,
        chunk: u64,
        payload: &[u8],
        dest_exists: bool,
    ) -> Option<PullerRun> {
        for tool in ["curl", "tar", "zstd", "sha256sum"] {
            if std::process::Command::new(tool).arg("--version").output().is_err() {
                eprintln!("skipping: {tool} is not on PATH");
                return None;
            }
        }
        let dir = std::env::temp_dir().join(format!("ztest-puller-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (src, seed) = (dir.join("src"), dir.join("seed"));
        std::fs::create_dir_all(&src).expect("src");
        std::fs::create_dir_all(&seed).expect("seed");
        std::fs::write(src.join("chain.dat"), payload).expect("payload");

        let archive = dir.join("chain.tar.zst");
        let tar = std::process::Command::new("tar")
            .args(["--zstd", "-cf"])
            .arg(&archive)
            .args(["-C", &src.display().to_string(), "chain.dat"])
            .status()
            .expect("tar runs");
        assert!(tar.success(), "fixture archive");
        let bytes = std::fs::read(&archive).expect("archive");
        use sha2::Digest as _;
        let digest = hex::encode(sha2::Sha256::digest(&bytes));

        let entry = SeedEntry {
            name: "chain.tar.zst".to_string(),
            oid: oid.unwrap_or(&digest).to_string(),
            size: bytes.len() as u64,
            uncompressed_bytes: 0,
            payload: SeedPayload::Archive,
            base_uri: crate::storage::BASE_URI.to_string(),
            key_prefix: crate::storage::KEY_PREFIX.to_string(),
        };
        // `/seed` and `/tmp` are the pod's paths; rewritten so concurrent tests cannot
        // collide on one FIFO. Everything else is the command the cluster runs
        // `/tmp/` first: rewriting `/seed` produces a path *under* the temp dir, which a
        // later `/tmp/` pass would mangle
        let cmd = puller_cmd_chunked(&entry, chunk, None)
            .expect("known suffix")
            .replace("/tmp/", &format!("{}/", dir.display()))
            .replace("/seed", &seed.display().to_string());
        if !dest_exists {
            std::fs::remove_dir_all(&seed).expect("drop destination");
        }
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .env("SEED_URL", format!("file://{}", archive.display()))
            .output()
            .expect("sh runs");
        let log = String::from_utf8_lossy(&out.stdout).into_owned();
        Some(PullerRun { dir, seed, status: out.status, log })
    }

    /// Many chunks, none of them aligned to the object's end — the case where an
    /// off-by-one in the range arithmetic corrupts the stream instead of failing
    #[test]
    fn the_puller_reassembles_an_archive_from_ranges_in_order() {
        let payload: Vec<u8> = (0..3_000_000u32).map(|i| (i ^ (i >> 8)) as u8).collect();
        let Some(run) = run_puller("ordered", None, 300_000, &payload, true) else {
            return;
        };
        assert!(run.status.success(), "puller failed: {:?}\n{}", run.status, run.log);
        let landed = std::fs::read(run.seed.join("chain.dat")).expect("extracted");
        assert_eq!(landed, payload, "reassembled bytes differ from the source");
        let _ = std::fs::remove_dir_all(&run.dir);
    }

    /// The digest gate still fires when the bytes are not what the seed is named for —
    /// the one corruption nothing downstream would catch
    #[test]
    fn a_seed_whose_bytes_hash_to_something_else_fails_the_pull() {
        let payload: Vec<u8> = (0..500_000u32).map(|i| i as u8).collect();
        let wrong = "0".repeat(64);
        let Some(run) = run_puller("mismatch", Some(&wrong), 300_000, &payload, true) else {
            return;
        };
        assert!(!run.status.success(), "a digest mismatch completed the pull:\n{}", run.log);
        let _ = std::fs::remove_dir_all(&run.dir);
    }

    /// Extraction failing on bytes that *do* hash correctly: the group's status used to
    /// come from the digest test, so a broken `tar` exited 0 and the PVC was marked ready
    /// over an empty tree
    #[test]
    fn a_failed_extraction_fails_the_pull_even_when_the_digest_matches() {
        let payload: Vec<u8> = (0..500_000u32).map(|i| i as u8).collect();
        let Some(run) = run_puller("no-dest", None, 300_000, &payload, false) else {
            return;
        };
        assert!(!run.status.success(), "a failed extraction reported success:\n{}", run.log);
        let _ = std::fs::remove_dir_all(&run.dir);
    }

    /// Pack a fixture into segments and drive the real generated command over `file://`.
    ///
    /// `resume` pre-seeds the marker the way a dead pod would leave it
    struct Segmented {
        dir: std::path::PathBuf,
        seed: std::path::PathBuf,
        segments: Vec<crate::storage::seekable::Segment>,
        entry: SeedEntry,
        blob: std::path::PathBuf,
    }

    fn segmented_fixture(tag: &str, members: usize) -> Option<Segmented> {
        for tool in ["curl", "tar", "zstd", "sha256sum"] {
            if std::process::Command::new(tool).arg("--version").output().is_err() {
                eprintln!("skipping: {tool} is not on PATH");
                return None;
            }
        }
        let dir = std::env::temp_dir().join(format!("ztest-seg-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (src, seed) = (dir.join("src"), dir.join("seed"));
        std::fs::create_dir_all(&src).expect("src");
        std::fs::create_dir_all(&seed).expect("seed");
        for i in 0..members {
            std::fs::write(src.join(format!("m{i}.dat")), vec![i as u8; 40_000]).expect("member");
        }
        let plain = dir.join("plain.tar.zst");
        assert!(
            std::process::Command::new("tar")
                .args(["--zstd", "-cf"])
                .arg(&plain)
                .args(["-C", &src.display().to_string()])
                .args((0..members).map(|i| format!("m{i}.dat")).collect::<Vec<_>>())
                .status()
                .expect("tar runs")
                .success(),
            "fixture archive"
        );
        // 1 byte = cut at the first legal boundary after every member → one member a segment
        let blob = dir.join("packed.tar.zst");
        let packed = crate::storage::pack::pack_with(&plain, &blob, 3, 1, &crate::progress::Silent)
            .expect("packs");
        let entry = SeedEntry {
            name: "chain.tar.zst".to_string(),
            oid: packed.sha256.clone(),
            size: packed.size_bytes,
            uncompressed_bytes: packed.uncompressed_bytes,
            payload: SeedPayload::Archive,
            base_uri: crate::storage::BASE_URI.to_string(),
            key_prefix: crate::storage::KEY_PREFIX.to_string(),
        };
        Some(Segmented { dir, seed, segments: packed.segments, entry, blob })
    }

    impl Segmented {
        /// Same rewrite the single-stream harness uses: `/tmp` and `/seed` are the pod's
        fn run(&self, segments: Option<&[crate::storage::seekable::Segment]>) -> (bool, String) {
            self.run_against(&self.blob, segments)
        }

        fn run_against(
            &self,
            url_path: &std::path::Path,
            segments: Option<&[crate::storage::seekable::Segment]>,
        ) -> (bool, String) {
            // Pod-local scratch; a resumed pull is a *new* pod, so it never inherits these
            for leftover in ["verify.fifo", "verify.sum", "chunk.a", "chunk.b"] {
                let _ = std::fs::remove_file(self.dir.join(leftover));
            }
            let cmd = puller_cmd_chunked(&self.entry, 4096, segments)
                .expect("known suffix")
                .replace("/tmp/", &format!("{}/", self.dir.display()))
                .replace("/seed", &self.seed.display().to_string());
            let out = std::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .env("SEED_URL", format!("file://{}", url_path.display()))
                .env(BACKOFF_ENV, "0")
                .output()
                .expect("sh runs");
            (out.status.success(), String::from_utf8_lossy(&out.stdout).into_owned())
        }

        fn marker(&self) -> Option<String> {
            std::fs::read_to_string(self.seed.join(".ztest-resume")).ok()
        }

        fn landed(&self, i: usize) -> bool {
            self.seed.join(format!("m{i}.dat")).exists()
        }
    }

    /// End to end over the segmented path: every member lands, and the resume marker is
    /// gone — it must never reach the snapshot every test clones
    #[test]
    fn a_segmented_pull_lands_the_whole_tree_and_leaves_no_marker() {
        let Some(f) = segmented_fixture("whole", 4) else { return };
        assert!(f.segments.len() > 1, "fixture was not segmented");
        let (ok, log) = f.run(Some(&f.segments));
        assert!(ok, "segmented pull failed:\n{log}");
        for i in 0..4 {
            assert!(f.landed(i), "m{i}.dat missing:\n{log}");
        }
        assert_eq!(f.marker(), None, "resume marker survived into the seed");
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// The point of the whole feature: a pull that resumes re-fetches nothing it already
    /// landed. Proven by deleting an extracted member — a resumed pull must leave it gone
    #[test]
    fn a_resumed_pull_skips_the_segments_already_extracted() {
        let Some(f) = segmented_fixture("resume", 4) else { return };
        let (ok, log) = f.run(Some(&f.segments));
        assert!(ok, "first pull failed:\n{log}");

        // A dead pod leaves exactly this: two segments recorded, their bytes on the volume
        std::fs::write(f.seed.join(".ztest-resume"), "2").expect("marker");
        for i in 0..4 {
            std::fs::remove_file(f.seed.join(format!("m{i}.dat"))).expect("clear");
        }
        let (ok, log) = f.run(Some(&f.segments));
        assert!(ok, "resumed pull failed:\n{log}");
        assert!(!f.landed(0) && !f.landed(1), "resume re-fetched a recorded segment:\n{log}");
        assert!(f.landed(2) && f.landed(3), "resume did not finish the tail:\n{log}");
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// A pull cut off mid-object records what it finished, and nothing more. Without this
    /// the marker would either over-claim (losing bytes) or never advance
    #[test]
    fn an_interrupted_pull_records_only_the_segments_that_completed() {
        let Some(f) = segmented_fixture("cut", 4) else { return };
        // Truncated object: the ranges of later segments cannot be served
        let whole = std::fs::read(&f.blob).expect("blob");
        let short = f.dir.join("short.tar.zst");
        let keep = (f.segments[0].compressed + f.segments[1].compressed) as usize;
        std::fs::write(&short, &whole[..keep]).expect("write");

        let (ok, log) = f.run_against(&short, Some(&f.segments));
        assert!(!ok, "a truncated object completed the pull:\n{log}");
        assert_eq!(f.marker().as_deref(), Some("2\n"), "marker disagrees with what landed");
        assert!(f.landed(0) && f.landed(1), "recorded segments are not on the volume");
        assert!(!f.landed(2), "a segment that never arrived was recorded");

        // Same seed volume, whole object: the tail completes over the top of the marker
        let (ok, log) = f.run_against(&f.blob, Some(&f.segments));
        assert!(ok, "resume after truncation failed:\n{log}");
        for i in 0..4 {
            assert!(f.landed(i), "m{i}.dat missing after resume:\n{log}");
        }
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// `-i` is what makes a segmented blob safe on the single-stream path: without it `tar`
    /// stops at the first segment's end-of-archive blocks and exits **0** over a fraction of
    /// the tree, and the PVC is marked ready over it
    #[test]
    fn a_segmented_blob_still_extracts_whole_through_the_single_stream_path() {
        let Some(f) = segmented_fixture("legacy", 4) else { return };
        let (ok, log) = f.run(None);
        assert!(ok, "single-stream pull of a segmented blob failed:\n{log}");
        for i in 0..4 {
            assert!(f.landed(i), "m{i}.dat lost to a missing --ignore-zeros:\n{log}");
        }
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// Guards the flag itself, so the hazard above cannot come back through an edit that
    /// never runs the end-to-end tests
    #[test]
    fn every_extraction_ignores_the_zero_blocks_between_segments() {
        let seed = seed("chain.tar.zst", SeedPayload::Archive);
        let segments = [crate::storage::seekable::Segment {
            offset: 0,
            compressed: seed.size,
            uncompressed: 4096,
        }];
        for cmd in [
            puller_cmd(&seed, None).expect("known suffix"),
            puller_cmd(&seed, Some(&segments)).expect("known suffix"),
        ] {
            assert!(cmd.contains("-ixf - -C /seed"), "extraction is not zero-block safe: {cmd}");
        }
    }

    /// Retry policy follows what a restart costs. Without a frame table a fresh pod re-fetches
    /// the whole object against a clock sized for one pass, which is why it must not happen
    #[test]
    fn only_a_resumable_object_is_worth_retrying_at_the_job() {
        assert_eq!(backoff_limit(false), 0, "an unresumable pull was given a second pass");
        assert!(backoff_limit(true) > 0, "a resumable pull cannot retry");
        let job = puller_job("puller-x", "seed-x", "true", "https://e/lfs/x", backoff_limit(true));
        let spec = job.spec.expect("spec");
        assert_eq!(spec.backoff_limit, Some(backoff_limit(true) as i32));
        assert_eq!(
            spec.ttl_seconds_after_finished,
            Some(JOB_TTL.as_secs() as i32),
            "a finished Job is left for the cluster to keep forever"
        );
    }

    /// Whole command is a shell program → a quoting slip is a parse error here, not a
    /// puller that dies on the cluster an hour in
    #[test]
    fn the_generated_command_parses_as_a_shell_program() {
        use std::io::Write;
        for payload in [SeedPayload::Archive, SeedPayload::File] {
            let name = match payload {
                SeedPayload::Archive => "chain.tar.zst",
                SeedPayload::File => "wallet.dat",
            };
            let cmd = puller_cmd(&seed(name, payload), None).expect("valid seed");
            let mut sh = std::process::Command::new("sh")
                .arg("-n")
                .stdin(std::process::Stdio::piped())
                .spawn()
                .expect("sh on PATH");
            sh.stdin.take().expect("piped").write_all(cmd.as_bytes()).expect("write script");
            assert!(sh.wait().expect("sh runs").success(), "{cmd}");
        }
    }

    /// Normalization runs on the extracted tree → chained behind the payload with
    /// `&&`, never after the delimiter
    #[test]
    fn normalization_stays_inside_the_metered_group() {
        let cmd =
            puller_cmd(&seed("chain.tar.zst", SeedPayload::Archive), None).expect("known suffix");
        let normalize = cmd.find(NORMALIZE_MODES).expect("normalization is chained");
        assert!(normalize < cmd.find(LINE_DELIMIT).expect("delimiter"), "{cmd}");
        assert!(cmd[..normalize].ends_with("-C /seed && "), "{cmd}");
    }
}
