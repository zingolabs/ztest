//! Bring a `seed-{sha8}-{driver}` PVC into existence and fill it from the snapshot bucket
//! — the master copy every test's mount clones from.
//!
//! - [`provision_seed`] = parent side (`ztest run` preflight): create PVC, run the
//!   puller Job, snapshot. Unauthenticated, like every read ztest makes
//! - [`await_seed`] = test side (`TestEnv::build`): waits + resolves the snapshot
//!   handle from the baked-in OID, nothing else
//! - Identity travels as the OID (a runner pod has no checkout, no bytes, no credentials)
//!
//! # The pull
//!
//! 1. Get-or-create the PVC (409 = lost the race → wait-for-ready)
//! 2. If created, or not `ready=true`, launch a puller Job ([`puller_cmd`]): `rclone copy` of
//!    `snap/<oid>/…` into `/seed`, then `sha256sum -c`. R2 → node, nothing through here,
//!    hence [`progress`]. Dead pod → next pod reruns the copy (complete files skipped)
//! 3. Label `seeds.ztest.io/ready=true` + create the paired `VolumeSnapshot`, from
//!    which `seeds::read_seed_handle` resolves the handle and `bind_seed` clones per pod
//!
//! Race losers watch the winner's Job on its own terms — byte meter + stall, never a budget.
//! No leader election (the Job's name is the lock)
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

pub mod progress;
pub mod snapshot;

const WAIT_INTERVAL: Duration = Duration::from_secs(2);
const WAIT_BUDGET: Duration = Duration::from_secs(300);

/// Bounded: a wrong base_uri hangs on connect, and this sits in front of every seed
const SUMS_PROBE_TIMEOUT: Duration = Duration::from_secs(20);

/// Cluster GC's hold on a finished puller Job — long enough to read a failure, short enough
/// that an interrupted run leaks nothing lasting
const JOB_TTL: Duration = Duration::from_secs(60 * 60);

/// Puller log lines quoted in an error. The meter writes one record a second, so the whole
/// log is thousands of them and the failure is always at the end
const LOG_TAIL_LINES: i64 = 50;

/// Refuse to fill a volume that cannot hold the snapshot.
///
/// - Name is content+driver, never capacity → a PVC created under an older sizing policy
///   is adopted silently and caps the pull
/// - Failure surfaces hours in as `No space left on device` naming an `.sst`, not the volume
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
    let want = crate::cluster_config::seed_size_for(seed.size);
    match volume_shortfall(have, &want) {
        None => Ok(()),
        Some((have, want)) => Err(EnvError::ArchiveMaterializeFailed {
            archive: seed.name.clone(),
            reason: format!(
                "seed volume {pvc_name} is {have}, but this snapshot needs {want}. \
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
        publish(client, &pvc_name, seed, progress).await?;
    }

    // `publish` returns only on `ready=true`, which is what says every byte is in — snapshotting
    // ahead of it captures a half-filled volume on the `InFlight` path
    //
    // Unconditional: published = PVC *and* snapshot, but `ready=true` records only
    // the first. A PVC outliving its snapshot otherwise parks every future run on
    // `wait_snapshot_ready` with nothing able to create what it waits for
    // 409-tolerant, so the warm path costs one GET
    progress.note("snapshotting");
    create_volume_snapshot(client, &pvc_name).await?;
    wait_snapshot_ready(client, &seed.name, &pvc_name, &driver, seed.size, progress).await?;
    seeds::read_seed_handle(client, &seed.name, &seed.oid, &driver).await
}

/// CSI driver this run's storage resolves to — the other half of a seed's identity
async fn selected_driver(client: &Client) -> Result<String, EnvError> {
    crate::storage_class::selected(client)
        .await
        .map(|s| s.provisioner.clone())
        .map_err(|e| EnvError::StorageClass { reason: e.to_string() })
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
    // Total unknown test side (a runner pod holds no inventory) → meter reports bytes, no bar
    wait_snapshot_ready(client, handle.name, &pvc_name, &driver, 0, &Silent).await?;
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
        .map_err(|e| EnvError::StorageClass { reason: e.to_string() })?;
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
                "storage": crate::cluster_config::seed_size_for(seed.size) } },
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

/// How a wait on another run's puller ended
enum InFlightEnd {
    Published,
    /// Job gone, volume still unlabelled → lock free, this run takes the pull
    TakeOver,
}

/// Turns one run may take at a seed. Each `TakeOver` means the lock was free and this run
/// claimed it, so a second lap is a lost race and a third is a loop
const PUBLISH_ATTEMPTS: usize = 3;

/// Fill and label the seed volume, whichever run's puller does the work.
///
/// - Returns only once `ready=true` is set, so nothing downstream waits on the label
/// - Waiting on another run is not a budget: its puller is a named Job over a byte-metered
///   pod, watched here on exactly the terms its owner watches it
async fn publish(
    client: &Client,
    pvc_name: &str,
    seed: &SeedEntry,
    progress: &dyn StepProgress,
) -> Result<(), EnvError> {
    for _ in 0..PUBLISH_ATTEMPTS {
        match materialize(client, pvc_name, seed, progress).await {
            Ok(()) => return mark_ready(client, pvc_name).await,
            Err(MaterializeErr::Fatal(e)) => return Err(e),
            Err(MaterializeErr::InFlight) => {}
        }
        progress.note("waiting on another run's pull");
        tracing::debug!(pvc = %pvc_name, "seed materialization in flight elsewhere; waiting");
        match await_inflight(client, pvc_name, seed, progress).await? {
            InFlightEnd::Published => return Ok(()),
            InFlightEnd::TakeOver => continue,
        }
    }
    Err(EnvError::ArchiveMaterializeFailed {
        archive: seed.name.clone(),
        reason: format!("seed pull changed hands {PUBLISH_ATTEMPTS}x without publishing — rerun"),
    })
}

/// Wait out the puller Job another run owns.
///
/// - No budget: the Job's own liveness is the verdict, same as owning the pull
/// - `Complete` but unlabelled = owner died between the Job succeeding and [`mark_ready`]; bytes
///   are on the volume, so finish its publish rather than redo the transfer
/// - Stall is reported, never reaped: the Job is another actor's, and a log-derived verdict is
///   not grounds to delete its work
async fn await_inflight(
    client: &Client,
    pvc_name: &str,
    seed: &SeedEntry,
    progress: &dyn StepProgress,
) -> Result<InFlightEnd, EnvError> {
    let jobs: Api<Job> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let pods: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let job_name = puller_job_name(pvc_name);

    let settled = tokio::select! {
        settled = inflight_settled(client, &jobs, pvc_name, &job_name) => settled?,
        stall = progress::watch_puller(&pods, &job_name, seed.size, progress) => {
            return Err(orphaned_puller(&seed.name, &job_name, stall));
        }
    };
    match settled {
        Settled::Published => Ok(InFlightEnd::Published),
        Settled::Gone => Ok(InFlightEnd::TakeOver),
        Settled::Finished { succeeded: true } => {
            mark_ready(client, pvc_name).await?;
            Ok(InFlightEnd::Published)
        }
        Settled::Finished { .. } => Err(EnvError::ArchiveMaterializeFailed {
            archive: seed.name.clone(),
            reason: format!("puller job failed: {}", job_logs(&pods, &job_name).await.trim()),
        }),
    }
}

/// Every way a wait on another run's puller can stop being a wait. Sole place those states are
/// spelled, so [`await_inflight`] reads as a mapping and not a second copy of the predicates
enum Settled {
    Published,
    Gone,
    Finished { succeeded: bool },
}

/// Block until the wait's premise changes, on the Job's own watch — same primitive its owner
/// waits on in [`materialize`], so no second polling idiom and no per-tick GET over an hour
async fn inflight_settled(
    client: &Client,
    jobs: &Api<Job>,
    pvc_name: &str,
    job_name: &str,
) -> Result<Settled, EnvError> {
    match await_condition(jobs.clone(), job_name, is_job_settled()).await.map_err(env_err)? {
        Some(job) => Ok(Settled::Finished { succeeded: succeeded(&job) }),
        // Absent reads identically whether its owner reaped a corpse or cleaned up after success
        // (`materialize` deletes on both) — only the label separates them, and taking over an
        // already-published seed would redo the whole transfer
        None if pvc_is_ready(client, pvc_name).await? => Ok(Settled::Published),
        None => Ok(Settled::Gone),
    }
}

/// Terminal *or* gone — both end a waiter's interest, and `await_condition` reports which
/// (`Ok(None)` = no Job left to wait on)
fn is_job_settled() -> impl Condition<Job> {
    |obj: Option<&Job>| obj.is_none() || is_job_finished().matches_object(obj)
}

/// Another run's pull, stuck. Not reaped from here — naming it is the whole remedy
fn orphaned_puller(archive: &str, job_name: &str, stall: progress::Stall) -> EnvError {
    EnvError::ArchiveMaterializeFailed {
        archive: archive.to_string(),
        reason: format!(
            "{stall}; pull owned by another run — `kubectl -n {SEEDS_NAMESPACE} delete job \
             {job_name}` to retake it"
        ),
    }
}

/// Job name = the seed's lock, derived identically by its owner and by every waiter
fn puller_job_name(pvc_name: &str) -> String {
    format!("puller-{}", pvc_name.trim_start_matches("seed-"))
}

/// `InFlight` = puller Job already exists (another actor filling this seed) → [`publish`] waits
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
    let job_name = puller_job_name(pvc_name);
    let jobs: Api<Job> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let pods: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);

    // Missing snapshot fails here, named, in ms (not as a puller retrying a 404 to its budget)
    progress.note("locating snapshot");
    let present =
        storage::sums_present(&seed.base_uri, &seed.key_prefix, &seed.oid, SUMS_PROBE_TIMEOUT)
            .await
            .map_err(|e| storage_fatal(&seed.name, e))?;
    if !present {
        return Err(MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
            archive: seed.name.clone(),
            reason: format!(
                "no {} at {} — unpublished or incomplete push",
                storage::SUMS_FILE,
                seed.tree_url()
            ),
        }));
    }

    let body = puller_job(&job_name, pvc_name, seed);
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

    // No duration predicted (transfer + verify time spans orders of magnitude across link, CPU
    // and CSI write path). Job condition decides; watcher only ends states no condition settles
    let stalled = tokio::select! {
        r = await_condition(jobs.clone(), &job_name, is_job_finished()) => {
            r.map_err(env_err)?;
            None
        }
        stall = progress::watch_puller(&pods, &job_name, seed.size, progress) => {
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
        delete_job(&jobs, &job_name).await;
        return Err(MaterializeErr::Fatal(puller_stuck(&seed.name, stall, &tail)));
    }
    progress.finalizing();

    if !job_succeeded(&jobs, &job_name).await.map_err(|e| MaterializeErr::Fatal(env_err(e)))? {
        let logs = job_logs(&pods, &job_name).await;
        return Err(MaterializeErr::Fatal(EnvError::ArchiveMaterializeFailed {
            archive: seed.name.clone(),
            reason: format!("puller job failed: {}", logs.trim()),
        }));
    }
    delete_job(&jobs, &job_name).await;
    Ok(())
}

/// Job name = the seed's lock → foreground delete (Job object outlives its pods, so the name
/// frees only once no puller still writes the PVC), awaited. Server default `Orphan` would
/// strand the pod pinning the PVC's `pvc-protection` finalizer
async fn delete_job(jobs: &Api<Job>, name: &str) {
    if let Err(e) =
        crate::cluster::delete_and_await(jobs, name, &kube::api::DeleteParams::foreground()).await
    {
        tracing::warn!(job = %name, error = %e, "puller job delete failed");
    }
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

/// Pinned official image: `rclone` + busybox `sh`/`sha256sum`/`cut`/`tee`/`grep`
const PULLER_IMAGE: &str = "docker.io/rclone/rclone:1.75.1";

/// Pod attempts per Job. Every retry resumes (rclone skips files already whole on the volume)
const PULLER_ATTEMPTS: u32 = 3;

/// Shell program the puller pod runs: fetch the tree under `$SEED_URL` into `/seed`, verified.
///
/// - SHA256SUMS first, gated on `sha256(SHA256SUMS) == $SEED_OID` (binds every file to the oid)
/// - File list = SHA256SUMS relpaths → HEAD+GET per file, no listing (Worker serves none)
/// - `--size-only` resume skip (rclone lands via `.partial` + rename → right size = whole file)
/// - `sha256sum -c` last (`relpath: OK` lines = parent's verify heartbeat, [`progress`])
/// - Failed verify re-emits the non-OK lines last (log tail = the diagnostic)
/// - Stats as JSON log lines on stderr (exact byte counts for [`progress`])
fn puller_cmd() -> String {
    const RETRY: &str = "--low-level-retries 100 --timeout 5m";
    [
        "set -o pipefail".to_string(),
        "export RCLONE_CONFIG=/dev/null".to_string(),
        format!("rclone copyurl {RETRY} \"${{SEED_URL}}{SUMS}\" /tmp/{SUMS} || exit 1"),
        format!("SUM=$(sha256sum < /tmp/{SUMS} | cut -d' ' -f1)"),
        format!(
            "[ \"$SUM\" = \"$SEED_OID\" ] || {{ echo \"{SUMS} hashes to $SUM, manifest says \
             $SEED_OID\" >&2; exit 1; }}"
        ),
        format!("cut -c67- /tmp/{SUMS} > /tmp/files"),
        format!(
            "rclone copy --http-url \"$SEED_URL\" :http: /seed --files-from-raw /tmp/files \
             --no-traverse --size-only --transfers 8 --retries 10 {RETRY} --use-json-log \
             --stats {STATS_SECS}s --stats-log-level NOTICE --stats-one-line || exit 1"
        ),
        "cd /seed || exit 1".to_string(),
        format!(
            "sha256sum -c /tmp/{SUMS} | tee /tmp/verify.log || \
             {{ grep -v ': OK$' /tmp/verify.log >&2; exit 1; }}"
        ),
    ]
    .join("\n")
}

const SUMS: &str = storage::SUMS_FILE;

/// rclone stats cadence = the UI's byte-sample interval (far under [`progress`]'s stall window)
const STATS_SECS: u32 = 1;

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

/// One-shot Job filling a seed PVC from the bucket ([`puller_cmd`]).
///
/// - Complete only once a pod exits 0 on the whole verified tree
/// - `ttlSecondsAfterFinished` backstops the delete-on-success (a Ctrl-C between create and
///   delete leaks the Job otherwise), long enough that a failure is still there to read
/// - No `activeDeadlineSeconds`: server-side wall clock, the same unmodelable duration
fn puller_job(name: &str, pvc_name: &str, seed: &SeedEntry) -> Job {
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
            "backoffLimit": PULLER_ATTEMPTS - 1,
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
                        "image": PULLER_IMAGE,
                        "command": ["sh", "-c", puller_cmd()],
                        "env": [
                            { "name": "SEED_URL", "value": seed.tree_url() },
                            { "name": "SEED_OID", "value": seed.oid },
                        ],
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
    let job = match jobs.get_opt(name).await {
        Ok(Some(job)) => job,
        // Vanished since the 409 — its owner cleaned up, name free
        Ok(None) => return true,
        // Unknown ≠ free: the caller waits and retries
        Err(e) => {
            tracing::warn!(job = %name, error = %e, "puller job lookup failed");
            return false;
        }
    };
    if !is_job_finished().matches_object(Some(&job)) {
        return false;
    }
    crate::cluster::delete_and_await(jobs, name, &kube::api::DeleteParams::foreground())
        .await
        .is_ok()
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
async fn job_succeeded(jobs: &Api<Job>, name: &str) -> Result<bool, kube::Error> {
    Ok(jobs.get_opt(name).await?.is_some_and(|job| succeeded(&job)))
}

/// Same verdict off a Job already in hand — no second GET to disagree with the first
fn succeeded(job: &Job) -> bool {
    job.status.as_ref().and_then(|s| s.succeeded).is_some_and(|n| n > 0)
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
        .map_err(|e| EnvError::StorageClass { reason: e.to_string() })?;
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

/// Test side only, and budgeted for it: preflight publishes before any runner pod is scheduled,
/// so this bounds label propagation, never a transfer ([`publish`] owns every real wait)
async fn wait_pvc_ready(client: &Client, pvc_name: &str) -> Result<(), EnvError> {
    let deadline = tokio::time::Instant::now() + WAIT_BUDGET;
    loop {
        if pvc_is_ready(client, pvc_name).await? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(EnvError::NotReady {
                component: "seed volume".into(),
                elapsed: WAIT_BUDGET,
            });
        }
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
}

/// Wait out the snapshot with no deadline.
///
/// - Verdict = silence, never duration: a copying driver's `CreateSnapshot` runs for as long as
///   the volume is big, and no constant spans that (same argument as [`materialize`])
/// - [`snapshot::watch`] is the other half of the race — it returns only for states readiness
///   would never arrive to settle
async fn wait_snapshot_ready(
    client: &Client,
    archive: &str,
    snap_name: &str,
    driver: &str,
    total: u64,
    progress: &dyn StepProgress,
) -> Result<(), EnvError> {
    let snap_gvk = volume_snapshot_gvk();
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &snap_gvk);
    tokio::select! {
        ready = snapshot_ready(&api, snap_name) => ready,
        fault = snapshot::watch(client, snap_name, driver, total, progress) => {
            Err(EnvError::ArchiveMaterializeFailed {
                archive: archive.to_string(),
                reason: fault.to_string(),
            })
        }
    }
}

/// Readiness alone, on the object's own watch — [`snapshot::watch`] owns every other ending.
///
/// Watch, not a poll: this wait is unbounded, and a copying driver would otherwise cost
/// thousands of GETs across one snapshot
async fn snapshot_ready(api: &Api<DynamicObject>, snap_name: &str) -> Result<(), EnvError> {
    await_condition(api.clone(), snap_name, is_snapshot_ready()).await.map_err(env_err)?;
    Ok(())
}

fn is_snapshot_ready() -> impl Condition<DynamicObject> {
    |obj: Option<&DynamicObject>| {
        obj.is_some_and(|s| s.data["status"]["readyToUse"].as_bool().unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_and_waiter_derive_the_same_lock_from_the_volume() {
        let pvc = storage::seed_pvc_name(&"b".repeat(64), "hostpath.csi.k8s.io");
        let job = puller_job_name(&pvc);
        assert_eq!(job, format!("puller-{}", pvc.trim_start_matches("seed-")));
        assert!(!job.contains("seed-"), "lock is derived once, not doubly prefixed: {job}");
    }

    #[test]
    fn a_stuck_pull_owned_elsewhere_names_the_job_and_the_remedy() {
        let stall = progress::Stall::NoProgress { transferred: 1 << 30, total: 4 << 30 };
        let EnvError::ArchiveMaterializeFailed { reason, .. } =
            orphaned_puller("chain", "puller-abc", stall)
        else {
            panic!("wrong variant");
        };
        assert!(reason.contains("puller-abc"), "names the Job: {reason}");
        assert!(reason.contains("delete job"), "carries the remedy: {reason}");
        assert!(reason.contains(SEEDS_NAMESPACE), "remedy is runnable: {reason}");
        assert_eq!(reason.lines().count(), 1, "one line: {reason}");
    }

    /// Volume too small for the tree → refused at adoption in ms, not hours in as ENOSPC
    #[test]
    fn a_volume_smaller_than_the_tree_is_refused() {
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

    /// Whole command = shell program → a quoting slip fails here, not an hour into a pull
    #[test]
    fn the_generated_command_parses_as_a_shell_program() {
        use std::io::Write;
        let mut sh = std::process::Command::new("sh")
            .arg("-n")
            .stdin(std::process::Stdio::piped())
            .spawn()
            .expect("sh on PATH");
        sh.stdin.take().expect("piped").write_all(puller_cmd().as_bytes()).expect("write script");
        assert!(sh.wait().expect("sh runs").success(), "{}", puller_cmd());
    }

    /// Every retry = a fresh pod rerunning the same copy (complete files skipped)
    #[test]
    fn the_job_retries_and_is_left_to_read_after_finishing() {
        let seed = SeedEntry {
            name: "chain".to_string(),
            oid: "a".repeat(64),
            size: 1,
            base_uri: "https://e".to_string(),
            key_prefix: storage::KEY_PREFIX.to_string(),
        };
        let spec = puller_job("puller-x", "seed-x", &seed).spec.expect("spec");
        assert_eq!(spec.backoff_limit, Some(PULLER_ATTEMPTS as i32 - 1));
        assert_eq!(spec.ttl_seconds_after_finished, Some(JOB_TTL.as_secs() as i32));
        let pod = spec.template.spec.expect("pod spec");
        let env = pod.containers[0].env.clone().expect("env");
        let url = env.iter().find(|e| e.name == "SEED_URL").and_then(|e| e.value.clone());
        assert_eq!(url.as_deref(), Some(format!("https://e/snap/{}/", "a".repeat(64)).as_str()));
    }

    /// Real rclone against a local `rclone serve http` of a fixture tree: every property the
    /// pod relies on, minus the cluster. Absent tooling skips (asserts ztest's script only)
    struct Served {
        dir: std::path::PathBuf,
        seed: std::path::PathBuf,
        server: std::process::Child,
        url: String,
        oid: String,
    }

    impl Drop for Served {
        fn drop(&mut self) {
            let _ = self.server.kill();
            let _ = self.server.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    const FIXTURE: &[(&str, usize)] = &[("a/b/big.sst", 3_000_000), ("a/x.log", 17), ("top", 1)];

    /// Publish [`FIXTURE`] the way `snapshot push` does, with `sums_for` deciding what the
    /// SHA256SUMS claims (the oid always binds whatever it claims)
    fn serve(tag: &str, sums_for: impl Fn(&str, &[u8]) -> String) -> Option<Served> {
        for tool in ["rclone", "sha256sum"] {
            if std::process::Command::new(tool).arg("--version").output().is_err() {
                eprintln!("skipping: {tool} is not on PATH");
                return None;
            }
        }
        let dir = std::env::temp_dir().join(format!("ztest-puller-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (tree, seed) = (dir.join("tree"), dir.join("seed"));
        std::fs::create_dir_all(&seed).expect("seed dir");
        let mut sums = String::new();
        for (rel, len) in FIXTURE {
            let bytes: Vec<u8> = (0..*len).map(|i| (i ^ (i >> 8)) as u8).collect();
            let path = tree.join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(&path, &bytes).expect("write");
            sums.push_str(&format!("{}  {rel}\n", sums_for(rel, &bytes)));
        }
        std::fs::write(tree.join(SUMS), &sums).expect("sums");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port")
            .port();
        let server = std::process::Command::new("rclone")
            .args(["serve", "http", "--addr", &format!("127.0.0.1:{port}")])
            .arg(&tree)
            .env("RCLONE_CONFIG", "/dev/null")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("rclone serve");
        let up = (0..100).any(|_| {
            std::thread::sleep(Duration::from_millis(50));
            std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
        });
        assert!(up, "rclone serve never listened");
        let oid = storage::oid_of(sums.as_bytes());
        Some(Served { dir, seed, server, url: format!("http://127.0.0.1:{port}/"), oid })
    }

    fn honest(_: &str, bytes: &[u8]) -> String {
        storage::oid_of(bytes)
    }

    impl Served {
        /// Pod paths `/tmp/` + `/seed` rewritten into the fixture dir; the rest runs verbatim
        fn pull(&self, oid: &str) -> (bool, String) {
            let cmd = puller_cmd()
                .replace("/tmp/", &format!("{}/", self.dir.display()))
                .replace("/seed", &self.seed.display().to_string());
            let out = std::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .env("SEED_URL", &self.url)
                .env("SEED_OID", oid)
                .output()
                .expect("sh runs");
            let log = String::from_utf8_lossy(&out.stdout) + String::from_utf8_lossy(&out.stderr);
            (out.status.success(), log.into_owned())
        }

        fn landed(&self, rel: &str) -> Option<Vec<u8>> {
            std::fs::read(self.seed.join(rel)).ok()
        }
    }

    #[test]
    fn the_puller_lands_the_whole_tree_verified_and_metered() {
        let Some(s) = serve("whole", honest) else { return };
        let (ok, log) = s.pull(&s.oid);
        assert!(ok, "pull failed:\n{log}");
        for (rel, len) in FIXTURE {
            assert_eq!(s.landed(rel).map(|b| b.len()), Some(*len), "{rel}:\n{log}");
        }
        assert!(log.lines().any(|l| progress::stats_bytes(l.as_bytes()).is_some()), "{log}");
        assert!(log.lines().any(|l| progress::verified_path(l.as_bytes()).is_some()), "{log}");
    }

    /// SHA256SUMS must hash to the manifest's oid, or nothing it lists is trusted
    #[test]
    fn sums_that_are_not_the_manifests_fail_before_any_file_moves() {
        let Some(s) = serve("oid", honest) else { return };
        let (ok, log) = s.pull(&"0".repeat(64));
        assert!(!ok, "a foreign SHA256SUMS completed the pull:\n{log}");
        assert!(log.contains("hashes to"), "{log}");
        assert_eq!(s.landed("top"), None, "files moved before the sums were trusted");
    }

    /// Bytes served != bytes the sums name → the pod fails, naming the file last in its log
    #[test]
    fn a_file_that_does_not_hash_to_its_sums_line_fails_the_pull() {
        let lie = |rel: &str, bytes: &[u8]| match rel {
            "a/x.log" => "0".repeat(64),
            _ => storage::oid_of(bytes),
        };
        let Some(s) = serve("corrupt", lie) else { return };
        let (ok, log) = s.pull(&s.oid);
        assert!(!ok, "a corrupt file completed the pull:\n{log}");
        let last = log.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or_default();
        assert!(log.contains("a/x.log: FAILED"), "{log}");
        assert!(!last.ends_with(": OK"), "diagnostic buried under OK lines:\n{log}");
    }

    /// Next pod after a death reruns the copy: files already whole are skipped, not refetched
    #[test]
    fn a_rerun_skips_files_already_on_the_volume() {
        let Some(s) = serve("resume", honest) else { return };
        let (ok, log) = s.pull(&s.oid);
        assert!(ok, "first pull failed:\n{log}");
        use std::os::unix::fs::MetadataExt as _;
        let inode = |rel| std::fs::metadata(s.seed.join(rel)).expect("landed").ino();
        let before = inode("a/b/big.sst");
        std::fs::remove_file(s.seed.join("top")).expect("drop one file");
        let (ok, log) = s.pull(&s.oid);
        assert!(ok, "rerun failed:\n{log}");
        assert_eq!(inode("a/b/big.sst"), before, "a complete file was refetched");
        assert_eq!(s.landed("top").map(|b| b.len()), Some(1), "missing file not refetched");
    }
}
