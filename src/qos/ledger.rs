//! Cross-run capacity reservation ledger (`docs/qos-cross-run-ledger-design.md`).
//!
//! The in-memory [`Scheduler`](super::scheduler::Scheduler) admits within one
//! run and can't see other concurrent runs, so two runs can both claim the same
//! headroom and overcommit the node. This ledger makes the claim shared: each
//! run holds a `coordination.k8s.io/Lease` in [`META_NAMESPACE`] reserving its
//! slice, so a run's ceiling is `min(sa_budget, allocatable − non-ztest usage −
//! Σ other runs' reservations)`. A crashed run's lease expires (TTL) and is
//! swept. This seeds the `Scheduler`'s ceiling; it does not replace it.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::Utc;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::{Pod, ServiceAccount};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Api, ListParams, ObjectList, Patch, PatchParams};
use kube::Client;

use super::{GIB, LABEL_RUN_ID, LABEL_USER, MIB, QosClass, Resources};
use crate::resource::impls::policy::RUN_NAMESPACE;

/// Namespace holding the reservation ledger.
pub const META_NAMESPACE: &str = "ztest-meta";

/// Reservation footprint, on the run's Lease.
const ANN_RESERVE_CPU: &str = "ztest.io/reserve-cpu-milli";
const ANN_RESERVE_MEM: &str = "ztest.io/reserve-mem-bytes";
/// Per-SA budget, on the ServiceAccount object.
const ANN_BUDGET_CPU: &str = "ztest.io/budget-cpu-milli";
const ANN_BUDGET_MEM: &str = "ztest.io/budget-mem-bytes";

/// Lease TTL. A run renews well within this; a crashed run's lease is
/// [`is_expired`] after it and swept on the next acquire.
const LEASE_DURATION_SECS: i32 = 60;
/// Renewal cadence — a third of the TTL, so two missed renewals still don't
/// expire a live run.
pub const RENEW_INTERVAL: Duration = Duration::from_secs(20);
/// How long to wait for other runs to release enough capacity before giving up.
const ACQUIRE_WAIT_TIMEOUT: Duration = Duration::from_secs(600);
/// Re-poll cadence while waiting for capacity.
const ACQUIRE_POLL: Duration = Duration::from_secs(3);

/// Budget for a SA with no budget annotation: a conservative slice so an
/// unconfigured user can still run.
const DEFAULT_BUDGET: Resources = Resources::new(8_000, 16 * GIB, 0, 0);

/// Smallest slice worth waiting for: the lightest tier's footprint. A ceiling
/// below this can't admit even one test, so acquisition blocks (up to
/// [`ACQUIRE_WAIT_TIMEOUT`]) for other runs to release rather than start a run
/// that can schedule nothing.
fn min_viable() -> Resources {
    QosClass::Basic.profile().footprint
}

/// A failure to acquire a reservation.
#[derive(Debug)]
pub enum LedgerError {
    /// The cluster stayed too full (other runs' reservations) for the whole
    /// wait window.
    CapacityTimeout { waited: Duration, headroom: Resources },
    /// A kube API call failed.
    Kube(String),
}

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LedgerError::CapacityTimeout { waited, headroom } => write!(
                f,
                "cluster capacity is reserved by other ztest runs: after {}s only \
                 {}m CPU / {} MiB free — try again when they finish",
                waited.as_secs(),
                headroom.cpu_milli,
                headroom.mem_bytes / MIB,
            ),
            LedgerError::Kube(e) => write!(f, "reservation ledger: {e}"),
        }
    }
}

/// A held reservation. Keep it alive for the run's duration (renew it), then
/// [`release`](Renewer::release) it. `slice` seeds the scheduler ceiling.
#[derive(Debug)]
pub struct Grant {
    /// The capacity this run reserved; the in-memory scheduler's ceiling.
    pub slice: Resources,
    /// Handle to renew and release the lease.
    pub renewer: Renewer,
}

/// Renews/releases a run's reservation lease. Cheap to clone (a `kube::Client`
/// handle + the lease name), so the renewal task and the release path share it.
#[derive(Clone)]
pub struct Renewer {
    client: Client,
    run_id: String,
    reserve: Resources,
    user: String,
}

// `kube::Client` is not `Debug`; the identifying detail is the run id.
impl std::fmt::Debug for Renewer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Renewer")
            .field("run_id", &self.run_id)
            .field("reserve", &self.reserve)
            .finish_non_exhaustive()
    }
}

impl Renewer {
    fn api(&self) -> Api<Lease> {
        Api::namespaced(self.client.clone(), META_NAMESPACE)
    }

    /// Renew the lease (bump `renewTime` to now). Best-effort per call; the
    /// caller loops on [`RENEW_INTERVAL`].
    pub async fn renew(&self) -> Result<(), LedgerError> {
        let lease = lease_object(&self.run_id, &self.user, self.reserve);
        self.api()
            .patch(
                &self.run_id,
                &PatchParams::apply("ztest-ledger").force(),
                &Patch::Apply(&lease),
            )
            .await
            .map(|_| ())
            .map_err(|e| LedgerError::Kube(format!("renew lease {}: {e}", self.run_id)))
    }

    /// Renew forever on [`RENEW_INTERVAL`] until the task is aborted. Renewal
    /// failures are transient (a re-apply next tick fixes them) and only matter
    /// if they persist past the TTL, so they are swallowed rather than aborting
    /// the run.
    pub async fn renew_forever(self) {
        loop {
            tokio::time::sleep(RENEW_INTERVAL).await;
            let _ = self.renew().await;
        }
    }

    /// Release the reservation (delete the lease). Best-effort: the TTL sweep and
    /// the run-id label reap both also remove it, so a failed delete self-heals.
    pub async fn release(&self) {
        let _ = self
            .api()
            .delete(&self.run_id, &Default::default())
            .await;
    }
}

/// Acquire this run's reservation: read the ledger + live cluster usage, compute
/// the slice `min(sa_budget, allocatable − non-ztest − Σ others)`, wait (not
/// error) while the slice is below [`min_viable`] in case other runs release,
/// then write the lease and return the [`Grant`]. `allocatable` is the probe's
/// whole-cluster [`ClusterCapacity::allocatable`](super::ClusterCapacity).
pub async fn acquire(
    client: &Client,
    run_id: &str,
    sa: &str,
    user: &str,
    allocatable: Resources,
) -> Result<Grant, LedgerError> {
    ensure_meta_namespace(client).await?;
    let leases: Api<Lease> = Api::namespaced(client.clone(), META_NAMESPACE);
    let budget = sa_budget(client, sa).await;

    let start = std::time::Instant::now();
    loop {
        sweep_expired(&leases).await?;
        let live = leases
            .list(&ListParams::default())
            .await
            .map_err(|e| LedgerError::Kube(format!("list leases: {e}")))?;
        let pods = all_pods(client).await?;

        // No leased run may run more than it reserved; a violation is a
        // scheduling bug, surfaced as a panic not silent overcommit. Only leased
        // runs are checked — a crashed run's orphan pods are a cleanup concern.
        assert_invariant(&live, &pods);

        let others = sum_reservations(&live, run_id);
        let non_ztest = non_ztest_usage(&pods);
        let headroom = allocatable
            .saturating_sub(&non_ztest)
            .saturating_sub(&others);
        let slice = component_min(&budget, &headroom);

        // Enough to admit at least the lightest tier → reserve it and go.
        // Otherwise wait (up to the timeout) for other runs to release.
        if fits(&min_viable(), &slice) {
            let renewer = Renewer {
                client: client.clone(),
                run_id: run_id.to_string(),
                reserve: slice,
                user: user.to_string(),
            };
            renewer.renew().await?; // server-side apply == create on first call
            return Ok(Grant { slice, renewer });
        }

        if start.elapsed() >= ACQUIRE_WAIT_TIMEOUT {
            return Err(LedgerError::CapacityTimeout {
                waited: start.elapsed(),
                headroom,
            });
        }
        tokio::time::sleep(ACQUIRE_POLL).await;
    }
}

/// Whether `needle` fits within `cap` (component-wise ≤). A tiny wrapper over
/// [`Resources::fits_within`] read in the natural order for the call sites here.
fn fits(needle: &Resources, cap: &Resources) -> bool {
    needle.fits_within(cap)
}

/// Ensure the ledger namespace exists (idempotent server-side apply). The run SA
/// holds `namespaces create`, so the ledger is self-provisioning — a cluster
/// that hasn't re-run `ztest setup` since this feature landed still works.
async fn ensure_meta_namespace(client: &Client) -> Result<(), LedgerError> {
    use k8s_openapi::api::core::v1::Namespace;
    let ns: Namespace = Namespace {
        metadata: ObjectMeta {
            name: Some(META_NAMESPACE.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    Api::<Namespace>::all(client.clone())
        .patch(
            META_NAMESPACE,
            &PatchParams::apply("ztest-ledger").force(),
            &Patch::Apply(&ns),
        )
        .await
        .map(|_| ())
        .map_err(|e| LedgerError::Kube(format!("ensure namespace {META_NAMESPACE}: {e}")))
}

/// Delete leases whose TTL has lapsed (crashed runs). Best-effort per lease.
async fn sweep_expired(api: &Api<Lease>) -> Result<(), LedgerError> {
    let now = Utc::now();
    let live = api
        .list(&ListParams::default())
        .await
        .map_err(|e| LedgerError::Kube(format!("list leases for sweep: {e}")))?;
    for lease in &live.items {
        if is_expired(lease, now) {
            if let Some(name) = lease.metadata.name.as_deref() {
                let _ = api.delete(name, &Default::default()).await;
            }
        }
    }
    Ok(())
}

/// Read the SA's budget from its annotations, or [`DEFAULT_BUDGET`] if the SA has
/// no (valid) budget annotation. A missing SA (e.g. a local SA name with no
/// cluster object) also falls back to the default.
async fn sa_budget(client: &Client, sa: &str) -> Resources {
    let api: Api<ServiceAccount> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    match api.get(sa).await {
        Ok(obj) => budget_from_annotations(obj.metadata.annotations.as_ref()),
        Err(_) => DEFAULT_BUDGET,
    }
}

/// All pods cluster-wide (for the usage split). One list call at run start.
async fn all_pods(client: &Client) -> Result<ObjectList<Pod>, LedgerError> {
    Api::<Pod>::all(client.clone())
        .list(&ListParams::default())
        .await
        .map_err(|e| LedgerError::Kube(format!("list pods: {e}")))
}

// ── Pure helpers (unit-tested; no cluster) ──────────────────────────────────

/// Component-wise minimum: the reservation can exceed neither the SA budget nor
/// the live headroom.
fn component_min(a: &Resources, b: &Resources) -> Resources {
    Resources::new(
        a.cpu_milli.min(b.cpu_milli),
        a.mem_bytes.min(b.mem_bytes),
        a.io_bps.min(b.io_bps),
        a.io_iops.min(b.io_iops),
    )
}

/// Sum the reservations of every live lease except `exclude` (this run's own).
fn sum_reservations(leases: &ObjectList<Lease>, exclude: &str) -> Resources {
    leases
        .items
        .iter()
        .filter(|l| l.metadata.name.as_deref() != Some(exclude))
        .fold(Resources::ZERO, |acc, l| {
            acc.saturating_add(&reservation_of(l))
        })
}

/// Parse one of ztest's own `-milli`/`-bytes` annotation values: a plain integer
/// (millicores or bytes), NOT a k8s quantity — so a bare `"32000"` is 32000
/// millicores, never 32000 cores.
fn parse_u64(s: &str) -> Option<u64> {
    s.trim().parse().ok()
}

/// A lease's reserved footprint, from its annotations (absent ⇒ zero).
fn reservation_of(lease: &Lease) -> Resources {
    let ann = lease.metadata.annotations.as_ref();
    let cpu = ann
        .and_then(|a| a.get(ANN_RESERVE_CPU))
        .and_then(|s| parse_u64(s))
        .unwrap_or(0);
    let mem = ann
        .and_then(|a| a.get(ANN_RESERVE_MEM))
        .and_then(|s| parse_u64(s))
        .unwrap_or(0);
    Resources::new(cpu, mem, 0, 0)
}

/// A SA's budget from its annotations, or [`DEFAULT_BUDGET`] when either
/// annotation is absent or unparseable (a typo must not silently become a zero
/// budget that rejects every request).
fn budget_from_annotations(ann: Option<&BTreeMap<String, String>>) -> Resources {
    let cpu = ann.and_then(|a| a.get(ANN_BUDGET_CPU)).and_then(|s| parse_u64(s));
    let mem = ann.and_then(|a| a.get(ANN_BUDGET_MEM)).and_then(|s| parse_u64(s));
    match (cpu, mem) {
        (Some(c), Some(m)) => Resources::new(c, m, u64::MAX, u64::MAX),
        _ => DEFAULT_BUDGET,
    }
}

/// Capacity consumed by pods that are NOT part of any ztest run (system pods,
/// other workloads, and the shared build pods, which carry no run-id label).
/// Subtracted from allocatable before dividing headroom among ztest runs.
fn non_ztest_usage(pods: &ObjectList<Pod>) -> Resources {
    pods.items
        .iter()
        .filter(|p| run_id_of(p).is_none() && consumes(p))
        .fold(Resources::ZERO, |acc, p| {
            acc.saturating_add(&pod_footprint(p))
        })
}

/// This run's-vs-others accounting: usage per run-id that currently holds a live
/// lease. Runs without a live lease (crashed, lease swept) are excluded — their
/// lingering pods are a cleanup concern, not an accounting bug.
fn usage_by_leased_run(
    leases: &ObjectList<Lease>,
    pods: &ObjectList<Pod>,
) -> BTreeMap<String, Resources> {
    let leased: std::collections::BTreeSet<&str> = leases
        .items
        .iter()
        .filter_map(|l| l.metadata.name.as_deref())
        .collect();
    let mut by_run: BTreeMap<String, Resources> = BTreeMap::new();
    for p in &pods.items {
        if !consumes(p) {
            continue;
        }
        if let Some(run) = run_id_of(p) {
            if leased.contains(run) {
                let e = by_run.entry(run.to_string()).or_insert(Resources::ZERO);
                *e = e.saturating_add(&pod_footprint(p));
            }
        }
    }
    by_run
}

/// Panic if any leased run is running more than it reserved — the ledger's hard
/// self-consistency invariant. A violation means a run created pods beyond its
/// reservation (a scheduler/ordering bug), so it is a `ztest` defect, not a
/// runtime condition to tolerate.
fn assert_invariant(leases: &ObjectList<Lease>, pods: &ObjectList<Pod>) {
    let reserved: BTreeMap<&str, Resources> = leases
        .items
        .iter()
        .filter_map(|l| l.metadata.name.as_deref().map(|n| (n, reservation_of(l))))
        .collect();
    for (run, usage) in usage_by_leased_run(leases, pods) {
        let cap = reserved.get(run.as_str()).copied().unwrap_or(Resources::ZERO);
        assert!(
            usage.fits_within(&cap),
            "ztest ledger invariant violated: run {run} is using {}m CPU / {} MiB but reserved \
             only {}m / {} MiB — a run created pods beyond its reservation",
            usage.cpu_milli,
            usage.mem_bytes / MIB,
            cap.cpu_milli,
            cap.mem_bytes / MIB,
        );
    }
}

/// A pod's run-id label, if any.
fn run_id_of(pod: &Pod) -> Option<&str> {
    pod.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(LABEL_RUN_ID))
        .map(String::as_str)
}

/// A pod's request footprint (spec-derived; the floor the kube-scheduler holds).
fn pod_footprint(pod: &Pod) -> Resources {
    pod.spec
        .as_ref()
        .map(super::units::pod_effective_request)
        .unwrap_or(Resources::ZERO)
}

/// Whether a pod still consumes scheduled capacity (not a settled Succeeded/
/// Failed pod, which the node has reclaimed).
fn consumes(pod: &Pod) -> bool {
    !matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some("Succeeded") | Some("Failed")
    )
}

/// Whether a lease's TTL has lapsed as of `now` (`renewTime + duration < now`).
/// A lease missing its `renewTime`/`duration` is treated as live (never sweep on
/// incomplete data).
fn is_expired(lease: &Lease, now: chrono::DateTime<Utc>) -> bool {
    let Some(spec) = lease.spec.as_ref() else {
        return false;
    };
    let (Some(renew), Some(dur)) = (spec.renew_time.as_ref(), spec.lease_duration_seconds) else {
        return false;
    };
    renew.0 + chrono::Duration::seconds(dur as i64) < now
}

/// The Lease object for `run_id` reserving `reserve`, stamped with the run-id and
/// user labels (so the existing label reap also cleans it up) and a fresh
/// `renewTime`. Built identically on acquire and every renew (server-side apply).
fn lease_object(run_id: &str, user: &str, reserve: Resources) -> Lease {
    let labels = BTreeMap::from([
        (LABEL_RUN_ID.to_string(), run_id.to_string()),
        (LABEL_USER.to_string(), user.to_string()),
    ]);
    let annotations = BTreeMap::from([
        (ANN_RESERVE_CPU.to_string(), reserve.cpu_milli.to_string()),
        (ANN_RESERVE_MEM.to_string(), reserve.mem_bytes.to_string()),
    ]);
    Lease {
        metadata: ObjectMeta {
            name: Some(run_id.to_string()),
            namespace: Some(META_NAMESPACE.to_string()),
            labels: Some(labels),
            annotations: Some(annotations),
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(run_id.to_string()),
            lease_duration_seconds: Some(LEASE_DURATION_SECS),
            renew_time: Some(MicroTime(Utc::now())),
            ..Default::default()
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(name: &str, cpu: u64, mem_gib: u64) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                annotations: Some(BTreeMap::from([
                    (ANN_RESERVE_CPU.to_string(), cpu.to_string()),
                    (ANN_RESERVE_MEM.to_string(), (mem_gib * GIB).to_string()),
                ])),
                ..Default::default()
            },
            spec: None,
        }
    }

    fn list<T: Clone + serde::de::DeserializeOwned>(items: Vec<T>) -> ObjectList<T> {
        ObjectList {
            types: Default::default(),
            metadata: Default::default(),
            items,
        }
    }

    #[test]
    fn sum_reservations_excludes_self() {
        let leases = list(vec![
            lease("run-a", 8_000, 16),
            lease("run-b", 4_000, 8),
            lease("me", 32_000, 24),
        ]);
        let others = sum_reservations(&leases, "me");
        assert_eq!(others.cpu_milli, 12_000);
        assert_eq!(others.mem_bytes, 24 * GIB);
    }

    #[test]
    fn component_min_takes_the_tighter_of_each_dimension() {
        let budget = Resources::new(32_000, 24 * GIB, u64::MAX, u64::MAX);
        let headroom = Resources::new(40_000, 16 * GIB, 0, 0);
        let slice = component_min(&budget, &headroom);
        assert_eq!(slice.cpu_milli, 32_000, "CPU bounded by budget");
        assert_eq!(slice.mem_bytes, 16 * GIB, "mem bounded by headroom");
    }

    #[test]
    fn budget_defaults_when_annotation_absent_or_bad() {
        assert_eq!(budget_from_annotations(None), DEFAULT_BUDGET);
        let partial = BTreeMap::from([(ANN_BUDGET_CPU.to_string(), "16000".to_string())]);
        assert_eq!(budget_from_annotations(Some(&partial)), DEFAULT_BUDGET);
        let bad = BTreeMap::from([
            (ANN_BUDGET_CPU.to_string(), "not-a-number".to_string()),
            (ANN_BUDGET_MEM.to_string(), (24 * GIB).to_string()),
        ]);
        assert_eq!(budget_from_annotations(Some(&bad)), DEFAULT_BUDGET);
    }

    #[test]
    fn budget_reads_valid_annotations() {
        let ann = BTreeMap::from([
            (ANN_BUDGET_CPU.to_string(), "72000".to_string()),
            (ANN_BUDGET_MEM.to_string(), (46 * GIB).to_string()),
        ]);
        let b = budget_from_annotations(Some(&ann));
        assert_eq!(b.cpu_milli, 72_000);
        assert_eq!(b.mem_bytes, 46 * GIB);
    }

    #[test]
    fn expiry_uses_renew_time_plus_duration() {
        let now = Utc::now();
        let mut l = lease("run", 1_000, 1);
        // No spec → never expired (incomplete data).
        assert!(!is_expired(&l, now));
        l.spec = Some(LeaseSpec {
            lease_duration_seconds: Some(60),
            renew_time: Some(MicroTime(now - chrono::Duration::seconds(30))),
            ..Default::default()
        });
        assert!(!is_expired(&l, now), "renewed 30s ago, TTL 60s → live");
        l.spec = Some(LeaseSpec {
            lease_duration_seconds: Some(60),
            renew_time: Some(MicroTime(now - chrono::Duration::seconds(90))),
            ..Default::default()
        });
        assert!(is_expired(&l, now), "renewed 90s ago, TTL 60s → expired");
    }
}
