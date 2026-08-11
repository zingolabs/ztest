//! Cross-run capacity reservation ledger (`docs/design-qos.md`).
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
use kube::Client;
use kube::api::{Api, ListParams, ObjectList, Patch, PatchParams};

use super::{LABEL_RUN_ID, LABEL_USER, MIB, QosClass, Resources};
use crate::resource::impls::policy::RUN_NAMESPACE;

/// Namespace holding the reservation ledger.
pub const META_NAMESPACE: &str = "ztest-meta";

/// Reservation footprint, on the run's Lease.
const ANN_RESERVE_CPU: &str = "ztest.io/reserve-cpu-milli";
const ANN_RESERVE_MEM: &str = "ztest.io/reserve-mem-bytes";
/// Per-SA budget, on the ServiceAccount object.
const ANN_BUDGET_CPU: &str = "ztest.io/budget-cpu-milli";
const ANN_BUDGET_MEM: &str = "ztest.io/budget-mem-bytes";

/// Lease TTL. The [`CapacityGovernor`](super::governor::CapacityGovernor) renews
/// well within this on every reconcile; a crashed run's lease is [`is_expired`]
/// after it and swept on the next acquire or governor tick.
const LEASE_DURATION_SECS: i32 = 60;
/// Heartbeat cadence for a lease with no governor driving it
/// ([`Renewer::spawn_heartbeat`]). A third of the TTL, so two consecutive missed
/// renewals still leave the lease live.
const RENEW_INTERVAL: Duration = Duration::from_secs(LEASE_DURATION_SECS as u64 / 3);
/// How long to wait for other runs to release enough capacity before giving up.
const ACQUIRE_WAIT_TIMEOUT: Duration = Duration::from_secs(600);
/// Re-poll cadence while waiting for capacity.
const ACQUIRE_POLL: Duration = Duration::from_secs(3);

/// Budget for a SA with no budget annotation: the whole node. An unconfigured
/// user is capped only by live cross-run fair-share (a lone run gets the cluster,
/// N runs get ~1/N each — see [`fair_reserve`]), not by a fixed conservative
/// slice that would strangle a single run to a couple of tests. The I/O
/// dimensions are unbounded here: the per-SA budget governs the contended
/// node-allocatable dimensions (CPU/memory); disk bandwidth/IOPS are gated by
/// cluster capacity, not per-identity policy (matching an explicit annotation,
/// which also sets I/O to `MAX`).
fn default_budget(allocatable: Resources) -> Resources {
    Resources::new(
        allocatable.cpu_milli,
        allocatable.mem_bytes,
        u64::MAX,
        u64::MAX,
    )
}

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
    CapacityTimeout {
        waited: Duration,
        headroom: Resources,
    },
    /// The ledger namespace does not exist. It is provisioned by `ztest setup`,
    /// not by a run — so this means the cluster hasn't been set up (or not since
    /// the ledger namespace moved into setup).
    MetaNamespaceMissing,
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
            LedgerError::MetaNamespaceMissing => write!(
                f,
                "the {META_NAMESPACE} namespace does not exist — run `ztest setup` \
                 (with a cluster-admin kubeconfig) to provision the cross-run \
                 reservation ledger"
            ),
            LedgerError::Kube(e) => write!(f, "reservation ledger: {e}"),
        }
    }
}

/// A held reservation. Keep it alive for the run's duration (renew it), then
/// [`release`](Renewer::release) it. `slice` seeds the scheduler ceiling.
#[derive(Debug)]
pub struct Grant {
    /// The capacity this run reserved at startup; the scheduler's initial ceiling.
    /// The [`CapacityGovernor`](super::governor::CapacityGovernor) drives it live
    /// from here on.
    pub slice: Resources,
    /// This SA's total budget (annotation or whole-node default). The governor
    /// caps the live reservation by it; the scheduler enforces it per-request.
    pub budget: Resources,
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

    /// Renew the lease at the acquire-time reservation (bump `renewTime`).
    /// Best-effort per call.
    pub async fn renew(&self) -> Result<(), LedgerError> {
        self.renew_at(self.reserve).await
    }

    /// Renew the lease, rewriting its reservation to `reserve`. The
    /// [`CapacityGovernor`](super::governor::CapacityGovernor) calls this every
    /// reconcile so the Lease always reflects the run's live reservation — the
    /// bump to `renewTime` doubles as the TTL heartbeat. Best-effort per call.
    pub async fn renew_at(&self, reserve: Resources) -> Result<(), LedgerError> {
        let lease = lease_object(&self.run_id, &self.user, reserve);
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

    /// Release the reservation (delete the lease). Best-effort: the TTL sweep and
    /// the run-id label reap both also remove it, so a failed delete self-heals.
    pub async fn release(&self) {
        let _ = self.api().delete(&self.run_id, &Default::default()).await;
    }

    /// Spawn a TTL heartbeat that renews this lease at its acquire-time
    /// reservation until the returned [`Heartbeat`] is dropped.
    ///
    /// A [`Reserve::Elastic`] run gets its heartbeat for free — the
    /// [`CapacityGovernor`](super::governor::CapacityGovernor) renews on every
    /// reconcile. A [`Reserve::Fixed`] holder has no governor (its reservation
    /// never changes), yet can outlive the TTL: a cold BuildKit compile runs for
    /// minutes against a 60 s lease. Without this the lease would be swept
    /// mid-build and the pod it covers would go back to being uncounted.
    pub fn spawn_heartbeat(&self) -> Heartbeat {
        let renewer = self.clone();
        Heartbeat {
            task: tokio::spawn(async move {
                let mut ticker = tokio::time::interval(RENEW_INTERVAL);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                ticker.tick().await; // fires immediately; the lease was just written
                loop {
                    ticker.tick().await;
                    let _ = renewer.renew().await;
                }
            }),
        }
    }
}

/// Keeps a fixed reservation's Lease alive. Dropping it stops the renewals, after
/// which the lease expires and is swept — so it must outlive the pods the
/// reservation covers. Release the Lease itself through the [`Renewer`].
#[derive(Debug)]
pub struct Heartbeat {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// How much of the available headroom a caller wants to hold.
///
/// Both modes wait on the same ledger and write the same kind of Lease; they
/// differ only in how much they take and how little is worth waiting for. Keeping
/// that as one axis (rather than a free-floating threshold parameter) is what lets
/// the build phase and the test run share [`acquire`] verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reserve {
    /// A test run: take the whole available slice and let the
    /// [`CapacityGovernor`](super::governor::CapacityGovernor) resize it live.
    /// Worth starting as soon as the lightest tier fits (`min_viable`).
    Elastic,
    /// A fixed consumer that knows its own footprint — the BuildKit pod. Takes
    /// exactly that much (never more, so peers keep the rest) and waits until
    /// exactly that much is free.
    Fixed(Resources),
}

impl Reserve {
    /// The smallest available slice worth returning from a wait.
    fn threshold(self) -> Resources {
        match self {
            Reserve::Elastic => min_viable(),
            Reserve::Fixed(r) => r,
        }
    }

    /// What to write onto the Lease, given the slice currently available.
    fn amount(self, slice: Resources) -> Resources {
        match self {
            Reserve::Elastic => slice,
            Reserve::Fixed(r) => r,
        }
    }
}

/// Acquire a reservation under `lease_id`: read the ledger + live cluster usage,
/// compute the available slice `min(sa_budget, allocatable − unreserved − Σ
/// others)`, wait (not error) while it is below `Reserve::threshold` in case
/// other runs release, then write the lease and return the [`Grant`].
/// `allocatable` is the probe's whole-cluster
/// [`ClusterCapacity::allocatable`](super::ClusterCapacity).
///
/// Every pod ztest places must be covered by one of these for the whole time it
/// occupies a node — that is the invariant `assert_invariant` enforces, and the
/// reason the build phase acquires one of its own rather than creating its pod
/// unbudgeted.
pub async fn acquire(
    client: &Client,
    lease_id: &str,
    sa: &str,
    user: &str,
    allocatable: Resources,
    want: Reserve,
) -> Result<Grant, LedgerError> {
    require_meta_namespace(client).await?;
    let leases: Api<Lease> = Api::namespaced(client.clone(), META_NAMESPACE);
    let budget = sa_budget(client, sa, default_budget(allocatable)).await;

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

        let others = sum_reservations(&live, lease_id);
        let unreserved = unreserved_usage(&pods, &live);
        let headroom = allocatable
            .saturating_sub(&unreserved)
            .saturating_sub(&others);
        let slice = budget.min(&headroom);

        // Enough for what this caller came for → reserve it and go. Otherwise
        // wait (up to the timeout) for other runs to release.
        if fits(&want.threshold(), &slice) {
            let reserve = want.amount(slice);
            let renewer = Renewer {
                client: client.clone(),
                run_id: lease_id.to_string(),
                reserve,
                user: user.to_string(),
            };
            renewer.renew().await?; // server-side apply == create on first call
            return Ok(Grant {
                slice: reserve,
                budget,
                renewer,
            });
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

/// Verify the ledger namespace exists. `ztest setup` provisions it (a fixed,
/// once-ever cluster object); the minimal run SA only reads it here and writes
/// Leases inside it — it never creates the namespace. A read-only `get` keeps the
/// run SA free of the cluster-scoped namespace-write grant, and an absent
/// namespace fails fast with an actionable message rather than a cryptic 403 or
/// 404 on the first Lease write.
async fn require_meta_namespace(client: &Client) -> Result<(), LedgerError> {
    use k8s_openapi::api::core::v1::Namespace;
    match Api::<Namespace>::all(client.clone())
        .get_opt(META_NAMESPACE)
        .await
        .map_err(|e| LedgerError::Kube(format!("get namespace {META_NAMESPACE}: {e}")))?
    {
        Some(_) => Ok(()),
        None => Err(LedgerError::MetaNamespaceMissing),
    }
}

/// The ledger's Lease API in [`META_NAMESPACE`].
pub(crate) fn lease_api(client: &Client) -> Api<Lease> {
    Api::namespaced(client.clone(), META_NAMESPACE)
}

/// List every live reservation lease. Shared by acquire and the governor.
pub(crate) async fn list_leases(api: &Api<Lease>) -> Result<ObjectList<Lease>, LedgerError> {
    api.list(&ListParams::default())
        .await
        .map_err(|e| LedgerError::Kube(format!("list leases: {e}")))
}

/// Delete leases whose TTL has lapsed (crashed runs). Best-effort per lease.
pub(crate) async fn sweep_expired(api: &Api<Lease>) -> Result<(), LedgerError> {
    let now = Utc::now();
    let live = api
        .list(&ListParams::default())
        .await
        .map_err(|e| LedgerError::Kube(format!("list leases for sweep: {e}")))?;
    for lease in &live.items {
        if is_expired(lease, now)
            && let Some(name) = lease.metadata.name.as_deref()
        {
            let _ = api.delete(name, &Default::default()).await;
        }
    }
    Ok(())
}

/// Read the SA's budget from its annotations, or `default` (the whole-node
/// [`default_budget`]) if the SA has no (valid) budget annotation. A missing SA
/// (e.g. a local SA name with no cluster object) also falls back to the default.
async fn sa_budget(client: &Client, sa: &str, default: Resources) -> Resources {
    let api: Api<ServiceAccount> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    match api.get(sa).await {
        Ok(obj) => budget_from_annotations(obj.metadata.annotations.as_ref(), default),
        Err(_) => default,
    }
}

/// All pods cluster-wide (for the usage split). One list call at run start.
pub(crate) async fn all_pods(client: &Client) -> Result<ObjectList<Pod>, LedgerError> {
    Api::<Pod>::all(client.clone())
        .list(&ListParams::default())
        .await
        .map_err(|e| LedgerError::Kube(format!("list pods: {e}")))
}

// ── Pure helpers (unit-tested; no cluster) ──────────────────────────────────

/// The reservation a run should hold *right now*, given live cluster state and
/// its own live demand. This is the heart of the elastic, work-conserving,
/// cross-run-fair policy the [`CapacityGovernor`](super::governor::CapacityGovernor)
/// drives; kept pure so it is unit-tested without a cluster.
///
/// - `allocatable` — whole-cluster node capacity (fixed from the probe).
/// - `non_ztest` — capacity used by pods outside any ztest run (system, build).
/// - `others` — Σ of *other* live runs' lease reservations (this run excluded).
/// - `active_runs` — count of live leases *including this run* (≥1).
/// - `budget` — this SA's total budget (whole-node default or annotation).
/// - `committed` — footprint this run is already running (its live leases).
/// - `demand` — committed plus everything this run still has queued.
///
/// The reservation is the run's fair share `(allocatable − non_ztest) / N`,
/// lowered to what it can actually use (`demand`) and its `budget`, capped by the
/// headroom physically left after other runs, and floored at `committed` — a run
/// never reserves below what it is already running, because there is no
/// preemption (`docs/design-qos.md`). A lone run (`N = 1`) with more work than the
/// cluster holds therefore reserves the whole cluster; `N` runs each converge to
/// `~1/N`, and a run wanting less than its share cedes the rest.
#[allow(clippy::too_many_arguments)]
fn fair_reserve(
    allocatable: Resources,
    non_ztest: Resources,
    others: Resources,
    active_runs: u64,
    budget: Resources,
    committed: Resources,
    demand: Resources,
) -> Resources {
    let n = active_runs.max(1);
    let usable = allocatable.saturating_sub(&non_ztest);
    let fair = Resources::new(
        usable.cpu_milli / n,
        usable.mem_bytes / n,
        usable.io_bps / n,
        usable.io_iops / n,
    );
    // What we'd like: our demand, but no more than our fair share or our budget.
    let want = demand.min(&fair).min(&budget);
    // Headroom physically available to us: `others` already excludes this run, so
    // our own current reservation is not subtracted here.
    let headroom = usable.saturating_sub(&others);
    // Never below what we are already running (no preemption).
    want.min(&headroom).max(&committed)
}

/// This run's live reservation from a fresh snapshot of the ledger and cluster
/// pods: the fair-share elastic policy ([`fair_reserve`]) applied to the current
/// lease population and this run's own `committed`/`demand`. The
/// [`CapacityGovernor`](super::governor::CapacityGovernor) lists both every
/// reconcile and calls this; keeping the accounting here means the governor never
/// touches the ledger's internal shapes.
pub(crate) fn reserve_from_state(
    leases: &ObjectList<Lease>,
    pods: &ObjectList<Pod>,
    run_id: &str,
    allocatable: Resources,
    budget: Resources,
    committed: Resources,
    demand: Resources,
) -> Resources {
    let others = sum_reservations(leases, run_id);
    let unreserved = unreserved_usage(pods, leases);
    let active_runs = leases.items.len() as u64;
    fair_reserve(
        allocatable,
        unreserved,
        others,
        active_runs,
        budget,
        committed,
        demand,
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

/// A SA's budget from its annotations, or `default` when either annotation is
/// absent or unparseable (a typo must not silently become a zero budget that
/// rejects every request).
fn budget_from_annotations(
    ann: Option<&BTreeMap<String, String>>,
    default: Resources,
) -> Resources {
    let cpu = ann
        .and_then(|a| a.get(ANN_BUDGET_CPU))
        .and_then(|s| parse_u64(s));
    let mem = ann
        .and_then(|a| a.get(ANN_BUDGET_MEM))
        .and_then(|s| parse_u64(s));
    match (cpu, mem) {
        (Some(c), Some(m)) => Resources::new(c, m, u64::MAX, u64::MAX),
        _ => default,
    }
}

/// Capacity consumed by every pod that no live Lease reserves: system pods and
/// other workloads (no run-id at all), plus pods whose run-id has no live lease —
/// a crashed run's orphans, and a run's pods still terminating after its lease was
/// released. Subtracted from allocatable before dividing headroom among ztest runs.
///
/// Membership is "covered by a live reservation", not "carries a run-id label".
/// The distinction is load-bearing: a labelled pod whose lease is gone holds real
/// node memory while being invisible to [`sum_reservations`], so keying on the
/// label alone drops it through both buckets and charges it to nobody. The build
/// pod made that hole expensive — it carries a run-id and occupies
/// [`BUILDKIT_BUILD`](super::build::BUILDKIT_BUILD), the largest footprint ztest
/// places — so admission handed out capacity a terminating builder still held and
/// the next run's builder landed `Insufficient memory`.
fn unreserved_usage(pods: &ObjectList<Pod>, leases: &ObjectList<Lease>) -> Resources {
    let leased: std::collections::BTreeSet<&str> = leases
        .items
        .iter()
        .filter_map(|l| l.metadata.name.as_deref())
        .collect();
    pods.items
        .iter()
        .filter(|p| consumes(p))
        .filter(|p| !run_id_of(p).is_some_and(|r| leased.contains(r)))
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
        if let Some(run) = run_id_of(p)
            && leased.contains(run)
        {
            let e = by_run.entry(run.to_string()).or_insert(Resources::ZERO);
            *e = e.saturating_add(&pod_footprint(p));
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
        let cap = reserved
            .get(run.as_str())
            .copied()
            .unwrap_or(Resources::ZERO);
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
pub(crate) fn is_expired(lease: &Lease, now: chrono::DateTime<Utc>) -> bool {
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
    use crate::qos::GIB;

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

    /// A scheduled pod requesting `cpu`m / `mem_gib`, optionally labelled with a
    /// run-id — the shape [`unreserved_usage`] classifies.
    fn pod(run_id: Option<&str>, cpu: u64, mem_gib: u64) -> Pod {
        let labels = run_id.map(|r| BTreeMap::from([(LABEL_RUN_ID.to_string(), r.to_string())]));
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "p", "labels": labels },
            "spec": { "containers": [{
                "name": "c",
                "resources": { "requests": {
                    "cpu": format!("{cpu}m"),
                    "memory": format!("{}Gi", mem_gib),
                }},
            }]},
            "status": { "phase": "Running" },
        }))
        .expect("static Pod manifest is valid")
    }

    /// The regression this module exists for. A build pod carries a run-id label,
    /// so the old `run_id_of(p).is_none()` test excluded it from "non-ztest" usage;
    /// its run holding no live Lease meant `sum_reservations` did not cover it
    /// either. Charged to nobody, its 24 GiB stayed invisible, and admission handed
    /// the same memory to tests — which k8s then refused to place.
    #[test]
    fn a_labelled_pod_whose_run_has_no_lease_is_charged_as_unreserved() {
        let leases = list(vec![lease("run-a", 8_000, 16)]);
        let pods = list(vec![pod(Some("elicb-47192"), 16_000, 24)]);
        let usage = unreserved_usage(&pods, &leases);
        assert_eq!(
            usage.mem_bytes,
            24 * GIB,
            "a build pod whose run holds no lease must be charged to unreserved usage"
        );
        assert_eq!(usage.cpu_milli, 16_000);
    }

    #[test]
    fn unreserved_usage_skips_pods_a_live_lease_already_covers() {
        let leases = list(vec![lease("run-a", 8_000, 16)]);
        let pods = list(vec![
            pod(Some("run-a"), 8_000, 16), // covered by run-a's reservation
            pod(None, 2_000, 4),           // system pod: never covered
        ]);
        let usage = unreserved_usage(&pods, &leases);
        assert_eq!(
            usage.mem_bytes,
            4 * GIB,
            "only the unlabelled system pod is unreserved; run-a's is on its lease"
        );
    }

    /// `Fixed` exists so the builder takes its own footprint and no more — taking
    /// the whole available slice would park capacity peers could have used, and
    /// taking less would leave its pod partly unbudgeted.
    #[test]
    fn fixed_reserves_its_footprint_while_elastic_takes_the_slice() {
        let slice = Resources::new(40_000, 32 * GIB, 0, 0);
        let build = crate::qos::build::BUILDKIT_BUILD;
        assert_eq!(Reserve::Elastic.amount(slice), slice);
        assert_eq!(Reserve::Fixed(build).amount(slice), build);
        assert_eq!(
            Reserve::Fixed(build).threshold(),
            build,
            "a fixed holder waits for exactly what it will occupy"
        );
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
    fn slice_takes_the_tighter_of_budget_and_headroom_per_dimension() {
        let budget = Resources::new(32_000, 24 * GIB, u64::MAX, u64::MAX);
        let headroom = Resources::new(40_000, 16 * GIB, 0, 0);
        let slice = budget.min(&headroom);
        assert_eq!(slice.cpu_milli, 32_000, "CPU bounded by budget");
        assert_eq!(slice.mem_bytes, 16 * GIB, "mem bounded by headroom");
    }

    #[test]
    fn budget_defaults_when_annotation_absent_or_bad() {
        let default = default_budget(Resources::new(72_000, 46 * GIB, 0, 0));
        assert_eq!(budget_from_annotations(None, default), default);
        let partial = BTreeMap::from([(ANN_BUDGET_CPU.to_string(), "16000".to_string())]);
        assert_eq!(budget_from_annotations(Some(&partial), default), default);
        let bad = BTreeMap::from([
            (ANN_BUDGET_CPU.to_string(), "not-a-number".to_string()),
            (ANN_BUDGET_MEM.to_string(), (24 * GIB).to_string()),
        ]);
        assert_eq!(budget_from_annotations(Some(&bad), default), default);
    }

    #[test]
    fn whole_node_default_budget_is_allocatable_with_unbounded_io() {
        let b = default_budget(Resources::new(72_000, 46 * GIB, 0, 0));
        assert_eq!(b.cpu_milli, 72_000);
        assert_eq!(b.mem_bytes, 46 * GIB);
        assert_eq!(b.io_bps, u64::MAX, "I/O ungoverned by the per-SA budget");
        assert_eq!(b.io_iops, u64::MAX);
    }

    // ── Fair-share elastic reservation ─────────────────────────────────

    /// A 72c / 48Gi node, nothing else on it.
    fn node() -> Resources {
        Resources::new(72_000, 48 * GIB, 0, 0)
    }

    #[test]
    fn lone_run_with_more_work_than_the_cluster_reserves_the_whole_cluster() {
        // N=1, huge demand, no other runs, no non-ztest, whole-node budget.
        let r = fair_reserve(
            node(),
            Resources::ZERO,
            Resources::ZERO,
            1,
            default_budget(node()),
            Resources::new(6_000, 4 * GIB, 0, 0), // committed
            Resources::new(300_000, 200 * GIB, 0, 0), // demand ≫ cluster
        );
        assert_eq!(r.cpu_milli, 72_000, "a lone run gets the whole node");
        assert_eq!(r.mem_bytes, 48 * GIB);
    }

    #[test]
    fn two_busy_runs_each_reserve_half() {
        // N=2: fair share is half the node. The other run reserves its half, so
        // headroom is the remaining half; demand is unbounded.
        let other_half = Resources::new(36_000, 24 * GIB, 0, 0);
        let r = fair_reserve(
            node(),
            Resources::ZERO,
            other_half, // others' reservation
            2,
            default_budget(node()),
            Resources::new(10_000, 8 * GIB, 0, 0),
            Resources::new(300_000, 200 * GIB, 0, 0),
        );
        assert_eq!(r.cpu_milli, 36_000, "fair half of 72c");
        assert_eq!(r.mem_bytes, 24 * GIB);
    }

    #[test]
    fn a_run_wanting_less_than_its_share_reserves_only_its_demand() {
        // Work-conserving: demand below the fair share caps the reservation, so
        // the surplus is left for other runs.
        let r = fair_reserve(
            node(),
            Resources::ZERO,
            Resources::ZERO,
            2,
            default_budget(node()),
            Resources::new(5_000, 3 * GIB, 0, 0),
            Resources::new(9_000, 6 * GIB, 0, 0), // demand < fair (36c)
        );
        assert_eq!(r.cpu_milli, 9_000, "reserve only what we can use");
        assert_eq!(r.mem_bytes, 6 * GIB);
    }

    #[test]
    fn reservation_never_drops_below_committed() {
        // A run that grabbed the cluster alone is now sharing (N=2, fair=36c) but
        // is still running 50c: it holds its committed footprint (no preemption),
        // even though that exceeds its new fair share.
        let r = fair_reserve(
            node(),
            Resources::ZERO,
            Resources::ZERO,
            2,
            default_budget(node()),
            Resources::new(50_000, 30 * GIB, 0, 0), // committed > fair
            Resources::new(300_000, 200 * GIB, 0, 0),
        );
        assert_eq!(
            r.cpu_milli, 50_000,
            "keeps running work despite a smaller share"
        );
        assert_eq!(r.mem_bytes, 30 * GIB);
    }

    #[test]
    fn headroom_caps_below_fair_share_when_others_and_non_ztest_crowd_the_node() {
        // Fair share (N=2) is 36c, but non-ztest load (10c) plus another run's
        // reservation (40c) leave only 22c of real headroom.
        let r = fair_reserve(
            node(),
            Resources::new(10_000, 4 * GIB, 0, 0),  // non-ztest
            Resources::new(40_000, 20 * GIB, 0, 0), // others
            2,
            default_budget(node()),
            Resources::ZERO,
            Resources::new(300_000, 200 * GIB, 0, 0),
        );
        // usable = 72-10 = 62c; headroom = 62-40 = 22c; fair = 62/2 = 31c.
        assert_eq!(
            r.cpu_milli, 22_000,
            "physical headroom binds below the fair share"
        );
    }

    #[test]
    fn budget_annotation_caps_below_fair_share() {
        // A lone run whose SA is annotated to 16c never reserves more than that.
        let r = fair_reserve(
            node(),
            Resources::ZERO,
            Resources::ZERO,
            1,
            Resources::new(16_000, 12 * GIB, u64::MAX, u64::MAX),
            Resources::ZERO,
            Resources::new(300_000, 200 * GIB, 0, 0),
        );
        assert_eq!(
            r.cpu_milli, 16_000,
            "SA budget is the hard per-identity cap"
        );
        assert_eq!(r.mem_bytes, 12 * GIB);
    }

    #[test]
    fn budget_reads_valid_annotations() {
        let default = default_budget(Resources::new(72_000, 46 * GIB, 0, 0));
        let ann = BTreeMap::from([
            (ANN_BUDGET_CPU.to_string(), "72000".to_string()),
            (ANN_BUDGET_MEM.to_string(), (46 * GIB).to_string()),
        ]);
        let b = budget_from_annotations(Some(&ann), default);
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
