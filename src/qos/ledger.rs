//! Cross-run capacity reservation ledger (`docs/design-qos.md`).
//!
//! - [`Scheduler`](super::scheduler::Scheduler) admits within one run and can't see peers
//!   → two runs claim the same headroom and overcommit the node
//! - Each run holds a `coordination.k8s.io/Lease` in [`META_NAMESPACE`] reserving its slice
//!   → ceiling = `min(sa_budget, allocatable − non-ztest usage − Σ others' reservations)`
//! - Crashed run's lease expires (TTL) and is swept
//! - Seeds the `Scheduler`'s ceiling, never replaces it

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::{Pod, ServiceAccount};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::Client;
use kube::api::{Api, ListParams, ObjectList, Patch, PatchParams};
use tokio::sync::watch;

use super::beacon::{ANN_RESERVE_CPU, ANN_RESERVE_MEM, Beacon, LeaseKind, Progress};
use super::{LABEL_RUN_ID, LABEL_USER, MIB, QosClass, Resources};
use crate::naming::RUN_NAMESPACE;

/// Namespace holding the reservation ledger
pub const META_NAMESPACE: &str = "ztest-meta";

/// Per-SA budget, on the ServiceAccount object
const ANN_BUDGET_CPU: &str = "ztest.io/budget-cpu-milli";
const ANN_BUDGET_MEM: &str = "ztest.io/budget-mem-bytes";

/// Lease TTL; a [`Reservation`] renews well inside it, a crashed run's lease goes
/// [`is_expired`] and is swept on the next acquire or reconcile tick
const LEASE_DURATION_SECS: i32 = 60;
/// Elastic resize cadence — freed capacity reaches admission within seconds
const RECONCILE_INTERVAL: Duration = Duration::from_secs(2);
/// Fixed-reservation renew cadence = TTL/3 (two missed ticks still leave it live)
///
/// Separate from [`RECONCILE_INTERVAL`]: a fixed lease has nothing to recompute,
/// so reconcile-rate writes would be pure apiserver load over a multi-hour sync
const RENEW_INTERVAL: Duration = Duration::from_secs(LEASE_DURATION_SECS as u64 / 3);
/// How long to wait for other runs to release enough capacity before giving up
const ACQUIRE_WAIT_TIMEOUT: Duration = Duration::from_secs(600);
/// Re-poll cadence while waiting for capacity
const ACQUIRE_POLL: Duration = Duration::from_secs(3);

/// Budget for an un-annotated SA = the whole node.
///
/// - Capped only by live cross-run fair share ([`fair_reserve`]), not a fixed slice that
///   would strangle a lone run to a couple of tests
/// - I/O + disk unbounded: the per-SA budget governs the contended node-allocatable
///   dimensions; storage gates on cluster capacity, not per-identity policy
fn default_budget(allocatable: Resources) -> Resources {
    Resources::cpu_mem_unbounded_rest(allocatable.cpu_milli, allocatable.mem_bytes)
}

/// Smallest slice worth waiting for = the lightest tier's footprint. Below it a ceiling
/// admits no test at all, so acquisition blocks (up to [`ACQUIRE_WAIT_TIMEOUT`]) rather
/// than starting a run that can schedule nothing
fn min_viable() -> Resources {
    QosClass::Basic.profile().footprint
}

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    /// `headroom` is the whole diagnosis: it says how far short the cluster is, which the
    /// "try again when they finish" this used to append did not
    #[error("capacity held by other runs: {}m cpu / {} MiB free after {}s",
            headroom.cpu_milli, headroom.mem_bytes / MIB, waited.as_secs())]
    CapacityTimeout { waited: Duration, headroom: Resources },

    /// Provisioned by `ztest cluster setup`, never by a run → the cluster isn't set up
    #[error("no {META_NAMESPACE} namespace; run `ztest cluster setup`")]
    MetaNamespaceMissing,

    #[error("reservation ledger: {0}")]
    Kube(String),
}

/// One live reservation = one `coordination.k8s.io/Lease`
///
/// - Owns the TTL heartbeat + (elastic only) a resize loop over live cluster state
/// - Lone run fills the cluster, busy run cedes its fair share
pub struct Reservation {
    inner: Inner,
    /// Scheduler ceiling (tracks the reservation when elastic, else fixed)
    ceiling: watch::Receiver<Resources>,
    /// Live appetite → sizes an elastic reservation (ignored when fixed)
    demand: watch::Sender<Demand>,
    budget: Resources,
    task: Option<tokio::task::JoinHandle<()>>,
}

/// Lease identity + the status currently written (cloned into the bg task).
///
/// One shared [`Beacon`]: the reconcile loop owns `reserve`, the engine owns the progress
/// fields via [`report_status`](Reservation::report_status), and every write emits both
#[derive(Clone)]
struct Inner {
    client: Client,
    id: String,
    user: String,
    beacon: Arc<Mutex<Beacon>>,
}

/// - `committed` = running now → reservation floor (no preemption)
/// - `demand` = most it could use → reservation ceiling (peers keep the rest)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Demand {
    committed: Resources,
    demand: Resources,
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reservation")
            .field("id", &self.inner.id)
            .field("reserved", &self.reserved())
            .finish_non_exhaustive()
    }
}

/// Drop = stop renewing → lease lapses at TTL. Deleting is explicit
/// ([`release`](Reservation::release)): the other exit is a hand-off (`ztest sync
/// start` → driver pod adopts), which a deleting Drop could not distinguish
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

impl Inner {
    fn api(&self) -> Api<Lease> {
        Api::namespaced(self.client.clone(), META_NAMESPACE)
    }

    /// Write at `reserve`, carrying the live status (`renewTime` bump = the TTL heartbeat)
    ///
    /// - Server-side apply → overlapping holders converge, never race
    /// - That is what lets a sync's lease pass CLI → driver pod
    async fn write(&self, reserve: Resources) -> Result<(), LedgerError> {
        // Cloned (never held across the await), amount applied only once the write lands
        let beacon = {
            let b = self.beacon.lock().expect("beacon mutex poisoned");
            Beacon { reserve, ..b.clone() }
        };
        let lease = lease_object(&self.id, &self.user, &beacon);
        self.api()
            .patch(&self.id, &PatchParams::apply("ztest-ledger").force(), &Patch::Apply(&lease))
            .await
            .map(|_| self.beacon.lock().expect("beacon mutex poisoned").reserve = reserve)
            .map_err(|e| LedgerError::Kube(format!("renew lease {}: {e}", self.id)))
    }

    fn current(&self) -> Resources {
        self.beacon.lock().expect("beacon mutex poisoned").reserve
    }

    async fn delete(&self) {
        let _ = self.api().delete(&self.id, &Default::default()).await;
    }
}

impl Reservation {
    /// Capacity currently reserved
    pub fn reserved(&self) -> Resources {
        self.inner.current()
    }

    /// SA's total budget = the per-request ceiling the scheduler enforces
    pub fn budget(&self) -> Resources {
        self.budget
    }

    /// Live scheduler ceiling = what this run may admit into
    pub fn ceiling(&self) -> watch::Receiver<Resources> {
        self.ceiling.clone()
    }

    /// Publish live appetite (elastic resizes next reconcile, fixed ignores)
    pub fn report_demand(&self, committed: Resources, demand: Resources) {
        let _ = self.demand.send(Demand { committed, demand });
    }

    /// Delete the lease → capacity returns to peers now, not at the TTL
    pub async fn release(mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
        self.inner.delete().await;
    }

    /// Adopt another process's reservation, renewing it here
    ///
    /// - `ztest sync start` acquires (admission must be refusable while watched) then exits
    /// - Driver pod picks it up for the pods' lifetime
    /// - `reserve` must match the acquired figure (else it rewrites the reservation)
    pub fn adopt(
        client: &Client,
        id: &str,
        user: &str,
        reserve: Resources,
        kind: LeaseKind,
    ) -> Self {
        let inner = Inner {
            client: client.clone(),
            id: id.to_string(),
            user: user.to_string(),
            beacon: Arc::new(Mutex::new(Beacon::new(id, user, kind, reserve))),
        };
        Reservation::spawn(inner, Reserve::Fixed(reserve), reserve, reserve, reserve)
    }

    /// Publish live test progress onto the lease. Free: the next heartbeat carries it
    pub fn report_status(&self, progress: Progress) {
        if let Ok(mut b) = self.inner.beacon.lock() {
            b.apply(progress);
        }
    }

    /// Background loop, shared by every reservation
    fn spawn(
        inner: Inner,
        want: Reserve,
        slice: Resources,
        budget: Resources,
        allocatable: Resources,
    ) -> Self {
        let (ceiling_tx, ceiling) = watch::channel(slice);
        let (demand, demand_rx) = watch::channel(Demand::default());
        let task =
            tokio::spawn(drive(inner.clone(), want, budget, allocatable, demand_rx, ceiling_tx));
        Reservation { inner, ceiling, demand, budget, task: Some(task) }
    }
}

/// Renew every tick (+ resize from live cluster state when elastic)
///
/// - Holds `available <= reserved`: write lease at `max(target, prev)` *before*
///   publishing the new ceiling
/// - Growth covered pre-admit; shrink still covers what the scheduler may admit
///   until it sees the lower ceiling
async fn drive(
    inner: Inner,
    want: Reserve,
    budget: Resources,
    allocatable: Resources,
    demand: watch::Receiver<Demand>,
    ceiling_tx: watch::Sender<Resources>,
) {
    let mut ticker = tokio::time::interval(match want {
        Reserve::Elastic => RECONCILE_INTERVAL,
        Reserve::Fixed(_) => RENEW_INTERVAL,
    });
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await; // fires immediately; the lease was just written
    let leases = lease_api(&inner.client);
    let mut prev = inner.current();
    loop {
        ticker.tick().await;

        let Reserve::Elastic = want else {
            let _ = inner.write(prev).await;
            continue;
        };

        // A crashed peer's stale reservation would pin our fair share down for
        // its whole TTL.
        let _ = sweep_expired(&leases).await;

        // Hold the acquire-time slice until the engine publishes real demand:
        // acting on the zero seed would drop the reservation below what this
        // run's already-scheduling pods hold.
        let d = *demand.borrow();
        if d == Demand::default() {
            let _ = inner.write(prev).await;
            continue;
        }

        let (Ok(live), Ok(pods)) = (list_leases(&leases).await, all_pods(&inner.client).await)
        else {
            continue; // transient; the TTL tolerates several missed reconciles
        };
        let target =
            reserve_from_state(&live, &pods, &inner.id, allocatable, budget, d.committed, d.demand);
        if inner.write(target.max(&prev)).await.is_err() {
            continue; // a missed write can only under-admit, never overcommit
        }
        if target != prev {
            let _ = ceiling_tx.send(target);
        }
        prev = target;
    }
}

/// How much of the available headroom a caller holds. Both modes wait on the same ledger
/// and write the same Lease, differing only in how much they take and how little is worth
/// waiting for — one axis, so the build phase and the test run share [`acquire`] verbatim
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reserve {
    /// A test run: whole available slice, resized live, started as soon as the lightest
    /// tier fits (`min_viable`)
    Elastic,
    /// A consumer that knows its footprint (the BuildKit pod): takes exactly that, never
    /// more (peers keep the rest), and waits for exactly that much free
    Fixed(Resources),
}

impl Reserve {
    fn threshold(self) -> Resources {
        match self {
            Reserve::Elastic => min_viable(),
            Reserve::Fixed(r) => r,
        }
    }

    fn amount(self, slice: Resources) -> Resources {
        match self {
            Reserve::Elastic => slice,
            Reserve::Fixed(r) => r,
        }
    }
}

/// Acquire a reservation under `lease_id`: slice = `min(sa_budget, allocatable −
/// unreserved − Σ others)`, waiting (never erroring) below `Reserve::threshold` in case
/// other runs release, then writing the lease.
///
/// Every pod ztest places must be covered by one of these while it occupies a node — the
/// invariant `assert_invariant` enforces
pub async fn acquire(
    client: &Client,
    lease_id: &str,
    sa: &str,
    user: &str,
    capacity: super::ClusterCapacity,
    want: Reserve,
    kind: LeaseKind,
) -> Result<Reservation, LedgerError> {
    require_meta_namespace(client).await?;
    let leases: Api<Lease> = Api::namespaced(client.clone(), META_NAMESPACE);
    let allocatable = capacity.allocatable;
    let budget = sa_budget(client, sa, default_budget(allocatable)).await;

    // Held from the first poll so a blocked run is visible to peers and to `ztest status`;
    // `started_at` = the wait's start, which is when the user launched
    let inner = Inner {
        client: client.clone(),
        id: lease_id.to_string(),
        user: user.to_string(),
        beacon: Arc::new(Mutex::new(Beacon::new(lease_id, user, kind, Resources::ZERO))),
    };

    let start = std::time::Instant::now();
    loop {
        sweep_expired(&leases).await?;
        let live = leases
            .list(&ListParams::default())
            .await
            .map_err(|e| LedgerError::Kube(format!("list leases: {e}")))?;
        let pods = all_pods(client).await?;

        // No leased run may run past its reservation; a violation is a scheduling bug,
        // panicked not silently overcommitted. Leased runs only (orphans = a cleanup concern)
        assert_invariant(&live, &pods);

        let others = sum_reservations(&live, lease_id);
        let unreserved = split_usage(&pods, &live).0;
        let headroom = allocatable.saturating_sub(&unreserved).saturating_sub(&others);
        let slice = budget.min(&headroom);

        // Enough for what this caller came for → take it; else wait for others to release
        if fits(&want.threshold(), &slice) {
            let reserve = want.amount(slice);
            {
                let mut b = inner.beacon.lock().expect("beacon mutex poisoned");
                b.kind = kind;
                b.needs = None;
            }
            inner.write(reserve).await?; // server-side apply == create
            return Ok(Reservation::spawn(inner, want, reserve, budget, allocatable));
        }

        if start.elapsed() >= ACQUIRE_WAIT_TIMEOUT {
            let _ = inner.delete().await;
            return Err(LedgerError::CapacityTimeout { waited: start.elapsed(), headroom });
        }
        // Claim: zero reserve (adds nothing to `sum_reservations`, no pods for
        // `assert_invariant`), rewritten each poll so it doubles as the TTL heartbeat
        {
            let mut b = inner.beacon.lock().expect("beacon mutex poisoned");
            b.kind = LeaseKind::Claim;
            b.needs = Some(want.threshold());
        }
        let _ = inner.write(Resources::ZERO).await;
        tokio::time::sleep(ACQUIRE_POLL).await;
    }
}

/// [`Resources::fits_within`] in the argument order the call sites here read naturally
fn fits(needle: &Resources, cap: &Resources) -> bool {
    needle.fits_within(cap)
}

/// Verify the ledger namespace exists.
///
/// - Read-only `get` keeps the run SA free of the cluster-scoped namespace-write grant
/// - Absent → fails fast with an actionable message, not a cryptic 403/404 on first write
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
pub fn lease_api(client: &Client) -> Api<Lease> {
    Api::namespaced(client.clone(), META_NAMESPACE)
}

pub async fn list_leases(api: &Api<Lease>) -> Result<ObjectList<Lease>, LedgerError> {
    api.list(&ListParams::default())
        .await
        .map_err(|e| LedgerError::Kube(format!("list leases: {e}")))
}

/// Delete leases whose TTL has lapsed (crashed runs). Best-effort per lease.
pub async fn sweep_expired(api: &Api<Lease>) -> Result<(), LedgerError> {
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

/// SA budget from its annotations, else `default` ([`default_budget`]); a missing SA
/// (a local SA name with no cluster object) also falls back
async fn sa_budget(client: &Client, sa: &str, default: Resources) -> Resources {
    let api: Api<ServiceAccount> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    match api.get(sa).await {
        Ok(obj) => budget_from_annotations(obj.metadata.annotations.as_ref(), default),
        Err(_) => default,
    }
}

/// All pods cluster-wide, for the usage split; one list call at run start
pub async fn all_pods(client: &Client) -> Result<ObjectList<Pod>, LedgerError> {
    Api::<Pod>::all(client.clone())
        .list(&ListParams::default())
        .await
        .map_err(|e| LedgerError::Kube(format!("list pods: {e}")))
}

// ── Pure helpers (unit-tested; no cluster) ──────────────────────────────────

/// The reservation a run should hold *right now* — the elastic, work-conserving,
/// cross-run-fair policy [`Reservation`] drives, kept pure for clusterless tests.
///
/// - Fair share `(allocatable − non_ztest) / active_runs`, lowered to `demand` and
///   `budget`, capped by the headroom left after `others`, floored at `committed`
/// - Floored because there is no preemption: never reserve below what is already running
/// - Lone run with excess work takes the cluster; N runs converge to `~1/N`; a run wanting
///   less than its share cedes the rest
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
        usable.disk_bps / n,
        usable.disk_iops / n,
    );
    let want = demand.min(&fair).min(&budget);
    // `others` already excludes this run → our own reservation isn't subtracted here
    let headroom = usable.saturating_sub(&others);
    // Never below what is already running (no preemption)
    want.min(&headroom).max(&committed)
}

/// [`fair_reserve`] over a fresh ledger + pod snapshot, so the reconcile loop that lists
/// both never touches the ledger's internal shapes
pub fn reserve_from_state(
    leases: &ObjectList<Lease>,
    pods: &ObjectList<Pod>,
    run_id: &str,
    allocatable: Resources,
    budget: Resources,
    committed: Resources,
    demand: Resources,
) -> Resources {
    let others = sum_reservations(leases, run_id);
    let unreserved = split_usage(pods, leases).0;
    // Claims hold nothing and run nothing: counting them would shrink every peer's fair
    // share on behalf of a run that may never start (FIFO admission is a separate change)
    let active_runs =
        leases.items.iter().filter(|l| Beacon::kind_of(l) != LeaseKind::Claim).count();
    fair_reserve(allocatable, unreserved, others, active_runs as u64, budget, committed, demand)
}

/// Σ reservations of every live lease except `exclude` (this run's own)
fn sum_reservations(leases: &ObjectList<Lease>, exclude: &str) -> Resources {
    leases
        .items
        .iter()
        .filter(|l| l.metadata.name.as_deref() != Some(exclude))
        .fold(Resources::ZERO, |acc, l| acc.saturating_add(&reservation_of(l)))
}

/// Plain integer (millicores or bytes), NOT a k8s quantity → `"32000"` is 32000 millicores,
/// never 32000 cores
fn parse_u64(s: &str) -> Option<u64> {
    s.trim().parse().ok()
}

/// A lease's reserved footprint, from its annotations (absent ⇒ zero)
fn reservation_of(lease: &Lease) -> Resources {
    let ann = lease.metadata.annotations.as_ref();
    let cpu = ann.and_then(|a| a.get(ANN_RESERVE_CPU)).and_then(|s| parse_u64(s)).unwrap_or(0);
    let mem = ann.and_then(|a| a.get(ANN_RESERVE_MEM)).and_then(|s| parse_u64(s)).unwrap_or(0);
    Resources::new(cpu, mem, 0, 0)
}

/// SA budget from its annotations, else `default` when either is absent or unparseable
/// (a typo must not become a zero budget that rejects every request)
fn budget_from_annotations(
    ann: Option<&BTreeMap<String, String>>,
    default: Resources,
) -> Resources {
    let cpu = ann.and_then(|a| a.get(ANN_BUDGET_CPU)).and_then(|s| parse_u64(s));
    let mem = ann.and_then(|a| a.get(ANN_BUDGET_MEM)).and_then(|s| parse_u64(s));
    match (cpu, mem) {
        (Some(c), Some(m)) => Resources::cpu_mem_unbounded_rest(c, m),
        _ => default,
    }
}

/// One pod walk, split by whether a live lease covers each pod
///
/// - `unreserved` = charged to nobody: foreign workloads + run-ids with no live lease
///   (crashed-run orphans, pods still terminating after release)
/// - Membership = "covered by a live lease", NOT "carries a run-id" (a labelled pod whose
///   lease is gone holds real memory, invisible to [`sum_reservations`]) — the hole the
///   [`BUILDKIT_BUILD`](super::build::BUILDKIT_BUILD) pod made expensive
/// - `by_run` = per-run usage for runs that do hold one
fn split_usage(
    pods: &ObjectList<Pod>,
    leases: &ObjectList<Lease>,
) -> (Resources, BTreeMap<String, Resources>) {
    let leased: BTreeSet<&str> =
        leases.items.iter().filter_map(|l| l.metadata.name.as_deref()).collect();
    let mut unreserved = Resources::ZERO;
    let mut by_run: BTreeMap<String, Resources> = BTreeMap::new();
    for p in pods.items.iter().filter(|p| super::units::pod_holds_capacity(p)) {
        match run_id_of(p).filter(|r| leased.contains(r)) {
            Some(run) => {
                let e = by_run.entry(run.to_string()).or_insert(Resources::ZERO);
                *e = e.saturating_add(&pod_footprint(p));
            }
            None => unreserved = unreserved.saturating_add(&pod_footprint(p)),
        }
    }
    (unreserved, by_run)
}

/// Panic if a leased run exceeds its reservation (ztest defect, not a runtime condition)
fn assert_invariant(leases: &ObjectList<Lease>, pods: &ObjectList<Pod>) {
    let reserved: BTreeMap<&str, Resources> = leases
        .items
        .iter()
        .filter_map(|l| l.metadata.name.as_deref().map(|n| (n, reservation_of(l))))
        .collect();
    for (run, usage) in split_usage(pods, leases).1 {
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

fn run_id_of(pod: &Pod) -> Option<&str> {
    pod.metadata.labels.as_ref().and_then(|l| l.get(LABEL_RUN_ID)).map(String::as_str)
}

/// A pod's request footprint, spec-derived = the floor the kube-scheduler holds
fn pod_footprint(pod: &Pod) -> Resources {
    pod.spec.as_ref().map(super::units::pod_effective_request).unwrap_or(Resources::ZERO)
}

/// TTL lapsed as of `now` (`renewTime + duration < now`); missing either field = live
/// (never sweep on incomplete data)
pub fn is_expired(lease: &Lease, now: chrono::DateTime<Utc>) -> bool {
    let Some(spec) = lease.spec.as_ref() else {
        return false;
    };
    let (Some(renew), Some(dur)) = (spec.renew_time.as_ref(), spec.lease_duration_seconds) else {
        return false;
    };
    renew.0 + chrono::Duration::seconds(dur as i64) < now
}

/// Lease for `run_id` carrying `beacon`, with run-id + user labels (so the existing label
/// reap covers it) and a fresh `renewTime`. Identical on acquire and every renew
fn lease_object(run_id: &str, user: &str, beacon: &Beacon) -> Lease {
    let labels = BTreeMap::from([
        (LABEL_RUN_ID.to_string(), run_id.to_string()),
        (LABEL_USER.to_string(), user.to_string()),
    ]);
    let annotations = beacon.annotations();
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
        ObjectList { types: Default::default(), metadata: Default::default(), items }
    }

    /// Scheduled pod requesting `cpu`m / `mem_gib`, optionally run-id labelled — the shape
    /// [`split_usage`] classifies
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

    /// A build pod carries a run-id, so a `run_id_of(p).is_none()` test excludes it from
    /// non-ztest usage; its run holding no lease excludes it from `sum_reservations` too.
    /// Charged to nobody, its 24 GiB went invisible and admission re-handed that memory out
    #[test]
    fn a_labelled_pod_whose_run_has_no_lease_is_charged_as_unreserved() {
        let leases = list(vec![lease("run-a", 8_000, 16)]);
        let pods = list(vec![pod(Some("elicb-47192"), 16_000, 24)]);
        let usage = split_usage(&pods, &leases).0;
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
            pod(None, 2_000, 4),           // system pod, never covered
        ]);
        let usage = split_usage(&pods, &leases).0;
        assert_eq!(
            usage.mem_bytes,
            4 * GIB,
            "only the unlabelled system pod is unreserved; run-a's is on its lease"
        );
    }

    /// `Fixed` = the builder's own footprint and no more: the whole slice would park
    /// capacity peers could use, less would leave its pod partly unbudgeted
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
        let budget = Resources::cpu_mem_unbounded_rest(32_000, 24 * GIB);
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
        assert_eq!(b.disk_bps, u64::MAX, "I/O ungoverned by the per-SA budget");
        assert_eq!(b.disk_iops, u64::MAX);
    }

    // ── Fair-share elastic reservation ─────────────────────────────────

    /// A 72c / 48Gi node, nothing else on it
    fn node() -> Resources {
        Resources::new(72_000, 48 * GIB, 0, 0)
    }

    #[test]
    fn lone_run_with_more_work_than_the_cluster_reserves_the_whole_cluster() {
        // N=1, huge demand, no other runs, no non-ztest, whole-node budget
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
        // N=2 → fair share is half the node; the other run holds its half, demand unbounded
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
        // Work-conserving: demand below the fair share caps the reservation, surplus ceded
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
        // A run that grabbed the cluster alone now shares (N=2, fair=36c) while still
        // running 50c → holds its committed footprint past its new fair share
        let r = fair_reserve(
            node(),
            Resources::ZERO,
            Resources::ZERO,
            2,
            default_budget(node()),
            Resources::new(50_000, 30 * GIB, 0, 0), // committed > fair
            Resources::new(300_000, 200 * GIB, 0, 0),
        );
        assert_eq!(r.cpu_milli, 50_000, "keeps running work despite a smaller share");
        assert_eq!(r.mem_bytes, 30 * GIB);
    }

    #[test]
    fn headroom_caps_below_fair_share_when_others_and_non_ztest_crowd_the_node() {
        // Fair share (N=2) is 36c, but non-ztest 10c + another run's 40c leave 22c real
        let r = fair_reserve(
            node(),
            Resources::new(10_000, 4 * GIB, 0, 0),  // non-ztest
            Resources::new(40_000, 20 * GIB, 0, 0), // others
            2,
            default_budget(node()),
            Resources::ZERO,
            Resources::new(300_000, 200 * GIB, 0, 0),
        );
        // usable = 72-10 = 62c; headroom = 62-40 = 22c; fair = 62/2 = 31c
        assert_eq!(r.cpu_milli, 22_000, "physical headroom binds below the fair share");
    }

    #[test]
    fn budget_annotation_caps_below_fair_share() {
        // A lone run whose SA is annotated to 16c never reserves past it
        let r = fair_reserve(
            node(),
            Resources::ZERO,
            Resources::ZERO,
            1,
            Resources::cpu_mem_unbounded_rest(16_000, 12 * GIB),
            Resources::ZERO,
            Resources::new(300_000, 200 * GIB, 0, 0),
        );
        assert_eq!(r.cpu_milli, 16_000, "SA budget is the hard per-identity cap");
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
        // No spec → never expired (incomplete data)
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
