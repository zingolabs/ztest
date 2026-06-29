//! The broker's pure admission/scheduling core.
//!
//! [`Scheduler`] owns the live capacity model and decides what to admit.
//! It is deliberately **pure**: no async, no sockets, no kube, no clock,
//! no randomness. The design doc's queue tiebreak of "request_time asc"
//! is realised here as a monotonic [`Scheduler`]-owned sequence counter,
//! and lease ids are a monotonic counter too — so the whole engine is a
//! deterministic function of its inputs and is unit-testable without a
//! cluster.
//!
//! ## One global capacity
//!
//! Capacity is a **single whole-cluster figure** (CPU × RAM). There is no
//! per-pool partition: the general/NVMe distinction is pure Kubernetes
//! placement (node taints + pod tolerations applied to pod specs), which
//! the scheduler never needs to account for — it admits against the one
//! global capacity and lets k8s place the pods.
//!
//! ## Policy, in one paragraph
//!
//! Greedy **priority admission with backfill** (`docs/qos-design.md`
//! §5.5): on every schedule pass, scan the queue in `(priority desc,
//! seq asc)` order and admit each request that fits *both* the live 2-D
//! capacity and its ServiceAccount's remaining budget; **keep scanning
//! past a non-fitting request** so a smaller, lower-priority one can
//! backfill the gap. A request that cannot fit even an empty cluster — or
//! that alone exceeds its SA's total budget — is **rejected** (fail fast,
//! §5.5/§5.6/§8); one that fits but is blocked only by current contention
//! or an SA at quota is **queued**.
//!
//! ## Deadlock-free by construction
//!
//! A queued request reserves *nothing* until a single atomic [`Grant`]
//! of its whole footprint — no hold-and-wait, hence no circular wait
//! (§5.5). The pure model makes this checkable: queueing a request never
//! changes [`Scheduler::free`].

use std::collections::HashMap;

use super::Resources;

/// The verdict of a single-request fit check over *reconstructed* state.
///
/// This is the pure decision primitive shared by the resident [`Scheduler`]
/// (which holds its own committed/usage) and the decentralized
/// [`crate::qos::allocator`] (which reconstructs committed/usage from k8s
/// objects on every admission). It is `LeaseId`-free — minting a lease is the
/// caller's job (an in-memory counter for the resident scheduler, a k8s Lease
/// for the allocator).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Fits live free capacity and SA remaining budget — admit.
    Fits,
    /// Fits the cluster/SA maxima but not right now — wait.
    Queue,
    /// Unschedulable even on an empty cluster / against the whole SA budget.
    Reject(RejectReason),
}

/// Pure single-request admission decision. `available` is the whole-cluster
/// capacity, `committed` the sum of active leases, `sa_usage` the requesting
/// SA's active usage, `sa_budget` its total budget (`None` = unlimited),
/// `footprint` the request's reserve.
///
/// Reject conditions are checked against the *maxima* (independent of current
/// load); the queue/fit split is checked against live free capacity and the
/// SA's remaining budget — exactly the two nested constraints of
/// `docs/qos-design.md` §5.6.
pub fn decide(
    available: Resources,
    committed: Resources,
    sa_usage: Resources,
    sa_budget: Option<Resources>,
    footprint: Resources,
) -> Verdict {
    if !footprint.fits_within(&available) {
        return Verdict::Reject(RejectReason::ExceedsClusterCapacity);
    }
    if let Some(budget) = sa_budget
        && !footprint.fits_within(&budget)
    {
        return Verdict::Reject(RejectReason::ExceedsSaBudget);
    }
    if !footprint.fits_within(&available.saturating_sub(&committed)) {
        return Verdict::Queue;
    }
    if let Some(budget) = sa_budget
        && !footprint.fits_within(&budget.saturating_sub(&sa_usage))
    {
        return Verdict::Queue;
    }
    Verdict::Fits
}

/// An opaque, monotonically-assigned lease handle. Bound to one admitted
/// test for the life of its topology; returned to the broker on release
/// or disconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LeaseId(pub u64);

/// A resolved admission request. The caller (the shell) has already
/// lowered a [`super::QosClass`] to its `footprint`/`priority` via
/// [`super::QosClass::profile`]; the scheduler operates purely on these
/// resolved numbers, so it is decoupled from the still-TBD reserve table
/// (`docs/qos-design.md` §11). The tier's NVMe-vs-general *placement* rides
/// on the pod specs (toleration/nodeSelector), not here. Identity
/// (`binary_id`/`test_name`) comes from nextest's env vars (§5.4) and is
/// echoed back in the [`Grant`] so the shell can message the right client on
/// backfill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// nextest `NEXTEST_BINARY_ID`.
    pub binary_id: String,
    /// nextest `NEXTEST_TEST_NAME`.
    pub test_name: String,
    /// The ServiceAccount the run authenticated as (authz scope, §5.6).
    pub sa: String,
    /// The namespace-aggregate reserve to schedule against.
    pub footprint: Resources,
    /// Scheduling priority; higher admitted first.
    pub priority: u8,
}

/// A successful admission: the lease plus the test identity it belongs
/// to, so the shell can send the grant to the correct client even when
/// the admission happened during a backfill pass (not in direct reply to
/// the test's own request).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    /// The lease the test now holds.
    pub lease_id: LeaseId,
    /// `NEXTEST_BINARY_ID` of the admitted test.
    pub binary_id: String,
    /// `NEXTEST_TEST_NAME` of the admitted test.
    pub test_name: String,
}

/// The outcome the broker replies with for a [`Scheduler::request`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Admitted now — the test may boot its topology.
    Granted(LeaseId),
    /// Fits in principle but blocked by current contention or an SA at
    /// quota; the test waits and is admitted by a later backfill pass.
    Queued,
    /// Unschedulable — fail fast rather than park forever (§5.5/§5.6/§8).
    Rejected(RejectReason),
}

/// Why a request was rejected outright rather than queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The footprint exceeds the cluster's *entire* capacity in some
    /// dimension — it could not fit even on an empty cluster.
    ExceedsClusterCapacity,
    /// The footprint alone exceeds the ServiceAccount's total budget —
    /// it could never fit even at zero usage. (Extends §5.6's fail-fast
    /// principle to the budget dimension; the doc frames the *at-quota*
    /// case as a queue, but an ask larger than the whole budget can
    /// never drain.)
    ExceedsSaBudget,
}

/// An active reservation tracked by the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Lease {
    binary_id: String,
    test_name: String,
    sa: String,
    footprint: Resources,
}

/// A pending request awaiting capacity, tagged with its arrival
/// sequence for the FIFO tiebreak within a priority level.
#[derive(Debug, Clone)]
struct Pending {
    seq: u64,
    req: Request,
}

/// The broker's pure admission core. See the module docs for the policy.
#[derive(Debug)]
pub struct Scheduler {
    /// Whole-cluster capacity — `available` is allocatable minus external
    /// (non-ztest) usage (what `reconcile` updates); `committed` is the sum
    /// of this broker's active-lease footprints.
    available: Resources,
    committed: Resources,
    leases: HashMap<LeaseId, Lease>,
    queue: Vec<Pending>,
    /// Per-SA total budgets. An SA absent from this map is treated as
    /// *unlimited* — the shell registers every authorized SA at startup
    /// (§5.6), so an unregistered SA is simply outside this core's authz
    /// scope rather than implicitly denied.
    sa_budgets: HashMap<String, Resources>,
    sa_usage: HashMap<String, Resources>,
    next_seq: u64,
    next_lease_id: u64,
}

impl Scheduler {
    /// Build a scheduler over the cluster's initially-available capacity.
    /// SA budgets default to empty (unlimited); register them with
    /// [`Scheduler::set_sa_budget`].
    pub fn new(available: Resources) -> Self {
        Scheduler {
            available,
            committed: Resources::ZERO,
            leases: HashMap::new(),
            queue: Vec::new(),
            sa_budgets: HashMap::new(),
            sa_usage: HashMap::new(),
            next_seq: 0,
            next_lease_id: 0,
        }
    }

    /// Register (or replace) a ServiceAccount's total budget (§5.6).
    pub fn set_sa_budget(&mut self, sa: impl Into<String>, budget: Resources) {
        self.sa_budgets.insert(sa.into(), budget);
    }

    /// Submit a request. Rejects unschedulable asks outright; otherwise
    /// enqueues and runs a schedule pass, returning [`Admission::Granted`]
    /// if the request itself was admitted in that pass, else
    /// [`Admission::Queued`].
    pub fn request(&mut self, req: Request) -> Admission {
        // Fail-fast checks against the theoretical maxima, before the
        // request ever joins the queue. Shares the pure `decide` primitive
        // with the decentralized allocator.
        if let Verdict::Reject(reason) = decide(
            self.available,
            self.committed,
            self.sa_usage(&req.sa),
            self.sa_budgets.get(&req.sa).copied(),
            req.footprint,
        ) {
            return Admission::Rejected(reason);
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        let (binary_id, test_name) = (req.binary_id.clone(), req.test_name.clone());
        self.queue.push(Pending { seq, req });

        // On a fresh enqueue, capacity has only shrunk (or held) since
        // the last pass, so the only request that can newly fit is this
        // one — a pass yields at most this grant. We still match by
        // identity to stay correct if that invariant ever changes.
        let grants = self.schedule_pass();
        debug_assert!(grants.len() <= 1, "fresh enqueue admitted >1 request");
        match grants
            .into_iter()
            .find(|g| g.binary_id == binary_id && g.test_name == test_name)
        {
            Some(g) => Admission::Granted(g.lease_id),
            None => Admission::Queued,
        }
    }

    /// Release a lease on normal teardown, returning capacity to the
    /// cluster and SA, then backfilling freed capacity. Returns the grants
    /// the freed capacity newly admitted (e.g. "testnet finishes →
    /// launch 2 basic"). An unknown lease id is a no-op.
    pub fn release(&mut self, lease_id: LeaseId) -> Vec<Grant> {
        let Some(lease) = self.leases.remove(&lease_id) else {
            return Vec::new();
        };
        self.committed = self.committed.saturating_sub(&lease.footprint);
        if let Some(usage) = self.sa_usage.get_mut(&lease.sa) {
            *usage = usage.saturating_sub(&lease.footprint);
            if *usage == Resources::ZERO {
                self.sa_usage.remove(&lease.sa);
            }
        }
        self.schedule_pass()
    }

    /// Crash-safety path (§5.4): a dropped socket means the test died
    /// without a clean teardown. Identical to [`Scheduler::release`] —
    /// the broker treats disconnect as release and reclaims capacity, so
    /// no reservation ever leaks.
    pub fn disconnect(&mut self, lease_id: LeaseId) -> Vec<Grant> {
        self.release(lease_id)
    }

    /// Update the cluster's available capacity from a fresh probe (§5.3
    /// reconcile), then backfill if capacity grew. Capacity that *shrank*
    /// below what is committed does not preempt running leases (no
    /// preemption in v1); free simply floors at zero until they release.
    pub fn reconcile(&mut self, new_available: Resources) -> Vec<Grant> {
        self.available = new_available;
        self.schedule_pass()
    }

    // ── Inspection (tests + future live display) ───────────────────────

    /// Free 2-D capacity: `available − committed`, floored at zero per
    /// dimension.
    pub fn free(&self) -> Resources {
        self.available.saturating_sub(&self.committed)
    }

    /// Sum of active-lease footprints currently committed.
    pub fn committed(&self) -> Resources {
        self.committed
    }

    /// Number of requests currently waiting for capacity.
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    /// Number of active leases.
    pub fn active_leases(&self) -> usize {
        self.leases.len()
    }

    /// An SA's current committed usage (zero if it holds nothing).
    pub fn sa_usage(&self, sa: &str) -> Resources {
        self.sa_usage.get(sa).copied().unwrap_or(Resources::ZERO)
    }

    // ── Internals ──────────────────────────────────────────────────────

    /// `true` iff the request fits both live free capacity and its SA's
    /// remaining budget right now. Delegates to the shared [`decide`]
    /// primitive so the resident and decentralized paths never diverge.
    fn fits_now(&self, req: &Request) -> bool {
        matches!(
            decide(
                self.available,
                self.committed,
                self.sa_usage(&req.sa),
                self.sa_budgets.get(&req.sa).copied(),
                req.footprint,
            ),
            Verdict::Fits
        )
    }

    /// One greedy priority-with-backfill pass over the queue. Admits
    /// every request that fits in `(priority desc, seq asc)` order,
    /// continuing past non-fitting requests so lower-priority ones
    /// backfill. A single pass suffices: admission only consumes
    /// capacity, so no admission can enable an earlier-skipped one.
    fn schedule_pass(&mut self) -> Vec<Grant> {
        self.queue.sort_by(|a, b| {
            b.req
                .priority
                .cmp(&a.req.priority)
                .then(a.seq.cmp(&b.seq))
        });

        let mut grants = Vec::new();
        let mut still_queued = Vec::new();
        for entry in std::mem::take(&mut self.queue) {
            if self.fits_now(&entry.req) {
                grants.push(self.admit(entry.req));
            } else {
                still_queued.push(entry);
            }
        }
        self.queue = still_queued;
        grants
    }

    /// Commit a request: mint a lease, charge capacity and the SA, and
    /// return the grant. The caller has already verified [`fits_now`].
    fn admit(&mut self, req: Request) -> Grant {
        let lease_id = LeaseId(self.next_lease_id);
        self.next_lease_id += 1;

        self.committed = self.committed.saturating_add(&req.footprint);

        let usage = self
            .sa_usage
            .entry(req.sa.clone())
            .or_insert(Resources::ZERO);
        *usage = usage.saturating_add(&req.footprint);

        let grant = Grant {
            lease_id,
            binary_id: req.binary_id.clone(),
            test_name: req.test_name.clone(),
        };
        self.leases.insert(
            lease_id,
            Lease {
                binary_id: req.binary_id,
                test_name: req.test_name,
                sa: req.sa,
                footprint: req.footprint,
            },
        );
        grant
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::GIB;

    // ── Test helpers ───────────────────────────────────────────────────

    /// A request at a given priority, identified uniquely by `name`,
    /// charged to SA `acme`.
    fn req(name: &str, cpu_milli: u64, mem_bytes: u64, priority: u8) -> Request {
        Request {
            binary_id: "bin".into(),
            test_name: name.into(),
            sa: "acme".into(),
            footprint: Resources::new(cpu_milli, mem_bytes),
            priority,
        }
    }

    fn lease_of(a: Admission) -> LeaseId {
        match a {
            Admission::Granted(id) => id,
            other => panic!("expected Granted, got {other:?}"),
        }
    }

    /// A roomy 8-CPU / 16Gi cluster.
    fn sched() -> Scheduler {
        Scheduler::new(Resources::new(8_000, 16 * GIB))
    }

    // ── Single fit ─────────────────────────────────────────────────────

    #[test]
    fn fitting_request_is_granted_and_consumes_exactly_its_footprint() {
        let mut s = sched();
        let before = s.free();
        let a = s.request(req("t", 1_000, GIB, 0));
        assert!(matches!(a, Admission::Granted(_)));
        assert_eq!(s.active_leases(), 1);
        assert_eq!(s.committed(), Resources::new(1_000, GIB));
        assert_eq!(s.free(), before.checked_sub(&Resources::new(1_000, GIB)).unwrap());
    }

    // ── Release returns capacity ───────────────────────────────────────

    #[test]
    fn release_returns_capacity() {
        let mut s = sched();
        let before = s.free();
        let id = lease_of(s.request(req("t", 2_000, 2 * GIB, 0)));
        assert_ne!(s.free(), before);
        let backfilled = s.release(id);
        assert!(backfilled.is_empty(), "nothing was queued to backfill");
        assert_eq!(s.free(), before);
        assert_eq!(s.active_leases(), 0);
    }

    // ── 2-D gating (both dimensions independently) ─────────────────────

    #[test]
    fn request_fitting_cpu_but_not_memory_queues() {
        // 8 CPU / 16Gi. Occupy 12Gi so only 4Gi is free; a 1 CPU / 8Gi ask
        // fits available (so it isn't rejected) but exceeds free memory.
        let mut s = sched();
        let _occ = lease_of(s.request(req("occ", 1_000, 12 * GIB, 0)));
        assert_eq!(s.request(req("t", 1_000, 8 * GIB, 0)), Admission::Queued);
        assert_eq!(s.queue_len(), 1);
    }

    #[test]
    fn request_fitting_memory_but_not_cpu_queues() {
        // 16 CPU / 16Gi. Occupy 10 CPU so only 6 remain; a 16 CPU ask
        // exceeds free CPU but not available → queue.
        let mut s = Scheduler::new(Resources::new(16_000, 16 * GIB));
        let _occ = lease_of(s.request(req("occ", 10_000, GIB, 0)));
        assert_eq!(s.request(req("t", 16_000, GIB, 0)), Admission::Queued);
    }

    // ── Atomic, whole-footprint grant ──────────────────────────────────

    #[test]
    fn grant_consumes_whole_footprint_atomically() {
        let mut s = sched();
        let fp = Resources::new(3_000, 5 * GIB);
        let before = s.free();
        lease_of(s.request(req("t", fp.cpu_milli, fp.mem_bytes, 0)));
        assert_eq!(s.free(), before.checked_sub(&fp).unwrap());
        assert_eq!(s.committed(), fp);
    }

    // ── Priority admission at t0 (priority beats arrival order) ─────────

    #[test]
    fn higher_priority_is_admitted_first_despite_later_arrival() {
        // An occupier fills the whole 8-CPU cluster. Two contenders each
        // need 5 CPU, so after the occupier frees, only ONE fits. A
        // low-prio job arrives first, then a high-prio one — the high-prio
        // must win the single slot despite arriving later.
        let mut s = sched();
        let occ = lease_of(s.request(req("occ", 8_000, 16 * GIB, 0)));
        assert_eq!(s.request(req("low", 5_000, 8 * GIB, 0)), Admission::Queued);
        assert_eq!(s.request(req("high", 5_000, 8 * GIB, 5)), Admission::Queued);
        let grants = s.release(occ);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].test_name, "high");
        assert_eq!(s.queue_len(), 1, "low stays queued");
    }

    // ── FIFO within a priority level ───────────────────────────────────

    #[test]
    fn equal_priority_breaks_ties_fifo_by_arrival() {
        let mut s = sched();
        let occ = lease_of(s.request(req("occ", 8_000, 16 * GIB, 0)));
        assert_eq!(s.request(req("first", 5_000, 8 * GIB, 1)), Admission::Queued);
        assert_eq!(s.request(req("second", 5_000, 8 * GIB, 1)), Admission::Queued);
        let grants = s.release(occ);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].test_name, "first");
    }

    // ── Backfill: a smaller lower-priority job fills the gap ───────────

    #[test]
    fn lower_priority_backfills_when_top_does_not_fit() {
        // 8 CPU, 6 free. A high-prio job needs 8 (doesn't fit free), a
        // low-prio job needs 4 (fits) → the low-prio backfills.
        let mut s = sched();
        let _occ = s.request(req("occ", 2_000, 2 * GIB, 0)); // 6 CPU free
        assert_eq!(s.request(req("big-hi", 8_000, 8 * GIB, 9)), Admission::Queued);
        assert_eq!(
            s.request(req("small-lo", 4_000, 4 * GIB, 0)),
            Admission::Granted(LeaseId(1))
        );
        assert_eq!(s.queue_len(), 1, "big-hi still waiting");
    }

    // ── Release-backfill: one big finishes → several small launch ──────

    #[test]
    fn release_backfills_multiple_queued_requests() {
        let mut s = Scheduler::new(Resources::new(6_000, 12 * GIB));
        let big = lease_of(s.request(req("testnet", 6_000, 12 * GIB, 2)));
        assert_eq!(s.request(req("basic-a", 3_000, 6 * GIB, 0)), Admission::Queued);
        assert_eq!(s.request(req("basic-b", 3_000, 6 * GIB, 0)), Admission::Queued);
        let grants = s.release(big);
        let mut names: Vec<_> = grants.iter().map(|g| g.test_name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["basic-a", "basic-b"]);
        assert_eq!(s.queue_len(), 0);
    }

    // ── decide(): the two ceilings are distinct (regression) ──────────
    //
    // These pin the contract `env::admit` relies on: `available` is the
    // *empty-of-ztest* ceiling (`ClusterCapacity::admission_ceiling`), checked
    // for Reject; the queue/fit split is `available − committed`. A footprint
    // that fits the ceiling but not the live free figure must QUEUE, never
    // Reject. Feeding live-free (`ClusterCapacity::free`) in as `available`
    // collapses this distinction under load and was the cause of spurious
    // `ExceedsClusterCapacity` rejections.

    #[test]
    fn decide_queues_when_fits_ceiling_but_not_free() {
        // Ceiling 8 CPU; 6 already committed → 2 free. A 4-CPU footprint fits
        // the ceiling but not free-right-now: it must wait, not fail fast.
        let v = decide(
            Resources::new(8_000, 16 * GIB), // available = ceiling
            Resources::new(6_000, 12 * GIB), // committed
            Resources::ZERO,                 // sa_usage
            None,                            // sa_budget
            Resources::new(4_000, 8 * GIB),  // footprint
        );
        assert_eq!(v, Verdict::Queue);
    }

    #[test]
    fn decide_rejects_only_when_footprint_exceeds_ceiling() {
        // Same footprint, ceiling now too small in the memory dimension →
        // genuinely unschedulable however many committed leases finish.
        let v = decide(
            Resources::new(8_000, 4 * GIB),
            Resources::ZERO,
            Resources::ZERO,
            None,
            Resources::new(4_000, 8 * GIB),
        );
        assert_eq!(v, Verdict::Reject(RejectReason::ExceedsClusterCapacity));
    }

    #[test]
    fn decide_fits_on_empty_ceiling() {
        let v = decide(
            Resources::new(8_000, 16 * GIB),
            Resources::ZERO,
            Resources::ZERO,
            None,
            Resources::new(4_000, 8 * GIB),
        );
        assert_eq!(v, Verdict::Fits);
    }

    // ── Reject: exceeds empty-cluster capacity (fail fast) ─────────────

    #[test]
    fn request_larger_than_cluster_is_rejected_not_queued() {
        let mut s = sched(); // 8 CPU / 16Gi
        assert_eq!(
            s.request(req("toobig", 16_000, 8 * GIB, 0)),
            Admission::Rejected(RejectReason::ExceedsClusterCapacity)
        );
        assert_eq!(s.queue_len(), 0, "rejected, never queued");
    }

    // ── Reject: exceeds SA total budget (fail fast) ────────────────────

    #[test]
    fn request_larger_than_sa_budget_is_rejected() {
        let mut s = sched();
        s.set_sa_budget("acme", Resources::new(2_000, 2 * GIB));
        assert_eq!(
            s.request(req("t", 4_000, GIB, 0)),
            Admission::Rejected(RejectReason::ExceedsSaBudget)
        );
    }

    // ── Queue: SA at quota, freed by its own test finishing ────────────

    #[test]
    fn request_within_budget_but_sa_at_quota_queues_then_admits_on_release() {
        let mut s = sched();
        s.set_sa_budget("acme", Resources::new(4_000, 8 * GIB));
        let first = lease_of(s.request(req("first", 3_000, 6 * GIB, 0)));
        assert_eq!(s.request(req("second", 3_000, 6 * GIB, 0)), Admission::Queued);
        let grants = s.release(first);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].test_name, "second");
    }

    // ── Both nested constraints enforced (§5.6) ────────────────────────

    #[test]
    fn admission_requires_both_cluster_and_sa_budget() {
        // Cluster: 10 CPU. SA budget: 5 CPU.
        let mut s = Scheduler::new(Resources::new(10_000, 20 * GIB));
        s.set_sa_budget("acme", Resources::new(5_000, 10 * GIB));
        // Fits SA but not cluster free: another SA hogs 8 CPU first.
        let hog = Request {
            sa: "other".into(),
            ..req("hog", 8_000, 8 * GIB, 0)
        };
        let hog_id = lease_of(s.request(hog));
        assert_eq!(s.request(req("a", 3_000, 4 * GIB, 0)), Admission::Queued);
        // Free the cluster; now it fits both → granted.
        let grants = s.release(hog_id);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].test_name, "a");
    }

    // ── Crash safety: disconnect == release ────────────────────────────

    #[test]
    fn disconnect_reclaims_capacity_like_release() {
        let mut s = Scheduler::new(Resources::new(4_000, 8 * GIB));
        let id = lease_of(s.request(req("dies", 4_000, 8 * GIB, 0)));
        assert_eq!(s.request(req("waits", 4_000, 8 * GIB, 0)), Admission::Queued);
        let grants = s.disconnect(id);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].test_name, "waits");
        assert_eq!(s.active_leases(), 1);
    }

    // ── Reconcile ──────────────────────────────────────────────────────

    #[test]
    fn reconcile_growth_backfills_and_noop_changes_nothing() {
        let mut s = sched();
        let _occ = lease_of(s.request(req("occ", 6_000, 12 * GIB, 0)));
        assert_eq!(s.request(req("t", 4_000, 8 * GIB, 0)), Admission::Queued);
        // No-op reconcile (same value) admits nothing.
        assert!(s.reconcile(Resources::new(8_000, 16 * GIB)).is_empty());
        assert_eq!(s.queue_len(), 1);
        // External capacity appears → available grows → the queued request
        // backfills without any lease releasing.
        let grants = s.reconcile(Resources::new(16_000, 32 * GIB));
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].test_name, "t");
    }

    #[test]
    fn reconcile_shrink_does_not_preempt_running_leases() {
        let mut s = sched();
        let _id = lease_of(s.request(req("running", 6_000, 12 * GIB, 0)));
        let grants = s.reconcile(Resources::new(4_000, 8 * GIB));
        assert!(grants.is_empty());
        assert_eq!(s.active_leases(), 1, "running lease is not preempted");
        assert_eq!(s.free(), Resources::ZERO);
    }

    // ── Deadlock-free: queueing reserves nothing ───────────────────────

    #[test]
    fn queueing_a_request_reserves_no_capacity() {
        let mut s = sched();
        let _occ = lease_of(s.request(req("occ", 8_000, 8 * GIB, 0)));
        let free_before = s.free();
        let committed_before = s.committed();
        assert_eq!(s.request(req("waiter", 4_000, 4 * GIB, 0)), Admission::Queued);
        assert_eq!(s.free(), free_before);
        assert_eq!(s.committed(), committed_before);
    }

    // ── Determinism ────────────────────────────────────────────────────

    #[test]
    fn identical_op_sequences_yield_identical_results() {
        fn run() -> (Vec<Admission>, Vec<String>) {
            let mut s = Scheduler::new(Resources::new(8_000, 16 * GIB));
            let admissions = vec![
                s.request(req("a", 4_000, 4 * GIB, 0)),
                s.request(req("b", 4_000, 4 * GIB, 2)),
                s.request(req("c", 4_000, 4 * GIB, 1)),
            ];
            let grants = s.release(LeaseId(0));
            let names = grants.into_iter().map(|g| g.test_name).collect();
            (admissions, names)
        }
        assert_eq!(run(), run());
    }
}
