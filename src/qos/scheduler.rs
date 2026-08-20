//! The broker's pure admission core.
//!
//! - [`Scheduler`] owns one whole-cluster 4-D capacity figure (no per-pool partition)
//! - Pure: no async/kube/clock/randomness (queue tiebreak + lease ids = monotonic counters)
//!   → a deterministic function of its inputs, testable without a cluster
//! - Policy (§5.5) = greedy priority admission with backfill: each pass scans
//!   `(priority desc, seq asc)`, admitting what fits live capacity + the SA's remaining
//!   budget and continuing past a non-fitting one so a smaller request backfills
//! - Over empty-cluster capacity or over the SA's whole budget → rejected; blocked only by
//!   contention or an SA at quota → queued
//! - Deadlock-free: a queued request reserves nothing until one atomic [`Admitted`] of its
//!   whole footprint (no hold-and-wait)

use std::collections::HashMap;

use super::Resources;

/// Pure single-request admission decision (`sa_budget` `None` = unlimited).
///
/// - Rejects checked against the maxima, independent of current load
/// - Queue/fit split against live free capacity + the SA's remaining budget (§5.6)
pub fn decide(
    available: Resources,
    committed: Resources,
    sa_usage: Resources,
    sa_budget: Option<Resources>,
    footprint: Resources,
) -> Admission<()> {
    if !footprint.fits_within(&available) {
        return Admission::Rejected(RejectReason::ExceedsClusterCapacity);
    }
    if let Some(budget) = sa_budget
        && !footprint.fits_within(&budget)
    {
        return Admission::Rejected(RejectReason::ExceedsSaBudget);
    }
    if !footprint.fits_within(&available.saturating_sub(&committed)) {
        return Admission::Queued;
    }
    if let Some(budget) = sa_budget
        && !footprint.fits_within(&budget.saturating_sub(&sa_usage))
    {
        return Admission::Queued;
    }
    Admission::Granted(())
}

/// Opaque, monotonically-assigned lease handle, bound to one admitted test for the life
/// of its topology
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotId(pub u64);

/// Resolved admission request: the caller already lowered a [`super::QosClass`] via
/// [`super::QosClass::profile`], keeping the scheduler decoupled from the reserve table
/// (§11). Pool placement rides on the pod specs, not here
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub binary_id: String,
    pub test_name: String,
    pub sa: String,
    pub footprint: Resources,
    pub priority: u8,
}

/// A successful admission. Carries the test identity so the shell routes a grant from a
/// backfill pass — not a direct reply to that test's own request — to the right client
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    pub slot_id: SlotId,
    pub binary_id: String,
    pub test_name: String,
}

/// Admission decision
///
/// - `Queued` = fits in principle, blocked by contention / SA at quota → backfilled later
/// - Payload: slot from [`Scheduler::request`], `()` from [`decide`] (no identity minted)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission<T = SlotId> {
    Granted(T),
    Queued,
    Rejected(RejectReason),
}

/// Rejected outright, not queued: the footprint alone tops the maximum, so no waiting
/// drains it. `ExceedsSaBudget` extends §5.6's fail-fast to the budget dimension (the doc
/// frames only the at-quota case, which stays a queue)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    ExceedsClusterCapacity,
    ExceedsSaBudget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Lease {
    binary_id: String,
    test_name: String,
    sa: String,
    footprint: Resources,
}

/// Request awaiting capacity; `seq` = arrival order, the FIFO tiebreak within a priority
#[derive(Debug, Clone)]
struct Pending {
    seq: u64,
    req: Request,
}

/// The broker's pure admission core; policy in the module docs.
///
/// - `available` = allocatable − external usage, as `reconcile` last set it
/// - `committed` = Σ active-lease footprints
/// - SA absent from `sa_budgets` = unlimited (the shell registers every authorized SA at
///   startup, so an unregistered one is outside this core's authz scope, not denied)
#[derive(Debug)]
pub struct Scheduler {
    available: Resources,
    committed: Resources,
    leases: HashMap<SlotId, Lease>,
    queue: Vec<Pending>,
    sa_budgets: HashMap<String, Resources>,
    sa_usage: HashMap<String, Resources>,
    next_seq: u64,
    next_slot_id: u64,
}

impl Scheduler {
    /// SA budgets start empty (unlimited); register via [`Scheduler::set_sa_budget`]
    pub fn new(available: Resources) -> Self {
        Scheduler {
            available,
            committed: Resources::ZERO,
            leases: HashMap::new(),
            queue: Vec::new(),
            sa_budgets: HashMap::new(),
            sa_usage: HashMap::new(),
            next_seq: 0,
            next_slot_id: 0,
        }
    }

    pub fn set_sa_budget(&mut self, sa: impl Into<String>, budget: Resources) {
        self.sa_budgets.insert(sa.into(), budget);
    }

    /// Reject an unschedulable ask outright; else enqueue + run a pass, granting only if
    /// *this* request was admitted in it
    pub fn request(&mut self, req: Request) -> Admission {
        // Fail-fast against the theoretical maxima, before joining the queue
        if let Admission::Rejected(reason) = decide(
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

        // Capacity has only shrunk (or held) since the last pass → this is the only request
        // that can newly fit. Matched by identity in case that invariant ever changes
        let grants = self.schedule_pass();
        assert!(grants.len() <= 1, "fresh enqueue admitted >1 request");
        match grants.into_iter().find(|g| g.binary_id == binary_id && g.test_name == test_name) {
            Some(g) => Admission::Granted(g.slot_id),
            None => Admission::Queued,
        }
    }

    /// Return capacity to the cluster + SA, then backfill; returns the grants the freed
    /// capacity newly admitted. Unknown lease id = no-op
    pub fn release(&mut self, slot_id: SlotId) -> Vec<Admitted> {
        let Some(lease) = self.leases.remove(&slot_id) else {
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

    /// Crash-safety path (§5.4): a dropped socket = the test died without teardown, so
    /// disconnect is a release and no reservation leaks
    pub fn disconnect(&mut self, slot_id: SlotId) -> Vec<Admitted> {
        self.release(slot_id)
    }

    /// Update available capacity from a fresh probe (§5.3), then backfill if it grew. A
    /// shrink below `committed` never preempts; free floors at zero until leases release
    pub fn reconcile(&mut self, new_available: Resources) -> Vec<Admitted> {
        self.available = new_available;
        self.schedule_pass()
    }

    // ── Inspection (tests + future live display) ───────────────────────

    pub fn free(&self) -> Resources {
        self.available.saturating_sub(&self.committed)
    }

    pub fn committed(&self) -> Resources {
        self.committed
    }

    /// Committed leases + every queued request's footprint. The cross-run reconcile loop
    /// caps the live reservation by this → a run reserves only what it can use
    pub fn demand(&self) -> Resources {
        self.queue.iter().fold(self.committed, |acc, p| acc.saturating_add(&p.req.footprint))
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn active_leases(&self) -> usize {
        self.leases.len()
    }

    pub fn sa_usage(&self, sa: &str) -> Resources {
        self.sa_usage.get(sa).copied().unwrap_or(Resources::ZERO)
    }

    // ── Internals ──────────────────────────────────────────────────────

    /// Fits live free capacity *and* the SA's remaining budget. Via [`decide`], so the
    /// resident and decentralized paths never diverge
    fn fits_now(&self, req: &Request) -> bool {
        matches!(
            decide(
                self.available,
                self.committed,
                self.sa_usage(&req.sa),
                self.sa_budgets.get(&req.sa).copied(),
                req.footprint,
            ),
            Admission::Granted(())
        )
    }

    /// One greedy priority-with-backfill pass in `(priority desc, seq asc)` order,
    /// continuing past non-fitting requests so lower-priority ones backfill. One pass
    /// suffices (admission only consumes capacity → no grant can enable a skipped one)
    fn schedule_pass(&mut self) -> Vec<Admitted> {
        self.queue.sort_by(|a, b| b.req.priority.cmp(&a.req.priority).then(a.seq.cmp(&b.seq)));

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

    /// Precondition: the caller has verified [`fits_now`]
    fn admit(&mut self, req: Request) -> Admitted {
        // "No overload": admitting must never push committed past the ceiling. Guarded
        // live so a regression aborts rather than overcommitting a real cluster
        assert!(
            req.footprint.fits_within(&self.available.saturating_sub(&self.committed)),
            "admit: footprint {:?} does not fit free capacity {:?} — fits_now precondition violated",
            req.footprint,
            self.available.saturating_sub(&self.committed),
        );

        let slot_id = SlotId(self.next_slot_id);
        self.next_slot_id += 1;

        self.committed = self.committed.saturating_add(&req.footprint);

        let usage = self.sa_usage.entry(req.sa.clone()).or_insert(Resources::ZERO);
        *usage = usage.saturating_add(&req.footprint);

        let grant = Admitted {
            slot_id,
            binary_id: req.binary_id.clone(),
            test_name: req.test_name.clone(),
        };
        self.leases.insert(
            slot_id,
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
    use crate::qos::{GIB, MIB};

    // ── Test helpers ───────────────────────────────────────────────────

    /// Request at a given priority, uniquely named, charged to SA `acme`
    fn req(name: &str, cpu_milli: u64, mem_bytes: u64, priority: u8) -> Request {
        Request {
            binary_id: "bin".into(),
            test_name: name.into(),
            sa: "acme".into(),
            footprint: Resources::new(cpu_milli, mem_bytes, 0, 0),
            priority,
        }
    }

    fn lease_of(a: Admission) -> SlotId {
        match a {
            Admission::Granted(id) => id,
            other => panic!("expected Granted, got {other:?}"),
        }
    }

    fn sched() -> Scheduler {
        Scheduler::new(Resources::new(8_000, 16 * GIB, 0, 0))
    }

    // ── Single fit ─────────────────────────────────────────────────────

    #[test]
    fn fitting_request_is_granted_and_consumes_exactly_its_footprint() {
        let mut s = sched();
        let before = s.free();
        let a = s.request(req("t", 1_000, GIB, 0));
        assert!(matches!(a, Admission::Granted(_)));
        assert_eq!(s.active_leases(), 1);
        assert_eq!(s.committed(), Resources::new(1_000, GIB, 0, 0));
        assert_eq!(s.free(), before.checked_sub(&Resources::new(1_000, GIB, 0, 0)).unwrap());
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

    // ── Per-dimension gating (each dimension independently) ────────────

    #[test]
    fn request_fitting_cpu_but_not_memory_queues() {
        // Occupy 12Gi of 16 → a 1 CPU / 8Gi ask fits `available` (not rejected) but not free
        let mut s = sched();
        let _occ = lease_of(s.request(req("occ", 1_000, 12 * GIB, 0)));
        assert_eq!(s.request(req("t", 1_000, 8 * GIB, 0)), Admission::Queued);
        assert_eq!(s.queue_len(), 1);
    }

    #[test]
    fn request_fitting_memory_but_not_cpu_queues() {
        // Occupy 10 of 16 CPU → a 16 CPU ask tops free CPU but not `available` → queues
        let mut s = Scheduler::new(Resources::new(16_000, 16 * GIB, 0, 0));
        let _occ = lease_of(s.request(req("occ", 10_000, GIB, 0)));
        assert_eq!(s.request(req("t", 16_000, GIB, 0)), Admission::Queued);
    }

    #[test]
    fn request_fitting_cpu_and_memory_but_not_io_bandwidth_queues() {
        // Fitting CPU + memory with room to spare must still queue on exhausted I/O
        // bandwidth — the dimension the 2-D packer could not see
        let io_req = |name: &str, disk_bps: u64| Request {
            binary_id: "bin".into(),
            test_name: name.into(),
            sa: "acme".into(),
            footprint: Resources::new(1_000, GIB, disk_bps, 0),
            priority: 0,
        };
        // Occupy 400 of 500 MB/s → 100 free
        let mut s = Scheduler::new(Resources::new(8_000, 16 * GIB, 500 * MIB, 0));
        let _occ = lease_of(s.request(io_req("occ", 400 * MIB)));
        assert_eq!(s.request(io_req("t", 300 * MIB)), Admission::Queued);
        assert_eq!(s.queue_len(), 1);
        // Within the remaining 100 MB/s still fits
        assert!(matches!(s.request(io_req("small", 100 * MIB)), Admission::Granted(_)));
    }

    #[test]
    fn request_fitting_all_else_but_not_io_iops_queues() {
        // IOPS gates independently of bandwidth
        let iops_req = |name: &str, disk_iops: u64| Request {
            binary_id: "bin".into(),
            test_name: name.into(),
            sa: "acme".into(),
            footprint: Resources::new(1_000, GIB, 0, disk_iops),
            priority: 0,
        };
        let mut s = Scheduler::new(Resources::new(8_000, 16 * GIB, 0, 10_000));
        let _occ = lease_of(s.request(iops_req("occ", 8_000)));
        assert_eq!(s.request(iops_req("t", 5_000)), Admission::Queued);
    }

    // ── Atomic, whole-footprint grant ──────────────────────────────────

    #[test]
    fn grant_consumes_whole_footprint_atomically() {
        let mut s = sched();
        let fp = Resources::new(3_000, 5 * GIB, 0, 0);
        let before = s.free();
        lease_of(s.request(req("t", fp.cpu_milli, fp.mem_bytes, 0)));
        assert_eq!(s.free(), before.checked_sub(&fp).unwrap());
        assert_eq!(s.committed(), fp);
    }

    // ── Priority admission at t0 (priority beats arrival order) ─────────

    #[test]
    fn higher_priority_is_admitted_first_despite_later_arrival() {
        // An occupier fills the 8-CPU cluster; two 5-CPU contenders → only ONE fits on
        // release. Low-prio arrives first, so the later high-prio must win the slot
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
        // 6 of 8 CPU free: high-prio needs 8 (no fit), low-prio needs 4 → low-prio backfills
        let mut s = sched();
        let _occ = s.request(req("occ", 2_000, 2 * GIB, 0)); // 6 CPU free
        assert_eq!(s.request(req("big-hi", 8_000, 8 * GIB, 9)), Admission::Queued);
        assert_eq!(s.request(req("small-lo", 4_000, 4 * GIB, 0)), Admission::Granted(SlotId(1)));
        assert_eq!(s.queue_len(), 1, "big-hi still waiting");
    }

    // Release-backfill: one big finishes, several small launch

    #[test]
    fn release_backfills_multiple_queued_requests() {
        let mut s = Scheduler::new(Resources::new(6_000, 12 * GIB, 0, 0));
        let big = lease_of(s.request(req("testnet", 6_000, 12 * GIB, 2)));
        assert_eq!(s.request(req("basic-a", 3_000, 6 * GIB, 0)), Admission::Queued);
        assert_eq!(s.request(req("basic-b", 3_000, 6 * GIB, 0)), Admission::Queued);
        let grants = s.release(big);
        let mut names: Vec<_> = grants.iter().map(|g| g.test_name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["basic-a", "basic-b"]);
        assert_eq!(s.queue_len(), 0);
    }

    // decide(): reject vs queue stays distinct (regression).
    //
    // `available` = seed capacity (`ClusterCapacity::free` at run startup); over it →
    // rejected (unschedulable however many tests finish). Queue/fit split is
    // `available - committed`: fitting `available` but not the live remainder must queue.

    #[test]
    fn decide_queues_when_fits_ceiling_but_not_free() {
        // 6 of an 8-CPU ceiling committed: a 4-CPU footprint fits the ceiling but not free
        // right now → must wait, not fail fast
        let v = decide(
            Resources::new(8_000, 16 * GIB, 0, 0), // available = ceiling
            Resources::new(6_000, 12 * GIB, 0, 0), // committed
            Resources::ZERO,                       // sa_usage
            None,                                  // sa_budget
            Resources::new(4_000, 8 * GIB, 0, 0),  // footprint
        );
        assert_eq!(v, Admission::Queued);
    }

    /// A declared disk reserve must not read as "exceeds budget" against a budget that
    /// only ever prices cpu/mem — every uncalibrated dimension seeds unbounded, or the
    /// first test to declare one is rejected outright
    #[test]
    fn a_disk_reserve_is_admitted_against_a_cpu_mem_budget() {
        let ceiling = Resources::cpu_mem_unbounded_rest(16_000, 16 * GIB);
        let budget = Resources::cpu_mem_unbounded_rest(16_000, 16 * GIB);
        let footprint = Resources::new(15_000, 15 * GIB, 0, 0).with_disk(400 * GIB);
        assert_eq!(
            decide(ceiling, Resources::ZERO, Resources::ZERO, Some(budget), footprint),
            Admission::Granted(())
        );
    }

    #[test]
    fn decide_rejects_only_when_footprint_exceeds_ceiling() {
        // Same footprint, ceiling now too small in memory → unschedulable however many
        // committed leases finish
        let v = decide(
            Resources::new(8_000, 4 * GIB, 0, 0),
            Resources::ZERO,
            Resources::ZERO,
            None,
            Resources::new(4_000, 8 * GIB, 0, 0),
        );
        assert_eq!(v, Admission::Rejected(RejectReason::ExceedsClusterCapacity));
    }

    #[test]
    fn decide_fits_on_empty_ceiling() {
        let v = decide(
            Resources::new(8_000, 16 * GIB, 0, 0),
            Resources::ZERO,
            Resources::ZERO,
            None,
            Resources::new(4_000, 8 * GIB, 0, 0),
        );
        assert_eq!(v, Admission::Granted(()));
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
        s.set_sa_budget("acme", Resources::new(2_000, 2 * GIB, 0, 0));
        assert_eq!(
            s.request(req("t", 4_000, GIB, 0)),
            Admission::Rejected(RejectReason::ExceedsSaBudget)
        );
    }

    // ── Queue: SA at quota, freed by its own test finishing ────────────

    #[test]
    fn request_within_budget_but_sa_at_quota_queues_then_admits_on_release() {
        let mut s = sched();
        s.set_sa_budget("acme", Resources::new(4_000, 8 * GIB, 0, 0));
        let first = lease_of(s.request(req("first", 3_000, 6 * GIB, 0)));
        assert_eq!(s.request(req("second", 3_000, 6 * GIB, 0)), Admission::Queued);
        let grants = s.release(first);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].test_name, "second");
    }

    // ── Both nested constraints enforced (§5.6) ────────────────────────

    #[test]
    fn admission_requires_both_cluster_and_sa_budget() {
        // Cluster 10 CPU, SA budget 5 CPU
        let mut s = Scheduler::new(Resources::new(10_000, 20 * GIB, 0, 0));
        s.set_sa_budget("acme", Resources::new(5_000, 10 * GIB, 0, 0));
        // Fits the SA but not cluster free: another SA hogs 8 CPU first
        let hog = Request { sa: "other".into(), ..req("hog", 8_000, 8 * GIB, 0) };
        let hog_id = lease_of(s.request(hog));
        assert_eq!(s.request(req("a", 3_000, 4 * GIB, 0)), Admission::Queued);
        // Freed → fits both → granted
        let grants = s.release(hog_id);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].test_name, "a");
    }

    // ── Crash safety: disconnect == release ────────────────────────────

    #[test]
    fn disconnect_reclaims_capacity_like_release() {
        let mut s = Scheduler::new(Resources::new(4_000, 8 * GIB, 0, 0));
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
        // No-op reconcile (same value) admits nothing
        assert!(s.reconcile(Resources::new(8_000, 16 * GIB, 0, 0)).is_empty());
        assert_eq!(s.queue_len(), 1);
        // External capacity appears → the queued request backfills with no lease releasing
        let grants = s.reconcile(Resources::new(16_000, 32 * GIB, 0, 0));
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].test_name, "t");
    }

    #[test]
    fn reconcile_shrink_does_not_preempt_running_leases() {
        let mut s = sched();
        let _id = lease_of(s.request(req("running", 6_000, 12 * GIB, 0)));
        let grants = s.reconcile(Resources::new(4_000, 8 * GIB, 0, 0));
        assert!(grants.is_empty());
        assert_eq!(s.active_leases(), 1, "running lease is not preempted");
        assert_eq!(s.free(), Resources::ZERO);
    }

    // ── Demand: committed plus queued footprints ───────────────────────

    #[test]
    fn demand_sums_committed_and_queued_footprints() {
        // Admit 6 of 8 CPU, then queue two 4-CPU asks (neither fits the 2 free) → demand is
        // the whole appetite, 6 committed + 4 + 4
        let mut s = sched();
        lease_of(s.request(req("run", 6_000, 4 * GIB, 0)));
        assert_eq!(s.request(req("q1", 4_000, GIB, 0)), Admission::Queued);
        assert_eq!(s.request(req("q2", 4_000, GIB, 0)), Admission::Queued);
        assert_eq!(s.demand(), Resources::new(14_000, 6 * GIB, 0, 0));
        assert_eq!(s.committed(), Resources::new(6_000, 4 * GIB, 0, 0));
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
            let mut s = Scheduler::new(Resources::new(8_000, 16 * GIB, 0, 0));
            let admissions = vec![
                s.request(req("a", 4_000, 4 * GIB, 0)),
                s.request(req("b", 4_000, 4 * GIB, 2)),
                s.request(req("c", 4_000, 4 * GIB, 1)),
            ];
            let grants = s.release(SlotId(0));
            let names = grants.into_iter().map(|g| g.test_name).collect();
            (admissions, names)
        }
        assert_eq!(run(), run());
    }
}

/// The invariants the cases above pin pointwise, over randomized op-sequences. Pure +
/// deterministic → no cluster, no clock
#[cfg(test)]
mod props {
    use super::*;
    use crate::qos::MIB;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, HashMap};

    /// Scripted op. `Release` indexes into the held leases modulo their count → always
    /// targets a real lease when one exists
    #[derive(Debug, Clone)]
    enum Op {
        Request { cpu: u64, mem_mib: u64, io_mbps: u64, io_kiops: u64, prio: u8 },
        Release { which: usize },
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (1u64..=4_000, 1u64..=8_192, 0u64..=400, 0u64..=50, 0u8..=3).prop_map(
                |(cpu, mem_mib, io_mbps, io_kiops, prio)| Op::Request {
                    cpu,
                    mem_mib,
                    io_mbps,
                    io_kiops,
                    prio
                }
            ),
            (0usize..256).prop_map(|which| Op::Release { which }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Headline "no overload" property, machine-checked: over *any* request/release
        /// sequence, committed never tops the ceiling in *any* of the four dimensions and
        /// the accounting stays exact against the live leases. The ceiling is finite
        /// everywhere and the generator emits I/O demands → I/O gating is exercised too
        #[test]
        fn committed_never_exceeds_ceiling_under_any_op_sequence(
            ops in proptest::collection::vec(op(), 1..80)
        ) {
            let ceiling = Resources::new(8_000, 16 * 1024 * MIB, 800 * MIB, 100_000);
            let mut s = Scheduler::new(ceiling);

            // Independent mirror of what *should* be committed, rebuilt from reported
            // grants/releases, never read back. BTreeMap keeps `Release` indexing
            // deterministic for shrinking
            let mut live: BTreeMap<SlotId, Resources> = BTreeMap::new();
            let mut footprint_of: HashMap<String, Resources> = HashMap::new();
            let mut seq = 0u64;

            for o in ops {
                match o {
                    Op::Request { cpu, mem_mib, io_mbps, io_kiops, prio } => {
                        let name = format!("t{seq}");
                        seq += 1;
                        let fp = Resources::new(cpu, mem_mib * MIB, io_mbps * MIB, io_kiops * 1_000);
                        footprint_of.insert(name.clone(), fp);
                        let req = Request {
                            binary_id: "b".into(),
                            test_name: name,
                            sa: "acme".into(),
                            footprint: fp,
                            priority: prio,
                        };
                        // A fresh enqueue grants at most the request itself
                        if let Admission::Granted(id) = s.request(req) {
                            live.insert(id, fp);
                        }
                    }
                    Op::Release { which } => {
                        if live.is_empty() {
                            continue;
                        }
                        let ids: Vec<SlotId> = live.keys().copied().collect();
                        let id = ids[which % ids.len()];
                        live.remove(&id);
                        // A release may backfill queued requests → fold each grant in
                        for g in s.release(id) {
                            let fp = footprint_of[&g.test_name];
                            live.insert(g.slot_id, fp);
                        }
                    }
                }

                let mirror = live
                    .values()
                    .fold(Resources::ZERO, |acc, fp| acc.saturating_add(fp));
                prop_assert_eq!(
                    s.committed(),
                    mirror,
                    "committed must equal the sum of live leases"
                );
                prop_assert!(
                    s.committed().fits_within(&ceiling),
                    "committed {:?} must never exceed ceiling {:?}",
                    s.committed(),
                    ceiling
                );
                prop_assert_eq!(s.free(), ceiling.saturating_sub(&mirror));
                prop_assert_eq!(s.active_leases(), live.len());
            }
        }
    }
}
