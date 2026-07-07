//! The decentralized admission protocol.
//!
//! There is no central broker process. Every `ztest run` carries an
//! [`Allocator`] that, to admit a topology-booting test, runs a short
//! read-decide-write transaction against the shared k8s ledger (via an
//! [`ObjectStore`]) and either claims a reservation Lease or backs off.
//! Concurrent runs coordinate through k8s.
//!
//! The lock does not serialize writes (k8s create-409 already gives per-object
//! write exclusion). It serializes the read-decide-write transaction
//! (reconstruct committed capacity, decide fit, commit), which k8s offers no
//! other primitive for. Without it, two runs could each list capacity, each
//! see room, and each create a reservation, overcommitting. The lock is a
//! `coordination.k8s.io` Lease used leader-election style: acquire-if-absent,
//! steal-if-expired via a `resourceVersion` precondition, back-off-if-held-fresh.
//!
//! Read-after-write hazard: a reservation created by one lock-holder must be
//! visible to the next. k8s `list` is watch-cache-served and only eventually
//! consistent, so the in-section reservation/Job list is issued not-older-than
//! the lock's version (its epoch). If the store cannot serve a read that fresh
//! it returns [`StoreError::StaleRead`], and the allocator refuses to decide
//! ([`Outcome::Retry`]) rather than act on a stale view.
//!
//! Crash safety and clock skew: a reservation Lease carries a renew tick +
//! duration; the [`ledger`] excludes it once `renew + lease + grace < now`, so
//! a crashed run's capacity is reclaimed without a central coordinator. The
//! GRACE margin makes reclaim conservative under cross-run clock skew (prefer
//! transient under-utilization over overcommit).
//!
//! Unit-test coverage gaps: the suite injects a single logical clock, so all
//! actors agree on `now`; the real cross-run clock-skew hazard is
//! unrepresentable here and is owned by the GRACE constant, integration tests,
//! and the `janitor/ttl` backstop. The in-memory fake is authoritative, so
//! tests prove the allocator refuses stale reads, not that the real `kube`
//! adapter requests consistent ones (that binding is the adapter's integration
//! test). Interleavings exercised are the ones the test driver chooses.

use std::collections::BTreeMap;

use crate::naming::{DNS_LABEL_MAX, slug};

use super::ledger;
use super::scheduler::{self, RejectReason, Verdict};
use super::store::{Kind, LabelSelector, NewObject, ObjectPatch, ObjectStore, StoreError};
use super::{
    ANN_CPU_MILLI, ANN_HOLDER, ANN_LEASE_TICKS, ANN_MEM_BYTES, ANN_RENEW_TICK, LABEL_ROLE, LABEL_SA,
    LABEL_TIER, LABEL_UNIT, LABEL_USER, QosClass, ROLE_ALLOCATOR_LOCK, ROLE_JOB, ROLE_RESERVATION,
    Resources,
};

/// Fixed name of the singleton allocator-lock Lease.
const LOCK_NAME: &str = "ztest-qos-allocator";

/// A request to reserve capacity for one test.
///
/// Capacity is one global figure, so there is no pool to choose: the tier's
/// NVMe-vs-general placement rides on the pod specs (toleration/nodeSelector),
/// applied elsewhere, not on the reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationRequest {
    /// The accounting unit: the test's namespace identity. Determines the
    /// reservation Lease name (so a retry is idempotent) and the
    /// [`LABEL_UNIT`] that links the reservation to its Jobs.
    pub unit: String,
    /// The ServiceAccount this reservation is charged to (budget scope).
    pub sa: String,
    /// The namespace-aggregate reserve to schedule against.
    pub footprint: Resources,
    /// The declared tier, recorded as [`LABEL_TIER`] on the reservation Lease
    /// for the live during-run panel. Display metadata only; admission keys on
    /// [`Self::footprint`].
    pub class: QosClass,
}

/// The result of an admission attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Admitted; the named reservation Lease is held. Boot the topology.
    Granted { reservation: String },
    /// Fits in principle but no room (or SA at quota) right now; retry later.
    Queued,
    /// Unschedulable even on an empty cluster / against the whole SA budget.
    Rejected(RejectReason),
    /// Transient: lock contended, or the in-section read wasn't fresh
    /// enough. Retry immediately; nothing was committed.
    Retry,
}

/// A run's allocator. Generic over the [`ObjectStore`] so the same protocol
/// runs against the in-memory fake (tests) and the real `kube` adapter.
#[derive(Debug)]
pub struct Allocator<S> {
    store: S,
    /// Whole-cluster capacity available to ztest (from the probe's
    /// `ClusterCapacity::free()`); config, not a writable k8s object.
    available: Resources,
    budgets: BTreeMap<String, Resources>,
    /// This run's holder identity, stamped on the lock and reservations.
    identity: String,
    /// This run's invoking user (slugged), stamped as [`LABEL_USER`] on
    /// reservations so `ztest cleanup` can reclaim one developer's leases.
    /// `None` in tests, which don't exercise by-user reaping.
    user: Option<String>,
    /// Allocator-lock duration in logical ticks (short: the section is brief).
    lock_ticks: u64,
    /// Reservation Lease duration in logical ticks (the heartbeat TTL).
    reservation_ticks: u64,
    /// Reclaim grace margin in ticks (skew guard, well above plausible skew).
    grace: u64,
}

impl<S: ObjectStore> Allocator<S> {
    /// Construct an allocator. `identity` is this run's holder id (e.g.
    /// `RunCoords::run_id`).
    pub fn new(
        store: S,
        available: Resources,
        identity: impl Into<String>,
        lock_ticks: u64,
        reservation_ticks: u64,
        grace: u64,
    ) -> Self {
        Allocator {
            store,
            available,
            budgets: BTreeMap::new(),
            identity: identity.into(),
            user: None,
            lock_ticks,
            reservation_ticks,
            grace,
        }
    }

    /// Stamp reservations this allocator creates with the invoking user, so
    /// `ztest cleanup` can select one developer's leases by [`LABEL_USER`].
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Register a ServiceAccount's total budget (policy/config).
    pub fn set_budget(&mut self, sa: impl Into<String>, budget: Resources) {
        self.budgets.insert(sa.into(), budget);
    }

    /// Attempt to admit `req` at logical time `now`. Acquires the lock,
    /// reconstructs committed capacity from a strongly-consistent read,
    /// decides, and (if it fits) atomically claims a reservation Lease.
    /// Always releases the lock before returning.
    pub async fn try_admit(
        &self,
        req: &ReservationRequest,
        now: u64,
    ) -> Result<Outcome, StoreError> {
        let epoch = match self.acquire_lock(now).await? {
            Some(epoch) => epoch,
            None => return Ok(Outcome::Retry),
        };
        let outcome = self.decide_and_commit(req, now, epoch).await;
        // Always release: a transient failure must not wedge admissions
        // cluster-wide. The lock's TTL is the backstop on a crash here.
        let _ = self.release_lock().await;
        outcome
    }

    /// The critical section, run while holding the lock at version `epoch`.
    async fn decide_and_commit(
        &self,
        req: &ReservationRequest,
        now: u64,
        epoch: u64,
    ) -> Result<Outcome, StoreError> {
        // Strongly-consistent reads: not-older-than the lock's epoch.
        let reservations = match self
            .store
            .list(
                Kind::Reservation,
                &LabelSelector::eq(LABEL_ROLE, ROLE_RESERVATION),
                Some(epoch),
            )
            .await
        {
            Ok(v) => v,
            Err(StoreError::StaleRead) => return Ok(Outcome::Retry),
            Err(e) => return Err(e),
        };
        let jobs = match self
            .store
            .list(
                Kind::Job,
                &LabelSelector::eq(LABEL_ROLE, ROLE_JOB),
                Some(epoch),
            )
            .await
        {
            Ok(v) => v,
            Err(StoreError::StaleRead) => return Ok(Outcome::Retry),
            Err(e) => return Err(e),
        };

        let recon = ledger::reconstruct(&reservations, &jobs, now, self.grace);
        let verdict = scheduler::decide(
            self.available,
            recon.committed,
            recon.usage(&sa_key(&req.sa)),
            self.budgets.get(&req.sa).copied(),
            req.footprint,
        );
        match verdict {
            Verdict::Reject(reason) => Ok(Outcome::Rejected(reason)),
            Verdict::Queue => Ok(Outcome::Queued),
            Verdict::Fits => {
                // Fence the commit on continued lock ownership. The lock isn't
                // renewed during the section, so a critical section that
                // outlives `LOCK_TICKS` can be stolen by another allocator that
                // then reads this same free snapshot and also grants
                // (overcommit). Re-check the lock immediately before creating:
                // if its version moved off `epoch` (a steal/renew) or it's no
                // longer ours, abort and retry rather than commit. This narrows
                // the TOCTOU window from the whole section to the gap between
                // this check and the create (sub-millisecond, far below
                // `LOCK_TICKS`).
                match self.store.get(Kind::AllocatorLock, LOCK_NAME).await? {
                    Some(o)
                        if o.resource_version == epoch
                            && o.annotations.get(ANN_HOLDER) == Some(&self.identity) => {}
                    _ => return Ok(Outcome::Retry),
                }
                let name = reservation_name(&req.unit);
                match self
                    .store
                    .create(Kind::Reservation, self.reservation_object(&name, req, now))
                    .await
                {
                    Ok(_) => Ok(Outcome::Granted { reservation: name }),
                    // Deterministic name already present: an earlier attempt of
                    // this same unit already claimed it. Idempotent grant.
                    Err(StoreError::AlreadyExists) => Ok(Outcome::Granted { reservation: name }),
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Try to acquire the allocator lock at `now`. `Some(epoch)` is held (epoch
    /// is the lock's version, used as the read watermark); `None` is contended,
    /// caller should [`Outcome::Retry`].
    pub async fn acquire_lock(&self, now: u64) -> Result<Option<u64>, StoreError> {
        match self.store.get(Kind::AllocatorLock, LOCK_NAME).await? {
            None => match self
                .store
                .create(Kind::AllocatorLock, self.lock_object(now))
                .await
            {
                Ok(rv) => Ok(Some(rv)),
                Err(StoreError::AlreadyExists) => Ok(None), // raced another creator
                Err(e) => Err(e),
            },
            Some(obj) => {
                let renew = obj.annot_u64(ANN_RENEW_TICK).unwrap_or(0);
                let lease = obj.annot_u64(ANN_LEASE_TICKS).unwrap_or(0);
                let expired = renew.saturating_add(lease) < now;
                let mine = obj.annotations.get(ANN_HOLDER) == Some(&self.identity);
                if expired {
                    // Steal via an rv precondition: a second stealer with the
                    // same stale view loses with Conflict.
                    match self
                        .store
                        .update(
                            Kind::AllocatorLock,
                            LOCK_NAME,
                            obj.resource_version,
                            self.lock_patch(now),
                        )
                        .await
                    {
                        Ok(rv) => Ok(Some(rv)),
                        Err(StoreError::Conflict) => Ok(None),
                        Err(e) => Err(e),
                    }
                } else if mine {
                    Ok(Some(obj.resource_version)) // re-entrant: already ours
                } else {
                    Ok(None) // held fresh by another: back off
                }
            }
        }
    }

    /// Release the allocator lock (best-effort; 404 means already gone).
    pub async fn release_lock(&self) -> Result<(), StoreError> {
        match self.store.delete(Kind::AllocatorLock, LOCK_NAME).await {
            Ok(()) | Err(StoreError::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Heartbeat a reservation Lease: bump its renew tick to `now` so it stays
    /// live. Called periodically by the holding test process.
    pub async fn renew(&self, reservation: &str, now: u64) -> Result<(), StoreError> {
        let obj = match self.store.get(Kind::Reservation, reservation).await? {
            Some(o) => o,
            None => return Err(StoreError::NotFound),
        };
        let patch = ObjectPatch {
            annotations: BTreeMap::from([(ANN_RENEW_TICK.to_string(), now.to_string())]),
            ..Default::default()
        };
        self.store
            .update(Kind::Reservation, reservation, obj.resource_version, patch)
            .await
            .map(|_| ())
    }

    /// Release a reservation on clean teardown (404 means already gone).
    pub async fn release(&self, reservation: &str) -> Result<(), StoreError> {
        match self.store.delete(Kind::Reservation, reservation).await {
            Ok(()) | Err(StoreError::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Housekeeping: delete reservation Leases expired past the grace margin
    /// (crashed runs that never released). Returns reclaimed names. Capacity is
    /// freed for admission by the ledger's expiry check before this runs; this
    /// stops the objects accumulating.
    pub async fn reclaim_expired(&self, now: u64) -> Result<Vec<String>, StoreError> {
        let reservations = self
            .store
            .list(
                Kind::Reservation,
                &LabelSelector::eq(LABEL_ROLE, ROLE_RESERVATION),
                None,
            )
            .await?;
        let mut reclaimed = Vec::new();
        for o in reservations {
            let renew = o.annot_u64(ANN_RENEW_TICK).unwrap_or(0);
            let lease = o.annot_u64(ANN_LEASE_TICKS).unwrap_or(0);
            if renew.saturating_add(lease).saturating_add(self.grace) < now {
                match self.store.delete(Kind::Reservation, &o.name).await {
                    Ok(()) | Err(StoreError::NotFound) => reclaimed.push(o.name),
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(reclaimed)
    }

    // ── object construction ────────────────────────────────────────────

    fn lock_object(&self, now: u64) -> NewObject {
        NewObject {
            name: LOCK_NAME.to_string(),
            labels: BTreeMap::from([(LABEL_ROLE.to_string(), ROLE_ALLOCATOR_LOCK.to_string())]),
            annotations: self.lock_annotations(now),
        }
    }

    fn lock_patch(&self, now: u64) -> ObjectPatch {
        ObjectPatch {
            annotations: self.lock_annotations(now),
            ..Default::default()
        }
    }

    fn lock_annotations(&self, now: u64) -> BTreeMap<String, String> {
        BTreeMap::from([
            (ANN_HOLDER.to_string(), self.identity.clone()),
            (ANN_RENEW_TICK.to_string(), now.to_string()),
            (ANN_LEASE_TICKS.to_string(), self.lock_ticks.to_string()),
        ])
    }

    fn reservation_object(&self, name: &str, req: &ReservationRequest, now: u64) -> NewObject {
        NewObject {
            name: name.to_string(),
            labels: {
                let mut labels = BTreeMap::from([
                    (LABEL_ROLE.to_string(), ROLE_RESERVATION.to_string()),
                    (LABEL_SA.to_string(), sa_key(&req.sa)),
                    (LABEL_UNIT.to_string(), unit_slug(&req.unit)),
                    (LABEL_TIER.to_string(), req.class.as_label().to_string()),
                ]);
                if let Some(user) = &self.user {
                    labels.insert(LABEL_USER.to_string(), user.clone());
                }
                labels
            },
            annotations: BTreeMap::from([
                (ANN_HOLDER.to_string(), self.identity.clone()),
                (
                    ANN_CPU_MILLI.to_string(),
                    req.footprint.cpu_milli.to_string(),
                ),
                (
                    ANN_MEM_BYTES.to_string(),
                    req.footprint.mem_bytes.to_string(),
                ),
                (ANN_RENEW_TICK.to_string(), now.to_string()),
                (
                    ANN_LEASE_TICKS.to_string(),
                    self.reservation_ticks.to_string(),
                ),
            ]),
        }
    }
}

/// Deterministic reservation Lease name for an accounting unit: same unit maps
/// to the same name (idempotent retry), different units to different names.
pub fn reservation_name(unit: &str) -> String {
    format!("qos-{}", unit_slug(unit))
}

/// Canonical slug of an accounting unit, feeding both the reservation Lease name
/// ([`reservation_name`]) and its [`LABEL_UNIT`] value — one slug keeps the two
/// in lockstep. Bounded well under [`DNS_LABEL_MAX`] to leave room for the
/// `qos-` name prefix; a `namespace_for` unit is already shorter, so it never
/// truncates in practice.
fn unit_slug(unit: &str) -> String {
    slug(unit, 56)
}

/// The label-safe key an SA is grouped/charged under. Must match between writing
/// the reservation's [`LABEL_SA`] and reconstructing per-SA usage.
fn sa_key(sa: &str) -> String {
    slug(sa, DNS_LABEL_MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::GIB;
    use crate::qos::store::fake::FakeStore;
    use crate::qos::store::{Kind, StoreError};

    /// Allocator over `store` with identity `id` and global `available`
    /// capacity; lock TTL 5, reservation TTL 100, grace 10 ticks.
    fn allocator(store: FakeStore, id: &str, available: Resources) -> Allocator<FakeStore> {
        Allocator::new(store, available, id, 5, 100, 10)
    }

    fn rr(unit: &str, sa: &str, cpu: u64, mem: u64) -> ReservationRequest {
        ReservationRequest {
            unit: unit.to_string(),
            sa: sa.to_string(),
            footprint: Resources::new(cpu, mem),
            class: QosClass::Basic,
        }
    }

    /// Inject a Job object straight into the store (simulating a workload).
    async fn put_job(store: &FakeStore, unit: &str, sa: &str, cpu: u64, mem: u64) {
        store
            .create(
                Kind::Job,
                NewObject {
                    name: format!("job-{unit}"),
                    labels: BTreeMap::from([
                        (LABEL_ROLE.to_string(), ROLE_JOB.to_string()),
                        (LABEL_SA.to_string(), sa.to_string()),
                        (LABEL_UNIT.to_string(), unit.to_string()),
                    ]),
                    annotations: BTreeMap::from([
                        (ANN_CPU_MILLI.to_string(), cpu.to_string()),
                        (ANN_MEM_BYTES.to_string(), mem.to_string()),
                    ]),
                },
            )
            .await
            .unwrap();
    }

    // 1. Happy path: admit, claim a reservation, release the lock.
    #[tokio::test]
    async fn admits_when_capacity_is_available_and_releases_the_lock() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::new(8_000, 16 * GIB));
        let out = a
            .try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 0)
            .await
            .unwrap();
        assert!(matches!(out, Outcome::Granted { .. }));
        assert_eq!(store.count(Kind::Reservation), 1);
        // Lock was released (not left held).
        assert_eq!(store.count(Kind::AllocatorLock), 0);
    }

    // 1b. The granted reservation Lease records its tier (for the live panel).
    #[tokio::test]
    async fn reservation_lease_carries_its_tier_label() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::new(8_000, 16 * GIB));
        let req = ReservationRequest {
            unit: "u1".to_string(),
            sa: "acme".to_string(),
            footprint: Resources::new(4_000, 4 * GIB),
            class: QosClass::Sync,
        };
        a.try_admit(&req, 0).await.unwrap();
        let obj = store
            .get(Kind::Reservation, &reservation_name("u1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(obj.labels.get(LABEL_TIER).map(String::as_str), Some("sync"));
    }

    // 2. Lock acquire-when-absent.
    #[tokio::test]
    async fn lock_is_acquired_when_absent() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::ZERO);
        assert!(a.acquire_lock(0).await.unwrap().is_some());
        assert_eq!(store.count(Kind::AllocatorLock), 1);
    }

    // 3. Fresh, not-mine lock: back off (no steal, store unchanged).
    #[tokio::test]
    async fn fresh_lock_held_by_another_backs_off() {
        let store = FakeStore::new();
        let a1 = allocator(store.clone(), "run-a", Resources::ZERO);
        let a2 = allocator(store.clone(), "run-b", Resources::ZERO);
        assert!(a1.acquire_lock(0).await.unwrap().is_some());
        let v = store.version();
        // a2 sees a1's fresh lock and backs off; nothing written.
        assert!(a2.acquire_lock(1).await.unwrap().is_none());
        assert_eq!(store.version(), v);
        // a1 re-acquiring its own fresh lock is fine (re-entrant).
        assert!(a1.acquire_lock(1).await.unwrap().is_some());
    }

    // 4. Expired lock is stolen; an rv precondition stops a double-steal.
    #[tokio::test]
    async fn expired_lock_steal_uses_rv_precondition() {
        let store = FakeStore::new();
        let a1 = allocator(store.clone(), "run-a", Resources::ZERO);
        let a2 = allocator(store.clone(), "run-b", Resources::ZERO);
        a1.acquire_lock(0).await.unwrap(); // renew 0, lease 5, expires at 5
        // Capture the (now-expired) version two racers would both see.
        let stale_rv = store
            .get(Kind::AllocatorLock, LOCK_NAME)
            .await
            .unwrap()
            .unwrap()
            .resource_version;
        // a2 steals at now=6.
        assert!(a2.acquire_lock(6).await.unwrap().is_some());
        // A second stealer holding the same stale rv loses with Conflict.
        assert_eq!(
            store
                .update(
                    Kind::AllocatorLock,
                    LOCK_NAME,
                    stale_rv,
                    ObjectPatch::default()
                )
                .await,
            Err(StoreError::Conflict)
        );
    }

    // 5. HEADLINE: two runs over one store cannot overcommit.
    #[tokio::test]
    async fn two_allocators_sharing_one_store_cannot_overcommit() {
        let store = FakeStore::new();
        // Pool fits exactly one 4-CPU reservation.
        let a1 = allocator(store.clone(), "run-a", Resources::new(4_000, 4 * GIB));
        let a2 = allocator(store.clone(), "run-b", Resources::new(4_000, 4 * GIB));
        let g = a1
            .try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 0)
            .await
            .unwrap();
        assert!(matches!(g, Outcome::Granted { .. }));
        // a2's strongly-consistent read sees u1, no room, so it Queues.
        let q = a2
            .try_admit(&rr("u2", "acme", 4_000, 4 * GIB), 1)
            .await
            .unwrap();
        assert_eq!(q, Outcome::Queued);
        assert_eq!(store.count(Kind::Reservation), 1, "exactly one reservation");
    }

    // 6. HEADLINE: a stale in-section read is refused, not acted on.
    #[tokio::test]
    async fn stale_in_section_list_is_refused_not_acted_on() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::new(8_000, 16 * GIB));
        // Simulate watch-cache lag on the in-section reservation list.
        store.fail_next_list(StoreError::StaleRead);
        let out = a
            .try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 0)
            .await
            .unwrap();
        assert_eq!(
            out,
            Outcome::Retry,
            "must refuse, not decide on a stale read"
        );
        assert_eq!(store.count(Kind::Reservation), 0, "nothing committed");
        assert_eq!(store.count(Kind::AllocatorLock), 0, "lock released");
    }

    // 7. Crash reclaim via expiry frees capacity for a waiter.
    #[tokio::test]
    async fn crash_reclaim_via_expiry_admits_the_waiter() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::new(4_000, 4 * GIB));
        a.try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 0)
            .await
            .unwrap();
        // u1 holds it (renew 0 + lease 100 + grace 10, expires at 110).
        assert_eq!(
            a.try_admit(&rr("u2", "acme", 4_000, 4 * GIB), 5)
                .await
                .unwrap(),
            Outcome::Queued
        );
        // Long after expiry, u1 is excluded, so u2 is admitted.
        assert!(matches!(
            a.try_admit(&rr("u2", "acme", 4_000, 4 * GIB), 200)
                .await
                .unwrap(),
            Outcome::Granted { .. }
        ));
    }

    // 8. Within GRACE, a past-lease reservation is NOT reclaimed.
    #[tokio::test]
    async fn reservation_within_grace_is_not_reclaimed() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::new(4_000, 4 * GIB));
        a.try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 0)
            .await
            .unwrap();
        // lease ends at 100; grace to 110. now=105 is past the lease but
        // within grace, so u1 still counts and the waiter Queues.
        assert_eq!(
            a.try_admit(&rr("u2", "acme", 4_000, 4 * GIB), 105)
                .await
                .unwrap(),
            Outcome::Queued
        );
    }

    // 9. Job pod requests count toward committed capacity.
    #[tokio::test]
    async fn job_requests_count_toward_committed() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::new(8_000, 8 * GIB));
        put_job(&store, "ju", "acme", 4_000, 4 * GIB).await;
        // 4 CPU of the 8 is taken by the Job; a 6-CPU ask doesn't fit.
        assert_eq!(
            a.try_admit(&rr("u1", "acme", 6_000, 2 * GIB), 0)
                .await
                .unwrap(),
            Outcome::Queued
        );
        // A 4-CPU ask exactly fills the rest.
        assert!(matches!(
            a.try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 0)
                .await
                .unwrap(),
            Outcome::Granted { .. }
        ));
    }

    // 10. Release frees capacity for a previously-queued footprint.
    #[tokio::test]
    async fn release_frees_capacity() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::new(4_000, 4 * GIB));
        let g = a
            .try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 0)
            .await
            .unwrap();
        let Outcome::Granted { reservation } = g else {
            panic!("expected grant")
        };
        assert_eq!(
            a.try_admit(&rr("u2", "acme", 4_000, 4 * GIB), 1)
                .await
                .unwrap(),
            Outcome::Queued
        );
        a.release(&reservation).await.unwrap();
        assert!(matches!(
            a.try_admit(&rr("u2", "acme", 4_000, 4 * GIB), 2)
                .await
                .unwrap(),
            Outcome::Granted { .. }
        ));
    }

    // 11. Releasing an absent reservation is success (idempotent teardown).
    #[tokio::test]
    async fn release_absent_reservation_is_ok() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::ZERO);
        assert!(a.release("qos-nope").await.is_ok());
    }

    // 12. Rejects: bigger than the pool, and bigger than the SA budget.
    #[tokio::test]
    async fn rejects_unschedulable_asks() {
        let store = FakeStore::new();
        let mut a = allocator(store.clone(), "run-a", Resources::new(4_000, 4 * GIB));
        a.set_budget("acme", Resources::new(2_000, 2 * GIB));
        // Exceeds the (empty) pool.
        assert_eq!(
            a.try_admit(&rr("u1", "acme", 8_000, GIB), 0).await.unwrap(),
            Outcome::Rejected(RejectReason::ExceedsClusterCapacity)
        );
        // Fits the pool but exceeds the whole SA budget.
        assert_eq!(
            a.try_admit(&rr("u2", "acme", 3_000, GIB), 0).await.unwrap(),
            Outcome::Rejected(RejectReason::ExceedsSaBudget)
        );
        assert_eq!(store.count(Kind::Reservation), 0);
    }

    // 13. Deterministic, collision-safe reservation names; retry idempotent.
    #[tokio::test]
    async fn reservation_name_is_deterministic_and_retry_is_idempotent() {
        assert_eq!(
            reservation_name("walletless::sync"),
            reservation_name("walletless::sync")
        );
        assert_ne!(reservation_name("a"), reservation_name("b"));
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::new(8_000, 8 * GIB));
        let g1 = a
            .try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 0)
            .await
            .unwrap();
        let g2 = a
            .try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 1)
            .await
            .unwrap();
        assert_eq!(g1, g2, "same unit re-admit is idempotent");
        assert_eq!(store.count(Kind::Reservation), 1);
    }

    // 14. A holder that crashes mid-section doesn't corrupt state: the next
    //     run steals the expired lock, re-reads, and sees the reservation.
    #[tokio::test]
    async fn crashed_lock_holder_does_not_corrupt_admission() {
        let store = FakeStore::new();
        let a1 = allocator(store.clone(), "run-a", Resources::new(4_000, 4 * GIB));
        let a2 = allocator(store.clone(), "run-b", Resources::new(4_000, 4 * GIB));
        // a1 grabs the lock and commits a reservation, then "crashes"
        // (never releases the lock).
        a1.acquire_lock(0).await.unwrap();
        store
            .create(
                Kind::Reservation,
                a1.reservation_object(
                    &reservation_name("u1"),
                    &rr("u1", "acme", 4_000, 4 * GIB),
                    0,
                ),
            )
            .await
            .unwrap();
        // Lock left held (renew 0, lease 5). At now=6 it's expired; a2 steals,
        // re-lists, sees u1's reservation, no room, so Queued (not overcommit).
        assert_eq!(
            a2.try_admit(&rr("u2", "acme", 4_000, 4 * GIB), 6)
                .await
                .unwrap(),
            Outcome::Queued
        );
        assert_eq!(store.count(Kind::Reservation), 1, "u1's reservation intact");
    }

    // 15. Determinism: identical scripts + clock yield identical outcomes/state.
    #[tokio::test]
    async fn identical_scripts_yield_identical_results() {
        async fn run() -> (Vec<Outcome>, usize) {
            let store = FakeStore::new();
            let a = allocator(store.clone(), "run-a", Resources::new(8_000, 8 * GIB));
            let mut outs = Vec::new();
            outs.push(
                a.try_admit(&rr("u1", "acme", 4_000, 4 * GIB), 0)
                    .await
                    .unwrap(),
            );
            outs.push(
                a.try_admit(&rr("u2", "acme", 4_000, 4 * GIB), 1)
                    .await
                    .unwrap(),
            );
            outs.push(
                a.try_admit(&rr("u3", "acme", 4_000, 4 * GIB), 2)
                    .await
                    .unwrap(),
            );
            (outs, store.count(Kind::Reservation))
        }
        assert_eq!(run().await, run().await);
    }

    // 16. reclaim_expired deletes only past-grace reservations.
    #[tokio::test]
    async fn reclaim_expired_deletes_only_dead_reservations() {
        let store = FakeStore::new();
        let a = allocator(store.clone(), "run-a", Resources::new(16_000, 16 * GIB));
        a.try_admit(&rr("old", "acme", 1_000, GIB), 0)
            .await
            .unwrap(); // expires at 110
        a.try_admit(&rr("new", "acme", 1_000, GIB), 1_000)
            .await
            .unwrap(); // expires at 1110
        let reclaimed = a.reclaim_expired(500).await.unwrap();
        assert_eq!(reclaimed, vec![reservation_name("old")]);
        assert_eq!(store.count(Kind::Reservation), 1);
    }
}
