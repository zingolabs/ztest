//! Pure reconstruction of committed capacity from the k8s ledger.
//!
//! Given the live reservation Leases and Jobs (already listed by the allocator
//! under the lock, with a strongly-consistent read), fold them into one global
//! committed figure and per-ServiceAccount usage: the inputs
//! [`crate::qos::scheduler::decide`] needs. (Capacity is a single whole-cluster
//! figure; NVMe vs general is k8s placement, not a partition.)
//!
//! Two rules:
//! - Expiry + GRACE: a reservation counts only while
//!   `renew_tick + lease_ticks + grace >= now`. The GRACE margin makes
//!   crash-reclaim conservative: under cross-run clock skew, prefer to briefly
//!   count a dead reservation (transient under-utilization) over dropping a
//!   live one and overcommitting. (Jobs have no lease heartbeat; a present Job
//!   always counts, see the dedup rule.)
//! - Per-unit `max` dedup: a test (one accounting unit =
//!   [`crate::qos::LABEL_UNIT`]) may hold both a reservation Lease and the Jobs
//!   it spawned. Its contribution is `max(live_reservation_footprint, Σ
//!   job_requests)`, never the sum, so a reservation and its Jobs don't
//!   double-count, while an orphan Job (no live reservation) is still counted
//!   as a reconcile correction.
//!
//! Pure and clock-free except for the injected `now` / `grace` ticks.

use std::collections::BTreeMap;

use super::store::StoredObject;
use super::{
    ANN_CPU_MILLI, ANN_LEASE_TICKS, ANN_MEM_BYTES, ANN_RENEW_TICK, LABEL_SA, LABEL_UNIT, Resources,
};

/// The reconstructed view fed to the scheduler's fit decision.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Reconstructed {
    /// Total committed capacity across the whole cluster.
    pub committed: Resources,
    /// Active usage per ServiceAccount (for the per-SA budget check).
    pub sa_usage: BTreeMap<String, Resources>,
}

impl Reconstructed {
    /// An SA's reconstructed usage (zero if it holds nothing).
    pub fn usage(&self, sa: &str) -> Resources {
        self.sa_usage.get(sa).copied().unwrap_or(Resources::ZERO)
    }
}

/// One accounting unit's running totals while folding.
#[derive(Default)]
struct Agg {
    sa: String,
    /// Sum of live reservation footprints for this unit (normally one).
    reservation: Resources,
    /// Sum of Job pod-request footprints for this unit.
    jobs: Resources,
}

fn footprint_of(o: &StoredObject) -> Resources {
    Resources::new(
        o.annot_u64(ANN_CPU_MILLI).unwrap_or(0),
        o.annot_u64(ANN_MEM_BYTES).unwrap_or(0),
    )
}

/// A reservation is live while `renew + lease + grace >= now`.
fn reservation_is_live(o: &StoredObject, now: u64, grace: u64) -> bool {
    let renew = o.annot_u64(ANN_RENEW_TICK).unwrap_or(0);
    let lease = o.annot_u64(ANN_LEASE_TICKS).unwrap_or(0);
    renew.saturating_add(lease).saturating_add(grace) >= now
}

fn unit_of(o: &StoredObject) -> String {
    o.labels.get(LABEL_UNIT).cloned().unwrap_or_default()
}

fn sa_of(o: &StoredObject) -> String {
    o.labels.get(LABEL_SA).cloned().unwrap_or_default()
}

/// Fold live reservations + Jobs into one global committed figure and
/// usage-per-SA.
pub fn reconstruct(
    reservations: &[StoredObject],
    jobs: &[StoredObject],
    now: u64,
    grace: u64,
) -> Reconstructed {
    let mut units: BTreeMap<String, Agg> = BTreeMap::new();

    for o in reservations {
        let agg = units.entry(unit_of(o)).or_default();
        agg.sa = sa_of(o);
        if reservation_is_live(o, now, grace) {
            agg.reservation = agg.reservation.saturating_add(&footprint_of(o));
        }
    }
    for o in jobs {
        let agg = units.entry(unit_of(o)).or_default();
        // A Job may belong to a unit with no (live) reservation; still
        // capture its SA so the orphan is charged correctly.
        if agg.sa.is_empty() {
            agg.sa = sa_of(o);
        }
        agg.jobs = agg.jobs.saturating_add(&footprint_of(o));
    }

    let mut out = Reconstructed::default();
    for agg in units.into_values() {
        let contribution = agg.reservation.max(&agg.jobs);
        out.committed = out.committed.saturating_add(&contribution);
        let entry = out.sa_usage.entry(agg.sa).or_insert(Resources::ZERO);
        *entry = entry.saturating_add(&contribution);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::GIB;
    use std::collections::BTreeMap;

    /// Build a reservation StoredObject.
    fn res(unit: &str, sa: &str, cpu: u64, mem: u64, renew: u64, lease: u64) -> StoredObject {
        StoredObject {
            name: format!("res-{unit}"),
            resource_version: 1,
            labels: BTreeMap::from([
                (LABEL_UNIT.into(), unit.into()),
                (LABEL_SA.into(), sa.into()),
            ]),
            annotations: BTreeMap::from([
                (ANN_CPU_MILLI.into(), cpu.to_string()),
                (ANN_MEM_BYTES.into(), mem.to_string()),
                (ANN_RENEW_TICK.into(), renew.to_string()),
                (ANN_LEASE_TICKS.into(), lease.to_string()),
            ]),
        }
    }

    /// Build a Job StoredObject (no lease/renew; Jobs don't heartbeat).
    fn job(unit: &str, sa: &str, cpu: u64, mem: u64) -> StoredObject {
        StoredObject {
            name: format!("job-{unit}"),
            resource_version: 1,
            labels: BTreeMap::from([
                (LABEL_UNIT.into(), unit.into()),
                (LABEL_SA.into(), sa.into()),
            ]),
            annotations: BTreeMap::from([
                (ANN_CPU_MILLI.into(), cpu.to_string()),
                (ANN_MEM_BYTES.into(), mem.to_string()),
            ]),
        }
    }

    #[test]
    fn live_reservation_is_counted_into_committed_and_sa() {
        let r = res("u1", "acme", 2_000, 4 * GIB, 0, 10);
        let recon = reconstruct(&[r], &[], 5, 3);
        assert_eq!(recon.committed, Resources::new(2_000, 4 * GIB));
        assert_eq!(recon.usage("acme"), Resources::new(2_000, 4 * GIB));
    }

    #[test]
    fn expiry_boundary_uses_renew_plus_lease_plus_grace() {
        // renew 0 + lease 10 + grace 5 = 15.
        let r = res("u1", "acme", 2_000, 4 * GIB, 0, 10);
        assert_eq!(
            reconstruct(std::slice::from_ref(&r), &[], 15, 5).committed,
            Resources::new(2_000, 4 * GIB)
        );
        // now == 16: expired, contributes nothing.
        assert_eq!(reconstruct(&[r], &[], 16, 5).committed, Resources::ZERO);
    }

    #[test]
    fn reservation_and_its_jobs_are_deduped_by_max_not_summed() {
        let r = res("u1", "acme", 4_000, 4 * GIB, 0, 100);
        let j = job("u1", "acme", 4_000, 4 * GIB);
        let recon = reconstruct(&[r], &[j], 1, 5);
        assert_eq!(recon.committed, Resources::new(4_000, 4 * GIB)); // max(4,4) == 4
        assert_eq!(recon.usage("acme"), Resources::new(4_000, 4 * GIB));
    }

    #[test]
    fn larger_job_dominates_smaller_reservation_per_dimension() {
        let r = res("u1", "acme", 2_000, 8 * GIB, 0, 100);
        let j = job("u1", "acme", 6_000, GIB);
        // Per-dimension max: cpu from job (6000), mem from reservation (8Gi).
        assert_eq!(
            reconstruct(&[r], &[j], 1, 5).committed,
            Resources::new(6_000, 8 * GIB)
        );
    }

    #[test]
    fn orphan_job_without_live_reservation_still_counts() {
        let r = res("u1", "acme", 4_000, 4 * GIB, 0, 1); // expires early
        let j = job("u1", "acme", 4_000, 4 * GIB);
        let recon = reconstruct(&[r], &[j], 100, 5); // well past expiry
        assert_eq!(recon.committed, Resources::new(4_000, 4 * GIB));
    }

    #[test]
    fn service_accounts_are_kept_separate_and_summed_globally() {
        let a = res("u1", "acme", 1_000, GIB, 0, 100);
        let b = res("u2", "other", 8_000, 16 * GIB, 0, 100);
        let recon = reconstruct(&[a, b], &[], 1, 5);
        // One global committed figure (no pool split).
        assert_eq!(recon.committed, Resources::new(9_000, 17 * GIB));
        assert_eq!(recon.usage("acme"), Resources::new(1_000, GIB));
        assert_eq!(recon.usage("other"), Resources::new(8_000, 16 * GIB));
    }
}
