//! Live during-run snapshot of the QoS ledger (`docs/qos-design.md` §8, the
//! live half).
//!
//! Decentralized: there is no broker process, so the `ztest run` parent learns
//! what's running by listing the `zaino-qos` reservation Leases while nextest
//! executes. This module is the pure summary `(reservations, now) -> snapshot`,
//! unit-testable without a cluster; `cli/run.rs` does the kube list, feeds the
//! result here, then renders the panel.
//!
//! Only what the ledger truthfully knows is surfaced: per-tier running (tests
//! currently holding a live reservation), the committed reserve, and
//! per-ServiceAccount usage. A test blocked in its admission loop holds no
//! Lease, so "queued" is not observable here; the panel derives an estimated
//! "remaining" from the planning total separately.

use std::collections::BTreeMap;

use super::store::StoredObject;
use super::{
    ANN_CPU_MILLI, ANN_LEASE_TICKS, ANN_MEM_BYTES, ANN_RENEW_TICK, LABEL_SA, LABEL_TIER, QosClass,
    Resources,
};

/// One tier's live footprint: how many reservations are held and their summed
/// reserve.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TierLive {
    pub count: u32,
    pub reserve: Resources,
}

/// A point-in-time view of the reservation ledger.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveSnapshot {
    /// Running reservations grouped by tier (only tiers with ≥1 running).
    pub running: BTreeMap<QosClass, TierLive>,
    /// Total reserve committed by all live reservations.
    pub committed: Resources,
    /// Live reserve charged per ServiceAccount.
    pub by_sa: BTreeMap<String, Resources>,
}

impl LiveSnapshot {
    /// Total running test count across all tiers.
    pub fn total_running(&self) -> u32 {
        self.running.values().map(|t| t.count).sum()
    }
}

/// `true` if a reservation is still live at `now`: `renew + lease + grace ≥
/// now`. Mirrors the [`super::ledger`] expiry rule (a crashed run's reservation
/// is excluded once past grace) so the panel and the allocator agree on what
/// counts.
fn is_live(o: &StoredObject, now: u64, grace: u64) -> bool {
    let renew = o.annot_u64(ANN_RENEW_TICK).unwrap_or(0);
    let lease = o.annot_u64(ANN_LEASE_TICKS).unwrap_or(0);
    renew.saturating_add(lease).saturating_add(grace) >= now
}

/// This reservation's footprint from its annotations (`0` if absent).
fn footprint_of(o: &StoredObject) -> Resources {
    Resources::new(
        o.annot_u64(ANN_CPU_MILLI).unwrap_or(0),
        o.annot_u64(ANN_MEM_BYTES).unwrap_or(0),
    )
}

/// Summarize the live reservation Leases into a [`LiveSnapshot`]. Expired
/// (past-grace) reservations and ones with an unknown/absent tier label are
/// skipped. `now`/`grace` are in the allocator's tick unit (unix seconds in
/// production).
pub fn summarize(reservations: &[StoredObject], now: u64, grace: u64) -> LiveSnapshot {
    let mut snap = LiveSnapshot::default();
    for o in reservations {
        if !is_live(o, now, grace) {
            continue;
        }
        let Some(class) = o
            .labels
            .get(LABEL_TIER)
            .and_then(|s| QosClass::from_label(s))
        else {
            continue; // not a tier-tagged reservation (or unknown tier)
        };
        let fp = footprint_of(o);
        let tier = snap.running.entry(class).or_default();
        tier.count += 1;
        tier.reserve = tier.reserve.saturating_add(&fp);
        snap.committed = snap.committed.saturating_add(&fp);
        if let Some(sa) = o.labels.get(LABEL_SA) {
            let e = snap.by_sa.entry(sa.clone()).or_default();
            *e = e.saturating_add(&fp);
        }
    }
    snap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::GIB;

    /// Build a reservation `StoredObject` with the given tier/sa/footprint and
    /// renew/lease ticks.
    fn res(
        name: &str,
        tier: &str,
        sa: &str,
        cpu: u64,
        mem: u64,
        renew: u64,
        lease: u64,
    ) -> StoredObject {
        StoredObject {
            name: name.to_string(),
            resource_version: 1,
            labels: BTreeMap::from([
                (LABEL_TIER.to_string(), tier.to_string()),
                (LABEL_SA.to_string(), sa.to_string()),
            ]),
            annotations: BTreeMap::from([
                (ANN_CPU_MILLI.to_string(), cpu.to_string()),
                (ANN_MEM_BYTES.to_string(), mem.to_string()),
                (ANN_RENEW_TICK.to_string(), renew.to_string()),
                (ANN_LEASE_TICKS.to_string(), lease.to_string()),
            ]),
        }
    }

    #[test]
    fn groups_live_reservations_by_tier_with_reserve_sums() {
        let r = vec![
            res("a", "sync", "acme", 8_000, 16 * GIB, 100, 90),
            res("b", "basic", "acme", 500, 512 * crate::qos::MIB, 100, 90),
            res("c", "basic", "beta", 500, 512 * crate::qos::MIB, 100, 90),
        ];
        let s = summarize(&r, 100, 10);
        assert_eq!(s.total_running(), 3);
        assert_eq!(s.running[&QosClass::Sync].count, 1);
        assert_eq!(s.running[&QosClass::Basic].count, 2);
        assert_eq!(
            s.running[&QosClass::Basic].reserve,
            Resources::new(1_000, GIB)
        );
        // Committed = all three.
        assert_eq!(s.committed, Resources::new(9_000, 17 * GIB));
    }

    #[test]
    fn expired_past_grace_reservations_are_excluded() {
        // renew 0 + lease 90 + grace 10 = 100; at now=150 it's long dead.
        let r = vec![
            res("dead", "sync", "acme", 8_000, 16 * GIB, 0, 90),
            res("live", "basic", "acme", 500, 512 * crate::qos::MIB, 140, 90),
        ];
        let s = summarize(&r, 150, 10);
        assert_eq!(s.total_running(), 1);
        assert!(s.running.contains_key(&QosClass::Basic));
        assert!(!s.running.contains_key(&QosClass::Sync));
    }

    #[test]
    fn within_grace_still_counts() {
        // lease ends at 90, grace to 100; now=95 is past lease but within grace.
        let r = vec![res("a", "testnet", "acme", 4_000, 8 * GIB, 0, 90)];
        let s = summarize(&r, 95, 10);
        assert_eq!(s.total_running(), 1);
    }

    #[test]
    fn per_sa_usage_is_summed() {
        let r = vec![
            res("a", "integration", "acme", 2_000, 2 * GIB, 100, 90),
            res("b", "basic", "acme", 500, 512 * crate::qos::MIB, 100, 90),
            res("c", "sync", "beta", 8_000, 16 * GIB, 100, 90),
        ];
        let s = summarize(&r, 100, 10);
        assert_eq!(
            s.by_sa["acme"],
            Resources::new(2_500, 2 * GIB + 512 * crate::qos::MIB)
        );
        assert_eq!(s.by_sa["beta"], Resources::new(8_000, 16 * GIB));
    }

    #[test]
    fn unknown_or_missing_tier_is_skipped() {
        let mut weird = res("x", "bogus", "acme", 1_000, GIB, 100, 90);
        // A reservation with no tier label at all.
        let mut untagged = res("y", "basic", "acme", 1_000, GIB, 100, 90);
        untagged.labels.remove(LABEL_TIER);
        weird
            .labels
            .insert(LABEL_TIER.to_string(), "bogus".to_string());
        let s = summarize(&[weird, untagged], 100, 10);
        assert!(s.running.is_empty());
        assert_eq!(s.committed, Resources::ZERO);
    }

    #[test]
    fn empty_ledger_is_an_empty_snapshot() {
        let s = summarize(&[], 100, 10);
        assert_eq!(s, LiveSnapshot::default());
        assert_eq!(s.total_running(), 0);
    }
}
