//! Pre-run scheduling plan for the preflight banner (`docs/design-qos.md` §8).
//!
//! - Pure `(per-test reserves, capacity) -> plan`: concurrency waves, peak reserve,
//!   tests whose footprint exceeds the cluster outright (fail-fast)
//! - Per *test*, not per tier: a test may override its tier's component reserve
//!   (`footprint = ".."`), so two tests in one tier can admit against different
//!   amounts and a per-tier constant would estimate a run nobody is going to get
//! - Not [`super::scheduler`], one letter apart — that one is the live admission core

use super::{QosClass, Resources};

/// One selected test: declared tier + the `admitted` reserve it is submitted with
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedTest {
    pub class: QosClass,
    pub admitted: Resources,
}

/// One tier's contribution, aggregated from its [`PlannedTest`]s.
///
/// - `per_test` = `None` when overrides make the tier non-uniform (renderer shows `subtotal`)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierPlan {
    pub class: QosClass,
    pub count: u32,
    pub per_test: Option<Resources>,
    pub subtotal: Resources,
}

/// Test that cannot fit an empty cluster; carries the amount (class no longer
/// determines it once overrides exist)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unschedulable {
    pub class: QosClass,
    pub admitted: Resources,
}

/// Estimated schedule for the selected tests against probed capacity.
///
/// - `total` = Σ every test's reserve, i.e. the reserve with everything at once
/// - `free` `None` = probe unavailable → `waves`/`peak` are `0`/`ZERO`, counts only
/// - `unschedulable` tests miss even the empty cluster (rejected at admission,
///   `ExceedsClusterCapacity`) and sit out the wave sim
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QosPlan {
    pub tiers: Vec<TierPlan>,
    pub total: Resources,
    pub free: Option<Resources>,
    pub waves: u32,
    pub peak: Resources,
    pub unschedulable: Vec<Unschedulable>,
}

/// Schedule estimate; `tests` = one per selected test, `free` = probed headroom
///
/// - Ordering/sim/fail-fast all per test → matches what `engine::plan` submits one-for-one
pub fn plan(tests: &[PlannedTest], free: Option<Resources>) -> QosPlan {
    // Highest priority first (sync, testnet, integration, basic): display order, and
    // the order the wave sim admits in
    let mut ordered: Vec<PlannedTest> = tests.to_vec();
    ordered.sort_by_key(|t| std::cmp::Reverse(t.class.profile().priority));

    let mut tiers: Vec<TierPlan> = Vec::new();
    for t in &ordered {
        match tiers.last_mut().filter(|tp| tp.class == t.class) {
            Some(tp) => {
                tp.count += 1;
                tp.subtotal = tp.subtotal.saturating_add(&t.admitted);
                if tp.per_test != Some(t.admitted) {
                    tp.per_test = None;
                }
            }
            None => tiers.push(TierPlan {
                class: t.class,
                count: 1,
                per_test: Some(t.admitted),
                subtotal: t.admitted,
            }),
        }
    }

    let total = ordered.iter().fold(Resources::ZERO, |acc, t| acc.saturating_add(&t.admitted));

    let mut unschedulable = Vec::new();
    let (waves, peak) = match free {
        None => (0, Resources::ZERO),
        Some(free) => {
            // Priority-ordered; a test missing even the empty cluster sits out the sim
            let mut units: Vec<Resources> = Vec::new();
            for t in &ordered {
                if t.admitted.fits_within(&free) {
                    units.push(t.admitted);
                } else {
                    unschedulable.push(Unschedulable { class: t.class, admitted: t.admitted });
                }
            }
            simulate_waves(&units, free)
        }
    };

    QosPlan { tiers, total, free, waves, peak, unschedulable }
}

/// Greedy priority+backfill wave sim mirroring `scheduler`'s policy → `(wave count,
/// per-dimension peak reserve)`. Each wave walks priority-ordered `units`, admitting
/// whatever still fits its 2-D capacity; the rest spill to the next
fn simulate_waves(units: &[Resources], free: Resources) -> (u32, Resources) {
    let mut remaining: Vec<Resources> = units.to_vec();
    let mut waves = 0;
    let mut peak = Resources::ZERO;
    while !remaining.is_empty() {
        waves += 1;
        let mut used = Resources::ZERO;
        let mut spill = Vec::new();
        for u in remaining {
            match used.checked_add(&u) {
                Some(after) if after.fits_within(&free) => used = after,
                _ => spill.push(u),
            }
        }
        peak = peak.max(&used);
        // Every unit fits `free` alone (unschedulable filtered) → each wave admits
        // >=1 and `remaining` shrinks; guard is defensive only
        if used == Resources::ZERO {
            break;
        }
        remaining = spill;
    }
    (waves, peak)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::GIB;

    /// `n` tests of `class` at its tier default
    fn at_tier(class: QosClass, n: u32) -> Vec<PlannedTest> {
        vec![PlannedTest { class, admitted: class.profile().admitted() }; n as usize]
    }

    fn tiers(sets: &[(QosClass, u32)]) -> Vec<PlannedTest> {
        sets.iter().flat_map(|&(c, n)| at_tier(c, n)).collect()
    }

    #[test]
    fn tiers_are_listed_highest_priority_first() {
        let p = plan(
            &tiers(&[(QosClass::Basic, 1), (QosClass::Sync, 1), (QosClass::Integration, 1)]),
            None,
        );
        let order: Vec<QosClass> = p.tiers.iter().map(|t| t.class).collect();
        assert_eq!(order, vec![QosClass::Sync, QosClass::Integration, QosClass::Basic]);
        // Zero-count tiers omitted (testnet undeclared)
        assert!(!order.contains(&QosClass::Testnet));
    }

    #[test]
    fn total_is_sum_of_count_times_admitted() {
        // Admitted (components + runner): basic 2c/1Gi, integration 4c/4Gi
        // → 3 basic + 1 integration = 10c, 7Gi
        let p = plan(&tiers(&[(QosClass::Basic, 3), (QosClass::Integration, 1)]), None);
        assert_eq!(p.total.cpu_milli, 3 * 2000 + 4000);
        assert_eq!(p.total.mem_bytes, 3 * GIB + 4 * GIB);
    }

    #[test]
    fn fits_in_one_wave_when_total_within_capacity() {
        // 4 basic = 8c / 4 GiB against 8c/16Gi → one wave
        let p = plan(&at_tier(QosClass::Basic, 4), Some(Resources::new(8000, 16 * GIB, 0, 0)));
        assert_eq!(p.waves, 1);
        assert_eq!(p.peak, Resources::new(8000, 4 * GIB, 0, 0));
        assert!(p.unschedulable.is_empty());
    }

    #[test]
    fn spills_into_multiple_waves_when_total_exceeds_capacity() {
        // 5 × 4c/4Gi on 4c/8Gi: CPU-bound, 1 per wave → 5 waves
        let p = plan(&at_tier(QosClass::Integration, 5), Some(Resources::new(4000, 8 * GIB, 0, 0)));
        assert_eq!(p.waves, 5);
        assert_eq!(p.peak, Resources::new(4000, 4 * GIB, 0, 0));
    }

    #[test]
    fn unschedulable_test_is_flagged_with_its_own_reserve_and_excluded_from_waves() {
        // sync admits 16c/16Gi against 4c/8Gi → never fits; a basic (2c/1Gi) still
        // plans normally around it
        let p = plan(
            &tiers(&[(QosClass::Sync, 2), (QosClass::Basic, 1)]),
            Some(Resources::new(4000, 8 * GIB, 0, 0)),
        );
        assert_eq!(p.unschedulable.len(), 2);
        assert!(p.unschedulable.iter().all(|u| u.class == QosClass::Sync));
        // Amount travels with the rejection (class no longer determines it)
        assert_eq!(p.unschedulable[0].admitted, QosClass::Sync.profile().admitted());
        // Only the basic test entered the sim
        assert_eq!(p.waves, 1);
        assert_eq!(p.peak, Resources::new(2000, GIB, 0, 0));
    }

    #[test]
    fn no_capacity_degrades_to_counts_only() {
        let p = plan(&at_tier(QosClass::Testnet, 2), None);
        assert_eq!(p.waves, 0);
        assert_eq!(p.peak, Resources::ZERO);
        assert!(p.unschedulable.is_empty());
        // Counts/footprints still populated
        assert_eq!(p.tiers.len(), 1);
        assert_eq!(p.tiers[0].count, 2);
    }

    #[test]
    fn empty_input_is_an_empty_plan() {
        let p = plan(&[], Some(Resources::new(8000, 16 * GIB, 0, 0)));
        assert!(p.tiers.is_empty());
        assert_eq!(p.total, Resources::ZERO);
        assert_eq!(p.waves, 0);
    }

    // ── overrides: the reason this works per test rather than per tier ──

    #[test]
    fn a_tier_with_uniform_reserves_still_reports_a_per_test_figure() {
        let admitted = QosClass::Basic.profile().admitted();
        let p = plan(&at_tier(QosClass::Basic, 3), None);
        assert_eq!(p.tiers[0].per_test, Some(admitted));
        // Stated as a multiple of the figure above, not a literal: the two must agree, and
        // a hardcoded subtotal is what let them drift apart
        assert_eq!(p.tiers[0].subtotal.cpu_milli, admitted.cpu_milli * 3);
        assert_eq!(p.tiers[0].subtotal.mem_bytes, admitted.mem_bytes * 3);
    }

    #[test]
    fn an_override_makes_its_tier_non_uniform_and_still_sums_correctly() {
        // Two basic tests, one overriding its component reserve upward
        let base = QosClass::Basic.profile();
        let raised = base.with_footprint(Some(Resources::new(4_000, 4 * GIB, 0, 0)));
        let p = plan(
            &[
                PlannedTest { class: QosClass::Basic, admitted: base.admitted() },
                PlannedTest { class: QosClass::Basic, admitted: raised.admitted() },
            ],
            None,
        );
        assert_eq!(p.tiers.len(), 1, "still one tier row");
        assert_eq!(p.tiers[0].count, 2);
        assert_eq!(p.tiers[0].per_test, None, "mixed reserves have no honest `each` figure");
        assert_eq!(p.tiers[0].subtotal, base.admitted().saturating_add(&raised.admitted()));
        assert_eq!(p.total, p.tiers[0].subtotal);
    }

    #[test]
    fn an_override_can_make_one_test_unschedulable_while_its_tier_peer_plans() {
        let base = QosClass::Basic.profile();
        let huge = base.with_footprint(Some(Resources::new(64_000, 512 * GIB, 0, 0)));
        let p = plan(
            &[
                PlannedTest { class: QosClass::Basic, admitted: base.admitted() },
                PlannedTest { class: QosClass::Basic, admitted: huge.admitted() },
            ],
            Some(Resources::new(8_000, 16 * GIB, 0, 0)),
        );
        // Per-tier accounting would flag both or neither
        assert_eq!(p.unschedulable.len(), 1);
        assert_eq!(p.unschedulable[0].admitted, huge.admitted());
        assert_eq!(p.waves, 1, "the ordinary test still plans");
    }
}
