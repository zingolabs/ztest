//! Pre-run scheduling **plan** for the preflight banner (`docs/qos-design.md`
//! §8, planning half).
//!
//! Distinct from [`super::scheduler`] (one letter apart, on purpose-adjacent):
//! - `scheduler` is the broker's *live, authoritative* admission decision core
//!   (one [`Request`](super::scheduler::Request) → fit/queue/reject).
//! - `schedule` (this module) is a *static estimate* shown before the run: given
//!   the selected tests grouped by tier and the probed cluster capacity, how
//!   many concurrency **waves** will it take, what's the peak reserve, and does
//!   any tier's footprint exceed the cluster outright (fail-fast)?
//!
//! It is pure — `(tier counts, capacity) → plan` — so it's unit-testable
//! without a cluster, and it consumes only what the `ztest run` parent already
//! has in hand (the QoS inventory dump + the cluster probe). No I/O, no ledger.

use std::collections::BTreeMap;

use super::{QosClass, Resources};

/// One tier's contribution to the plan: how many selected tests declared it,
/// and the per-test reserve ([`QosClass::profile`]'s footprint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierPlan {
    pub class: QosClass,
    pub count: u32,
    pub footprint: Resources,
}

/// The estimated schedule for the selected test set against probed capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QosPlan {
    /// Per declared tier, highest priority first (`sync` → `basic`).
    pub tiers: Vec<TierPlan>,
    /// Σ over tiers of `count · footprint` — the reserve if everything ran at
    /// once.
    pub total: Resources,
    /// Probed free capacity the plan was computed against; `None` when the
    /// cluster probe was unavailable (then `waves`/`peak` are `0`/`ZERO` and
    /// the display shows counts only).
    pub free: Option<Resources>,
    /// Greedy priority+backfill wave count to drain all *schedulable* tests.
    /// `0` when capacity is unknown.
    pub waves: u32,
    /// Per-dimension high-water reserve across waves (concurrency peak).
    pub peak: Resources,
    /// Tiers whose single footprint doesn't fit even the empty cluster — they
    /// will be **rejected** at admission (the broker's `ExceedsClusterCapacity`),
    /// surfaced here so the operator sees it before launching.
    pub unschedulable: Vec<QosClass>,
}

/// `fp · n`, saturating per dimension.
fn scaled(fp: Resources, n: u32) -> Resources {
    Resources::new(
        fp.cpu_milli.saturating_mul(n as u64),
        fp.mem_bytes.saturating_mul(n as u64),
    )
}

/// Compute the schedule estimate. `tier_counts` is the number of selected tests
/// per declared tier; `free` is the probed cluster headroom (or `None`).
pub fn plan(tier_counts: &BTreeMap<QosClass, u32>, free: Option<Resources>) -> QosPlan {
    // Highest priority first (sync, testnet, integration, basic) — both for
    // display and so the wave sim admits high-priority tests first.
    let mut tiers: Vec<TierPlan> = tier_counts
        .iter()
        .filter(|(_, n)| **n > 0)
        .map(|(&class, &count)| TierPlan {
            class,
            count,
            footprint: class.profile().footprint,
        })
        .collect();
    tiers.sort_by_key(|t| std::cmp::Reverse(t.class.profile().priority));

    let total = tiers
        .iter()
        .fold(Resources::ZERO, |acc, t| acc.saturating_add(&scaled(t.footprint, t.count)));

    let mut unschedulable = Vec::new();
    let (waves, peak) = match free {
        None => (0, Resources::ZERO),
        Some(free) => {
            // Expand schedulable tests to a priority-ordered footprint list;
            // a tier whose single footprint can't fit the empty cluster is
            // unschedulable (will be rejected) and excluded from the sim.
            let mut units: Vec<Resources> = Vec::new();
            for t in &tiers {
                if t.footprint.fits_within(&free) {
                    units.extend(std::iter::repeat_n(t.footprint, t.count as usize));
                } else {
                    unschedulable.push(t.class);
                }
            }
            simulate_waves(&units, free)
        }
    };

    QosPlan {
        tiers,
        total,
        free,
        waves,
        peak,
        unschedulable,
    }
}

/// Greedy priority+backfill wave simulation mirroring `scheduler`'s policy:
/// `units` is priority-ordered; each wave walks the remaining list admitting
/// every test that still fits the wave's 2-D capacity, the rest spill to the
/// next wave. Returns `(wave_count, per-dimension peak reserve)`.
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
        // Every unit individually fits `free` (unschedulable were filtered),
        // so each wave admits ≥1 and `remaining` strictly shrinks; the guard
        // is purely defensive against a future regression.
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

    fn counts(pairs: &[(QosClass, u32)]) -> BTreeMap<QosClass, u32> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn tiers_are_listed_highest_priority_first() {
        let p = plan(
            &counts(&[
                (QosClass::Basic, 1),
                (QosClass::Sync, 1),
                (QosClass::Integration, 1),
            ]),
            None,
        );
        let order: Vec<QosClass> = p.tiers.iter().map(|t| t.class).collect();
        assert_eq!(order, vec![QosClass::Sync, QosClass::Integration, QosClass::Basic]);
        // Zero-count tiers are omitted (testnet wasn't declared).
        assert!(!order.contains(&QosClass::Testnet));
    }

    #[test]
    fn total_is_sum_of_count_times_footprint() {
        // 3 basic (500m/512Mi) + 1 integration (2000m/2Gi).
        let p = plan(
            &counts(&[(QosClass::Basic, 3), (QosClass::Integration, 1)]),
            None,
        );
        assert_eq!(p.total.cpu_milli, 3 * 500 + 2000);
        assert_eq!(p.total.mem_bytes, 3 * 512 * crate::qos::MIB + 2 * GIB);
    }

    #[test]
    fn fits_in_one_wave_when_total_within_capacity() {
        // 4 basic = 2 cores / 2 GiB total; cluster has 8/16 → one wave.
        let p = plan(
            &counts(&[(QosClass::Basic, 4)]),
            Some(Resources::new(8000, 16 * GIB)),
        );
        assert_eq!(p.waves, 1);
        assert_eq!(p.peak, Resources::new(2000, 2 * GIB));
        assert!(p.unschedulable.is_empty());
    }

    #[test]
    fn spills_into_multiple_waves_when_total_exceeds_capacity() {
        // 5 integration (2 cores / 2 GiB each) on a 4-core / 8-GiB cluster:
        // CPU-bound → 2 fit per wave → ceil(5/2) = 3 waves.
        let p = plan(
            &counts(&[(QosClass::Integration, 5)]),
            Some(Resources::new(4000, 8 * GIB)),
        );
        assert_eq!(p.waves, 3);
        assert_eq!(p.peak, Resources::new(4000, 4 * GIB));
    }

    #[test]
    fn unschedulable_tier_is_flagged_and_excluded_from_waves() {
        // sync needs 8 cores / 16 GiB; cluster has 4/8 → can never fit.
        // A schedulable basic still plans normally around it.
        let p = plan(
            &counts(&[(QosClass::Sync, 2), (QosClass::Basic, 1)]),
            Some(Resources::new(4000, 8 * GIB)),
        );
        assert_eq!(p.unschedulable, vec![QosClass::Sync]);
        // Only the basic test entered the wave sim.
        assert_eq!(p.waves, 1);
        assert_eq!(p.peak, Resources::new(500, 512 * crate::qos::MIB));
    }

    #[test]
    fn no_capacity_degrades_to_counts_only() {
        let p = plan(&counts(&[(QosClass::Testnet, 2)]), None);
        assert_eq!(p.waves, 0);
        assert_eq!(p.peak, Resources::ZERO);
        assert!(p.unschedulable.is_empty());
        // Counts/footprints are still populated.
        assert_eq!(p.tiers.len(), 1);
        assert_eq!(p.tiers[0].count, 2);
    }

    #[test]
    fn empty_input_is_an_empty_plan() {
        let p = plan(&BTreeMap::new(), Some(Resources::new(8000, 16 * GIB)));
        assert!(p.tiers.is_empty());
        assert_eq!(p.total, Resources::ZERO);
        assert_eq!(p.waves, 0);
    }
}
