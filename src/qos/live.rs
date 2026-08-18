//! Live during-run snapshot of QoS admission (`docs/design-qos.md` §8).
//!
//! Summary types only: `engine::panel` folds
//! [`Scheduler`](super::scheduler::Scheduler) leases into a [`LiveSnapshot`] to render

use std::collections::BTreeMap;

use super::{QosClass, Resources};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TierLive {
    pub count: u32,
    pub reserve: Resources,
}

/// Point-in-time view of the reservation ledger
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveSnapshot {
    pub running: BTreeMap<QosClass, TierLive>,
    pub committed: Resources,
}

/// Group running work by tier. Sole fold — the live panel walks `WorkItem`s and
/// `ztest status` walks decoded `RunningTest`s, and the two must not be able to disagree
/// about what is running
pub fn tier_tally(
    running: impl IntoIterator<Item = (QosClass, Resources)>,
) -> BTreeMap<QosClass, TierLive> {
    let mut tiers: BTreeMap<QosClass, TierLive> = BTreeMap::new();
    for (class, footprint) in running {
        let e = tiers.entry(class).or_default();
        e.count += 1;
        e.reserve = e.reserve.saturating_add(&footprint);
    }
    tiers
}

impl LiveSnapshot {
    pub fn total_running(&self) -> u32 {
        self.running.values().map(|t| t.count).sum()
    }
}
