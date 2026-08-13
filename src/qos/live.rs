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
    pub by_sa: BTreeMap<String, Resources>,
}

impl LiveSnapshot {
    pub fn total_running(&self) -> u32 {
        self.running.values().map(|t| t.count).sum()
    }
}
