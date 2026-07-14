//! Live during-run snapshot of QoS admission (`docs/qos-design.md` §8, the
//! live half).
//!
//! Plain data: the [`Scheduler`](super::scheduler::Scheduler) is the ground
//! truth for what's running, so `engine::panel` folds its live leases into a
//! [`LiveSnapshot`] and the run panel renders it. This module owns only the
//! summary types, unit-testable without a cluster or scheduler.

use std::collections::BTreeMap;

use super::{QosClass, Resources};

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
