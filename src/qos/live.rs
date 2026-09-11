//! Live during-run snapshot of QoS admission: what runs, what waits, what is held of what.
//!
//! Folded per frame by `engine::panel` from the [`Scheduler`](super::scheduler::Scheduler)'s
//! own bookkeeping. `limit` always known here (a run never starts without a probed ceiling);
//! only preflight can lack capacity, typed there as an `Option`

use super::Resources;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LiveSnapshot {
    pub running: u32,
    pub queued: u32,
    pub committed: Resources,
    pub limit: Resources,
}
