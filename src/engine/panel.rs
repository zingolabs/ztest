//! Live QoS panel inputs, synthesized per frame from the in-memory
//! [`Scheduler`](crate::qos::scheduler::Scheduler)'s own bookkeeping — no cluster poll

use std::time::Duration;

use super::RunProgress;
use crate::engine::events::RunStats;
use crate::qos::Resources;
use crate::qos::live::LiveSnapshot;

/// - `queued` derived, not counted (neither finished nor running = waiting, wherever it sits),
///   the same fold the lease beacon publishes
/// - `committed` of `limit` = the scheduler's own committed total against its ceiling
pub fn live_snapshot(
    running: usize,
    stats: RunStats,
    committed: Resources,
    limit: Resources,
) -> LiveSnapshot {
    let running = running as u32;
    let queued = (stats.total as u32).saturating_sub(stats.finished()).saturating_sub(running);
    LiveSnapshot { running, queued, committed, limit }
}

pub fn run_progress(stats: RunStats, elapsed: Duration) -> RunProgress {
    RunProgress { elapsed, passed: stats.passed, failed: stats.failed, total: stats.total as u32 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_is_what_has_neither_finished_nor_started() {
        let stats = RunStats { passed: 3, failed: 1, skipped: 1, total: 12, ..RunStats::default() };
        let (committed, limit) = (Resources::new(6_000, 0, 0, 0), Resources::new(8_000, 0, 0, 0));
        let snap = live_snapshot(2, stats, committed, limit);
        assert_eq!(snap, LiveSnapshot { running: 2, queued: 5, committed, limit });
    }

    #[test]
    fn nothing_selected_folds_to_an_empty_snapshot() {
        assert_eq!(
            live_snapshot(0, RunStats::default(), Resources::ZERO, Resources::ZERO),
            LiveSnapshot::default()
        );
    }
}
