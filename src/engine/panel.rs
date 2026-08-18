//! Live QoS panel inputs, synthesized per frame from the in-memory
//! [`Scheduler`](crate::qos::scheduler::Scheduler)'s own bookkeeping — no cluster poll

use std::time::Duration;

use super::RunProgress;
use crate::engine::events::RunStats;
use crate::engine::plan::WorkItem;
use crate::qos::Resources;
use crate::qos::live::{LiveSnapshot, tier_tally};

/// `committed` = the scheduler's committed total (Σ of the running footprints)
pub fn live_snapshot<'a>(
    running: impl Iterator<Item = &'a WorkItem>,
    committed: Resources,
) -> LiveSnapshot {
    LiveSnapshot { running: tier_tally(running.map(|i| (i.class, i.footprint))), committed }
}

pub fn run_progress(stats: RunStats, elapsed: Duration) -> RunProgress {
    RunProgress { elapsed, passed: stats.passed, failed: stats.failed, total: stats.total as u32 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::QosClass;
    use std::path::PathBuf;

    fn item(class: QosClass) -> WorkItem {
        let p = class.profile();
        WorkItem {
            binary_id: "pkg::b".into(),
            test_name: "t".into(),
            binary_path: PathBuf::from("/t"),
            cwd: PathBuf::from("/t"),
            class,
            footprint: p.admitted(),
            priority: p.priority,
            hard_cap: p.hard_cap,
            retries: 0,
            deps: Vec::new(),
        }
    }

    #[test]
    fn folds_running_per_tier() {
        let running =
            [item(QosClass::Integration), item(QosClass::Integration), item(QosClass::Sync)];
        // Independent echo value for the committed check (cpu-only)
        let committed = Resources::new(2_000 * 2 + 16_000, 0, 0, 0);
        let snap = live_snapshot(running.iter(), committed);

        let integ = &snap.running[&QosClass::Integration];
        assert_eq!(integ.count, 2);
        // Per-tier reserve folds each item's admitted total (components + runner)
        assert_eq!(
            integ.reserve.cpu_milli,
            QosClass::Integration.profile().admitted().cpu_milli * 2
        );
        assert_eq!(snap.running[&QosClass::Sync].count, 1);
        assert_eq!(snap.committed, committed);
    }

    #[test]
    fn nothing_running_folds_to_an_empty_tally() {
        let snap = live_snapshot(std::iter::empty(), Resources::ZERO);
        assert!(snap.running.is_empty());
        assert_eq!(snap.committed, Resources::ZERO);
    }
}
