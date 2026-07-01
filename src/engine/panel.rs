//! Synthesize the live QoS panel inputs each frame from the engine's own
//! bookkeeping, with no cluster poll. The in-memory
//! [`Scheduler`](crate::qos::scheduler::Scheduler) is the ground truth, so this
//! is more accurate than the Lease-ledger summary.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::engine::events::RunStats;
use crate::engine::plan::WorkItem;
use crate::preflight::RunProgress;
use crate::qos::Resources;
use crate::qos::live::{LiveSnapshot, TierLive};

/// Fold the currently-running work-items into a [`LiveSnapshot`] for
/// [`render_live_panel`](crate::preflight::render::render_live_panel).
///
/// `committed` is the scheduler's authoritative committed total (equals the
/// sum of running footprints); `sa` is the single ServiceAccount of a local run.
pub fn live_snapshot<'a>(
    running: impl Iterator<Item = &'a WorkItem>,
    committed: Resources,
    sa: &str,
) -> LiveSnapshot {
    let mut tiers: BTreeMap<crate::qos::QosClass, TierLive> = BTreeMap::new();
    for item in running {
        let entry = tiers.entry(item.class).or_insert(TierLive {
            count: 0,
            reserve: Resources::ZERO,
        });
        entry.count += 1;
        entry.reserve = entry.reserve.saturating_add(&item.footprint);
    }

    let mut by_sa = BTreeMap::new();
    if committed != Resources::ZERO {
        by_sa.insert(sa.to_string(), committed);
    }

    LiveSnapshot {
        running: tiers,
        committed,
        by_sa,
    }
}

/// Build the [`RunProgress`] line inputs from the running tally.
pub fn run_progress(stats: RunStats, elapsed: Duration) -> RunProgress {
    RunProgress {
        elapsed,
        passed: stats.passed,
        failed: stats.failed,
        total: stats.total as u32,
    }
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
            footprint: p.footprint,
            priority: p.priority,
            hard_cap: p.hard_cap,
            retries: 0,
            deps: Vec::new(),
        }
    }

    #[test]
    fn folds_running_per_tier() {
        let running = [
            item(QosClass::Integration),
            item(QosClass::Integration),
            item(QosClass::Sync),
        ];
        let committed = Resources::new(2_000 * 2 + 16_000, 0); // cpu-only for the check
        let snap = live_snapshot(running.iter(), committed, "ztest-local");

        let integ = &snap.running[&QosClass::Integration];
        assert_eq!(integ.count, 2);
        assert_eq!(
            integ.reserve.cpu_milli,
            QosClass::Integration.profile().footprint.cpu_milli * 2
        );
        assert_eq!(snap.running[&QosClass::Sync].count, 1);
        assert_eq!(snap.committed, committed);
        assert_eq!(snap.by_sa["ztest-local"], committed);
    }

    #[test]
    fn empty_running_has_no_sa_entry() {
        let snap = live_snapshot(std::iter::empty(), Resources::ZERO, "sa");
        assert!(snap.running.is_empty());
        assert!(snap.by_sa.is_empty());
    }
}
