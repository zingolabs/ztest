//! The work-list: one schedulable [`WorkItem`] per selected test, joined with
//! its declared QoS tier (footprint / priority / hard cap).
//!
//! Pure and cluster-free — a deterministic function of the inventory + QoS dump,
//! unit-tested with fixtures.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::inventory::QosEntry;
use crate::pipeline::SelectedBinary;
use crate::qos::{QosClass, Resources};

/// One schedulable test: a (binary, test) pair with its resolved tier shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkItem {
    /// Nextest's `<package>::<bin>` identifier.
    pub binary_id: String,
    /// The libtest test name (the `--exact` target).
    pub test_name: String,
    /// Absolute path to the test binary.
    pub binary_path: PathBuf,
    /// Working directory to run the binary in.
    pub cwd: PathBuf,
    /// Declared tier (defaults to [`QosClass::Basic`] when undeclared).
    pub class: QosClass,
    /// Per-test reserve packed against cluster capacity.
    pub footprint: Resources,
    /// Scheduling priority (higher admitted first).
    pub priority: u8,
    /// Hard execution cap — the kill deadline.
    pub hard_cap: Duration,
    /// Max retry attempts on failure (0 = run once).
    pub retries: u32,
}

/// Strip the leading crate segment from a QoS `test_id` to recover the libtest
/// test name. `qos_attr::marker_basic` → `marker_basic`; `crate::m::t` → `m::t`.
///
/// Verified against a built binary: `test_id` is `concat!(module_path!(),"::",fn)`
/// (crate-rooted) while nextest's `selected_tests` are the crate-relative libtest
/// names. Because the QoS dump is already grouped per binary, the first
/// `::`-segment is always that binary's crate — the strip is exact and
/// unambiguous within a binary.
fn libtest_name(test_id: &str) -> &str {
    test_id.split_once("::").map_or(test_id, |(_crate, rest)| rest)
}

/// Build the work-list from the selected binaries and the per-binary QoS dump.
///
/// Tests without a QoS declaration default to [`QosClass::Basic`] (matching the
/// in-test default at `qos::current`). `retries` is applied uniformly from the
/// run options.
pub fn build_work_list(
    selected_binaries: &[SelectedBinary],
    qos_by_binary: &[(String, Vec<QosEntry>)],
    retries: u32,
) -> Vec<WorkItem> {
    // binary_id -> (libtest_name -> class), built once.
    let tiers: HashMap<&str, HashMap<&str, QosClass>> = qos_by_binary
        .iter()
        .map(|(binary_id, entries)| {
            let by_name = entries
                .iter()
                .map(|e| (libtest_name(&e.test_id), e.class))
                .collect();
            (binary_id.as_str(), by_name)
        })
        .collect();

    let mut items: Vec<WorkItem> = Vec::new();
    for bin in selected_binaries {
        let bin_tiers = tiers.get(bin.binary_id.as_str());
        for test_name in &bin.selected_tests {
            let class = bin_tiers
                .and_then(|m| m.get(test_name.as_str()).copied())
                .unwrap_or(QosClass::Basic);
            let profile = class.profile();
            items.push(WorkItem {
                binary_id: bin.binary_id.clone(),
                test_name: test_name.clone(),
                binary_path: bin.binary_path.clone(),
                cwd: bin.cwd.clone(),
                class,
                footprint: profile.footprint,
                priority: profile.priority,
                hard_cap: profile.hard_cap,
                retries,
            });
        }
    }

    sort_for_admission(&mut items);
    items
}

/// Order the work-list for request submission: highest priority first, then
/// smallest footprint first within a priority (so small tests pack into the
/// initial capacity and large ones backfill as room appears), with a stable
/// id tiebreak. The [`Scheduler`](crate::qos::scheduler::Scheduler) re-sorts by
/// `(priority desc, seq asc)`, so this only governs the seq tiebreak.
fn sort_for_admission(items: &mut [WorkItem]) {
    items.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then(a.footprint.cpu_milli.cmp(&b.footprint.cpu_milli))
            .then(a.footprint.mem_bytes.cmp(&b.footprint.mem_bytes))
            .then(a.binary_id.cmp(&b.binary_id))
            .then(a.test_name.cmp(&b.test_name))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bin(id: &str, tests: &[&str]) -> SelectedBinary {
        SelectedBinary {
            binary_path: PathBuf::from(format!("/t/{id}")),
            cwd: PathBuf::from("/t"),
            binary_id: id.to_string(),
            selected_tests: tests.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn entry(test_id: &str, class: QosClass) -> QosEntry {
        QosEntry {
            test_id: test_id.to_string(),
            class,
        }
    }

    #[test]
    fn libtest_name_strips_crate_segment() {
        assert_eq!(libtest_name("qos_attr::marker_basic"), "marker_basic");
        assert_eq!(libtest_name("mycrate::mod::deep::t"), "mod::deep::t");
        // No `::` — return as-is (defensive).
        assert_eq!(libtest_name("bare"), "bare");
    }

    #[test]
    fn joins_tier_by_stripped_test_id() {
        let bins = [bin("ztest::qos_attr", &["marker_basic", "marker_sync"])];
        let qos = [(
            "ztest::qos_attr".to_string(),
            vec![
                // dump test_ids are crate-rooted ("qos_attr::...")
                entry("qos_attr::marker_basic", QosClass::Basic),
                entry("qos_attr::marker_sync", QosClass::Sync),
            ],
        )];
        let items = build_work_list(&bins, &qos, 0);
        let by_name: HashMap<_, _> = items.iter().map(|w| (w.test_name.as_str(), w)).collect();
        assert_eq!(by_name["marker_basic"].class, QosClass::Basic);
        assert_eq!(by_name["marker_sync"].class, QosClass::Sync);
        assert_eq!(
            by_name["marker_sync"].footprint,
            QosClass::Sync.profile().footprint
        );
    }

    #[test]
    fn undeclared_tests_default_to_basic() {
        let bins = [bin("pkg::b", &["lonely"])];
        let items = build_work_list(&bins, &[], 2);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].class, QosClass::Basic);
        assert_eq!(items[0].retries, 2);
    }

    #[test]
    fn sorted_high_priority_then_smallest_first() {
        let bins = [bin("pkg::b", &["s", "i", "y"])];
        let qos = [(
            "pkg::b".to_string(),
            vec![
                entry("pkg::s", QosClass::Sync),        // priority 3
                entry("pkg::i", QosClass::Integration), // priority 1
                entry("pkg::y", QosClass::Testnet),     // priority 2
            ],
        )];
        let items = build_work_list(&bins, &qos, 0);
        // Highest priority (Sync) first.
        assert_eq!(items[0].test_name, "s");
        assert_eq!(items[1].test_name, "y");
        assert_eq!(items[2].test_name, "i");
    }
}
