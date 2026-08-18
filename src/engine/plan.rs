//! Work-list: one [`WorkItem`] per selected test, joined with its declared QoS tier.
//! Pure, cluster-free function of the inventory + QoS dump.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::inventory::{QosEntry, SyncTestEntry};
use crate::pipeline::SelectedBinary;
use crate::qos::{QosClass, Resources};
use crate::resource::NodeId;

/// One schedulable (binary, test) pair with its resolved tier shape.
///
/// - `footprint` = tier's [`admitted`](crate::qos::QosProfile::admitted) total (components
///   + runner pod), so the scheduler accounts every pod the test places
/// - `deps` all `Ready` gates admission; one failed → `SkipReason::DependencyUnavailable`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkItem {
    pub binary_id: String,
    pub test_name: String,
    pub binary_path: PathBuf,
    pub cwd: PathBuf,
    pub class: QosClass,
    pub footprint: Resources,
    pub priority: u8,
    pub hard_cap: Duration,
    pub retries: u32,
    pub deps: Vec<NodeId>,
}

/// Resolved dependencies keyed for attachment to each [`WorkItem`]; default = depend on nothing.
///
/// - Images = binary-level edge (every test in a binary depends on all its `dev!` images)
/// - Seeds = per-test edge from `#[ztest::archive]` / `#[ztest::needs]`
#[derive(Debug, Default)]
pub struct ResourceDeps {
    pub images_by_binary: HashMap<String, Vec<NodeId>>,
    pub seeds_by_test: HashMap<(String, String), Vec<NodeId>>,
}

impl ResourceDeps {
    fn for_item(&self, binary_id: &str, test_name: &str) -> Vec<NodeId> {
        let mut deps = self.images_by_binary.get(binary_id).cloned().unwrap_or_default();
        if let Some(seeds) = self.seeds_by_test.get(&(binary_id.to_string(), test_name.to_string()))
        {
            deps.extend(seeds.iter().cloned());
        }
        deps
    }
}

/// Crate-rooted QoS `test_id` → libtest name (`qos_attr::marker_basic` → `marker_basic`).
/// Exact within a binary: the dump is per-binary, so segment 1 is always that crate
pub fn libtest_name(test_id: &str) -> &str {
    test_id.split_once("::").map_or(test_id, |(_crate, rest)| rest)
}

/// Tier declared for `test_name`, `None` if none.
///
/// - Exact match misses `rstest` cases (one libtest entry per case, `QosEntry` names only
///   the annotated fn) → walk off trailing `::` segments to the declaring ancestor
/// - Without it, cases fall to `Basic` and a fixture-restoring testnet test dies at Basic's
///   60s `hard_cap`, looking like a product hang
fn declared_qos<'a>(
    by_name: &HashMap<&str, &'a QosEntry>,
    test_name: &str,
) -> Option<&'a QosEntry> {
    if let Some(entry) = by_name.get(test_name) {
        return Some(entry);
    }
    // One generated segment per step; first declaring ancestor owns this case. Declared
    // nothing → walks to root, `None`, Basic default preserved
    let mut rest = test_name;
    while let Some((parent, _)) = rest.rsplit_once("::") {
        if let Some(entry) = by_name.get(parent) {
            return Some(entry);
        }
        rest = parent;
    }
    None
}

/// Why a `sync`-tier test left a `ztest run` selection. `TierOnly` = tier declared with no
/// profile behind it, so nothing can run it (tier exists only for the detached lifecycle)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncExclusion {
    Profile(String),
    TierOnly,
}

/// Sync-tier test removed from the run's selection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludedSync {
    pub binary_id: String,
    pub test_name: String,
    pub reason: SyncExclusion,
}

/// Subtract every sync-tier test from a `ztest run` selection, returning what was dropped.
///
/// - Empty binaries removed (preserves [`summarize_selection`](crate::pipeline::build)'s
///   ≥1-test invariant)
/// - Both forms caught: `#[ztest::sync_test]` compiles to a plain `#[tokio::test]` that
///   `nextest list` matches; bare `#[ztest::qos::sync]` declares the tier with no profile
/// - Either admitted parks a 48h top-priority item + a panel row the engine never launches
/// - Binary-scoped, via [`declared_tier`] so parameterized cases leave with their parent
pub fn drop_sync_tests(
    selected: &mut Vec<SelectedBinary>,
    sync_by_binary: &[(String, Vec<SyncTestEntry>)],
    qos_by_binary: &[(String, Vec<QosEntry>)],
) -> Vec<ExcludedSync> {
    let profiles: HashMap<&str, HashMap<&str, &str>> = sync_by_binary
        .iter()
        .map(|(binary_id, entries)| {
            let by_name =
                entries.iter().map(|e| (libtest_name(&e.test_id), e.name.as_str())).collect();
            (binary_id.as_str(), by_name)
        })
        .collect();
    let tiers = tiers_by_binary(qos_by_binary);

    let mut excluded = Vec::new();
    for bin in selected.iter_mut() {
        let bin_profiles = profiles.get(bin.binary_id.as_str());
        let bin_tiers = tiers.get(bin.binary_id.as_str());
        bin.selected_tests.retain(|test_name| {
            let reason = match bin_profiles.and_then(|p| p.get(test_name.as_str())) {
                Some(profile) => Some(SyncExclusion::Profile((*profile).to_string())),
                None => bin_tiers
                    .and_then(|t| declared_qos(t, test_name))
                    .filter(|e| e.class == QosClass::Sync)
                    .map(|_| SyncExclusion::TierOnly),
            };
            match reason {
                Some(reason) => {
                    excluded.push(ExcludedSync {
                        binary_id: bin.binary_id.clone(),
                        test_name: test_name.clone(),
                        reason,
                    });
                    false
                }
                None => true,
            }
        });
    }
    selected.retain(|bin| !bin.selected_tests.is_empty());
    excluded
}

/// QoS dump indexed for [`declared_qos`]: `binary_id` → libtest name → declaration
///
/// - Whole entry, not just class (dropping the override here = every item at tier default)
fn tiers_by_binary(
    qos_by_binary: &[(String, Vec<QosEntry>)],
) -> HashMap<&str, HashMap<&str, &QosEntry>> {
    qos_by_binary
        .iter()
        .map(|(binary_id, entries)| {
            let by_name = entries.iter().map(|e| (libtest_name(&e.test_id), e)).collect();
            (binary_id.as_str(), by_name)
        })
        .collect()
}

/// Undeclared → [`QosClass::Basic`], matching `qos::current`; `retries` applied uniformly
pub fn build_work_list(
    selected_binaries: &[SelectedBinary],
    qos_by_binary: &[(String, Vec<QosEntry>)],
    retries: u32,
    deps: &ResourceDeps,
) -> Vec<WorkItem> {
    let tiers = tiers_by_binary(qos_by_binary);

    let mut items: Vec<WorkItem> = Vec::new();
    for bin in selected_binaries {
        let bin_tiers = tiers.get(bin.binary_id.as_str());
        for test_name in &bin.selected_tests {
            // Undeclared → `Basic`, matching `qos::current`; declared → effective profile
            // (override must reach the scheduler request, not flatten to the tier default)
            let declared = bin_tiers.and_then(|m| declared_qos(m, test_name.as_str()));
            let class = declared.map_or(QosClass::Basic, |e| e.class);
            let profile = declared.map_or_else(|| QosClass::Basic.profile(), |e| e.profile());
            let item_deps = deps.for_item(&bin.binary_id, test_name);
            items.push(WorkItem {
                binary_id: bin.binary_id.clone(),
                test_name: test_name.clone(),
                binary_path: bin.binary_path.clone(),
                cwd: bin.cwd.clone(),
                class,
                footprint: profile.admitted(),
                priority: profile.priority,
                hard_cap: profile.hard_cap,
                retries,
                deps: item_deps,
            });
        }
    }

    sort_for_admission(&mut items);
    items
}

/// Submission order: priority desc, then smallest footprint, stable id tiebreak.
/// [`Scheduler`](crate::qos::scheduler::Scheduler) re-sorts by `(priority desc, seq asc)`
/// → this governs only the seq tiebreak
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
        QosEntry { test_id: test_id.to_string(), class, footprint: None }
    }

    fn sync_entry(test_id: &str, name: &str) -> SyncTestEntry {
        SyncTestEntry {
            test_id: test_id.to_string(),
            name: name.to_string(),
            description: String::new(),
            subject: "wallet".to_string(),
            timeout: "48h".to_string(),
            qos: "sync".to_string(),
            footprint: None,
            tags: Vec::new(),
        }
    }

    #[test]
    fn sync_profiles_leave_the_selection() {
        let mut bins =
            vec![bin("zaino-sync-tests::zaino_sync", &["ordinary_test", "zaino_state_sync"])];
        let syncs = [(
            "zaino-sync-tests::zaino_sync".to_string(),
            vec![sync_entry("zaino_sync::zaino_state_sync", "zaino_state_sync")],
        )];

        let excluded = drop_sync_tests(&mut bins, &syncs, &[]);

        assert_eq!(bins[0].selected_tests, ["ordinary_test"]);
        assert_eq!(excluded.len(), 1);
        assert_eq!(excluded[0].test_name, "zaino_state_sync");
        assert_eq!(excluded[0].reason, SyncExclusion::Profile("zaino_state_sync".to_string()));
        assert!(
            build_work_list(&bins, &[], 0, &ResourceDeps::default())
                .iter()
                .all(|w| w.test_name != "zaino_state_sync")
        );
    }

    #[test]
    fn exclusion_is_binary_scoped() {
        let mut bins = vec![bin("pkg::syncs", &["state_sync"]), bin("pkg::unit", &["state_sync"])];
        let syncs =
            [("pkg::syncs".to_string(), vec![sync_entry("syncs::state_sync", "state_sync")])];

        let excluded = drop_sync_tests(&mut bins, &syncs, &[]);

        // Same-named ordinary test in another binary survives; emptied binary dropped
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].binary_id, "pkg::unit");
        assert_eq!(bins[0].selected_tests, ["state_sync"]);
        assert_eq!(excluded.len(), 1);
        assert_eq!(excluded[0].binary_id, "pkg::syncs");
    }

    #[test]
    fn selection_without_sync_tests_is_untouched() {
        let mut bins = vec![bin("pkg::b", &["a", "b"])];
        let qos = [(
            "pkg::b".to_string(),
            vec![entry("b::a", QosClass::Basic), entry("b::b", QosClass::Testnet)],
        )];
        assert!(drop_sync_tests(&mut bins, &[], &qos).is_empty());
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].selected_tests, ["a", "b"]);
    }

    /// - Bare `#[ztest::qos::sync]` = no profile → profile-only pruning left it in, putting
    ///   a `sync` panel row on work the engine never launches
    /// - Parameterized cases leave with their parent
    #[test]
    fn sync_tier_tests_leave_even_without_a_profile() {
        let mut bins = vec![bin(
            "ztest::qos_attr",
            &[
                "marker_basic",
                "marker_sync",
                "parameterized_sync::case_1",
                "parameterized_sync::case_2",
            ],
        )];
        let qos = [(
            "ztest::qos_attr".to_string(),
            vec![
                entry("qos_attr::marker_basic", QosClass::Basic),
                entry("qos_attr::marker_sync", QosClass::Sync),
                entry("qos_attr::parameterized_sync", QosClass::Sync),
            ],
        )];

        let excluded = drop_sync_tests(&mut bins, &[], &qos);

        assert_eq!(bins[0].selected_tests, ["marker_basic"]);
        assert_eq!(excluded.len(), 3);
        assert!(excluded.iter().all(|e| e.reason == SyncExclusion::TierOnly));
        assert!(
            build_work_list(&bins, &qos, 0, &ResourceDeps::default())
                .iter()
                .all(|w| w.class != QosClass::Sync)
        );
    }

    #[test]
    fn libtest_name_strips_crate_segment() {
        assert_eq!(libtest_name("qos_attr::marker_basic"), "marker_basic");
        assert_eq!(libtest_name("mycrate::mod::deep::t"), "mod::deep::t");
        // No `::` → as-is
        assert_eq!(libtest_name("bare"), "bare");
    }

    /// `rstest` emits one entry per case, the QoS attribute submits only the parent id →
    /// exact matching dropped every case to `Basic`, whose 60s `hard_cap` killed
    /// fixture-restoring testnet tests, indistinguishable from a product hang
    #[test]
    fn parameterized_cases_inherit_the_tier_declared_on_their_parent() {
        let bins = [bin(
            "clientless::state_service",
            &[
                "zebra::get::z::subtrees_by_index_testnet::case_1_sapling",
                "zebra::get::z::subtrees_by_index_testnet::case_2_orchard",
            ],
        )];
        let qos = [(
            "clientless::state_service".to_string(),
            // Attribute names the function, never the generated cases
            vec![entry(
                "state_service::zebra::get::z::subtrees_by_index_testnet",
                QosClass::Testnet,
            )],
        )];
        let items = build_work_list(&bins, &qos, 0, &ResourceDeps::default());
        assert_eq!(items.len(), 2);
        for item in &items {
            assert_eq!(
                item.class,
                QosClass::Testnet,
                "{} must inherit its parent's tier, not fall back to Basic",
                item.test_name
            );
            assert_eq!(item.hard_cap, QosClass::Testnet.profile().hard_cap);
        }
    }

    /// Fallback survives the parent walk: declared-nothing stays `Basic`, so the lookup is
    /// not "inherit from any ancestor at all"
    #[test]
    fn a_test_declaring_no_tier_still_defaults_to_basic() {
        let bins = [bin("pkg::b", &["some::module::undeclared_test"])];
        let qos =
            [("pkg::b".to_string(), vec![entry("b::other::declared_test", QosClass::Testnet)])];
        let items = build_work_list(&bins, &qos, 0, &ResourceDeps::default());
        assert_eq!(items[0].class, QosClass::Basic);
    }

    #[test]
    fn joins_tier_by_stripped_test_id() {
        let bins = [bin("ztest::qos_attr", &["marker_basic", "marker_sync"])];
        let qos = [(
            "ztest::qos_attr".to_string(),
            vec![
                // dump test_ids are crate-rooted
                entry("qos_attr::marker_basic", QosClass::Basic),
                entry("qos_attr::marker_sync", QosClass::Sync),
            ],
        )];
        let items = build_work_list(&bins, &qos, 0, &ResourceDeps::default());
        let by_name: HashMap<_, _> = items.iter().map(|w| (w.test_name.as_str(), w)).collect();
        assert_eq!(by_name["marker_basic"].class, QosClass::Basic);
        assert_eq!(by_name["marker_sync"].class, QosClass::Sync);
        // Reserves the whole admitted total (components + runner), so the runner is accounted
        assert_eq!(by_name["marker_sync"].footprint, QosClass::Sync.profile().admitted());
    }

    #[test]
    fn undeclared_tests_default_to_basic() {
        let bins = [bin("pkg::b", &["lonely"])];
        let items = build_work_list(&bins, &[], 2, &ResourceDeps::default());
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
        let items = build_work_list(&bins, &qos, 0, &ResourceDeps::default());
        // Highest priority (Sync) first
        assert_eq!(items[0].test_name, "s");
        assert_eq!(items[1].test_name, "y");
        assert_eq!(items[2].test_name, "i");
    }
}
