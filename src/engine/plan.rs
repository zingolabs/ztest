//! The work-list: one schedulable [`WorkItem`] per selected test, joined with its
//! declared QoS tier. A pure, cluster-free function of the inventory and QoS dump.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::inventory::{QosEntry, SyncTestEntry};
use crate::pipeline::SelectedBinary;
use crate::qos::{QosClass, Resources};
use crate::resource::NodeId;

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
    /// Per-test reserve packed against cluster capacity: the tier's
    /// [`admitted`](crate::qos::QosProfile::admitted) total (component pods plus
    /// the runner pod), so the scheduler accounts every pod the test places.
    pub footprint: Resources,
    /// Scheduling priority (higher admitted first).
    pub priority: u8,
    /// Hard execution cap — the kill deadline.
    pub hard_cap: Duration,
    /// Max retry attempts on failure (0 = run once).
    pub retries: u32,
    /// Resource nodes this test needs before it can run: its binary's images
    /// (binary-level edge) plus any `#[ztest::archive]`/`#[needs]` seeds (per-test
    /// edge). The run loop gates admission on these being `Ready` and skips the
    /// test (`SkipReason::DependencyUnavailable`) if one failed.
    pub deps: Vec<NodeId>,
}

/// Resolved resource dependencies, keyed for attachment to each [`WorkItem`].
/// Built by `cli::run`; the default (empty) makes every item depend on nothing.
#[derive(Debug, Default)]
pub struct ResourceDeps {
    /// `binary_id` → image node ids. The binary-level edge: every test in a
    /// binary depends on all images that binary declares (`dev!`).
    pub images_by_binary: HashMap<String, Vec<NodeId>>,
    /// `(binary_id, libtest_name)` → seed node ids. The per-test edge from
    /// `#[ztest::archive]` / `#[ztest::needs]`.
    pub seeds_by_test: HashMap<(String, String), Vec<NodeId>>,
}

impl ResourceDeps {
    /// The dependency node ids for one work item: its binary's images unioned
    /// with its own declared seeds.
    fn for_item(&self, binary_id: &str, test_name: &str) -> Vec<NodeId> {
        let mut deps = self
            .images_by_binary
            .get(binary_id)
            .cloned()
            .unwrap_or_default();
        if let Some(seeds) = self
            .seeds_by_test
            .get(&(binary_id.to_string(), test_name.to_string()))
        {
            deps.extend(seeds.iter().cloned());
        }
        deps
    }
}

/// Strip the leading crate segment from a crate-rooted QoS `test_id` to recover
/// the crate-relative libtest name (`qos_attr::marker_basic` → `marker_basic`).
/// The dump is grouped per binary, so the first `::`-segment is always that
/// binary's crate — the strip is exact and unambiguous within a binary.
pub(crate) fn libtest_name(test_id: &str) -> &str {
    test_id
        .split_once("::")
        .map_or(test_id, |(_crate, rest)| rest)
}

/// The tier declared for `test_name`, or `None` if the test declared none.
///
/// An exact hit is the common case. A *parameterized* test is not: `rstest`
/// expands one `#[ztest::qos::*]`-annotated function into one libtest entry per
/// case (`walk_the_boundary::case_2_orchard`), while the attribute — and so the
/// `QosEntry` it submits — names only the function it was written on
/// (`test_id: concat!(module_path!(), "::", stringify!(#fn_ident))`). Exact
/// matching therefore misses every case of every parameterized test.
///
/// Walking off trailing `::` segments attributes each case to the declaration it
/// was generated from. The alternative is the silent
/// [`QosClass::Basic`](crate::qos::QosClass::Basic) fallback in
/// [`build_work_list`], which reads as "undeclared" but here means "declared
/// `testnet`, mis-read as a 60-second unit test" — and a fixture-restoring test
/// killed at Basic's 60 s `hard_cap` looks like a product hang, not a
/// misclassification.
fn declared_tier(by_name: &HashMap<&str, QosClass>, test_name: &str) -> Option<QosClass> {
    if let Some(class) = by_name.get(test_name) {
        return Some(*class);
    }
    // Each step drops one generated segment; the first ancestor that declared a
    // tier owns this case. A test that genuinely declared nothing walks to the
    // root and yields `None`, preserving the Basic default.
    let mut rest = test_name;
    while let Some((parent, _)) = rest.rsplit_once("::") {
        if let Some(class) = by_name.get(parent) {
            return Some(*class);
        }
        rest = parent;
    }
    None
}

/// Why a test wearing the `sync` tier left a `ztest run` selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncExclusion {
    /// A `#[ztest::sync_test]` profile; the payload is the profile name
    /// `ztest sync start` takes.
    Profile(String),
    /// A test that declared the `sync` tier without being a profile. Nothing
    /// can run it: the tier exists only for the detached sync lifecycle.
    TierOnly,
}

/// A sync-tier test removed from the run's selection: its binary, its libtest
/// name, and which of the two shapes it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludedSync {
    pub binary_id: String,
    pub test_name: String,
    pub reason: SyncExclusion,
}

/// Subtract every sync-tier test from a `ztest run` selection, returning what
/// was dropped (empty binaries removed, preserving
/// [`summarize_selection`](crate::pipeline::build)'s invariant that a selected
/// binary has ≥1 test).
///
/// Two shapes reach the `sync` tier and `ztest run` executes neither:
///
/// * A `#[ztest::sync_test]` profile compiles to an ordinary `#[tokio::test]`,
///   so `cargo nextest list` matches it like any other test; only the inventory
///   dump knows it is a profile. Its lifecycle owner is `ztest sync start` — a
///   detached, k8s-owned pod with a PVC datadir and a 48 h cap.
/// * A bare `#[ztest::qos::sync]` test declares the tier without the profile
///   behind it.
///
/// Either one admitted into a run parks a 48 h item at the top-priority `sync`
/// tier, and — because the QoS plan is folded from the surviving selection —
/// puts a `sync` row in the live panel for work the engine never launches.
/// Excluding by tier, not just by profile registration, keeps that row out.
///
/// Binary-scoped: matching on libtest name alone would drop an unrelated test
/// that happens to share a name with a profile in another binary. The tier
/// lookup goes through [`declared_tier`] so a parameterized sync test's
/// generated cases leave with their parent.
pub fn drop_sync_tests(
    selected: &mut Vec<SelectedBinary>,
    sync_by_binary: &[(String, Vec<SyncTestEntry>)],
    qos_by_binary: &[(String, Vec<QosEntry>)],
) -> Vec<ExcludedSync> {
    let profiles: HashMap<&str, HashMap<&str, &str>> = sync_by_binary
        .iter()
        .map(|(binary_id, entries)| {
            let by_name = entries
                .iter()
                .map(|e| (libtest_name(&e.test_id), e.name.as_str()))
                .collect();
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
                    .and_then(|t| declared_tier(t, test_name))
                    .filter(|class| *class == QosClass::Sync)
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

/// Index the QoS dump for [`declared_tier`] lookups: `binary_id` → libtest name
/// → declared class.
fn tiers_by_binary(
    qos_by_binary: &[(String, Vec<QosEntry>)],
) -> HashMap<&str, HashMap<&str, QosClass>> {
    qos_by_binary
        .iter()
        .map(|(binary_id, entries)| {
            let by_name = entries
                .iter()
                .map(|e| (libtest_name(&e.test_id), e.class))
                .collect();
            (binary_id.as_str(), by_name)
        })
        .collect()
}

/// Build the work-list from the selected binaries and the per-binary QoS dump.
/// Undeclared tests default to [`QosClass::Basic`] (matching `qos::current`);
/// `retries` is applied uniformly; `deps` attaches each item's resource nodes.
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
            let class = bin_tiers
                .and_then(|m| declared_tier(m, test_name.as_str()))
                .unwrap_or(QosClass::Basic);
            let profile = class.profile();
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

/// Order the work-list for submission: highest priority first, then smallest
/// footprint first within a priority, with a stable id tiebreak. The
/// [`Scheduler`](crate::qos::scheduler::Scheduler) re-sorts by `(priority desc,
/// seq asc)`, so this only governs the seq tiebreak itself.
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

    fn sync_entry(test_id: &str, name: &str) -> SyncTestEntry {
        SyncTestEntry {
            test_id: test_id.to_string(),
            name: name.to_string(),
            description: String::new(),
            subject: "wallet".to_string(),
            timeout: "48h".to_string(),
            qos: "sync".to_string(),
            tags: Vec::new(),
        }
    }

    #[test]
    fn sync_profiles_leave_the_selection() {
        let mut bins = vec![bin(
            "zaino-sync-tests::zaino_sync",
            &["ordinary_test", "zaino_state_sync"],
        )];
        let syncs = [(
            "zaino-sync-tests::zaino_sync".to_string(),
            vec![sync_entry(
                "zaino_sync::zaino_state_sync",
                "zaino_state_sync",
            )],
        )];

        let excluded = drop_sync_tests(&mut bins, &syncs, &[]);

        assert_eq!(bins[0].selected_tests, ["ordinary_test"]);
        assert_eq!(excluded.len(), 1);
        assert_eq!(excluded[0].test_name, "zaino_state_sync");
        assert_eq!(
            excluded[0].reason,
            SyncExclusion::Profile("zaino_state_sync".to_string())
        );
        assert!(
            build_work_list(&bins, &[], 0, &ResourceDeps::default())
                .iter()
                .all(|w| w.test_name != "zaino_state_sync")
        );
    }

    #[test]
    fn exclusion_is_binary_scoped() {
        let mut bins = vec![
            bin("pkg::syncs", &["state_sync"]),
            bin("pkg::unit", &["state_sync"]),
        ];
        let syncs = [(
            "pkg::syncs".to_string(),
            vec![sync_entry("syncs::state_sync", "state_sync")],
        )];

        let excluded = drop_sync_tests(&mut bins, &syncs, &[]);

        // The identically-named ordinary test in another binary survives, and
        // the binary left with no tests is dropped from the selection.
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
            vec![
                entry("b::a", QosClass::Basic),
                entry("b::b", QosClass::Testnet),
            ],
        )];
        assert!(drop_sync_tests(&mut bins, &[], &qos).is_empty());
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].selected_tests, ["a", "b"]);
    }

    /// The panel regression: a bare `#[ztest::qos::sync]` test is no profile, so
    /// profile-only pruning left it in the selection — and the QoS plan folded
    /// from that selection put a `sync` row in the live panel for work the
    /// engine never launches. Parameterized cases leave with their parent.
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
        // No `::`: return as-is (defensive).
        assert_eq!(libtest_name("bare"), "bare");
    }

    /// The regression: `rstest` emits one libtest entry per case, but the QoS
    /// attribute submits only the parent function's id. Exact matching dropped
    /// every case to `Basic`, whose 60 s `hard_cap` then killed fixture-restoring
    /// testnet tests at 60 s — indistinguishable from a product hang.
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
            // The attribute names the function, never the generated cases.
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

    /// The fallback must survive the parent walk: a test that declared nothing is
    /// still `Basic`, so the lookup can't be "inherit from any ancestor at all".
    #[test]
    fn a_test_declaring_no_tier_still_defaults_to_basic() {
        let bins = [bin("pkg::b", &["some::module::undeclared_test"])];
        let qos = [(
            "pkg::b".to_string(),
            vec![entry("b::other::declared_test", QosClass::Testnet)],
        )];
        let items = build_work_list(&bins, &qos, 0, &ResourceDeps::default());
        assert_eq!(items[0].class, QosClass::Basic);
    }

    #[test]
    fn joins_tier_by_stripped_test_id() {
        let bins = [bin("ztest::qos_attr", &["marker_basic", "marker_sync"])];
        let qos = [(
            "ztest::qos_attr".to_string(),
            vec![
                // dump test_ids are crate-rooted ("qos_attr::...").
                entry("qos_attr::marker_basic", QosClass::Basic),
                entry("qos_attr::marker_sync", QosClass::Sync),
            ],
        )];
        let items = build_work_list(&bins, &qos, 0, &ResourceDeps::default());
        let by_name: HashMap<_, _> = items.iter().map(|w| (w.test_name.as_str(), w)).collect();
        assert_eq!(by_name["marker_basic"].class, QosClass::Basic);
        assert_eq!(by_name["marker_sync"].class, QosClass::Sync);
        // The work item reserves the tier's whole admitted total (components +
        // runner), so the scheduler accounts the runner pod too.
        assert_eq!(
            by_name["marker_sync"].footprint,
            QosClass::Sync.profile().admitted()
        );
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
        // Highest priority (Sync) runs first.
        assert_eq!(items[0].test_name, "s");
        assert_eq!(items[1].test_name, "y");
        assert_eq!(items[2].test_name, "i");
    }
}
