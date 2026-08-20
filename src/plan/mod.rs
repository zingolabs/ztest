//! Resolved provisioning plan for one selection — the value `run`/`sync start` provision and
//! `describe` renders.
//!
//! - One planner, two consumers (a re-derived `describe` drifts from what provisioning does)
//! - [`needed_seeds`] = the prune: dump unions seeds across *binaries*, selection is per *test*
//! - Pure, no cluster contact (`--full` overlays live state in [`render`], never here)
//! - Design: `docs/design-describe.md`

use std::collections::{BTreeMap, BTreeSet};

use crate::inventory::{DevImageEntry, QosEntry, SeedEntry, SyncTestEntry, TestDepEntry};
use crate::pipeline::build::SelectedBinary;
use crate::qos::{QosClass, Resources};

#[derive(Debug, Clone)]
pub struct Plan {
    pub roots: Vec<PlanRoot>,
    pub pruned: Vec<PrunedSeed>,
}

#[derive(Debug, Clone)]
pub struct PlanRoot {
    pub label: String,
    pub description: String,
    pub qos: QosNode,
    pub tags: Vec<String>,
    pub images: Vec<DevImageEntry>,
    pub seeds: Vec<SeedEntry>,
}

/// `hard_cap` from the tier table; `declared` only where `#[sync_test(timeout)]` set one
#[derive(Debug, Clone)]
pub struct QosNode {
    pub class: QosClass,
    pub admitted: Resources,
    pub declared_timeout: Option<String>,
}

/// Seed declared by the compiled selection, needed by none of its selected tests
#[derive(Debug, Clone)]
pub struct PrunedSeed {
    pub seed: SeedEntry,
    pub declared_by: Vec<String>,
}

/// Seeds the selected tests actually need, plus what was dropped.
///
/// - Dump unions every `SeedEntry` across every linked binary; selection is per test
/// - Match is NOT equality: `TestDepEntry::test_id` = `module_path!()::fn`, selection = libtest
/// - `rstest` emits one entry per case (`parent::case_1_x`), `#[needs]` submits only `parent`
///   → prefix walk, else a seed every case needs gets pruned (same trap `declared_tier` fixed)
pub fn needed_seeds(
    binaries: &[SelectedBinary],
    deps_by_binary: &[(String, Vec<TestDepEntry>)],
    seeds: &[SeedEntry],
) -> (Vec<SeedEntry>, Vec<PrunedSeed>) {
    let selected: BTreeSet<&str> =
        binaries.iter().flat_map(|b| b.selected_tests.iter().map(String::as_str)).collect();

    let mut declared_by: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    let mut needed: BTreeSet<&str> = BTreeSet::new();
    for (_binary, deps) in deps_by_binary {
        for dep in deps {
            declared_by.entry(dep.resource.as_str()).or_default().push(dep.test_id.clone());
            if selects(&selected, crate::engine::plan::libtest_name(&dep.test_id)) {
                needed.insert(dep.resource.as_str());
            }
        }
    }

    let (mut keep, mut drop) = (Vec::new(), Vec::new());
    for seed in seeds {
        if needed.contains(seed.oid.as_str()) {
            keep.push(seed.clone());
        } else {
            let mut by = declared_by.get(seed.oid.as_str()).cloned().unwrap_or_default();
            by.sort();
            by.dedup();
            drop.push(PrunedSeed { seed: seed.clone(), declared_by: by });
        }
    }
    (keep, drop)
}

/// Exact hit, or an `rstest` case beneath the declaring fn
fn selects(selected: &BTreeSet<&str>, declarant: &str) -> bool {
    selected.contains(declarant)
        || selected.iter().any(|t| t.strip_prefix(declarant).is_some_and(|r| r.starts_with("::")))
}

/// One root per sync profile (`sync describe`); images ride the binary edge, not the test edge
pub fn for_sync(
    binaries: &[SelectedBinary],
    entry: &SyncTestEntry,
    images_by_binary: &[(String, Vec<DevImageEntry>)],
    deps_by_binary: &[(String, Vec<TestDepEntry>)],
    seeds: &[SeedEntry],
) -> Plan {
    let owner: Vec<SelectedBinary> = binaries
        .iter()
        .filter(|b| {
            b.selected_tests.iter().any(|t| t == crate::engine::plan::libtest_name(&entry.test_id))
        })
        .cloned()
        .collect();
    let scope: &[SelectedBinary] = if owner.is_empty() { binaries } else { &owner };
    let (keep, pruned) = needed_seeds(scope, deps_by_binary, seeds);

    let images = scope
        .iter()
        .flat_map(|b| images_by_binary.iter().filter(|(id, _)| *id == b.binary_id))
        .flat_map(|(_, imgs)| imgs.iter().cloned())
        .collect();

    // Unknown tier → `Sync` for display only; reserve still comes from that tier's table
    let class = entry.class().unwrap_or(QosClass::Sync);
    let admitted = entry.profile().unwrap_or_else(|| class.profile()).admitted();
    Plan {
        roots: vec![PlanRoot {
            label: entry.name.clone(),
            description: entry.description.clone(),
            qos: QosNode {
                class,
                admitted,
                declared_timeout: Some(entry.timeout.clone()).filter(|t| !t.is_empty()),
            },
            tags: entry.tags.clone(),
            images,
            seeds: keep,
        }],
        pruned,
    }
}

/// One root per selected test (`run describe`); a shared image/seed expands once, then `(*)`
pub fn for_run(
    binaries: &[SelectedBinary],
    images_by_binary: &[(String, Vec<DevImageEntry>)],
    deps_by_binary: &[(String, Vec<TestDepEntry>)],
    qos_by_binary: &[(String, Vec<QosEntry>)],
    seeds: &[SeedEntry],
) -> Plan {
    let (keep, pruned) = needed_seeds(binaries, deps_by_binary, seeds);
    let by_oid: BTreeMap<&str, &SeedEntry> = keep.iter().map(|s| (s.oid.as_str(), s)).collect();

    let mut roots = Vec::new();
    for bin in binaries {
        let images: Vec<DevImageEntry> = images_by_binary
            .iter()
            .find(|(id, _)| *id == bin.binary_id)
            .map(|(_, i)| i.clone())
            .unwrap_or_default();
        let tiers: BTreeMap<&str, &QosEntry> = qos_by_binary
            .iter()
            .find(|(id, _)| *id == bin.binary_id)
            .map(|(_, q)| {
                q.iter().map(|e| (crate::engine::plan::libtest_name(&e.test_id), e)).collect()
            })
            .unwrap_or_default();

        for test in &bin.selected_tests {
            let declared = tier_for(&tiers, test);
            let class = declared.map_or(QosClass::Basic, |e| e.class);
            // Reserve admission will ask for, override included
            let admitted =
                declared.map_or_else(|| QosClass::Basic.profile(), |e| e.profile()).admitted();
            let seeds = deps_by_binary
                .iter()
                .filter(|(id, _)| *id == bin.binary_id)
                .flat_map(|(_, deps)| deps.iter())
                .filter(|d| {
                    selects(&[test.as_str()].into(), crate::engine::plan::libtest_name(&d.test_id))
                })
                .filter_map(|d| by_oid.get(d.resource.as_str()).map(|s| (*s).clone()))
                .collect();
            roots.push(PlanRoot {
                label: test.clone(),
                description: String::new(),
                qos: QosNode { class, admitted, declared_timeout: None },
                tags: Vec::new(),
                images: images.clone(),
                seeds,
            });
        }
    }
    Plan { roots, pruned }
}

/// Declared QoS entry, walking to the declaring ancestor for `rstest` cases
fn tier_for<'a>(tiers: &BTreeMap<&str, &'a QosEntry>, test: &str) -> Option<&'a QosEntry> {
    if let Some(e) = tiers.get(test) {
        return Some(e);
    }
    let mut rest = test;
    while let Some((parent, _)) = rest.rsplit_once("::") {
        if let Some(e) = tiers.get(parent) {
            return Some(e);
        }
        rest = parent;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::SeedPayload;

    fn seed(oid: &str) -> SeedEntry {
        SeedEntry {
            name: format!("{oid}.tar.zst"),
            oid: oid.to_string(),
            size: 1,
            uncompressed_bytes: 0,
            payload: SeedPayload::Archive,
            base_uri: crate::storage::r2::BASE_URI.to_string(),
            key_prefix: crate::storage::r2::KEY_PREFIX.to_string(),
        }
    }

    fn bin(id: &str, tests: &[&str]) -> SelectedBinary {
        SelectedBinary {
            binary_path: "/dev/null".into(),
            cwd: ".".into(),
            binary_id: id.to_string(),
            selected_tests: tests.iter().map(|t| t.to_string()).collect(),
        }
    }

    fn dep(test_id: &str, oid: &str) -> TestDepEntry {
        TestDepEntry { test_id: test_id.to_string(), resource: oid.to_string() }
    }

    /// The bug: `sync start <profile>` pulled a clientless seed no selected test declared
    #[test]
    fn seed_declared_only_by_an_unselected_test_is_pruned() {
        let bins = [bin("sync::zaino_sync", &["zaino_index_construction"])];
        let deps = vec![(
            "sync::zaino_sync".to_string(),
            vec![
                dep("zaino_sync::zaino_index_construction", "1106bc19"),
                dep("the_pub_testnet_ironwood_boundary::ironwood_boundary", "3545da25"),
            ],
        )];
        let (keep, pruned) = needed_seeds(&bins, &deps, &[seed("1106bc19"), seed("3545da25")]);

        assert_eq!(keep.iter().map(|s| s.oid.as_str()).collect::<Vec<_>>(), ["1106bc19"]);
        assert_eq!(pruned.len(), 1);
        assert_eq!(pruned[0].seed.oid, "3545da25");
        assert_eq!(pruned[0].declared_by, ["the_pub_testnet_ironwood_boundary::ironwood_boundary"]);
    }

    /// `rstest` emits one libtest entry per case; `#[needs]` submits only the parent fn.
    /// Exact matching would prune the fixture every case restores
    #[test]
    fn parameterized_cases_keep_the_seed_declared_on_their_parent() {
        let bins = [bin(
            "clientless",
            &["testnet_parity::case_1_sapling", "testnet_parity::case_2_orchard"],
        )];
        let deps = vec![("clientless".to_string(), vec![dep("parity::testnet_parity", "abcd")])];
        let (keep, pruned) = needed_seeds(&bins, &deps, &[seed("abcd")]);

        assert_eq!(keep.len(), 1);
        assert!(pruned.is_empty());
    }

    /// Prefix walk must not match a sibling sharing a name prefix
    #[test]
    fn a_sibling_sharing_a_name_prefix_does_not_keep_the_seed() {
        let bins = [bin("c", &["parity_extended"])];
        let deps = vec![("c".to_string(), vec![dep("c::parity", "abcd")])];
        let (keep, pruned) = needed_seeds(&bins, &deps, &[seed("abcd")]);

        assert!(keep.is_empty());
        assert_eq!(pruned.len(), 1);
    }

    #[test]
    fn one_seed_declared_by_two_selected_tests_is_kept_once() {
        let bins = [bin("c", &["a", "b"])];
        let deps = vec![("c".to_string(), vec![dep("c::a", "abcd"), dep("c::b", "abcd")])];
        let (keep, _) = needed_seeds(&bins, &deps, &[seed("abcd")]);

        assert_eq!(keep.len(), 1);
    }
}
