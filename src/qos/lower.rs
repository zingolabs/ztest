//! Lowering declared QoS tiers into a generated nextest `--tool-config-file`.
//!
//! This is **Layer 1** of the §5.2 two-layer split (`docs/qos-design.md`): we
//! turn the per-binary QoS inventory dump into nextest config so nextest
//! provides *coarse* backpressure (`threads-required`), priority ordering, and
//! a *loose* slow-timeout backstop. Doing so keeps the broker's admission
//! queue short by construction — nextest only spawns ≈capacity-worth of
//! processes, so the precise 2-D/NVMe/budget decision (Layer 2, the broker)
//! runs among a handful of live tests, not a fork-bomb.
//!
//! Everything here is **pure** (string/number → string) so the policy is
//! unit-testable without a cluster or a nextest invocation; `cli/run.rs` does
//! the file I/O and arg assembly.
//!
//! ## test-id → nextest filterset (validated spike, §3 / §10.1)
//!
//! `QosDecl::test_id` is `concat!(module_path!(), "::", fn)`, i.e.
//! `<crate-root>::…::<fn>` (e.g. `qos_attr::marker_basic`). nextest's test
//! name **omits the crate root** (`marker_basic`), so [`nextest_test_name`]
//! strips the leading `::`-segment. Because exact test names can collide
//! across binaries, each filter atom is scoped by `binary_id` — the dump is
//! per-binary, so the id is known.
//!
//! ## The 1-D fold (§5.2)
//!
//! nextest's `threads-required` / `--test-threads` are scalars, but footprints
//! are 2-D. We fold a footprint onto a single "thread-unit" axis of
//! [`UNIT_CPU_MILLI`] CPU × [`UNIT_MEM_BYTES`] RAM, taking the max over both
//! dimensions — so nextest's coarse gate already approximates the broker's 2-D
//! decision.

use std::collections::BTreeMap;
use std::time::Duration;

use super::{GIB, QosClass, Resources};

/// One nextest thread-unit's CPU share (millicores). One core.
pub const UNIT_CPU_MILLI: u64 = 1000;
/// One nextest thread-unit's memory share (bytes). 2 GiB.
pub const UNIT_MEM_BYTES: u64 = 2 * GIB;
/// Fallback nextest pool when cluster capacity is unknown (probe `Missing`) and
/// the user didn't pass `--test-threads` — preserves the prior default.
pub const DEFAULT_POOL: u32 = 6;

/// A selected test, ready to become a `binary_id(=B) & test(=name)` atom.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestRef {
    /// nextest `binary_id` (`<package>::<binary>`; lib tests are `<package>`).
    pub binary_id: String,
    /// nextest test name — the [`nextest_test_name`] of the dumped `test_id`.
    pub name: String,
}

/// Strip the leading crate-root `::`-segment from a dumped `test_id` to get the
/// name nextest matches on. A `test_id` with no `::` (defensive — shouldn't
/// happen, the macro always prepends `module_path!()`) is returned unchanged.
pub fn nextest_test_name(test_id: &str) -> &str {
    match test_id.split_once("::") {
        Some((_crate_root, rest)) => rest,
        None => test_id,
    }
}

/// Divide-round-up.
fn div_ceil(num: u64, den: u64) -> u64 {
    num.div_ceil(den)
}

/// The 1-D fold of a 2-D footprint onto thread-units: `max` over CPU and RAM,
/// floored at 1 (every test occupies at least one slot).
pub fn threads_required(fp: Resources) -> u32 {
    let cpu = div_ceil(fp.cpu_milli, UNIT_CPU_MILLI);
    let mem = div_ceil(fp.mem_bytes, UNIT_MEM_BYTES);
    cpu.max(mem).max(1) as u32
}

/// How many thread-units the cluster's free capacity offers: `min` over CPU and
/// RAM (floored division — partial units don't count), floored at 1 so the pool
/// is never zero.
pub fn cluster_units(free: Resources) -> u32 {
    let cpu = free.cpu_milli / UNIT_CPU_MILLI;
    let mem = free.mem_bytes / UNIT_MEM_BYTES;
    cpu.min(mem).max(1) as u32
}

/// The effective `--test-threads` pool: `min(user-or-default, cluster_units)`.
/// `--test-threads` stays a *local* ceiling; cluster capacity is the other
/// ceiling; the effective pool is the smaller (§6). When `free` is `None` (no
/// probe), only the local ceiling applies — today's behavior.
pub fn pool(user: Option<u32>, free: Option<Resources>, default: u32) -> u32 {
    let local = user.unwrap_or(default).max(1);
    match free {
        Some(free) => local.min(cluster_units(free)),
        None => local,
    }
}

/// Render a [`Duration`] as a nextest-parseable duration string, preferring the
/// largest whole unit (`48h`, `10m`, `60s`) for legibility.
fn duration_str(d: Duration) -> String {
    let secs = d.as_secs();
    if secs != 0 && secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs != 0 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// The nextest `test-group` `sync` tests are serialized under.
///
/// Test groups defined in a **tool** config (`--tool-config-file
/// ztest:<path>`) MUST be namespaced `@tool:<tool-name>:<group>` — nextest
/// rejects a bare name from a tool config (validated against 0.9.114). The
/// tool name here is the fixed `ztest` used by
/// `cli::run::exec_nextest_run_inherited`.
const SYNC_GROUP: &str = "@tool:ztest:qos-sync";

/// Generate the nextest tool-config TOML for the selected, tier-tagged tests,
/// targeting profile `profile` (e.g. `default`). Returns `None` when no QoS
/// tests were dumped — the caller then skips `--tool-config-file` entirely.
///
/// One `[[profile.<p>.overrides]]` block per non-empty tier carries the tier's
/// loose `slow-timeout` backstop, coarse `threads-required`, and `priority`;
/// the `sync` block also joins the `qos-sync` test-group, sized to
/// `sync_max_threads` (the probed NVMe-node count, §11) floored at 1 so `sync`
/// never overcommits the NVMe pool.
pub fn render_tool_config(
    by_tier: &BTreeMap<QosClass, Vec<TestRef>>,
    profile: &str,
    sync_max_threads: u32,
) -> Option<String> {
    let nonempty: Vec<(&QosClass, &Vec<TestRef>)> =
        by_tier.iter().filter(|(_, v)| !v.is_empty()).collect();
    if nonempty.is_empty() {
        return None;
    }

    let mut out = String::new();

    // The `qos-sync` test-group caps concurrent sync tests at the probed
    // NVMe-node count (`sync_max_threads`) so sync never oversubscribes the
    // NVMe pool. Floors at 1 so a sync test still spawns when the count is
    // unknown/zero in the lowering input — in-process admission is the
    // authoritative gate that rejects sync when no NVMe node is schedulable.
    let has_sync = by_tier
        .get(&QosClass::Sync)
        .is_some_and(|v| !v.is_empty());
    if has_sync {
        let max = sync_max_threads.max(1);
        out.push_str("[test-groups]\n");
        // The group name contains `@`/`:`, so it must be a quoted TOML key.
        out.push_str(&format!("\"{SYNC_GROUP}\" = {{ max-threads = {max} }}\n\n"));
    }

    for (class, tests) in nonempty {
        let profile_lowered = class.profile();
        let filter = tests
            .iter()
            .map(|t| format!("(binary_id(={}) & test(={}))", t.binary_id, t.name))
            .collect::<Vec<_>>()
            .join(" | ");
        out.push_str(&format!("[[profile.{profile}.overrides]]\n"));
        out.push_str(&format!("filter = '{filter}'\n"));
        // `slow-timeout` is the *sole* exec-cap enforcement (no broker timer):
        // it measures from process spawn, so `terminate-after = 2` lets a test
        // absorb queue time — it's flagged SLOW at one `hard_cap` and hard-killed
        // (SIGTERM→SIGKILL) at 2×, giving roughly a full execution budget even
        // after a queue wait. Teardown on a timeout-kill falls to the
        // `janitor/ttl` backstop (the kill skips `TestEnv::drop`).
        out.push_str(&format!(
            "slow-timeout = {{ period = \"{}\", terminate-after = 2 }}\n",
            duration_str(profile_lowered.hard_cap)
        ));
        // `sync` runs *off* the general pool — it's serialized by the
        // NVMe-sized `qos-sync` test-group above, not the thread pool — so it
        // claims a single general slot (§6). Charging its full 16-unit
        // footprint here would also gate it on `--test-threads`, and on any
        // cluster whose pool is < 16 sync would never spawn. Other tiers
        // reserve their footprint's thread-units as coarse backpressure.
        let threads = if *class == QosClass::Sync {
            1
        } else {
            threads_required(profile_lowered.footprint)
        };
        out.push_str(&format!("threads-required = {threads}\n"));
        out.push_str(&format!("priority = {}\n", profile_lowered.priority));
        if *class == QosClass::Sync {
            out.push_str(&format!("test-group = \"{SYNC_GROUP}\"\n"));
        }
        out.push('\n');
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tref(binary_id: &str, name: &str) -> TestRef {
        TestRef {
            binary_id: binary_id.to_string(),
            name: name.to_string(),
        }
    }

    // ── test-id → nextest name (the validated spike mapping) ───────────

    #[test]
    fn nextest_test_name_strips_the_crate_root() {
        // Integration-test shape (crate root = file stem).
        assert_eq!(nextest_test_name("qos_attr::marker_basic"), "marker_basic");
        // Lib-test shape (crate root = package name; nested modules kept).
        assert_eq!(nextest_test_name("ztest::qos::tests::foo"), "qos::tests::foo");
        // No `::` → unchanged (defensive).
        assert_eq!(nextest_test_name("bare"), "bare");
    }

    #[test]
    fn spike_regression_marker_sync_maps_to_marker_sync() {
        // Encodes the empirically-validated cargo-nextest 0.9.114 behavior:
        // the dumped `qos_attr::marker_sync` matches nextest's `marker_sync`.
        assert_eq!(nextest_test_name("qos_attr::marker_sync"), "marker_sync");
    }

    // ── the 1-D fold ───────────────────────────────────────────────────

    #[test]
    fn threads_required_folds_the_tier_footprints() {
        // basic 500m/512Mi→1, integration 2c/2Gi→2, testnet 8c/18Gi→max(8,9)=9,
        // sync 16c/32Gi→max(16,16)=16.
        assert_eq!(threads_required(QosClass::Basic.profile().footprint), 1);
        assert_eq!(threads_required(QosClass::Integration.profile().footprint), 2);
        assert_eq!(threads_required(QosClass::Testnet.profile().footprint), 9);
        assert_eq!(threads_required(QosClass::Sync.profile().footprint), 16);
    }

    #[test]
    fn threads_required_takes_the_max_dimension_and_floors_at_one() {
        // RAM-bound: 0.5 core but 4 GiB → ceil(4Gi/2Gi)=2 dominates.
        assert_eq!(threads_required(Resources::new(500, 4 * GIB)), 2);
        // CPU-bound: 3 cores, 1 GiB → 3 dominates.
        assert_eq!(threads_required(Resources::new(3000, GIB)), 3);
        // Tiny → at least one slot.
        assert_eq!(threads_required(Resources::new(1, 1)), 1);
    }

    #[test]
    fn cluster_units_is_the_min_dimension_floored() {
        // 8 cores / 8 GiB: cpu=8, mem=4 → min 4.
        assert_eq!(cluster_units(Resources::new(8000, 8 * GIB)), 4);
        // Never zero even on a tiny cluster.
        assert_eq!(cluster_units(Resources::new(100, MIB_SMALL)), 1);
    }
    const MIB_SMALL: u64 = 1024 * 1024;

    #[test]
    fn pool_is_min_of_local_ceiling_and_cluster() {
        // User asks 16, cluster offers 4 → 4.
        assert_eq!(pool(Some(16), Some(Resources::new(4000, 8 * GIB)), 6), 4);
        // User asks 2, cluster offers 4 → local ceiling 2 wins.
        assert_eq!(pool(Some(2), Some(Resources::new(4000, 8 * GIB)), 6), 2);
        // No probe → default applies (today's behavior).
        assert_eq!(pool(None, None, 6), 6);
        assert_eq!(pool(Some(3), None, 6), 3);
    }

    #[test]
    fn duration_str_prefers_the_largest_whole_unit() {
        // 60s collapses to the larger whole unit (1m) — nextest parses both;
        // we prefer the largest for legibility.
        assert_eq!(duration_str(Duration::from_secs(60)), "1m");
        assert_eq!(duration_str(Duration::from_secs(10 * 60)), "10m");
        assert_eq!(duration_str(Duration::from_secs(6 * 3600)), "6h");
        assert_eq!(duration_str(Duration::from_secs(48 * 3600)), "48h");
        // Not a whole minute → seconds.
        assert_eq!(duration_str(Duration::from_secs(90)), "90s");
    }

    // ── TOML rendering ─────────────────────────────────────────────────

    #[test]
    fn render_returns_none_when_no_tests() {
        let empty = BTreeMap::new();
        assert_eq!(render_tool_config(&empty, "default", 1), None);
        // A tier present but with an empty vec is also "no tests".
        let mut m = BTreeMap::new();
        m.insert(QosClass::Basic, Vec::new());
        assert_eq!(render_tool_config(&m, "default", 1), None);
    }

    #[test]
    fn render_emits_one_block_per_tier_with_exact_atoms() {
        let mut by_tier: BTreeMap<QosClass, Vec<TestRef>> = BTreeMap::new();
        by_tier.insert(
            QosClass::Basic,
            vec![tref("ztest::qos_attr", "marker_basic")],
        );
        by_tier.insert(
            QosClass::Sync,
            vec![
                tref("ztest::qos_attr", "marker_sync"),
                tref("wallet-tests::wt", "syncs_from_genesis"),
            ],
        );
        // 3 NVMe nodes → sync runs up to 3-wide.
        let toml = render_tool_config(&by_tier, "default", 3).unwrap();

        // Sync present → the serializing test-group is emitted (namespaced for
        // tool configs, as nextest requires) and sized to the NVMe node count.
        assert!(toml.contains("[test-groups]"), "{toml}");
        assert!(
            toml.contains("\"@tool:ztest:qos-sync\" = { max-threads = 3 }"),
            "{toml}"
        );

        // Basic block: exact atom, basic timeout/threads/priority, no group.
        assert!(toml.contains(
            "[[profile.default.overrides]]\nfilter = '(binary_id(=ztest::qos_attr) & test(=marker_basic))'"
        ), "{toml}");
        assert!(toml.contains("period = \"1m\""), "{toml}");
        // Sole-cap backstop: SLOW at 1× hard_cap, hard kill at 2×.
        assert!(toml.contains("terminate-after = 2"), "{toml}");
        assert!(toml.contains("threads-required = 1"), "{toml}");

        // Sync block: unioned atoms, 48h, priority 3, in the group. Sync runs
        // off the general pool (test-group gated), so it claims 1 thread, NOT
        // its 16-unit footprint — else `--test-threads < 16` would block it.
        assert!(toml.contains(
            "filter = '(binary_id(=ztest::qos_attr) & test(=marker_sync)) | (binary_id(=wallet-tests::wt) & test(=syncs_from_genesis))'"
        ), "{toml}");
        assert!(toml.contains("period = \"48h\""), "{toml}");
        assert!(toml.contains("priority = 3"), "{toml}");
        // Locate the sync override block and assert it carries threads = 1.
        let sync_block = &toml[toml.find("test(=marker_sync)").unwrap()..];
        assert!(
            sync_block.contains("threads-required = 1"),
            "sync must claim 1 general slot (test-group gated): {toml}"
        );
        assert!(toml.contains("test-group = \"@tool:ztest:qos-sync\""), "{toml}");

        // Determinism: tiers appear in declaration order (Basic before Sync).
        let basic_at = toml.find("test(=marker_basic)").unwrap();
        let sync_at = toml.find("test(=marker_sync)").unwrap();
        assert!(basic_at < sync_at, "tiers should be ordered: {toml}");
    }

    #[test]
    fn render_without_sync_omits_the_test_group() {
        let mut by_tier: BTreeMap<QosClass, Vec<TestRef>> = BTreeMap::new();
        by_tier.insert(
            QosClass::Integration,
            vec![tref("ztest::it", "does_a_thing")],
        );
        let toml = render_tool_config(&by_tier, "ci", 1).unwrap();
        assert!(!toml.contains("[test-groups]"), "{toml}");
        assert!(!toml.contains("qos-sync"), "{toml}");
        // Targets the requested profile.
        assert!(toml.contains("[[profile.ci.overrides]]"), "{toml}");
        assert!(toml.contains("period = \"10m\""), "{toml}");
        assert!(toml.contains("threads-required = 2"), "{toml}");
    }

    #[test]
    fn sync_group_floors_at_one_when_no_nvme_nodes() {
        let mut by_tier: BTreeMap<QosClass, Vec<TestRef>> = BTreeMap::new();
        by_tier.insert(QosClass::Sync, vec![tref("ztest::s", "syncs")]);
        // 0 probed NVMe nodes (dev/kind) → still serialize at 1, never 0.
        let toml = render_tool_config(&by_tier, "default", 0).unwrap();
        assert!(
            toml.contains("\"@tool:ztest:qos-sync\" = { max-threads = 1 }"),
            "{toml}"
        );
    }
}
