//! Phase C: dev-image inventory discovery. Each selected test binary is spawned
//! with `ZTEST_DUMP_INVENTORY=1` and emits `"kind"`-tagged JSON `InventoryLine`s.
//! Results are deduped across binaries so a `dev!` linked into N binaries builds
//! once (a `rust_versions` matrix still forks one image per version).

use std::collections::BTreeSet;
use std::process::Stdio;

use tokio::process::Command;

use crate::inventory::{
    DevImageEntry, InventoryLine, QosEntry, SeedEntry, SeedPayload, SyncTestEntry, TestDepEntry,
};
use crate::pipeline::build::SelectedBinary;

/// All registries demuxed from one binary's tagged inventory dump stream.
#[derive(Debug, Default)]
pub(crate) struct Dumped {
    dev: Vec<DevImageEntry>,
    qos: Vec<QosEntry>,
    seeds: Vec<SeedEntry>,
    deps: Vec<TestDepEntry>,
    sync_tests: Vec<SyncTestEntry>,
}

/// Demux one binary's `ZTEST_DUMP_INVENTORY=1` stdout (tagged JSON-lines) into
/// the four registries. Pure — the transport (local subprocess vs a builder-pod
/// `exec`, see [`crate::pipeline::remote_compile`]) is the caller's concern.
pub(crate) fn parse_inventory(stdout: &str) -> Result<Dumped, String> {
    let mut dumped = Dumped::default();
    for line in stdout.lines() {
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<InventoryLine>(line) {
            Ok(InventoryLine::Dev(d)) => dumped.dev.push(d),
            Ok(InventoryLine::Qos(q)) => dumped.qos.push(q),
            Ok(InventoryLine::Seed(s)) => dumped.seeds.push(s),
            Ok(InventoryLine::Dep(d)) => dumped.deps.push(d),
            // Sync-test declarations feed the `ztest sync` controller (`start`
            // resolves a profile name → its `test_id`, hence binary + libtest
            // test) and `ztest run`'s selection prune, which is the only way the
            // engine can tell a profile from an ordinary `#[tokio::test]`.
            Ok(InventoryLine::SyncTest(s)) => dumped.sync_tests.push(s),
            Err(e) => return Err(format!("malformed inventory line `{line}`: {e}")),
        }
    }
    Ok(dumped)
}

/// Result of Phase C: the deduped resources (dev images + data seeds) the
/// selection declares, ready to become resource-graph nodes, plus the per-binary
/// associations the engine gates admission on.
#[derive(Debug, Clone)]
pub enum DumpOutcome {
    /// Inventory was successfully dumped from every selected binary.
    Discovered {
        /// Deduped dev images across the whole selection (the graph's image nodes).
        images: Vec<DevImageEntry>,
        /// Deduped seeds across the whole selection (the graph's seed nodes).
        seeds: Vec<SeedEntry>,
        /// Per-binary dev images (deduped within a binary): the binary-level image
        /// edge — every test in a binary depends on that binary's images.
        images_by_binary: Vec<(String, Vec<DevImageEntry>)>,
        /// Per-binary test→resource edges (`#[ztest::archive]`/`#[needs]`): the
        /// sound per-test seed edge. Binary-scoped because `test_id`s can collide
        /// across binaries.
        deps_by_binary: Vec<(String, Vec<TestDepEntry>)>,
        /// `#[ztest::sync_test]` profiles across the selection (deduped by
        /// `test_id`). The `ztest sync` controller resolves a profile name to its
        /// binary + libtest test from these.
        sync_tests: Vec<SyncTestEntry>,
        /// Per-binary sync profiles, undeduped: the exclusion edge `ztest run`
        /// subtracts from its work-list. Binary-scoped for the same reason as
        /// `deps_by_binary` — a profile's libtest name can collide with an
        /// ordinary test in another binary, and dropping that one would silently
        /// shrink the run.
        sync_by_binary: Vec<(String, Vec<SyncTestEntry>)>,
    },
    /// One or more binaries failed to dump (non-zero exit, stderr captured). The
    /// CLI surfaces this and aborts before provisioning.
    Failed { detail: String },
}

/// Dump each selected binary's inventory (serial; each is sub-100ms), then
/// [`assemble`] the deduped resource lists and per-binary edges. Per-binary lists
/// stay binary-scoped because test names can collide across binaries.
pub async fn discover(binaries: &[SelectedBinary]) -> (DumpOutcome, Vec<(String, Vec<QosEntry>)>) {
    let mut dumps: Vec<Dumped> = Vec::with_capacity(binaries.len());
    for bin in binaries {
        match dump_one(bin).await {
            Ok(d) => dumps.push(d),
            Err(detail) => {
                return (
                    DumpOutcome::Failed {
                        detail: format!("{}: {detail}", bin.binary_id),
                    },
                    Vec::new(),
                );
            }
        }
    }
    assemble(binaries, dumps)
}

/// Fold each binary's [`Dumped`] into the deduped resource set plus per-binary
/// edges. Shared by the local ([`discover`]) and on-cluster
/// ([`crate::pipeline::remote_compile`]) paths so dedup lives in one place.
/// `binaries` and `dumps` are index-aligned.
pub(crate) fn assemble(
    binaries: &[SelectedBinary],
    dumps: Vec<Dumped>,
) -> (DumpOutcome, Vec<(String, Vec<QosEntry>)>) {
    let mut seen_img: BTreeSet<DedupKey> = BTreeSet::new();
    let mut seen_seed: BTreeSet<(String, SeedPayload)> = BTreeSet::new();
    let mut images: Vec<DevImageEntry> = Vec::new();
    let mut seeds: Vec<SeedEntry> = Vec::new();
    let mut qos_by_binary: Vec<(String, Vec<QosEntry>)> = Vec::new();
    let mut images_by_binary: Vec<(String, Vec<DevImageEntry>)> = Vec::new();
    let mut deps_by_binary: Vec<(String, Vec<TestDepEntry>)> = Vec::new();
    let mut seen_sync: BTreeSet<String> = BTreeSet::new();
    let mut sync_tests: Vec<SyncTestEntry> = Vec::new();
    let mut sync_by_binary: Vec<(String, Vec<SyncTestEntry>)> = Vec::new();

    for (bin, dumped) in binaries.iter().zip(dumps) {
        let Dumped {
            dev,
            qos,
            seeds: s,
            deps,
            sync_tests: syncs,
        } = dumped;
        // Per-binary images, deduped within the binary (the binary edge).
        let mut seen_bin_img: BTreeSet<DedupKey> = BTreeSet::new();
        let mut bin_images: Vec<DevImageEntry> = Vec::new();
        for d in dev {
            if seen_bin_img.insert(DedupKey::from(&d)) {
                bin_images.push(d.clone());
            }
            if seen_img.insert(DedupKey::from(&d)) {
                images.push(d);
            }
        }
        if !bin_images.is_empty() {
            images_by_binary.push((bin.binary_id.clone(), bin_images));
        }
        for e in s {
            // Dedup on the OID: the same artifact declared from two binaries is
            // one seed, and content addressing makes that true without the two
            // needing to agree on where the file lives.
            if seen_seed.insert((e.oid.clone(), e.payload)) {
                seeds.push(e);
            }
        }
        if !qos.is_empty() {
            qos_by_binary.push((bin.binary_id.clone(), qos));
        }
        if !deps.is_empty() {
            deps_by_binary.push((bin.binary_id.clone(), deps));
        }
        // Sync profiles dedup by `test_id` across binaries (a profile is defined
        // once, but a shared helper crate could link it into several binaries).
        // The per-binary edge keeps every copy: each binary that links a profile
        // must have it excluded from that binary's selection.
        if !syncs.is_empty() {
            sync_by_binary.push((bin.binary_id.clone(), syncs.clone()));
        }
        for st in syncs {
            if seen_sync.insert(st.test_id.clone()) {
                sync_tests.push(st);
            }
        }
    }
    (
        DumpOutcome::Discovered {
            images,
            seeds,
            images_by_binary,
            deps_by_binary,
            sync_tests,
            sync_by_binary,
        },
        qos_by_binary,
    )
}

/// Spawn one binary with `ZTEST_DUMP_INVENTORY=1` and parse the
/// JSON-lines stdout into both registries.
async fn dump_one(bin: &SelectedBinary) -> Result<Dumped, String> {
    let mut cmd = Command::new(&bin.binary_path);
    cmd.env("ZTEST_DUMP_INVENTORY", "1")
        .current_dir(&bin.cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null())
        .kill_on_drop(true);

    // Inventory dumps are tiny (sub-100ms binaries), so capture end-to-end and
    // demux via the shared `parse_inventory` rather than streaming.
    let out = cmd
        .output()
        .await
        .map_err(|e| format!("spawn `{}`: {e}", bin.binary_path.display()))?;
    let stderr_tail = String::from_utf8_lossy(&out.stderr).into_owned();
    let dumped = parse_inventory(&String::from_utf8_lossy(&out.stdout))
        .map_err(|e| format!("{e}\nstderr:\n{}", tail(&stderr_tail, 20)))?;
    let status = out.status;

    if !status.success() {
        return Err(format!(
            "binary exited {} during inventory dump; stderr:\n{}",
            status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "?".into()),
            tail(&stderr_tail, 20)
        ));
    }
    Ok(dumped)
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DedupKey {
    repo: String,
    /// Debug repr of the `DevSource` — fully discriminating (local paths or
    /// git url+rev+paths) and `Ord`, so it keys the dedup set directly.
    source: String,
    features: Vec<String>,
    /// Different rust toolchains fork the tag, so the pinned version must
    /// discriminate too — else the variants collapse and only one gets built.
    rust_version: Option<String>,
}

impl From<&DevImageEntry> for DedupKey {
    fn from(d: &DevImageEntry) -> Self {
        let mut features = d.features.clone();
        features.sort();
        DedupKey {
            repo: d.repo.clone(),
            source: format!("{:?}", d.source),
            features,
            rust_version: d.rust_version.clone(),
        }
    }
}

fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}
