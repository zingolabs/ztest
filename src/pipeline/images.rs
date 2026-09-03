//! Phase C: dev-image inventory discovery.
//!
//! - Each selected binary spawned with `ZTEST_DUMP_INVENTORY=1`, emitting `"kind"`-tagged
//!   JSON `InventoryLine`s
//! - Deduped across binaries → a `dev!` linked into N binaries builds once (a
//!   `rust_versions` matrix still forks one image per version)

use std::collections::BTreeSet;
use std::process::Stdio;

use tokio::process::Command;

use crate::inventory::{
    DevImageEntry, InventoryLine, QosEntry, SeedEntry, SeedPayload, SyncTestEntry, TestDepEntry,
};
use crate::pipeline::build::SelectedBinary;

#[derive(Debug, Default)]
pub struct Dumped {
    dev: Vec<DevImageEntry>,
    qos: Vec<QosEntry>,
    seeds: Vec<SeedEntry>,
    deps: Vec<TestDepEntry>,
    sync_tests: Vec<SyncTestEntry>,
}

/// Pure — transport (local subprocess vs builder-pod `exec`, see
/// [`crate::pipeline::runner`]) is the caller's concern
pub fn parse_inventory(stdout: &str) -> Result<Dumped, crate::error::PipelineError> {
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
            // Sync-test declarations feed the `ztest sync` controller (`start` resolves a
            // profile name → its `test_id` → binary + libtest test) and `ztest run`'s
            // selection prune, the only way the engine tells a profile from a plain test
            Ok(InventoryLine::SyncTest(s)) => dumped.sync_tests.push(s),
            Err(e) => return Err(format!("malformed inventory line `{line}`: {e}").into()),
        }
    }
    Ok(dumped)
}

/// Deduped resources the selection declares, ready to become resource-graph nodes, plus
/// the per-binary associations the engine gates admission on.
///
/// - `*_by_binary` binary-scoped (`test_id`s collide across binaries)
/// - `sync_by_binary` also undeduped: every binary linking a profile must exclude it from
///   *its own* selection, or the run silently shrinks
/// - `Failed` aborts the CLI before any provisioning
#[derive(Debug, Clone)]
pub enum DumpOutcome {
    Discovered {
        images: Vec<DevImageEntry>,
        seeds: Vec<SeedEntry>,
        images_by_binary: Vec<(String, Vec<DevImageEntry>)>,
        deps_by_binary: Vec<(String, Vec<TestDepEntry>)>,
        sync_tests: Vec<SyncTestEntry>,
        sync_by_binary: Vec<(String, Vec<SyncTestEntry>)>,
    },
    Failed {
        detail: String,
    },
}

/// Serial — each dump is sub-100ms
pub async fn discover(binaries: &[SelectedBinary]) -> (DumpOutcome, Vec<(String, Vec<QosEntry>)>) {
    let mut dumps: Vec<Dumped> = Vec::with_capacity(binaries.len());
    for bin in binaries {
        match dump_one(bin).await {
            Ok(d) => dumps.push(d),
            Err(detail) => {
                return (
                    DumpOutcome::Failed { detail: format!("{}: {detail}", bin.binary_id) },
                    Vec::new(),
                );
            }
        }
    }
    assemble(binaries, dumps)
}

/// `binaries` and `dumps` index-aligned. Shared by the local ([`discover`]) and on-cluster
/// ([`crate::pipeline::runner`]) paths → dedup lives in one place
pub fn assemble(
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
        let Dumped { dev, qos, seeds: s, deps, sync_tests: syncs } = dumped;
        // Per-binary images, deduped within the binary (the binary edge)
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
            // Dedup on the OID: one artifact declared from two binaries is one seed, and
            // content addressing gets there without the two agreeing on a file path
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
        // Sync profiles dedup by `test_id` across binaries (defined once, but a shared
        // helper crate can link one into several). The per-binary edge keeps every copy —
        // each linking binary must exclude it from that binary's selection
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

async fn dump_one(bin: &SelectedBinary) -> Result<Dumped, crate::error::PipelineError> {
    let mut cmd = Command::new(&bin.binary_path);
    cmd.env("ZTEST_DUMP_INVENTORY", "1")
        .current_dir(&bin.cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null())
        .kill_on_drop(true);

    // Dumps are tiny (sub-100ms binaries) → capture end-to-end and demux via the shared
    // `parse_inventory`, no streaming
    let out =
        cmd.output().await.map_err(|e| format!("spawn `{}`: {e}", bin.binary_path.display()))?;
    let stderr_tail = String::from_utf8_lossy(&out.stderr).into_owned();
    let dumped = parse_inventory(&String::from_utf8_lossy(&out.stdout))
        .map_err(|e| format!("{e}\nstderr:\n{}", tail(&stderr_tail, 20)))?;
    let status = out.status;

    if !status.success() {
        return Err(format!(
            "binary exited {} during inventory dump; stderr:\n{}",
            status.code().map(|c| c.to_string()).unwrap_or_else(|| "?".into()),
            tail(&stderr_tail, 20)
        )
        .into());
    }
    Ok(dumped)
}

/// `source` = the `DevSource`'s Debug repr — fully discriminating and `Ord`, so it keys
/// the set directly. `rust_version` must discriminate too (toolchains fork the tag;
/// without it the variants collapse and only one gets built)
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DedupKey {
    repo: String,
    source: String,
    features: Vec<String>,
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
