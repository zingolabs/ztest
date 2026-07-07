//! Phase C: dev-image inventory discovery.
//!
//! For each test binary with a selected test, spawn it with
//! `ZTEST_DUMP_INVENTORY=1`. Each binary writes one `"kind"`-tagged JSON
//! `InventoryLine` per line to stdout, then exits 0; we keep the `Dev` lines and
//! ignore `Qos` ones (consumed elsewhere).
//!
//! Results are deduped by `(dockerfile, context, repo, features, rust_version)`:
//! the same `dev!` call linked into N binaries yields N submissions, only one
//! needing a build — but a `rust_versions` matrix fans one decl into one image
//! per version, each a distinct build. An empty inventory short-circuits, so the
//! docker-build / kind-load phases become no-ops.

use std::collections::BTreeSet;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::inventory::{
    DevImageEntry, InventoryLine, QosEntry, SeedEntry, SeedPayload, TestDepEntry,
};
use crate::pipeline::build::SelectedBinary;

/// All registries demuxed from one binary's tagged inventory dump stream.
#[derive(Debug, Default)]
struct Dumped {
    dev: Vec<DevImageEntry>,
    qos: Vec<QosEntry>,
    seeds: Vec<SeedEntry>,
    deps: Vec<TestDepEntry>,
}

/// Result of Phase C: the deduped set of resources (dev images + data seeds) the
/// currently-selected test set declares, ready to become resource-graph nodes,
/// plus the per-binary associations the engine needs to gate admission on
/// resource readiness.
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
    },
    /// One or more binaries failed to dump (non-zero exit, stderr captured). The
    /// CLI surfaces this and aborts before provisioning.
    Failed { detail: String },
}

/// Run the inventory dump for each selected binary in turn (each is sub-100ms,
/// and serial keeps the output legible). The dump stream carries every registry:
/// returns the deduped dev-image + seed lists for the resource graph, the
/// per-binary image and test→resource edges the engine gates admission on, and
/// the per-binary QoS tier declarations the engine (`engine::plan`) assigns each
/// test from. All per-binary lists stay binary-scoped because the match is keyed
/// by `binary_id` (exact test names can collide across binaries).
pub async fn discover(binaries: &[SelectedBinary]) -> (DumpOutcome, Vec<(String, Vec<QosEntry>)>) {
    let mut seen_img: BTreeSet<DedupKey> = BTreeSet::new();
    let mut seen_seed: BTreeSet<(String, SeedPayload)> = BTreeSet::new();
    let mut images: Vec<DevImageEntry> = Vec::new();
    let mut seeds: Vec<SeedEntry> = Vec::new();
    let mut qos_by_binary: Vec<(String, Vec<QosEntry>)> = Vec::new();
    let mut images_by_binary: Vec<(String, Vec<DevImageEntry>)> = Vec::new();
    let mut deps_by_binary: Vec<(String, Vec<TestDepEntry>)> = Vec::new();

    for bin in binaries {
        match dump_one(bin).await {
            Ok(Dumped {
                dev,
                qos,
                seeds: s,
                deps,
            }) => {
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
                    if seen_seed.insert((e.source.clone(), e.payload)) {
                        seeds.push(e);
                    }
                }
                if !qos.is_empty() {
                    qos_by_binary.push((bin.binary_id.clone(), qos));
                }
                if !deps.is_empty() {
                    deps_by_binary.push((bin.binary_id.clone(), deps));
                }
            }
            Err(detail) => {
                // On failure the caller aborts before lowering, so the
                // partial QoS accumulation is moot.
                return (
                    DumpOutcome::Failed {
                        detail: format!("{}: {detail}", bin.binary_id),
                    },
                    qos_by_binary,
                );
            }
        }
    }
    (
        DumpOutcome::Discovered {
            images,
            seeds,
            images_by_binary,
            deps_by_binary,
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

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("spawn `{}`: {e}", bin.binary_path.display()))?;

    // Read stdout line-by-line so a binary with a huge inventory
    // (unlikely) doesn't buffer end-to-end before we see anything.
    let stdout = child.stdout.take().expect("piped");
    let stderr = child.stderr.take().expect("piped");

    let stdout_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut dumped = Dumped::default();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() {
                continue;
            }
            // Lines are tagged; demux the two registries onto one stream.
            match serde_json::from_str::<InventoryLine>(&line) {
                Ok(InventoryLine::Dev(d)) => dumped.dev.push(d),
                Ok(InventoryLine::Qos(q)) => dumped.qos.push(q),
                Ok(InventoryLine::Seed(s)) => dumped.seeds.push(s),
                Ok(InventoryLine::Dep(d)) => dumped.deps.push(d),
                Err(e) => {
                    return Err(format!("malformed inventory line `{line}`: {e}"));
                }
            }
        }
        Ok(dumped)
    });

    let stderr_task = tokio::spawn(async move {
        let mut buf = String::new();
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            buf.push_str(&line);
            buf.push('\n');
        }
        buf
    });

    let status = child
        .wait()
        .await
        .map_err(|e| format!("wait on `{}`: {e}", bin.binary_path.display()))?;

    let stderr_tail = stderr_task.await.unwrap_or_default();
    let dumped = stdout_task
        .await
        .map_err(|e| format!("stdout join: {e}"))?
        .map_err(|e| format!("{e}\nstderr:\n{}", tail(&stderr_tail, 20)))?;

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
    /// Different rust toolchains are different images (they fork the tag), so
    /// the pinned version discriminates too — else the two variants collapse and
    /// only one gets built.
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
