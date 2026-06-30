//! Phase C — dev-image inventory discovery.
//!
//! For each test binary with ≥1 selected test, spawn it with
//! `ZTEST_DUMP_INVENTORY=1` (and `--`-terminated argv so no test
//! filter args confuse the harness — the ctor hook exits before
//! libtest reads argv anyway, but cleaner). Each binary writes one
//! `"kind"`-tagged JSON `InventoryLine` per line to stdout, then exits 0;
//! we keep the `Dev` lines and ignore `Qos` ones (consumed elsewhere).
//!
//! We collect across every binary and dedupe by
//! `(dockerfile, context, repo, features)` — the same `dev!` call
//! linked into N binaries produces N submissions, only one of which
//! needs to be built.
//!
//! On an empty inventory (no `dev!` site reachable from any selected
//! binary) we short-circuit; the docker-build / kind-load phases
//! become no-ops.

use std::collections::BTreeSet;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::inventory::{DevImageEntry, InventoryLine, QosEntry};
use crate::pipeline::build::SelectedBinary;

/// Both registries demuxed from one binary's tagged inventory dump stream.
#[derive(Debug, Default)]
struct Dumped {
    dev: Vec<DevImageEntry>,
    qos: Vec<QosEntry>,
}

/// Result of Phase C — the deduped set of dev images required by the
/// currently-selected test set.
#[derive(Debug, Clone)]
pub enum ImagesOutcome {
    /// Inventory was successfully dumped from every selected binary.
    /// `images` is the deduped declaration list.
    Discovered { images: Vec<DevImageEntry> },
    /// One or more binaries failed to dump (non-zero exit, stderr
    /// captured). The CLI surfaces this and aborts before docker
    /// build.
    Failed { detail: String },
}

/// Run the inventory dump for each selected binary in turn (cheap —
/// each is sub-100ms — and serial keeps the output legible). The same
/// dump stream carries both registries: returns the deduped dev-image
/// list for the docker pipeline **and** the per-binary QoS tier
/// declarations the engine (`engine::plan`) assigns each test from. QoS
/// entries are kept binary-scoped because the match is keyed by `binary_id`
/// (exact test names can collide across binaries).
pub async fn discover(
    binaries: &[SelectedBinary],
) -> (ImagesOutcome, Vec<(String, Vec<QosEntry>)>) {
    let mut seen: BTreeSet<DedupKey> = BTreeSet::new();
    let mut images: Vec<DevImageEntry> = Vec::new();
    let mut qos_by_binary: Vec<(String, Vec<QosEntry>)> = Vec::new();

    for bin in binaries {
        match dump_one(bin).await {
            Ok(Dumped { dev, qos }) => {
                for d in dev {
                    if seen.insert(DedupKey::from(&d)) {
                        images.push(d);
                    }
                }
                if !qos.is_empty() {
                    qos_by_binary.push((bin.binary_id.clone(), qos));
                }
            }
            Err(detail) => {
                // On failure the caller aborts before lowering, so the
                // partial QoS accumulation is moot.
                return (
                    ImagesOutcome::Failed {
                        detail: format!("{}: {detail}", bin.binary_id),
                    },
                    qos_by_binary,
                );
            }
        }
    }
    (ImagesOutcome::Discovered { images }, qos_by_binary)
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
    dockerfile: String,
    context: String,
    features: Vec<String>,
}

impl From<&DevImageEntry> for DedupKey {
    fn from(d: &DevImageEntry) -> Self {
        let mut features = d.features.clone();
        features.sort();
        DedupKey {
            repo: d.repo.clone(),
            dockerfile: d.dockerfile.clone(),
            context: d.context.clone(),
            features,
        }
    }
}

fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}
