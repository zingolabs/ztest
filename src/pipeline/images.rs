//! Phase C — dev-image inventory discovery.
//!
//! For each test binary with ≥1 selected test, spawn it with
//! `ZTEST_DUMP_INVENTORY=1` (and `--`-terminated argv so no test
//! filter args confuse the harness — the ctor hook exits before
//! libtest reads argv anyway, but cleaner). Each binary writes one
//! JSON-serialized `DevImageEntry` per line to stdout, then exits 0.
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

use crate::inventory::DevImageEntry;
use crate::pipeline::build::SelectedBinary;

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
/// each is sub-100ms — and serial keeps the output legible). Returns
/// a deduped list ready for the docker pipeline.
pub async fn discover(binaries: &[SelectedBinary]) -> ImagesOutcome {
    let mut seen: BTreeSet<DedupKey> = BTreeSet::new();
    let mut images: Vec<DevImageEntry> = Vec::new();

    for bin in binaries {
        match dump_one(bin).await {
            Ok(decls) => {
                for d in decls {
                    if seen.insert(DedupKey::from(&d)) {
                        images.push(d);
                    }
                }
            }
            Err(detail) => {
                return ImagesOutcome::Failed {
                    detail: format!("{}: {detail}", bin.binary_id),
                };
            }
        }
    }
    ImagesOutcome::Discovered { images }
}

/// Spawn one binary with `ZTEST_DUMP_INVENTORY=1` and parse the
/// JSON-lines stdout.
async fn dump_one(bin: &SelectedBinary) -> Result<Vec<DevImageEntry>, String> {
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
        let mut decls = Vec::new();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<DevImageEntry>(&line) {
                Ok(d) => decls.push(d),
                Err(e) => {
                    return Err(format!("malformed inventory line `{line}`: {e}"));
                }
            }
        }
        Ok(decls)
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
    let decls = stdout_task
        .await
        .map_err(|e| format!("stdout join: {e}"))?
        .map_err(|e| format!("{e}\nstderr:\n{}", tail(&stderr_tail, 20)))?;

    if !status.success() {
        return Err(format!(
            "binary exited {} during inventory dump; stderr:\n{}",
            status.code().map(|c| c.to_string()).unwrap_or_else(|| "?".into()),
            tail(&stderr_tail, 20)
        ));
    }
    Ok(decls)
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
