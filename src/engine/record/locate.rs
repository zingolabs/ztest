//! Where recordings live and how a [`RunSelector`] resolves to one.
//!
//! Recordings sit under the per-user cache dir (`~/.cache/ztest/records` on
//! Linux), keyed by a hash of the workspace root — the same directory the `run`
//! and `replay` commands both derive from the current workspace, so a replay
//! finds what a run wrote regardless of the target dir or `cargo clean`.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use directories::ProjectDirs;

use super::RunSelector;

/// The workspace root: the furthest ancestor of the current directory that
/// still contains a `Cargo.toml` (the top of a cargo workspace), canonicalized.
/// Falls back to the current directory when none is found. Both `run` (at record
/// time) and `replay` call this, so the derived key matches.
pub fn current_workspace() -> io::Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    let start = cwd.canonicalize().unwrap_or(cwd);
    let mut root = None;
    for ancestor in start.ancestors() {
        if ancestor.join("Cargo.toml").is_file() {
            root = Some(ancestor.to_path_buf());
        }
    }
    Ok(root.unwrap_or(start))
}

/// A stable identity for a workspace: a hex blake3 of its canonical path. Used
/// as the per-workspace directory name so unrelated workspaces don't collide.
pub fn workspace_id(workspace: &Path) -> String {
    let bytes = workspace.as_os_str().as_encoded_bytes();
    blake3::hash(bytes).to_hex()[..32].to_string()
}

/// The root of all recordings for `workspace`.
pub fn workspace_records_dir(workspace: &Path) -> io::Result<PathBuf> {
    let dirs = ProjectDirs::from("io", "ztest", "ztest").ok_or_else(|| {
        io::Error::other("cannot determine a cache directory for recordings")
    })?;
    Ok(dirs
        .cache_dir()
        .join("records")
        .join(workspace_id(workspace)))
}

/// The directory for one specific run.
pub fn run_dir(workspace: &Path, run_id: &str) -> io::Result<PathBuf> {
    Ok(workspace_records_dir(workspace)?.join(run_id))
}

/// Resolve a selector to a concrete recording directory.
pub fn resolve(workspace: &Path, selector: &RunSelector) -> io::Result<PathBuf> {
    match selector {
        RunSelector::Path(p) => {
            if p.is_dir() {
                Ok(p.clone())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no recording at {}", p.display()),
                ))
            }
        }
        RunSelector::Latest => latest(workspace),
        RunSelector::Id(prefix) => by_prefix(workspace, prefix),
    }
}

/// One recorded run: its id and directory. Ordered newest-first by the caller.
#[derive(Debug, Clone)]
pub struct RunEntry {
    pub run_id: String,
    pub dir: PathBuf,
    pub modified: std::time::SystemTime,
}

/// Every recorded run for a workspace, newest first. Empty (not an error) when
/// the workspace has no recordings dir yet.
pub fn list_runs(workspace: &Path) -> io::Result<Vec<RunEntry>> {
    list_runs_in(&workspace_records_dir(workspace)?)
}

/// The recorded runs directly under `root` (a workspace's records dir), newest
/// first. Factored out so tests can point at a temp directory instead of the
/// real user cache.
pub fn list_runs_in(root: &Path) -> io::Result<Vec<RunEntry>> {
    let mut runs = Vec::new();
    let entries = match fs::read_dir(root) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(runs),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        // A directory is a complete recording only once its log exists.
        if !entry.path().join("run.log.zst").is_file() {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        runs.push(RunEntry {
            run_id: entry.file_name().to_string_lossy().into_owned(),
            dir: entry.path(),
            modified,
        });
    }
    runs.sort_by_key(|r| std::cmp::Reverse(r.modified));
    Ok(runs)
}

/// The most recently completed run.
fn latest(workspace: &Path) -> io::Result<PathBuf> {
    list_runs(workspace)?
        .into_iter()
        .next()
        .map(|r| r.dir)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no recorded runs for this workspace"))
}

/// The unique run whose id begins with `prefix`; errors when zero or several
/// match (nextest's unambiguous-prefix rule).
fn by_prefix(workspace: &Path, prefix: &str) -> io::Result<PathBuf> {
    let matches: Vec<RunEntry> = list_runs(workspace)?
        .into_iter()
        .filter(|r| r.run_id.starts_with(prefix))
        .collect();
    match matches.as_slice() {
        [] => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no recorded run matches '{prefix}'"),
        )),
        [one] => Ok(one.dir.clone()),
        many => {
            let ids: Vec<&str> = many.iter().map(|r| r.run_id.as_str()).collect();
            Err(io::Error::other(format!(
                "'{prefix}' is ambiguous; matches: {}",
                ids.join(", ")
            )))
        }
    }
}
