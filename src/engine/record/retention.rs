//! Keeping the recordings cache bounded.
//!
//! - Best-effort GC after each new recording, pruning past an age/count/total-size budget
//! - nextest's defaults (30 days / 100 runs / 1 GiB), newest kept
//! - Also drives `ztest store prune`

use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

use super::locate::{list_runs_in, workspace_records_dir};

/// Retention budget; `None` on a field disables that limit
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub max_age: Option<Duration>,
    pub max_runs: Option<usize>,
    pub max_bytes: Option<u64>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age: Some(Duration::from_secs(30 * 24 * 60 * 60)),
            max_runs: Some(100),
            max_bytes: Some(1024 * 1024 * 1024),
        }
    }
}

/// Prune recordings for `workspace` past `policy`, newest kept; returns the delete count.
/// Best-effort — a failed list or delete is ignored (the stale run reappears next sweep)
pub fn gc(workspace: &Path, policy: RetentionPolicy) -> usize {
    match workspace_records_dir(workspace) {
        Ok(root) => gc_in(&root, policy),
        Err(_) => 0,
    }
}

/// [`gc`] against a specific records-root → tests point at a temp dir, not the user cache
pub fn gc_in(root: &Path, policy: RetentionPolicy) -> usize {
    let Ok(runs) = list_runs_in(root) else {
        return 0;
    };
    let now = SystemTime::now();
    let mut kept_bytes = 0u64;
    let mut deleted = 0;
    // `runs` newest-first → index = newest-rank, and the size budget fills from that end
    for (rank, run) in runs.iter().enumerate() {
        let too_old = policy
            .max_age
            .is_some_and(|age| now.duration_since(run.modified).is_ok_and(|d| d > age));
        let over_count = policy.max_runs.is_some_and(|n| rank >= n);
        let size = dir_size(&run.dir);
        let over_bytes = policy.max_bytes.is_some_and(|cap| kept_bytes.saturating_add(size) > cap);

        if too_old || over_count || over_bytes {
            if fs::remove_dir_all(&run.dir).is_ok() {
                deleted += 1;
            }
        } else {
            kept_bytes = kept_bytes.saturating_add(size);
        }
    }
    deleted
}

/// Total size in bytes of every file under `dir`
pub fn dir_size(dir: &Path) -> u64 {
    walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rooted in a temp dir, never the real user cache → the suite can't touch `~/.cache`
    fn root_with_runs(tag: &str, n: usize) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "ztest-retention-{tag}-{}-{}",
            std::process::id(),
            blake3::hash(format!("{:?}", std::thread::current().id()).as_bytes()).to_hex()
        ));
        for i in 0..n {
            let dir = root.join(format!("run-{i}"));
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("run.log.zst"), b"x").unwrap();
        }
        root
    }

    #[test]
    fn max_runs_keeps_the_budget() {
        let root = root_with_runs("count", 5);
        let policy = RetentionPolicy { max_age: None, max_runs: Some(2), max_bytes: None };
        let deleted = gc_in(&root, policy);
        assert_eq!(deleted, 3, "5 runs, keep 2 → delete 3");
        assert_eq!(list_runs_in(&root).unwrap().len(), 2);
    }

    #[test]
    fn no_limits_deletes_nothing() {
        let root = root_with_runs("nolimit", 3);
        let policy = RetentionPolicy { max_age: None, max_runs: None, max_bytes: None };
        assert_eq!(gc_in(&root, policy), 0);
        assert_eq!(list_runs_in(&root).unwrap().len(), 3);
        fs::remove_dir_all(&root).ok();
    }
}
