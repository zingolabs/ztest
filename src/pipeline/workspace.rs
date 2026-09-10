//! Pre-compile gate: cwd's workspace must link `ztest`, or no inventory dump can answer.
//!
//! - Dump hook = `#[ctor]` inside `ztest` ([`crate::inventory`]) → binary w/o `ztest` linked
//!   runs libtest instead of dumping
//! - `cargo metadata --no-deps` (~20ms) ahead of compiling a whole unrelated workspace
//! - Direct deps of members only (every ztest test crate names `ztest` itself)
//! - Metadata failure = no verdict (nextest surfaces cargo's error, the dump parse backs this)

use std::path::{Path, PathBuf};

use super::profiles;

/// Workspace none of whose members depend on `ztest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unlinked {
    pub workspace_root: PathBuf,
    /// Workspaces elsewhere in the repo declaring a `ztest` dependency (`cd` targets)
    pub linked_workspaces: Vec<PathBuf>,
}

/// `Some` = veto with the `cd` targets, `None` = linked or undecidable
pub fn check() -> Option<Unlinked> {
    let meta = profiles::cargo_metadata().ok()?;
    if links_ztest(&meta) {
        return None;
    }
    let workspace_root = PathBuf::from(meta["workspace_root"].as_str()?);
    let linked_workspaces = linked_workspaces(&workspace_root);
    Some(Unlinked { workspace_root, linked_workspaces })
}

fn links_ztest(meta: &serde_json::Value) -> bool {
    meta["packages"].as_array().is_some_and(|packages| {
        packages.iter().any(|package| {
            package["dependencies"]
                .as_array()
                .is_some_and(|deps| deps.iter().any(|dep| dep["name"].as_str() == Some("ztest")))
        })
    })
}

/// Text match over manifests (candidates only, same trade as
/// [`profiles::workspaces_with_profiles`])
fn linked_workspaces(from: &Path) -> Vec<PathBuf> {
    profiles::workspaces_where(from, |path| {
        path.file_name().is_some_and(|name| name == "Cargo.toml")
            && std::fs::read_to_string(path).is_ok_and(|manifest| declares_ztest(&manifest))
    })
}

/// `ztest = …` at line start (plain, `{ workspace = true }`, or `[workspace.dependencies]`)
fn declares_ztest(manifest: &str) -> bool {
    manifest.lines().any(|line| {
        line.trim_start()
            .strip_prefix("ztest")
            .is_some_and(|rest| rest.trim_start().starts_with('='))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(deps: &[&str]) -> serde_json::Value {
        let deps: Vec<serde_json::Value> =
            deps.iter().map(|name| serde_json::json!({ "name": name })).collect();
        serde_json::json!({
            "workspace_root": "/repo",
            "packages": [
                { "name": "helper", "dependencies": [{ "name": "serde" }] },
                { "name": "suite", "dependencies": deps },
            ],
        })
    }

    #[test]
    fn member_depending_on_ztest_links() {
        assert!(links_ztest(&metadata(&["tokio", "ztest"])));
    }

    #[test]
    fn members_without_ztest_do_not_link() {
        assert!(!links_ztest(&metadata(&["tokio", "ztest_attr"])));
        assert!(!links_ztest(&serde_json::json!({ "workspace_root": "/repo" })));
    }

    #[test]
    fn manifest_dependency_spellings() {
        assert!(declares_ztest("[dependencies]\nztest = \"0.1\"\n"));
        assert!(declares_ztest("[dev-dependencies]\n  ztest={ workspace = true }\n"));
        assert!(declares_ztest("[workspace.dependencies]\nztest = { path = \"..\" }\n"));
        assert!(!declares_ztest("[dependencies]\nztest_attr = \"0.1\"\n"));
        assert!(!declares_ztest("# ztest = \"0.1\"\nserde = \"1\"\n"));
    }
}
