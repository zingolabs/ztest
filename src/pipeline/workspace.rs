//! Pre-compile gate: cwd's workspace must link `ztest`, or no inventory dump can answer.
//!
//! - Dump hook = `#[ctor]` inside `ztest` ([`crate::inventory`]) → binary w/o `ztest` linked
//!   runs libtest instead of dumping
//! - `cargo metadata --no-deps` (~20ms) ahead of compiling a whole unrelated workspace
//! - Metadata failure = no verdict (nextest surfaces cargo's error, the dump parse backs this)

use std::path::PathBuf;

use super::profiles;

/// `Some(workspace_root)` = no member depends on `ztest`; `None` = linked or undecidable
pub fn unlinked_workspace() -> Option<PathBuf> {
    let meta = profiles::cargo_metadata().ok()?;
    if links_ztest(&meta) {
        return None;
    }
    meta["workspace_root"].as_str().map(PathBuf::from)
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
}
