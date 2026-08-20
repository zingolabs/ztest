//! Pre-compile discovery of `#[ztest::sync_test]` profiles.
//!
//! - Link-time `inventory` readable only by running a built binary → name checks cost a full build
//! - Here: `cargo metadata --no-deps` (~20ms) + `syn` over test roots (~100ms) = gate + catalogue
//! - Attribute only: no images/seeds/pods, no feature resolution (`resolve_target` = authority)
//! - Blind spots possible ([`BlindSpot`]) → miss = suggestion, never veto

use std::path::{Path, PathBuf};
use std::process::Stdio;

use syn::spanned::Spanned as _;
use ztest_attr::{SyncTestArgs, is_sync_test_path};

/// One `#[ztest::sync_test(..)]` declaration, as written in source.
///
/// - `line` = attribute, not fn
/// - `gated` = under `#[cfg(..)]` → presence *and* absence non-authoritative
#[derive(Debug, Clone)]
pub struct ProfileStub {
    pub name: String,
    pub description: String,
    pub subject: String,
    pub qos: String,
    pub footprint: Option<ztest_attr::Footprint>,
    pub timeout: String,
    pub tags: Vec<String>,
    pub package: String,
    pub target: String,
    pub file: PathBuf,
    pub line: usize,
    pub gated: bool,
}

impl ProfileStub {
    /// Compiles this profile's test binary only (unrelated broken targets never build).
    ///
    /// - Cargo-level required: `-E` filtersets pick tests to run, not crates to build
    pub fn cargo_args(&self) -> Vec<String> {
        vec!["-p".to_string(), self.package.clone(), "--test".to_string(), self.target.clone()]
    }
}

/// Region the scan could not see through (silent omission > admitted incompleteness).
#[derive(Debug, Clone)]
pub struct BlindSpot {
    pub file: PathBuf,
    pub reason: String,
}

/// One workspace scan.
///
/// - `profiles` sorted by name
/// - `branch` reaches the listing header (zero profiles ≈ wrong branch)
#[derive(Debug, Clone)]
pub struct Scan {
    pub profiles: Vec<ProfileStub>,
    pub workspace_root: PathBuf,
    pub branch: Option<String>,
    pub blind_spots: Vec<BlindSpot>,
}

/// Lookup returning other than exactly one profile.
///
/// - `Empty` ≠ `Unknown`: fix is `git checkout`, not a corrected spelling
#[derive(Debug, Clone)]
pub enum Miss {
    Empty,
    Unknown { suggestions: Vec<String> },
    Ambiguous { found: Vec<ProfileStub> },
}

impl Scan {
    pub fn find(&self, name: &str) -> Result<&ProfileStub, Miss> {
        let hits: Vec<&ProfileStub> = self.profiles.iter().filter(|p| p.name == name).collect();
        match hits.as_slice() {
            [one] => Ok(one),
            [] if self.profiles.is_empty() => Err(Miss::Empty),
            [] => Err(Miss::Unknown { suggestions: self.suggest(name) }),
            many => Err(Miss::Ambiguous { found: many.iter().map(|p| (*p).clone()).collect() }),
        }
    }

    /// Unparsed file | any `cfg`-gated decl → source not the whole story (compiler settles it)
    pub fn is_uncertain(&self) -> bool {
        !self.blind_spots.is_empty() || self.profiles.iter().any(|p| p.gated)
    }

    fn suggest(&self, name: &str) -> Vec<String> {
        // tolerance scales with query (short names must not match everything)
        let budget = (name.len() / 3).max(2);
        let mut scored: Vec<(usize, &str)> = self
            .profiles
            .iter()
            .map(|p| (edit_distance(name, &p.name), p.name.as_str()))
            .filter(|(d, _)| *d <= budget)
            .collect();
        scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));
        scored.into_iter().map(|(_, n)| n.to_string()).collect()
    }
}

/// Cargo workspace containing cwd.
pub fn scan() -> Result<Scan, crate::error::PipelineError> {
    let meta = cargo_metadata()?;
    let workspace_root = meta
        .get("workspace_root")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .ok_or_else(|| "cargo metadata has no workspace_root".to_string())?;

    let packages = meta
        .get("packages")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "cargo metadata has no packages".to_string())?;

    let mut profiles = Vec::new();
    let mut blind_spots = Vec::new();

    for pkg in packages {
        let Some(pkg_name) = pkg.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(targets) = pkg.get("targets").and_then(|v| v.as_array()) else {
            continue;
        };
        for target in targets {
            // `tests/*.rs` only (profile → libtest entry, invoked `<bin> --exact <test>`)
            // lib unit tests would need cross-file `mod` resolution → recorded as blind spots
            let is_test = target
                .get("kind")
                .and_then(|v| v.as_array())
                .is_some_and(|ks| ks.iter().any(|k| k.as_str() == Some("test")));
            if !is_test {
                continue;
            }
            let (Some(target_name), Some(src_path)) = (
                target.get("name").and_then(|v| v.as_str()),
                target.get("src_path").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            let path = PathBuf::from(src_path);
            let found = scan_file(&path, pkg_name, target_name, &mut blind_spots);
            profiles.extend(found);
        }
    }

    profiles.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(Scan { profiles, branch: git_branch(&workspace_root), workspace_root, blind_spots })
}

fn scan_file(
    path: &Path,
    package: &str,
    target: &str,
    blind_spots: &mut Vec<BlindSpot>,
) -> Vec<ProfileStub> {
    let source = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            blind_spots
                .push(BlindSpot { file: path.to_path_buf(), reason: format!("unreadable: {e}") });
            return Vec::new();
        }
    };
    let file = match syn::parse_file(&source) {
        Ok(f) => f,
        Err(e) => {
            blind_spots
                .push(BlindSpot { file: path.to_path_buf(), reason: format!("unparsable: {e}") });
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    visit_items(&file.items, false, path, package, target, &mut out, blind_spots);
    out
}

#[allow(clippy::too_many_arguments)]
fn visit_items(
    items: &[syn::Item],
    gated: bool,
    path: &Path,
    package: &str,
    target: &str,
    out: &mut Vec<ProfileStub>,
    blind_spots: &mut Vec<BlindSpot>,
) {
    for item in items {
        match item {
            syn::Item::Fn(f) => {
                let gated = gated || has_cfg(&f.attrs);
                for attr in &f.attrs {
                    if !is_sync_test_path(attr.path()) {
                        continue;
                    }
                    match attr.parse_args::<SyncTestArgs>() {
                        Ok(args) => out.push(stub(
                            &args,
                            package,
                            target,
                            path,
                            attr.span().start().line,
                            gated,
                        )),
                        Err(e) => blind_spots.push(BlindSpot {
                            file: path.to_path_buf(),
                            reason: format!("sync_test attribute did not parse: {e}"),
                        }),
                    }
                }
            }
            syn::Item::Mod(m) => match &m.content {
                Some((_, items)) => visit_items(
                    items,
                    gated || has_cfg(&m.attrs),
                    path,
                    package,
                    target,
                    out,
                    blind_spots,
                ),
                // not followed = rustc module resolution unimplemented (`#[path]`, foo.rs vs
                // foo/mod.rs, nesting)
                None => blind_spots.push(BlindSpot {
                    file: path.to_path_buf(),
                    reason: format!("out-of-line `mod {};` not followed", m.ident),
                }),
            },
            _ => {}
        }
    }
}

fn stub(
    args: &SyncTestArgs,
    package: &str,
    target: &str,
    path: &Path,
    line: usize,
    gated: bool,
) -> ProfileStub {
    ProfileStub {
        name: args.name.value(),
        description: args.description.value(),
        subject: args.subject.to_string(),
        qos: args.qos.to_string(),
        footprint: args.footprint,
        timeout: args.timeout.value(),
        tags: args.tags.iter().map(syn::LitStr::value).collect(),
        package: package.to_string(),
        target: target.to_string(),
        file: path.to_path_buf(),
        line,
        gated,
    }
}

fn has_cfg(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| a.path().is_ident("cfg"))
}

/// Directories elsewhere in this repo whose own workspace declares profiles, for the
/// zero-profile message.
///
/// - [`scan`] only ever sees cwd's workspace → an `exclude`d or sibling one reads as "none
///   anywhere", and the fix is a `cd` the error cannot otherwise name
/// - Text match, not `syn`: candidates only, and running `cargo metadata` per workspace to
///   confirm costs more than the whole error path
pub fn workspaces_with_profiles(from: &Path) -> Vec<PathBuf> {
    let Some(repo) = git_toplevel(from) else {
        return Vec::new();
    };
    let mut hits = Vec::new();
    let mut pending = vec![repo.clone()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                if name.starts_with('.') || name == "target" {
                    continue;
                }
                pending.push(path);
            } else if path.extension().is_some_and(|e| e == "rs")
                && std::fs::read_to_string(&path).is_ok_and(|s| s.contains("sync_test"))
                && let Some(root) = enclosing_workspace(&path, &repo)
                && !hits.contains(&root)
            {
                hits.push(root);
            }
        }
    }
    hits.sort();
    hits
}

/// Nearest ancestor declaring `[workspace]`, bounded by `repo`
fn enclosing_workspace(file: &Path, repo: &Path) -> Option<PathBuf> {
    file.ancestors()
        .skip(1)
        .take_while(|d| d.starts_with(repo))
        .find(|dir| {
            std::fs::read_to_string(dir.join("Cargo.toml")).is_ok_and(|s| s.contains("[workspace]"))
        })
        .map(Path::to_path_buf)
}

fn git_toplevel(from: &Path) -> Option<PathBuf> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(from)
        .args(["rev-parse", "--show-toplevel"])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!path.is_empty()).then(|| PathBuf::from(path))
}

/// - `--no-deps`: skips resolution (seconds → ~20ms) + restricts `packages` to workspace members
/// - Not `remote_compile::cargo_metadata` (that one needs the full graph for `SourceLayout`)
fn cargo_metadata() -> Result<serde_json::Value, crate::error::PipelineError> {
    let out = std::process::Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("run `cargo metadata` (is `cargo` on PATH?): {e}"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let detail = err.lines().next().unwrap_or("cargo metadata failed");
        return Err(format!("cargo metadata failed: {detail}").into());
    }
    serde_json::from_slice(&out.stdout).map_err(|e| format!("parse cargo metadata: {e}").into())
}

fn git_branch(root: &Path) -> Option<String> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let name = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!name.is_empty()).then_some(name)
}

/// Levenshtein, two rows (inputs = identifiers, so clarity > allocation).
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() {
        return b.len();
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let sub = prev[j] + usize::from(ca != cb);
            cur[j + 1] = sub.min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_source(src: &str) -> (Vec<ProfileStub>, Vec<BlindSpot>) {
        let file = syn::parse_file(src).expect("test source parses");
        let mut out = Vec::new();
        let mut blind = Vec::new();
        visit_items(&file.items, false, Path::new("tests/t.rs"), "pkg", "t", &mut out, &mut blind);
        (out, blind)
    }

    #[test]
    fn finds_a_declaration_and_records_where_it_is() {
        let (found, blind) = scan_source(
            r#"
            #[ztest::needs(BLOSSOM)]
            #[ztest::sync_test(
                name = "zaino_index_construction",
                description = "builds an index",
                subject = indexer,
                timeout = "48h",
                qos = sync,
                tags = ["mainnet", "zaino"],
            )]
            async fn zaino_index_construction(run: SyncRunner) -> SyncOutcome {}
            "#,
        );
        assert!(blind.is_empty(), "unexpected blind spots: {blind:?}");
        assert_eq!(found.len(), 1);
        let p = &found[0];
        assert_eq!(p.name, "zaino_index_construction");
        assert_eq!(p.subject, "indexer");
        assert_eq!(p.qos, "sync");
        assert_eq!(p.timeout, "48h");
        assert_eq!(p.tags, ["mainnet", "zaino"]);
        assert!(!p.gated);
        // attribute, not fn
        assert_eq!(p.line, 3);
    }

    #[test]
    fn narrows_the_build_to_one_target() {
        let (found, _) = scan_source(
            r#"
            #[ztest::sync_test(name = "p", subject = indexer, qos = sync)]
            async fn p(run: SyncRunner) -> SyncOutcome {}
            "#,
        );
        assert_eq!(found[0].cargo_args(), ["-p", "pkg", "--test", "t"]);
    }

    #[test]
    fn ignores_the_attribute_named_in_a_doc_comment() {
        // both in-tree profiles name the attribute in docs above the real decl (textual scan
        // double-counts)
        let (found, _) = scan_source(
            r#"
            /// Set on the runner explicitly: `#[ztest::sync_test(timeout = ..)]`
            /// records the cap, and this is the only place it is written.
            const STALL: u32 = 1;
            "#,
        );
        assert!(found.is_empty());
    }

    #[test]
    fn finds_declarations_nested_in_inline_modules() {
        let (found, _) = scan_source(
            r#"
            mod inner {
                #[ztest::sync_test(name = "nested", subject = wallet, qos = sync)]
                async fn nested(run: SyncRunner) -> SyncOutcome {}
            }
            "#,
        );
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "nested");
    }

    #[test]
    fn flags_a_cfg_gated_declaration_as_uncertain() {
        let (found, _) = scan_source(
            r#"
            #[cfg(feature = "librustzcash")]
            #[ztest::sync_test(name = "gated", subject = wallet, qos = sync)]
            async fn gated(run: SyncRunner) -> SyncOutcome {}
            "#,
        );
        assert!(found[0].gated, "cfg-gated profile must be marked uncertain");
    }

    #[test]
    fn inherits_gating_from_an_enclosing_module() {
        let (found, _) = scan_source(
            r#"
            #[cfg(feature = "librustzcash")]
            mod inner {
                #[ztest::sync_test(name = "gated", subject = wallet, qos = sync)]
                async fn gated(run: SyncRunner) -> SyncOutcome {}
            }
            "#,
        );
        assert!(found[0].gated);
    }

    #[test]
    fn records_an_unfollowed_module_as_a_blind_spot() {
        let (_, blind) = scan_source("mod common;");
        assert_eq!(blind.len(), 1);
        assert!(blind[0].reason.contains("out-of-line"));
    }

    #[test]
    fn ignores_another_crates_attribute_of_the_same_name() {
        let (found, _) = scan_source(
            r#"
            #[other::sync_test(name = "x", subject = wallet, qos = sync)]
            async fn x() {}
            "#,
        );
        assert!(found.is_empty());
    }

    fn scan_with(names: &[&str]) -> Scan {
        Scan {
            profiles: names
                .iter()
                .map(|n| ProfileStub {
                    name: (*n).to_string(),
                    description: String::new(),
                    subject: "indexer".into(),
                    qos: "sync".into(),
                    footprint: None,
                    timeout: "48h".into(),
                    tags: Vec::new(),
                    package: "sync".into(),
                    target: "t".into(),
                    file: PathBuf::from("tests/t.rs"),
                    line: 1,
                    gated: false,
                })
                .collect(),
            workspace_root: PathBuf::from("/w"),
            branch: None,
            blind_spots: Vec::new(),
        }
    }

    #[test]
    fn resolves_a_known_name() {
        let s = scan_with(&["zaino_index_construction", "zaino_state_fetch_parity"]);
        assert_eq!(s.find("zaino_index_construction").expect("found").target, "t");
    }

    #[test]
    fn suggests_the_near_miss_that_actually_happens() {
        let s = scan_with(&["zaino_index_construction", "zaino_state_fetch_parity"]);
        // one dropped char = the realistic typo
        match s.find("zaino_index_constructon") {
            Err(Miss::Unknown { suggestions }) => {
                assert_eq!(
                    suggestions.first().map(String::as_str),
                    Some("zaino_index_construction")
                );
            }
            other => panic!("expected an unknown-name miss, got {other:?}"),
        }
    }

    #[test]
    fn offers_no_suggestion_for_an_unrelated_name() {
        let s = scan_with(&["zaino_index_construction"]);
        match s.find("wallet_send") {
            Err(Miss::Unknown { suggestions }) => assert!(suggestions.is_empty()),
            other => panic!("expected an unknown-name miss, got {other:?}"),
        }
    }

    #[test]
    fn distinguishes_an_empty_workspace_from_a_wrong_name() {
        // wrong-branch case (fix = `git checkout`, not a spelling correction)
        match scan_with(&[]).find("anything") {
            Err(Miss::Empty) => {}
            other => panic!("expected the empty-workspace miss, got {other:?}"),
        }
    }

    #[test]
    fn reports_a_duplicated_name_rather_than_picking_one() {
        let s = scan_with(&["dup", "dup"]);
        match s.find("dup") {
            Err(Miss::Ambiguous { found }) => assert_eq!(found.len(), 2),
            other => panic!("expected an ambiguity, got {other:?}"),
        }
    }

    #[test]
    fn uncertainty_tracks_gating_and_blind_spots() {
        assert!(!scan_with(&["a"]).is_uncertain());

        let mut gated = scan_with(&["a"]);
        gated.profiles[0].gated = true;
        assert!(gated.is_uncertain());

        let mut blind = scan_with(&["a"]);
        blind
            .blind_spots
            .push(BlindSpot { file: PathBuf::from("tests/t.rs"), reason: "unparsable".into() });
        assert!(blind.is_uncertain());
    }

    #[test]
    fn edit_distance_matches_known_values() {
        assert_eq!(edit_distance("", "abc"), 3);
        assert_eq!(edit_distance("abc", ""), 3);
        assert_eq!(edit_distance("abc", "abc"), 0);
        assert_eq!(edit_distance("kitten", "sitting"), 3);
    }
}
