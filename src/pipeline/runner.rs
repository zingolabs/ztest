//! Runner-image compile: test binaries + inventory, from one multi-stage build.
//!
//! - [`docker/runner.Dockerfile`](RUNNER_DOCKERFILE) built twice — `runner` (the pushed
//!   image) and `inventory-export` (`list.json` + framed `inventory.jsonl`, layer reuse →
//!   no re-compile)
//! - *Where* it builds is [`ImageProvider`](image::ImageProvider)'s to know; this module
//!   only says what to build and folds the result
//! - Source ships as the build context, so the compile happens wherever the build does

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use crate::backends::image::{
    self, DevSource,
    buildpod::{TempDir, shell_quote, tail},
};
use crate::error::PipelineError;
use crate::inventory::{DevImageEntry, QosEntry};
use crate::pipeline::build::{self, BuildOutcome, SelectedBinary};
use crate::pipeline::images::{self, Dumped};
use crate::resource::Cx;

/// Runner build recipe (compile → inventory → runner)
const RUNNER_DOCKERFILE: &str = include_str!("../../docker/runner.Dockerfile");

/// In-image source root (`WORKDIR /src` + `COPY . .`). Inventory ctors resolve `dev!`/seed
/// paths under here → come back `/src`-rooted, re-homed by [`rehome_dump`]
const IMAGE_SRC_ROOT: &str = "/src";

/// Same shapes as a laptop compile + Phase-C dump, plus the published runner ref
#[derive(Debug)]
pub struct CompileOutcome {
    pub build: BuildOutcome,
    pub dump: images::DumpOutcome,
    pub qos_by_binary: Vec<(String, Vec<QosEntry>)>,
    pub runner_image_ref: String,
}

impl CompileOutcome {
    fn binary_count(&self) -> usize {
        match &self.build {
            BuildOutcome::Ok { selected_binaries, .. } => selected_binaries.len(),
            _ => 0,
        }
    }
}

/// Compile progress transition. [`compile`] emits + times; the CLI owns all formatting
#[derive(Debug)]
pub enum Phase<'a> {
    Start(&'a str),
    Done { label: &'a str, dur: Duration },
    Note(&'a str),
}

/// Phase-transition sink. `mut` (callers update panel state)
pub type PhaseSink<'a> = &'a mut dyn FnMut(Phase<'_>);

/// - `list_args` = the `cargo nextest list` argv every path passes
/// - `run_id` tags the published runner image per run
pub async fn compile(
    cx: &Cx,
    list_args: &[String],
    run_id: &str,
    mut on_phase: Option<PhaseSink<'_>>,
) -> Result<CompileOutcome, PipelineError> {
    let mut emit = |ev: Phase<'_>| {
        if let Some(cb) = on_phase.as_deref_mut() {
            cb(ev);
        }
    };

    let provider = image::from_env();
    let src = SourceLayout::resolve()?;
    // Quoted per arg, not joined: the Dockerfile `eval`s NEXTEST_ARGS, so `-E test(=x)`
    // unquoted arrives as broken tokens & silently selects nothing
    let nextest_args = list_args.iter().map(|a| shell_quote(a)).collect::<Vec<_>>().join(" ");
    let build_args = vec![
        ("NEXTEST_ARGS".to_string(), nextest_args),
        ("WORKSPACE_REL".to_string(), src.workspace_rel.to_string_lossy().into_owned()),
    ];
    let context = image::Context { root: src.ancestor.clone(), repos: src.repos.clone() };
    let request = |target: &str, output: image::Output| image::BuildRequest {
        context: context.clone(),
        dockerfile: image::Dockerfile::Text(RUNNER_DOCKERFILE.to_string()),
        target: Some(target.to_string()),
        build_args: build_args.clone(),
        output,
    };

    // Compile happens inside this build's `compile` stage
    emit(Phase::Start("building the runner image"));
    let t = Instant::now();
    let tag = format!("{}:dev-{run_id}", crate::naming::RUNNER_REPO);
    let built = provider
        .build(cx, &request("runner", image::Output::Image { tag }))
        .await
        .map_err(|e| PipelineError(format!("runner image build failed: {e}")))?;
    let Some(runner_image_ref) = built.into_image() else {
        return Err("runner build produced no image reference".into());
    };
    emit(Phase::Done { label: "runner image built + published", dur: t.elapsed() });

    emit(Phase::Start("dumping test inventory"));
    let t = Instant::now();
    let inv = TempDir::new("ztest-inv")?;
    let dest = inv.path().to_path_buf();
    provider
        .build(cx, &request("inventory-export", image::Output::Files { dest }))
        .await
        .map_err(|e| PipelineError(format!("inventory export failed: {e}")))?;
    let list_json = std::fs::read_to_string(inv.path().join("list.json"))
        .map_err(|e| format!("read exported list.json: {e}"))?;
    let inventory = std::fs::read_to_string(inv.path().join("inventory.jsonl"))
        .map_err(|e| format!("read exported inventory.jsonl: {e}"))?;

    let outcome = assemble_outcome(&list_json, &inventory, &src.ancestor, runner_image_ref)?;
    emit(Phase::Done {
        label: &format!("inventory dumped ({} binaries)", outcome.binary_count()),
        dur: t.elapsed(),
    });
    emit(Phase::Note(&format!("runner image ready: {}", outcome.runner_image_ref)));
    Ok(outcome)
}

/// Fold a build's `list.json` + framed `inventory.jsonl` into the run pipeline's outcome
fn assemble_outcome(
    list_json: &str,
    inventory: &str,
    ancestor: &Path,
    runner_image_ref: String,
) -> Result<CompileOutcome, PipelineError> {
    let build = build::parse_list_summary(list_json.as_bytes())
        .map_err(|e| format!("parse nextest list: {e}"))?;
    let BuildOutcome::Ok { selected_binaries, .. } = &build else {
        return Err("nextest list produced no selection".into());
    };
    if selected_binaries.is_empty() {
        return Err("nextest list selected no test binaries".into());
    }

    let sections = split_dumps_by_name(inventory, selected_binaries)?;
    let mut dumps: Vec<Dumped> = Vec::with_capacity(selected_binaries.len());
    for (bin, (chunk, rc)) in selected_binaries.iter().zip(sections) {
        if rc != 0 {
            return Err(format!(
                "inventory dump of {} failed (exit {rc}):\n{}",
                bin.binary_id,
                tail(&chunk, 40)
            )
            .into());
        }
        let dumped = images::parse_inventory(&chunk)
            .map_err(|e| format!("parse inventory of {}: {e}", bin.binary_id))?;
        dumps.push(dumped);
    }
    let (mut dump, qos_by_binary) = images::assemble(selected_binaries, dumps);
    // `dev!` images + seeds provisioned laptop-side → re-home captured `/src/…` contexts
    rehome_dump(&mut dump, ancestor);

    Ok(CompileOutcome { build, dump, qos_by_binary, runner_image_ref })
}

/// Demux exported `inventory.jsonl` → `(stdout, exit_code)` per selected binary, in
/// `selected` order.
///
/// - Keyed by binary FILENAME (Dockerfile frames `ZTEST_DUMP_BEGIN/END <name> rc=<code>`)
/// - Missing block = error (truncated export must fail loud, never drop a binary)
fn split_dumps_by_name(
    out: &str,
    selected: &[SelectedBinary],
) -> Result<Vec<(String, i32)>, PipelineError> {
    use std::collections::HashMap;
    let mut by_name: HashMap<String, (String, i32)> = HashMap::new();
    let mut cur: Option<(String, String)> = None;
    for line in out.lines() {
        if let Some(rest) = line.strip_prefix("ZTEST_DUMP_BEGIN ") {
            cur = Some((rest.trim().to_string(), String::new()));
        } else if let Some(rest) = line.strip_prefix("ZTEST_DUMP_END ") {
            let mut it = rest.split_whitespace();
            let name =
                it.next().ok_or_else(|| format!("bad dump end marker: {line:?}"))?.to_string();
            let rc: i32 = it
                .next()
                .and_then(|s| s.strip_prefix("rc="))
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);
            match cur.take() {
                Some((n, buf)) if n == name => {
                    by_name.insert(name, (buf, rc));
                }
                _ => return Err(format!("mismatched dump markers at {name:?}").into()),
            }
        } else if let Some((_, buf)) = cur.as_mut() {
            buf.push_str(line);
            buf.push('\n');
        }
    }
    selected
        .iter()
        .map(|b| {
            let name = b
                .binary_path
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            by_name
                .remove(&name)
                .ok_or_else(|| PipelineError(format!("inventory: no block for {name:?}")))
        })
        .collect()
}

// ── Source set resolution ─────────────────────────────────────────────

/// `ancestor` = common-ancestor dir of the backing git `repos` = tar root & re-home base.
///
/// - Whole repos ship, not package subtrees: `dev!`/`mount_*`/seed paths resolve against
///   `CARGO_MANIFEST_DIR` and routinely escape the crate into its repo
/// - Scoping to repos that hold packages keeps sibling repos out
#[derive(Debug)]
struct SourceLayout {
    ancestor: PathBuf,
    workspace_rel: PathBuf,
    repos: Vec<PathBuf>,
}

impl SourceLayout {
    fn resolve() -> Result<Self, PipelineError> {
        let meta = cargo_metadata()?;
        let workspace_root =
            meta["workspace_root"].as_str().ok_or("cargo metadata: no workspace_root")?;
        let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
        dirs.insert(PathBuf::from(workspace_root));
        if let Some(pkgs) = meta["packages"].as_array() {
            for p in pkgs {
                if !p["source"].is_null() {
                    continue;
                }
                if let Some(mp) = p["manifest_path"].as_str()
                    && let Some(dir) = Path::new(mp).parent()
                {
                    dirs.insert(dir.to_path_buf());
                }
            }
        }
        // Repo, not package dir, is the shipping unit (see struct docs)
        let mut repos: BTreeSet<PathBuf> = BTreeSet::new();
        for d in &dirs {
            repos.insert(image::buildpod::git_repo_root(d)?);
        }
        let ancestor = common_ancestor(&repos).ok_or("cannot derive a common source ancestor")?;
        if ancestor.parent().is_none() || ancestor == Path::new("/home") {
            return Err(format!("source ancestor too wide: {}", ancestor.display()).into());
        }
        let workspace_rel = Path::new(workspace_root)
            .strip_prefix(&ancestor)
            .map_err(|_| "workspace root not under the source ancestor")?
            .to_path_buf();
        Ok(Self { ancestor, workspace_rel, repos: repos.into_iter().collect() })
    }
}

/// [`IMAGE_SRC_ROOT`] → laptop `ancestor` for laptop-provisioned source paths.
///
/// - Local (path) sources only; per-binary test paths stay pod-side
/// - Seeds + their dep edges **never** re-homed (named by oid = same everywhere)
fn rehome_dump(dump: &mut images::DumpOutcome, ancestor: &Path) {
    let images::DumpOutcome::Discovered {
        images,
        seeds: _,
        images_by_binary,
        deps_by_binary: _,
        sync_tests: _,
        sync_by_binary: _,
    } = dump
    else {
        return;
    };
    for e in images.iter_mut() {
        rehome_dev(e, ancestor);
    }
    for (_, es) in images_by_binary.iter_mut() {
        for e in es.iter_mut() {
            rehome_dev(e, ancestor);
        }
    }
}

fn rehome_dev(e: &mut DevImageEntry, ancestor: &Path) {
    if let DevSource::Local { dockerfile, context } = &mut e.source {
        *dockerfile = rehome_path(dockerfile, ancestor);
        *context = rehome_path(context, ancestor);
    }
}

fn rehome_path(p: &Path, ancestor: &Path) -> PathBuf {
    match p.strip_prefix(IMAGE_SRC_ROOT) {
        Ok(rel) => ancestor.join(rel),
        Err(_) => p.to_path_buf(),
    }
}

fn cargo_metadata() -> Result<serde_json::Value, PipelineError> {
    let out = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1"])
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| format!("run cargo metadata: {e}"))?;
    if !out.status.success() {
        return Err("cargo metadata failed".into());
    }
    serde_json::from_slice(&out.stdout).map_err(|e| format!("parse cargo metadata: {e}").into())
}

fn common_ancestor(dirs: &BTreeSet<PathBuf>) -> Option<PathBuf> {
    let mut iter = dirs.iter();
    let mut acc: PathBuf = iter.next()?.clone();
    for d in iter {
        while !d.starts_with(&acc) {
            acc = acc.parent()?.to_path_buf();
        }
    }
    Some(acc)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bin(id: &str, path: &str) -> SelectedBinary {
        SelectedBinary {
            binary_id: id.to_string(),
            binary_path: PathBuf::from(path),
            cwd: PathBuf::from("/src"),
            selected_tests: vec![],
        }
    }

    #[test]
    fn split_dumps_by_name_maps_blocks_to_selected_order() {
        // Emitted out of order; demux returns `selected` order, keyed by binary file name
        let out = "\nZTEST_DUMP_BEGIN beta-xyz\nB\nZTEST_DUMP_END beta-xyz rc=0\n\
                   \nZTEST_DUMP_BEGIN alpha-abc\n{}\nZTEST_DUMP_END alpha-abc rc=0\n";
        let selected = [
            bin("pkg::alpha", "/cache/target/debug/deps/alpha-abc"),
            bin("pkg::beta", "/cache/target/debug/deps/beta-xyz"),
        ];
        let s = split_dumps_by_name(out, &selected).expect("both blocks present");
        assert_eq!(s[0], ("{}\n".to_string(), 0));
        assert_eq!(s[1], ("B\n".to_string(), 0));
    }

    #[test]
    fn split_dumps_by_name_errors_on_missing_binary() {
        let out = "\nZTEST_DUMP_BEGIN alpha-abc\n{}\nZTEST_DUMP_END alpha-abc rc=0\n";
        let selected = [bin("pkg::alpha", "/x/alpha-abc"), bin("pkg::beta", "/x/beta-xyz")];
        assert!(split_dumps_by_name(out, &selected).is_err());
    }

    #[test]
    fn common_ancestor_of_sibling_repos() {
        let dirs =
            [PathBuf::from("/home/u/proj/zaino/live-tests"), PathBuf::from("/home/u/proj/ztest")]
                .into_iter()
                .collect();
        assert_eq!(common_ancestor(&dirs), Some(PathBuf::from("/home/u/proj")));
    }

    #[test]
    fn rehome_maps_image_src_to_ancestor() {
        let anc = Path::new("/home/u/proj");
        assert_eq!(
            rehome_path(Path::new("/src/zaino/live-tests/clientless"), anc),
            PathBuf::from("/home/u/proj/zaino/live-tests/clientless")
        );
        assert_eq!(
            rehome_path(Path::new("/some/git/cache/ctx"), anc),
            PathBuf::from("/some/git/cache/ctx")
        );
    }
}
