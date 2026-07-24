//! On-cluster compilation, buildkit-native: the runner image (compiled test
//! binaries + inventory) is produced by a single multi-stage `buildctl` build of
//! [`docker/runner.Dockerfile`](RUNNER_DOCKERFILE) in the ephemeral BuildKit pod
//! `ztest run` created. The laptop ships *source* as the build context; the
//! build compiles, assembles the runtime image (pushed), and exports the test
//! inventory (`list.json` + framed `inventory.jsonl`) via `--output type=local`,
//! which is `oc cp`'d back and parsed exactly as the laptop path parses a dump.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, AttachParams, TerminalSize};
use tokio::io::AsyncReadExt as _;

use crate::pipeline::build::{self, BuildOutcome, SelectedBinary};
use crate::pipeline::images::{self, Dumped};
use crate::resource::impls::buildkit::{BUILDKIT_CONTAINER, WORK_MOUNT};
use crate::resource::impls::policy::RUN_NAMESPACE;

/// The buildkit-native runner build recipe (compile → inventory → runner).
const RUNNER_DOCKERFILE: &str = include_str!("../../docker/runner.Dockerfile");

/// The in-image source root the runner Dockerfile compiles under
/// (`WORKDIR /src` + `COPY . .`). A binary's inventory ctor resolves `dev!`/seed
/// paths relative to its manifest dir under here, so they come back rooted at
/// `/src` and are re-homed to the laptop's source ancestor ([`rehome_dump`]).
const IMAGE_SRC_ROOT: &str = "/src";

/// Everything the run pipeline needs from an on-cluster compile — the same
/// shapes the laptop compile + Phase-C dump produce, plus the pushed runner ref.
#[derive(Debug)]
pub struct RemoteCompileOutcome {
    pub build: BuildOutcome,
    pub dump: images::DumpOutcome,
    pub qos_by_binary: Vec<(String, Vec<crate::inventory::QosEntry>)>,
    /// Pull reference of the runner image the build pushed.
    pub runner_image_ref: String,
}

/// Registry coordinates for the runner build's push target.
#[derive(Debug)]
pub struct BakeRefs {
    /// In-cluster registry repo the runner image is pushed to (`<reg>/<project>/<repo>`).
    pub runner_repo_ref: String,
}

/// A remote-compile progress transition. `compile_on_cluster` emits these and
/// measures the timing; the caller ([`crate::cli::run`]) owns all formatting.
#[derive(Debug)]
pub enum Phase<'a> {
    /// A new phase began; `label` names it for the live panel row and `•` line.
    Start(&'a str),
    /// The current phase finished after `dur` → a top-level `✓` line.
    Done { label: &'a str, dur: Duration },
    /// A sub-step result under the current phase → an indented `✓` line.
    Step { label: &'a str, dur: Duration },
    /// A terminal informational line (the pushed runner ref).
    Note(&'a str),
}

/// A phase-transition sink. Held mutably because the caller updates panel state.
pub type PhaseSink<'a> = &'a mut dyn FnMut(Phase<'_>);

/// Drive an on-cluster compile end to end against the ephemeral BuildKit `pod`.
/// `list_args` is the same `cargo nextest list` argv the laptop path uses;
/// `run_id` tags the pushed runner image uniquely per run. `compile_out` selects
/// how the runner build's BuildKit progress streams (a remote PTY into the
/// caller's emulator, or CI lines).
pub async fn compile_on_cluster(
    client: &kube::Client,
    pod: &str,
    list_args: &[String],
    refs: &BakeRefs,
    run_id: &str,
    compile_out: Option<CompileOut<'_>>,
    on_phase: Option<PhaseSink<'_>>,
) -> Result<RemoteCompileOutcome, String> {
    let mut on_phase = on_phase;
    let mut emit = |ev: Phase<'_>| {
        if let Some(cb) = on_phase.as_deref_mut() {
            cb(ev);
        }
    };
    let api: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);

    // 1. Resolve + ship the first-party source as the build context, plus the
    //    runner Dockerfile (embedded, so it need not be found in the synced tree).
    let src = SourceLayout::resolve()?;
    let ctx_dir = format!("{WORK_MOUNT}/ctx");
    emit(Phase::Start("syncing source to the build pod"));
    let t = Instant::now();
    ship_source(&src, pod, &ctx_dir)?;
    stage_dockerfile(&api, pod, &ctx_dir).await?;
    emit(Phase::Done {
        label: "source synced",
        dur: t.elapsed(),
    });

    let runner_ref = format!("{}:dev-{run_id}", refs.runner_repo_ref);
    let workspace_rel = src.workspace_rel.to_string_lossy().into_owned();
    let nextest_args = list_args
        .iter()
        .map(|a| shell_quote(a))
        .collect::<Vec<_>>()
        .join(" ");

    // 2. Build + push the runner image (compile + assemble), streaming BuildKit's
    //    progress. The compile happens inside this build's `compile` stage.
    emit(Phase::Start("building the runner image on the cluster"));
    let t = Instant::now();
    let build_cmd = buildctl_cmd(
        &ctx_dir,
        "runner",
        &nextest_args,
        &workspace_rel,
        &format!(
            "--output type=image,name={},push=true,{} --progress=auto",
            shell_quote(&runner_ref),
            crate::backends::image::IMAGE_OUTPUT_COMPRESSION,
        ),
        Some(registry_host(&runner_ref)),
    );
    let (tail_out, code) = match compile_out {
        Some(CompileOut::Pty { size, sink }) => exec_tty(&api, pod, &build_cmd, size, sink).await?,
        Some(CompileOut::Lines { sink }) => {
            let (_o, e, c) = exec_streamed(&api, pod, &build_cmd, Some(sink)).await?;
            (e, c)
        }
        None => {
            let (_o, e, c) = exec_capture(&api, pod, &build_cmd).await?;
            (e, c)
        }
    };
    if code != 0 {
        return Err(format!(
            "on-cluster runner build failed (exit {code}):\n{}",
            tail(&tail_out, 40)
        ));
    }
    emit(Phase::Done {
        label: "runner image built + pushed",
        dur: t.elapsed(),
    });

    // 3. Export the inventory: a second build of the `inventory-export` stage
    //    (reusing this build's layer + cache-mount cache, so the compile does not
    //    re-run) writes list.json + inventory.jsonl into the pod, then `oc cp`
    //    brings them to the laptop.
    emit(Phase::Start("dumping test inventory"));
    let t = Instant::now();
    let inv_dir = format!("{WORK_MOUNT}/inv");
    let inv_cmd = buildctl_cmd(
        &ctx_dir,
        "inventory-export",
        &nextest_args,
        &workspace_rel,
        &format!("--output type=local,dest={}", shell_quote(&inv_dir)),
        None,
    );
    let (_o, inv_err, inv_code) = exec_capture(&api, pod, &inv_cmd).await?;
    if inv_code != 0 {
        return Err(format!(
            "on-cluster inventory export failed (exit {inv_code}):\n{}",
            tail(&inv_err, 40)
        ));
    }
    let local = oc_cp_from_pod(pod, &inv_dir)?;
    let list_json = std::fs::read_to_string(local.path().join("list.json"))
        .map_err(|e| format!("read exported list.json: {e}"))?;
    let inventory = std::fs::read_to_string(local.path().join("inventory.jsonl"))
        .map_err(|e| format!("read exported inventory.jsonl: {e}"))?;

    let build = build::parse_list_summary(list_json.as_bytes())
        .map_err(|e| format!("parse on-cluster nextest list: {e}"))?;
    let BuildOutcome::Ok {
        selected_binaries, ..
    } = &build
    else {
        return Err("on-cluster nextest list produced no selection".to_string());
    };
    if selected_binaries.is_empty() {
        return Err("on-cluster nextest list selected no test binaries".to_string());
    }

    let sections = split_dumps_by_name(&inventory, selected_binaries)?;
    let mut dumps: Vec<Dumped> = Vec::with_capacity(selected_binaries.len());
    for (bin, (chunk, rc)) in selected_binaries.iter().zip(sections) {
        if rc != 0 {
            return Err(format!(
                "on-cluster inventory dump of {} failed (exit {rc}):\n{}",
                bin.binary_id,
                tail(&chunk, 40)
            ));
        }
        let dumped = images::parse_inventory(&chunk)
            .map_err(|e| format!("parse inventory of {}: {e}", bin.binary_id))?;
        dumps.push(dumped);
    }
    let (mut dump, qos_by_binary) = images::assemble(selected_binaries, dumps);
    // `dev!` images + seeds are provisioned laptop-side, so re-home their captured
    // in-image `/src/…` contexts back to the laptop's source ancestor.
    rehome_dump(&mut dump, &src.ancestor);
    emit(Phase::Done {
        label: &format!("inventory dumped ({} binaries)", selected_binaries.len()),
        dur: t.elapsed(),
    });
    emit(Phase::Note(&format!("runner image ready: {runner_ref}")));

    Ok(RemoteCompileOutcome {
        build,
        dump,
        qos_by_binary,
        runner_image_ref: runner_ref,
    })
}

/// The `buildctl build` shell for a Dockerfile stage in the build pod. Writes a
/// docker `config.json` from the pod SA token when `push_host` is set (the push
/// authenticates to the internal registry; its service-ca TLS is trusted through
/// the system store), then builds `target` with the shared source `ctx` and the
/// `NEXTEST_ARGS`/`WORKSPACE_REL` build-args. `output` is the caller's
/// `--output …` spec.
fn buildctl_cmd(
    ctx: &str,
    target: &str,
    nextest_args: &str,
    workspace_rel: &str,
    output: &str,
    push_host: Option<String>,
) -> String {
    let auth = match push_host {
        Some(host) => format!(
            "export DOCKER_CONFIG=/tmp/.docker\n\
             mkdir -p \"$DOCKER_CONFIG\"\n\
             TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)\n\
             AUTH=$(printf 'ztest:%s' \"$TOKEN\" | base64 | tr -d '\\n')\n\
             printf '{{\"auths\":{{\"%s\":{{\"auth\":\"%s\"}}}}}}' {host} \"$AUTH\" > \"$DOCKER_CONFIG/config.json\"\n",
            host = shell_quote(&host),
        ),
        None => String::new(),
    };
    format!(
        "set -eu\n\
         {auth}\
         buildctl build --frontend dockerfile.v0 \
           --local context={ctx} --local dockerfile={ctx} --opt filename=Dockerfile \
           --opt target={target} \
           --opt {na} --opt {wr} \
           {output}\n",
        ctx = shell_quote(ctx),
        target = shell_quote(target),
        na = shell_quote(&format!("build-arg:NEXTEST_ARGS={nextest_args}")),
        wr = shell_quote(&format!("build-arg:WORKSPACE_REL={workspace_rel}")),
    )
}

/// Write the embedded runner Dockerfile into the pod's context dir at
/// `<ctx>/Dockerfile`, so the build need not locate it in the synced tree.
async fn stage_dockerfile(api: &Api<Pod>, pod: &str, ctx_dir: &str) -> Result<(), String> {
    // A heredoc keeps the (multi-line, special-char) Dockerfile intact through the
    // single `sh -c`; the sentinel is unlikely to collide with Dockerfile text.
    let cmd = format!(
        "mkdir -p {dir}\ncat > {dir}/Dockerfile <<'ZTEST_DF_EOF'\n{df}\nZTEST_DF_EOF\n",
        dir = shell_quote(ctx_dir),
        df = RUNNER_DOCKERFILE,
    );
    let (_o, err, code) = exec_capture(api, pod, &cmd).await?;
    if code != 0 {
        return Err(format!(
            "stage runner Dockerfile in pod:\n{}",
            tail(&err, 20)
        ));
    }
    Ok(())
}

/// `oc cp <pod>:<dir>` the exported inventory to a fresh local temp dir, returned
/// as a self-cleaning handle.
fn oc_cp_from_pod(pod: &str, dir: &str) -> Result<TempDir, String> {
    let local = TempDir::new("ztest-inv")?;
    let mut cmd = std::process::Command::new("oc");
    cmd.arg("cp");
    if let Some(ctx) =
        std::env::var_os(crate::cluster_config::KUBE_CONTEXT_ENV).filter(|v| !v.is_empty())
    {
        cmd.arg("--context").arg(ctx);
    }
    cmd.args([
        "-n",
        RUN_NAMESPACE,
        "-c",
        BUILDKIT_CONTAINER,
        &format!("{pod}:{dir}"),
        &local.path().to_string_lossy(),
    ])
    .stdout(Stdio::null())
    .stderr(Stdio::piped());
    let out = cmd
        .output()
        .map_err(|e| format!("spawn `oc cp` (is `oc` on PATH?): {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "oc cp of exported inventory failed:\n{}",
            tail(&String::from_utf8_lossy(&out.stderr), 40)
        ));
    }
    Ok(local)
}

/// A temp dir removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Result<Self, String> {
        let base = std::env::temp_dir().join(format!("{tag}-{:08x}", rand::random::<u32>()));
        std::fs::create_dir_all(&base).map_err(|e| format!("create temp dir: {e}"))?;
        Ok(Self(base))
    }
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The outer shell wrapping every `exec`: runs the real command in a nested `sh`
/// and prints a `ZTEST_EXIT=<n>` sentinel — the exit-code source, not the k8s
/// `exec` Status (whose Rust shape is version-fragile).
const OUTER: &str = r#"sh -c "$1"; printf '\nZTEST_EXIT=%s\n' "$?""#;

/// A live line sink for a remote command's stderr on the non-interactive (CI)
/// path — one line per call. `ztest run` points it at stderr.
pub type LineSink<'a> = &'a dyn Fn(&str);

/// A raw-bytes sink for the interactive PTY stream. The bytes carry ANSI + cursor
/// control verbatim; `ztest run` feeds them into the console emulator.
pub type ByteSink<'a> = &'a dyn Fn(&[u8]);

/// Where the build's output goes, and thus whether it runs under a PTY.
pub enum CompileOut<'a> {
    /// Interactive: a remote PTY of `size` (cols, rows) so BuildKit streams its
    /// `--progress=auto` UI; the raw bytes go to `sink`.
    Pty {
        size: (u16, u16),
        sink: ByteSink<'a>,
    },
    /// Non-interactive (CI): no PTY; each stderr line to `sink`.
    Lines { sink: LineSink<'a> },
}

impl std::fmt::Debug for CompileOut<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompileOut::Pty { size, .. } => f
                .debug_struct("Pty")
                .field("size", size)
                .finish_non_exhaustive(),
            CompileOut::Lines { .. } => f.write_str("Lines"),
        }
    }
}

/// `exec` a command in the build pod, capturing `(stdout, stderr, exit_code)` and
/// forwarding each stderr line to `on_line` (when set). No TTY, so the two
/// streams share ONE websocket and MUST be drained concurrently.
async fn exec_streamed(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
    on_line: Option<LineSink<'_>>,
) -> Result<(String, String, i32), String> {
    use tokio::io::AsyncBufReadExt as _;

    let ap = AttachParams::default()
        .container(BUILDKIT_CONTAINER)
        .stdin(false)
        .stdout(true)
        .stderr(true);
    let mut attached = api
        .exec(pod, ["/bin/sh", "-c", OUTER, "ztest-exec", cmd], &ap)
        .await
        .map_err(|e| format!("exec in build pod {pod}: {e}"))?;

    let mut stdout = attached.stdout();
    let stderr = attached.stderr();
    let mut out = String::new();
    let mut err = String::new();
    let (ro, re) = tokio::join!(
        async {
            match stdout.as_mut() {
                Some(s) => s.read_to_string(&mut out).await.map(|_| ()),
                None => Ok(()),
            }
        },
        async {
            let Some(s) = stderr else { return Ok(()) };
            let mut lines = tokio::io::BufReader::new(s).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        if let Some(cb) = on_line {
                            cb(&line);
                        }
                        err.push_str(&line);
                        err.push('\n');
                    }
                    Ok(None) => break Ok(()),
                    Err(e) => break Err(e),
                }
            }
        },
    );
    ro.map_err(|e| format!("read exec stdout: {e}"))?;
    re.map_err(|e| format!("read exec stderr: {e}"))?;
    let _ = attached.join().await;

    let (clean, code) = split_exit_sentinel(&out);
    Ok((clean, err, code))
}

/// `exec` under a remote **PTY** and stream the merged raw output to `on_bytes`,
/// so BuildKit renders its progress UI into the caller's terminal emulator.
async fn exec_tty(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
    size: (u16, u16),
    on_bytes: ByteSink<'_>,
) -> Result<(String, i32), String> {
    let ap = AttachParams::default()
        .container(BUILDKIT_CONTAINER)
        .stdin(false)
        .stdout(true)
        .stderr(false)
        .tty(true);
    let mut attached = api
        .exec(pod, ["/bin/sh", "-c", OUTER, "ztest-exec", cmd], &ap)
        .await
        .map_err(|e| format!("exec (tty) in build pod {pod}: {e}"))?;

    if let Some(mut tx) = attached.terminal_size() {
        let (width, height) = size;
        let _ = tx.try_send(TerminalSize { width, height });
    }

    let mut out = String::new();
    if let Some(mut stdout) = attached.stdout() {
        let mut buf = [0u8; 8192];
        loop {
            match stdout.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    on_bytes(visible_prefix(&buf[..n]));
                    out.push_str(&String::from_utf8_lossy(&buf[..n]));
                }
                Err(e) => return Err(format!("read exec tty stdout: {e}")),
            }
        }
    }
    let _ = attached.join().await;

    let (_clean, code) = split_exit_sentinel(&out);
    Ok((out, code))
}

/// The chunk up to the `ZTEST_EXIT=` marker (dropping a preceding newline), so the
/// sentinel never reaches the terminal emulator.
fn visible_prefix(chunk: &[u8]) -> &[u8] {
    const M: &[u8] = b"ZTEST_EXIT=";
    match chunk.windows(M.len()).position(|w| w == M) {
        Some(i) if i > 0 && chunk[i - 1] == b'\n' => &chunk[..i - 1],
        Some(i) => &chunk[..i],
        None => chunk,
    }
}

/// Buffered [`exec_streamed`] with no live sink — for the quick, quiet steps whose
/// output is only interesting on failure.
async fn exec_capture(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
) -> Result<(String, String, i32), String> {
    exec_streamed(api, pod, cmd, None).await
}

/// Split the trailing `ZTEST_EXIT=<n>` marker off captured stdout, returning the
/// clean output and the exit code (absent marker ⇒ died before the outer shell's
/// `printf`, treated as failure).
fn split_exit_sentinel(out: &str) -> (String, i32) {
    const MARKER: &str = "ZTEST_EXIT=";
    for (idx, _) in out.match_indices(MARKER) {
        if idx == 0 || out.as_bytes()[idx - 1] == b'\n' {
            let code = out[idx + MARKER.len()..]
                .split_whitespace()
                .next()
                .and_then(|n| n.parse::<i32>().ok())
                .unwrap_or(1);
            let clean = out[..idx].trim_end_matches('\n').to_string();
            return (clean, code);
        }
    }
    (out.to_string(), 1)
}

/// Demux the exported `inventory.jsonl` into `(stdout, exit_code)` per selected
/// binary, keyed by binary FILENAME (the Dockerfile frames each block
/// `ZTEST_DUMP_BEGIN <name>` / `ZTEST_DUMP_END <name> rc=<code>` where `<name>` is
/// the binary's file name). Returned in `selected` order. Errors if a selected
/// binary has no block — a truncated export must fail loud, not drop a binary.
fn split_dumps_by_name(
    out: &str,
    selected: &[SelectedBinary],
) -> Result<Vec<(String, i32)>, String> {
    use std::collections::HashMap;
    let mut by_name: HashMap<String, (String, i32)> = HashMap::new();
    let mut cur: Option<(String, String)> = None;
    for line in out.lines() {
        if let Some(rest) = line.strip_prefix("ZTEST_DUMP_BEGIN ") {
            cur = Some((rest.trim().to_string(), String::new()));
        } else if let Some(rest) = line.strip_prefix("ZTEST_DUMP_END ") {
            let mut it = rest.split_whitespace();
            let name = it
                .next()
                .ok_or_else(|| format!("bad dump end marker: {line:?}"))?
                .to_string();
            let rc: i32 = it
                .next()
                .and_then(|s| s.strip_prefix("rc="))
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);
            match cur.take() {
                Some((n, buf)) if n == name => {
                    by_name.insert(name, (buf, rc));
                }
                _ => return Err(format!("mismatched dump markers at {name:?}")),
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
                .ok_or_else(|| format!("inventory export missing a block for binary {name:?}"))
        })
        .collect()
}

// ── Source set resolution + sync ──────────────────────────────────────

/// The local first-party source topology: the common-ancestor directory of the
/// git `repos` backing the build (the tar root and re-home base), the workspace's
/// path relative to it, and those repo roots.
///
/// Shipping whole repos — not just the cargo package subtrees — is required for
/// correctness, not thrift: a `dev!`/`mount_*`/seed path is resolved against the
/// invoking crate's `CARGO_MANIFEST_DIR` and routinely escapes the crate into its
/// repo (e.g. `dev!(Indexer::Zainod, "../../Dockerfile")` from
/// `zaino/live-tests/clientless` points at `zaino/Dockerfile`). Those files must
/// exist in the shipped tree at compile time. The git repo is the natural,
/// well-defined boundary that contains every such in-repo reference; scoping to
/// the repos that actually hold packages also keeps unrelated sibling repos out.
struct SourceLayout {
    ancestor: PathBuf,
    workspace_rel: PathBuf,
    repos: Vec<PathBuf>,
}

impl SourceLayout {
    fn resolve() -> Result<Self, String> {
        let meta = cargo_metadata()?;
        let workspace_root = meta["workspace_root"]
            .as_str()
            .ok_or("cargo metadata: no workspace_root")?;
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
        // Collapse the package dirs onto the git repos that contain them; the
        // repo is the unit we ship (see the struct docs).
        let mut repos: BTreeSet<PathBuf> = BTreeSet::new();
        for d in &dirs {
            repos.insert(git_repo_root(d)?);
        }
        let ancestor = common_ancestor(&repos).ok_or("cannot derive a common source ancestor")?;
        if ancestor.parent().is_none() || ancestor == Path::new("/home") {
            return Err(format!(
                "local source spans too wide a tree (common ancestor {}); a path \
                 dependency likely points outside the project",
                ancestor.display()
            ));
        }
        let workspace_rel = Path::new(workspace_root)
            .strip_prefix(&ancestor)
            .map_err(|_| "workspace root not under the source ancestor")?
            .to_path_buf();
        Ok(Self {
            ancestor,
            workspace_rel,
            repos: repos.into_iter().collect(),
        })
    }
}

/// The git repository root containing `dir` (`git rev-parse --show-toplevel`).
/// The on-cluster compile requires the source to be a git checkout — that's how
/// [`ship_source`] enumerates the files to send (honouring each repo's
/// `.gitignore`) — so a non-git `dir` is a hard, clearly-named error.
fn git_repo_root(dir: &Path) -> Result<PathBuf, String> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .map_err(|e| format!("run `git rev-parse` (is `git` on PATH?): {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "on-cluster compile needs the source in a git checkout, but {} is not in one:\n{}",
            dir.display(),
            tail(&String::from_utf8_lossy(&out.stderr), 5)
        ));
    }
    Ok(PathBuf::from(
        String::from_utf8_lossy(&out.stdout).trim_end(),
    ))
}

/// Re-home a dump's laptop-provisioned source paths from the build's in-image
/// source root ([`IMAGE_SRC_ROOT`]) back to the laptop's source `ancestor`. Only
/// local (path) sources need it; per-binary test paths stay pod-side.
fn rehome_dump(dump: &mut images::DumpOutcome, ancestor: &Path) {
    let images::DumpOutcome::Discovered {
        images,
        seeds,
        images_by_binary,
        deps_by_binary,
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
    for s in seeds.iter_mut() {
        s.source = rehome_str(&s.source, ancestor);
    }
    for (_, ds) in deps_by_binary.iter_mut() {
        for d in ds.iter_mut() {
            d.resource = rehome_str(&d.resource, ancestor);
        }
    }
}

fn rehome_dev(e: &mut crate::inventory::DevImageEntry, ancestor: &Path) {
    if let crate::backends::image::DevSource::Local {
        dockerfile,
        context,
    } = &mut e.source
    {
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

fn rehome_str(s: &str, ancestor: &Path) -> String {
    rehome_path(Path::new(s), ancestor)
        .to_string_lossy()
        .into_owned()
}

fn cargo_metadata() -> Result<serde_json::Value, String> {
    let out = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1"])
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| format!("run cargo metadata: {e}"))?;
    if !out.status.success() {
        return Err("cargo metadata failed".to_string());
    }
    serde_json::from_slice(&out.stdout).map_err(|e| format!("parse cargo metadata: {e}"))
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

/// Ship the first-party source into the build pod's context dir by streaming a
/// local `tar` of the git-tracked (and untracked-but-unignored) files of the
/// backing [`repos`](SourceLayout::repos) into the pod's `tar -x`.
///
/// `oc rsync` is unusable here: the buildkit image ([`BUILDKIT_IMAGE`]) ships no
/// `rsync`, and `oc rsync`'s tar fallback *walks* the excluded `target/` trees
/// rather than pruning them — so it grinds through hundreds of GiB of build
/// artifacts for a source tree of a few MiB. Instead each repo's own `.gitignore`
/// (via `git ls-files`) decides what ships: build artifacts (`target/`) and VCS
/// metadata drop out with no hand-maintained exclude list, and only the repos
/// that back the build go up. Files keep their ancestor-relative layout, so the
/// tree under `/src` matches the laptop's — cargo's path-deps and the `dev!`
/// macros' `CARGO_MANIFEST_DIR`-relative mount paths resolve identically.
fn ship_source(src: &SourceLayout, pod: &str, ctx_dir: &str) -> Result<(), String> {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;

    // Enumerate the files to ship as one NUL-delimited, ancestor-relative list.
    let mut list: Vec<u8> = Vec::new();
    for repo in &src.repos {
        let repo_rel = repo
            .strip_prefix(&src.ancestor)
            .map_err(|_| format!("repo {} not under the source ancestor", repo.display()))?
            .as_os_str()
            .as_bytes();
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args([
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
            ])
            .output()
            .map_err(|e| format!("run `git ls-files` (is `git` on PATH?): {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "git ls-files in {} failed:\n{}",
                repo.display(),
                tail(&String::from_utf8_lossy(&out.stderr), 20)
            ));
        }
        for name in out.stdout.split(|b| *b == 0).filter(|n| !n.is_empty()) {
            let mut rel = Vec::with_capacity(repo_rel.len() + 1 + name.len());
            if !repo_rel.is_empty() {
                rel.extend_from_slice(repo_rel);
                rel.push(b'/');
            }
            rel.extend_from_slice(name);
            // `--cached` lists a tracked-but-locally-deleted file too; skip any
            // missing path so `tar` can't hard-fail mid-stream on it.
            if !src.ancestor.join(OsStr::from_bytes(&rel)).exists() {
                continue;
            }
            list.extend_from_slice(&rel);
            list.push(0);
        }
    }
    if list.is_empty() {
        return Err("no source files to ship (git ls-files returned nothing)".to_string());
    }

    // `tar --null -T <file>` reads the list from a file, leaving its stdin free —
    // simpler and deadlock-free versus feeding the list over stdin while `oc`
    // drains stdout. The temp dir outlives `tar` (we wait on it below).
    let tmp = TempDir::new("ztest-ship")?;
    let list_path = tmp.path().join("files.0");
    std::fs::write(&list_path, &list).map_err(|e| format!("write tar file list: {e}"))?;

    let mut tar = std::process::Command::new("tar");
    tar.arg("-C")
        .arg(&src.ancestor)
        .arg("--null")
        .arg("-T")
        .arg(&list_path)
        .arg("-cf")
        .arg("-")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut tar_child = tar
        .spawn()
        .map_err(|e| format!("spawn local `tar` (is `tar` on PATH?): {e}"))?;
    let tar_stdout = tar_child.stdout.take().expect("tar stdout is piped");
    let mut tar_stderr = tar_child.stderr.take().expect("tar stderr is piped");
    // Drain tar's stderr on a thread so a chatty tar can never fill its pipe and
    // deadlock against `oc` (which we block on below).
    let tar_err = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = std::io::Read::read_to_string(&mut tar_stderr, &mut s);
        s
    });

    let mut oc = std::process::Command::new("oc");
    oc.arg("exec")
        .arg("-i")
        .args(["-n", RUN_NAMESPACE, "-c", BUILDKIT_CONTAINER]);
    if let Some(ctx) =
        std::env::var_os(crate::cluster_config::KUBE_CONTEXT_ENV).filter(|v| !v.is_empty())
    {
        oc.arg("--context").arg(ctx);
    }
    oc.arg(pod)
        .arg("--")
        .args(["sh", "-c"])
        .arg(format!(
            "mkdir -p {c} && exec tar -xf - -C {c}",
            c = shell_quote(ctx_dir)
        ))
        .stdin(Stdio::from(tar_stdout))
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let oc_out = oc
        .output()
        .map_err(|e| format!("spawn `oc exec` (is `oc` on PATH?): {e}"))?;

    let tar_status = tar_child
        .wait()
        .map_err(|e| format!("wait for local `tar`: {e}"))?;
    let tar_err = tar_err.join().unwrap_or_default();
    if !tar_status.success() {
        return Err(format!(
            "local `tar` of source failed ({tar_status}):\n{}",
            tail(&tar_err, 40)
        ));
    }
    if !oc_out.status.success() {
        return Err(format!(
            "streaming source into the build pod failed:\n{}",
            tail(&String::from_utf8_lossy(&oc_out.stderr), 40)
        ));
    }
    Ok(())
}

// ── small helpers ─────────────────────────────────────────────────────

/// The registry host[:port] of a `host[:port]/project/repo[:tag]` reference.
fn registry_host(reference: &str) -> String {
    reference.split('/').next().unwrap_or(reference).to_string()
}

/// Single-quote a value for `/bin/sh`, escaping embedded single quotes.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn bin(id: &str, path: &str) -> SelectedBinary {
        SelectedBinary {
            binary_id: id.to_string(),
            binary_path: PathBuf::from(path),
            cwd: PathBuf::from("/src"),
            selected_tests: vec![],
        }
    }

    #[test]
    fn exit_sentinel_splits_code_and_output() {
        let (clean, code) = split_exit_sentinel("hello\nworld\nZTEST_EXIT=0\n");
        assert_eq!(clean, "hello\nworld");
        assert_eq!(code, 0);
        assert_eq!(split_exit_sentinel("boom\nZTEST_EXIT=101\n").1, 101);
        assert_eq!(split_exit_sentinel("partial output").1, 1);
    }

    #[test]
    fn split_dumps_by_name_maps_blocks_to_selected_order() {
        // Emitted out of order; demux returns them in `selected` order, keyed by
        // binary file name.
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
        let selected = [
            bin("pkg::alpha", "/x/alpha-abc"),
            bin("pkg::beta", "/x/beta-xyz"),
        ];
        assert!(split_dumps_by_name(out, &selected).is_err());
    }

    #[test]
    fn visible_prefix_hides_exit_sentinel() {
        assert_eq!(visible_prefix(b"Building foo\n"), b"Building foo\n");
        assert_eq!(visible_prefix(b"done\nZTEST_EXIT=0\n"), b"done");
    }

    #[test]
    fn common_ancestor_of_sibling_repos() {
        let dirs = [
            PathBuf::from("/home/u/proj/zaino/live-tests"),
            PathBuf::from("/home/u/proj/ztest"),
        ]
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

    #[test]
    fn registry_host_strips_project_and_tag() {
        assert_eq!(
            registry_host("image-registry.svc:5000/ztest-images/ztest-runner:dev-abc"),
            "image-registry.svc:5000"
        );
    }
}
