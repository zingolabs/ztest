//! On-cluster compilation, buildkit-native.
//!
//! - Runner image (test binaries + inventory) = one multi-stage `buildctl` build of
//!   [`docker/runner.Dockerfile`](RUNNER_DOCKERFILE) in the ephemeral BuildKit pod
//! - Laptop ships *source* as the build context; build compiles, assembles + pushes the image
//! - Inventory (`list.json` + framed `inventory.jsonl`) exported `--output type=local`,
//!   streamed back over `exec`, parsed exactly as the laptop path parses a dump
//! - Every cluster call rides `kube` — no host CLI on the remote path

use std::collections::BTreeSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, AttachParams, AttachedProcess, TerminalSize};
use tokio::io::AsyncReadExt as _;

use crate::naming::RUN_NAMESPACE;
use crate::pipeline::build::{self, BuildOutcome, SelectedBinary};
use crate::pipeline::images::{self, Dumped};
use crate::resource::impls::buildkit::{BUILDKIT_CONTAINER, WORK_MOUNT};

/// Runner build recipe (compile → inventory → runner)
pub const RUNNER_DOCKERFILE: &str = include_str!("../../docker/runner.Dockerfile");

/// In-image source root (`WORKDIR /src` + `COPY . .`). Inventory ctors resolve `dev!`/seed
/// paths under here → come back `/src`-rooted, re-homed by [`rehome_dump`]
const IMAGE_SRC_ROOT: &str = "/src";

/// Same shapes as the laptop compile + Phase-C dump, plus the pushed runner ref
#[derive(Debug)]
pub struct RemoteCompileOutcome {
    pub build: BuildOutcome,
    pub dump: images::DumpOutcome,
    pub qos_by_binary: Vec<(String, Vec<crate::inventory::QosEntry>)>,
    pub runner_image_ref: String,
}

impl RemoteCompileOutcome {
    fn binary_count(&self) -> usize {
        match &self.build {
            BuildOutcome::Ok { selected_binaries, .. } => selected_binaries.len(),
            _ => 0,
        }
    }
}

#[derive(Debug)]
pub struct BakeRefs {
    pub runner_repo_ref: String,
}

/// Remote-compile progress transition. `compile_on_cluster` emits + times; caller
/// ([`crate::cli::run`]) owns all formatting
#[derive(Debug)]
pub enum Phase<'a> {
    Start(&'a str),
    Done { label: &'a str, dur: Duration },
    Note(&'a str),
}

/// Phase-transition sink. `mut` (callers update panel state)
pub type PhaseSink<'a> = &'a mut dyn FnMut(Phase<'_>);

/// - `list_args` = the laptop path's `cargo nextest list` argv
/// - `run_id` tags the pushed runner image per run
/// - `compile_out` = BuildKit progress route (remote PTY → caller's emulator, or CI lines)
pub async fn compile_on_cluster(
    client: &kube::Client,
    pod: &str,
    list_args: &[String],
    refs: &BakeRefs,
    run_id: &str,
    compile_out: Option<CompileOut<'_>>,
    on_phase: Option<PhaseSink<'_>>,
) -> Result<RemoteCompileOutcome, crate::error::PipelineError> {
    let mut on_phase = on_phase;
    let mut emit = |ev: Phase<'_>| {
        if let Some(cb) = on_phase.as_deref_mut() {
            cb(ev);
        }
    };
    let api: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);

    // 1. Ship first-party source as build context + the embedded runner Dockerfile
    //    (embedded → need not be found in the synced tree)
    let src = SourceLayout::resolve()?;
    let ctx_dir = format!("{WORK_MOUNT}/ctx");
    emit(Phase::Start("syncing source to the build pod"));
    let t = Instant::now();
    ship_source(&api, &src, pod, &ctx_dir).await?;
    stage_dockerfile(&api, pod, &ctx_dir).await?;
    emit(Phase::Done { label: "source synced", dur: t.elapsed() });

    let runner_ref = format!("{}:dev-{run_id}", refs.runner_repo_ref);
    let workspace_rel = src.workspace_rel.to_string_lossy().into_owned();
    let nextest_args = list_args.iter().map(|a| shell_quote(a)).collect::<Vec<_>>().join(" ");

    // 2. Build + push the runner image, streaming BuildKit progress (compile happens
    //    inside this build's `compile` stage)
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
        Push::Yes,
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
        )
        .into());
    }
    emit(Phase::Done { label: "runner image built + pushed", dur: t.elapsed() });

    // 3. Second build of the `inventory-export` stage writes list.json + inventory.jsonl
    //    into the pod, streamed back over `exec` (layer + cache-mount reuse → no re-compile)
    emit(Phase::Start("dumping test inventory"));
    let t = Instant::now();
    let inv_dir = format!("{WORK_MOUNT}/inv");
    let inv_cmd = buildctl_cmd(
        &ctx_dir,
        "inventory-export",
        &nextest_args,
        &workspace_rel,
        &format!("--output type=local,dest={}", shell_quote(&inv_dir)),
        Push::No,
    );
    let (_o, inv_err, inv_code) = exec_capture(&api, pod, &inv_cmd).await?;
    if inv_code != 0 {
        return Err(format!("inventory export exited {inv_code}:\n{}", tail(&inv_err, 40)).into());
    }
    let local = fetch_inventory(&api, pod, &inv_dir).await?;
    let list_json = std::fs::read_to_string(local.path().join("list.json"))
        .map_err(|e| format!("read exported list.json: {e}"))?;
    let inventory = std::fs::read_to_string(local.path().join("inventory.jsonl"))
        .map_err(|e| format!("read exported inventory.jsonl: {e}"))?;

    let outcome = assemble_outcome(&list_json, &inventory, &src.ancestor, runner_ref.clone())?;
    emit(Phase::Done {
        label: &format!("inventory dumped ({} binaries)", outcome.binary_count()),
        dur: t.elapsed(),
    });
    emit(Phase::Note(&format!("runner image ready: {runner_ref}")));
    Ok(outcome)
}

/// Fold a bake's `list.json` + framed `inventory.jsonl` into the run pipeline's outcome.
/// Shared by both bakes of [`RUNNER_DOCKERFILE`] (same files, same stage, only *where* differs)
pub fn assemble_outcome(
    list_json: &str,
    inventory: &str,
    ancestor: &Path,
    runner_image_ref: String,
) -> Result<RemoteCompileOutcome, crate::error::PipelineError> {
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

    Ok(RemoteCompileOutcome { build, dump, qos_by_binary, runner_image_ref })
}

/// Whether this stage pushes, hence whether it needs registry credentials
#[derive(Clone, Copy, PartialEq, Eq)]
enum Push {
    Yes,
    No,
}

/// `buildctl build` shell for one Dockerfile stage in the build pod.
///
/// - Pushing stage + a configured push Secret → point `DOCKER_CONFIG` at its mount;
///   unset = anonymous push, which is every registry reachable only in-cluster
/// - Credentials are the Secret's to hold: no token is minted, printed or logged here
/// - Builds `target` from `ctx` with `NEXTEST_ARGS`/`WORKSPACE_REL` build-args
fn buildctl_cmd(
    ctx: &str,
    target: &str,
    nextest_args: &str,
    workspace_rel: &str,
    output: &str,
    push: Push,
) -> String {
    let auth = match (push, crate::backends::image::push_secret()) {
        (Push::Yes, Some(_)) => {
            format!("export DOCKER_CONFIG={}\n", crate::resource::impls::buildkit::REGISTRY_MOUNT)
        }
        _ => String::new(),
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

/// Lands at `<ctx>/Dockerfile`, so the build need not locate it in the synced tree.
async fn stage_dockerfile(
    api: &Api<Pod>,
    pod: &str,
    ctx_dir: &str,
) -> Result<(), crate::error::PipelineError> {
    // Heredoc keeps the multi-line, special-char Dockerfile intact through one `sh -c`
    let cmd = format!(
        "mkdir -p {dir}\ncat > {dir}/Dockerfile <<'ZTEST_DF_EOF'\n{df}\nZTEST_DF_EOF\n",
        dir = shell_quote(ctx_dir),
        df = RUNNER_DOCKERFILE,
    );
    let (_o, err, code) = exec_capture(api, pod, &cmd).await?;
    if code != 0 {
        return Err(format!("stage runner Dockerfile in pod:\n{}", tail(&err, 20)).into());
    }
    Ok(())
}

/// Exported inventory back to a local dir: `tar -c` in the pod, `tar -x` here.
///
/// - Two small files (`list.json` + `inventory.jsonl`) → collected whole, not streamed
async fn fetch_inventory(
    api: &Api<Pod>,
    pod: &str,
    dir: &str,
) -> Result<TempDir, crate::error::PipelineError> {
    use std::io::Write as _;

    let local = TempDir::new("ztest-inv")?;
    let archive =
        stream_from_pod(api, pod, &format!("tar -cf - -C {d} .", d = shell_quote(dir))).await?;

    let mut untar = std::process::Command::new("tar")
        .arg("-xf")
        .arg("-")
        .arg("-C")
        .arg(local.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn local `tar -x` (is `tar` on PATH?): {e}"))?;
    untar
        .stdin
        .take()
        .expect("tar stdin is piped")
        .write_all(&archive)
        .map_err(|e| format!("write inventory archive to local `tar`: {e}"))?;
    let out = untar.wait_with_output().map_err(|e| format!("wait for local `tar -x`: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "unpacking the exported inventory failed:\n{}",
            tail(&String::from_utf8_lossy(&out.stderr), 40)
        )
        .into());
    }
    Ok(local)
}

#[derive(Debug)]
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Result<Self, crate::error::PipelineError> {
        let base = std::env::temp_dir().join(format!("{tag}-{:08x}", rand::random::<u32>()));
        std::fs::create_dir_all(&base).map_err(|e| format!("create temp dir: {e}"))?;
        Ok(Self(base))
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Outer shell wrapping every `exec`: nested `sh` + a `ZTEST_EXIT=<n>` sentinel =
/// the exit-code source (k8s `exec` Status has a version-fragile Rust shape)
const OUTER: &str = r#"sh -c "$1"; printf '\nZTEST_EXIT=%s\n' "$?""#;

/// Live stderr sink for the non-interactive (CI) path, one line per call
pub type LineSink<'a> = &'a dyn Fn(&str);

/// Raw-bytes sink for the PTY stream (ANSI + cursor control verbatim → console emulator)
pub type ByteSink<'a> = &'a dyn Fn(&[u8]);

/// Build-output route, hence whether it runs under a PTY. `Pty.size` = (cols, rows) of the
/// remote terminal BuildKit renders `--progress=auto` into
pub enum CompileOut<'a> {
    Pty { size: (u16, u16), sink: ByteSink<'a> },
    Lines { sink: LineSink<'a> },
}

impl std::fmt::Debug for CompileOut<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompileOut::Pty { size, .. } => {
                f.debug_struct("Pty").field("size", size).finish_non_exhaustive()
            }
            CompileOut::Lines { .. } => f.write_str("Lines"),
        }
    }
}

/// `exec` in the build pod → `(stdout, stderr, exit_code)`, each stderr line to `on_line`.
/// No TTY → both streams share ONE websocket and MUST be drained concurrently
async fn exec_streamed(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
    on_line: Option<LineSink<'_>>,
) -> Result<(String, String, i32), crate::error::PipelineError> {
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

/// `exec` under a remote **PTY**, merged raw output → `on_bytes` (BuildKit's progress UI
/// lands in the caller's terminal emulator)
async fn exec_tty(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
    size: (u16, u16),
    on_bytes: ByteSink<'_>,
) -> Result<(String, i32), crate::error::PipelineError> {
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
                Err(e) => return Err(format!("read exec tty stdout: {e}").into()),
            }
        }
    }
    let _ = attached.join().await;

    let (_clean, code) = split_exit_sentinel(&out);
    Ok((out, code))
}

/// Chunk up to the `ZTEST_EXIT=` marker, dropping a preceding newline (sentinel never
/// reaches the emulator)
fn visible_prefix(chunk: &[u8]) -> &[u8] {
    const M: &[u8] = b"ZTEST_EXIT=";
    match chunk.windows(M.len()).position(|w| w == M) {
        Some(i) if i > 0 && chunk[i - 1] == b'\n' => &chunk[..i - 1],
        Some(i) => &chunk[..i],
        None => chunk,
    }
}

/// Buffered [`exec_streamed`], no live sink — quick quiet steps, output interesting only on failure
async fn exec_capture(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
) -> Result<(String, String, i32), crate::error::PipelineError> {
    exec_streamed(api, pod, cmd, None).await
}

// ── Binary exec: tar over the websocket ───────────────────────────────
//
// The [`OUTER`] wrapper reports its exit code as a sentinel *on stdout*, unusable where
// stdout carries a tar stream. These two take the code off the websocket's status channel
// instead, leaving the payload byte-exact.

/// Feed `cmd`'s stdin from `chunks`; stdout discarded, stderr kept for the error.
async fn stream_into_pod(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
    mut chunks: tokio::sync::mpsc::Receiver<Vec<u8>>,
) -> Result<(), crate::error::PipelineError> {
    use tokio::io::AsyncWriteExt as _;

    let ap = AttachParams::default()
        .container(BUILDKIT_CONTAINER)
        .stdin(true)
        .stdout(false)
        .stderr(true);
    let mut attached = api
        .exec(pod, ["sh", "-c", cmd], &ap)
        .await
        .map_err(|e| format!("exec in build pod {pod}: {e}"))?;
    let status = attached.take_status();

    let mut stdin = attached.stdin().ok_or_else(|| "exec stdin not attached".to_string())?;
    let stderr = attached.stderr();
    let (wrote, err) = tokio::join!(
        async {
            while let Some(chunk) = chunks.recv().await {
                stdin.write_all(&chunk).await?;
            }
            stdin.shutdown().await
        },
        drain(stderr),
    );
    wrote.map_err(|e| format!("write exec stdin: {e}"))?;
    exit_ok(attached, status, &err).await
}

/// `cmd`'s stdout collected whole; stderr kept for the error.
async fn stream_from_pod(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
) -> Result<Vec<u8>, crate::error::PipelineError> {
    let ap = AttachParams::default()
        .container(BUILDKIT_CONTAINER)
        .stdin(false)
        .stdout(true)
        .stderr(true);
    let mut attached = api
        .exec(pod, ["sh", "-c", cmd], &ap)
        .await
        .map_err(|e| format!("exec in build pod {pod}: {e}"))?;
    let status = attached.take_status();

    let mut stdout = attached.stdout().ok_or_else(|| "exec stdout not attached".to_string())?;
    let stderr = attached.stderr();
    let (read, err) = tokio::join!(
        async {
            let mut buf = Vec::new();
            stdout.read_to_end(&mut buf).await.map(|_| buf)
        },
        drain(stderr),
    );
    let out = read.map_err(|e| format!("read exec stdout: {e}"))?;
    exit_ok(attached, status, &err).await?;
    Ok(out)
}

async fn drain(stream: Option<impl tokio::io::AsyncRead + Unpin>) -> String {
    let Some(mut s) = stream else { return String::new() };
    let mut buf = Vec::new();
    let _ = s.read_to_end(&mut buf).await;
    String::from_utf8_lossy(&buf).into_owned()
}

/// Status channel resolves only once the streams are done and the process joined
async fn exit_ok(
    attached: AttachedProcess,
    status: Option<
        impl Future<Output = Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Status>>,
    >,
    stderr: &str,
) -> Result<(), crate::error::PipelineError> {
    let _ = attached.join().await;
    match status {
        Some(fut) => match fut.await {
            Some(s) if s.status.as_deref() == Some("Success") => Ok(()),
            Some(s) => Err(format!(
                "{}\n{}",
                s.message.unwrap_or_else(|| "command failed in the build pod".into()),
                tail(stderr, 40)
            )
            .into()),
            None => Err(format!("no exit status from the build pod\n{}", tail(stderr, 40)).into()),
        },
        None => Err(format!("no status channel on the exec\n{}", tail(stderr, 40)).into()),
    }
}

/// Split the trailing `ZTEST_EXIT=<n>` off captured stdout → (clean output, code).
/// Marker absent ⇒ died before the outer shell's `printf` → failure
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

/// Demux exported `inventory.jsonl` → `(stdout, exit_code)` per selected binary, in
/// `selected` order.
///
/// - Keyed by binary FILENAME (Dockerfile frames `ZTEST_DUMP_BEGIN/END <name> rc=<code>`)
/// - Missing block = error (truncated export must fail loud, never drop a binary)
fn split_dumps_by_name(
    out: &str,
    selected: &[SelectedBinary],
) -> Result<Vec<(String, i32)>, crate::error::PipelineError> {
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
            by_name.remove(&name).ok_or_else(|| {
                crate::error::PipelineError(format!("inventory: no block for {name:?}"))
            })
        })
        .collect()
}

// ── Source set resolution + sync ──────────────────────────────────────

/// `ancestor` = common-ancestor dir of the backing git `repos` = tar root & re-home base.
///
/// - Whole repos ship, not package subtrees: `dev!`/`mount_*`/seed paths resolve against
///   `CARGO_MANIFEST_DIR` and routinely escape the crate into its repo
/// - Scoping to repos that hold packages keeps sibling repos out
#[derive(Debug)]
pub struct SourceLayout {
    pub ancestor: PathBuf,
    pub workspace_rel: PathBuf,
    repos: Vec<PathBuf>,
}

impl SourceLayout {
    pub fn resolve() -> Result<Self, crate::error::PipelineError> {
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
            repos.insert(git_repo_root(d)?);
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

/// Git repo root containing `dir` (`git rev-parse --show-toplevel`). Non-git `dir` = hard
/// named error ([`ship_source`] enumerates via git, honouring each repo's `.gitignore`)
fn git_repo_root(dir: &Path) -> Result<PathBuf, crate::error::PipelineError> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .map_err(|e| format!("run `git rev-parse` (is `git` on PATH?): {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "{} is not in a git checkout:\n{}",
            dir.display(),
            tail(&String::from_utf8_lossy(&out.stderr), 5)
        )
        .into());
    }
    Ok(PathBuf::from(String::from_utf8_lossy(&out.stdout).trim_end()))
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

fn rehome_dev(e: &mut crate::inventory::DevImageEntry, ancestor: &Path) {
    if let crate::backends::image::DevSource::Local { dockerfile, context } = &mut e.source {
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

fn cargo_metadata() -> Result<serde_json::Value, crate::error::PipelineError> {
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

/// Build context = per-repo `git ls-files` streamed as a local tar, ancestor-relative
/// (cargo path-deps + `CARGO_MANIFEST_DIR` mounts resolve identically under `/src`).
///
/// - `tar` over `exec`, not a sync tool: the buildkit image ships no `rsync`, and a
///   directory-walking copy descends the excluded `target/` trees instead of pruning
/// - Chain archives are gitignored, so `--exclude-standard` drops them here: the build
///   context never sees a multi-GB payload, and a seed is addressed by its manifest
/// - Total bounded by [`CONTEXT_MAX_BYTES`], offender-naming error (a silent multi-GB stall
///   reads as a hung cluster)
fn spawn_source_tar(src: &SourceLayout) -> Result<SourceStream, crate::error::PipelineError> {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;

    // One NUL-delimited ancestor-relative list, sized as we go (ceiling below names offenders).
    // Temp dir outlives `tar`: holds the file list `tar` reads
    let tmp = TempDir::new("ztest-ship")?;

    let mut list: Vec<u8> = Vec::new();
    let mut total: u64 = 0;
    let mut sized: Vec<(u64, PathBuf)> = Vec::new();
    for repo in &src.repos {
        let repo_rel = repo
            .strip_prefix(&src.ancestor)
            .map_err(|_| format!("repo {} not under the source ancestor", repo.display()))?
            .as_os_str()
            .as_bytes();
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["ls-files", "-z", "--cached", "--others", "--exclude-standard"])
            .output()
            .map_err(|e| format!("run `git ls-files` (is `git` on PATH?): {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "git ls-files in {} failed:\n{}",
                repo.display(),
                tail(&String::from_utf8_lossy(&out.stderr), 20)
            )
            .into());
        }
        let names: Vec<&[u8]> = out.stdout.split(|b| *b == 0).filter(|n| !n.is_empty()).collect();
        for name in names {
            let mut rel = Vec::with_capacity(repo_rel.len() + 1 + name.len());
            if !repo_rel.is_empty() {
                rel.extend_from_slice(repo_rel);
                rel.push(b'/');
            }
            rel.extend_from_slice(name);

            // `--cached` also lists locally-deleted files; skip missing paths (`tar` would
            // hard-fail mid-stream)
            let abs = src.ancestor.join(OsStr::from_bytes(&rel));
            let Ok(md) = std::fs::metadata(&abs) else {
                continue;
            };
            // Regular files only: `tar` stores a symlink as the link, so charging its
            // target's size would overcount
            if md.is_file() {
                total += md.len();
                sized.push((md.len(), abs));
            }
            list.extend_from_slice(&rel);
            list.push(0);
        }
    }
    if list.is_empty() {
        return Err("git ls-files returned nothing".into());
    }
    if total > context_max_bytes() {
        return Err(oversized_context(total, sized).into());
    }

    // `-T <file>` leaves tar's stdin free (feeding the list in while draining its stdout
    // from the same task can deadlock)
    let list_path = tmp.path().join("files.0");
    std::fs::write(&list_path, &list).map_err(|e| format!("write tar file list: {e}"))?;

    let mut tar = std::process::Command::new("tar");
    tar.arg("-C").arg(&src.ancestor).arg("--null").arg("-T").arg(&list_path);
    tar.arg("-cf").arg("-").stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = tar.spawn().map_err(|e| format!("spawn local `tar` (is `tar` on PATH?): {e}"))?;
    Ok(SourceStream { child, _tmp: tmp })
}

/// Spawned `tar` streaming the build context on stdout. Temp dir held so the file list
/// outlives [`spawn_source_tar`]
struct SourceStream {
    child: std::process::Child,
    _tmp: TempDir,
}

/// Local `tar -c` piped over `exec` into `tar -x` in the build pod.
///
/// - Bounded channel = backpressure (`tar` blocks once the websocket falls behind)
/// - `tar`'s stderr drained on its own thread (a full pipe deadlocks the stdout reader)
async fn ship_source(
    api: &Api<Pod>,
    src: &SourceLayout,
    pod: &str,
    ctx_dir: &str,
) -> Result<(), crate::error::PipelineError> {
    let mut stream = spawn_source_tar(src)?;
    let mut tar_stdout = stream.child.stdout.take().expect("tar stdout is piped");
    let mut tar_stderr = stream.child.stderr.take().expect("tar stderr is piped");

    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(CONTEXT_CHUNKS_IN_FLIGHT);
    let pump = tokio::task::spawn_blocking(move || -> Result<(), String> {
        use std::io::Read as _;

        let errs = std::thread::spawn(move || {
            let mut s = String::new();
            let _ = std::io::Read::read_to_string(&mut tar_stderr, &mut s);
            s
        });

        let mut buf = vec![0u8; CONTEXT_CHUNK_BYTES];
        loop {
            match tar_stdout.read(&mut buf) {
                Ok(0) => break,
                // Receiver gone = the exec already failed; its error is the useful one
                Ok(n) => {
                    if tx.blocking_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(e) => return Err(format!("read local `tar`: {e}")),
            }
        }
        drop(tar_stdout);

        let status = stream.child.wait().map_err(|e| format!("wait for local `tar`: {e}"))?;
        let errs = errs.join().unwrap_or_default();
        match status.success() {
            true => Ok(()),
            false => Err(format!("local `tar` of source failed ({status}):\n{}", tail(&errs, 40))),
        }
    });

    let cmd = format!("mkdir -p {c} && exec tar -xf - -C {c}", c = shell_quote(ctx_dir));
    let shipped = stream_into_pod(api, pod, &cmd, rx).await;
    let pumped = pump.await.map_err(|e| format!("source pump panicked: {e}"))?;

    // Exec failure wins: a torn-down websocket SIGPIPEs `tar`, whose status would mask it
    shipped.map_err(|e| format!("streaming source into the build pod failed:\n{e}"))?;
    pumped?;
    Ok(())
}

/// Same build context into a local dir, for the laptop-side bake
/// ([`crate::pipeline::local_bake`]). Staged rather than handing `docker build` the source
/// ancestor (what keeps gitignored payloads out — see [`spawn_source_tar`])
pub fn extract_source_to(
    src: &SourceLayout,
    dest: &Path,
) -> Result<(), crate::error::PipelineError> {
    let mut stream = spawn_source_tar(src)?;
    let tar_stdout = stream.child.stdout.take().expect("tar stdout is piped");
    let out = std::process::Command::new("tar")
        .arg("-xf")
        .arg("-")
        .arg("-C")
        .arg(dest)
        .stdin(Stdio::from(tar_stdout))
        .output()
        .map_err(|e| format!("spawn local `tar -x`: {e}"))?;
    let status = stream.child.wait().map_err(|e| format!("wait for local `tar`: {e}"))?;
    if !status.success() {
        return Err(format!("local `tar` of source failed ({status})").into());
    }
    if !out.status.success() {
        return Err(format!(
            "staging the build context failed:\n{}",
            tail(&String::from_utf8_lossy(&out.stderr), 40)
        )
        .into());
    }
    Ok(())
}

/// Ceiling on the shipped build context (first-party source = a few MiB, so orders of
/// headroom while still catching a stray artifact before it reads as a hung cluster)
const CONTEXT_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Read size off the local `tar`, matched to the websocket's own framing
const CONTEXT_CHUNK_BYTES: usize = 256 * 1024;

/// Queue depth between `tar` and the websocket — the backpressure window
const CONTEXT_CHUNKS_IN_FLIGHT: usize = 4;

/// Env override for [`CONTEXT_MAX_BYTES`], k8s-style quantity (`512Mi`, `2Gi`) — escape
/// hatch so the ceiling never becomes a reason to disable the check
const CONTEXT_MAX_ENV: &str = "ZTEST_CONTEXT_MAX";

fn context_max_bytes() -> u64 {
    std::env::var(CONTEXT_MAX_ENV)
        .ok()
        .and_then(|v| crate::qos::units::parse_mem_bytes_opt(&v))
        .unwrap_or(CONTEXT_MAX_BYTES)
}

/// Over-ceiling error naming the largest offenders (operator learns *which* file to exclude
/// without measuring the tree by hand)
fn oversized_context(total: u64, mut sized: Vec<(u64, PathBuf)>) -> String {
    const SHOWN: usize = 5;
    sized.sort_unstable_by_key(|(n, _)| std::cmp::Reverse(*n));
    let mut msg = format!(
        "build context is {} (ceiling {}); refusing to ship it.\nLargest files:\n",
        bytes(total),
        bytes(context_max_bytes())
    );
    for (n, p) in sized.iter().take(SHOWN) {
        msg.push_str(&format!("  {:>10}  {}\n", bytes(*n), p.display()));
    }
    msg.push_str(
        "\nOnly first-party source belongs in the context. .gitignore a large \
         artifact to drop it from the ship set; raise the ceiling with ",
    );
    msg.push_str(CONTEXT_MAX_ENV);
    msg.push_str(" (e.g. 1Gi) only if the tree really must ship this much.");
    msg
}

/// IEC bytes. Wraps [`unit_value`](crate::fmt::unit_value) so a build-context size reads
/// the same as every other byte figure ztest prints
fn bytes(n: u64) -> String {
    crate::fmt::unit_value(crate::fmt::Unit::Bytes, n as f64)
}

// ── small helpers ─────────────────────────────────────────────────────

pub fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

pub fn tail(s: &str, n: usize) -> String {
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
    fn context_ceiling_names_the_biggest_offenders() {
        let total = 13 * (1 << 30);
        let msg = oversized_context(
            total,
            vec![
                (3 << 30, PathBuf::from("ztest/fixtures/chains/b.tar.zst")),
                (1 << 10, PathBuf::from("ztest/src/lib.rs")),
                (8 << 30, PathBuf::from("ztest/fixtures/chains/a.tar.zst")),
            ],
        );
        assert!(msg.contains("13.0 GiB"), "reports the total: {msg}");
        // Descending by size → the file worth excluding reads first
        let a = msg.find("a.tar.zst").expect("largest listed");
        let b = msg.find("b.tar.zst").expect("second listed");
        assert!(a < b, "offenders are ordered largest-first: {msg}");
        assert!(msg.contains(CONTEXT_MAX_ENV), "names the override: {msg}");
    }

    #[test]
    fn byte_quantities_use_binary_prefixes() {
        assert_eq!(bytes(8_751_733_052), "8.2 GiB");
        assert_eq!(bytes(649_866_689), "619.8 MiB");
        assert_eq!(bytes(1 << 10), "1.0 KiB");
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(0), "0 B");
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
    fn visible_prefix_hides_exit_sentinel() {
        assert_eq!(visible_prefix(b"Building foo\n"), b"Building foo\n");
        assert_eq!(visible_prefix(b"done\nZTEST_EXIT=0\n"), b"done");
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

    #[test]
    fn push_stage_without_a_secret_writes_no_auth_prelude() {
        let cmd = buildctl_cmd("/build/ctx", "runner", "", ".", "--output type=image", Push::Yes);
        assert!(!cmd.contains("DOCKER_CONFIG"), "unexpected auth prelude: {cmd}");
    }
}
