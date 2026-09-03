//! `exec` transport to the run's BuildKit pod: ship a context in, run a build, fetch files back.
//!
//! - Every cluster call rides `kube` — no host CLI on the remote path
//! - Text execs report their code as a stdout sentinel ([`OUTER`]); the tar execs take it
//!   off the websocket status channel instead (stdout carries bytes, not a marker)
//! - `tar -c` locally → `tar -x` in the pod, git file selection = the exclusion

use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, AttachParams, AttachedProcess, TerminalSize};
use tokio::io::AsyncReadExt as _;

use crate::error::PipelineError;
use crate::proc::ChildHost;
use crate::resource::impls::buildkit::BUILDKIT_CONTAINER;

/// Outer shell wrapping every text `exec`: nested `sh` + a `ZTEST_EXIT=<n>` sentinel =
/// the exit-code source (k8s `exec` Status has a version-fragile Rust shape)
const OUTER: &str = r#"sh -c "$1"; printf '\nZTEST_EXIT=%s\n' "$?""#;

/// Live stderr sink for the non-interactive (CI) path, one line per call.
/// `Send + Sync` — held across an await in [`ImageProvider`]'s boxed `Send` future
///
/// [`ImageProvider`]: super::ImageProvider
pub(crate) type LineSink<'a> = &'a (dyn Fn(&str) + Send + Sync);

/// Where one build's output travels. BuildKit gates its progress UI on a TTY, so the
/// `--progress` flag baked into the command and the exec that runs it must agree — one
/// value both read, never the same predicate answered twice
pub(crate) enum Route<'a> {
    Pty { host: &'a dyn ChildHost, size: (u16, u16) },
    Lines,
}

impl<'a> Route<'a> {
    pub(crate) fn of(host: Option<&'a dyn ChildHost>) -> Route<'a> {
        match host.and_then(|h| h.live_size().map(|size| (h, size))) {
            Some((host, size)) => Route::Pty { host, size },
            None => Route::Lines,
        }
    }

    /// `buildctl --progress` mode this route can actually render
    pub(crate) fn progress(&self) -> &'static str {
        match self {
            Route::Pty { .. } => "auto",
            Route::Lines => "plain",
        }
    }
}

/// One build `exec` over its [`Route`] → `(output tail, exit code)`
pub(crate) async fn exec_build(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
    route: &Route<'_>,
    on_line: Option<LineSink<'_>>,
) -> Result<(String, i32), PipelineError> {
    match route {
        Route::Pty { host, size } => {
            exec_tty(api, pod, cmd, *size, &|bytes| host.write_live(bytes)).await
        }
        Route::Lines => {
            let (_out, err, code) = exec_streamed(api, pod, cmd, on_line).await?;
            Ok((err, code))
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
) -> Result<(String, String, i32), PipelineError> {
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
    on_bytes: &(dyn Fn(&[u8]) + Send + Sync),
) -> Result<(String, i32), PipelineError> {
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
pub(crate) async fn exec_capture(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
) -> Result<(String, String, i32), PipelineError> {
    exec_streamed(api, pod, cmd, None).await
}

/// Already-staged probe. Exec failure → `false`, so the caller re-stages rather than
/// building against something it never confirmed
pub(crate) async fn dir_exists(api: &Api<Pod>, pod: &str, dir: &str) -> bool {
    let cmd = format!("test -d {}", shell_quote(dir));
    matches!(exec_capture(api, pod, &cmd).await, Ok((_, _, 0)))
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

/// Heredoc → multi-line/special-char file through one `sh -c` (quoted delimiter = no expansion)
pub(crate) async fn stage_file(
    api: &Api<Pod>,
    pod: &str,
    dir: &str,
    name: &str,
    contents: &str,
) -> Result<(), PipelineError> {
    let cmd = format!(
        "mkdir -p {dir}\ncat > {dir}/{name} <<'ZTEST_DF_EOF'\n{contents}\nZTEST_DF_EOF\n",
        dir = shell_quote(dir),
        name = shell_quote(name),
    );
    let (_o, err, code) = exec_capture(api, pod, &cmd).await?;
    if code != 0 {
        return Err(format!("stage {name} in pod:\n{}", tail(&err, 20)).into());
    }
    Ok(())
}

/// `--output type=local` export back into `dest`: `tar -c` in the pod, `tar -x` here.
///
/// - Small files (`list.json` + `inventory.jsonl`) → collected whole, not streamed
pub(crate) async fn fetch_into(
    api: &Api<Pod>,
    pod: &str,
    dir: &str,
    dest: &Path,
) -> Result<(), PipelineError> {
    use std::io::Write as _;

    std::fs::create_dir_all(dest).map_err(|e| format!("create {}: {e}", dest.display()))?;
    let archive =
        stream_from_pod(api, pod, &format!("tar -cf - -C {d} .", d = shell_quote(dir))).await?;

    let mut untar = std::process::Command::new("tar")
        .arg("-xf")
        .arg("-")
        .arg("-C")
        .arg(dest)
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
        .map_err(|e| format!("write exported archive to local `tar`: {e}"))?;
    let out = untar.wait_with_output().map_err(|e| format!("wait for local `tar -x`: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "unpacking the exported files failed:\n{}",
            tail(&String::from_utf8_lossy(&out.stderr), 40)
        )
        .into());
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(tag: &str) -> Result<Self, PipelineError> {
        let base = std::env::temp_dir().join(format!("{tag}-{:08x}", rand::random::<u32>()));
        std::fs::create_dir_all(&base).map_err(|e| format!("create temp dir: {e}"))?;
        Ok(Self(base))
    }
    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
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
) -> Result<(), PipelineError> {
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
async fn stream_from_pod(api: &Api<Pod>, pod: &str, cmd: &str) -> Result<Vec<u8>, PipelineError> {
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
) -> Result<(), PipelineError> {
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

// ── Build context: local `tar -c` → `tar -x` in the pod ───────────────

/// Spawned `tar` streaming a build context on stdout. Temp dir held so the file list
/// outlives [`spawn_tar`]
pub(crate) struct ContextStream {
    pub(crate) child: std::process::Child,
    _tmp: TempDir,
}

/// One build context into the build pod.
///
/// Git-selected, never a plain `tar` (context = normally a repo root → `.git` + `target/`
/// ride along, multi-GB, over the ceiling)
pub(crate) async fn ship_context(
    api: &Api<Pod>,
    ctx: &super::Context,
    pod: &str,
    dest: &str,
) -> Result<(), PipelineError> {
    ship_stream(api, tar_context(ctx)?, pod, dest).await
}

/// Same selection into a local dir, for a host engine that cannot be handed a wide root
pub(crate) fn extract_context(ctx: &super::Context, dest: &Path) -> Result<(), PipelineError> {
    let mut stream = tar_context(ctx)?;
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

/// Work tree required, not preferred: git selection *is* the exclusion, so outside one
/// there is nothing keeping `target/` and gitignored payloads out
fn tar_context(ctx: &super::Context) -> Result<ContextStream, PipelineError> {
    for repo in &ctx.repos {
        git_repo_root(repo).map_err(|_| {
            PipelineError(format!(
                "build context {} is not in a git work tree; ztest ships a context by its \
                 git file selection (`git ls-files`), which is also what keeps `target/` out",
                repo.display()
            ))
        })?;
    }
    spawn_tar(&ctx.root, &ctx.repos)
}

/// Local `tar -c` piped over `exec` into `tar -x` in the build pod.
///
/// - Bounded channel = backpressure (`tar` blocks once the websocket falls behind)
/// - `tar`'s stderr drained on its own thread (a full pipe deadlocks the stdout reader)
async fn ship_stream(
    api: &Api<Pod>,
    mut stream: ContextStream,
    pod: &str,
    ctx_dir: &str,
) -> Result<(), PipelineError> {
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

/// `tar -c` over `repos`' git file selection, rooted at `root`.
///
/// - `tar` over `exec`, not a sync tool: the buildkit image ships no `rsync`, and a
///   directory-walking copy descends the excluded `target/` trees instead of pruning
/// - Chain archives are gitignored, so `--exclude-standard` drops them here: the build
///   context never sees a multi-GB payload, and a seed is addressed by its manifest
/// - Total bounded by [`CONTEXT_MAX_BYTES`], offender-naming error (a silent multi-GB stall
///   reads as a hung cluster)
fn spawn_tar(root: &Path, repos: &[PathBuf]) -> Result<ContextStream, PipelineError> {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;

    // One NUL-delimited root-relative list, sized as we go (ceiling below names offenders).
    // Temp dir outlives `tar`: holds the file list `tar` reads
    let tmp = TempDir::new("ztest-ship")?;

    let mut list: Vec<u8> = Vec::new();
    let mut total: u64 = 0;
    let mut sized: Vec<(u64, PathBuf)> = Vec::new();
    for repo in repos {
        let repo_rel = repo
            .strip_prefix(root)
            .map_err(|_| format!("repo {} not under the tar root", repo.display()))?
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
            let abs = root.join(OsStr::from_bytes(&rel));
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
    tar.arg("-C").arg(root).arg("--null").arg("-T").arg(&list_path);
    tar.arg("-cf").arg("-").stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = tar.spawn().map_err(|e| format!("spawn local `tar` (is `tar` on PATH?): {e}"))?;
    Ok(ContextStream { child, _tmp: tmp })
}

/// Git repo root containing `dir` (`git rev-parse --show-toplevel`). Non-git `dir` = hard
/// named error ([`spawn_tar`] enumerates via git, honouring each repo's `.gitignore`)
pub(crate) fn git_repo_root(dir: &Path) -> Result<PathBuf, PipelineError> {
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

pub(crate) fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

pub(crate) fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn visible_prefix_hides_exit_sentinel() {
        assert_eq!(visible_prefix(b"Building foo\n"), b"Building foo\n");
        assert_eq!(visible_prefix(b"done\nZTEST_EXIT=0\n"), b"done");
    }

    struct Emulator;

    #[async_trait::async_trait]
    impl ChildHost for Emulator {
        async fn run_child(
            &self,
            _: &str,
            _: &[String],
            _: &[(&str, String)],
        ) -> std::io::Result<i32> {
            Ok(0)
        }
        fn live_size(&self) -> Option<(u16, u16)> {
            Some((100, 8))
        }
    }

    /// No emulator → BuildKit's TTY UI would arrive as escape soup with nothing to draw it;
    /// an emulator left on `plain` would draw nothing
    #[test]
    fn progress_follows_whether_the_host_offers_an_emulator() {
        assert_eq!(Route::of(None).progress(), "plain");
        assert_eq!(Route::of(Some(&Emulator)).progress(), "auto");
    }
}
