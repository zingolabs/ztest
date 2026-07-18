//! On-cluster compilation: the laptop ships *source*, the cluster produces the
//! binaries.
//!
//! On an on-cluster-build target `ztest run` does not compile locally. Instead
//! it drives the long-lived builder pod ([`crate::resource::impls::builder`]):
//!
//! 1. resolve the local first-party source set (`cargo metadata`, no compile)
//!    and its common-ancestor subtree;
//! 2. **rsync the delta** of that subtree into the builder's cache PVC
//!    (`oc rsync`, which handles the `kubectl exec` transport) — an unchanged
//!    tree transfers ~nothing;
//! 3. `exec` `cargo nextest list --message-format=json` (incremental compile on
//!    the cache) → reuse [`build::parse_list_summary`];
//! 4. one `exec` running every binary with `ZTEST_DUMP_INVENTORY=1` (batched,
//!    marker-delimited) → reuse [`images::parse_inventory`] + [`images::assemble`];
//! 5. `exec` `crane` to append the freshly-compiled binaries as per-binary layers
//!    onto the runner base and push the runner image — pure registry blob
//!    manipulation, no daemon/buildah/privileged.
//!
//! The result is the same `(BuildOutcome, DumpOutcome, qos, runner-image-ref)`
//! the laptop path produces, with `binary_path`/`cwd` naming the *pod's* paths
//! (`/cache/target/…`, `/cache/src/…`) — exactly what the baked-image
//! pod-per-test execution runs.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, AttachParams, ListParams, TerminalSize};
use tokio::io::AsyncReadExt as _;

use crate::pipeline::build::{self, BuildOutcome};
use crate::pipeline::images::{self, Dumped};
use crate::resource::impls::builder::BUILDER_CONTAINER;
use crate::resource::impls::policy::RUN_NAMESPACE;

/// Where the synced source lands on the cache PVC (mirrors the local
/// common-ancestor subtree, preserving relative layout so path-deps resolve).
const SRC_ROOT: &str = "/cache/src";
/// How long to wait for the builder pod to become Ready before giving up.
const BUILDER_READY_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const POLL: Duration = Duration::from_secs(2);

/// Everything the run pipeline needs from an on-cluster compile — the same
/// shapes the laptop compile + Phase-C dump produce, plus the baked runner ref.
#[derive(Debug)]
pub struct RemoteCompileOutcome {
    pub build: BuildOutcome,
    pub dump: images::DumpOutcome,
    pub qos_by_binary: Vec<(String, Vec<crate::inventory::QosEntry>)>,
    /// Pull reference of the runner image the builder baked + pushed.
    pub runner_image_ref: String,
}

/// Registry coordinates the builder needs for the `crane` bake.
#[derive(Debug)]
pub struct BakeRefs {
    /// In-cluster pull ref of the on-cluster-built runner base the bake appends onto.
    pub base_ref: String,
    /// In-cluster registry base to push the runner image to (`<reg>/<project>`).
    pub runner_repo_ref: String,
}

/// A remote-compile progress transition. `compile_on_cluster` emits these; the
/// caller ([`crate::cli::run`]) owns all formatting, colour, and panel updates —
/// keeping this module free of theme/terminal concerns. Timing is measured here
/// (only this side knows each phase's boundaries) and reported as a `Duration`.
#[derive(Debug)]
pub enum Phase<'a> {
    /// A new phase began; `label` names it for the live panel row (which resets
    /// its timer) and the `•` scrollback line.
    Start(&'a str),
    /// The current phase finished after `dur` → a top-level `✓` line.
    Done { label: &'a str, dur: Duration },
    /// A sub-step result under the current phase (the bake's tar/push split) →
    /// an indented `✓` line; the panel row is untouched.
    Step { label: &'a str, dur: Duration },
    /// A terminal informational line (the pushed runner ref).
    Note(&'a str),
}

/// A phase-transition sink. Held mutably because the caller updates panel state.
pub type PhaseSink<'a> = &'a mut dyn FnMut(Phase<'_>);

/// Drive an on-cluster compile end to end. `list_args` is the same
/// `cargo nextest list` argv the laptop path uses. `compile_out` selects how the
/// compile pass streams (a remote PTY into the caller's emulator, or CI lines);
/// `on_phase` receives the coarse [`Phase`] transitions the caller renders as
/// timed status lines and the live panel row.
pub async fn compile_on_cluster(
    client: &kube::Client,
    list_args: &[String],
    refs: &BakeRefs,
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
    emit(Phase::Start("waiting for the on-cluster builder"));
    let t = Instant::now();
    let pod = wait_for_builder(&api).await?;
    emit(Phase::Done {
        label: "builder ready",
        dur: t.elapsed(),
    });

    // 1–2. Resolve + sync the source subtree; derive where the workspace lands
    // in the pod so cargo runs from the right dir.
    let src = SourceLayout::resolve()?;
    emit(Phase::Start("syncing source to the builder (delta)"));
    let t = Instant::now();
    rsync_source(&src, &pod)?;
    emit(Phase::Done {
        label: "source synced",
        dur: t.elapsed(),
    });
    let pod_workspace = format!("{SRC_ROOT}/{}", src.workspace_rel.display());

    // 3. Compile, then list. Two passes so the compile can run under a PTY (its
    // real progress bar streams live) while the JSON list keeps a clean, tty-free
    // stdout — mirroring the laptop path's `run --no-run` + `list` split.
    //
    // `CARGO_PROFILE_TEST_STRIP=debuginfo`: the test binaries are baked into the
    // runner image and pulled by every pod, so their DWARF (the bulk of an
    // `opt-level=3, debug=true` binary — gigabytes across a suite) is dead weight
    // on the cluster. Stripping it at link keeps symbol *names* (backtraces still
    // name functions) while cutting the baked layer ~5-8x. It MUST be identical on
    // both passes, or pass 2 sees a different profile and recompiles.
    emit(Phase::Start("compiling test binaries on the cluster"));
    let t = Instant::now();
    let args = list_args
        .iter()
        .map(|a| shell_quote(a))
        .collect::<Vec<_>>()
        .join(" ");
    const CARGO_ENV: &str = "CARGO_PROFILE_TEST_STRIP=debuginfo";

    // Pass 1: compile only. Under a PTY cargo is unbuffered and emits its in-place
    // progress bar; `run --no-run` writes nothing machine-readable to stdout, so
    // it can share the PTY's merged stream. `TERM` gives cargo the capabilities
    // the emulator understands.
    let compile_cmd = format!(
        "cd {ws} && {CARGO_ENV} TERM=xterm-256color cargo nextest run --no-run {args}",
        ws = shell_quote(&pod_workspace),
    );
    let (compile_tail, compile_code) = match compile_out {
        Some(CompileOut::Pty { size, sink }) => {
            exec_tty(&api, &pod, &compile_cmd, size, sink).await?
        }
        Some(CompileOut::Lines { sink }) => {
            let (_out, err, code) = exec_streamed(&api, &pod, &compile_cmd, Some(sink)).await?;
            (err, code)
        }
        None => {
            let (_out, err, code) = exec_capture(&api, &pod, &compile_cmd).await?;
            (err, code)
        }
    };
    if compile_code != 0 {
        return Err(format!(
            "on-cluster compile failed (exit {compile_code}):\n{}",
            tail(&compile_tail, 40)
        ));
    }

    // Pass 2: list as JSON — already compiled, so this is the fast freshness pass,
    // and without a TTY stdout stays clean for the parse.
    let list_cmd = format!(
        "cd {ws} && {CARGO_ENV} cargo nextest list --message-format=json {args}",
        ws = shell_quote(&pod_workspace),
    );
    let (list_json, list_err, list_code) = exec_capture(&api, &pod, &list_cmd).await?;
    if list_code != 0 {
        return Err(format!(
            "on-cluster `cargo nextest list` failed (exit {list_code}):\n{}",
            tail(&list_err, 40)
        ));
    }
    let build = build::parse_list_summary(list_json.as_bytes())
        .map_err(|e| format!("parse on-cluster nextest list: {e}"))?;
    let BuildOutcome::Ok {
        selected_binaries, ..
    } = &build
    else {
        // A `Failed` here would mean a non-zero list exit, already handled above.
        return Err("on-cluster nextest list produced no selection".to_string());
    };
    if selected_binaries.is_empty() {
        return Err("on-cluster nextest list selected no test binaries".to_string());
    }
    emit(Phase::Done {
        label: &format!("compiled {} test binaries", selected_binaries.len()),
        dur: t.elapsed(),
    });

    // 4. Inventory dump — every binary in ONE `exec`. A dump is sub-100ms, so N
    // separate execs pay N websocket setups for nothing; a single shell brackets
    // each binary's stdout with begin/end markers (carrying its exit code) that
    // `split_dumps` demuxes back into per-binary chunks.
    emit(Phase::Start("dumping test inventory"));
    let t = Instant::now();
    let dump_cmd = dump_script(selected_binaries);
    let (out, _err, _code) = exec_capture(&api, &pod, &dump_cmd).await?;
    let sections = split_dumps(&out, selected_binaries.len())?;
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
    emit(Phase::Done {
        label: "inventory dumped",
        dur: t.elapsed(),
    });
    // The compiled test binaries run in a pod, so their `/cache/target` paths are
    // correct as-is — but component `dev!` images and data seeds are still hashed
    // + provisioned laptop-side (the pre-existing local-build path), and the dump
    // captured their contexts as the pod's `/cache/src/…` paths. Re-home those to
    // the laptop's source ancestor (the identical tree — it was the rsync source),
    // so context hashing + build staging resolve.
    rehome_dump(&mut dump, &src.ancestor);

    // 5. Bake + push the runner image from the compiled binaries.
    emit(Phase::Start("baking + pushing the runner image (crane)"));
    let t = Instant::now();
    let (runner_image_ref, bake) = bake_runner(&api, &pod, selected_binaries, refs).await?;
    emit(Phase::Step {
        label: &format!("tar + pigz, {}", human_bytes(bake.bytes)),
        dur: Duration::from_secs(bake.tar_secs),
    });
    emit(Phase::Step {
        label: "push layer",
        dur: Duration::from_secs(bake.push_secs),
    });
    emit(Phase::Done {
        label: "runner image baked + pushed",
        dur: t.elapsed(),
    });
    emit(Phase::Note(&format!(
        "runner image ready: {runner_image_ref}"
    )));

    Ok(RemoteCompileOutcome {
        build,
        dump,
        qos_by_binary,
        runner_image_ref,
    })
}

/// Find the builder pod and wait until it reports Ready. Returns its name.
async fn wait_for_builder(api: &Api<Pod>) -> Result<String, String> {
    let selector = "ztest.io/component=builder";
    let start = Instant::now();
    loop {
        if let Ok(list) = api.list(&ListParams::default().labels(selector)).await {
            for p in &list.items {
                if pod_ready(p) {
                    return Ok(p.metadata.name.clone().expect("a listed pod has a name"));
                }
            }
        }
        if start.elapsed() >= BUILDER_READY_TIMEOUT {
            return Err(format!(
                "builder pod not Ready within {}s — is the `ztest-builder` \
                 Deployment provisioned (`ztest setup`) and its image seeded?",
                BUILDER_READY_TIMEOUT.as_secs()
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

fn pod_ready(p: &Pod) -> bool {
    p.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// The outer shell wrapping every builder `exec`: it runs the real command in a
/// nested `sh` (a distinct argv, so no quoting is needed and its `set -e` cannot
/// skip the marker) and prints a `ZTEST_EXIT=<n>` sentinel. That sentinel — not
/// the k8s `exec` Status object, whose Rust shape is version-fragile — is the
/// exit-code source.
const OUTER: &str = r#"sh -c "$1"; printf '\nZTEST_EXIT=%s\n' "$?""#;

/// A live line sink for a remote command's stderr (cargo's `Compiling …`
/// progress) on the non-interactive (CI) compile path — one line per call, no
/// terminal control. `ztest run` points it at stderr.
pub type LineSink<'a> = &'a dyn Fn(&str);

/// A raw-bytes sink for the interactive compile path's live PTY stream. The bytes
/// carry cargo's ANSI + cursor control verbatim; `ztest run` feeds them straight
/// into the console's terminal emulator so the progress bar renders live.
pub type ByteSink<'a> = &'a dyn Fn(&[u8]);

/// Where the compile pass's output goes, and thus whether it runs under a PTY.
pub enum CompileOut<'a> {
    /// Interactive: allocate a remote PTY of `size` (cols, rows) so cargo streams
    /// its real progress bar, and forward the raw bytes to `sink`.
    Pty {
        size: (u16, u16),
        sink: ByteSink<'a>,
    },
    /// Non-interactive (CI): no PTY; forward each stderr line to `sink`.
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

/// `exec` a command in the builder container and capture `(stdout, stderr,
/// exit_code)`, forwarding each stderr line to `on_line` as it arrives (when set).
///
/// No TTY: stdout and stderr stay separate (JSON on stdout stays clean) but are
/// multiplexed over ONE websocket, so they MUST be drained concurrently — a
/// compile that floods stderr fills that channel's buffer while an unread reader
/// blocks the shared stream; draining stdout to EOF first deadlocks, since stdout
/// never reaches EOF until the whole (stderr-blocked) stream closes.
async fn exec_streamed(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
    on_line: Option<LineSink<'_>>,
) -> Result<(String, String, i32), String> {
    use tokio::io::AsyncBufReadExt as _;

    let ap = AttachParams::default()
        .container(BUILDER_CONTAINER)
        .stdin(false)
        .stdout(true)
        .stderr(true);
    let mut attached = api
        .exec(pod, ["/bin/sh", "-c", OUTER, "ztest-exec", cmd], &ap)
        .await
        .map_err(|e| format!("exec in builder pod {pod}: {e}"))?;

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

/// `exec` a command under a remote **PTY** (`tty: true`, à la `kubectl exec -it`)
/// and stream the merged output raw to `on_bytes` as it arrives. The PTY makes
/// cargo behave interactively — unbuffered, coloured, emitting its in-place
/// progress bar — and the bytes (cursor control included) feed straight into the
/// caller's terminal emulator. `size` sets the remote (cols, rows) so the bar
/// matches the live grid. With a TTY stdout and stderr are one stream, so this is
/// only for passes that put no machine-readable data on stdout.
async fn exec_tty(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
    size: (u16, u16),
    on_bytes: ByteSink<'_>,
) -> Result<(String, i32), String> {
    let ap = AttachParams::default()
        .container(BUILDER_CONTAINER)
        .stdin(false)
        .stdout(true)
        .stderr(false)
        .tty(true);
    let mut attached = api
        .exec(pod, ["/bin/sh", "-c", OUTER, "ztest-exec", cmd], &ap)
        .await
        .map_err(|e| format!("exec (tty) in builder pod {pod}: {e}"))?;

    // Size the remote pty so cargo's progress bar wraps to our grid, not its 80×24
    // default. Best-effort: a full buffer just means the bar keeps the default.
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
                    // Keep the exit sentinel out of the live grid (and thus the
                    // committed scrollback); still capture it for the code.
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

/// The chunk up to the `ZTEST_EXIT=` marker (dropping an immediately-preceding
/// newline), so the sentinel never reaches the terminal emulator. The marker is
/// the outer shell's last write — after the child exits — so it arrives intact in
/// one chunk; a chunk without it passes through whole.
fn visible_prefix(chunk: &[u8]) -> &[u8] {
    const M: &[u8] = b"ZTEST_EXIT=";
    match chunk.windows(M.len()).position(|w| w == M) {
        Some(i) if i > 0 && chunk[i - 1] == b'\n' => &chunk[..i - 1],
        Some(i) => &chunk[..i],
        None => chunk,
    }
}

/// Buffered [`exec_streamed`] with no live sink — for the quick, quiet steps
/// (inventory dump, `crane` bake) whose output is only interesting on failure.
async fn exec_capture(
    api: &Api<Pod>,
    pod: &str,
    cmd: &str,
) -> Result<(String, String, i32), String> {
    exec_streamed(api, pod, cmd, None).await
}

/// Split the trailing `ZTEST_EXIT=<n>` marker off captured stdout, returning the
/// clean output and the exit code (absent marker ⇒ the process died before the
/// outer shell's `printf`, treated as failure).
fn split_exit_sentinel(out: &str) -> (String, i32) {
    const MARKER: &str = "ZTEST_EXIT=";
    for (idx, _) in out.match_indices(MARKER) {
        // Must be at a line start (not a substring of some other output).
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

/// One shell that dumps every binary's inventory in sequence, bracketing each
/// with `ZTEST_DUMP_BEGIN <i>` / `ZTEST_DUMP_END <i> rc=<code>` markers. No
/// `set -e`: a binary that exits non-zero records its code in the end marker
/// rather than aborting the run, so [`split_dumps`] can attribute the failure to
/// the right binary.
fn dump_script(binaries: &[build::SelectedBinary]) -> String {
    let mut s = String::new();
    for (i, bin) in binaries.iter().enumerate() {
        s.push_str(&format!(
            "printf '\\nZTEST_DUMP_BEGIN {i}\\n'\n\
             ( cd {cwd} && ZTEST_DUMP_INVENTORY=1 {bin} )\n\
             printf '\\nZTEST_DUMP_END {i} rc=%s\\n' \"$?\"\n",
            cwd = shell_quote(&bin.cwd.to_string_lossy()),
            bin = shell_quote(&bin.binary_path.to_string_lossy()),
        ));
    }
    s
}

/// Demux the batched dump stdout into `(stdout, exit_code)` per binary, in the
/// emitted order (index-aligned with the binaries `dump_script` was given). The
/// marker lines are stripped; each chunk is exactly that binary's stdout, ready
/// for `parse_inventory`. Errors if a marker is malformed or the section count
/// doesn't match `n` — a truncated stream (builder died mid-dump) must fail loud,
/// not silently drop a binary's images.
fn split_dumps(out: &str, n: usize) -> Result<Vec<(String, i32)>, String> {
    let mut result: Vec<(String, i32)> = Vec::with_capacity(n);
    let mut cur: Option<(usize, String)> = None;
    for line in out.lines() {
        if let Some(rest) = line.strip_prefix("ZTEST_DUMP_BEGIN ") {
            let idx: usize = rest
                .trim()
                .parse()
                .map_err(|_| format!("bad dump begin marker: {line:?}"))?;
            cur = Some((idx, String::new()));
        } else if let Some(rest) = line.strip_prefix("ZTEST_DUMP_END ") {
            let mut it = rest.split_whitespace();
            let idx: usize = it
                .next()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| format!("bad dump end marker: {line:?}"))?;
            let rc: i32 = it
                .next()
                .and_then(|s| s.strip_prefix("rc="))
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);
            match cur.take() {
                Some((i, buf)) if i == idx => result.push((buf, rc)),
                _ => return Err(format!("mismatched dump markers at index {idx}")),
            }
        } else if let Some((_, buf)) = cur.as_mut() {
            buf.push_str(line);
            buf.push('\n');
        }
    }
    if result.len() != n {
        return Err(format!(
            "batched inventory dump returned {} sections, expected {n} \
             (builder stream truncated?)",
            result.len()
        ));
    }
    Ok(result)
}

// ── Source set resolution + sync ──────────────────────────────────────

/// The local first-party source topology: the common-ancestor directory that
/// contains every local (path) package + the workspace root, and the
/// workspace's path relative to that ancestor. Syncing the ancestor subtree to
/// [`SRC_ROOT`] preserves the relative layout, so `cargo`'s relative path-deps
/// resolve identically in the pod.
struct SourceLayout {
    ancestor: PathBuf,
    workspace_rel: PathBuf,
}

impl SourceLayout {
    fn resolve() -> Result<Self, String> {
        let meta = cargo_metadata()?;
        let workspace_root = meta["workspace_root"]
            .as_str()
            .ok_or("cargo metadata: no workspace_root")?;
        // Local (path) packages have `source: null`; their manifest dirs plus
        // the workspace root are the first-party tree we must ship.
        let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
        dirs.insert(PathBuf::from(workspace_root));
        if let Some(pkgs) = meta["packages"].as_array() {
            for p in pkgs {
                if !p["source"].is_null() {
                    continue; // registry/git dep — comes from CARGO_HOME on the cache
                }
                if let Some(mp) = p["manifest_path"].as_str()
                    && let Some(dir) = Path::new(mp).parent()
                {
                    dirs.insert(dir.to_path_buf());
                }
            }
        }
        let ancestor = common_ancestor(&dirs).ok_or("cannot derive a common source ancestor")?;
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
        })
    }
}

/// Re-home a dump's laptop-provisioned source paths from the pod's synced tree
/// ([`SRC_ROOT`]) back to the laptop's source `ancestor`. Only local (path)
/// sources need it — `git` sources resolve from a content-addressed cache, and
/// the per-binary test paths stay pod-side (they execute in a pod).
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
    // A test→resource edge keys on the seed source string, so it must be re-homed
    // identically or the edge stops resolving to its node.
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
    match p.strip_prefix(SRC_ROOT) {
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

/// rsync the source subtree into the builder pod via `oc rsync` (which drives
/// rsync over the `kubectl exec` transport — the delta algorithm means an
/// unchanged tree ships ~nothing). `target/` and `.git/` never go up.
fn rsync_source(src: &SourceLayout, pod: &str) -> Result<(), String> {
    let dest = format!("{pod}:{SRC_ROOT}/");
    // Trailing slash on the source: copy the *contents* of the ancestor into
    // SRC_ROOT, preserving the subtree layout.
    let src_arg = format!("{}/", src.ancestor.display());
    let mut cmd = std::process::Command::new("oc");
    cmd.args(["rsync", "--no-perms", "--delete", "--compress"])
        .args(["--exclude", "target", "--exclude", ".git"])
        .args(["-n", RUN_NAMESPACE, "-c", BUILDER_CONTAINER]);
    // The kube *client* (exec) targets the profile's `ZTEST_KUBE_CONTEXT`
    // in-memory, ignoring the kubeconfig's current-context — but `oc` is a
    // separate process that honours only the kubeconfig. Without pinning
    // `--context` here, `oc rsync` would fall back to whatever current-context
    // the pinned kubeconfig happens to carry (frequently a stale local `kind`),
    // silently syncing into the wrong cluster. Pass the same context the client
    // resolved so exec and rsync always hit one cluster.
    if let Some(ctx) =
        std::env::var_os(crate::cluster_config::KUBE_CONTEXT_ENV).filter(|v| !v.is_empty())
    {
        cmd.arg("--context").arg(ctx);
    }
    cmd.arg(&src_arg)
        .arg(&dest)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let out = cmd
        .output()
        .map_err(|e| format!("spawn `oc rsync` (is `oc` on PATH?): {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "oc rsync of source into builder failed:\n{}",
            tail(&String::from_utf8_lossy(&out.stderr), 40)
        ));
    }
    Ok(())
}

// ── Runner-image bake (crane) ─────────────────────────────────────────

/// Append the compiled test binaries onto the runner base and push the runner
/// image, all in-pod via `crane`. Each binary is its **own** layer, placed at its
/// exact compile path (`/cache/target/<profile>/deps/<bin>`) so the baked image's
/// absolute paths match `WorkItem::binary_path`. Per-binary layers (not one
/// monolith) mean an edit to a single test re-pushes only that binary's blob —
/// crane skips the layers the registry already has. Returns the pushed pull ref
/// (content-addressed on the whole binary set).
/// Where the "baking + pushing" time actually goes, split so a regression in
/// either half (a fat layer to tar, a slow registry push) is visible rather than
/// hidden behind one total.
#[derive(Debug)]
pub struct BakeStats {
    /// Uncompressed size of the binaries layers.
    pub bytes: u64,
    /// Seconds spent tarring the binaries into layers.
    pub tar_secs: u64,
    /// Seconds spent in `crane append` (the registry push); 0 when the
    /// content-addressed ref already existed and the bake was skipped.
    pub push_secs: u64,
}

async fn bake_runner(
    api: &Api<Pod>,
    pod: &str,
    binaries: &[build::SelectedBinary],
    refs: &BakeRefs,
) -> Result<(String, BakeStats), String> {
    // Binary paths relative to `/` (tar preserves them, so they land at the same
    // absolute path in the image).
    let rel_paths: Vec<String> = binaries
        .iter()
        .map(|b| {
            b.binary_path
                .to_string_lossy()
                .trim_start_matches('/')
                .to_string()
        })
        .collect();
    let quoted: Vec<String> = rel_paths.iter().map(|p| shell_quote(p)).collect();

    // One layer per binary: an edit to a single test changes only that binary's
    // blob, so crane re-pushes just it and skips the layers the registry already
    // has. Two determinism choices make that cross-run skip actually fire:
    // reproducible tar (fixed mtime/owner) and `pigz -n` (no header timestamp,
    // and pigz's output is independent of thread count) → identical binary bytes
    // yield an identical compressed blob digest, which crane finds already
    // present. `pigz` also moves the gzip off crane's single stdlib thread onto
    // all cores. The tag is content-addressed on the *set* of uncompressed layer
    // digests (combining per-layer hashes, not re-reading the bytes), so an
    // unchanged suite yields the same ref — which the `crane manifest` guard
    // detects to skip tar+compress+push entirely. `date +%s` (integer, portable —
    // no GNU `%N`) brackets the phases so the laptop can attribute the time.
    let script = format!(
        r#"set -eu
cd /
TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
crane auth login "{registry}" -u ztest -p "$TOKEN" --insecure >/dev/null 2>&1
T0=$(date +%s)
HASHES=
BYTES=0
LAYERS=
for P in {paths}; do
  L=$(mktemp /tmp/runner-layer.XXXXXX.tar)
  tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf "$L" "$P"
  HASHES="$HASHES$(sha256sum "$L" | cut -c1-64)"
  BYTES=$((BYTES + $(wc -c < "$L")))
  pigz -n "$L"
  LAYERS="$LAYERS -f $L.gz"
done
T1=$(date +%s)
TAG="dev-$(printf '%s' "$HASHES" | sha256sum | cut -c1-16)"
REF="{repo}:$TAG"
if crane manifest --insecure "$REF" >/dev/null 2>&1; then
  echo "ZTEST_BAKE=$T0 $T1 $T1 $BYTES"
  echo "ZTEST_RUNNER_REF=$REF"
  exit 0
fi
crane append --insecure -b "{base}" $LAYERS -t "$REF" >/dev/null
T2=$(date +%s)
echo "ZTEST_BAKE=$T0 $T1 $T2 $BYTES"
echo "ZTEST_RUNNER_REF=$REF"
"#,
        paths = quoted.join(" "),
        repo = refs.runner_repo_ref,
        base = refs.base_ref,
        registry = registry_host(&refs.runner_repo_ref),
    );
    let (out, err, code) = exec_capture(api, pod, &script).await?;
    if code != 0 {
        return Err(format!(
            "on-cluster runner-image bake (crane) failed (exit {code}):\n{}",
            tail(&err, 40)
        ));
    }
    let reference = out
        .lines()
        .find_map(|l| l.strip_prefix("ZTEST_RUNNER_REF=").map(str::to_string))
        .ok_or_else(|| format!("bake produced no runner ref; output:\n{}", tail(&out, 20)))?;
    let stats = parse_bake_stats(&out).unwrap_or(BakeStats {
        bytes: 0,
        tar_secs: 0,
        push_secs: 0,
    });
    Ok((reference, stats))
}

/// Parse the `ZTEST_BAKE=<t0> <t1> <t2> <bytes>` marker into split durations.
fn parse_bake_stats(out: &str) -> Option<BakeStats> {
    let rest = out.lines().find_map(|l| l.strip_prefix("ZTEST_BAKE="))?;
    let mut it = rest.split_whitespace();
    let t0: u64 = it.next()?.parse().ok()?;
    let t1: u64 = it.next()?.parse().ok()?;
    let t2: u64 = it.next()?.parse().ok()?;
    let bytes: u64 = it.next()?.parse().ok()?;
    Some(BakeStats {
        bytes,
        tar_secs: t1.saturating_sub(t0),
        push_secs: t2.saturating_sub(t1),
    })
}

/// The registry host[:port] of a `host[:port]/project/repo[:tag]` reference.
fn registry_host(reference: &str) -> String {
    reference.split('/').next().unwrap_or(reference).to_string()
}

// ── small helpers ─────────────────────────────────────────────────────

/// Single-quote a value for `/bin/sh`, escaping embedded single quotes.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Human-readable byte count (MiB/GiB) for the bake layer size.
fn human_bytes(n: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    let mib = n as f64 / MIB;
    if mib >= 1024.0 {
        format!("{:.1} GiB", mib / 1024.0)
    } else {
        format!("{mib:.0} MiB")
    }
}

fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_sentinel_splits_code_and_output() {
        let (clean, code) = split_exit_sentinel("hello\nworld\nZTEST_EXIT=0\n");
        assert_eq!(clean, "hello\nworld");
        assert_eq!(code, 0);
        let (_, code) = split_exit_sentinel("boom\nZTEST_EXIT=101\n");
        assert_eq!(code, 101);
    }

    #[test]
    fn exit_sentinel_ignores_midline_and_missing() {
        // A "ZTEST_EXIT=" that isn't at a line start is real output, not the marker.
        let (clean, code) = split_exit_sentinel("echo ZTEST_EXIT=7 here\nZTEST_EXIT=3\n");
        assert_eq!(clean, "echo ZTEST_EXIT=7 here");
        assert_eq!(code, 3);
        // No marker at all ⇒ treated as failure.
        assert_eq!(split_exit_sentinel("partial output").1, 1);
    }

    #[test]
    fn split_dumps_demuxes_per_binary_output_and_codes() {
        let out = "\nZTEST_DUMP_BEGIN 0\n{\"kind\":\"dev\"}\nZTEST_DUMP_END 0 rc=0\n\
                   \nZTEST_DUMP_BEGIN 1\nboom\nZTEST_DUMP_END 1 rc=101\n";
        let sections = split_dumps(out, 2).expect("two well-formed sections");
        assert_eq!(sections[0], ("{\"kind\":\"dev\"}\n".to_string(), 0));
        assert_eq!(sections[1], ("boom\n".to_string(), 101));
    }

    #[test]
    fn split_dumps_rejects_a_truncated_stream() {
        // Builder died after the first binary — the second section never closed.
        let out = "\nZTEST_DUMP_BEGIN 0\n{}\nZTEST_DUMP_END 0 rc=0\n\
                   \nZTEST_DUMP_BEGIN 1\npartial\n";
        assert!(split_dumps(out, 2).is_err());
    }

    #[test]
    fn common_ancestor_of_sibling_repos() {
        let dirs = [
            PathBuf::from("/home/u/proj/zaino/live-tests"),
            PathBuf::from("/home/u/proj/ztest"),
            PathBuf::from("/home/u/proj/zaino"),
        ]
        .into_iter()
        .collect();
        assert_eq!(common_ancestor(&dirs), Some(PathBuf::from("/home/u/proj")));
    }

    #[test]
    fn visible_prefix_hides_exit_sentinel() {
        assert_eq!(visible_prefix(b"Compiling foo\n"), b"Compiling foo\n");
        assert_eq!(visible_prefix(b"done\nZTEST_EXIT=0\n"), b"done");
        assert_eq!(visible_prefix(b"ZTEST_EXIT=101\n"), b"");
    }

    #[test]
    fn bake_stats_split_from_marker() {
        let out = "noise\nZTEST_BAKE=100 104 130 734003200\nZTEST_RUNNER_REF=r:t\n";
        let s = parse_bake_stats(out).expect("marker present");
        assert_eq!(s.tar_secs, 4);
        assert_eq!(s.push_secs, 26);
        assert_eq!(s.bytes, 734003200);
        assert_eq!(human_bytes(s.bytes), "700 MiB");
        assert!(parse_bake_stats("no marker here").is_none());
    }

    #[test]
    fn registry_host_strips_project_and_tag() {
        assert_eq!(
            registry_host("image-registry.svc:5000/ztest-images/ztest-runner:dev-abc"),
            "image-registry.svc:5000"
        );
    }

    #[test]
    fn shell_quote_escapes_single_quotes() {
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
        assert_eq!(shell_quote("plain"), "'plain'");
    }

    #[test]
    fn rehome_maps_pod_src_paths_to_the_laptop_ancestor() {
        let anc = Path::new("/home/u/proj");
        // A pod path under SRC_ROOT is remapped, `..` components preserved.
        assert_eq!(
            rehome_path(
                Path::new("/cache/src/zaino/live-tests/clientless/../.."),
                anc
            ),
            PathBuf::from("/home/u/proj/zaino/live-tests/clientless/../..")
        );
        // A path outside SRC_ROOT (e.g. a git-cache context) is left untouched.
        assert_eq!(
            rehome_path(Path::new("/some/git/cache/ctx"), anc),
            PathBuf::from("/some/git/cache/ctx")
        );
    }
}
