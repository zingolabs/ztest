//! Component-pod log capture for test-failure diagnostics.
//!
//! Each [`TestEnv`](crate::env::TestEnv) spawns one `kubectl logs` child that
//! follows every component pod in the test's namespace
//! (`-l ztest.io/component --all-containers --follow`), prefixing each line
//! with its source pod via `--prefix`. Its stdout is drained verbatim into a
//! byte-bounded ring for the test's lifetime. On teardown the ring is rendered
//! into the diagnostic the engine reporter replays only for *failed* tests — a
//! passing test discards the whole buffer, a failing one gets the merged
//! component history that explains it.
//!
//! The capture is *faithful*: kubectl streams each component's own bytes and
//! merges the pods in arrival order, and the components own their formatting
//! and colour — zebrad via `force_use_color`, zaino via a single-line, coloured
//! source config (`ZAINOLOG_FORMAT=stream`). So ztest neither parses, reassembles,
//! nor recolours the stream: it buffers, and on a non-colour sink strips ANSI
//! once at the display boundary. A single follow started after both deploy
//! phases uses `--tail=-1` to backfill each pod's full history, so nothing
//! earlier is lost.

use std::collections::VecDeque;
use std::io::{BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// Total buffered bytes retained per test. Past this the oldest lines are
/// evicted (a failure post-mortem wants the tail nearest the failure); the
/// count of dropped lines is surfaced in the render so truncation is never
/// silent.
const CAP_BYTES: usize = 1024 * 1024;

/// Total component-log lines shown across *all* component pods: the most recent
/// `MAX_LINES` by timestamp, merged. Also the per-pod server-side tail requested
/// from the kube API (`kubectl logs --tail`), which is exactly enough to recover
/// that global tail. The failure diagnostic is a short recent-history summary, not
/// the full stream — and not `MAX_LINES` per pod. The runner pod's own output (the
/// panic / error) is the primary signal and is fetched in full, separately.
const MAX_LINES: usize = 40;

/// The label every component pod carries (see [`crate::manifest`]). One
/// existence selector follows exactly this test's components — the per-test
/// namespace scopes it to this run.
const COMPONENT_SELECTOR: &str = "ztest.io/component";

/// A byte-bounded, arrival-ordered ring of captured log lines.
struct Ring {
    lines: VecDeque<String>,
    bytes: usize,
    dropped: usize,
}

impl Ring {
    fn new() -> Self {
        Ring {
            lines: VecDeque::new(),
            bytes: 0,
            dropped: 0,
        }
    }

    fn push(&mut self, line: String) {
        self.bytes += line.len();
        self.lines.push_back(line);
        while self.bytes > CAP_BYTES {
            let Some(evicted) = self.lines.pop_front() else {
                break;
            };
            self.bytes -= evicted.len();
            self.dropped += 1;
        }
    }
}

/// A live `kubectl logs -f` capture: the child process and the thread draining
/// its stdout into [`ring`](Self::ring). Stopped in [`TestEnv`](crate::env)'s
/// `Drop`; the [`Drop`] impl below is a backstop so a dropped capture never
/// leaks the child.
pub(crate) struct LogCapture {
    child: Mutex<Option<Child>>,
    reader: Mutex<Option<JoinHandle<()>>>,
    ring: Arc<Mutex<Ring>>,
}

impl LogCapture {
    /// Follow every component pod in `namespace`. `pods` sets
    /// `--max-log-requests` (kubectl's default of 5 errors past that many
    /// concurrent follows). Best-effort: if `kubectl` can't be spawned the
    /// capture is inert and renders nothing, never failing the test.
    pub(crate) fn spawn(namespace: &str, pods: usize) -> Self {
        let ring = Arc::new(Mutex::new(Ring::new()));

        let mut cmd = Command::new("kubectl");
        // kubectl honours only the kubeconfig's current-context, so pin the
        // profile's context (as the `oc` shellouts do) or a stale local context
        // could be streamed from. In-cluster the mounted ServiceAccount config
        // is used and no context applies.
        if !crate::cluster::in_cluster()
            && let Ok(ctx) = std::env::var(crate::cluster_config::KUBE_CONTEXT_ENV)
            && !ctx.is_empty()
        {
            cmd.arg("--context").arg(ctx);
        }
        cmd.args([
            "logs",
            "-n",
            namespace,
            "-l",
            COMPONENT_SELECTOR,
            "--all-containers",
            "--prefix",
            "--follow",
            "--tail=-1",
            "--ignore-errors",
            &format!("--max-log-requests={}", pods.max(1)),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    path = %std::env::var("PATH").unwrap_or_default(),
                    "component-log capture unavailable: `kubectl` not on this process's PATH"
                );
                return LogCapture {
                    child: Mutex::new(None),
                    reader: Mutex::new(None),
                    ring,
                };
            }
        };

        let reader = child.stdout.take().map(|out| {
            let ring = ring.clone();
            std::thread::spawn(move || drain(out, &ring))
        });

        LogCapture {
            child: Mutex::new(Some(child)),
            reader: Mutex::new(reader),
            ring,
        }
    }

    /// Kill the follow child and join its reader. Idempotent.
    pub(crate) fn stop(&self) {
        if let Some(mut child) = lock(&self.child).take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(handle) = lock(&self.reader).take() {
            let _ = handle.join();
        }
    }

    /// Render the buffered lines as one block, or `None` when nothing was
    /// captured. The bytes are the components' own: emitted verbatim when
    /// `color`, ANSI-stripped once here when the sink can't render colour.
    pub(crate) fn render(&self, color: bool) -> Option<String> {
        render_ring(&lock(&self.ring), color)
    }
}

impl Drop for LogCapture {
    fn drop(&mut self) {
        self.stop();
    }
}

// ───────────────────── laptop-side unified FAIL diagnostic ────────────────────

/// A timestamped log line staged for the merge: `(RFC3339 key, display body)`.
/// Keys come from the kube-injected timestamp for component pods and from the
/// runner's own tracing timestamp; both are UTC RFC3339, so they sort together.
type TsLine = (String, String);

/// Fetch each component pod's last [`MAX_LINES`] in `namespace`, with server-side
/// timestamps, as `(timestamp, "[pod] body")` lines for the unified timeline.
/// Fetching each pod's tail is exactly enough to recover the global tail (any
/// line in the merged last-`MAX_LINES` is within its own pod's tail).
///
/// A plain definitive fetch — not a live follow — is correct *because* the parent
/// `ztest run` owns teardown: it calls this at the test's terminal, while every
/// component pod still exists, and only then deletes the namespace. That ordering
/// removes the attach-timing and mid-stream-EOF races a `log_stream` follow is
/// prone to. Empty when the namespace has no component pods or none logged.
pub(crate) async fn fetch_component_lines(client: &kube::Client, namespace: &str) -> Vec<TsLine> {
    use k8s_openapi::api::core::v1::Pod;
    use kube::Api;
    use kube::api::{ListParams, LogParams};

    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let Ok(list) = pods.list(&ListParams::default()).await else {
        return Vec::new();
    };
    let mut lines: Vec<TsLine> = Vec::new();
    for pod in &list {
        let Some(name) = pod.metadata.name.as_deref() else {
            continue;
        };
        let logs = pods
            .logs(
                name,
                &LogParams {
                    tail_lines: Some(MAX_LINES as i64),
                    timestamps: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_default();
        for line in logs.lines() {
            // `timestamps: true` prefixes each line with an RFC3339 stamp + space:
            // split it off as the merge key, show the original body (which already
            // carries the component's own timestamp).
            let (ts, body) = line.split_once(' ').unwrap_or(("", line));
            lines.push((ts.to_string(), format!("[{name}] {}", decode(body.as_bytes()))));
        }
    }
    lines
}

/// Build the pod-path FAIL diagnostic: one `[runner]`/`[component]` timeline
/// merged by timestamp and capped to the most recent [`MAX_LINES`], then the
/// runner's panic / assertion block pinned verbatim (never capped), then any
/// dead-pod terminal reasons.
///
/// `runner_raw` is the runner pod's libtest-framed stdout+stderr (no timestamps).
/// Its frame is stripped, and the result split into the test's tracing (which
/// carries its own RFC3339 stamps, so it interleaves with the components) and the
/// panic/assertion tail (untimestamped — it is the conclusion, pinned at the end
/// so the cap can never drop it). Pure, so the assembly is unit-tested without a
/// cluster.
pub(crate) fn unified_output(
    runner_raw: &[u8],
    test_name: &str,
    component_lines: Vec<TsLine>,
    dead: &str,
    color: bool,
) -> Vec<u8> {
    let stripped = crate::engine::reporter::strip_libtest_frame(runner_raw, test_name);
    let stripped = String::from_utf8_lossy(&stripped);
    let (tracing, pinned) = split_assertion(&stripped);

    // Runner tracing joins the merge pool, tagged and keyed by its own leading
    // timestamp; a continuation line (no stamp) inherits the previous key so a
    // multi-line entry stays put.
    let mut pool = component_lines;
    let mut last = String::new();
    for line in tracing {
        let key = line_timestamp(&line)
            .map(str::to_string)
            .unwrap_or_else(|| last.clone());
        last.clone_from(&key);
        pool.push((key, format!("[runner] {line}")));
    }

    let mut out = String::new();
    if let Some(timeline) = render_recent(pool, color) {
        out.push_str(&timeline);
    }
    // Pinned, verbatim, always shown — indented to match the timeline lines.
    for block in [pinned.as_str(), dead] {
        for line in block.lines() {
            out.push_str("  ");
            out.push_str(&if color {
                line.to_string()
            } else {
                console::strip_ansi_codes(line).into_owned()
            });
            out.push('\n');
        }
    }
    out.into_bytes()
}

/// Split a frame-stripped runner body into `(tracing lines, pinned tail)`. The
/// tail is everything from the first failure marker — a Rust panic
/// (`thread '…' panicked`) or a libtest `Error:` return — to the end; it is the
/// assertion/error, pinned so the timeline cap never drops it. No marker (e.g. a
/// timeout with no panic) ⇒ all tracing, empty tail.
fn split_assertion(body: &str) -> (Vec<String>, String) {
    let lines: Vec<&str> = body.lines().collect();
    let cut = lines.iter().position(|l| {
        (l.starts_with("thread ") && l.contains("panicked")) || l.starts_with("Error: ")
    });
    match cut {
        Some(i) => (
            lines[..i].iter().map(|s| s.to_string()).collect(),
            lines[i..].join("\n"),
        ),
        None => (lines.iter().map(|s| s.to_string()).collect(), String::new()),
    }
}

/// The leading RFC3339 timestamp token of a tracing line (the merge key), or
/// `None` for a line that doesn't begin with one (a continuation line). Structural
/// check only — no date parsing / chrono dep: `YYYY-MM-DDT…` with the `-`/`-`/`T`
/// separators in place is enough to distinguish a stamp from ordinary text.
fn line_timestamp(line: &str) -> Option<&str> {
    let tok = line.split_whitespace().next()?;
    let b = tok.as_bytes();
    (b.len() >= 20
        && b[0].is_ascii_digit()
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T')
        .then_some(tok)
}

/// Merge `(timestamp, body)` lines chronologically and render the most recent
/// [`MAX_LINES`] as one block (the earlier count reported). Pure — the fetch is
/// separate — so the merge/cap is unit-tested without a cluster.
fn render_recent(mut lines: Vec<TsLine>, color: bool) -> Option<String> {
    if lines.is_empty() {
        return None;
    }
    // RFC3339 sorts lexically; the sort is stable, so a multi-line entry's
    // same-timestamp continuation lines keep their order.
    lines.sort_by(|a, b| a.0.cmp(&b.0));
    let keep = MAX_LINES.min(lines.len());
    let mut ring = Ring::new();
    ring.dropped = lines.len() - keep;
    for (_, body) in lines.split_off(lines.len() - keep) {
        ring.push(body);
    }
    render_ring(&ring, color)
}

/// Drain one child's stdout into `ring`, one line per entry, verbatim. Decodes
/// lossily so a stray non-UTF-8 byte can't kill the capture; component ANSI is
/// ASCII and survives untouched.
fn drain(out: impl Read, ring: &Mutex<Ring>) {
    let mut reader = BufReader::new(out);
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match reader.read(&mut byte) {
            Ok(0) => break,
            Ok(_) if byte[0] == b'\n' => {
                lock(ring).push(decode(&line));
                line.clear();
            }
            Ok(_) => line.push(byte[0]),
            Err(_) => break,
        }
    }
    if !line.is_empty() {
        lock(ring).push(decode(&line));
    }
}

fn decode(line: &[u8]) -> String {
    let mut s = String::from_utf8_lossy(line).into_owned();
    if s.ends_with('\r') {
        s.pop();
    }
    s
}

fn render_ring(ring: &Ring, color: bool) -> Option<String> {
    if ring.lines.is_empty() {
        return None;
    }
    let mut out = String::new();
    if ring.dropped > 0 {
        out.push_str(&format!(
            "  ⋯ {} earlier line(s) dropped (showing the most recent {})\n",
            ring.dropped,
            ring.lines.len(),
        ));
    }
    for line in &ring.lines {
        out.push_str("  ");
        if color {
            out.push_str(line);
        } else {
            out.push_str(&console::strip_ansi_codes(line));
        }
        out.push('\n');
    }
    Some(out)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring_of(lines: &[&str]) -> Ring {
        let mut r = Ring::new();
        for l in lines {
            r.push((*l).to_string());
        }
        r
    }

    #[test]
    fn empty_ring_renders_nothing() {
        assert!(render_ring(&Ring::new(), true).is_none());
    }

    #[test]
    fn preserves_arrival_order_verbatim_when_coloured() {
        let out = render_ring(
            &ring_of(&["[pod/zebrad/zebrad] a", "[pod/zaino/zaino] b"]),
            true,
        )
        .unwrap();
        let bodies: Vec<&str> = out.lines().map(str::trim).collect();
        assert_eq!(bodies, ["[pod/zebrad/zebrad] a", "[pod/zaino/zaino] b"]);
    }

    #[test]
    fn strips_component_ansi_when_colour_is_off() {
        let out = render_ring(
            &ring_of(&["[pod/zebrad/zebrad] \x1b[33mWARN\x1b[0m x"]),
            false,
        )
        .unwrap();
        assert!(
            !out.contains('\x1b'),
            "ANSI must be stripped on a no-colour sink"
        );
        assert!(out.contains("WARN x"));
    }

    #[test]
    fn keeps_component_ansi_verbatim_when_colour_is_on() {
        let out = render_ring(
            &ring_of(&["[pod/zebrad/zebrad] \x1b[33mWARN\x1b[0m x"]),
            true,
        )
        .unwrap();
        assert!(out.contains("\x1b[33mWARN\x1b[0m x"));
    }

    #[test]
    fn eviction_past_cap_is_reported_and_keeps_the_tail() {
        let mut r = Ring::new();
        let big = "z".repeat(100 * 1024);
        for _ in 0..20 {
            r.push(big.clone());
        }
        r.push("final line".to_string());
        let out = render_ring(&r, false).unwrap();
        assert!(out.contains("earlier line(s) dropped"));
        assert!(out.contains("final line"));
        assert!(r.bytes <= CAP_BYTES);
    }

    #[test]
    fn drain_splits_on_newline_and_trims_cr() {
        let ring = Mutex::new(Ring::new());
        drain(&b"[pod/a/a] one\r\n[pod/b/b] two\n"[..], &ring);
        let r = lock(&ring);
        let lines: Vec<&String> = r.lines.iter().collect();
        assert_eq!(
            lines,
            [&"[pod/a/a] one".to_string(), &"[pod/b/b] two".to_string()]
        );
    }

    #[test]
    fn drain_flushes_a_final_unterminated_line() {
        let ring = Mutex::new(Ring::new());
        drain(&b"[pod/a/a] no trailing newline"[..], &ring);
        assert_eq!(lock(&ring).lines.len(), 1);
    }

    #[test]
    fn render_recent_keeps_last_max_lines_total_across_pods_in_order() {
        // MAX_LINES+5 lines interleaved across two pods, pushed out of order to
        // prove the merge sorts by timestamp. Only the most recent MAX_LINES
        // survive — globally, not per pod — and in chronological order.
        let n = MAX_LINES + 5;
        let mut lines: Vec<(String, String)> = (0..n)
            .map(|i| {
                let pod = if i % 2 == 0 { "zebrad" } else { "zaino" };
                (format!("{i:04}"), format!("[{pod}] line {i}"))
            })
            .collect();
        lines.reverse(); // unsorted input

        let out = render_recent(lines, false).unwrap();
        let body: Vec<&str> = out.lines().collect();

        // One dropped-count header + exactly MAX_LINES kept lines.
        assert_eq!(body.len(), MAX_LINES + 1);
        assert!(body[0].contains(&format!("{} earlier line(s) dropped", n - MAX_LINES)));
        assert!(body[0].contains(&format!("showing the most recent {MAX_LINES}")));
        // First kept line is index 5 (the oldest survivor); last is n-1.
        assert!(body[1].ends_with("line 5"));
        assert!(body[MAX_LINES].ends_with(&format!("line {}", n - 1)));
    }

    #[test]
    fn render_recent_under_the_cap_shows_everything_with_no_drop_note() {
        let lines = vec![
            ("0001".to_string(), "[zebrad] a".to_string()),
            ("0002".to_string(), "[zaino] b".to_string()),
        ];
        let out = render_recent(lines, false).unwrap();
        assert!(!out.contains("dropped"));
        let body: Vec<&str> = out.lines().map(str::trim).collect();
        assert_eq!(body, ["[zebrad] a", "[zaino] b"]);
    }

    #[test]
    fn line_timestamp_recognizes_rfc3339_prefix_only() {
        assert_eq!(
            line_timestamp("2026-07-29T16:54:40.454305Z  INFO ztest::env: hi"),
            Some("2026-07-29T16:54:40.454305Z"),
        );
        // A continuation / non-timestamped line has no key.
        assert_eq!(line_timestamp("    at packages/zaino-state/src/lib.rs:12"), None);
        assert_eq!(line_timestamp(""), None);
    }

    #[test]
    fn split_assertion_pins_from_the_panic_or_error_marker() {
        let (tracing, pinned) = split_assertion(
            "2026-07-29T00:00:01Z INFO starting\n\
             thread 'my::test' panicked at src/foo.rs:1:1:\n\
             values disagree\n\
             note: run with RUST_BACKTRACE=1",
        );
        assert_eq!(tracing, ["2026-07-29T00:00:01Z INFO starting"]);
        assert!(pinned.starts_with("thread 'my::test' panicked"));
        assert!(pinned.contains("values disagree"));

        // A libtest `Error:` return is also a pin point.
        let (tracing, pinned) = split_assertion("log line\nError: archive missing");
        assert_eq!(tracing, ["log line"]);
        assert_eq!(pinned, "Error: archive missing");

        // No failure marker (e.g. a timeout) ⇒ everything is tracing.
        let (tracing, pinned) = split_assertion("a\nb");
        assert_eq!(tracing, ["a", "b"]);
        assert!(pinned.is_empty());
    }

    #[test]
    fn unified_output_interleaves_runner_with_components_and_pins_the_panic() {
        // Runner tracing (its own RFC3339 stamps) at :01 and :03; one component
        // line at :02 — so the merge must weave the component between the two
        // runner lines, and the panic must land last, after the timeline.
        let runner_raw = b"running 1 test\n\
test my::test ... 2026-07-29T00:00:01Z  INFO ztest::env: starting\n\
2026-07-29T00:00:03Z  INFO ztest::env: calling getdifficulty\n\
thread 'my::test' panicked at json.rs:22:5:\n\
responses disagree: left 1.0 right 1.19\n\
note: run with RUST_BACKTRACE=1\n\
FAILED\n\
\n\
failures:\n\
    my::test\n\
\n\
test result: FAILED. 0 passed; 1 failed; finished in 0.01s\n"
            .to_vec();
        let components = vec![(
            "2026-07-29T00:00:02Z".to_string(),
            "[zebrad] block 1".to_string(),
        )];

        let out = String::from_utf8(unified_output(
            &runner_raw,
            "my::test",
            components,
            "",
            false,
        ))
        .unwrap();
        let body: Vec<&str> = out.lines().map(str::trim).collect();

        // Interleaved by timestamp: runner :01, component :02, runner :03.
        assert_eq!(
            body[0],
            "[runner] 2026-07-29T00:00:01Z  INFO ztest::env: starting"
        );
        assert_eq!(body[1], "[zebrad] block 1");
        assert_eq!(
            body[2],
            "[runner] 2026-07-29T00:00:03Z  INFO ztest::env: calling getdifficulty"
        );
        // The panic/assertion is pinned after the timeline, in full.
        assert_eq!(body[3], "thread 'my::test' panicked at json.rs:22:5:");
        assert!(out.contains("responses disagree: left 1.0 right 1.19"));
        // The libtest footer (FAILED / failures / test result) is gone.
        assert!(!out.contains("test result:"));
    }
}
