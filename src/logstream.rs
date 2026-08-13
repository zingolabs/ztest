//! Component-pod log capture for test-failure diagnostics.
//!
//! - One `kubectl logs -f --prefix -l ztest.io/component` child per
//!   [`TestEnv`](crate::env::TestEnv) → byte-bounded ring, replayed on failure only
//! - Faithful: components own formatting + colour (zebrad `force_use_color`, zaino
//!   `ZAINOLOG_FORMAT=stream`); ztest buffers, never parses/reassembles/recolours
//! - ANSI stripped once at the display boundary, only for a non-colour sink
//! - Single follow after both deploy phases, `--tail=-1` backfilling full history

use std::collections::VecDeque;
use std::io::{BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// Bytes retained per test; past this the oldest evict (a post-mortem wants the
/// tail nearest the failure). Drop count reaches the render — truncation is never silent
const CAP_BYTES: usize = 1024 * 1024;

/// Component-log lines shown across *all* pods, most recent by timestamp — not
/// per pod.
///
/// - Doubles as the per-pod `kubectl logs --tail`, exactly enough to recover that
///   global tail
/// - Runner output (panic/error) = primary signal, fetched in full separately
const MAX_LINES: usize = 40;

/// Carried by every component pod (see [`crate::manifest`]). Existence selector,
/// scoped to this run by the per-test namespace
const COMPONENT_SELECTOR: &str = "ztest.io/component";

struct Ring {
    lines: VecDeque<String>,
    bytes: usize,
    dropped: usize,
}

impl Ring {
    fn new() -> Self {
        Ring { lines: VecDeque::new(), bytes: 0, dropped: 0 }
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

/// Live `kubectl logs -f` capture: child + the thread draining its stdout into
/// [`ring`](Self::ring). Stopped in [`TestEnv`](crate::env)'s `Drop`; the [`Drop`]
/// below is a backstop against leaking the child
pub(crate) struct LogCapture {
    child: Mutex<Option<Child>>,
    reader: Mutex<Option<JoinHandle<()>>>,
    ring: Arc<Mutex<Ring>>,
}

impl LogCapture {
    /// Follow every component pod in `namespace`.
    ///
    /// - `pods` → `--max-log-requests` (kubectl's default 5 errors past that many)
    /// - Best-effort: unspawnable `kubectl` = inert capture, never a failed test
    pub(crate) fn spawn(namespace: &str, pods: usize) -> Self {
        let ring = Arc::new(Mutex::new(Ring::new()));

        let mut cmd = Command::new("kubectl");
        // kubectl honours only the kubeconfig's current-context → pin the profile's
        // (as the `oc` shellouts do), else a stale local context gets streamed.
        // In-cluster uses the mounted ServiceAccount config, no context applies.
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
                return LogCapture { child: Mutex::new(None), reader: Mutex::new(None), ring };
            }
        };

        let reader = child.stdout.take().map(|out| {
            let ring = ring.clone();
            std::thread::spawn(move || drain(out, &ring))
        });

        LogCapture { child: Mutex::new(Some(child)), reader: Mutex::new(reader), ring }
    }

    /// Kill the follow child, join its reader. Idempotent
    pub(crate) fn stop(&self) {
        if let Some(mut child) = lock(&self.child).take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(handle) = lock(&self.reader).take() {
            let _ = handle.join();
        }
    }

    /// Buffered lines as one block, `None` when nothing was captured. Components'
    /// own bytes: verbatim under `color`, ANSI-stripped once here otherwise
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

/// One line staged for the merge: `(RFC3339 key, display body)`. Keys are
/// kube-injected for pods, tracing's for the runner — both UTC, so they sort together
type TsLine = (String, String);

/// Each pod's last [`MAX_LINES`], as `(RFC3339 timestamp, "[pod] body")`.
///
/// - Per-pod tail recovers the global tail (any merged survivor is within its own)
/// - One-shot fetch, not a follow — correct only because `ztest run` calls this at
///   the test's terminal, before it deletes the namespace
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
            // `timestamps: true` prefixes an RFC3339 stamp + space — split off as
            // the merge key; the body already carries the component's own stamp.
            let (ts, body) = line.split_once(' ').unwrap_or(("", line));
            lines.push((ts.to_string(), format!("[{name}] {}", decode(body.as_bytes()))));
        }
    }
    lines
}

/// Pod-path FAIL diagnostic: runner output (unframed, uncapped), then components
/// capped to [`MAX_LINES`], then dead-pod terminal reasons.
///
/// Separate budgets, never one pool — zaino health-checks every ~100 ms and would
/// evict the test's own panic
pub(crate) fn unified_output(
    runner_raw: &[u8],
    test_name: &str,
    component_lines: Vec<TsLine>,
    dead: &str,
    color: bool,
) -> Vec<u8> {
    let stripped = crate::engine::reporter::strip_libtest_frame(runner_raw, test_name);
    let stripped = String::from_utf8_lossy(&stripped);

    let mut out = String::new();
    let emit = |out: &mut String, line: &str| {
        out.push_str("  ");
        out.push_str(&if color {
            line.to_string()
        } else {
            console::strip_ansi_codes(line).into_owned()
        });
        out.push('\n');
    };

    for line in stripped.lines() {
        emit(&mut out, line);
    }
    // Recent component lines, own cap. The render emits its own drop note, so the
    // header stays accurate capped or not.
    if let Some(timeline) = render_recent(component_lines, color) {
        out.push_str("  ── component logs ──\n");
        out.push_str(&timeline);
    }
    for line in dead.lines() {
        emit(&mut out, line);
    }
    out.into_bytes()
}

/// Merge `(timestamp, body)` chronologically → most recent [`MAX_LINES`] as one
/// block, earlier count reported. Pure (fetch is separate), so unit-testable clusterless
fn render_recent(mut lines: Vec<TsLine>, color: bool) -> Option<String> {
    if lines.is_empty() {
        return None;
    }
    // RFC3339 sorts lexically, and stably → same-timestamp continuation lines of
    // a multi-line entry keep their order.
    lines.sort_by(|a, b| a.0.cmp(&b.0));
    let keep = MAX_LINES.min(lines.len());
    let mut ring = Ring::new();
    ring.dropped = lines.len() - keep;
    for (_, body) in lines.split_off(lines.len() - keep) {
        ring.push(body);
    }
    render_ring(&ring, color)
}

/// Child stdout → `ring`, one verbatim line per entry. Lossy decode (a stray
/// non-UTF-8 byte must not kill the capture; component ANSI is ASCII, untouched)
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
        let out =
            render_ring(&ring_of(&["[pod/zebrad/zebrad] a", "[pod/zaino/zaino] b"]), true).unwrap();
        let bodies: Vec<&str> = out.lines().map(str::trim).collect();
        assert_eq!(bodies, ["[pod/zebrad/zebrad] a", "[pod/zaino/zaino] b"]);
    }

    #[test]
    fn strips_component_ansi_when_colour_is_off() {
        let out =
            render_ring(&ring_of(&["[pod/zebrad/zebrad] \x1b[33mWARN\x1b[0m x"]), false).unwrap();
        assert!(!out.contains('\x1b'), "ANSI must be stripped on a no-colour sink");
        assert!(out.contains("WARN x"));
    }

    #[test]
    fn keeps_component_ansi_verbatim_when_colour_is_on() {
        let out =
            render_ring(&ring_of(&["[pod/zebrad/zebrad] \x1b[33mWARN\x1b[0m x"]), true).unwrap();
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
        assert_eq!(lines, [&"[pod/a/a] one".to_string(), &"[pod/b/b] two".to_string()]);
    }

    #[test]
    fn drain_flushes_a_final_unterminated_line() {
        let ring = Mutex::new(Ring::new());
        drain(&b"[pod/a/a] no trailing newline"[..], &ring);
        assert_eq!(lock(&ring).lines.len(), 1);
    }

    #[test]
    fn render_recent_keeps_last_max_lines_total_across_pods_in_order() {
        // Two pods interleaved, pushed out of order → merge must sort by timestamp,
        // keeping the most recent MAX_LINES globally, not per pod.
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

        // One drop-count header + exactly MAX_LINES kept.
        assert_eq!(body.len(), MAX_LINES + 1);
        assert!(body[0].contains(&format!("{} earlier line(s) dropped", n - MAX_LINES)));
        assert!(body[0].contains(&format!("showing the most recent {MAX_LINES}")));
        // Oldest survivor = index 5, newest = n-1.
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
    fn unified_output_shows_runner_in_full_then_capped_components() {
        // Runner output = small + primary; components far exceed MAX_LINES. Runner
        // section must survive in full under a capped component section.
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
        let components: Vec<TsLine> = (0..MAX_LINES + 10)
            .map(|i| (format!("{i:04}"), format!("[zaino] status check {i}")))
            .collect();

        let out = String::from_utf8(unified_output(
            &runner_raw,
            "my::test",
            components,
            "container `zebrad` exit 137 (OOMKilled)",
            false,
        ))
        .unwrap();

        // Runner tracing AND panic survive verbatim, never evicted by the
        // high-volume component stream.
        assert!(out.contains("INFO ztest::env: starting"));
        assert!(out.contains("INFO ztest::env: calling getdifficulty"));
        assert!(out.contains("thread 'my::test' panicked at json.rs:22:5:"));
        assert!(out.contains("responses disagree: left 1.0 right 1.19"));
        // libtest footer stripped.
        assert!(!out.contains("test result:"));

        // Runner section first; components capped (drop note), runner not.
        let runner_at = out.find("panicked").unwrap();
        let header_at = out.find("── component logs ──").unwrap();
        assert!(runner_at < header_at, "runner output must come first");
        assert!(out.contains("earlier line(s) dropped"));
        assert!(out.contains(&format!("showing the most recent {MAX_LINES}")));

        // Dead-pod terminal reason last.
        assert!(out.contains("exit 137 (OOMKilled)"));
        assert!(header_at < out.find("OOMKilled").unwrap());
    }

    #[test]
    fn unified_output_omits_component_section_when_none_captured() {
        let runner_raw = b"running 1 test\n\
test my::test ... Error: archive missing\n\
test result: FAILED. 0 passed; 1 failed; finished in 0.01s\n"
            .to_vec();
        let out = String::from_utf8(unified_output(&runner_raw, "my::test", Vec::new(), "", false))
            .unwrap();
        assert!(out.contains("Error: archive missing"));
        assert!(!out.contains("── component logs ──"));
    }
}
