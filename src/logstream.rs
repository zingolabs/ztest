//! Component-pod log capture for test-failure diagnostics.
//!
//! - Capture: one-shot kube-API fetch at the test's terminal (pods still alive), every line,
//!   merged chronologically across pods
//! - Display: [`component_section`] tails to [`LogTail`] (capture + record never cap)
//! - Faithful: components own formatting + colour (zebrad `force_use_color`, zaino
//!   its default `terminal` format); ztest never parses/reassembles/recolours a body
//! - ANSI stripped once at the display boundary, only for a non-colour sink

use crate::engine::output::LogTail;

const COMPONENT_HEADER: &str = "  ── component logs ──\n";

/// One line staged for the merge: `(RFC3339 key, display body)`. Keys are
/// kube-injected for pods, tracing's for the runner — both UTC, so they sort together
type TsLine = (String, String);

/// Every pod's full log as `[pod] body` lines, merged by kube timestamp.
///
/// - One-shot fetch, not a follow — correct only because both runners call this at the
///   test's terminal, before the namespace delete
pub async fn fetch_component_log(client: &kube::Client, namespace: &str) -> Vec<u8> {
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
            .logs(name, &LogParams { timestamps: true, ..Default::default() })
            .await
            .unwrap_or_default();
        for line in logs.lines() {
            // `timestamps: true` prefixes an RFC3339 stamp + space — split off as
            // the merge key; the body already carries the component's own stamp.
            let (ts, body) = line.split_once(' ').unwrap_or(("", line));
            lines.push((ts.to_string(), format!("[{name}] {}", decode(body.as_bytes()))));
        }
    }
    merge(lines)
}

/// Chronological merge → newline-terminated bodies. Pure (fetch separate), so
/// unit-testable clusterless
fn merge(mut lines: Vec<TsLine>) -> Vec<u8> {
    // RFC3339 sorts lexically, and stably → same-timestamp continuation lines of
    // a multi-line entry keep their order.
    lines.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = Vec::new();
    for (_, body) in lines {
        out.extend_from_slice(body.as_bytes());
        out.push(b'\n');
    }
    out
}

/// Headed section of `log`'s most recent `tail` lines, `None` when nothing to show. Sole
/// renderer (live + replay reporter)
pub fn component_section(log: &[u8], tail: LogTail, color: bool) -> Option<String> {
    let text = String::from_utf8_lossy(log);
    let lines: Vec<&str> = text.lines().collect();
    let kept = tail.keep(lines.len());
    if kept == 0 {
        return None;
    }
    let dropped = lines.len() - kept;
    let mut out = String::from(COMPONENT_HEADER);
    if dropped > 0 {
        out.push_str(&format!(
            "  ⋯ {dropped} earlier line(s) dropped (showing the most recent {kept}; \
             `--log-tail all` shows every line)\n",
        ));
    }
    for line in &lines[dropped..] {
        emit(&mut out, line, color);
    }
    Some(out)
}

/// Test-process output as both executors return it: libtest frame stripped, then dead-pod
/// terminal reasons (appended after the strip, else the `test result:` cut eats them). Uncapped
pub fn runner_output(raw: &[u8], test_name: &str, dead: &str) -> Vec<u8> {
    let mut out = crate::libtest::strip_libtest_frame(raw, test_name);
    if !dead.is_empty() {
        if !out.is_empty() && !out.ends_with(b"\n") {
            out.push(b'\n');
        }
        out.extend_from_slice(dead.as_bytes());
    }
    out
}

fn emit(out: &mut String, line: &str, color: bool) {
    out.push_str("  ");
    if color {
        out.push_str(line);
    } else {
        out.push_str(&console::strip_ansi_codes(line));
    }
    out.push('\n');
}

/// Lossy decode (a stray non-UTF-8 byte must not kill the capture; component ANSI is
/// ASCII, untouched)
fn decode(line: &[u8]) -> String {
    let mut s = String::from_utf8_lossy(line).into_owned();
    if s.ends_with('\r') {
        s.pop();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_keeps_every_line_and_the_section_tails_only_at_display() {
        // Two pods interleaved, pushed out of order → merge must sort by timestamp
        let n = 45;
        let mut lines: Vec<TsLine> = (0..n)
            .map(|i| {
                let pod = if i % 2 == 0 { "zebrad" } else { "zaino" };
                (format!("{i:04}"), format!("[{pod}] line {i}"))
            })
            .collect();
        lines.reverse();
        let log = merge(lines);
        let captured: Vec<String> =
            String::from_utf8(log.clone()).unwrap().lines().map(String::from).collect();
        let expected: Vec<String> = (0..n)
            .map(|i| format!("[{}] line {i}", if i % 2 == 0 { "zebrad" } else { "zaino" }))
            .collect();
        assert_eq!(captured, expected, "capture = every line, chronological, uncapped");

        let tailed = component_section(&log, LogTail::Lines(30), false).unwrap();
        let body: Vec<&str> = tailed.lines().collect();
        assert_eq!(body.len(), 1 + 1 + 30, "header + drop note + 30 lines:\n{tailed}");
        assert_eq!(body[0], "  ── component logs ──");
        assert!(body[1].contains("15 earlier line(s) dropped (showing the most recent 30;"));
        assert!(body[1].contains("`--log-tail all`"));
        assert_eq!(body[2], "  [zaino] line 15");
        assert_eq!(body[31], "  [zebrad] line 44");

        let all = component_section(&log, LogTail::All, false).unwrap();
        assert!(!all.contains("dropped"), "`all` = no drop note:\n{all}");
        assert_eq!(all.lines().count(), 1 + n);

        let few = component_section(b"[zebrad] a\n[zaino] b\n", LogTail::Lines(30), false).unwrap();
        assert_eq!(
            few, "  ── component logs ──\n  [zebrad] a\n  [zaino] b\n",
            "under the tail = no note"
        );

        assert_eq!(component_section(&log, LogTail::Lines(0), false), None, "0 hides the section");
        assert_eq!(component_section(b"", LogTail::All, false), None, "nothing captured");
    }

    #[test]
    fn component_ansi_kept_for_colour_sinks_and_stripped_otherwise() {
        let log = b"[zebrad] \x1b[33mWARN\x1b[0m x\n";
        let coloured = component_section(log, LogTail::DEFAULT, true).unwrap();
        assert!(coloured.contains("\x1b[33mWARN\x1b[0m x"), "{coloured:?}");
        let plain = component_section(log, LogTail::DEFAULT, false).unwrap();
        assert!(!plain.contains('\x1b') && plain.contains("WARN x"), "{plain:?}");
    }

    #[test]
    fn runner_output_drops_the_libtest_frame_and_appends_dead_pod_reasons() {
        let runner_raw = b"running 1 test\n\
test my::test ... 2026-07-29T00:00:01Z  INFO ztest::env: starting\n\
thread 'my::test' panicked at json.rs:22:5:\n\
responses disagree: left 1.0 right 1.19\n\
FAILED\n\
\n\
failures:\n\
    my::test\n\
\n\
test result: FAILED. 0 passed; 1 failed; finished in 0.01s\n";

        let out = String::from_utf8(runner_output(
            runner_raw,
            "my::test",
            "container `zebrad` exit 137 (OOMKilled)\n",
        ))
        .unwrap();

        assert!(out.contains("INFO ztest::env: starting"), "{out}");
        assert!(out.contains("thread 'my::test' panicked at json.rs:22:5:"), "{out}");
        assert!(out.contains("responses disagree: left 1.0 right 1.19"), "{out}");
        assert!(!out.contains("test result:"), "libtest frame stripped:\n{out}");
        assert!(
            out.find("panicked").unwrap() < out.find("OOMKilled").unwrap(),
            "dead-pod reasons follow the runner:\n{out}"
        );
    }
}
