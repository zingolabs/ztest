//! Component-pod log capture for test-failure diagnostics.
//!
//! - One-shot kube-API fetch at the test's terminal, pods still alive → per-pod `--tail`,
//!   merged chronologically across pods, capped once
//! - Faithful: components own formatting + colour (zebrad `force_use_color`, zaino
//!   `ZAINOLOG_FORMAT=stream`); ztest never parses/reassembles/recolours a body
//! - ANSI stripped once at the display boundary, only for a non-colour sink

/// Component-log lines shown across *all* pods, most recent by timestamp — not per pod.
///
/// - Doubles as the per-pod `kubectl logs --tail`, exactly enough to recover that global tail
/// - Runner output (panic/error) = primary signal, fetched in full separately
const MAX_LINES: usize = 30;

const COMPONENT_HEADER: &str = "  ── component logs ──\n";

/// One line staged for the merge: `(RFC3339 key, display body)`. Keys are
/// kube-injected for pods, tracing's for the runner — both UTC, so they sort together
type TsLine = (String, String);

/// Each pod's last [`MAX_LINES`], as `(RFC3339 timestamp, "[pod] body")`.
///
/// - Per-pod tail recovers the global tail (any merged survivor is within its own)
/// - One-shot fetch, not a follow — correct only because both runners call this at the
///   test's terminal, before the namespace delete
pub async fn fetch_component_lines(client: &kube::Client, namespace: &str) -> Vec<TsLine> {
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

/// Headed component section, `None` when nothing was captured. Sole renderer — both
/// runners emit this, so the cap and the merge cannot drift apart again
pub fn component_section(lines: Vec<TsLine>, color: bool) -> Option<String> {
    let body = render_recent(lines, color)?;
    Some(format!("{COMPONENT_HEADER}{body}"))
}

/// Pod-path FAIL diagnostic: runner output (unframed, uncapped), then components
/// capped to [`MAX_LINES`], then dead-pod terminal reasons.
///
/// Separate budgets, never one pool — zaino health-checks every ~100 ms and would
/// evict the test's own panic
pub fn unified_output(
    runner_raw: &[u8],
    test_name: &str,
    component_lines: Vec<TsLine>,
    dead: &str,
    color: bool,
) -> Vec<u8> {
    let stripped = crate::libtest::strip_libtest_frame(runner_raw, test_name);
    let stripped = String::from_utf8_lossy(&stripped);

    let mut out = String::new();
    for line in stripped.lines() {
        emit(&mut out, line, color);
    }
    if let Some(section) = component_section(component_lines, color) {
        out.push_str(&section);
    }
    for line in dead.lines() {
        emit(&mut out, line, color);
    }
    out.into_bytes()
}

/// Merge `(timestamp, body)` chronologically → most recent [`MAX_LINES`] as one block,
/// earlier count reported. Pure (fetch is separate), so unit-testable clusterless
fn render_recent(mut lines: Vec<TsLine>, color: bool) -> Option<String> {
    if lines.is_empty() {
        return None;
    }
    // RFC3339 sorts lexically, and stably → same-timestamp continuation lines of
    // a multi-line entry keep their order.
    lines.sort_by(|a, b| a.0.cmp(&b.0));
    let dropped = lines.len().saturating_sub(MAX_LINES);
    let kept = lines.split_off(dropped);

    let mut out = String::new();
    if dropped > 0 {
        out.push_str(&format!(
            "  ⋯ {dropped} earlier line(s) dropped (showing the most recent {})\n",
            kept.len(),
        ));
    }
    for (_, body) in kept {
        emit(&mut out, &body, color);
    }
    Some(out)
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

    fn ts_lines(bodies: &[&str]) -> Vec<TsLine> {
        bodies.iter().enumerate().map(|(i, b)| (format!("{i:04}"), (*b).to_string())).collect()
    }

    #[test]
    fn empty_input_renders_nothing() {
        assert!(render_recent(Vec::new(), true).is_none());
        assert!(component_section(Vec::new(), true).is_none());
    }

    #[test]
    fn strips_component_ansi_when_colour_is_off() {
        let out = render_recent(ts_lines(&["[zebrad] \x1b[33mWARN\x1b[0m x"]), false).unwrap();
        assert!(!out.contains('\x1b'), "ANSI must be stripped on a no-colour sink");
        assert!(out.contains("WARN x"));
    }

    #[test]
    fn keeps_component_ansi_verbatim_when_colour_is_on() {
        let out = render_recent(ts_lines(&["[zebrad] \x1b[33mWARN\x1b[0m x"]), true).unwrap();
        assert!(out.contains("\x1b[33mWARN\x1b[0m x"));
    }

    #[test]
    fn keeps_last_max_lines_total_across_pods_in_order() {
        // Two pods interleaved, pushed out of order → merge must sort by timestamp,
        // keeping the most recent MAX_LINES globally, not per pod.
        let n = MAX_LINES + 5;
        let mut lines: Vec<TsLine> = (0..n)
            .map(|i| {
                let pod = if i % 2 == 0 { "zebrad" } else { "zaino" };
                (format!("{i:04}"), format!("[{pod}] line {i}"))
            })
            .collect();
        lines.reverse();

        let out = render_recent(lines, false).unwrap();
        let body: Vec<&str> = out.lines().collect();

        assert_eq!(body.len(), MAX_LINES + 1);
        assert!(body[0].contains(&format!("{} earlier line(s) dropped", n - MAX_LINES)));
        assert!(body[0].contains(&format!("showing the most recent {MAX_LINES}")));
        assert!(body[1].ends_with("line 5"));
        assert!(body[MAX_LINES].ends_with(&format!("line {}", n - 1)));
    }

    #[test]
    fn under_the_cap_shows_everything_with_no_drop_note() {
        let out = render_recent(ts_lines(&["[zebrad] a", "[zaino] b"]), false).unwrap();
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
        assert!(!out.contains("test result:"));

        let runner_at = out.find("panicked").unwrap();
        let header_at = out.find("── component logs ──").unwrap();
        assert!(runner_at < header_at, "runner output must come first");
        assert!(out.contains("earlier line(s) dropped"));
        assert!(out.contains(&format!("showing the most recent {MAX_LINES}")));

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
