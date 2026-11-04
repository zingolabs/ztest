//! libtest stdout framing.
//!
//! - Layer-0: both the engine reporter and log capture strip the same frame

/// Strip one `--exact <test> --nocapture` libtest run's framing from captured
/// stdout+stderr, leaving the test's own output.
///
/// - Framing dropped wherever it landed, never sliced at the `test <name> ... ` marker
///   (pod path merges by read-arrival → body routinely precedes the marker)
/// - Trailing summary popped bottom-up, exactly one verdict (a user line reading
///   `FAILED` survives); no `test result: ` anchor → left un-cut, never silently eaten
pub fn strip_libtest_frame(output: &[u8], test_name: &str) -> Vec<u8> {
    let marker = format!("test {test_name} ... ");
    let marker = marker.as_bytes();

    let mut lines: Vec<&[u8]> = output.split(|&b| b == b'\n').collect();

    if let Some(r) = lines.iter().rposition(|l| l.starts_with(b"test result: ")) {
        lines.truncate(r);
        strip_footer_grammar(&mut lines);
    }

    let mut kept: Vec<&[u8]> = Vec::with_capacity(lines.len());
    for line in lines {
        if is_run_header(line) {
            continue;
        }
        if let Some(rest) = line.strip_prefix(marker) {
            // TTY: first body line glued after the marker → keep it. Pod: only a bare
            // verdict trails → drop the whole line
            if !rest.is_empty() && !is_verdict(rest) {
                kept.push(rest);
            }
            continue;
        }
        kept.push(line);
    }
    let mut lines = kept;

    // Drop the blank lines framing leaves at either edge, then re-join
    while lines.first().is_some_and(|l| l.is_empty()) {
        lines.remove(0);
    }
    while lines.last().is_some_and(|l| l.is_empty()) {
        lines.pop();
    }
    lines.join(&b'\n')
}

/// libtest's `running N tests` run header?
fn is_run_header(line: &[u8]) -> bool {
    let Some(rest) = line.strip_prefix(b"running ") else {
        return false;
    };
    let count = rest.strip_suffix(b" tests").or_else(|| rest.strip_suffix(b" test"));
    count.is_some_and(|c| !c.is_empty() && c.iter().all(u8::is_ascii_digit))
}

/// Pop libtest's end-of-run summary grammar off `lines`: trailing blanks, `failures:`
/// headers, indented names / `---- … ----` capture headers, then exactly one verdict
/// token. Stops at the first real output line; caller has already dropped `test result:`
fn strip_footer_grammar(lines: &mut Vec<&[u8]>) {
    while let Some(&last) = lines.last() {
        if last.is_empty()
            || last == b"failures:"
            || last.starts_with(b"    ")
            || (last.starts_with(b"---- ") && last.ends_with(b" ----"))
        {
            lines.pop();
            continue;
        }
        if is_verdict(last) {
            lines.pop();
        }
        break;
    }
}

/// libtest per-test verdict token: `ok`, `ignored`, `FAILED`, or `FAILED (…)` carrying
/// a `should_panic` note
fn is_verdict(line: &[u8]) -> bool {
    line == b"ok" || line == b"ignored" || line == b"FAILED" || line.starts_with(b"FAILED (")
}

#[cfg(test)]
mod tests {
    use super::strip_libtest_frame;

    #[test]
    fn strip_frame_removes_libtest_scaffolding_from_result_err_run() {
        // Real `--exact <t> --nocapture` capture: libtest header, held-open
        // `test <t> ... ` prefix with the first log glued on, merged body, verdict footer
        let raw = b"\nrunning 1 test\ntest t ... 2026 INFO starting\n2026 INFO provisioning\nError: archive materialize failed\nFAILED\n\nfailures:\n\nfailures:\n    t\n\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.45s\n\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "t")).unwrap();
        assert_eq!(
            got, "2026 INFO starting\n2026 INFO provisioning\nError: archive materialize failed",
            "{got:?}"
        );
    }

    #[test]
    fn strip_frame_removes_scaffolding_from_passing_run() {
        let raw = b"\nrunning 1 test\ntest t ... hello from the test\nok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s\n\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "t")).unwrap();
        assert_eq!(got, "hello from the test", "{got:?}");
    }

    #[test]
    fn strip_frame_keeps_user_line_that_reads_failed() {
        // Only the single trailing verdict token is consumed → a log line reading
        // literally `FAILED` survives
        let raw = b"\nrunning 1 test\ntest t ... step one\nFAILED\nstep two\nFAILED\n\nfailures:\n\nfailures:\n    t\n\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "t")).unwrap();
        assert_eq!(got, "step one\nFAILED\nstep two", "{got:?}");
    }

    #[test]
    fn strip_frame_preserves_panic_body() {
        // Panic prints `thread … panicked` to stderr before the verdict = signal, kept
        let raw = b"\nrunning 1 test\ntest t ... \nthread 't' panicked at src/x.rs:9:5:\nassertion failed\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace\nFAILED\n\nfailures:\n\nfailures:\n    t\n\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "t")).unwrap();
        assert_eq!(
            got,
            "thread 't' panicked at src/x.rs:9:5:\nassertion failed\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
            "{got:?}"
        );
    }

    #[test]
    fn strip_frame_recovers_panic_that_merged_before_the_marker() {
        // Pod path: body → stderr, `test <t> ... ` marker → stdout, and the runtime
        // merges the panic *ahead* of the marker. Slicing at the marker would drop the
        // whole failure; the marker (now a bare verdict) must go instead
        let raw = b"\nrunning 1 test\nthread 't' panicked at src/x.rs:9:5:\nassertion `left == right` failed\n  left: 1\n  right: 2\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace\ntest t ... FAILED\n\nfailures:\n\nfailures:\n    t\n\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "t")).unwrap();
        assert_eq!(
            got,
            "thread 't' panicked at src/x.rs:9:5:\nassertion `left == right` failed\n  left: 1\n  right: 2\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
            "{got:?}"
        );
    }

    #[test]
    fn strip_frame_falls_back_to_verbatim_when_marker_absent() {
        // No `test <t> ... ` marker and no `test result:` anchor → nothing cut but edge
        // blanks (an unexpected format keeps its content rather than risk eating it)
        let raw = b"some unexpected output shape\nwith two lines\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "t")).unwrap();
        assert_eq!(got, "some unexpected output shape\nwith two lines", "{got:?}");
    }

    #[test]
    fn strip_frame_handles_module_qualified_test_name() {
        let raw = b"\nrunning 1 test\ntest mod::sub::it ... log line\nok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "mod::sub::it")).unwrap();
        assert_eq!(got, "log line", "{got:?}");
    }
}
