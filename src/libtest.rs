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
