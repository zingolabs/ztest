//! ztest-owned run reporter reproducing `cargo nextest run`'s default human output.
//!
//! - Every line owned here → no `nextest-runner` dep (mirrors `reporter/displayer/imp.rs`
//!   down to indentation, casing and colours)
//! - Scrollback only; the pinned QoS panel belongs to the run loop
//! - [`progress_line`]/[`render_running`] kept for a future scrollback progress feed
//! - Ceiling: [`Verdict`] = pass/fail/timeout/spawn-error → nextest's
//!   leak/flaky/slow-pass/abort words unreachable without a richer event model
//!
//! Divergences from nextest, each documented at its site:
//!
//! 1. Ctrl-C death → own `sigkilled` term, kept out of the failure recap
//!    ([`RunStats::terminated`]); nextest folds signal deaths into `failed`
//! 2. Its captured output never replayed ([`handle`](StyledReporter::handle))
//! 3. libtest's per-run framing stripped before replay ([`strip_libtest_frame`]) —
//!    pure noise beside the `FAIL […] name` line and `Summary` block

use std::io::Write as _;
use std::time::Duration;

use owo_colors::OwoColorize as _;

use crate::engine::events::{CancelReason, RunReporter, RunStats, RunningView, TestEvent, Verdict};
use crate::engine::output::OutputConfig;

/// Status-line colour intent, mirroring nextest's `Styles` (`reporter/helpers.rs`)
#[derive(Clone, Copy)]
enum Ink {
    Pass,
    Fail,
    Retry,
    Skip,
}

/// nextest-style reporter. `color`/`unicode` resolved by the caller from terminal
/// support (`supports_color`/`supports_unicode`); non-TTY → `color = false`.
///
/// - `failures` = each failing test's styled line, for [`summary`](Self::summary)'s
///   end-of-run recap (else a long run's failures scroll away unconsolidated)
/// - `final_outputs` defers `Final`/`ImmediateFinal` as (styled line, raw bytes, passed);
///   `passed` inks the replay header by verdict
#[derive(Debug)]
pub struct StyledReporter {
    color: bool,
    unicode: bool,
    buf: Vec<u8>,
    stats: RunStats,
    cancelled: Option<CancelReason>,
    failures: Vec<String>,
    output: OutputConfig,
    final_outputs: Vec<(String, Vec<u8>, bool)>,
}

impl StyledReporter {
    /// `unicode` selects `─`/`───` over the ASCII `-`/`---` fallback (nextest's
    /// `ThemeCharacters`)
    pub fn new(color: bool, unicode: bool, output: OutputConfig) -> Self {
        Self {
            color,
            unicode,
            buf: Vec::new(),
            stats: RunStats::default(),
            cancelled: None,
            failures: Vec::new(),
            output,
            final_outputs: Vec::new(),
        }
    }

    /// Right-align a status word in a fixed 12-col field and colour it. Padding first,
    /// then painting = byte-identical to nextest styling *through* the formatter
    fn status(&self, word: &str, ink: Ink) -> String {
        let padded = format!("{word:>12}");
        self.paint(&padded, ink)
    }

    fn paint(&self, s: &str, ink: Ink) -> String {
        paint_word(s, ink, self.color)
    }

    /// Horizontal rule of `n` chars: `─` (U+2500) with unicode, else ASCII `-`
    /// (nextest's `ThemeCharacters::hbar`, `helpers.rs`)
    fn hbar(&self, n: usize) -> String {
        let c = if self.unicode { '─' } else { '-' };
        std::iter::repeat_n(c, n).collect()
    }

    fn styled_instance(&self, bin: &str, test: &str) -> String {
        instance_str(bin, test, self.color)
    }

    /// One status line, no trailing newline: `{status:>12} {bracket}{instance}`
    /// (`bracket` carries nextest's trailing space). Shared by [`line`](Self::line) and
    /// the failure recap → a recapped line is byte-identical to its inline form
    fn format_line(&self, word: &str, ink: Ink, bracket: &str, bin: &str, test: &str) -> String {
        format!("{} {bracket}{}", self.status(word, ink), self.styled_instance(bin, test),)
    }

    fn line(&mut self, word: &str, ink: Ink, bracket: &str, bin: &str, test: &str) {
        let line = self.format_line(word, ink, bracket, bin, test);
        let _ = writeln!(self.buf, "{line}");
    }

    /// Replay captured output in nextest's default indented combined-stream layout
    /// (`unit_output.rs`): `output ───` header inked by `ink`, body indented four, no
    /// closing rule. Combined header, not split stdout/stderr (the engine merges).
    /// `output` must already be [`strip_libtest_frame`]d
    fn replay_output(&mut self, output: &[u8], ink: Ink) {
        if output.is_empty() {
            return;
        }
        // Header: " " + "output" + hbar(3), each piece styled separately
        let header = format!(
            "{} {} {}",
            self.paint(" ", ink),
            self.paint("output", ink),
            self.paint(&self.hbar(3), ink),
        );
        let _ = writeln!(self.buf, "{header}");
        // Indent non-empty lines by four (blanks stay bare, per the `indenter` crate
        // nextest uses); trailing newline so the next scrollback line isn't glued on
        let mut start = 0;
        for i in 0..output.len() {
            if output[i] == b'\n' {
                let l = &output[start..i];
                if !l.is_empty() {
                    let _ = self.buf.write_all(b"    ");
                }
                let _ = self.buf.write_all(l);
                let _ = self.buf.write_all(b"\n");
                start = i + 1;
            }
        }
        if start < output.len() {
            let _ = self.buf.write_all(b"    ");
            let _ = self.buf.write_all(&output[start..]);
            let _ = self.buf.write_all(b"\n");
        }
    }
}

/// Strip one `--exact <test> --nocapture` libtest run's framing from captured
/// stdout+stderr, leaving the test's own output.
///
/// - Framing dropped wherever it landed, never sliced at the `test <name> ... ` marker
///   (pod path merges by read-arrival → body routinely precedes the marker)
/// - Trailing summary popped bottom-up, exactly one verdict (a user line reading
///   `FAILED` survives); no `test result: ` anchor → left un-cut, never silently eaten
pub(crate) fn strip_libtest_frame(output: &[u8], test_name: &str) -> Vec<u8> {
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

impl RunReporter for StyledReporter {
    fn handle(&mut self, ev: &TestEvent<'_>) {
        match ev {
            TestEvent::RunStarted { total, .. } => {
                self.stats.total = *total;
                // Faithful subset of nextest's RunStarted: no run-ID/profile line, no
                // "across N binaries" (neither exists in the ztest event model)
                let word = if *total == 1 { "test" } else { "tests" };
                let _ = writeln!(
                    self.buf,
                    "{} {} {word}",
                    self.status("Starting", Ink::Pass),
                    bold_count(*total as u64, self.color),
                );
            }
            // Test starts show in the pinned QoS panel, not scrollback
            TestEvent::TestStarted { .. } => {}
            TestEvent::TestSlow { binary_id, test_name, elapsed, will_terminate, attempt } => {
                // *Slow* bracket `[>  d.ddds] `, not the ordinary one (`imp.rs` →
                // `DisplaySlowDuration`)
                let bracket = bracket_slow(*elapsed);
                if *will_terminate {
                    self.line("TERMINATING", Ink::Fail, &bracket, binary_id, test_name);
                } else if *attempt > 1 {
                    let word = format!("TRY {attempt} SLOW");
                    self.line(&word, Ink::Skip, &bracket, binary_id, test_name);
                } else {
                    self.line("SLOW", Ink::Skip, &bracket, binary_id, test_name);
                }
            }
            TestEvent::TestRetrying {
                binary_id,
                test_name,
                next_attempt,
                verdict,
                duration,
                ..
            } => {
                // The attempt that just failed (nextest's `retry_data.attempt`) →
                // magenta `TRY {n} {short}` with its real duration
                let failed_attempt = next_attempt.saturating_sub(1).max(1);
                let word = format!("TRY {failed_attempt} {}", short_status(verdict));
                self.line(&word, Ink::Retry, &bracket_dur(*duration), binary_id, test_name);
            }
            TestEvent::TestFinished {
                binary_id,
                test_name,
                verdict,
                duration,
                attempt,
                output,
            } => {
                let bracket = bracket_dur(*duration);
                let passed = matches!(verdict, Verdict::Pass);
                let (word, ink) = if passed {
                    ("PASS".to_string(), Ink::Pass)
                } else {
                    // Attempt 1 → long word (`FAIL`/`TIMEOUT`/`XFAIL`); a later attempt
                    // → `TRY {n} {short}`. Both red (`ExecutionDescription::Failure`)
                    let w = if *attempt > 1 {
                        format!("TRY {attempt} {}", short_status(verdict))
                    } else {
                        long_status(verdict).to_string()
                    };
                    (w, Ink::Fail)
                };
                let line = self.format_line(&word, ink, &bracket, binary_id, test_name);
                let _ = writeln!(self.buf, "{line}");
                let terminated = matches!(verdict, Verdict::Terminated);
                if passed {
                    self.stats.passed += 1;
                } else if terminated {
                    self.stats.terminated += 1;
                } else {
                    // Only real failures join the recap (a recap = a to-do list; a test
                    // the operator killed asks nothing). `sigkilled` carries the count
                    self.failures.push(line.clone());
                    self.stats.failed += 1;
                }
                // Captured-output display per verdict (`success-output`/`failure-output`):
                // inline now if `immediate`, deferred to the end block if `final`. Framing
                // stripped once here so both paths show the same bytes.
                //
                // Terminated tests exempt — divergence: nextest routes every `Fail{..}`,
                // signal aborts included, through `failure_output`, so Ctrl-C with N in
                // flight buries the summary under N component-log streams. Bytes stay in
                // the run record for `ztest replay`; the `SIGKILL` line still prints
                if !terminated {
                    let display = self.output.display_for(passed);
                    // Pod path: `output` is already the laptop-assembled unified
                    // timeline (frame-free) → no-op. Local path: strips libtest framing
                    let shown = strip_libtest_frame(output, test_name);
                    if display.is_immediate() {
                        self.replay_output(&shown, ink);
                    }
                    if display.is_final() && !shown.is_empty() {
                        self.final_outputs.push((line, shown, passed));
                    }
                }
            }
            TestEvent::TestSkipped { binary_id, test_name, reason } => {
                // nextest's skip line: `SKIP` + empty-duration placeholder + instance
                // (`imp.rs` → `write_skip_line`). Reason always appended — no nextest
                // analogue, and a run that skips everything must say why
                use crate::engine::events::SkipReason;
                let note = match reason {
                    SkipReason::DependencyUnavailable { resource } => {
                        format!("resource unavailable: {resource}")
                    }
                    SkipReason::ExceedsClusterCapacity => {
                        "exceeds cluster capacity (raise the cluster ceiling or lower the tier)"
                            .to_string()
                    }
                    SkipReason::ExceedsSaBudget => "exceeds ServiceAccount budget".to_string(),
                };
                let _ = writeln!(
                    self.buf,
                    "{} {BRACKET_SKIP}{} {}",
                    self.status("SKIP", Ink::Skip),
                    self.styled_instance(binary_id, test_name),
                    self.paint(&format!("({note})"), Ink::Skip),
                );
                self.stats.skipped += 1;
            }
            TestEvent::RunCancelling { reason, running } => {
                // nextest's `RunBeginCancel`: fail-styled `Canceling` naming the reason
                // and how many tests are being terminated
                self.cancelled = Some(*reason);
                let noun = if *running == 1 { "test" } else { "tests" };
                let _ = writeln!(
                    self.buf,
                    "{} due to {}: {} {noun} still running",
                    self.status("Canceling", Ink::Fail),
                    reason.as_str(),
                    bold_count(*running as u64, self.color),
                );
            }
            TestEvent::RunFinished { stats, elapsed } => {
                self.emit_final_outputs();
                self.summary(stats, *elapsed);
            }
        }
    }

    fn take_scrollback(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }
}

impl StyledReporter {
    /// End-of-run captured-output block for `Final`/`ImmediateFinal` policies (nextest
    /// groups these after the run, not inline). No-op when nothing was deferred
    fn emit_final_outputs(&mut self) {
        if self.final_outputs.is_empty() {
            return;
        }
        for (line, output, passed) in std::mem::take(&mut self.final_outputs) {
            let ink = if passed { Ink::Pass } else { Ink::Fail };
            let _ = writeln!(self.buf, "{line}");
            self.replay_output(&output, ink);
        }
    }

    /// Closing summary: a rule, then
    /// `Summary [   d.ddds] N tests run: X passed[, Y failed], Z skipped` (nextest's
    /// `RunFinished`, `imp.rs`), then the failure recap when something failed
    fn summary(&mut self, stats: &RunStats, elapsed: Duration) {
        let _ = writeln!(self.buf, "{}", self.hbar(12));
        // "Summary" inked by outcome: fail if anything failed or was cancelled, skip
        // if nothing ran, else pass
        let ink = if stats.failed > 0 || self.cancelled.is_some() {
            Ink::Fail
        } else if stats.finished() == 0 {
            Ink::Skip
        } else {
            Ink::Pass
        };
        let label = self.paint(&format!("{:>12}", "Summary"), ink);
        // `29/132` when the run stopped short, else the count. Numerator = tests that
        // executed → skips sit in the tally, not the ratio (nextest's `finished_count`)
        let executed = stats.ran();
        let ran = if (executed as usize) != stats.total {
            format!(
                "{}/{}",
                bold_count(executed as u64, self.color),
                bold_count(stats.total as u64, self.color),
            )
        } else {
            bold_count(executed as u64, self.color)
        };
        // Singular "test run" only when both counts are 1 (nextest's
        // `tests_plural_if(initial != 1 || finished != 1)`)
        let tests = if stats.total != 1 || executed != 1 { "tests" } else { "test" };
        let _ = writeln!(
            self.buf,
            "{label} {}{ran} {tests} run: {}",
            bracket_dur(elapsed),
            counts_tail(stats, self.color),
        );
        // nextest's failure recap (`final-status-level = fail`): each failing status
        // line re-listed under Summary, no output replay, only when something failed
        for line in std::mem::take(&mut self.failures) {
            let _ = writeln!(self.buf, "{line}");
        }
        // nextest's cancelled `FinalRunStats`: fail-styled `cancelled due to {reason}`
        if let Some(reason) = self.cancelled {
            let _ = writeln!(
                self.buf,
                "{} due to {}",
                self.status("Canceled", Ink::Fail),
                reason.as_str(),
            );
        }
        self.not_run_warning(stats);
    }

    /// nextest's final not-run warning (`write_final_warnings_for_failure`):
    /// `warning: 103/132 tests were not run due to interrupt`, reason clause only when
    /// cancelled. Still-queued tests are *not* folded into `skipped` — across ztest that
    /// means "terminal without running" and gates a setup-error exit; Ctrl-C is not one
    fn not_run_warning(&mut self, stats: &RunStats) {
        let not_run = stats.not_run();
        if not_run == 0 {
            return;
        }
        let plural = stats.total != 1 || not_run != 1;
        let due_to = match self.cancelled {
            Some(reason) => format!(" due to {}", self.paint(reason.as_str(), Ink::Skip),),
            None => String::new(),
        };
        let _ = writeln!(
            self.buf,
            "{}: {}/{} {} {} not run{due_to}",
            self.paint("warning", Ink::Skip),
            bold_count(not_run as u64, self.color),
            bold_count(stats.total as u64, self.color),
            if plural { "tests" } else { "test" },
            if plural { "were" } else { "was" },
        );
    }
}

/// nextest's long `status_str` for a failing verdict, on attempt-1 and final lines
/// (`imp.rs`). `Pass` never reaches here
fn long_status(v: &Verdict) -> &'static str {
    match v {
        Verdict::Fail(_) => "FAIL",
        Verdict::Timeout => "TIMEOUT",
        Verdict::SpawnError => "XFAIL",
        Verdict::Pass => "PASS",
        // Killed by the run's cancellation (we SIGKILL its process group) → the signal
        // is the honest word, matching nextest's per-test `SIG…` display
        Verdict::Terminated => "SIGKILL",
    }
}

/// nextest's `short_status_str` for retry and post-retry final lines, max 6 chars:
/// `FAIL`/`TMT`/`XFAIL` (`imp.rs`)
fn short_status(v: &Verdict) -> &'static str {
    match v {
        Verdict::Fail(_) => "FAIL",
        Verdict::Timeout => "TMT",
        Verdict::SpawnError => "XFAIL",
        Verdict::Pass => "PASS",
        Verdict::Terminated => "SIGKILL",
    }
}

/// `[   1.000s] ` — nextest's `DisplayBracketedDuration`: right-aligned 8-wide, 3-dp
/// seconds, trailing space included
fn bracket_dur(d: Duration) -> String {
    format!("[{:>8.3}s] ", d.as_secs_f64())
}

/// `[>  1.000s] ` — nextest's `DisplaySlowDuration`: literal `>`, 7-wide 3-dp seconds
/// field, trailing space
fn bracket_slow(d: Duration) -> String {
    format!("[>{:>7.3}s] ", d.as_secs_f64())
}

/// nextest's empty-duration placeholder for SKIP: `[` + 9 spaces + `]` (a real
/// duration's width) + trailing space → the instance column stays aligned
const BRACKET_SKIP: &str = "[         ] ";

/// `{binary_id} {module::path}::{leaf}` — binary-id magenta·bold, path cyan, leaf
/// blue·bold (nextest's `DisplayTestInstance`). Shared by verdict lines and live region
fn instance_str(bin: &str, test: &str, color: bool) -> String {
    if !color {
        return format!("{bin} {test}");
    }
    let bin = bin.magenta().bold().to_string();
    let test = match test.rsplit_once("::") {
        Some((module, leaf)) => {
            format!("{}{}{}", module.cyan(), "::".cyan(), leaf.blue().bold())
        }
        None => test.blue().bold().to_string(),
    };
    format!("{bin} {test}")
}

/// `HH:MM:SS` elapsed, matching nextest's `DisplayBracketedHhMmSs` (`formatters.rs`).
///
/// nextest's *running-row* clock (`progress.rs`) has a minutes-overflow bug
/// (`as_secs()/60`, no `% 60`); this `% 60` version stays correct past an hour
// Retained for a future scrollback progress/START feed; unused since the pinned
// per-test live window gave way to the constant QoS panel (`engine::run_tty`)
#[allow(dead_code)]
fn hms(d: Duration) -> String {
    let s = d.as_secs();
    format!("{:0>2}:{:0>2}:{:0>2}", s / 3600, (s / 60) % 60, s % 60)
}

/// Live "running" region (nextest's `--show-progress=running`): one line per in-flight
/// test — `{status} [HH:MM:SS] {binary} {test}` — plus `... and K more running` past
/// `max_rows`. `running` must arrive sorted longest-first.
///
/// Exactly as tall as its content, never padded (the sticky footer keeps the panel
/// bottom-anchored and clears the rows this block gives back)
#[allow(dead_code)] // see note on `hms`: retained for scrollback progress events
pub(crate) fn render_running(running: &[RunningView], max_rows: usize, color: bool) -> Vec<String> {
    if max_rows == 0 {
        return Vec::new();
    }

    // Reserve the last row for an overflow summary when the set doesn't fit
    let overflow = running.len() > max_rows;
    let shown = if overflow { max_rows - 1 } else { running.len() };

    let mut content: Vec<String> = running[..shown]
        .iter()
        .map(|r| {
            let status =
                if r.slow { paint_word(" SLOW", Ink::Skip, color) } else { "     ".to_string() };
            format!(
                "       {} [{:>9}] {}",
                status,
                hms(r.elapsed),
                instance_str(&r.binary_id, &r.test_name, color),
            )
        })
        .collect();

    if overflow {
        let more = running.len() - shown;
        let noun = if more == 1 { "test" } else { "tests" };
        content.push(format!(
            "             ... and {} more {noun} running",
            bold_count(more as u64, color),
        ));
    }

    content
}

/// Top-level live progress line, copied from nextest's `progress_str`
/// (`reporter/displayer/progress.rs`):
///
/// ```text
///    Running [ HH:MM:SS] {finished}/{total}: {running} running, {p} passed, {f} failed, {s} skipped
/// ```
///
/// - `Running` prefix (right-aligned 12) green·bold, red·bold once anything failed
/// - `{wide_bar}` gauge omitted (the QoS panel below already carries one)
#[allow(dead_code)] // see note on `hms`: retained for scrollback progress events
pub(crate) fn progress_line(
    stats: &RunStats,
    running: usize,
    elapsed: Duration,
    color: bool,
) -> String {
    let prefix_ink = if stats.failed > 0 { Ink::Fail } else { Ink::Pass };
    let prefix = paint_word(&format!("{:>12}", "Running"), prefix_ink, color);
    format!(
        "{prefix} [{:>9}] {}/{}: {} running, {}",
        hms(elapsed),
        stats.finished(),
        stats.total,
        bold_count(running as u64, color),
        counts_tail(stats, color),
    )
}

/// Shared `{p} passed[, {f} failed][, {t} sigkilled], {s} skipped` tail, used by
/// [`progress_line`] and [`StyledReporter::summary`]. Which terms appear is
/// [`RunStats::tally`]'s call; this only inks them
fn counts_tail(stats: &RunStats, color: bool) -> String {
    stats
        .tally()
        .into_iter()
        .map(|(n, word)| {
            let ink = match word {
                "passed" => Ink::Pass,
                "failed" | "sigkilled" => Ink::Fail,
                _ => Ink::Skip,
            };
            format!("{} {}", bold_count(n as u64, color), paint_word(word, ink, color))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Bold count number (nextest's `count` style), no-op when colour is off
fn bold_count(n: u64, color: bool) -> String {
    if color { n.to_string().bold().to_string() } else { n.to_string() }
}

/// Apply an [`Ink`] to a word — the single styling primitive, routed through by
/// [`StyledReporter::paint`] and the free renderers
fn paint_word(s: &str, ink: Ink, color: bool) -> String {
    if !color {
        return s.to_string();
    }
    match ink {
        Ink::Pass => s.green().bold().to_string(),
        Ink::Fail => s.red().bold().to_string(),
        Ink::Retry => s.magenta().bold().to_string(),
        Ink::Skip => s.yellow().bold().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finished(bin: &str, test: &str, v: Verdict, attempt: u32, out: &[u8]) -> TestEvent<'static> {
        // Leaked for 'static test events (test-only)
        let bin: &'static str = Box::leak(bin.to_string().into_boxed_str());
        let test: &'static str = Box::leak(test.to_string().into_boxed_str());
        let out: &'static [u8] = Box::leak(out.to_vec().into_boxed_slice());
        TestEvent::TestFinished {
            binary_id: bin,
            test_name: test,
            verdict: v,
            duration: Duration::from_millis(234),
            attempt,
            output: out,
        }
    }

    #[test]
    fn pass_line_matches_nextest_layout() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished("pkg::bin", "mod::ok", Verdict::Pass, 1, b""));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert_eq!(out, "        PASS [   0.234s] pkg::bin mod::ok\n", "{out:?}");
        assert!(!out.contains('\u{1b}'), "unexpected ANSI: {out:?}");
    }

    #[test]
    fn fail_word_and_output_replay_match_nextest() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished(
            "pkg::bin",
            "mod::boom",
            Verdict::Fail(101),
            1,
            b"panicked at boom\nsecond line\n",
        ));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert_eq!(
            out,
            "        FAIL [   0.234s] pkg::bin mod::boom\n  output ───\n    panicked at boom\n    second line\n",
            "{out:?}"
        );
    }

    #[test]
    fn exec_fail_is_xfail() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished("p::b", "t", Verdict::SpawnError, 1, b""));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.starts_with("       XFAIL [   0.234s] p::b t"), "{out:?}");
    }

    #[test]
    fn timeout_uses_long_word() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished("p::b", "t", Verdict::Timeout, 1, b""));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.starts_with("     TIMEOUT [   0.234s] p::b t"), "{out:?}");
    }

    #[test]
    fn final_fail_after_retries_uses_try_short() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        // Failed on attempt 3 with a timeout → short word `TMT`
        r.handle(&finished("p::b", "t", Verdict::Timeout, 3, b""));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.starts_with("   TRY 3 TMT [   0.234s] p::b t"), "{out:?}");
    }

    #[test]
    fn retry_line_is_magenta_try_with_duration() {
        let mut r = StyledReporter::new(true, true, OutputConfig::default());
        r.handle(&TestEvent::TestRetrying {
            binary_id: "p::b",
            test_name: "t",
            next_attempt: 3, // attempt 2 just failed
            delay: Duration::ZERO,
            verdict: Verdict::Fail(1),
            duration: Duration::from_millis(500),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        let bare = strip_ansi(&out);
        assert_eq!(bare, "  TRY 2 FAIL [   0.500s] p::b t\n", "{bare:?}");
        assert!(out.contains("\u{1b}[35m"), "expected magenta: {out:?}");
    }

    #[test]
    fn slow_line_uses_slow_bracket() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::TestSlow {
            binary_id: "p::b",
            test_name: "t",
            elapsed: Duration::from_secs(30),
            will_terminate: false,
            attempt: 1,
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert_eq!(out, "        SLOW [> 30.000s] p::b t\n", "{out:?}");
    }

    #[test]
    fn slow_on_retry_prefixes_try() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::TestSlow {
            binary_id: "p::b",
            test_name: "t",
            elapsed: Duration::from_secs(30),
            will_terminate: false,
            attempt: 2,
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.starts_with("  TRY 2 SLOW [> 30.000s] p::b t"), "{out:?}");
    }

    #[test]
    fn skip_line_names_the_capacity_reason() {
        // A capacity skip must say why (else a too-small cluster reads as a no-op)
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::TestSkipped {
            binary_id: "pkg::bin",
            test_name: "mod::sk",
            reason: crate::engine::events::SkipReason::ExceedsClusterCapacity,
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(
            out.starts_with("        SKIP [         ] pkg::bin mod::sk (exceeds cluster capacity"),
            "{out:?}"
        );
    }

    #[test]
    fn color_emits_ansi() {
        let mut r = StyledReporter::new(true, true, OutputConfig::default());
        r.handle(&finished("p::b", "t", Verdict::Pass, 1, b""));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.contains('\u{1b}'), "expected ANSI escapes: {out:?}");
    }

    #[test]
    fn summary_matches_nextest_and_pluralizes() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 8, failed: 2, terminated: 0, skipped: 1, total: 11 },
            elapsed: Duration::from_secs(2),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert_eq!(
            out,
            "────────────\n     Summary [   2.000s] 10/11 tests run: 8 passed, 2 failed, 1 skipped\n",
            "{out:?}"
        );
    }

    #[test]
    fn summary_singular_and_no_failed_clause() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 1, failed: 0, terminated: 0, skipped: 0, total: 1 },
            elapsed: Duration::from_millis(100),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.contains("] 1 test run: 1 passed, 0 skipped\n"), "{out:?}");
        assert!(!out.contains("failed"), "{out:?}");
    }

    #[test]
    fn summary_partial_total_shows_ratio() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 5, failed: 0, terminated: 0, skipped: 2, total: 10 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        // Numerator counts tests that executed; the two skips ride the tally, not it
        assert!(out.contains("5/10 tests run"), "{out:?}");
        assert!(out.contains("5 passed, 2 skipped"), "{out:?}");
        // Remaining three never started
        assert!(out.contains("warning: 3/10 tests were not run\n"), "{out:?}");
    }

    #[test]
    fn summary_ascii_rule_when_not_unicode() {
        let mut r = StyledReporter::new(false, false, OutputConfig::default());
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 1, failed: 0, terminated: 0, skipped: 0, total: 1 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.starts_with("------------\n"), "{out:?}");
    }

    #[test]
    fn failures_are_recapped_after_summary() {
        // Regression: nextest re-lists every failing test after `Summary` (its
        // `final-status-level = fail` recap), free when ztest shelled out to nextest
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished(
            "e2e::wallet",
            "zebrad::send_to_orchard::case_1_fetch",
            Verdict::Fail(101),
            1,
            b"boom\n",
        ));
        r.handle(&finished(
            "e2e::wallet",
            "zebrad::z_get_treestate::case_1_fetch",
            Verdict::Pass,
            1,
            b"",
        ));
        r.handle(&finished(
            "e2e::wallet",
            "zebrad::send_to_sapling::case_2_state",
            Verdict::Timeout,
            1,
            b"",
        ));
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 1, failed: 2, terminated: 0, skipped: 0, total: 3 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();

        // Tail = rule, Summary, both failures re-listed in failure order (final word
        // preserved, no output replay)
        let expected_tail = "\
────────────
     Summary [   1.000s] 3 tests run: 1 passed, 2 failed, 0 skipped
        FAIL [   0.234s] e2e::wallet zebrad::send_to_orchard::case_1_fetch
     TIMEOUT [   0.234s] e2e::wallet zebrad::send_to_sapling::case_2_state
";
        assert!(out.ends_with(expected_tail), "recap tail wrong:\n{out}");

        // Passing test never recapped; the failure's replay stays inline, pre-Summary
        let summary_pos = out.find("Summary").unwrap();
        assert!(!out[summary_pos..].contains("z_get_treestate"), "pass recapped:\n{out}");
        assert!(out[..summary_pos].contains("boom"), "inline output missing:\n{out}");
    }

    #[test]
    fn clean_run_emits_no_recap() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished("p::b", "ok", Verdict::Pass, 1, b""));
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 1, failed: 0, terminated: 0, skipped: 0, total: 1 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        // Nothing after the Summary line
        assert!(out.trim_end().ends_with("1 test run: 1 passed, 0 skipped"), "{out:?}");
        assert!(!out.contains("FAIL"), "no failure recap on a clean run:\n{out}");
    }

    #[test]
    fn recap_line_is_byte_identical_to_the_inline_line() {
        // Recap re-lists exactly what streamed inline, incl. a `TRY n FAIL` word/colour
        let mut r = StyledReporter::new(true, true, OutputConfig::default());
        r.handle(&finished("p::b", "t", Verdict::Fail(1), 3, b""));
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 0, failed: 1, terminated: 0, skipped: 0, total: 1 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        // `TRY 3 FAIL` appears twice: inline + recap
        let inline = strip_ansi(&out);
        let count = inline.matches("TRY 3 FAIL [   0.234s] p::b t").count();
        assert_eq!(count, 2, "inline + recap:\n{inline}");
    }

    #[test]
    fn cancel_notice_and_summary_line_match_nextest() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        // Terminated test still streams its status line inline...
        r.handle(&finished("pkg::b", "mod::slow", Verdict::Terminated, 1, b""));
        r.handle(&TestEvent::RunCancelling { reason: CancelReason::Interrupt, running: 2 });
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 1, failed: 0, terminated: 1, skipped: 0, total: 5 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();

        // Terminated test renders with the signal word
        assert!(out.contains("     SIGKILL [   0.234s] pkg::b mod::slow"), "{out:?}");
        // Mid-run cancel notice names the reason and running count
        assert!(out.contains("   Canceling due to interrupt: 2 tests still running"), "{out:?}");
        // Summary: short ratio, kill tallied apart from `failed`, cancel line, then the
        // three that never started
        assert!(out.contains("2/5 tests run"), "{out:?}");
        assert!(out.contains("1 passed, 1 sigkilled, 0 skipped"), "{out:?}");
        assert!(!out.contains("failed"), "a kill is not a failure:\n{out}");
        assert!(
            out.trim_end().ends_with("warning: 3/5 tests were not run due to interrupt"),
            "{out:?}"
        );
        assert!(out.contains("    Canceled due to interrupt\n"), "{out:?}");
    }

    #[test]
    fn sigkilled_output_is_never_replayed_or_recapped() {
        // UX regression: Ctrl-C with N in flight replayed N component-log streams over
        // the summary the operator pressed Ctrl-C to reach. nextest does replay them;
        // ztest does not — but the one-line verdict survives and the kill stays out of
        // the failure recap
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished(
            "pkg::b",
            "mod::killed",
            Verdict::Terminated,
            1,
            b"[zebrad] INFO spawning syncer task\n[zainod] INFO syncing block\n",
        ));
        r.handle(&finished("pkg::b", "mod::broke", Verdict::Fail(101), 1, b"boom\n"));
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 0, failed: 1, terminated: 1, skipped: 0, total: 2 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();

        assert!(out.contains("SIGKILL [   0.234s] pkg::b mod::killed"), "{out}");
        assert!(!out.contains("zebrad"), "kill output replayed:\n{out}");
        assert!(!out.contains("output ───\n    [zainod"), "{out}");
        // A real failure in the same run keeps both its output and its recap
        assert!(out.contains("boom"), "{out}");
        let summary = out.find("Summary").unwrap();
        assert!(out[summary..].contains("mod::broke"), "{out}");
        assert!(!out[summary..].contains("mod::killed"), "a kill is not a to-do item:\n{out}");
    }

    #[test]
    fn final_output_policy_also_exempts_a_kill() {
        // Deferred (`--failure-output=final`) path must call it the same way as the
        // immediate one, else the logs reappear in the end block
        let cfg = OutputConfig {
            failure: crate::engine::output::TestOutputDisplay::Final,
            ..OutputConfig::default()
        };
        let mut r = StyledReporter::new(false, true, cfg);
        r.handle(&finished("p::b", "t", Verdict::Terminated, 1, b"component noise\n"));
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 0, failed: 0, terminated: 1, skipped: 0, total: 1 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(!out.contains("component noise"), "{out}");
    }

    #[test]
    fn a_complete_run_emits_no_not_run_warning() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 2, failed: 0, terminated: 0, skipped: 1, total: 3 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        // Skips accounted for; nothing unaccounted
        assert!(!out.contains("warning"), "{out}");
        assert!(out.contains("2/3 tests run: 2 passed, 1 skipped"), "{out}");
    }

    #[test]
    fn not_run_warning_singularizes() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::RunCancelling { reason: CancelReason::Interrupt, running: 1 });
        r.handle(&TestEvent::RunFinished {
            stats: RunStats { passed: 0, failed: 0, terminated: 0, skipped: 0, total: 1 },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.contains("warning: 1/1 test was not run due to interrupt"), "{out}");
    }

    #[test]
    fn starting_line_pluralizes() {
        let mut one = StyledReporter::new(false, true, OutputConfig::default());
        one.handle(&TestEvent::RunStarted { total: 1, run_id: "r" });
        let s1 = String::from_utf8(one.take_scrollback()).unwrap();
        assert_eq!(s1, "    Starting 1 test\n", "{s1:?}");

        let mut many = StyledReporter::new(false, true, OutputConfig::default());
        many.handle(&TestEvent::RunStarted { total: 42, run_id: "r" });
        let s2 = String::from_utf8(many.take_scrollback()).unwrap();
        assert_eq!(s2, "    Starting 42 tests\n", "{s2:?}");
    }

    fn running(bin: &str, test: &str, secs: u64, slow: bool) -> RunningView {
        RunningView {
            binary_id: bin.into(),
            test_name: test.into(),
            elapsed: Duration::from_secs(secs),
            slow,
        }
    }

    #[test]
    fn running_block_is_as_tall_as_the_live_set() {
        // One running test under a 4-row ceiling → one line, no blank padding
        let r = vec![running("pkg::b", "mod::a", 5, false)];
        let lines = render_running(&r, 4, false);
        assert_eq!(lines.len(), 1, "as tall as content, not padded to `max_rows`");
        assert_eq!(lines[0], "             [ 00:00:05] pkg::b mod::a", "{:?}", lines[0]);
    }

    #[test]
    fn running_block_overflows_with_summary() {
        let r: Vec<_> = (0..5).map(|i| running("pkg::b", &format!("t{i}"), i, false)).collect();
        let lines = render_running(&r, 3, false);
        assert_eq!(lines.len(), 3);
        assert!(lines[2].contains("... and 3 more tests running"), "{:?}", lines[2]);
    }

    #[test]
    fn progress_line_matches_nextest_layout() {
        let stats = RunStats { passed: 30, failed: 6, terminated: 0, skipped: 1, total: 122 };
        // 1h 2m 3s = 3723s
        let line = progress_line(&stats, 9, Duration::from_secs(3723), false);
        assert_eq!(
            line, "     Running [ 01:02:03] 37/122: 9 running, 30 passed, 6 failed, 1 skipped",
            "{line}"
        );
    }

    #[test]
    fn progress_line_omits_failed_when_zero() {
        let stats = RunStats { passed: 5, failed: 0, terminated: 0, skipped: 2, total: 10 };
        let line = progress_line(&stats, 3, Duration::from_secs(1), false);
        assert!(line.contains("5 passed, 2 skipped"), "{line}");
        assert!(!line.contains("failed"), "{line}");
    }

    #[test]
    fn running_block_empty_when_nothing_runs() {
        // Nothing running → no live rows at all (panel alone), not blank padding
        assert!(render_running(&[], 3, false).is_empty());
    }

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

    /// Strip CSI SGR sequences so colour tests can assert on the text
    fn strip_ansi(s: &str) -> String {
        let mut out = String::new();
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\u{1b}' {
                for d in chars.by_ref() {
                    if d == 'm' {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }
}
