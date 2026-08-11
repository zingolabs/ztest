//! A ztest-owned run reporter that reproduces `cargo nextest run`'s default
//! human output.
//!
//! Formats the engine's lifecycle events into the exact scrolling status lines
//! nextest emits — `PASS`/`FAIL`/`XFAIL`/`SLOW`/`TRY n …` verdicts with
//! right-aligned durations, failure-output replay under `output ───` headers,
//! the `Summary` block, and the end-of-run failure recap (each failing test
//! re-listed after the summary, nextest's `final-status-level = fail`) — matching
//! nextest's indentation, capitalisation, and colours (`owo-colors`). We own
//! every line, so there is no `nextest-runner` dependency; each status line is
//! indistinguishable from nextest's (`reporter/displayer/imp.rs`).
//!
//! Three deliberate divergences, each documented at its site:
//!
//! 1. A Ctrl-C-terminated test gets its own `sigkilled` tally term and is kept
//!    out of the failure recap, where nextest folds signal deaths into `failed`
//!    ([`RunStats::terminated`]).
//! 2. Its captured output is never replayed
//!    ([`handle`](StyledReporter::handle)).
//! 3. The captured-output block strips libtest's framing
//!    ([`strip_libtest_frame`]), described next.
//!
//! nextest replays the
//! child's bytes verbatim, so its `output ───` block inherits libtest's per-run
//! framing (`running 1 test`, the `test <name> ... ` prefix, and the trailing
//! `failures:` / `test result:` summary). For a single `--exact` process that
//! framing is pure noise, redundant with the `FAIL […] name` line and `Summary`
//! block we already render. So [`strip_libtest_frame`] removes it and we replay
//! only the test's own stdout/stderr and panic/`Error:` output. See it for the
//! grammar and its fidelity fallbacks.
//!
//! Ceiling: the engine's [`Verdict`] models only pass/fail/timeout/spawn-error,
//! so nextest's leak/flaky/slow-pass/abort status words can't be produced —
//! those need a richer event model and are intentionally out of scope.
//!
//! Emits only scrollback — its verdict lines flow into the terminal's native
//! scrollback like the build/image subprocess output. The pinned QoS panel is
//! rendered separately by the run loop. The [`progress_line`] / [`render_running`]
//! helpers are retained (see their `#[allow(dead_code)]` notes) for a future
//! nextest-style scrollback progress feed, but nothing is pinned live now.

use std::io::Write as _;
use std::time::Duration;

use owo_colors::OwoColorize as _;

use crate::engine::events::{CancelReason, RunReporter, RunStats, RunningView, TestEvent, Verdict};
use crate::engine::output::OutputConfig;

/// Status-line colour intent, mirroring nextest's `Styles`
/// (`reporter/helpers.rs`): pass = green·bold, fail = red·bold,
/// retry = magenta·bold, skip/slow = yellow·bold.
#[derive(Clone, Copy)]
enum Ink {
    Pass,
    Fail,
    Retry,
    Skip,
}

/// The nextest-style reporter. `color`/`unicode` are resolved by the caller
/// from terminal support (`supports_color` / `supports_unicode`, exactly as
/// nextest does); non-TTY runs pass `color = false`.
#[derive(Debug)]
pub struct StyledReporter {
    color: bool,
    unicode: bool,
    buf: Vec<u8>,
    stats: RunStats,
    /// Set once cancellation is requested (Ctrl-C). Drives the mid-run
    /// `Canceling …` notice and the `cancelled due to …` line under the summary,
    /// mirroring nextest's `RunBeginCancel` / cancelled `FinalRunStats`.
    cancelled: Option<CancelReason>,
    /// Each failing test's already-styled status line (no trailing newline),
    /// captured as it streams so the final [`summary`](Self::summary) can re-list
    /// them after the `Summary` line — nextest's end-of-run failure recap
    /// (`final-status-level = fail`). Without this, failures from a long run
    /// scroll away at their inline point with no consolidated list.
    failures: Vec<String>,
    /// When and whether captured output is shown, per verdict — nextest's
    /// `success-output`/`failure-output`.
    output: OutputConfig,
    /// Captured output deferred to the end-of-run block (`TestOutputDisplay`
    /// `Final`/`ImmediateFinal`): the styled status line, the raw output bytes,
    /// and whether the test passed (so the replay header is coloured by verdict).
    final_outputs: Vec<(String, Vec<u8>, bool)>,
}

impl StyledReporter {
    /// A reporter; `color` enables ANSI styling, `unicode` selects `─`/`───`
    /// over the ASCII `-`/`---` fallback (nextest's `ThemeCharacters`).
    /// `output` sets the capture-display policy (nextest defaults:
    /// failure=immediate, success=never).
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

    /// Right-align a status word in a fixed 12-col field and colour it. nextest
    /// styles the value *through* the formatter (`"{:>12}"`), so the padding
    /// lands inside the ANSI wrapper — padding first, then painting, is
    /// byte-identical.
    fn status(&self, word: &str, ink: Ink) -> String {
        let padded = format!("{word:>12}");
        self.paint(&padded, ink)
    }

    /// Apply an [`Ink`] to a string (no-op when colour is off).
    fn paint(&self, s: &str, ink: Ink) -> String {
        paint_word(s, ink, self.color)
    }

    /// A horizontal rule of `n` chars: `─` (U+2500) with unicode, else the
    /// ASCII `-` — nextest's `ThemeCharacters::hbar` (`helpers.rs`).
    fn hbar(&self, n: usize) -> String {
        let c = if self.unicode { '─' } else { '-' };
        std::iter::repeat_n(c, n).collect()
    }

    /// `{binary_id} {module::path}::{leaf}` styled like nextest's
    /// `DisplayTestInstance`: binary-id magenta·bold, module path cyan, leaf
    /// blue·bold.
    fn styled_instance(&self, bin: &str, test: &str) -> String {
        instance_str(bin, test, self.color)
    }

    /// Format one status line (no trailing newline): `{status:>12} {bracket}{instance}`.
    /// `bracket` already carries nextest's trailing space
    /// (`DisplayBracketedDuration` &c.), so the only literal space here is the one
    /// after the status field. Shared by the streaming [`line`](Self::line) writer
    /// and the failure recap, so a recapped line is byte-identical to its inline
    /// form.
    fn format_line(&self, word: &str, ink: Ink, bracket: &str, bin: &str, test: &str) -> String {
        format!(
            "{} {bracket}{}",
            self.status(word, ink),
            self.styled_instance(bin, test),
        )
    }

    /// Write one status line to the scrollback buffer.
    fn line(&mut self, word: &str, ink: Ink, bracket: &str, bin: &str, test: &str) {
        let line = self.format_line(word, ink, bracket, bin, test);
        let _ = writeln!(self.buf, "{line}");
    }

    /// Replay a test's captured output under nextest's default (indented)
    /// combined-stream layout (`unit_output.rs`): a `  output ───` header
    /// coloured by `ink` (fail-red for a failing test, pass-green for a passing
    /// one shown via `success-output`), then `output` indented four spaces, with
    /// no closing rule. The engine captures a merged stdout+stderr stream, so
    /// nextest's combined `output` header is the faithful choice (vs the split
    /// `stdout`/`stderr` headers). `output` is expected to already have had its
    /// libtest framing removed by [`strip_libtest_frame`].
    fn replay_output(&mut self, output: &[u8], ink: Ink) {
        if output.is_empty() {
            return;
        }
        // Header: " " + "output" + hbar(3), each piece styled — as nextest's
        // `format!("{} {} {}", start, "output", end)` with start=" ", end="───".
        let header = format!(
            "{} {} {}",
            self.paint(" ", ink),
            self.paint("output", ink),
            self.paint(&self.hbar(3), ink),
        );
        let _ = writeln!(self.buf, "{header}");
        // Body: indent every non-empty line by four spaces (blank lines stay
        // bare, matching the `indenter` crate nextest uses), and guarantee a
        // trailing newline so the next scrollback line isn't glued on.
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

/// Strip a single `--exact <test> --nocapture` libtest run's framing from its
/// captured stdout+stderr, leaving only the test's own output (its logs, and any
/// panic message or `Result::Err` the harness printed).
///
/// libtest wraps every run in boilerplate that, for a one-test process, is pure
/// noise duplicated by our own reporter frame:
///
/// ```text
/// running 1 test
/// test <name> ... <first log glued here under --nocapture>
/// <more logs>
/// FAILED                                    // or `ok`
///
/// failures:
///
/// failures:
///     <name>
///
/// test result: FAILED. 0 passed; 1 failed; …
/// ```
///
/// The shape above is what a TTY produces, where stdout and stderr interleave in
/// write order and the body glues after the marker. The remote pod path does
/// **not** hold to it: under `--nocapture` the test body (panic / `Error:`) is
/// written to stderr while libtest's own `running`/marker/verdict lines go to
/// stdout, and the container runtime merges the two pipes by read-arrival — so
/// the body routinely lands *before* the `test <name> ... ` marker, not after it.
/// Slicing at the marker would then discard the entire failure body (the whole
/// point of the block). So we do not slice: we drop libtest's framing lines
/// wherever they merged and keep every other line in place.
///
/// The framing is: the `running N tests` header (dropped anywhere it appears);
/// the `test <name> ... ` line (dropped when only a bare verdict trails it — the
/// pod case — else un-glued to keep the body glued after it, the TTY case); and
/// the trailing summary, which libtest always flushes last so it sits at the end
/// of the merged log — anchored on the final `test result: ` line and popped
/// bottom-up over libtest's grammar, consuming exactly one verdict so a user line
/// that merely reads `FAILED` survives. The `test result:` anchor is a fidelity
/// fallback: a stream missing it is left un-cut on the tail, so an unrecognised
/// format degrades to nextest's verbatim replay rather than silently eating
/// output.
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
            // TTY: the first body line is glued after the marker — keep it. Pod:
            // only a bare verdict trails the marker — drop the whole line.
            if !rest.is_empty() && !is_verdict(rest) {
                kept.push(rest);
            }
            continue;
        }
        kept.push(line);
    }
    let mut lines = kept;

    // Drop the blank lines the framing leaves at either edge, then re-join.
    while lines.first().is_some_and(|l| l.is_empty()) {
        lines.remove(0);
    }
    while lines.last().is_some_and(|l| l.is_empty()) {
        lines.pop();
    }
    lines.join(&b'\n')
}

/// Whether `line` is libtest's `running N tests` run header.
fn is_run_header(line: &[u8]) -> bool {
    let Some(rest) = line.strip_prefix(b"running ") else {
        return false;
    };
    let count = rest
        .strip_suffix(b" tests")
        .or_else(|| rest.strip_suffix(b" test"));
    count.is_some_and(|c| !c.is_empty() && c.iter().all(u8::is_ascii_digit))
}

/// Pop libtest's end-of-run summary grammar off the end of `lines`: trailing
/// blanks, `failures:` headers, indented failure names / `---- … ----` capture
/// headers, and finally exactly one verdict token. Stops at the first line that
/// is real test output. The caller has already dropped the `test result:` line.
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

/// Whether a line is a libtest per-test verdict token (`ok`, `ignored`, or
/// `FAILED` optionally carrying a `should_panic` note like `FAILED (…)`).
fn is_verdict(line: &[u8]) -> bool {
    line == b"ok" || line == b"ignored" || line == b"FAILED" || line.starts_with(b"FAILED (")
}

impl RunReporter for StyledReporter {
    fn handle(&mut self, ev: &TestEvent<'_>) {
        match ev {
            TestEvent::RunStarted { total, .. } => {
                self.stats.total = *total;
                // nextest's full RunStarted block also carries a run-ID/profile
                // line and "across N binaries" — neither is in the ztest event
                // model, so this is the faithful subset: right-aligned green·bold
                // "Starting" + bold count.
                let word = if *total == 1 { "test" } else { "tests" };
                let _ = writeln!(
                    self.buf,
                    "{} {} {word}",
                    self.status("Starting", Ink::Pass),
                    bold_count(*total as u64, self.color),
                );
            }
            // Test starts are reflected in the pinned QoS panel, not scrollback.
            TestEvent::TestStarted { .. } => {}
            TestEvent::TestSlow {
                binary_id,
                test_name,
                elapsed,
                will_terminate,
                attempt,
            } => {
                // nextest uses the *slow* duration bracket `[>  d.ddds] ` here,
                // not the ordinary one (`imp.rs` → `DisplaySlowDuration`).
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
                // The attempt that just failed (nextest's `retry_data.attempt`),
                // rendered magenta as `TRY {n} {short}` with its real duration —
                // nextest's `TestAttemptFailedWillRetry` line.
                let failed_attempt = next_attempt.saturating_sub(1).max(1);
                let word = format!("TRY {failed_attempt} {}", short_status(verdict));
                self.line(
                    &word,
                    Ink::Retry,
                    &bracket_dur(*duration),
                    binary_id,
                    test_name,
                );
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
                    // A failing terminal verdict. Attempt 1 uses the long status
                    // word (`FAIL`/`TIMEOUT`/`XFAIL`); a later attempt failing
                    // after retries renders `TRY {n} {short}` — both red
                    // (`imp.rs` → `ExecutionDescription::Failure`).
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
                    // Only real failures join the end-of-run recap: a recap is a
                    // to-do list, and a test the operator killed asks nothing of
                    // them. The `sigkilled` tally term carries the count.
                    self.failures.push(line.clone());
                    self.stats.failed += 1;
                }
                // Captured-output display, per verdict (`success-output` /
                // `failure-output`): replay inline now if `immediate`, and/or
                // defer to the end-of-run block if `final`. The child's libtest
                // framing is stripped once here so both paths show the same
                // signal-only bytes.
                //
                // A terminated test is exempt, and this is a deliberate
                // divergence: nextest routes every `Fail{..}` — signal aborts
                // included — through `failure_output`
                // (`displayer/unit_output.rs`), so a Ctrl-C with N tests in
                // flight replays N full component-log streams over the summary
                // the operator pressed Ctrl-C to reach. The bytes are the
                // mid-provision log of a test that was interrupted, not a
                // diagnosis of anything, and they remain in the run record for
                // `ztest replay`. The `SIGKILL` status line still prints, so
                // which tests were killed is never hidden.
                if !terminated {
                    let display = self.output.display_for(passed);
                    // On the pod path `output` is already the laptop-assembled
                    // unified timeline (frame-free), so the strip is a no-op; on
                    // the local path it removes the child's libtest framing.
                    let shown = strip_libtest_frame(output, test_name);
                    if display.is_immediate() {
                        self.replay_output(&shown, ink);
                    }
                    if display.is_final() && !shown.is_empty() {
                        self.final_outputs.push((line, shown, passed));
                    }
                }
            }
            TestEvent::TestSkipped {
                binary_id,
                test_name,
                reason,
            } => {
                // nextest's skip line: `SKIP` + the empty-duration placeholder
                // `[         ] ` + instance (`imp.rs` → `write_skip_line`). Every
                // ztest skip reason is a ztest-specific concept with no nextest
                // analogue, so we always append it — a run that skips everything
                // (a too-small cluster, an unavailable dependency) must say why,
                // not leave the author guessing.
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
                // nextest's `RunBeginCancel` notice: a fail-styled `Canceling`
                // line naming the reason and how many tests are being terminated.
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
    /// The end-of-run captured-output block, for tests whose display policy is
    /// `Final`/`ImmediateFinal` (nextest replays these after the run, grouped,
    /// rather than inline). Each entry re-prints the test's status line then its
    /// output, header coloured by verdict. No-op when nothing was deferred.
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

    /// The closing summary block — a rule then
    /// `Summary [   d.ddds] N tests run: X passed[, Y failed], Z skipped`,
    /// matching nextest's `RunFinished` line (`imp.rs`): the rule + a `Summary`
    /// label coloured by final status (green pass / red fail / yellow no-run),
    /// the elapsed bracket, `finished[/total]`, then the shared counts tail —
    /// followed by the failure recap (each failing test's status line re-listed),
    /// present only when something failed.
    fn summary(&mut self, stats: &RunStats, elapsed: Duration) {
        let _ = writeln!(self.buf, "{}", self.hbar(12));
        // nextest colours "Summary" by outcome: fail if anything failed or the
        // run was cancelled, skip if nothing ran, else pass.
        let ink = if stats.failed > 0 || self.cancelled.is_some() {
            Ink::Fail
        } else if stats.finished() == 0 {
            Ink::Skip
        } else {
            Ink::Pass
        };
        let label = self.paint(&format!("{:>12}", "Summary"), ink);
        // `29/132` when the run stopped short, else just the count. The
        // numerator is tests that actually executed, so skips sit in the tally
        // rather than inflating the ratio — nextest's `finished_count`, which
        // likewise excludes skips (it filters them out of `initial_run_count`).
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
        // `tests_plural_if(initial != 1 || finished != 1)`).
        let tests = if stats.total != 1 || executed != 1 {
            "tests"
        } else {
            "test"
        };
        let _ = writeln!(
            self.buf,
            "{label} {}{ran} {tests} run: {}",
            bracket_dur(elapsed),
            counts_tail(stats, self.color),
        );
        // nextest's end-of-run failure recap (`final-status-level = fail`):
        // re-list each failing test's status line (no output replay) directly
        // under the Summary, so a long run's failures are visible together
        // instead of scrolled away at their inline point. Emitted only when
        // something failed, exactly like nextest.
        for line in std::mem::take(&mut self.failures) {
            let _ = writeln!(self.buf, "{line}");
        }
        // nextest's cancelled `FinalRunStats`: a fail-styled `cancelled due to
        // {reason}` line closing the summary.
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

    /// nextest's final not-run warning (`displayer/formatters.rs`
    /// `write_final_warnings_for_failure`): `warning: 103/132 tests were not run
    /// due to interrupt`.
    ///
    /// This is the accounting for tests still queued when a run closed short.
    /// They are deliberately *not* folded into `skipped`, which across ztest
    /// means "reached a terminal state without running" — unschedulable
    /// footprint, unavailable dependency — and gates a setup-error exit. A
    /// Ctrl-C is not a setup error, so it gets its own line rather than
    /// overloading that word.
    ///
    /// The reason clause is present only for a cancelled run; a run that closed
    /// short for any other reason takes nextest's own unattributed branch.
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

/// nextest's long `status_str` for a failing verdict — used on attempt-1 and
/// final status lines (`imp.rs`). `Pass` never reaches this path.
fn long_status(v: &Verdict) -> &'static str {
    match v {
        Verdict::Fail(_) => "FAIL",
        Verdict::Timeout => "TIMEOUT",
        Verdict::SpawnError => "XFAIL",
        Verdict::Pass => "PASS",
        // A test killed by the run's cancellation: we SIGKILL its process group,
        // so the honest status word is the signal, matching nextest's per-test
        // signal display (`SIG…`).
        Verdict::Terminated => "SIGKILL",
    }
}

/// nextest's `short_status_str` for retry lines and post-retry final lines
/// (max 6 chars): `FAIL` / `TMT` / `XFAIL` (`imp.rs`).
fn short_status(v: &Verdict) -> &'static str {
    match v {
        Verdict::Fail(_) => "FAIL",
        Verdict::Timeout => "TMT",
        Verdict::SpawnError => "XFAIL",
        Verdict::Pass => "PASS",
        Verdict::Terminated => "SIGKILL",
    }
}

/// `[   1.000s] ` — nextest's `DisplayBracketedDuration`: right-aligned 8-wide,
/// 3-dp seconds, with the trailing space it carries.
fn bracket_dur(d: Duration) -> String {
    format!("[{:>8.3}s] ", d.as_secs_f64())
}

/// `[>  1.000s] ` — nextest's `DisplaySlowDuration`: a literal `>` then a
/// 7-wide, 3-dp seconds field, plus the trailing space.
fn bracket_slow(d: Duration) -> String {
    format!("[>{:>7.3}s] ", d.as_secs_f64())
}

/// nextest's empty-duration placeholder for SKIP lines: an 11-char bracket
/// (`[` + 9 spaces + `]`) — same width as a real duration — plus its trailing
/// space, so the instance column stays aligned with the verdict lines.
const BRACKET_SKIP: &str = "[         ] ";

/// `{binary_id} {module::path}::{leaf}` — binary-id magenta·bold, module path
/// cyan, leaf blue·bold (nextest's `DisplayTestInstance` / `list::Styles`).
/// Shared by the scrollback verdict lines and the live running region.
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

/// `HH:MM:SS` elapsed, matching nextest's `DisplayBracketedHhMmSs`
/// (`formatters.rs`) used by the top progress line.
///
/// Note: nextest's *running-row* clock (`progress.rs`) has a minutes-overflow
/// bug (`as_secs()/60`, no `% 60`) that mis-renders past one hour; this
/// (correct) `% 60` version is shared by both our lines, so it matches nextest
/// under an hour and stays correct — rather than bug-compatible — beyond it.
// Retained for a future "emit nextest-style progress/START events to
// scrollback" feature; unused since the pinned per-test live window was dropped
// in favour of the constant QoS panel (see `engine::run_tty`).
#[allow(dead_code)]
fn hms(d: Duration) -> String {
    let s = d.as_secs();
    format!("{:0>2}:{:0>2}:{:0>2}", s / 3600, (s / 60) % 60, s % 60)
}

/// Render the live "running" region (nextest's `--show-progress=running` block):
/// one line per in-flight test — `{status} [HH:MM:SS] {binary} {test}` —
/// longest-running first, sitting directly beneath the progress line, with a
/// trailing `... and K more running` when the set exceeds `max_rows` (the rows the
/// live region can show above the panel).
///
/// The block is exactly as tall as its content — no blank padding. The sticky
/// footer keeps the pinned panel bottom-anchored regardless, and clears the rows
/// the block gives back as tests finish, so the region shrinks and grows with the
/// live set instead of reserving a fixed height.
///
/// `running` is assumed already sorted longest-first. Returns ANSI strings (one
/// per line); empty when nothing is running.
#[allow(dead_code)] // see note on `hms`: retained for scrollback progress events
pub(crate) fn render_running(running: &[RunningView], max_rows: usize, color: bool) -> Vec<String> {
    if max_rows == 0 {
        return Vec::new();
    }

    // Reserve the last row for an overflow summary when the set doesn't fit.
    let overflow = running.len() > max_rows;
    let shown = if overflow {
        max_rows - 1
    } else {
        running.len()
    };

    let mut content: Vec<String> = running[..shown]
        .iter()
        .map(|r| {
            let status = if r.slow {
                paint_word(" SLOW", Ink::Skip, color)
            } else {
                "     ".to_string()
            };
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

/// The top-level live progress line, copied from nextest's `progress_str`
/// (`reporter/displayer/progress.rs`):
///
/// ```text
///    Running [ HH:MM:SS] {finished}/{total}: {running} running, {p} passed, {f} failed, {s} skipped
/// ```
///
/// Prefix `Running` (right-aligned 12) is green·bold normally, red·bold once
/// anything has failed; the elapsed clock is `elapsed_precise`-style; counts are
/// bold with their words coloured (passed green, failed red, skipped yellow).
/// nextest's `{wide_bar}` gauge is intentionally omitted — the QoS panel below
/// already carries a capacity gauge.
#[allow(dead_code)] // see note on `hms`: retained for scrollback progress events
pub(crate) fn progress_line(
    stats: &RunStats,
    running: usize,
    elapsed: Duration,
    color: bool,
) -> String {
    let prefix_ink = if stats.failed > 0 {
        Ink::Fail
    } else {
        Ink::Pass
    };
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

/// The shared `{p} passed[, {f} failed][, {t} sigkilled], {s} skipped` tail used
/// by both the live [`progress_line`] and the final
/// [`StyledReporter::summary`]. Which terms appear is
/// [`RunStats::tally`]'s call (nextest's `write_summary_str` convention); this
/// only inks them.
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
            format!(
                "{} {}",
                bold_count(n as u64, color),
                paint_word(word, ink, color)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A bold count number (nextest's `count` style), no-op when colour is off.
fn bold_count(n: u64, color: bool) -> String {
    if color {
        n.to_string().bold().to_string()
    } else {
        n.to_string()
    }
}

/// Apply an [`Ink`] to a word. The single styling primitive — both
/// [`StyledReporter::paint`] and the free renderers route through it.
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
        // Leak the strings for 'static test events (test-only).
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
        assert_eq!(
            out, "        PASS [   0.234s] pkg::bin mod::ok\n",
            "{out:?}"
        );
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
        assert!(
            out.starts_with("       XFAIL [   0.234s] p::b t"),
            "{out:?}"
        );
    }

    #[test]
    fn timeout_uses_long_word() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished("p::b", "t", Verdict::Timeout, 1, b""));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(
            out.starts_with("     TIMEOUT [   0.234s] p::b t"),
            "{out:?}"
        );
    }

    #[test]
    fn final_fail_after_retries_uses_try_short() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        // Failed on attempt 3 (after retries) with a timeout → short word `TMT`.
        r.handle(&finished("p::b", "t", Verdict::Timeout, 3, b""));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(
            out.starts_with("   TRY 3 TMT [   0.234s] p::b t"),
            "{out:?}"
        );
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
        assert!(
            out.starts_with("  TRY 2 SLOW [> 30.000s] p::b t"),
            "{out:?}"
        );
    }

    #[test]
    fn skip_line_names_the_capacity_reason() {
        // A capacity skip must say why: a run that skips everything on a
        // too-small cluster is otherwise indistinguishable from a no-op.
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
            stats: RunStats {
                passed: 8,
                failed: 2,
                terminated: 0,
                skipped: 1,
                total: 11,
            },
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
            stats: RunStats {
                passed: 1,
                failed: 0,
                terminated: 0,
                skipped: 0,
                total: 1,
            },
            elapsed: Duration::from_millis(100),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(
            out.contains("] 1 test run: 1 passed, 0 skipped\n"),
            "{out:?}"
        );
        assert!(!out.contains("failed"), "{out:?}");
    }

    #[test]
    fn summary_partial_total_shows_ratio() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 5,
                failed: 0,
                terminated: 0,
                skipped: 2,
                total: 10,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        // The ratio numerator counts tests that executed; the two skips are
        // reported by the tally, not folded into it.
        assert!(out.contains("5/10 tests run"), "{out:?}");
        assert!(out.contains("5 passed, 2 skipped"), "{out:?}");
        // The remaining three never started at all.
        assert!(
            out.contains("warning: 3/10 tests were not run\n"),
            "{out:?}"
        );
    }

    #[test]
    fn summary_ascii_rule_when_not_unicode() {
        let mut r = StyledReporter::new(false, false, OutputConfig::default());
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 1,
                failed: 0,
                terminated: 0,
                skipped: 0,
                total: 1,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.starts_with("------------\n"), "{out:?}");
    }

    #[test]
    fn failures_are_recapped_after_summary() {
        // The regression this guards: nextest re-lists every failing test after
        // the `Summary` line (its `final-status-level = fail` recap). A day-one
        // ztest got this free from the real `cargo nextest run` subprocess; the
        // native engine must reproduce it.
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
            stats: RunStats {
                passed: 1,
                failed: 2,
                terminated: 0,
                skipped: 0,
                total: 3,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();

        // The exact end-of-run tail: rule, Summary, then both failures re-listed
        // (final word preserved, no output replay), in the order they failed.
        let expected_tail = "\
────────────
     Summary [   1.000s] 3 tests run: 1 passed, 2 failed, 0 skipped
        FAIL [   0.234s] e2e::wallet zebrad::send_to_orchard::case_1_fetch
     TIMEOUT [   0.234s] e2e::wallet zebrad::send_to_sapling::case_2_state
";
        assert!(out.ends_with(expected_tail), "recap tail wrong:\n{out}");

        // The passing test is never recapped, and the failure's output replay
        // stays inline (before the Summary), not in the recap.
        let summary_pos = out.find("Summary").unwrap();
        assert!(
            !out[summary_pos..].contains("z_get_treestate"),
            "pass recapped:\n{out}"
        );
        assert!(
            out[..summary_pos].contains("boom"),
            "inline output missing:\n{out}"
        );
    }

    #[test]
    fn clean_run_emits_no_recap() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished("p::b", "ok", Verdict::Pass, 1, b""));
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 1,
                failed: 0,
                terminated: 0,
                skipped: 0,
                total: 1,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        // Nothing after the Summary line.
        assert!(
            out.trim_end().ends_with("1 test run: 1 passed, 0 skipped"),
            "{out:?}"
        );
        assert!(
            !out.contains("FAIL"),
            "no failure recap on a clean run:\n{out}"
        );
    }

    #[test]
    fn recap_line_is_byte_identical_to_the_inline_line() {
        // Whatever we streamed inline for a failure is exactly what the recap
        // re-lists — including a post-retry `TRY n FAIL` word and colour.
        let mut r = StyledReporter::new(true, true, OutputConfig::default());
        r.handle(&finished("p::b", "t", Verdict::Fail(1), 3, b""));
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 0,
                failed: 1,
                terminated: 0,
                skipped: 0,
                total: 1,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        // The `TRY 3 FAIL` line appears twice: once inline, once in the recap.
        let inline = strip_ansi(&out);
        let count = inline.matches("TRY 3 FAIL [   0.234s] p::b t").count();
        assert_eq!(count, 2, "inline + recap:\n{inline}");
    }

    #[test]
    fn cancel_notice_and_summary_line_match_nextest() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        // A test terminated by the cancellation streams its status line inline...
        r.handle(&finished(
            "pkg::b",
            "mod::slow",
            Verdict::Terminated,
            1,
            b"",
        ));
        r.handle(&TestEvent::RunCancelling {
            reason: CancelReason::Interrupt,
            running: 2,
        });
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 1,
                failed: 0,
                terminated: 1,
                skipped: 0,
                total: 5,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();

        // The terminated test renders with the signal word.
        assert!(
            out.contains("     SIGKILL [   0.234s] pkg::b mod::slow"),
            "{out:?}"
        );
        // The mid-run cancel notice names the reason and running count.
        assert!(
            out.contains("   Canceling due to interrupt: 2 tests still running"),
            "{out:?}"
        );
        // The summary shows the short ratio, tallies the kill apart from
        // `failed`, closes with the cancel line, then accounts for the three
        // tests that never started.
        assert!(out.contains("2/5 tests run"), "{out:?}");
        assert!(out.contains("1 passed, 1 sigkilled, 0 skipped"), "{out:?}");
        assert!(!out.contains("failed"), "a kill is not a failure:\n{out}");
        assert!(
            out.trim_end()
                .ends_with("warning: 3/5 tests were not run due to interrupt"),
            "{out:?}"
        );
        assert!(out.contains("    Canceled due to interrupt\n"), "{out:?}");
    }

    #[test]
    fn sigkilled_output_is_never_replayed_or_recapped() {
        // The UX regression this guards: Ctrl-C with N tests in flight replayed
        // N full component-log streams over the summary the operator pressed
        // Ctrl-C to reach. nextest does replay them (every `Fail{..}`, signal
        // aborts included, routes to `failure-output`); ztest deliberately does
        // not. The one-line verdict must survive — which tests were killed is
        // never hidden — and the kill must stay out of the failure recap.
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&finished(
            "pkg::b",
            "mod::killed",
            Verdict::Terminated,
            1,
            b"[zebrad] INFO spawning syncer task\n[zainod] INFO syncing block\n",
        ));
        r.handle(&finished(
            "pkg::b",
            "mod::broke",
            Verdict::Fail(101),
            1,
            b"boom\n",
        ));
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 0,
                failed: 1,
                terminated: 1,
                skipped: 0,
                total: 2,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();

        assert!(
            out.contains("SIGKILL [   0.234s] pkg::b mod::killed"),
            "{out}"
        );
        assert!(!out.contains("zebrad"), "kill output replayed:\n{out}");
        assert!(!out.contains("output ───\n    [zainod"), "{out}");
        // A real failure in the same run keeps both its output and its recap.
        assert!(out.contains("boom"), "{out}");
        let summary = out.find("Summary").unwrap();
        assert!(out[summary..].contains("mod::broke"), "{out}");
        assert!(
            !out[summary..].contains("mod::killed"),
            "a kill is not a to-do item:\n{out}"
        );
    }

    #[test]
    fn final_output_policy_also_exempts_a_kill() {
        // The deferred (`--failure-output=final`) path must make the same call
        // as the immediate one, or the logs simply reappear in the end block.
        let cfg = OutputConfig {
            failure: crate::engine::output::TestOutputDisplay::Final,
            ..OutputConfig::default()
        };
        let mut r = StyledReporter::new(false, true, cfg);
        r.handle(&finished(
            "p::b",
            "t",
            Verdict::Terminated,
            1,
            b"component noise\n",
        ));
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 0,
                failed: 0,
                terminated: 1,
                skipped: 0,
                total: 1,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(!out.contains("component noise"), "{out}");
    }

    #[test]
    fn a_complete_run_emits_no_not_run_warning() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 2,
                failed: 0,
                terminated: 0,
                skipped: 1,
                total: 3,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        // Skips are accounted for; nothing is unaccounted for.
        assert!(!out.contains("warning"), "{out}");
        assert!(out.contains("2/3 tests run: 2 passed, 1 skipped"), "{out}");
    }

    #[test]
    fn not_run_warning_singularizes() {
        let mut r = StyledReporter::new(false, true, OutputConfig::default());
        r.handle(&TestEvent::RunCancelling {
            reason: CancelReason::Interrupt,
            running: 1,
        });
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 0,
                failed: 0,
                terminated: 0,
                skipped: 0,
                total: 1,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(
            out.contains("warning: 1/1 test was not run due to interrupt"),
            "{out}"
        );
    }

    #[test]
    fn starting_line_pluralizes() {
        let mut one = StyledReporter::new(false, true, OutputConfig::default());
        one.handle(&TestEvent::RunStarted {
            total: 1,
            run_id: "r",
        });
        let s1 = String::from_utf8(one.take_scrollback()).unwrap();
        assert_eq!(s1, "    Starting 1 test\n", "{s1:?}");

        let mut many = StyledReporter::new(false, true, OutputConfig::default());
        many.handle(&TestEvent::RunStarted {
            total: 42,
            run_id: "r",
        });
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
        // One running test under a 4-row ceiling → one line, no blank padding.
        let r = vec![running("pkg::b", "mod::a", 5, false)];
        let lines = render_running(&r, 4, false);
        assert_eq!(
            lines.len(),
            1,
            "as tall as content, not padded to `max_rows`"
        );
        assert_eq!(
            lines[0], "             [ 00:00:05] pkg::b mod::a",
            "{:?}",
            lines[0]
        );
    }

    #[test]
    fn running_block_overflows_with_summary() {
        let r: Vec<_> = (0..5)
            .map(|i| running("pkg::b", &format!("t{i}"), i, false))
            .collect();
        let lines = render_running(&r, 3, false);
        assert_eq!(lines.len(), 3);
        assert!(
            lines[2].contains("... and 3 more tests running"),
            "{:?}",
            lines[2]
        );
    }

    #[test]
    fn progress_line_matches_nextest_layout() {
        let stats = RunStats {
            passed: 30,
            failed: 6,
            terminated: 0,
            skipped: 1,
            total: 122,
        };
        // 1h 2m 3s = 3723s.
        let line = progress_line(&stats, 9, Duration::from_secs(3723), false);
        assert_eq!(
            line, "     Running [ 01:02:03] 37/122: 9 running, 30 passed, 6 failed, 1 skipped",
            "{line}"
        );
    }

    #[test]
    fn progress_line_omits_failed_when_zero() {
        let stats = RunStats {
            passed: 5,
            failed: 0,
            terminated: 0,
            skipped: 2,
            total: 10,
        };
        let line = progress_line(&stats, 3, Duration::from_secs(1), false);
        assert!(line.contains("5 passed, 2 skipped"), "{line}");
        assert!(!line.contains("failed"), "{line}");
    }

    #[test]
    fn running_block_empty_when_nothing_runs() {
        // Nothing running → no live rows at all (the panel sits alone), not a block
        // of blank padding.
        assert!(render_running(&[], 3, false).is_empty());
    }

    #[test]
    fn strip_frame_removes_libtest_scaffolding_from_result_err_run() {
        // A real `--exact <t> --nocapture` capture: libtest header, the held-open
        // `test <t> ... ` prefix with the first log glued to it, the merged
        // stdout/stderr body, then the verdict + summary footer.
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
        // Only the single trailing verdict token is consumed, so a log line whose
        // text is literally `FAILED` must survive.
        let raw = b"\nrunning 1 test\ntest t ... step one\nFAILED\nstep two\nFAILED\n\nfailures:\n\nfailures:\n    t\n\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "t")).unwrap();
        assert_eq!(got, "step one\nFAILED\nstep two", "{got:?}");
    }

    #[test]
    fn strip_frame_preserves_panic_body() {
        // A panic prints the `thread … panicked` block to stderr inline, before
        // the verdict; it is signal and must be kept.
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
        // The remote pod path writes the test body (panic) to stderr and libtest's
        // `test <t> ... ` marker to stdout; the container runtime merges the panic
        // *ahead* of the marker, so the body precedes the marker instead of gluing
        // after it. Slicing at the marker would drop the whole failure — it must
        // survive, and the marker (now carrying only the bare verdict) must go.
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
        // No recognizable `test <t> ... ` marker and no `test result:` anchor →
        // no head/tail scaffolding is cut (only edge blank lines are trimmed), so
        // an unexpected format keeps all its content rather than risk eating it.
        let raw = b"some unexpected output shape\nwith two lines\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "t")).unwrap();
        assert_eq!(
            got, "some unexpected output shape\nwith two lines",
            "{got:?}"
        );
    }

    #[test]
    fn strip_frame_handles_module_qualified_test_name() {
        let raw = b"\nrunning 1 test\ntest mod::sub::it ... log line\nok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s\n";
        let got = String::from_utf8(strip_libtest_frame(raw, "mod::sub::it")).unwrap();
        assert_eq!(got, "log line", "{got:?}");
    }

    /// Strip CSI SGR sequences so colour tests can assert on the text.
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
