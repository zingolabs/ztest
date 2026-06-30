//! A ztest-owned, nextest-*style* run reporter.
//!
//! Formats the engine's lifecycle events into scrolling status lines that mirror
//! `cargo nextest run`'s default human output — `PASS`/`FAIL`/`SLOW`/retry lines
//! with right-aligned durations, failure-output replay, and a final summary —
//! plus colour via `owo-colors`. Visually faithful, not byte-identical: we own
//! every line, so there is no `nextest-runner` dependency and no vendored fork
//! to re-sync (see the plan's rendering decision).
//!
//! This reporter emits only scrollback. The live region (the nextest-style
//! [`progress_line`] + [`render_running`] list, plus the QoS panel) is rendered
//! separately by the run loop.

use std::io::Write as _;
use std::time::Duration;

use owo_colors::OwoColorize as _;

use crate::engine::events::{RunReporter, RunStats, RunningView, SkipReason, TestEvent, Verdict};

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
/// from terminal support; non-TTY runs pass `color = false`.
#[derive(Debug)]
pub struct StyledReporter {
    color: bool,
    buf: Vec<u8>,
    stats: RunStats,
}

impl StyledReporter {
    /// A reporter; `color` enables ANSI styling.
    pub fn new(color: bool) -> Self {
        Self {
            color,
            buf: Vec::new(),
            stats: RunStats::default(),
        }
    }

    /// Right-align a status word in a fixed 12-col field and colour it (nextest
    /// pads the visible text, then wraps it in ANSI — so we pad first too).
    fn status(&self, word: &str, ink: Ink) -> String {
        let padded = format!("{word:>12}");
        self.paint(&padded, ink)
    }

    /// Apply an [`Ink`] to a string (no-op when colour is off).
    fn paint(&self, s: &str, ink: Ink) -> String {
        paint_word(s, ink, self.color)
    }

    /// `[   1.234s]` — right-aligned seconds, unstyled (matching nextest).
    fn bracket_dur(d: Duration) -> String {
        format!("[{:>8.3}s]", d.as_secs_f64())
    }

    /// `{binary_id} {module::path}::{leaf}` styled like nextest's
    /// `DisplayTestInstance`: binary-id magenta·bold, module path cyan, leaf
    /// blue·bold.
    fn styled_instance(&self, bin: &str, test: &str) -> String {
        instance_str(bin, test, self.color)
    }

    /// One verdict line: `{status} [   d.ddds] {binary} {module::test}`.
    fn verdict_line(&mut self, status: &str, ink: Ink, dur: Duration, bin: &str, test: &str) {
        let _ = writeln!(
            self.buf,
            "{} {} {}",
            self.status(status, ink),
            Self::bracket_dur(dur),
            self.styled_instance(bin, test),
        );
    }

    /// Replay a failed test's captured output under a header.
    fn replay_output(&mut self, bin: &str, test: &str, output: &[u8]) {
        if output.is_empty() {
            return;
        }
        let header = format!("--- output: {bin} {test} ---");
        let _ = writeln!(self.buf, "\n{}", self.paint(&header, Ink::Fail));
        // Verbatim, then a trailing newline if the output didn't end in one.
        let _ = self.buf.write_all(output);
        if !output.ends_with(b"\n") {
            let _ = self.buf.write_all(b"\n");
        }
        let _ = writeln!(self.buf, "{}", self.paint("---", Ink::Fail));
    }
}

impl RunReporter for StyledReporter {
    fn handle(&mut self, ev: &TestEvent<'_>) {
        match ev {
            TestEvent::RunStarted { total, .. } => {
                self.stats.total = *total;
                // nextest: right-aligned green·bold "Starting", bold count.
                let _ = writeln!(
                    self.buf,
                    "{} {} tests",
                    self.status("Starting", Ink::Pass),
                    self.count(*total as u64),
                );
            }
            // Test starts are reflected in the pinned QoS panel, not scrollback.
            TestEvent::TestStarted { .. } => {}
            TestEvent::TestSlow {
                binary_id,
                test_name,
                elapsed,
                will_terminate,
            } => {
                let (word, ink) = if *will_terminate {
                    ("TERMINATING", Ink::Fail)
                } else {
                    ("SLOW", Ink::Skip)
                };
                self.verdict_line(word, ink, *elapsed, binary_id, test_name);
            }
            TestEvent::TestRetrying {
                binary_id,
                test_name,
                next_attempt,
                ..
            } => {
                let word = format!("TRY {next_attempt}");
                self.verdict_line(&word, Ink::Retry, Duration::ZERO, binary_id, test_name);
            }
            TestEvent::TestFinished {
                binary_id,
                test_name,
                verdict,
                duration,
                output,
            } => {
                let (word, ink) = match verdict {
                    Verdict::Pass => ("PASS", Ink::Pass),
                    Verdict::Fail(_) => ("FAIL", Ink::Fail),
                    Verdict::Timeout => ("TIMEOUT", Ink::Fail),
                    Verdict::SpawnError => ("ERROR", Ink::Fail),
                };
                self.verdict_line(word, ink, *duration, binary_id, test_name);
                if !verdict.is_pass() {
                    self.replay_output(binary_id, test_name, output);
                }
                if verdict.is_pass() {
                    self.stats.passed += 1;
                } else {
                    self.stats.failed += 1;
                }
            }
            TestEvent::TestSkipped {
                binary_id,
                test_name,
                reason,
            } => {
                let why = match reason {
                    SkipReason::ExceedsClusterCapacity => "exceeds cluster capacity",
                    SkipReason::ExceedsSaBudget => "exceeds SA budget",
                };
                let _ = writeln!(
                    self.buf,
                    "{} {} {} ({why})",
                    self.status("SKIP", Ink::Skip),
                    bin_pad(),
                    self.styled_instance(binary_id, test_name),
                );
                self.stats.skipped += 1;
            }
            TestEvent::RunFinished { stats, elapsed } => {
                self.summary(stats, *elapsed);
            }
        }
    }

    fn take_scrollback(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }
}

impl StyledReporter {
    /// A bold count number (nextest's `count` style).
    fn count(&self, n: u64) -> String {
        bold_count(n, self.color)
    }

    /// The closing summary block — a unicode rule then
    /// `Summary [   d.ddds] N tests run: X passed[, Y failed], Z skipped`,
    /// matching nextest's `RunFinished` line (imp.rs): the rule + a `Summary`
    /// label coloured by final status (green pass / red fail / yellow no-run),
    /// the elapsed bracket, `finished[/total]`, then the shared counts tail.
    fn summary(&mut self, stats: &RunStats, elapsed: Duration) {
        // nextest draws a 12-char horizontal rule (U+2500), not ASCII dashes.
        let _ = writeln!(self.buf, "{}", "\u{2500}".repeat(12));
        // nextest colours "Summary" by outcome: fail if anything failed, skip
        // if nothing ran, else pass.
        let ink = if stats.failed > 0 {
            Ink::Fail
        } else if stats.finished() == 0 {
            Ink::Skip
        } else {
            Ink::Pass
        };
        let label = self.paint(&format!("{:>12}", "Summary"), ink);
        // `9/122` when the run stopped short, else just the finished count.
        let finished = stats.finished();
        let ran = if (finished as usize) != stats.total {
            format!("{}/{}", self.count(finished as u64), self.count(stats.total as u64))
        } else {
            self.count(finished as u64)
        };
        let _ = writeln!(
            self.buf,
            "{label} {} {ran} tests run: {}",
            Self::bracket_dur(elapsed),
            counts_tail(stats, self.color),
        );
    }
}

/// A blank field matching [`StyledReporter::bracket_dur`]'s 11-char width
/// (`[` + 8-wide secs + `s` + `]`), so SKIP lines — which have no duration —
/// keep the instance column aligned with the verdict lines above them.
fn bin_pad() -> &'static str {
    "           "
}

/// `{binary_id} {module::path}::{leaf}` — binary-id magenta·bold, module path
/// cyan, leaf blue·bold (nextest's `DisplayTestInstance`). Shared by the
/// scrollback verdict lines and the live running region.
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

/// `HH:MM:SS` elapsed, matching nextest's running-test clock.
fn hms(d: Duration) -> String {
    let s = d.as_secs();
    format!("{:0>2}:{:0>2}:{:0>2}", s / 3600, (s / 60) % 60, s % 60)
}

/// Render the live "running" region (nextest's `--show-progress=running`
/// block) as exactly `rows` lines: one per in-flight test —
/// `{status} [HH:MM:SS] {binary} {test}` — longest-running first, sitting
/// directly beneath the progress line, with a trailing `... and K more running`
/// when the set overflows, then blank padding below so the region's height
/// stays stable frame-to-frame.
///
/// `running` is assumed already sorted longest-first. Returns ANSI strings (one
/// per line); the caller bridges them into the viewport's live region.
pub(crate) fn render_running(running: &[RunningView], rows: usize, color: bool) -> Vec<String> {
    if rows == 0 {
        return Vec::new();
    }

    // Reserve the last row for an overflow summary when the set doesn't fit.
    let overflow = running.len() > rows;
    let shown = if overflow { rows - 1 } else { running.len() };

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

    // Pad at the *bottom* (blanks between the list and the pinned panel) so the
    // running rows sit directly beneath the progress line with no gap — matching
    // nextest, which appends blank lines after the running set to keep the
    // region's height stable.
    let mut lines = content;
    lines.resize(rows, String::new());
    lines
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

/// The shared `{p} passed[, {f} failed], {s} skipped` tail used by both the
/// live [`progress_line`] and the final [`StyledReporter::summary`], matching
/// nextest's `write_summary_str` (progress.rs): `passed` and `skipped` are
/// always shown, `failed` only when non-zero.
fn counts_tail(stats: &RunStats, color: bool) -> String {
    let tally = |n: u32, word: &str, ink: Ink| {
        format!("{} {}", bold_count(n as u64, color), paint_word(word, ink, color))
    };
    let mut s = format!("{}, ", tally(stats.passed, "passed", Ink::Pass));
    if stats.failed > 0 {
        s.push_str(&format!("{}, ", tally(stats.failed, "failed", Ink::Fail)));
    }
    s.push_str(&tally(stats.skipped, "skipped", Ink::Skip));
    s
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

    fn finished(bin: &str, test: &str, v: Verdict, out: &[u8]) -> TestEvent<'static> {
        // Leak the strings for 'static test events (test-only).
        let bin: &'static str = Box::leak(bin.to_string().into_boxed_str());
        let test: &'static str = Box::leak(test.to_string().into_boxed_str());
        let out: &'static [u8] = Box::leak(out.to_vec().into_boxed_slice());
        TestEvent::TestFinished {
            binary_id: bin,
            test_name: test,
            verdict: v,
            duration: Duration::from_millis(234),
            output: out,
        }
    }

    #[test]
    fn pass_line_is_plain_when_no_color() {
        let mut r = StyledReporter::new(false);
        r.handle(&finished("pkg::bin", "mod::ok", Verdict::Pass, b""));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.contains("PASS"), "{out}");
        assert!(out.contains("0.234s"), "{out}");
        assert!(out.contains("pkg::bin mod::ok"), "{out}");
        // No ANSI escapes when color is off.
        assert!(!out.contains('\u{1b}'), "unexpected ANSI: {out:?}");
    }

    #[test]
    fn fail_replays_captured_output() {
        let mut r = StyledReporter::new(false);
        r.handle(&finished(
            "pkg::bin",
            "mod::boom",
            Verdict::Fail(101),
            b"panicked at boom",
        ));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.contains("FAIL"), "{out}");
        assert!(out.contains("--- output: pkg::bin mod::boom ---"), "{out}");
        assert!(out.contains("panicked at boom"), "{out}");
    }

    #[test]
    fn color_emits_ansi() {
        let mut r = StyledReporter::new(true);
        r.handle(&finished("p::b", "t", Verdict::Pass, b""));
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.contains('\u{1b}'), "expected ANSI escapes: {out:?}");
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
    fn running_block_hugs_label_and_pads_below() {
        let r = vec![running("pkg::b", "mod::a", 5, false)];
        let lines = render_running(&r, 4, false);
        assert_eq!(lines.len(), 4, "padded to exactly `rows`");
        // Running line first (directly under the progress label), blanks below.
        assert!(lines[0].contains("pkg::b mod::a"), "{:?}", lines[0]);
        assert!(lines[0].contains("00:00:05"), "{:?}", lines[0]);
        assert_eq!(lines[3], "");
    }

    #[test]
    fn running_block_overflows_with_summary() {
        let r: Vec<_> = (0..5)
            .map(|i| running("pkg::b", &format!("t{i}"), i, false))
            .collect();
        let lines = render_running(&r, 3, false);
        assert_eq!(lines.len(), 3);
        // 3 rows: 2 tests shown + 1 overflow summary (5 - 2 = 3 more).
        assert!(lines[2].contains("3 more tests running"), "{:?}", lines[2]);
    }

    #[test]
    fn progress_line_matches_nextest_layout() {
        let stats = RunStats {
            passed: 30,
            failed: 6,
            skipped: 1,
            total: 122,
        };
        // 1h 2m 3s = 3723s.
        let line = progress_line(&stats, 9, Duration::from_secs(3723), false);
        // `   Running [ 01:02:03] 37/122: 9 running, 30 passed, 6 failed, 1 skipped`
        assert!(line.trim_start().starts_with("Running"), "{line}");
        assert!(line.contains("[ 01:02:03]"), "{line}");
        assert!(line.contains("37/122:"), "{line}"); // finished = 30+6+1
        assert!(line.contains("9 running"), "{line}");
        assert!(line.contains("30 passed"), "{line}");
        assert!(line.contains("6 failed"), "{line}");
        assert!(line.contains("1 skipped"), "{line}");
    }

    #[test]
    fn progress_line_omits_failed_when_zero() {
        // nextest's write_summary_str shows passed + skipped always, failed
        // only when non-zero — so no "0 failed" clause.
        let stats = RunStats {
            passed: 5,
            failed: 0,
            skipped: 2,
            total: 10,
        };
        let line = progress_line(&stats, 3, Duration::from_secs(1), false);
        assert!(line.contains("5 passed, 2 skipped"), "{line}");
        assert!(!line.contains("failed"), "{line}");
    }

    #[test]
    fn summary_omits_failed_when_zero_and_shows_partial_total() {
        let mut r = StyledReporter::new(false);
        // 7 finished of 10 total (an interrupted run) with no failures.
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 5,
                failed: 0,
                skipped: 2,
                total: 10,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.contains("7/10 tests run"), "{out}");
        assert!(out.contains("5 passed, 2 skipped"), "{out}");
        assert!(!out.contains("failed"), "{out}");
    }

    #[test]
    fn running_block_empty_is_all_blank() {
        let lines = render_running(&[], 3, false);
        assert_eq!(lines, vec!["".to_string(), "".to_string(), "".to_string()]);
    }

    #[test]
    fn summary_reports_counts() {
        let mut r = StyledReporter::new(false);
        r.handle(&TestEvent::RunFinished {
            stats: RunStats {
                passed: 8,
                failed: 2,
                skipped: 1,
                total: 11,
            },
            elapsed: Duration::from_secs(1),
        });
        let out = String::from_utf8(r.take_scrollback()).unwrap();
        assert!(out.contains("11 tests run"), "{out}");
        assert!(out.contains("8 passed"), "{out}");
        assert!(out.contains("2 failed"), "{out}");
        assert!(out.contains("1 skipped"), "{out}");
    }
}
