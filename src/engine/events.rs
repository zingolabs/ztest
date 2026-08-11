//! The lifecycle events the run loop emits and the [`RunReporter`] trait it
//! drives — a ztest-native vocabulary mapping one-to-one onto nextest's
//! `TestEventKind`. [`StyledReporter`](crate::engine::reporter::StyledReporter)

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::qos::QosClass;

/// Terminal result of a single test process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// Exited 0.
    Pass,
    /// Exited non-zero (the captured exit code).
    Fail(i32),
    /// Killed at the tier hard cap (or the `--slow-timeout` terminate).
    Timeout,
    /// The process could not be spawned at all.
    SpawnError,
    /// Killed by the run's cancellation (Ctrl-C) while in flight, before reaching
    /// its own verdict. ztest models only the fact of termination.
    Terminated,
}

/// Why a run was cancelled before every test reached a verdict. Ctrl-C is the
/// only cause today, so [`Interrupt`](CancelReason::Interrupt) is the only variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelReason {
    /// Ctrl-C (SIGINT), caught by the console render thread.
    Interrupt,
}

impl CancelReason {
    /// The word nextest prints after "cancelled due to" (`to_static_str`).
    pub fn as_str(&self) -> &'static str {
        match self {
            CancelReason::Interrupt => "interrupt",
        }
    }
}

impl Verdict {
    /// Whether this verdict counts as a passing test.
    pub fn is_pass(&self) -> bool {
        matches!(self, Verdict::Pass)
    }
}

/// Why a test was never run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkipReason {
    /// Footprint exceeds the empty-of-ztest cluster ceiling: unschedulable.
    ExceedsClusterCapacity,
    /// Footprint exceeds the ServiceAccount budget.
    ExceedsSaBudget,
    /// A resource the test declared (`#[ztest::archive]`/`dev!`) failed to
    /// provision, so it's skipped cleanly rather than failing at
    /// `TestEnv::build()`. Carries a description of the unavailable resource.
    DependencyUnavailable { resource: String },
}

/// A single in-flight test, for the live "running" region (nextest's
/// `--show-progress=running` block). Built fresh each render tick from the
/// loop's in-flight set.
#[derive(Debug, Clone)]
pub struct RunningView {
    /// `<package>::<bin>` identifier.
    pub binary_id: String,
    /// The libtest test name.
    pub test_name: String,
    /// How long this attempt has been running.
    pub elapsed: Duration,
    /// Whether it has crossed the slow threshold.
    pub slow: bool,
}

/// Running tally of a run, fed into the panel and the final summary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStats {
    /// Tests that finished passing (after any retries).
    pub passed: u32,
    /// Tests that finished failing (after exhausting retries).
    pub failed: u32,
    /// Tests killed in flight by the run's cancellation
    /// ([`Verdict::Terminated`]).
    ///
    /// nextest has no equivalent: it renders a signal death as
    /// `Fail { Abort { UnixSignal } }` and folds it into `failed`. That is right
    /// for a test the *kernel* killed (SIGSEGV, SIGABRT) but wrong for one the
    /// operator killed — the test never reached a verdict, so calling it a
    /// failure blames the code for the Ctrl-C. nextest's own tally already
    /// splits `exec failed` and `timed out` out of `failed` on exactly this
    /// principle; this is the same split for the cause nextest lacks a verdict
    /// for.
    #[serde(default)]
    pub terminated: u32,
    /// Tests skipped without running (e.g. unschedulable). As in nextest, this
    /// does *not* include tests that were still queued when a run was cancelled
    /// — those are `total - ran()`, reported by the not-run warning.
    pub skipped: u32,
    /// Total tests in the work-list.
    pub total: usize,
}

impl RunStats {
    /// Tests that have reached a terminal state (ran to a verdict, or skipped).
    pub fn finished(&self) -> u32 {
        self.ran() + self.skipped
    }

    /// Tests that actually executed — the numerator of the summary's
    /// `ran/total` ratio, matching nextest's `finished_count` (which likewise
    /// excludes skips, since nextest filters them out of its initial count).
    pub fn ran(&self) -> u32 {
        self.passed + self.failed + self.terminated
    }

    /// Tests that never started: the work-list minus everything that ran or was
    /// skipped. Non-zero only when a run closed short (cancellation, fail-fast).
    pub fn not_run(&self) -> u32 {
        (self.total as u32).saturating_sub(self.finished())
    }

    /// Whether the run should exit non-zero.
    ///
    /// A terminated test counts: it never reached a verdict, so the run cannot
    /// be called green just because nothing had failed yet when Ctrl-C landed.
    pub fn any_failed(&self) -> bool {
        self.failed > 0 || self.terminated > 0
    }

    /// The summary tally terms, in display order.
    ///
    /// `passed` and `skipped` are always present; `failed` and `sigkilled` only
    /// when non-zero — nextest's `write_summary_str` convention
    /// (`displayer/progress.rs`), which omits its own zero-valued `failed` /
    /// `exec failed` / `timed out` terms. Shared by the styled reporter tail and
    /// the plain `ztest store` listings so the omit-rule lives in one place.
    pub fn tally(&self) -> Vec<(u32, &'static str)> {
        let mut terms = vec![(self.passed, "passed")];
        if self.failed > 0 {
            terms.push((self.failed, "failed"));
        }
        if self.terminated > 0 {
            terms.push((self.terminated, "sigkilled"));
        }
        terms.push((self.skipped, "skipped"));
        terms
    }
}

/// One lifecycle event emitted by the run loop. Borrowed identity fields keep
/// allocation off the hot path; the reporter clones what it needs to retain.
#[derive(Debug, Clone)]
pub enum TestEvent<'a> {
    /// The run is starting; `total` work-items, `run_id` shared by all children.
    RunStarted { total: usize, run_id: &'a str },
    /// A test process has been spawned (attempt is 1-based).
    TestStarted {
        binary_id: &'a str,
        test_name: &'a str,
        class: QosClass,
        attempt: u32,
    },
    /// A running test crossed its slow threshold; `will_terminate` if the hard
    /// cap will kill it. `attempt > 1` renders as nextest's `TRY {attempt} SLOW`.
    TestSlow {
        binary_id: &'a str,
        test_name: &'a str,
        elapsed: Duration,
        will_terminate: bool,
        attempt: u32,
    },
    /// A failed attempt will be retried after `delay`. `verdict`/`duration`
    /// describe the attempt that just failed, so the reporter can render
    /// nextest's magenta `TRY {n} {status}` retry line with its real duration.
    TestRetrying {
        binary_id: &'a str,
        test_name: &'a str,
        next_attempt: u32,
        delay: Duration,
        verdict: Verdict,
        duration: Duration,
    },
    /// A test reached a terminal verdict on its `attempt`-th run (1-based); an
    /// `attempt > 1` renders as nextest's `TRY {attempt} {status}` final line.
    TestFinished {
        binary_id: &'a str,
        test_name: &'a str,
        verdict: Verdict,
        duration: Duration,
        attempt: u32,
        /// Merged stdout+stderr captured from the child (for failure replay).
        output: &'a [u8],
    },
    /// A test was skipped without running.
    TestSkipped {
        binary_id: &'a str,
        test_name: &'a str,
        reason: SkipReason,
    },
    /// Cancellation was requested mid-run (nextest's `RunBeginCancel`): no new
    /// tests will start, the `running` in-flight tests are being terminated, and
    /// the run will close short. Emitted exactly once.
    RunCancelling {
        reason: CancelReason,
        /// In-flight tests at the moment of cancellation (being terminated).
        running: usize,
    },
    /// The run is finished.
    RunFinished {
        stats: RunStats,
        /// Total wall time of the run (for the summary line).
        elapsed: Duration,
    },
}

/// The run-phase reporter. It produces only scroll-lines (PASS/FAIL/summary),
/// accumulated as bytes drained by
/// [`take_scrollback`](RunReporter::take_scrollback); the live progress element
/// is rendered separately by the run loop from
/// [`PanelFrame`](crate::engine::schedule::PanelFrame) data.
pub trait RunReporter {
    /// Consume one lifecycle event.
    fn handle(&mut self, ev: &TestEvent<'_>);
    /// Bytes for native scrollback produced since the last drain (may be empty).
    fn take_scrollback(&mut self) -> Vec<u8>;
}

/// A reporter that discards everything: the sink the scheduler tests drive.
/// The shipping reporter is [`StyledReporter`](crate::engine::reporter::StyledReporter).
#[cfg(test)]
#[derive(Debug, Default)]
pub struct NullReporter;

#[cfg(test)]
impl RunReporter for NullReporter {
    fn handle(&mut self, _ev: &TestEvent<'_>) {}
    fn take_scrollback(&mut self) -> Vec<u8> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_stats_finished_and_failed() {
        let s = RunStats {
            passed: 3,
            failed: 1,
            terminated: 0,
            skipped: 2,
            total: 6,
        };
        assert_eq!(s.finished(), 6);
        assert!(s.any_failed());
    }
}
