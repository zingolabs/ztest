//! The lifecycle events the run loop emits, plus the [`RunReporter`] trait the
//! loop drives.
//!
//! A small ztest-native vocabulary mapping one-to-one onto nextest's
//! `TestEventKind` (started, slow, retrying, finished, skipped, plus run
//! started/finished). The concrete reporter that turns them into nextest-style
//! output is [`StyledReporter`](crate::engine::reporter::StyledReporter);
//! [`NullReporter`] is a discard sink for tests.

use std::time::Duration;

use crate::qos::QosClass;

/// Terminal result of a single test process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Exited 0.
    Pass,
    /// Exited non-zero (the captured exit code).
    Fail(i32),
    /// Killed at the tier hard cap (or the `--slow-timeout` terminate).
    Timeout,
    /// The process could not be spawned at all.
    SpawnError,
}

impl Verdict {
    /// Whether this verdict counts as a passing test.
    pub fn is_pass(&self) -> bool {
        matches!(self, Verdict::Pass)
    }
}

/// Why a test was never run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// Footprint exceeds the empty-of-ztest cluster ceiling: unschedulable.
    ExceedsClusterCapacity,
    /// Footprint exceeds the ServiceAccount budget.
    ExceedsSaBudget,
    /// A resource the test declared (`#[ztest::archive]`/`dev!`) failed to
    /// provision (or is unreachable), so the test can't run — skipped cleanly
    /// rather than failing at `TestEnv::build()`. Carries a human-readable
    /// description of the unavailable resource.
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunStats {
    /// Tests that finished passing (after any retries).
    pub passed: u32,
    /// Tests that finished failing (after exhausting retries).
    pub failed: u32,
    /// Tests skipped without running (e.g. unschedulable).
    pub skipped: u32,
    /// Total tests in the work-list.
    pub total: usize,
}

impl RunStats {
    /// Tests that have reached a terminal state (passed, failed, or skipped).
    pub fn finished(&self) -> u32 {
        self.passed + self.failed + self.skipped
    }

    /// Whether the run should exit non-zero (any test failed).
    pub fn any_failed(&self) -> bool {
        self.failed > 0
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
    /// The run is finished.
    RunFinished {
        stats: RunStats,
        /// Total wall time of the run (for the summary line).
        elapsed: Duration,
    },
}

/// The run-phase reporter. Implemented by
/// [`StyledReporter`](crate::engine::reporter::StyledReporter) (nextest-style
/// output) and [`NullReporter`] (a discard sink for tests).
///
/// Scroll-lines (PASS/FAIL/summary) accumulate as bytes drained by
/// [`take_scrollback`](RunReporter::take_scrollback). The live element (the
/// nextest-style progress line plus running list) is rendered separately by the
/// run loop from [`PanelFrame`](crate::engine::schedule::PanelFrame) data.
pub trait RunReporter {
    /// Consume one lifecycle event.
    fn handle(&mut self, ev: &TestEvent<'_>);
    /// Bytes for native scrollback produced since the last drain (may be empty).
    fn take_scrollback(&mut self) -> Vec<u8>;
}

/// A reporter that discards everything: a sink for tests and a safe default.
/// The real output reporter is [`StyledReporter`](crate::engine::reporter::StyledReporter).
#[derive(Debug, Default)]
pub struct NullReporter;

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
            skipped: 2,
            total: 6,
        };
        assert_eq!(s.finished(), 6);
        assert!(s.any_failed());
    }
}
