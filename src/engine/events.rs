//! Run-loop lifecycle events + the [`RunReporter`] trait they drive.
//!
//! - ztest-native vocabulary, 1:1 onto nextest's `TestEventKind`
//! - [`StyledReporter`](crate::engine::reporter::StyledReporter) = the production impl

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::qos::QosClass;

/// Terminal result of one test process. `Terminated` = killed in flight by the run's
/// cancellation, before any verdict of its own
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    Pass,
    Fail(i32),
    Timeout,
    SpawnError,
    Terminated,
}

/// Why a run was cancelled short of every verdict
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelReason {
    Interrupt,
}

impl CancelReason {
    /// Word nextest prints after "cancelled due to" (`to_static_str`)
    pub fn as_str(&self) -> &'static str {
        match self {
            CancelReason::Interrupt => "interrupt",
        }
    }
}

impl Verdict {
    pub fn is_pass(&self) -> bool {
        matches!(self, Verdict::Pass)
    }
}

/// Why a test never ran. `DependencyUnavailable.resource` names the declared
/// (`#[ztest::archive]` / `dev!`) resource that failed to provision
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkipReason {
    ExceedsClusterCapacity,
    ExceedsSaBudget,
    DependencyUnavailable { resource: String },
}

/// One in-flight test for the live region (nextest's `--show-progress=running` block)
#[derive(Debug, Clone)]
pub struct RunningView {
    pub binary_id: String,
    pub test_name: String,
    pub elapsed: Duration,
    pub slow: bool,
}

/// Running tally, fed to the panel and the final summary.
///
/// - `terminated` split out of `failed` (a Ctrl-C'd test reached no verdict of its own)
/// - `skipped` excludes tests still queued at cancellation, which are `total - ran()`
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStats {
    pub passed: u32,
    pub failed: u32,
    #[serde(default)]
    pub terminated: u32,
    pub skipped: u32,
    pub total: usize,
}

impl RunStats {
    pub fn finished(&self) -> u32 {
        self.ran() + self.skipped
    }

    /// Tests that executed = numerator of the summary's `ran/total`, matching
    /// nextest's `finished_count` (skips excluded)
    pub fn ran(&self) -> u32 {
        self.passed + self.failed + self.terminated
    }

    /// Tests that never started. Non-zero only on a run closed short (cancel, fail-fast)
    pub fn not_run(&self) -> u32 {
        (self.total as u32).saturating_sub(self.finished())
    }

    /// Exit non-zero. Terminated counts (no verdict reached → the run is not green
    /// merely because nothing had failed when Ctrl-C landed)
    pub fn any_failed(&self) -> bool {
        self.failed > 0 || self.terminated > 0
    }

    /// Summary tally terms in display order. `passed`/`skipped` always, `failed`/
    /// `sigkilled` only when non-zero (nextest's `write_summary_str` omit-rule, shared
    /// by the styled reporter tail and the plain `ztest store` listings)
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

/// One lifecycle event from the run loop. Borrowed identity fields keep allocation off
/// the hot path (reporters clone what they retain).
///
/// - `attempt` 1-based throughout
/// - `TestSlow.will_terminate` = the hard cap will kill it
/// - `TestRetrying`'s `verdict`/`duration` describe the attempt that just failed
/// - `RunCancelling` at most once, and no test starts after it
#[derive(Debug, Clone)]
pub enum TestEvent<'a> {
    RunStarted {
        total: usize,
        run_id: &'a str,
    },
    TestStarted {
        binary_id: &'a str,
        test_name: &'a str,
        class: QosClass,
        attempt: u32,
    },
    TestSlow {
        binary_id: &'a str,
        test_name: &'a str,
        elapsed: Duration,
        will_terminate: bool,
        attempt: u32,
    },
    TestRetrying {
        binary_id: &'a str,
        test_name: &'a str,
        next_attempt: u32,
        delay: Duration,
        verdict: Verdict,
        duration: Duration,
    },
    TestFinished {
        binary_id: &'a str,
        test_name: &'a str,
        verdict: Verdict,
        duration: Duration,
        attempt: u32,
        output: &'a [u8],
    },
    TestSkipped {
        binary_id: &'a str,
        test_name: &'a str,
        reason: SkipReason,
    },
    RunCancelling {
        reason: CancelReason,
        running: usize,
    },
    RunFinished {
        stats: RunStats,
        elapsed: Duration,
    },
}

/// Run-phase reporter: scroll-lines only (PASS/FAIL/summary), as bytes drained by
/// [`take_scrollback`](RunReporter::take_scrollback). Live progress is the run loop's,
/// from [`PanelFrame`](crate::engine::schedule::PanelFrame)
pub trait RunReporter {
    fn handle(&mut self, ev: &TestEvent<'_>);
    fn take_scrollback(&mut self) -> Vec<u8>;
}

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
        let s = RunStats { passed: 3, failed: 1, terminated: 0, skipped: 2, total: 6 };
        assert_eq!(s.finished(), 6);
        assert!(s.any_failed());
    }
}
