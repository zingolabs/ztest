//! When a test's captured output is shown, mirroring nextest's model: two
//! orthogonal choices, *how* stdout/stderr is captured ([`CaptureStrategy`]) and
//! *when* it's displayed ([`TestOutputDisplay`], chosen independently for pass
//! and fail). The engine captures a merged stream, so the default is
//! [`CaptureStrategy::Combined`]; `--no-capture` → [`CaptureStrategy::None`],
//! which (as in nextest) also forces serial execution and immediate display.

use std::str::FromStr;

/// When a test's captured output is written to the report. Chosen independently
/// for passing and failing tests. Kebab-case string forms match nextest's CLI
/// values (`immediate`, `immediate-final`, `final`, `never`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestOutputDisplay {
    /// Show the output as soon as the test completes (interleaved into the live
    /// scrollback feed as each verdict lands).
    Immediate,
    /// Show it both immediately AND again in the end-of-run summary block.
    ImmediateFinal,
    /// Show it only in the end-of-run summary block.
    Final,
    /// Never show it.
    Never,
}

impl TestOutputDisplay {
    /// Whether the output is shown inline the moment the test finishes.
    pub fn is_immediate(self) -> bool {
        matches!(self, Self::Immediate | Self::ImmediateFinal)
    }

    /// Whether the output is (also) shown in the end-of-run summary block.
    pub fn is_final(self) -> bool {
        matches!(self, Self::Final | Self::ImmediateFinal)
    }
}

impl FromStr for TestOutputDisplay {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "immediate" => Ok(Self::Immediate),
            "immediate-final" => Ok(Self::ImmediateFinal),
            "final" => Ok(Self::Final),
            "never" => Ok(Self::Never),
            other => Err(format!(
                "invalid output display {other:?} (expected immediate, immediate-final, final, or never)"
            )),
        }
    }
}

/// How a test process's stdout+stderr is captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureStrategy {
    /// Buffer a merged stdout+stderr stream per test, replayed by the reporter
    /// per [`OutputConfig`]. The default; keeps concurrent tests' output from
    /// interleaving on the live console.
    Combined,
    /// Do not capture: the test streams straight to the terminal
    /// (`--no-capture`). As in nextest this forces serial execution and implies
    /// immediate display for both success and failure.
    None,
}

/// The resolved output policy for a run. Defaults match nextest's built-in
/// `default` profile: failing tests' output is shown immediately, passing tests'
/// output is captured but not displayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputConfig {
    /// When to show a passing test's captured output.
    pub success: TestOutputDisplay,
    /// When to show a failing test's captured output.
    pub failure: TestOutputDisplay,
    /// How output is captured.
    pub capture: CaptureStrategy,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            success: TestOutputDisplay::Never,
            failure: TestOutputDisplay::Immediate,
            capture: CaptureStrategy::Combined,
        }
    }
}

impl OutputConfig {
    /// Apply `--no-capture` semantics: as in nextest, uncaptured output implies
    /// immediate display for both success and failure (there is nothing to defer
    /// to a final block), and the caller must also run serially — see
    /// [`is_serial`](Self::is_serial).
    pub fn with_no_capture(mut self) -> Self {
        self.capture = CaptureStrategy::None;
        self.success = TestOutputDisplay::Immediate;
        self.failure = TestOutputDisplay::Immediate;
        self
    }

    /// The display policy for a test that ended in `passed`.
    pub fn display_for(&self, passed: bool) -> TestOutputDisplay {
        if passed { self.success } else { self.failure }
    }

    /// Whether any output is captured at all. `false` under `--no-capture`,
    /// where the test inherits the terminal directly.
    pub fn captures(&self) -> bool {
        matches!(self.capture, CaptureStrategy::Combined)
    }

    /// Whether the run must execute serially (one test at a time). True only
    /// under `--no-capture`, matching nextest's `test_threads = 1` coupling.
    pub fn is_serial(&self) -> bool {
        matches!(self.capture, CaptureStrategy::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_display_values() {
        assert_eq!(
            "immediate".parse(),
            Ok(TestOutputDisplay::Immediate)
        );
        assert_eq!(
            "immediate-final".parse(),
            Ok(TestOutputDisplay::ImmediateFinal)
        );
        assert_eq!("final".parse(), Ok(TestOutputDisplay::Final));
        assert_eq!("never".parse(), Ok(TestOutputDisplay::Never));
        assert!("bogus".parse::<TestOutputDisplay>().is_err());
    }

    #[test]
    fn immediate_and_final_predicates() {
        use TestOutputDisplay::*;
        assert!(Immediate.is_immediate() && !Immediate.is_final());
        assert!(Final.is_final() && !Final.is_immediate());
        assert!(ImmediateFinal.is_immediate() && ImmediateFinal.is_final());
        assert!(!Never.is_immediate() && !Never.is_final());
    }

    #[test]
    fn defaults_match_nextest() {
        let c = OutputConfig::default();
        assert_eq!(c.success, TestOutputDisplay::Never);
        assert_eq!(c.failure, TestOutputDisplay::Immediate);
        assert!(c.captures());
        assert!(!c.is_serial());
    }

    #[test]
    fn no_capture_forces_immediate_and_serial() {
        let c = OutputConfig::default().with_no_capture();
        assert_eq!(c.success, TestOutputDisplay::Immediate);
        assert_eq!(c.failure, TestOutputDisplay::Immediate);
        assert!(!c.captures());
        assert!(c.is_serial());
    }

    #[test]
    fn display_for_selects_by_verdict() {
        let c = OutputConfig {
            success: TestOutputDisplay::Final,
            failure: TestOutputDisplay::Immediate,
            capture: CaptureStrategy::Combined,
        };
        assert_eq!(c.display_for(true), TestOutputDisplay::Final);
        assert_eq!(c.display_for(false), TestOutputDisplay::Immediate);
    }
}
