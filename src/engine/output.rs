//! Captured-output policy, on nextest's model: *how* ([`CaptureStrategy`]) and
//! *when* ([`TestOutputDisplay`], per pass/fail) are orthogonal.
//!
//! - Engine merges the stream → default [`CaptureStrategy::Combined`]
//! - `--no-capture` = [`CaptureStrategy::None`] → serial + immediate (nextest parity)

use std::str::FromStr;

/// When captured output reaches the report, chosen per pass/fail.
/// Kebab-case forms = nextest's CLI values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestOutputDisplay {
    Immediate,
    ImmediateFinal,
    Final,
    Never,
}

impl TestOutputDisplay {
    pub fn is_immediate(self) -> bool {
        matches!(self, Self::Immediate | Self::ImmediateFinal)
    }

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
            other => Err(format!("invalid output display {other:?}")),
        }
    }
}

/// - `Combined` (default) buffers one merged stream per test (concurrent tests
///   would interleave on the live console)
/// - `None` = `--no-capture` → serial + immediate display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureStrategy {
    Combined,
    None,
}

/// Resolved output policy. Defaults = nextest's `default` profile (fail shown
/// immediately, pass captured but hidden)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputConfig {
    pub success: TestOutputDisplay,
    pub failure: TestOutputDisplay,
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
    /// `--no-capture`: uncaptured → immediate both ways (nothing left to defer).
    /// Caller must also run serially, see [`is_serial`](Self::is_serial)
    pub fn with_no_capture(mut self) -> Self {
        self.capture = CaptureStrategy::None;
        self.success = TestOutputDisplay::Immediate;
        self.failure = TestOutputDisplay::Immediate;
        self
    }

    pub fn display_for(&self, passed: bool) -> TestOutputDisplay {
        if passed { self.success } else { self.failure }
    }

    /// `false` under `--no-capture` (test inherits the terminal)
    pub fn captures(&self) -> bool {
        matches!(self.capture, CaptureStrategy::Combined)
    }

    /// True only under `--no-capture` (nextest's `test_threads = 1` coupling)
    pub fn is_serial(&self) -> bool {
        matches!(self.capture, CaptureStrategy::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_display_values() {
        assert_eq!("immediate".parse(), Ok(TestOutputDisplay::Immediate));
        assert_eq!("immediate-final".parse(), Ok(TestOutputDisplay::ImmediateFinal));
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
