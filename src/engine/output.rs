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

/// Component-log lines shown per test (`--log-tail`): most recent across all pods, or `All`.
/// Display-only (capture + record keep every line)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogTail {
    Lines(usize),
    All,
}

impl LogTail {
    pub const DEFAULT: Self = Self::Lines(30);

    /// Lines kept out of `available`
    pub fn keep(self, available: usize) -> usize {
        match self {
            Self::Lines(n) => n.min(available),
            Self::All => available,
        }
    }
}

impl Default for LogTail {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl FromStr for LogTail {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(Self::All),
            n => n
                .parse()
                .map(Self::Lines)
                .map_err(|_| format!("invalid log tail {n:?} (expected a line count or `all`)")),
        }
    }
}

impl std::fmt::Display for LogTail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lines(n) => write!(f, "{n}"),
            Self::All => f.write_str("all"),
        }
    }
}

/// TOML form: `30` or `"all"`
impl serde::Serialize for LogTail {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Lines(n) => s.serialize_u64(*n as u64),
            Self::All => s.serialize_str("all"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for LogTail {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Lines(usize),
            Word(String),
        }
        match Raw::deserialize(d)? {
            Raw::Lines(n) => Ok(Self::Lines(n)),
            Raw::Word(w) => w.parse().map_err(serde::de::Error::custom),
        }
    }
}

/// Resolved output policy. Defaults = nextest's `default` profile (fail shown
/// immediately, pass captured but hidden)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputConfig {
    pub success: TestOutputDisplay,
    pub failure: TestOutputDisplay,
    pub capture: CaptureStrategy,
    pub log_tail: LogTail,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            success: TestOutputDisplay::Never,
            failure: TestOutputDisplay::Immediate,
            capture: CaptureStrategy::Combined,
            log_tail: LogTail::DEFAULT,
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
            log_tail: LogTail::DEFAULT,
        };
        assert_eq!(c.display_for(true), TestOutputDisplay::Final);
        assert_eq!(c.display_for(false), TestOutputDisplay::Immediate);
    }

    #[test]
    fn log_tail_parses_and_round_trips_through_toml() {
        assert_eq!("0".parse(), Ok(LogTail::Lines(0)));
        assert_eq!("200".parse(), Ok(LogTail::Lines(200)));
        assert_eq!("all".parse(), Ok(LogTail::All));
        assert!("-1".parse::<LogTail>().is_err());
        assert!("everything".parse::<LogTail>().is_err());

        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Doc {
            tail: LogTail,
        }
        for (tail, toml_form) in
            [(LogTail::Lines(30), "tail = 30\n"), (LogTail::All, "tail = \"all\"\n")]
        {
            assert_eq!(toml::to_string(&Doc { tail }).unwrap(), toml_form);
            assert_eq!(toml::from_str::<Doc>(toml_form).unwrap(), Doc { tail });
        }
        assert!(toml::from_str::<Doc>("tail = \"lots\"").is_err());

        assert_eq!(LogTail::Lines(30).keep(10), 10);
        assert_eq!(LogTail::Lines(30).keep(100), 30);
        assert_eq!(LogTail::All.keep(100), 100);
    }
}
