//! Record, replay, and (in future) rerun of a test run — ztest's take on
//! nextest's recording subsystem.
//!
//! The design rests on the engine's existing seam: every run flows through the
//! [`TestEvent`](crate::engine::events::TestEvent) stream that
//! [`StyledReporter`](crate::engine::reporter::StyledReporter) renders. If that
//! stream can be persisted, replay is a consequence, not a feature: [`record`]
//! tees the stream to disk, and [`replay`] reads it back and feeds the identical
//! events to an identical reporter — which cannot tell live from replayed.
//!
//! What's persisted, per run, under a per-user cache dir keyed by workspace:
//!
//! ```text
//! {cache}/records/{workspace_id}/{run_id}/
//!     meta.json        run id, start time, argv, format version
//!     run.log.zst      zstd JSON Lines: one RecordedEvent per line
//!     out/{hash}-combined   content-addressed, deduped, zstd output blobs
//! ```
//!
//! This mirrors nextest's split of a streamed event log from a content-addressed
//! output store. Two deliberate right-sizings versus nextest: ztest has a single
//! output-bearing event ([`TestFinished`](crate::engine::events::TestEvent::TestFinished)),
//! so the stored form is one owned [`RecordedEvent`] rather than a form generic
//! over an `OutputSpec`; and the store is a plain directory rather than a zip —
//! portable single-file export is deferred.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::engine::events::{CancelReason, RunStats, SkipReason, Verdict};
use crate::qos::QosClass;

pub mod locate;
pub mod portable;
pub mod recorder;
pub mod replay;
pub mod rerun;
pub mod retention;
pub mod store;

pub use recorder::{RecordingReporter, RunRecorder};
pub use replay::replay;
pub use rerun::passed_tests;
pub use store::StoreRef;

/// On-disk format version. Bumped on a breaking change to [`RecordedEvent`] /
/// [`RunMeta`] / the store layout; a reader refuses a newer major.
pub const FORMAT_VERSION: u32 = 1;

/// The environment opt-out. Recording is on by default (a decorator plus a
/// compressed file); `ZTEST_NO_RECORD=1` disables it.
pub fn recording_enabled() -> bool {
    !std::env::var_os("ZTEST_NO_RECORD").is_some_and(|v| v == "1" || v == "true")
}

/// Run-level metadata written once at run start (`meta.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMeta {
    /// [`FORMAT_VERSION`] the recording was written with.
    pub format_version: u32,
    /// The shared `run_id` every child inherited.
    pub run_id: String,
    /// RFC 3339 UTC start time.
    pub started_at: String,
    /// The full `ztest` argv (for `store list` and provenance).
    pub args: Vec<String>,
    /// Whether test output was captured. `false` under `--no-capture`, where a
    /// test's output streamed straight to the terminal and was never stored — so
    /// replay renders `(output not captured)` rather than blank output.
    pub captured: bool,
}

/// Read a run's `meta.json`.
pub fn read_meta(run_dir: &std::path::Path) -> std::io::Result<RunMeta> {
    let bytes = std::fs::read(run_dir.join("meta.json"))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("meta.json: {e}")))
}

/// The final [`RunStats`] recorded for a run, or `None` if it never reached
/// `RunFinished` (e.g. a hard crash mid-run).
pub fn final_stats(run_dir: &std::path::Path) -> std::io::Result<Option<RunStats>> {
    use std::io::BufRead;
    let log = std::fs::File::open(run_dir.join("run.log.zst"))?;
    let decoder = zstd::stream::read::Decoder::new(std::io::BufReader::new(log))?;
    let mut stats = None;
    for line in std::io::BufReader::new(decoder).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(RecordedEvent::RunFinished { stats: s, .. }) =
            serde_json::from_str::<RecordedEvent>(&line)
        {
            stats = Some(s);
        }
    }
    Ok(stats)
}

/// The owned, serializable mirror of [`TestEvent`](crate::engine::events::TestEvent).
/// Identity fields are owned `String`s (vs the live event's borrowed slices) and
/// `TestFinished`'s output is a [`StoreRef`] into the output store rather than
/// inline bytes. One line of `run.log.zst` deserializes into one of these.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RecordedEvent {
    RunStarted {
        total: usize,
        run_id: String,
    },
    TestStarted {
        binary_id: String,
        test_name: String,
        class: QosClass,
        attempt: u32,
    },
    TestSlow {
        binary_id: String,
        test_name: String,
        elapsed: Duration,
        will_terminate: bool,
        attempt: u32,
    },
    TestRetrying {
        binary_id: String,
        test_name: String,
        next_attempt: u32,
        delay: Duration,
        verdict: Verdict,
        duration: Duration,
    },
    TestFinished {
        binary_id: String,
        test_name: String,
        verdict: Verdict,
        duration: Duration,
        attempt: u32,
        output: StoreRef,
    },
    TestSkipped {
        binary_id: String,
        test_name: String,
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

/// How the user names the run to replay/rerun, mirroring nextest's selector
/// grammar: `latest`, a run-id (full or unambiguous prefix), or a filesystem
/// path to a recording directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunSelector {
    /// The most recent run for this workspace.
    Latest,
    /// A run id, or an unambiguous prefix of one.
    Id(String),
    /// A direct path to a recording directory.
    Path(PathBuf),
}

impl FromStr for RunSelector {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(if s == "latest" {
            RunSelector::Latest
        } else if is_path_like(s) {
            RunSelector::Path(PathBuf::from(s))
        } else {
            RunSelector::Id(s.to_string())
        })
    }
}

/// Whether a selector token denotes a filesystem path rather than a run id:
/// it contains a path separator or ends in a recording suffix (nextest's rule).
fn is_path_like(s: &str) -> bool {
    s.contains('/')
        || s.contains(std::path::MAIN_SEPARATOR)
        || s.ends_with(".zst")
        || s.ends_with(".zip")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_grammar_matches_nextest() {
        assert_eq!("latest".parse(), Ok(RunSelector::Latest));
        assert_eq!(
            "ztest-run-1234".parse(),
            Ok(RunSelector::Id("ztest-run-1234".into()))
        );
        assert_eq!("b0b".parse(), Ok(RunSelector::Id("b0b".into())));
        assert_eq!(
            "/tmp/rec".parse(),
            Ok(RunSelector::Path("/tmp/rec".into()))
        );
        assert_eq!(
            "./rec".parse(),
            Ok(RunSelector::Path("./rec".into()))
        );
    }

    #[test]
    fn recorded_event_roundtrips_through_json() {
        let ev = RecordedEvent::TestFinished {
            binary_id: "pkg::bin".into(),
            test_name: "mod::t".into(),
            verdict: Verdict::Fail(101),
            duration: Duration::from_millis(234),
            attempt: 2,
            output: StoreRef::Full {
                name: "deadbeef-combined".into(),
            },
        };
        let line = serde_json::to_string(&ev).unwrap();
        assert!(line.contains("\"kind\":\"test-finished\""), "{line}");
        let back: RecordedEvent = serde_json::from_str(&line).unwrap();
        match back {
            RecordedEvent::TestFinished {
                verdict, attempt, ..
            } => {
                assert_eq!(verdict, Verdict::Fail(101));
                assert_eq!(attempt, 2);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
