//! `ztest run -R/--rerun <SELECTOR>`: re-execute what did not pass in a prior
//! recorded run.
//!
//! - Recording contributes one thing: the *passed* identity set
//! - Run the complement against the fresh test list → failed + never-completed +
//!   new-since, with no persisted test list (nextest's default)

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

use super::RecordedEvent;
use crate::engine::events::Verdict;

pub type TestId = (String, String);

/// Identities with a passing terminal verdict in the recording at `run_dir`.
///
/// - One entry per test (retries emit `TestRetrying`; only the terminal outcome
///   emits `TestFinished`) → an eventually-passing flake counts as passed
pub fn passed_tests(run_dir: &Path) -> io::Result<HashSet<TestId>> {
    let log = File::open(run_dir.join("run.log.zst"))?;
    let decoder = zstd::stream::read::Decoder::new(BufReader::new(log))?;
    let mut passed = HashSet::new();
    for line in BufReader::new(decoder).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: RecordedEvent = serde_json::from_str(&line).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("corrupt event: {e}"))
        })?;
        if let RecordedEvent::TestFinished {
            binary_id, test_name, verdict: Verdict::Pass, ..
        } = rec
        {
            passed.insert((binary_id, test_name));
        }
    }
    Ok(passed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::events::TestEvent;
    use crate::engine::record::recorder::RunRecorder;
    use crate::engine::record::{FORMAT_VERSION, RunMeta};
    use crate::qos::QosClass;
    use std::time::Duration;

    #[test]
    fn collects_only_passing_identities() {
        let dir = std::env::temp_dir().join(format!(
            "ztest-rerun-test-{}-{}",
            std::process::id(),
            blake3::hash(format!("{:?}", std::thread::current().id()).as_bytes()).to_hex()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let meta = RunMeta {
            format_version: FORMAT_VERSION,
            run_id: "r".into(),
            started_at: "t".into(),
            args: vec![],
            captured: true,
        };
        {
            let mut rec = RunRecorder::create(&dir, &meta).unwrap();
            rec.record(&TestEvent::TestStarted {
                binary_id: "pkg::bin",
                test_name: "ok",
                class: QosClass::Integration,
                attempt: 1,
            })
            .unwrap();
            rec.record(&TestEvent::TestFinished {
                binary_id: "pkg::bin",
                test_name: "ok",
                verdict: Verdict::Pass,
                duration: Duration::from_millis(1),
                attempt: 1,
                output: b"",
            })
            .unwrap();
            rec.record(&TestEvent::TestFinished {
                binary_id: "pkg::bin",
                test_name: "boom",
                verdict: Verdict::Fail(1),
                duration: Duration::from_millis(1),
                attempt: 1,
                output: b"",
            })
            .unwrap();
            // `parked` never finishes → absent from the passed set → a rerun runs it
        }
        let passed = passed_tests(&dir).unwrap();
        assert!(passed.contains(&("pkg::bin".into(), "ok".into())));
        assert!(!passed.contains(&("pkg::bin".into(), "boom".into())));
        assert!(!passed.contains(&("pkg::bin".into(), "parked".into())));
        assert_eq!(passed.len(), 1);
    }
}
