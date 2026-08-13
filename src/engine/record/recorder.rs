//! Recording writer.
//!
//! - [`RunRecorder`]: [`TestEvent`] → owned [`RecordedEvent`], one JSON line per
//!   event into the zstd log (output spilled to [`OutputStore`], left as a [`StoreRef`])
//! - [`RecordingReporter`]: [`RunReporter`] decorator, records then forwards (run loop unchanged)

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use zstd::stream::write::AutoFinishEncoder;

use super::store::OutputStore;
use super::{RecordedEvent, RunMeta, StoreRef};
use crate::engine::events::{RunReporter, TestEvent};

/// Per-test output cap before head/tail truncation
const MAX_OUTPUT: usize = 4 * 1024 * 1024;

/// One run's recording: zstd JSON-Lines event log + content-addressed output store.
/// Best-effort (write error disables recording, never fails the run — see [`RecordingReporter`])
pub struct RunRecorder {
    log: AutoFinishEncoder<'static, BufWriter<File>>,
    store: OutputStore,
    seen: HashSet<String>,
    max_output: usize,
}

impl std::fmt::Debug for RunRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunRecorder")
            .field("store", &self.store)
            .field("blobs_written", &self.seen.len())
            .field("max_output", &self.max_output)
            .finish_non_exhaustive()
    }
}

impl RunRecorder {
    /// `meta.json` + output store + event-log encoder under `run_dir`.
    /// Encoder auto-finishes on drop (dropping [`RecordingReporter`] finalizes)
    pub fn create(run_dir: &Path, meta: &RunMeta) -> io::Result<Self> {
        fs::create_dir_all(run_dir)?;
        fs::write(run_dir.join("meta.json"), serde_json::to_vec_pretty(meta)?)?;
        let store = OutputStore::create(run_dir)?;
        let file = File::create(run_dir.join("run.log.zst"))?;
        let log =
            zstd::stream::write::Encoder::new(BufWriter::new(file), ZSTD_LEVEL)?.auto_finish();
        Ok(Self { log, store, seen: HashSet::new(), max_output: MAX_OUTPUT })
    }

    pub fn record(&mut self, ev: &TestEvent<'_>) -> io::Result<()> {
        let rec = self.build_recorded(ev)?;
        serde_json::to_writer(&mut self.log, &rec)?;
        self.log.write_all(b"\n")?;
        Ok(())
    }

    /// `TestFinished` = only event touching the store (its bytes → [`StoreRef`])
    fn build_recorded(&mut self, ev: &TestEvent<'_>) -> io::Result<RecordedEvent> {
        Ok(match *ev {
            TestEvent::RunStarted { total, run_id } => {
                RecordedEvent::RunStarted { total, run_id: run_id.to_string() }
            }
            TestEvent::TestStarted { binary_id, test_name, class, attempt } => {
                RecordedEvent::TestStarted {
                    binary_id: binary_id.to_string(),
                    test_name: test_name.to_string(),
                    class,
                    attempt,
                }
            }
            TestEvent::TestSlow { binary_id, test_name, elapsed, will_terminate, attempt } => {
                RecordedEvent::TestSlow {
                    binary_id: binary_id.to_string(),
                    test_name: test_name.to_string(),
                    elapsed,
                    will_terminate,
                    attempt,
                }
            }
            TestEvent::TestRetrying {
                binary_id,
                test_name,
                next_attempt,
                delay,
                ref verdict,
                duration,
            } => RecordedEvent::TestRetrying {
                binary_id: binary_id.to_string(),
                test_name: test_name.to_string(),
                next_attempt,
                delay,
                verdict: verdict.clone(),
                duration,
            },
            TestEvent::TestFinished {
                binary_id,
                test_name,
                ref verdict,
                duration,
                attempt,
                output,
            } => {
                let stored: StoreRef = self.store.put(&mut self.seen, output, self.max_output)?;
                RecordedEvent::TestFinished {
                    binary_id: binary_id.to_string(),
                    test_name: test_name.to_string(),
                    verdict: verdict.clone(),
                    duration,
                    attempt,
                    output: stored,
                }
            }
            TestEvent::TestSkipped { binary_id, test_name, ref reason } => {
                RecordedEvent::TestSkipped {
                    binary_id: binary_id.to_string(),
                    test_name: test_name.to_string(),
                    reason: reason.clone(),
                }
            }
            TestEvent::RunCancelling { reason, running } => {
                RecordedEvent::RunCancelling { reason, running }
            }
            TestEvent::RunFinished { stats, elapsed } => {
                RecordedEvent::RunFinished { stats, elapsed }
            }
        })
    }
}

/// nextest's default
const ZSTD_LEVEL: i32 = 3;

/// [`RunReporter`] decorator: record to [`RunRecorder`], then forward.
///
/// - First write error → `tracing::warn` (never the TTY panel) + recording off for the run
pub struct RecordingReporter {
    inner: Box<dyn RunReporter>,
    recorder: Option<RunRecorder>,
}

impl std::fmt::Debug for RecordingReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingReporter")
            .field("recording", &self.recorder.is_some())
            .finish_non_exhaustive()
    }
}

impl RecordingReporter {
    pub fn new(inner: Box<dyn RunReporter>, recorder: RunRecorder) -> Self {
        Self { inner, recorder: Some(recorder) }
    }
}

impl RunReporter for RecordingReporter {
    fn handle(&mut self, ev: &TestEvent<'_>) {
        if let Some(rec) = self.recorder.as_mut()
            && let Err(e) = rec.record(ev)
        {
            tracing::warn!("ztest: recording disabled after write error: {e}");
            self.recorder = None;
        }
        self.inner.handle(ev);
    }

    fn take_scrollback(&mut self) -> Vec<u8> {
        self.inner.take_scrollback()
    }
}
