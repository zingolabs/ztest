//! Writing a recording: [`RunRecorder`] is the sink that converts each live
//! [`TestEvent`] to its owned [`RecordedEvent`] form — spilling captured output
//! into the [`OutputStore`] and replacing it with a [`StoreRef`] — then appends
//! it as one JSON line to the zstd event log. [`RecordingReporter`] wires that
//! sink into the run loop as a [`RunReporter`] decorator: it records each event,
//! then forwards it to the real reporter, so nothing in the loop changes.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use zstd::stream::write::AutoFinishEncoder;

use super::store::OutputStore;
use super::{RecordedEvent, RunMeta, StoreRef};
use crate::engine::events::{RunReporter, TestEvent};

/// The per-test captured-output cap before head/tail truncation kicks in (4 MiB).
const MAX_OUTPUT: usize = 4 * 1024 * 1024;

/// Writes one run's recording: the zstd JSON-Lines event log plus the
/// content-addressed output store. Best-effort — a write error disables further
/// recording rather than failing the run (see [`RecordingReporter`]).
pub struct RunRecorder {
    log: AutoFinishEncoder<'static, BufWriter<File>>,
    store: OutputStore,
    /// Blobs already written this run, so identical output isn't rewritten.
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
    /// Create a recording under `run_dir`: write `meta.json`, make the output
    /// store, and open the event-log encoder. The encoder auto-finishes on drop,
    /// so a recording is finalized simply by dropping the [`RecordingReporter`].
    pub fn create(run_dir: &Path, meta: &RunMeta) -> io::Result<Self> {
        fs::create_dir_all(run_dir)?;
        fs::write(run_dir.join("meta.json"), serde_json::to_vec_pretty(meta)?)?;
        let store = OutputStore::create(run_dir)?;
        let file = File::create(run_dir.join("run.log.zst"))?;
        let log =
            zstd::stream::write::Encoder::new(BufWriter::new(file), ZSTD_LEVEL)?.auto_finish();
        Ok(Self {
            log,
            store,
            seen: HashSet::new(),
            max_output: MAX_OUTPUT,
        })
    }

    /// Record one event: convert to the owned form (storing output for
    /// `TestFinished`) and append it as a JSON line.
    pub fn record(&mut self, ev: &TestEvent<'_>) -> io::Result<()> {
        let rec = self.build_recorded(ev)?;
        serde_json::to_writer(&mut self.log, &rec)?;
        self.log.write_all(b"\n")?;
        Ok(())
    }

    /// Convert a borrowed live event to its owned recorded form. The only event
    /// that touches the store is `TestFinished`, whose output bytes become a
    /// [`StoreRef`].
    fn build_recorded(&mut self, ev: &TestEvent<'_>) -> io::Result<RecordedEvent> {
        Ok(match *ev {
            TestEvent::RunStarted { total, run_id } => RecordedEvent::RunStarted {
                total,
                run_id: run_id.to_string(),
            },
            TestEvent::TestStarted {
                binary_id,
                test_name,
                class,
                attempt,
            } => RecordedEvent::TestStarted {
                binary_id: binary_id.to_string(),
                test_name: test_name.to_string(),
                class,
                attempt,
            },
            TestEvent::TestSlow {
                binary_id,
                test_name,
                elapsed,
                will_terminate,
                attempt,
            } => RecordedEvent::TestSlow {
                binary_id: binary_id.to_string(),
                test_name: test_name.to_string(),
                elapsed,
                will_terminate,
                attempt,
            },
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
            TestEvent::TestSkipped {
                binary_id,
                test_name,
                ref reason,
            } => RecordedEvent::TestSkipped {
                binary_id: binary_id.to_string(),
                test_name: test_name.to_string(),
                reason: reason.clone(),
            },
            TestEvent::RunCancelling { reason, running } => {
                RecordedEvent::RunCancelling { reason, running }
            }
            TestEvent::RunFinished { stats, elapsed } => {
                RecordedEvent::RunFinished { stats, elapsed }
            }
        })
    }
}

/// zstd level for the event log — level 3, nextest's default.
const ZSTD_LEVEL: i32 = 3;

/// A [`RunReporter`] that records each event to a [`RunRecorder`] before
/// forwarding it to the wrapped reporter. This is the whole wiring: the run loop
/// still sees one `&mut dyn RunReporter`.
///
/// Recording is best-effort — the first write error is logged (via `tracing`, so
/// it never corrupts a live TTY panel) and recording is disabled for the rest of
/// the run; rendering continues untouched.
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
    /// Wrap `inner`, recording into `recorder`.
    pub fn new(inner: Box<dyn RunReporter>, recorder: RunRecorder) -> Self {
        Self {
            inner,
            recorder: Some(recorder),
        }
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
