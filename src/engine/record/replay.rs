//! Reading a recording back and re-rendering it. [`replay`] streams the zstd
//! event log, resolves each `TestFinished`'s [`StoreRef`](super::StoreRef) from
//! the output store, reconstructs the borrowed [`TestEvent`] each line stood
//! for, and feeds it to a fresh [`StyledReporter`] — the same renderer the live
//! run used, so replayed output is byte-identical to the original (modulo any
//! reporter options overridden for the replay).

use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::process::ExitCode;

use nextest_metadata::NextestExitCode;

use super::RecordedEvent;
use super::store::OutputStore;
use crate::engine::events::{RunReporter, RunStats, TestEvent};
use crate::engine::output::OutputConfig;
use crate::engine::reporter::StyledReporter;

/// Re-render the recording at `run_dir` to stdout.
///
/// `output`/`color`/`unicode` configure the reporter exactly as a live run
/// would. `exit_code` selects nextest's semantics: off (the default) exits 0 if
/// the replay itself succeeds; on, it mirrors the original run's outcome.
pub fn replay(
    run_dir: &Path,
    output: OutputConfig,
    color: bool,
    unicode: bool,
    exit_code: bool,
) -> io::Result<ExitCode> {
    let stdout = io::stdout();
    let mut out = stdout.lock();
    replay_into(run_dir, output, color, unicode, exit_code, &mut out)
}

/// [`replay`], rendering to an arbitrary sink. The CLI passes stdout; tests pass
/// a buffer so the whole record→read→render path is exercised hermetically.
pub fn replay_into<W: Write>(
    run_dir: &Path,
    output: OutputConfig,
    color: bool,
    unicode: bool,
    exit_code: bool,
    out: &mut W,
) -> io::Result<ExitCode> {
    // Whether the original run captured output. When it didn't (`--no-capture`),
    // a finished test has no stored bytes, and replay shows `(output not
    // captured)` where the output block would be — as nextest does. Missing meta
    // (an older recording) is treated as captured.
    let captured = super::read_meta(run_dir)
        .map(|m| m.captured)
        .unwrap_or(true);
    let store = OutputStore::open(run_dir);
    let log = File::open(run_dir.join("run.log.zst")).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("{}: {e}", run_dir.join("run.log.zst").display()),
        )
    })?;
    let decoder = zstd::stream::read::Decoder::new(BufReader::new(log))?;
    let lines = BufReader::new(decoder).lines();

    let mut reporter = StyledReporter::new(color, unicode, output);
    let mut final_stats: Option<RunStats> = None;

    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: RecordedEvent = serde_json::from_str(&line).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("corrupt event: {e}"))
        })?;
        if let RecordedEvent::RunFinished { stats, .. } = &rec {
            final_stats = Some(*stats);
        }
        feed(&mut reporter, &store, captured, rec)?;
        let scrollback = reporter.take_scrollback();
        if !scrollback.is_empty() {
            out.write_all(&scrollback)?;
        }
    }
    out.flush()?;

    Ok(ExitCode::from(resolve_exit(exit_code, final_stats) as u8))
}

/// Reconstruct the borrowed [`TestEvent`] a recorded line stood for and hand it
/// to the reporter. Owned locals back the event's borrowed fields for the
/// duration of the `handle` call.
fn feed(
    reporter: &mut StyledReporter,
    store: &OutputStore,
    captured: bool,
    rec: RecordedEvent,
) -> io::Result<()> {
    match rec {
        RecordedEvent::RunStarted { total, run_id } => {
            reporter.handle(&TestEvent::RunStarted {
                total,
                run_id: &run_id,
            });
        }
        RecordedEvent::TestStarted {
            binary_id,
            test_name,
            class,
            attempt,
        } => reporter.handle(&TestEvent::TestStarted {
            binary_id: &binary_id,
            test_name: &test_name,
            class,
            attempt,
        }),
        RecordedEvent::TestSlow {
            binary_id,
            test_name,
            elapsed,
            will_terminate,
            attempt,
        } => reporter.handle(&TestEvent::TestSlow {
            binary_id: &binary_id,
            test_name: &test_name,
            elapsed,
            will_terminate,
            attempt,
        }),
        RecordedEvent::TestRetrying {
            binary_id,
            test_name,
            next_attempt,
            delay,
            verdict,
            duration,
        } => reporter.handle(&TestEvent::TestRetrying {
            binary_id: &binary_id,
            test_name: &test_name,
            next_attempt,
            delay,
            verdict,
            duration,
        }),
        RecordedEvent::TestFinished {
            binary_id,
            test_name,
            verdict,
            duration,
            attempt,
            output,
        } => {
            let mut bytes = store.get(&output)?;
            // A finished test with no stored bytes under a non-capturing run
            // means output was never captured; surface that rather than blank
            // output (the reporter still gates on the display policy).
            if bytes.is_empty() && !captured {
                bytes = b"(output not captured)".to_vec();
            }
            reporter.handle(&TestEvent::TestFinished {
                binary_id: &binary_id,
                test_name: &test_name,
                verdict,
                duration,
                attempt,
                output: &bytes,
            });
        }
        RecordedEvent::TestSkipped {
            binary_id,
            test_name,
            reason,
        } => reporter.handle(&TestEvent::TestSkipped {
            binary_id: &binary_id,
            test_name: &test_name,
            reason,
        }),
        RecordedEvent::RunCancelling { reason, running } => {
            reporter.handle(&TestEvent::RunCancelling { reason, running })
        }
        RecordedEvent::RunFinished { stats, elapsed } => {
            reporter.handle(&TestEvent::RunFinished { stats, elapsed })
        }
    }
    Ok(())
}

/// The replay's exit code: [`OK`](NextestExitCode::OK) unless `--exit-code` was
/// given, in which case mirror the original run's outcome from its final stats
/// (the same mapping [`engine::run`](crate::engine::run) applies live).
fn resolve_exit(mirror_original: bool, stats: Option<RunStats>) -> i32 {
    if !mirror_original {
        return NextestExitCode::OK;
    }
    match stats {
        Some(s) if s.any_failed() => NextestExitCode::TEST_RUN_FAILED,
        Some(s) if s.skipped > 0 => NextestExitCode::SETUP_ERROR,
        _ => NextestExitCode::OK,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::events::{TestEvent, Verdict};
    use crate::engine::record::recorder::RunRecorder;
    use crate::engine::record::{FORMAT_VERSION, RunMeta};
    use crate::qos::QosClass;
    use std::time::Duration;

    /// The whole pipeline end-to-end, no cluster: record a run to a temp dir,
    /// then replay it and assert the reporter reproduces the live output —
    /// verdict lines, the replayed failure output, and the summary.
    #[test]
    fn record_then_replay_reproduces_the_run() {
        let dir = tempdir("replay-e2e");
        {
            let mut rec = RunRecorder::create(&dir, &meta("ztest-run-e2e")).unwrap();
            rec.record(&TestEvent::RunStarted {
                total: 2,
                run_id: "ztest-run-e2e",
            })
            .unwrap();
            rec.record(&TestEvent::TestStarted {
                binary_id: "pkg::bin",
                test_name: "mod::ok",
                class: QosClass::Integration,
                attempt: 1,
            })
            .unwrap();
            rec.record(&TestEvent::TestFinished {
                binary_id: "pkg::bin",
                test_name: "mod::ok",
                verdict: Verdict::Pass,
                duration: Duration::from_millis(100),
                attempt: 1,
                output: b"",
            })
            .unwrap();
            rec.record(&TestEvent::TestFinished {
                binary_id: "pkg::bin",
                test_name: "mod::boom",
                verdict: Verdict::Fail(101),
                duration: Duration::from_millis(200),
                attempt: 1,
                output: b"panic: boom\n",
            })
            .unwrap();
            rec.record(&TestEvent::RunFinished {
                stats: RunStats {
                    passed: 1,
                    failed: 1,
                    terminated: 0,
                    skipped: 0,
                    total: 2,
                },
                elapsed: Duration::from_secs(1),
            })
            .unwrap();
            // `rec` drops here → the zstd log frame is finalized.
        }

        let mut buf: Vec<u8> = Vec::new();
        replay_into(&dir, OutputConfig::default(), false, true, false, &mut buf).unwrap();
        let out = String::from_utf8(buf).unwrap();

        assert!(out.contains("Starting 2 tests"), "{out}");
        assert!(out.contains("PASS [   0.100s] pkg::bin mod::ok"), "{out}");
        assert!(out.contains("FAIL [   0.200s] pkg::bin mod::boom"), "{out}");
        assert!(
            out.contains("panic: boom"),
            "failure output must replay:\n{out}"
        );
        assert!(
            out.contains("Summary [   1.000s] 2 tests run: 1 passed, 1 failed, 0 skipped"),
            "{out}"
        );
    }

    #[test]
    fn exit_code_flag_mirrors_original_outcome() {
        let failed = Some(RunStats {
            passed: 0,
            failed: 1,
            terminated: 0,
            skipped: 0,
            total: 1,
        });
        let skipped = Some(RunStats {
            passed: 0,
            failed: 0,
            terminated: 0,
            skipped: 1,
            total: 1,
        });
        // Without --exit-code, replay always reports success.
        assert_eq!(resolve_exit(false, failed), NextestExitCode::OK);
        // With it, the original outcome is mirrored.
        assert_eq!(resolve_exit(true, failed), NextestExitCode::TEST_RUN_FAILED);
        assert_eq!(resolve_exit(true, skipped), NextestExitCode::SETUP_ERROR);
        assert_eq!(resolve_exit(true, None), NextestExitCode::OK);
    }

    fn meta(run_id: &str) -> RunMeta {
        RunMeta {
            format_version: FORMAT_VERSION,
            run_id: run_id.to_string(),
            started_at: "2026-07-29T00:00:00Z".to_string(),
            args: vec!["ztest".into(), "run".into()],
            captured: true,
        }
    }

    fn tempdir(tag: &str) -> std::path::PathBuf {
        let base = std::env::temp_dir().join(format!(
            "ztest-{tag}-{}-{}",
            std::process::id(),
            blake3::hash(format!("{:?}", std::thread::current().id()).as_bytes()).to_hex()
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }
}
