//! ztest diagnostics channel: a `tracing` subscriber, separate from the per-test
//! captured output the reporter replays. `ZTEST_LOG` = nextest's `NEXTEST_LOG`.
//!
//! Sink choice:
//! - In-pod (`ZTEST_ENGINE=1`) → stdout, so diagnostics ride the capture/replay
//!   path ([`init_in_pod`])
//! - TTY laptop → per-run file ([`Sink::File`]); stderr would tear the pinned panel
//! - Non-TTY (CI/piped) → stderr ([`Sink::Stderr`])
//!
//! Filters: `ztest::pod=debug` (runner-pod lifecycle), `ztest::build=debug`
//! (in-pod `TestEnv::build` phases), `ztest=debug` (all).

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Once};

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

#[derive(Debug, Clone)]
pub enum Sink {
    Stderr,
    Stdout,
    File(PathBuf),
}

static INIT: Once = Once::new();

/// Filter when `ZTEST_LOG` unset/empty (overridden wholesale when set)
const DEFAULT_FILTER: &str = "warn,ztest=info";

fn env_filter() -> EnvFilter {
    match std::env::var("ZTEST_LOG") {
        Ok(v) if !v.trim().is_empty() => EnvFilter::builder().parse_lossy(v),
        _ => EnvFilter::builder().parse_lossy(DEFAULT_FILTER),
    }
}

/// Install the global subscriber over `sink`. Idempotent: first call wins, later
/// ones no-op (callers need not coordinate)
pub fn init(sink: Sink) {
    INIT.call_once(|| install(sink));
}

/// Install the in-pod (stdout) subscriber under `ZTEST_ENGINE=1`; no-op elsewhere
/// (plain `cargo test` unaffected). Call early from
/// [`TestEnv::build`](crate::env)
pub fn init_in_pod() {
    if std::env::var_os("ZTEST_ENGINE").is_some() {
        init(Sink::Stdout);
    }
}

fn install(sink: Sink) {
    let builder =
        tracing_subscriber::fmt().with_env_filter(env_filter()).with_target(true).with_level(true);
    // try_init, not init: a host-installed subscriber must not panic us (diagnostics = best-effort)
    match sink {
        Sink::Stderr => {
            let _ = builder.with_writer(std::io::stderr).try_init();
        }
        Sink::Stdout => {
            let _ = builder.with_writer(std::io::stdout).try_init();
        }
        Sink::File(path) => match open_append(&path) {
            Ok(file) => {
                let _ = builder
                    .with_ansi(false)
                    .with_writer(FileMaker(Arc::new(Mutex::new(file))))
                    .try_init();
            }
            // No file → drop to a null writer, never stderr (Sink::File exists to
            // keep output off the render thread's terminal)
            Err(_) => {
                let _ = builder.with_writer(std::io::sink).try_init();
            }
        },
    }
}

fn open_append(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::OpenOptions::new().create(true).append(true).open(path)
}

/// [`MakeWriter`] over a shared file handle (tracing-subscriber ships none
/// without `tracing-appender`)
#[derive(Clone)]
struct FileMaker(Arc<Mutex<std::fs::File>>);

impl<'a> MakeWriter<'a> for FileMaker {
    type Writer = FileHandle;
    fn make_writer(&'a self) -> Self::Writer {
        FileHandle(self.0.clone())
    }
}

struct FileHandle(Arc<Mutex<std::fs::File>>);

impl Write for FileHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log file mutex poisoned").write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().expect("log file mutex poisoned").flush()
    }
}
