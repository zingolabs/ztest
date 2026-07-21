//! ztest's own diagnostics channel: a `tracing` subscriber, distinct from the
//! per-test captured output the reporter replays.
//!
//! This mirrors cargo-nextest, which routes its internal diagnostics through
//! `tracing` to stderr gated by the `NEXTEST_LOG` env var (a
//! `tracing_subscriber` target-filter string), entirely separate from the test
//! processes' own stdout/stderr. ztest uses `ZTEST_LOG` the same way.
//!
//! Two facts shape where the output goes:
//!   * In a test **pod** (`ZTEST_ENGINE=1`), the test process's stdout is
//!     captured as the test's output. Diagnostics emitted there (e.g. the
//!     `ztest::build` phase timings in [`env`](crate::env)) must go to stdout so
//!     they ride the normal capture/replay path — see [`init_in_pod`].
//!   * On the **laptop**, a TTY run hands the terminal to the console render
//!     thread; writing diagnostics to stderr would tear the pinned panel. So a
//!     TTY run routes them to a per-run file ([`Sink::File`]); a non-TTY run
//!     (CI/piped) writes to stderr like nextest ([`Sink::Stderr`]).
//!
//! Useful target filters (`ZTEST_LOG=…`): `ztest::pod=info` (runner-pod
//! lifecycle timing), `ztest::build=info` (in-pod `TestEnv::build` phase
//! timing), `ztest=debug` (everything ztest).

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Once};

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

/// Where the diagnostics subscriber writes.
#[derive(Debug, Clone)]
pub enum Sink {
    /// Standard error (non-TTY laptop runs; the nextest default).
    Stderr,
    /// Standard output (in-pod test processes, so diagnostics are captured as
    /// the test's output and replayed by the reporter).
    Stdout,
    /// A file, so a TTY run's pinned console panel is never corrupted.
    File(PathBuf),
}

static INIT: Once = Once::new();

/// The default target filter when `ZTEST_LOG` is unset or empty: quiet the noisy
/// transitive crates (kube/hyper/tower at info), keep ztest's own diagnostics at
/// info. `ZTEST_LOG` overrides it wholesale, exactly as `NEXTEST_LOG` does.
const DEFAULT_FILTER: &str = "warn,ztest=info";

fn env_filter() -> EnvFilter {
    match std::env::var("ZTEST_LOG") {
        Ok(v) if !v.trim().is_empty() => EnvFilter::builder().parse_lossy(v),
        _ => EnvFilter::builder().parse_lossy(DEFAULT_FILTER),
    }
}

/// Install the global subscriber writing to `sink`. Idempotent: the first call
/// wins and later calls (or a second entry point in the same process) are no-ops,
/// so callers need not coordinate.
pub fn init(sink: Sink) {
    INIT.call_once(|| install(sink));
}

/// Install the in-pod subscriber (stdout) when running as an orchestrated test
/// child (`ZTEST_ENGINE=1`). A no-op outside a pod so a plain `cargo test`
/// invocation is unaffected. Call early in the test-side entry
/// ([`TestEnv::build`](crate::env)).
pub fn init_in_pod() {
    if std::env::var_os("ZTEST_ENGINE").is_some() {
        init(Sink::Stdout);
    }
}

fn install(sink: Sink) {
    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter())
        .with_target(true)
        .with_level(true);
    // `try_init` (not `init`) so a stray subscriber already installed by a host
    // (e.g. a test harness) doesn't panic the process; our diagnostics are
    // best-effort.
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
            // No file (e.g. unwritable target dir) → fall back to stderr rather
            // than lose diagnostics entirely; on a TTY this is imperfect but
            // strictly better than silence, and the path is chosen under a dir
            // we just created.
            Err(_) => {
                let _ = builder.with_writer(std::io::stderr).try_init();
            }
        },
    }
}

fn open_append(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

/// A [`MakeWriter`] over a shared file handle. tracing-subscriber has no
/// built-in file writer without the `tracing-appender` crate; this is the
/// minimal thread-safe equivalent (a `Mutex` around the `File`, cloned per
/// write span).
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
