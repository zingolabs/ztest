//! Spawn one test as its own OS process (nextest's process-per-test model), capture the
//! result.
//!
//! - Own process group → a hard-cap kill reaches the pods/port-forwards it spawned (a
//!   bare `child.kill()` leaks them)
//! - Hard cap only; the run loop owns the soft "slow" signal

use std::ffi::OsString;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;

use crate::cancel::Cancel;
use crate::engine::events::Verdict;
use crate::engine::plan::WorkItem;

/// Unit the run loop awaits, independent of how the test was executed
pub type OutcomeFuture = Pin<Box<dyn Future<Output = TestOutcome> + Send + 'static>>;

/// How a single test is executed; the run loop only needs a [`TestOutcome`] back.
/// [`LocalExecutor`] forks a child (default); a remote executor runs it in a sibling pod
pub trait Executor: Send + Sync + 'static {
    fn run(&self, item: WorkItem, cancel: Cancel) -> OutcomeFuture;
}

#[derive(Debug, Clone)]
pub struct LocalExecutor {
    pub env: EngineEnv,
}

impl Executor for LocalExecutor {
    fn run(&self, item: WorkItem, cancel: Cancel) -> OutcomeFuture {
        let env = self.env.clone();
        Box::pin(async move {
            let cap = item.hard_cap;
            spawn_test(&item, &env, cap, &cancel).await
        })
    }
}

/// Per-run environment shared by every child, computed once.
///
/// - `ztest_log` forwarded verbatim → an in-pod subscriber matches the laptop's
/// - `capture = false` under `--no-capture`, where the child inherits this process's stdio
/// - `color` rides as `ZTEST_COLOR` (the child's stderr is piped, so it cannot decide)
#[derive(Debug, Clone)]
pub struct EngineEnv {
    pub dylib_path: OsString,
    pub run_id: String,
    pub sa: String,
    pub no_cleanup: bool,
    pub ztest_log: Option<String>,
    pub capture: bool,
    pub color: bool,
}

/// Outcome of one test process. `output` = merged stdout+stderr, or on the pod path the
/// laptop-assembled unified timeline (`logstream::unified_output`): runner output woven
/// with component-pod logs, panic pinned last
#[derive(Debug, Clone)]
pub struct TestOutcome {
    pub verdict: Verdict,
    pub output: Vec<u8>,
    pub duration: Duration,
}

/// Run a single test to completion, capturing its output.
///
/// - `hard_cap` → process group SIGKILLed, [`Verdict::Timeout`]
/// - `cancel` (run-wide Ctrl-C) → SIGKILLed, [`Verdict::Terminated`], so the run loop
///   still reports every in-flight test instead of dropping it
pub async fn spawn_test(
    item: &WorkItem,
    env: &EngineEnv,
    hard_cap: Duration,
    cancel: &Cancel,
) -> TestOutcome {
    let started = Instant::now();

    let mut cmd = build_command(item, env);
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(_) => {
            return TestOutcome {
                verdict: Verdict::SpawnError,
                output: Vec::new(),
                duration: started.elapsed(),
            };
        }
    };

    // stdout+stderr read concurrently → a full pipe buffer can't deadlock the child
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    let read_out = async {
        let mut buf = Vec::new();
        if let Some(s) = stdout.as_mut() {
            let _ = s.read_to_end(&mut buf).await;
        }
        buf
    };
    let read_err = async {
        let mut buf = Vec::new();
        if let Some(s) = stderr.as_mut() {
            let _ = s.read_to_end(&mut buf).await;
        }
        buf
    };

    let pid = child.id();
    let verdict = tokio::select! {
        status = child.wait() => match status {
            Ok(s) if s.success() => Verdict::Pass,
            Ok(s) => Verdict::Fail(s.code().unwrap_or(-1)),
            Err(_) => Verdict::SpawnError,
        },
        _ = tokio::time::sleep(hard_cap) => {
            kill_group(pid);
            let _ = child.wait().await;
            Verdict::Timeout
        }
        _ = cancel.cancelled() => {
            kill_group(pid);
            let _ = child.wait().await;
            Verdict::Terminated
        }
    };

    // Readers complete at pipe EOF = child and every fd-inheriting descendant gone
    let (mut out, mut err) = tokio::join!(read_out, read_err);
    out.append(&mut err);

    TestOutcome { verdict, output: out, duration: started.elapsed() }
}

/// Build the `tokio` command: argv, cwd, stdio, env, and (Unix) a dedicated process group
/// so the whole tree dies at the hard cap
fn build_command(item: &WorkItem, env: &EngineEnv) -> tokio::process::Command {
    // `--no-capture` → inherit stdio and stream live; else pipe both for reporter replay
    let (out, err) = if env.capture {
        (Stdio::piped(), Stdio::piped())
    } else {
        (Stdio::inherit(), Stdio::inherit())
    };
    let mut std_cmd = std::process::Command::new(&item.binary_path);
    std_cmd
        .arg("--exact")
        .arg(&item.test_name)
        .arg("--nocapture")
        .current_dir(&item.cwd)
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(err)
        // Dynamic-library path (the libstdc++-exit-127 fix)
        .env(super::dylib::dylib_path_envvar(), &env.dylib_path)
        .env("NEXTEST", "1")
        .env("NEXTEST_EXECUTION_MODE", "process-per-test")
        .env("NEXTEST_RUN_ID", &env.run_id)
        .env("CARGO_MANIFEST_DIR", &item.cwd)
        // Marks the child orchestrated; a `TestEnv` refuses to provision outside a
        // `ztest run` (`cluster::require_orchestrator`)
        .env("ZTEST_ENGINE", "1")
        .env("ZTEST_SA", &env.sa)
        .env("ZTEST_COLOR", if env.color { "1" } else { "0" });
    if env.no_cleanup {
        std_cmd.env("ZTEST_NO_CLEANUP", "1");
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        // New process group → `kill(-pgid, ...)` reaches the test's spawned helpers
        std_cmd.process_group(0);
    }

    let mut cmd = tokio::process::Command::from(std_cmd);
    cmd.kill_on_drop(true);
    cmd
}

/// SIGKILL the child's process group; no-op once the pid is gone
#[cfg(unix)]
fn kill_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        // Negative pid targets the process group (matches `pty.rs`)
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_group(_pid: Option<u32>) {}

/// Write `body` as an executable `#!/bin/sh` script at `path`, atomically.
///
/// A child forked by *any* thread inherits every fd open at that moment → a concurrent
/// `spawn_test` child can hold a write handle to a half-written script and executing it
/// fails `ETXTBSY`. Write-to-scratch + rename closes the window, and dropping the file
/// *before* the rename is load-bearing (rename keeps the inode, so an open handle follows)
#[cfg(all(test, unix))]
pub(super) fn write_script(path: &std::path::Path, body: &str) {
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;

    let scratch = path.with_extension("sh.partial");
    let mut f = std::fs::File::create(&scratch).unwrap();
    write!(f, "#!/bin/sh\n{body}\n").unwrap();
    let mut perms = f.metadata().unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&scratch, perms).unwrap();
    drop(f);
    std::fs::rename(&scratch, path).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::QosClass;
    use std::path::PathBuf;
    use std::sync::OnceLock;

    /// Serializes the process-spawning tests in *this* module only — `engine::e2e` spawns
    /// children too and cannot take this lock, which is why
    /// [`write_script`](super::write_script) closes the handle before the path exists
    fn serial() -> &'static tokio::sync::Mutex<()> {
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn env() -> EngineEnv {
        EngineEnv {
            dylib_path: OsString::new(),
            run_id: "run-test".into(),
            sa: "ztest-local".into(),
            no_cleanup: false,
            capture: true,
            color: false,
            ztest_log: None,
        }
    }

    fn item(bin: &str, name: &str) -> WorkItem {
        let p = QosClass::Basic.profile();
        WorkItem {
            binary_id: "t::b".into(),
            test_name: name.into(),
            binary_path: PathBuf::from(bin),
            cwd: PathBuf::from("/"),
            class: QosClass::Basic,
            footprint: p.footprint,
            priority: p.priority,
            hard_cap: p.hard_cap,
            retries: 0,
            deps: Vec::new(),
        }
    }

    /// Script ignores argv (so the fixed `--exact ... --nocapture` don't matter) and uses
    /// `/bin/sh`, which exists where `/bin/true`/`/bin/false` don't (NixOS)
    #[cfg(unix)]
    fn script(tag: &str, body: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("ztest-{tag}-{}.sh", std::process::id()));
        super::write_script(&path, body);
        path
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pass_on_zero_exit() {
        let _g = serial().lock().await;
        let p = script("pass", "exit 0");
        let out = spawn_test(
            &item(p.to_str().unwrap(), "x"),
            &env(),
            Duration::from_secs(5),
            &Cancel::never(),
        )
        .await;
        let _ = std::fs::remove_file(&p);
        assert_eq!(out.verdict, Verdict::Pass);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fail_on_nonzero_exit() {
        let _g = serial().lock().await;
        let p = script("fail", "exit 3");
        let out = spawn_test(
            &item(p.to_str().unwrap(), "x"),
            &env(),
            Duration::from_secs(5),
            &Cancel::never(),
        )
        .await;
        let _ = std::fs::remove_file(&p);
        assert_eq!(out.verdict, Verdict::Fail(3));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn captures_child_output() {
        let _g = serial().lock().await;
        let p = script("cap", "echo hello-stdout");
        let out = spawn_test(
            &item(p.to_str().unwrap(), "x"),
            &env(),
            Duration::from_secs(5),
            &Cancel::never(),
        )
        .await;
        let _ = std::fs::remove_file(&p);
        assert!(
            String::from_utf8_lossy(&out.output).contains("hello-stdout"),
            "{:?}",
            String::from_utf8_lossy(&out.output)
        );
    }

    /// A dropped `ZTEST_ENGINE` fails every cluster test fast at `require_orchestrator`
    #[cfg(unix)]
    #[tokio::test]
    async fn children_run_marked_as_orchestrated() {
        let _g = serial().lock().await;
        let p = script("engineenv", "printf 'ENGINE=[%s]\\n' \"$ZTEST_ENGINE\"");
        let out = spawn_test(
            &item(p.to_str().unwrap(), "x"),
            &env(),
            Duration::from_secs(5),
            &Cancel::never(),
        )
        .await;
        let _ = std::fs::remove_file(&p);
        assert_eq!(out.verdict, Verdict::Pass);
        assert!(
            String::from_utf8_lossy(&out.output).contains("ENGINE=[1]"),
            "children must inherit ZTEST_ENGINE=1; got {:?}",
            String::from_utf8_lossy(&out.output)
        );
    }

    #[tokio::test]
    async fn spawn_error_on_missing_binary() {
        let out = spawn_test(
            &item("/nonexistent/zzz", "x"),
            &env(),
            Duration::from_secs(5),
            &Cancel::never(),
        )
        .await;
        assert_eq!(out.verdict, Verdict::SpawnError);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_long_sleeper() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;
        let _g = serial().lock().await;

        // Sleeps far past the cap
        let path = std::env::temp_dir().join(format!("ztest-sleeper-{}.sh", std::process::id()));
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"#!/bin/sh\nsleep 30\n").unwrap();
            let mut perms = f.metadata().unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&path, perms).unwrap();
        }

        let it = item(path.to_str().unwrap(), "x");
        let out = spawn_test(&it, &env(), Duration::from_millis(300), &Cancel::never()).await;
        let _ = std::fs::remove_file(&path);

        assert_eq!(out.verdict, Verdict::Timeout);
        assert!(out.duration < Duration::from_secs(5));
    }
}
