//! Spawn one test as its own OS process and capture its result.
//!
//! Mirrors nextest's process-per-test model: run the test binary with
//! `--exact <name> --nocapture`, pipe and capture stdout+stderr, enforce a hard
//! kill deadline, and run the child in its own process group so the kill reaches
//! the pods/port-forwards a ztest test spawns (a bare `child.kill()` would leak
//! them, same reasoning as the Ctrl-C path in `cli/console/pty.rs`).
//!
//! The soft "slow" signal is emitted by the run loop on its render tick (it owns
//! per-test start times), so this module only handles the hard cap.

use std::ffi::OsString;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;

use crate::cancel::Cancel;
use crate::engine::events::Verdict;
use crate::engine::plan::WorkItem;

/// A `'static` boxed future of one test's outcome — the unit the run loop
/// awaits, independent of how the test was executed.
pub type OutcomeFuture = Pin<Box<dyn Future<Output = TestOutcome> + Send + 'static>>;

/// How a single test is executed. The run loop is agnostic to this: it only
/// needs a [`TestOutcome`] back. [`LocalExecutor`] forks a child process (the
/// default, and the only executor on local/kind runs); a remote executor runs
/// the test in a sibling pod so both the compute and the test are hermetic.
pub trait Executor: Send + Sync + 'static {
    fn run(&self, item: WorkItem, cancel: Cancel) -> OutcomeFuture;
}

/// Executes each test as a local OS child process via [`spawn_test`].
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
#[derive(Debug, Clone)]
pub struct EngineEnv {
    /// The full dynamic-library search path value (see [`super::dylib`]).
    pub dylib_path: OsString,
    /// `NEXTEST_RUN_ID` shared across all children.
    pub run_id: String,
    /// `ZTEST_SA` — the ServiceAccount the run charges against.
    pub sa: String,
    /// Whether `ZTEST_NO_CLEANUP=1` should be set.
    pub no_cleanup: bool,
}

/// The outcome of one test process.
#[derive(Debug, Clone)]
pub struct TestOutcome {
    /// Terminal verdict.
    pub verdict: Verdict,
    /// Merged stdout+stderr captured from the child.
    pub output: Vec<u8>,
    /// Wall time the process ran.
    pub duration: Duration,
}

/// Run a single test to completion (or hard-cap kill), capturing its output.
///
/// `hard_cap` is the kill deadline (the tier's hard cap); on hitting it the
/// child's process group is SIGKILLed and the verdict is [`Verdict::Timeout`].
/// If `cancel` fires (run-wide Ctrl-C) while the test is in flight, the group is
/// SIGKILLed and the verdict is [`Verdict::Terminated`], so the run loop can
/// drain and report every in-flight test rather than dropping it silently.
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

    // Read stdout+stderr concurrently so a full pipe buffer can't deadlock the
    // child before it exits.
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

    // Both readers complete once the pipes hit EOF: the child and all its
    // fd-inheriting children have exited or been killed.
    let (mut out, mut err) = tokio::join!(read_out, read_err);
    out.append(&mut err);

    TestOutcome {
        verdict,
        output: out,
        duration: started.elapsed(),
    }
}

/// Build the `tokio` command: argv, cwd, stdio, env, and (Unix) a dedicated
/// process group so the whole tree can be killed at the hard cap.
fn build_command(item: &WorkItem, env: &EngineEnv) -> tokio::process::Command {
    let mut std_cmd = std::process::Command::new(&item.binary_path);
    std_cmd
        .arg("--exact")
        .arg(&item.test_name)
        .arg("--nocapture")
        .current_dir(&item.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Dynamic-library path (the libstdc++-exit-127 fix).
        .env(super::dylib::dylib_path_envvar(), &env.dylib_path)
        // nextest-compat vars some ecosystem crates read.
        .env("NEXTEST", "1")
        .env("NEXTEST_EXECUTION_MODE", "process-per-test")
        .env("NEXTEST_RUN_ID", &env.run_id)
        .env("CARGO_MANIFEST_DIR", &item.cwd)
        // Mark the child as orchestrated: the parent owns capacity admission,
        // and a `TestEnv` refuses to provision outside a `ztest run`
        // (cluster::require_orchestrator).
        .env("ZTEST_ENGINE", "1")
        .env("ZTEST_SA", &env.sa);
    if env.no_cleanup {
        std_cmd.env("ZTEST_NO_CLEANUP", "1");
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        // New process group (pgid == child pid) so `kill(-pgid, ...)` reaches the
        // test's spawned helpers.
        std_cmd.process_group(0);
    }

    let mut cmd = tokio::process::Command::from(std_cmd);
    cmd.kill_on_drop(true);
    cmd
}

/// SIGKILL the child's process group. No-op if the pid is gone.
#[cfg(unix)]
fn kill_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        // Negative pid targets the process group (matches `pty.rs`).
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_group(_pid: Option<u32>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::QosClass;
    use std::path::PathBuf;
    use std::sync::OnceLock;

    /// Serialize the process-spawning tests. They write-then-exec temp scripts;
    /// running them concurrently lets one test's `fork` inherit another's open
    /// write-fd, producing a transient `ETXTBSY` (spurious `SpawnError`).
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

    /// Write an executable `#!/bin/sh` script that ignores its argv (so the
    /// fixed `--exact ... --nocapture` don't matter). Uses `/bin/sh`, which exists
    /// even where coreutils `/bin/true`/`/bin/false` don't (e.g. NixOS).
    #[cfg(unix)]
    fn script(tag: &str, body: &str) -> PathBuf {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;
        let path = std::env::temp_dir().join(format!("ztest-{tag}-{}.sh", std::process::id()));
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, "#!/bin/sh\n{body}\n").unwrap();
        let mut perms = f.metadata().unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();
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

    /// Children must run marked as orchestrated so `TestEnv::build` proceeds
    /// (a bare test binary is refused by `cluster::require_orchestrator`). If
    /// `ZTEST_ENGINE` regressed or got dropped, every cluster test would fail
    /// fast, so assert the child actually sees `ZTEST_ENGINE=1`.
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

        // A script that ignores its argv (so the fixed `--exact ... --nocapture`
        // don't matter) and sleeps far past the cap.
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
