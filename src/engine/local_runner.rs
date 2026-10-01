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

pub struct LocalExecutor {
    pub client: kube::Client,
    pub env: EngineEnv,
}

// `kube::Client` is not `Debug`
impl std::fmt::Debug for LocalExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExecutor").field("env", &self.env).finish_non_exhaustive()
    }
}

impl Executor for LocalExecutor {
    fn run(&self, item: WorkItem, cancel: Cancel) -> OutcomeFuture {
        let (client, env) = (self.client.clone(), self.env.clone());
        Box::pin(async move {
            let started = Instant::now();
            let ns = match super::test_ns::open(&client, &env.run, &item).await {
                Ok(ns) => ns,
                Err(e) => {
                    return TestOutcome {
                        verdict: Verdict::SpawnError,
                        output: e.into_bytes(),
                        components: Vec::new(),
                        duration: started.elapsed(),
                    };
                }
            };
            let out = spawn_test(&item, &env, &ns, item.hard_cap, &cancel).await;
            let harvest = super::test_ns::close(&client, &ns, env.no_cleanup).await;
            TestOutcome {
                output: crate::logstream::runner_output(
                    &out.output,
                    &item.test_name,
                    &harvest.dead,
                ),
                components: harvest.components,
                duration: started.elapsed(),
                ..out
            }
        })
    }
}

/// Per-run environment shared by every child, computed once.
///
/// - `capture = false` under `--no-capture`, where the child inherits this process's stdio
/// - `storage` = (storage class, snapshot class); `None` on the local path (child discovers)
#[derive(Debug, Clone)]
pub struct EngineEnv {
    pub dylib_path: OsString,
    pub run: crate::naming::RunCoords,
    pub no_cleanup: bool,
    pub ztest_log: Option<String>,
    pub capture: bool,
    pub image_refs: std::collections::BTreeMap<String, String>,
    pub storage: Option<(String, String)>,
}

impl EngineEnv {
    /// Vars every test process gets, local child or runner pod alike (one list → the two
    /// spawn paths cannot drift). Unresolved optionals stay absent, never empty
    pub fn test_vars(&self, test_ns: &str) -> Vec<(&'static str, String)> {
        let mut vars = vec![
            ("NEXTEST", "1".to_string()),
            ("NEXTEST_EXECUTION_MODE", "process-per-test".to_string()),
            ("NEXTEST_RUN_ID", self.run.run_id.clone()),
            (crate::naming::RUN_ID_ENV, self.run.run_id.clone()),
            ("USER", self.run.user.clone()),
            (crate::naming::TEST_NAMESPACE_ENV, test_ns.to_string()),
        ];
        if let Some(filter) = &self.ztest_log {
            vars.push(("ZTEST_LOG", filter.clone()));
        }
        // Test process has no Dockerfile/manifest → resolves `dev!` images by this map
        if !self.image_refs.is_empty() {
            let json = serde_json::to_string(&self.image_refs).expect("string map serializes");
            vars.push((crate::backends::image::IMAGE_REFS_ENV, json));
        }
        // Driver SA may not list cluster-scoped storage/snapshot classes → answer handed down
        if let Some((class, snapshot_class)) = &self.storage {
            vars.push((crate::cluster_config::STORAGE_CLASS_ENV, class.clone()));
            vars.push((crate::cluster_config::SNAPSHOT_CLASS_ENV, snapshot_class.clone()));
        }
        vars
    }
}

/// Outcome of one test process. `output` = merged stdout+stderr (pod path:
/// [`runner_output`](crate::logstream::runner_output)), `components` = full merged
/// component-pod log (tailed only at display)
#[derive(Debug, Clone)]
pub struct TestOutcome {
    pub verdict: Verdict,
    pub output: Vec<u8>,
    pub components: Vec<u8>,
    pub duration: Duration,
}

/// Run a single test process to completion, capturing its raw output (`components` empty:
/// the namespace harvest is the executor's).
///
/// - `hard_cap` → process group SIGKILLed, [`Verdict::Timeout`]
/// - `cancel` (run-wide Ctrl-C) → SIGKILLed, [`Verdict::Terminated`], so the run loop
///   still reports every in-flight test instead of dropping it
pub async fn spawn_test(
    item: &WorkItem,
    env: &EngineEnv,
    test_ns: &str,
    hard_cap: Duration,
    cancel: &Cancel,
) -> TestOutcome {
    let started = Instant::now();

    // A spawn failure has no child and so no output of its own. Reported blank it reaches the
    // user as a bare `XFAIL` with an empty pane — and the causes are all environmental
    // (missing binary, unresolved shared library, wrong arch), none guessable from the test
    // name. `pod_runner` already names its reason; this is the same contract
    let failed = |e: std::io::Error| TestOutcome {
        verdict: Verdict::SpawnError,
        output: format!("cannot execute {}: {e}", item.binary_path.display()).into_bytes(),
        components: Vec::new(),
        duration: started.elapsed(),
    };

    let mut cmd = build_command(item, env, test_ns);
    let reader = match attach_stdio(&mut cmd, env.capture) {
        Ok(r) => r,
        Err(e) => return failed(e),
    };
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return failed(e),
    };
    // Parent's writer ends must close, else the read below never sees EOF
    drop(cmd);

    // Drained concurrently with the wait → a full pipe buffer can't deadlock the child
    let drain = reader.map(|mut r| {
        tokio::task::spawn_blocking(move || {
            let mut buf = Vec::new();
            let _ = std::io::Read::read_to_end(&mut r, &mut buf);
            buf
        })
    });

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

    // Reader completes at pipe EOF = child and every fd-inheriting descendant gone
    let output = match drain {
        Some(task) => task.await.unwrap_or_default(),
        None => Vec::new(),
    };
    TestOutcome { verdict, output, components: Vec::new(), duration: started.elapsed() }
}

/// One pipe for both fds, so the capture is interleaved as written.
///
/// - Two pipes concatenated put stderr *after* libtest's `test result:` footer
/// - [`strip_libtest_frame`](crate::libtest::strip_libtest_frame) truncates from that
///   footer → an `Err`-returning test's `Error: …` line disappears
/// - `--no-capture` → inherit and stream live, nothing to read
fn attach_stdio(
    cmd: &mut tokio::process::Command,
    capture: bool,
) -> std::io::Result<Option<std::io::PipeReader>> {
    if !capture {
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        return Ok(None);
    }
    let (reader, writer) = std::io::pipe()?;
    let dup = writer.try_clone()?;
    cmd.stdout(Stdio::from(writer)).stderr(Stdio::from(dup));
    Ok(Some(reader))
}

/// Build the `tokio` command: argv, cwd, env, and (Unix) a dedicated process group so the
/// whole tree dies at the hard cap. Stdout/stderr attached by [`attach_stdio`]
fn build_command(item: &WorkItem, env: &EngineEnv, test_ns: &str) -> tokio::process::Command {
    let mut std_cmd = std::process::Command::new(&item.binary_path);
    std_cmd
        .arg("--exact")
        .arg(&item.test_name)
        .arg("--nocapture")
        .current_dir(&item.cwd)
        .stdin(Stdio::null())
        // Dynamic-library path (the libstdc++-exit-127 fix)
        .env(super::dylib::dylib_path_envvar(), &env.dylib_path)
        .env("CARGO_MANIFEST_DIR", &item.cwd)
        .envs(env.test_vars(test_ns));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::fixture_child as child;
    use crate::qos::QosClass;
    use std::path::{Path, PathBuf};

    /// No lock and no temp files: the child is this binary, so these tests share nothing and
    /// run fully parallel. The `serial()` mutex they used to take existed only to order
    /// writes to fixture scripts
    fn env() -> EngineEnv {
        EngineEnv {
            dylib_path: OsString::new(),
            run: crate::naming::RunCoords { run_id: child::RUN_ID.into(), user: "tester".into() },
            no_cleanup: false,
            capture: true,
            ztest_log: None,
            image_refs: std::collections::BTreeMap::new(),
            storage: None,
        }
    }

    fn item(bin: &Path, name: &str) -> WorkItem {
        let p = QosClass::Integration.profile();
        WorkItem {
            binary_id: "t::b".into(),
            test_name: name.into(),
            binary_path: bin.to_path_buf(),
            cwd: PathBuf::from("/"),
            class: QosClass::Integration,
            footprint: p.footprint,
            hard_cap: p.hard_cap,
            retries: 0,
            deps: Vec::new(),
        }
    }

    /// Run one [`fixture_child`](crate::engine::fixture_child) helper to completion
    async fn play(name: &str, env: &EngineEnv) -> TestOutcome {
        spawn_test(
            &item(&child::exe(), &child::test_name(name)),
            env,
            "ztest-t-b-x-0a1b2c3d",
            Duration::from_secs(5),
            &Cancel::never(),
        )
        .await
    }

    fn output(out: &TestOutcome) -> String {
        String::from_utf8_lossy(&out.output).to_string()
    }

    /// The marker matters: a wrong `--exact` selects no test and libtest still exits 0, so
    /// verdict alone cannot tell "the child passed" from "the child never ran"
    #[tokio::test]
    async fn pass_on_zero_exit() {
        let out = play("exits_zero", &env()).await;
        assert_eq!(out.verdict, Verdict::Pass);
        assert!(output(&out).contains("zero-ok"), "{:?}", output(&out));
    }

    #[tokio::test]
    async fn fail_carries_the_childs_exit_code() {
        let out = play("exits_three", &env()).await;
        assert_eq!(out.verdict, Verdict::Fail(3));
        assert!(output(&out).contains("three-ok"), "{:?}", output(&out));
    }

    #[tokio::test]
    async fn captures_child_output() {
        let out = play("prints_stdout", &env()).await;
        assert!(output(&out).contains("hello-stdout"), "{:?}", output(&out));
    }

    /// - Missing run id or namespace → every `TestEnv::build` refuses
    /// - Missing manifest → every `dev!` component resolves to `DevImageMissing`
    #[tokio::test]
    async fn children_receive_the_run_identity_namespace_and_image_manifest() {
        let mut e = env();
        e.image_refs.insert("zainod@sha".into(), "zainod:dev-abc".into());
        let out = play("prints_test_vars", &e).await;
        let shown = output(&out);
        assert_eq!(out.verdict, Verdict::Pass, "{shown:?}");
        for want in [
            format!("RUN=[{}]", child::RUN_ID),
            "USER=[tester]".into(),
            "NS=[ztest-t-b-x-0a1b2c3d]".into(),
            r#"REFS={"zainod@sha":"zainod:dev-abc"}"#.into(),
        ] {
            assert!(shown.contains(&want), "missing {want} in {shown:?}");
        }
    }

    /// stderr must land *inside* the frame: appended after libtest's `test result:` line it
    /// is truncated by `strip_libtest_frame`, taking the failure reason with it. The frame
    /// here is libtest's own — the child is a real test binary, not a script imitating one
    #[tokio::test]
    async fn stderr_survives_the_libtest_footer_strip() {
        let name = child::test_name("fails_with_an_error_line");
        let out = play("fails_with_an_error_line", &env()).await;
        assert!(matches!(out.verdict, Verdict::Fail(_)), "{:?}", out.verdict);
        let shown = crate::libtest::strip_libtest_frame(&out.output, &name);
        let shown = String::from_utf8_lossy(&shown).to_string();
        assert!(shown.contains("Error: image build failed"), "{shown:?}");
    }

    #[tokio::test]
    async fn spawn_error_names_the_binary_it_could_not_run() {
        let out = spawn_test(
            &item(Path::new("/nonexistent/zzz"), "x"),
            &env(),
            "ztest-t-b-x-0a1b2c3d",
            Duration::from_secs(5),
            &Cancel::never(),
        )
        .await;
        assert_eq!(out.verdict, Verdict::SpawnError);
        // A blank pane under `XFAIL` is unactionable; the pod runner already names its
        // reason and the local path must match
        let shown = output(&out);
        assert!(shown.contains("/nonexistent/zzz"), "{shown:?}");
        assert!(!shown.trim().is_empty(), "spawn failure must explain itself");
    }

    #[tokio::test]
    async fn timeout_kills_long_sleeper() {
        let out = spawn_test(
            &item(&child::exe(), &child::test_name("sleeps_past_any_cap")),
            &env(),
            "ztest-t-b-x-0a1b2c3d",
            Duration::from_millis(300),
            &Cancel::never(),
        )
        .await;
        assert_eq!(out.verdict, Verdict::Timeout);
        assert!(out.duration < Duration::from_secs(5));
    }
}
