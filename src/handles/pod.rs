//! Component pod access: exec in the component's container, SIGKILL its main process, watch it
//! come back.
//!
//! - Restart needs the pod rendered restartable (`ComponentBuilder::restartable`): container
//!   restart keeps the pod → `emptyDir` scratch + PVCs survive, pod IP + forwards stay valid

use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Pod, PodStatus};
use kube::Client;
use kube::api::Api;

use crate::EnvError;
use crate::component::ComponentCategory;
use crate::env::{ComponentHandle, ComponentState, EnvInner};

use super::HandleInner;

/// Pod-status poll cadence while a killed container comes back
const RESTART_POLL: Duration = Duration::from_millis(500);

/// Kill exec's own budget (container teardown can cut the session, never hang it)
const KILL_EXEC_TIMEOUT: Duration = Duration::from_secs(30);

/// SIGKILL the container's main process from inside the pod.
///
/// - Needs `shareProcessNamespace` (PID 1 = pause; own-namespace init ignores SIGKILL)
/// - Main = earliest-started process parented outside the namespace (exec sessions start later
///   and die with the container they joined)
const KILL_MAIN: &str = r#"self=$$ best= bstart=
for d in /proc/[0-9]*; do
  p=${d#/proc/}
  if [ "$p" = 1 ] || [ "$p" = "$self" ]; then continue; fi
  read -r s 2>/dev/null < "$d/stat" || continue
  s=${s##*") "}
  set -- $s
  [ "$2" = 0 ] || continue
  if [ -z "$best" ] || [ "${20}" -lt "$bstart" ]; then best=$p bstart=${20}; fi
done
[ -n "$best" ] || { echo "no main process in a shared process namespace" >&2; exit 3; }
kill -9 "$best""#;

/// One finished exec. `status` = the command's exit code
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

impl ExecOutput {
    pub fn success(&self) -> bool {
        self.status == 0
    }
}

/// Container back after a kill. `downtime` = kill issued → container Ready again
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Restart {
    pub restart_count: u32,
    pub downtime: Duration,
}

/// Kubelet's view of the component container at one poll
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContainerSample {
    pub restarts: u32,
    pub ready: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum PodError {
    #[error(transparent)]
    Env(#[from] EnvError),
    #[error("{pod}: `{argv}` did not finish within {after:?}")]
    Timeout { pod: String, argv: String, after: Duration },
    #[error("{pod}: exec `{argv}`: {reason}")]
    Exec { pod: String, argv: String, reason: String },
    #[error("{pod}: read status: {reason}")]
    Status { pod: String, reason: String },
    #[error(
        "{pod}: not restartable (restartPolicy {policy}, shareProcessNamespace {shared}) — \
         declare `.restartable()` on the component"
    )]
    NotRestartable { pod: String, policy: String, shared: bool },
    #[error("{pod}: kill: {reason}")]
    Kill { pod: String, reason: String },
    #[error("{pod}: not back within {after:?} of the kill ({last:?})")]
    RestartTimeout { pod: String, after: Duration, last: Option<ContainerSample> },
}

/// Live pod of one component (cheap clone, resolved from the env's component table)
#[derive(Clone)]
pub struct ComponentPod {
    api: Api<Pod>,
    name: String,
    container: String,
    category: ComponentCategory,
}

impl std::fmt::Debug for ComponentPod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentPod")
            .field("name", &self.name)
            .field("container", &self.container)
            .finish_non_exhaustive()
    }
}

impl ComponentPod {
    fn new(client: &Client, state: &ComponentState) -> Self {
        let category = match state.handle {
            ComponentHandle::Validator(_) => ComponentCategory::Validator,
            ComponentHandle::Indexer(_) => ComponentCategory::Indexer,
        };
        ComponentPod {
            api: Api::namespaced(client.clone(), &state.namespace),
            name: state.pod_name.clone(),
            // Container named after the component label (`manifest::PodSpec::render`)
            container: state.label.to_string(),
            category,
        }
    }

    pub(crate) async fn of(inner: &EnvInner, component_id: u64) -> Result<Self, EnvError> {
        let state = inner.component_state(component_id).await?;
        Ok(Self::new(inner.client_ref()?, &state))
    }

    /// Every provisioned component's pod (wallets run in-process → none)
    pub(crate) async fn all(inner: &EnvInner) -> Result<Vec<Self>, EnvError> {
        let client = inner.client_ref()?;
        let comps = inner.components.read().await;
        let mut pods: Vec<Self> = comps.values().map(|s| Self::new(client, s)).collect();
        pods.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(pods)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn container(&self) -> &str {
        &self.container
    }
    pub fn category(&self) -> ComponentCategory {
        self.category
    }
    /// `name` = pod name (`.named(..)`) or container label (`zainod`)
    pub fn answers_to(&self, name: &str) -> bool {
        self.name == name || self.container == name
    }

    /// Run `argv` (no shell) in the component container, output captured whole
    pub async fn exec(&self, argv: &[&str], timeout: Duration) -> Result<ExecOutput, PodError> {
        let shown = || argv.join(" ");
        let run = crate::exec::exec(&self.api, &self.name, &self.container, argv, None);
        let captured = tokio::time::timeout(timeout, run)
            .await
            .map_err(|_| PodError::Timeout {
                pod: self.name.clone(),
                argv: shown(),
                after: timeout,
            })?
            .map_err(|e| PodError::Exec {
                pod: self.name.clone(),
                argv: shown(),
                reason: e.to_string(),
            })?;
        let exec_err =
            |reason: String| PodError::Exec { pod: self.name.clone(), argv: shown(), reason };
        let status = captured
            .status
            .as_ref()
            .ok_or_else(|| exec_err("stream closed without an exit status".into()))?;
        let status = crate::exec::exit_code(status).map_err(exec_err)?;
        Ok(ExecOutput { status, stdout: captured.stdout, stderr: captured.stderr })
    }

    /// Whole file at `path` inside the container (`cat`, so the image needs one)
    pub async fn read_file(&self, path: &str, timeout: Duration) -> Result<String, PodError> {
        let out = self.exec(&["cat", path], timeout).await?;
        if !out.success() {
            return Err(PodError::Exec {
                pod: self.name.clone(),
                argv: format!("cat {path}"),
                reason: format!("exit {}: {}", out.status, out.stderr.trim()),
            });
        }
        Ok(out.stdout)
    }

    async fn get(&self) -> Result<Pod, PodError> {
        self.api
            .get(&self.name)
            .await
            .map_err(|e| PodError::Status { pod: self.name.clone(), reason: e.to_string() })
    }

    /// Rendered with `.restartable()`? (else a kill ends the pod for good)
    pub async fn ensure_restartable(&self) -> Result<(), PodError> {
        let pod = self.get().await?;
        restartable(&pod).map_err(|(policy, shared)| PodError::NotRestartable {
            pod: self.name.clone(),
            policy,
            shared,
        })
    }

    /// Current restart count + readiness of the component container
    pub async fn sample(&self) -> Result<ContainerSample, PodError> {
        let pod = self.get().await?;
        pod.status.as_ref().and_then(|s| sample_of(s, &self.container)).ok_or_else(|| {
            PodError::Status { pod: self.name.clone(), reason: "no container status yet".into() }
        })
    }

    /// SIGKILL the main process, returning the pre-kill sample to wait against.
    ///
    /// - Refuses a pod not rendered restartable (the kill would end the pod for good)
    /// - Exec session may die with the container → a lost stream is not a failed kill
    pub async fn kill(&self) -> Result<ContainerSample, PodError> {
        self.ensure_restartable().await?;
        let before = self.sample().await?;
        match self.exec(&["sh", "-c", KILL_MAIN], KILL_EXEC_TIMEOUT).await {
            Ok(out) if out.success() => {}
            Ok(out) => {
                return Err(PodError::Kill {
                    pod: self.name.clone(),
                    reason: format!("exit {}: {}", out.status, out.stderr.trim()),
                });
            }
            Err(PodError::Exec { reason, .. }) => {
                tracing::debug!(pod = %self.name, %reason, "kill exec cut short by teardown");
            }
            Err(e) => return Err(e),
        }
        Ok(before)
    }

    /// Wait until the container restarted past `before` and reports Ready again
    pub async fn await_restart(
        &self,
        before: ContainerSample,
        timeout: Duration,
    ) -> Result<ContainerSample, PodError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut last = None;
        loop {
            if let Ok(sample) = self.sample().await {
                last = Some(sample);
                if came_back(before, sample) {
                    return Ok(sample);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(PodError::RestartTimeout {
                    pod: self.name.clone(),
                    after: timeout,
                    last,
                });
            }
            tokio::time::sleep(RESTART_POLL).await;
        }
    }

    /// [`kill`](Self::kill) + [`await_restart`](Self::await_restart).
    ///
    /// - `timeout` must cover kubelet's crash backoff (10 s first, doubling per kill within
    ///   10 min, 5 min cap) + the component's own reopen
    pub async fn kill_and_await_restart(&self, timeout: Duration) -> Result<Restart, PodError> {
        let started = tokio::time::Instant::now();
        let before = self.kill().await?;
        let after = self.await_restart(before, timeout).await?;
        Ok(Restart { restart_count: after.restarts, downtime: started.elapsed() })
    }
}

/// Pod-backed handle → exec + kill on the handle a profile already holds
#[async_trait]
pub trait PodHandle: Send + Sync {
    fn plumbing(&self) -> &HandleInner;

    async fn pod(&self) -> Result<ComponentPod, EnvError> {
        self.plumbing().pod().await
    }
    async fn exec(&self, argv: &[&str], timeout: Duration) -> Result<ExecOutput, PodError> {
        self.pod().await?.exec(argv, timeout).await
    }
    async fn kill_and_await_restart(&self, timeout: Duration) -> Result<Restart, PodError> {
        self.pod().await?.kill_and_await_restart(timeout).await
    }
}

/// Sync runner's view of a component: sampled each tick, killed by a nemesis fault
#[async_trait]
pub(crate) trait Watched: Send + Sync + std::fmt::Debug {
    fn answers_to(&self, name: &str) -> bool;
    async fn sample(&self) -> Result<ContainerSample, PodError>;
    async fn kill(&self) -> Result<ContainerSample, PodError>;
    async fn ensure_restartable(&self) -> Result<(), PodError>;
}

#[async_trait]
impl Watched for ComponentPod {
    fn answers_to(&self, name: &str) -> bool {
        ComponentPod::answers_to(self, name)
    }
    async fn sample(&self) -> Result<ContainerSample, PodError> {
        ComponentPod::sample(self).await
    }
    async fn kill(&self) -> Result<ContainerSample, PodError> {
        ComponentPod::kill(self).await
    }
    async fn ensure_restartable(&self) -> Result<(), PodError> {
        ComponentPod::ensure_restartable(self).await
    }
}

fn sample_of(status: &PodStatus, container: &str) -> Option<ContainerSample> {
    let cs = status.container_statuses.as_ref()?.iter().find(|c| c.name == container)?;
    let running = cs.state.as_ref().is_some_and(|s| s.running.is_some());
    Some(ContainerSample { restarts: cs.restart_count.max(0) as u32, ready: running && cs.ready })
}

/// Back = kubelet counted a restart past the pre-kill baseline AND the new container is Ready
fn came_back(before: ContainerSample, now: ContainerSample) -> bool {
    now.restarts > before.restarts && now.ready
}

/// `Err((restartPolicy, shareProcessNamespace))` unless the kill can come back
fn restartable(pod: &Pod) -> Result<(), (String, bool)> {
    let spec = pod.spec.as_ref();
    let policy = spec.and_then(|s| s.restart_policy.clone()).unwrap_or_else(|| "Always".into());
    let shared = spec.and_then(|s| s.share_process_namespace).unwrap_or(false);
    match (policy.as_str(), shared) {
        ("Always" | "OnFailure", true) => Ok(()),
        _ => Err((policy, shared)),
    }
}

/// Opt-in render for [`ComponentPod::kill`]: container restart on non-zero exit + a shared
/// PID namespace (own-namespace init ignores SIGKILL)
pub(crate) fn make_restartable(pod: &mut Pod) {
    if let Some(spec) = pod.spec.as_mut() {
        spec.restart_policy = Some("OnFailure".into());
        spec.share_process_namespace = Some(true);
    }
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateRunning, ContainerStateWaiting, ContainerStatus, PodSpec,
    };

    use super::*;

    fn status(restarts: i32, ready: bool, waiting: Option<&str>) -> PodStatus {
        let state = match waiting {
            Some(reason) => ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some(reason.into()),
                    ..ContainerStateWaiting::default()
                }),
                ..ContainerState::default()
            },
            None => ContainerState {
                running: Some(ContainerStateRunning::default()),
                ..ContainerState::default()
            },
        };
        PodStatus {
            container_statuses: Some(vec![ContainerStatus {
                name: "zainod".into(),
                restart_count: restarts,
                ready,
                state: Some(state),
                ..ContainerStatus::default()
            }]),
            ..PodStatus::default()
        }
    }

    /// Kill → backoff → running-not-ready → Ready: only the last counts as back
    #[test]
    fn restart_is_back_only_once_counted_and_ready() {
        let before = sample_of(&status(0, true, None), "zainod").expect("sampled");
        assert_eq!(before, ContainerSample { restarts: 0, ready: true });
        assert_eq!(sample_of(&status(0, true, None), "other"), None, "wrong container");

        let timeline = [
            (status(0, true, None), false),
            (status(0, false, Some("Error")), false),
            (status(1, false, Some("CrashLoopBackOff")), false),
            (status(1, false, None), false),
            (status(1, true, None), true),
        ];
        for (st, want) in timeline {
            let now = sample_of(&st, "zainod").expect("sampled");
            assert_eq!(came_back(before, now), want, "{now:?}");
        }
        // Stale ready flag on a waiting container never reads as up
        let stale = sample_of(&status(2, true, Some("CrashLoopBackOff")), "zainod");
        assert_eq!(stale, Some(ContainerSample { restarts: 2, ready: false }));
    }

    #[test]
    fn only_a_restartable_render_admits_a_kill() {
        let rendered = |policy: &str| Pod {
            spec: Some(PodSpec { restart_policy: Some(policy.into()), ..PodSpec::default() }),
            ..Pod::default()
        };
        assert_eq!(restartable(&rendered("Never")), Err(("Never".into(), false)));
        assert_eq!(restartable(&rendered("OnFailure")), Err(("OnFailure".into(), false)));

        let mut pod = rendered("Never");
        make_restartable(&mut pod);
        assert_eq!(restartable(&pod), Ok(()));
    }

    #[test]
    fn kill_script_parses_under_posix_sh() {
        let out = std::process::Command::new("sh")
            .args(["-n", "-c", KILL_MAIN])
            .output()
            .expect("spawn sh");
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    }
}
