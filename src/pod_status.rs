//! Pure classification of an observed pod `status` into the states the harness's
//! two poll-and-wait loops act on: the runner-pod terminal wait
//! ([`engine::pod_runner`](crate::engine)) and the dependency-pod readiness wait
//! ([`env::TestEnv`](crate::env)).
//!
//! These are the load-bearing distinctions behind the "no flaky tests"
//! contract: a pod that is merely `Pending` on scheduling capacity is the
//! broker's backlog to clear, not a test failure, whereas an `OOMKilled` or
//! `CrashLoopBackOff` pod will never self-heal and must fail fast. Keeping the
//! logic here and I/O-free lets both loops unit-test their decisions against
//! synthetic `PodStatus` values.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::{Pod, PodStatus};

/// How often a pod's status is polled while a wait loop awaits a state change.
pub const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How long a pod may sit in a *transient* image-pull error before it is
/// declared terminal. The kubelet retries pulls with backoff, so a transient
/// cause — most visibly a `pull QPS exceeded` storm when many pods launch at
/// once on a single node, which clears the moment the first pod warms the
/// node's image cache (`imagePullPolicy: IfNotPresent`) — self-heals well
/// within this window. `InvalidImageName` is never transient and ignores the
/// grace (see [`pull_error_is_terminal`]); a genuinely absent image fails after
/// the grace.
pub const IMAGE_PULL_GRACE: Duration = Duration::from_secs(90);

/// Whether the pod reports a `Ready` condition of `True` — the container is up
/// and its readiness probe (a TCP-socket check on the component's port) passes.
pub fn is_ready(status: &PodStatus) -> bool {
    status
        .conditions
        .as_ref()
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// Whether the pod has reached the `Running` phase: scheduled onto a node with
/// every container created and started. This is the boundary from which
/// time-to-ready is the application's own responsibility rather than the
/// scheduler's placement backlog — i.e. the point at which a readiness deadline
/// becomes a meaningful signal.
pub fn is_running(status: &PodStatus) -> bool {
    status.phase.as_deref() == Some("Running")
}

/// A non-image fault that will not self-heal, if present: a whole-pod `Failed`
/// phase, a container wedged in `CrashLoopBackOff`, or one `OOMKilled`. The
/// returned reason lets a wait loop fail fast instead of waiting out its
/// deadline on a pod that can never become ready. Image-pull errors are
/// deliberately *not* covered here — they carry a grace window and are handled
/// via [`image_error`] + [`pull_error_is_terminal`].
pub fn fault(status: &PodStatus) -> Option<String> {
    if status.phase.as_deref() == Some("Failed") {
        return Some(
            status
                .reason
                .clone()
                .unwrap_or_else(|| "pod entered Failed phase".to_string()),
        );
    }
    status.container_statuses.as_ref()?.iter().find_map(|cs| {
        let waiting = cs.state.as_ref().and_then(|s| s.waiting.as_ref());
        if waiting.and_then(|w| w.reason.as_deref()) == Some("CrashLoopBackOff") {
            return Some(format!("{}: CrashLoopBackOff", cs.name));
        }
        // OOM shows on the current termination, or the previous one once the
        // kubelet has moved the container into its backoff-restart waiting state.
        let terminated = cs
            .state
            .as_ref()
            .and_then(|s| s.terminated.as_ref())
            .or_else(|| cs.last_state.as_ref().and_then(|s| s.terminated.as_ref()));
        if terminated.and_then(|t| t.reason.as_deref()) == Some("OOMKilled") {
            return Some(format!("{}: OOMKilled", cs.name));
        }
        None
    })
}

/// An unrecoverable image-pull waiting reason, if any container is stuck on one.
pub fn image_error(status: &PodStatus) -> Option<String> {
    let stuck = ["ImagePullBackOff", "ErrImagePull", "InvalidImageName"];
    status.container_statuses.as_ref()?.iter().find_map(|cs| {
        let w = cs.state.as_ref()?.waiting.as_ref()?;
        let reason = w.reason.as_deref()?;
        stuck.contains(&reason).then(|| reason.to_string())
    })
}

/// Whether an observed image-pull error should end the wait. `InvalidImageName`
/// can never resolve, so it's terminal immediately; a pull *failure*
/// (`ErrImagePull`/`ImagePullBackOff`) is transient until it has persisted for
/// `grace`, giving the kubelet's backoff time to clear a `pull QPS exceeded`
/// storm before the wait is failed. `first_seen` is when this pod's pull error
/// was first observed.
pub fn pull_error_is_terminal(
    reason: &str,
    first_seen: Instant,
    now: Instant,
    grace: Duration,
) -> bool {
    reason == "InvalidImageName" || now.duration_since(first_seen) >= grace
}

/// The first container's terminated exit code, if it has one.
pub fn exit_code(status: &PodStatus) -> Option<i32> {
    status
        .container_statuses
        .as_ref()?
        .iter()
        .find_map(|cs| cs.state.as_ref()?.terminated.as_ref().map(|t| t.exit_code))
}

/// The kube-server-stamped instants that bound a pod's lifecycle, for latency
/// attribution. Each is `None` until the pod has reached that point (or if the
/// apiserver hasn't populated it). Derived durations ([`PodPhases::schedule`],
/// [`PodPhases::pull_init`], [`PodPhases::body`]) are `None` when either
/// endpoint is missing.
///
/// These are *authoritative* server timestamps, not laptop-side measurements:
/// they isolate where a slow test's wall time actually went — waiting in the
/// scheduler's queue, pulling/starting the image, or in the test body itself —
/// which a single client-measured total cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodPhases {
    /// `metadata.creationTimestamp`: the apiserver accepted the pod.
    pub created: Option<DateTime<Utc>>,
    /// `PodScheduled` condition true: a node was assigned. Gap from `created`
    /// is scheduler-queue time.
    pub scheduled: Option<DateTime<Utc>>,
    /// First container's `state.running.startedAt` (or `terminated.startedAt`
    /// once it has exited): the container process began. Gap from `scheduled`
    /// is image pull + container create/init.
    pub container_started: Option<DateTime<Utc>>,
    /// First container's `state.terminated.finishedAt`: it exited. Gap from
    /// `container_started` is the test body's own wall time.
    pub container_finished: Option<DateTime<Utc>>,
}

impl PodPhases {
    /// Scheduler-queue wait: `created → scheduled`.
    pub fn schedule(&self) -> Option<Duration> {
        delta(self.created, self.scheduled)
    }
    /// Image pull + container create/init: `scheduled → container_started`.
    pub fn pull_init(&self) -> Option<Duration> {
        delta(self.scheduled, self.container_started)
    }
    /// Test-body wall time (server-observed): `container_started → finished`.
    pub fn body(&self) -> Option<Duration> {
        delta(self.container_started, self.container_finished)
    }
}

/// Non-negative wall duration between two optional instants; `None` if either
/// endpoint is absent, and clamped to zero if clocks report `end < start`
/// (server-timestamp skew on distinct events must never surface as a negative).
fn delta(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Option<Duration> {
    let (start, end) = (start?, end?);
    Some((end - start).to_std().unwrap_or(Duration::ZERO))
}

/// The `lastTransitionTime` of the pod condition of `type_` whose status is
/// `"True"`.
fn condition_true_at(pod: &Pod, type_: &str) -> Option<DateTime<Utc>> {
    pod.status.as_ref()?.conditions.as_ref()?.iter().find_map(|c| {
        (c.type_ == type_ && c.status == "True")
            .then(|| c.last_transition_time.as_ref().map(|t| t.0))
            .flatten()
    })
}

/// Extract the [`PodPhases`] lifecycle boundaries from an observed pod. Reads
/// the first container's status (ztest pods are single-container: the runner
/// pod's `test`, a component pod's `zebrad`/`zainod`).
pub fn pod_phases(pod: &Pod) -> PodPhases {
    let first = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .and_then(|cs| cs.first());
    let state = first.and_then(|c| c.state.as_ref());
    let container_started = state.and_then(|s| {
        s.running
            .as_ref()
            .and_then(|r| r.started_at.as_ref())
            .or_else(|| s.terminated.as_ref().and_then(|t| t.started_at.as_ref()))
            .map(|t| t.0)
    });
    let container_finished = state
        .and_then(|s| s.terminated.as_ref())
        .and_then(|t| t.finished_at.as_ref())
        .map(|t| t.0);
    PodPhases {
        created: pod.metadata.creation_timestamp.as_ref().map(|t| t.0),
        scheduled: condition_true_at(pod, "PodScheduled"),
        container_started,
        container_finished,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateTerminated, ContainerStateWaiting, ContainerStatus,
        PodCondition,
    };

    fn status(phase: Option<&str>) -> PodStatus {
        PodStatus {
            phase: phase.map(String::from),
            ..Default::default()
        }
    }

    fn with_container(mut s: PodStatus, cs: ContainerStatus) -> PodStatus {
        s.container_statuses.get_or_insert_with(Vec::new).push(cs);
        s
    }

    fn container(name: &str) -> ContainerStatus {
        ContainerStatus {
            name: name.into(),
            image: "img".into(),
            image_id: String::new(),
            ready: false,
            restart_count: 0,
            ..Default::default()
        }
    }

    #[test]
    fn running_only_in_running_phase() {
        assert!(!is_running(&status(Some("Pending"))));
        assert!(is_running(&status(Some("Running"))));
        assert!(!is_running(&status(None)));
    }

    // ── Lifecycle phase extraction ──────────────────────────────────────────

    use k8s_openapi::api::core::v1::{ContainerStateRunning, PodSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

    fn t(secs: i64) -> Time {
        Time(DateTime::from_timestamp(secs, 0).unwrap())
    }

    fn scheduled_cond(at: i64) -> PodCondition {
        PodCondition {
            type_: "PodScheduled".into(),
            status: "True".into(),
            last_transition_time: Some(t(at)),
            ..Default::default()
        }
    }

    /// A pod that finished the whole lifecycle: created@100, scheduled@101,
    /// container started@110 (a 9s pull), exited@112 (a 2s body).
    fn settled_pod() -> Pod {
        let mut s = with_container(status(Some("Succeeded")), {
            let mut c = container("test");
            c.state = Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    exit_code: 0,
                    started_at: Some(t(110)),
                    finished_at: Some(t(112)),
                    ..Default::default()
                }),
                ..Default::default()
            });
            c
        });
        s.conditions = Some(vec![scheduled_cond(101)]);
        Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                creation_timestamp: Some(t(100)),
                ..Default::default()
            },
            spec: Some(PodSpec::default()),
            status: Some(s),
        }
    }

    #[test]
    fn phases_derive_scheduler_pull_and_body_gaps() {
        let p = pod_phases(&settled_pod());
        assert_eq!(p.schedule(), Some(Duration::from_secs(1)));
        assert_eq!(p.pull_init(), Some(Duration::from_secs(9)));
        assert_eq!(p.body(), Some(Duration::from_secs(2)));
    }

    #[test]
    fn phases_use_running_started_at_before_termination() {
        // A still-running pod: container_started comes from `running.startedAt`
        // and there is no body duration yet.
        let mut s = with_container(status(Some("Running")), {
            let mut c = container("test");
            c.state = Some(ContainerState {
                running: Some(ContainerStateRunning {
                    started_at: Some(t(110)),
                }),
                ..Default::default()
            });
            c
        });
        s.conditions = Some(vec![scheduled_cond(101)]);
        let pod = Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                creation_timestamp: Some(t(100)),
                ..Default::default()
            },
            spec: None,
            status: Some(s),
        };
        let p = pod_phases(&pod);
        assert_eq!(p.pull_init(), Some(Duration::from_secs(9)));
        assert_eq!(p.body(), None, "no body until the container terminates");
    }

    #[test]
    fn phases_absent_endpoints_yield_none_and_never_go_negative() {
        // A bare pending pod: nothing but creation is known.
        let mut pod = settled_pod();
        pod.status = Some(status(Some("Pending")));
        let p = pod_phases(&pod);
        assert_eq!(p.schedule(), None);
        assert_eq!(p.pull_init(), None);
        assert_eq!(p.body(), None);

        // Skewed timestamps (finished before started) clamp to zero, never a
        // panic or a negative.
        let skewed = PodPhases {
            created: Some(t(100).0),
            scheduled: Some(t(101).0),
            container_started: Some(t(112).0),
            container_finished: Some(t(110).0),
        };
        assert_eq!(skewed.body(), Some(Duration::ZERO));
    }

    #[test]
    fn ready_reads_the_ready_condition() {
        let mut s = status(Some("Running"));
        s.conditions = Some(vec![PodCondition {
            type_: "Ready".into(),
            status: "True".into(),
            ..Default::default()
        }]);
        assert!(is_ready(&s));
        s.conditions = Some(vec![PodCondition {
            type_: "Ready".into(),
            status: "False".into(),
            ..Default::default()
        }]);
        assert!(!is_ready(&s));
    }

    #[test]
    fn pending_pod_is_not_a_fault() {
        // The whole point: a pod queued on capacity has no fault and must not
        // be failed — the caller waits for it without charging a deadline.
        assert!(fault(&status(Some("Pending"))).is_none());
        assert!(fault(&status(Some("Running"))).is_none());
    }

    #[test]
    fn crashloop_and_oom_and_failed_are_faults() {
        let crash = with_container(status(Some("Running")), {
            let mut c = container("zebrad");
            c.state = Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some("CrashLoopBackOff".into()),
                    ..Default::default()
                }),
                ..Default::default()
            });
            c
        });
        assert_eq!(fault(&crash).as_deref(), Some("zebrad: CrashLoopBackOff"));

        let oom = with_container(status(Some("Running")), {
            let mut c = container("zainod");
            c.last_state = Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    reason: Some("OOMKilled".into()),
                    exit_code: 137,
                    ..Default::default()
                }),
                ..Default::default()
            });
            c
        });
        assert_eq!(fault(&oom).as_deref(), Some("zainod: OOMKilled"));

        assert!(fault(&status(Some("Failed"))).is_some());
    }

    #[test]
    fn image_error_detects_stuck_pull() {
        let p = with_container(status(Some("Pending")), {
            let mut c = container("test");
            c.state = Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some("ImagePullBackOff".into()),
                    ..Default::default()
                }),
                ..Default::default()
            });
            c
        });
        assert_eq!(image_error(&p).as_deref(), Some("ImagePullBackOff"));
        assert!(image_error(&status(Some("Pending"))).is_none());
    }

    #[test]
    fn transient_pull_error_terminal_only_after_grace() {
        let t0 = Instant::now();
        let grace = Duration::from_secs(90);
        assert!(!pull_error_is_terminal(
            "ErrImagePull",
            t0,
            t0 + Duration::from_secs(30),
            grace
        ));
        assert!(pull_error_is_terminal(
            "ErrImagePull",
            t0,
            t0 + Duration::from_secs(91),
            grace
        ));
        assert!(pull_error_is_terminal("InvalidImageName", t0, t0, grace));
    }
}
