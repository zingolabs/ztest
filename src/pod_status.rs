//! Pure pod-`status` classification for every poll-and-wait loop: runner-pod terminal wait
//! ([`engine::pod_runner`](crate::engine)), dependency readiness ([`env::TestEnv`](crate::env)),
//! csi-hostpath install (`ztest cluster setup`).
//!
//! - [`ReadyWatch`] = the shared deadline policy; predicates below stay separately usable
//!   (the terminal wait wants faults, not readiness)
//! - `Pending` on capacity = broker's backlog, never a test failure; `OOMKilled` /
//!   `CrashLoopBackOff` never self-heal → fail fast ("no flaky tests")
//! - `Pending` too coarse: [`is_scheduled`] splits queued from unplaceable by node
//!   assignment, [`PENDING_TIMEOUT`] bounds the unscheduled window
//! - I/O-free → every loop unit-tests its decisions against synthetic `PodStatus`

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::{Pod, PodStatus};

/// Status poll cadence inside a wait loop
pub const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Unscheduled budget before the wait gives up.
///
/// - Covers the slow legitimate paths (`WaitForFirstConsumer` provisioning, autoscaler
///   bring-up); past it placement is unsatisfiable, not contended
/// - Cluster property → a constant, unlike the per-test `ready_timeout`
pub const PENDING_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Grace on a *transient* image-pull error before it counts as terminal.
///
/// - Kubelet backoff clears a `pull QPS exceeded` storm well inside this (first pod warms
///   the node cache, `imagePullPolicy: IfNotPresent`)
/// - `InvalidImageName` skips the grace ([`pull_error_is_terminal`])
pub const IMAGE_PULL_GRACE: Duration = Duration::from_secs(90);

/// `Ready=True` = container up + its TCP-socket readiness probe passing
pub fn is_ready(status: &PodStatus) -> bool {
    status
        .conditions
        .as_ref()
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// `Running` = boundary where time-to-ready becomes the app's, not the scheduler's →
/// where a readiness deadline starts meaning something
pub fn is_running(status: &PodStatus) -> bool {
    status.phase.as_deref() == Some("Running")
}

/// Node assigned = the boundary [`PENDING_TIMEOUT`] clocks. Never revoked once `True`
pub fn is_scheduled(status: &PodStatus) -> bool {
    status
        .conditions
        .as_ref()
        .map(|cs| cs.iter().any(|c| c.type_ == "PodScheduled" && c.status == "True"))
        .unwrap_or(false)
}

/// Scheduler's verdict from `PodScheduled=False` (`Unschedulable: 0/1 nodes are available…`).
/// `None` once placed, or before the scheduler records one
pub fn schedule_blocker(status: &PodStatus) -> Option<String> {
    let c = status
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.type_ == "PodScheduled" && c.status != "True")?;
    match (c.reason.as_deref(), c.message.as_deref()) {
        (Some(r), Some(m)) => Some(format!("{r}: {m}")),
        (Some(r), None) => Some(r.to_string()),
        (None, Some(m)) => Some(m.to_string()),
        (None, None) => None,
    }
}

/// Non-image fault that will not self-heal: `Failed` phase, `CrashLoopBackOff`, `OOMKilled`
/// → wait loop fails fast rather than burning its deadline.
///
/// Pull errors excluded (they carry a grace: [`image_error`] + [`pull_error_is_terminal`])
pub fn fault(status: &PodStatus) -> Option<String> {
    if status.phase.as_deref() == Some("Failed") {
        return Some(
            status.reason.clone().unwrap_or_else(|| "pod entered Failed phase".to_string()),
        );
    }
    status.container_statuses.as_ref()?.iter().find_map(|cs| {
        let waiting = cs.state.as_ref().and_then(|s| s.waiting.as_ref());
        if waiting.and_then(|w| w.reason.as_deref()) == Some("CrashLoopBackOff") {
            return Some(format!("{}: CrashLoopBackOff", cs.name));
        }
        // OOM shows on the current termination, or the previous one once the kubelet has
        // moved the container into backoff-restart waiting
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

/// Image-pull waiting reason any container is stuck on
pub fn image_error(status: &PodStatus) -> Option<String> {
    let stuck = ["ImagePullBackOff", "ErrImagePull", "InvalidImageName"];
    status.container_statuses.as_ref()?.iter().find_map(|cs| {
        let w = cs.state.as_ref()?.waiting.as_ref()?;
        let reason = w.reason.as_deref()?;
        stuck.contains(&reason).then(|| reason.to_string())
    })
}

/// Should this pull error end the wait?
///
/// - `InvalidImageName` = terminal at once (never resolves)
/// - `ErrImagePull`/`ImagePullBackOff` transient until persisting `grace` past `first_seen`
pub fn pull_error_is_terminal(
    reason: &str,
    first_seen: Instant,
    now: Instant,
    grace: Duration,
) -> bool {
    reason == "InvalidImageName" || now.duration_since(first_seen) >= grace
}

/// Verdict on one pod-`status` sample, from [`ReadyWatch::observe`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Ready,
    Waiting,
    Unschedulable { reason: String, elapsed: Duration },
    Faulted(String),
    PullFailed(String),
    ReadyTimeout(Duration),
}

/// The three readiness deadlines, folded one sample at a time.
///
/// - Owns a clock per deadline; caller owns the poll and the reporting
/// - I/O-free → one policy for every wait loop, tested on synthetic `PodStatus`
#[derive(Debug)]
pub struct ReadyWatch {
    ready_timeout: Duration,
    unscheduled_since: Option<Instant>,
    running_since: Option<Instant>,
    pull_error_since: Option<Instant>,
}

impl ReadyWatch {
    /// `ready_timeout` clocks from `Running` (time-to-ready = the app's, not the scheduler's)
    pub fn new(ready_timeout: Duration) -> Self {
        Self { ready_timeout, unscheduled_since: None, running_since: None, pull_error_since: None }
    }

    /// Order load-bearing: placement gates the rest (no node → every later check vacuous)
    pub fn observe(&mut self, status: &PodStatus, now: Instant) -> Verdict {
        if is_ready(status) {
            return Verdict::Ready;
        }
        if !is_scheduled(status) {
            let since = *self.unscheduled_since.get_or_insert(now);
            let elapsed = now.saturating_duration_since(since);
            if elapsed >= PENDING_TIMEOUT {
                let reason = schedule_blocker(status)
                    .unwrap_or_else(|| "no PodScheduled condition".to_string());
                return Verdict::Unschedulable { reason, elapsed };
            }
        }
        if let Some(reason) = fault(status) {
            return Verdict::Faulted(reason);
        }
        match image_error(status) {
            Some(reason) => {
                let first = *self.pull_error_since.get_or_insert(now);
                if pull_error_is_terminal(&reason, first, now, IMAGE_PULL_GRACE) {
                    return Verdict::PullFailed(reason);
                }
            }
            // Kubelet backoff cleared → the grace restarts, never carries over
            None => self.pull_error_since = None,
        }
        if is_running(status) {
            let since = *self.running_since.get_or_insert(now);
            if now.saturating_duration_since(since) >= self.ready_timeout {
                return Verdict::ReadyTimeout(self.ready_timeout);
            }
        }
        Verdict::Waiting
    }
}

pub fn exit_code(status: &PodStatus) -> Option<i32> {
    status
        .container_statuses
        .as_ref()?
        .iter()
        .find_map(|cs| cs.state.as_ref()?.terminated.as_ref().map(|t| t.exit_code))
}

/// Kube-server timestamps bounding a pod's lifecycle; each `None` until reached.
///
/// Derived gaps isolate where a slow test's wall time went, which a client-measured
/// total cannot
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodPhases {
    pub created: Option<DateTime<Utc>>,
    pub scheduled: Option<DateTime<Utc>>,
    pub container_started: Option<DateTime<Utc>>,
    pub container_finished: Option<DateTime<Utc>>,
}

impl PodPhases {
    /// Scheduler-queue wait: `created → scheduled`
    pub fn schedule(&self) -> Option<Duration> {
        delta(self.created, self.scheduled)
    }
    /// Image pull + container create/init: `scheduled → container_started`
    pub fn pull_init(&self) -> Option<Duration> {
        delta(self.scheduled, self.container_started)
    }
    /// Test-body wall time, server-observed: `container_started → finished`
    pub fn body(&self) -> Option<Duration> {
        delta(self.container_started, self.container_finished)
    }
}

/// `None` if either endpoint absent; clamped to zero on `end < start` (server-timestamp
/// skew across distinct events must never surface as a negative)
fn delta(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Option<Duration> {
    let (start, end) = (start?, end?);
    Some((end - start).to_std().unwrap_or(Duration::ZERO))
}

fn condition_true_at(pod: &Pod, type_: &str) -> Option<DateTime<Utc>> {
    pod.status.as_ref()?.conditions.as_ref()?.iter().find_map(|c| {
        (c.type_ == type_ && c.status == "True")
            .then(|| c.last_transition_time.as_ref().map(|t| t.0))
            .flatten()
    })
}

/// Reads the *first* container's status — exact, ztest pods are single-container
pub fn pod_phases(pod: &Pod) -> PodPhases {
    let first =
        pod.status.as_ref().and_then(|s| s.container_statuses.as_ref()).and_then(|cs| cs.first());
    let state = first.and_then(|c| c.state.as_ref());
    let container_started = state.and_then(|s| {
        s.running
            .as_ref()
            .and_then(|r| r.started_at.as_ref())
            .or_else(|| s.terminated.as_ref().and_then(|t| t.started_at.as_ref()))
            .map(|t| t.0)
    });
    let container_finished =
        state.and_then(|s| s.terminated.as_ref()).and_then(|t| t.finished_at.as_ref()).map(|t| t.0);
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
        PodStatus { phase: phase.map(String::from), ..Default::default() }
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

    /// Full lifecycle: created@100, scheduled@101, started@110 (9s pull), exited@112 (2s body)
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
        // Still running → container_started from `running.startedAt`, no body yet
        let mut s = with_container(status(Some("Running")), {
            let mut c = container("test");
            c.state = Some(ContainerState {
                running: Some(ContainerStateRunning { started_at: Some(t(110)) }),
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
        // Bare pending pod: only creation known
        let mut pod = settled_pod();
        pod.status = Some(status(Some("Pending")));
        let p = pod_phases(&pod);
        assert_eq!(p.schedule(), None);
        assert_eq!(p.pull_init(), None);
        assert_eq!(p.body(), None);

        // Skew (finished < started) clamps to zero, never a panic or a negative
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
        // Queued-on-capacity = no fault; caller waits without charging a deadline
        assert!(fault(&status(Some("Pending"))).is_none());
        assert!(fault(&status(Some("Running"))).is_none());
    }

    /// `PodScheduled` condition with optional reason/message
    fn sched_cond(status: &str, reason: Option<&str>, message: Option<&str>) -> PodCondition {
        PodCondition {
            type_: "PodScheduled".into(),
            status: status.into(),
            reason: reason.map(String::from),
            message: message.map(String::from),
            ..Default::default()
        }
    }

    fn with_conditions(mut s: PodStatus, cs: Vec<PodCondition>) -> PodStatus {
        s.conditions = Some(cs);
        s
    }

    #[test]
    fn scheduled_reads_the_pod_scheduled_condition() {
        // Absent condition = too new for a verdict → not scheduled
        assert!(!is_scheduled(&status(Some("Pending"))));
        assert!(!is_scheduled(&with_conditions(
            status(Some("Pending")),
            vec![sched_cond("False", Some("Unschedulable"), None)],
        )));
        assert!(is_scheduled(&with_conditions(
            status(Some("Pending")),
            vec![sched_cond("True", None, None)],
        )));
    }

    #[test]
    fn scheduled_ignores_other_condition_types() {
        // `Ready=True` with no PodScheduled must not read as scheduled (independent
        // conditions, only one is the placement signal)
        let s = with_conditions(
            status(Some("Running")),
            vec![PodCondition {
                type_: "Ready".into(),
                status: "True".into(),
                ..Default::default()
            }],
        );
        assert!(!is_scheduled(&s));
    }

    #[test]
    fn schedule_blocker_reports_the_schedulers_verdict() {
        // Real shape of the failure this exists to surface
        let unbound = with_conditions(
            status(Some("Pending")),
            vec![sched_cond(
                "False",
                Some("Unschedulable"),
                Some("0/1 nodes are available: pod has unbound immediate PersistentVolumeClaims"),
            )],
        );
        assert_eq!(
            schedule_blocker(&unbound).as_deref(),
            Some(
                "Unschedulable: 0/1 nodes are available: pod has unbound immediate \
                 PersistentVolumeClaims"
            )
        );
    }

    #[test]
    fn schedule_blocker_degrades_with_partial_conditions() {
        let reason_only = with_conditions(
            status(Some("Pending")),
            vec![sched_cond("False", Some("Unschedulable"), None)],
        );
        assert_eq!(schedule_blocker(&reason_only).as_deref(), Some("Unschedulable"));

        let message_only = with_conditions(
            status(Some("Pending")),
            vec![sched_cond("False", None, Some("no nodes"))],
        );
        assert_eq!(schedule_blocker(&message_only).as_deref(), Some("no nodes"));

        // Nothing to report → `None`, never an empty or misleading string
        let bare = with_conditions(status(Some("Pending")), vec![sched_cond("False", None, None)]);
        assert_eq!(schedule_blocker(&bare), None);
    }

    #[test]
    fn schedule_blocker_is_silent_once_placement_succeeds() {
        // Placed → no blocker, so no stale reason against a merely slow-starting pod
        let placed = with_conditions(status(Some("Running")), vec![sched_cond("True", None, None)]);
        assert_eq!(schedule_blocker(&placed), None);
        assert_eq!(schedule_blocker(&status(Some("Pending"))), None);
    }

    #[test]
    fn an_unschedulable_pod_is_still_not_a_fault() {
        // Why the deadline exists: unplaceable == queued under `fault`/phase alone, only
        // the elapsed-unscheduled clock separates them
        let wedged = with_conditions(
            status(Some("Pending")),
            vec![sched_cond("False", Some("Unschedulable"), Some("unbound PVC"))],
        );
        assert!(fault(&wedged).is_none());
        assert!(!is_running(&wedged));
        assert!(!is_scheduled(&wedged));
        assert!(schedule_blocker(&wedged).is_some());
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
        assert!(!pull_error_is_terminal("ErrImagePull", t0, t0 + Duration::from_secs(30), grace));
        assert!(pull_error_is_terminal("ErrImagePull", t0, t0 + Duration::from_secs(91), grace));
        assert!(pull_error_is_terminal("InvalidImageName", t0, t0, grace));
    }

    // ── ReadyWatch: the shared deadline fold ────────────────────────────────

    fn waiting_on(reason: &str) -> PodStatus {
        with_conditions(
            with_container(status(Some("Pending")), {
                let mut c = container("hostpath");
                c.state = Some(ContainerState {
                    waiting: Some(ContainerStateWaiting {
                        reason: Some(reason.into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                c
            }),
            vec![sched_cond("True", None, None)],
        )
    }

    fn ready_pod() -> PodStatus {
        with_conditions(
            status(Some("Running")),
            vec![PodCondition {
                type_: "Ready".into(),
                status: "True".into(),
                ..Default::default()
            }],
        )
    }

    #[test]
    fn a_ready_pod_short_circuits_every_deadline() {
        let mut w = ReadyWatch::new(Duration::from_secs(1));
        let t0 = Instant::now();
        assert_eq!(w.observe(&ready_pod(), t0 + Duration::from_secs(9_999)), Verdict::Ready);
    }

    #[test]
    fn an_unplaceable_pod_is_named_only_once_pending_timeout_expires() {
        let mut w = ReadyWatch::new(Duration::from_secs(60));
        let unplaceable = with_conditions(
            status(Some("Pending")),
            vec![sched_cond("False", Some("Unschedulable"), Some("0/1 nodes are available"))],
        );
        let t0 = Instant::now();
        assert_eq!(w.observe(&unplaceable, t0), Verdict::Waiting);
        assert_eq!(
            w.observe(&unplaceable, t0 + PENDING_TIMEOUT - Duration::from_secs(1)),
            Verdict::Waiting
        );
        let verdict = w.observe(&unplaceable, t0 + PENDING_TIMEOUT);
        let Verdict::Unschedulable { reason, .. } = verdict else {
            panic!("expected Unschedulable, got {verdict:?}");
        };
        assert!(reason.contains("0/1 nodes are available"), "{reason}");
    }

    /// The clock starts at the *first* unscheduled sample, not at construction
    #[test]
    fn the_pending_clock_starts_on_the_first_sample() {
        let mut w = ReadyWatch::new(Duration::from_secs(60));
        let unplaceable =
            with_conditions(status(Some("Pending")), vec![sched_cond("False", None, None)]);
        let t0 = Instant::now();
        let late = t0 + Duration::from_secs(600);
        assert_eq!(w.observe(&unplaceable, late), Verdict::Waiting);
        assert!(matches!(
            w.observe(&unplaceable, late + PENDING_TIMEOUT),
            Verdict::Unschedulable { .. }
        ));
    }

    #[test]
    fn a_crashloop_fails_fast_with_no_grace() {
        let mut w = ReadyWatch::new(Duration::from_secs(60));
        let crashed = waiting_on("CrashLoopBackOff");
        assert_eq!(
            w.observe(&crashed, Instant::now()),
            Verdict::Faulted("hostpath: CrashLoopBackOff".to_string())
        );
    }

    #[test]
    fn a_pull_error_is_terminal_only_past_the_grace() {
        let mut w = ReadyWatch::new(Duration::from_secs(60));
        let stuck = waiting_on("ImagePullBackOff");
        let t0 = Instant::now();
        assert_eq!(w.observe(&stuck, t0), Verdict::Waiting);
        assert_eq!(
            w.observe(&stuck, t0 + IMAGE_PULL_GRACE),
            Verdict::PullFailed("ImagePullBackOff".to_string())
        );
    }

    /// Kubelet backoff clearing must restart the grace, not resume a spent one
    #[test]
    fn a_cleared_pull_error_resets_the_grace() {
        let mut w = ReadyWatch::new(Duration::from_secs(60));
        let t0 = Instant::now();
        assert_eq!(w.observe(&waiting_on("ImagePullBackOff"), t0), Verdict::Waiting);
        assert_eq!(
            w.observe(&waiting_on("ContainerCreating"), t0 + IMAGE_PULL_GRACE),
            Verdict::Waiting
        );
        assert_eq!(
            w.observe(&waiting_on("ImagePullBackOff"), t0 + IMAGE_PULL_GRACE),
            Verdict::Waiting
        );
    }

    /// Pull time sits before `Running`, so a slow cold pull never trips the ready budget
    #[test]
    fn the_ready_budget_clocks_from_running_not_from_creation() {
        let mut w = ReadyWatch::new(Duration::from_secs(60));
        let t0 = Instant::now();
        assert_eq!(w.observe(&waiting_on("ContainerCreating"), t0), Verdict::Waiting);
        let running =
            with_conditions(status(Some("Running")), vec![sched_cond("True", None, None)]);
        let up = t0 + Duration::from_secs(600);
        assert_eq!(w.observe(&running, up), Verdict::Waiting);
        assert_eq!(
            w.observe(&running, up + Duration::from_secs(60)),
            Verdict::ReadyTimeout(Duration::from_secs(60))
        );
    }
}
