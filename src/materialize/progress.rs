//! Puller liveness + progress, parent-side: what the pod's log says draws the row *and* is the
//! only thing the verdict reads.
//!
//! - Bytes move R2 → node inside the puller pod → its log = the only signal
//! - Transfer phase: rclone JSON stats records (`stats.bytes`, exact, cumulative per pod)
//! - Verify phase: `sha256sum -c` `relpath: OK` lines, relpaths in SHA256SUMS order (sorted)
//! - Verdict = *silence*, never duration (no constant models transfer time — [`STALL_WINDOW`])
//! - Counters clamped monotonic (re-attach backfill replays records harmlessly)
use std::fmt;
use std::time::{Duration, Instant};

use futures::AsyncReadExt as _;
use k8s_openapi::api::core::v1::{Pod, PodStatus};
use kube::Api;
use kube::api::{ListParams, LogParams};

use crate::pod_status;
use crate::progress::StepProgress;

const PRE_RUN_POLL: Duration = Duration::from_millis(500);

const REATTACH_DELAY: Duration = Duration::from_secs(1);

/// Re-attach overlap. Counters clamped monotonic → replay is free, a gap strands the bar
const REATTACH_BACKFILL_SECS: i64 = 10;

/// Cap on an undelimited run held while seeking a record boundary (bounds a
/// never-delimiting producer)
const MAX_RECORD: usize = 64 * 1024;

/// Silence that means stuck — at every payload size, on every cluster.
///
/// - Bounds the *gap between signals*, never the transfer (which no constant can predict)
/// - Widest legit gap: rclone's `--timeout` + retry ladder on one file, or hashing one large file
const STALL_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Puller state the Job would hold forever, so the parent ends it.
///
/// Never a failed pull: a pod that exits nonzero is the Job condition's verdict, reported
/// against its logs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stall {
    Unschedulable { reason: String, elapsed: Duration },
    ImagePull(String),
    NoProgress { transferred: u64, total: u64 },
    Finalizing { total: u64 },
}

impl Stall {
    /// Container ran → its log tail is the diagnostic. Otherwise the reason already is
    pub fn ran(&self) -> bool {
        matches!(self, Stall::NoProgress { .. } | Stall::Finalizing { .. })
    }
}

impl fmt::Display for Stall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mins = STALL_WINDOW.as_secs() / 60;
        match self {
            Stall::Unschedulable { reason, elapsed } => {
                write!(f, "puller unschedulable after {}s — {reason}", elapsed.as_secs())
            }
            Stall::ImagePull(reason) => {
                write!(f, "puller image {reason} — check the node can reach the puller image")
            }
            Stall::NoProgress { transferred, total } => write!(
                f,
                "puller stalled at {}/{} after {mins}m without a byte — check node disk/network",
                human(*transferred),
                human(*total)
            ),
            Stall::Finalizing { total } => write!(
                f,
                "puller took all {} but verified nothing new within {mins}m — check node disk",
                human(*total)
            ),
        }
    }
}

/// Byte count at the scale it lands on (seeds run from a 100 MB cache to a 250 GiB chain)
pub(super) fn human(bytes: u64) -> String {
    for (unit, scale) in [("GiB", 1u64 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)] {
        if bytes >= scale {
            return format!("{:.1} {unit}", bytes as f64 / scale as f64);
        }
    }
    format!("{bytes} B")
}

/// Pre-`Running` deadlines: what the scheduler and kubelet own, before any byte is the
/// puller's to move. Same clocks as [`pod_status::ReadyWatch`], with `Running` as the goal
/// (a Job pod has no readiness probe)
#[derive(Debug, Default)]
struct StartWatch {
    unscheduled_since: Option<Instant>,
    pull_error_since: Option<Instant>,
}

impl StartWatch {
    fn observe(&mut self, status: &PodStatus, now: Instant) -> Option<Stall> {
        if !pod_status::is_scheduled(status) {
            let since = *self.unscheduled_since.get_or_insert(now);
            let elapsed = now.saturating_duration_since(since);
            if elapsed >= pod_status::PENDING_TIMEOUT {
                let reason = pod_status::schedule_blocker(status)
                    .unwrap_or_else(|| "no PodScheduled condition".to_string());
                return Some(Stall::Unschedulable { reason, elapsed });
            }
        }
        match pod_status::image_error(status) {
            Some(reason) => {
                let first = *self.pull_error_since.get_or_insert(now);
                let grace = pod_status::IMAGE_PULL_GRACE;
                pod_status::pull_error_is_terminal(&reason, first, now, grace)
                    .then_some(Stall::ImagePull(reason))
            }
            // Kubelet backoff cleared → the grace restarts, never carries over
            None => {
                self.pull_error_since = None;
                None
            }
        }
    }
}

/// Forward-motion clock: `idle_since` is the whole verdict, and only a rising signal moves it.
/// `base` = bytes earlier pods moved (each pod's rclone counts from zero)
#[derive(Debug)]
struct Liveness {
    base: u64,
    transferred: u64,
    total: u64,
    verified: Option<Vec<u8>>,
    idle_since: Instant,
}

impl Liveness {
    fn new(total: u64, now: Instant) -> Self {
        Self { base: 0, transferred: 0, total, verified: None, idle_since: now }
    }

    /// Fresh pod: its byte count restarts at zero, its verify at the first relpath
    fn next_attempt(&mut self) {
        self.base = self.transferred;
        self.verified = None;
    }

    /// Per-pod byte count, clamped monotonic across the whole pull. `true` = bytes moved
    fn observe(&mut self, count: u64, now: Instant) -> bool {
        let absolute = self.base.saturating_add(count);
        if absolute <= self.transferred {
            return false;
        }
        self.transferred = absolute;
        self.idle_since = now;
        true
    }

    /// Verify line. Relpaths arrive in SHA256SUMS order → only a later one is forward motion
    fn observe_verified(&mut self, relpath: &[u8], now: Instant) -> bool {
        if self.verified.as_deref().is_some_and(|last| relpath <= last) {
            return false;
        }
        self.verified = Some(relpath.to_vec());
        self.idle_since = now;
        true
    }

    fn shown(&self) -> u64 {
        self.transferred.min(self.total)
    }

    fn remaining(&self, now: Instant) -> Duration {
        STALL_WINDOW.saturating_sub(now.saturating_duration_since(self.idle_since))
    }

    fn expired(&self, now: Instant) -> bool {
        self.remaining(now).is_zero()
    }

    /// Whole payload moved (or verifying) = the tail stalled, which reads nothing like a dead link
    fn stall(&self) -> Stall {
        match self.verified.is_some() || self.transferred >= self.total {
            true => Stall::Finalizing { total: self.total },
            false => Stall::NoProgress { transferred: self.transferred, total: self.total },
        }
    }
}

/// Pod being followed, and what its log has left to give
struct Attempt {
    name: String,
    ended: bool,
    resuming: bool,
}

/// Track the puller Job's pod, report progress, **return only to end the pull**.
///
/// Caller races this against the Job's terminal condition, so returning cancels that wait:
/// every [`Stall`] must be a state no Job condition would ever arrive to settle
pub async fn watch_puller(
    pods: &Api<Pod>,
    job_name: &str,
    total: u64,
    progress: &dyn StepProgress,
) -> Stall {
    let mut start = StartWatch::default();
    let mut clock: Option<Liveness> = None;
    let mut attempt: Option<Attempt> = None;

    loop {
        let Some(pod) = puller_pod(pods, job_name).await else {
            progress.note("scheduling puller");
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        };
        let Some(name) = pod.metadata.name.clone() else {
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        };
        // Next pod reruns the copy (complete files skipped) → bar carries on from here
        if attempt.as_ref().is_some_and(|a| a.name != name) {
            if let Some(clock) = clock.as_mut() {
                clock.next_attempt();
            }
            progress.note("resuming pull");
            attempt = None;
        }
        let status = pod.status.clone().unwrap_or_default();

        // Nothing here is the puller's yet — placement and image are the cluster's to answer
        if pod_status::is_pending(&status) {
            if let Some(stall) = start.observe(&status, Instant::now()) {
                return stall;
            }
            progress.note(&pre_run_note(&pod));
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        }

        // Clocked from `Running`: a queued pod must not spend the transfer's silence budget
        let clock = clock.get_or_insert_with(|| Liveness::new(total, Instant::now()));

        // Here, not only inside `follow`: a log stream that never opens (RBAC, evicted pod)
        // reaches no read to time out, and would otherwise re-attach forever
        if clock.expired(Instant::now()) {
            return clock.stall();
        }

        // Log spent: the Job condition's arrival has no signal of its own → same window
        if attempt.as_ref().is_some_and(|a| a.ended) {
            settle(&pod, progress);
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        }

        let backfill = attempt.as_ref().and_then(|a| a.resuming.then_some(REATTACH_BACKFILL_SECS));
        match follow(pods, &name, backfill, progress, clock).await {
            Ok(Followed::Stalled) => return clock.stall(),
            // Clean EOF = the container exited — succeeded *or* died. What remains
            // isn't byte-shaped either way, and the Job's condition is the verdict
            Ok(Followed::Ended) => {
                settle(&pod, progress);
                attempt = Some(Attempt { name, ended: true, resuming: false });
            }
            // Dropped mid-pull (apiserver hiccup / pod gone) — not the parent's to
            // adjudicate. The clock rides through, so a re-attach cannot launder a stall
            Err(e) => {
                tracing::debug!(job = %job_name, error = %e, "puller log dropped; re-attaching");
                progress.note("re-attaching to puller");
                attempt = Some(Attempt { name, ended: false, resuming: true });
                tokio::time::sleep(REATTACH_DELAY).await;
            }
        }
    }
}

/// Why [`follow`] gave the stream back
enum Followed {
    Ended,
    Stalled,
}

/// Post-EOF state of one attempt, onto the row.
///
/// `finalizing` drops the bar, rate and ETA, so it must be reserved for a pull that
/// actually landed — a dead attempt parked there reads as progress for the whole backoff
fn settle(pod: &Pod, progress: &dyn StepProgress) {
    match failure_note(pod) {
        Some(note) => progress.note(&note),
        None => progress.finalizing(),
    }
}

/// Attempt died, in its own exit code.
///
/// - Status trails the log close → absent code is *undecided*, reported as neither
/// - Verdict is the Job's terminal condition; this only keeps the row honest until it lands
fn failure_note(pod: &Pod) -> Option<String> {
    match pod.status.as_ref().and_then(pod_status::exit_code) {
        Some(code) if code != 0 => Some(format!("pull failed (exit {code})")),
        _ => None,
    }
}

/// Job's most recent pod, found by the template's stamped label (ownership is indirect)
async fn puller_pod(pods: &Api<Pod>, job_name: &str) -> Option<Pod> {
    let lp = ListParams::default().labels(&format!("job-name={job_name}"));
    pods.list(&lp).await.ok()?.items.into_iter().next_back()
}

fn pre_run_note(pod: &Pod) -> String {
    let Some(status) = pod.status.as_ref() else {
        return "scheduling puller".to_string();
    };
    if let Some(err) = pod_status::image_error(status) {
        return format!("puller image: {err}");
    }
    let waiting = status
        .container_statuses
        .as_ref()
        .and_then(|cs| cs.first())
        .and_then(|c| c.state.as_ref())
        .and_then(|s| s.waiting.as_ref())
        .and_then(|w| w.reason.as_deref());
    match waiting {
        Some("ContainerCreating" | "PodInitializing") => "starting puller".to_string(),
        Some(reason) => format!("puller {reason}"),
        None => "scheduling puller".to_string(),
    }
}

/// Follow one pod's log until clean end / stall (`Ok`) or drop (`Err`), reporting each signal.
///
/// - `clock` is the caller's, carried across re-attaches: replay never walks the bar
///   backwards, and a re-attach never launders elapsed silence
/// - Read bounded by what the clock has left, not a fixed interval (rclone emits flat stats
///   records over a wedged transfer → a per-read timeout would never fire)
async fn follow(
    pods: &Api<Pod>,
    pod: &str,
    backfill_secs: Option<i64>,
    progress: &dyn StepProgress,
    clock: &mut Liveness,
) -> Result<Followed, kube::Error> {
    let lp = LogParams { follow: true, since_seconds: backfill_secs, ..Default::default() };
    let mut stream = Box::pin(pods.log_stream(pod, &lp).await?);

    progress.note("transferring");
    let mut pending = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = tokio::time::timeout(clock.remaining(Instant::now()), stream.read(&mut chunk));
        let Ok(read) = read.await else {
            return Ok(Followed::Stalled);
        };
        let n = read.map_err(kube::Error::ReadEvents)?;
        if n == 0 {
            return Ok(Followed::Ended);
        }
        pending.extend_from_slice(&chunk[..n]);

        let mut consumed = 0;
        for (i, b) in pending.iter().enumerate() {
            if *b != b'\n' {
                continue;
            }
            let record = &pending[consumed..i];
            if let Some(done) = stats_bytes(record) {
                clock.observe(done, Instant::now());
                progress.bytes(clock.shown(), clock.total);
            } else if let Some(relpath) = verified_path(record) {
                if clock.verified.is_none() {
                    progress.note("verifying");
                }
                clock.observe_verified(relpath, Instant::now());
            }
            consumed = i + 1;
        }
        pending.drain(..consumed);
        if pending.len() > MAX_RECORD {
            pending.clear();
        }
    }
}

/// Cumulative bytes out of an rclone `--use-json-log` stats record, else `None`
pub(super) fn stats_bytes(record: &[u8]) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_slice(record).ok()?;
    v.get("stats")?.get("bytes")?.as_u64()
}

/// Relpath out of a `sha256sum -c` success line (`a/b.sst: OK`), else `None`
pub(super) fn verified_path(record: &[u8]) -> Option<&[u8]> {
    record.strip_suffix(b": OK").filter(|p| !p.is_empty() && !p.starts_with(b"{"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn terminated(exit_code: i32) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "puller-abc" },
            "status": {
                "containerStatuses": [{
                    "name": "puller",
                    "ready": false,
                    "restartCount": 0,
                    "image": "rclone",
                    "imageID": "",
                    "state": { "terminated": { "exitCode": exit_code, "reason": "Error" } },
                }],
            },
        }))
        .expect("valid Pod")
    }

    /// Dead attempt closes its log exactly like a finished one (`finalizing` would hide it)
    #[test]
    fn a_nonzero_exit_is_a_failure_not_a_finalizing_pull() {
        assert_eq!(failure_note(&terminated(2)).as_deref(), Some("pull failed (exit 2)"));
    }

    #[test]
    fn a_clean_exit_leaves_the_row_finalizing() {
        assert_eq!(failure_note(&terminated(0)), None);
    }

    /// Container status trails the log close; an undecided attempt must not be called failed
    #[test]
    fn an_unreported_exit_is_undecided_rather_than_failed() {
        let pending: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "puller-abc" },
            "status": { "phase": "Running" },
        }))
        .expect("valid Pod");
        assert_eq!(failure_note(&pending), None);
    }

    const GB: u64 = 1024 * 1024 * 1024;

    fn scheduled(reason: Option<&str>) -> PodStatus {
        serde_json::from_value(serde_json::json!({
            "phase": "Pending",
            "conditions": [{
                "type": "PodScheduled",
                "status": if reason.is_some() { "False" } else { "True" },
                "reason": reason,
                "message": reason.map(|_| "0/1 nodes are available"),
            }],
        }))
        .expect("valid PodStatus")
    }

    fn pulling(reason: &str) -> PodStatus {
        serde_json::from_value(serde_json::json!({
            "phase": "Pending",
            "conditions": [{ "type": "PodScheduled", "status": "True" }],
            "containerStatuses": [{
                "name": "puller",
                "ready": false,
                "restartCount": 0,
                "image": "rclone",
                "imageID": "",
                "state": { "waiting": { "reason": reason } },
            }],
        }))
        .expect("valid PodStatus")
    }

    /// Slower than any budget ≠ failure: alive as long as bytes keep landing
    #[test]
    fn an_arbitrarily_slow_pull_never_stalls_while_bytes_keep_landing() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(20 * GB, t0);
        let mut at = t0;
        let mut moved = 0;
        for tick in 1..=(10 * 60 * 60) {
            at = t0 + Duration::from_secs(tick);
            moved += 1024 * 1024;
            clock.observe(moved, at);
            assert!(!clock.expired(at), "a moving transfer was called stuck at {tick}s");
        }
        assert!(clock.expired(at + STALL_WINDOW), "silence after the last byte is still a stall");
    }

    /// Flat stats records over a wedged transfer = heartbeat, never forward motion
    #[test]
    fn a_repeated_count_is_a_heartbeat_and_not_progress() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(20 * GB, t0);
        clock.observe(4 * GB, t0);
        for tick in 1..STALL_WINDOW.as_secs() {
            assert!(!clock.observe(4 * GB, t0 + Duration::from_secs(tick)), "flat record moved it");
        }
        assert!(clock.expired(t0 + STALL_WINDOW));
        assert_eq!(clock.stall(), Stall::NoProgress { transferred: 4 * GB, total: 20 * GB });
    }

    /// Re-attach backfill replays counted records; a replay must never push the deadline out
    #[test]
    fn a_replayed_record_cannot_launder_elapsed_silence() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(20 * GB, t0);
        clock.observe(4 * GB, t0);
        let late = t0 + STALL_WINDOW - Duration::from_secs(1);
        assert!(!clock.observe(3 * GB, late), "a lower count reset the clock");
        assert_eq!(clock.transferred, 4 * GB, "the bar walked backwards");
        assert!(clock.expired(t0 + STALL_WINDOW));
    }

    /// Next pod's rclone counts from zero again: the bar carries on instead of freezing
    #[test]
    fn a_resumed_pod_moves_the_bar_from_where_the_last_one_stopped() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(20 * GB, t0);
        clock.observe(4 * GB, t0);
        clock.next_attempt();
        let later = t0 + Duration::from_secs(60);
        assert!(clock.observe(GB, later), "the new pod's first bytes were not progress");
        assert_eq!(clock.transferred, 5 * GB);
        assert_eq!(clock.remaining(later), STALL_WINDOW);
    }

    /// Refetched partial files can overcount; the row never reads past the whole tree
    #[test]
    fn the_bar_never_passes_the_tree_size() {
        let mut clock = Liveness::new(GB, Instant::now());
        clock.observe(2 * GB, Instant::now());
        assert_eq!(clock.shown(), GB);
    }

    /// Verify = hashing only, no bytes move → the sorted relpaths are its forward motion
    #[test]
    fn verification_is_progress_only_in_sums_order() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(GB, t0);
        clock.observe(GB, t0);
        let t1 = t0 + Duration::from_secs(600);
        assert!(clock.observe_verified(b"a/000013.sst", t1));
        assert!(!clock.observe_verified(b"a/000012.sst", t1 + STALL_WINDOW), "replay moved it");
        assert!(clock.expired(t1 + STALL_WINDOW));
        assert_eq!(clock.stall(), Stall::Finalizing { total: GB });
        clock.next_attempt();
        assert!(clock.observe_verified(b"a/000001.sst", t1), "new pod's verify never counted");
    }

    /// Past `PENDING_TIMEOUT` the scheduler's own message is the error
    #[test]
    fn an_unplaceable_puller_fails_with_the_schedulers_reason() {
        let t0 = Instant::now();
        let mut start = StartWatch::default();
        let status = scheduled(Some("Unschedulable"));
        assert_eq!(start.observe(&status, t0), None);
        let late = t0 + pod_status::PENDING_TIMEOUT;
        let Some(Stall::Unschedulable { reason, .. }) = start.observe(&status, late) else {
            panic!("an unplaceable puller was not reported");
        };
        assert!(reason.contains("0/1 nodes are available"), "{reason}");
    }

    #[test]
    fn a_placed_puller_never_trips_the_pending_clock() {
        let t0 = Instant::now();
        let mut start = StartWatch::default();
        let status = scheduled(None);
        assert_eq!(start.observe(&status, t0), None);
        assert_eq!(start.observe(&status, t0 + pod_status::PENDING_TIMEOUT * 10), None);
    }

    /// Kubelet backoff clears a transient pull storm → only a *persisting* error ends the wait
    #[test]
    fn a_transient_image_pull_error_is_waited_out_and_a_persisting_one_is_not() {
        let t0 = Instant::now();
        let mut start = StartWatch::default();
        assert_eq!(start.observe(&pulling("ImagePullBackOff"), t0), None);
        assert_eq!(start.observe(&scheduled(None), t0 + pod_status::IMAGE_PULL_GRACE), None);

        let mut start = StartWatch::default();
        assert_eq!(start.observe(&pulling("ErrImagePull"), t0), None);
        assert_eq!(
            start.observe(&pulling("ErrImagePull"), t0 + pod_status::IMAGE_PULL_GRACE),
            Some(Stall::ImagePull("ErrImagePull".to_string()))
        );
    }

    #[test]
    fn a_byte_count_is_named_at_the_scale_it_lands_on() {
        assert_eq!(human(20 * GB), "20.0 GiB");
        assert_eq!(human(100 * 1024 * 1024), "100.0 MiB");
        assert_eq!(human(4096), "4.0 KiB");
        assert_eq!(human(17), "17 B");
    }

    /// Unplaceable pod has no log; its reason already carries the scheduler's words
    #[test]
    fn only_a_stall_that_ran_has_a_log_worth_quoting() {
        assert!(Stall::NoProgress { transferred: 0, total: GB }.ran());
        assert!(Stall::Finalizing { total: GB }.ran());
        assert!(!Stall::ImagePull("ErrImagePull".into()).ran());
        assert!(!Stall::Unschedulable { reason: "x".into(), elapsed: STALL_WINDOW }.ran());
    }

    #[test]
    fn every_stall_reads_as_a_fact_and_an_action() {
        let stalls = [
            Stall::Unschedulable { reason: "Unschedulable: no disk".into(), elapsed: STALL_WINDOW },
            Stall::ImagePull("ImagePullBackOff".into()),
            Stall::NoProgress { transferred: 4 * GB, total: 20 * GB },
            Stall::Finalizing { total: 20 * GB },
        ];
        for stall in stalls {
            let msg = stall.to_string();
            assert!(!msg.contains('\n'), "{msg}");
            assert!(msg.starts_with("puller "), "{msg}");
        }
    }

    /// Verbatim rclone 1.75 `--use-json-log --stats-one-line` record
    #[test]
    fn a_stats_record_yields_its_cumulative_byte_count() {
        let record = br#"{"time":"2026-09-29T18:50:39.466706624-07:00","level":"notice","msg":"   10.027 MiB / 28.610 MiB, 35%, 5.027 MiB/s, ETA 3s\n","stats":{"bytes":10514435,"checks":0,"totalBytes":30000003,"transfers":1},"source":"accounting/stats.go:551"}"#;
        assert_eq!(stats_bytes(record), Some(10514435));
    }

    /// rclone errors are JSON too; sha256sum lines are not → neither reads as a count
    #[test]
    fn non_stats_output_is_not_a_count() {
        for line in [
            &br#"{"level":"error","msg":"a/b.sst: Failed to copy: 404 Not Found"}"#[..],
            b"a/b.sst: OK",
            b"SHA256SUMS hashes to 00, manifest says 11",
            b"",
            b"1024",
        ] {
            assert_eq!(stats_bytes(line), None, "{}", String::from_utf8_lossy(line));
        }
    }

    #[test]
    fn only_a_passing_verify_line_names_a_verified_path() {
        assert_eq!(
            verified_path(b"state/v28/mainnet/000013.sst: OK"),
            Some(&b"state/v28/mainnet/000013.sst"[..])
        );
        for line in [&b"a/x.log: FAILED"[..], b": OK", br#"{"msg":"x: OK"}"#, b"sha256sum: WARNING"]
        {
            assert_eq!(verified_path(line), None, "{}", String::from_utf8_lossy(line));
        }
    }
}
