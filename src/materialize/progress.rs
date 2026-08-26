//! Puller liveness + progress, parent-side: one byte counter draws the row *and* is the
//! only thing the verdict reads.
//!
//! - Bytes move R2 → node inside the puller pod, so the only signal is what its log says
//! - `dd status=progress` mid-pipe = that signal (`pv` unavailable: puller image cannot
//!   install at pod start under `restricted-v2`, no common base ships it)
//! - Mid-pipe = backpressure, so the counter advances only as `tar` consumes → one number
//!   covers link, decompress and volume writes alike
//! - Verdict is *silence*, never duration (no constant models transfer time — [`STALL_WINDOW`]);
//!   a sub-floor trickle is `curl`'s to kill per-range, in the pod, not this module's
//! - Counter absolute, not incremental (replayed records harmless once clamped monotonic)
//! - Every record reported, not only rising ones (`dd` emits ~1/s regardless → flat
//!   reports = the heartbeat a rate window needs to decay a stall to zero)
//! - Counter `\r`-separated → [`super::puller_cmd`] pipes through `stdbuf -oL tr '\r' '\n'`
//!   (CRI batches an undelimited stream into ~16 KiB = minutes of silence); split on either
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

/// Re-attach overlap. Counter absolute + clamped monotonic → replay is free, a gap strands the bar
const REATTACH_BACKFILL_SECS: i64 = 10;

/// Cap on an undelimited run held while seeking a record boundary (bounds a
/// never-delimiting producer)
const MAX_RECORD: usize = 64 * 1024;

/// Silence that means stuck — at every payload size, on every cluster.
///
/// - Bounds the *gap between signals*, never the transfer (which no constant can predict)
/// - Widest legit gap is `curl`'s own: a chunk redialed through its ladder (≈6m) or landing
///   at the `--speed-limit` floor (256 MiB ≈ 4m)
/// - Post-transfer it bounds the counter-less tail (mode normalize + digest join + Job
///   condition) — work sized by file count and buffers, not by bytes
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
    Restarted { transferred: u64 },
}

impl Stall {
    /// Container ran → its log tail is the diagnostic. Otherwise the reason already is
    pub fn ran(&self) -> bool {
        matches!(
            self,
            Stall::NoProgress { .. } | Stall::Finalizing { .. } | Stall::Restarted { .. }
        )
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
                "puller took all {} but did not finish extracting within {mins}m — check node disk",
                human(*total)
            ),
            Stall::Restarted { transferred } => write!(
                f,
                "puller restarted after {} — the transfer cannot resume, so the pull is abandoned",
                human(*transferred)
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

/// Forward-motion clock: `idle_since` is the whole verdict, and only a rising counter
/// moves it
#[derive(Debug)]
struct Liveness {
    /// Absolute offset the counter is relative to; `dd` restarts at every frame
    base: u64,
    transferred: u64,
    total: u64,
    idle_since: Instant,
}

impl Liveness {
    fn new(total: u64, now: Instant) -> Self {
        Self { base: 0, transferred: 0, total, idle_since: now }
    }

    /// Segment boundary: following counts restart from zero against this offset
    fn rebase(&mut self, base: u64) {
        self.base = base;
    }

    /// Meter count, clamped monotonic against the whole object. `true` = bytes moved
    fn observe(&mut self, count: u64, now: Instant) -> bool {
        let absolute = self.base.saturating_add(count);
        if absolute <= self.transferred {
            return false;
        }
        self.transferred = absolute;
        self.idle_since = now;
        true
    }

    fn remaining(&self, now: Instant) -> Duration {
        STALL_WINDOW.saturating_sub(now.saturating_duration_since(self.idle_since))
    }

    fn expired(&self, now: Instant) -> bool {
        self.remaining(now).is_zero()
    }

    /// Whole payload metered = the tail is what stalled, and it reads nothing like a dead link
    fn stall(&self) -> Stall {
        match self.transferred >= self.total {
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
/// - Caller races this against the Job's terminal condition, so returning cancels that wait:
///   every [`Stall`] must be a state no Job condition would ever arrive to settle
/// - `resumable` = the object carries a frame table, so a fresh pod continues off the
///   on-volume marker rather than starting the transfer again
pub async fn watch_puller(
    pods: &Api<Pod>,
    job_name: &str,
    total: u64,
    resumable: bool,
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
        // Second pod resumes off the marker (segmented) or restarts at byte 0 (not) — only the
        // second walks the count backwards out from under every window here
        if attempt.as_ref().is_some_and(|a| a.name != name) {
            if !resumable {
                let transferred = clock.as_ref().map_or(0, |c| c.transferred);
                return Stall::Restarted { transferred };
            }
            // Counter is absolute and clamped, and the new pod re-announces its offset →
            // nothing to reset, the bar simply carries on from where the marker left it
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

        // Log spent: the tail (extract flush, mode normalize, digest, Job condition) has no
        // counter of its own, so the same window bounds it
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

/// Follow one pod's log until clean end / stall (`Ok`) or drop (`Err`), reporting each count.
///
/// - `clock` is the caller's, carried across re-attaches: replay never walks the bar
///   backwards, and a re-attach never launders elapsed silence
/// - Read is bounded by what the clock has left, not a fixed interval — `dd` chatters ~1/s
///   over a wedged extract, so a per-read timeout would never fire
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
            if !matches!(b, b'\r' | b'\n') {
                continue;
            }
            let record = &pending[consumed..i];
            if let Some(base) = base_mark(record) {
                clock.rebase(base);
            } else if let Some(done) = dd_bytes(record) {
                clock.observe(done, Instant::now());
                progress.bytes(clock.transferred, clock.total);
            }
            consumed = i + 1;
        }
        pending.drain(..consumed);
        if pending.len() > MAX_RECORD {
            pending.clear();
        }
    }
}

/// Absolute offset out of a segment marker, else `None`.
///
/// Emitted by the puller once a frame, because the meter's own count restarts with it
fn base_mark(record: &[u8]) -> Option<u64> {
    let mut fields = std::str::from_utf8(record).ok()?.split_ascii_whitespace();
    (fields.next()? == super::BASE_MARK).then(|| fields.next()?.parse().ok())?
}

/// Byte count out of a `dd status=progress` record, else `None`.
///
/// - `314572800 bytes (315 MB, 300 MiB) copied, 12 s, 26.2 MB/s` → `314572800`
/// - Other puller output fails the `bytes` check (failure path reads the log in full)
fn dd_bytes(record: &[u8]) -> Option<u64> {
    let mut fields = std::str::from_utf8(record).ok()?.split_ascii_whitespace();
    let count = fields.next()?.parse().ok()?;
    (fields.next()? == "bytes").then_some(count)
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
                    "image": "fedora:40",
                    "imageID": "",
                    "state": { "terminated": { "exitCode": exit_code, "reason": "Error" } },
                }],
            },
        }))
        .expect("valid Pod")
    }

    /// The stuck-at-`finalizing…` bug: a dead attempt closes its log exactly like a
    /// finished one, and `finalizing` drops the bar/rate/ETA until the Job gives up
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
                "image": "fedora:40",
                "imageID": "",
                "state": { "waiting": { "reason": reason } },
            }],
        }))
        .expect("valid PodStatus")
    }

    /// The whole point of the rewrite: a pull slower than any budget anyone would have
    /// guessed is *not* a failure. 20 GiB at ~1 MiB/s outruns every wall-clock deadline
    /// this module used to impose, and must still be alive as long as bytes keep landing
    #[test]
    fn an_arbitrarily_slow_pull_never_stalls_while_bytes_keep_landing() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(20 * GB, t0);
        let mut at = t0;
        let mut moved = 0;
        // 10 hours, a megabyte at a time — far past any budget a throughput constant yields
        for tick in 1..=(10 * 60 * 60) {
            at = t0 + Duration::from_secs(tick);
            moved += 1024 * 1024;
            clock.observe(moved, at);
            assert!(!clock.expired(at), "a moving transfer was called stuck at {tick}s");
        }
        assert!(clock.expired(at + STALL_WINDOW), "silence after the last byte is still a stall");
    }

    /// `dd` chatters ~1/s over a wedged extract, so the verdict must read the *count*, not
    /// the record. A repeated total is a heartbeat, never forward motion
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

    /// Re-attach backfills the last 10s, so records already counted arrive a second time.
    /// Clamped monotonic or not, a replay must never push the stall deadline out
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

    /// Post-transfer there is no counter at all (extract flush, mode normalize, digest), so
    /// the same window bounds the tail — but it must not be reported as a dead link
    #[test]
    fn silence_after_the_last_byte_is_named_as_the_tail_not_the_transfer() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(20 * GB, t0);
        clock.observe(20 * GB, t0);
        assert!(!clock.expired(t0 + STALL_WINDOW - Duration::from_secs(1)));
        assert!(clock.expired(t0 + STALL_WINDOW));
        assert_eq!(clock.stall(), Stall::Finalizing { total: 20 * GB });
    }

    /// Placement is the cluster's answer, not a duration to infer: past `PENDING_TIMEOUT`
    /// the scheduler's own message is the error, rather than "did not finish"
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

    /// A placed pod's clock never starts — the old code burned the whole budget here
    #[test]
    fn a_placed_puller_never_trips_the_pending_clock() {
        let t0 = Instant::now();
        let mut start = StartWatch::default();
        let status = scheduled(None);
        assert_eq!(start.observe(&status, t0), None);
        assert_eq!(start.observe(&status, t0 + pod_status::PENDING_TIMEOUT * 10), None);
    }

    /// Kubelet backoff clears a transient pull storm, so the grace must survive it and only
    /// a *persisting* error ends the wait
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

    /// A 100 MB cache and a 250 GiB chain are both seeds; "0.1 GiB" names neither well
    #[test]
    fn a_byte_count_is_named_at_the_scale_it_lands_on() {
        assert_eq!(human(20 * GB), "20.0 GiB");
        assert_eq!(human(100 * 1024 * 1024), "100.0 MiB");
        assert_eq!(human(4096), "4.0 KiB");
        assert_eq!(human(17), "17 B");
    }

    /// The tail is the diagnostic only where a container reached the point of writing one;
    /// an unplaceable pod has no log, and its reason already carries the scheduler's words
    #[test]
    fn only_a_stall_that_ran_has_a_log_worth_quoting() {
        assert!(Stall::NoProgress { transferred: 0, total: GB }.ran());
        assert!(Stall::Finalizing { total: GB }.ran());
        assert!(Stall::Restarted { transferred: GB }.ran());
        assert!(!Stall::ImagePull("ErrImagePull".into()).ran());
        assert!(!Stall::Unschedulable { reason: "x".into(), elapsed: STALL_WINDOW }.ran());
    }

    /// Every message names what to go look at, on one line
    #[test]
    fn every_stall_reads_as_a_fact_and_an_action() {
        let stalls = [
            Stall::Unschedulable { reason: "Unschedulable: no disk".into(), elapsed: STALL_WINDOW },
            Stall::ImagePull("ImagePullBackOff".into()),
            Stall::NoProgress { transferred: 4 * GB, total: 20 * GB },
            Stall::Finalizing { total: 20 * GB },
            Stall::Restarted { transferred: 4 * GB },
        ];
        for stall in stalls {
            let msg = stall.to_string();
            assert!(!msg.contains('\n'), "{msg}");
            assert!(msg.starts_with("puller "), "{msg}");
        }
    }

    #[test]
    fn a_progress_record_yields_its_absolute_count() {
        assert_eq!(
            dd_bytes(b"314572800 bytes (315 MB, 300 MiB) copied, 12 s, 26.2 MB/s"),
            Some(314572800)
        );
    }

    #[test]
    fn the_final_summary_record_is_read_the_same_way() {
        assert_eq!(
            dd_bytes(b"4194304000 bytes (4.2 GB, 3.9 GiB) copied, 231.4 s, 18.1 MB/s"),
            Some(4194304000)
        );
    }

    /// Everything else on the merged stderr must fall through (else a `curl`
    /// diagnostic reads as a byte count)
    #[test]
    fn non_progress_output_is_not_a_count() {
        for line in [
            &b"4000+0 records in"[..],
            b"curl: (22) The requested URL returned error: 403",
            b"tar: Unexpected EOF in archive",
            b"",
            b"1024",
            b"1024 records",
        ] {
            assert_eq!(dd_bytes(line), None, "{}", String::from_utf8_lossy(line));
        }
    }
}
