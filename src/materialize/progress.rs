//! Live seed-pull progress, parent-side.
//!
//! - Bytes move R2 → node inside the puller pod, so the only signal is what its log says
//! - `dd status=progress` mid-pipe = that signal (`pv` unavailable: puller image cannot
//!   install at pod start under `restricted-v2`, no common base ships it)
//! - Counter absolute, not incremental (replayed records harmless once clamped monotonic)
//! - Every record reported, not only rising ones (`dd` emits ~1/s regardless → flat
//!   reports = the heartbeat a rate window needs to decay a stall to zero)
//! - Counter `\r`-separated → [`super::puller_cmd`] pipes through `stdbuf -oL tr '\r' '\n'`
//!   (CRI batches an undelimited stream into ~16 KiB = minutes of silence); split on either
use std::time::Duration;

use futures::AsyncReadExt as _;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{ListParams, LogParams};

use crate::pod_status;
use crate::resource::NodeProgress;

const PRE_RUN_POLL: Duration = Duration::from_millis(500);

const REATTACH_DELAY: Duration = Duration::from_secs(1);

/// Re-attach overlap. Counter absolute + clamped monotonic → replay is free, a gap strands the bar
const REATTACH_BACKFILL_SECS: i64 = 10;

/// Cap on an undelimited run held while seeking a record boundary (bounds a
/// never-delimiting producer)
const MAX_RECORD: usize = 64 * 1024;

/// Track the puller Job's pod, report progress until cancelled.
///
/// **Never returns**: caller races this against the Job's terminal condition
/// (resolving would cancel the wait that decides the outcome)
pub(crate) async fn watch_puller(
    pods: &Api<Pod>,
    job_name: &str,
    total: u64,
    progress: &NodeProgress,
) {
    let mut transferred = 0u64;
    // Pod whose log already ended. Closed exactly once → separates "payload in,
    // Job settling" from "`backoffLimit` started a fresh attempt"
    let mut exhausted: Option<String> = None;
    // Next attach resumes the same pod (backfill window applies) vs opens a new one
    let mut resuming = false;

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

        if exhausted.as_deref() == Some(name.as_str()) {
            // Bytes in; only the Job's terminal condition (the caller's race) outstanding
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        }
        if exhausted.take().is_some() {
            // Different pod = Job retried (carrying the old count would gate
            // every byte behind a mark it must beat)
            transferred = 0;
            resuming = false;
            progress.note("retrying pull");
        }

        if !pod.status.as_ref().is_some_and(pod_status::is_running) {
            progress.note(pre_run_note(&pod));
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        }

        let backfill = resuming.then_some(REATTACH_BACKFILL_SECS);
        match follow(pods, &name, total, backfill, progress, &mut transferred).await {
            // Clean EOF = payload in; what remains (extract tail, Job condition) isn't byte-shaped
            Ok(()) => {
                progress.finalizing();
                exhausted = Some(name);
                resuming = false;
            }
            // Dropped mid-pull (apiserver hiccup / pod gone) — not the parent's to
            // adjudicate, so re-attach and let the Job's condition stay the verdict
            Err(e) => {
                tracing::debug!(job = %job_name, error = %e, "puller log follow dropped; re-attaching");
                progress.note("re-attaching to puller");
                resuming = true;
                tokio::time::sleep(REATTACH_DELAY).await;
            }
        }
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

/// Follow one pod's log until clean end (`Ok`) or drop (`Err`), reporting each count.
///
/// `transferred` = caller's high-water mark, carried across re-attaches (replay
/// can never walk the bar backwards)
async fn follow(
    pods: &Api<Pod>,
    pod: &str,
    total: u64,
    backfill_secs: Option<i64>,
    progress: &NodeProgress,
    transferred: &mut u64,
) -> Result<(), kube::Error> {
    let lp = LogParams { follow: true, since_seconds: backfill_secs, ..Default::default() };
    let mut stream = Box::pin(pods.log_stream(pod, &lp).await?);

    progress.note("transferring");
    let mut pending = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream.read(&mut chunk).await.map_err(kube::Error::ReadEvents)?;
        if n == 0 {
            return Ok(());
        }
        pending.extend_from_slice(&chunk[..n]);

        let mut consumed = 0;
        for (i, b) in pending.iter().enumerate() {
            if !matches!(b, b'\r' | b'\n') {
                continue;
            }
            if let Some(done) = dd_bytes(&pending[consumed..i]) {
                *transferred = (*transferred).max(done);
                progress.bytes(*transferred, total);
            }
            consumed = i + 1;
        }
        pending.drain(..consumed);
        if pending.len() > MAX_RECORD {
            pending.clear();
        }
    }
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
