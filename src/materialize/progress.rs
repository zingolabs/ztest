//! Live progress for a seed pull, from the parent's point of view.
//!
//! The bytes move inside the puller pod ([`super::puller_cmd`]) — R2 → node,
//! never through this process — so the parent can only learn a byte count if the
//! pod says one out loud. It does: `dd status=progress` sits mid-pipe and writes
//! a running total to stderr, which the pod log carries here.
//!
//! `pv -n` is the tool built for this job and would be the first choice, but the
//! puller image has to *contain* it (installing at pod start is impossible under
//! `restricted-v2` — see [`super::detect_puller_image`]) and no widely-available
//! base ships `pv`. `dd` is coreutils, so it is present wherever `tar` is, and
//! its `status=progress` counter is the same signal.
//!
//! Two details of that counter drive the shape of this module:
//!
//! - It is **absolute**, not incremental, so a reconnect that replays old
//!   records is harmless as long as the count is clamped monotonic.
//! - It is **`\r`-separated**. A CRI log writer only emits a partial record when
//!   its buffer fills, so `\r`-only output would arrive in ~16 KiB batches —
//!   minutes of silence. [`super::puller_cmd`] therefore pipes the pod's stderr
//!   through `stdbuf -oL tr '\r' '\n'`, and this module splits on either.

use std::time::Duration;

use futures::AsyncReadExt as _;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{ListParams, LogParams};

use crate::pod_status;

/// How often the pod is re-listed while it is still scheduling or pulling.
const PRE_RUN_POLL: Duration = Duration::from_millis(500);

/// How long to wait before re-attaching after the log stream drops mid-pull.
const REATTACH_DELAY: Duration = Duration::from_secs(1);

/// Overlap accepted on re-attach. The counter is absolute and clamped monotonic
/// below, so replayed records cost nothing while a gap would strand the bar.
const REATTACH_BACKFILL_SECS: i64 = 10;

/// Cap on an undelimited run of log bytes held while looking for a record
/// boundary. A producer that never delimits must not grow the buffer without
/// bound; nothing this parses is anywhere near this long.
const MAX_RECORD: usize = 64 * 1024;

/// Where a seed's provisioning progress is reported.
///
/// Deliberately narrower than [`ProgressSink`](crate::resource::ProgressSink):
/// this side of the codebase has no [`NodeId`](crate::resource::NodeId) to key
/// on, and binding one is the resource node's job.
pub trait SeedProgress: Send + Sync {
    /// The current sub-phase, as a short lowercase phrase.
    fn stage(&self, note: &str);
    /// Absolute bytes transferred against the manifest's blob size.
    fn bytes(&self, done: u64, total: u64);
    /// Bytes are all in; a tail step (extract, snapshot) is still running.
    fn finalizing(&self);
}

/// The reporter for callers with nowhere to report: the test side
/// ([`await_seed`](super::await_seed)) and every non-TTY run.
pub struct Silent;

impl SeedProgress for Silent {
    fn stage(&self, _note: &str) {}
    fn bytes(&self, _done: u64, _total: u64) {}
    fn finalizing(&self) {}
}

/// Track the puller Job's pod and report its progress until cancelled.
///
/// **Never returns**, by construction: the caller races this against the Job's
/// terminal condition, so resolving would cancel the wait that actually decides
/// the pull's outcome. Every path through the loop either reports or sleeps.
pub(crate) async fn watch_puller(
    pods: &Api<Pod>,
    job_name: &str,
    total: u64,
    progress: &dyn SeedProgress,
) {
    let mut transferred = 0u64;
    // The pod whose log has already ended. A container closes its log exactly
    // once, so this is what separates "the payload is in, waiting on the Job to
    // settle" from "`backoffLimit` has started a fresh attempt".
    let mut exhausted: Option<String> = None;
    // Whether the next attach resumes the pod we were already following (so the
    // backfill window applies) rather than opening a new one.
    let mut resuming = false;

    loop {
        let Some(pod) = puller_pod(pods, job_name).await else {
            progress.stage("scheduling puller");
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        };
        let Some(name) = pod.metadata.name.clone() else {
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        };

        if exhausted.as_deref() == Some(name.as_str()) {
            // Nothing further to report: the bytes are in and the Job's terminal
            // condition — which the caller races this against — is the only
            // thing still outstanding.
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        }
        if exhausted.take().is_some() {
            // A *different* pod, so the Job retried. The previous attempt's
            // count describes a transfer that no longer exists; carrying it
            // would gate every byte of this one behind a mark it must beat.
            transferred = 0;
            resuming = false;
            progress.stage("retrying pull");
        }

        if !pod.status.as_ref().is_some_and(pod_status::is_running) {
            progress.stage(&pre_run_note(&pod));
            tokio::time::sleep(PRE_RUN_POLL).await;
            continue;
        }

        let backfill = resuming.then_some(REATTACH_BACKFILL_SECS);
        match follow(pods, &name, total, backfill, progress, &mut transferred).await {
            // Clean EOF: the container closed its log, so its payload is in and
            // whatever remains (the extract tail, mode normalization, the Job's
            // own terminal condition) is not byte-shaped.
            Ok(()) => {
                progress.finalizing();
                exhausted = Some(name);
                resuming = false;
            }
            // The stream dropped with the pull still live — an apiserver hiccup,
            // or the pod going away under us. Neither is the parent's to
            // adjudicate: the Job's terminal condition remains the verdict, so
            // re-attach and keep the bar honest in the meantime.
            Err(e) => {
                tracing::debug!(job = %job_name, error = %e, "puller log follow dropped; re-attaching");
                progress.stage("re-attaching to puller");
                resuming = true;
                tokio::time::sleep(REATTACH_DELAY).await;
            }
        }
    }
}

/// The Job's most recent pod. A Job owns its pods indirectly, so they are found
/// by the label the template stamps.
async fn puller_pod(pods: &Api<Pod>, job_name: &str) -> Option<Pod> {
    let lp = ListParams::default().labels(&format!("job-name={job_name}"));
    pods.list(&lp).await.ok()?.items.into_iter().next_back()
}

/// What a not-yet-running puller is waiting on, as a sub-phase phrase.
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

/// Follow one pod's log, reporting every byte count it announces, until the
/// stream ends cleanly (`Ok`) or drops (`Err`).
///
/// `transferred` is the caller's monotonic high-water mark, carried across
/// re-attaches so a replayed record can never walk the bar backwards.
async fn follow(
    pods: &Api<Pod>,
    pod: &str,
    total: u64,
    backfill_secs: Option<i64>,
    progress: &dyn SeedProgress,
    transferred: &mut u64,
) -> Result<(), kube::Error> {
    let lp = LogParams {
        follow: true,
        since_seconds: backfill_secs,
        ..Default::default()
    };
    let mut stream = Box::pin(pods.log_stream(pod, &lp).await?);

    progress.stage("transferring");
    let mut pending = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(kube::Error::ReadEvents)?;
        if n == 0 {
            return Ok(());
        }
        pending.extend_from_slice(&chunk[..n]);

        let mut consumed = 0;
        for (i, b) in pending.iter().enumerate() {
            if !matches!(b, b'\r' | b'\n') {
                continue;
            }
            if let Some(done) = dd_bytes(&pending[consumed..i])
                && done > *transferred
            {
                *transferred = done;
                progress.bytes(done, total);
            }
            consumed = i + 1;
        }
        pending.drain(..consumed);
        if pending.len() > MAX_RECORD {
            pending.clear();
        }
    }
}

/// The byte count from a `dd status=progress` record, if that is what this is.
///
/// `314572800 bytes (315 MB, 300 MiB) copied, 12 s, 26.2 MB/s` → `314572800`.
/// Every other line the puller emits (curl's errors, tar's complaints) fails the
/// `bytes` check and is ignored here — the failure path reads the log in full.
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

    /// Everything else on the puller's merged stderr must fall through, or a
    /// `curl` diagnostic would be read as a byte count.
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
