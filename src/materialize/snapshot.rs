//! Snapshot liveness, parent-side: verdict = motion, never duration.
//!
//! - CSI carries no progress (`CreateSnapshotResponse` = `ready_to_use` bool, VolumeSnapshot
//!   `status` = readiness + error) → readiness alone cannot split "copying" from "wedged"
//! - Copying drivers outrun csi-snapshotter `--timeout` (1m default, unset upstream), so a
//!   `DeadlineExceeded` here means "still copying", never "failed"
//! - Retries are NOT a heartbeat: the sidecar drops an error identical to the one already stored
//!   (`updateContentErrorStatusWithEvent`), so a storm surfaces as one stamp, not a stream
//! - csi-hostpath meters: `tar czf` grows one `<uuid>.snap` under `--statedir`, exec-readable
//!   (alpine + coreutils image, local clusters only). Every other driver watches blind
//! - Metered = no outer bound, silence is the whole verdict (no constant models a copy — same
//!   argument as [`super::materialize`])
//! - Blind = [`BLIND_STALL`] *is* an outer bound, honestly: with no byte channel there is
//!   nothing to be silent against. Sound because every unmeterable driver ztest supports is CoW
//!   (topolvm, ceph-rbd), where `readyToUse` flips in seconds and 20m of nothing is pathological

use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, AttachParams, DynamicObject, ListParams};
use tokio::io::AsyncReadExt as _;

use super::progress::human;
use crate::progress::StepProgress;
use crate::seeds::{SEEDS_NAMESPACE, volume_snapshot_gvk};

/// Driver whose snapshot is a foreground copy this module can meter
const HOSTPATH_DRIVER: &str = "hostpath.csi.k8s.io";

/// Upstream `deploy.sh` pins both (namespace + selector), so neither is ours to configure
const PLUGIN_NAMESPACE: &str = "default";
const PLUGIN_SELECTOR: &str = "app.kubernetes.io/name=csi-hostpathplugin";
const PLUGIN_CONTAINER: &str = "hostpath";

/// Tick for both channels. Each metered tick is a `pods/exec` (WS upgrade + a process in the
/// driver container), so the rate is the apiserver's cost, not the window's need — a 10m window
/// is not served any better by 12x the sessions
const SAMPLE: Duration = Duration::from_secs(10);

/// Silence that means stuck, metered: `tar czf` grows the file continuously, so a gap this
/// wide is the node's disk, not the copy's shape
const METERED_STALL: Duration = Duration::from_secs(10 * 60);

/// Silence that means stuck, blind. Floor is the sidecar's own retry ladder — csi-snapshotter
/// `--timeout` (1m) + `retry-interval-max` (5m) — so anything under 6m reads normal backoff as
/// a hang. Doubled again for a controller resync
const BLIND_STALL: Duration = Duration::from_secs(20 * 60);

/// CSI codes no retry clears — spending the window on them only delays the same verdict.
///
/// - Grounded, not guessed: each is one a `CreateSnapshot` actually returns
/// - `Unimplemented` is absent because it cannot arrive — `check_seed_support` refuses a driver
///   without snapshot support before any of this runs
/// - Anything unlisted keeps waiting, which is the safe default: a wrong "fatal" aborts a healthy
///   copy, a wrong "retryable" only costs the window
const FATAL_CODES: [&str; 4] =
    ["InvalidArgument", "OutOfRange", "PermissionDenied", "AlreadyExists"];

/// Driver-level exhaustion, below the gRPC code (hostpath writes the snapshot beside its
/// source → the copy doubles the node's footprint)
const FATAL_TEXT: [&str; 2] = ["no space left on device", "quota exceeded"];

/// Why the wait ended without a snapshot. Every variant = a state no readiness would arrive
/// to settle
#[derive(Debug)]
pub enum SnapFault {
    Unbound { last: Option<String> },
    NoProgress { copied: u64, total: u64 },
    Silent { last: Option<String> },
    Driver(String),
}

impl std::fmt::Display for SnapFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapFault::Unbound { last } => write!(
                f,
                "snapshot never reached a driver after {}m — check the VolumeSnapshotClass and \
                 snapshot-controller{}",
                BLIND_STALL.as_secs() / 60,
                suffix(last)
            ),
            SnapFault::NoProgress { copied, total } => write!(
                f,
                "snapshot copy stalled at {} after {}m without a byte — check node disk",
                fraction(*copied, *total),
                METERED_STALL.as_secs() / 60
            ),
            SnapFault::Silent { last } => write!(
                f,
                "snapshot made no progress for {}m — check the CSI driver pod{}",
                BLIND_STALL.as_secs() / 60,
                suffix(last)
            ),
            SnapFault::Driver(msg) => write!(f, "snapshot refused by the driver: {msg}"),
        }
    }
}

fn suffix(last: &Option<String>) -> String {
    last.as_deref().map(|m| format!("; last said: {m}")).unwrap_or_default()
}

/// Denominator is the bucket object, whose codec is not `tar czf`'s → approximate by design,
/// and dropped rather than shown wrong once the copy runs past it
fn fraction(copied: u64, total: u64) -> String {
    match total > 0 && copied <= total {
        true => format!("{}/~{}", human(copied), human(total)),
        false => human(copied),
    }
}

/// Newest `.snap` size = the copy in flight.
///
/// - Driver's global mutex serialises `CreateSnapshot`, so at most one file grows cluster-wide
/// - `<uuid>.snap` name is minted inside the RPC + published only on success → mtime, not name,
///   is what identifies it mid-copy
/// - Completed siblings sort below it (their mtime froze at their own last write)
/// - Path = `--statedir` default, where the driver keeps volumes and snapshots side by side
const NEWEST_SNAP: &str =
    "stat -c '%Y %n %s' /csi-data-dir/*.snap 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2,3";

/// One `.snap` as the meter sees it. Name rides along because size alone cannot tell our copy
/// from a finished sibling — a previous seed's snapshot is the newest file until our `tar`
/// makes its first write, and its size is a ceiling ours would have to climb past to look alive
#[derive(Debug, PartialEq, Eq)]
struct Snap {
    name: String,
    bytes: u64,
}

/// Byte view of the copy, where the driver affords one
enum Meter {
    HostPath { pods: Api<Pod>, pod: String },
    Blind,
}

impl Meter {
    /// Blind unless the driver is one whose snapshot is a foreground copy *and* its pod is
    /// reachable — a probe that cannot attach must not become the verdict
    async fn attach(client: &Client, driver: &str) -> Self {
        if driver != HOSTPATH_DRIVER {
            return Meter::Blind;
        }
        let pods: Api<Pod> = Api::namespaced(client.clone(), PLUGIN_NAMESPACE);
        let params = ListParams::default().labels(PLUGIN_SELECTOR);
        let found = pods.list(&params).await.ok().and_then(|l| {
            l.items.into_iter().find_map(|p| p.metadata.name).filter(|n| !n.is_empty())
        });
        match found {
            Some(pod) => Meter::HostPath { pods, pod },
            None => {
                tracing::debug!(driver, "csi-hostpath plugin pod not listable; watching blind");
                Meter::Blind
            }
        }
    }

    /// `None` = no reading (blind driver, exec refused, copy not started). Never `Some(0)` as
    /// a stand-in — a missing reading must not read as a stalled one
    async fn sample(&mut self) -> Option<Snap> {
        let Meter::HostPath { pods, pod } = self else {
            return None;
        };
        match exec_capture(pods, pod, NEWEST_SNAP).await {
            Ok(out) => parse_snap(&out),
            // Test side holds no `pods/exec`, and a restarting plugin drops the stream. Either
            // way the reading is gone for good, so widen to the blind window rather than
            // failing on a probe that was only ever an optimisation
            Err(e) => {
                tracing::debug!(error = %e, "snapshot meter unavailable; watching blind");
                *self = Meter::Blind;
                None
            }
        }
    }
}

async fn exec_capture(pods: &Api<Pod>, pod: &str, script: &str) -> Result<String, kube::Error> {
    let params = AttachParams::default().container(PLUGIN_CONTAINER).stdout(true).stderr(false);
    let mut proc = pods.exec(pod, ["sh", "-c", script], &params).await?;
    let mut out = String::new();
    if let Some(mut stdout) = proc.stdout() {
        let _ = stdout.read_to_string(&mut out).await;
    }
    let _ = proc.join().await;
    Ok(out)
}

/// `<name> <bytes>` from [`NEWEST_SNAP`], or `None` when no `.snap` exists yet
fn parse_snap(out: &str) -> Option<Snap> {
    let (name, bytes) = out.trim().rsplit_once(' ')?;
    Some(Snap { name: name.trim().to_string(), bytes: bytes.trim().parse().ok()? })
}

/// Forward-motion clock. Only a *new* observation moves it, and every channel feeds the same
/// one — a growing meter, a re-raised driver error, a content binding
struct Liveness {
    copied: u64,
    total: u64,
    idle_since: Instant,
    bound: bool,
    last_stamp: Option<String>,
    last_error: Option<String>,
    snap: Option<Snap>,
}

impl Liveness {
    fn new(total: u64, now: Instant) -> Self {
        Self {
            copied: 0,
            total,
            idle_since: now,
            bound: false,
            last_stamp: None,
            last_error: None,
            snap: None,
        }
    }

    fn moved(&mut self, now: Instant) {
        self.idle_since = now;
    }

    /// Motion = this file grew, or a different file became the newest one.
    ///
    /// - First reading is a baseline, never motion (it may be a finished sibling)
    /// - New name resets the count rather than clamping (driver restart mints a fresh uuid, and
    ///   its `tar` really did start over)
    fn meter(&mut self, snap: Snap, now: Instant) {
        let motion = match &self.snap {
            None => false,
            Some(prev) => prev.name != snap.name || snap.bytes > prev.bytes,
        };
        if motion {
            self.copied = snap.bytes;
            self.moved(now);
        }
        self.snap = Some(snap);
    }

    fn expired(&self, now: Instant, window: Duration) -> bool {
        now.saturating_duration_since(self.idle_since) >= window
    }

    /// A *newly raised* error = the driver was just reached. One-shot, not a pulse.
    ///
    /// - Keyed on `error.time`: the sidecar drops a repeat of the stored message outright, so an
    ///   identical retry updates neither field and correctly reads as silence
    /// - Worth counting anyway — first contact, and any later *change* of error, are both real
    ///   motion — but nothing here may be mistaken for a liveness channel ([`BLIND_STALL`])
    /// - `None` is not motion: an error clearing is the driver going quiet, not working
    fn error_raised(&mut self, stamp: Option<String>, now: Instant) {
        if stamp.is_some() && stamp != self.last_stamp {
            self.moved(now);
        }
        self.last_stamp = stamp;
    }

    /// Window the phase earns, not the driver: only a *bound* snapshot on a metered driver has
    /// a byte signal to be silent against. Pre-binding there is no copy yet, so the blind ladder
    /// governs — and every [`SnapFault`] then quotes the window it actually fired at
    fn window(&self, metered: bool) -> Duration {
        match metered && self.bound {
            true => METERED_STALL,
            false => BLIND_STALL,
        }
    }

    fn fault(&self, metered: bool) -> SnapFault {
        match (self.bound, metered) {
            (false, _) => SnapFault::Unbound { last: self.last_error.clone() },
            (true, true) => SnapFault::NoProgress { copied: self.copied, total: self.total },
            (true, false) => SnapFault::Silent { last: self.last_error.clone() },
        }
    }
}

/// Watch the machinery behind `snap_name`, **returning only to end the wait**.
///
/// - Caller races this against readiness, so every return must be a state readiness would
///   never arrive to settle
/// - `total` = bucket object size, the only denominator either caller holds
pub async fn watch(
    client: &Client,
    snap_name: &str,
    driver: &str,
    total: u64,
    progress: &dyn StepProgress,
) -> SnapFault {
    let api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &volume_snapshot_gvk());
    let mut meter = Meter::attach(client, driver).await;
    let started = Instant::now();
    let mut clock = Liveness::new(total, started);

    loop {
        let now = Instant::now();
        if let Ok(Some(snap)) = api.get_opt(snap_name).await {
            let status = &snap.data["status"];

            // Bound = the controller handed it to a driver. One-way, and the only motion a
            // pre-driver snapshot ever shows
            if !clock.bound && status["boundVolumeSnapshotContentName"].is_string() {
                clock.bound = true;
                clock.moved(now);
            }

            let message = status["error"]["message"].as_str().map(str::to_owned);
            if let Some(reason) = message.as_deref().and_then(fatal) {
                return SnapFault::Driver(reason);
            }
            clock.error_raised(status["error"]["time"].as_str().map(str::to_owned), now);
            clock.last_error = message;
        }

        if let Some(snap) = meter.sample().await {
            clock.meter(snap, now);
        }

        let metered = matches!(meter, Meter::HostPath { .. });
        if clock.expired(now, clock.window(metered)) {
            return clock.fault(metered);
        }
        report(progress, &clock, metered, started);
        tokio::time::sleep(SAMPLE).await;
    }
}

/// Row while the copy runs. Bytes where they exist, else elapsed — never a bare spinner
fn report(progress: &dyn StepProgress, clock: &Liveness, metered: bool, started: Instant) {
    let secs = started.elapsed().as_secs();
    match (metered, clock.copied) {
        (true, copied) if copied > 0 => {
            // Bar only against a denominator that exists: `total` is 0 test side, and
            // `max(copied)` would paint every copy as complete
            if clock.total > 0 {
                progress.bytes(copied, clock.total.max(copied));
            }
            progress.note(&format!("snapshotting {}", fraction(copied, clock.total)));
        }
        _ if !clock.bound => progress.note(&format!("binding snapshot ({secs}s)")),
        _ => progress.note(&format!("waiting for snapshot ({secs}s)")),
    }
}

/// Message a retry cannot clear → the reason to quote, or `None` to keep waiting
fn fatal(msg: &str) -> Option<String> {
    let lower = msg.to_ascii_lowercase();
    if let Some(text) = FATAL_TEXT.iter().find(|t| lower.contains(*t)) {
        return Some(format!("{text} — snapshot is a full copy beside its source volume"));
    }
    FATAL_CODES
        .iter()
        .find(|c| msg.contains(&format!("code = {c}")))
        .map(|_| msg.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_deadline_exceeded_is_the_copy_working_not_a_failure() {
        let msg = "Failed to create snapshot: rpc error: code = DeadlineExceeded desc = \
                   context deadline exceeded";
        assert_eq!(fatal(msg), None);
    }

    #[test]
    fn a_full_disk_ends_the_wait_at_once() {
        let msg = "rpc error: code = Internal desc = failed create snapshot: exit status 2: \
                   tar: write error: No space left on device";
        let reason = fatal(msg).expect("fatal");
        assert!(reason.contains("beside its source volume"), "names the cause: {reason}");
    }

    #[test]
    fn an_oversized_request_is_not_retried() {
        let msg = "rpc error: code = OutOfRange desc = Requested capacity 2000 exceeds maximum";
        assert!(fatal(msg).is_some());
    }

    #[test]
    fn a_transient_unavailable_keeps_the_wait_alive() {
        assert_eq!(fatal("rpc error: code = Unavailable desc = connection refused"), None);
    }

    #[test]
    fn the_blind_window_clears_the_sidecars_whole_retry_ladder() {
        let ladder = Duration::from_secs(60) + Duration::from_secs(5 * 60);
        assert!(BLIND_STALL > ladder, "{BLIND_STALL:?} must exceed {ladder:?}");
    }

    fn snap(name: &str, bytes: u64) -> Snap {
        Snap { name: name.to_string(), bytes }
    }

    #[test]
    fn a_finished_siblings_size_is_a_baseline_and_never_counts_as_motion() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(100, t0);
        clock.meter(snap("/csi-data-dir/old.snap", 25 << 30), t0 + Duration::from_secs(1));
        assert_eq!(clock.idle_since, t0, "first reading is a baseline");
        assert_eq!(clock.copied, 0, "a sibling's bytes are not ours");
    }

    #[test]
    fn our_copy_starting_under_a_finished_sibling_still_reads_as_motion() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(100, t0);
        clock.meter(snap("/csi-data-dir/old.snap", 25 << 30), t0);
        let t1 = t0 + Duration::from_secs(5);
        clock.meter(snap("/csi-data-dir/new.snap", 4 << 20), t1);
        assert_eq!(clock.idle_since, t1, "a different newest file is motion");
        assert_eq!(clock.copied, 4 << 20);
    }

    #[test]
    fn a_flat_file_is_not_motion() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(100, t0);
        clock.meter(snap("/csi-data-dir/a.snap", 10), t0);
        clock.meter(snap("/csi-data-dir/a.snap", 40), t0 + Duration::from_secs(1));
        let idle = clock.idle_since;
        clock.meter(snap("/csi-data-dir/a.snap", 40), t0 + Duration::from_secs(2));
        assert_eq!(clock.idle_since, idle, "same file, same size = silence");
        assert_eq!(clock.copied, 40);
    }

    #[test]
    fn the_meter_reads_a_path_with_no_spaces_and_its_size() {
        assert_eq!(
            parse_snap("/csi-data-dir/abc.snap 4096\n"),
            Some(snap("/csi-data-dir/abc.snap", 4096))
        );
        assert_eq!(parse_snap(""), None);
        assert_eq!(parse_snap("garbage"), None);
    }

    #[test]
    fn first_contact_with_the_driver_is_motion_and_so_is_a_changed_error() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(0, t0);
        clock.error_raised(Some("2026-08-25T10:00:00Z".into()), t0 + Duration::from_secs(60));
        let first = clock.idle_since;
        assert_ne!(first, t0, "first error reached the driver");
        clock.error_raised(Some("2026-08-25T10:01:00Z".into()), t0 + Duration::from_secs(120));
        assert_ne!(clock.idle_since, first, "a different error is a new observation");
    }

    /// Sidecar drops a repeat of the stored message, so an identical retry updates neither
    /// field — the storm is invisible here by design, and must not launder itself as motion
    #[test]
    fn an_unchanged_error_is_silence_not_a_pulse() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(0, t0);
        let stamp = "2026-08-25T10:00:00Z";
        clock.error_raised(Some(stamp.into()), t0 + Duration::from_secs(60));
        let idle = clock.idle_since;
        clock.error_raised(Some(stamp.into()), t0 + Duration::from_secs(120));
        assert_eq!(clock.idle_since, idle, "same stamp re-read is silence");
    }

    #[test]
    fn a_cleared_error_is_not_motion() {
        let t0 = Instant::now();
        let mut clock = Liveness::new(0, t0);
        clock.error_raised(None, t0 + Duration::from_secs(60));
        assert_eq!(clock.idle_since, t0, "absence of an error says nothing about progress");
    }

    #[test]
    fn an_unbound_snapshot_waits_out_the_blind_ladder_even_on_a_metered_driver() {
        let clock = Liveness::new(0, Instant::now());
        assert_eq!(clock.window(true), BLIND_STALL, "no copy has started, so no byte signal");
        assert_eq!(clock.window(false), BLIND_STALL);
    }

    #[test]
    fn a_bound_copy_on_a_metered_driver_earns_the_tighter_window() {
        let mut clock = Liveness::new(0, Instant::now());
        clock.bound = true;
        assert_eq!(clock.window(true), METERED_STALL);
        assert_eq!(clock.window(false), BLIND_STALL, "bound but blind stays on the ladder");
    }

    #[test]
    fn an_unbound_snapshot_names_the_controller_not_the_disk() {
        let clock = Liveness::new(0, Instant::now());
        assert!(matches!(clock.fault(false), SnapFault::Unbound { .. }));
    }

    #[test]
    fn a_copy_past_the_approximate_total_drops_the_fraction() {
        assert_eq!(fraction(3 << 30, 2 << 30), "3.0 GiB");
        assert_eq!(fraction(1 << 30, 2 << 30), "1.0 GiB/~2.0 GiB");
    }
}
