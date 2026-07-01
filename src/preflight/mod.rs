//! Preflight banner: session-startup status surface for the ztest harness.
//!
//! Drives the bottom status panel [`cli::run`](crate::cli::run) keeps pinned
//! through a `ztest run` session: the compact [`render_preflight_panel`] /
//! [`render_live_panel`] panels on a TTY, and the full [`render`](render())
//! banner printed once for the log on a non-TTY (CI) run. Output style is
//! aligned with `cargo nextest`'s own reporter (same colour palette, glyph set,
//! and right-aligned 12-column action-label convention) so the block reads as a
//! continuation of nextest's startup banner rather than a parallel UI.
//!
//! Layers:
//! - [`theme`]: colour palette and glyph table; one `Theme::detect()`
//!   constructor handles `NO_COLOR` / TTY / Unicode-support gating.
//! - [`render`]: pure formatters. Take a fully-known [`BannerState`] (plus, for
//!   the panels, live run state) and a [`Theme`], produce a `String`. No I/O, no
//!   async. [`render`](render()) is the full banner; [`render_preflight_panel`]
//!   and [`render_live_panel`] are the compact bottom-console panels for the
//!   preflight and run phases.
//!
//! The terminal mechanics that display these strings (pinning a panel, forwarding
//! output into native scrollback) live in [`cli::console`](crate::cli::console).
//!
//! Spec: [`docs/running-tests.md#preflight`].
//!
//! [`docs/running-tests.md#preflight`]: https://github.com/zingolabs/ztest/blob/dev/docs/running-tests.md#preflight

mod render;
mod theme;

pub use self::render::{
    RunProgress, render, render_cancel_panel, render_live_panel, render_preflight_panel,
};
pub use self::theme::Theme;
pub use crate::qos::schedule::{QosPlan, TierPlan};

// ─────────────────────────── data model ───────────────────────────────

/// Everything the banner needs to produce one frame.
///
/// Built up by the live preflight loop and passed by value to [`render`].
/// Future-feature rows (tier, queue, reservation) are [`FutureRow`] entries so
/// they render as `not yet implemented` placeholders without changing the layout.
#[derive(Debug, Clone)]
pub struct BannerState {
    pub cluster: ClusterState,
    pub build: BuildState,
    pub archives: Vec<ArchiveRow>,
    pub snapshots: Vec<SnapshotRow>,
    /// F1–F5 placeholder rows, rendered between snapshots and the
    /// bottom rule.
    pub future: Vec<FutureRow>,
    /// The QoS scheduling plan (per-tier counts, wave estimate vs capacity,
    /// unschedulable warnings); `Some` once the inventory dump and probe have
    /// landed. Rendered as the `Scheduling` block. The live during-run
    /// reservation view is a deferred follow-up (noted in the block).
    pub qos_plan: Option<QosPlan>,
}

/// Phase-B status. Owns the `Inventory` row of the banner.
///
/// Two passes of `cargo nextest list`: a chatty compile pass (`Compiling`) where
/// cargo's stderr is inherited so the user sees fetch / compile / warning output,
/// then a silent JSON parse pass (`Indexing`) that yields the test count.
#[derive(Debug, Clone)]
pub enum BuildState {
    /// Phase B hasn't started yet.
    Pending,
    /// First cargo invocation running (compile pass). `started_at` lets the
    /// renderer display elapsed seconds.
    Compiling { started_at: std::time::Instant },
    /// Compile pass succeeded; second cargo invocation
    /// (`--message-format=json`) running for the inventory parse.
    Indexing { started_at: std::time::Instant },
    /// Both passes complete.
    Ok {
        test_count: usize,
        binary_count: usize,
        elapsed: std::time::Duration,
    },
    /// Either pass returned non-zero.
    Failed {
        exit_code: i32,
        stage: BuildStage,
        elapsed: std::time::Duration,
    },
}

/// Which pass of Phase B failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildStage {
    Compile,
    Index,
}

#[derive(Debug, Clone)]
pub struct ClusterState {
    /// Kube context name as resolved from the kubeconfig in use.
    pub context: String,
    /// Count of `zaino-{ci,dev}-*` namespaces observed on the cluster (proxy for
    /// current concurrency, until F1 lands a cluster-wide session registry).
    pub slots_used: u32,
    /// Hard slot cap; see `running-tests.md` "Hard cap: 16 slots".
    pub slots_total: u32,
    /// `--test-threads` value for this invocation.
    pub slots_configured: u32,
    pub nodes_ready: u32,
    pub nodes_cordoned: u32,
    /// Whole-cluster schedulable capacity (allocatable minus sum of requested).
    /// One global figure: NVMe vs general is k8s placement, not a split.
    pub capacity: crate::qos::ClusterCapacity,
}

#[derive(Debug, Clone)]
pub struct ArchiveRow {
    pub name: String,
    pub status: ArchiveStatus,
}

#[derive(Debug, Clone)]
pub enum ArchiveStatus {
    /// PVC labelled `seeds.zaino.io/ready=true`.
    Cached { size_bytes: u64 },
    /// PVC absent or not ready; bytes streaming in.
    ///
    /// `bytes_total` is the LFS pointer's `size=` value, known up front.
    /// `bytes_done` is the running byte count from the reconcile-Job's log stream.
    /// Percent is derived for display; callers need not keep it in sync.
    Downloading {
        source: DownloadSource,
        bytes_done: u64,
        bytes_total: u64,
    },
    /// LFS pointer present, blob unreachable; soft fail.
    Missing { detail: String },
}

impl ArchiveStatus {
    /// Convenience for the downloading state. Returns
    /// `(percent in 0..=100, bytes_done, bytes_total)` for the
    /// downloading variant; `None` otherwise.
    pub fn download_progress(&self) -> Option<(u8, u64, u64)> {
        match self {
            Self::Downloading {
                bytes_done,
                bytes_total,
                ..
            } => {
                let percent = if *bytes_total == 0 {
                    0
                } else {
                    ((*bytes_done as u128 * 100) / *bytes_total as u128).min(100) as u8
                };
                Some((percent, *bytes_done, *bytes_total))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadSource {
    /// `git lfs pull` against the configured remote.
    Lfs,
    /// F6: cluster-resident LFS cache.
    ClusterCache,
}

#[derive(Debug, Clone)]
pub struct SnapshotRow {
    /// PVC reference, e.g. `pvc/zebra-testnet-cache`.
    pub pvc: String,
    pub status: SnapshotStatus,
}

#[derive(Debug, Clone)]
pub enum SnapshotStatus {
    BoundReady,
    Provisioning {
        /// Name of the archive whose materialization this snapshot is
        /// waiting on.
        from_archive: String,
    },
}

/// A future-feature row that has reserved layout but no live data
/// yet. Renders as `<label>  not yet implemented`.
#[derive(Debug, Clone)]
pub struct FutureRow {
    pub label: &'static str,
}
