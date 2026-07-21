//! Preflight banner: session-startup status surface for the ztest harness.
//!
//! [`theme`] holds the palette and glyph table; [`render`] holds pure
//! formatters that turn a [`BannerState`] and [`Theme`] into a `String` (the
//! full [`render`](render()) banner for non-TTY/CI logs, the compact
//! [`render_preflight_panel`] / [`render_live_panel`] panels for a TTY). Output
//! is aligned with `cargo nextest`'s reporter so it reads as a continuation of
//! its banner. The terminal mechanics that display these strings live in
//! [`cli::console`](crate::cli::console).
//!
//! Spec: [`docs/running-tests.md#preflight`].
//!
//! [`docs/running-tests.md#preflight`]: https://github.com/zingolabs/ztest/blob/dev/docs/running-tests.md#preflight

mod render;
mod theme;

pub(crate) use self::render::SPINNER_STEP_MS;
pub use self::render::{
    RunProgress, render, render_cancel_panel, render_live_panel, render_preflight_panel,
    render_transfers,
};
pub use self::theme::Theme;
pub use crate::qos::schedule::{QosPlan, TierPlan};

// ─────────────────────────── data model ───────────────────────────────

/// Everything the banner needs to produce one frame. Built up by the live
/// preflight loop and passed by value to [`render`].
#[derive(Debug, Clone)]
pub struct BannerState {
    pub cluster: ClusterState,
    pub build: BuildState,
    pub archives: Vec<ArchiveRow>,
    pub snapshots: Vec<SnapshotRow>,
    /// F1–F5 placeholder rows, rendered between snapshots and the
    /// bottom rule.
    pub future: Vec<FutureRow>,
    /// The QoS scheduling plan; `Some` once the inventory dump and probe have
    /// landed. Rendered as the `Scheduling` block.
    pub qos_plan: Option<QosPlan>,
}

/// Phase-B status. Owns the `Inventory` row of the banner.
///
/// Two passes of `cargo nextest list`: a chatty compile pass (`Compiling`, with
/// cargo's stderr inherited) then a silent JSON parse pass (`Indexing`) that
/// yields the test count.
#[derive(Debug, Clone)]
pub enum BuildState {
    /// Phase B hasn't started yet.
    Pending,
    /// First cargo invocation running (compile pass). `started_at` drives the
    /// elapsed display. `phase`, when set, overrides the generic label so the
    /// on-cluster path can name its current sub-phase on the one live row,
    /// resetting the timer at each transition.
    Compiling {
        started_at: std::time::Instant,
        phase: Option<String>,
    },
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
    /// PVC labelled `seeds.ztest.io/ready=true`.
    Cached { size_bytes: u64 },
    /// PVC absent or not ready; bytes streaming in. `bytes_total` is the LFS
    /// pointer's `size=`; `bytes_done` is the running count from the
    /// reconcile-Job's log stream. Percent is derived for display.
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
    /// Fetched from the configured LFS server (rudolfs) over the batch API and
    /// streamed into the seed uploader pod. See `crate::storage::lfs`.
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

// ─────────────────────────── transfers (right column) ─────────────────

/// The right-column model: background acquisitions in flight (archive/seed
/// downloads, dev-image build+load). Session-long and phase-independent. Only
/// *in-flight* and *failed* rows are retained; a completed transfer leaves the
/// column (its result becomes a one-line summary in scrollback).
#[derive(Debug, Clone, Default)]
pub struct Transfers {
    pub rows: Vec<TransferRow>,
}

impl Transfers {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// One background acquisition shown in the right column.
#[derive(Debug, Clone)]
pub struct TransferRow {
    /// Short human label, e.g. `dev-zainod` or `testnet-3.1m`.
    pub label: String,
    /// What kind of transfer, for the direction glyph.
    pub kind: TransferKind,
    /// Live progress.
    pub progress: TransferProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferKind {
    /// Bytes coming down (archive/seed materialization).
    Download,
    /// A dev image being built and loaded into the cluster.
    Image,
    /// A data seed PVC being provisioned.
    Seed,
}

/// Live state of a [`TransferRow`].
#[derive(Debug, Clone)]
pub enum TransferProgress {
    /// In flight. `note` is the current sub-phase (`building`, `load→kind`,
    /// `provisioning`); `bytes` is `Some((done, total))` when a byte count is
    /// known (a real `%` bar), else `None` (spinner + note only).
    Active {
        note: String,
        bytes: Option<(u64, u64)>,
    },
    /// Provisioning failed; kept in the column (with a warn marker) so the
    /// failure is visible until the phase ends. Detail also goes to scrollback.
    Failed { detail: String },
}
