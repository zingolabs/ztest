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
    render_sync_build_panel, render_sync_metrics, render_sync_watch_panel, render_transfers,
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

// ─────────────────────────── sync watch (both columns) ────────────────

/// The `ztest sync watch` panel model: what a detached sync's driver has
/// published so far, folded into one view. A *view* model, deliberately not the
/// driver's wire events — the panel's shape is a display concern, and a sync in
/// the 48-hour `sync` tier can outlive the build that launched it, so the two
/// must be free to change independently.
#[derive(Debug, Clone, Default)]
pub struct SyncWatchState {
    pub profile: String,
    pub sync_id: String,
    pub context: String,
    /// The driver pod's k8s phase. The only signal before the engine's first
    /// event lands — and the one that explains a silent sync (`Pending`,
    /// `ImagePullBackOff`) rather than leaving it looking merely slow.
    pub pod_phase: String,
    /// The provisioning step the driver last reported. A sync spends the large
    /// majority of its wall-clock in `TestEnv::build`, before any tick exists, so
    /// this is the row that distinguishes a slow gate from a hung one.
    pub setup: Option<SetupStep>,
    /// `None` until the first tick arrives.
    pub vitals: Option<SyncVitals>,
    /// The probe board, in registration order.
    pub probes: Vec<ProbeRow>,
    /// Latest server-side metrics sample.
    pub metrics: Vec<MetricRow>,
    /// Why [`metrics`](Self::metrics) holds what it does. An empty column has
    /// several distinct causes and the panel must name the one in force rather
    /// than leaving the reader to guess whether anything is wrong.
    pub metrics_state: MetricsAvailability,
    /// Violations published so far — a count the panel shows even when the
    /// detail has already scrolled out of the terminal.
    pub violations: usize,
}

/// One provisioning milestone as the panel shows it.
#[derive(Debug, Clone)]
pub struct SetupStep {
    /// What the step is about: the component when it names one, else the phase.
    pub subject: String,
    /// The gate itself, in the driver's words.
    pub detail: String,
    /// Session-elapsed reading when it arrived, so the renderer ages it by
    /// subtraction instead of reading a clock.
    pub received_at: std::time::Duration,
}

/// Live sync vitals, from the most recent tick.
#[derive(Debug, Clone)]
pub struct SyncVitals {
    pub height: u32,
    pub target: Option<u32>,
    pub pct: f32,
    pub phase: String,
    pub reorg_depth: u32,
    /// Smoothed scan rate. `None` until two ticks have been seen.
    pub blocks_per_sec: Option<f64>,
    /// Projected time to `target` at the current rate. `None` without a target
    /// or a rate, or when the rate is too near zero to project honestly.
    pub eta: Option<std::time::Duration>,
    /// Session-elapsed reading when this tick arrived. The renderer subtracts it
    /// from the frame's `elapsed` to show tick age — the "is it still alive"
    /// signal — while staying a pure function of its inputs.
    pub received_at: std::time::Duration,
}

/// One probe's row on the board.
#[derive(Debug, Clone)]
pub struct ProbeRow {
    pub name: String,
    pub state: crate::sync::ProbeState,
    /// `eventually` only: time since last satisfied, and the window it must beat.
    /// Together they're the countdown that shows a stall coming.
    pub since_satisfied: Option<std::time::Duration>,
    pub window: Option<std::time::Duration>,
}

/// Why the metrics column is showing what it is. An empty column has three
/// distinguishable causes, and a reader who cannot tell them apart goes hunting a
/// broken exporter: there is nothing to scrape yet, the exporters answered with
/// nothing, or they could not be read.
///
/// The view-model counterpart of [`crate::metrics::live::State`], kept separate
/// because this is what a renderer is allowed to know — the panel must not depend
/// on how the reading was obtained.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum MetricsAvailability {
    /// No pod in the run's namespace exposes a metrics port yet — the components
    /// are still being provisioned. Not a statement about the engine.
    #[default]
    Idle,
    /// The exporters answered, but none of the live families are present. Normal
    /// briefly at startup; past that it means the component publishes nothing.
    AwaitingScrape,
    /// The exporters could not be read, with the scrape's own reason.
    Unavailable(String),
    /// Live values are present.
    Sampled,
}

/// One server-side metric on the right column. The label carries its own unit
/// (see [`crate::metrics::MetricSample`]).
#[derive(Debug, Clone)]
pub struct MetricRow {
    pub name: String,
    pub value: Option<f64>,
}

impl SyncWatchState {
    /// The probe nearest to failing, for the one-line board summary: an already
    /// violating probe outranks a draining `eventually` window, which outranks
    /// one that hasn't reported. `None` when every probe is satisfied.
    pub fn worst_probe(&self) -> Option<&ProbeRow> {
        use crate::sync::ProbeState;
        // Fraction of its window an `eventually` probe has burned; a probe with
        // no window ranks below any that has one, so a real countdown wins the
        // slot over a merely-pending probe.
        let drain = |r: &ProbeRow| match (r.since_satisfied, r.window) {
            (Some(since), Some(window)) if !window.is_zero() => {
                since.as_secs_f64() / window.as_secs_f64()
            }
            _ => 0.0,
        };
        let rank = |r: &ProbeRow| match r.state {
            ProbeState::Violating => 3,
            ProbeState::Pending => 2,
            ProbeState::NotYet => 1,
            ProbeState::Ok => 0,
        };
        self.probes
            .iter()
            .filter(|r| !r.state.is_ok())
            .max_by(|a, b| rank(a).cmp(&rank(b)).then(drain(a).total_cmp(&drain(b))))
    }

    /// How many probes are satisfied, of how many registered.
    pub fn probe_tally(&self) -> (usize, usize) {
        (
            self.probes.iter().filter(|r| r.state.is_ok()).count(),
            self.probes.len(),
        )
    }

    /// Look up one metric by its label.
    pub fn metric(&self, name: &str) -> Option<f64> {
        self.metrics
            .iter()
            .find(|m| m.name == name)
            .and_then(|m| m.value)
    }
}
