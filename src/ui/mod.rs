//! Every status surface drawn: preflight banner, live run panels, sync dashboard.
//!
//! - Pure formatters: state + [`Theme`] in, `String` out, no terminal touched
//!   (mechanics live in [`cli::console`](crate::cli::console)) → testable by
//!   string comparison
//! - [`theme`] palette/glyphs · [`layout`] geometry · [`text`] number/duration
//!   vocabulary · [`plot`] time-series · [`render`] banner + panels · [`status`]
//!   sync dashboard
//! - Shared bottom layers keep surfaces from drifting (disagreeing line budgets
//!   tear the frame; two abbreviations of one magnitude read as two numbers)
//! - Aligned with `cargo nextest`'s reporter (reads as a continuation of it)
//!
//! Spec: [`docs/guide-running-tests.md#preflight`].
//!
//! [`docs/guide-running-tests.md#preflight`]: https://github.com/zingolabs/ztest/blob/dev/docs/guide-running-tests.md#preflight

mod layout;
pub mod plot;
mod render;
mod status;
pub mod text;
mod theme;

pub(crate) use self::layout::SPINNER_STEP_MS;
pub use self::render::{
    RunProgress, render, render_cancel_panel, render_live_panel, render_preflight_panel,
    render_sync_build_panel, render_sync_cost, render_sync_watch_panel, render_sync_work,
    render_transfers,
};
pub use self::status::render_sync_status;
pub use self::theme::Theme;
pub use crate::qos::schedule::{QosPlan, TierPlan};

// ─────────────────────────── data model ───────────────────────────────

/// One banner frame's inputs. Accumulated by the preflight loop, passed by value
/// to [`render`]
#[derive(Debug, Clone)]
pub struct BannerState {
    pub cluster: ClusterState,
    pub build: BuildState,
    pub archives: Vec<ArchiveRow>,
    pub qos_plan: Option<QosPlan>,
}

/// Phase-B status; owns the banner's `Inventory` row.
///
/// Two `cargo nextest list` passes: `Compiling` (chatty, cargo stderr inherited)
/// then `Indexing` (silent JSON parse → test count)
#[derive(Debug, Clone)]
pub enum BuildState {
    Pending,
    Compiling { started_at: std::time::Instant, phase: Option<String> },
    Indexing { started_at: std::time::Instant },
    Ok { test_count: usize, binary_count: usize, elapsed: std::time::Duration },
    Failed { exit_code: i32, stage: BuildStage, elapsed: std::time::Duration },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildStage {
    Compile,
    Index,
}

/// - `slots_used` = observed `zaino-{ci,dev}-*` namespaces (concurrency proxy)
/// - `capacity` = whole-cluster allocatable − requested (NVMe vs general is k8s
///   placement, not a second pool)
#[derive(Debug, Clone)]
pub struct ClusterState {
    pub context: String,
    pub slots_used: u32,
    pub slots_total: u32,
    pub slots_configured: u32,
    pub nodes_ready: u32,
    pub nodes_cordoned: u32,
    pub capacity: crate::qos::ClusterCapacity,
}

#[derive(Debug, Clone)]
pub struct ArchiveRow {
    pub name: String,
    pub status: ArchiveStatus,
}

#[derive(Debug, Clone)]
pub enum ArchiveStatus {
    Cached { size_bytes: u64 },
    Missing { detail: String },
}

// ─────────────────────────── transfers (right column) ─────────────────

/// Right-column model: background acquisitions (archive/seed download, dev-image
/// build+load). Session-long, phase-independent; only in-flight & failed rows
/// retained
#[derive(Debug, Clone, Default)]
pub struct Transfers {
    pub rows: Vec<TransferRow>,
}

#[derive(Debug, Clone)]
pub struct TransferRow {
    pub label: String,
    pub kind: TransferKind,
    pub progress: TransferProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferKind {
    Download,
    Image,
    Seed,
}

/// Live state of a [`TransferRow`].
///
/// - `Stage` = spinner + text, the phases with no byte count to show
/// - `Bytes` carries no text: bar + counts + rate + ETA already fill the row, and a
///   stage word beside them is what a narrow column can least afford
/// - `pace` bytes/sec + its ETA, `None` = unmeasured (too few samples / counter reset),
///   never an idle zero
/// - `Failed` stays in the column until the phase ends
#[derive(Debug, Clone)]
pub enum TransferProgress {
    Stage(String),
    Bytes { done: u64, total: u64, pace: Option<crate::rate::Pace> },
    Failed { detail: String },
}

// ─────────────────────────── sync watch (both columns) ────────────────

/// `ztest sync watch` panel model: the driver's publications folded into one view.
///
/// - Not the driver's wire events (a 48h sync outlives the build that launched it)
/// - `metrics_note` = why `vitals` is empty (blank rows can't separate warm-up
///   from a broken subject)
#[derive(Debug, Clone, Default)]
pub struct SyncWatchState {
    pub profile: String,
    pub sync_id: String,
    pub context: String,
    pub pod_phase: String,
    pub setup: Option<SetupStep>,
    pub vitals: Option<SyncVitals>,
    pub metrics_note: Option<String>,
    pub probes: Vec<ProbeRow>,
    pub violations: usize,
    pub timeline: Option<crate::sync::Timeline>,
}

/// `received_at` session-elapsed → renderer ages by subtraction, no clock read
#[derive(Debug, Clone)]
pub struct SetupStep {
    pub subject: String,
    pub detail: String,
    pub received_at: std::time::Duration,
}

/// Live sync vitals.
///
/// - All but `phase`/`reorg_depth` (engine-only) come from one 1s watcher scrape
///   → no row lags another by a tick
/// - `None` rate = unmeasured, not idle; renders `—` not `0`
/// - `pace` blocks/sec + its ETA, together so the countdown can never outlive the rate
///   it was projected from
/// - `pool_rates` in [`CHANNELS`](crate::sync::CHANNELS) order = graph stacking order
/// - `received_at` session-elapsed → stale rates blank by subtraction
#[derive(Debug, Clone)]
pub struct SyncVitals {
    pub height: u32,
    pub target: Option<u32>,
    pub pct: f32,
    pub phase: String,
    pub reorg_depth: u32,
    pub pace: Option<crate::rate::Pace>,
    pub work_rate: Option<f64>,
    pub pool_rates: Vec<(&'static str, Option<f64>)>,
    pub cost: crate::sync::Cost,
    pub received_at: std::time::Duration,
}

/// `since_satisfied` + `window` are `eventually`-only; together = the countdown
/// that shows a stall coming
#[derive(Debug, Clone)]
pub struct ProbeRow {
    pub name: String,
    pub state: crate::sync::ProbeState,
    pub since_satisfied: Option<std::time::Duration>,
    pub window: Option<std::time::Duration>,
}

impl SyncWatchState {
    pub fn probe_tally(&self) -> (usize, usize) {
        (self.probes.iter().filter(|r| r.state.is_ok()).count(), self.probes.len())
    }
}
