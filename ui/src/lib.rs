//! Every status surface drawn: preflight banner, live run panels, sync dashboard.
//!
//! - Pure formatters: state + [`Theme`] in, `String` out, no terminal touched
//!   (mechanics live in [`console`](crate::console)) → testable by
//!   string comparison
//! - [`theme`] palette/glyphs · [`layout`] geometry · [`text`] number/duration
//!   vocabulary · [`plot`] time-series · [`render`] banner + panels · [`report`]
//!   sync dashboard
//! - Shared bottom layers keep surfaces from drifting (disagreeing line budgets
//!   tear the frame; two abbreviations of one magnitude read as two numbers)
//! - Aligned with `cargo nextest`'s reporter (reads as a continuation of it)
//!
//! Spec: [`docs/guide-running-tests.md#preflight`].
//!
//! [`docs/guide-running-tests.md#preflight`]: https://github.com/zingolabs/ztest/blob/dev/docs/guide-running-tests.md#preflight

mod boxes;
pub mod console;
mod layout;
mod plan;
pub mod plot;
mod render;
mod report;
mod runview;
mod status;
pub mod template;
pub mod text;
mod theme;

use ztest::api::{BuildStage, NodeSummary};

pub use self::layout::{SPINNER_STEP_MS, display_width, pad, truncate, truncate_with};
pub use self::plan::render as render_plan;
pub use self::render::{
    render, render_cancel_panel, render_live_panel, render_preflight_panel,
    render_sync_build_panel, render_sync_load, render_sync_watch_panel, render_sync_work,
    render_transfer_line, render_transfers,
};
pub use self::report::{
    ComponentResources, ReportView, render_sync_report, render_sync_verdict, status_mark,
};
pub use self::runview::ConsoleView;
pub use self::status::render_status;
pub use self::theme::Theme;
pub use ztest::api::{QosPlan, TierPlan};

/// Cross-crate test support. `pub` only so `ztest_cli`'s tests can reach it — nothing
/// here is part of the rendering API, and `#[cfg(test)]` would not cross the crate line
#[doc(hidden)]
pub mod testing {
    /// Assert an ASCII-mode render leaked no Unicode.
    ///
    /// The one gate every surface's golden test runs through. A hardcoded glyph is
    /// invisible in a Unicode golden — it renders identically to the themed one — so the
    /// only mechanical way to catch it is to draw the same frame with
    /// [`ThemeChars::ascii`](crate::Theme) and check what came out. Without this, a leak
    /// is found by a user on a terminal that cannot render it
    ///
    /// # Panics
    /// With the offending characters named, and the line each sits on
    pub fn assert_ascii_clean(surface: &str, drawn: &str) {
        let bad: Vec<String> = drawn
            .lines()
            .enumerate()
            .filter_map(|(i, line)| {
                let leaked: Vec<char> = line.chars().filter(|c| !c.is_ascii()).collect();
                match leaked.is_empty() {
                    true => None,
                    false => Some(format!("  line {}: {leaked:?} in {line:?}", i + 1)),
                }
            })
            .collect();
        assert!(
            bad.is_empty(),
            "`{surface}` leaked non-ascii glyphs in ascii mode — they belong in `ThemeChars`:\n{}",
            bad.join("\n")
        );
    }
}

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
    pub capacity: ztest::api::ClusterCapacity,
}

#[derive(Debug, Clone)]
pub struct ArchiveRow {
    pub name: String,
    pub status: ArchiveStatus,
}

// ─────────────────────────── ztest status ─────────────────────────────

/// One frame of `ztest status` (`docs/design-status.md`).
///
/// - Folded from the `ztest-meta` lease beacons + one node read, nothing else
/// - `now` travels with the frame so every row ages against one clock read
/// - `Serialize` is the `--json` surface; display-only fields are skipped there
#[derive(Debug, Clone, serde::Serialize)]
pub struct StatusView {
    pub context: String,
    pub server: String,
    pub nodes: NodeSummary,
    pub allocatable: ztest::api::Resources,
    pub capacity: ztest::api::Resources,
    pub runs: Vec<RunRow>,
    pub claims: Vec<ClaimRow>,
    pub anomaly: Option<String>,
    pub now: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RunRow {
    pub beacon: ztest::api::Beacon,
    pub yours: bool,
    /// User has >1 active run → the id is the only thing telling the rows apart
    #[serde(skip)]
    pub show_run_id: bool,
    #[serde(rename = "eta_seconds", serialize_with = "secs")]
    pub eta: Option<std::time::Duration>,
}

/// Durations reach `--json` as whole seconds; the derived `{secs,nanos}` pair is a Rust
/// detail no consumer should have to reassemble
fn secs<S: serde::Serializer>(d: &Option<std::time::Duration>, s: S) -> Result<S::Ok, S::Error> {
    match d {
        Some(d) => s.serialize_some(&d.as_secs()),
        None => s.serialize_none(),
    }
}

/// `projected_start` = when running peers free enough for `beacon.needs`; `None` = past
/// the axis window or unprojectable
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClaimRow {
    pub beacon: ztest::api::Beacon,
    pub yours: bool,
    #[serde(rename = "projected_start_seconds", serialize_with = "secs")]
    pub projected_start: Option<std::time::Duration>,
    pub position: usize,
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
    Upload,
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
    Bytes { done: u64, total: u64, pace: Option<ztest::api::Pace> },
    Failed { detail: String },
}

/// Byte reports arrive ~1/s (`dd status=progress`; image pulls no faster), sizing the
/// [`Window`](ztest::api::Window) that smooths them
const BYTE_SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// One row's [`Progress`](ztest::api::Progress) reports folded into a [`TransferProgress`],
/// owning the rate window behind them.
///
/// - Single fold for every surface (console build phase, `snapshot push`, `snapshot warm`)
/// - Window dropped on leaving byte mode (resumed bar must not date its rate to the gap)
/// - `Failed` latches (a failed row keeps its failure until the phase ends)
#[derive(Debug)]
pub struct TransferState {
    rate: ztest::api::Window<u64>,
    progress: TransferProgress,
}

impl TransferState {
    pub fn new(stage: impl Into<String>) -> TransferState {
        TransferState {
            rate: ztest::api::Window::new(BYTE_SAMPLE_INTERVAL),
            progress: TransferProgress::Stage(stage.into()),
        }
    }

    /// `at` = arrival, passed in rather than read here (a fold reading the clock cannot be
    /// driven over a scripted timeline)
    pub fn apply(&mut self, ev: ztest::api::Progress, at: std::time::Instant) {
        if matches!(self.progress, TransferProgress::Failed { .. }) {
            return;
        }
        self.progress = match ev {
            ztest::api::Progress::Bytes { done, total } => {
                self.rate.push(at, done);
                let pace = self.rate.pace(Some(total.saturating_sub(done) as f64));
                TransferProgress::Bytes { done, total, pace }
            }
            ztest::api::Progress::Note(note) => {
                self.rate = ztest::api::Window::new(BYTE_SAMPLE_INTERVAL);
                TransferProgress::Stage(note)
            }
            // Bar dropped, not parked at 100% (full bar that keeps sitting = reads as a hang)
            ztest::api::Progress::Finalizing => {
                self.rate = ztest::api::Window::new(BYTE_SAMPLE_INTERVAL);
                TransferProgress::Stage("finalizing".to_string())
            }
        };
    }

    pub fn fail(&mut self, detail: String) {
        self.progress = TransferProgress::Failed { detail };
    }

    pub fn progress(&self) -> &TransferProgress {
        &self.progress
    }
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
    /// From the subject's own `Observing` — no figure shown without naming what produced it
    pub subject: Option<String>,
    pub vitals: Option<SyncVitals>,
    pub metrics_note: Option<String>,
    pub probes: Vec<ProbeRow>,
    pub violations: usize,
    pub timeline: Option<ztest::api::Timeline>,
    /// Sampled on [`SAMPLE_PERIOD`](ztest::api::SAMPLE_PERIOD), not the 1s
    /// scrape — empty until the first sample, and stays empty on a cluster with no
    /// metrics API (`pods_note` says which)
    pub pods: Vec<ztest::api::PodLoad>,
    pub pods_note: Option<String>,
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
/// - `phase` = chain-walk progress, never a run's standing; `None` until the first tick
/// - `None` rate = unmeasured, not idle; renders `—` not `0`
/// - `pace` blocks/sec + its ETA, together so the countdown can never outlive the rate
///   it was projected from
/// - `pool_rates` in [`CHANNELS`](ztest::api::CHANNELS) order = graph stacking order
/// - `received_at` session-elapsed → stale rates blank by subtraction
#[derive(Debug, Clone)]
pub struct SyncVitals {
    pub height: u32,
    pub target: Option<u32>,
    pub pct: f32,
    pub phase: Option<ztest::api::Phase>,
    /// Subject's own stage word (`"scanning"`, `"indexing"`) — harness owns no such vocabulary
    pub phase_detail: Option<String>,
    pub reorg_depth: u32,
    pub pace: Option<ztest::api::Pace>,
    pub tx_rate: Option<f64>,
    pub work_rate: Option<f64>,
    pub pool_rates: Vec<(&'static str, Option<f64>)>,
    pub cost: ztest::api::CostMs,
    pub received_at: std::time::Duration,
}

/// `since_satisfied` + `window` are `eventually`-only; together = the countdown
/// that shows a stall coming
#[derive(Debug, Clone)]
pub struct ProbeRow {
    pub name: String,
    pub state: ztest::api::ProbeState,
    pub since_satisfied: Option<std::time::Duration>,
    pub window: Option<std::time::Duration>,
}

impl SyncWatchState {
    pub fn probe_tally(&self) -> (usize, usize) {
        (self.probes.iter().filter(|r| r.state.is_ok()).count(), self.probes.len())
    }
}
