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

// ─────────────────────────── sync watch (all three columns) ───────────

/// `ztest sync watch` panel model: one [`ReportView`] per scrape interval + the driver pod's phase.
///
/// - Same TSDB read `status` draws (a live panel off another source would disagree with the report)
/// - `metrics_note` / `loads_note` = why a column is empty (blank rows read as an idle subject)
#[derive(Debug, Clone, Default)]
pub struct SyncWatchState {
    pub profile: String,
    pub sync_id: String,
    pub context: String,
    pub pod_phase: String,
    pub vitals: Option<SyncVitals>,
    pub metrics_note: Option<String>,
    pub loads: Vec<ContainerLoad>,
    pub loads_note: Option<String>,
}

/// Live sync vitals, all off one [`ReportView`] → no row describes another instant.
///
/// - `None` rate = unmeasured → `—`, never `0`
/// - `pools` one series per [`Channel`](ztest::api::Channel), oldest pool first
/// - `received_at` session-elapsed → stale rates blank by subtraction
#[derive(Debug, Clone)]
pub struct SyncVitals {
    pub height: u32,
    pub target: u32,
    pub pace: Option<ztest::api::Pace>,
    pub tx_rate: Option<f64>,
    pub blocks: Option<ztest::api::Series>,
    pub pools: Vec<ztest::api::Series>,
    pub span: std::time::Duration,
    pub received_at: std::time::Duration,
}

/// Countdown basis, trailing seconds (rides out a burst, still tracks the chain's changing density)
const ETA_BASIS_SECS: f64 = 600.0;

impl SyncVitals {
    /// `None` until the view holds a committed height
    pub fn of(view: &ReportView, received_at: std::time::Duration) -> Option<SyncVitals> {
        let (height, target) = view.height?;
        let blocks = view.blocks.first().cloned();
        let pace = blocks.as_ref().and_then(|b| {
            Some(ztest::api::Pace {
                per_sec: b.last()?,
                eta: eta(b, target.saturating_sub(height)),
            })
        });
        let pools: Vec<ztest::api::Series> =
            view.transparent.iter().chain(&view.shielded).cloned().collect();
        Some(SyncVitals {
            height,
            target,
            pace,
            tx_rate: view.throughput.first().and_then(ztest::api::Series::last),
            blocks,
            pools: report::fold_pools(&pools),
            span: view.elapsed().unwrap_or_default(),
            received_at,
        })
    }

    pub fn pct(&self) -> f64 {
        match self.target {
            0 => 0.0,
            target => (f64::from(self.height) / f64::from(target) * 100.0).min(100.0),
        }
    }

    /// Sum over measured pools; `None` = none measured
    pub fn work_rate(&self) -> Option<f64> {
        self.pools
            .iter()
            .filter_map(ztest::api::Series::last)
            .fold(None, |acc, r| Some(acc.unwrap_or(0.0) + r))
    }
}

/// Mean over [`ETA_BASIS_SECS`], not the newest point (one grid point swings the countdown by hours)
fn eta(blocks: &ztest::api::Series, remaining: u32) -> Option<std::time::Duration> {
    let (newest, _) = *blocks.points.last()?;
    let recent: Vec<f64> = blocks
        .points
        .iter()
        .filter(|(t, _)| *t >= newest - ETA_BASIS_SECS)
        .map(|(_, v)| *v)
        .collect();
    let mean = recent.iter().sum::<f64>() / recent.len() as f64;
    (mean > 0.0).then(|| std::time::Duration::from_secs_f64(f64::from(remaining) / mean))
}

/// One container's newest draw. `limit` unset = none declared (Burstable) → bare usage, never an
/// invented denominator
#[derive(Debug, Clone, PartialEq)]
pub struct ContainerLoad {
    pub container: String,
    pub usage: ztest::api::Resources,
    pub limit: Option<ztest::api::Resources>,
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::{ReportView, SyncVitals};
    use ztest::api::{Channel, Series, Unit};

    fn per_minute(label: &str, channel: Option<Channel>, values: &[f64]) -> Series {
        Series {
            reading: None,
            label: label.into(),
            unit: Unit::PerSec,
            facet: None,
            channel,
            points: values.iter().enumerate().map(|(i, v)| (i as f64 * 60.0, *v)).collect(),
            total: None,
            coverage: None,
        }
    }

    fn view(blocks: &[f64]) -> ReportView {
        ReportView {
            height: Some((900, 1_000)),
            span: Some((UNIX_EPOCH, UNIX_EPOCH + Duration::from_secs(3_600))),
            blocks: vec![per_minute("blocks", None, blocks)],
            ..ReportView::default()
        }
    }

    #[test]
    fn no_committed_height_is_no_vitals() {
        let view = ReportView { height: None, ..view(&[10.0]) };
        assert!(SyncVitals::of(&view, Duration::ZERO).is_none());
    }

    /// Transparent ships by direction; the panel reads one row per pool
    #[test]
    fn split_rows_fold_into_one_row_per_pool() {
        let view = ReportView {
            transparent: vec![
                per_minute("tsp-in", Some(Channel::Transparent), &[10.0]),
                per_minute("tsp-out", Some(Channel::Transparent), &[30.0]),
            ],
            shielded: vec![per_minute("orchard", Some(Channel::Orchard), &[5.0])],
            ..view(&[10.0])
        };
        let v = SyncVitals::of(&view, Duration::ZERO).expect("a committed height");
        let rates: Vec<_> = v.pools.iter().map(|s| (s.channel, s.last())).collect();
        assert_eq!(
            rates,
            [(Some(Channel::Transparent), Some(40.0)), (Some(Channel::Orchard), Some(5.0))]
        );
        assert_eq!(v.work_rate(), Some(45.0));
    }

    /// Run mean would promise a mainnet finish hours early (the early chain is the fast one)
    #[test]
    fn the_countdown_runs_off_the_recent_pace() {
        let mut blocks = vec![1_000.0; 10];
        blocks.extend([10.0; 11]);
        let pace = SyncVitals::of(&view(&blocks), Duration::ZERO)
            .and_then(|v| v.pace)
            .expect("a measured pace");
        assert_eq!(pace.per_sec, 10.0);
        assert_eq!(pace.eta, Some(Duration::from_secs(10)));
    }
}
