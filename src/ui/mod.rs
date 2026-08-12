//! Every status surface the harness draws: the preflight banner, the live run
//! panels, and the sync dashboard.
//!
//! Everything here is a **pure formatter** — state and a [`Theme`] in, a
//! `String` out, no terminal touched. The mechanics that put these strings on a
//! terminal (the render thread, the viewport, the PTY) live in
//! [`cli::console`](crate::cli::console), and that separation is what lets the
//! whole surface be tested by comparing strings.
//!
//! - [`theme`] — palette and glyph table, gated on terminal capability.
//! - [`layout`] — panel geometry, rules, spinner, resource formatters.
//! - [`text`] — the shared number, duration, and gauge vocabulary.
//! - [`plot`] — time-series plotting primitives.
//! - [`render`] — the banner and panel formatters, over the model below.
//! - [`status`] — the sync dashboard.
//!
//! The bottom three layers exist so the surfaces cannot drift apart: two panels
//! that disagreed about the line budget would tear the frame, and a magnitude
//! abbreviated two ways reads as two different numbers.
//!
//! Output is aligned with `cargo nextest`'s reporter so it reads as a
//! continuation of its banner.
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
    render_sync_build_panel, render_sync_metrics, render_sync_watch_panel, render_sync_work,
    render_transfers,
};
pub use self::status::render_sync_status;
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
    /// LFS pointer present, blob unreachable; soft fail.
    Missing { detail: String },
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
    /// Violations published so far — a count the panel shows even when the
    /// detail has already scrolled out of the terminal.
    pub violations: usize,
    /// The run's shape so far, every channel on one shared time axis.
    ///
    /// `None` until the driver publishes its first series — which includes the
    /// whole of a sync launched by a driver predating the event, where the
    /// panel falls back to instantaneous values with no graph.
    pub timeline: Option<crate::sync::Timeline>,
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
    /// Protocol work per second, total and by pool, as the driver measured it.
    ///
    /// `None` entries mean **unmeasured**, not idle: a tier-B op nobody counted
    /// and a pool with no activity are different facts and the panel renders
    /// them differently (`—` against `0`).
    pub work_rate: Option<f64>,
    /// Per-pool rates in [`CHANNELS`](crate::sync::CHANNELS) order, which is
    /// also the order they stack in the graph.
    pub pool_rates: Vec<(&'static str, Option<f64>)>,
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
}
