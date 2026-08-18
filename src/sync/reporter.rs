//! [`EventReporter`]: the detached driver's [`SyncReporter`], publishing live
//! engine state as a [`SyncEvent`] stream on stdout.
//!
//! - Detached: pod log = the wire, so every hook becomes one tagged line
//! - Local (`cargo test`): `StderrReporter` instead (nobody is tailing a pod)

use tokio::time::Instant;

use super::event::{self, Probe, SyncEvent, Tick};
use super::observe::Observation;
use super::probe::{ProbeStatus, Verdict};
use super::runner::{SyncOutcome, SyncReporter};
use super::series::{Timeline, plot_channels};
use super::snapshot::Snapshot;

/// Timeline republication period. Not per-tick (would dominate the pod log), not
/// once at the end (`sync status` plots a *live* run)
const SERIES_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Publishes engine state as [`SyncEvent`] lines on stdout. `published_at` is
/// elapsed, not a tick count (a skipped tick must not stretch the interval)
#[derive(Debug)]
pub struct EventReporter {
    sync_id: String,
    profile: String,
    tick: std::time::Duration,
    probes: usize,
    started: Option<Instant>,
    timeline: Timeline,
    /// Same estimator the controller runs over the published stream, so the driver's
    /// own graph and a watcher's columns cannot disagree about a rate
    window: crate::rate::Window<Observation, std::time::Duration>,
    published_at: Option<std::time::Duration>,
}

impl EventReporter {
    pub fn new(
        sync_id: impl Into<String>,
        profile: impl Into<String>,
        tick: std::time::Duration,
        probes: usize,
    ) -> Self {
        EventReporter {
            sync_id: sync_id.into(),
            profile: profile.into(),
            // One bucket per tick = finest the data supports; self-coarsens from there
            timeline: Timeline::new(plot_channels(), tick),
            window: crate::rate::Window::new(tick),
            tick,
            probes,
            started: None,
            published_at: None,
        }
    }

    fn emit(event: &SyncEvent) {
        event::publish(event);
    }

    fn elapsed(&self) -> std::time::Duration {
        self.started.map(|s| Instant::now().saturating_duration_since(s)).unwrap_or_default()
    }

    /// Fold one tick into the window, then its smoothed rates into the timeline.
    /// Unmeasured op contributes `None` → gap, not a floor drawn for an uncounted pool
    fn record(&mut self, snap: &Snapshot, elapsed: std::time::Duration) {
        self.window.push(elapsed, Observation::from(snap));
        let mut values = vec![self.window.block_pace().map(|p| p.per_sec)];
        match self.window.work_rate() {
            Some(rate) => values.extend(rate.channels().map(|(_, r)| r)),
            None => values.extend(std::iter::repeat_n(None, plot_channels().count() - 1)),
        }
        self.timeline.push(elapsed, &values);
    }

    /// Timeline due for republication. First tick always publishes, so `status`
    /// against a young sync gets a graph, not a minute of nothing
    fn series_due(&self, elapsed: std::time::Duration) -> bool {
        self.published_at.is_none_or(|last| elapsed.saturating_sub(last) >= SERIES_INTERVAL)
    }
}

impl SyncReporter for EventReporter {
    fn on_start(&mut self) {
        self.started = Some(Instant::now());
        Self::emit(&SyncEvent::Started {
            profile: self.profile.clone(),
            sync_id: self.sync_id.clone(),
            tick_ms: self.tick.as_millis() as u64,
            probes: self.probes,
        });
    }

    fn on_tick(&mut self, snap: &Snapshot) {
        let elapsed = self.elapsed();
        Self::emit(&SyncEvent::Tick(Tick::from_snapshot(snap, elapsed)));
        self.record(snap, elapsed);
        if self.series_due(elapsed) {
            self.published_at = Some(elapsed);
            Self::emit(&SyncEvent::Series { timeline: self.timeline.clone() });
        }
    }

    fn on_probes(&mut self, _snap: &Snapshot, board: &[ProbeStatus]) {
        Self::emit(&SyncEvent::Probes { board: board.iter().map(Probe::from).collect() });
    }

    fn on_probe(&mut self, name: &str, verdict: &Verdict) {
        let (height, detail) = match verdict {
            Verdict::Violated(v) => (v.height, v.detail.clone()),
            Verdict::ProbeError(e) => (None, format!("probe error: {e}")),
            // Satisfied/Pending never reach this hook; the board carries them
            _ => return,
        };
        Self::emit(&SyncEvent::Violation { probe: name.to_string(), height, detail });
    }

    fn on_finish(&mut self, outcome: &SyncOutcome) {
        Self::emit(&SyncEvent::Finished {
            verdict: outcome.verdict,
            violations: outcome.violations.len(),
            coverage_gaps: outcome.coverage_gaps.len(),
            ticks: outcome.ticks,
        });
    }
}
