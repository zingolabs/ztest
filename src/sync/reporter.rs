//! [`EventReporter`] — the detached driver's [`SyncReporter`], publishing the
//! engine's live state as a [`SyncEvent`] stream on stdout.
//!
//! This is the impl the runner's trait doc anticipated: in detached mode the pod
//! log *is* the wire, so every hook becomes one tagged line for
//! `ztest sync watch` to consume. Local (`cargo test`) runs keep the
//! human-readable `StderrReporter` instead — nobody is tailing a pod.

use tokio::time::Instant;

use super::event::{self, Probe, SyncEvent, Tick};
use super::probe::{ProbeStatus, Verdict};
use super::runner::{SyncOutcome, SyncReporter};
use super::series::{Timeline, plot_channels};
use super::snapshot::Snapshot;

/// How often the whole timeline is republished.
///
/// Not every tick: the timeline is orders of magnitude larger than a tick, and
/// at a 5-second cadence over 48 hours that would dominate the pod log. Not
/// once at the end either — `ztest sync status` has to be able to plot a *live*
/// run. A minute is short enough that a status read is never meaningfully
/// stale, and long enough that the series costs a fraction of the tick stream.
const SERIES_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Publishes engine state as [`SyncEvent`] lines on stdout.
#[derive(Debug)]
pub(crate) struct EventReporter {
    sync_id: String,
    profile: String,
    tick: std::time::Duration,
    probes: usize,
    started: Option<Instant>,
    timeline: Timeline,
    /// Elapsed reading at the last series publication, so the cadence is
    /// measured on the driver's own clock rather than a tick count — a tick
    /// that skips must not stretch the interval.
    published_at: Option<std::time::Duration>,
}

impl EventReporter {
    pub(crate) fn new(
        sync_id: impl Into<String>,
        profile: impl Into<String>,
        tick: std::time::Duration,
        probes: usize,
    ) -> Self {
        EventReporter {
            sync_id: sync_id.into(),
            profile: profile.into(),
            // Start the timeline one bucket per tick: the finest the data
            // supports, and it coarsens itself from there as the run grows.
            timeline: Timeline::new(plot_channels(), tick),
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
        self.started
            .map(|s| Instant::now().saturating_duration_since(s))
            .unwrap_or_default()
    }

    /// Fold one tick's rates into the timeline.
    ///
    /// An unmeasured op contributes `None`, which leaves a gap rather than a
    /// zero — the graph must not draw a floor for a pool nobody counted.
    fn record(&mut self, snap: &Snapshot, elapsed: std::time::Duration) {
        let secs = self.tick.as_secs_f64().max(f64::MIN_POSITIVE);
        let blocks = f64::from(snap.height().saturating_sub(snap.prev_height())) / secs;
        let mut values = vec![Some(blocks)];
        values.extend(snap.rate().channels().map(|(_, rate)| rate));
        self.timeline.push(elapsed, &values);
    }

    /// Whether the timeline is due for republication. The first tick always
    /// publishes, so a `status` run against a young sync gets a graph rather
    /// than a minute of nothing.
    fn series_due(&self, elapsed: std::time::Duration) -> bool {
        self.published_at
            .is_none_or(|last| elapsed.saturating_sub(last) >= SERIES_INTERVAL)
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
            Self::emit(&SyncEvent::Series {
                timeline: self.timeline.clone(),
            });
        }
    }

    fn on_probes(&mut self, _snap: &Snapshot, board: &[ProbeStatus]) {
        Self::emit(&SyncEvent::Probes {
            board: board.iter().map(Probe::from).collect(),
        });
    }

    fn on_probe(&mut self, name: &str, verdict: &Verdict) {
        let (height, detail) = match verdict {
            Verdict::Violated(v) => (v.height, v.detail.clone()),
            Verdict::ProbeError(e) => (None, format!("probe error: {e}")),
            // Satisfied/Pending never reach this hook; the board carries them.
            _ => return,
        };
        Self::emit(&SyncEvent::Violation {
            probe: name.to_string(),
            height,
            detail,
        });
    }

    fn on_finish(&mut self, outcome: &SyncOutcome) {
        Self::emit(&SyncEvent::Finished {
            verdict: format!("{:?}", outcome.verdict),
            violations: outcome.violations.len(),
            coverage_gaps: outcome.coverage_gaps.len(),
            ticks: outcome.ticks,
        });
    }
}
