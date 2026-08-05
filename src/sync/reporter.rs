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
use super::snapshot::Snapshot;

/// Publishes engine state as [`SyncEvent`] lines on stdout.
#[derive(Debug)]
pub(crate) struct EventReporter {
    sync_id: String,
    profile: String,
    tick: std::time::Duration,
    probes: usize,
    started: Option<Instant>,
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
            tick,
            probes,
            started: None,
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
        Self::emit(&SyncEvent::Tick(Tick::from_snapshot(snap, self.elapsed())));
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
