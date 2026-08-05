//! The driver→controller live event stream.
//!
//! A detached sync's only channel to a terminal watching it is the driver pod's
//! log: the controller is stateless and holds no connection to the run between
//! commands. So the per-tick state the engine already computes is written to
//! that log as one machine-readable line per event, [`EVENT_PREFIX`]-tagged so
//! `ztest sync watch` can lift events out of a stream it otherwise passes
//! through verbatim.
//!
//! The driver emits data and never formatting: every renderer lives
//! controller-side, so the panel's layout can change without redeploying a
//! running sync — a sync in the `sync` tier can outlive several ztest builds.

use serde::{Deserialize, Serialize};

use super::probe::{Class, ProbeState, ProbeStatus};
use super::snapshot::Snapshot;
use crate::handles::wallet::Pool;

/// Tags a driver stdout line as a serialized [`SyncEvent`]. Deliberately not a
/// plausible log-line opening, so no component's output can be mistaken for an
/// event; a line that fails to parse after the tag is passed through as log
/// output rather than dropped.
pub(crate) const EVENT_PREFIX: &str = "@ztest-sync-event ";

/// One observation published by the driver. Additive by contract: a controller
/// older than the driver it watches must skip unknown events rather than fail,
/// so this is `#[serde(other)]`-terminated and every field stays optional-safe.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub(crate) enum SyncEvent {
    /// A `TestEnv::build` milestone: which provisioning gate the driver is in.
    ///
    /// Published because a topology takes minutes to come up and the engine does
    /// not exist yet — so without these the whole provisioning window, which is
    /// most of a sync's wall-clock, was indistinguishable from a hang.
    Setup {
        /// The build phase (`validator`, `indexer`, …).
        phase: String,
        /// What that phase is doing or waiting on, already human-readable: the
        /// driver publishes data and never formatting, but *which gate this is* is
        /// data, and only the gate itself knows it.
        detail: String,
        /// The component the step concerns, when it concerns exactly one.
        #[serde(default)]
        component: Option<String>,
    },
    /// The engine began its run loop; carries what the panel needs before the
    /// first tick lands.
    Started {
        profile: String,
        sync_id: String,
        tick_ms: u64,
        probes: usize,
    },
    /// One captured snapshot.
    Tick(Tick),
    /// Every probe's live state, as of the tick just evaluated.
    Probes { board: Vec<Probe> },
    /// A probe fired — recorded whether or not it ends the run.
    Violation {
        probe: String,
        height: Option<u32>,
        detail: String,
    },
    /// The run reached a terminal verdict.
    Finished {
        verdict: String,
        violations: usize,
        coverage_gaps: usize,
        ticks: u64,
    },
    /// An event kind this controller does not know about.
    #[serde(other)]
    Unknown,
}

/// The wire form of a [`Snapshot`]: the fields a watcher renders, with the
/// history-derived ones the engine already folded in.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Tick {
    pub seq: u64,
    pub elapsed_ms: u64,
    pub height: u32,
    pub target: Option<u32>,
    pub pct: f32,
    pub phase: String,
    pub reorg_depth: u32,
    pub sapling_outputs: u64,
    pub orchard_outputs: u64,
    pub ironwood_outputs: u64,
    pub balance: i64,
}

impl Tick {
    pub(crate) fn from_snapshot(snap: &Snapshot, elapsed: std::time::Duration) -> Self {
        Tick {
            seq: snap.seq(),
            elapsed_ms: elapsed.as_millis() as u64,
            height: snap.height(),
            target: snap.target(),
            pct: snap.pct(),
            phase: snap.phase().as_str().to_string(),
            reorg_depth: snap.reorg_depth(),
            sapling_outputs: snap.outputs(Pool::Sapling),
            orchard_outputs: snap.outputs(Pool::Orchard),
            ironwood_outputs: snap.outputs(Pool::Ironwood),
            balance: snap.balances().total(),
        }
    }
}

/// The wire form of a [`ProbeStatus`]: durations as milliseconds and the class
/// as a tag, so a raw `kubectl logs` read is legible and the format doesn't
/// inherit `Duration`'s `{secs,nanos}` encoding.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Probe {
    pub name: String,
    pub class: String,
    pub state: ProbeState,
    pub fatal: bool,
    pub since_ms: Option<u64>,
    pub window_ms: Option<u64>,
}

impl From<&ProbeStatus> for Probe {
    fn from(p: &ProbeStatus) -> Self {
        Probe {
            name: p.name.clone(),
            class: match p.class {
                Class::Always => "always",
                Class::Eventually => "eventually",
                Class::Sometimes => "sometimes",
                Class::AtCompletion => "at_completion",
            }
            .to_string(),
            state: p.state,
            fatal: p.severity == super::probe::Severity::Fatal,
            since_ms: p.since_satisfied.map(|d| d.as_millis() as u64),
            window_ms: p.window.map(|d| d.as_millis() as u64),
        }
    }
}

/// One event as it travels: the event itself plus the position it occupies in the
/// driver's stream.
///
/// The sequence exists because a log stream is not a reliable channel. A watcher
/// that loses its connection to a long sync resumes *by time*, which is
/// at-least-once — it must not lose an event, so it accepts replaying a few. The
/// sequence is what lets the fold stay exactly-once across that overlap: without
/// it, a replayed `Violation` would be counted twice.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Envelope {
    /// Absent from a driver predating the sequence — a sync in the `sync` tier
    /// can outlive the build that launched it — in which case a fold has no
    /// choice but to accept every event, as it did before.
    #[serde(default)]
    pub n: Option<u64>,
    #[serde(flatten)]
    pub event: SyncEvent,
}

/// The driver's event counter. Process-global rather than owned by the reporter
/// because the metrics poller publishes on its own task; [`encode`] is the single
/// point every event passes through, so stamping here is the only way the numbers
/// can be monotonic across both.
static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Serialize `event` as one tagged, sequenced line, newline-terminated.
pub(crate) fn encode(event: &SyncEvent) -> String {
    let n = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Serialization of these plain DTOs cannot fail; a bug here must not take
    // down a 48-hour sync, so it degrades to a line the controller skips.
    match serde_json::to_string(&Envelope {
        n: Some(n),
        event: event.clone(),
    }) {
        Ok(json) => format!("{EVENT_PREFIX}{json}\n"),
        Err(e) => format!("{EVENT_PREFIX}{{\"event\":\"encode_error\",\"detail\":\"{e}\"}}\n"),
    }
}

/// Write one event to the wire — the driver's stdout, which is the log a watcher
/// tails.
///
/// One `write_all` under a held lock, because `tracing` shares this fd: a split
/// write could interleave with a log line and corrupt both. Failure is ignored — a
/// broken stdout must not abort a 48-hour sync.
///
/// Lives beside the encoder rather than on the reporter because the reporter is
/// only reachable once an engine exists, and the provisioning events that precede
/// one still have to reach the same wire the same way.
pub(crate) fn publish(event: &SyncEvent) {
    use std::io::Write as _;

    let line = encode(event);
    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    let _ = lock.write_all(line.as_bytes());
    let _ = lock.flush();
}

/// Lift an event out of a driver log line. `None` means "ordinary log output" —
/// including a tagged line this build cannot parse, which stays visible as text
/// rather than vanishing.
pub(crate) fn decode(line: &str) -> Option<Envelope> {
    let json = line.trim_start().strip_prefix(EVENT_PREFIX)?;
    serde_json::from_str(json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick() -> Tick {
        Tick {
            seq: 42,
            elapsed_ms: 210_000,
            height: 901,
            target: Some(1024),
            pct: 88.1,
            phase: "Scanning".into(),
            reorg_depth: 0,
            sapling_outputs: 12,
            orchard_outputs: 34,
            ironwood_outputs: 0,
            balance: 1_000_000,
        }
    }

    #[test]
    fn tick_round_trips_through_a_log_line() {
        let line = encode(&SyncEvent::Tick(tick()));
        let Some(Envelope {
            event: SyncEvent::Tick(t),
            ..
        }) = decode(line.trim_end())
        else {
            panic!("did not decode as a tick: {line}");
        };
        assert_eq!((t.seq, t.height, t.target), (42, 901, Some(1024)));
    }

    /// Two events encoded in order must be distinguishable in order, whichever
    /// task produced them — that is what makes a resumed stream de-duplicable.
    #[test]
    fn successive_events_are_numbered_in_order() {
        let first = decode(encode(&SyncEvent::Tick(tick())).trim_end()).expect("an event");
        let second = decode(encode(&SyncEvent::Tick(tick())).trim_end()).expect("an event");
        assert!(
            second.n > first.n,
            "{:?} did not follow {:?}",
            second.n,
            first.n
        );
    }

    /// A sync outliving the build that launched it means unnumbered lines can
    /// still arrive; they must fold, not vanish.
    #[test]
    fn an_unnumbered_event_from_an_older_driver_still_decodes() {
        let line = format!(
            "{EVENT_PREFIX}{{\"event\":\"tick\",\"seq\":1,\"elapsed_ms\":0,\"height\":5,\"target\":null,\"pct\":0.0,\"phase\":\"Historic\",\"reorg_depth\":0,\"sapling_outputs\":0,\"orchard_outputs\":0,\"ironwood_outputs\":0,\"balance\":0}}"
        );
        let env = decode(&line).expect("an unnumbered event is still an event");
        assert_eq!(env.n, None);
        assert!(matches!(env.event, SyncEvent::Tick(_)));
    }

    /// The gates that report no single component omit `component` entirely, so the
    /// field has to be optional on the wire, not merely nullable.
    #[test]
    fn a_setup_event_without_a_component_still_decodes() {
        let line = format!(
            "{EVENT_PREFIX}{{\"n\":2,\"event\":\"setup\",\"phase\":\"indexer\",\
             \"detail\":\"waiting for gRPC GetLightdInfo\"}}"
        );
        let Some(Envelope {
            event: SyncEvent::Setup {
                phase, component, ..
            },
            ..
        }) = decode(&line)
        else {
            panic!("did not decode as a setup event: {line}");
        };
        assert_eq!((phase.as_str(), component), ("indexer", None));
    }

    #[test]
    fn an_ordinary_log_line_is_not_an_event() {
        assert!(decode("2026-08-04T15:28:07Z  INFO ztest::env: starting").is_none());
        assert!(decode("").is_none());
    }

    #[test]
    fn a_tagged_line_this_build_cannot_parse_is_left_as_output() {
        assert!(decode(&format!("{EVENT_PREFIX}not json at all")).is_none());
    }

    #[test]
    fn an_unknown_event_kind_decodes_rather_than_failing() {
        let line = format!("{EVENT_PREFIX}{{\"n\":7,\"event\":\"from_a_newer_driver\",\"x\":1}}");
        assert!(matches!(
            decode(&line),
            Some(Envelope {
                event: SyncEvent::Unknown,
                ..
            })
        ));
    }

    #[test]
    fn an_event_is_exactly_one_line() {
        let line = encode(&SyncEvent::Tick(tick()));
        assert!(line.ends_with('\n'));
        assert_eq!(line.trim_end().lines().count(), 1);
    }
}
