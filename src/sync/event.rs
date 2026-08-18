//! Driver→controller live event stream.
//!
//! - Controller stateless between commands (driver-pod log = only channel to live term)
//! - One [`EVENT_PREFIX`]-tagged line per event, lifted from otherwise-verbatim stream
//! - Data only, no formatting (renderers controller-side, so layout floats under a live sync)

use serde::{Deserialize, Serialize};

use super::probe::{Class, ProbeState, ProbeStatus};
use super::series::Timeline;
#[cfg(feature = "librustzcash")]
use super::snapshot::Snapshot;
use super::work::Work;

/// Stdout tag for a serialized [`SyncEvent`] (not a plausible log-line opening)
pub(crate) const EVENT_PREFIX: &str = "@ztest-sync-event ";

/// One observation published by the driver.
///
/// - Additive only (older controller skips unknown events, never fails)
/// - `Setup` = pre-engine window, else indistinguishable from a hang
/// - `Series` republishes whole run at constant size (controllers fold bounded tail)
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub(crate) enum SyncEvent {
    Setup {
        phase: String,
        detail: String,
        #[serde(default)]
        component: Option<String>,
    },
    Started {
        profile: String,
        sync_id: String,
        tick_ms: u64,
        probes: usize,
    },
    Tick(Tick),
    Series {
        timeline: Timeline,
    },
    Probes {
        board: Vec<Probe>,
    },
    Violation {
        probe: String,
        height: Option<u32>,
        detail: String,
    },
    Finished {
        verdict: super::SyncVerdict,
        violations: usize,
        coverage_gaps: usize,
        ticks: u64,
    },
    #[serde(other)]
    Unknown,
}

/// Wire form of [`Snapshot`].
///
/// - `work` ships **cumulative**, differenced controller-side through the one
///   [`rate::Window`](crate::rate::Window) the scrape path uses (`elapsed_ms` is the
///   driver's clock, so a resumed stream's gap is a real gap and the window restarts
///   across it rather than inventing a rate)
/// - op absent from map = unmeasured, renders `—` not `0`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Tick {
    pub seq: u64,
    pub elapsed_ms: u64,
    pub height: u32,
    pub target: Option<u32>,
    pub pct: f32,
    pub phase: super::Phase,
    pub reorg_depth: u32,
    #[serde(default)]
    pub work: Work,
}

impl Tick {
    #[cfg(feature = "librustzcash")]
    pub(crate) fn from_snapshot(snap: &Snapshot, elapsed: std::time::Duration) -> Self {
        Tick {
            seq: snap.seq(),
            elapsed_ms: elapsed.as_millis() as u64,
            height: snap.height(),
            target: snap.target(),
            pct: snap.pct(),
            phase: snap.phase(),
            reorg_depth: snap.reorg_depth(),
            work: snap.work(),
        }
    }

    pub(crate) fn at(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.elapsed_ms)
    }
}

impl From<&Tick> for crate::sync::Observation {
    fn from(t: &Tick) -> crate::sync::Observation {
        crate::sync::Observation {
            height: Some(t.height),
            target: t.target,
            reported_pct: Some(t.pct),
            transactions: None,
            work: t.work,
            cost: crate::sync::Cost::default(),
        }
    }
}

/// Wire form of [`ProbeStatus`]. Millis + tags, not `Duration`'s `{secs,nanos}`
/// (raw `kubectl logs` stays legible)
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

/// Event + its position in the driver's stream.
///
/// - Watchers resume by time = at-least-once; `n` de-dupes the replayed overlap
/// - `n` = `None` from a pre-sequence driver (sync outlives its build) → fold accepts all
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Envelope {
    #[serde(default)]
    pub n: Option<u64>,
    #[serde(flatten)]
    pub event: SyncEvent,
}

/// Driver event counter. Process-global so every publisher draws one sequence
/// ([`encode`] = sole stamp point → monotonic)
static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Serialize `event` as one tagged, sequenced line, newline-terminated.
pub(crate) fn encode(event: &SyncEvent) -> String {
    let n = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Cannot fail for plain DTOs; degrade to a skippable line, never kill a 48h sync
    match serde_json::to_string(&Envelope { n: Some(n), event: event.clone() }) {
        Ok(json) => format!("{EVENT_PREFIX}{json}\n"),
        Err(e) => format!("{EVENT_PREFIX}{{\"event\":\"encode_error\",\"detail\":\"{e}\"}}\n"),
    }
}

/// Write one event to driver stdout, the log a watcher tails.
///
/// - Single `write_all` under lock (`tracing` shares this fd, split write corrupts both)
/// - Failure ignored (broken stdout must not abort a 48h sync)
pub(crate) fn publish(event: &SyncEvent) {
    use std::io::Write as _;

    let line = encode(event);
    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    let _ = lock.write_all(line.as_bytes());
    let _ = lock.flush();
}

/// Lift an event out of a driver log line. `None` = ordinary log output, incl. a
/// tagged line this build cannot parse (stays visible as text, never dropped)
pub(crate) fn decode(line: &str) -> Option<Envelope> {
    let json = line.trim_start().strip_prefix(EVENT_PREFIX)?;
    serde_json::from_str(json).ok()
}

#[cfg(test)]
mod tests {
    use super::super::work::{Op, Work};
    use super::*;

    fn tick() -> Tick {
        Tick {
            seq: 42,
            elapsed_ms: 210_000,
            height: 901,
            target: Some(1024),
            pct: 88.1,
            phase: crate::sync::Phase::Historic,
            reorg_depth: 0,
            work: {
                let mut w = Work::ZERO;
                w.set(Op::SaplingOutput, 5);
                w
            },
        }
    }

    #[test]
    fn tick_round_trips_through_a_log_line() {
        let line = encode(&SyncEvent::Tick(tick()));
        let Some(Envelope { event: SyncEvent::Tick(t), .. }) = decode(line.trim_end()) else {
            panic!("did not decode as a tick: {line}");
        };
        assert_eq!((t.seq, t.height, t.target), (42, 901, Some(1024)));
    }

    /// Ordering across producers = what makes a resumed stream de-dupable
    #[test]
    fn successive_events_are_numbered_in_order() {
        let first = decode(encode(&SyncEvent::Tick(tick())).trim_end()).expect("an event");
        let second = decode(encode(&SyncEvent::Tick(tick())).trim_end()).expect("an event");
        assert!(second.n > first.n, "{:?} did not follow {:?}", second.n, first.n);
    }

    /// - Unnumbered lines from an older driver must fold, not vanish
    /// - Literal = real pre-work-vector tick: retired `*_outputs` ignored, work map
    ///   empty = *unmeasured* → why the panel shows `—` not `0`
    #[test]
    fn an_unnumbered_event_from_an_older_driver_still_decodes() {
        let line = format!(
            "{EVENT_PREFIX}{{\"event\":\"tick\",\"seq\":1,\"elapsed_ms\":0,\"height\":5,\"target\":null,\"pct\":0.0,\"phase\":\"Historic\",\"reorg_depth\":0,\"sapling_outputs\":0,\"orchard_outputs\":0,\"ironwood_outputs\":0,\"balance\":0}}"
        );
        let env = decode(&line).expect("an unnumbered event is still an event");
        assert_eq!(env.n, None);
        let SyncEvent::Tick(t) = env.event else {
            panic!("did not decode as a tick");
        };
        assert_eq!(t.height, 5);
        assert_eq!(t.reorg_depth, 0, "an absent field decodes to its default");
        assert_eq!(t.work.get(Op::SaplingOutput), None);
        assert_eq!(t.work.total(), None);
    }

    /// Gates with no single component omit the field → optional on the wire, not nullable
    #[test]
    fn a_setup_event_without_a_component_still_decodes() {
        let line = format!(
            "{EVENT_PREFIX}{{\"n\":2,\"event\":\"setup\",\"phase\":\"indexer\",\
             \"detail\":\"waiting for gRPC GetLightdInfo\"}}"
        );
        let Some(Envelope { event: SyncEvent::Setup { phase, component, .. }, .. }) = decode(&line)
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
        assert!(matches!(decode(&line), Some(Envelope { event: SyncEvent::Unknown, .. })));
    }

    #[test]
    fn an_event_is_exactly_one_line() {
        let line = encode(&SyncEvent::Tick(tick()));
        assert!(line.ends_with('\n'));
        assert_eq!(line.trim_end().lines().count(), 1);
    }
}
