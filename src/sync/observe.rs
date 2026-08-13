//! Observing a component from outside the cluster: one scrape resolved into the
//! columns a live display draws, and a trailing window over successive scrapes
//! that turns cumulative counters into rates.
//!
//! Two things live here and nothing else. [`Observe`] is what a backend
//! implements — the one place that knows which of its exposed families mean
//! "height", "work" and "per-block cost". [`Window`] is the smoothing, stated
//! once for every observer rather than per column.
//!
//! Deliberately *not* a [`ProgressView`](super::ProgressView): a subject's own
//! tick knows its [`Phase`](super::Phase), and a scrape from outside cannot.
//! Inventing an `Unknown` phase to satisfy the trait would put a variant no
//! subject ever reports in front of every probe that matches on one. What the
//! two genuinely share is [`Work`] — the op taxonomy, its measured/unmeasured
//! bitset, and the `delta`/`rate` arithmetic below — and that is reused whole.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::work::{Rate, Work};
use crate::metrics::Exposition;

/// Where a component's per-block time goes. Every field `None` for a component
/// that publishes no timing summaries — which is a fact about that component,
/// not a zero.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Cost {
    /// Waiting on the upstream validator, per block.
    pub fetch_ms: Option<f64>,
    /// The component's own work per block, with the upstream wait taken out.
    /// The number that says whether a slow sync is this component's fault.
    pub parse_ms: Option<f64>,
    /// Mean latency of the requests it serves.
    pub grpc_ms: Option<f64>,
}

/// One scrape, resolved into the live columns.
///
/// Counters stay **cumulative**, exactly as the wire has them: differencing is
/// [`Window`]'s job, because only the reader knows the span it wants to measure
/// over. A resolver that pre-computed rates would be fixing that choice at the
/// wrong layer, and would need per-call state to do it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Observation {
    /// How far the component has ingested. The *moving* frontier, not the
    /// durable one: a display's rate is only as smooth as the height under it,
    /// and a frontier that advances once per commit cannot carry a per-second
    /// number.
    pub height: Option<u32>,
    /// What it is working towards, if it knows.
    pub target: Option<u32>,
    /// Cumulative protocol work, in the canonical taxonomy.
    pub work: Work,
    pub cost: Cost,
}

impl Observation {
    /// Fraction complete in `0.0..=100.0`, or `0.0` until both ends are known.
    ///
    /// The denominator is a per-scrape reading, not a constant, so on a live
    /// chain this can move backwards — the component gained ground while the
    /// tip gained more. That is a true reading; nothing may assume it rises.
    pub fn pct(&self) -> f32 {
        match (self.height, self.target) {
            (Some(h), Some(t)) if t > 0 => (100.0 * f64::from(h) / f64::from(t)) as f32,
            _ => 0.0,
        }
    }
}

/// A component whose live progress can be read from one scrape of its exporter.
///
/// The seam that keeps a display free of a component's metric names. A backend
/// implements it beside the family constants it already owns, so a family that
/// is renamed or dropped breaks here — in the module that knows about it —
/// rather than rendering as an em-dash indistinguishable from a value that has
/// not arrived yet.
pub trait Observe {
    /// Resolve one exposition. `None` when this is not this component's
    /// exposition at all — nothing it is expected to publish is present.
    fn observe(exposition: &Exposition) -> Option<Observation>;
}

/// Scrapes per rate window.
///
/// Prometheus's own `rate()` measures across a window rather than between
/// adjacent samples, and Grafana's `$__rate_interval` makes that window at
/// least four scrape intervals — below that, one late or dropped scrape
/// dominates the result. This is that rule, and it is the whole answer to a
/// bursty exporter: no exponential smoothing on top, because a sliding window
/// *is* the smoother, and stacking a second one would add lag without adding
/// stability.
const WINDOW_SCRAPES: u32 = 4;

/// A trailing window over successive [`Observation`]s, yielding per-second
/// rates.
///
/// Holds the observations themselves rather than one accumulator per column:
/// every rate then comes from the same two endpoints, so the pool rates, the
/// scan rate and the total cannot disagree about which span they describe.
#[derive(Debug)]
pub struct Window {
    span: Duration,
    ring: VecDeque<(Instant, Observation)>,
}

impl Window {
    /// A window over a source scraped every `scrape`.
    pub fn new(scrape: Duration) -> Window {
        Window {
            span: scrape * WINDOW_SCRAPES,
            ring: VecDeque::new(),
        }
    }

    /// Record one scrape, dropping whatever has aged out of the window.
    ///
    /// A gap wider than the window itself starts the window over rather than
    /// measuring across it: a watcher that lost its target for ten minutes and
    /// regained it has no idea what happened in between, and averaging the whole
    /// outage into one rate would state that it does.
    pub fn push(&mut self, at: Instant, observation: Observation) {
        if let Some((last, _)) = self.ring.back()
            && at.saturating_duration_since(*last) > self.span
        {
            self.ring.clear();
        }
        while let Some((oldest, _)) = self.ring.front() {
            if at.saturating_duration_since(*oldest) <= self.span {
                break;
            }
            self.ring.pop_front();
        }
        self.ring.push_back((at, observation));
    }

    /// The newest scrape.
    pub fn latest(&self) -> Option<&Observation> {
        self.ring.back().map(|(_, o)| o)
    }

    /// The window's endpoints and the interval between them, once there are two
    /// distinct instants to measure across.
    fn span(&self) -> Option<(&Observation, &Observation, Duration)> {
        let ((first_at, first), (last_at, last)) = (self.ring.front()?, self.ring.back()?);
        let elapsed = last_at.saturating_duration_since(*first_at);
        (!elapsed.is_zero()).then_some((first, last, elapsed))
    }

    /// Blocks ingested per second across the window.
    ///
    /// `None` while the frontier is going backwards — a reorg is not a negative
    /// scan rate, and a restarted component's counters are not a rate at all.
    /// Both self-heal as the window rolls forward.
    pub fn blocks_per_sec(&self) -> Option<f64> {
        let (first, last, elapsed) = self.span()?;
        let (from, to) = (first.height?, last.height?);
        (to >= from).then(|| f64::from(to - from) / elapsed.as_secs_f64())
    }

    /// Protocol work per second across the window, per op.
    ///
    /// [`Work::delta`] is saturating and speaks only about ops **both** ends
    /// measured, so a counter reset reads as a window of zero rather than as a
    /// negative rate, and an op the component stopped publishing drops out
    /// instead of freezing.
    pub fn work_rate(&self) -> Option<Rate> {
        let (first, last, elapsed) = self.span()?;
        Some(last.work.delta(&first.work).rate(elapsed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::Op;

    fn observation(height: u32, transparent: u64) -> Observation {
        let mut work = Work::ZERO;
        work.set(Op::TransparentOut, transparent);
        Observation {
            height: Some(height),
            target: Some(1_000),
            work,
            cost: Cost::default(),
        }
    }

    /// A window fed `(second, height, transparent-ops)` samples off one origin,
    /// so the interval under test is exactly the one written here.
    fn window(samples: &[(u64, u32, u64)]) -> Window {
        let origin = Instant::now();
        let mut w = Window::new(Duration::from_secs(1));
        for &(secs, height, transparent) in samples {
            w.push(
                origin + Duration::from_secs(secs),
                observation(height, transparent),
            );
        }
        w
    }

    #[test]
    fn one_scrape_is_not_a_rate() {
        let w = window(&[(0, 100, 10)]);
        assert_eq!(w.blocks_per_sec(), None);
        assert_eq!(w.work_rate(), None);
    }

    #[test]
    fn rates_measure_across_the_window_not_between_the_last_two() {
        // 100 blocks over 4s, but the last second alone did 10. A per-sample
        // rate would report 10/s; the window reports the average that makes a
        // bursty exporter readable.
        let w = window(&[
            (0, 0, 0),
            (1, 10, 100),
            (2, 80, 800),
            (3, 90, 900),
            (4, 100, 1_000),
        ]);
        assert_eq!(w.blocks_per_sec(), Some(25.0));
        assert_eq!(w.work_rate().unwrap().get(Op::TransparentOut), Some(250.0));
    }

    #[test]
    fn samples_older_than_the_window_are_dropped() {
        // A 1s scrape gives a 4s window: the sample at t=0 is out by t=5.
        let w = window(&[(0, 0, 0), (5, 500, 0), (9, 900, 0)]);
        assert_eq!(
            w.blocks_per_sec(),
            Some(100.0),
            "measured from t=5, not t=0"
        );
    }

    #[test]
    fn a_frontier_going_backwards_is_not_a_negative_rate() {
        let w = window(&[(0, 500, 0), (1, 400, 0)]);
        assert_eq!(w.blocks_per_sec(), None);
    }

    /// A restarted component re-publishes its counters from zero. Saturating
    /// [`Work::delta`] must read that as no work rather than as a negative rate.
    #[test]
    fn a_counter_reset_reads_as_zero_work_not_as_a_negative_rate() {
        let w = window(&[(0, 500, 9_000), (1, 501, 5)]);
        assert_eq!(w.work_rate().unwrap().get(Op::TransparentOut), Some(0.0));
    }

    #[test]
    fn an_unmeasured_op_stays_unmeasured_through_the_window() {
        let w = window(&[(0, 0, 0), (1, 10, 100)]);
        assert_eq!(
            w.work_rate().unwrap().get(Op::OrchardAction),
            None,
            "a pool nobody counted must not surface as an idle zero"
        );
    }
}
