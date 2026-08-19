//! Observing a component from outside the cluster: one scrape → live columns, plus a
//! trailing window turning cumulative counters into rates.
//!
//! - [`Observe`] = backend-implemented, the only place knowing which exposed families
//!   mean height / work / per-block cost
//! - [`Window`] = smoothing, stated once per observer rather than per column, over the
//!   crate-wide [`rate`](crate::rate) estimator
//! - Not a [`ProgressView`](super::ProgressView): an outside scrape cannot know
//!   [`Phase`](super::Phase), and an `Unknown` variant no subject reports would face
//!   every probe matching on one. The shared part is [`Work`], reused whole

use super::work::{Op, Rate, Work};
use crate::metrics::Exposition;
use crate::rate::{Pace, Stamp};

/// Where a component's per-block time goes; all-`None` for one publishing no timing
/// summaries (a fact about it, not a zero).
///
/// - `fetch_ms`/`treestate_ms` = source reads (in-process backend → this cpu, not upstream)
/// - `parse_ms` = build - both = assembly the component does itself
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Cost {
    pub fetch_ms: Option<f64>,
    pub treestate_ms: Option<f64>,
    pub parse_ms: Option<f64>,
    pub grpc_ms: Option<f64>,
}

/// One scrape resolved into the live columns.
///
/// - Counters stay **cumulative** as the wire has them (differencing = [`Window`]'s
///   job; only the reader knows the span it wants)
/// - `height` = the *moving* frontier, not the durable one (once-per-commit carries no
///   per-second number)
/// - `reported_pct` = the subject's own figure where it has one; height/target is only
///   the fallback (a wallet scanning tip-first is far further along than height says)
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Observation {
    pub height: Option<u32>,
    pub target: Option<u32>,
    pub reported_pct: Option<f32>,
    /// Cumulative transactions scanned. Outside [`Work`]: a tx spans many ops, so an
    /// `Op` variant would double-count every total and stack a pool graph wrong
    pub transactions: Option<u64>,
    pub work: Work,
    pub cost: Cost,
}

impl Observation {
    /// Fraction complete in `0.0..=100.0`, `0.0` until both ends are known. Denominator
    /// is a per-scrape reading → can move backwards on a live chain; never assume it rises
    pub fn pct(&self) -> f32 {
        if let Some(pct) = self.reported_pct {
            return pct;
        }
        match (self.height, self.target) {
            (Some(h), Some(t)) if t > 0 => (100.0 * f64::from(h) / f64::from(t)) as f32,
            _ => 0.0,
        }
    }
}

/// Subject's own reading, the driver-side counterpart of a scrape — so both sides of
/// the sync UI difference the same shape through the same [`Window`].
///
/// `cost` unset: per-block timing lives in an exporter's summaries, not in the subject
impl From<&super::Snapshot> for Observation {
    fn from(snap: &super::Snapshot) -> Observation {
        Observation {
            height: Some(snap.height()),
            target: snap.target(),
            reported_pct: Some(snap.pct()),
            // Driver ticks carry no tx counter (exporter-only, like `cost`)
            transactions: None,
            work: snap.work(),
            cost: Cost::default(),
        }
    }
}

/// The height families a component publishes. Read in a different order by a probe than
/// by a display, so the preference lives here as data rather than as two hand-written
/// resolvers that can disagree.
#[derive(Debug, Clone, Copy)]
pub struct Heights {
    /// Durable frontier — written and fsynced. What a probe gates on
    pub committed: &'static str,
    /// Frontier built ahead of the next commit; moves per block. What a display shows,
    /// since `committed` steps once per commit and carries no per-second rate
    pub live: Option<&'static str>,
    /// Completion denominator. `None` = component publishes no target
    pub target: Option<&'static str>,
}

/// Live progress readable from one scrape of a component's exporter.
///
/// Keeps metric names out of the display: implemented beside the family constants a
/// backend owns → a renamed family breaks here, not as an em-dash indistinguishable
/// from a value still in flight.
///
/// [`HEIGHTS`](Self::HEIGHTS) and [`WORK_OPS`](Self::WORK_OPS) are the whole declaration;
/// every reader below is derived from them, so a probe and a panel cannot drift. Only
/// [`observe`](Self::observe) is hand-written, and only for the parts that are genuinely
/// per-component (cost breakdown, transaction count)
pub trait Observe: crate::metrics::MetricLayout {
    const HEIGHTS: Heights;

    /// Per-[`Op`] counters this component publishes. An `Op` absent here stays
    /// unmeasured: [`Work::require`] panics rather than compare a zero that can never fail
    const WORK_OPS: &'static [(Op, &'static str)];

    /// `None` = not this component's exposition (nothing it should publish is present)
    fn observe(exposition: &Exposition) -> Option<Observation>;

    /// Counters named by [`WORK_OPS`](Self::WORK_OPS); absent ones left unmeasured
    fn work_of(exposition: &Exposition) -> Work {
        let mut work = Work::ZERO;
        for &(op, family) in Self::WORK_OPS {
            if let Some(n) = exposition.counter_total(family) {
                work.set(op, n);
            }
        }
        work
    }

    /// Which family measures `op`, or `None` when this component does not count it
    fn work_source(op: Op) -> Option<&'static str> {
        Self::WORK_OPS.iter().find_map(|&(o, family)| (o == op).then_some(family))
    }

    /// Zero filtered out: a tip not yet known, not a zero-length chain (renders 100 %)
    fn target_of(exposition: &Exposition) -> Option<u32> {
        Self::HEIGHTS.target.and_then(|f| exposition.height_gauge(f)).filter(|&t| t > 0)
    }

    /// Durable first — what a probe gates on
    fn committed_height(exposition: &Exposition) -> Option<u32> {
        Self::height(exposition, [Some(Self::HEIGHTS.committed), Self::HEIGHTS.live])
    }

    /// Live first — what a display shows, so it moves per block
    fn live_height(exposition: &Exposition) -> Option<u32> {
        Self::height(exposition, [Self::HEIGHTS.live, Some(Self::HEIGHTS.committed)])
    }

    #[doc(hidden)]
    fn height(exposition: &Exposition, order: [Option<&'static str>; 2]) -> Option<u32> {
        order.into_iter().flatten().find_map(|f| exposition.height_gauge(f))
    }
}

/// Trailing window over successive [`Observation`]s → per-second rates.
///
/// - Holds observations, not per-column accumulators → every rate shares two endpoints,
///   so pool rates / scan rate / total cannot disagree about the span
/// - `S` = the clock the source measured on: [`Instant`](std::time::Instant) for a
///   scrape observed here, [`Duration`](std::time::Duration) for a driver publishing
///   its own elapsed
pub type Window<S = std::time::Instant> = crate::rate::Window<Observation, S>;

impl<S: Stamp> Window<S> {
    /// Blocks/sec + time to `target`. `None` while the frontier retreats (a reorg is no
    /// negative scan rate, a restart's counters no rate at all); both self-heal as the
    /// window rolls forward
    pub fn block_pace(&self) -> Option<Pace> {
        let remaining = self
            .latest()
            .and_then(|o| Some(f64::from(o.target?.saturating_sub(o.height.unwrap_or(0)))));
        self.pace_by(|o| o.height.map(f64::from), remaining)
    }

    /// Transactions/sec across the window. `None` while unmeasured or after a restart
    /// re-published the counter from zero (a decreasing cumulative = no rate, not a
    /// negative one)
    pub fn tx_rate(&self) -> Option<f64> {
        self.pace_by(|o| o.transactions.map(|t| t as f64), None).map(|p| p.per_sec)
    }

    /// Protocol work/sec across the window, per op. [`Work::delta`] saturates and covers
    /// only ops **both** ends measured → a counter reset reads zero, not negative, and a
    /// dropped op leaves rather than freezes
    pub fn work_rate(&self) -> Option<Rate> {
        let (first, last, elapsed) = self.endpoints()?;
        Some(last.work.delta(&first.work).rate(elapsed))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::sync::Op;

    fn observation(height: u32, transparent: u64) -> Observation {
        let mut work = Work::ZERO;
        work.set(Op::TransparentOut, transparent);
        Observation {
            height: Some(height),
            target: Some(1_000),
            reported_pct: None,
            transactions: None,
            work,
            cost: Cost::default(),
        }
    }

    /// `(second, height, transparent-ops)` off one origin → the interval under test is
    /// exactly the one written here
    fn window(samples: &[(u64, u32, u64)]) -> Window {
        let origin = Instant::now();
        let mut w = Window::new(Duration::from_secs(1));
        for &(secs, height, transparent) in samples {
            w.push(origin + Duration::from_secs(secs), observation(height, transparent));
        }
        w
    }

    #[test]
    fn one_scrape_is_not_a_rate() {
        let w = window(&[(0, 100, 10)]);
        assert_eq!(w.block_pace(), None);
        assert_eq!(w.work_rate(), None);
    }

    #[test]
    fn rates_measure_across_the_window_not_between_the_last_two() {
        // 100 blocks over 4s, last second alone did 10: per-sample would say 10/s,
        // the window averages so a bursty exporter stays readable
        let w = window(&[(0, 0, 0), (1, 10, 100), (2, 80, 800), (3, 90, 900), (4, 100, 1_000)]);
        assert_eq!(w.block_pace().map(|p| p.per_sec), Some(25.0));
        assert_eq!(w.work_rate().unwrap().get(Op::TransparentOut), Some(250.0));
    }

    #[test]
    fn samples_older_than_the_window_are_dropped() {
        // 1s scrape = 4s window → t=0 is out by t=5
        let w = window(&[(0, 0, 0), (5, 500, 0), (9, 900, 0)]);
        assert_eq!(w.block_pace().map(|p| p.per_sec), Some(100.0), "measured from t=5, not t=0");
    }

    #[test]
    fn a_frontier_going_backwards_is_not_a_negative_rate() {
        let w = window(&[(0, 500, 0), (1, 400, 0)]);
        assert_eq!(w.block_pace(), None);
    }

    /// Restart re-publishes counters from zero → saturating [`Work::delta`] must read
    /// no work, not a negative rate
    #[test]
    fn a_counter_reset_reads_as_zero_work_not_as_a_negative_rate() {
        let w = window(&[(0, 500, 9_000), (1, 501, 5)]);
        assert_eq!(w.work_rate().unwrap().get(Op::TransparentOut), Some(0.0));
    }

    /// `(second, cumulative transactions)` → the tx window under test
    fn tx_window(samples: &[(u64, Option<u64>)]) -> Window {
        let origin = Instant::now();
        let mut w = Window::new(Duration::from_secs(10));
        for &(secs, transactions) in samples {
            let mut o = observation(0, 0);
            o.transactions = transactions;
            w.push(origin + Duration::from_secs(secs), o);
        }
        w
    }

    #[test]
    fn transactions_per_second_measures_across_the_window() {
        let w = tx_window(&[(0, Some(1_000)), (2, Some(1_400)), (4, Some(2_600))]);
        assert_eq!(w.tx_rate(), Some(400.0));
    }

    /// A component publishing no tx counter must read `—`, never an idle `0`
    #[test]
    fn an_unpublished_tx_counter_is_not_a_zero_rate() {
        assert_eq!(tx_window(&[(0, None), (1, None)]).tx_rate(), None);
    }

    /// Restart re-publishes from zero; a decreasing cumulative is no rate at all
    #[test]
    fn a_tx_counter_reset_reads_as_no_rate() {
        assert_eq!(tx_window(&[(0, Some(9_000)), (1, Some(5))]).tx_rate(), None);
    }

    #[test]
    fn one_tx_sample_is_not_a_rate() {
        assert_eq!(tx_window(&[(0, Some(1_000))]).tx_rate(), None);
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
