//! Trailing sliding window over sampled counters → smoothed rates + projections.
//!
//! Sole rate estimator in the crate: metric scrapes, driver ticks and seed/image byte
//! counters all derive their per-second figures here.
//!
//! - Holds samples, not per-column accumulators → every rate off one window shares its
//!   two endpoints (pool rates / scan rate / total cannot disagree about the span)
//! - Sliding window *is* the smoother (no EWMA on top)
//! - [`Pace::eta`] gated on the window agreeing with itself, never on a per-domain rate
//!   floor — steadiness is dimensionless, so bytes and blocks qualify on one rule

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Monotonic stamp a window measures across.
///
/// - `Instant` = observed here (scrape arrival, byte report)
/// - `Duration` = the source's own elapsed, where arrival time is not measurement time
///   (a resumed driver log replays a minute of ticks in milliseconds)
pub trait Stamp: Copy {
    fn since(self, earlier: Self) -> Duration;
}

impl Stamp for Instant {
    fn since(self, earlier: Instant) -> Duration {
        self.saturating_duration_since(earlier)
    }
}

impl Stamp for Duration {
    fn since(self, earlier: Duration) -> Duration {
        self.saturating_sub(earlier)
    }
}

/// Scalar counter a window differences directly, with no projection function
pub trait Counter: Copy {
    fn as_f64(self) -> f64;
}

impl Counter for u32 {
    fn as_f64(self) -> f64 {
        f64::from(self)
    }
}

impl Counter for u64 {
    fn as_f64(self) -> f64 {
        self as f64
    }
}

/// Samples per window, per Grafana's `$__rate_interval` rule of ≥ 4 sample intervals
/// (below that one late/dropped sample dominates)
const WINDOW_SAMPLES: u32 = 4;

/// Intervals that must agree before a projection is offered — one is a measurement,
/// three a trend
const STEADY_INTERVALS: usize = 3;

/// Ceiling on the per-interval rates' coefficient of variation. Dimensionless by
/// construction, so bytes, blocks and protocol ops earn an ETA on the same evidence
const STEADY_COV: f64 = 0.35;

/// Smoothed rate over a window, plus what it supports projecting.
///
/// `eta` is `None` wherever the window disagrees with itself — ramp-up, a stall, a
/// bursty source — since a countdown off an unsteady rate moves backwards as often as
/// forwards
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Pace {
    pub per_sec: f64,
    pub eta: Option<Duration>,
}

/// Trailing window over successive samples of `T`, keyed by `S`
#[derive(Debug)]
pub struct Window<T, S = Instant> {
    span: Duration,
    ring: VecDeque<(S, T)>,
}

impl<T, S: Stamp> Window<T, S> {
    /// Window over a source sampled every `interval`
    pub fn new(interval: Duration) -> Window<T, S> {
        Window { span: interval * WINDOW_SAMPLES, ring: VecDeque::new() }
    }

    /// Record one sample, dropping what aged out. A gap wider than the window restarts
    /// it (averaging an outage into one rate claims knowledge of what happened inside)
    pub fn push(&mut self, at: S, sample: T) {
        if let Some((last, _)) = self.ring.back()
            && at.since(*last) > self.span
        {
            self.ring.clear();
        }
        while let Some((oldest, _)) = self.ring.front() {
            if at.since(*oldest) <= self.span {
                break;
            }
            self.ring.pop_front();
        }
        self.ring.push_back((at, sample));
    }

    pub fn latest(&self) -> Option<&T> {
        self.ring.back().map(|(_, s)| s)
    }

    /// Endpoints + interval, once two distinct stamps exist to measure across
    pub(crate) fn endpoints(&self) -> Option<(&T, &T, Duration)> {
        let ((first_at, first), (last_at, last)) = (self.ring.front()?, self.ring.back()?);
        let elapsed = last_at.since(*first_at);
        (!elapsed.is_zero()).then_some((first, last, elapsed))
    }

    /// Pace of the scalar `read` extracts, projected over `remaining` units.
    ///
    /// - `None` = under two readings, no span, or a counter that regressed (a reset or
    ///   reorg is no negative rate); each self-heals as the window rolls forward
    /// - Samples `read` declines to answer sit out entirely — they neither anchor the
    ///   span nor count toward steadiness
    pub fn pace_by(
        &self,
        read: impl Fn(&T) -> Option<f64>,
        remaining: Option<f64>,
    ) -> Option<Pace> {
        let series: Vec<(S, f64)> =
            self.ring.iter().filter_map(|(at, s)| Some((*at, read(s)?))).collect();
        let ((first_at, first), (last_at, last)) = (series.first()?, series.last()?);
        let elapsed = last_at.since(*first_at);
        if elapsed.is_zero() || last < first {
            return None;
        }
        let per_sec = (last - first) / elapsed.as_secs_f64();
        let eta = remaining
            .filter(|_| per_sec > 0.0 && steady(&series))
            .map(|left| Duration::from_secs_f64(left / per_sec));
        Some(Pace { per_sec, eta })
    }
}

impl<C: Counter, S: Stamp> Window<C, S> {
    pub fn pace(&self, remaining: Option<f64>) -> Option<Pace> {
        self.pace_by(|c| Some(c.as_f64()), remaining)
    }

    pub fn per_sec(&self) -> Option<f64> {
        Some(self.pace(None)?.per_sec)
    }
}

/// Do the per-interval rates agree? Endpoint arithmetic alone cannot separate a steady
/// stream from one that stalled and then burst to the same average
fn steady<S: Stamp>(series: &[(S, f64)]) -> bool {
    let rates: Vec<f64> = series
        .windows(2)
        .filter_map(|pair| {
            let dt = pair[1].0.since(pair[0].0).as_secs_f64();
            (dt > 0.0 && pair[1].1 >= pair[0].1).then(|| (pair[1].1 - pair[0].1) / dt)
        })
        .collect();
    if rates.len() < STEADY_INTERVALS {
        return false;
    }
    let mean = rates.iter().sum::<f64>() / rates.len() as f64;
    if mean <= 0.0 {
        return false;
    }
    let variance = rates.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / rates.len() as f64;
    variance.sqrt() / mean <= STEADY_COV
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(second, counter)` off one origin → the interval under test is exactly the one
    /// written here
    fn window(samples: &[(u64, u64)]) -> Window<u64, Duration> {
        let mut w = Window::new(Duration::from_secs(1));
        for &(secs, count) in samples {
            w.push(Duration::from_secs(secs), count);
        }
        w
    }

    fn steady_window() -> Window<u64, Duration> {
        window(&[(0, 0), (1, 100), (2, 200), (3, 300), (4, 400)])
    }

    #[test]
    fn one_sample_is_not_a_rate() {
        assert_eq!(window(&[(0, 100)]).per_sec(), None);
    }

    #[test]
    fn the_rate_measures_across_the_window_not_between_the_last_two() {
        // 400 units over 4s, last second alone did 100: per-sample would say 100/s
        assert_eq!(steady_window().per_sec(), Some(100.0));
    }

    #[test]
    fn samples_older_than_the_window_are_dropped() {
        // 1s interval = 4s window → t=0 is out by t=5
        let w = window(&[(0, 0), (5, 500), (9, 900)]);
        assert_eq!(w.per_sec(), Some(100.0), "measured from t=5, not t=0");
    }

    /// A stalled transfer keeps reporting its count; flat samples must read as a real
    /// zero, not the last rate frozen in place
    #[test]
    fn a_flat_counter_reads_as_a_stall() {
        assert_eq!(window(&[(0, 500), (1, 500), (2, 500)]).per_sec(), Some(0.0));
    }

    /// Job retry re-pulls from zero: no rate until the window holds only new samples
    #[test]
    fn a_counter_reset_is_not_a_negative_rate() {
        assert_eq!(window(&[(0, 900), (1, 5)]).per_sec(), None);
    }

    #[test]
    fn a_gap_wider_than_the_window_restarts_it() {
        let mut w = window(&[(0, 0), (1, 100)]);
        w.push(Duration::from_secs(60), 200);
        assert_eq!(w.per_sec(), None, "the outage is not averaged into a rate");
    }

    // ─────────────────────────── projection ───────────────────────────

    #[test]
    fn a_steady_window_projects_the_time_remaining() {
        let pace = steady_window().pace(Some(1_000.0)).expect("a rate");
        assert_eq!(pace.per_sec, 100.0);
        assert_eq!(pace.eta, Some(Duration::from_secs(10)));
    }

    /// The rate still publishes — only the countdown off it is withheld
    #[test]
    fn a_bursty_window_measures_but_does_not_project() {
        // Same 400 units over the same 4s, delivered in one burst after three idle seconds
        let w = window(&[(0, 0), (1, 0), (2, 0), (3, 0), (4, 400)]);
        let pace = w.pace(Some(1_000.0)).expect("a rate");
        assert_eq!(pace.per_sec, 100.0, "the average is unchanged");
        assert_eq!(pace.eta, None, "a stall-then-burst window projects nothing");
    }

    /// Under [`STEADY_INTERVALS`] there is a measurement but no trend
    #[test]
    fn a_young_window_measures_but_does_not_project() {
        let pace = window(&[(0, 0), (1, 100)]).pace(Some(1_000.0)).expect("a rate");
        assert_eq!(pace.per_sec, 100.0);
        assert_eq!(pace.eta, None);
    }

    #[test]
    fn a_stall_projects_nothing_however_steady_it_looks() {
        let w = window(&[(0, 5), (1, 5), (2, 5), (3, 5), (4, 5)]);
        let pace = w.pace(Some(1_000.0)).expect("a rate");
        assert_eq!(pace.per_sec, 0.0);
        assert_eq!(pace.eta, None, "an infinite arrival is not an ETA");
    }

    /// Nothing left to wait for; the tail step owns that, not a `0s left`
    #[test]
    fn no_remaining_means_no_projection() {
        assert_eq!(steady_window().pace(None).expect("a rate").eta, None);
    }

    /// Mild jitter is what a real transfer looks like — it must still project
    #[test]
    fn ordinary_jitter_still_projects() {
        let w = window(&[(0, 0), (1, 95), (2, 205), (3, 295), (4, 400)]);
        assert!(w.pace(Some(400.0)).expect("a rate").eta.is_some());
    }

    /// A sample the reader cannot resolve sits out rather than breaking the series
    #[test]
    fn unreadable_samples_sit_out_of_the_span() {
        let mut w: Window<Option<u64>, Duration> = Window::new(Duration::from_secs(1));
        for (secs, v) in [(0, Some(0)), (1, None), (2, Some(200)), (3, Some(300)), (4, Some(400))] {
            w.push(Duration::from_secs(secs), v);
        }
        let pace = w.pace_by(|v| v.map(|n| n as f64), Some(400.0)).expect("a rate");
        assert_eq!(pace.per_sec, 100.0);
    }

    /// Arrival time is not measurement time: both stamps are first-class so a replayed
    /// stream measures on its own clock
    #[test]
    fn a_window_measures_on_whichever_clock_it_was_given() {
        let origin = Instant::now();
        let mut w: Window<u64> = Window::new(Duration::from_secs(1));
        for secs in 0..5u64 {
            w.push(origin + Duration::from_secs(secs), secs * 100);
        }
        assert_eq!(w.per_sec(), Some(100.0));
    }
}
