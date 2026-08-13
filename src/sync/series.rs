//! Bounded, self-coarsening timeline of banded samples — what a long sync's
//! graphs are drawn from.
//!
//! - One axis for all channels ([`Timeline`] owns the bucketing; independent
//!   bucketing would drift x-coords and make cross-channel alignment a lie)
//! - Bounded at [`CAPACITY`], width doubles on overflow → whole run in constant
//!   memory & wire bytes (driver republishes it whole; `status` plots off a log tail)
//! - Bands, not means: min/max answers "did it stall?", a mean only "did it slow?"
//! - Empty bucket renders as a gap, never interpolated (absence = the observation)

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Buckets retained. Wider than any pane it draws into (braille packs 2/column),
/// so the render decimates rather than the timeline having discarded detail
pub const CAPACITY: usize = 120;

/// One bucket of one channel: range of values observed. `count == 0` = empty,
/// `min`/`max` meaningless (what a gap in the graph is drawn from)
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Cell {
    #[serde(rename = "n")]
    pub count: u32,
    #[serde(rename = "lo")]
    pub min: f64,
    #[serde(rename = "hi")]
    pub max: f64,
}

impl Cell {
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Observed band, `None` for an empty bucket
    pub fn band(&self) -> Option<(f64, f64)> {
        (!self.is_empty()).then_some((self.min, self.max))
    }

    fn add(&mut self, v: f64) {
        match self.count {
            0 => (self.min, self.max) = (v, v),
            _ => (self.min, self.max) = (self.min.min(v), self.max.max(v)),
        }
        self.count += 1;
    }

    /// Union of two adjacent buckets, as coarsening produces
    fn merge(a: Cell, b: Cell) -> Cell {
        match (a.is_empty(), b.is_empty()) {
            (true, _) => b,
            (_, true) => a,
            _ => Cell { count: a.count + b.count, min: a.min.min(b.min), max: a.max.max(b.max) },
        }
    }
}

/// One named series within a [`Timeline`], on the timeline's axis
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Channel {
    pub name: String,
    pub cells: Vec<Cell>,
}

/// Not protocol work, but the axis a reader recognises — beside work it separates
/// a cheap-per-block range from a slowly-scanned one
pub const BLOCKS: &str = "blocks";

/// Plotted channels in stacking order: blocks, then
/// [`CHANNELS`](super::work::CHANNELS). Stated once — driver and watcher both
/// build from it, and a builder-dependent stack order would be unreadable
pub fn plot_channels() -> impl Iterator<Item = &'static str> {
    std::iter::once(BLOCKS).chain(super::work::CHANNELS.iter().map(|(name, _)| *name))
}

/// Fixed-capacity multi-channel timeline on a shared, self-coarsening axis.
/// Channels fixed at construction (a mid-run shift changes stack order under the reader)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Timeline {
    #[serde(rename = "w_ms")]
    width_ms: u64,
    channels: Vec<Channel>,
}

impl Timeline {
    /// Timeline over `names`, starting at `width` per bucket. `width` sets time
    /// to first coarsening, not a resolution floor (faster samples widen the band)
    pub fn new<I, S>(names: I, width: Duration) -> Timeline
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Timeline {
            // Zero width = every sample in bucket 0 + divide-by-zero on lookup;
            // read a passed zero as "as fine as you can"
            width_ms: (width.as_millis() as u64).max(1),
            channels: names
                .into_iter()
                .map(|name| Channel { name: name.into(), cells: Vec::new() })
                .collect(),
        }
    }

    pub fn width(&self) -> Duration {
        Duration::from_millis(self.width_ms)
    }

    pub fn len(&self) -> usize {
        self.channels.first().map_or(0, |c| c.cells.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Construction order = stack order for a stacked-area render
    pub fn channels(&self) -> &[Channel] {
        &self.channels
    }

    pub fn channel(&self, name: &str) -> Option<&Channel> {
        self.channels.iter().find(|c| c.name == name)
    }

    /// Elapsed time at the start of bucket `i`
    pub fn bucket_start(&self, i: usize) -> Duration {
        Duration::from_millis(self.width_ms * i as u64)
    }

    pub fn span(&self) -> Duration {
        self.bucket_start(self.len())
    }

    /// Record one sample per channel at `at` elapsed. `values` positional against
    /// [`channels`](Self::channels); `None` leaves the bucket untouched (a downed
    /// metrics plane must leave a gap, not a floor)
    pub fn push(&mut self, at: Duration, values: &[Option<f64>]) {
        let mut idx = (at.as_millis() as u64 / self.width_ms) as usize;
        while idx >= CAPACITY {
            self.coarsen();
            idx = (at.as_millis() as u64 / self.width_ms) as usize;
        }
        for (ch, value) in
            self.channels.iter_mut().zip(values.iter().chain(std::iter::repeat(&None)))
        {
            if ch.cells.len() <= idx {
                ch.cells.resize(idx + 1, Cell::default());
            }
            if let Some(v) = value {
                ch.cells[idx].add(*v);
            }
        }
    }

    /// Halve the bucket count by merging adjacent pairs, doubling the width. Odd
    /// trailing bucket carries over unmerged (pairing it with a not-yet bucket flickers)
    fn coarsen(&mut self) {
        for ch in &mut self.channels {
            let merged = ch
                .cells
                .chunks(2)
                .map(|pair| match pair {
                    [a, b] => Cell::merge(*a, *b),
                    [a] => *a,
                    _ => Cell::default(),
                })
                .collect();
            ch.cells = merged;
        }
        self.width_ms *= 2;
    }

    /// Band series for `name`, one entry per bucket, `None` where empty
    pub fn bands(&self, name: &str) -> Vec<Option<(f64, f64)>> {
        self.channel(name).map(|c| c.cells.iter().map(Cell::band).collect()).unwrap_or_default()
    }

    /// Largest value across `names` = y-axis ceiling. `None` when they hold no
    /// data → draw an empty frame, not an axis labelled zero
    pub fn peak(&self, names: &[&str]) -> Option<f64> {
        names
            .iter()
            .filter_map(|n| self.channel(n))
            .flat_map(|c| c.cells.iter())
            .filter(|cell| !cell.is_empty())
            .map(|cell| cell.max)
            .fold(None, |acc: Option<f64>, v| Some(acc.map_or(v, |a| a.max(v))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn timeline() -> Timeline {
        Timeline::new(["work", "cpu"], secs(1))
    }

    #[test]
    fn samples_land_in_the_bucket_covering_their_time() {
        let mut t = timeline();
        t.push(secs(0), &[Some(10.0), Some(1.0)]);
        t.push(secs(3), &[Some(30.0), Some(3.0)]);
        assert_eq!(t.len(), 4);
        assert_eq!(t.bands("work")[0], Some((10.0, 10.0)));
        assert_eq!(t.bands("work")[3], Some((30.0, 30.0)));
    }

    /// Bucket holding a peak and a stall reports both (a mean erases the stall)
    #[test]
    fn a_bucket_keeps_the_range_of_what_it_saw() {
        let mut t = Timeline::new(["work"], secs(10));
        for v in [100.0, 0.0, 90.0, 95.0] {
            t.push(secs(1), &[Some(v)]);
        }
        assert_eq!(t.bands("work")[0], Some((0.0, 100.0)));
    }

    /// Downed metrics plane != channel reading zero
    #[test]
    fn a_missing_value_leaves_a_gap_rather_than_a_zero() {
        let mut t = timeline();
        t.push(secs(0), &[Some(10.0), None]);
        assert_eq!(t.bands("work")[0], Some((10.0, 10.0)));
        assert_eq!(t.bands("cpu")[0], None);
    }

    /// Skipped time leaves empty buckets, never interpolated (absence = the observation)
    #[test]
    fn skipped_time_leaves_empty_buckets() {
        let mut t = timeline();
        t.push(secs(0), &[Some(10.0), Some(1.0)]);
        t.push(secs(5), &[Some(20.0), Some(2.0)]);
        let bands = t.bands("work");
        assert_eq!(bands.len(), 6);
        assert!(bands[1..5].iter().all(Option::is_none));
    }

    /// Bound = what makes the whole run publishable in constant bytes
    #[test]
    fn the_timeline_never_exceeds_its_capacity() {
        let mut t = timeline();
        for s in 0..10_000 {
            t.push(secs(s), &[Some(s as f64), Some(1.0)]);
        }
        assert!(t.len() <= CAPACITY, "held {} buckets", t.len());
        assert!(t.span() >= secs(10_000));
    }

    /// Coarsening widens the axis, never drops the tail (span keeps covering the run)
    #[test]
    fn coarsening_doubles_the_width_and_preserves_the_span() {
        let mut t = timeline();
        for s in 0..CAPACITY as u64 {
            t.push(secs(s), &[Some(1.0), Some(1.0)]);
        }
        assert_eq!(t.width(), secs(1));
        t.push(secs(CAPACITY as u64), &[Some(1.0), Some(1.0)]);
        assert_eq!(t.width(), secs(2));
        assert!(t.span() > secs(CAPACITY as u64));
    }

    /// Merge keeps both halves' extremes, else a stall vanishes at the next coarsening
    #[test]
    fn merging_keeps_the_extremes_of_both_halves() {
        let mut t = Timeline::new(["work"], secs(1));
        t.push(secs(0), &[Some(100.0)]);
        t.push(secs(1), &[Some(0.0)]);
        for s in 2..=CAPACITY as u64 {
            t.push(secs(s), &[Some(50.0)]);
        }
        assert_eq!(t.bands("work")[0], Some((0.0, 100.0)));
    }

    /// Empty + full = the full one, not a band anchored to a meaningless zero
    #[test]
    fn merging_an_empty_bucket_does_not_drag_the_band_to_zero() {
        let merged = Cell::merge(Cell::default(), Cell { count: 1, min: 40.0, max: 60.0 });
        assert_eq!(merged.band(), Some((40.0, 60.0)));
        assert_eq!(Cell::merge(Cell::default(), Cell::default()).band(), None);
    }

    /// Independent coarsening would make cross-graph alignment an artefact
    #[test]
    fn every_channel_stays_on_the_same_axis() {
        let mut t = timeline();
        for s in 0..500u64 {
            t.push(secs(s), &[Some(1.0), None]);
        }
        let widths: Vec<usize> = t.channels().iter().map(|c| c.cells.len()).collect();
        assert!(widths.windows(2).all(|w| w[0] == w[1]), "{widths:?}");
    }

    #[test]
    fn peak_spans_the_named_channels_only() {
        let mut t = timeline();
        t.push(secs(0), &[Some(10.0), Some(900.0)]);
        t.push(secs(1), &[Some(42.0), Some(900.0)]);
        assert_eq!(t.peak(&["work"]), Some(42.0));
        assert_eq!(t.peak(&["work", "cpu"]), Some(900.0));
    }

    /// No data = no axis (a graph labelled `0` claims an observation never made)
    #[test]
    fn a_timeline_with_no_data_has_no_peak() {
        let mut t = timeline();
        assert_eq!(t.peak(&["work"]), None);
        t.push(secs(0), &[None, None]);
        assert_eq!(t.peak(&["work"]), None);
        assert_eq!(t.peak(&["nonexistent"]), None);
    }

    /// Crosses the driver→controller log as one event, so must round-trip exactly
    #[test]
    fn a_timeline_round_trips_through_json() {
        let mut t = timeline();
        for s in 0..300u64 {
            t.push(secs(s), &[Some(s as f64), (s % 3 == 0).then_some(1.0)]);
        }
        let wire = serde_json::to_string(&t).expect("serialize");
        assert_eq!(serde_json::from_str::<Timeline>(&wire).expect("parse"), t);
    }

    /// Zero width = every sample in bucket 0 + divide-by-zero on lookup
    #[test]
    fn a_zero_bucket_width_is_clamped_rather_than_dividing_by_zero() {
        let mut t = Timeline::new(["work"], Duration::ZERO);
        t.push(secs(1), &[Some(5.0)]);
        assert!(t.width() > Duration::ZERO);
        assert!(!t.is_empty());
    }

    /// Short positional slice leaves the rest as gaps, never panics on the index
    #[test]
    fn fewer_values_than_channels_leaves_the_rest_empty() {
        let mut t = timeline();
        t.push(secs(0), &[Some(10.0)]);
        assert_eq!(t.bands("work")[0], Some((10.0, 10.0)));
        assert_eq!(t.bands("cpu")[0], None);
    }
}
