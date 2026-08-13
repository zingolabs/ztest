//! A bounded, self-coarsening timeline of banded samples — the shape a long
//! sync's graphs are drawn from.
//!
//! Three properties drive the design:
//!
//! **One time axis, all channels.** The reason to plot work beside CPU beside
//! I/O is to read causation off their alignment: throughput fell *while* CPU
//! stayed pinned and I/O collapsed. If each channel bucketed independently
//! their x-coordinates would drift apart and that alignment would be a lie. So
//! a [`Timeline`] owns the bucketing and every channel shares it.
//!
//! **Bounded, whatever the run length.** A 48-hour sync at a 5-second tick
//! produces ~35k samples per channel. The timeline holds [`CAPACITY`] buckets
//! and doubles its bucket width whenever it would overflow, so it always spans
//! the whole run in constant memory and constant wire bytes — which is what
//! lets the driver republish it whole and lets `ztest sync status` plot a run
//! from a bounded log tail rather than the entire log.
//!
//! **Bands, not means.** Each bucket keeps min and max rather than an average,
//! because a sync raises two different questions — "did it get slower?" and
//! "did it stall?" — and a mean answers only the first. Once buckets coarsen to
//! half an hour, a 90-second stall inside one is invisible to a mean and
//! obvious as a band reaching the floor.
//!
//! A bucket that received no samples stays **empty** and renders as a gap. It
//! is never interpolated across: the absence of data during a partition is the
//! observation, and drawing a line through it would erase the event.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Buckets retained. Sized for a terminal graph: wider than any pane it is
/// drawn into (braille packs two samples per column), so the render decimates
/// for display rather than the timeline having thrown away detail it needed.
pub const CAPACITY: usize = 120;

/// One bucket of one channel: the range of values observed within it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Cell {
    /// Samples folded in. `0` means the bucket is empty and `min`/`max` carry
    /// no meaning — the distinction a gap in the graph is drawn from.
    #[serde(rename = "n")]
    pub count: u32,
    #[serde(rename = "lo")]
    pub min: f64,
    #[serde(rename = "hi")]
    pub max: f64,
}

impl Cell {
    /// Whether this bucket observed anything.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The observed band, or `None` for an empty bucket.
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

    /// Union of two adjacent buckets, as coarsening produces.
    fn merge(a: Cell, b: Cell) -> Cell {
        match (a.is_empty(), b.is_empty()) {
            (true, _) => b,
            (_, true) => a,
            _ => Cell {
                count: a.count + b.count,
                min: a.min.min(b.min),
                max: a.max.max(b.max),
            },
        }
    }
}

/// One named series within a [`Timeline`], bucketed on the timeline's axis.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Channel {
    pub name: String,
    pub cells: Vec<Cell>,
}

/// Blocks are not protocol work, but they are the axis a reader recognises, and
/// plotting them beside work is what distinguishes a range that is cheap per
/// block from one that is merely being scanned slowly.
pub const BLOCKS: &str = "blocks";

/// The channels a sync plots, in stacking order: blocks, then the protocol work
/// channels exactly as [`CHANNELS`](super::work::CHANNELS) defines them.
///
/// Stated once because two timelines are built from it — the driver's, published
/// as `Series` for `ztest sync status`, and the watcher's own second-by-second
/// one — and a graph whose stack order depended on which built it would be
/// unreadable across the two.
pub fn plot_channels() -> impl Iterator<Item = &'static str> {
    std::iter::once(BLOCKS).chain(super::work::CHANNELS.iter().map(|(name, _)| *name))
}

/// A fixed-capacity multi-channel timeline over a shared, self-coarsening time
/// axis.
///
/// Channels are fixed at construction: a timeline whose channel set could shift
/// mid-run would produce a graph whose stack order changed under the reader.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Timeline {
    /// Current bucket width. Doubles on each coarsening.
    #[serde(rename = "w_ms")]
    width_ms: u64,
    channels: Vec<Channel>,
}

impl Timeline {
    /// A timeline over `names`, starting at `width` per bucket.
    ///
    /// The initial width only sets how long the timeline runs before its first
    /// coarsening; it is not a floor on resolution, since samples arriving
    /// faster than one per bucket simply widen that bucket's band.
    pub fn new<I, S>(names: I, width: Duration) -> Timeline
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Timeline {
            // A zero width would make every sample land in bucket 0 and divide
            // by zero on lookup; the caller passing one means "as fine as you
            // can", which is one millisecond.
            width_ms: (width.as_millis() as u64).max(1),
            channels: names
                .into_iter()
                .map(|name| Channel {
                    name: name.into(),
                    cells: Vec::new(),
                })
                .collect(),
        }
    }

    /// Current bucket width.
    pub fn width(&self) -> Duration {
        Duration::from_millis(self.width_ms)
    }

    /// Buckets currently held.
    pub fn len(&self) -> usize {
        self.channels.first().map_or(0, |c| c.cells.len())
    }

    /// Whether nothing has been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The channels, in construction order — which is stack order for a
    /// stacked-area render.
    pub fn channels(&self) -> &[Channel] {
        &self.channels
    }

    /// A channel by name.
    pub fn channel(&self, name: &str) -> Option<&Channel> {
        self.channels.iter().find(|c| c.name == name)
    }

    /// Elapsed time at the start of bucket `i`.
    pub fn bucket_start(&self, i: usize) -> Duration {
        Duration::from_millis(self.width_ms * i as u64)
    }

    /// Elapsed time covered so far.
    pub fn span(&self) -> Duration {
        self.bucket_start(self.len())
    }

    /// Record one sample per channel, taken at `at` elapsed into the run.
    ///
    /// `values` is positional against [`channels`](Self::channels); a channel
    /// with nothing to report this tick passes `None`, which leaves its bucket
    /// untouched rather than recording a zero. A metrics plane that is down
    /// must leave a gap, not a floor.
    pub fn push(&mut self, at: Duration, values: &[Option<f64>]) {
        let mut idx = (at.as_millis() as u64 / self.width_ms) as usize;
        while idx >= CAPACITY {
            self.coarsen();
            idx = (at.as_millis() as u64 / self.width_ms) as usize;
        }
        for (ch, value) in self
            .channels
            .iter_mut()
            .zip(values.iter().chain(std::iter::repeat(&None)))
        {
            if ch.cells.len() <= idx {
                ch.cells.resize(idx + 1, Cell::default());
            }
            if let Some(v) = value {
                ch.cells[idx].add(*v);
            }
        }
    }

    /// Halve the bucket count by merging adjacent pairs, doubling the width.
    ///
    /// An odd trailing bucket carries over unmerged; it is the newest data and
    /// pairing it with a bucket that does not exist yet would make the last
    /// column of every graph span half the time of its neighbours only until
    /// the next sample, i.e. flicker.
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

    /// The band series for `name`, one entry per bucket, `None` where empty.
    pub fn bands(&self, name: &str) -> Vec<Option<(f64, f64)>> {
        self.channel(name)
            .map(|c| c.cells.iter().map(Cell::band).collect())
            .unwrap_or_default()
    }

    /// Largest value observed across `names` — the y-axis ceiling for a graph
    /// drawn over them. `None` when they hold no data, which is the signal to
    /// draw an empty frame rather than an axis labelled zero.
    pub fn peak(&self, names: &[&str]) -> Option<f64> {
        names
            .iter()
            .filter_map(|n| self.channel(n))
            .flat_map(|c| c.cells.iter())
            .filter(|cell| !cell.is_empty())
            .map(|cell| cell.max)
            .fold(None, |acc: Option<f64>, v| {
                Some(acc.map_or(v, |a| a.max(v)))
            })
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

    /// The band is the whole point: a bucket holding both a peak and a stall
    /// has to report both, because a mean would erase the stall.
    #[test]
    fn a_bucket_keeps_the_range_of_what_it_saw() {
        let mut t = Timeline::new(["work"], secs(10));
        for v in [100.0, 0.0, 90.0, 95.0] {
            t.push(secs(1), &[Some(v)]);
        }
        assert_eq!(t.bands("work")[0], Some((0.0, 100.0)));
    }

    /// A tick that has nothing to say about a channel must leave a gap. A
    /// metrics plane that went down is not a channel reading zero.
    #[test]
    fn a_missing_value_leaves_a_gap_rather_than_a_zero() {
        let mut t = timeline();
        t.push(secs(0), &[Some(10.0), None]);
        assert_eq!(t.bands("work")[0], Some((10.0, 10.0)));
        assert_eq!(t.bands("cpu")[0], None);
    }

    /// Ticks that skip time leave empty buckets, and those are never
    /// interpolated across — the absence *is* the observation.
    #[test]
    fn skipped_time_leaves_empty_buckets() {
        let mut t = timeline();
        t.push(secs(0), &[Some(10.0), Some(1.0)]);
        t.push(secs(5), &[Some(20.0), Some(2.0)]);
        let bands = t.bands("work");
        assert_eq!(bands.len(), 6);
        assert!(bands[1..5].iter().all(Option::is_none));
    }

    /// The bound is what makes the whole run publishable in constant bytes.
    #[test]
    fn the_timeline_never_exceeds_its_capacity() {
        let mut t = timeline();
        for s in 0..10_000 {
            t.push(secs(s), &[Some(s as f64), Some(1.0)]);
        }
        assert!(t.len() <= CAPACITY, "held {} buckets", t.len());
        assert!(t.span() >= secs(10_000));
    }

    /// Coarsening must widen the axis, not drop the tail — the span has to keep
    /// covering the whole run.
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

    /// A merged bucket must keep the extremes of both halves, or a stall that
    /// survived one coarsening would vanish at the next.
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

    /// An empty bucket merged with a full one yields the full one, rather than
    /// a band anchored to a meaningless zero.
    #[test]
    fn merging_an_empty_bucket_does_not_drag_the_band_to_zero() {
        let merged = Cell::merge(
            Cell::default(),
            Cell {
                count: 1,
                min: 40.0,
                max: 60.0,
            },
        );
        assert_eq!(merged.band(), Some((40.0, 60.0)));
        assert_eq!(Cell::merge(Cell::default(), Cell::default()).band(), None);
    }

    /// Every channel shares one axis, so alignment across graphs is real. If
    /// they coarsened independently, reading causation off their alignment
    /// would be reading an artefact.
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

    /// No data means no axis: a graph labelled `0` would claim an observation
    /// that was never made.
    #[test]
    fn a_timeline_with_no_data_has_no_peak() {
        let mut t = timeline();
        assert_eq!(t.peak(&["work"]), None);
        t.push(secs(0), &[None, None]);
        assert_eq!(t.peak(&["work"]), None);
        assert_eq!(t.peak(&["nonexistent"]), None);
    }

    /// The timeline crosses the driver→controller log as one event, so it has
    /// to survive the round trip exactly.
    #[test]
    fn a_timeline_round_trips_through_json() {
        let mut t = timeline();
        for s in 0..300u64 {
            t.push(secs(s), &[Some(s as f64), (s % 3 == 0).then_some(1.0)]);
        }
        let wire = serde_json::to_string(&t).expect("serialize");
        assert_eq!(serde_json::from_str::<Timeline>(&wire).expect("parse"), t);
    }

    /// A zero width would put every sample in bucket 0 and divide by zero on
    /// lookup.
    #[test]
    fn a_zero_bucket_width_is_clamped_rather_than_dividing_by_zero() {
        let mut t = Timeline::new(["work"], Duration::ZERO);
        t.push(secs(1), &[Some(5.0)]);
        assert!(t.width() > Duration::ZERO);
        assert!(!t.is_empty());
    }

    /// Positional values shorter than the channel list must leave the rest as
    /// gaps rather than panicking on the index.
    #[test]
    fn fewer_values_than_channels_leaves_the_rest_empty() {
        let mut t = timeline();
        t.push(secs(0), &[Some(10.0)]);
        assert_eq!(t.bands("work")[0], Some((10.0, 10.0)));
        assert_eq!(t.bands("cpu")[0], None);
    }
}
