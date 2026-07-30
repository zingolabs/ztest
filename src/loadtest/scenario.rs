//! L1 — what each virtual connection does. Kept deterministic: the range a
//! connection targets is a pure function of its index, so a run is reproducible
//! without an RNG (a load test that can't be re-run doesn't help a developer
//! bisect a regression).

use std::ops::Range;

use crate::loadtest::report::OpKind;

/// How connection windows are spread across the block pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distribution {
    /// Windows spread evenly: connection 0 starts at `pool.start`, the last
    /// connection ends at `pool.end`. Ported from `zaino-admin`'s `concurrent`.
    Even,
    /// Windows scattered by a deterministic hash of the connection index —
    /// reproducible pseudo-randomness, no RNG state.
    Scatter,
}

/// What a connection fetches. Extensible; today the block-range sweep covers the
/// three `zaino-admin` load modes.
#[derive(Debug, Clone)]
pub enum Scenario {
    /// Each connection repeatedly fetches a `blocks`-sized window somewhere in
    /// `pool`, positioned per [`Distribution`].
    BlockRangeSweep {
        pool: Range<u64>,
        blocks: u64,
        dist: Distribution,
    },
}

impl Scenario {
    pub(crate) fn op_kind(&self) -> OpKind {
        match self {
            Scenario::BlockRangeSweep { .. } => OpKind::GetBlockRange,
        }
    }

    /// The inclusive `(start, end)` range connection `index` of `count` targets.
    /// Windows may overlap when `blocks * count` exceeds the pool — fine for
    /// load; the point is concurrent readers, not disjoint coverage.
    pub(crate) fn range_for(&self, index: usize, count: usize) -> (u64, u64) {
        match self {
            Scenario::BlockRangeSweep { pool, blocks, dist } => {
                let pool_size = pool.end.saturating_sub(pool.start);
                let span = pool_size.saturating_sub(*blocks);
                let offset = match dist {
                    Distribution::Even => {
                        if count <= 1 || span == 0 {
                            0
                        } else {
                            let step = span as f64 / (count - 1) as f64;
                            (step * index as f64).round() as u64
                        }
                    }
                    Distribution::Scatter => {
                        if span == 0 {
                            0
                        } else {
                            splitmix64(index as u64) % (span + 1)
                        }
                    }
                };
                let start = pool.start + offset;
                let end = (start + blocks.saturating_sub(1)).min(pool.end.saturating_sub(1));
                (start, end)
            }
        }
    }
}

/// A deterministic bit-mixer — reproducible scatter without RNG state.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sweep(dist: Distribution) -> Scenario {
        Scenario::BlockRangeSweep {
            pool: 0..1000,
            blocks: 100,
            dist,
        }
    }

    #[test]
    fn even_spreads_first_and_last_to_the_edges() {
        let s = sweep(Distribution::Even);
        assert_eq!(s.range_for(0, 10), (0, 99));
        let (start, end) = s.range_for(9, 10);
        assert_eq!(end, 999);
        assert_eq!(start, 900);
    }

    #[test]
    fn windows_stay_inside_the_pool() {
        for dist in [Distribution::Even, Distribution::Scatter] {
            let s = sweep(dist);
            for i in 0..50 {
                let (start, end) = s.range_for(i, 50);
                assert!(end <= 999, "end {end} escaped pool for conn {i}");
                assert!(start <= end);
            }
        }
    }

    #[test]
    fn scatter_is_reproducible() {
        let s = sweep(Distribution::Scatter);
        assert_eq!(s.range_for(7, 100), s.range_for(7, 100));
    }
}
