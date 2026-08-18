//! Zcash-defined work over a chain range, per op class. Denominator of every
//! sync-harness throughput number.
//!
//! - Property of the **chain**, not its processor (validator/indexer/wallet timed on one vector)
//! - Tier A = sapling outputs + orchard/ironwood actions, from `chainMetadata`
//!   tree sizes (2 RPCs, no scan — [`super::chainwork`])
//! - Tier B = sapling spends, transparent in/out, sprout JoinSplits: needs a
//!   scan, unimplemented ([`super::chainwork::ChainWork::support`] reports at runtime)
//! - Unmeasured != zero ([`Work`] keeps them apart, uncounted renders `—`)

use std::time::Duration;

/// One class of protocol work. Finer than a value pool (Sapling spend proof !=
/// Sapling output proof); display collapses to [`CHANNELS`], weighting stays here
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    TransparentIn,
    TransparentOut,
    SproutJoinSplit,
    SaplingSpend,
    SaplingOutput,
    OrchardAction,
    IronwoodAction,
}

/// Display channels: name + its ops, in stacking order (oldest pool first).
///
/// Sole source of every per-pool list (panel rows, plot stack, timeline
/// channels) — a stack order disagreeing with its legend mislabels silently
pub const CHANNELS: [(&str, &[Op]); 5] = [
    ("transparent", &[Op::TransparentIn, Op::TransparentOut]),
    ("sprout", &[Op::SproutJoinSplit]),
    ("sapling", &[Op::SaplingSpend, Op::SaplingOutput]),
    ("orchard", &[Op::OrchardAction]),
    ("ironwood", &[Op::IronwoodAction]),
];

impl Op {
    /// Display order = graph stacking order (oldest pool first)
    pub const ALL: [Op; 7] = [
        Op::TransparentIn,
        Op::TransparentOut,
        Op::SproutJoinSplit,
        Op::SaplingSpend,
        Op::SaplingOutput,
        Op::OrchardAction,
        Op::IronwoodAction,
    ];

    /// Width of a [`Work`] vector
    pub const COUNT: usize = Op::ALL.len();

    /// Position in a [`Work`] vector
    pub const fn index(self) -> usize {
        match self {
            Op::TransparentIn => 0,
            Op::TransparentOut => 1,
            Op::SproutJoinSplit => 2,
            Op::SaplingSpend => 3,
            Op::SaplingOutput => 4,
            Op::OrchardAction => 5,
            Op::IronwoodAction => 6,
        }
    }

    /// Stable wire id (crosses driver→controller stream & keys the durable
    /// report — a rename breaks archived runs)
    pub const fn label(self) -> &'static str {
        match self {
            Op::TransparentIn => "transparent-in",
            Op::TransparentOut => "transparent-out",
            Op::SproutJoinSplit => "sprout-joinsplit",
            Op::SaplingSpend => "sapling-spend",
            Op::SaplingOutput => "sapling-output",
            Op::OrchardAction => "orchard-action",
            Op::IronwoodAction => "ironwood-action",
        }
    }
}

/// Ops a [`Work`] vector measured. Bitset, not per-count `Option` (keeps `Work`
/// plain `Copy`; subtraction's intersection rule = one `&`)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OpSet(u8);

impl OpSet {
    pub const NONE: OpSet = OpSet(0);

    pub const fn of(ops: &[Op]) -> OpSet {
        let mut bits = 0u8;
        let mut i = 0;
        while i < ops.len() {
            bits |= 1 << ops[i].index();
            i += 1;
        }
        OpSet(bits)
    }

    pub const fn has(self, op: Op) -> bool {
        self.0 & (1 << op.index()) != 0
    }

    pub const fn with(self, op: Op) -> OpSet {
        OpSet(self.0 | (1 << op.index()))
    }

    /// Only ops a comparison of the two vectors can speak about
    pub const fn intersect(self, other: OpSet) -> OpSet {
        OpSet(self.0 & other.0)
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// Protocol work over a span of chain, per [`Op`], + which ops were counted.
/// Serialized as a map keyed by [`Op::label`], never positionally
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Work {
    counts: [u64; Op::COUNT],
    known: OpSet,
}

impl Work {
    /// Empty vector, nothing measured
    pub const ZERO: Work = Work { counts: [0; Op::COUNT], known: OpSet::NONE };

    /// Record `count` for `op`, marking it measured (recorded 0 = real
    /// observation, still distinguishable from absent)
    pub fn set(&mut self, op: Op, count: u64) -> &mut Self {
        self.counts[op.index()] = count;
        self.known = self.known.with(op);
        self
    }

    /// Count for `op`, `None` when unmeasured
    pub fn get(&self, op: Op) -> Option<u64> {
        self.known.has(op).then(|| self.counts[op.index()])
    }

    pub fn known(&self) -> OpSet {
        self.known
    }

    /// Count for `op`, panicking when unmeasured.
    ///
    /// - For probe predicates: unmeasured read = bug in the *test*, not an observation
    /// - Absent-as-zero would make such a probe unfailable (reports green)
    /// - Use [`get`](Self::get) where absence is a legitimate answer
    pub fn require(&self, op: Op) -> u64 {
        self.get(op).unwrap_or_else(|| missing(op))
    }

    /// Sum across every measured op.
    ///
    /// - Corpus-dependent (equal-length transparent- vs shielded-heavy ranges differ wildly)
    /// - Comparable **only** across an identical height segment (`perf --base` refuses otherwise)
    /// - Never present without its composition alongside
    pub fn total(&self) -> Option<u64> {
        (!self.known.is_empty()).then(|| Op::ALL.iter().filter_map(|&op| self.get(op)).sum::<u64>())
    }

    /// Work done between two cumulative readings.
    ///
    /// - Saturating (a reorg rolls the trees back; rollback belongs to
    ///   [`Snapshot::reorg_depth`](crate::sync::Snapshot::reorg_depth))
    /// - Speaks only of ops **both** readings measured
    pub fn delta(&self, earlier: &Work) -> Work {
        let known = self.known.intersect(earlier.known);
        let mut out = Work { counts: [0; Op::COUNT], known };
        for op in Op::ALL {
            if known.has(op) {
                out.counts[op.index()] =
                    self.counts[op.index()].saturating_sub(earlier.counts[op.index()]);
            }
        }
        out
    }

    /// Per-second rates over `elapsed`. Non-positive interval → no rates, not infinity
    pub fn rate(&self, elapsed: Duration) -> Rate {
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            return Rate::default();
        }
        let mut out = Rate { per_sec: [0.0; Op::COUNT], known: self.known };
        for op in Op::ALL {
            if self.known.has(op) {
                out.per_sec[op.index()] = self.counts[op.index()] as f64 / secs;
            }
        }
        out
    }

    /// Per-[`CHANNELS`] totals, stacking order. `None` = unmeasured (renders
    /// `—`), `Some(0)` = counted zero
    pub fn channels(&self) -> [(&'static str, Option<u64>); 5] {
        CHANNELS.map(|(name, ops)| (name, sum(ops.iter().map(|&op| self.get(op)))))
    }

    /// Each channel's % share of the total (over a fixed span totals are constant
    /// across runs, so composition = the only signal)
    pub fn composition(&self) -> [(&'static str, Option<f64>); 5] {
        let total = self.total().unwrap_or(0) as f64;
        self.channels().map(|(name, n)| {
            let share = n.filter(|_| total > 0.0).map(|n| n as f64 / total * 100.0);
            (name, share)
        })
    }
}

fn missing<T>(op: Op) -> T {
    panic!(
        "sync probe read `{}`, which this run never measured.\n\
         Nothing counted it, which is not the same as it being zero — treating \
         it as zero would make this probe unfailable.\n\
         Either the op is tier B (transparent, sprout, sapling-spend: not \
         collected yet), or the indexer does not populate `chainMetadata` for \
         its pool. Use `Work::get` if absence is a legitimate answer here.",
        op.label()
    )
}

/// Total of measured entries, `None` when none were (keeps an unmeasured channel
/// from summing to a misleading zero)
fn sum<T>(vals: impl Iterator<Item = Option<T>>) -> Option<T>
where
    T: std::ops::Add<Output = T> + Default,
{
    vals.flatten().fold(None, |acc: Option<T>, n| Some(acc.unwrap_or_default() + n))
}

/// Span of chain a run traversed + how long. Span = identity, so elapsed time is
/// all a two-run comparison can be about.
///
/// - `to` = height **reached**, never the one asked for
/// - `network` as the indexer names it; two `None`s != same chain
/// - `elapsed_ms` spans first→last reading (excludes provisioning)
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Segment {
    pub network: Option<String>,
    pub from: u32,
    pub to: u32,
    pub work: Work,
    pub elapsed_ms: u64,
}

/// Why two segments cannot be compared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mismatch {
    Network { base: Option<String>, head: Option<String> },
    Range { base: String, head: String },
}

impl std::fmt::Display for Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mismatch::Network { base: Some(base), head: Some(head) } => {
                write!(f, "different networks ({base} vs {head})")
            }
            Mismatch::Network { .. } => f.write_str(
                "one of these runs had no indexer to name its chain, so the two \
                 cannot be assumed to be on the same one",
            ),
            Mismatch::Range { base, head } => write!(
                f,
                "they covered different spans ({base} vs {head}) — give both runs \
                 the same `run.until_height(..)` so they traverse the same chain",
            ),
        }
    }
}

impl Segment {
    pub fn comparable_with(&self, other: &Segment) -> Result<(), Mismatch> {
        let (Some(head), Some(base)) = (&self.network, &other.network) else {
            return Err(Mismatch::Network {
                base: other.network.clone(),
                head: self.network.clone(),
            });
        };
        if head != base {
            return Err(Mismatch::Network { base: Some(base.clone()), head: Some(head.clone()) });
        }
        if (self.from, self.to) != (other.from, other.to) {
            return Err(Mismatch::Range { base: other.describe(), head: self.describe() });
        }
        Ok(())
    }

    pub fn elapsed(&self) -> Duration {
        Duration::from_millis(self.elapsed_ms)
    }

    pub fn rate(&self) -> Rate {
        self.work.rate(self.elapsed())
    }

    /// Human form: `regtest 840,000..855,000`
    pub fn describe(&self) -> String {
        use crate::fmt::thousands;
        format!(
            "{} {}..{}",
            self.network.as_deref().unwrap_or("unknown-network"),
            thousands(u64::from(self.from)),
            thousands(u64::from(self.to)),
        )
    }
}

/// Per-second [`Work`], same measured-op set (unmeasured stays != idle).
/// Serialized like [`Work`]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Rate {
    per_sec: [f64; Op::COUNT],
    known: OpSet,
}

/// Label-keyed serde for [`Work`] and [`Rate`].
///
/// - Positional would bake [`Op::ALL`]'s order into the durable format (tier B inserts mid-array)
/// - Additive both ways: unknown key skipped, missing key = unmeasured
mod op_keyed {
    use super::{Op, OpSet};
    use serde::de::{Deserialize, Deserializer};
    use serde::ser::{Serialize, SerializeMap, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S, T>(values: &[T; Op::COUNT], known: OpSet, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        let mut map = s.serialize_map(None)?;
        for op in Op::ALL {
            if known.has(op) {
                map.serialize_entry(op.label(), &values[op.index()])?;
            }
        }
        map.end()
    }

    pub fn deserialize<'de, D, T>(d: D) -> Result<([T; Op::COUNT], OpSet), D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de> + Copy + Default,
    {
        let raw = BTreeMap::<String, T>::deserialize(d)?;
        let mut values = [T::default(); Op::COUNT];
        let mut known = OpSet::NONE;
        for op in Op::ALL {
            if let Some(&v) = raw.get(op.label()) {
                values[op.index()] = v;
                known = known.with(op);
            }
        }
        Ok((values, known))
    }
}

impl serde::Serialize for Work {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        op_keyed::serialize(&self.counts, self.known, s)
    }
}

impl<'de> serde::Deserialize<'de> for Work {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (counts, known) = op_keyed::deserialize(d)?;
        Ok(Work { counts, known })
    }
}

impl serde::Serialize for Rate {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        op_keyed::serialize(&self.per_sec, self.known, s)
    }
}

impl<'de> serde::Deserialize<'de> for Rate {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (per_sec, known) = op_keyed::deserialize(d)?;
        Ok(Rate { per_sec, known })
    }
}

impl Rate {
    /// Rate for `op`, `None` when unmeasured
    pub fn get(&self, op: Op) -> Option<f64> {
        self.known.has(op).then(|| self.per_sec[op.index()])
    }

    pub fn known(&self) -> OpSet {
        self.known
    }

    /// Rate for `op`, panicking when unmeasured. See [`Work::require`]
    pub fn require(&self, op: Op) -> f64 {
        self.get(op).unwrap_or_else(|| missing(op))
    }

    /// Sum across measured ops. Carries [`Work::total`]'s corpus caveat
    pub fn total(&self) -> Option<f64> {
        (!self.known.is_empty()).then(|| Op::ALL.iter().filter_map(|&op| self.get(op)).sum::<f64>())
    }

    pub fn channels(&self) -> [(&'static str, Option<f64>); 5] {
        CHANNELS.map(|(name, ops)| (name, sum(ops.iter().map(|&op| self.get(op)))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tier_a(sapling: u64, orchard: u64, ironwood: u64) -> Work {
        let mut w = Work::ZERO;
        w.set(Op::SaplingOutput, sapling)
            .set(Op::OrchardAction, orchard)
            .set(Op::IronwoodAction, ironwood);
        w
    }

    /// Uncounted op answering `0` reads as "no transparent activity" when nothing looked
    #[test]
    fn an_unmeasured_op_is_not_a_zero_one() {
        let w = tier_a(10, 20, 0);
        assert_eq!(w.get(Op::TransparentIn), None);
        assert_eq!(w.channels()[0].1, None, "transparent");
        assert_eq!(w.get(Op::IronwoodAction), Some(0));
        assert_eq!(w.channels()[4].1, Some(0), "ironwood");
    }

    #[test]
    fn a_recorded_zero_is_measured() {
        let mut w = Work::ZERO;
        w.set(Op::SaplingOutput, 0);
        assert!(w.known().has(Op::SaplingOutput));
        assert_eq!(w.get(Op::SaplingOutput), Some(0));
        assert_eq!(w.total(), Some(0));
    }

    #[test]
    fn nothing_measured_has_no_total() {
        assert_eq!(Work::ZERO.total(), None);
        assert_eq!(Work::ZERO.rate(Duration::from_secs(1)).total(), None);
    }

    /// Sapling = 2 ops; measuring one gives a channel floor, not nothing
    #[test]
    fn a_partially_measured_channel_sums_what_it_has() {
        let mut w = Work::ZERO;
        w.set(Op::SaplingOutput, 7);
        assert_eq!(w.channels()[2].1, Some(7));
        w.set(Op::SaplingSpend, 3);
        assert_eq!(w.channels()[2].1, Some(10));
    }

    /// Probe over an uncounted op = probe bug; answering 0 would make it unfailable
    #[test]
    #[should_panic(expected = "transparent-in")]
    fn requiring_an_unmeasured_op_panics() {
        tier_a(10, 20, 0).require(Op::TransparentIn);
    }

    #[test]
    fn requiring_a_measured_op_yields_its_count() {
        assert_eq!(tier_a(10, 20, 0).require(Op::SaplingOutput), 10);
        assert_eq!(tier_a(10, 20, 0).require(Op::IronwoodAction), 0);
    }

    #[test]
    fn a_delta_is_the_work_between_two_cumulative_readings() {
        let d = tier_a(150, 90, 5).delta(&tier_a(100, 40, 5));
        assert_eq!(d.get(Op::SaplingOutput), Some(50));
        assert_eq!(d.get(Op::OrchardAction), Some(50));
        assert_eq!(d.get(Op::IronwoodAction), Some(0));
        assert_eq!(d.total(), Some(100));
    }

    /// Reorg rolls the trees back; rollback belongs to `reorg_depth`, not a negative rate
    #[test]
    fn a_reorg_does_not_produce_negative_work() {
        let d = tier_a(100, 40, 0).delta(&tier_a(150, 90, 0));
        assert_eq!(d.get(Op::SaplingOutput), Some(0));
        assert_eq!(d.total(), Some(0));
    }

    /// Delta against a vector that measured less must not invent the missing op
    #[test]
    fn a_delta_speaks_only_of_ops_both_sides_measured() {
        let mut richer = tier_a(150, 90, 0);
        richer.set(Op::TransparentIn, 400);
        let d = richer.delta(&tier_a(100, 40, 0));
        assert_eq!(d.get(Op::SaplingOutput), Some(50));
        assert_eq!(d.get(Op::TransparentIn), None);
    }

    #[test]
    fn rates_divide_by_the_interval() {
        let r = tier_a(120, 60, 0).rate(Duration::from_secs(10));
        assert_eq!(r.get(Op::SaplingOutput), Some(12.0));
        assert_eq!(r.require(Op::OrchardAction), 6.0);
        assert_eq!(r.total(), Some(18.0));
        assert_eq!(r.get(Op::TransparentIn), None);
    }

    /// Zero interval != infinite rate (an instant poll shares a tick instant)
    #[test]
    fn a_zero_interval_yields_no_rates_rather_than_infinity() {
        let r = tier_a(120, 60, 0).rate(Duration::ZERO);
        assert_eq!(r.total(), None);
        assert!(r.known().is_empty());
    }

    /// Positional would bake `Op::ALL`'s order into the durable format; tier B
    /// inserts mid-array, silently renumbering every report already written
    #[test]
    fn work_travels_keyed_by_label_not_by_position() {
        let wire = serde_json::to_string(&tier_a(10, 20, 0)).expect("serialize");
        assert_eq!(wire, r#"{"sapling-output":10,"orchard-action":20,"ironwood-action":0}"#);
    }

    /// Absent key = unmeasured, not zero; must survive the round trip or the
    /// panel prints `0` for an uncounted pool
    #[test]
    fn an_absent_key_decodes_as_unmeasured_rather_than_zero() {
        let w: Work = serde_json::from_str(r#"{"sapling-output":10}"#).expect("parse");
        assert_eq!(w.get(Op::SaplingOutput), Some(10));
        assert_eq!(w.get(Op::OrchardAction), None);
        assert_eq!(w.total(), Some(10));
    }

    /// `sync`-tier syncs outlive builds → controller reads newer drivers; an
    /// unheard-of op must be skipped, not refused
    #[test]
    fn an_unknown_key_from_a_newer_driver_is_skipped() {
        let w: Work =
            serde_json::from_str(r#"{"sapling-output":10,"quantum-widget":99}"#).expect("parse");
        assert_eq!(w.get(Op::SaplingOutput), Some(10));
        assert_eq!(w.total(), Some(10));
    }

    #[test]
    fn a_rate_round_trips_through_json() {
        let r = tier_a(120, 60, 0).rate(Duration::from_secs(10));
        let back: Rate =
            serde_json::from_str(&serde_json::to_string(&r).expect("ser")).expect("de");
        assert_eq!(back, r);
        assert_eq!(back.get(Op::SaplingOutput), Some(12.0));
        assert_eq!(back.get(Op::TransparentIn), None);
    }

    fn segment(network: Option<&str>, from: u32, to: u32, secs: u64) -> Segment {
        Segment {
            network: network.map(str::to_string),
            from,
            to,
            work: tier_a(1000, 500, 0),
            elapsed_ms: secs * 1000,
        }
    }

    #[test]
    fn identical_segments_are_comparable() {
        let a = segment(Some("regtest"), 840_000, 855_000, 100);
        assert_eq!(a.comparable_with(&a.clone()), Ok(()));
    }

    /// Keeps `perf --base` honest: across different heights a throughput delta
    /// speaks about block choice, not code
    #[test]
    fn segments_over_different_spans_are_refused() {
        let head = segment(Some("regtest"), 840_000, 855_000, 100);
        for other in [
            segment(Some("regtest"), 840_000, 860_000, 100),
            segment(Some("regtest"), 500_000, 855_000, 100),
        ] {
            let Err(Mismatch::Range { base, head: h }) = head.comparable_with(&other) else {
                panic!("a span mismatch must be refused and name both sides");
            };
            assert_eq!(base, other.describe());
            assert_eq!(h, head.describe());
        }
    }

    /// Comparability = observation, not declaration (tip-chaser & declared-stop
    /// reaching the same heights covered the same ground)
    #[test]
    fn covering_the_same_ground_is_what_makes_runs_comparable() {
        let declared = segment(Some("regtest"), 840_000, 855_000, 100);
        let to_tip = segment(Some("regtest"), 840_000, 855_000, 90);
        assert_eq!(declared.comparable_with(&to_tip), Ok(()));
    }

    /// Short run reports its reached height → refused, never compared against a
    /// complete traversal as though both finished
    #[test]
    fn a_run_that_stopped_short_is_refused_on_its_reached_height() {
        let complete = segment(Some("regtest"), 840_000, 855_000, 100);
        let timed_out = segment(Some("regtest"), 840_000, 850_112, 100);
        assert!(matches!(complete.comparable_with(&timed_out), Err(Mismatch::Range { .. })));
    }

    #[test]
    fn segments_on_different_networks_are_refused() {
        let head = segment(Some("main"), 840_000, 855_000, 100);
        let Err(Mismatch::Network { base, head: h }) =
            head.comparable_with(&segment(Some("test"), 840_000, 855_000, 100))
        else {
            panic!("a network mismatch must name both sides");
        };
        assert_eq!((base.as_deref(), h.as_deref()), (Some("test"), Some("main")));
    }

    /// Unnamed chain != same chain (unknowns matching would compare mainnet
    /// against regtest silently)
    #[test]
    fn an_unrecorded_network_never_matches_another_unrecorded_one() {
        let a = segment(None, 840_000, 855_000, 100);
        assert!(matches!(a.comparable_with(&a.clone()), Err(Mismatch::Network { .. })));
        assert!(Mismatch::Network { base: None, head: None }.to_string().contains("no indexer"));
    }

    #[test]
    fn a_segment_describes_its_span_readably() {
        assert_eq!(
            segment(Some("regtest"), 840_000, 855_000, 100).describe(),
            "regtest 840,000..855,000"
        );
        assert_eq!(segment(None, 0, 10, 1).describe(), "unknown-network 0..10");
    }

    /// Same work / less time = the only difference two comparable runs express
    #[test]
    fn a_segments_rate_is_its_work_over_its_elapsed_time() {
        let s = segment(Some("regtest"), 840_000, 855_000, 100);
        assert_eq!(s.rate().get(Op::SaplingOutput), Some(10.0));
        assert_eq!(s.rate().total(), Some(15.0));
        let faster = segment(Some("regtest"), 840_000, 855_000, 50);
        assert_eq!(faster.rate().total(), Some(30.0));
    }

    /// Rides the durable report = the API other tools read
    #[test]
    fn a_segment_round_trips_through_json() {
        let s = segment(Some("regtest"), 840_000, 855_000, 100);
        let wire = serde_json::to_string(&s).expect("serialize");
        let back: Segment = serde_json::from_str(&wire).expect("parse");
        assert_eq!(back, s);
        assert_eq!(back.work.get(Op::SaplingOutput), Some(1000));
        assert_eq!(back.work.get(Op::TransparentIn), None);
        assert_eq!(back.elapsed(), Duration::from_secs(100));
    }

    #[test]
    fn op_indices_are_distinct_and_in_range() {
        let mut seen = [false; Op::COUNT];
        for op in Op::ALL {
            assert!(!seen[op.index()], "{} reuses an index", op.label());
            seen[op.index()] = true;
        }
    }

    /// Channel order = plot stacking order, must stay oldest-pool-first
    #[test]
    fn channels_render_unmeasured_pools_as_absent() {
        let channels = tier_a(10, 20, 0).channels();
        assert_eq!(
            channels.map(|(name, _)| name),
            ["transparent", "sprout", "sapling", "orchard", "ironwood"]
        );
        assert_eq!(channels[0].1, None);
        assert_eq!(channels[1].1, None);
        assert_eq!(channels[2].1, Some(10));
        assert_eq!(channels[4].1, Some(0));
    }

    /// Op in 0 or 2 channels → a complete-looking list drops or double-counts work
    #[test]
    fn every_op_appears_in_exactly_one_channel() {
        let mut seen = Vec::new();
        for (_, ops) in CHANNELS {
            seen.extend_from_slice(ops);
        }
        for op in Op::ALL {
            assert_eq!(
                seen.iter().filter(|&&o| o == op).count(),
                1,
                "{} is not in exactly one channel",
                op.label()
            );
        }
    }

    /// Unmeasured channel has no share, not a zero one
    #[test]
    fn composition_is_each_channels_share_of_the_total() {
        let shares = tier_a(75, 25, 0).composition();
        assert_eq!(shares[2].1, Some(75.0));
        assert_eq!(shares[3].1, Some(25.0));
        assert_eq!(shares[4].1, Some(0.0));
        assert_eq!(shares[0].1, None, "unmeasured has no share");
    }

    /// Zero total = no shares, never a divide by it
    #[test]
    fn composition_of_no_work_reports_no_shares() {
        assert!(tier_a(0, 0, 0).composition().iter().all(|(_, share)| share.is_none()));
    }
}
