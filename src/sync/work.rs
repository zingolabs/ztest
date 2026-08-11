//! The protocol-shaped work vector: how much Zcash-defined work a range of
//! chain contains, counted per operation class.
//!
//! This is the denominator every throughput number in the sync harness divides
//! by, and it is deliberately a property of the **chain** rather than of
//! whatever processed it. A validator verifies these operations, an indexer
//! indexes them, and a wallet trial-decrypts them; all three see the same
//! blocks, so all three can be timed against the same vector without any of
//! them exposing a ztest-shaped counter. That is what lets a Go lightwalletd
//! and a C++ zcashd be measured on equal terms with a Rust zaino.
//!
//! Two acquisition tiers, differing only in cost:
//!
//! - **A** — sapling outputs, orchard actions, ironwood actions. Carried by
//!   `CompactBlock.chainMetadata` as cumulative commitment-tree sizes, so a
//!   range costs two RPCs and no scan (see [`super::chainwork`]).
//! - **B** — sapling spends, transparent inputs/outputs, sprout JoinSplits.
//!   Not derivable from a tree size; needs a range scan or validator RPC. Not
//!   yet implemented; [`super::chainwork::ChainWork::support`] is what reports
//!   an op as unobtainable, at runtime, rather than a static claim here that
//!   could drift from what the acquisition path actually manages.
//!
//! A tier-B op is therefore *unmeasured*, which is a different statement from
//! *zero*, and [`Work`] keeps the two apart: a row nobody counted must render
//! as `—`, never as `0`, or a reader concludes a range holds no transparent
//! activity when in fact nothing looked.

use std::time::Duration;

/// One class of protocol work. Finer than a value pool because costs inside a
/// pool are not uniform — a Sapling spend proof and a Sapling output proof are
/// different work, and collapsing them would make a spend-heavy range
/// indistinguishable from an output-heavy one of the same size.
///
/// Display collapses to channels ([`CHANNELS`]); comparison and any future
/// weighting stay at this granularity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    /// Transparent input: script + signature verification.
    TransparentIn,
    /// Transparent output: UTXO creation.
    TransparentOut,
    /// Sprout JoinSplit. Legacy, but real work on historic mainnet — a
    /// taxonomy without it reports pre-Sapling ranges as free.
    SproutJoinSplit,
    /// Sapling spend description (Groth16 spend proof).
    SaplingSpend,
    /// Sapling output description (Groth16 output proof / trial-decryption
    /// target).
    SaplingOutput,
    /// Orchard action (Halo2, unified spend+output).
    OrchardAction,
    /// Ironwood action.
    IronwoodAction,
}

/// The display channels: a name and the ops it aggregates, in stacking order
/// (oldest pool first).
///
/// The single source for every per-pool list in ztest — the status panel's
/// rows, the graph's stack order, and the driver's timeline channels. These
/// three were separate literals that had to be kept in lockstep by hand, and a
/// graph whose stack order disagrees with its legend mislabels silently rather
/// than failing.
pub const CHANNELS: [(&str, &[Op]); 5] = [
    ("transparent", &[Op::TransparentIn, Op::TransparentOut]),
    ("sprout", &[Op::SproutJoinSplit]),
    ("sapling", &[Op::SaplingSpend, Op::SaplingOutput]),
    ("orchard", &[Op::OrchardAction]),
    ("ironwood", &[Op::IronwoodAction]),
];

impl Op {
    /// Every op, in display order (transparent → sprout → sapling → orchard →
    /// ironwood, i.e. oldest pool first, which is also the order they stack in
    /// a graph).
    pub const ALL: [Op; 7] = [
        Op::TransparentIn,
        Op::TransparentOut,
        Op::SproutJoinSplit,
        Op::SaplingSpend,
        Op::SaplingOutput,
        Op::OrchardAction,
        Op::IronwoodAction,
    ];

    /// Number of distinct ops — the width of a [`Work`] vector.
    pub const COUNT: usize = Op::ALL.len();

    /// Position in a [`Work`] vector.
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

    /// Stable identifier, used on the wire and in the `perf` diff table. Fixed
    /// strings rather than `Debug` because they cross the driver→controller
    /// event stream, where a rename would silently change what a 48-hour sync
    /// publishes.
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

/// The set of ops a [`Work`] vector actually measured.
///
/// A bitset rather than making every count an `Option`, so a `Work` stays a
/// plain `Copy` value the size of its counters and the intersection rule for
/// subtraction is one `&`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OpSet(u8);

impl OpSet {
    /// The empty set — nothing measured.
    pub const NONE: OpSet = OpSet(0);

    /// Build a set from a slice, at compile time.
    pub const fn of(ops: &[Op]) -> OpSet {
        let mut bits = 0u8;
        let mut i = 0;
        while i < ops.len() {
            bits |= 1 << ops[i].index();
            i += 1;
        }
        OpSet(bits)
    }

    /// Whether `op` was measured.
    pub const fn has(self, op: Op) -> bool {
        self.0 & (1 << op.index()) != 0
    }

    /// The set with `op` added.
    pub const fn with(self, op: Op) -> OpSet {
        OpSet(self.0 | (1 << op.index()))
    }

    /// Ops measured by both — the only ops a comparison between the two can
    /// speak about.
    pub const fn intersect(self, other: OpSet) -> OpSet {
        OpSet(self.0 & other.0)
    }

    /// Whether nothing at all was measured.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// Protocol work over some span of chain, per [`Op`], with the set of ops that
/// were actually counted.
///
/// Serialized as a map keyed by [`Op::label`], never positionally. In memory it
/// stays a fixed array, because that is what makes it `Copy` and makes a
/// subtraction seven adds and a mask.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Work {
    counts: [u64; Op::COUNT],
    known: OpSet,
}

impl Work {
    /// An empty vector with nothing measured.
    pub const ZERO: Work = Work {
        counts: [0; Op::COUNT],
        known: OpSet::NONE,
    };

    /// Record `count` for `op`, marking it measured. A recorded zero is a real
    /// observation and stays distinguishable from an absent one.
    pub fn set(&mut self, op: Op, count: u64) -> &mut Self {
        self.counts[op.index()] = count;
        self.known = self.known.with(op);
        self
    }

    /// The count for `op`, or `None` when nothing measured it.
    pub fn get(&self, op: Op) -> Option<u64> {
        self.known.has(op).then(|| self.counts[op.index()])
    }

    /// Which ops this vector speaks about.
    pub fn known(&self) -> OpSet {
        self.known
    }

    /// The count for `op`, panicking when nothing measured it.
    ///
    /// For probe predicates, where reading an unmeasured op is an error in the
    /// *test*, not an observation about the run. The alternative — treating
    /// absent as zero — turns a probe over an uncollected op into a check that
    /// can never fail, which is worse than no probe at all because it reports
    /// green. Use [`get`](Self::get) where absence is a legitimate answer.
    pub fn require(&self, op: Op) -> u64 {
        self.get(op).unwrap_or_else(|| missing(op))
    }

    /// Sum across every measured op.
    ///
    /// Corpus-dependent by construction: a transparent-heavy range and a
    /// shielded-heavy range of equal length produce wildly different totals
    /// with no code change anywhere. It is comparable **only** between runs
    /// over an identical height segment, which is the comparison
    /// `until_height` exists to make possible and which `perf --base` refuses
    /// to perform without. Never present it without its composition alongside.
    pub fn total(&self) -> Option<u64> {
        (!self.known.is_empty()).then(|| Op::ALL.iter().filter_map(|&op| self.get(op)).sum::<u64>())
    }

    /// Work done between two cumulative readings.
    ///
    /// Saturating, because a reorg rolls the commitment trees backwards and
    /// negative work is not a thing — the rollback itself is reported by
    /// [`Snapshot::reorg_depth`](crate::sync::Snapshot::reorg_depth), which is
    /// where that event belongs. The result speaks only about ops **both**
    /// readings measured.
    pub fn delta(&self, earlier: &Work) -> Work {
        let known = self.known.intersect(earlier.known);
        let mut out = Work {
            counts: [0; Op::COUNT],
            known,
        };
        for op in Op::ALL {
            if known.has(op) {
                out.counts[op.index()] =
                    self.counts[op.index()].saturating_sub(earlier.counts[op.index()]);
            }
        }
        out
    }

    /// Per-second rates over `elapsed`. A non-positive interval yields no
    /// rates rather than an infinite one.
    pub fn rate(&self, elapsed: Duration) -> Rate {
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            return Rate::default();
        }
        let mut out = Rate {
            per_sec: [0.0; Op::COUNT],
            known: self.known,
        };
        for op in Op::ALL {
            if self.known.has(op) {
                out.per_sec[op.index()] = self.counts[op.index()] as f64 / secs;
            }
        }
        out
    }

    /// Per-[`CHANNELS`] totals, in stacking order. `None` means *unmeasured*
    /// and must render as `—`; `Some(0)` means a counted zero.
    pub fn channels(&self) -> [(&'static str, Option<u64>); 5] {
        CHANNELS.map(|(name, ops)| (name, sum(ops.iter().map(|&op| self.get(op)))))
    }

    /// Each channel's percentage share of the total.
    ///
    /// This is what a pinned segment's work vector is *for*. Over a fixed span
    /// the totals are a constant every run shares, so they cannot express a
    /// difference between runs — the one thing they can tell a reader is what
    /// the range is made of, which is what explains why an optimisation landed
    /// or didn't.
    pub fn composition(&self) -> [(&'static str, Option<f64>); 5] {
        let total = self.total().unwrap_or(0) as f64;
        self.channels().map(|(name, n)| {
            let share = n.filter(|_| total > 0.0).map(|n| n as f64 / total * 100.0);
            (name, share)
        })
    }
}

/// The panic behind [`Work::require`], naming both what was asked for and the
/// two reasons it can be absent, so the author knows which one they hit.
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

/// Total of the measured entries, or `None` when none were measured — the rule
/// that keeps an unmeasured channel from summing to a misleading zero.
fn sum<T>(vals: impl Iterator<Item = Option<T>>) -> Option<T>
where
    T: std::ops::Add<Output = T> + Default,
{
    vals.flatten()
        .fold(None, |acc: Option<T>, n| Some(acc.unwrap_or_default() + n))
}

/// The span of chain a run traversed, and how long it took.
///
/// **A run measures the time to traverse a fixed span of chain.** The span is
/// the identity; the work inside it is a property of the *chain*, so two runs
/// over the same heights necessarily contain the same work; the elapsed time is
/// the only thing that varies, and therefore the only thing a comparison can be
/// about. `work / elapsed` is the throughput, computed at display time.
///
/// [`to`](Self::to) is the height actually **reached**, never the one that was
/// asked for. That single choice is what makes comparability an *observation*
/// rather than a declaration: two runs are comparable exactly when they covered
/// the same ground, whether or not either declared a stop. A run that timed out
/// short of its target reports where it truly got to and is refused, and a run
/// that chased the tip is comparable if it happens to have covered the same
/// span — which it did, so the comparison is sound.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Segment {
    /// Chain name as the indexer reports it (`main`, `test`, `regtest`), or
    /// `None` when there was no indexer to ask. Two unknowns are not evidence
    /// of the same chain, and [`comparable_with`](Self::comparable_with)
    /// refuses them rather than letting them match each other.
    pub network: Option<String>,
    /// Height the run started from.
    pub from: u32,
    /// Height the run reached.
    pub to: u32,
    /// Work the span contained.
    pub work: Work,
    /// Wall-clock spent traversing `from..=to`, in milliseconds.
    ///
    /// Measured between the first and last readings, so it excludes the
    /// provisioning that precedes them — a topology taking four minutes to come
    /// up is not part of how fast the subject syncs. Milliseconds rather than a
    /// `Duration` so the durable report stays legible to anything reading it as
    /// plain JSON, matching the event stream's convention.
    pub elapsed_ms: u64,
}

/// Why two segments cannot be compared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mismatch {
    /// Not known to be the same chain — either they differ, or one did not
    /// record which chain it ran against.
    Network {
        base: Option<String>,
        head: Option<String>,
    },
    /// Different spans of chain.
    Range { base: String, head: String },
}

impl std::fmt::Display for Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mismatch::Network {
                base: Some(base),
                head: Some(head),
            } => write!(f, "different networks ({base} vs {head})"),
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
    /// Whether `self` and `other` covered the same ground, and so may be
    /// compared.
    pub fn comparable_with(&self, other: &Segment) -> Result<(), Mismatch> {
        let (Some(head), Some(base)) = (&self.network, &other.network) else {
            return Err(Mismatch::Network {
                base: other.network.clone(),
                head: self.network.clone(),
            });
        };
        if head != base {
            return Err(Mismatch::Network {
                base: Some(base.clone()),
                head: Some(head.clone()),
            });
        }
        if (self.from, self.to) != (other.from, other.to) {
            return Err(Mismatch::Range {
                base: other.describe(),
                head: self.describe(),
            });
        }
        Ok(())
    }

    /// Time spent traversing the span.
    pub fn elapsed(&self) -> Duration {
        Duration::from_millis(self.elapsed_ms)
    }

    /// Throughput over the span. The headline number, and the only quantity in
    /// which two runs over the same span can actually differ.
    pub fn rate(&self) -> Rate {
        self.work.rate(self.elapsed())
    }

    /// Human form: `regtest 840,000..855,000`.
    pub fn describe(&self) -> String {
        use crate::ui::text::thousands;
        format!(
            "{} {}..{}",
            self.network.as_deref().unwrap_or("unknown-network"),
            thousands(u64::from(self.from)),
            thousands(u64::from(self.to)),
        )
    }
}

/// Per-second [`Work`], carrying the same measured-op set so an unmeasured op
/// stays distinguishable from an idle one. Serialized like [`Work`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Rate {
    per_sec: [f64; Op::COUNT],
    known: OpSet,
}

/// Label-keyed serde for [`Work`] and [`Rate`].
///
/// The in-memory form is positional — a `[_; 7]` indexed by [`Op::index`] — and
/// serializing it that way would make the array's *order* part of the durable
/// format. Tier B will insert ops into the middle of [`Op::ALL`], silently
/// renumbering every report already written; a `perf --base` against an
/// archived run would then read sapling counts as sprout ones and say nothing.
///
/// Keying by [`Op::label`] makes absence explicit rather than positional, and
/// makes the format additive in both directions: an unknown key is skipped, so
/// a controller can read a newer driver's reports, and a missing key is simply
/// unmeasured, which is exactly what it means.
mod op_keyed {
    use super::{Op, OpSet};
    use serde::de::{Deserialize, Deserializer};
    use serde::ser::{Serialize, SerializeMap, Serializer};
    use std::collections::BTreeMap;

    pub(super) fn serialize<S, T>(
        values: &[T; Op::COUNT],
        known: OpSet,
        s: S,
    ) -> Result<S::Ok, S::Error>
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

    pub(super) fn deserialize<'de, D, T>(d: D) -> Result<([T; Op::COUNT], OpSet), D::Error>
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
    /// Rate for `op`, or `None` when unmeasured.
    pub fn get(&self, op: Op) -> Option<f64> {
        self.known.has(op).then(|| self.per_sec[op.index()])
    }

    /// Which ops this rate speaks about.
    pub fn known(&self) -> OpSet {
        self.known
    }

    /// Rate for `op`, panicking when nothing measured it. See
    /// [`Work::require`].
    pub fn require(&self, op: Op) -> f64 {
        self.get(op).unwrap_or_else(|| missing(op))
    }

    /// Sum across measured ops. Carries [`Work::total`]'s corpus caveat.
    pub fn total(&self) -> Option<f64> {
        (!self.known.is_empty()).then(|| Op::ALL.iter().filter_map(|&op| self.get(op)).sum::<f64>())
    }

    /// Per-[`CHANNELS`] rates, matching [`Work::channels`].
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

    /// The distinction the whole type exists for: an op nobody counted must not
    /// answer `0`, because a reader would conclude the range holds no
    /// transparent activity when in fact nothing looked.
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

    /// Sapling counts two ops; measuring only one gives a floor on the channel
    /// rather than nothing at all.
    #[test]
    fn a_partially_measured_channel_sums_what_it_has() {
        let mut w = Work::ZERO;
        w.set(Op::SaplingOutput, 7);
        assert_eq!(w.channels()[2].1, Some(7));
        w.set(Op::SaplingSpend, 3);
        assert_eq!(w.channels()[2].1, Some(10));
    }

    /// A probe reading an op nobody counted is a bug in the probe. Answering
    /// zero would make it unfailable, so it panics and names the op.
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

    /// A reorg rolls the commitment trees backwards. Negative work is not a
    /// thing, and the rollback is reported as `reorg_depth` rather than as a
    /// nonsense rate.
    #[test]
    fn a_reorg_does_not_produce_negative_work() {
        let d = tier_a(100, 40, 0).delta(&tier_a(150, 90, 0));
        assert_eq!(d.get(Op::SaplingOutput), Some(0));
        assert_eq!(d.total(), Some(0));
    }

    /// Comparing a vector against one that measured less must not invent the
    /// missing op — the delta can only speak about what both sides counted.
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

    /// A zero interval is not an infinite rate. Two snapshots can share a tick
    /// instant when a poll returns instantly.
    #[test]
    fn a_zero_interval_yields_no_rates_rather_than_infinity() {
        let r = tier_a(120, 60, 0).rate(Duration::ZERO);
        assert_eq!(r.total(), None);
        assert!(r.known().is_empty());
    }

    /// The property the whole label-keyed encoding exists for. A positional
    /// array would make `Op::ALL`'s order part of the durable format, and tier B
    /// inserts ops into the middle of it — silently renumbering every report
    /// already written.
    #[test]
    fn work_travels_keyed_by_label_not_by_position() {
        let wire = serde_json::to_string(&tier_a(10, 20, 0)).expect("serialize");
        assert_eq!(
            wire,
            r#"{"sapling-output":10,"orchard-action":20,"ironwood-action":0}"#
        );
    }

    /// An op absent from the wire was never measured, which is not zero — the
    /// distinction has to survive the round trip or the panel prints `0` for a
    /// pool nobody counted.
    #[test]
    fn an_absent_key_decodes_as_unmeasured_rather_than_zero() {
        let w: Work = serde_json::from_str(r#"{"sapling-output":10}"#).expect("parse");
        assert_eq!(w.get(Op::SaplingOutput), Some(10));
        assert_eq!(w.get(Op::OrchardAction), None);
        assert_eq!(w.total(), Some(10));
    }

    /// A sync in the `sync` tier outlives ztest builds, so a controller reads
    /// reports from drivers newer than itself. An op it has never heard of must
    /// be skipped, not refused.
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

    /// The refusal that keeps `perf --base` honest: over different heights a
    /// throughput difference is mostly a statement about which blocks were
    /// chosen, not about the code.
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

    /// Comparability is an observation, not a declaration. A run that chased
    /// the tip and one that declared a stop covered the same ground if they
    /// reached the same heights — and if they did, the comparison is sound.
    #[test]
    fn covering_the_same_ground_is_what_makes_runs_comparable() {
        let declared = segment(Some("regtest"), 840_000, 855_000, 100);
        let to_tip = segment(Some("regtest"), 840_000, 855_000, 90);
        assert_eq!(declared.comparable_with(&to_tip), Ok(()));
    }

    /// A run that stopped short of its target reports where it truly got to,
    /// which is what makes it refuse rather than compare a partial traversal
    /// against a complete one as though both had finished.
    #[test]
    fn a_run_that_stopped_short_is_refused_on_its_reached_height() {
        let complete = segment(Some("regtest"), 840_000, 855_000, 100);
        let timed_out = segment(Some("regtest"), 840_000, 850_112, 100);
        assert!(matches!(
            complete.comparable_with(&timed_out),
            Err(Mismatch::Range { .. })
        ));
    }

    #[test]
    fn segments_on_different_networks_are_refused() {
        let head = segment(Some("main"), 840_000, 855_000, 100);
        let Err(Mismatch::Network { base, head: h }) =
            head.comparable_with(&segment(Some("test"), 840_000, 855_000, 100))
        else {
            panic!("a network mismatch must name both sides");
        };
        assert_eq!(
            (base.as_deref(), h.as_deref()),
            (Some("test"), Some("main"))
        );
    }

    /// Two runs that could not name their chain are not thereby on the same
    /// chain. Letting unknowns match each other would compare mainnet against
    /// regtest without a word.
    #[test]
    fn an_unrecorded_network_never_matches_another_unrecorded_one() {
        let a = segment(None, 840_000, 855_000, 100);
        assert!(matches!(
            a.comparable_with(&a.clone()),
            Err(Mismatch::Network { .. })
        ));
        assert!(
            Mismatch::Network {
                base: None,
                head: None
            }
            .to_string()
            .contains("no indexer")
        );
    }

    #[test]
    fn a_segment_describes_its_span_readably() {
        assert_eq!(
            segment(Some("regtest"), 840_000, 855_000, 100).describe(),
            "regtest 840,000..855,000"
        );
        assert_eq!(segment(None, 0, 10, 1).describe(), "unknown-network 0..10");
    }

    /// Throughput is the whole measurement: the same work over less time is the
    /// only difference two comparable runs can express.
    #[test]
    fn a_segments_rate_is_its_work_over_its_elapsed_time() {
        let s = segment(Some("regtest"), 840_000, 855_000, 100);
        assert_eq!(s.rate().get(Op::SaplingOutput), Some(10.0));
        assert_eq!(s.rate().total(), Some(15.0));
        let faster = segment(Some("regtest"), 840_000, 855_000, 50);
        assert_eq!(faster.rate().total(), Some(30.0));
    }

    /// The segment rides the durable report, which is the API other tools read.
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

    /// Channels are what the panel lists and the plot stacks, so their order is
    /// the stacking order and must stay oldest-pool-first.
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

    /// Every op belongs to exactly one channel, or a channel list that looked
    /// complete would quietly drop work from every total drawn from it.
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

    /// Composition is what a shared work vector is for; an unmeasured channel
    /// has no share rather than a zero one.
    #[test]
    fn composition_is_each_channels_share_of_the_total() {
        let shares = tier_a(75, 25, 0).composition();
        assert_eq!(shares[2].1, Some(75.0));
        assert_eq!(shares[3].1, Some(25.0));
        assert_eq!(shares[4].1, Some(0.0));
        assert_eq!(shares[0].1, None, "unmeasured has no share");
    }

    /// A total of zero has no shares to report, and must not divide by it.
    #[test]
    fn composition_of_no_work_reports_no_shares() {
        assert!(
            tier_a(0, 0, 0)
                .composition()
                .iter()
                .all(|(_, share)| share.is_none())
        );
    }
}
