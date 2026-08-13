//! L2 — how load is applied. One [`LoadDriver`] → N connections against one
//! endpoint ([`LoadDriver::new`]) or a lock-step pair ([`LoadDriver::pair`]).
//!
//! - Paired requests hit A & B from one task (same node, same instant =
//!   precondition for trustworthy A/B-relative timing)
//! - One channel per connection (exercises the accept path + per-connection
//!   server state, not one multiplexed socket)
//! - Handshake timed as [`OpKind::Connect`], excluded from throughput
//! - Violations & parity records deduped per task (*which* height diverged is the
//!   signal, not that it recurred 10⁴ times)

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use crate::EnvError;
use crate::loadtest::client::LwdClient;
use crate::loadtest::oracle::{BlockOracle, Violation};
use crate::loadtest::report::{LoadReport, OpKind, ParityRecord, Side};
use crate::loadtest::scenario::Scenario;
use crate::proto::CompactBlock;

const SIGFIG: u8 = 3;

fn new_hist() -> Histogram<u64> {
    Histogram::new(SIGFIG).expect("valid sigfig")
}

/// Per-task accumulator for one backend, merged across tasks after the join
struct Tally {
    hists: BTreeMap<OpKind, Histogram<u64>>,
    ops: u64,
    errors: u64,
    violations: Vec<Violation>,
}

impl Tally {
    fn new() -> Self {
        Self { hists: BTreeMap::new(), ops: 0, errors: 0, violations: Vec::new() }
    }

    /// Record a latency sample; never touches `ops` (throughput counts scenario
    /// ops only, so [`OpKind::Connect`] cannot inflate it — call sites bump `ops`)
    fn record(&mut self, op: OpKind, elapsed: Duration) {
        let h = self.hists.entry(op).or_insert_with(new_hist);
        let _ = h.record(elapsed.as_micros() as u64);
    }

    fn merge(&mut self, other: Tally) {
        self.ops += other.ops;
        self.errors += other.errors;
        self.violations.extend(other.violations);
        for (k, h) in other.hists {
            let dst = self.hists.entry(k).or_insert_with(new_hist);
            let _ = dst.add(&h);
        }
    }

    fn into_side(self, wall: Duration) -> Side {
        Side {
            by_op: self
                .hists
                .iter()
                .map(|(k, h)| (*k, crate::loadtest::report::LatencyStats::from_hist(h)))
                .collect(),
            throughput: throughput(self.ops, wall),
            total_ops: self.ops,
            errors: self.errors,
            violations: self.violations,
        }
    }
}

/// `Two` = differential mode: both issued concurrently from one task, so their
/// latencies compare
#[derive(Debug)]
enum Target {
    One(LwdClient),
    Two(LwdClient, LwdClient),
}

/// Fan-out load generator: one endpoint, or two in lock-step
#[derive(Debug)]
pub struct LoadDriver {
    target: Target,
    connections: usize,
    stagger: Duration,
    scenario: Scenario,
    stable_below: Option<u64>,
    duration: Duration,
    label: String,
}

impl LoadDriver {
    /// Single endpoint → [`b`](LoadReport::b) `None`, so
    /// [`assert_parity`](LoadReport::assert_parity) /
    /// [`assert_relative`](LoadReport::assert_relative) reject rather than pass
    /// vacuously
    pub fn new(client: LwdClient) -> Self {
        Self::with_target(Target::One(client))
    }

    /// Two endpoints differentially: every request issued to both from one task,
    /// responses compared for byte equality
    pub fn pair(a: LwdClient, b: LwdClient) -> Self {
        Self::with_target(Target::Two(a, b))
    }

    fn with_target(target: Target) -> Self {
        Self {
            target,
            connections: 1,
            stagger: Duration::from_millis(1),
            scenario: Scenario::BlockRangeSweep {
                pool: 1..2,
                blocks: 1,
                dist: crate::loadtest::scenario::Distribution::Even,
            },
            stable_below: None,
            duration: Duration::from_secs(1),
            label: "load".into(),
        }
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }
    pub fn connections(mut self, n: usize) -> Self {
        self.connections = n.max(1);
        self
    }
    pub fn spawn_stagger(mut self, d: Duration) -> Self {
        self.stagger = d;
        self
    }
    pub fn scenario(mut self, s: Scenario) -> Self {
        self.scenario = s;
        self
    }
    /// Also hold settled history immutable: no height < `height` may change hash
    /// for the run ([`BlockOracle::stable_below`])
    pub fn stable_below(mut self, height: u64) -> Self {
        self.stable_below = Some(height);
        self
    }
    /// Per-connection runtime; clock starts at that connection's start (a
    /// staggered spawn does not shorten the later ones)
    pub fn duration(mut self, d: Duration) -> Self {
        self.duration = d;
        self
    }

    pub async fn run(self) -> Result<LoadReport, EnvError> {
        let scenario = Arc::new(self.scenario);
        let op_kind = scenario.op_kind();
        let run_for = self.duration;
        let wall = Instant::now();

        // One oracle for all connections: global settled-history baseline lets one
        // connection cross-check what another saw
        let oracle = Arc::new(match self.stable_below {
            Some(h) => BlockOracle::new().stable_below(h),
            None => BlockOracle::new(),
        });

        let (client_a, client_b) = match self.target {
            Target::One(a) => (a, None),
            Target::Two(a, b) => (a, Some(b)),
        };
        let paired = client_b.is_some();

        let mut handles = Vec::with_capacity(self.connections);
        for i in 0..self.connections {
            let (a, dial_a) = dial(&client_a).await?;
            let b = match &client_b {
                Some(b) => Some(dial(b).await?),
                None => None,
            };
            let (start, end) = scenario.range_for(i, self.connections);
            let oracle = Arc::clone(&oracle);
            handles.push(tokio::spawn(async move {
                let mut ta = Tally::new();
                let mut tb = Tally::new();
                ta.record(OpKind::Connect, dial_a);
                let b = b.map(|(client, dial_b)| {
                    tb.record(OpKind::Connect, dial_b);
                    client
                });

                let mut parity: Vec<ParityRecord> = Vec::new();
                let mut seen_diff: BTreeSet<(u64, &'static str)> = BTreeSet::new();
                let mut seen_a: BTreeSet<(u64, String)> = BTreeSet::new();
                let mut seen_b: BTreeSet<(u64, String)> = BTreeSet::new();

                let deadline = Instant::now() + run_for;
                while Instant::now() < deadline {
                    // Concurrent, each with its own timer → the A/B ratio compares
                    // like with like
                    let (ra, rb) = match &b {
                        Some(b) => {
                            let (x, y) = tokio::join!(timed(&a, start, end), timed(b, start, end),);
                            (x, Some(y))
                        }
                        None => (timed(&a, start, end).await, None),
                    };

                    match (ra, rb) {
                        ((Ok(ba), da), None) => {
                            ta.ops += 1;
                            ta.record(op_kind, da);
                            observe_into(&oracle, start, end, &ba, &mut seen_a, &mut ta);
                        }
                        ((Ok(ba), da), Some((Ok(bb), db))) => {
                            ta.ops += 1;
                            tb.ops += 1;
                            ta.record(op_kind, da);
                            tb.record(op_kind, db);
                            // Both validated: parity alone misses a defect both
                            // backends share, and B's own findings would go unseen
                            observe_into(&oracle, start, end, &ba, &mut seen_a, &mut ta);
                            observe_into(&oracle, start, end, &bb, &mut seen_b, &mut tb);
                            for rec in diff_ranges(&ba, &bb) {
                                if seen_diff.insert((rec.height, rec.detail)) {
                                    parity.push(rec);
                                }
                            }
                        }
                        // Either failure voids the comparison; each error attributed
                        // to its own side
                        ((ra, _), rb) => {
                            if ra.is_err() {
                                ta.errors += 1;
                            }
                            if matches!(rb, Some((Err(_), _))) {
                                tb.errors += 1;
                            }
                        }
                    }
                }
                (ta, tb, parity)
            }));

            if !self.stagger.is_zero() {
                tokio::time::sleep(self.stagger).await;
            }
        }

        let mut ma = Tally::new();
        let mut mb = Tally::new();
        let mut parity: Vec<ParityRecord> = Vec::new();
        let mut seen: BTreeSet<(u64, &'static str)> = BTreeSet::new();
        for joined in futures::future::join_all(handles).await {
            // Panicked connection must fail the run, not vanish (survivors' numbers
            // would look clean)
            let (ta, tb, p) = joined.map_err(crate::error::env_err)?;
            ma.merge(ta);
            mb.merge(tb);
            for rec in p {
                if seen.insert((rec.height, rec.detail)) {
                    parity.push(rec);
                }
            }
        }
        let wall = wall.elapsed();
        parity.sort_by_key(|p| p.height);
        Ok(LoadReport {
            label: self.label,
            wall,
            connections: self.connections,
            a: ma.into_side(wall),
            b: paired.then(|| mb.into_side(wall)),
            parity_diffs: parity,
        })
    }
}

async fn timed(
    client: &LwdClient,
    start: u64,
    end: u64,
) -> (Result<Vec<CompactBlock>, tonic::Status>, Duration) {
    let t = Instant::now();
    let r = client.block_range(start, end).await;
    (r, t.elapsed())
}

async fn dial(client: &LwdClient) -> Result<(LwdClient, Duration), EnvError> {
    let t = Instant::now();
    let dialed = client.dial().await?;
    Ok((dialed, t.elapsed()))
}

/// Oracle over one response, keeping only findings new to this task
fn observe_into(
    oracle: &BlockOracle,
    start: u64,
    end: u64,
    blocks: &[CompactBlock],
    seen: &mut BTreeSet<(u64, String)>,
    tally: &mut Tally,
) {
    for v in oracle.observe(start, end, blocks) {
        if seen.insert((v.height, v.field.clone())) {
            tally.violations.push(v);
        }
    }
}

fn throughput(ops: u64, wall: Duration) -> f64 {
    let s = wall.as_secs_f64();
    if s > 0.0 { ops as f64 / s } else { 0.0 }
}

/// Align two block sets by height, compare the overlap, report presence gaps.
/// Whole-block derived `PartialEq` = exhaustive by construction (a new proto field
/// is covered the day it lands, no differ to keep in sync)
fn diff_ranges(a: &[CompactBlock], b: &[CompactBlock]) -> Vec<ParityRecord> {
    let ma: BTreeMap<u64, &CompactBlock> = a.iter().map(|x| (x.height, x)).collect();
    let mb: BTreeMap<u64, &CompactBlock> = b.iter().map(|x| (x.height, x)).collect();
    let heights: BTreeSet<u64> = ma.keys().chain(mb.keys()).copied().collect();
    let mut out = Vec::new();
    for height in heights {
        let detail = match (ma.get(&height), mb.get(&height)) {
            (Some(x), Some(y)) if x == y => continue,
            (Some(_), Some(_)) => ParityRecord::DIFFERS,
            (Some(_), None) => ParityRecord::MISSING_FROM_B,
            (None, Some(_)) => ParityRecord::MISSING_FROM_A,
            (None, None) => unreachable!("height came from one of the two maps"),
        };
        out.push(ParityRecord { height, detail });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(height: u64) -> CompactBlock {
        CompactBlock {
            proto_version: 1,
            height,
            hash: vec![height as u8; 32],
            prev_hash: vec![height as u8 - 1; 32],
            time: 1000,
            header: vec![],
            vtx: vec![],
            chain_metadata: None,
        }
    }

    #[test]
    fn identical_ranges_produce_no_parity_records() {
        let a = vec![block(10), block(11)];
        assert!(diff_ranges(&a, &a.clone()).is_empty());
    }

    #[test]
    fn a_height_missing_from_b_is_reported() {
        let recs = diff_ranges(&[block(10), block(11)], &[block(10)]);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].height, 11);
        assert_eq!(recs[0].detail, ParityRecord::MISSING_FROM_B);
    }

    #[test]
    fn a_height_missing_from_a_is_reported() {
        let recs = diff_ranges(&[block(10)], &[block(10), block(11)]);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].detail, ParityRecord::MISSING_FROM_A);
    }

    /// Any inequality must produce a record, whatever field carries it (derived
    /// `PartialEq` = exhaustive, no differ to keep in sync)
    #[test]
    fn any_field_difference_is_reported() {
        let a = block(10);
        for mutate in [
            |b: &mut CompactBlock| b.time += 1,
            |b: &mut CompactBlock| b.proto_version += 1,
            |b: &mut CompactBlock| b.hash = vec![0xAA; 32],
            |b: &mut CompactBlock| b.header = vec![1, 2, 3],
            |b: &mut CompactBlock| b.chain_metadata = Some(Default::default()),
        ] {
            let mut b = a.clone();
            mutate(&mut b);
            let recs = diff_ranges(std::slice::from_ref(&a), &[b]);
            assert_eq!(recs.len(), 1);
            assert_eq!(recs[0].detail, ParityRecord::DIFFERS);
        }
    }
}
