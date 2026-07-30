//! L2 — how load is applied. [`LoadDriver`] fans out to N connections against one
//! endpoint; [`DiffLoadDriver`] drives two backends in lock-step so each request
//! hits A and B in the same task (same node, same instant — the precondition for
//! trustworthy A/B-relative timing).
//!
//! Per-op latency lands in an `hdrhistogram`. Oracle violations and parity diffs
//! are **deduplicated per task** by `(height, field)`: under a duration-bounded
//! run every connection re-fetches its window many times, and the actionable
//! signal is *which* height/field diverged, not that it recurred 10⁴ times.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use crate::EnvError;
use crate::loadtest::client::LwdClient;
use crate::loadtest::oracle::{FieldDiff, Observed, Oracle, Violation, diff_compact_block};
use crate::loadtest::report::{LatencyStats, LoadReport, OpKind, ParityRecord};
use crate::loadtest::scenario::Scenario;
use crate::proto::CompactBlock;

/// Whether every connection shares one multiplexed channel, or dials its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnMode {
    /// One HTTP/2 channel, cloned across all tasks. Exercises server-side
    /// stream multiplexing.
    Shared,
    /// One channel per connection — genuine socket fan-out (the `zaino-admin`
    /// model), exercising the accept path and per-connection server state.
    PerTask,
}

/// When a run stops.
#[derive(Debug, Clone, Copy)]
pub enum Until {
    /// Run for this wall-clock duration.
    Duration(Duration),
    /// Each connection performs this many ops, then stops.
    CountPerConn(u64),
}

const SIGFIG: u8 = 3;

fn new_hist() -> Histogram<u64> {
    Histogram::new(SIGFIG).expect("valid sigfig")
}

/// Per-task accumulator. Merged across tasks after the join.
struct Tally {
    hists: BTreeMap<OpKind, Histogram<u64>>,
    ops: u64,
    errors: u64,
    violations: Vec<Violation>,
}

impl Tally {
    fn new() -> Self {
        Self {
            hists: BTreeMap::new(),
            ops: 0,
            errors: 0,
            violations: Vec::new(),
        }
    }

    fn record(&mut self, op: OpKind, elapsed: Duration) {
        self.ops += 1;
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

    fn stats(&self) -> BTreeMap<OpKind, LatencyStats> {
        self.hists
            .iter()
            .map(|(k, h)| (*k, LatencyStats::from_hist(h)))
            .collect()
    }
}

/// Fan-out load generator against a single endpoint.
#[derive(Debug)]
pub struct LoadDriver {
    client: LwdClient,
    connections: usize,
    conn_mode: ConnMode,
    stagger: Duration,
    scenario: Scenario,
    oracle: Option<Arc<dyn Oracle>>,
    until: Until,
    label: String,
}

impl LoadDriver {
    pub fn new(client: LwdClient) -> Self {
        Self {
            client,
            connections: 1,
            conn_mode: ConnMode::Shared,
            stagger: Duration::from_millis(1),
            scenario: Scenario::BlockRangeSweep {
                pool: 1..2,
                blocks: 1,
                dist: crate::loadtest::scenario::Distribution::Even,
            },
            oracle: None,
            until: Until::CountPerConn(1),
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
    pub fn conn_mode(mut self, mode: ConnMode) -> Self {
        self.conn_mode = mode;
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
    pub fn oracle(mut self, o: impl Oracle + 'static) -> Self {
        self.oracle = Some(Arc::new(o));
        self
    }
    pub fn until(mut self, u: Until) -> Self {
        self.until = u;
        self
    }

    pub async fn run(self) -> Result<LoadReport, EnvError> {
        let scenario = Arc::new(self.scenario);
        let op_kind = scenario.op_kind();
        let stop = StopCond::from(self.until);
        let wall = Instant::now();

        let mut handles = Vec::with_capacity(self.connections);
        for i in 0..self.connections {
            let client = match self.conn_mode {
                ConnMode::Shared => self.client.clone(),
                ConnMode::PerTask => self.client.dial().await?,
            };
            let (start, end) = scenario.range_for(i, self.connections);
            let oracle = self.oracle.clone();
            handles.push(tokio::spawn(async move {
                let mut tally = Tally::new();
                let mut seen: BTreeSet<(u64, String)> = BTreeSet::new();
                let mut done = stop.starter();
                while !done.reached() {
                    let t = Instant::now();
                    match client.block_range(start, end).await {
                        Ok(blocks) => {
                            tally.record(op_kind, t.elapsed());
                            if let Some(o) = &oracle {
                                for v in o.observe(&Observed {
                                    start,
                                    end,
                                    blocks: &blocks,
                                }) {
                                    if seen.insert((v.height, v.field.clone())) {
                                        tally.violations.push(v);
                                    }
                                }
                            }
                        }
                        Err(_) => tally.errors += 1,
                    }
                    done.tick();
                }
                tally
            }));

            if !self.stagger.is_zero() {
                tokio::time::sleep(self.stagger).await;
            }
        }

        let mut merged = Tally::new();
        for h in futures::future::join_all(handles).await {
            if let Ok(t) = h {
                merged.merge(t);
            }
        }
        let wall = wall.elapsed();
        let throughput = throughput(merged.ops, wall);
        Ok(LoadReport {
            label: self.label,
            by_op: merged.stats(),
            throughput,
            total_ops: merged.ops,
            errors: merged.errors,
            wall,
            connections: self.connections,
            violations: merged.violations,
            b_by_op: BTreeMap::new(),
            b_throughput: 0.0,
            parity_diffs: Vec::new(),
        })
    }
}

/// Differential driver: every request is issued to both backends in the same
/// task, and their responses are diffed field-by-field.
#[derive(Debug)]
pub struct DiffLoadDriver {
    a: LwdClient,
    b: LwdClient,
    connections: usize,
    conn_mode: ConnMode,
    stagger: Duration,
    scenario: Scenario,
    oracle: Option<Arc<dyn Oracle>>,
    until: Until,
    label: String,
}

impl DiffLoadDriver {
    pub fn pair(a: LwdClient, b: LwdClient) -> Self {
        Self {
            a,
            b,
            connections: 1,
            conn_mode: ConnMode::Shared,
            stagger: Duration::from_millis(1),
            scenario: Scenario::BlockRangeSweep {
                pool: 1..2,
                blocks: 1,
                dist: crate::loadtest::scenario::Distribution::Even,
            },
            oracle: None,
            until: Until::CountPerConn(1),
            label: "diff".into(),
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
    pub fn conn_mode(mut self, mode: ConnMode) -> Self {
        self.conn_mode = mode;
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
    pub fn oracle(mut self, o: impl Oracle + 'static) -> Self {
        self.oracle = Some(Arc::new(o));
        self
    }
    pub fn until(mut self, u: Until) -> Self {
        self.until = u;
        self
    }

    pub async fn run(self) -> Result<LoadReport, EnvError> {
        let scenario = Arc::new(self.scenario);
        let op_kind = scenario.op_kind();
        let stop = StopCond::from(self.until);
        let wall = Instant::now();

        let mut handles = Vec::with_capacity(self.connections);
        for i in 0..self.connections {
            let (a, b) = match self.conn_mode {
                ConnMode::Shared => (self.a.clone(), self.b.clone()),
                ConnMode::PerTask => (self.a.dial().await?, self.b.dial().await?),
            };
            let (start, end) = scenario.range_for(i, self.connections);
            let oracle = self.oracle.clone();
            handles.push(tokio::spawn(async move {
                let mut ta = Tally::new();
                let mut tb = Tally::new();
                let mut parity: Vec<ParityRecord> = Vec::new();
                let mut seen_diff: BTreeSet<(u64, String)> = BTreeSet::new();
                let mut seen_viol: BTreeSet<(u64, String)> = BTreeSet::new();
                let mut done = stop.starter();
                while !done.reached() {
                    // Independent timers: A and B run concurrently but each
                    // measures its own latency, so the A/B ratio is meaningful.
                    let fa = async {
                        let t = Instant::now();
                        (a.block_range(start, end).await, t.elapsed())
                    };
                    let fb = async {
                        let t = Instant::now();
                        (b.block_range(start, end).await, t.elapsed())
                    };
                    let ((ra, da), (rb, db)) = tokio::join!(fa, fb);
                    match (ra, rb) {
                        (Ok(ba), Ok(bb)) => {
                            ta.record(op_kind, da);
                            tb.record(op_kind, db);
                            if let Some(o) = &oracle {
                                for v in o.observe(&Observed {
                                    start,
                                    end,
                                    blocks: &ba,
                                }) {
                                    if seen_viol.insert((v.height, v.field.clone())) {
                                        ta.violations.push(v);
                                    }
                                }
                            }
                            for rec in diff_ranges(&ba, &bb) {
                                if seen_diff.insert((rec.height, rec.diff.field.clone())) {
                                    parity.push(rec);
                                }
                            }
                        }
                        (ra, rb) => {
                            if ra.is_err() {
                                ta.errors += 1;
                            }
                            if rb.is_err() {
                                tb.errors += 1;
                            }
                        }
                    }
                    done.tick();
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
        let mut seen: BTreeSet<(u64, String)> = BTreeSet::new();
        for h in futures::future::join_all(handles).await {
            if let Ok((ta, tb, p)) = h {
                ma.merge(ta);
                mb.merge(tb);
                for rec in p {
                    if seen.insert((rec.height, rec.diff.field.clone())) {
                        parity.push(rec);
                    }
                }
            }
        }
        let wall = wall.elapsed();
        parity.sort_by_key(|p| p.height);
        Ok(LoadReport {
            label: self.label,
            throughput: throughput(ma.ops, wall),
            by_op: ma.stats(),
            total_ops: ma.ops,
            errors: ma.errors + mb.errors,
            wall,
            connections: self.connections,
            violations: ma.violations,
            b_by_op: mb.stats(),
            b_throughput: throughput(mb.ops, wall),
            parity_diffs: parity,
        })
    }
}

fn throughput(ops: u64, wall: Duration) -> f64 {
    let s = wall.as_secs_f64();
    if s > 0.0 { ops as f64 / s } else { 0.0 }
}

/// Align two block sets by height and diff the overlap; report presence gaps.
fn diff_ranges(a: &[CompactBlock], b: &[CompactBlock]) -> Vec<ParityRecord> {
    let ma: BTreeMap<u64, &CompactBlock> = a.iter().map(|x| (x.height, x)).collect();
    let mb: BTreeMap<u64, &CompactBlock> = b.iter().map(|x| (x.height, x)).collect();
    let heights: BTreeSet<u64> = ma.keys().chain(mb.keys()).copied().collect();
    let mut out = Vec::new();
    for h in heights {
        match (ma.get(&h), mb.get(&h)) {
            (Some(x), Some(y)) => {
                for fd in diff_compact_block(x, y) {
                    out.push(ParityRecord {
                        height: h,
                        diff: fd,
                    });
                }
            }
            (Some(_), None) => out.push(ParityRecord {
                height: h,
                diff: FieldDiff {
                    field: "block".into(),
                    value_a: "present".into(),
                    value_b: "missing".into(),
                },
            }),
            (None, Some(_)) => out.push(ParityRecord {
                height: h,
                diff: FieldDiff {
                    field: "block".into(),
                    value_a: "missing".into(),
                    value_b: "present".into(),
                },
            }),
            (None, None) => unreachable!(),
        }
    }
    out
}

/// Snapshot of the stop condition, cloned into each task.
#[derive(Clone, Copy)]
enum StopCond {
    Deadline(Duration),
    Count(u64),
}

impl From<Until> for StopCond {
    fn from(u: Until) -> Self {
        match u {
            Until::Duration(d) => StopCond::Deadline(d),
            Until::CountPerConn(n) => StopCond::Count(n),
        }
    }
}

impl StopCond {
    fn starter(self) -> Progress {
        match self {
            StopCond::Deadline(d) => Progress::Deadline(Instant::now() + d),
            StopCond::Count(n) => Progress::Count { done: 0, target: n },
        }
    }
}

enum Progress {
    Deadline(Instant),
    Count { done: u64, target: u64 },
}

impl Progress {
    fn reached(&self) -> bool {
        match self {
            Progress::Deadline(dl) => Instant::now() >= *dl,
            Progress::Count { done, target } => done >= target,
        }
    }
    fn tick(&mut self) {
        if let Progress::Count { done, .. } = self {
            *done += 1;
        }
    }
}
