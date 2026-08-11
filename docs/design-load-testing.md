# Load, stress & differential testing

A test class modelled on `hhanh00/zaino`'s `zaino-admin` tool: drive a live
indexer's gRPC surface under concurrency and assert it stays **correct**, stays
**in parity** with a second backend, and stays **within a performance budget**.

## Goals

| Goal                         | Signal                                                                                 | Gating                                 |
| ---------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------- |
| Correctness under load       | chain-link invariant holds on every streamed block while N clients hammer the endpoint | yes                                    |
| Differential parity (A vs B) | two indexer backends on one validator return field-identical compact blocks            | yes                                    |
| Perf regression              | latency / throughput within budget                                                     | absolute *or* A/B-relative (see below) |
| Exploratory benchmarking     | latency histograms + throughput, for humans                                            | no                                     |

## The one reframing: where concurrency lives

`zaino-admin` fans out N clients *inside one process*. ztest's own concurrency
is at a different altitude — the engine's 4-D QoS scheduler runs concurrency
**across tests** (pod-per-test / process-per-test), not within one.

So load does **not** map onto "1000 ztest tests". It maps onto **one test that
fans out internally**: a single `#[tokio::test]` whose body owns its runtime and
spawns N clients. The engine admits the whole test as one Guaranteed pod at a
declared footprint, and its process-group SIGKILL already reaps the spawned
tasks on cancel. Consequence: **this is a library the test body calls, not an
engine change.**

## Why measurements are trustworthy (and where they aren't)

Runner pods are **Guaranteed** (`requests == limits`, `pod_runner.rs:417`) with
**whole-integer cores** (`qos::Resources::guaranteed_cpu_mem`), which is the
precondition for the kubelet CPU-Manager `static` policy to pin **exclusive
physical cores**. Memory is Guaranteed; disk bandwidth is throttled per-PVC via
cgroup `io.max` once calibrated (`design-qos.md`). CPU, memory and disk are
therefore partitioned per-pod — co-scheduled neighbours do not time-share a
load test's cores.

Residuals the static policy does **not** partition: LLC / memory-bandwidth
(no RDT/CAT by default) and NUMA alignment (Topology-Manager policy). These are
the only absolute-latency jitter sources on a calibrated cluster, and they are
second-order for LMDB/protobuf block serving.

This shapes the perf gate:

- **Absolute** p99 / throughput → gateable with a tolerance band on a cluster
  running the static CPU policy + fio-calibrated I/O. Reported always.
- **A/B-relative** ratio → the precision gate. Because A and B run on the same
  node at the same instant, the residuals (LLC/membw/NUMA) hit both equally, so
  a *ratio* is robust even where an absolute threshold would drift.

Prerequisites to confirm on the target cluster before leaning on absolute
gating: nodes run `--cpu-manager-policy=static`, and I/O is fio-calibrated.
Uncalibrated cluster or the laptop `LocalExecutor` ⇒ fall back to the
A/B-relative gate. Load tests must run through the on-cluster `PodExecutor`.

## Architecture

A thin library over primitives ztest already owns (the generated tonic
`CompactTxStreamerClient` in `src/proto/`). Layers:

- **L0 — `LwdClient`**: a cheap-clone wrapper over
  `CompactTxStreamerClient<Channel>` with a persistent, HTTP/2-multiplexed
  channel. Closes the one real gap: today every `IndexerBackend` RPC reopens a
  channel, which is fatal for load generation.
- **L1 — `Scenario`**: what each virtual connection does (deterministic range
  distribution by default; reproducible without an RNG).
- **L2 — `LoadDriver`**: fan out to N connections, record per-op latency into an
  hdrhistogram (true p50/p90/p99, fixing `zaino-admin`'s min/mean/max-only
  reporting). `LoadDriver::pair` is the differential mode — each request goes to
  A *and* B from the same task, so their latencies are comparable and their
  responses can be compared for equality. Every connection dials its own
  channel; the handshake is timed as `OpKind::Connect` and excluded from
  throughput.
- **L3 — `BlockOracle`**: validates responses (this is what makes it a *test*,
  not a benchmark). One type carrying three invariants — chain link (genesis
  zeros, 32-byte hashes, `prev_hash == prior.hash`), completeness (the heights
  served are exactly those requested), and opt-in stable history (a settled
  height never changes hash).
- **L4 — result**: `LoadReport` with one `Side` per backend driven (latency,
  throughput, ops, errors, violations) plus the heights at which A and B
  diverged; assertion helpers gate the test.

The `zaino-admin` modes collapse onto one driver: **stress** = `LoadDriver::new`;
**differential** = two `add_indexer` handles (the ≤2-indexer cap fits exactly) +
`LoadDriver::pair`. `zaino-admin`'s third mode, the `grpc-test` RPC sweep, is
deliberately **not** ported — the ~150 existing live tests already assert each
RPC individually, and far more strictly than a sweep that treated `Err` as a
pass on eight of its twenty arms.

**On parity depth.** A/B comparison is prost's derived `PartialEq` over the whole
`CompactBlock`, and the report names the diverging *height*. An earlier revision
also carried a hand-written field-by-field differ so the report could name the
field. It was removed: equality was always the actual gate, the differ only
decorated the message, and a hand-maintained walker goes stale every time the
proto grows a field — exactly the blind spot a parity test exists to prevent. A
divergence reproduces with one `GetBlock` per backend.

**On absolute SLOs.** There is deliberately no `assert_slo`. It would need a
calibrated cluster (static CPU policy, fio-calibrated I/O) to mean anything, and
the load generator itself runs in a resource-capped runner pod — so a latency
number here partly measures the harness. Correctness and A/B ratios gate;
absolute numbers are printed for humans only. See the measurement-model section.

## API

```rust
// L0 — on IndexerBackend
async fn grpc_client(&self)  -> Result<LwdClient, EnvError>; // persistent multiplexed channel, cheap Clone
async fn grpc_channel(&self) -> Result<Channel, EnvError>;   // raw, for per-connection dialing

#[derive(Clone)]
pub struct LwdClient { /* CompactTxStreamerClient<Channel> */ }

// L1 — what a connection does
pub enum Distribution { Even, Scatter }
pub enum Scenario {
    BlockRangeSweep { pool: Range<u64>, blocks: usize, dist: Distribution },
}

// L2 — how load is applied
impl LoadDriver {
    pub fn new(client: LwdClient) -> Self;                  // single endpoint
    pub fn pair(a: LwdClient, b: LwdClient) -> Self;        // differential

    pub fn label(self, label: impl Into<String>) -> Self;
    pub fn connections(self, n: usize) -> Self;
    pub fn spawn_stagger(self, d: Duration) -> Self;
    pub fn scenario(self, s: Scenario) -> Self;
    pub fn stable_below(self, height: u64) -> Self;         // enable the stable-history check
    pub fn duration(self, d: Duration) -> Self;

    pub async fn run(self) -> Result<LoadReport, EnvError>;
}

// L3 — correctness under load
pub struct BlockOracle;                 // built by the driver; `stable_below` is its one knob
pub struct Violation { pub height: u64, pub field: String, pub detail: String }

// L4 — result
pub struct LatencyStats { pub p50: Duration, pub p90: Duration, pub p99: Duration, pub p999: Duration, pub max: Duration, pub count: u64 }
pub struct Side {
    pub by_op: BTreeMap<OpKind, LatencyStats>,
    pub throughput: f64,          // successful ops/sec
    pub total_ops: u64,
    pub errors: u64,
    pub violations: Vec<Violation>,
}
pub struct LoadReport {
    pub label: String,
    pub wall: Duration,
    pub connections: usize,
    pub a: Side,
    pub b: Option<Side>,          // Some(..) only for a differential run
    pub parity_diffs: Vec<ParityRecord>,
}
impl LoadReport {
    pub fn print(&self);
    pub fn assert_correct(&self) -> Result<(), CorrectnessError>;    // every side: zero violations, zero errors
    pub fn assert_parity(&self) -> Result<(), ParityError>;          // A == B; errors on a single-endpoint run
    pub fn assert_relative(&self, rel: Rel) -> Result<(), RelError>; // A/B ratio; ditto
}
pub struct Rel { pub p99_ratio_max: f64, pub throughput_ratio_min: f64 }
```

### Example — stress / correctness under load

```rust
#[ztest::qos::testnet] // heavy footprint tier; honest reserve so admission doesn't overcommit
#[tokio::test]
async fn zaino_block_range_stays_consistent_under_load() -> Result<()> {
    let mut t = TestEnv::builder();
    let _zeb = t.add_validator(Validator::zebrad("1.9.1"));
    let zai  = t.add_indexer(Indexer::zaino("0.4.0").peer("zeb"));
    t.build().await?;
    zai.wait_for_block_num(H).await?;

    let report = LoadDriver::new(zai.grpc_client().await?)
        .connections(256)
        .spawn_stagger(Duration::from_millis(1))
        .scenario(Scenario::BlockRangeSweep { pool: LO..HI, blocks: 1000, dist: Distribution::Even })
        .duration(Duration::from_secs(30))
        .run()
        .await?;

    report.print();            // absolute histograms → engine reporter, non-gating
    report.assert_correct()?;  // zero violations, zero errors  (gate)
    Ok(())
}
```

### Example — differential parity + relative perf (A vs B on one validator)

```rust
#[ztest::qos::testnet]
#[tokio::test]
async fn fetch_and_state_backends_agree_under_load() -> Result<()> {
    let mut t = TestEnv::builder();
    let vol = t.shared_volume("zebra-db");
    let zebra = t.add_validator(Validator::zebrad("1.9.1").regtest().mount(&vol));

    // ZainoTuning::State reads the shared zebra-state DB, mounted with the same `vol`.
    let a = t.add_indexer(Indexer::zaino("0.4.0").regtest().tuning(ZainoTuning::Fetch).peer("zeb"));
    let b = t.add_indexer(Indexer::zaino("0.4.0").regtest().tuning(ZainoTuning::State).mount(&vol).peer("zeb"));
    t.build().await?;
    a.wait_for_block_num(H).await?;
    b.wait_for_block_num(H).await?;

    let report = LoadDriver::pair(a.grpc_client().await?, b.grpc_client().await?)
        .connections(256)
        .scenario(Scenario::BlockRangeSweep { pool: LO..HI, blocks: 1000, dist: Distribution::Even })
        .duration(Duration::from_secs(30))
        .run() // each request issued to A and B in the same task
        .await?;

    report.assert_parity()?;   // A ≡ B, byte-identical  (gate)
    report.assert_correct()?;  // oracle clean on BOTH endpoints  (gate)
    report.assert_relative(Rel { p99_ratio_max: 1.3, throughput_ratio_min: 0.8 })?; // gate
    report.print();            // absolute numbers for both, non-gating
    Ok(())
}
```

## Build order

1. **`LwdClient` + `grpc_client()`** — the reusable multiplexed client. Small,
   unblocks everything, useful beyond load tests. *(done)*
1. **`BlockOracle` + `LoadDriver` + hdrhistogram `LoadReport`** — correctness
   under load and absolute numbers (goals: correctness, exploratory). *(done)*
1. **`LoadDriver::pair` + `assert_parity` / `assert_relative`** — parity and
   relative perf gating on the shared backbone. *(done)*
1. **Re-org under load** (`stable_below`) — the only check that sees settled
   history being rewritten. Needs the `invalidateblock` / `reconsiderblock`
   validator primitives. *(done)*

Not planned: a `Conformance` RPC sweep. It was in the original design as the
third `zaino-admin` mode, but the ~150 existing live tests already cover the RPC
surface individually and assert response *content*, where the sweep only checked
that a call returned.

Open, in rough priority order:

1. **Verify the lightwalletd differential on a cluster.** It is the only test
   here with a genuinely independent oracle, and it has never run.
1. **Write load concurrent with read load** — every scenario today drives a
   quiesced chain except the re-org test.
1. **Widen the scenario beyond `GetBlockRange`** — the `CompactTxStreamer`
   surface is ~20 RPCs; load covers one.
1. **Reads spanning the fork point during a re-org** — the current test reads
   strictly below it.

## Dependencies

- `hdrhistogram` for percentile aggregation (battle-tested; avoids hand-rolled
  bucketing).
- No new engine surface, no external-endpoint client surface (differential
  target is two backends in one `TestEnv`, per the ≤2-indexer topology cap).
