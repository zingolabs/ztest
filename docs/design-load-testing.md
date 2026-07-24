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
- **L2 — `LoadDriver` / `DiffLoadDriver`**: fan out to N connections, record
  per-op latency into an hdrhistogram (true p50/p90/p99, fixing `zaino-admin`'s
  min/mean/max-only reporting). The differential driver issues each request to
  A *and* B in the same task.
- **L3 — `Oracle`**: validates responses (this is what makes it a *test*, not a
  benchmark). `ChainLinkOracle` (genesis zeros, 32-byte hashes,
  `prev_hash == prior.hash`) + a built-in differential compact-block diff.
- **L4 — result**: `LoadReport` with per-op `LatencyStats`, throughput, error
  and oracle-violation counts; assertion helpers gate the test.

The three `zaino-admin` modes collapse onto shared substrate: **stress** =
`LoadDriver` + `ChainLinkOracle`; **differential** = two `add_indexer` handles
(the ≤2-indexer cap fits exactly) + `DiffLoadDriver`; **conformance** = a
`Conformance` sweep over every RPC with an explicit per-RPC oracle strength.

## API

```rust
// L0 — on IndexerBackend
async fn grpc_client(&self)  -> Result<LwdClient, EnvError>; // shared multiplexed channel, cheap Clone
async fn grpc_channel(&self) -> Result<Channel, EnvError>;   // raw, for per-connection dialing

#[derive(Clone)]
pub struct LwdClient { /* CompactTxStreamerClient<Channel> */ }

// L1 — what a connection does
pub enum Distribution { Even, Random }
pub enum Scenario {
    BlockRangeSweep { pool: Range<u64>, blocks: usize, dist: Distribution },
    Mixed(Vec<(u32 /*weight*/, Op)>),
}

// L2 — how load is applied
pub enum ConnMode { Shared, PerTask } // one channel for all conns, or one per conn (zaino-admin style)
pub enum Until { Duration(Duration), Count(u64) }

// L3 — correctness under load
pub trait Oracle: Send + Sync {
    fn observe(&self, obs: &Observed) -> Result<(), Violation>;
}
pub struct ChainLinkOracle; // genesis zeros, 32-byte hashes, prev_hash == prior.hash

// L4 — result
pub struct LatencyStats { pub p50: Duration, pub p90: Duration, pub p99: Duration, pub max: Duration, pub count: u64 }
pub struct LoadReport {
    pub by_op: BTreeMap<OpKind, LatencyStats>,
    pub throughput: f64,          // successful ops/sec
    pub errors: u64,
    pub violations: u64,
}
impl LoadReport {
    pub fn print(&self);
    pub fn assert_slo(&self, slo: Slo) -> Result<(), SloError>;        // absolute
    pub fn assert_parity(&self) -> Result<(), ParityError>;           // A == B (DiffLoadDriver)
    pub fn assert_relative(&self, rel: Rel) -> Result<(), RelError>;  // A/B ratio
}
pub struct Slo { pub max_p99: Duration, pub min_throughput: f64, pub max_error_rate: f64, pub zero_violations: bool }
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
        .conn_mode(ConnMode::PerTask)
        .spawn_stagger(Duration::from_millis(1))
        .scenario(Scenario::BlockRangeSweep { pool: LO..HI, blocks: 1000, dist: Distribution::Even })
        .oracle(ChainLinkOracle)
        .until(Until::Duration(Duration::from_secs(30)))
        .run()
        .await?;

    report.print(); // absolute histograms → captured by the engine reporter, non-gating
    report.assert_slo(Slo {
        max_p99: Duration::from_millis(200),
        min_throughput: 100_000.0,
        max_error_rate: 0.0,
        zero_violations: true,
    })?;
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

    let report = DiffLoadDriver::pair(a.grpc_client().await?, b.grpc_client().await?)
        .connections(256)
        .scenario(Scenario::BlockRangeSweep { pool: LO..HI, blocks: 1000, dist: Distribution::Even })
        .oracle(ChainLinkOracle) // correctness enforced on BOTH endpoints
        .until(Until::Duration(Duration::from_secs(30)))
        .run() // each request issued to A and B in the same task
        .await?;

    report.assert_parity()?; // A ≡ B, field + per-tx-count level  (gate)
    report.assert_relative(Rel { p99_ratio_max: 1.3, throughput_ratio_min: 0.8 })?; // gate
    report.print();          // absolute numbers for both, non-gating
    Ok(())
}
```

### Example — conformance sweep (every RPC)

```rust
#[ztest::qos::basic]
#[tokio::test]
async fn zaino_grpc_surface_is_wired() -> Result<()> {
    let mut t = TestEnv::builder();
    let _zeb = t.add_validator(Validator::zebrad("1.9.1"));
    let zai  = t.add_indexer(Indexer::zaino("0.4.0").peer("zeb"));
    t.build().await?;

    Conformance::new(zai.grpc_client().await?)
        .discover_from_chain()               // tip + a sample txid from the live chain
        .strength(OracleStrength::ContentValid) // stricter than zaino-admin's "Ok-or-Err passes"
        .run()
        .await?
        .assert_all_ok()?;
    Ok(())
}
```

## Build order

1. **`LwdClient` + `grpc_client()`** — the reusable multiplexed client. Small,
   unblocks everything, useful beyond load tests.
1. **`ChainLinkOracle` + `LoadDriver` + hdrhistogram `LoadReport`** — correctness
   under load and absolute numbers (goals: correctness, exploratory).
1. **`DiffLoadDriver` + `assert_parity` / `assert_relative`** — parity and
   relative perf gating on the shared backbone.
1. **`Conformance` sweep** — the wiring/liveness net over every RPC.

## Dependencies

- `hdrhistogram` for percentile aggregation (battle-tested; avoids hand-rolled
  bucketing).
- No new engine surface, no external-endpoint client surface (differential
  target is two backends in one `TestEnv`, per the ≤2-indexer topology cap).
