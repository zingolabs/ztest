# QoS: tiers, capacity, scheduling, cross-run ledger, calibration

How `ztest run` prices, admits, and schedules tests: author declares a tier at the call site → pod
resources + a 4-D capacity plan + a greedy scheduler + a cross-run reservation ledger + (long runs)
calibration.

- Execution end-to-end via the engine ([design-execution-engine.md](design-execution-engine.md))
- `qos::scheduler::Scheduler` = intra-run admission authority; the k8s-Lease ledger coordinates
  concurrent runs
- Orchestration mandatory: `TestEnv::build()` refuses outside `ztest run`
  (`cluster::require_orchestrator`, keyed on `ZTEST_ENGINE`) → a bare `cargo test` errors instead of
  creating unbudgeted pods

## Per-pod defaults

Sizing lives with the component, not with the tier. Each backend renders its own reserve
(`qos::pod`), `.resources(cpu, mem)` overrides it per pod, and the tier only bounds their sum.

| Pod                        | Reserve    | Set by                       |
| -------------------------- | ---------- | ---------------------------- |
| validator (zebrad, zcashd) | 1c / 2 GiB | `qos::pod::VALIDATOR`        |
| indexer (zainod, lwd)      | 1c / 2 GiB | `qos::pod::INDEXER`          |
| runner                     | tier       | `QosProfile::runner`         |

- Memory-led: 2 GiB clears regtest Orchard proving and the NU6 boundary work
- Whole cores, `requests == limits` → Guaranteed, and eligible for CPU Manager `static`
  (exclusive pinned CPUs); a fractional reserve drops the container into the shared pool
- One pinned core per pod is the floor, so cluster cores ÷ (pods + 1) caps run concurrency

## Tier ladder

```rust
#[ztest::qos::integration] #[ztest::qos::wallet]
#[ztest::qos::testnet]     #[ztest::qos::sync(footprint = "…")]
```

| Tier          | Hard cap | Ceiling     | Runner     | Admitted    | Scheduling                      |
| ------------- | -------- | ----------- | ---------- | ----------- | ------------------------------- |
| `integration` | 10 min   | 2c / 4 GiB  | 1c / 1 GiB | 3c / 5 GiB  | general pool                    |
| `wallet`      | 10 min   | 2c / 4 GiB  | 2c / 2 GiB | 4c / 6 GiB  | general pool                    |
| `testnet`     | 6 h      | 8c / 10 GiB | 1c / 1 GiB | 9c / 11 GiB | general pool                    |
| `sync`        | 48 h     | *declared*  | 1c / 1 GiB | *declared*  | NVMe node-selector + toleration |

- *Ceiling* = `QosProfile::footprint`, the bound on Σ component-pod reserves — the only
  overridable column. It does **not** size pods; `2c/4Gi` is the two-pod validator+indexer
  shape at the defaults above
- *Admitted* = `ceiling + runner`, what admission, the lease and the namespace quota all charge
- `wallet` differs from `integration` in the runner alone (the in-process wallet lives there);
  every other runner only orchestrates
- Caps + reserves locked in the `QosClass::profile` const table; priority ascends with tier
  order, and the default tier sits lowest so a flood of ordinary tests cannot starve the
  rare heavy ones
- `sync` off the general pool → nodeSelector + toleration (`qos::NVME_*`, label
  `ztest.io/pool=nvme`); NVMe node count sizes the sync concurrency ceiling
- Un-annotated → `integration`. A test smaller than the default two-pod shape declares
  `footprint = "1c/2Gi"` rather than reaching for a lighter tier
- Cap enforced by nextest `slow-timeout` (`period = hard_cap, terminate-after = 2`): SLOW at 1×,
  hard-kill at 2×. Teardown after a timeout-kill falls to the janitor/ttl backstop

### `sync` declares no default

`sync` is a routing marker before it is a tier: `plan::drop_sync_tests` subtracts the whole tier
from every `ztest run` selection, so the annotation exists to keep those tests *out* of the engine
and to make them discoverable by `ztest sync`. Its topologies share nothing to size from — a
validator serving a frozen snapshot beside an indexer building one — so the table reserves
`Resources::ZERO` and `footprint = ".."` is mandatory.

- Enforced at compile time by `qos_attr` / `sync_test` (a missing footprint is a build error)
- Backstopped at runtime in `QosClass::profile_with`, which refuses to hand back a zero ceiling
- The admission floor (`ledger::min_viable`, the `ztest status` verdict) reads
  `QosClass::default_footprint` — the default tier's ceiling — so a zero-reserve tier never
  drags it to nothing

## Per-test footprint override

Tier ceiling = a default, not an allotment. A topology that doesn't fit declares its own:

```rust
#[ztest::qos::integration(footprint = "3c/6Gi")]   // e.g. a 3-pod state/fetch comparison
#[ztest::qos::sync(footprint = "15c/29Gi")]        // required: sync has no default
#[ztest::sync_test(name = "…", subject = indexer, qos = sync, footprint = "15c/29Gi")]
```

- Replaces the **component** half only — `runner`, `pool`, `priority`, `hard_cap` still come from the
  tier (a test that could raise its own priority or cap would starve its peers)
- Raises the ceiling, never the pods: a third pod at `qos::pod`'s defaults needs a third core of
  headroom, and `DeployBudget` names the whole topology when the sum does not fit
- Grammar `"<cpu>/<mem>"` (`ztest_attr::footprint`), shared by proc-macro, CLI source scan, `qos::units`
- Units mandatory on both halves, CPU whole cores: a bare `29` = a 29-**byte** reserve, and a fractional
  core renders (rounded up) as a pod larger than the reserve it was admitted against
- No I/O dimensions — those reserves are `0` pending calibration and nothing charges them; no syntax for
  a promise no accounting collects

Lowering = `QosClass::profile_with(Option<Resources>)` → effective `QosProfile`, the single point the
override takes effect:

| Consumer                              | Reads        |
| ------------------------------------- | ------------ |
| namespace `ResourceQuota`             | `footprint`  |
| `DeployBudget` ceiling                | `footprint`  |
| ledger reservation, scheduler request | `admitted()` |

- Pods never sized from one number and admitted against another; `DeployBudget` still refuses a topology
  whose pods sum past the declared footprint
- Over-declaring *holds* the difference for the run's life — the number is a promise to the rest of the
  cluster, so keep it close to what the pods request
- In-process it rides beside the tier through `qos::__enter`, read back as `qos::current_profile()`;
  out-of-process it travels pre-parsed in the link-time inventory (`FootprintDecl`), so no reader
  re-parses a quantity string
- `ztest sync` resolves tier + override from `SyncTestEntry::profile`, never assuming `sync` from the
  subcommand, and refuses a reserve larger than cluster `allocatable` up front rather than polling the
  ledger to a timeout

## The attribute macro — dual emission

Outer attribute on the test; re-emits the item intact (inner `#[tokio::test]` included) + two bridges,
mirroring `dev!` → inventory → image:

```rust
#[ztest::qos::sync(footprint = "15c/29Gi")]
#[tokio::test(flavor = "multi_thread")]
async fn syncs_from_genesis() { /* body */ }

// expands to:
::ztest::__private::inventory::submit! {
    ::ztest::qos::QosDecl {
        test_id: concat!(module_path!(), "::", stringify!(syncs_from_genesis)),
        class: ::ztest::qos::QosClass::Sync,
        footprint: Some(::ztest::qos::Resources::new(15_000, 31_138_512_896, 0, 0)),
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn syncs_from_genesis() {
    ::ztest::qos::__enter(::ztest::qos::QosClass::Sync, /* footprint */);  // task-local
    /* body */
}
```

- **inventory submit** (out-of-process): dumped by the `ZTEST_DUMP_INVENTORY` ctor → `ztest run` groups
  by tier and builds the capacity plan. `QosDecl` (submit, `&'static`) / `QosEntry` (owned read) flow
  through `src/inventory.rs` beside `DevImageDecl` / `TestDepDecl` / `SeedDecl`
- **task-local enter** (in-process): `TestEnv::build()` reads the tier for requests/limits/scheduling
- Macros in `ztest_macros` (`qos_attr()`); `qos` re-exports the four plus `#[ztest::calibrated]`

## 4-D capacity model

`qos::Resources` prices four dimensions, matching the cgroup v2 `io.max` units the harness enforces:

| Dimension       | Field       | Unit       | Source                                      |
| --------------- | ----------- | ---------- | ------------------------------------------- |
| CPU             | `cpu_milli` | millicores | pod `requests`/`limits`                     |
| Memory          | `mem_bytes` | bytes      | pod `requests`/`limits`                     |
| Disk bandwidth  | `disk_bps`    | bytes/sec  | PVC annotation / VAC → `io.max` `{r,w}bps`  |
| Disk operations | `disk_iops`   | ops/sec    | PVC annotation / VAC → `io.max` `{r,w}iops` |

- "Fits" = fits in **every** dimension; the machinery (`decide`, `Scheduler`, ledger) is
  dimension-agnostic, all through `Resources::{fits_within, checked_add, saturating_sub, …}`
- Per-pool capacity = `allocatable − Σ reserved`, where a scheduled pod reserves the **per-dimension max
  of its effective request and its observed usage** (`qos::units::pod_effective_request`, `max`'d with
  live usage by the probe) — the request keeps admission scheduler-safe, the usage term catches a
  Burstable co-tenant running over its request (notably build pods)
- `pipeline/cluster.rs::cluster_reserved` sums that cluster-wide

### I/O inert until calibrated

k8s exposes no I/O `allocatable` and a test's I/O demand is unknown a priori → both the node ceiling and
the per-tier reserve start unset, so an uncalibrated cluster behaves byte-for-byte like CPU×memory:

- Node ceiling from annotations (below); absent → `u64::MAX` (`Resources::cpu_mem_unbounded_rest`)
- Per-tier reserve `0` until calibration fills it, guarded by
  `qos::tests::every_tier_reserves_zero_io_pending_calibration`

### The I/O cap lives on the PVC, not the pod

No disk-I/O field in pod `resources` (KEP-3008 never reached alpha), no I/O `ResourceQuota` dimension.
I/O is a property of the volume → cap on the **PVC**, namespace budget = Σ its pods' volume caps. Native
vehicle is a `VolumeAttributesClass`, honored by neither backend today (topolvm/LVMS has none; ceph-csi
RBD's krbd `io.max` path is `devel`-only, targeting v3.18 + k8s 1.34). So:

- **Declared** on the PVC via `qos::ANNOTATION_DISK_BPS` / `ANNOTATION_DISK_IOPS` — backend-uniform carrier,
  swaps to a VAC on Ceph once ceph-csi ≥ v3.18
- **Enforced** with cgroup v2 `io.max` on the pod cgroup via **CRI-O `blockio` classes** (works on a
  topolvm `/dev/dm-N` and a krbd `/dev/rbdN` alike). Node config (MachineConfig): cgroup v2,
  `DefaultIOAccounting=yes`, `blockio_config_file`; pods opt in with
  `blockio.resources.beta.kubernetes.io/pod: <class>`
- **Accounted** by the probe: pod → mounted PVC(s) → `units::pvc_io_reservation`, summed like CPU/memory.
  RWO storage → one PVC binds one pod, no double counting

### Node I/O ceiling — fio → annotation

`ztest cluster setup` runs one fio job per node, writing `ztest.io/io-bps` (aggregate sequential
bandwidth) and `ztest.io/io-iops` (random-4k ceiling).

- Governing benchmark = large-block sequential bandwidth under simultaneous read+write contention
  (two-section fio: `rw=read` chain-reader + `rw=write` compiler-writer, `bs=1M direct=1`) — a single
  4k-random IOPS number does not model sequential streams
- `pipeline/cluster.rs::node_allocatable` reads them; absent → `u64::MAX`

## Scheduler

Greedy **priority admission with backfill**: each pass admits the highest-priority queued request that
fits its pool's live 4-D capacity, lower-priority requests backfill the remainder, a lease release
triggers a fresh pass.

- A request exceeding even the empty-pool capacity is **rejected** (unschedulable), never queued
- Each request acquires its whole 4-D footprint atomically → no hold-and-wait, no deadlock. A test never
  escalates while holding (tier fixes the need up front) and tests are mutually independent
- `sync`: `build()` fails fast when no NVMe node is schedulable, rather than leaving the pod Pending on
  an unsatisfiable selector
- Preflight (`ui/render.rs` + a `qos::schedule` planning pass) fills the `tier`/`queue`/`reservation`
  banner rows — group by tier, compute peak concurrent namespaces and wave structure against probed
  capacity, warn when a tier's footprint exceeds its pool. Live lease state updates `reservation`

## Guaranteed-QoS pods

Every component pod renders `requests == limits` (`manifest::PodSpec`) = kubelet "Guaranteed". The
amount comes from the pod, never from arithmetic on the tier: the backend's `qos::pod` default,
replaced by an explicit `.resources()`.

- CPU per pod is **whole cores** (≥1) for CPU Manager `static` eligibility (exclusive pinned CPUs);
  a fractional core drops the container into the shared pool
- `DeployBudget` charges each spec as it is built and refuses the topology whose *sum* tops the tier
  ceiling — per-pod checks miss it, since 9c + 9c each fit a 15c ceiling that their sum does not
- Killed, never migrated: bare `Pod`s, `restartPolicy: Never`, and the auto-added
  `node.kubernetes.io/{not-ready,unreachable}` tolerations overridden to `tolerationSeconds: 0` so a lost
  node deletes immediately
- Per-test namespace also gets a requests- and pod-count-scoped `ResourceQuota` sized to the tier
  ceiling (`cluster::apply_resource_quota`) as an API-server backstop

## Cross-run reservation ledger

The in-memory scheduler cannot coordinate *concurrent* runs — two runs (or the `builder` compile pod
overlapping the `buildkit` grow inside one run) read the same free capacity, both reserve it, kubelet
`ResizeDeferred`s the grow, build pod OOMKills at 137. Three layers:

1. **Ledger (cross-run, k8s)** — one `coordination.k8s.io/Lease` per live run in `ztest-meta`.
   `holderIdentity = <run-id>`; reservation in annotations `ztest.io/reserve-cpu-milli` /
   `ztest.io/reserve-mem-bytes`; TTL via `leaseDurationSeconds` + heartbeat, so a crashed run's lease
   expires and is swept alongside the `LABEL_RUN_ID` teardown reap
1. **Per-SA budget (policy)** — max one SA may reserve, from `ztest.io/budget-cpu-milli` /
   `ztest.io/budget-mem-bytes` on the SA named by `ZTEST_SA`. Budget travels with the identity;
   unannotated SA falls back to a conservative default. Needs `get` on its own ServiceAccount.
   Enforcement seam kept: `set_sa_budget` / `RejectReason::ExceedsSaBudget`
1. **In-memory `Scheduler` (intra-run)** — `available` ceiling is the reserved slice, not a raw snapshot

### Acquire (run start, before scheduling and any build-pod grow)

```
1. sweep: delete expired Leases in ztest-meta (reap crashed runs).
2. read: Σ(other live Leases' reservations)      = reserved_by_others
3. read: cluster allocatable                       = capacity
         actual ztest usage (pods by run-id label) = committed_actual
4. budget = sa_budget(ZTEST_SA)
   slice  = min(budget, capacity − reserved_by_others)
5. if slice < MIN_VIABLE (can't fit the heaviest build/tier footprint):
       WAIT — poll from step 1 until other runs release (bounded timeout).
       This is contention, not an error.
6. write my Lease reserving `slice`; start the renewal heartbeat.
7. seed Scheduler.available = slice.
```

Invariant — `committed_actual (Σ real ztest pods, by run-id) ≤ Σ(all ledger reservations)`:

- ztest must never run more than it reserved; a violation = internal accounting bug (lease released while
  its pods ran, or a pod created without a reservation) → `assert!` + fail
- Counting only ztest-labeled pods keeps foreign workloads from tripping it
- `Σ(reservations) > capacity` is **not** a panic — a cluster can legitimately shrink; log and wait

### Build-pod in-place grow

```
grow_to(pod, container, cpu, mem):
  ensure the footprint fits this run's reserved slice
  patch the `resize` subresource
  poll pod status until:
    - actuated  (containerStatuses[].resources.limits reach target)   → Ok
    - Infeasible (PodResizePending, reason=Infeasible)                 → Err (node too small)
    - Deferred   (PodResizePending, reason=Deferred)                   → keep waiting
    - timeout                                                          → Err
  build; then shrink back (best-effort, no wait).
```

- `BUILDKIT_BUILD` reserves 32c / 24 GiB (node has 72c; 24 GiB clears the compile peak)
- `ztest-meta`, the run SA's `Lease` CRUD, and its `nodes`/`pods` read RBAC come from the resource graph
  (`resource::impls::policy`) at `ztest cluster setup`

## Calibration & metrics

`#[ztest::calibrated]` marks a long-running test (chiefly `sync`) for three capabilities: measure the
delivered per-dimension performance a runner pod actually gets, persist + normalize per-run runtimes,
capture a space-efficient flamegraph + time-series trace. Same mold as the tier macros (`qos_attr()`):
`inventory::submit!(CalibrationDecl { test_id })` out-of-process, task-local run/test id in-process.

### Calibration probe

Runs as an **init container** before the test binary, under the same cgroup limits → it reads the
**reserved slice**, the guaranteed capability, not raw hardware. `pod_effective_requests` already counts
init containers, so the packer prices it unchanged.

| Dimension        | Field      | Unit         | Tool                                                      |
| ---------------- | ---------- | ------------ | --------------------------------------------------------- |
| CPU              | `cpu_bogo` | bogo-ops/sec | `stress-ng --cpu --cpu-method matrixprod --metrics-brief` |
| Memory bandwidth | `mem_bw`   | bytes/sec    | `stress-ng --stream` (STREAM Triad port)                  |
| Disk bandwidth   | `io_bps`   | bytes/sec    | `fio` seq r+w, `bs=1M direct=1`                           |
| Disk operations  | `io_iops`  | ops/sec      | `fio` rand 4k                                             |

- Bogo-ops are not stable across stress-ng versions or `--cpu-method`s → the vector records
  `stressng_version` + `cpu_method` inline (method pinned to `matrixprod`, never `all`), so a version
  bump makes historical CPU numbers visibly incomparable instead of silently corrupting a trend
- fio (not `stress-ng --hdd`) keeps per-pod and per-node disk figures on one basis

### Normalization

Sync runtime = a mixture of resource-bound phases:

```
T_normalized = Σ_r  t_r · (C_run,r / C_ref,r)
```

`t_r` = time bound by resource `r`, `C_run,r` = this pod's calibrated capability, `C_ref,r` = fixed
reference. Higher delivered capability ⇒ less time here ⇒ scale up to reference terms.

- **v1 — dominant dimension**: chain sync is I/O-bound on sequential bandwidth →
  `T_norm ≈ T_obs · (io_bps_run / io_bps_ref)`
- **v2 — 2-term roofline**: split on-CPU time (flamegraph samples) from I/O-wait (off-CPU / `io.stat`),
  normalizing each; needs off-CPU attribution from RPC spans
- Record keeps raw duration, normalized duration **and** the vector → re-normalizing under a new
  reference or model is a pure recompute from stored data

### Persistence

Two tiers on the RWO Ceph PVC (single-writer, no contention):

- **Cross-run scalar index → SQLite** (`rusqlite`, WAL), one row per `(run, test)`: raw + normalized
  duration, calibration vector, verdict, content hash of the trace blob. The regression-trend surface
- **Per-run trace → one Perfetto protobuf blob**, whole-blob zstd, content-addressed via `src/storage/`
  (`StorageBackend`), referenced by hash from the row
- One Perfetto trace carries all three artifacts on a shared timeline — time-series as counter tracks,
  flamegraph as callstack samples (Perfetto ingests Linux `perf`/pprof), spans as slices — viewable and
  SQL-queryable through TraceProcessor, no server

```sql
CREATE TABLE runs (
  run_id TEXT PRIMARY KEY, git_sha TEXT, started_utc TEXT, cluster TEXT
);
CREATE TABLE test_metrics (
  run_id TEXT, test_id TEXT, qos_class TEXT, verdict TEXT,
  raw_duration_ms INTEGER, normalized_duration_ms INTEGER,
  cpu_bogo REAL, mem_bw INTEGER, io_bps INTEGER, io_iops INTEGER,
  stressng_version TEXT, cpu_method TEXT,
  trace_hash TEXT,
  PRIMARY KEY (run_id, test_id),
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
```

### Capture

Sync wall-time lives in the zebrad + zaino pods, not the ztest driver → capture targets the component
processes. Injected in `PodSpec::render` for tests carrying `CalibrationDecl`, beside the
nodeSelector/toleration/blockio injection.

- **Flamegraph**: `perf record -F 99 -g -p <pid>` inside the validator/indexer pods, ingested natively by
  Perfetto. Needs `CAP_PERFMON`/privileged — acceptable at the `sync` tier (already privileged, dedicated
  NVMe pool)
- **Time-series**: periodic height / blocks-per-sec / rss + cgroup `io.stat` deltas as counter tracks;
  `io.stat` doubles as a cross-check that delivered throughput matches the calibrated slice
- **Spans (optional)**: client-side RPC spans from the driver at the RPC boundary (no zebrad/zaino
  change) → cross-service latency, wait attribution, and the off-CPU time v2 needs
- Measured per-tier `io_bps`/`io_iops` feed back into `QosClass::profile`, replacing the `0` placeholders
  that keep the I/O dimension inert
