# QoS: tiers, capacity, scheduling, cross-run ledger, calibration

How `ztest run` prices, admits, and schedules tests: authors declare a QoS tier
at the call site; the harness lowers that into pod resources, a 4-D capacity
plan, a greedy scheduler, a cross-run reservation ledger, and (for long runs) a
calibration + metrics pipeline.

`ztest run` owns execution end-to-end via the engine (see
[design-execution-engine.md](design-execution-engine.md)). The in-memory
`qos::scheduler::Scheduler` is the intra-run admission authority; the k8s-Lease
ledger (below) coordinates concurrent runs. Orchestration is mandatory:
`TestEnv::build()` refuses to run outside `ztest run` (`cluster::require_orchestrator`,
keyed on `ZTEST_ENGINE`), so a bare `cargo test` gets a clear error instead of
creating unbudgeted pods.

## Tier ladder

Authors annotate a test with one of five attribute macros (snake_case path):

```rust
#[ztest::qos::basic]   #[ztest::qos::wallet]   #[ztest::qos::integration]
#[ztest::qos::testnet] #[ztest::qos::sync]
```

| Tier          | Hard cap | Components   | Runner      | Admitted     | Scheduling                      |
|---------------|----------|--------------|-------------|--------------|---------------------------------|
| `basic`       | 60 s     | 1c / 512 MiB | 1c / 512MiB | 2c / 1 GiB   | general pool                    |
| `wallet`      | 10 min   | 4c / 2 GiB   | 4c / 1 GiB  | 8c / 3 GiB   | general pool                    |
| `integration` | 10 min   | 3c / 3 GiB   | 1c / 1 GiB  | 4c / 4 GiB   | general pool                    |
| `testnet`     | 6 h      | 8c / 10 GiB  | 1c / 1 GiB  | 9c / 11 GiB  | general pool                    |
| `sync`        | 48 h     | 15c / 15 GiB | 1c / 1 GiB  | 16c / 16 GiB | NVMe node-selector + toleration |

*Components* = `QosProfile::footprint`, the component-pod aggregate and the
only overridable column. *Admitted* = `footprint + runner` — what admission,
the lease and the namespace quota all charge. `wallet`'s runner is heavy
because the in-process wallet runs there; every other tier's runner only
orchestrates.

- Caps (timeouts) and per-tier CPU/RAM reserves are locked in the
  `QosClass::profile` const table. Priority ascends with tier order; `wallet`
  sits between `basic` and `integration`.
- **Reserve** is the per-namespace aggregate budget the topology may consume:
  both the scheduling reservation and the default ceiling for pod `limits`.
  Per-component `.resources(cpu, mem)` overrides individual pods.
- `sync` is off the general pool: it targets dedicated NVMe nodes via
  nodeSelector + toleration (`qos::NVME_*`, placeholder label `ztest.io/pool=nvme`).
  The NVMe node count sizes the sync concurrency ceiling via the probe.
- Un-annotated tests default to `basic`.
- The hard cap is enforced by nextest's `slow-timeout` (`period = hard_cap,
  terminate-after = 2`): flagged SLOW at one cap, hard-killed at 2×. Teardown on
  a timeout-kill relies on the janitor/ttl backstop.

## Per-test footprint override

A tier's reserve is a default, not a fixed allotment. A test whose topology does
not fit its tier's component reserve declares its own:

```rust
#[ztest::qos::sync(footprint = "15c/29Gi")]

#[ztest::sync_test(name = "…", subject = indexer, qos = sync, footprint = "15c/29Gi")]
```

- Replaces `QosProfile::footprint` — the **component** half — and nothing else.
  `runner`, `pool`, `priority` and `hard_cap` still come from the tier: a test
  that could raise its own priority or cap would starve its peers.
- Grammar `"<cpu>/<mem>"`, parsed by `ztest_attr::footprint`, shared by the
  proc-macro, the CLI's pre-compile source scan and `qos::units`. Units are
  mandatory on both halves and CPU must be whole cores — a bare `29` would
  otherwise become a 29-byte reserve, and a fractional core renders (rounded up)
  as a pod larger than the reserve it was admitted against.
- No I/O dimensions: those reserves are `0` pending calibration and nothing
  charges them, so the grammar has no syntax for a promise no accounting collects.

Lowering is `QosClass::profile_with(Option<Resources>)` → an *effective*
`QosProfile`, and that is the single point the override takes effect. Everything
downstream reads `profile.footprint` / `profile.admitted()`:

| Consumer                              | Reads                    |
|---------------------------------------|--------------------------|
| namespace `ResourceQuota`             | `footprint`              |
| default per-pod share (`share(n)`)     | `footprint`              |
| `DeployBudget` ceiling                | `footprint`              |
| ledger reservation, scheduler request | `admitted()`             |

The reserve therefore stays honest in both directions: pods are never sized from
one number and admitted against another, and `DeployBudget` still refuses a
topology whose pods sum past the declared footprint. A test that declares more
than it uses *holds* the difference for the life of the run — the amount is a
promise to the rest of the cluster, so it is the author's job to keep it close to
what the pods request.

In-process, the override rides beside the tier through `qos::__enter` and is read
back as `qos::current_profile()`. Out-of-process it travels pre-parsed in the
link-time inventory (`FootprintDecl`), so no reader re-parses a quantity string.

`ztest sync` resolves both tier and override from the profile's own declaration
(`SyncTestEntry::profile`) rather than assuming the `sync` tier from the
subcommand, and refuses a reserve larger than cluster `allocatable` up front
instead of polling the ledger until it times out.

## The attribute macro — dual emission

Each tier macro is the outer attribute on a test. It re-emits the item intact
(including any inner `#[tokio::test]`) and injects two bridges, mirroring the
`dev!` → inventory → image pipeline:

```rust
#[ztest::qos::sync]
#[tokio::test(flavor = "multi_thread")]
async fn syncs_from_genesis() { /* body */ }

// expands to:
::ztest::__private::inventory::submit! {
    ::ztest::qos::QosDecl {
        test_id: concat!(module_path!(), "::", stringify!(syncs_from_genesis)),
        class: ::ztest::qos::QosClass::Sync,
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn syncs_from_genesis() {
    ::ztest::qos::__enter(::ztest::qos::QosClass::Sync);   // task-local
    /* body */
}
```

- **inventory submit** (out-of-process): dumped by the `ZTEST_DUMP_INVENTORY`
  ctor so `ztest run` can group tests by tier and build the capacity plan.
  `QosDecl` (submit side, `&'static` fields) / `QosEntry` (owned read side) flow
  through `src/inventory.rs` alongside `DevImageDecl` / `TestDepDecl` / `SeedDecl`.
- **task-local enter** (in-process): `TestEnv::build()` reads the current tier to
  set pod requests/limits/scheduling.

Macros live in `ztest_macros` (`qos_attr()`); `qos` re-exports the five tier
macros plus `#[ztest::calibrated]` (see below).

## 4-D capacity model

`qos::Resources` prices four dimensions matching the cgroup v2 `io.max` units the
harness enforces:

| Dimension       | Field       | Unit        | Source                                   |
|-----------------|-------------|-------------|------------------------------------------|
| CPU             | `cpu_milli` | millicores  | pod `requests`/`limits`                  |
| Memory          | `mem_bytes` | bytes       | pod `requests`/`limits`                  |
| Disk bandwidth  | `io_bps`    | bytes/sec   | PVC annotation / VAC → `io.max` `{r,w}bps`|
| Disk operations | `io_iops`   | ops/sec     | PVC annotation / VAC → `io.max` `{r,w}iops`|

"Fits" means fits in **every** dimension. The scheduler machinery (`decide`,
`Scheduler`, the ledger) is dimension-agnostic — it operates through
`Resources::{fits_within, checked_add, saturating_sub, …}`.

Per-pool capacity is `allocatable − Σ reserved`, where a scheduled pod's
reservation is the **per-dimension max of its requests and its limits**
(`qos::units::pod_effective_reservation`) — what it can burst to, not merely what
it requests. Counting the burst ceiling keeps admission contention-safe against
Burstable co-tenants, notably the on-cluster build pods. The probe
(`pipeline/cluster.rs::cluster_reserved`) sums this across all pods cluster-wide.

### I/O is inert until calibrated

Kubernetes exposes no I/O `allocatable`, and a test's I/O demand is not known a
priori. Both the node ceiling and the per-tier I/O reserve start unset, so an
uncalibrated cluster behaves byte-for-byte like the CPU×memory model:

- **Node I/O ceiling**: from node annotations (below). Absent → `u64::MAX`
  (`Resources::cpu_mem_unbounded_io`). Unbounded I/O never gates.
- **Per-tier I/O reserve** (`QosClass::profile`): `0` until calibration fills it.
  Guarded by `qos::tests::every_tier_reserves_zero_io_pending_calibration`.

### Where the I/O cap lives — the PVC, not the pod

Kubernetes has no disk-I/O field in pod `resources` (KEP-3008 never reached
alpha) and no I/O `ResourceQuota` dimension. I/O is a property of the volume, so
the cap is declared on the **PVC** and the per-namespace budget is the sum of its
pods' volume caps. The native vehicle is a `VolumeAttributesClass` (VAC,
`storage.k8s.io/v1`), but neither backend honors one today (topolvm/LVMS has no
VAC; ceph-csi RBD's cgroup-`io.max` path for krbd is `devel`-only, targeting
v3.18 + k8s 1.34). So today the cap is:

- **Declared** on the PVC via `qos::ANNOTATION_IO_BPS` / `qos::ANNOTATION_IO_IOPS`
  (a backend-uniform carrier that swaps to a VAC on Ceph once ceph-csi ≥ v3.18).
- **Enforced** with cgroup v2 `io.max` on the pod cgroup via **CRI-O `blockio`
  classes** (works on both a topolvm `/dev/dm-N` and a krbd `/dev/rbdN`). Node
  config (MachineConfig): cgroup v2, `DefaultIOAccounting=yes`, and
  `blockio_config_file`; pods opt in with
  `blockio.resources.beta.kubernetes.io/pod: <class>`.
- **Accounted** by the probe: pod → mounted PVC(s) → the PVC reservation
  (`units::pvc_io_reservation`), summed like CPU/memory. Storage is RWO, so a PVC
  binds to one pod — no double counting.

### Node I/O ceiling — fio benchmark → annotation

`ztest cluster setup` runs one fio job per node and writes:

- `ztest.io/io-bps` — aggregate sequential bandwidth (bytes/sec).
- `ztest.io/io-iops` — random-4k IOPS ceiling (ops/sec).

The governing benchmark is large-block sequential bandwidth under simultaneous
read+write contention (a two-section fio job: `rw=read` chain-reader +
`rw=write` compiler-writer, `bs=1M direct=1`); a single 4k-random IOPS number
does not model sequential streams. `pipeline/cluster.rs::node_allocatable` reads
the annotations; absent → `u64::MAX`.

## Scheduler

Greedy **priority admission with backfill**: each schedule pass admits the
highest-priority queued request that fits the live 4-D capacity for its pool;
lower-priority requests backfill the remainder. A lease release triggers a fresh
pass that backfills the next-fitting requests.

- A request that exceeds even the empty-pool capacity is **rejected**
  (unschedulable), not queued — fail fast.
- Each request acquires its entire 4-D footprint atomically, so there is no
  hold-and-wait and no deadlock. A test never escalates its reservation while
  holding it (the tier fixes the full need up front), and tests are mutually
  independent.
- For `sync`, `build()` fails fast if no NVMe-pool node is schedulable rather
  than leaving the pod Pending on an unsatisfiable selector.

Preflight (`ui/render.rs` + a `qos::schedule` planning pass) fills the
`tier`/`queue`/`reservation` banner rows: group selected tests by tier, compute
peak concurrent namespaces and the wave structure against probed capacity, and
warn if any single tier's footprint exceeds the pool. Live lease state updates
the `reservation` row during the run.

## Guaranteed-QoS pods

When a test does not call `.resources()`, the tier footprint is split **evenly
across the env's pods** (validators + indexers; in-process wallets get none) and
rendered as `requests == limits` (`env::even_share` + `manifest::PodSpec`),
giving each pod the kubelet "Guaranteed" QoS class.

- CPU per pod is rounded **up to whole cores** (≥1) so each container is eligible
  for the kubelet CPU Manager `static` policy (exclusive pinned CPUs); fractional
  CPU would drop it into the shared pool. Memory is the exact even share.
- Because CPU is rounded up, `pods × per-pod` can exceed the raw tier footprint
  (8 cores over 3 pods → 9). Admission reserves the **deployed** footprint
  (`env::deployed_footprint` = per-pod share × pods), so the ledger never
  under-counts.
- Pods are killed, never migrated: bare `Pod`s with `restartPolicy: Never`, and
  the auto-added `node.kubernetes.io/{not-ready,unreachable}` tolerations are
  overridden to `tolerationSeconds: 0` so a lost node deletes the pod immediately.
- An explicit `.resources()` overrides this (requests-only).

Each per-test namespace also gets a requests- and pod-count-scoped
`ResourceQuota` sized to the tier's deployed footprint
(`cluster::apply_resource_quota`) as an API-server backstop.

## Cross-run reservation ledger

The in-memory scheduler cannot coordinate *concurrent* runs — two runs (or the
`builder` compile pod overlapping the `buildkit` grow within one run) both read
the same free capacity, both reserve it, and the kubelet `ResizeDeferred`s the
grow (build pod OOMKilled at 137). The ledger is the cross-run reservation
authority. Three layers:

1. **Ledger (cross-run, k8s):** one `coordination.k8s.io/Lease` per live run in
   the `ztest-meta` namespace. `holderIdentity = <run-id>`; the reservation rides
   in annotations `ztest.io/reserve-cpu-milli` / `ztest.io/reserve-mem-bytes`.
   TTL via `leaseDurationSeconds`, renewed by a heartbeat; a crashed run's lease
   expires and is swept (alongside the `LABEL_RUN_ID` teardown reap).
2. **Per-SA budget (policy):** the maximum a single SA may reserve, from
   annotations `ztest.io/budget-cpu-milli` / `ztest.io/budget-mem-bytes` on the
   SA named by `ZTEST_SA`. Budget travels with the identity; a SA with no
   annotation falls back to a conservative built-in default. Requires the run SA
   to have `get` on its own ServiceAccount object. (The `Scheduler` retains the
   enforcement seam `set_sa_budget` / `RejectReason::ExceedsSaBudget`.)
3. **In-memory `Scheduler` (intra-run):** this run's admission engine; its
   `available` ceiling is the reserved slice, not a raw snapshot.

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

**Invariant:**

```
assert: committed_actual (Σ real ztest pods, by run-id) ≤ Σ(all ledger reservations)
```

ztest must never be running more than it has reserved. A violation is an internal
accounting bug (a lease released while its pods still ran, or a pod created
without a reservation) → `assert!` + fail. Counting only ztest-labeled pods keeps
non-ztest workloads from tripping it. `Σ(reservations) > capacity` is **not** a
panic — the cluster can legitimately shrink; it is logged and the run waits.

### Build-pod in-place grow

Growing the build pod is a checked operation:

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

`BUILDKIT_BUILD` reserves **32c / 24 GiB** (node has 72c; 24 GiB clears the
compile peak).

`ztest-meta`, the run SA's `Lease` CRUD, and its `nodes`/`pods` read RBAC are
provisioned by the resource graph (`resource::impls::policy`) during `ztest cluster setup`.

## Calibration & metrics

`#[ztest::calibrated]` marks a long-running test (chiefly `sync`) for three
capabilities: it measures the delivered per-dimension performance a runner pod
actually gets, persists + normalizes per-run runtimes, and captures a
space-efficient flamegraph + time-series trace. The attribute follows the tier
macro mold (`qos_attr()`): out-of-process `inventory::submit!(CalibrationDecl {
test_id })` and an in-process task-local stamping the run/test id.

### Calibration probe

Runs as an **init container** in the runner pod before the test binary, under the
same cgroup limits the test gets, so it reads the **reserved slice** — the
guaranteed capability, not raw hardware. `pod_effective_requests` already accounts
for init containers, so the packer prices it with no change. `CalibrationVector`:

| Dimension        | Field      | Unit           | Tool                                                     |
|------------------|------------|----------------|---------------------------------------------------------|
| CPU              | `cpu_bogo` | bogo-ops/sec   | `stress-ng --cpu --cpu-method matrixprod --metrics-brief`|
| Memory bandwidth | `mem_bw`   | bytes/sec      | `stress-ng --stream` (STREAM Triad port)                |
| Disk bandwidth   | `io_bps`   | bytes/sec      | `fio` seq r+w, `bs=1M direct=1`                          |
| Disk operations  | `io_iops`  | ops/sec        | `fio` rand 4k                                            |

Bogo-ops are not stable across stress-ng versions or `--cpu-method`s, so the
vector records `stressng_version` and `cpu_method` inline (method pinned to
`matrixprod`, never `all`) — a version bump makes historical CPU numbers visibly
incomparable rather than silently corrupting a trend. fio (not `stress-ng --hdd`)
keeps the per-pod and per-node disk figures on the same basis.

### Normalization

A sync runtime is a mixture of resource-bound phases:

```
T_normalized = Σ_r  t_r · (C_run,r / C_ref,r)
```

`t_r` = time bound by resource `r`; `C_run,r` = this pod's calibrated capability;
`C_ref,r` = a fixed reference. Higher delivered capability ⇒ less time here ⇒
scale up to reference terms.

- **v1 — dominant dimension.** Chain sync is I/O-bound on sequential bandwidth,
  so `T_norm ≈ T_obs · (io_bps_run / io_bps_ref)`.
- **v2 — 2-term roofline.** Split on-CPU time (flamegraph samples) from I/O-wait
  time (off-CPU / `io.stat`), normalizing each; needs the off-CPU attribution
  from RPC spans.

The record always keeps raw duration, normalized duration, **and** the
calibration vector, so a re-normalization (new reference or model) is a pure
recompute from stored data.

### Persistence

Two tiers on the RWO Ceph PVC (single-writer, no contention):

- **Cross-run scalar index → SQLite** (`rusqlite`, WAL). One row per
  `(run, test)`: raw + normalized duration, calibration vector, verdict, and the
  content hash of the run's trace blob. This is the cross-run regression-trend
  surface.
- **Per-run trace → one Perfetto protobuf blob**, whole-blob zstd-compressed and
  stored content-addressed via `src/storage/` (`StorageBackend`), referenced by
  hash from the SQLite row.

A single Perfetto trace carries all three artifacts on a shared timeline —
time-series as **counter tracks**, the flamegraph as **callstack samples**
(Perfetto ingests Linux `perf`/pprof), and spans as **slices** — viewable and
SQL-queryable via TraceProcessor without a server.

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

During a sync test wall-time lives inside the zebrad validator and zaino indexer
(separate pods), not the ztest driver, so capture targets the component
processes. Injection is done in `PodSpec::render` for tests carrying
`CalibrationDecl`, alongside the nodeSelector/toleration/blockio injection.

- **Flamegraph.** `perf record -F 99 -g -p <pid>` inside the validator/indexer
  pods; Perfetto ingests the samples natively. Needs `CAP_PERFMON`/privileged —
  acceptable for the `sync` tier (already privileged, dedicated NVMe pool).
- **Time-series.** Periodic samples of height / blocks-per-sec / rss and cgroup
  `io.stat` deltas as Perfetto counter tracks; the `io.stat` series doubles as a
  cross-check that delivered throughput matches the calibrated slice.
- **Spans (optional).** Client-side RPC spans emitted by the ztest driver at the
  RPC boundary (no zebrad/zaino change) give cross-service latency and wait
  attribution, and are the source of the off-CPU time the v2 roofline needs.

The measured per-tier `io_bps`/`io_iops` feed back into `QosClass::profile`
reserves, replacing the `0` placeholders that keep the I/O dimension inert.
