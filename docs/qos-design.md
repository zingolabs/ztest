# QOS classes — design

Status: **draft / under refinement.** Owner: design in progress with Eli.
This doc is the working reference; it will move faster than the code.

> **Historical (superseded by `engine-design.md`).** §5.2 (the two-layer
> split), §6 (nextest config lowering), and §12 (the "nextest has no live event
> stream / the broker must be the sole live UI source" framing) describe the
> old `cargo nextest`-wrapper architecture. That is **gone**: `ztest run` now
> owns execution end-to-end via the engine (`src/engine/`), the QoS
> `Scheduler` is the single spawn authority (no `--test-threads`, no
> tool-config `qos/lower.rs`), and the run has a native event stream. Read
> those three sections as background only; `engine-design.md` is current.

## 1. Problem & mandate

Tests on this harness range from sub-second pure-logic checks to 48-hour
chain syncs. Today the only resource knob is a single hardcode —
`--test-threads=6` in `cli/run.rs` — chosen because a single-node kind
cluster saturates past ~6 concurrent pod-booting tests. That number is
really "the Integration tier on a small cluster," generalised to every
test regardless of weight.

We want test authors to declare a test's *quality-of-service tier* at the
call site, and have the harness translate that declaration into three
distinct things:

1. **Reservation & limits** — k8s `requests`/`limits` on the test's pods,
   tiered. (README TODO: "All pods should have requests and limits.
   Limits will be tiered based on QOS classes.")
2. **Scheduling** — how much cluster capacity the test reserves, in what
   priority order tests are admitted, and how freed capacity is
   reclaimed and backfilled.
3. **Authorization** — the ServiceAccount / runner identity has a maximum
   tier it is *authorized* to request. (README TODO: "ServiceAccounts
   will have the authorized level of service it can provide.")

The declaration is the **single source of truth**; the harness is the
**compiler** that lowers it to nextest config, pod specs, and broker
admission policy. This mirrors the existing `dev!` → `inventory` → image
pipeline pattern.

## 2. The tier ladder

```
#[ztest::qos::basic]        #[ztest::qos::integration]
#[ztest::qos::testnet]      #[ztest::qos::sync]
```

Attribute path is snake_case (Rust convention for module + macro names).

| Tier          | Hard cap | Reserve CPU | Reserve RAM | Scheduling                           |
|---------------|----------|-------------|-------------|--------------------------------------|
| `basic`       | 60 s     | **TBD**     | **TBD**     | general pool                         |
| `integration` | 10 min   | **TBD**     | **TBD**     | general pool                         |
| `testnet`     | 6 h      | **TBD**     | **TBD**     | general pool                         |
| `sync`        | 48 h     | **TBD**     | **TBD**     | NVMe node-selector + NVMe toleration |

- Hard caps (timeouts) are **locked**. The per-test CPU/RAM reservations
  are pending the definitive table — they are *smaller* than the
  ServiceAccount **budgets** (§5.6); the 4/8 … 16/48 GiB figures discussed
  earlier turned out to describe SA budgets, not per-test sizes.
- **Reserve** = the per-namespace *aggregate* budget the topology may
  consume. It is both the scheduling reservation (§5) and the default
  ceiling for pod `limits`. Per-component `.resources(cpu, mem)` (already
  in the builder API) still overrides individual pods; the tier budget is
  the namespace aggregate the broker schedules against.
- Authorization is **orthogonal to tier**: it is a per-SA total budget,
  not a tier ceiling (§5.6). Any tier is allowed as long as the SA's
  concurrently-active reservations fit its budget.
- **Hard cap** is enforced by the broker's execution-cap timer (§5.5),
  *not* primarily by nextest's slow-timeout — see §5.2 for why.
- `sync` is **off the general pool**: it targets dedicated NVMe nodes via
  nodeSelector + toleration, so it never contends with the other tiers.

These numbers are v1 defaults living in one const table; tune later.

## 3. Surface — the attribute macro

`#[ztest::qos::sync]` is the **outer** attribute on a test. It receives
the whole item (including any inner `#[tokio::test]`), re-emits it intact,
and injects two things:

```rust
// Input:
#[ztest::qos::sync]
#[tokio::test(flavor = "multi_thread")]
async fn syncs_from_genesis() { /* body */ }

// Expansion:
::ztest::__private::inventory::submit! {            // (a) lowering → nextest config + plan
    ::ztest::qos::QosDecl {
        test_id: concat!(module_path!(), "::", stringify!(syncs_from_genesis)),
        class: ::ztest::qos::QosClass::Sync,
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn syncs_from_genesis() {
    ::ztest::qos::__enter(::ztest::qos::QosClass::Sync);   // (b) runtime → pod specs + lease
    /* body */
}
```

- **(a) inventory submit** is the *out-of-process* bridge: dumped by the
  existing `ZTEST_DUMP_INVENTORY` ctor so `ztest run` can group tests by
  tier and lower to nextest config + a capacity plan.
- **(b) task-local enter** is the *in-process* bridge: `TestEnv::build()`
  reads the current tier to set pod requests/limits/scheduling and to
  request a broker lease. It carries **only the tier** — the broker
  learns the test's *identity* from nextest's own `NEXTEST_TEST_NAME` +
  `NEXTEST_BINARY_ID` env vars (§12), so no `test_id` is threaded here.

One declaration, two consumers — same shape as `dev!`.

Macro lives in `ztest_macros`; the `qos` module re-exports the four
attribute macros. Snake_case throughout, no lint allows needed.

### Implementation risk: test-id mapping (must spike first)

`module_path!()` includes the test-binary crate root; nextest's `test()`
filterset name omits it. The lowering step must strip the leading
segment (or correlate per-binary, since each binary dumps its own
inventory). **A 30-minute spike validating the exact-match filterset
format precedes any lowering code** — it is the one load-bearing unknown.

This risk is scoped to **pre-run lowering only** (building `test(=…)`
filtersets for the config). The *runtime* broker path is immune: it uses
nextest's canonical `NEXTEST_TEST_NAME`/`NEXTEST_BINARY_ID` env vars, not
`module_path!()`.

## 4. The inventory bridge

Mirrors `inventory::DevImageDecl` exactly:

```rust
#[derive(Serialize)]                       // submit!-able: &'static fields
pub struct QosDecl { pub test_id: &'static str, pub class: QosClass }
inventory::collect!(QosDecl);

#[derive(Serialize, Deserialize)]          // owned read side
pub struct QosEntry { pub test_id: String, pub class: QosClass }
```

The `dump_hook` in `src/inventory.rs` grows a second pass emitting
`QosDecl` lines (tagged so the reader can demux `DevImageDecl` vs
`QosDecl` on one stream). No new process machinery.

## 5. The broker (admission controller)

Chosen over parameterising nextest's scheduler because we want true 2-D
(CPU×RAM) packing, allocatable-minus-requested accounting on a shared
cluster, NVMe-pool partitioning, authorization enforcement, and a live
reservation view — none expressible in nextest config alone.

### 5.1 Process topology

```
ztest run  (parent: cluster probe + BROKER + live display)
  └─ spawns: cargo nextest run        (env: ZTEST_BROKER_SOCK=/run/ztest-<id>.sock)
        └─ spawns: test process …      (env inherited)
              └─ TestEnv::build() ──UDS──▶ broker   (request → grant → … → release)
```

- `ztest run` changes from *exec-and-replace* to *spawn-child +
  concurrently run the broker event loop* until nextest exits.
- Env var handoff means the socket reaches every test process for free.
- **Graceful degradation**: if `ZTEST_BROKER_SOCK` is unset (a dev runs
  `cargo nextest run` directly, no `ztest`), `TestEnv::build()` skips
  admission and proceeds as today. The harness must keep working without
  the broker.

### 5.2 The critical interaction: slow-timeout vs queue time

The broker introduces the hazard that defines the whole design: **nextest's
slow-timeout measures wall-clock from process *spawn*, not from
*admission*.** A `basic` test (60 s cap) spawned but blocked for hours
behind a `sync` test would be killed by its own timeout while doing
nothing. Resolution is a **two-layer split**:

- **Layer 1 — nextest = coarse backpressure + priority ordering.** We
  still lower `threads-required` (= the tier's 1-D footprint, see §6) and
  `priority` into nextest config, with the pool sized to cluster
  capacity. This stops nextest from fork-bombing 1000 blocked processes
  and makes it spawn high-priority tiers first. Because nextest only
  spawns ≈capacity-worth of processes, **the broker's queue stays short**
  (seconds–minutes of transient contention, not hours).
- **Layer 2 — broker = authoritative 2-D admission + the real hard cap.**
  The broker does the precise allocatable/2-D/NVMe/authorization decision
  among the few spawned processes, and **arms the execution-cap timer at
  *admission*** — so the 60 s / 6 h / 48 h cap measures pure execution,
  queue-independent.

The two layers *agree* (both prefer high-priority tiers, both gate near
capacity) rather than fight. nextest's slow-timeout is lowered to a
**loose backstop** (≈ exec-cap, catching genuinely hung post-admission
tests); the broker timer is the precise cap.

The 1-D footprint nextest sees folds RAM in as
`max(cpu, ceil(mem / gib_per_unit))` so nextest's coarse gate already
approximates the broker's 2-D decision — keeping the queue short by
construction.

### 5.3 Capacity model

Built from an extended cluster probe (`pipeline/cluster.rs`):

- Partition nodes into **general pool** and **NVMe pool** (by node label /
  taint — the "NVMe NodeTolerance").
- Per pool, capacity = **allocatable − Σ requested** (sum of pod resource
  requests across all namespaces), not raw node capacity. This is the
  accuracy choice for shared clusters.
- The broker keeps a running tally (subtract on grant, add on release)
  and **periodically reconciles** against the live cluster to catch
  external (non-ztest) changes.

State: `free: {general: {cpu, mem}, nvme: {cpu, mem, nodes}}`,
`leases: Map<LeaseId, {test_id, tier, cpu, mem, pool, admitted_at}>`,
`queue: BinaryHeap<by (priority desc, request_time asc)>`.

### 5.4 Wire protocol (length-prefixed JSON over UDS)

```
client → Request { binary_id, test_name, tier }    // id from NEXTEST_* env; tier from task-local
broker → Grant   { lease_id, namespace }          // when it fits + authorized
broker → Reject  { reason }                        // unauthorized tier (§5.6)
client → Release { lease_id }                      // on teardown
broker → Cancel  { lease_id, reason }              // exec-cap exceeded → test aborts + tears down
```

**Crash safety**: the lease is bound to the connection. `TestEnv` holds
the socket open for the env's lifetime; normal teardown sends `Release`,
but a hard crash (SIGKILL, no Drop) simply closes the socket — the broker
treats **disconnect = release** and reclaims capacity. No leaked
reservations.

Namespace identity stays decentralised: names are deterministic
(`kn-{test_id}`, per `cluster.rs`), so the broker grants a *capacity
lease* and `TestEnv` creates its own namespace as today. The broker
tracks capacity, not namespace objects — minimal change to `env.rs` /
`cluster.rs`.

### 5.5 Scheduling policy

Greedy **priority admission with backfill**: on each schedule pass, admit
the highest-priority queued request that fits the live 2-D capacity for
its pool; lower-priority requests backfill the remaining capacity. At
t0 the cluster is empty, so the highest tier present (`testnet`/`sync`)
wins first dibs — satisfying "Testnet scheduled first" — and the
remainder backfills with `basic`. When a lease releases, freed capacity
triggers a schedule pass that backfills the next-fitting requests
("Testnet finishes → return resources → launch 2 basic").

Since all tests are known at t0, mid-run starvation of a big job by a
stream of small jobs does not arise; if it ever does, escalate to
EASY-style reservation (hold capacity for the head-of-line big job).
Noted, not built for v1.

**Deadlock-free by construction**: each request acquires its *entire* 2-D
footprint atomically (one `Request` → one `Grant`, §5.4) — no
hold-and-wait, hence no circular wait, hence no deadlock. A blocked test
only *waits*; the leases ahead of it are by definition running and will
release. Two invariants protect this: (a) a test never escalates its
reservation while holding a lease — the full need is fixed up front by the
tier; (b) tests are mutually independent — none blocks on another's
result, which would otherwise route a dependency cycle through the hidden
nextest-process-slot resource. A request exceeding even the empty-pool
capacity is **rejected**, not queued (§5.6, §8) — fail fast rather than
park forever on an unsatisfiable ask.

### 5.6 Authorization — per-SA budget

Each runner identity (ServiceAccount) carries a **total CPU/RAM budget**,
read from a **k8s annotation on the SA**, provisioned by cluster admins.
This is *not* a tier ceiling — any tier is permitted as long as the SA's
concurrently-active reservations fit the budget. The broker therefore
enforces **two nested constraints** on every admission:

1. `Σ(this SA's active reservations) + request ≤ SA budget`
2. `request ≤ live cluster free capacity` (for the request's pool)

A request that fits the cluster but exceeds the SA budget **queues**
(the SA is at quota; capacity frees when one of *its* tests finishes) —
distinct from a request that exceeds even the empty-cluster capacity,
which is **rejected** (unschedulable). The broker tracks per-SA usage
alongside the global pools.

The SA identity reaches the broker from the kubeconfig/SA `ztest run`
authenticates with; the broker reads the budget annotation off that SA at
startup. A `Reject` fails the test with a clear "tier T needs C CPU / M
GiB; SA budget is …" message.

Budget annotations are validated when read (`qos::parse_sa_budget`): an
**absent** annotation means unbudgeted/unlimited, a **partial** budget leaves
the unset dimension unlimited, but a **present-but-unparseable** value fails the
run with a clear error rather than silently degrading to a `0` budget (which
would reject every request) or being ignored — a typo must not masquerade as a
real limit. Infrastructure errors reading the SA (missing object, transient API
failure) stay best-effort and fall back to unbudgeted, so they never block on a
cluster hiccup.

## 6. nextest config lowering

After `nextest list` + the QOS dump, `ztest run` writes a generated
`--tool-config-file ztest:<path>`:

```toml
[test-groups]
qos-sync = { max-threads = <#NVMe nodes> }        # sync gated by NVMe capacity

[[profile.<p>.overrides]]                          # one block per tier
filter = 'test(=mod::a) | test(=mod::b) | …'       # exact-match ids from the dump
slow-timeout = { period = "<cap>", terminate-after = 1 }   # loose backstop (§5.2)
threads-required = <footprint units>               # coarse backpressure
priority = <tier priority>                         # testnet/sync high
# sync block also: test-group = 'qos-sync', threads-required = 1 (off general pool)
```

Run: `cargo nextest run --tool-config-file ztest:<gen>
--test-threads <pool> …`, where `pool = min(user --test-threads (LOCAL
max), cluster_units)`. `--test-threads` stays a *local ceiling*; the
cluster capacity is the other ceiling; the effective pool is the min.
Un-annotated tests fall through to the profile default = `basic`.

**The generated config is advisory, not authoritative.** `tool:` configs
sit *below* the repository `.config/nextest.toml` and per-test overrides
in nextest's precedence (CLI > env > repo per-test overrides > repo
profile config > **tool config** > defaults). A repo that adds its own
nextest config can therefore shadow our tier overrides. We accept this by
design: the config layer only provides **coarse backpressure + spawn
ordering** (`threads-required`, `priority`) and a loose `slow-timeout`
backstop. All *policy* — the real hard-cap timer, 2-D admission, per-SA
budget, NVMe gating — is enforced by the broker, which nothing in nextest
config can override. Correctness never depends on the tool config
winning. (This repo ships no `.config/nextest.toml` today, so the shadow
case is currently latent; `ztest run` can detect and warn if one
appears.) The scalar `--test-threads` we force via CLI (`--config`-grade
precedence) so the pool ceiling always takes effect.

## 7. Runtime path — pods

`TestEnv::build()` (`env.rs:483`):
1. Read tier from the task-local (default `basic` if unset).
2. Acquire a broker lease (if `ZTEST_BROKER_SOCK` set) — block on `Grant`.
3. Create the namespace as today; apply tier `requests`/`limits` to each
   pod (tier aggregate distributed across components / as defaults, with
   explicit `.resources()` overriding); for `sync`, add the NVMe
   nodeSelector + toleration to pod specs.
4. On teardown / Drop: tear down namespace, `Release` the lease.

`requests == limits` at the tier budget gives k8s "Guaranteed" QoS for
reserved test workloads (distinct from *ztest* tiers — name collision to
call out in docs).

**Resolved — pod sizing for maximum performance (`env::even_share` +
`manifest::PodSpec`):** when a test doesn't call `.resources()`, the tier
footprint is split **evenly across the env's pods** (validators + indexers;
in-process wallets get none) and rendered as `requests == limits`. The CPU
share is rounded **up to whole cores** (≥1) so each container is eligible for
the kubelet **CPU Manager `static` policy** (exclusive, pinned CPUs) — fractional
CPU would drop the pod into the shared pool. Memory is the exact even share.
Pods are **killed, never migrated**, on node failure: they are bare `Pod`s with
`restartPolicy: Never` (k8s never recreates them), and the auto-added
`node.kubernetes.io/{not-ready,unreachable}` tolerations are overridden to
`tolerationSeconds: 0` so a lost node deletes the pod immediately instead of
after the default 5-minute grace. An explicit `.resources()` overrides this
(requests-only, as before).

Because the per-pod CPU is rounded up to whole cores, `pods × per-pod` can
exceed the raw tier footprint (8 cores over 3 pods → 3+3+3 = 9). Admission
therefore reserves the **deployed** footprint (`env::deployed_footprint` =
per-pod share × pods), *not* the raw tier footprint, so the ledger can never
under-count and grant capacity a pod then can't schedule into. For the `sync`
tier (NVMe nodeSelector), `build()` also fails fast if **no NVMe-pool node is
schedulable** rather than admitting against global capacity and leaving the pod
Pending on an unsatisfiable selector until `ready_timeout`.

## 8. Preflight planning & display

`ztest run` already reserves `tier`, `queue`, `reservation` banner rows
(`preflight/render.rs`). Filled from a `qos::schedule` planning pass:
group selected tests by tier, compute peak concurrent namespaces and the
wave structure against probed capacity, and warn if any single tier's
footprint exceeds the pool (e.g. a `sync` test needs 16 CPU on a 8-CPU
cluster — unschedulable, fail fast). Live lease state updates the
`reservation` row during the run.

## 9. Module map

| File | Change |
|---|---|
| `macros/src/lib.rs` | new `qos` module: 4 attribute macros, dual emission |
| `src/qos/mod.rs` | `QosClass`, `QosProfile` const table, task-local `__enter`/`current` |
| `src/qos/protocol.rs` | broker wire types (Request/Grant/Reject/Release/Cancel) |
| `src/qos/broker.rs` | admission controller: capacity, queue, policy, UDS loop, timers, reconcile, authz |
| `src/qos/client.rs` | `TestEnv`-side lease client |
| `src/qos/schedule.rs` | grouping + planning for preflight |
| `src/inventory.rs` | `QosDecl`/`QosEntry`; extend dump hook |
| `src/pipeline/cluster.rs` | probe: allocatable−requested; NVMe vs general partition; node labels |
| `src/env.rs` | `build()`: lease acquire, tier requests/limits, sync nodeSelector; teardown release |
| `src/cli/run.rs` | spawn-concurrent nextest + broker loop; generate tool-config; set env |
| `src/preflight/*` | fill tier/queue/reservation rows |
| `src/lib.rs` | export `qos`; prelude additions |

## 10. Phased implementation plan

1. **Spike** — validate `module_path!()` → nextest exact-match filterset
   (§3 risk). Gate on this.
2. **Declarative core** — `qos` attribute macros + `QosClass`/`QosProfile`
   + inventory `QosDecl` + dump-hook extension. Verifiable by dumping a
   test binary's QOS inventory.
3. **Lowering (no broker)** — group-by-tier + generate tool-config
   (slow-timeout/priority/threads-required) + retire the `=6` hardcode.
   Tests get correct timeouts/concurrency via nextest alone.
4. **Pods** — tier requests/limits + sync nodeSelector/toleration via the
   task-local. Independent of the broker.
5. **Probe** — allocatable−requested + NVMe partition.
6. **Broker** — protocol, capacity model, scheduling, lease lifecycle,
   crash safety; wire `TestEnv` client + `ztest run` concurrency change.
7. **Authorization** — authorized-tier ceiling + reject path.
8. **Display** — planning pass + live reservation rows.

Phases 2–4 deliver real value (timeouts, limits, scheduling-via-nextest)
*before* the broker exists; the broker (6) upgrades admission from
coarse-nextest to precise-2-D.

## 11. Open questions

- **Per-test reservation table** (§2): _Resolved_ — `basic` 500m/512Mi,
  `integration` 2c/2Gi, `testnet` 8c/18Gi, `sync` 16c/32Gi
  (`QosClass::profile`). Caps/timeouts remain locked.
- **Tier budget vs per-pod resources** (§2, §7): _Resolved_ — split the
  aggregate footprint **evenly across the env's pods**, `requests == limits`,
  CPU rounded up to whole cores for static-policy pinning; explicit
  `.resources()` overrides (see §7).
- **SA budget annotation key** (§5.6): _Resolved_ —
  `qos.zaino.io/budget-cpu` / `qos.zaino.io/budget-mem` (k8s quantities) on the
  run's ServiceAccount, identified by the `ZTEST_SA` env var, read in-cluster
  (`qos::parse_sa_budget` + `TestEnv::build`).
- **slow-timeout backstop value** (§5.2): _Resolved_ — no admission-anchored
  exec-cap timer was built, so nextest's `slow-timeout` is the **sole** cap.
  Since it measures from process *spawn*, it's set `period = hard_cap,
  terminate-after = 2`: flagged SLOW at one `hard_cap`, hard-killed at 2× — a
  test keeps roughly a full execution budget after a (short, by §5.2) queue
  wait. Teardown on a timeout-kill relies on the `janitor/ttl` backstop. A
  future in-process watchdog could anchor the cap at admission and demote this
  to a loose backstop.
- **NVMe node identification**: exact label/taint key for the NVMe pool.
  _(Placeholder `zaino.io/pool=nvme` in `qos::NVME_*`; node count sizes the
  `qos-sync` group via the probe.)_
_Resolved during audit (§12):_ admission point = `TestEnv::build()` (not
wrapper scripts), so topology-less tests reserve nothing and we take no
dependency on experimental nextest scripts; tool-config precedence
handled by making the broker authoritative (§6).

**Audit hardening (correctness fixes):**
- **Admission lock TOCTOU** — the per-process lock isn't renewed mid-section,
  so a critical section that outlives `LOCK_TICKS` could be stolen and let two
  allocators commit against one free snapshot (overcommit). The reservation
  `create` is now **fenced** on continued lock ownership (re-check the lock's
  version == the acquired epoch immediately before committing; abort → `Retry`
  otherwise), narrowing the window from the whole section to a sub-millisecond
  gap ≪ `LOCK_TICKS`.
- **Live-panel signal handling** — `ztest run`'s live panel resets the DECSTBM
  scroll region on `Drop`, but a signal skips `Drop`. The poll loop now selects
  on `ctrl_c()` and resets the terminal + cleans the temp tool-config explicitly
  before exiting `130`, so Ctrl-C (the normal exit for a long `sync` run) never
  leaves a stuck scroll region.

**Known limitation (deferred):** a present `Job` always counts toward committed
capacity with no liveness check — a lingering completed/failed Job (no TTL GC,
stuck finalizer) keeps consuming capacity until externally deleted. Reservations
self-heal via lease expiry; Jobs lean on the cluster's Job GC / the planned
`janitor/ttl` backstop. Filtering Jobs by pod phase in `ledger::reconstruct` is
the eventual fix but needs the kube adapter to surface Job status first.

## 12. nextest integration strategy (research findings)

### What nextest exposes — every channel, by direction

nextest has **no bidirectional API, no callback, no live event stream.**
Every channel is one-way; all live coordination in this design is our own
UDS (§5), threaded through processes nextest merely spawns.

| Channel | Direction | When | Carries |
|---|---|---|---|
| Config (`.config/nextest.toml`, `--tool-config-file tool:path`, `--config K=V`, env) | orchestrator → nextest | pre-run | profiles, overrides, test-groups, scripts |
| Env vars (`NEXTEST_TEST_NAME`, `NEXTEST_BINARY_ID`, `NEXTEST_RUN_ID`, `NEXTEST_PROFILE`, `NEXTEST_TEST_GROUP`, slots, `NEXTEST_ATTEMPT`) | nextest → test process | runtime | read-only run facts; **test's canonical identity** |
| Setup-script `$NEXTEST_ENV` file *(experimental)* | setup script → tests (via nextest) | pre-tests | env vars injected into matching tests |
| Wrapper script *(experimental, ≥0.9.98)* | process interposition | per-test | `wrapper <test-bin> <args…>`; **no data back to nextest** |
| Output: human TTY · JUnit XML · libtest-json *(experimental)* | nextest → orchestrator | **post-run only** | results; **no live stream** (issue #20) |
| Exit code | nextest → orchestrator | post-run | pass/fail |

Consequences baked into the design:
- **Identity is free**: the broker client reads `NEXTEST_TEST_NAME` +
  `NEXTEST_BINARY_ID`; the macro's task-local need only carry the tier
  (§3b, §5.4).
- **No experimental dependency**: admission lives in `TestEnv::build()`,
  so we use neither setup nor wrapper scripts. They stay a documented
  fallback (wrapper scripts need `experimental = ["wrapper-scripts"]` and
  nextest ≥ 0.9.98).
- **Broker is the only live UI source** — not a preference, a necessity:
  there is no live machine-readable stream to parse.

### nextest's model, and how we ride it:

- **Process-per-test.** nextest builds binaries (list phase), then spawns
  *each test as its own OS process* in parallel up to a pool (run phase).
  There is **no "test flavor"** concept — `flavor = "multi_thread"` is a
  *tokio* runtime knob, orthogonal. Concurrency is controlled purely at
  the process level: `test-threads` (pool), `threads-required` (per-test
  slots), and `[test-groups] max-threads` (per-group semaphore).
- **Single invocation, not per-tier.** One `cargo nextest run` with
  per-tier *overrides* (filterset-matched `slow-timeout`/`priority`/
  `threads-required`/`test-group`). Separate per-tier invocations would
  **serialize the tiers** (killing cross-tier backfill — the whole point)
  and fragment the output into N banners/progress bars. Rejected.
- **Single-concurrency** for a set = a `test-group { max-threads = 1 }`,
  assigned via override filter (this is how `sync` serializes on NVMe).
- **Admission / queue-until-resources** = block on the broker UDS. Two
  mechanisms:
  - *wrapper scripts* (`[scripts.wrapper.*]`): nextest invokes
    `cmd <test-bin> <args…>` per-test, the cmd must exec the binary, may
    block first. Language-agnostic, no `TestEnv` change — but gates
    *every* matched test, including topology-less `basic`.
  - *`TestEnv::build()` admission* **(recommended)**: only topology-booting
    tests hit the broker; pure-logic tests reserve nothing.
- **slow-timeout starts at process spawn**, so any queue wait burns it.
  Hence the §5.2 two-layer split: nextest `threads-required` keeps the
  queue short (coarse backpressure), the broker arms the *real* hard-cap
  timer at admission. nextest `slow-timeout` is a loose backstop;
  `on-timeout`, `grace-period`, `terminate-after` are available.
- **No live machine-readable event stream** (only post-run JUnit /
  experimental libtest-json; live JSON is unshipped — nextest issue #20).
  So we do **not** parse nextest output for the UI. **The broker is the
  live event source** — it sees every admission/start/release and drives
  the live QOS panel; nextest renders per-test pass/fail below it.
- **Useful run knobs**: `--show-progress {auto,bar,counter,none,only}`,
  `--status-level`, `--final-status-level`, `--no-capture`,
  `global-timeout` (whole-run cap), `run-extra-args`.

### Screen layout

Reuse the existing DECSTBM pinned-header (`cli/run.rs`): pin a
broker-driven **QOS scheduler panel** at top (capacity gauges, per-tier
running/queued, per-SA budget, wave progress); nextest's per-test output
scrolls beneath. Optionally quiet nextest with `--show-progress none` and
own all progress from the broker.

Sources: nexte.st docs — [how-it-works](https://nexte.st/docs/design/how-it-works/),
[wrapper-scripts](https://nexte.st/docs/configuration/wrapper-scripts/),
[setup-scripts](https://nexte.st/docs/configuration/setup-scripts/),
[test-groups](https://nexte.st/docs/configuration/test-groups/),
[config reference](https://nexte.st/docs/configuration/reference/),
[machine-readable](https://nexte.st/docs/machine-readable/),
[running](https://nexte.st/docs/running/).
