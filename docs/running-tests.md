# Running Tests

`cargo nextest run`, in both dev and CI. No wrapper binary, no special
entry point.

## Requirements

- `cargo nextest` **≥ 0.9.x** — the library reads
  `NEXTEST_TEST_GLOBAL_SLOT` from env, set by nextest when scheduling
  parallel test processes. Older nextest doesn't set it.
- A reachable cluster API. CI: in-pod SA token (automatic). Dev: a
  kubeconfig pointing at the cluster's tailnet API server (see
  [architecture-overview.md](architecture-overview.md#cluster-access)).

## Dev

```bash
cargo nextest run -p zaino-integration-tests
cargo nextest run -p zaino-integration-tests indexer::wallet_sync
KUBECONFIG=~/.kube/kind-zaino cargo nextest run -p zaino-integration-tests
```

Each test process bootstraps its own namespace on first
`TestEnv::build()`:

```
zaino-dev-${USER}-${NEXTEST_PID}-${NEXTEST_TEST_GLOBAL_SLOT}
```

Two parallel `cargo nextest` invocations have different `NEXTEST_PID`
(nextest's PID via `getppid()`), so their namespaces don't collide.

Dev namespaces are **not** cleaned up at process exit. They survive
until the cluster TTL controller GC's them (default 1h after
`last_accessed_at`). To force cleanup:

```bash
kubectl delete ns -l zaino.io/owner=${USER}
```

## CI (GitHub Actions)

One job. One ARC runner pod. One `cargo nextest` invocation. nextest
fans out test processes across slots inside the pod; the cluster does
the actual work via the slot-→-namespace model.

```yaml
env:
  ZTEST_RUN_ID: ${{ github.run_id }}-${{ github.run_attempt }}

jobs:
  test:
    runs-on: [self-hosted, zaino-arc]
    steps:
      - uses: actions/checkout@v4
        with: { lfs: true }

      - run: cargo nextest run -p zaino-integration-tests --test-threads 8

      - if: always()
        run: kubectl delete ns -l zaino.io/run-id=$ZTEST_RUN_ID
```

`ZTEST_RUN_ID` is the only env var the library reads in CI;
test processes use it as the namespace name prefix and stamp it as a
label on every resource (so observability queries can filter by run).

No artifact collection — logs, events, and metrics are picked up
continuously by cluster-resident observability (Promtail → Loki,
Prometheus, kube-state-metrics) in the `observability` namespace and
retained well past the run. Anything a post-mortem needs is queryable
by `run-id` label after the fact (see
[architecture-overview.md](architecture-overview.md#observability)).

If the cleanup step doesn't run (runner preempted, force-cancelled
job), the cluster TTL controller is the backstop.

## Slot mechanics

Nextest is process-per-test. With `--test-threads N`, up to N test
processes run concurrently; nextest pulls tests off a queue and assigns
each to a slot in `0..N`. When test A finishes in slot 3, test B starts
in slot 3 as a new process, inheriting the same slot number.

```
nextest run --test-threads 4
│
├─ slot 0 ┬─ test_a (process pid 1001)  ─┐ same namespace zaino-ci-X-0
│         └─ test_d (process pid 1009)  ─┘ across the slot's lifetime
│
├─ slot 1 ┬─ test_b
├─ slot 2
└─ slot 3
```

Within a slot, tests share the namespace; pods don't collide
(UID-suffixed); per-test cleanup is via the sentinel ConfigMap pattern
documented in
[architecture-overview.md](architecture-overview.md#lifecycle).

**Hard cap: 16 slots.** The library refuses to start if
`NEXTEST_TEST_GLOBAL_SLOT >= 16`. Set `--test-threads ≤ 16`; for heavy
parallelism, scale the cluster, not the slot count.

## Preflight

`zkn-preflight` runs once per `cargo nextest` invocation as a
[nextest setup script](https://nexte.st/book/configuration.html#setup-scripts).
It prints the session banner, fetches missing archives via Git LFS,
materializes archive PVCs and binds VolumeSnapshots, and refuses to
start the run if the cluster is unreachable.

Wired in by the workspace's `.config/nextest.toml`:

```toml
[scripts.setup.preflight]
command = ['cargo', 'run', '--quiet', '--bin', 'zkn-preflight']
# capture-stdout / capture-stderr default to false — output streams
# straight to the terminal between nextest's "Starting N tests" banner
# and the first test result.
slow-timeout = { period = "120s", terminate-after = 3 }

[[profile.default.scripts]]
setup = ['preflight']
```

### Banner

```
┌─ ztest ────────────────────────────────────────────────────
│ cluster
│   context        kind-zaino-local
│   capacity       12 / 16 slots used  (configured: 6 via --test-threads)
│   nodes          3 ready · 0 cordoned  (12 cores · 48 GiB)
│ archives (4)
│   ✓ regtest-nu5-h128        cached · 412 MiB
│   ✓ testnet-2.6m            cached · 18.4 GiB
│   ⇣ testnet-3.1m            downloading from LFS  [█████░░] 64%
│   ! mainnet-snapshot-9.0    missing  (LFS pointer present, blob absent)
│ snapshots
│   ✓ pvc/zebra-testnet-cache   bound · ready
│   ⇣ pvc/zebra-mainnet-cache   provisioning from archive testnet-3.1m
└────────────────────────────────────────────────────────────
```

Marker semantics:

- `✓` — already in the target state, no work to do.
- `⇣` — work in progress (live-refreshed in place).
- `!` — soft failure. The run proceeds; tests that actually need the
  affected resource will fail at `TestEnv::build()` with a clearer
  error than they would get otherwise.

Archives that no selected test references are pruned before render —
the banner shows only what the *current* invocation will touch.

The banner refreshes in place while any `⇣` row is live. Once every
`⇣` has settled to `✓` or `!`, a final snapshot is printed and the
session hands off to nextest.

### Render contract

- One block, ≤ 25 lines. Wider than 60 columns is fine; nextest's
  reporter doesn't wrap it.
- Single-line in-place refresh per `⇣` row via `\r`; multi-line
  refresh uses `\x1b[A` cursor-up sequences only while no row above
  has finalized.
- Plain ASCII fallback when `NO_COLOR=1` or `stdout` is not a TTY —
  no box-drawing, no escape codes; `✓` becomes `OK`, `⇣` becomes
  `..`, `!` becomes `WARN`.
- Final-state output is the same lines without any escape codes, so
  CI logs are diffable.

### What preflight does

1. **Resolve the test selection.** Read `NEXTEST_PROFILE` and the
   filter expression nextest passed in; intersect with the per-binary
   mount inventory (see *Mount enumeration* below). Archives no
   selected test references are pruned from the work list.
2. **Probe the cluster.** Resolve `KUBECONFIG`, hit the API server,
   list nodes, count `zaino-{ci,dev}-*` namespaces as a proxy for
   current concurrency.
3. **Resolve archives.** For each required `seed-{sha8}` PVC in
   `zaino-seeds`:
   - PVC labelled `seeds.zaino.io/ready=true` → `✓ cached`.
   - PVC exists but not ready → `⇣ provisioning`; attach to the
     in-flight reconcile Job's log stream and render byte-progress.
   - PVC absent + LFS blob local → create PVC + reconcile Job per
     the materialization flow in
     [architecture-overview.md](architecture-overview.md#archive-pvcs),
     render progress.
   - PVC absent + LFS blob remote → `git lfs pull --include=<ptr>` for
     just that pointer, render progress, then continue.
   - LFS pointer present, blob unreachable → `!` and warn; proceed.
4. **Resolve snapshots.** For each `VolumeSnapshot` the selected
   tests will clone, ensure its source PVC is ready (recurses into
   step 3) and the snapshot is bound.
5. **Emit a final snapshot** of the banner and exit 0.

Soft failures (`!`) localize impact to the tests that actually need
the missing resource. Hard failures (cluster unreachable, auth
failure, malformed manifest) abort the run via exit ≠ 0; nextest
treats setup-script failure as a suite-level abort, so no test binary
runs.

### Failure modes

| Condition                          | Marker | Run continues? |
| ---------------------------------- | ------ | -------------- |
| Cluster API unreachable            | n/a    | No — exit ≠ 0  |
| Auth failed                        | n/a    | No — exit ≠ 0  |
| Mount enumeration failed           | n/a    | No — exit ≠ 0  |
| LFS pointer present, blob missing  | `!`    | Yes; affected tests fail at `TestEnv::build()` with the missing-archive error |
| Archive reconcile Job failed       | `!`    | Yes; same      |
| VolumeSnapshot stuck in `Pending`  | `!`    | Yes; tests cloning it time out at `build()` |

### Mount enumeration

Each test binary publishes the set of `mount_archive!` /
`mount_file!` / `mount_config!` paths it would invoke if run.
Preflight reads this and intersects with the test filter to produce
the work list.

**v1 — `build.rs` inventory.** A `linkme` distributed slice
collects every `mount_*!` macro expansion into a `&'static [Mount]`
table compiled into each test binary. Preflight invokes
`<test-bin> --zkn-list-mounts` (an arg parsed before nextest's own
arg-parser) and reads JSON on stdout. The arg is a stable contract
between preflight and the test binary — adding it is one shim line
per `tests/*.rs` via `ztest::prelude::*`.

This avoids a parallel source-of-truth file that would rot. The
trade-off is that preflight pays one `exec` per test binary at
session start (~50 ms each); nextest already builds and links every
binary before the setup script runs, so the binary is on disk.

### Future — session-level features

These all depend on a cluster-wide *session registry* and a
*ClusterCapacity* policy object that don't exist yet (see
[architecture-overview.md](architecture-overview.md#observability)
for what *is* there, and `cluster-administration.md` for the
operator surface). The banner reserves space for them so adding rows
later is a non-event. Each row renders today as
`not yet implemented` or is omitted, and resolves into a real value
once the underlying primitive lands.

#### F1 — Cluster-configured concurrency

Today the banner's `capacity` line reports `configured: N via
--test-threads`. That's the *per-invocation* limit, not a cluster
limit. A future `ClusterCapacity` CR (or a `ResourceQuota` on a
`zaino-system` namespace) carries an authoritative
`spec.maxConcurrentSessions`. Once present, the line reads:

```
│   capacity       12 / 24 slots used  (this run: 6 via --test-threads)
```

Preflight refuses to start sessions that would push usage past the
cluster cap; instead it falls through to F2 (queue).

**Prerequisite:** `ClusterCapacity` CR + admission webhook on
`Session` creation. Specced in
[`session-allocation.md`](session-allocation.md) (TODO).

#### F2 — Queue position

When the cluster is at capacity, preflight blocks at the
session-registry admission step and renders a queue row:

```
│ queue
│   waiting       3 sessions ahead · est. 4m12s
│     ┝ run-id 9912481  user: zancas     started 2m ago
│     ┝ run-id 9912439  user: ci-arc-7   started 47s ago
│     ┕ run-id 9912438  user: elicb      started 12s ago
```

The estimate is from the rolling median session duration computed
over Loki query results, falling back to a 5-minute default until
the controller has a sample.

**Prerequisite:** F1 + the `Session` CR carrying
`spec.launcher.{user,runId,kind}` and a `status.phase` of
`Pending|Running|Completed|Preempted`. The watch-and-render loop is
preflight-local.

#### F3 — QoS tiers

Today every session is the same priority. Tiers (per `README.md`
TODO): `interactive`, `ci`, `nightly`. Tier comes from the
launcher's ServiceAccount via a ClusterRoleBinding mapping. The
banner renders it as a third cluster-block row:

```
│   tier           interactive (preempts ci, nightly)
```

`zkn-preflight --tier=<name>` overrides the SA-derived tier within
the bounds the SA is authorized for (the admission webhook is the
truth, not the flag).

**Prerequisite:** `PriorityClass`es + ServiceAccount-to-tier
ClusterRoleBindings + admission validation.

#### F4 — Preemption

A higher-tier session arriving while the cluster is at capacity
preempts the lowest-priority running session. Two-sided banner
behaviour:

- *Being preempted:* the running session's preflight observes a
  `status.phase=Preempted` transition (via the watch from F2) and
  exits with a structured code, surfaced by ztest as a final block:
  ```
  ┌─ ztest — preempted ────────────────────────────────────────
  │ this session (run-id 9912481) was preempted by tier:interactive
  │ owner: elicb · age 4m12s · 5/12 tests completed
  │ retry policy: re-queue at session boundary
  └────────────────────────────────────────────────────────────
  ```
- *Causing preemption:* a `preempted` row lists the displaced
  sessions and their owners.

**Prerequisite:** F1 + F3 + the controller-side preemption loop.

#### F5 — Per-test resource accounting

Requires the planned
`#[zaino_test(qos = ..., cpu = ..., mem = ...)]` macro (TODO in
[`README.md`](README.md)). Once present:

```
│   reservation    18 cores · 24 GiB  (sum of selected tests)
```

Preflight rejects sessions whose reservation exceeds the tier's
allocation. This row is the link between *test selection* and
*cluster capacity*; without it, F1's cap is a session count, not a
resource budget.

**Prerequisite:** macro + per-pod requests/limits propagation +
admission summation.

#### F6 — Live archive download from a remote LFS server with a
shared cluster cache

Today the LFS fetch in step 3 runs in the launcher's local working
tree. On a fresh CI runner that's a cold fetch every time. Future
work: a cluster-resident LFS cache pod in `zaino-system` that holds
recently-fetched blobs and is queried by hash before the launcher's
local Git LFS is consulted. The banner gains:

```
│   ⇣ testnet-3.1m            streaming from cluster cache  [█████░░] 64%
```

**Prerequisite:** the cache deployment + a tiny client shim in
preflight that prefers it over local LFS.

### Future rows render today as

```
│ tier           not yet implemented
│ queue          not yet implemented
│ reservation    not yet implemented
```

…until the matching feature ships. Keeping the rows visible — rather
than omitted — anchors the banner's vertical layout against the
final design and makes the gap legible to anyone looking at the
session output.

## Layout

A *suite* is a directory under `tests/`; a *test case*
is a `#[tokio::test]`. Cargo treats each top-level file under `tests/`
as a separate binary, so nextest's `hash:N/M` distributes binaries
across workers — a flaky test's retry lands on the same worker.

```
crates/zaino-integration-tests/tests/
├── indexer/        # Zaino ↔ validator
├── interop/        # zebrad ↔ zcashd parity
├── state/          # snapshot / clone
└── wallet/
```

## Filtering

```bash
cargo nextest run -p zaino-integration-tests indexer::wallet_sync     # substring match
cargo nextest run -p zaino-integration-tests --filter-expr 'test(reorg)'
cargo nextest run -p zaino-integration-tests --filter-expr 'binary(indexer) and not test(slow)'
cargo nextest run -p zaino-integration-tests --skip slow               # by name substring
```

Substring filters match against the fully-qualified test name
(`crate::module::test_fn`); `--filter-expr` accepts nextest's
[filtering DSL](https://nexte.st/book/filter-expressions.html) and is
the right tool for anything beyond a single substring.

Cross-version regression uses `rstest` with version
strings inline — each `#[case]` becomes its own nextest target, so
filters operate on cases too. See
[test-author-api.md#rstest](test-author-api.md#rstest).

## Namespace summary

|                     | Dev                                           | CI                                         |
| ------------------- | --------------------------------------------- | ------------------------------------------ |
| Namespace           | `zaino-dev-${user}-${nextest_pid}-${slot}`    | `zaino-ci-${run_id}-${slot}`               |
| Created by          | Library, first `TestEnv::build()` in slot     | Same                                       |
| Reused across tests | Within a slot, yes (sequential tests)         | Same                                       |
| End-of-run cleanup  | None (TTL controller GC, default 1h idle)     | Workflow step deletes ns by `run-id` label |
| Logs / metrics      | Cluster Loki + Prometheus (query by `run-id`) | Same                                       |

## Open

1. **Live log streaming in dev.** `env.handle(&ref).logs().subscribe()` on top of the kube `Pod/log` watch — deferred to v2 because in CI Loki already covers it.
1. **`#[requires(cluster_capability)]`.** A capabilities probe at `TestEnv::build()` (e.g. Ceph snapshots present, ARC version, GPU nodes)
   so tests skip cleanly on incompatible clusters instead of failing opaquely.
1. **Deterministic seed warmup in CI.** First test of a slot pays the archive-materialize cost on a cold node
