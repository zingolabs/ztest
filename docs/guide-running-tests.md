# Running Tests

The suite runs via `cargo nextest run` — no wrapper binary, same command in dev and CI.

## Requirements

- `cargo nextest` **≥ 0.9.x** — ztest reads `NEXTEST_TEST_GLOBAL_SLOT`, which older nextest doesn't set.
- A reachable cluster, resolved by `kube::Config::infer()`: an in-pod ServiceAccount token, a SA-token `KUBECONFIG` (CI), or a dev `KUBECONFIG` for a local kind cluster.
- Dev-image distribution — `kind load` by default, or registry push when `ZTEST_IMAGE_REGISTRY` is set. See [ops-clusters.md](ops-clusters.md) for the full `ZTEST_IMAGE_*` env-var table.

## Dev

```bash
cargo nextest run -p zaino-integration-tests
cargo nextest run -p zaino-integration-tests indexer::wallet_sync
KUBECONFIG=~/.kube/kind-zaino cargo nextest run -p zaino-integration-tests
```

Each test process bootstraps its own namespace on first `TestEnv::build()`:

```
ztest-dev-${USER}-${NEXTEST_PID}-${NEXTEST_TEST_GLOBAL_SLOT}
```

`NEXTEST_PID` (nextest's PID) differs between parallel invocations, so their namespaces don't collide.

Dev namespaces are **not** cleaned up at exit; the cluster TTL controller GCs them (default 1h after `last_accessed_at`). To force cleanup:

```bash
ztest cleanup            # your finished runs and syncs; --dry-run to preview
ztest cleanup --all-users
```

`cleanup` skips anything still live (an in-flight run, a `Running` sync) and
never touches the cluster itself or the seed cache; `--force` overrides the
liveness gate.

## CI (GitHub Actions)

One job on a default GitHub-hosted runner (`ubuntu-latest`, no self-hosted / ARC). The runner builds/pushes the dev image and drives the test binary over kubeconfig; every expensive operation runs on the cluster.

Auth is a ServiceAccount-token `KUBECONFIG` stored as `KUBECONFIG_B64`: a cluster SA with run RBAC (namespace CRUD, VolumeSnapshot create, node/CSIDriver read), token embedded in a kubeconfig. Images go to `ghcr.io` — the runner pushes with `GITHUB_TOKEN`, the cluster pulls over egress (no cluster ingress needed).

```yaml
env:
  ZTEST_RUN_ID: ${{ github.run_id }}-${{ github.run_attempt }}
  ZTEST_IMAGE_REGISTRY: ghcr.io/${{ github.repository_owner }}

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { lfs: true }

      - name: kubeconfig
        run: |
          mkdir -p ~/.kube
          echo "${{ secrets.KUBECONFIG_B64 }}" | base64 -d > ~/.kube/config

      - name: registry login
        run: echo "${{ secrets.GITHUB_TOKEN }}" | docker login ghcr.io -u ${{ github.actor }} --password-stdin

      - run: ztest run -p clientless -p e2e --test-threads 8

      - if: always()
        run: kubectl delete ns -l ztest.io/run-id=$ZTEST_RUN_ID
```

`ZTEST_RUN_ID` prefixes each namespace name and labels every resource, so the cleanup step and cluster-resident observability filter by run — there is no artifact collection step; query logs/events/metrics by `run-id` (see [design-architecture.md](design-architecture.md#observability)). If the cleanup step is skipped (runner preempted), the TTL controller is the backstop.

## Slot mechanics

Nextest is process-per-test. With `--test-threads N`, up to N processes run concurrently, each assigned a slot in `0..N`. When a test finishes in a slot, the next test starts in that same slot as a new process, inheriting the slot number and its namespace.

```
nextest run --test-threads 4
│
├─ slot 0 ┬─ test_a (pid 1001)  ─┐ same namespace ztest-ci-X-0
│         └─ test_d (pid 1009)  ─┘ across the slot's lifetime
├─ slot 1 ── test_b
├─ slot 2
└─ slot 3
```

Pods in a shared namespace are UID-suffixed so they don't collide. Per-test cleanup uses the sentinel ConfigMap pattern (see [design-architecture.md](design-architecture.md#lifecycle)).

**Hard cap: 16 slots.** ztest refuses to start if `NEXTEST_TEST_GLOBAL_SLOT >= 16`. Set `--test-threads ≤ 16`; for heavier parallelism, scale the cluster.

## Filtering

```bash
cargo nextest run -p zaino-integration-tests indexer::wallet_sync              # substring match
cargo nextest run -p zaino-integration-tests --filter-expr 'test(reorg)'
cargo nextest run -p zaino-integration-tests --filter-expr 'binary(indexer) and not test(slow)'
cargo nextest run -p zaino-integration-tests --skip slow                       # by name substring
```

Substring filters match the fully-qualified test name (`crate::module::test_fn`); `--filter-expr` accepts nextest's [filtering DSL](https://nexte.st/book/filter-expressions.html). Cross-version regression uses `rstest` — each `#[case]` is its own nextest target, so filters operate on cases too (see [guide-writing-tests.md](guide-writing-tests.md)).

## Layout

A *suite* is a directory under `tests/`; a *test case* is a `#[tokio::test]`. Cargo compiles each top-level file under `tests/` as a separate binary, so nextest's `hash:N/M` distributes binaries across workers — a flaky test's retry lands on the same worker.

```
crates/zaino-integration-tests/tests/
├── indexer/        # Zaino ↔ validator
├── interop/        # zebrad ↔ zcashd parity
├── state/          # snapshot / clone
└── wallet/
```

## Preflight

`zkn-preflight` runs once per invocation as a [nextest setup script](https://nexte.st/book/configuration.html#setup-scripts), wired in via `.config/nextest.toml`:

```toml
[scripts.setup.preflight]
command = ['cargo', 'run', '--quiet', '--bin', 'zkn-preflight']
slow-timeout = { period = "120s", terminate-after = 3 }

[[profile.default.scripts]]
setup = ['preflight']
```

`capture-stdout`/`capture-stderr` stay false so the banner streams straight to the terminal.

### What preflight does

1. **Resolve the test selection.** Intersect the filter expression with the per-binary mount inventory; prune archives no selected test references.
2. **Probe the cluster.** Resolve `KUBECONFIG`, list nodes, count `zaino-{ci,dev}-*` namespaces as a concurrency proxy.
3. **Resolve archives.** For each required `seed-{sha8}` PVC in `ztest-seeds`: ready → cached; not ready → attach to the reconcile Job's log stream; absent with local LFS blob → create PVC + reconcile Job; absent with remote blob → `git lfs pull` that pointer; pointer present but blob unreachable → soft-fail and proceed. Materialization flow: [design-architecture.md](design-architecture.md#archive-pvcs).
4. **Resolve snapshots.** For each `VolumeSnapshot` the selection clones, ensure its source PVC is ready (recurses into step 3) and the snapshot is bound.
5. **Emit a final banner and exit 0.**

Hard failures (cluster unreachable, auth, malformed manifest) exit ≠ 0; nextest treats a setup-script failure as a suite-level abort, so no test binary runs.

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

Markers: `✓` in target state; `⇣` in progress (refreshed in place); `!` soft failure — the run proceeds and only tests that need the affected resource fail at `TestEnv::build()`. Plain-ASCII fallback (`OK`/`..`/`WARN`, no escape codes) applies under `NO_COLOR=1` or a non-TTY stdout, so CI logs are diffable.

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

Each test binary publishes the `mount_archive!` / `mount_file!` / `mount_config!` paths it would invoke, via a `linkme` distributed slice compiled into a `&'static [Mount]` table. Preflight calls `<test-bin> --zkn-list-mounts` (parsed before nextest's arg-parser) and reads JSON on stdout, then intersects with the filter to build the work list. One `exec` per binary (~50 ms); the binary is already linked before setup scripts run.

## Namespace summary

|                     | Dev                                           | CI                                         |
| ------------------- | --------------------------------------------- | ------------------------------------------ |
| Namespace           | `ztest-dev-${user}-${nextest_pid}-${slot}`    | `ztest-ci-${run_id}-${slot}`               |
| Created by          | Library, first `TestEnv::build()` in slot     | Same                                       |
| Reused across tests | Within a slot, yes (sequential tests)         | Same                                       |
| End-of-run cleanup  | None (TTL controller GC, default 1h idle)     | Workflow step deletes ns by `run-id` label |
| Logs / metrics      | Cluster Loki + Prometheus (query by `run-id`) | Same                                       |
