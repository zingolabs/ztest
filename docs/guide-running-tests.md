# Running tests

Cluster tests run through **`ztest run`**, never bare `cargo nextest`.

- Arguments after `run` forward verbatim to nextest — migration is `s/cargo nextest/ztest/`
- `TestEnv::build()` refuses outside the orchestrator (`ZTEST_ENGINE`), naming the command to use;
  a bare `cargo test` would otherwise create unbudgeted pods on whatever kubeconfig is loaded
- ztest owns the run loop itself (`src/engine/`); nextest is invoked only for `list`

## Requirements

- `cargo nextest` on PATH (inventory + selection)
- A reachable cluster from `kube::Config::infer()`: in-pod SA token, or a `KUBECONFIG`
- Cluster capabilities present — `ztest cluster check`
  ([ops-cluster-requirements.md](ops-cluster-requirements.md))
- Dev-image distribution: `kind load` locally, registry push remotely
  ([ops-clusters.md](ops-clusters.md))

## Invoking

```bash
ztest run -p zaino-integration-tests
ztest run -p zaino-integration-tests indexer::wallet_sync     # substring on crate::module::test_fn
ztest run -E 'test(reorg)'                                    # nextest filter DSL
ztest run --cluster okd-home -p e2e                           # named profile: context + class + registry
ztest run --no-cleanup -E 'test(flaky_one)'                   # keep the namespace for a post-mortem
ztest run -R latest                                           # rerun what didn't pass last time
```

- `--cluster` and `-R/--rerun` must appear **before** the nextest args (everything after is forwarded)
- `-j`/`--test-threads` is advisory — the engine auto-scales concurrency to QoS capacity
- Engine consumes `--retries`, `--fail-fast`/`--no-fail-fast`, `--no-capture`, `-P/--profile`,
  `--message-format`, `--success-output`/`--failure-output` directly
- `--no-capture` serializes (nextest's `test_threads = 1` coupling) and streams live, so the pinned TTY
  panel steps aside

## Namespaces and cleanup

One namespace per `TestEnv`, `ztest-{package}-{test}-{suffix}`; every resource is labeled
`ztest.io/run-id` ([design-architecture.md](design-architecture.md)).

- Normal exit: the laptop tears the namespace down after collecting logs
- Ctrl-C: teardown runs in the surviving parent, `reap_run` by run-id, 30 s deadline
- Crash: the 1 h `janitor/ttl` annotation is the unconditional backstop
- `--no-cleanup` suppresses only the `Drop` teardown; the TTL still applies, so nothing leaks permanently

```bash
ztest cleanup                # your finished runs and syncs; --dry-run previews
ztest cleanup --all-users
```

`cleanup` skips anything live (in-flight run, `Running` sync) unless `--force`, and never touches the
cluster itself or the seed cache.

## What a run does, in order

1. **Probe** the cluster → capacity, kept live during the run
1. **Admit** across concurrent runs via the k8s-Lease ledger ([design-qos.md](design-qos.md))
1. **Inventory** — `cargo nextest list --message-format=json` + a per-binary `ZTEST_DUMP_INVENTORY` dump
   (QoS tiers, `dev!` images, declared seeds)
1. **Resource graph** — build/push `dev!` images, materialize declared seeds into content-addressed PVCs
   - snapshots ([design-resources.md](design-resources.md))
1. **Run loop** — scheduler grants by tier footprint, one process (or pod) per test

`ztest run describe` prints the plan without running it ([design-describe.md](design-describe.md)).

## Seeds at preflight

Per required `seed-<sha8>-<driver>` PVC in `ztest-seeds`:

- ready → cached
- not ready → attach to the puller Job's log stream
- absent → create PVC + puller Job, which `curl`s the public URL for `lfs/<oid>` straight
  into `tar -x` — bytes go **R2 → node**, never through ztest or the apiserver

Archives are gitignored, so a checkout holds none and nothing is fetched at clone time. The OID comes
from `snapshots/<network>/zebra-<version>-<upgrade>.toml`, read at compile time, along with the
`base_uri`/`key_prefix` the bytes are fetched from. Reads from object storage are public.

`ztest cluster check` will verify that configured seeds are reachable from the current cluster

A missing seed fails only the tests that declared it (`#[ztest::needs]`), as a skip with a named reason —
never the whole run.

## Failure modes

| Condition                         | Effect                                                      |
| --------------------------------- | ----------------------------------------------------------- |
| Cluster unreachable / auth failed | Run aborts before anything is created                       |
| Missing required capability       | `ztest cluster setup` / `check` names it; run refuses       |
| Bucket unreachable, object absent | Tests declaring that seed skip; the rest run                |
| Image build/push failed           | Run fails — there is no fallback path                       |
| Pod `Pending` on capacity         | Waits; bounded only by the tier's hard cap, never a failure |
| Test exceeds hard cap             | SLOW at 1×, killed at 2× (janitor reaps the namespace)      |

## Output

- Reporter is byte-identical to `cargo nextest run`, one divergence: the captured block is stripped of
  libtest's per-run framing ([design-execution-engine.md](design-execution-engine.md))
- A failing test's block carries the runner's own output in full, then component logs (chronologically
  merged, most recent 40 lines), then any pod terminal reason (OOMKilled/Evicted)
- Every run is recorded: `ztest store list`, `ztest replay <run>`, `ztest run --rerun`

## CI

One job on a stock GitHub runner; every expensive operation runs on the cluster.

```yaml
env:
  ZTEST_RUN_ID: ${{ github.run_id }}-${{ github.run_attempt }}
  ZTEST_IMAGE_REGISTRY: ghcr.io/${{ github.repository_owner }}

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: kubeconfig
        run: |
          mkdir -p ~/.kube
          echo "${{ secrets.KUBECONFIG_B64 }}" | base64 -d > ~/.kube/config
      - name: registry login
        run: echo "${{ secrets.GITHUB_TOKEN }}" | docker login ghcr.io -u ${{ github.actor }} --password-stdin
      - run: ztest run -p clientless -p e2e
      - if: always()
        run: kubectl delete ns -l ztest.io/run-id=$ZTEST_RUN_ID
```

- Auth = a ServiceAccount-token kubeconfig; the SA needs namespace CRUD, VolumeSnapshot create,
  node/CSIDriver read, and `Lease` CRUD in `ztest-meta`
- Images push to `ghcr.io` with `GITHUB_TOKEN`; the cluster pulls over egress, so no cluster ingress
- `ZTEST_RUN_ID` labels every resource → the cleanup step and cluster-resident observability filter by
  run; no artifact-collection step, query by `run-id`
- Skipped cleanup (preempted runner) falls to the TTL janitor
