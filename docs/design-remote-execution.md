# Remote execution — pod-per-test, on-cluster compile + build

On a remote cluster a test runs in a sibling **runner pod**, not a local child process: the cluster
compiles the binaries and builds every image, the laptop ships source, arbitrates admission, and renders
progress. One engine, one `Executor` seam, both targets — the cluster profile picks the delivery.

|                 | local (kind)                      | remote                           |
| --------------- | --------------------------------- | -------------------------------- |
| compile         | laptop (`cargo nextest --no-run`) | on-cluster BuildKit pod          |
| binary delivery | `hostPath` mount                  | baked into the runner image      |
| `PodRunConfig`  | `::hostpath`                      | `::baked`                        |
| images          | `docker build` + `kind load`      | on-cluster BuildKit build + push |

## The run, in order

1. **Prologue** (`cli/run.rs`) — parse flags; `cluster_config::activate` sets kube context, backend, and
   push/pull registries from the profile; force one `ZTEST_RUN_ID` (the teardown selector); start the
   console thread
1. **Probe** → `ClusterCapacity` (`qos`), kept live by `pipeline/capacity_watch`
1. **Admit across runs** — a k8s-Lease `ledger` reserves this run's fair-share slice; a 2 s `governor`
   keeps the scheduler ceiling in step with peers
1. **Compile on-cluster** (`pipeline/remote_compile.rs`) — ephemeral BuildKit pod, source shipped as a
   `git ls-files` tar, one multi-stage `buildctl` build of `docker/runner.Dockerfile` that compiles +
   pushes the runner image and exports the inventory (`list.json` + per-binary `ZTEST_DUMP_INVENTORY`
   dump), parsed exactly as the local path parses it
1. **Resource graph** (`resource/`, `plan_runtime`) — DAG builds/pushes component `dev!` images and
   materializes seeds (content-addressed PVC + snapshot → CoW shadow-clone per test). Idempotent,
   label-before-populate, reverse-topo teardown
1. **Run loop** (`engine/schedule.rs`) — sole admission authority; the pure `qos::Scheduler` packs by
   CPU×memory with priority + backfill, gated on resource-dep readiness, reconciled from the governor
1. **Pod-per-test** (`engine/pod_runner.rs`) — laptop creates the per-test namespace, then a Guaranteed
   single-container runner pod running `<bin> --exact <test> --nocapture` (labeled `ztest.io/run-id`),
   injecting the namespace via `ZTEST_TEST_NAMESPACE`; polls phase until Succeeded/Failed (exit code =
   verdict), timeout, or cancel
1. **In-pod `TestEnv::build`** (`env.rs`) — the body provisions its hermetic topology as sibling pods in
   the **laptop-provided** namespace (quota-capped): validators (warmed one block), then indexers. Wallet
   is **in-process**, no pod. Reads `ZTEST_TEST_NAMESPACE`, skips namespace create + teardown; `Drop` is
   a no-op on the pod path
1. **Logs & report** (`logstream.rs`, `engine/reporter.rs`) — at the test's terminal the laptop fetches
   every log over the kube API *before* deleting anything; `unified_output` assembles two sections
1. **Teardown** (`engine/pod_runner.rs`) — after the collector drains, delete this test's shadow VSCs
   (by `ztest.io/test-ns`: cluster-scoped, no namespace cascade), the per-test namespace (cascading
   pods, PVCs, quota), and the runner pod. `reap_run` by `run-id` is the crash-safety net; admission +
   lease release on exit

### Output assembly (step 9)

- Runner pod's own frame-stripped output first — its tracing *and* the panic/assertion — in full, uncapped
- Then component logs: each pod's kube-timestamped tail, merged chronologically, capped to the most
  recent 40 lines total
- Then any dead-pod terminal reason (OOMKilled/Evicted)
- Separate budgets on purpose: the runner is the test's primary voice, so a high-volume component (zaino
  health-checks every ~100 ms) can never evict it
- A plain fetch, not a live follow — the laptop owns teardown ordering, so pods still exist at fetch time
  and there is no attach-timing or mid-stream-EOF race
- Reporter is byte-identical to `cargo nextest run` (no `nextest-runner` dep) and replays this block only
  for FAILED tests (`--success-output`/`--failure-output` policy)

## Decisions that aren't obvious from the code

- **Baked binaries sit at their compile-time absolute path** (`runner` stage `COPY`s into
  `/cache/target/debug/deps/`) → the engine execs each by the exact `binary_path` the inventory reported;
  `::baked` needs no volume and no path map
- **`ZTEST_ENGINE=1`** marks the child orchestrated — a `TestEnv` refuses to provision outside a
  `ztest run` (the parent owns capacity admission)
- **`ZTEST_IMAGE_REFS`** carries a `DevImageId → pull ref` map into the pod: the baked image has no
  source tree, so an in-pod test cannot recompute a `dev-<hash>` and resolves by this map
- **ErrImagePull is terminal only after a grace window** — a run's pods pull one image at once and the
  kubelet throttles concurrent pulls; it self-heals
- **Pending is never a failure** — a pod parked on capacity waits indefinitely, bounded only by the
  per-test hard cap. Over-allocation never reddens a test
- **The laptop owns per-test teardown *because* it owns the logs** — collecting laptop-side while the pod
  deleted its own namespace would race the fetch, so namespace lifecycle moved to the laptop too. This is
  why the runner image no longer carries `kubectl`
- **glibc-dynamic only** — compile and runtime stages both pin Debian `bookworm` for one glibc
- **On-cluster BuildKit runs rootless**, on upstream's documented Kubernetes posture: `-rootless` image,
  uid 1000, `--oci-worker-no-process-sandbox`, Unconfined seccomp/AppArmor. No `privileged`, no
  `hostUsers: false`, no `CAP_SYS_ADMIN`. An earlier design took `RUN` steps to be impossible rootless on
  CRI-O; measured and disproved — `RUN` succeeds and the snapshotter selects `overlayfs`. Unconfined
  seccomp is the one thing a default `restricted` policy still rejects, so a cluster needs a narrowed SCC
  (or PSA label) admitting exactly that — [ops-cluster-requirements.md](ops-cluster-requirements.md#builder)
- **No fallback** — a failed build/push fails the run, never degrades to another path

See [design-execution-engine.md](design-execution-engine.md) (engine/console),
[design-qos.md](design-qos.md) (scheduler/ledger/governor),
[design-resources.md](design-resources.md) (the DAG),
[design-architecture.md](design-architecture.md) (namespaces/seeds).
