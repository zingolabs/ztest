# Remote execution — pod-per-test, on-cluster compile + build

On a remote cluster a test runs inside a sibling **runner pod** instead of a
local child process. The cluster compiles the binaries and builds every image;
the laptop only ships source, arbitrates admission, and renders progress. One
engine and one `Executor` seam serve both targets — the cluster profile picks
the delivery.

| | local (kind) | remote |
| --- | --- | --- |
| compile | laptop (`cargo nextest --no-run`) | on-cluster BuildKit pod |
| binary delivery | `hostPath` mount | baked into the runner image |
| `PodRunConfig` | `::hostpath` | `::baked` |
| images | `docker build` + `kind load` | on-cluster BuildKit build + push |

## The run, in order

1. **Prologue** (`cli/run.rs`). Parse flags; `cluster_config::activate` sets the
   kube context, backend, and push/pull registries from the profile; force one
   `ZTEST_RUN_ID` (the teardown selector); start the console thread.
2. **Probe** → `ClusterCapacity` (`qos`), kept live by `pipeline/capacity_watch`.
3. **Admit across runs** — a k8s-Lease `ledger` reserves this run's fair-share
   slice; a 2 s `governor` keeps the scheduler ceiling in step with peers.
4. **Compile on-cluster** (`pipeline/remote_compile.rs`, remote clusters). Stand up an
   ephemeral BuildKit pod; ship source as a `git ls-files` tar; one multi-stage
   `buildctl` build of `docker/runner.Dockerfile` compiles + pushes the runner
   image and exports the inventory (`list.json` + per-binary
   `ZTEST_DUMP_INVENTORY` dump), parsed the same way the local path parses it.
5. **Resource graph** (`resource/`, `plan_runtime`). A DAG builds/pushes component
   `dev!` images and materializes seeds (content-addressed PVC + snapshot →
   CoW shadow-clone per test). Idempotent, label-before-populate, reverse-topo
   teardown.
6. **Run loop** (`engine/schedule.rs`) — the sole admission authority. The pure
   `qos::Scheduler` packs tests by CPU×memory footprint with priority + backfill,
   gated on resource-dep readiness, reconciled from the governor ceiling.
7. **Pod-per-test** (`engine/pod_runner.rs`). The laptop creates the per-test
   namespace, then a Guaranteed single-container runner pod running
   `<bin> --exact <test> --nocapture` (labeled `ztest.io/run-id`), injecting the
   namespace name via `ZTEST_TEST_NAMESPACE`. It polls the pod's phase until
   Succeeded/Failed (exit code = verdict), timeout, or cancel.
8. **In-pod `TestEnv::build`** (`env.rs`). The body provisions its own hermetic
   topology as sibling pods in the **laptop-provided** namespace (quota-capped):
   validators (warmed one block), then indexers. The **wallet is in-process** —
   no pod. It reads `ZTEST_TEST_NAMESPACE` and skips namespace create + teardown;
   `Drop` is a no-op on the pod path (the laptop owns that).
9. **Logs & report** (`logstream.rs`, `engine/reporter.rs`). At the test's terminal
   the laptop fetches every log definitively over the kube API — *before* deleting
   anything — and `unified_output` assembles **two sections**: first the runner
   pod's own frame-stripped output (its tracing *and* the panic/assertion) in full,
   uncapped; then the supporting component logs (each pod's kube-timestamped tail,
   merged chronologically and capped to the most recent 40 lines total); then any
   dead-pod terminal reason (OOMKilled/Evicted). The runner and component sections
   draw from separate budgets on purpose: the runner is the test's primary voice,
   so a high-volume component (zaino health-checks every ~100 ms) can never evict
   it. A plain fetch, not a live follow: since the laptop owns teardown ordering,
   the pods still exist at fetch time, so there's no attach-timing or
   mid-stream-EOF race. The reporter is byte-identical to `cargo nextest run` (no
   `nextest-runner` dep) and replays this block only for FAILED tests
   (`--success-output`/`--failure-output` policy).
10. **Teardown** (`engine/pod_runner.rs`). After the collector drains, the laptop
    deletes this test's shadow VSCs (by the `ztest.io/test-ns` label — cluster-
    scoped, so no namespace cascade), the per-test namespace (cascading its pods,
    PVCs, quota), and the runner pod. `reap_run` by `run-id` is the crash-safety
    net; admission + lease release on exit.

## Decisions that aren't obvious from the code

- **Baked binaries sit at their compile-time absolute path** (`runner` stage
  `COPY`s into `/cache/target/debug/deps/`), so the engine execs each by the exact
  `binary_path` inventory reported — `::baked` needs no volume and no path map.
- **`ZTEST_ENGINE=1`** on the pod marks the child orchestrated; a `TestEnv`
  refuses to provision outside a `ztest run` (the parent owns capacity admission).
- **`ZTEST_IMAGE_REFS`** carries a `DevImageId → pull ref` map into the pod: the
  baked image has no source tree, so an in-pod test can't recompute a `dev-<hash>`
  and resolves component images by this map instead.
- **ErrImagePull is terminal only after a grace window** — a run's pods pull the
  same image at once and the kubelet throttles concurrent pulls; it self-heals.
- **Pending is never a failure.** A pod parked on capacity waits indefinitely; the
  only bound is the per-test hard cap. Over-allocation never reddens a test.
- **The laptop owns per-test teardown *because* it owns the logs.** Collecting logs
  laptop-side while the pod deletes its own namespace would race the fetch, so
  namespace lifecycle moved to the laptop too: it creates the namespace, and at
  the test's terminal fetches every pod's logs, then deletes. The in-pod `Drop`'s
  historical reason (a `?`-return skipping teardown) is moot — the laptop tears
  down unconditionally after the pod finishes, and `reap_run` covers a laptop
  crash. This is why the runner image no longer carries `kubectl`.
- **glibc-dynamic only.** Compile and runtime stages both pin Debian `bookworm`
  for one glibc.
- **On-cluster BuildKit runs rootless**, on upstream's documented Kubernetes
  posture: the `-rootless` image, uid 1000, `--oci-worker-no-process-sandbox`,
  and Unconfined seccomp/AppArmor. No `privileged`, no `hostUsers: false`, no
  `CAP_SYS_ADMIN`. An earlier design took `RUN` steps to be impossible rootless
  on CRI-O; that was measured and disproved — `RUN` succeeds, and the
  snapshotter selects `overlayfs`. Unconfined seccomp is the one thing a default
  `restricted` policy still rejects, so a cluster needs a narrowed SCC (or PSA
  label) admitting exactly that. See
  [ops-cluster-requirements.md](ops-cluster-requirements.md#builder).
- **No fallback.** A failed build/push fails the run — it never degrades to
  another path.

See [design-execution-engine.md](design-execution-engine.md) (engine/console),
[design-qos.md](design-qos.md) (scheduler/ledger/governor),
[design-resources.md](design-resources.md) (the DAG),
[design-architecture.md](design-architecture.md) (namespaces/seeds).
</content>
