# Kubernetes Substrate

How `ztest` maps a test suite onto a cluster: per-slot namespaces, ownership-driven cleanup, networking, the seed→snapshot→CoW volume pipeline, and observability.

`ztest` is a **library** linked into each test binary; it builds typed `k8s-openapi` objects, applies them via `kube-rs` server-side apply, drives readiness with `kube::runtime` watchers, and hands typed component handles back to the test. Tests run under `cargo nextest run` in dev and CI. `ztest run` is a thin front-end that does preflight + cluster orchestration then shells out to nextest. The API server has no public ingress and is reached over Tailscale (see [ops-production-cluster.md](ops-production-cluster.md)).

## Cluster access

Both consumers authenticate as ServiceAccounts via `kube::Config::infer()`: the ARC runner pod resolves to `InClusterConfig` (mounted SA token); a developer laptop resolves to `KubeConfig` (`KUBECONFIG`). Both bind a `ztest-test-runner` SA (one in `arc-runners` for CI, one per developer namespace prefix), whose Role grants:

| Resource | Scope | Verbs |
| --- | --- | --- |
| `pods`, `pvc`, `configmaps`, `events`, `volumesnapshots` | own namespace | `*` |
| `namespaces` (names `ztest-{ci,dev}-*`, via `ValidatingAdmissionPolicy`) | cluster | `create`, `get`, `delete` |
| `volumesnapshotcontents` | cluster | `create`, `delete`, `get` |
| `pvc` in `ztest-seeds` | `ztest-seeds` | `get`, `list`, `patch` |

## Namespace model — per-slot

`cargo nextest` runs each `#[test]` as its own process across N parallel slots. The library derives a deterministic namespace per slot:

```
CI:  ztest-ci-${GITHUB_RUN_ID}-${SLOT}          e.g. ztest-ci-17234819-3
Dev: ztest-dev-${USER}-${NEXTEST_PID}-${SLOT}   e.g. ztest-dev-eli-48217-3
```

- `RUN_ID` = `GITHUB_RUN_ID` in CI (exposed as `ZTEST_RUN_ID`); dev has none, so `getppid()` (nextest's PID) disambiguates concurrent invocations.
- `SLOT` = `NEXTEST_TEST_GLOBAL_SLOT`, in `0..test_threads`. Hard cap 16: the library refuses to start when `SLOT >= 16`.

Sequential tests in a slot **share** its namespace for the slot's lifetime. Each pod name carries a per-test random suffix (chosen at `TestEnv::build()`) so pods from earlier tests never collide. Per-test resources are grouped under a **sentinel ConfigMap**, created at `build()`; every resource the test creates carries `ownerReferences` → its sentinel. On `TestEnv` drop the sentinel is deleted and k8s GC cascades the rest. A panicking test leaks only until the slot namespace is GC'd at end-of-run.

## Lifecycle

| Event | Trigger | Action |
| --- | --- | --- |
| First `TestEnv::build()` | Library | Ensure namespace + SA RoleBinding (idempotent); create sentinel |
| Component created | Library | Apply Pod/PVC/CM with `ownerReferences` → sentinel |
| `TestEnv` drops | `Drop` + tokio shutdown hook | Delete sentinel; resources cascade |
| nextest exits (CI) | CI step `kubectl delete ns -l ztest.io/run-id=…` | Delete run's namespaces + contents |
| nextest exits (dev) | — | Namespace lives until TTL GC (default 1h idle) |
| Catastrophic failure | Cluster namespace janitor | Delete namespaces whose `janitor/ttl` annotation expired |

The janitor ([`kube-janitor`](https://codeberg.org/hjacobs/kube-janitor), keyed on the `janitor/ttl` annotation the library stamps on each namespace) is the unconditional backstop; everything else is best-effort.

## Networking

The library detects its config at init and routes `Endpoint`s from component handles through the right transport; test code never chooses.

- **In-cluster** (test bin in ARC pod): direct TCP to `pod.status.podIP:container_port` over the pod CIDR.
- **Out-of-cluster** (laptop): TCP to `127.0.0.1:ephemeral` → `kube-rs` portforward → API server (Tailscale) → `pod:container_port`.

Port-forwards are **lazy** (established on first `endpoint()` per pod+port), bound to an OS-assigned ephemeral local port (`127.0.0.1:0`), and closed when the `Endpoint` (and `TestEnv`) drops. There are no Service/LoadBalancer/NodePort objects; the API server portforward proxy is the only out-of-cluster path.

## Snapshot / clone

Ceph RBD snapshots the live PVC in place — no SIGTERM, no restart; clones boot crash-recovery-style, which regtest validators tolerate. Mid-test cloning of a *running* component is not implemented: the snapshot machinery ztest ships today is the seed pipeline below, which clones a pre-provisioned, immutable seed rather than a live PVC.

## Seeds — content-addressed archive PVCs

Pre-baked PVC content (chain/indexer state) is **content-addressed and bucket-hosted**: git holds a sidecar `<stem>.toml` manifest recording the archive's `sha256`/`size_bytes`, and the bytes live in the snapshot bucket at `lfs/<oid>` (see [design-snapshots.md](design-snapshots.md)). The chain snapshots ztest ships are `ChainSnapshot` consts in `src/snapshots.rs`, manifests under `fixtures/chains/snapshots/<network>/<upgrade>.toml`; a consuming test crate declares its own the same way. Tests reference them via `#[ztest::needs(CONST)]` or `mount_archive!` (see [guide-writing-tests.md](guide-writing-tests.md)), which reads the sidecar manifest at compile time and fails the build if it is missing — the archive bytes are never opened, so identity bakes in a build pod that cannot read them. Archive PVCs are keyed by that oid, so tests referencing identical bytes share one archive PVC.

| Property | Value |
| --- | --- |
| Namespace | `ztest-seeds` |
| Name | `seed-{sha8}` |
| Labels | `seeds.ztest.io/sha`, `seeds.ztest.io/ready` |
| Annotation | `last_accessed_at` (bumped per clone) |
| Backing | Ceph archive pool, `size=1` (recreatable from the snapshot bucket) |

Each archive has a paired `VolumeSnapshot`; tests always clone from the snapshot, never the live PVC.

**Publishing.** Boot the component locally, drive it to state, then:

```
tar -I zstd -cf tests/assets/<name>.tar.zst -C <data-dir> .
ztest snapshot manifest <archive> > snapshots/<net>/zebra-<ver>-<up>.toml && ztest snapshot push <archive>
```

**Materialization** (lazy, on first use). At `TestEnv::build()`, per archive mount:

```
sha = sha256(file at source)
if PVC seed-{sha8} exists and labelled ready=true: reuse
else:
    atomically create the PVC (loser of a race falls through to reuse)
    spawn reconcile Job: attach PVC, `kubectl exec` stream tarball in, `tar -xf`
      success → label ready=true, create VolumeSnapshot
      failure → leave un-ready; next build() retries
```

**Cross-namespace clone (shadow VSC).** PVC `dataSource` is namespace-local, so the library mints a shadow `VolumeSnapshotContent` per cloned seed per slot namespace, sharing the CSI backend snapshot handle — the archive's cluster-scoped VSC and the shadow VSC point at the same `snapshotHandle` (one Ceph RBD snap, many adopters). At `build()` it looks up the archive by SHA (fail fast with `EnvError::ArchiveMaterializeFailed`), creates the shadow VSC + shadow VolumeSnapshot in the test ns, then the test PVC with `dataSource` = shadow snapshot. Teardown splits by scope:

- **Namespaced** shadow `VolumeSnapshot` — cascades from the sentinel.
- **Cluster-scoped** shadow `VolumeSnapshotContent` — k8s GC [refuses ownerReferences from cluster-scoped dependents to namespaced owners](https://kubernetes.io/docs/concepts/architecture/garbage-collection/#owners-dependents). The library deletes it explicitly on `TestEnv` drop; the janitor sweeps orphan VSCs whose `snapshotRef.namespace` no longer exists.

**GC.** A daily `CronJob` in `ztest-seeds` drops archive PVCs with `last_accessed_at` > 30 days and reconcile-failure stragglers (no `ready=true`, > 1h).

## Ownership cascade

- **Namespaced** (k8s GC cascades): `Namespace ztest-{ci,dev}-…` → sentinel ConfigMap → Pod, PVC, ConfigMap, shadow VolumeSnapshot. Cleaned by the library (sentinel on drop), CI (`delete ns -l ztest.io/run-id=…`), and the janitor (TTL).
- **Cluster-scoped** (GC does not cross scopes): shadow `VolumeSnapshotContent`. Cleaned by the library (explicit delete on drop) and the janitor (orphan-VSC sweep).

Label-reap / run-id teardown mechanics are owned by [design-resources.md](design-resources.md).

## Observability

Cluster-resident infrastructure in the `observability` namespace captures logs, metrics, and events continuously; the harness and CI collect nothing per-run. Pods are scraped once their namespace exists; data persists per the retention windows after deletion.

| Component | Role | Retention |
| --- | --- | --- |
| Promtail (DaemonSet) | Tail every container log on every node | — |
| Loki | Log store, indexed by k8s labels | 7 d |
| Prometheus | Scrape `prometheus.io/scrape=true` pods | 30 d |
| kube-state-metrics | Surface k8s object state as metrics | (scraped) |
| Grafana | Dashboards + ad-hoc queries (Tailscale) | — |

Every library-created resource carries:

```yaml
labels:
  ztest.io/run-id:    "${RUN_ID}"        # or dev-${USER}-${NEXTEST_PID}
  ztest.io/slot:      "${SLOT}"
  ztest.io/test:      "<test-name>"      # pods only
  ztest.io/component: "zebrad|zcashd|zaino"
```

Promtail forwards each label as a Loki stream label. Post-mortem query:

```
{ztest_io_run_id="17234819-1", ztest_io_test="indexer::wallet_sync"}
kube_event{ztest_io_run_id="17234819-1"}
```

Components with a metrics port are annotated `prometheus.io/scrape=true` + `prometheus.io/port=<n>` by the library. Durable output is emitted as a structured stdout log line, which reaches Loki.

## Repo layout

```
ztest/
├── src/      ztest library + `ztest` CLI binary
├── macros/   ztest_macros: mount_file!, mount_archive!, dev!
├── fixtures/ regtest configs, kind/k3s class manifests
├── proto/    vendored protobuf bindings
└── docs/
```

Test crates depend on `ztest` as a dev-dependency; the macro reference lives in [guide-writing-tests.md](guide-writing-tests.md).

## Non-goals

- Cross-namespace fixtures (one logical test spanning multiple namespaces).
- Restart-tolerant tests; use `nextest --retries` or GitHub "Re-run failed jobs".
- Non-Rust test drivers; no IPC surface.
- Per-namespace resource quotas.
