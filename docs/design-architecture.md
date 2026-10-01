# Kubernetes substrate

How ztest maps a test suite onto a cluster: per-test namespaces, ownership-driven cleanup, networking,
the seed→snapshot→CoW volume pipeline.

- ztest = a **library** linked into each test binary — typed `k8s-openapi` objects, `kube-rs`
  server-side apply, `kube::runtime` watchers for readiness, typed handles back to the test
- `ztest run` = preflight + cluster orchestration + the engine ([design-execution-engine.md](design-execution-engine.md))
- API server has no public ingress; reached over Tailscale

## Cluster access

Both consumers authenticate as ServiceAccounts via `kube::Config::infer()` — an in-cluster runner pod
resolves to `InClusterConfig` (mounted SA token), a laptop to `KubeConfig` (`KUBECONFIG`). Run-SA RBAC
is provisioned by the resource graph at `ztest cluster setup` (`resource::impls::policy`):

| Resource                                                 | Scope         | Verbs                     |
| -------------------------------------------------------- | ------------- | ------------------------- |
| `pods`, `pvc`, `configmaps`, `events`, `volumesnapshots` | own namespace | `*`                       |
| `namespaces` (`ztest-*`)                                 | cluster       | `create`, `get`, `delete` |
| `volumesnapshotcontents`                                 | cluster       | `create`, `delete`, `get` |
| `pvc` in `ztest-seeds`                                   | `ztest-seeds` | `get`, `list`, `patch`    |
| `leases` in `ztest-meta`                                 | `ztest-meta`  | `*` (the QoS ledger)      |

## Namespace model — per `TestEnv`

One namespace per `TestEnv`, named `ztest-{package}-{test}-{suffix}` (`src/naming.rs`), slugs truncated
into the 63-char DNS-1123 limit, 8-hex suffix separating re-runs and `case_N`.

- Name is cosmetic (legible `kubectl get ns` during a hang) — cleanup, janitor and RBAC all select on
  `ztest.io/role=test-env` + the `janitor/ttl` annotation
- Untruncated identity lives in labels + the `ztest.io/test-full` annotation
- Components keep short stable names (`zebrad`, …) at `{name}.{ns}.svc.cluster.local`; concurrency needs
  no slot pattern, because different tests are in different namespaces
- `run_id` = `{user}-{random}`, minted once by `ztest` and injected as `ZTEST_RUN_ID` (= ledger lease
  name) — stamped on every resource so one run groups
- Per-test resources hang off a **sentinel** ConfigMap created at `build()` via `ownerReferences`; on
  `TestEnv` drop the sentinel goes and k8s GC cascades the rest

## Lifecycle

| Event                | Trigger                | Action                                                                          |
| -------------------- | ---------------------- | ------------------------------------------------------------------------------- |
| `TestEnv::build()`   | Library                | Ensure namespace (idempotent, 409 = success) + `ResourceQuota`; create sentinel |
| Component created    | Library                | Apply Pod/PVC/CM with `ownerReferences` → sentinel                              |
| `TestEnv` drops      | `Drop` + shutdown hook | Delete sentinel; resources cascade                                              |
| Run ends             | `reap_run` (parent)    | `delete_collection` by `ztest.io/run-id`                                        |
| Catastrophic failure | Namespace janitor      | Delete namespaces whose `janitor/ttl` expired                                   |

`janitor/ttl: 1h` is stamped even under `--no-cleanup` (which suppresses only the `Drop` teardown), so a
stale namespace never leaks permanently. The janitor is the unconditional backstop; everything else is
best-effort. Label-reap / run-id mechanics: [design-resources.md](design-resources.md).

## Networking

Config detected at init, `Endpoint`s routed through the right transport — test code never chooses.

- **In-cluster** (runner pod): direct TCP to `pod.status.podIP:container_port` over the pod CIDR
- **Out-of-cluster** (laptop): `127.0.0.1:ephemeral` → `kube-rs` portforward → API server → pod

Port-forwards are **lazy** (first `endpoint()` per pod+port), bound to an OS-assigned port
(`127.0.0.1:0`), closed when the `Endpoint`/`TestEnv` drops. No LoadBalancer/NodePort objects — the API
server portforward proxy is the only out-of-cluster path.

## Snapshot / clone

CSI snapshots the live PVC in place — no SIGTERM, no restart; clones boot crash-recovery-style, which
regtest validators tolerate. Mid-test cloning of a *running* component is not implemented: the shipped
machinery is the seed pipeline below, cloning a pre-provisioned immutable seed, never a live PVC.

## Seeds — content-addressed archive PVCs

Pre-baked PVC content (chain state) is content-addressed and bucket-hosted: git holds a manifest
recording `sha256`/`size_bytes`, the tree's files live in the snapshot bucket at `snap/<oid>/…`
([design-snapshots.md](design-snapshots.md)).

- Shipped chain snapshots = `ChainSnapshot` consts in `src/snapshots.rs`, manifests under
  `snapshots/<network>/*.toml`; a consuming crate declares its own the same way
- Referenced by `#[ztest::needs(CONST)]`; `artifact!` reads the manifest at compile time and fails the
  build when missing — no bytes are opened, so identity bakes in a build pod that cannot read them
- Seed PVCs are keyed by oid → tests referencing identical trees share one PVC

| Property   | Value                                                |
| ---------- | ---------------------------------------------------- |
| Namespace  | `ztest-seeds`                                        |
| Name       | `seed-{sha8}-{driver}`                               |
| Labels     | `seeds.ztest.io/sha`, `seeds.ztest.io/ready`         |
| Annotation | `last_accessed_at` (bumped per clone)                |
| Backing    | archive pool, `size=1` (recreatable from the bucket) |

Each seed has a paired `VolumeSnapshot`; tests always clone the snapshot, never the live PVC.

**Publishing** — boot the component, drive it to state, stop it, then:

```
ztest snapshot push <data-dir> --name <name> > snapshots/<net>/<name>.toml
```

`push` shells out to `rclone`, whose config holds the bucket credentials (setup in
[design-snapshots.md](design-snapshots.md#publishing)); ztest itself stores none. Reading never needs
any.

**Materialization** (`ztest run` preflight), per declared seed:

```
oid = the manifest's sha256 (compile-time; a runner pod has no checkout to hash)
if PVC seed-{sha8}-{driver} exists and labelled ready=true: reuse
else:
    atomically create the PVC (loser of a race falls through to reuse)
    spawn puller Job: attach PVC, rclone snap/<oid>/… into /seed, sha256sum -c SHA256SUMS
      pod dies → next pod reruns the copy, skipping files already whole
      success → label ready=true, create VolumeSnapshot
      failure → leave un-ready; next run retries
```

**Cross-namespace clone (shadow VSC).** PVC `dataSource` is namespace-local, so the library mints a
shadow `VolumeSnapshotContent` per cloned seed per test namespace, sharing the CSI `snapshotHandle` with
the archive's cluster-scoped VSC (one backend snap, many adopters). `build()` looks up the archive by
SHA (fail fast: `EnvError::ArchiveMaterializeFailed`), creates shadow VSC + shadow VolumeSnapshot in the
test ns, then the test PVC with `dataSource` = shadow snapshot. Teardown splits by scope:

- **Namespaced** shadow `VolumeSnapshot` — cascades from the sentinel
- **Cluster-scoped** shadow `VolumeSnapshotContent` — k8s GC
  [refuses ownerReferences from cluster-scoped dependents to namespaced owners](https://kubernetes.io/docs/concepts/architecture/garbage-collection/#owners-dependents),
  so the library deletes it explicitly on drop and the janitor sweeps orphans whose
  `snapshotRef.namespace` is gone

**GC** — a daily `CronJob` in `ztest-seeds` drops archive PVCs with `last_accessed_at` > 30 days, plus
reconcile-failure stragglers (no `ready=true`, > 1 h).

## Ownership cascade

- **Namespaced** (GC cascades): `Namespace` → sentinel ConfigMap → Pod, PVC, ConfigMap, shadow
  VolumeSnapshot. Cleaned by the library (sentinel on drop), `reap_run`, and the janitor (TTL)
- **Cluster-scoped** (GC does not cross scopes): shadow `VolumeSnapshotContent`. Cleaned by the library
  (explicit delete on drop) and the janitor (orphan-VSC sweep)

## Labels

Every library-created resource carries:

```yaml
labels:
  ztest.io/run-id:  "${ZTEST_RUN_ID}"
  ztest.io/user:    "${USER}"
  ztest.io/role:    "test-env"         # what cleanup/janitor/RBAC select on
  ztest.io/package: "<crate>"
  ztest.io/test:    "<slugged test>"   # untruncated in the ztest.io/test-full annotation
```

Metrics + profiling planes, Prometheus discovery, and what a run records: [design-observability.md](design-observability.md).
Component log capture: `src/logstream.rs` (kube-API fetch, timestamp-merged, capture-gated).

## Repo layout

```
ztest/
├── src/       ztest library
├── cli/       the `ztest` binary
├── ui/        terminal rendering (console, theme, panels)
├── macros/    ztest_macros: mount_config!, artifact!, dev!, qos tiers
├── attr/      ztest_attr: the #[sync_test] grammar, shared with the CLI's source scan
├── proto/     lightwalletd .proto (bindings checked in at src/proto/)
├── snapshots/ chain-snapshot manifests (.toml; the trees live in the bucket)
└── docs/
```

Test crates depend on `ztest` as a dev-dependency; macro reference in
[guide-writing-tests.md](guide-writing-tests.md).

## Non-goals

- Cross-namespace fixtures (one logical test spanning several namespaces)
- Restart-tolerant tests — use `--retries` or a CI re-run
- Non-Rust test drivers; no IPC surface
