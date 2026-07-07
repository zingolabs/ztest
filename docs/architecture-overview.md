# Architecture

## Shape

```
  ┌──────────────────────────┐
  │ Dev laptop               │
  │   kubeconfig + kube-rs   │──┐
  │   (over Tailscale)       │  │
  └──────────────────────────┘  │     ┌─────────────────────┐     ┌────────────┐
                                ├───► │  kube API server    │───► │    Pods    │
  ┌──────────────────────────┐  │     │   apply / watch /   │     │   zebrad   │
  │ ARC runner pod           │  │     │   port-forward      │     │   zaino    │
  │   in-cluster SA token    │──┘     └─────────────────────┘     │   zingo    │
  │                          │                                    └────────────┘
  │   cargo nextest          │
  │     ├─ test bin          │
  │     ├─ test bin          │
  │     └─ test bin          │
  │       └─ ztest           │
  └──────────────────────────┘
```

`ztest` is a **library** linked into each test binary. It builds k8s manifests,
applies them via `kube-rs`, waits on watchers, and exposes typed handles to the test code. Tests run under `cargo nextest run`
in both dev and CI — no wrapper between them.

The "API" the test author sees is a Rust API in the lib; the API the lib uses is the kube API. End-of-run cleanup is plain
`kubectl delete ns -l …` in a CI workflow step. Logs and metrics are not collected per-run — cluster-resident observability
captures them continuously (see [Observability](#observability)).

## Cluster access

The k8s API server is reachable over the cluster's Tailscale (see [cluster-administration.md](cluster-administration.md)).
Both consumers authenticate as ServiceAccounts; the cluster API has no public ingress.

| Consumer         | Credential                                                   | Resolution                                                |
| ---------------- | ------------------------------------------------------------ | --------------------------------------------------------- |
| ARC runner pod   | Mounted SA token at `/var/run/secrets/.../token`             | `kube::Config::infer()` → `InClusterConfig`               |
| Developer laptop | Personal kubeconfig pointing at tailnet IP for Control-Plane | `kube::Config::infer()` → `KubeConfig` (env `KUBECONFIG`) |

**ServiceAccount model.** Both ARC and developers authenticate as
`zaino-test-runner` ServiceAccounts (one in `arc-runners` namespace for
CI, one per developer namespace prefix for dev). Each has a Role that
permits:

- `pods`, `pvc`, `configmaps`, `events`, `volumesnapshots` (`*`) — own
  namespace only
- `namespaces` (`create`, `get`, `delete`) — restricted to names
  matching `zaino-{ci,dev}-*` via an admission policy (`ValidatingAdmissionPolicy`)
- `volumesnapshotcontents` (cluster) — `create`, `delete`, `get` only
  (load-bearing for the shadow-VSC dance)
- `pvc` in `ztest-seeds` — `get`, `list`, `patch` (read archives + bump
  `last_accessed_at`)

## Namespace model — per-slot

Tests run under `cargo nextest`, which schedules each `#[test]` as its
own process across N parallel slots (`--test-threads N`, capped at 16).
The library computes a deterministic namespace name from the slot:

```
CI:  ztest-ci-${GITHUB_RUN_ID}-${SLOT}              # e.g. ztest-ci-17234819-3
Dev: ztest-dev-${USER}-${NEXTEST_PID}-${SLOT}        # e.g. ztest-dev-eli-48217-3
```

- `RUN_ID` = `GITHUB_RUN_ID` in CI (set as `ZTEST_RUN_ID` env);
  dev has no run-id — `getppid()` (nextest's PID) disambiguates
  concurrent `cargo nextest` invocations instead.
- `SLOT` = `NEXTEST_TEST_GLOBAL_SLOT` (nextest 0.9.x+), in `0..test_threads`.
- Hard cap: 16. The library refuses to start a test if `SLOT >= 16` —
  protects against runaway `--test-threads`.

Within one nextest run, sequential tests in the same slot **share the
namespace** for the slot's lifetime. Each pod's name carries a
per-test suffix (random short token chosen by the library at
`TestEnv::build()`) so concurrently-living pods from past tests in the
same slot don't collide. Per-test resources are scoped via a sentinel:

```
  Namespace: ztest-ci-${RUN_ID}-3       (lifetime ≈ slot 3's lifetime)
  ┌──────────────────────────────────────────────────────────────┐
  │  sentinel-cm-${test_a_uid}                                   │
  │    ├─ Pod  zebrad-<suffix>                                   │
  │    ├─ PVC  data-<suffix>                                     │
  │    └─ ConfigMap  zebrad-cfg-<suffix>                         │
  │       (each carries ownerRef → sentinel-cm-${test_a_uid})    │
  │                                                              │
  │  sentinel-cm-${test_d_uid}                                   │
  │    └─ Pod, PVC, ConfigMap …                                  │
  └──────────────────────────────────────────────────────────────┘

  Lifecycle inside slot 3 (single nextest run):
    test_a runs ─► TestEnv drops ─► sentinel-A deleted ─► cascade
    test_d runs ─► TestEnv drops ─► sentinel-D deleted ─► cascade
    …
    nextest exits  ─► namespace deleted by CI cleanup step
                      (or kube-janitor TTL in dev)
```

The sentinel ConfigMap is created at `TestEnv::build()`; every resource
the test creates carries `ownerReferences` → sentinel. On `TestEnv`
drop, the sentinel is deleted and cascades take down the rest. A test
that panics or crashes leaks only until its slot's namespace is GC'd at
end-of-run (or by TTL backstop).

## Lifecycle

| Event                       | Triggered by                                                | Action                                                                |
| --------------------------- | ----------------------------------------------------------- | --------------------------------------------------------------------- |
| Test process starts         | Library, on first `TestEnv::build()`                        | Ensure namespace (idempotent); ensure SA RoleBinding; create sentinel |
| Test creates components     | Library                                                     | Apply Pod/PVC/CM manifests with `ownerReferences` → sentinel          |
| `TestEnv` drops             | Library, via `Drop` + tokio shutdown hook                   | Delete sentinel; resources cascade                                    |
| `cargo nextest` exits (CI)  | CI workflow step (`kubectl delete ns -l ztest.io/run-id=…`) | Namespaces + everything in them                                       |
| `cargo nextest` exits (dev) | nothing immediate                                           | Namespace lives until TTL controller GC's it (default 1h idle)        |
| Catastrophic failure        | Cluster-resident namespace janitor                          | Delete namespaces whose TTL annotation has expired                    |

The namespace janitor is the unconditional backstop. Everything else
is best-effort. v1 plans to use [`kube-janitor`](https://codeberg.org/hjacobs/kube-janitor)
configured against the `janitor/ttl` annotation set by the library on
each namespace it creates — see `apps/janitor/` in
[cluster-administration.md](cluster-administration.md). No custom
controller; if `kube-janitor`'s annotation model proves limiting we
revisit.

## What the library does

```
ztest/
├── src/          the ztest library + the `ztest` CLI binary
├── macros/       ztest_macros: mount_file!, mount_archive!, dev!
├── fixtures/     regtest configs, kind/k3s class manifests
├── proto/        vendored protobuf bindings
└── docs/
```

Two crates: the `ztest` library and the `ztest_macros` proc-macro crate.
Test crates depend on `ztest` as a `dev-dependency`. The `ztest` CLI
(`ztest run`) is a thin wrapper that shells out to `cargo nextest run`
after preflight and cluster orchestration. No `_proto`, no `_client`.

## Networking

```
  In-cluster   (test bin runs inside an ARC pod)
  ──────────────────────────────────────────────────────────────────────
    test bin  ── TCP ──►  pod.status.podIP : container_port
                          (direct over pod CIDR; no proxy)

  Out-of-cluster   (test bin runs on a dev laptop)
  ──────────────────────────────────────────────────────────────────────
    test bin  ── TCP ──►  127.0.0.1 : ephemeral
                                │
                                │  kube-rs portforward
                                ▼
                          kube API server  (over Tailscale)
                                │
                                ▼
                          pod : container_port
```

The library detects which case applies at init (`InClusterConfig` ⇒
direct; otherwise port-forward) and `Endpoint`s returned by component
handles transparently route through the right transport. Test code
doesn't choose.

Port-forward allocation:

- Lazy: forward set up on first `endpoint()` call per (pod, port).
- Local port: OS-assigned ephemeral (bind to `127.0.0.1:0`, read back).
- Lifetime: tied to the `Endpoint` value's `Drop`. When the
  `ValidatorHandle` drops (`TestEnv` drops), pending forwards close.

No Service objects, no LoadBalancer, no NodePort. The kube API server's
portforward proxy is the only mechanism.

Pod-to-pod (inside the cluster): direct TCP via pod IPs. Wiring peers
into mounted config files is an open question — the call-site API is
`t.peer(&alice, &bob)` (see
[test-author-api.md#peering](test-author-api.md#peering)) but the
substitution mechanism (template strings? `envsubst` over the
ConfigMap? env vars in the container?) is not yet decided. v1 will
pick one and document it here.

## Snapshot / clone

Ceph RBD snapshots the live PVC — no SIGTERM, no restart. Clones boot
crash-recovery-style; regtest validators handle this.

```
  test                library                kube                pod-A
   │                     │                     │                   │
   │ mine_blocks(100) ───┼─────────────────────┼─────────────────► │  running
   │                     │                     │                   │
   │ snapshot()      ────►  create             │                   │
   │                     │  VolumeSnapshot  ──►│                   │  still
   │                     │                     │                   │  running
   │ ◄── SnapshotRef ────│                     │                   │
   │                     │                     │                   │
   │ spawn(…             │                     │                   │
   │   mount(from snap)) │                     │                   │
   │                 ────►  create PVC      ──►│                   │
   │                     │  (dataSource=snap)  │                   │
   │                     │  + Pod-B         ──►│ ─── boots ──►   pod-B
   │ ◄── Ref<Validator>──│                     │                   │
```

A `SnapshotRef` is owned by the sentinel of the test that created it.
It lives until the test's `TestEnv` drops, then cascades.

## Seeds — content-addressed archive PVCs

Pre-baked PVC content (chain state, indexer state) lives **in the
consuming test crate**, not in this repo. Convention:
`<test-crate>/tests/assets/*.tar.zst`, committed via Git LFS. There is
no `seeds/` folder here, no static catalog, no manual lock file.

Tests reference assets via `mount_archive!` (see
[test-author-api.md](test-author-api.md#mounts)) which resolves the
path relative to `CARGO_MANIFEST_DIR` at compile time and fails the
build if the file is missing. The runtime payload is the absolute path
— test binaries don't embed multi-GB tarballs.

LFS owns content delivery in the consuming repo; Ceph owns CoW
snapshots in the cluster. Archive PVCs are content-addressed: the lib
hashes the tarball at first use and dedupes against existing archives
by SHA. Two tests referencing identical bytes share one archive PVC.

### Publishing

Engineer workflow, in whatever repo owns the tests: boot the component
locally, drive it to state, `tar -I zstd -cf tests/assets/<name>.tar.zst -C <data-dir> .`, `git lfs track` + commit + PR. No cluster-side
reproducibility check; if reproducibility matters, commit the Rust
harness alongside.

### Materialization (lazy, on first use)

```
  At TestEnv::build(), for each Mount { kind: DirArchive, source: <abs> }:

    sha = sha256(file at source)

    if PVC seed-{sha[..8]} exists and labelled ready=true:
        reuse it
    else:
        atomically create the PVC (loser of any race → reuse)
        spawn reconcile Job:
            attach PVC, `kubectl exec` stream tarball in, `tar -xf`
            on success: label ready=true, create VolumeSnapshot
            on failure: leave un-ready; next build() retries
```

The test process reads the tarball directly from local fs (same pod in
CI; same fs in dev). Bytes reach the reconcile Job via `kubectl exec`
streaming into a worker pod attached to the target PVC. Multiple tests
racing to materialize the same SHA: optimistic-concurrency creation;
loser falls through to "get existing."

Archive PVC: namespace `ztest-seeds`, name `seed-{sha8}`, labels
`seeds.ztest.io/{sha,ready}`, annotation `last_accessed_at` (bumped
per clone). Backed by the Ceph archive pool (`size=1`, see
[cluster-administration.md#cluster-shape](cluster-administration.md#cluster-shape))
— archives are recreatable from LFS, no point paying 3× replication.
Each archive has a paired `VolumeSnapshot`; tests clone from the
snapshot, never the live PVC.

### Cross-namespace clone (shadow VSC)

PVC `dataSource` is namespace-local. The lib mints a shadow
`VolumeSnapshotContent` per cloned seed per slot-ns, sharing the CSI
backend snapshot handle. This is the only piece of the seed story with
real complexity, and k8s forces it on us.

```
  ztest-seeds  (archive ns)              ztest-ci-X-N  (test ns)
  ─────────────────────────              ────────────────────────────
   archive PVC                             test PVC ◄── dataSource
       │                                       ▲
       ▼                                       │
   VolumeSnapshot                          VolumeSnapshot (shadow)
       │                                       │
       ▼                                       ▼
   VolumeSnapshotContent              VolumeSnapshotContent (shadow,
      (cluster-scoped)                   cluster-scoped, no owner)
       │                                       │
       └──────────────────┬────────────────────┘
                          │   same snapshotHandle
                          ▼
                  ┌───────────────────┐
                  │  Ceph RBD snap    │ ◄── one backend snapshot,
                  └───────────────────┘     many adopters
```

Resolution at `build()`: look up archive by SHA, fail fast with
`EnvError::ArchiveMaterializeFailed` on failure; create shadow VSC +
shadow VolumeSnapshot in the test ns; create test PVC with
`dataSource = shadow snapshot`.

Teardown: the namespaced shadow `VolumeSnapshot` cascades from the
sentinel normally. The cluster-scoped shadow `VolumeSnapshotContent`
**cannot** — k8s GC refuses ownerReferences from cluster-scoped
dependents to namespaced owners ([k8s
docs](https://kubernetes.io/docs/concepts/architecture/garbage-collection/#owners-dependents)).
The library deletes the shadow VSC explicitly on `TestEnv` drop, with
the namespace janitor as backstop (it sweeps orphan VSCs whose
`snapshotRef.namespace` no longer exists).

### GC

Daily `CronJob` in `ztest-seeds`: drop archive PVCs whose
`last_accessed_at` > 30 days. Reconcile-failure stragglers (no
`ready=true`, > 1h) also swept. Re-materialization on next use is cheap.

## Ownership cascade

```
  Namespaced (k8s GC cascades these):
  ─────────────────────────────────────────────────────────────────
    Namespace  zaino-{ci,dev}-…
    └── Sentinel ConfigMap
        ├── Pod  (component)
        │   ├── PVC
        │   └── ConfigMap
        └── VolumeSnapshot (shadow)

      cleaned up by:
        • library — sentinel deleted on TestEnv drop
        • CI       — `kubectl delete ns -l ztest.io/run-id=…`
        • backstop — kube-janitor by TTL annotation


  Cluster-scoped (k8s GC will NOT cross scopes — see Seeds):
  ─────────────────────────────────────────────────────────────────
    VolumeSnapshotContent (shadow)

      cleaned up by:
        • library — explicit delete on TestEnv drop
        • backstop — kube-janitor sweep of orphaned VSCs
```

Per-test sentinel + namespace TTL is belt-and-suspenders for everything
namespaced. The cluster-scoped shadow VSC is the one resource that
*must* be cleaned up by the library itself or the janitor — there is no
ownership cascade that can reach it.

## Observability

Logs, metrics, and events are captured continuously by cluster-resident
infrastructure, not by anything the test harness or CI workflow runs.
Once a namespace is created, its pods are scraped automatically; once
it's deleted, the data persists in the retention windows below.

Stack lives in the `observability` namespace, deployed by Flux alongside
the rest of the cluster (see
[cluster-administration.md](cluster-administration.md)):

| Component            | Role                                    | Retention |
| -------------------- | --------------------------------------- | --------- |
| Promtail (DaemonSet) | Tail every container log on every node  | —         |
| Loki                 | Log store, indexed by k8s labels        | 7 d       |
| Prometheus           | Scrape `prometheus.io/scrape=true` pods | 30 d      |
| kube-state-metrics   | Surface k8s object state as metrics     | (scraped) |
| Grafana              | Dashboards + ad-hoc queries (Tailscale) | —         |

Every resource the library creates carries:

```
labels:
  ztest.io/run-id:    "${RUN_ID}"        # or dev-${USER}-${NEXTEST_PID}
  ztest.io/slot:      "${SLOT}"
  ztest.io/test:      "<test-name>"      # on pods only
  ztest.io/component: "zebrad|zcashd|zaino|zingo"
```

Promtail forwards every label as a Loki stream label; Prometheus
scrapes by label selector. Post-mortem on a CI failure is:

```
# in Grafana, Explore tab, Loki datasource:
{zaino_io_run_id="17234819-1", zaino_io_test="indexer::wallet_sync"}

# or, on a node-down event, kube events for the run:
kube_event{zaino_io_run_id="17234819-1"}
```

Components configured to expose Prometheus metrics (zebrad's metrics
endpoint, zaino's, etc.) are annotated `prometheus.io/scrape=true` plus
`prometheus.io/port=<n>` by the library when they declare a metrics
port. No per-test scrape config.

The test harness writes nothing to disk for archival. If a test author
wants something durable, they emit it as a structured log line — it
ends up in Loki by virtue of being on stdout. The pattern intentionally
forecloses "let me upload one more thing" feature creep.

## Not in scope (v1)

- **Cross-namespace fixtures.** Pod IPs are reachable cross-namespace
  in Cilium-by-default, so the technical block is not networking — it's
  that the sentinel + lifetime model is per-namespace, and coordinating
  one logical test across multiple namespaces complicates teardown
  without buying anything the slot model doesn't already give us.
- **Restart-tolerant tests.** No automatic recovery on test-process
  crash. The author's tools are `nextest --retries` for transient
  flakes and "Re-run failed jobs" in the GitHub UI for everything else.
- **Non-Rust test drivers.** The library is Rust-only; we don't expose
  an IPC surface.
- **Resource quotas per namespace.** Trust the dev cluster; tighten if
  it bites.

## Open

1. **Per-developer SA isolation.** v1 ships one shared `engineer` SA
   bound to all `ztest-dev-*` namespaces. Move to per-user SA + name
   prefix admission policy if audit becomes important.
1. **Orchestrator image build cadence.** N/A under lib-only model — the
   library is published as a crate, no separate image needed.
