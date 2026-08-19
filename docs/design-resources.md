# Provider DAG & storage byte-source

How ztest provisions and tears down cached and per-run resources through one `Provider` graph, and where
a seed's bytes come from.

## Two graphs

Split by lifetime, one `Provider` trait:

- **Shared resource graph** — content-addressed, cached across runs, provisioned once, shared by many
  tests: images (`zebrad:dev-abef4589`), seed PVCs (`seed-1234`), paired snapshots. Independent
  readiness, skip-on-failed-dep and pipelining apply here; teardown is a no-op — it *is* the cache
- **Per-run instance graph** — per-test namespaces + their cascade (pods, cloned PVCs, services,
  configmaps), cluster-scoped shadow VolumeSnapshotContents, the QoS lease. Ephemeral, created per test

A node's `Lifetime` decides whether `teardown` acts: `Cached` → kept, `RunScoped`/`Shared` → reaped.

## The abstraction (`src/resource/`)

```rust
trait NodeId: Clone + Eq + Hash + Debug + Send + Sync + 'static {}

enum Lifetime { Cached, RunScoped, Shared }
enum Readiness { Ready, Absent }
enum NodeState { Pending, Acquiring, Ready, Failed(String), Blocked }

#[async_trait]
trait Provider<Id, Cx> {
    fn id(&self) -> Id;
    fn deps(&self) -> Vec<Id>;
    fn lifetime(&self) -> Lifetime;
    async fn probe(&self, cx: &Cx) -> Readiness;        // already present? → skip provision
    async fn provision(&self, cx: &Cx) -> Result<()>;   // absent → Ready; idempotent
    async fn teardown(&self, cx: &Cx) -> Result<()>;    // ensure absent; idempotent; no-op for Cached
}
```

`Graph` (`src/resource/graph.rs`) is the executor — generic over `Id`/`Cx`, no Kubernetes code — walking
the DAG both directions:

- **`provision`** (forward): a node runs once every dep is `Ready`; independent nodes run concurrently
  (`FuturesUnordered`); a node with an unavailable dep is `Blocked`, never attempted, and blocking
  propagates transitively; a `probe` reporting `Ready` short-circuits `provision`
- **`teardown`** (reverse): a node is reaped only once every dependent is gone; `Cached` and
  never-provisioned nodes skipped; failures isolated into a report

| Provider           | probe                       | provision              | lifetime | teardown |
| ------------------ | --------------------------- | ---------------------- | -------- | -------- |
| `ImageProvider`    | `exists_in_kind(tag)`       | build + kind load      | `Cached` | no-op    |
| `SeedProvider`     | seed PVC label `ready=true` | puller-Job materialize | `Cached` | no-op    |
| `SnapshotProvider` | snapshot `readyToUse`       | create from seed       | `Cached` | no-op    |

New resource kind = a new `Provider` impl, never a new phase.

## Teardown principles

1. **Idempotent** — every teardown is "ensure absent"; 404 = success; safe twice (normal completion
   racing a cancel)
1. **Runs in the surviving parent** — the only process guaranteed alive during a cancel; the child's
   `TestEnv::Drop` is the fast path on normal exit
1. **Reconstructable from durable identity** — everything labeled `ztest.io/run-id`, so run-scoped
   teardown is a label-selector delete needing no in-memory ledger. Providers label *before* they
   populate, so a child SIGKILLed mid-provision leaves findable work
1. **Reverse-dependency order, leaning on the k8s cascade** — `delete namespace` does the intra-namespace
   work; the graph only orders cross-boundary escapees (shadow VSCs, leases)
1. **Bounded deadline** — fans out concurrently, awaited ~30 s; blocks until the API *accepts* the delete
   (202), not until the object is gone (PV reclaim/finalizers run async over minutes)
1. **Failure isolation** — one failed delete logs, siblings continue; teardown never panics

## Per-run reap (`provisioning/reap.rs`)

Per-run ephemerals are **not** graph nodes: the preflight graph is conditional (built only when
images/seeds are declared) but every run makes namespaces, so their reap must be unconditional.

- `reap_run(client, run_id)` — `delete_collection` on namespaces (cascading per-test resources) and
  shadow VSCs, both selected by `ztest.io/run-id`; the Ctrl-C teardown, 30 s deadline, respects
  `--no-cleanup`
- Only ever touches *this* run's `run_id` — reaping old/abandoned resources is never automatic (a prior
  run's `--no-cleanup` resources are kept on purpose); that is `ztest cleanup`
- The child creates the per-test namespace at runtime (topology is built then), so it is never a node

### Run-id propagation

Tests derive `run_id` from `ZTEST_RUN_ID`, else `{user}-{ppid}` — parent and child disagree unless forced
(a child's ppid is the orchestrator, the orchestrator's is the shell). So the parent **sets
`ZTEST_RUN_ID` before any thread starts** and every child inherits it, putting parent-reaper and children
on one id. Shadow VSCs (cluster-scoped, uncascaded) are labeled with it at mint time.

## Shutdown state machine

```
SIGINT/SIGTERM/SIGHUP  ──▶  ShutdownRequest { source, Graceful }
  render thread: panel → "Cancelling"
  1. SIGKILL running test children (stop cluster mutation)
  2. spawn teardown on work_rt (background):
       Graph::teardown → reverse-topo, concurrent, 404=ok, failures isolated
       progress streamed to the panel: "reaping 4/7…"
  3. main flow awaits teardown up to DEADLINE
       2nd Ctrl-C → stop waiting, exit(130); startup-reap owns the rest
  4. exit(130)
```

**Startup reap** — `ztest run` sweeps this user's stale run-ids, self-healing after a `kill -9` of the
parent with no cluster component involved.

## QoS integration

DAG readiness gates admission: a test is admissible when its deps are `Ready` **and** QoS capacity
admits; a test whose dep is `Failed`/`Blocked` is `Skipped` (reported skipped, not failed). The graph
decides *what can run*, the scheduler ([design-qos.md](design-qos.md)) decides *what fits*.

## Storage byte-source (`src/storage/`)

One answer to "where do a seed's bytes come from": the snapshot bucket (`r2.rs`), addressed by OID.

### Identity is the OID

- Identity = `sha256`/`size_bytes` from the artifact's sidecar `.toml`; the SHA-256 is both bucket key
  and PVC name
- Manifest is plaintext and committed while the archive is not → present in every checkout and every
  build context, so `archive!` bakes identity at expansion time with no `git` and no archive bytes
- `storage::seed_sha8(oid)` names the PVC `seed-{oid[..8]}` — a pure function of a compile-time constant,
  so laptop, build pod, runner pod and puller Job all derive the same name without reading the file

> **What this replaced.** Identity used to be the archive's *path*, hashed at runtime, with a `Local`
> backend for a real file and an `Lfs` backend for a Git LFS pointer. Works on a laptop and nowhere else:
> under on-cluster compile the path is `/src/…` inside a build container, existing on no other machine —
> every seeded test failed with "does not exist", naming a path never openable where the error was
> raised. `Local`, the pointer sniffing, and the path-addressed `seed-unresolved-*` placeholder went with it.

### Who moves bytes

Only the side holding credentials, and never through this process:

|                       | `provision_seed` (parent)      | `await_seed` (test)                 |
| --------------------- | ------------------------------ | ----------------------------------- |
| runs in               | `ztest run` preflight          | the runner pod, at `TestEnv::build` |
| needs bucket creds    | yes — the only thing that does | no                                  |
| needs the archive     | no (only its OID)              | no                                  |
| creates the PVC / Job | yes                            | no                                  |

- `provision_seed` presigns a GET for `lfs/<oid>` and hands the URL to a puller Job; the pod `curl`s it
  into `tar -x -C /seed` itself → R2 → node at cluster bandwidth. URL is scoped to one object and one
  verb and expires, which is why no credential Secret is ever mounted into the cluster
- `await_seed` only waits. A seed missing there is a preflight bug (a test mounting an archive it never
  declared with `#[ztest::needs]`) and the error says so instead of timing out

### Compression

Derived from the artifact's filename (`compression_from_name`), recorded in the manifest. No magic-byte
fallback — there are no local bytes to sniff, and GNU `tar` cannot auto-detect on the non-seekable pipe
the puller feeds it, so the flag resolves before the Job is created.
