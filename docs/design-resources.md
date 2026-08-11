# Provider DAG & storage byte-source

How ztest provisions and tears down cached and per-run resources through one
`Provider` graph, and where a seed's bytes come from.

## Two graphs

Resources split by lifetime, both expressed with one `Provider` trait:

- **Shared resource graph** — content-addressed, cached across runs, provisioned
  once, shared by many tests: images (`zebrad:dev-abef4589`), seed PVCs
  (`seed-1234`), their paired snapshots. Independent readiness,
  skip-on-failed-dep, and pipelining apply here. Teardown is a no-op — it is the
  cache.
- **Per-run instance graph** — per-test namespaces and their cascade (pods, cloned
  PVCs, services, configmaps), cluster-scoped shadow VolumeSnapshotContents, the
  QoS lease. Ephemeral, run-scoped, created fresh per test.

A node's `Lifetime` decides whether its `teardown` acts: `Cached` → kept;
`RunScoped`/`Shared` → reaped.

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

`Graph` (`src/resource/graph.rs`) is the executor, generic over `Id`/`Cx` with no
Kubernetes code, and walks the DAG both directions:

- **`provision`** (forward) — a node runs when all deps are `Ready`; independent
  nodes run concurrently (`FuturesUnordered`); a node with an unavailable dep is
  `Blocked` (never attempted) and blocking propagates transitively; a `probe`
  reporting `Ready` short-circuits `provision`.
- **`teardown`** (reverse) — a node is reaped only once every dependent is gone;
  `Cached` and never-provisioned nodes are skipped; failures are isolated into a
  report.

## Concrete Provider nodes

| Provider | probe | provision | lifetime | teardown |
|---|---|---|---|---|
| `ImageProvider` | `exists_in_kind(tag)` | build + kind load | `Cached` | no-op |
| `SeedProvider` | seed PVC label `ready=true` | puller-Job materialize | `Cached` | no-op |
| `SnapshotProvider` | snapshot `readyToUse` | create from seed | `Cached` | no-op |

New resource kind = new `Provider` impl, not a new phase.

## Teardown principles

1. **Idempotent** — every teardown is "ensure absent"; 404 = success; safe to run
   twice (a normal completion racing a cancel).
2. **Runs in the surviving parent** — the only process guaranteed alive during a
   cancel. The child's `TestEnv::Drop` is the fast path on normal exit.
3. **Reconstructable from durable identity** — everything is labeled
   `ztest.io/run-id`, so run-scoped teardown is a label-selector delete needing no
   in-memory ledger. Providers label before they populate, so partial work
   (a child SIGKILLed mid-provision) is findable.
4. **Reverse-dependency order, leaning on the k8s cascade** — `delete namespace`
   does the intra-namespace teardown; the graph only orders cross-boundary
   escapees (shadow VSCs, leases).
5. **Bounded deadline** — teardown fans out concurrently, awaited up to ~30 s.
   Block until the API *accepts* the delete (202), not until the object is gone
   (PV reclaim/finalizers finish async over minutes).
6. **Failure isolation** — one failed delete logs and does not abort siblings;
   teardown never panics.

## Per-run reap (`provisioning/reap.rs`)

Per-run ephemeral resources are **not** graph nodes: the preflight graph is
conditional (built only when images/seeds are declared), but every run makes
namespaces, so their reap must be unconditional. It is a direct label-delete:

- **`reap_run(client, run_id)`** — `delete_collection` on namespaces (cascading
  all per-test resources) and shadow VSCs, both selected by `ztest.io/run-id`; the
  Ctrl-C teardown, 30 s deadline, respects `--no-cleanup`.

`reap_run` only ever touches *this* run's `run_id`. Reaping old/abandoned
resources is never automatic (a prior run's `--no-cleanup` resources are kept on
purpose); it is the explicit `ztest cleanup`.

The child creates the per-test namespace at runtime (topology is built then), so
it is never a graph node.

### Run-id propagation

Tests derive `run_id` from `ZTEST_RUN_ID`, else `{user}-{ppid}`. Parent and child
disagree unless forced (a child's ppid is the orchestrator; the orchestrator's is
the shell). The parent therefore **sets `ZTEST_RUN_ID` before any thread starts**,
and every test child inherits it, so parent-reaper and children label/reap under
one id. Shadow VSCs (cluster-scoped, uncascaded) are labeled with the run-id at
mint time so the reap can target them.

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

**Startup reap** — on `ztest run`, sweep this user's stale run-ids. Self-heals
after a `kill -9` of the parent, needing no cluster component.

## QoS integration

DAG readiness gates admission: a test is admissible when its deps are `Ready`
**and** QoS capacity admits; a test whose dep is `Failed`/`Blocked` is `Skipped`
(reported skipped, not failed). The resource graph decides *what can run*; the 2D
scheduler ([design-qos.md](design-qos.md)) decides *what fits*.

## Storage byte-source (`src/storage/`)

`storage` answers **where a seed's bytes come from**. There is one answer: the
snapshot bucket (`r2.rs`), addressed by OID.

### Identity is the OID

A seed is a Git LFS object. Its identity is the `sha256`/`size_bytes` recorded in
the artifact's sidecar `.toml`, which are *exactly* the committed LFS pointer's
`oid`/`size` — an LFS object id is the SHA-256 of the file. The manifest is
plaintext, never LFS-tracked, and present in every checkout and every build
context, so `archive!` bakes the identity at expansion time with no `git` and no
access to the archive bytes.

`storage::seed_sha8(oid)` names the PVC `seed-{oid[..8]}`. It is a pure function
of a compile-time constant, so the laptop, the build pod, the runner pod and the
puller Job all derive the same name without any of them reading the file.

> **What this replaced.** Identity used to be the archive's *path*, hashed at
> runtime, with a `Local` backend for a real file and an `Lfs` backend for a
> pointer. That works on a laptop and nowhere else: under on-cluster compile the
> path is `/src/…` inside a build container, which exists on no other machine —
> so every seeded test failed with "does not exist", naming a path that had never
> been openable where the error was raised. `Local`, the pointer sniffing, and
> the path-addressed `seed-unresolved-*` placeholder are all gone with it.

### Who moves bytes

Only the side holding credentials, and never through this process:

| | `provision_seed` (parent) | `await_seed` (test) |
|---|---|---|
| runs in | `ztest run` preflight | the runner pod, at `TestEnv::build` |
| needs bucket creds | yes — the only thing that does | no |
| needs the archive | no (only its OID) | no |
| creates the PVC / Job | yes | no |

`provision_seed` presigns a GET for `lfs/<oid>` and hands the URL to a puller
Job; the pod `curl`s it into `tar -x -C /seed` itself, so the transfer is R2 →
node at cluster bandwidth. The URL is scoped to one object and one verb and
expires, which is why no credential Secret is mounted into the cluster at all.

`await_seed` only waits. A seed missing there is a preflight bug — a test
mounting an archive it never declared with `#[ztest::needs]` — and the error says
so rather than timing out.

### Compression

Derived from the artifact's filename (`compression_from_name`), which the
manifest records. There is no magic-byte fallback because there are no local
bytes to sniff, and GNU `tar` cannot auto-detect on the non-seekable pipe the
puller feeds it — so the flag is resolved before the Job is created.
