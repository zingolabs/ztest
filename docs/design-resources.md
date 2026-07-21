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
| `SeedProvider` | seed PVC label `ready=true` | uploader-pod materialize | `Cached` | no-op |
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

`storage` abstracts **where a seed's bytes come from**, decoupled from how they
are materialised into a `seed-{sha8}` PVC. Same shape as `ImageProvider`: one
trait, file-per-backend, one selection point.

Two orthogonal axes:

- **LFS blob storage** — where multi-GB `.tar.zst` blobs live off the git object
  store: Git LFS + `.lfsconfig` pointing at a self-hosted rudolfs server. Pure
  git-layer config, no ztest code.
- **Provision-time byte production** — how ztest gets a seed's real bytes into a
  PVC when the working tree holds only an LFS pointer. This module.

### Dispatch by content-sniffing

A seed source (the macro-baked absolute path from `#[ztest::archive]` /
`mount_file!`) is classified per file by sniffing for an LFS pointer, not by an
env var:

- real archive/blob on disk → `Local` — streamed straight off disk.
- Git LFS pointer, blob absent → `Lfs` — fetched from the server.

`git lfs pull` turns a pointer into a real file, which then takes the `Local`
path. `ZTEST_LFS_URL` configures only *where* the server is, never *whether* to
use LFS.

### Content address (sha8 == oid == SHA-256)

`seeds::sha8` names a PVC `seed-{sha8}` by the SHA-256 of the `.tar.*` bytes. An
LFS pointer's `oid` **is** that SHA-256, so:

- `storage::content_sha8` resolves a pointer's seed id from the pointer text
  alone — no transfer needed to name the seed.
- a cold CI run (fetches from LFS) and a warm laptop (`git lfs pull`ed) produce
  the same `seed-{sha8}`, sharing the same content-addressed seed and snapshot.

`seeds::sha8` delegates to `content_sha8`, so every call site (materialize, the
`SeedProvider` node, `cli snapshot`) is pointer-aware.

### The `materialize::ensure_seed` seam

`ensure_seed` couples to the byte source in two places, both routed through the
backend:

1. content address — `seeds::sha8` → `content_sha8`.
2. byte production — the uploader-pod path uses
   `storage::for_source(source)?.open()`, a `dyn AsyncRead`, in place of
   `tokio::fs::File::open`.

Compression is resolved by `backend.compression()` **before** `open()`, from the
filename extension (both backends) with a magic-byte fallback for on-disk files.
The uploader `tar` command is fixed before any download starts, and the LFS
`open()` is deferred until the uploader pod is scheduled and ready on stdin — no
HTTP connection is held across pod scheduling. Everything downstream (uploader
pod, stdin attach, `ready` label, VolumeSnapshot, shadow clone) never learns which
backend produced the bytes.

### LFS backend (`lfs.rs`)

Speaks the Git LFS batch API over HTTP to rudolfs, not the `git lfs` binary:

```
POST {endpoint}/objects/batch   {operation:"download", objects:[{oid,size}]}
  → {objects:[{actions:{download:{href, header}}}]}
GET  href                        (streamed into the uploader pod's stdin)
```

No git checkout, no `git-lfs` binary, so it runs anywhere the orchestrator does.

**Endpoint resolution order:** `ZTEST_LFS_URL` (with optional `ZTEST_LFS_TOKEN` →
`Authorization: Bearer`), else the `[lfs] url` of the nearest `.lfsconfig` walking
up from the source. A pointer with neither configured fails fast with an
actionable error before any pod is created.
