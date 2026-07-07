# Resource graph — unified provisioning & teardown

## The reframe: two graphs, not one

The load-bearing decision is to separate two things that look alike but have
**opposite lifetimes**:

1. **The shared resource graph** — `zebrad:dev-abef4589`, `seed-1234`, its paired
   snapshot. Content-addressed, cached across runs, provisioned once, shared by
   many tests. This is where the *scheduling* value lives (independent readiness,
   skip-on-failed-dep, pipelining). Its teardown is **almost always a no-op** —
   it's the cache.

2. **The per-run instance graph** — the per-test namespace and its cascade (pods,
   PVCs from clones, services, configmaps), the cluster-scoped shadow VSCs, and
   the QoS lease. Ephemeral, run-scoped, created fresh per test. **This is what
   leaks on Ctrl-C.**

Both are expressed with one [`Provider`] trait; a node's [`Lifetime`] decides
whether its `teardown` does anything. `Cached` → kept; `RunScoped`/`Shared` →
reaped.

## Why this is needed (the bug it fixes)

All per-test cleanup currently lives in `impl Drop for TestEnv` — **inside the
test binary**. Drop runs on normal return, `Err`, and panic, but **never on a
signal death, and SIGKILL can't be caught at all**. The cancellation path kills
tests via `kill_on_drop` (SIGKILL), so on Ctrl-C the test binary is vaporized and
its cleanup never runs. The only backstop is a `janitor/ttl: 1h` annotation — but
**no janitor is deployed** (`fixtures/kind/` has none; docs call kube-janitor a
"v1 plan"). Net today: **a mid-test Ctrl-C orphans the namespace and the
cluster-scoped shadow VSCs indefinitely.**

The fix is an inversion: **the surviving parent must own teardown**, because the
process being cancelled is the one that can't run its destructors.

## Design decisions for a cleanup operation

1. **Idempotent** — every teardown is *"ensure absent"*; 404 = success; safe to
   run twice (a normal completion racing a cancel).
2. **Runs in the parent** — the only process guaranteed alive during a cancel.
   RAII-in-the-child (`TestEnv::Drop`) stays the fast path for *normal* exit.
3. **Reconstructable from durable identity** — the parent must reap resources it
   never observed being created (a child SIGKILLed mid-provision). Everything is
   labeled `ztest.io/run-id`, so run-scoped teardown is a **label-selector
   delete** needing no in-memory ledger. Providers must **label before populate**
   so partial work is findable.
4. **Lifetime is per-node** (`Cached` / `RunScoped` / `Shared`) — decides what is
   reaped and when.
5. **Reverse-dependency order, leaning on k8s cascade** — `delete namespace` does
   the intra-namespace teardown; the graph only orders the cross-boundary
   escapees (shadow VSCs, leases).
6. **Async, backgrounded, awaited to a deadline** — on Ctrl-C teardown fans out
   concurrently, the panel shows progress, and the main flow waits *bounded by a
   deadline (~30 s)*. Block until the API **accepts** the delete (202), not until
   the object is *gone* (PV reclaim/finalizers take minutes; k8s finishes async).
7. **Escalation is the shutdown state machine** — 1st Ctrl-C: graceful teardown;
   2nd: stop waiting, exit, leave the rest to the backstop; 3rd: hard `exit`.
8. **Failure isolation** — one failed delete logs and does not abort its
   siblings; teardown never panics.
9. **Kill children before reaping** — SIGKILL test binaries first (they stop
   mutating the cluster), then run the label reap. Don't teach children to clean
   up on SIGTERM — redundant with the parent reap and fragile.
10. **Defense in depth** — parent teardown is primary; a **startup reap** (sweep
    this user's stale run-ids on `ztest run`) self-heals after a `kill -9` of the
    parent, needing no cluster component.

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

The [`Graph`] executor (generic over `Id`/`Cx`, no Kubernetes code, unit-tested
against fakes) walks it both ways:

- **`provision`** — forward. A node runs when all deps are `Ready`; independent
  nodes run concurrently (`FuturesUnordered`); a node with an unavailable dep is
  `Blocked` (never attempted) and blocking propagates transitively; a `probe`
  that reports `Ready` short-circuits `provision`.
- **`teardown`** — reverse. A node is reaped only once every dependent is gone;
  `Cached` and never-provisioned nodes are skipped; failures are isolated into a
  report.

**Status: implemented and tested** (`src/resource/{provider,graph}.rs`).

## Concrete nodes (to implement)

| Provider | probe | provision | lifetime | teardown |
|---|---|---|---|---|
| `ImageProvider` | `exists_in_kind(tag)` | docker build + kind load | `Cached` | no-op |
| `SeedProvider` | seed PVC label `ready=true` | uploader-pod materialize | `Cached` | no-op |
| `SnapshotProvider` | snapshot `readyToUse` | create from seed | `Cached` | no-op |
The per-run ephemeral resources (per-test namespaces + cluster-scoped shadow
VSCs) are **not** a graph node — the preflight graph is *conditional* (built only
when images/seeds are declared), but every run makes namespaces, so their reap
must be **unconditional**. It's therefore a direct label-delete
(`provisioning/reap.rs`), not a `Provider`:

- **`reap_run(client, run_id)`** — `delete_collection` on namespaces (cascading
  all per-test resources) + shadow VSCs, both by `ztest.io/run-id`. Idempotent
  (404 = ok), failure-isolated. This is the Ctrl-C teardown, run with a 30 s
  deadline (respecting `--no-cleanup`); the janitor is the backstop past it.

Reaping **old/abandoned** resources is deliberately **not** automatic — not at
startup, not for a peer's leftovers. It's an explicit operation (`ztest cleanup`),
because a previous run's `--no-cleanup` resources are kept on purpose until the
user asks for them to go. `reap_run` only ever touches *this* run's `run_id`.

The per-test namespace stays **created by the child** (tests build topology at
runtime); the child's `Drop` is the fast path on normal exit, and the parent's
label-reap is the authoritative cancel/crash path. Both idempotent, so a race is
harmless. (k8s namespace cascade already does the intra-namespace topological
teardown, so a reverse-topo graph walk would be over-engineering here.)

**Run-id propagation (the linchpin):** tests derive `run_id` from
`ZTEST_RUN_ID` else `{user}-{ppid}`; parent and child *disagree* unless
forced (a child's ppid is us, ours is the shell). So the parent **sets
`ZTEST_RUN_ID` before any thread starts**, and every test child inherits
it — parent-reaper and children then label/reap with the same id. Shadow VSCs
(cluster-scoped, uncascaded) are labeled with the run-id at mint time so the reap
can target them.

## Cancellation flow

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

## Integration with the QoS run scheduler

The DAG readiness gates admission: a test is admissible when **its deps are
`Ready`** *and* **QoS capacity admits**; a test whose dep is `Failed`/`Blocked`
is `Skipped` (reported skipped, not failed). The resource graph decides *what can
run*; the 2D scheduler decides *what fits*.

## What this consolidates

`pipeline/images.rs` + `pipeline/archives.rs` + `cli/snapshot.rs` + the fixed
phase sequence in `run.rs` (`pipeline_console` → `run_image_phases`) — three
bespoke provisioning pipelines with hand-rolled ordering and no unified teardown
— collapse into **N `Provider` impls + one executor + one teardown scheduler.**
New resource kind = new `Provider`, not a new phase spliced into `run.rs`.

## Implementation order

1. **`resource` module** — trait, state model, executor, teardown. ✅ *done, tested.*
2. **Port provisioning** into `ImageProvider` / `SeedProvider` / `SnapshotProvider`
   (behavior-preserving); delete the three bespoke pipelines.
3. **`RunScopeProvider`** — label-reap teardown + startup-reap.
4. **Wire the shutdown model** (`ShutdownRequest`, SIGTERM/SIGHUP/panic-hook) to
   the teardown scheduler; render thread streams reap progress.
5. **Thread DAG readiness into the QoS run loop** — gate admission, skip on failed
   deps.

## Non-goals / kept as-is

- Content-addressed caches (images, seeds) are **never** reaped on cancel; eviction
  stays an explicit `ztest snapshot prune`.
- Tests keep building their topology at runtime; the parent does not pre-provision
  per-test namespaces (it reaps them by label).
