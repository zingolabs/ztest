# Sync testing

Test class for **long-running chain sync** — wallet, indexer, or validator, birthday/genesis → tip.

- Runs in a ztest-owned pod, up to the `sync` tier's 48 h cap
- Not `arrange → act → assert` — continuous monitor + completion predicate
- Runner owns the lifecycle, snapshots each tick, evaluates probes at their own cadences
- `pass` = completion predicate fires; `fail` = fatal invariant violated

## Goals

| Goal                   | Signal                                                                       | Gating |
| ---------------------- | ---------------------------------------------------------------------------- | ------ |
| End-state correctness  | at tip: balances / note-commitment-tree root match an independent authority  | yes    |
| Continuous invariants  | core Zcash chain guarantees hold on every tick of a multi-hour sync          | yes    |
| Live progress          | streamed scan phase + % to tip, surfaced in `ztest sync watch`               | no     |
| Throughput / resource  | blocks-s, outputs-s, CPU/mem/IO profile over the run (feeds the phase model) | no     |
| Robustness under chaos | sync recovers from partitions / packet loss / dropped links                  | yes    |

Perf-gating + calibration explicitly out of scope (`sync` tier = admission/placement, not a latency budget).

## The subject seam

ztest ships **no sync engine and knows none**. What it ships is the harness: runner, probe scheduler,
invariant taxonomy, nemesis, history, report, `watch`, the QoS tier, the detached pod lifecycle.
Everything it watches enters through one object-safe trait:

```rust
#[async_trait]
pub trait SyncSubject: Send + Sync {          // Sync, not just Send: progress(&self) borrows across await
    async fn launch(&mut self) -> Result<(), RpcError>;              // once; no-op if it syncs itself
    async fn progress(&self) -> Result<Box<dyn ProgressView>, RpcError>;
    async fn is_complete(&self) -> bool;
    fn work_source(&self, op: Op) -> Option<&'static str> { None }   // series `progress` reads op from
    async fn stop(&mut self) -> Result<(), RpcError> { Ok(()) }      // graceful checkpoint; observers no-op
}
```

- **Boxed, not an associated type** — a profile binds one `dyn SyncSubject`, so ztest carries no enum of
  known subjects and a new one is a new impl (in any crate), never a new arm here. One allocation per
  tick, seconds apart
- Two roles, from the same trait: a **driving** subject starts the engine it owns in `launch`; an
  **observing** subject leaves `launch`/`stop` empty and the runner is a pure prober over its RPC
- Probes read the common `ProgressView` (height, target, pct, phase); subject extras (balances, tree
  roots, work counters) ride `Option` defaults, so an observer reports *unreported*, never a zero a
  probe would read as passing
- Subject owns its endpoint resolution → `Endpoint { IpAddr, u16 }` never has to carry scheme/DNS/TLS
- The harness compiles with **no backend feature at all**; wallet, indexer and validator subjects are
  interchangeable to it

### Phase vocabulary belongs to the subject

`Phase` is `Starting` / `Syncing` / `Done` — lifecycle only. A subject's own stage word (`"scanning"`,
`"indexing"`, `"downloading headers"`) rides `ProgressView::detail` and is rendered beside it.

- No engine's scan taxonomy lands in the harness enum. An earlier `Phase` carried one wallet engine's
  `ScanPriority` names, four of which no producer ever emitted
- Unknown stage words decode to `Syncing` rather than failing — a 48 h detached sync outlives the CLI
  build watching it

## One gRPC substrate

```
SyncSubject ─▶ Channel ─▶ [optional Nemesis decorator] ─▶ ┬─ IndexerBackend RPCs (oracle authority)
 (owns dialing + TLS)                                     ├─ LwdClient (load driver)
                                                          └─ whatever engine the subject drives
```

- One dialing/TLS/port-forward/fault layer, every consumer
- Oracle rides the same channel — an independent authority (indexer `GetTreeState` against a wallet's
  own root; CometBFT app-hash / Hive head-hash analog) rather than a second implementation of the SUT

## Continuous verification: the invariant taxonomy

Per-tick callbacks alone are insufficient (Jepsen/Antithesis/TLA+). Four probe classes + a recorded
history checked at the end:

| Class                     | Semantics                    | Evaluated               | Failure means   | Examples                                                                                                                 |
| ------------------------- | ---------------------------- | ----------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------ |
| **always** (safety)       | true at *every* tick         | per-tick, live          | a **bug**       | height monotonic (except bounded reorg); balances monotonic absent a reorg; per-pool outputs monotonic; chain continuity |
| **eventually** (liveness) | becomes true before deadline | live + terminal         | a **stall**     | progress within a window; recovers after a heal; reaches tip                                                             |
| **sometimes** (coverage)  | true on ≥1 tick over the run | end-of-run over history | a **weak test** | observed a reorg handled; exercised the reconnect path                                                                   |
| **at_completion**         | post-condition once at tip   | terminal                | final-state bug | balances match oracle; note-commitment-tree root == indexer's                                                            |

**sometimes** is load-bearing, not decoration — a green 48 h run whose coverage probes never fired means
the chaos never reached the interesting state. Each fault is gated behind one, so a pass *proves* the
adverse state happened.

## Scheduled probes

Grounded in Prometheus rule evaluation + Gomega `Consistently` + CronJob semantics.

- **Per-probe cadence** (`every(dur)` / `every_blocks(n)` / `each_tick` / `window(dur)`) — no global tick;
  cheap safety checks tick fast, RPC-backed ones slow
- **Snapshot-then-evaluate** — one immutable `Snapshot` per tick, shared by every probe due (pure
  predicates, one read of the subject)
- **`hold_for` / `keep_for`** (Prometheus `for:` / `keep_firing_for:`), quantized to cadence (reorgs +
  RPC gaps make a bare threshold flap)
- **Missed tick = Skip/Coalesce, never backlog** (thousands of stale evals over 48 h); a skip is a
  distinct visible outcome
- **Four outcomes, not a bool**: `Satisfied` / `Pending(retry)` / `Violated(record)` / `ProbeError(abort)`
  — a *throwing* probe means the harness/RPC broke, a false one means keep going
- **Multi-cadence = multiple registrations**, not a combinator (SLO multi-window multi-burn-rate): same
  predicate at 30 s and 1 h with different severities

## Chaos: the nemesis

Three altitudes, all **outside** the SUT:

| Altitude      | Mechanism                                                   | Models                                                                  | Determinism                       |
| ------------- | ----------------------------------------------------------- | ----------------------------------------------------------------------- | --------------------------------- |
| Channel       | `tower` layer wrapping the tonic `Channel`                  | subject↔peer link: latency, drops, errors, throttle                     | high — seed-reproducible, per-RPC |
| k8s network   | NetworkPolicy + netem on real pods                          | anywhere in the topology: validator↔indexer, peer partitions, bandwidth | low — real kernel netem           |
| Indexer proxy | ztest-owned `CompactTxStreamer` between subject and indexer | reorgs, stalled ranges, malformed blocks, scripted chains               | total — the generator             |

- A subject syncing inside its own pod has no in-process channel to wrap → k8s-level only, which makes
  `NetworkChaos` the centerpiece for every observed subject
- Indexer proxy is gRPC-only (validator P2P = the Zcash wire protocol → netem, not a proxy)
- Same cron-tick scheduling; probabilistic faults use a seed-logged `buggify` posture (FoundationDB)
- **Reversion is a correctness property**: a partition leaking past a crashed test wedges the shared
  cluster. Every fault = a resource-graph node (parent-owned label reap) **and** carries a kernel-side
  dead-man's-switch (netem TTL / revert-on-timeout, per Chaos Mesh `duration`). Chaos that cannot
  guarantee its own reversion is not shipped
- Native, not delegated: NetworkPolicy for partitions (declarative, zero privilege) + a privileged netem
  sidecar reusing the buildkit admission machinery — no Chaos Mesh controller dependency

## Test-author API

`#[ztest::sync_test(...)]` annotation + a body that is a **registration program**:

- **Annotation = static, known pre-run**: name, description, subject kind, timeout, QoS tier, tags.
  Powers `ztest sync list` / `--help` + admission (scheduler sizes the pod before the body runs)
- **Body = runs in the pod**: topology, subject binding, named invariant fns at chosen cadences, nemesis
  schedule
- Body is pure registration up to `run.run()` → two modes. **Execute** provisions and syncs; **Collect**
  makes `run.run()` inert and returns the manifest, so `ztest sync describe <name>` prints the full
  invariant + nemesis manifest **without a cluster**
- **Invariants are nested `fn` items**, not closures: cannot capture the enclosing scope (accidental
  capture = compile error, predicate stays pure), but *can* read block-level `const`/`fn` items; nested
  `async fn` allowed for RPC-backed checks. Shared ones factor into a module, referenced by path
- Predicate decoupled from class/cadence/severity — same `height_monotonic` registers `always/Fatal/5s`
  in one profile, `always/Recorded/30s` in another
- Auto-named from the fn (last `type_name` segment), `.named()` overrides; each is its own `watch` row
- `check()` takes `fn(&Snapshot) -> Verdict` or `async fn(&Snapshot, &SyncCtx) -> Verdict` via a blanket
  `Probe` impl — cheap predicates stay sync, indexer-backed go async, both register identically

### Example: verifying a wallet sync

The only backend-specific line is the one that constructs the subject; everything else is harness API.

```rust
#[ztest::sync_test(name = "wallet_state_sync", description = "genesis→tip wallet sync under chaos",
                   subject = wallet, timeout = "48h", qos = sync, tags = ["chaos", "regtest"])]
async fn wallet_state_sync(mut run: SyncRunner) -> SyncOutcome {
    let (zeb, zai, wallet) = run.topology(|t| (
        t.add_validator(Validator::zebrad("1.9.1").regtest().snapshot(ORCHARD_TESTNET)),
        t.add_indexer(Indexer::zaino("0.4.0").peer("zeb")),
        t.add_wallet(Wallet::librustzcash()),
    )).await?;
    let account = wallet.account(&zeb, &zai, MNEMONIC, BIRTHDAY).await?;
    run.sync(account.wallet().sync_subject(account.id(), Some(PerformanceLevel::High)).await?);

    run.always(Fatal).every(secs(5)).check(height_monotonic);          // nested fns, defined below
    run.eventually(Fatal).window(mins(10)).check(no_stall);
    run.sometimes().check(reorg_handled);
    run.at_completion(Fatal).check(tree_root_matches_indexer);
    run.nemesis().named("split").at(mins(20)).for_(mins(3)).partition(&zai, &zeb).heal_all_on_drop();

    fn height_monotonic(s: &Snapshot) -> Verdict {
        ensure!(s.height() >= s.prev_height() || s.reorg_depth() <= MAX_REORG, "rolled back too far");
        Verdict::Pass
    }
    // …no_stall, reorg_handled, tree_root_matches_indexer likewise
    run.run().await
}
```

- Swap those two subject lines for `run.sync(zaino_indexer)` or a validator handle and every probe,
  fault and report above them is unchanged — that substitution is the whole point of the seam
- A subject a consuming crate wrote binds identically; ztest needs no knowledge of it

Two invariant-authoring rules the harness leans on:

- `balances()`/`work()` return `Option` — a probe reading an extra the subject does not publish
  **panics**, rather than comparing zeroes that can never fail. `run.requires_work(..)` moves that to a
  preflight check against one live reading, so a missing series fails by name in seconds, not hours in
- Balances are `u64`, so "non-negative" claims nothing; the real invariant is "never falls, absent a
  reorg" — a drop means an un-applied block or a lost credited note

## Execution model: ztest-owned pods

Sync outlives the launching terminal → cluster = the daemon, CLI = a stateless controller. All state in
k8s (no local daemon, no `~/.ztest` db).

- Pod labelled `ztest.io/{kind=sync,sync-id,owner}`; `list` = a labelled pod query, so any kubeconfig can
  `list`/`watch`/`stop`
- PVC-backed subject datadir (RWO, NVMe) → a restart *within* the run resumes rather than restarting
- `SyncReport` mirrored to a ConfigMap in `ztest-obs`, beside the same run's Prometheus series + profiles
- **Record and footprint have separate lifetimes**: topology lives in `ztest-sync-{id}`, torn down by the
  driver at finish (`--no-cleanup` keeps it); report/series/profiles live in cluster-lifetime `ztest-obs`
  and are reclaimed only by `ztest cleanup`. A verdict survives everything that produced it — the datadir
  does not (teardown takes the PVC, so `stop`'s checkpoint outlives the run only under `--no-cleanup`)
- `ztest cleanup` must skip `Running` `kind=sync` pods
- Live progress rides the pod log: one structured sentinel line per tick (in-pod `EmitSink`), followed and
  parsed by `ztest sync watch`; k8s log retention means re-attaching resumes mid-flight
- `stop` calls the subject's own graceful stop (checkpoint), never a kill

`ztest sync start` is a profile's **sole** lifecycle owner; `ztest run` never executes at the `sync` tier:

- A profile compiles to an ordinary `#[tokio::test]`, so `cargo nextest list` selects it like any test
- The engine subtracts it using the Phase-C inventory (`sync_by_binary` + `qos_by_binary` →
  `plan::drop_sync_tests`), naming what it dropped
- Subtraction is by *tier*, not by profile registration — a bare `#[ztest::qos::sync]` test must leave too,
  else it survives into the QoS plan and puts a `sync` row in the live panel for work that never launches
- Admitting either parks a 48 h item at top priority for the length of the run

## CLI (provisional)

```
ztest sync list [--all-users] [--json]        # labelled pod query: id, subject, phase, %, age
ztest sync describe <name>                    # body in Collect mode → invariant + nemesis manifest
ztest sync start <name> [--watch] [--no-cleanup]
ztest sync watch <id>                         # attach to live progress; Ctrl-C DETACHES only
ztest sync status <id> [--json]               # finished: final SyncReport (works after the pod is gone);
                                              #   running: the last snapshot
ztest sync stop <id>                          # graceful: sync_mode=Shutdown → checkpoint → exit 0
ztest cleanup <id>                            # namespace + driver pod + record (report + series)
```

- Deletion is deliberately not a `sync` verb — reclaiming is one verb, `ztest cleanup`, run or sync
- Load-bearing UX invariant: `watch` / `start --watch` are read-only tails, detaching never stops a sync

## QoS

- `QosClass::Sync` (`src/qos/`): NVMe pool, 48 h cap, top priority, NVMe taint/toleration
- NVMe node count = the concurrency ceiling; a full pool leaves the pod `Pending` (k8s-native), not failed
- From-genesis in-topology sync wears `#[ztest::qos::sync]`; an external sync (in-process client to a
  remote server) needs only `#[ztest::qos::wallet]`
- A subject's own aggressiveness knob (e.g. the wallet's `PerformanceLevel` batch size) must fit the
  tier's footprint — the harness admits against the tier, not against what the engine decides to buffer

## Status

- Landed: object-safe subject seam (`sync/subject.rs`) + backend-free facade (`sync/facade.rs`), which
  compiles with no wallet feature; probe scheduler
  and taxonomy (`sync/runner.rs`, `sync/probe.rs`); `#[ztest::sync_test]`; nemesis injectors
  (`sync/nemesis.rs` → `NetworkChaos`); detached pod lifecycle (`sync/detached.rs`, `cli/sync/`)
- Outstanding: the indexer proxy — programmable gRPC subjects, turning the chaos surface from "what the
  network can do to a client" into "what a misbehaving server can do to one"

## Open decisions

- Cron expressions vs `Duration` intervals for cadence (intervals + multi-registration cover the real need)
- Snapshot retention over 48 h (~11k small snapshots at 15 s is trivial; rich snapshots ring-buffer)

## See also

- `src/loadtest` — `LwdClient` / `LoadDriver` on the same gRPC substrate
- [design-qos.md](design-qos.md) — the `sync` tier, NVMe pool, calibration
- [design-resources.md](design-resources.md) — the graph `NetworkChaos` plugs into
- [guide-writing-tests.md](guide-writing-tests.md) — `TestEnv` / handle API the topology builder uses
