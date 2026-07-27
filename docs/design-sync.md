# Sync testing

A test class for **long-running chain sync** — a wallet, indexer, or validator
syncing from birthday/genesis to tip — that runs in a ztest-owned pod (up to the
`sync` QoS tier's 48 h cap), **continuously asserts** correctness invariants
against the in-progress sync, injects network chaos, and passes only when the
subject reaches tip with every invariant intact.

Sync is not a one-shot `arrange → act → assert`. It is a **continuous monitor
with a completion predicate**: the runner owns the sync lifecycle, produces a
snapshot each tick, evaluates a set of probes at their own cadences, and
terminates `pass` when the completion predicate fires or `fail` when a fatal
invariant is violated.

## Goals

| Goal                   | Signal                                                                       | Gating |
| ---------------------- | ---------------------------------------------------------------------------- | ------ |
| End-state correctness  | at tip: balances / note-commitment-tree root match an independent authority  | yes    |
| Continuous invariants  | core Zcash chain guarantees hold on every tick of a multi-hour sync          | yes    |
| Live progress          | streamed scan phase + % to tip, surfaced in `ztest sync watch`               | no     |
| Throughput / resource  | blocks-s, outputs-s, CPU/mem/IO profile over the run (feeds the phase model) | no     |
| Robustness under chaos | sync recovers from partitions / packet loss / dropped links                  | yes    |

Perf-gating and calibration are explicitly **not** primary here; the `sync` QoS
tier is an admission/placement concern (§QoS), not a latency budget.

## One harness, three subjects

There is no shared sync *engine*: a wallet scans over lightwallet gRPC
(pepper-sync), an indexer ingests blocks from its backing validator, and a
validator downloads + verifies from P2P peers. Each engine is internal to its
component. What is shared is the **harness** — runner, probes, nemesis, history,
report, `watch`, the QoS tier, the pod lifecycle. Only two things vary per
subject: where progress comes from, and whether ztest drives the sync or only
observes it.

| Subject   | Engine              | ztest's role          | Progress source                               | Runs where         |
| --------- | ------------------- | --------------------- | --------------------------------------------- | ------------------ |
| Wallet    | pepper-sync         | **driver + observer** | `pepper_sync::sync_status` (in-process)       | the runner process |
| Indexer   | zaino/lwd ingestion | observer + chaos      | `GetLightdInfo` vs validator height (RPC)     | its own pod        |
| Validator | zebrad/zcashd P2P   | observer + chaos      | `getblockchaininfo` (blocks/headers/progress) | its own pod        |

```rust
#[async_trait]
pub trait SyncSubject {
    type Snapshot: ProgressView;                       // height / target / pct / phase — common view
    async fn snapshot(&self) -> Result<Self::Snapshot, RpcError>;
    async fn is_complete(&self, s: &Self::Snapshot) -> bool;
    // driver hooks — real for wallet, no-ops for the self-syncing subjects
    async fn start(&mut self) -> Result<(), RpcError> { Ok(()) }
    async fn stop(&mut self)  -> Result<(), RpcError> { Ok(()) }
}
```

`SyncRunner<S: SyncSubject>` is generic. Probes read the common `ProgressView`
(monotonic height, no-stall, reached-target work for all three subjects) and
reach subject extras when needed. The progress sources already exist on the
handles: `IndexerBackend::{latest_block_height, indexer_info, wait_for_block_num}`
and `ValidatorBackend::{chain_height, tip}`.

For a **wallet** ztest owns the engine (§pepper-sync). For **indexer/validator**
the component drives itself; the runner is a pure prober + chaos injector over
the component's RPC.

## `SyncTarget`

The wallet subject syncs against a `SyncTarget` — in-topology *or* external, the
test body does not care which:

```rust
SyncTarget::in_topology(&validator, &indexer).await?   // queries activation + grpc_uri
SyncTarget::external(uri, Network::Mainnet)            // no env; real public endpoint
```

`Endpoint { host: IpAddr, port: u16 }` cannot hold a scheme/DNS-host/TLS URI, so
`SyncTarget` carries a full URI string, not an `Endpoint`. In-topology targets
format `http://…` from the resolved port-forward/pod IP; external targets carry
`https://…` (TLS from the scheme) and bypass the env entirely.

## pepper-sync integration (the wallet subject)

pepper-sync is a **standalone sync engine** — multi-task (fetcher, mempool
monitor, batcher, parallel scan workers), non-linear (chain-tip-first,
spend-before-sync, shard-based), with reorg verification built in. Its whole
public surface:

```rust
async fn sync<P, W>(client: CompactTxStreamerClient<Channel>, params: &P,
                    wallet: Arc<RwLock<W>>, sync_mode: Arc<AtomicU8>,
                    config: SyncConfig) -> Result<SyncResult, _>
async fn sync_status<W>(wallet: &W) -> Result<SyncStatus, _>   // poll anytime, from outside
// config::{SyncConfig, PerformanceLevel(Low/Medium/High/Maximum), TransparentAddressDiscovery}
// wallet::{SyncWallet + SyncBlocks + SyncTransactions + SyncNullifiers + SyncOutPoints + SyncShardTrees}
```

The engine is parameterized on four things the consumer owns and one it
implements:

| Seam                                  | Owner               | Unlocks                                               |
| ------------------------------------- | ------------------- | ----------------------------------------------------- |
| `client: CompactTxStreamerClient`     | **ztest** (we dial) | dialing, TLS, external URI — **and wrapping** (chaos) |
| `sync_mode: Arc<AtomicU8>`            | **ztest**           | pause / resume / stop / checkpoint                    |
| `wallet: Arc<RwLock<W>>`              | **ztest**           | poll `sync_status`, read balances/tree-roots          |
| `params: P: consensus::Parameters`    | **ztest**           | network selection (mainnet/testnet/regtest)           |
| `W: Sync{Wallet,Blocks,…,ShardTrees}` | the wallet impl     | note storage, shard trees, serialization              |

**We rent pepper-sync; we do not reimplement it.** pepper-sync *is* the system
under test — a wallet-sync harness exists to exercise the code users run. A
rolled-own scanner would test nothing real. Rolling our own is rejected with
prejudice; if intra-scan-loop determinism is ever needed, the correct move is
upstreaming a fault/progress callback into pepper-sync, not forking it.

**Integrate directly with `pepper_sync::sync`, below zingolib's `LightClient`.**
zingolib's `LightClient::sync_and_await()` (the current call in
`src/backends/zingo.rs`) hides all four seams. Driving `sync` directly lets
ztest own the client (→ chaos), `sync_mode` (→ stop/checkpoint), and the wallet
lock (→ status on our schedule), while **renting** zingolib's `LightWallet` as
the `W` impl (those six storage traits are shard trees + note stores; ztest
already constructs a `LightWallet` in `build_light_client`). The `W` sits behind
a ztest seam so pepper-sync's own `mocks.rs` test wallet, or a future
non-zingolib wallet, can slot in.

`params: P: consensus::Parameters` **is the unified network carrier.** zingolib's
`ChainType` already implements `consensus::Parameters` for all three networks, so
`ChainType::{Mainnet, Testnet, Regtest(configured)}` passes straight into `sync`
and the `load_clientconfig` + `LightClient` detour disappears. `ChainType` stays
inside the `zingo`-gated backend; the ztest-neutral `Network`/`ChainParams` at
the `WalletBackend` boundary maps to it.

Two gotchas, pinned:

- **Completion keys on `sync_mode`, not the percentage.** When ranges are
  `RefetchingNullifiers`/`ScannedWithoutMapping`, `sync_status` caps its reported
  percentage at 99 % until truly done. A "100 % == done" predicate fires early.
  Complete on `sync_mode == NotRunning` + the returned `SyncResult`.
- **Status polling contends with the scan write-lock.** The engine takes the
  wallet write-lock per batch (longer under `PerformanceLevel::Maximum`).
  Mitigate with a coarse probe cadence (seconds), and `Paused`-then-read for
  expensive `at_completion` checks — `Paused` releases the lock by design.

Use `percentage_total_outputs_scanned` as the headline metric (scanning is
non-linear in height; its own doc-comment says outputs are the accurate signal).
`scan_ranges` + `ScanPriority` (Verify / ChainTip / Historic / FoundNote /
RefetchingNullifiers / Scanned) give the live phase, including the reorg
verification phase — surfaced in `watch` and observable by probes.

## The unified gRPC substrate

`pepper_sync::sync` takes the same generated `CompactTxStreamerClient<Channel>`
that lives in `src/proto/`, that the load driver's `LwdClient` wraps, and that
`IndexerBackend` RPCs use. So one substrate underlies everything:

```
SyncTarget ─▶ Channel ─▶ [optional Nemesis tower layer] ─▶ ┬─ IndexerBackend RPCs (oracle authority)
 (endpoint OR external URI+TLS)                            ├─ LwdClient (load driver)
                                                           └─ pepper_sync::sync (the engine)
```

One dialing/TLS/port-forward/fault layer, three consumers. The correctness
oracle (wallet tree-root vs `IndexerBackend::get_tree_state`) rides the same
channel — no second wallet impl needed; the indexer is the independent authority
(the CometBFT app-hash / Hive head-hash analog).

## Continuous verification: the invariant taxonomy

Per-tick callbacks alone are insufficient (Jepsen/Antithesis/TLA+). A probe is
one of four classes, plus a recorded snapshot history checked at the end:

| Class                     | Semantics                    | Evaluated               | Failure means   | Wallet-sync examples                                                                                |
| ------------------------- | ---------------------------- | ----------------------- | --------------- | --------------------------------------------------------------------------------------------------- |
| **always** (safety)       | true at *every* tick         | per-tick, live          | a **bug**       | height monotonic (except bounded reorg); balances ≥ 0; per-pool outputs monotonic; chain continuity |
| **eventually** (liveness) | becomes true before deadline | live + terminal         | a **stall**     | progress within a window; recovers after a heal; reaches tip                                        |
| **sometimes** (coverage)  | true on ≥1 tick over the run | end-of-run over history | a **weak test** | observed a reorg handled; exercised the reconnect path                                              |
| **at_completion**         | post-condition once at tip   | terminal                | final-state bug | balances match oracle; note-commitment-tree root == indexer's                                       |

The **sometimes** class is load-bearing, not decoration: a green 48 h run that
never triggered its coverage probes means the chaos never entered the interesting
state — a weak pass, not a real one. Each fault is gated behind a `sometimes` so
a pass *proves* the adverse state was reached.

## Scheduled probes

Grounded in Prometheus rule evaluation + Gomega `Consistently` + CronJob
semantics:

- **Per-probe cadence** (`every(dur)` / `every_blocks(n)` / `each_tick` /
  `window(dur)`). Each probe runs on its own timer; cheap safety checks tick
  fast, expensive RPC-backed checks slow. No shared global tick.
- **Snapshot-then-evaluate.** One immutable `Snapshot` per tick; all probes due
  at that tick read it. Keeps predicates pure and shares one wallet read.
- **`hold_for` / `keep_for`** (Prometheus `for:` / `keep_firing_for:`), quantized
  to the cadence — sync signals are noisy (reorgs, RPC gaps); a bare threshold
  flaps.
- **Missed-tick policy is explicit.** If a probe is still running at its next
  tick, default **Skip/Coalesce** — never backlog (thousands of stale evals over
  48 h). A skip is a distinct, visible outcome.
- **Three outcomes, not a bool** (Gomega's discipline): `Satisfied` /
  `Pending(retry)` / `Violated(record)` / `ProbeError(abort)`. A probe that
  *throws* means the harness/RPC is broken and aborts the run; a probe that
  returns false is "keep going". This lets the reporter distinguish "invariant
  violated" from "probe crashed".
- **Multi-cadence = multiple registrations**, not a per-probe combinator (SLO
  multi-window multi-burn-rate): register the same predicate at 30 s and 1 h with
  different severities.

## Chaos: the nemesis

Adversity is injected at three altitudes, all **outside** pepper-sync (the SUT):

| Altitude      | Mechanism                                                  | Models                                                                  | Determinism                       |
| ------------- | ---------------------------------------------------------- | ----------------------------------------------------------------------- | --------------------------------- |
| Channel       | `tower` layer wrapping the tonic `Channel`                 | wallet↔indexer link: latency, drops, errors, throttle                   | high — seed-reproducible, per-RPC |
| k8s network   | NetworkPolicy + netem on real pods                         | anywhere in the topology: validator↔indexer, peer partitions, bandwidth | low — real kernel netem           |
| Indexer proxy | ztest-owned `CompactTxStreamer` between wallet and indexer | reorgs, stalled ranges, malformed blocks, scripted chains               | total — the generator             |

For **indexer/validator** subjects there is no in-process channel to wrap (the
sync link lives inside the component's pod), so chaos is necessarily
k8s-network-level: partition a validator from its P2P peers → heal → assert reorg
recovery; partition indexer↔validator → assert catch-up. This makes the
`NetworkChaos` resource the centerpiece for two of three subjects. The indexer
proxy is gRPC-only (validator P2P is the Zcash wire protocol; netem, not a proxy).

Chaos is scheduled on the same cron-tick model, probabilistic faults use a
seed-logged `buggify` posture (FoundationDB) for deterministic replay, and each
fault is recorded in the history.

**Safety is a harness correctness property, not a nicety.** A partition or netem
rule that leaks past a crashed test wedges the shared cluster. Every fault is a
first-class node in the resource graph (`src/resource/`), reaped by the
parent-owned label cleanup, **and** carries a kernel-side dead-man's-switch
(netem TTL / revert-on-timeout, per Chaos Mesh's `duration`). Chaos that cannot
guarantee its own reversion is not shipped.

Native vs delegated: ztest owns chaos natively — NetworkPolicy for partitions
(declarative, zero privilege) and a privileged netem sidecar for delay/loss/
bandwidth (reusing the buildkit SCC machinery) — rather than taking a Chaos Mesh
controller dependency, consistent with ztest's self-contained posture.

## Test-author API

A profile is authored with a `#[ztest::sync_test(...)]` annotation whose body is
a **registration program**. The split is principled:

- **Annotation — static, known before running:** name, description, subject
  kind, timeout, QoS tier, tags. Powers `ztest sync list` / `--help` and QoS
  admission (the scheduler sizes the pod before the body runs).
- **Body — runs in the pod:** builds the topology, binds the subject, and
  registers named invariant fns — defined **inline in the body** — at chosen
  cadences, plus the nemesis schedule.

Because the body is pure registration up to `run.run()`, it executes in two
modes: **Execute** (provision + sync) and **Collect** (`run.run()` inert; returns
the recorded manifest). `ztest sync describe <name>` runs Collect mode and prints
the full invariant + nemesis manifest **without touching a cluster** — static
discoverability from a readable imperative flow.

Refinements over a plain builder or per-assertion macro:

- **Invariants are nested `fn` items in the body.** Each assertion is its own
  named function, defined inline right next to its registration. A nested `fn`
  is an item, not a closure: it cannot capture the enclosing scope, so an
  accidental capture is a compile error and the invariant stays a pure
  predicate — but it *can* see block-level `const`/`fn` items (e.g. a local
  `MAX_REORG`), and nested `async fn` items are allowed for RPC-backed checks.
  Genuinely shared invariants factor into a module and are referenced by path
  (see the second profile).
- **Predicate decoupled from class/cadence/severity.** The fn is just a
  predicate; the body decides how it is used, so the same `height_monotonic`
  registers `always/Fatal/5s` in one profile and `always/Recorded/30s` in
  another.
- **Auto-naming** from the fn (last segment of `type_name`), `.named()` override.
  Each invariant is its own live row in `watch`.

`check()` accepts both `fn(&Snapshot) -> Verdict` and
`async fn(&Snapshot, &SyncCtx) -> Verdict` via a blanket `Probe` impl — cheap
invariants stay pure sync fns, indexer-backed ones go async, both register
identically.

### Profile: chaos wallet state-sync

Invariants are nested `fn`s in the body — registered up top, defined below.

```rust
use ztest::prelude::*;
use ztest::sync::{Fault::*, Severity::*};

#[ztest::sync_test(
    name        = "state_sync",
    description = "genesis→tip wallet sync through zebrad+zaino under network chaos",
    subject     = wallet,
    timeout     = "48h",
    qos         = sync,
    tags        = ["chaos", "wallet", "regtest"],
)]
async fn test_state_sync(mut run: SyncRunner) -> SyncOutcome {
    const MAX_REORG: u32 = 100;   // Zcash rollback bound (~coinbase maturity); deeper = violation

    // topology + subject (real code — too rich for an annotation)
    let (zeb, zai, account) = run.topology(|t| {
        let zeb = t.add_validator(Validator::zebrad("1.9.1").regtest()
            .mount(mount_archive!("tests/assets/zebrad-testnet-1M.tar.zst", "/cache")));  // cached-state seed
        let zai = t.add_indexer(Indexer::zaino("0.4.0").peer("zeb"));
        let wal = t.add_wallet(Wallet::zingo());
        (zeb, zai, wal)
    }).await?;
    run.sync(Subject::wallet(account).performance(PerformanceLevel::High));

    // register invariants — each is a nested fn (defined below); names auto-derive
    run.always(Fatal).every(secs(5)).check(height_monotonic);
    run.always(Fatal).every_blocks(2_000).check(chain_continuity);
    run.always(Fatal).every(secs(10)).check(pool_outputs_monotonic);
    run.always(Recorded).each_tick().check(balance_nonnegative);

    run.eventually(Fatal).window(mins(10)).check(no_stall);
    run.eventually(Fatal).window(mins(15)).after("net-split").check(recovers_from_partition);

    run.sometimes().check(reorg_handled);
    run.sometimes().check(reconnect_after_drop);

    run.at_completion(Fatal).check(tree_root_matches_indexer);
    run.at_completion(Fatal).check(reached_network_tip);

    // nemesis — network errors + minor injected faults, scheduled + probabilistic
    run.nemesis()
       .named("net-split").at(mins(20)).for_(mins(3)).partition(&zai, &zeb)         // k8s NetworkPolicy
       .at(mins(35)).for_(mins(2)).netem(&zai, Delay(300).jitter(80).loss(0.03))    // tc/netem on the pod
       .channel(&account).buggify(0.01, DropConnection)                             // 1% of gRPC calls dropped
       .channel(&account).buggify(0.02, SlowResponse(secs(2)))                      // 2% minor latency
       .seed(0x5EC0_1DAB)                                                           // deterministic replay
       .heal_all_on_drop();                                                         // dead-man's-switch

    // ── invariants: nested fns; pure predicates over the snapshot (+ ctx for RPC) ──
    fn height_monotonic(s: &Snapshot) -> Verdict {
        ensure!(s.height() >= s.prev_height() || s.reorg_depth() <= MAX_REORG,
                "height went backwards {} → {} beyond reorg bound", s.prev_height(), s.height());
        Verdict::Pass
    }
    fn pool_outputs_monotonic(s: &Snapshot) -> Verdict {
        ensure!(s.outputs(Sapling) >= s.prev_outputs(Sapling));
        ensure!(s.outputs(Orchard) >= s.prev_outputs(Orchard));
        Verdict::Pass
    }
    fn balance_nonnegative(s: &Snapshot) -> Verdict { verdict(s.balances().total() >= 0) }
    fn no_stall(s: &Snapshot) -> Verdict {
        ensure!(s.progressed_within(mins(10)), "no progress in 10m at height {}", s.height());
        Verdict::Pass
    }
    fn recovers_from_partition(s: &Snapshot) -> Verdict {
        ensure!(s.progressed_since_fault(), "sync did not resume after the partition healed");
        Verdict::Pass
    }
    fn reorg_handled(s: &Snapshot) -> Verdict { want!(s.observed_reorg()) }          // coverage
    fn reconnect_after_drop(s: &Snapshot) -> Verdict { want!(s.observed_reconnect()) }

    async fn chain_continuity(s: &Snapshot, cx: &SyncCtx) -> Verdict {
        let blocks = cx.indexer().get_block_range(s.prev_height(), s.height()).await?;
        chain_link(&blocks)                    // genesis zeros · 32-byte hashes · prev_hash == prior.hash
    }
    async fn tree_root_matches_indexer(f: &FinalView, cx: &SyncCtx) -> Verdict {
        let ts = cx.indexer().get_tree_state(f.tip()).await?;
        ensure_eq!(f.tree_root(Orchard), ts.orchard_root);
        ensure_eq!(f.tree_root(Sapling), ts.sapling_root);
        Verdict::Pass
    }
    async fn reached_network_tip(f: &FinalView, cx: &SyncCtx) -> Verdict {
        ensure_eq!(f.tip(), cx.indexer().latest_block_height().await?);
        Verdict::Pass
    }

    run.run().await
}
```

Item order inside the body is irrelevant — the nested fns are visible to the
`.check(...)` calls above them, and each can read the block-level `MAX_REORG`.

### Profile: shared invariants, different subject

A validator sync reuses the genuinely-common invariants from a shared module
(`inv::`) — the factoring the first profile's inline fns would graduate into
once a second profile wants them — and keeps `subject = validator` so the
runner only observes (zebrad drives itself).

```rust
#[ztest::sync_test(
    name = "mainnet_full", description = "zebrad full mainnet sync from genesis, long-haul",
    subject = validator, timeout = "48h", qos = sync, tags = ["mainnet", "longhaul"],
)]
async fn test_mainnet_full_sync(mut run: SyncRunner) -> SyncOutcome {
    let zeb = run.topology(|t| t.add_validator(
        Validator::zebrad("1.9.1").mainnet().mount(mount_archive!("tests/assets/zebrad-main-cache.tar.zst", "/cache"))
    )).await?;
    run.sync(Subject::validator(zeb));                          // observer subject — zebrad drives itself

    run.always(Fatal).every(secs(30)).check(inv::height_monotonic);   // same fn, coarser cadence
    run.eventually(Fatal).window(mins(20)).check(inv::no_stall);      // same fn, wider window
    run.sometimes().check(inv::reorg_handled);

    run.nemesis().named("peer-split").at(hours(2)).for_(mins(10)).isolate_p2p(&zeb).seed(1).heal_all_on_drop();
    run.run().await
}
```

## Execution model: ztest-owned pods

A sync outlives the launching terminal, so the cluster is the daemon and the CLI
is a stateless controller. State lives in k8s (no local daemon, no `~/.ztest`
DB): a **ztest-owned pod** labelled `ztest.io/{kind=sync,sync-id,owner}`, a
**PVC-backed wallet/cache datadir** (RWO, NVMe) so a restart resumes rather than
rescans, and a `SyncReport` written to the PVC + mirrored to a ConfigMap so it
outlives the pod. `list` is a labelled pod query; any machine with the kubeconfig
can `list`/`watch`/`stop`.

Detached syncs live in a **persistent, user-scoped** namespace (not an ephemeral
per-run one). `ztest cleanup` must skip `kind=sync` pods that are `Running`.

Live progress rides the pod's **log stream**: the runner prints a structured
sentinel line per tick (the in-pod `EmitSink`); `ztest sync watch` follows the
pod log, parses it, and renders per-invariant rows + scan phase. k8s log
retention means re-attaching resumes mid-flight. `stop` flips `sync_mode` to
`Shutdown` (graceful checkpoint to PVC), not a kill.

Same sync library, two lifecycle owners: a `#[ztest::sync_test]` run via the
engine (pod-per-test, reaped on Ctrl-C) for CI gating, and `ztest sync start`
owned by k8s directly (survives the terminal; ended only by `stop`).

## CLI (provisional — surface subject to change)

```
ztest sync list [--all-users] [--json]        # labelled pod query: id, subject, phase, %, age
ztest sync describe <name>                     # body in Collect mode → invariant + nemesis manifest
ztest sync start <name> [--watch]              # admit + create the ztest-owned pod; --watch attaches
ztest sync watch <id>                          # attach to live progress; Ctrl-C DETACHES only
ztest sync status <id> [--json]                # one-shot last snapshot
ztest sync report <id> [--json]                # final SyncReport (works after the pod is gone)
ztest sync stop <id>                           # graceful: sync_mode=Shutdown → checkpoint → exit 0
ztest sync rm <id> [--purge]                   # delete pod (+ PVC with --purge)
```

The load-bearing UX invariant: `watch` / `start --watch` are read-only tails;
detaching never stops the sync. Ending a sync is only `stop`.

## QoS

`QosClass::Sync` already exists (`src/qos/`): NVMe pool, 48 h cap, top priority,
NVMe taint/toleration; the NVMe node count is the concurrency ceiling. A
from-genesis in-topology sync wears `#[ztest::qos::sync]`; an external sync
(in-process client to a remote server) needs only `#[ztest::qos::wallet]`.
`PerformanceLevel::Maximum` (quadrupled batch, unbounded nullifier map) must be
consistent with the tier's 32 GiB. `start` admits against the tier; a full NVMe
pool queues the pod `Pending` (k8s-native) rather than failing fast.

## Build order

1. **Unified channel substrate + `SyncTarget`** — `SyncTarget → Channel`, shared
   by `IndexerBackend`, `LwdClient`, and `pepper_sync::sync`. Owns dialing/TLS.
1. **Direct pepper-sync driver + `SyncReport`** — drive `pepper_sync::sync`
   (rent `LightWallet`), poll `sync_status`, produce the report. Replaces
   `LightClient::sync_and_await`.
1. **`SyncRunner` + probe scheduler + invariant taxonomy** — the tick scheduler,
   snapshot capture, history, probe isolation, the four classes.
1. **`#[ztest::sync_test]` macro + Collect/Execute modes** — the authoring
   surface, `list`/`describe`.
1. **`NetworkChaos` resource (reversion-safe) + nemesis** — NetworkPolicy +
   netem sidecar, dead-man's-switch, the schedule/buggify API.
1. **ztest-owned pod lifecycle + `ztest sync` CLI** — detached job model, PVC
   resume, `watch` log-stream renderer.
1. **Indexer proxy** — the programmable-environment capability multiplier
   (gRPC subjects), fast-follow.

## Open decisions

- Nemesis in v1 vs fast-follow: build the channel-ownership seam now (avoids a
  later re-architecture), land the fault injectors after the probe core.
- `SyncSubject` implemented for wallet + validator first (validator forces the
  k8s-chaos design to be real), indexer next.
- Full cron expressions vs `Duration` intervals for cadence (intervals +
  multi-registration cover the real need).
- Snapshot retention richness over 48 h (~11k small snapshots at 15 s is trivial;
  rich snapshots ring-buffer).

## See also

- [design-load-testing.md](design-load-testing.md) — the `LwdClient` / `LoadDriver`
  sharing the same gRPC substrate
- [design-qos.md](design-qos.md) — the `sync` tier, NVMe pool, calibration
- [design-resources.md](design-resources.md) — the resource graph the
  `NetworkChaos` node plugs into
- [guide-writing-tests.md](guide-writing-tests.md) — the `TestEnv` / handle API
  the topology builder uses
