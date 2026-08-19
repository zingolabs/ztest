# Status: the live cluster view

`ztest status` = bottom-pinned, always-live view of everything ztest is doing on the connected cluster.

Answers three questions, refuses the fourth:

1. Can I start a run right now, or will I wait?
1. Who do I go talk to?
1. Is anything stuck?
1. ~~Is *my* run progressing?~~ — that is `ztest run`'s own pinned panel
   ([design-execution-engine.md](design-execution-engine.md)); `status` is a cluster view, not a run view

## Why the ledger is the feed

Every run already holds a `coordination.k8s.io/Lease` in `ztest-meta` reserving its slice (`qos::ledger`,
[design-qos.md](design-qos.md)), rewritten each reconcile tick (2 s elastic, 20 s fixed) → publishing
status onto that object costs **zero extra API calls**. One namespace, one watch, `O(runs)` objects.

**`status` reads reservations, not occupancy** — it never lists pods. Sound because
`ledger::assert_invariant` *panics* when a leased run's pods exceed its reservation: over-use is a ztest
defect that aborts the run, not a state to render. Reserved is a code-enforced upper bound, so no second
"committed" figure can disagree.

### The one read outside `ztest-*`

The denominator (allocatable CPU/RAM, node count, cordons) lives only on `Node` objects.

- Reuses `pipeline::cluster::cluster_allocatable` → the figure matches the preflight banner and the
  scheduler ceiling exactly
- A cached ConfigMap copy was rejected: node capacity changes precisely when it matters (cordon, scaled
  pool), and cordon state gets its own line
- Cluster-scoped, read-only, a handful of objects; everything else comes from `ztest-meta`

### What this feed cannot show

| Not shown                        | Why                                    | Where to look instead       |
| -------------------------------- | -------------------------------------- | --------------------------- |
| Foreign / orphan pod load        | Requires a cluster-wide pod list       | `ztest cleanup --all-users` |
| Per-run actual usage vs reserved | No committed figure exists (see above) | —                           |
| Finished runs                    | `release()` deletes the lease          | `ztest store list`          |

A `RECENT` band fed by viewer-side memory was rejected — the only thing on screen the cluster cannot
reproduce, so two terminals watching one cluster would disagree.

## The beacon

Status published as annotations on the run's own lease. `qos::beacon` owns the vocabulary; writer
(`ledger::drive`) and reader (`cli::status`) both go through it → one definition, a `#[derive(Serialize, Deserialize)]` on `Beacon`.

| Key                          | Type | Meaning                                        |
| ---------------------------- | ---- | ---------------------------------------------- |
| `ztest.io/beacon`            | JSON | **The record.** One serialized `Beacon`        |
| `ztest.io/reserve-cpu-milli` | int  | Reserved CPU — index key                       |
| `ztest.io/reserve-mem-bytes` | int  | Reserved RAM — index key                       |
| `ztest.io/kind`              | enum | `run` · `build` · `sync` · `claim` — index key |

Labels `ztest.io/run-id` + `ztest.io/user` already carry the identity the display groups by.

**One record, three index keys.** The flat keys are a denormalized projection of the blob, written by the
same function in the same patch, because the ledger hot path (`sum_reservations`, `reservation_of`,
`assert_invariant`, `kind_of`) classifies and sums every lease per tick and must not parse JSON. Nothing
reads a flat key and the blob for the same field, and neither is written independently → no drift.

Exception: identity. `run_id`/`user` are re-read from `metadata.name` and the label on every decode,
overruling the blob — the label reap and the ledger key on the object, so a stale blob must not be able
to disagree about which run a lease belongs to.

```json
{
  "run_id": "elicb-47192",
  "user": "elicb",
  "kind": "Run",
  "reserve": { "cpu_milli": 30000, "mem_bytes": 32212254720 },
  "started_at": "2026-08-17T14:20:00Z",
  "total": 17, "queued": 12, "failed": 0,
  "running": [
    {
      "name": "sync::feat_nu6_3_topology",
      "footprint": { "cpu_milli": 15000, "mem_bytes": 16106127360 },
      "started_at": "2026-08-17T14:33:03Z",
      "tier": "Sync"
    }
  ],
  "running_count": 2,
  "running_footprint": { "cpu_milli": 30000, "mem_bytes": 32212254720 },
  "needs": null,
  "eta_override": null
}
```

- **Serde, not a hand-rolled codec** — the previous encoder/decoder pair was 86 lines edited in lockstep
  per field, with a back-compat arm for a field the encoder had always written
- **Footprints are sent, never derived from a tier**: `.resources()` overrides via
  `QosClass::profile_with`, so tier + table lookup would display a figure the scheduler never reserved.
  `tier` ships *beside* `footprint` (the left panel tallies by tier)
- `io_bps`/`io_iops` omitted when zero = every beacon ever written (inert pending calibration); carrying
  them doubles every serialized footprint for two zeroes
- `running` truncates at 8, display shows 3; `running_count`/`running_footprint` stay exact so the
  overflow row (`+ 12 more · 12c/6Gi`) is exact though its source list is not
- `completed = total − queued − running_count` — no counter, nothing to drift
- `started-at` explicit, not `creationTimestamp`: `Reservation::adopt` hands a lease from CLI to driver
  pod, and a lapse + re-create would reset the wall clock the whole time axis projects from

### Claim leases

A run blocked in `ledger::acquire` holds nothing today — spinning in the poll loop, invisible to other
runs and to this display. It should write a lease with **`reserve = 0`**, `kind: claim`:

- `sum_reservations` adds zero → admission arithmetic untouched
- `assert_invariant` passes trivially (a claim has no pods)
- TTL sweep reaps it if the waiter dies, like any lease
- Pending queue becomes derivable from the lease set alone → lease-only feed survives
- Claims ordered by `creationTimestamp` = FIFO fair queueing for free, whenever wanted

## Derived quantities

Computed in `qos::beacon`, pure and clusterless.

- **Verdict** — `OPEN`/`TIGHT`/`FULL` from free capacity against the lightest tier's footprint
  (`QosClass::Basic.profile().footprint`, the same `min_viable` threshold `acquire` waits on); below it
  nothing can start
- **ETA** — from `started-at`, `completed`, `queued`, `tests-running-count`. `Option`: a run with nothing
  completed has no throughput → renders `?` rather than a fabricated countdown. Quantized to the minute
  under an hour, 15 min beyond, so a bar's right edge steps instead of shimmering every frame
- ETA is **count-based, assumes uniform cost** (no per-queued-test tier tally) → a queue of 12 `basic`
  - 1 `sync` projects badly. Accepted; the fix is a per-tier queue histogram on the beacon, addable later
    without touching the display
- **Projected start** — for a claim: walk running ETAs ascending, accumulate their reserved footprints
  against free capacity, return the first point covering the claim's `needs-*`. Rendered `◇ ~+6m`. No
  bar — a run that has not started has no throughput, so any bar *length* would be invented

## Layout

Two columns. Left fixed-width text, no bars. Right a gantt, bars running `started-at → now → projected end`, elapsed solid + projection light. Every bar's `█`→`▒` transition sits exactly at NOW, so the
transitions self-align into a vertical seam — the now-line needs no glyph and cannot drift off the axis.

```
── ztest v0.1.0 ─── okd-home · 100.64.0.3:6443 ──────────────────────────────────────────────────────── 14:33:07 ──

 FULL — 0.0c free        │ USER          −20m        NOW         +20m        +40m        +60m
                         │               │           ▼           │           │           │
 cpu    96.0 / 96.0c 100%│▸elicb 47192       ████████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒┤ 3/17   30c/30Gi   ~48m
 mem    97.0 /128.0Gi 76%│                           ├→ …nu6_3_topology         15c/15Gi     0:04
 yours  46.0c  54.0Gi    │                           └→ …::send_shielded        15c/15Gi     3:38
 peers  50.0c  43.0Gi    │ ci                   ███▒▒┤ 5/23   15c/8Gi   ~11m
                         │                           ├→ …::rpc_getinfo           1c/512Mi    0:12
 runs    5  ·  3 users   │                           ├→ …::getblock_verbose      1c/512Mi    0:31
 tests  20 running · 18 q│                           └→ …::sendtoaddress         1c/512Mi  + 12 more · 12c/6Gi
 pending 2               │▸elicb 51120            █▒▒▒▒┤ build   16c/24Gi   ~6m
 sync    3 · 45c/45Gi    │ jgold 3312    ◀███████████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▶ 2/9    20c/20Gi   ~1h04
 basic  15 · 15c/ 7Gi    │                           ├→ …::reorg_depth_6          3c/3Gi     9:51
 integ   2 ·  6c/ 6Gi    │                           └→ …::mempool_evict          3c/3Gi     2:18
                         │ jgold zsync   ◀══2d══█████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▶ sync   15c/15Gi   ~31h
 okd-home  v1.31.2       │                           └→ …::mainnet_full          15c/15Gi   2d 3h
 6 nodes · 3 cp · 3 wkr  │── PENDING ─────────────────────────────────────────────────────────────────────
 ⊘ okd-worker-2          │ dstaite                   ◇ ~+6m    needs  8c/10Gi   waiting 3:12   1st
 112.0c /160.0Gi capacity│ amber                           ◇ ~+48m  needs 15c/15Gi   waiting 0:41   2nd
```

### Rows

One row per **run**, not per user (two runs = two rows). Sorted `started-at` ascending, own runs pinned
first — start time never changes, so rows never reorder underneath you, and the diagonal cascade is what
makes a gantt readable as a gantt.

`USER` shows the user with the run-id suffix stripped; the run-id is appended **only** when that user has
more than one active run, the only time it is load-bearing.

### Connector rows

Up to **3** running tests hang from the NOW column — same anchor as the `█▒` seam, so names left-align
into a block and the position is semantically exact (these tests are running *now*).

```
├→ …nu6_3_topology         15c/15Gi     0:04
└→ …::send_shielded        15c/15Gi     3:38
```

- Newest-launched first
- Names head-truncate: `…nu6_3_topology` keeps the distinguishing half of a Rust test path,
  `sync::feat_nu…` does not
- Columns: name, footprint, age
- Overflow rides the third row (`…::sendtoaddress  1c/512Mi  + 12 more · 12c/6Gi`), the aggregate being
  what the elided tests hold
- Unpadded — one test = one connector row, not three blanks
- `build` runs have no connectors (the bar-row label already says `build`)
- Stalled run prints `└→ (none running)`; a blank reads as a rendering bug, not a diagnosis

### Axis

Fixed asymmetric window `−20m … +60m`: elapsed times cluster within half an hour, projections spread to
days, and the decision lives in the future half. `◀`/`▶` mark runs starting before or ending after the
window — a 48 h sync is permanently clipped at both ends, the correct rendering of a run nobody waits on.

Scale never changes. Auto-fit was rejected: it rescales every time a sync starts, and stability beats
fitting the outlier.

### Pending band

Below a `── PENDING ──` rule. Claims keep the axis but not a bar: `◇` at the projected start, then
required footprint, wait duration, queue position. A user both running and waiting appears in both bands
— separate lists, not a duplicate row.

### Degradation

| Pressure              | Response                                                                           |
| --------------------- | ---------------------------------------------------------------------------------- |
| Terminal < 100 cols   | Left panel collapses to two summary lines above the gantt; single column           |
| Terminal < 90 cols    | Connector age column drops; footprint survives                                     |
| Height exceeded       | Connector blocks degrade 3 slots → 1 across the board **before** any run is elided |
| Height still exceeded | Runs elide into `+ N more runs (users) — Xc/YGi`                                   |
| Cluster idle          | Axis collapses entirely; four lines                                                |

A display that hides a run lies about the cluster; one that hides two test names is merely terse. Detail
sheds before rows.

### Anomalies

One slot, most severe only, above the cluster block:

```
⚠ ci/8841029 holds 15c/8Gi and has completed no tests in 14m
```

Always carries `user/run-id` — a sentence, not a row, so it cannot lean on the column for identity.
Severity: capacity timeout > stalled > failing.

## Refresh and RBAC

| Source                | Mechanism           | Cadence  |
| --------------------- | ------------------- | -------- |
| Leases (`ztest-meta`) | `list`              | 1 s      |
| Nodes                 | `list`              | 1 s      |
| Render                | Whole-frame repaint | Per poll |

- Both collections number in the tens and an elastic run rewrites its lease only every 2 s → a paired
  `list` costs less than a watch's reflector bookkeeping, and `--once` shares one code path with the live
  loop instead of being a second way to read the same state
- Repaint walks the cursor back over its own height and clears to end of screen; the clear is what lets a
  frame *shrink* when a run finishes, so height tracks the cluster, not a fixed budget

Read-only, and must work for someone who cannot run tests:

```yaml
# Role in ztest-meta
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "watch"]
# ClusterRole
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "list", "watch"]
```

Bound by `ztest cluster setup`, beside the `ztest-meta` namespace it already provisions.

## Surface

```
ztest status              # live, Ctrl-C detaches (never mutates)
ztest status --once       # one frame, exits — CI and scripts
ztest status --json       # machine-readable snapshot
```

Non-TTY falls back to `--once`, mirroring `ztest sync watch`'s `linear` path.

## Deferred

- **Foreign / unreserved load** — recoverable without a pod list: the reconcile loop already computes it
  (`reserve_from_state` → `split_usage`), so a live run can stamp `ztest.io/observed-unreserved-*` on its
  own lease and the viewer takes the freshest. Add when an incident justifies it
- **Per-tier queue histogram** on the beacon → cost-weighted ETA
- **Piecewise-compressed axis** (linear to +1 h, log beyond), if syncs and runs routinely coexist and `▶`
  stops being enough
- **FIFO admission** off claim-lease ordering — claims exist; `acquire` still takes whatever fits
