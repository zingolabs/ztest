# Status: the live cluster view

`ztest status` is a bottom-pinned, always-live view of everything ztest is
doing on the connected cluster: who is running what, how much of the cluster
they hold, when it frees up, and who is waiting. It exists to answer three
questions and deliberately refuses the fourth:

1. **Can I start a run right now, or will I wait?**
2. **Who do I go talk to?**
3. **Is anything stuck?**
4. ~~Is *my* run progressing?~~ — that is `ztest run`'s own pinned panel
   ([design-execution-engine.md](design-execution-engine.md)). `status` is a
   cluster view, not a run view.

## Why the ledger is the feed

Every ztest run already holds a `coordination.k8s.io/Lease` in `ztest-meta`
reserving its slice of the cluster (`qos::ledger`,
[design-qos.md](design-qos.md)). The lease is rewritten on every reconcile tick
— 2 s for an elastic reservation, 20 s for a fixed one — so a run can publish
its status onto that object at **zero additional API cost**. The lease set is
therefore the whole feed: one namespace, one watch, `O(runs)` objects.

This has a consequence worth stating plainly, because it shapes everything
below: **`status` reads reservations, not occupancy.** It never lists pods.
That is sound because `ledger::assert_invariant` *panics* when a leased run's
pods exceed its reservation — over-use is a ztest defect that aborts the run,
not a runtime state the display must render. Reserved is an upper bound the
code enforces, so there is no second "committed" figure that could disagree
with it.

### The one read outside `ztest-*`

The denominator — allocatable CPU/RAM, node count, which nodes are cordoned —
lives on `Node` objects and nowhere else. `status` reads them, reusing
`pipeline::cluster::cluster_allocatable` so the figure matches the preflight
banner and the scheduler ceiling exactly. A cached copy in a ConfigMap was
rejected: node capacity changes precisely when it matters (a cordon, a scaled
pool), and the display prints cordon state as a line of its own.

Nodes are cluster-scoped, read-only, and a handful of objects. Everything else
comes from `ztest-meta`.

### What this feed cannot show

| Not shown | Why | Where to look instead |
| --- | --- | --- |
| Foreign / orphan pod load | Requires a cluster-wide pod list | `ztest cleanup --all-users` |
| Per-run actual usage vs reserved | No committed figure exists (see above) | — |
| Finished runs | `release()` deletes the lease | `ztest store list` |

The finished-run case was considered as a `RECENT` band fed by viewer-side
memory and **rejected**: it would be the only thing on screen the cluster
cannot reproduce, so two terminals watching the same cluster would disagree
about it.

## The beacon

A run publishes its status as annotations on its own lease. `qos::beacon` owns
this vocabulary; both the writer (`ledger::drive`) and the reader
(`cli::status`) go through it, so the encoding has exactly one definition —
a `#[derive(Serialize, Deserialize)]` on `Beacon` itself.

| Key | Type | Meaning |
| --- | --- | --- |
| `ztest.io/beacon` | JSON | **The record.** One serialized `Beacon` |
| `ztest.io/reserve-cpu-milli` | int | Reserved CPU — index key |
| `ztest.io/reserve-mem-bytes` | int | Reserved RAM — index key |
| `ztest.io/kind` | enum | `run` · `build` · `sync` · `claim` — index key |

Labels `ztest.io/run-id` and `ztest.io/user` already exist and already carry
the identity the display groups by.

**One record, three index keys.** The blob is the beacon; the three flat keys
are a denormalized projection of it, written by the same function in the same
patch. They exist because the ledger's hot path — `sum_reservations`,
`reservation_of`, `assert_invariant`, `kind_of` — classifies and sums every
lease in the namespace on each reconcile tick, and must not parse JSON to do
it. Nothing reads a flat key and the blob for the same field: `decode` reads
the blob, the ledger reads the projection, and neither can drift because
neither is written independently.

The one exception is identity. `run_id` and `user` are re-read from
`metadata.name` and the label on every decode, overruling whatever the blob
says — the label reap and the ledger key on the object, so a stale blob must
not be able to disagree about which run a lease belongs to.

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

- **Serde, not a hand-rolled codec.** An earlier revision encoded ten scalars
  by hand beside one serde field in the same struct; the encoder and decoder
  were 86 lines that had to be edited in lockstep for every new field, and the
  decoder carried a back-compat arm for a field the encoder had always
  written. One `derive` is the whole codec now.
- **Footprints are sent, never derived from a tier.** `QosClass::profile_with`
  lets a test override its footprint via `.resources()`, so a tier name plus a
  table lookup would display a figure the scheduler never reserved. `tier`
  ships *beside* `footprint`, not instead of it — an override makes the two
  independent, and the left panel's tally groups by tier.
- The `io_bps`/`io_iops` dimensions are omitted when zero, which is every
  beacon ever written (they are inert pending node calibration). Carrying them
  would double the size of every serialized footprint for two zeroes.
- `running` truncates at 8 because the display shows 3. `running_count` and
  `running_footprint` stay exact, so the overflow row (`+ 12 more · 12c/6Gi`)
  is exact even though the list that produced it is not.
- `completed = total − queued − running_count`. No completed counter, so
  nothing can drift out of agreement with the others.

`started-at` is explicit rather than reusing `creationTimestamp` because
`Reservation::adopt` hands a lease from the CLI to a driver pod; a lapse and
re-create would reset the wall clock the whole time axis is projected from.

### Claim leases

A run blocked in `ledger::acquire` holds nothing today — it spins in the poll
loop, invisible to every other run and to this display. It should instead write
a lease with **`reserve = 0`** and `kind: claim`.

This costs nothing and buys the pending section outright:

- `sum_reservations` adds zero, so admission arithmetic is untouched.
- `assert_invariant` passes trivially (a claim has no pods).
- The TTL sweep reaps it if the waiter dies, like any other lease.
- The pending queue becomes derivable from the lease set alone, so the
  lease-only feed survives intact.
- Claims are ordered by `creationTimestamp`, which is FIFO fair queueing for
  free whenever we want it.

## Derived quantities

Everything the display shows beyond raw annotations is computed in
`qos::beacon`, pure and clusterless.

**Verdict** — `OPEN` / `TIGHT` / `FULL`, from free capacity against the
lightest tier's footprint (`QosClass::Basic.profile().footprint`, the same
`min_viable` threshold `acquire` waits on). Below it, nothing can start.

**ETA** — projected completion for a run, from `started-at`, `completed`,
`queued`, and `tests-running-count`. Returns `Option`: a run with nothing
completed has no throughput, and the display renders `?` rather than a
fabricated countdown. Quantized to the minute under an hour and to 15 minutes
beyond, so a bar's right edge steps instead of shimmering every frame.

Because the tier tally is not carried per-queued-test, the estimate is
**count-based and assumes uniform cost**. A run whose queue is 12 `basic`
tests and 1 `sync` test will project badly. This is accepted; the alternative
is shipping a per-tier queue histogram on the beacon, which can be added later
without changing the display.

**Projected start** — for a claim, the first moment enough capacity frees:
walk the running runs' ETAs in ascending order, accumulate their reserved
footprints against current free capacity, and return the first point that
covers the claim's `needs-*`. Rendered `◇ ~+6m`. No bar: a run that has not
started has no throughput, so any bar *length* would be invented.

## Layout

Two columns. Left is fixed-width text, no bars. Right is a gantt whose bars run
`started-at → now → projected end`, with the elapsed portion solid and the
projection light. Because every bar's `█`→`▒` transition sits exactly at NOW,
the transitions self-align into a vertical seam — the now-line needs no glyph
and can never drift out of register with the axis.

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

One row per **run**, not per user — a user with two runs gets two rows.
Sorted by `started-at` ascending, with your own runs pinned first; start time
never changes, so rows never reorder underneath you, and the resulting diagonal
cascade is what makes a gantt readable as a gantt.

The `USER` cell shows the user with the run-id suffix stripped. The run-id is
appended **only when that user has more than one active run**, which is the
only time it is load-bearing.

### Connector rows

Under each bar, up to **3** running tests hang from the NOW column — the same
anchor as the `█▒` seam, so the names left-align into a readable block and the
position is semantically exact (these tests are running *now*).

```
├→ …nu6_3_topology         15c/15Gi     0:04
└→ …::send_shielded        15c/15Gi     3:38
```

- Newest-launched first, so the latest test is always the first row.
- Names head-truncate: `…nu6_3_topology` keeps the distinguishing half of a
  Rust test path, `sync::feat_nu…` does not.
- Columns are name, footprint, age.
- Overflow rides the third row: `…::sendtoaddress  1c/512Mi  + 12 more · 12c/6Gi`,
  the aggregate being what the elided tests hold.
- Unpadded: a run with one test gets one connector row, not three blanks.
- A `build` run has no connectors; its bar-row label already says `build`.
- A stalled run prints `└→ (none running)` — a blank would read as a rendering
  bug rather than a diagnosis.

### Axis

Fixed asymmetric window, `−20m … +60m`: elapsed times cluster within half an
hour while projections spread to days, and the decision lives in the future
half. `◀` and `▶` mark runs that start before or end after the window; a
48-hour sync is permanently clipped at both ends, which is the correct
rendering of a run nobody is waiting on.

The scale never changes. Auto-fitting to the data was rejected — it would
rescale every time a sync starts, and a stable scale is worth more than fitting
the outlier.

### Pending band

Below a `── PENDING ──` rule. Claims keep the axis but not a bar: a `◇` marks
the projected start, followed by the required footprint, wait duration, and
queue position.

A user who is simultaneously running and waiting appears in both bands. The
bands are separate lists, so this is not a duplicate row.

### Degradation

| Pressure | Response |
| --- | --- |
| Terminal < 100 cols | Left panel collapses to two summary lines above the gantt; single column |
| Terminal < 90 cols | Connector age column drops; footprint survives |
| Height exceeded | Connector blocks degrade 3 slots → 1 across the board **before** any run is elided |
| Height still exceeded | Runs elide into `+ N more runs (users) — Xc/YGi` |
| Cluster idle | Axis collapses entirely; four lines |

A display that hides a run is lying about the cluster; one that hides two test
names is merely terse. Detail is always shed before rows.

### Anomalies

One slot, most severe only, above the cluster block:

```
⚠ ci/8841029 holds 15c/8Gi and has completed no tests in 14m
```

Always carries `user/run-id` — it is a sentence, not a row, so it cannot lean
on the column for identity. Severity order: capacity timeout > stalled >
failing.

## Refresh and RBAC

| Source | Mechanism | Cadence |
| --- | --- | --- |
| Leases (`ztest-meta`) | `list` | 1 s |
| Nodes | `list` | 1 s |
| Render | Whole-frame repaint | Per poll |

Both collections number in the tens, and an elastic run only rewrites its lease
every 2 s, so a paired `list` costs less than the reflector bookkeeping a watch
would need — and `--once` then shares one code path with the live loop rather
than being a second way to read the same state.

The frame repaints by walking the cursor back over its own height and clearing
to end of screen. The clear is what lets the frame *shrink* when a run finishes;
height tracks the cluster rather than a fixed budget.

`status` is strictly read-only and must work for someone who cannot run tests:

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

Bound by `ztest cluster setup`, alongside the `ztest-meta` namespace it already
provisions.

## Surface

```
ztest status              # live, Ctrl-C detaches (never mutates)
ztest status --once       # one frame, exits — CI and scripts
ztest status --json       # machine-readable snapshot
```

Non-TTY falls back to `--once`, mirroring `ztest sync watch`'s `linear` path.

## Deferred

- **Foreign / unreserved load.** Recoverable without a pod list: the reconcile
  loop already computes it (`reserve_from_state` → `split_usage`), so a live
  run can stamp `ztest.io/observed-unreserved-*` on its own lease and the
  viewer takes the freshest. No new reads; add when a foreign-load incident
  justifies it.
- **Per-tier queue histogram** on the beacon, to make the ETA cost-weighted
  rather than count-based.
- **Piecewise-compressed axis** (linear to +1 h, log beyond), if detached syncs
  and test runs routinely coexist and the `▶` escape stops being enough.
- **FIFO admission** off claim-lease ordering. The claims exist; `acquire` still
  takes whatever fits whenever it polls.
