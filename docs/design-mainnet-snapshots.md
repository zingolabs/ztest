# Mainnet chain snapshots

Companion to `fixtures/chains/README.md`, which documents the testnet ladder
this extends. The goal is a boundary ladder of pre-populated zebra states on
**Mainnet**, so `ztest sync` can measure zaino index construction against real
mainnet density rather than testnet's sparse chain.

The testnet ladder proved the mechanism. Mainnet is the same mechanism at
roughly 30× the bytes, and the interesting work is entirely in the places where
"30×" stops being a number and starts being a broken assumption.

## 1. Why mainnet at all

A zaino sync run is *index construction*: the validator boots from the archive
and is complete on tick one, while the indexer reads that clone and builds its
own index into pod-local scratch, empty at start (`src/regtest.rs:186-191`).
What the run measures is how fast zaino turns N blocks of chain into an index.

Testnet blocks are not mainnet blocks. Testnet's whole chain at 4,140,000 is
9.7 GB; mainnet is documented at ~300 GB (`zebra/book/src/user/requirements.md:23`).
The ratio is shielded-output and transaction density, and density is exactly
the variable zaino's indexer is sensitive to — `zaino_sync_sapling_outputs_total`
and `zaino_sync_orchard_actions_total` are what the zaino subject reports as
`Work` (`src/backends/zainod.rs:924-946`). A throughput number drawn from
testnet is a number about a chain nobody uses.

## 2. The ladder

Boundary-pinned, mirroring testnet: each rung sits **6,000 blocks past** its
activation, for the reason the testnet set already documents — a snapshot pinned
*at* an activation holds none of the data it is named for, and every assertion
drawn from it passes while proving nothing.

| Rung | Mainnet activation | Pin | Why |
| --- | --- | --- | --- |
| Sapling | 419,200 | 425,200 | Sprout JoinSplits and v1–v4 transaction diversity; the small rung |
| Blossom | 653,600 | 659,600 | No pool introduced; block timing and a denser address graph |
| NU5 / Orchard | 1,687,104 | 1,693,104 | First v5 transactions and a funded Orchard pool |
| NU6.3 / Ironwood | 3,428,143 | 3,434,143 | The scale rung: commitment trees and the finalised seam under real load |

Heartwood (903,000), Canopy (1,046,400) and NU6 (2,726,400) are contained within
the deeper pins and introduce nothing that changes what a compact block carries,
which is the same reasoning that kept them off the testnet ladder. If the size
budget bites, **Blossom is the rung to drop** — it is the one whose value is not
a pool.

A rung between NU5 and the deepest pin (NU6 + 6,000 = 2,732,400) is worth
considering *only* as a size rung: it would sit between the ~tens-of-GB and
~hundreds-of-GB extremes and give the harness something to exercise per-artifact
PVC sizing against without paying for the deepest artifact. Decide after §4's
measurements, not before.

## 3. Step zero: derive the mainnet activation table from the producer

**Do not read activation heights from a source checkout.** The producer script
says so in its own comment (`zaino/scripts/produce-chain-fixture.sh:71-89`) and
it is not hypothetical: the local `zebra` checkout is a v4.4.x lineage that has
no NU6.2 or NU6.3 constants at all, and its mainnet table ends at NU6.1
(3,146,400). A ladder built from it would silently omit whatever the deepest
mainnet boundary actually is.

Derive it from a running `zfnd/zebra:6.2.3` on mainnet:

```
curl -su "$(cat .cookie)" --data-binary \
  '{"jsonrpc":"2.0","id":1,"method":"getblockchaininfo","params":[]}' \
  -H 'content-type: application/json' http://127.0.0.1:8232/ \
| jq -r '.result.upgrades|to_entries[]|"\(.value.name) \(.value.activationheight)"'
```

**Resolved 2026-08-11.** Run against `zfnd/zebra:6.2.3`, mainnet's schedule is
Overwinter 347,500 · Sapling 419,200 · Blossom 653,600 · Heartwood 903,000 ·
Canopy 1,046,400 · NU5 1,687,104 · NU6 2,726,400 · NU6.1 3,146,400 ·
NU6.2 3,364,600 · NU6.3 3,428,143. NU6.3/Ironwood **does** exist on mainnet, so
the deepest rung is 3,428,143 + 6,000 = 3,434,143. The RPC does not report
`before_overwinter`; it comes from the network constant (1).

## 4. Size and time: what is estimated and what must be measured

Nothing below is load-bearing until measured. It exists to set the storage
budget and to say which numbers to watch.

**Sizes.** Growth tracks cumulative shielded activity, not height, so the ladder
is steeply back-loaded — most of mainnet's state is post-NU5. Zebra's ~300 GB
figure appears to describe a settled, compacted node (its testnet figure of
10 GB matches our *compacted* 9.7 GB almost exactly), which means the
pre-compaction intermediate from a fast checkpoint sync is larger still. The
testnet deep rung compacted 22.7 GB → 9.7 GB; expect the same shape.

Production is self-calibrating: each rung's manifest records
`uncompressed_bytes`, so rung N tells you what to budget for rung N+1. Record
them in this table as they land.

**Time.** Testnet sustained 15,000–22,000 blk/min once warm. Mainnet blocks are
heavier and this will not hold; treat mainnet throughput as unknown until the
first rung, and do not measure inside the first ~10 minutes after a container
recreate — that window is peer-set rebuild, not steady state, and reading it as
throughput is wrong by roughly 50×.

**One sync pass produces the whole ladder.** Heights ascend on one volume, so
the run goes genesis → 425,200 → archive → 659,600 → archive → … Each rung
after the first costs only its delta. This is why the ladder is cheap once
started and why losing the volume is expensive.

**Upload.** The multipart path already exists (`src/storage/r2.rs:301-340`),
built for exactly this — R2's single-PUT cap is 4.995 GiB and the testnet deep
rung is 8.15 GiB. The mainnet rungs are bounded by uplink, not by the protocol.

## 5. Distribution

Git LFS in-tree, same as testnet: `.tar.zst` LFS-tracked, `.toml` manifest
plaintext beside it. One code path, and the key layout stays compiler-checked
between `ztest lfs-transfer` (writer) and `storage::lfs` (reader) because they
share `storage::r2::Bucket::key`.

The cost is a footgun: a bare `git lfs pull` in a fresh clone drags the entire
ladder. `archive!` resolves identity from the manifest and never needs the
bytes, so a checkout is only expensive if someone asks for it — document
`git lfs pull --include` in `fixtures/chains/README.md` and leave it at that.

## 6. Status

Updated 2026-08-12, after producing the first three rungs and landing the
consumer work. Group A/B item numbers below are the plan's original labels.

**Produced and in-tree** (`fixtures/chains/`, LFS-tracked, manifests committed):

| Rung | Pin | Compressed | Extracted | % of a 32Gi seed PVC | Boundary gate |
| --- | --- | --- | --- | --- | --- |
| Sapling | 425,200 | 11.37 GB | 18.75 GB | 54.6% | `sapling` 0 → 2,299,333,991,832 |
| Blossom | 659,600 | 14.02 GB | 22.46 GB | 65.4% | skipped — introduces no pool |
| NU5 / Orchard | 1,693,104 | 21.78 GB | 32.79 GB | **95.4%** | `orchard` 0 → 325,252,861,549 |
| NU6.3 / Ironwood | 3,434,143 | ~230 GB (in progress) | ~277 GB | far over | `ironwood` 0 → 83,439,267,169,646 |

**Done.** Step 0 (mainnet activations derived from the running producer; NU6.3
exists on mainnet at 3,428,143). C1 and C2 (mainnet activation table and the
free-space guard in the producer). A1–A5, A7 (network-parameterized
`public_conf`, `.mainnet()` verb, explicit network dispatch, `ZEBRAD_PUBLIC_RPC`,
`IndexerMode::Public`, verification parity). Plus a claim-vs-artifact check that
rejects `.mainnet(testnet::ORCHARD)` at `env.build()`.

**Not done, and blocking a cluster run:**

1. **Upload.** None of the mainnet blobs are in R2. `materialize` pulls only
   from the bucket, so every mainnet profile fails preflight until
   `git lfs push`. ~47 GB for the first three rungs.
2. **A6 — per-artifact seed PVC sizing.** Still a flat `32Gi`
   (`seed_size()`, `src/materialize/mod.rs`). Blossom fits at 65%; Orchard at
   95.4% does not survive filesystem overhead and ext4's default 5% reserve;
   Ironwood is out of the question. This gates every rung past Blossom.
3. **A8 — `db_format` enforcement.** Recorded, exposed, still read by nothing.
4. **A9 — readiness budget.** `DEFAULT_READY_TIMEOUT` is 20 s against a 22.5 GB
   RocksDB open on a fresh clone.
5. **B1 — the first sync profile.** Now exists, but in the *zaino* repo:
   `live-tests/sync/tests/{zaino_sync,zaino_state_fetch_parity}.rs`, both on
   `mainnet::BLOSSOM`. Neither has ever run against a cluster.

**Corrections to the estimates in §4.** Growth is far more back-loaded than the
early rungs suggested: 15.9 KB/block from Sapling to Blossom, 10.0 KB/block to
NU5, then ~140 KB/block to Ironwood — 14× denser. Compression degrades with
depth for the same reason (0.61 at Sapling, ~0.84 at Ironwood): post-NU5 state
is dominated by high-entropy shielded ciphertext. Sync throughput measured
~3,000 blk/min cold from genesis, 7,500–9,600 blk/min on a warm volume.

## 7. Work items

Grouped by whether they can be proven **before** any mainnet byte exists. That
split is the plan's main risk control: the mainnet sync is multi-day and not
cheaply repeatable, and it must land on a consumer that is already known-good
against the existing testnet artifacts.

### Group A — consumer generalization (provable today, on testnet artifacts)

**A1. Network-parameterize the config generators.**
`src/testnet_conf.rs` hardcodes `network = "Testnet"` (`:61`),
`initial_testnet_peers = []` (`:59`) and, in the zainod twin,
`network = 'Testnet'` (`:132`). Take `ArchiveNetwork` and derive all three.
The frozen-chain contract around them — `debug_force_finished_sync = true`,
`crawl_new_peer_interval = "365d"`, empty peer set, `cache_dir = false` for the
*peer* cache — is network-independent and should be stated once, not twice.
Rename the module accordingly; a copy-pasted `mainnet_conf.rs` would duplicate
that contract and let the two copies drift.

**A2. Replace the `Testnet` builder verb.**
`trait Testnet { fn testnet(self, archive) }` (`src/regtest.rs:204`) is the only
entry point for a public-network archive, and the handle it takes already
carries its own network (`ArchiveHandle::chain().network()`). A verb naming one
network is simply wrong for the other. Rename to `Chain { fn chain(self, archive) }`.
Mechanical: the trait plus two impls (`src/backends/zebra.rs:562`,
`src/backends/zainod.rs:1164`).

**A3. Make the network dispatch explicit and make the fall-through fatal.**
`is_testnet_restore` (`src/backends/zebra.rs:516`), `rpc_port` (`:529`),
`serves_indexer_grpc` (`:544`) and `fn testnet` (`:568`) all treat
"not Testnet" as "regtest". A mainnet archive today silently boots on a regtest
config. Match on `ArchiveNetwork` with a real `Mainnet` arm, and make an
unhandled combination an error rather than a route.

**A4. Resolve the mainnet port collision.**
Mainnet canonical is 8232 RPC / 8233 P2P, and `handles::ports::ZAINO_JSONRPC`
is already 8232. Pick deliberately — the pods are network-isolated so canonical
numbering is a convenience, not a requirement, but the collision must not be
discovered at runtime.

**A5. Wire `IndexerMode::Mainnet`.**
`src/backends/zainod.rs:248` returns `"zaino mainnet mode is not yet supported"`.
The variant already exists (`src/component.rs:71`) and is constructed nowhere.
Falls out of A1.

**A6. Per-artifact seed PVC sizing.** *The hard blocker.*
`seed_size()` is a flat `"32Gi"` behind one global env var
(`src/materialize/mod.rs:914`). No mainnet rung past Blossom fits, and a global
knob cannot be right for a ladder whose rungs differ by two orders of magnitude.
The manifest already records the answer — `uncompressed_bytes` (the testnet deep
rung: 10,459,813,376), parsed at compile time and exposed as
`ChainInfo::uncompressed_bytes()`. Thread it onto `SeedEntry` and size the PVC
from it plus headroom, the same way `pull_budget()` already derives its deadline
from size rather than taking a constant (`src/materialize/mod.rs:91`).

This is not only a mainnet fix. Three testnet seeds are bound at 32Gi each for
~15 GB of actual data; per-artifact sizing reclaims most of ~96 GiB immediately.

Before sizing anything in the hundreds of Gi, **confirm whether the topolvm
device class backing `rook-ceph-block-archive` is thin or thick**. The node
advertises `capacity.topolvm.io/vg1 = 997 GB` against a single `crc` node, and
whether an over-provisioned PVC costs real extents changes both the headroom
factor and whether the deepest rung fits at all. (Current RBAC cannot read
`logicalvolumes.topolvm.io`; this needs a cluster-admin check.)

**A7. Verification parity.**
`verify_restored_chain` returns `Ok(())` for anything that is not Testnet
(`src/env.rs:1003`), and the fixture-quality gate at `src/env.rs:604` is
likewise Testnet-only. A mainnet archive would therefore get **zero**
manifest-vs-mounted verification — no tip check, no activation cross-check, no
`boundary_check` re-assertion. That is precisely the silent-pass failure mode
the boundary gate exists to prevent, reintroduced one network over. Extend both
to Mainnet in the same change that makes mainnet mountable, not after.

**A8. Enforce `db_format`.**
Every manifest records it, `ChainInfo::db_format()` exposes it, and its doc
comment promises that a validator bumping the format becomes "a named failure"
rather than an opaque crash-loop. Nothing reads it. The version-string check at
`src/env.rs:557-572` is a coarse proxy that over-rejects and still cannot catch
a format bump inside one image tag. Cheap to add, and the cost of not having it
rises with artifact size: re-producing the mainnet ladder is days, not hours.

**A9. Derive the readiness budget from chain size.**
`DEFAULT_READY_TIMEOUT` is 20 s (`src/env.rs:324`). A mainnet RocksDB open on a
freshly restored clone will not make that. Derive it the way `pull_budget` does
rather than adding a per-profile knob that every mainnet profile must remember.

### Group B — the first sync profile (provable on testnet, required for mainnet to mean anything)

**B1. Write one.** There is no `#[ztest::sync_test]` profile in the tree — the
subject, probes, nemesis, CLI, reporter and report-mirroring are all built and
the first profile is still unwritten. Write it against a testnet rung, where the
edit-run loop is minutes.

It needs three things mainnet will make expensive to get wrong:

- **An explicit wallet birthday inside the snapshot's indexed range.** The only
  default is regtest height 1 (`src/handles/wallet.rs:298`), and account creation
  resolves the birthday against the *indexer* via `get_tree_state(birthday)`
  (`src/backends/librustzcash.rs:301-314`), which must be answerable.
- **`run.until_height(..)`.** Without it, `Segment::comparable_with` refuses to
  compare two runs, and `ztest sync perf --base` hard-fails — a run to tip covers
  whatever chain existed while it ran.
- **A driver datadir PVC.** `docs/design-sync.md` claims one; `build_driver_pod`
  has none and `LrzWallet` puts its sqlite in a `tempfile::tempdir()`
  (`src/backends/librustzcash.rs:296-298`), which dies with the pod. On a
  multi-hour mainnet run, a restart that rescans from scratch is the difference
  between a result and a wasted day.

### Group C — production (only after A and B are green)

**C1. Mainnet activation table in the producer.** Add `MAINNET_ACTIVATIONS`
alongside the testnet one and parameterize `emit_activations`, which reads
`$TESTNET_ACTIVATIONS` directly today. The script is otherwise already
network-generic: `NETWORK` is argument 3, `.env.mainnet` exists in z3, the
compose stack is mainnet-*default*, and the stop height reaches the container
through the generated `.env` via `env_file` on the zebra service.

**C2. A free-space guard.** The script has none. A rung that fills the disk
partway through `tar | zstd` costs the whole sync, and on mainnet that is days.
Refuse to start a rung whose projected output does not fit, using the previous
rung's measured `uncompressed_bytes` as the projection basis.

**C3. Produce the ladder**, one ascending pass, archiving at each rung. Then
`git lfs push`, add `archive!` consts to `src/snapshots.rs`, and extend that
module's tests — `every_shipped_snapshot_carries_chain_info` currently asserts
`network() == ArchiveNetwork::Testnet` for every handle
(`src/snapshots.rs:88-94`) and must become per-handle.

**C4. Re-point the B1 profile at mainnet** and record the first real number.

## 8. Sequencing

```
step 0   derive mainnet activations from zfnd/zebra:6.2.3     (minutes)
         └─ fixes the ladder's deepest rung

group A  consumer generalization, proven on testnet artifacts (days)
group B  first sync profile, proven on a testnet rung          (days)
         └─ A and B are independent; run them in parallel

group C  mainnet production                                    (days, serial)
         └─ starts only once A+B are green
```

The one ordering that matters: **nothing in group C starts before A and B are
green.** A multi-day mainnet sync landing on `IndexerMode::Mainnet` returning
`"not yet supported"`, or on a 32Gi seed PVC, or on a verification path that
returns `Ok(())` without checking anything, is the expensive version of a
mistake that costs nothing to find on the 620 MiB testnet rung.

## 9. Known open questions

- ~~Does mainnet have NU6.2/NU6.3 under zebra 6.2.3?~~ Yes — §3.
- ~~Actual sizes per rung.~~ Measured — §6.
- **Is the seed device class thin or thick?** Still unanswered, and it now
  matters more: the Ironwood rung is ~277 GB extracted against a ~1 TB VG that
  also carries every test PVC. Needs a cluster-admin read of
  `logicalvolumes.topolvm.io`.
- **Does the Ironwood rung fit the cluster at all?** Almost certainly not as a
  master seed plus per-pod clones under the current sizing. A6 is a
  prerequisite for even trying.
- **Do the mainnet profiles pass?** Neither `zaino_index_construction` nor
  `zaino_state_fetch_parity` has run against a cluster on a mainnet fixture.
