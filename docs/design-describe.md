# `ztest describe` — one planner, two consumers

## Why this exists

`ztest sync start zaino_index_construction` pulled **two** chain snapshots: the 13.05 GiB mainnet BLOSSOM
its profile declares, plus an 8.15 GiB testnet IRONWOOD declared by
`clientless::the_pub_testnet_ironwood_boundary` — a test neither selected nor run. 21 GiB of R2 transfer
for a 13 GiB dependency, unknowable in advance.

Two defects, one root:

1. **Plan never pruned** — `pipeline::images::assemble` unions every `SeedEntry` across every dumped
   binary, `resource::plan_runtime` turns each into a node, neither consults the selection. The per-test
   edges needed to prune (`deps_by_binary`) are already collected, used only for admission ordering
1. **Plan never shown** — `sync describe` printed seven lines off the `SyncTestDecl` annotation: nothing
   about images, seeds, dependencies, reservations, or what provisioning creates. `ztest run` had no
   describe at all

One fix, because the second is the proof of the first.

## The governing constraint

A `describe` that re-derives the plan drifts from what `run`/`sync start` provision, and a describe that
lies is worse than none.

> **One planner, two consumers.** `describe` must never own a code path `start` does not take.

```
        selection  (nextest filter | sync profile name)
                          │
                          ▼
              ┌───────────────────────────┐
              │      plan::build()        │   pure — no cluster contact
              │  binaries → tests → deps  │
              │  → seeds(pruned) → images │
              │  → qos reservations       │
              └─────────────┬─────────────┘
                            │  Plan
              ┌─────────────┴─────────────┐
              ▼                           ▼
       Graph::provision              plan::render
    (run · sync start)              (describe — tree)
```

A node in the tree ⟺ a node `Graph` would provision. That is the whole design.

## Surface

```
ztest run  describe [NEXTEST FILTER]   [--full] [--format tree|json]
ztest sync describe <PROFILE>          [--full] [--format tree|json]
```

| mode     | source                                                                            | cluster | shows                                                                                            |
| -------- | --------------------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------ |
| bare     | link-time inventory (`#[needs]`, `#[sync_test]`, `#[qos]`, `dev!`) + nextest list | no      | selection, binaries, dev images, seeds, dep edges, QoS tier, timeouts                            |
| `--full` | the above + resolved topology + live cluster state                                | yes     | pods, per-pod cpu/mem, mounts, PVC clones, reservations, seed cache hit/miss, image tag presence |

Bare is default = the cheap, always-correct view (one `cargo nextest list` + a sub-100 ms inventory dump
per binary). `--full` is the complete picture and pays for it.

### Why `--full` must execute the topology closure

Pods, mounts and per-pod resources are not statically knowable — `topology(|t| …)` is a Rust closure
running inside the driver pod. Attributes give everything else free and give nothing here.

- `--full` runs the selected test in **plan-only** mode: `TestEnv::build()` resolves component specs,
  serializes, returns without creating one Kubernetes object
- Direct precedent: `ZTEST_DUMP_INVENTORY=1` already dumps-and-exits before `main` sees `argv`;
  `ZTEST_PLAN_ONLY=1` is the same mechanism one layer later
- Inert **by construction, not discipline**: the `Cx` handed to `TestEnv::build()` carries no
  `kube::Client`, so a write is a type error, not a code-review question

## The `Plan` IR

```rust
pub struct Plan {
    pub root:     Vec<PlanRoot>,
    pub pruned:   Vec<PrunedSeed>,
    pub totals:   Totals,
}

pub struct PlanRoot {
    pub label:    String,
    pub kind:     RootKind,
    pub qos:      QosNode,
    pub tags:     Vec<String>,
    pub images:   Vec<ImageNode>,
    pub seeds:    Vec<SeedNode>,
    pub pods:     Vec<PodNode>,
}
```

- `RootKind` = `SyncProfile` | `Test`, the only difference between the two front doors
- `pods` empty unless `--full` resolved a topology
- `pruned` is load-bearing, not a footnote: silently dropping a seed and silently over-provisioning one
  are one class of invisible behaviour, so the tree prints what it dropped and who declared it — what
  makes the IRONWOOD bug legible instead of merely absent

## Pruning rule

```
selected  = ⋃ binary.selected_tests                      (nextest already applied the filter)
needed    = { dep.resource | dep ∈ deps, declares(dep.test_id, selected) }
seeds     = dump.seeds ∩ needed          (by OID — seeds are content-addressed)
```

`declares` is **not** string equality — two reconciliations are mandatory, both already in the engine and
to be reused, never re-derived:

1. **ID space** — `TestDepEntry.test_id` is `module_path!()::fn` (crate-rooted), `selected_tests` are
   libtest names; `engine::plan::libtest_name` is the bridge, the same one `resource_deps` construction
   applies
1. **`rstest` cases** — one libtest entry per case (`parent::case_1_sapling`) while `#[ztest::needs]`
   submits only `parent`, so exact matching prunes a seed every case needs. Walk off trailing `::`
   segments to the declaring ancestor, exactly as `engine::plan::declared_tier` does for tiers (a bug the
   suite has already paid for once)

Images are **not** pruned per-test: `dev!` sites are per-binary by construction (`images_by_binary`) and
the engine already gates on the binary edge. Pruning them needs a call-graph, not an inventory.

## Landing order

Pruning fix and describe land as one change — `plan::build()` *is* the pruning fix, `describe` is its
second consumer, and `excluded` is its regression test in human-readable form.

1. `src/plan/{mod,render}.rs` — `Plan`, `build()`, tree/JSON renderers
1. `cli/run.rs` + `cli/sync/mod.rs` provisioning call `plan::build()`, feeding `Plan::seeds` to
   `plan_runtime` instead of the unpruned dump union
1. `cli/sync/mod.rs::describe` renders a `Plan` rather than printing an entry
1. `ztest run describe` intercept
1. `--full`: `ZTEST_PLAN_ONLY=1`, client-free `Cx`, pod/mount serialization

Steps 1–4 = bug fix + static tree; step 5 is separable behind the same flag.

### One CLI wrinkle

`run::Args` is `trailing_var_arg` — everything after `run` goes to nextest verbatim, the documented
migration promise (`s/cargo nextest/ztest/`).

- `describe` is recognized only as the **first** token after `run`
- To filter for a test whose name contains `describe`: `ztest run -E 'test(describe)'` or
  `ztest run -- describe`
- A `--describe` flag would avoid the ambiguity but breaks symmetry with `ztest sync describe`, the
  surface actually asked for

## Render

`cargo tree` grammar: every node is `kind name facts`, nesting is real containment, an already-printed
node repeats as `(*)`. `(*)` is not cosmetic — one seed shared by twenty tests must print once.

Node kinds: `qos`, `tags`, `image`, `seed`, and (`--full`) `pod`. Deliberately no `test` or `binary` node
— the test id is the root's own label under `sync describe` and the root itself under `run describe`, and
the binary is recoverable from the test id. Both were structure without information.

### `ztest sync describe zaino_index_construction`

```
zaino_index_construction
zaino builds its chain index over the pinned Blossom mainnet snapshot; zebrad is the authority
├── qos sync · reserve 16c / 16 GiB · hard cap 48h · declared 48h
├── tags mainnet, zaino, index, blossom
├── image zainod BUILD
│   ├── dockerfile <root>/Dockerfile ctx <root>
│   └── features no_tls_with_prometheus, allow_unencrypted_public_json_rpc_bind, profile
└── seed zebra-v6.2.3-mainnet-659600.tar.zst 1106bc19 13.05 GiB
    └── pvc ztest-seeds/seed-1106bc19-<driver> 48Gi
```

`describe` compiles with the profile's own `ProfileStub::cargo_args` — the same `-p/--test` narrowing
`start` uses; a describe modelling a wider selection than the run it describes would report a `pruned`
set that run never had.

On a profile whose binary declares only what it needs, `pruned` is **empty** = the healthy state. It
fills in the two cases cargo-level scoping cannot reach:

```
pruned
└── seed zebra-v6.2.3-testnet-4140000.tar.zst 3545da25 8.15 GiB
    └── declared by the_pub_testnet_ironwood_boundary::value_pools_respect_the_boundary…
```

1. Scan cannot identify the profile → empty `cargo_args` → whole-workspace bake, every linked binary's
   seeds in the dump
1. Two tests in one binary declaring different seeds — one target, one dump, no cargo-level split

### `ztest run describe -E 'package(clientless)'`

Roots = the selected tests; shared nodes expand once, repeat as `(*)`.

```
3 tests · 2 images · 1 seed · 8.15 GiB to pull

the_pub_testnet_ironwood_boundary
├── qos testnet · reserve 8c / 8 GiB · hard cap 6h
├── image dev:zainod BUILD
│   └── features no_tls_with_prometheus
└── seed IRONWOOD_TESTNET 3545da25 8.15 GiB
    ├── archive zebra-v6.2.3-testnet-4140000.tar.zst
    └── pvc ztest-seeds/seed-3545da25-<driver> 48Gi

testnet_parity::case_1_sapling
├── qos testnet · reserve 8c / 8 GiB · hard cap 6h
├── image dev:zainod (*)
└── seed IRONWOOD_TESTNET (*)
```

### `--full`

Same grammar, same node kinds, extra children — never a second layout. Three additions: `cache` on each
seed, `registry` on each image, `pod` children carrying the resolved topology.

```
└── seed BLOSSOM_MAINNET 1106bc19 13.05 GiB
    ├── archive zebra-v6.2.3-mainnet-659600.tar.zst
    ├── pvc ztest-seeds/seed-1106bc19-topolvm-io 48Gi
    ├── cache ✗ MISS published on hostpath.csi.k8s.io, cluster uses topolvm.io → re-pull 13.05 GiB
    └── clone zebrad → <cache_dir>, zainod → /var/lib/zaino/zebra-db
├── pod zebrad zebra:6.2.3 5c / 4 GiB
│   ├── mount pvc clone ← seed-1106bc19 → <cache_dir>
│   └── port 18232 rpc, 9999 metrics
└── pod zainod dev:zainod 9c / 11 GiB
    ├── mount pvc clone ← seed-1106bc19 → /var/lib/zaino/zebra-db (read)
    └── mount emptyDir → /var/lib/zaino/db (index built here)
```

`cache ✗ MISS` is the whole point — the exact condition that failed `zaino_index_construction` after
33 minutes, reported before a byte moves.

### Color

Only `ui::Theme`'s existing roles, no new `Styles` field: node kind = `script_id`; names + magnitudes =
`count`; `BUILD` and the whole `pruned` subtree = `skip`; `✓`/`✗` cache state = `pass`/`fail`; tree
glyphs, paths, parentheticals = `dim`. `--format json` emits the `Plan` verbatim.

## Non-goals

- Not a scheduler preview — says what will be *created*, not when tests get admitted against live capacity
- Not a substitute for `sync list` — that stays the catalogue, `describe` is depth on one selection
- Bare mode never contacts the cluster → reports a PVC *name*, never a PVC *state*; the driver suffix
  renders as `<driver>` unless `--full` resolved it
