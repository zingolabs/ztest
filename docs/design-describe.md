# `ztest describe` — one planner, two consumers

## Why this exists

`ztest sync start zaino_index_construction` pulled **two** chain snapshots: the
13.05 GiB mainnet BLOSSOM its profile declares, and an 8.15 GiB testnet IRONWOOD
declared by `clientless::the_pub_testnet_ironwood_boundary` — a test that was not
selected and did not run. 21 GiB of R2 transfer for a 13 GiB dependency, and no
way to have known beforehand.

Two defects, one root:

1. **The plan is never pruned.** `pipeline::images::assemble` unions every
   `SeedEntry` across every dumped binary and `resource::plan_runtime` turns each
   into a graph node. Neither consults the selection. The per-test edges needed to
   prune (`deps_by_binary`) are already collected — they are used for engine
   admission ordering and nothing else.
2. **The plan is never shown.** `ztest sync describe` prints seven lines off the
   `SyncTestDecl` annotation. It says nothing about images, seeds, dependencies,
   reservations, or what provisioning will create. `ztest run` has no describe at
   all.

They share a fix, because the second is the proof of the first.

## The governing constraint

A `describe` that re-derives the plan will drift from what `run`/`sync start`
actually provision, and a describe that lies is worse than none. So:

> **One planner, two consumers.** `describe` must never own a code path that
> `start` does not take.

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

If the tree shows a node, it is because `Graph` would provision that node. That
is the whole design.

## Surface

```
ztest run  describe [NEXTEST FILTER]   [--full] [--format tree|json]
ztest sync describe <PROFILE>          [--full] [--format tree|json]
```

Two modes, no more:

| mode | source | cluster | shows |
|---|---|---|---|
| bare | link-time inventory (`#[needs]`, `#[sync_test]`, `#[qos]`, `dev!`) + nextest list | no | selection, binaries, dev images, seeds, dep edges, QoS tier, timeouts |
| `--full` | the above + resolved topology + live cluster state | yes | pods, per-pod cpu/mem, mounts, PVC clones, reservations, seed cache hit/miss, image tag presence |

Bare is the default because it is the cheap, always-correct view: one `cargo
nextest list` and a sub-100 ms inventory dump per binary. `--full` is the
complete picture and pays for it.

### Why `--full` needs to execute the topology closure

Pods, mounts and per-pod resources are not statically knowable. `topology(|t| …)`
is a Rust closure that runs inside the driver pod; attributes give us everything
else for free but give us nothing about the topology.

`--full` therefore runs the selected test under a new **plan-only** mode:
`TestEnv::build()` resolves component specs, serializes them, and returns without
creating a single Kubernetes object. This has direct precedent —
`ZTEST_DUMP_INVENTORY=1` already makes a test binary dump-and-exit before `main`
sees `argv`. `ZTEST_PLAN_ONLY=1` is the same mechanism one layer later.

Plan-only mode must be inert by construction, not by discipline: the `Cx` handed
to `TestEnv::build()` carries no `kube::Client`, so a write is a type error rather
than a code-review question.

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

`RootKind` is `SyncProfile` or `Test` — the only difference between the two front
doors. `pods` is empty unless `--full` resolved a topology.

`pruned` is load-bearing, not a footnote. Silently dropping a seed and silently
over-provisioning one are the same class of invisible behaviour; the tree prints
what it dropped and which test declared it. That section is what makes the
IRONWOOD bug legible instead of merely absent.

## Pruning rule

```
selected  = ⋃ binary.selected_tests                      (nextest already applied the filter)
needed    = { dep.resource | dep ∈ deps, declares(dep.test_id, selected) }
seeds     = dump.seeds ∩ needed          (by OID — seeds are content-addressed)
```

`declares` is **not** string equality. Two reconciliations are mandatory, both of
which already exist in the engine and must be reused rather than re-derived:

1. **ID space.** `TestDepEntry.test_id` is `module_path!()::fn` (crate-rooted);
   `selected_tests` are libtest names. `engine::plan::libtest_name` is the bridge —
   the same one `resource_deps` construction already applies at `cli/run.rs:1667`.
2. **`rstest` cases.** One libtest entry per case (`parent::case_1_sapling`), while
   `#[ztest::needs]` submits only `parent`. Exact matching would prune a seed that
   every case needs. Walk off trailing `::` segments to the declaring ancestor,
   exactly as `engine::plan::declared_tier` does for QoS tiers — a bug that suite
   has already paid for once.

Images are **not** pruned per-test. `dev!` sites are per-binary by construction
(`images_by_binary`), and the engine already gates on the binary edge. Pruning
them would need a call-graph, not an inventory.

## Landing order

The pruning fix and the describe land as one change, because `plan::build()` is
the pruning fix — `describe` is just its second consumer, and `excluded` is its
regression test in human-readable form.

1. `src/plan/{mod,render}.rs` — `Plan`, `build()`, tree/JSON renderers.
2. `cli/run.rs` and `cli/sync/mod.rs` provisioning paths call `plan::build()` and
   feed `Plan::seeds` to `plan_runtime` instead of the unpruned dump union.
3. `cli/sync/mod.rs::describe` renders a `Plan` rather than printing an entry.
4. `ztest run describe` intercept.
5. `--full`: `ZTEST_PLAN_ONLY=1`, client-free `Cx`, pod/mount serialization.

Steps 1–4 are the bug fix plus the static tree. Step 5 is separable and lands
behind the same flag.

### One CLI wrinkle, called out

`run::Args` is `trailing_var_arg` — everything after `run` goes to nextest
verbatim, which is the documented migration promise (`s/cargo nextest/ztest/`).
`describe` is therefore recognized only as the **first** token after `run`, and
that carve-out is documented: to filter for a test whose name contains
`describe`, use `ztest run -E 'test(describe)'` or `ztest run -- describe`.
A flag (`--describe`) would avoid the ambiguity but breaks the symmetry with
`ztest sync describe`, which is the surface actually asked for.

## Render

`cargo tree` grammar: every node is `kind name facts`, nesting is real
containment, and a node already printed repeats as `(*)` instead of expanding.
`(*)` is not cosmetic — one seed shared by twenty tests must print once.

Node kinds are `qos`, `tags`, `image`, `seed`, and (`--full` only) `pod`. There
is deliberately no `test` or `binary` node: the test id is the root's own label
under `sync describe` and the root itself under `run describe`, and the binary is
recoverable from the test id. Both were structure without information.

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

`describe` compiles with the profile's own `ProfileStub::cargo_args` — the same
`-p/--test` narrowing `start` uses. A describe modelling a wider selection than
the run it describes would report a `pruned` set that run never had.

So on a profile whose binary declares only what it needs, `pruned` is **empty**,
and that is the healthy state. It fills in the two cases cargo-level scoping
cannot reach:

```
pruned
└── seed zebra-v6.2.3-testnet-4140000.tar.zst 3545da25 8.15 GiB
    └── declared by the_pub_testnet_ironwood_boundary::value_pools_respect_the_boundary…
```

1. The scan cannot identify the profile → `cargo_args` is empty → whole-workspace
   bake, every linked binary's seeds in the dump.
2. Two tests in one binary declaring different seeds — one target, one dump, no
   cargo-level split available.

### `ztest run describe -E 'package(clientless)'`

Roots are the selected tests; shared nodes expand once and repeat as `(*)`.

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

Same grammar, same node kinds, additional children — never a second layout.
Three additions: a `cache` child on each seed, a `registry` child on each image,
and `pod` roots-children carrying the resolved topology.

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

That `cache ✗ MISS` line is the whole point: it is the exact condition that
failed `zaino_index_construction` after 33 minutes, reported before a byte moves.

### Color

Only `ui::Theme`'s existing roles — `pass`, `fail`, `skip`, `script_id`, `count`,
`dim`. No new `Styles` field. Node kind = `script_id`; names and magnitudes =
`count`; `BUILD` and the whole `pruned` subtree = `skip`; `✓`/`✗` cache state =
`pass`/`fail`; tree glyphs, paths and parentheticals = `dim`. `--format json`
emits the `Plan` verbatim for anything that wants to compute on it.

## Non-goals

- Not a scheduler preview. `describe` says what will be *created*, not when tests
  will be admitted against live QoS capacity.
- Not a substitute for `sync list`. That stays the catalogue; `describe` is depth
  on one selection.
- Bare mode never contacts the cluster, so it can report a PVC *name* but never a
  PVC *state*. The driver suffix is rendered as `<driver>` unless `--full`
  resolved it.
