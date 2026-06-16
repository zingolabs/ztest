# Testnet chain archives

Pre-synced chain snapshots consumed by `Validator::*.testnet(variant)` /
`Indexer::zaino(...).testnet(variant)` / `...testnet_state(variant)`.
Resolved by `ztest::regtest::testnet_chain_archive(variant, kind, dest)`;
mounted as a content-addressed PVC via `materialize::ensure_seed`.

## Layout

```
fixtures/testnet/
├── orchard/
│   ├── zebra.tar.xz       # zebrad state dir, post-Orchard activation
│   └── zcashd.tar.xz      # zcashd datadir, post-Orchard activation
└── sapling/
    ├── zebra.tar.xz       # zebrad state dir, post-Sapling activation
    └── zcashd.tar.xz      # zcashd datadir, post-Sapling activation
```

Variant ↔ archive is 1:1 by filename. Configs are **generated** in
`ztest::testnet_conf` — no per-variant TOMLs live here. zaino pods
pair with a zebrad pod and consume `zebra.tar.xz`.

## Setups

| Variant   | What it carries                                                  |
| --------- | ---------------------------------------------------------------- |
| `orchard` | Chain synced past NU5 activation; Orchard pool funded.           |
| `sapling` | Chain synced past Sapling activation, pre-NU5; Sapling-only.     |

## TODO — missing archives

None of the archive files exist in tree yet. Until they land, every
`*.testnet(variant)` test fails at materialization with
`archive materialize failed: No such file or directory`.

- [ ] `orchard/zebra.tar.xz`
- [ ] `orchard/zcashd.tar.xz`
- [ ] `sapling/zebra.tar.xz`
- [ ] `sapling/zcashd.tar.xz`

### Producing an archive

1. Sync the target backend against public testnet to the desired
   activation height.
2. Stop the process cleanly (state dir must be quiescent).
3. `tar -caf <variant>/<backend>.tar.xz -C <state-dir-parent> <state-dir-leaf>`
   — `-a` picks the compressor from the `.xz` suffix.
4. Drop the file at the path above and `git add` it (Git LFS if the
   repo policy requires).

A `cargo xtask snapshot-testnet` / `just snapshot-testnet` recipe to
codify this is open work — see ztest issues.
