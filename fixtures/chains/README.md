# Chain snapshots

Immutable, height-pinned snapshots of the public Zcash networks, consumed
through the named consts in `ztest::snapshots::{testnet, mainnet}` —
`.testnet(testnet::ORCHARD)`, `.mainnet(mainnet::BLOSSOM)`. Nothing here is
resolved by string at runtime; see `src/snapshots.rs` for the declarations and
`src/archive.rs` for why the handle is typed.

The verb and the handle must name the same network. That is redundant on
purpose — the config generator reads the network off the artifact, so the verb
cannot steer it — and the redundancy is checked at `env.build()`, because
`.mainnet(testnet::ORCHARD)` would otherwise run green against a chain the test
never asked for.

Supersedes the old `fixtures/testnet/<variant>/{zebra,zcashd}.tar.xz` layout,
which resolved paths with a runtime `format!` and so could not be declared as a
seed — testnet fixtures bypassed preflight entirely, and the first test to touch
one materialized a multi-GB archive lazily inside its own `ready_timeout`.

## Layout

Flat, and every identity field is in the filename, so an artifact stays
self-describing when copied into a bucket or attached to a bug report:

```
<backend>-v<producer-version>-testnet-<pinned-height>.tar.zst   # LFS
<backend>-v<producer-version>-testnet-<pinned-height>.toml      # plaintext
```

`testnet_snapshot!(pub ORCHARD = zebra, "6.2.3", 1_848_420)` *derives* both
names from its typed arguments, so a pin that disagrees with the tree is a
compile error rather than a convention nobody checked.

| Artifact | Compressed | Extracted | Boundary |
| --- | --- | --- | --- |
| `zebra-v6.2.3-testnet-286000` | 620 MiB | 795 MB | Sapling (280,000) |
| `zebra-v6.2.3-testnet-590000` | 1.2 GiB | 1.5 GB | Blossom (584,000) |
| `zebra-v6.2.3-testnet-1848420` | 3.3 GiB | 4.0 GB | NU5 / Orchard (1,842,420) |
| `zebra-v6.2.3-testnet-4140000` | 8.2 GiB | 9.7 GB | NU6.3 / Ironwood (4,134,000) |
| `zebra-v6.2.3-mainnet-425200` | 11.4 GB | 18.7 GB | Sapling (419,200) |
| `zebra-v6.2.3-mainnet-659600` | 14.0 GB | 22.5 GB | Blossom (653,600) |
| `zebra-v6.2.3-mainnet-1693104` | 21.8 GB | 32.8 GB | NU5 / Orchard (1,687,104) |

Mainnet is roughly an order of magnitude past testnet at every rung: the
*smallest* mainnet artifact is larger than the deepest testnet one. Prefer
testnet unless the test specifically needs mainnet's transaction density, and
note that fetching the mainnet set is a ~47 GB `git lfs pull` — use
`git lfs pull --include=<path>` rather than the bare form.

Each is pinned **6,000 blocks past** its activation. A snapshot pinned *at* an
activation holds essentially none of the data it is named for, and every
assertion drawn from it passes while proving nothing; the `[boundary_check]`
table in each manifest is the producer's evidence that the introduced pool
actually moved across the post-activation window.

## LFS

Archives are LFS-tracked; manifests deliberately are not. `testnet_snapshot!`
reads the manifest at compile time to cross-check the pin and size the seed PVC,
and that has to work in a checkout whose archives are still unfetched pointers.

There is **no LFS server**. Blobs live in an S3-compatible bucket (Cloudflare
R2) and are moved by `ztest lfs-transfer`, a Git LFS [custom transfer agent]
built into this crate. Two reasons it is not the stock `basic` adapter and not a
server:

1. `basic` is exactly one `PUT` per object, which inherits R2's 4.995 GiB
   single-request cap. The Ironwood snapshot is 8.15 GiB and cannot be pushed
   that way at all. The agent runs a real S3 multipart upload.
2. The agent *writes* the objects and `storage::lfs` *reads* them at test time.
   In one binary they share `storage::r2::Bucket::key`, so the key layout they
   must agree on is checked by the compiler instead of by convention.

### Environment

Both directions read the standard AWS variables, so the same exports serve
`ztest`, `aws s3`, and anything else pointed at the bucket:

```
AWS_BUCKET_NAME=ztest-archives
AWS_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
AWS_ACCESS_KEY_ID=…
AWS_SECRET_ACCESS_KEY=…
# AWS_REGION defaults to `auto`, which is what R2 expects.
```

A laptop that has run `git lfs pull` never needs them at test time: the archive
is a real file, `storage::for_source` picks the local backend, and both paths
resolve to the same content-addressed seed.

### Per-clone setup (one time)

git-lfs **refuses** to take `lfs.customtransfer.*` or
`lfs.standalonetransferagent` from a committed `.lfsconfig` — a repo that could
name the binary git-lfs executes on clone would be a code-execution hole. So the
wiring has to live in each clone's own `.git/config`:

```
git config --local lfs.customtransfer.ztest.path ztest
git config --local lfs.customtransfer.ztest.args lfs-transfer
git config --local lfs.standalonetransferagent ztest
```

`ztest` must be on `PATH` — `nix develop` provides it, and consumers installing
from crates.io already have it.

[custom transfer agent]: https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md

## Decompression on the cluster

The archives are `zstd -19 --long=27`. Two properties make that safe for the
puller, and both are load-bearing:

- **Level does not affect decode.** zstd decompresses at essentially the same
  speed regardless of compression level, so `-19` is paid once here and costs
  the cluster nothing. Measured on the 11 GB mainnet Sapling artifact inside a
  container capped exactly like the puller pod: 24 s, and the output matched
  the manifest's `uncompressed_bytes` byte for byte.
- **`--long=27` is a ceiling, not a knob.** A 128 MiB window is exactly zstd's
  default decoder memory limit, and `materialize`'s puller decodes with a bare
  `tar --zstd` — no `--long`, no `--memory` — in a 256 MiB pod (measured peak:
  135.6 MiB). Widening the window to `--long=28` costs nothing at production
  time and makes every seed fail to materialize with "Frame requires too much
  memory for decoding".

Decompression memory therefore depends on the window, not the artifact, so the
mainnet rungs cost the puller no more memory than the 620 MiB testnet one.

## Producing one

`zaino/scripts/produce-chain-fixture.sh <height> <version> testnet`, which does
the clean stop (`state.debug_stop_at_height`, so the DB is quiescent by
construction rather than by a well-timed SIGTERM), compacts the RocksDB tree,
writes the manifest, and validates the manifest against the artifact it just
produced. Heights must ascend on one volume.
