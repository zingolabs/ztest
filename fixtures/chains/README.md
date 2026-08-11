# Testnet chain snapshots

Immutable, height-pinned snapshots of The Public Testnet, consumed through the
named consts in `ztest::snapshots` — `.testnet(ORCHARD)`. Nothing here is
resolved by string at runtime; see `src/snapshots.rs` for the declarations and
`src/snapshot.rs` for why the handle is typed.

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

## Known limit

`zebra-v6.2.3-testnet-4140000` does not currently mount: streaming 8.2 GiB
through the uploader pod's stdin cannot finish inside
`materialize::WAIT_BUDGET` (300 s), which covers every wait including the
upload. That is a harness limit, not a property of the artifact.

## Producing one

`zaino/scripts/produce-chain-fixture.sh <height> <version> testnet`, which does
the clean stop (`state.debug_stop_at_height`, so the DB is quiescent by
construction rather than by a well-timed SIGTERM), compacts the RocksDB tree,
writes the manifest, and validates the manifest against the artifact it just
produced. Heights must ascend on one volume.
