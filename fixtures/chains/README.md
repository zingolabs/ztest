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
<backend>-v<producer-version>-testnet-<pinned-height>.tar.zst   # gitignored, lives in the bucket
snapshots/<network>/<upgrade>.toml                               # committed, 4 keys
```

The filename is a producer convention for humans; every consumer reads the
snapshot's identity from `snapshots/<network>/<upgrade>.toml` and its chain
facts from the `ChainSnapshot` const, so a rename changes nothing.

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
note that the mainnet set is ~47 GB — fetch a single rung rather than all of
them.

Each is pinned **6,000 blocks past** its activation. A snapshot pinned *at* an
activation holds essentially none of the data it is named for, and every
assertion drawn from it passes while proving nothing; the `[boundary_check]`
table in each manifest is the producer's evidence that the introduced pool
actually moved across the post-activation window.

## Not in the tree

The archives are **gitignored**. What git holds is
`snapshots/<network>/<upgrade>.toml` — four machine-written keys addressing the
bytes — and the `ChainSnapshot` const in `src/snapshots.rs` carrying the chain
facts. The bytes live in an S3-compatible bucket (Cloudflare R2) at
`lfs/<sha256>`, and the seed puller `curl`s a presigned GET for them straight
onto the node.

git-lfs is not involved anywhere: not in the fetch path, not in publishing, not
in a checkout. A machine with no `git lfs` installed runs the full suite. See
[docs/design-snapshots.md](../../docs/design-snapshots.md) for why.

### Publishing one

```sh
ztest snapshot manifest ./zebra-v6.2.3-testnet-1848420.tar.zst \
    > snapshots/testnet/zebra-6.2.3-orchard.toml
ztest snapshot push     ./zebra-v6.2.3-testnet-1848420.tar.zst
# then add the const to src/snapshots.rs and commit
```

Push **before** committing: a committed manifest is a claim the object exists,
and `ztest snapshot verify` is what enforces it across the whole declared set.

`manifest` reads the archive once, streaming — the sha256 is taken on the way
into the decompressor and its output counted for the extracted size, so a 21 GB
artifact is never buffered. It needs no cluster, no bucket and no validator.

### Environment

The bucket is addressed by the standard AWS variables, so one export set serves
`ztest`, `aws s3`, and anything else pointed at it:

```
AWS_BUCKET_NAME=ztest-archives
AWS_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
AWS_ACCESS_KEY_ID=…
AWS_SECRET_ACCESS_KEY=…
# AWS_REGION defaults to `auto`, which is what R2 expects.
```

`~/.config/ztest/bucket.toml` is the alternative, and belongs to the ztest
installation rather than any one checkout. These are needed at test time
**whether or not** the archive is on your disk: the seed's bytes are fetched by
a Job on the cluster, which cannot see a local checkout. `ztest cluster check`
reports the bucket as its own row.

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
