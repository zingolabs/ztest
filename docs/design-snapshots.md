# Chain snapshots: manifest-as-lockfile

Chain fixtures = **build inputs, not source**: pinned by hash, fetched on demand — the model Cargo, Go
modules, Bazel `http_archive` and Nix fixed-output derivations all use. Git holds a four-key record; the
bytes live in the snapshot bucket, addressed by content.

Replaced Git LFS, whose premise (materialise large files on checkout so they feel like source) ztest
worked against at every layer: 116 GiB of disk for 58 GiB of fixtures, ~550 lines existing only to undo
LFS, and seeding that worked on machines with no `git lfs` installed at all. The one thing LFS genuinely
provided — a git-native binding between a name and a hash — is now the manifest, and
`ztest snapshot verify` replaces `git lfs fsck`.

## The two types

An artifact = a blob in a bucket. A chain snapshot = an artifact + which chain it holds. Nothing else.

```rust
pub struct Artifact      { name, oid, size, uncompressed_bytes }         // src/archive.rs
pub struct ChainSnapshot { tip_height, network, backend, artifact }
```

- Plain data — no methods, no derivation, nothing to keep in sync; every field `pub`, so a consuming
  crate declares its own
- Dissolved the old `Option<ChainInfo>`: a mount always wanted the blob and never the chain, which is
  exactly why the field was optional. `MountSource::Seed` takes an `Artifact` and the option is gone

## Declaring one

```rust
pub const ORCHARD_TESTNET: ChainSnapshot = ChainSnapshot {
    tip_height: 1_848_420,
    network: Network::Testnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/testnet/zebra-6.2.3-orchard.toml"),
};
```

- Chain facts written beside the doc comment describing them — a wrong height is visible in review, the
  only check that was ever going to work
- `tip_height` is checked against the running validator at `env.build()` → a wrong one fails by name on
  first use
- `artifact!` is the only macro left, doing the one thing a `const fn` cannot: read a file at compile
  time. The path names the *manifest*, because the archive is the one file never in the tree

## Manifest schema

Four keys, every one machine-computed, never hand-written.

```toml
name               = "zebra-v6.2.3-testnet-1848420.tar.zst"
sha256             = "8c350d15ecc54c5610707093e31293bc13f9d24acfc9bade9d987e60660ac9a6"
size_bytes         = 3499975919
uncompressed_bytes = 4468310016
```

| Key                  | Consumed by                                                        |
| -------------------- | ------------------------------------------------------------------ |
| `sha256`             | bucket key `lfs/<oid>`, seed PVC name, the puller's digest check   |
| `size_bytes`         | `Bucket::has()` idempotent push, pull budget, progress denominator |
| `uncompressed_bytes` | seed PVC sizing                                                    |
| `name`               | `compression_from_name` → the puller's `tar` flag; error messages  |

Deserialises to `Artifact` one-to-one — same four fields, no mapping layer.

The schema shrank to this by audit: `tip_hash`, `db_format`, `version`, `contents`, `produced_by`,
`stop_method` had **zero** readers anywhere; `[activations]` were consensus constants that need
maintaining every time an upgrade ships; `[boundary_check]` moved to the producer, which has a live node.
`sha256` already pins the bytes, so any fact *derived* from those bytes is implied by it — what survived
is what addresses the blob plus what a test reads.

## Trust chain

```
  git (trust root)
    │
    ├── snapshots/<network>/zebra-<ver>-<upgrade>.toml ──── name, sha256, sizes
    │        │                                              committed, plaintext, 4 keys
    │        └── read at COMPILE TIME by artifact!() ──► Artifact
    │                                                      ├─► bucket key  lfs/<oid>
    │                                                      └─► seed PVC    seed-<oid[..8]>-<driver>
    │
    └── src/snapshots.rs ──── tip_height, network, backend  (human, reviewed)

  R2 (bytes only, zero metadata)
    └── lfs/<oid> ──── verified against <oid> on every fetch
```

## Publishing

```
  produce the archive                                          ← needs a live node
    │  zebrad --state.debug_stop_at_height=H, compact, tar --zstd
    │  boundary gate: did the pool move across [activation, H]?
    ▼
  ./zebra-v6.2.3-testnet-1848420.tar.zst
    │
    ├─►  ztest snapshot manifest <path> > snapshots/testnet/zebra-6.2.3-orchard.toml
    │       one streaming read: sha256 of the compressed bytes taken on the way
    │       into the decompressor, whose output is counted. Never buffered
    │
    ├─►  ztest snapshot push <path>
    │       oid → has(oid,size)? skip : multipart PUT to lfs/<oid>
    │
    └─►  add a const to src/snapshots.rs, commit

  ORDER MATTERS: push before commit. A committed manifest claims the object
  exists; `ztest snapshot verify` is what enforces it.
```

Every `ztest snapshot` step is cluster-free — publishing a fixture must not require a cluster.

## Consuming

```
  compile time   artifact!("snapshots/testnet/zebra-6.2.3-orchard.toml")  → no I/O, no git

  preflight      seed-<sha8>-<driver> in ztest-seeds?
                   ├── ready ─────────────────────► cached
                   └── absent ─► create PVC + puller Job
                                    │ presigned GET (one object, one verb, TTL)
                                    ▼
  puller pod     mkfifo /tmp/verify.fifo
                 sha256sum < fifo > /tmp/verify.sum &        VERIFY_PID=$!
                 curl "$SEED_URL" | tee fifo | dd | tar --zstd -x -C /seed
                   && wait $VERIFY_PID; [ "$ACTUAL" = "<oid>" ] || exit 1
                   │
                   ▼
                 VolumeSnapshot ─► CoW clone per pod (~5 s on TopoLVM)

  build time     verify_restored_chain: tip_height == the running validator's
```

- Digest taken **in flight** — bytes stream past a FIFO into a background hasher, so a 21 GB archive
  needs no second copy
- `mkfifo` stays in the foreground: `mkfifo … && { … } &` backgrounds the whole list and lets `tee` reach
  the path before it is a FIFO
- Hasher joined on a real pid, not a `>(…)` substitution, which the shell never waits for
- A mismatch fails the Job → PVC never marked ready, never snapshotted. Extraction has already written
  bytes by then (you cannot stream-verify before writing), but nothing downstream can reach them

## Known trade-offs

1. **Metadata and bytes are no longer atomic** — push, then commit: two systems, two steps, and a window
   where a committed manifest names an unpushed object. `ztest snapshot verify` closes it after the fact;
   nothing closes it during
1. **A bespoke store replaces a standard one** — LFS ships `ls-files`, `fsck`, `migrate`, `prune`;
   `ztest snapshot` is ours
1. **No automatic fetch** — producing a successor rung needs the archive fetched deliberately;
   `snapshot warm` still seeds from a local one when the bucket is unreachable
1. **The producer keeps the boundary gate** — "is this fixture non-vacuous" belongs where a live node
   exists, not at build time, where it would surface long after publishing

## See also

- [design-architecture.md](design-architecture.md#seeds--content-addressed-archive-pvcs) — seed PVC materialisation
- [guide-running-tests.md](guide-running-tests.md) — preflight's archive resolution
