# Chain snapshots: manifest-as-lockfile

Chain fixtures are **build inputs, not source**. They are pinned by hash and
fetched on demand — the model Cargo, Go modules, Bazel `http_archive` and Nix
fixed-output derivations all use. Git holds a four-key record; the bytes live in
the snapshot bucket, addressed by content.

This replaced Git LFS, whose premise — materialise large files on checkout so
they feel like source — is one ztest worked against at every layer.

## Why not Git LFS

Measured, not argued:

| Observation | Consequence |
| --- | --- |
| `.git/lfs` 58 G + worktree 58 G, real repo 230 M | 116 GiB of disk for 58 GiB of fixtures |
| `git lfs` not installed on a working dev machine, seeding works | LFS was never on the fetch path |
| `lfs_transfer.rs` 349 lines + `lfs_names`/`lfs_pointer` ~199 | ~550 lines existed only to undo LFS |
| `git ls-files --exclude-standard` already skips ignored files | gitignoring the archives replaced the build-context workaround outright |

The pointer was the one thing LFS genuinely provided: a git-native binding
between a name and a hash. That binding is now the manifest, and
`ztest snapshot verify` replaces `git lfs fsck`.

## The two types

An artifact is a blob in a bucket. A chain snapshot is an artifact plus which
chain it holds. Nothing else.

```rust
pub struct Artifact      { name, oid, size, uncompressed_bytes }         // src/archive.rs
pub struct ChainSnapshot { tip_height, network, backend, artifact }
```

Both are plain data — no methods, no derivation, nothing to keep in sync. Every
field is `pub`, so a consuming crate can declare its own snapshot.

This is what dissolved the old `Option<ChainInfo>`: a mount always wanted the
blob and never the chain, which is exactly why the field was optional.
`MountSource::Seed` now takes an `Artifact`, and the option is gone.

## Declaring one

The whole declaration, in `src/snapshots.rs`:

```rust
pub const ORCHARD_TESTNET: ChainSnapshot = ChainSnapshot {
    tip_height: 1_848_420,
    network: Network::Testnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/testnet/orchard.toml"),
};
```

Chain facts are written here, beside the doc comment describing them — a wrong
height is visible in review, which is the only check that was ever going to
work. `tip_height` is checked against the running validator at `env.build()`,
so a wrong one fails by name on first use.

`artifact!` is the only macro left, and it does the one thing a `const fn`
cannot: read a file at compile time. The path names the *manifest*, because the
archive is the one file never in the tree.

## Manifest schema

Four keys, every one machine-computed. Nothing here is ever hand-written.

```toml
name               = "zebra-v6.2.3-testnet-1848420.tar.zst"
sha256             = "8c350d15ecc54c5610707093e31293bc13f9d24acfc9bade9d987e60660ac9a6"
size_bytes         = 3499975919
uncompressed_bytes = 4468310016
```

| Key | Consumed by |
| --- | --- |
| `sha256` | bucket key `lfs/<oid>`, seed PVC name, the puller's digest check |
| `size_bytes` | `Bucket::has()` idempotent push, pull budget, progress denominator |
| `uncompressed_bytes` | seed PVC sizing |
| `name` | `compression_from_name` → the puller's `tar` flag; error messages |

The file deserialises to `Artifact` one-to-one — same four fields, same owner,
no mapping layer.

### What the old schema carried, and where it went

Audited by grepping every accessor across ztest and both zaino sync profiles.

| Field | Callers | Disposition |
| --- | --- | --- |
| `tip_hash` | **zero, anywhere** | deleted |
| `db_format` | **zero** | deleted |
| `uncompressed_bytes` | **zero** — its doc claimed it sized the PVC; the sizing site said otherwise by name | kept, now genuinely read |
| `[activations]`, `[activations.above_tip]` | `verify_restored_chain` | deleted — consensus constants, and a table of them is the thing that needs maintaining every time an upgrade ships |
| `[boundary_check]` | one site | deleted — the producer script keeps the gate, where a live node already exists |
| `backend`, `network`, `version` | 1, 3, 2 | `backend`/`network` moved to the declaration; `version` deleted |
| `contents`, `produced_by`, `stop_method` | never parsed | deleted |

`sha256` already pins the bytes, so any fact *derived* from those bytes is
implied by it. What survived is what addresses the blob, plus what a test reads.

## Trust chain

```
  git (trust root)
    │
    ├── snapshots/<network>/<upgrade>.toml ──── name, sha256, sizes
    │        │                                  committed, plaintext, 4 keys
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
  produce-chain-fixture.sh <height> <version> <network>
    │  zebrad --state.debug_stop_at_height=H, compact, tar --zstd
    │  boundary gate: did the pool move across [activation, H]?   ← stays here;
    ▼                                                               it has a live node
  ./zebra-v6.2.3-testnet-1848420.tar.zst
    │
    ├─►  ztest snapshot manifest <path> > snapshots/testnet/orchard.toml
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

All three are cluster-free — publishing a fixture must not require a cluster.

## Consuming

```
  compile time   artifact!("snapshots/testnet/orchard.toml")  → no I/O of bytes, no git

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

The digest is taken **in flight**: the bytes stream past a FIFO into a
background hasher, so a 21 GB archive needs no second copy. `mkfifo` stays in
the foreground — `mkfifo … && { … } &` backgrounds the whole list and lets
`tee` reach the path before it is a FIFO. The hasher is joined on a real pid
rather than a `>(…)` substitution, which the shell never waits for.

A mismatch fails the Job, so the PVC is never marked ready and never
snapshotted. Extraction has already written bytes by then — you cannot
stream-verify before writing — but nothing downstream can reach them.

## Known trade-offs

1. **Metadata and bytes are no longer atomic.** Push, then commit — two systems,
   two steps, and a window where a committed manifest names an unpushed object.
   `ztest snapshot verify` closes it after the fact; nothing closes it during.
2. **A bespoke store replaces a standard one.** LFS ships `ls-files`, `fsck`,
   `migrate`, `prune` and other people's answers. `ztest snapshot` is ours.
3. **No automatic fetch.** `git clone && git lfs pull` produced working blobs.
   Producing a successor rung now needs the archive fetched deliberately;
   `snapshot warm` still seeds from a local one when the bucket is unreachable.
4. **The producer keeps its boundary gate.** Deleting `[boundary_check]` moved
   the "is this fixture non-vacuous" question to the script, which has a live
   node — not to build time, where it would be discovered long after publishing.

## See also

- [fixtures/chains/README.md](../fixtures/chains/README.md) — producing a fixture
- [design-architecture.md](design-architecture.md#archive-pvcs) — seed PVC materialisation
- [guide-running-tests.md](guide-running-tests.md#preflight) — preflight's archive resolution
