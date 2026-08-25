# Chain snapshots: manifest-as-lockfile

Chain fixtures = **build inputs, not source**: pinned by hash, fetched on demand — the model Cargo, Go
modules, Bazel `http_archive` and Nix fixed-output derivations all use. Git holds a small plaintext record; the
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

Six keys, every one machine-computed, never hand-written. They describe the **packed** object
`push` produced, not the file handed to it.

```toml
name               = "zebra-v6.2.3-testnet-1848420.tar.zst"
sha256             = "8c350d15ecc54c5610707093e31293bc13f9d24acfc9bade9d987e60660ac9a6"
size_bytes         = 3499975919
uncompressed_bytes = 4468310016
base_uri           = "https://ztest-seeds.elicbarbieri.workers.dev"
key_prefix         = "lfs"
```

| Key                  | Consumed by                                                             |
| -------------------- | ----------------------------------------------------------------------- |
| `sha256`             | bucket key `<key_prefix>/<oid>`, seed PVC name, the puller's digest check |
| `size_bytes`         | `Bucket::has()` idempotent push, seek-table tail probe, progress denominator |
| `uncompressed_bytes` | seed PVC sizing                                                         |
| `name`               | `compression_from_name` → the puller's `tar` flag; error messages       |
| `base_uri`           | the read path, per artifact — repointing is a text edit, not a release  |
| `key_prefix`         | object namespace inside the bucket                                      |

Deserialises to `Artifact` one-to-one — same fields, no mapping layer.

The frame table is deliberately *not* here: it lives in the object (see
[Segmented archives](#segmented-archives)), so a 258 GiB snapshot adds 4 KiB to the blob rather
than ~500 rows to a committed file.

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
    │        │                                              committed, plaintext, 6 keys
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
    ├─►  ztest snapshot push <path> > snapshots/testnet/zebra-6.2.3-orchard.toml
    │       ← the only credentialed command in ztest; credentials from
    │         `ztest snapshot config`, never the environment
    │
    │    1. pack: re-emit as member-aligned 512 MiB zstd frames + a seek table.
    │       One streaming pass, one copy on disk; sha256 taken as bytes are written
    │    2. has(oid,size)? skip : resumable multipart → lfs/<oid>
    │          part i ─► R2 ─► ETag ─► append ~/.cache/ztest/uploads/<oid>.ledger
    │          killed? the next push adopts that uploadId and sends only what is missing
    │    3. the manifest → stdout. Progress and results → stderr
    │
    └─►  add a const to src/snapshots.rs, commit

  There is no `snapshot manifest` command. The record describes the packed object,
  which does not exist until it is packed — so a describe-only command could only
  print an oid for bytes nobody uploaded. A manifest now exists *because* a push
  succeeded, which is what used to depend on running two commands in the right order.
```

Every `ztest snapshot` step is cluster-free — publishing a fixture must not require a cluster.

## Consuming

```
  compile time   artifact!("snapshots/testnet/zebra-6.2.3-orchard.toml")  → no I/O, no git

  preflight      seed-<sha8>-<driver> in ztest-seeds?
                   ├── ready ─────────────────────► cached
                   └── absent ─► create PVC + puller Job
                                    │ manifest.base_uri/lfs/<oid> — public, no credential
                                    ▼
                 seek table ─ one ranged GET of the object's last 128 KiB
                   ├── present ─► segments, resumable      ← everything published now
                   └── absent  ─► one stream               ← pre-segmentation objects

  puller pod     mkfifo /tmp/verify.fifo
                 sha256sum < fifo > /tmp/verify.sum &        VERIFY_PID=$!
                 RESUME=$(cat /seed/.ztest-resume || echo 0)
                 for segment k >= RESUME:
                   { fetch range n+1 in the background; cat range n }   ← inside the segment
                     | tee fifo | dd | zstd -dc | tar -ixf - -C /seed
                   echo $((k+1)) > /seed/.ztest-resume     ← only once tar has returned
                 RESUME==0 && { wait $VERIFY_PID; [ "$ACTUAL" = "<oid>" ] || exit 1; }
                   │
                   ▼
                 VolumeSnapshot ─► CoW clone per pod (~5 s on TopoLVM)

  build time     verify_restored_chain: tip_height == the running validator's
```

- **Reads are unauthenticated, always.** There is no credentialed pull path and no fallback to one: the
  library carries no S3 client, so a run cannot sign a request even if someone exported a key. The read
  URL is whatever the manifest's `base_uri` names, which must be
  [`workers/seed-cdn/`](../workers/seed-cdn/README.md) — a read-only Worker over the bucket binding,
  and the bucket's only public read path (`r2.dev` is disabled). It replaced `r2.dev`, which
  Cloudflare documents as non-production, rate-limited *and* bandwidth throttled
  ([R2 limits](https://developers.cloudflare.com/r2/platform/limits/)). The throttle is *variable* —
  one object measured 1.4 MB/s at its worst and 23.7 MB/s at its best across a day, with three
  multi-hour pulls killed mid-stream. The Worker is not reliably faster instant-to-instant; it is the
  endpoint with no documented throttle to collapse
- **Ranges, not one stream.** A multi-hour single GET is dropped by any throttle, and a stream feeding
  `tar` cannot rewind — so the object arrives as 256 MiB ranges, each retried on its own. Every chunk is
  staged and emitted only whole: a partial range already on stdout would be re-sent by the retry,
  duplicating bytes past the hasher
- **One chunk ahead, never more.** Staging alone serialises transfer against extraction; chunk *n+1*
  downloads while *n* feeds `tar`. Measured 17.1 → 20.3 MB/s locally, 13.3 → 19.1 MB/s in-cluster, and
  the floor rises more than the mean. A deeper queue would stage more disk for a transfer already at the
  link's ceiling: 1 / 4 / 8 / 16 concurrent ranges all aggregate to ~20 MB/s, so parallelism is not the
  lever — overlap is
- **Resume is per segment.** The marker is written only once `tar` has returned, so an interrupted pull
  redoes at most one segment, and redoing one is harmless — `tar` overwrites what it already wrote. It
  lives on the PVC because that is the only thing outliving the pod, and is removed before the seed is
  published or every clone would carry it
- Digest taken **in flight** — bytes stream past a FIFO into a background hasher, so a 21 GB archive
  needs no second copy. Compared only on a pull that ran the whole object: a resumed one never held the
  earlier bytes, and what covers those is the per-frame checksum zstd enforces at decompression. A whole
  pull also draws the trailing seek table through the hasher, which is what binds the table the parent
  read off the network to the oid committed in the tree
- `mkfifo` stays in the foreground: `mkfifo … && { … } &` backgrounds the whole list and lets `tee` reach
  the path before it is a FIFO
- Hasher joined on a real pid, not a `>(…)` substitution, which the shell never waits for
- A mismatch fails the Job → PVC never marked ready, never snapshotted. Extraction has already written
  bytes by then (you cannot stream-verify before writing), but nothing downstream can reach them

## Segmented archives

A published object is a concatenation of independently-extractable segments, each a complete
`.tar.zst` over whole `tar` members, followed by a seek table in a skippable frame.

```
  lfs/<oid> = [ segment 0 ][ segment 1 ] … [ segment n ][ seek table ]
                   │                                         │
                   │  complete tar.zst, ~512 MiB extracted   │  skippable frame: sizes only,
                   │  fetch its range → zstd -dc → tar -x    │  offsets are the running sum
```

- **Still one plain `.tar.zst`.** [RFC 8878](https://datatracker.ietf.org/doc/html/rfc8878) makes
  concatenated frames mandatory decoder behaviour and requires skippable frames to be skipped, so the
  whole object decompresses and extracts normally — with `-i`, see the trade-off below
- **Why segments, not staging.** The alternative for resumability was downloading the blob to disk
  before extracting. These archives barely compress (mainnet Ironwood: 244.8 GiB packed against a
  257.8 GiB tree — RocksDB SSTs are already compressed), so staging is ~2× the volume, and on the seed
  PVC that inflation propagates to every per-test clone. Segments cost one segment of scratch instead
- **Why complete tars per segment, not one stream cut into frames.** A mid-stream fragment has no
  end-of-archive block, so `tar -x` on it fails at EOF. The price is 1 KiB of zero blocks per segment
  and the rule that a file cannot span a segment — irrelevant at 64 MiB SSTs against 512 MiB segments
- **Cuts land on member boundaries**, and never between a GNU long-name / pax extended header and the
  member it describes. `pack` copies members byte-for-byte rather than re-creating them, so modes,
  owners and sparse maps survive untouched; a pax *global* header is refused outright, because every
  segment would have to repeat it
- **The seek table is not in the manifest.** Sizes only, 8 bytes a frame, so ~500 frames is 4 KiB in the
  object rather than ~500 rows in a committed file. The parent reads it with one ranged GET beside the
  `blob_present` probe it already makes; the pod is shell and gets byte ranges already resolved
- **Only the format's principle is borrowed.** zstd's seekable format lives in `contrib/`, has no CLI in
  any standard distribution, and its jump table says nothing about `tar` member alignment — which is the
  actual constraint. What ztest writes is a compatible seek table over frames it aligned itself
- **`backoffLimit` follows what a restart costs**: 2 for a segmented object, which resumes off its
  marker; 0 without one, where a fresh pod would re-fetch from byte 0 against a clock sized for one pass

## Known trade-offs

1. **Metadata and bytes are no longer atomic** — push, then commit: two systems, two steps, and a window
   where a committed manifest names an unpushed object. `ztest snapshot verify` closes it after the fact;
   nothing closes it during. Narrowed, though not shut, by folding the record into `push`: the TOML only
   comes out of an upload that succeeded, so the manifest can no longer *precede* the bytes
1. **Segmented objects require `--ignore-zeros` on every consumer** — each segment ends with its own
   end-of-archive blocks, and a `tar` without `-i` stops at the first pair and exits **0** over a
   fraction of the tree. Nothing downstream would notice: the PVC gets marked ready over it. The flag is
   unconditional in `puller_cmd` and pinned by a test, but anything else that ever reads these objects
   inherits the requirement
1. **A bespoke store replaces a standard one** — LFS ships `ls-files`, `fsck`, `migrate`, `prune`;
   `ztest snapshot` is ours
1. **No automatic fetch** — producing a successor rung needs the archive fetched deliberately;
   `snapshot warm` still seeds from a local one when the bucket is unreachable
1. **The producer keeps the boundary gate** — "is this fixture non-vacuous" belongs where a live node
   exists, not at build time, where it would surface long after publishing
1. **The read path is a hard dependency with no fallback** — every seeded test fails if the Worker is
   down or over its request budget. Deliberate: a fallback is what let a throttled path masquerade as a
   working one for a day. The escape hatch is data, not code — `base_uri` lives per manifest, so
   repointing is a text edit in eight files, no release
1. **The upload ledger is local-only** — `ListParts` is the authoritative resume and would let another
   machine adopt an upload, but `object_store` exposes no `list_parts`. A push resumes on the machine
   that started it; anywhere else it starts over

## See also

- [design-architecture.md](design-architecture.md#seeds--content-addressed-archive-pvcs) — seed PVC materialisation
- [guide-running-tests.md](guide-running-tests.md) — preflight's archive resolution
