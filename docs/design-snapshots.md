# Chain snapshots: manifest-as-lockfile

Chain fixtures = **build inputs, not source**: pinned by hash, fetched on demand — the model Cargo, Go
modules, Bazel `http_archive` and Nix fixed-output derivations all use. Git holds a small plaintext record;
the bytes live in the snapshot bucket, addressed by content.

A snapshot is **a plain directory tree**, stored file-by-file. No archive, no packer, no custom upload
protocol: ztest is a thin shim around [rclone](https://rclone.org) on both ends.

## The two types

An artifact = a tree in the bucket. A chain snapshot = an artifact + which chain it holds.

```rust
pub struct Artifact      { name, oid, size, base_uri, key_prefix }   // src/archive.rs
pub struct ChainSnapshot { tip_height, network, backend, artifact }
```

```rust
pub const ORCHARD_TESTNET: ChainSnapshot = ChainSnapshot {
    tip_height: 1_848_420,
    network: Network::Testnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/testnet/zebra-6.2.3-orchard.toml"),
};
```

- Chain facts written at the declaration (reviewed); `tip_height` is checked against the running
  validator at `env.build()`
- `artifact!` reads the manifest at compile time — no bytes, no network, no `git`

## Bucket layout

```
snap/<oid>/<relpath>       every file of the tree, e.g. snap/<oid>/state/v28/mainnet/000013.sst
snap/<oid>/SHA256SUMS      uploaded last: a tree without it is an incomplete push, never referenced
```

`SHA256SUMS` is standard coreutils `sha256sum` output: one `<64 hex>  <relpath>\n` line per file,
sorted bytewise (`LC_ALL=C`) by relpath, relpaths relative to the tree root with no `./` prefix.
Relpaths use only `A-Z a-z 0-9 . _ -` segments joined by `/` (the Worker serves nothing else).

**oid = sha256 of the exact `SHA256SUMS` bytes.**

## Manifest schema

`snapshots/<network>/<name>.toml`, written by `ztest snapshot push`, never by hand:

```toml
name       = "zebra-6.2.3-orchard-testnet"
sha256     = "<oid>"
size_bytes = 4468318208
base_uri   = "https://ztest-seeds.elicbarbieri.workers.dev"
key_prefix = "snap"
```

| Key          | Consumed by                                                             |
| ------------ | ----------------------------------------------------------------------- |
| `sha256`     | key prefix `snap/<oid>/`, seed PVC name, the puller's SHA256SUMS check  |
| `size_bytes` | Σ file sizes: seed PVC sizing (+15 % headroom), progress denominator    |
| `name`       | diagnostics                                                             |
| `base_uri`   | read path, per artifact — repointing is a text edit, not a release      |
| `key_prefix` | object namespace inside the bucket                                      |

## Trust chain

```
  git (trust root)
    └── snapshots/<network>/<name>.toml ── sha256 = oid
           │  compile time: artifact!() → Artifact
           ▼
  R2    snap/<oid>/SHA256SUMS ── sha256(bytes) must equal oid       (puller, verify, check)
           │  one line per file
           ▼
        snap/<oid>/<relpath> ── sha256(bytes) must equal its line   (puller: sha256sum -c)
```

The committed `sha256` binds `SHA256SUMS`, and `SHA256SUMS` binds every file — so one 64-hex value in
git pins the whole tree.

## Publishing

Only publishers need credentials; reads are credential-free through the Worker. ztest stores and reads
no credentials — they live in rclone's config.

### rclone setup (once per publishing machine)

```sh
rclone config create ztest-r2 s3 provider=Cloudflare env_auth=false \
  access_key_id=<R2 key id> secret_access_key=<R2 secret> \
  endpoint=https://<account-id>.r2.cloudflarestorage.com no_check_bucket=true
rclone config create ztest-seeds alias remote=ztest-r2:ztest-archives
rclone lsf ztest-seeds:   # sanity check
```

- The R2 API token needs **Object Read & Write** on the `ztest-archives` bucket (never account-wide)
- `no_check_bucket=true` is required for object-scoped tokens (they cannot create or probe buckets)
- ztest only ever names `ztest-seeds:`; the alias keeps the credentialed remote's name yours

### Push

```sh
ztest snapshot push <state-dir> --name zebra-6.2.3-orchard-testnet \
  > snapshots/testnet/zebra-6.2.3-orchard.toml
```

1. `rclone hashsum sha256 <dir>` → normalised into `SHA256SUMS` (sorted, validated relpaths, non-empty)
1. oid = sha256(`SHA256SUMS`); `size_bytes` = Σ file sizes
1. `rclone copy <dir> ztest-seeds:snap/<oid>` — exactly the hashed files; retries/timeouts tuned for
   multi-hour uploads; progress on stderr
1. `rclone copyto SHA256SUMS ztest-seeds:snap/<oid>/SHA256SUMS` — **last**
1. manifest TOML → stdout

Rerunning is the resume: rclone skips files already uploaded. Then add a const to `src/snapshots.rs`
and commit. Every `ztest snapshot` publish/verify step is cluster-free.

## Consuming

```
  compile time   artifact!("snapshots/…toml")                       → no I/O, no git

  preflight      seed-<sha8>-<driver> in ztest-seeds?
                   ├── ready ─► cached
                   └── absent ─► GET SHA256SUMS (exists? hashes to oid?) ─► PVC + puller Job

  puller pod     docker.io/rclone/rclone (pinned), SEED_URL = <base_uri>/snap/<oid>/
                 rclone copyurl  ${SEED_URL}SHA256SUMS → sha256 == oid, else fail
                 rclone copy --http-url $SEED_URL :http: /seed --files-from-raw <relpaths> --no-traverse
                 cd /seed && sha256sum -c SHA256SUMS            (mismatch → Job fails)
                   │
                   ▼
                 ready=true ─► VolumeSnapshot ─► CoW clone per pod
```

- **Reads are unauthenticated, always.** The library has no S3 client. The read URL is the manifest's
  `base_uri`: [`workers/seed-cdn/`](../workers/seed-cdn/README.md), a read-only Worker over the bucket
  binding (the bucket's only public path; `r2.dev` is disabled for its variable throttle)
- **No listings.** The file list comes from `SHA256SUMS`; rclone's http backend with `--files-from
  --no-traverse` issues one HEAD + one GET per file
- **Resume = rerun.** A dead pod's successor reruns the same copy; files already whole (`--size-only`,
  rclone lands via `.partial` + rename) are skipped. Every Job retry resumes
- **Progress** = rclone's JSON stats records (exact cumulative bytes) during the copy, then the
  `relpath: OK` lines of `sha256sum -c` (sorted, so monotonic) as the verify heartbeat
- **No chown/chmod.** The seed is a standard mount; containers needing other ownership handle it
- A mismatch fails the Job → PVC never marked ready, never snapshotted

## Known trade-offs

1. **Metadata and bytes are not atomic** — push, then commit. The TOML only comes out of a push that
   finished (including `SHA256SUMS`), and `ztest snapshot verify` checks every committed manifest after
   the fact
1. **Request count scales with file count** — 2 requests per file per pull against the Worker's daily
   budget
1. **The read path is a hard dependency with no fallback** — every seeded test fails if the Worker is
   down. The escape hatch is data: `base_uri` lives per manifest

## See also

- [design-architecture.md](design-architecture.md#seeds--content-addressed-archive-pvcs) — seed PVC
  materialisation
- [guide-running-tests.md](guide-running-tests.md) — preflight's seed resolution
