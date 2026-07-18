# Storage backend for archive seeds

`src/storage/` abstracts **where a seed's bytes come from**, decoupled from how
they're materialised into a `seed-{sha8}` PVC. It mirrors the `ImageProvider`
shape (`src/backends/image/`): one trait, file-per-backend, one selection point.

## The two axes, kept separate

The confusion this design resolves: "use Git LFS" conflates two orthogonal
things.

- **Storage of committed assets** — where multi-GB `.tar.zst` blobs live, off
  the git object store. That's Git LFS + `.lfsconfig` pointing at a self-hosted
  rudolfs server. Pure git-layer config; no ztest code.
- **Byte production at provision time** — how ztest gets a seed's real bytes
  into a PVC when the working tree holds only an LFS *pointer*. That's this
  module.

`.lfsconfig` alone doesn't solve the second: a committed archive is a pointer,
the blob is absent, and `materialize` needs the real bytes. That fetch is what
`storage` owns.

## Dispatch is by content, not configuration

A seed source (the macro-baked absolute path from `#[ztest::archive]` /
`mount_file!`) is classified **per file, by sniffing for an LFS pointer** —
*not* by an env var:

- real archive/blob on disk → [`Local`] — dev-authored, or already
  `git lfs pull`ed; streamed straight off disk.
- Git LFS pointer, blob absent → [`Lfs`] — fetched from the server.

This is deliberately different from `ImageProvider`'s env-based selection,
because it matches the real axis: *committed-LFS vs local-uncommitted*. A laptop
that runs `git lfs pull` turns a pointer into a real file, so it transparently
takes the `Local` path. Env (`ZTEST_LFS_URL`) only configures *where the server
is*, never *whether* to use LFS — the pointer's presence decides that.

## Content address is free across backends

`seeds::sha8` names a PVC `seed-{sha8}` by the SHA-256 of the `.tar.*` bytes. An
LFS pointer's `oid` **is** that SHA-256. So:

- `storage::content_sha8` resolves a pointer's seed id from the pointer text
  alone — **no transfer** to name the seed.
- a cold CI run (fetches from LFS) and a warm laptop (`git lfs pull`ed) produce
  the **same** `seed-{sha8}`, so they share and reuse the same content-addressed
  seed and snapshot.

`seeds::sha8` delegates to `content_sha8`, so every existing call site
(materialize, the `SeedProvider` resource node, `cli snapshot`) became
pointer-aware in one change.

## The seam in `materialize`

`materialize::ensure_seed` is unchanged in shape. The only coupling to "the
source is a local file" was two lines, both now routed through the backend:

1. content address — `seeds::sha8` → `content_sha8` (above).
2. byte production — the uploader-pod path replaced `tokio::fs::File::open` with
   `storage::for_source(source)?.open()`, a `dyn AsyncRead`.

Compression is resolved by `backend.compression()` **before** `open()`, from the
filename extension (both backends) with a magic-byte fallback for on-disk files.
So the uploader `tar` command is fixed before any download starts, and the LFS
`open()` is deferred until the uploader pod is scheduled and ready on stdin — no
HTTP connection is held across pod scheduling.

Everything downstream — uploader pod, stdin attach, `ready` label, VolumeSnapshot,
shadow clone — never learns which backend produced the bytes.

## The LFS backend (`lfs.rs`)

Speaks the **Git LFS batch API over HTTP** to rudolfs, not `git lfs`:

```
POST {endpoint}/objects/batch   {operation:"download", objects:[{oid,size}]}
  → {objects:[{actions:{download:{href, header}}}]}
GET  href                        (streamed into the uploader pod's stdin)
```

No git checkout, no `git-lfs` binary — so it runs anywhere the orchestrator does
(cold CI, and later an in-cluster cache pod). The blob streams *through* the
orchestrator into the existing uploader pod's stdin, reusing the whole
materialisation path; only the byte source differs.

Endpoint resolution: `ZTEST_LFS_URL` (with optional `ZTEST_LFS_TOKEN` →
`Authorization: Bearer`), else the `[lfs] url` of the nearest `.lfsconfig`
walking up from the source. A pointer with neither configured fails fast with an
actionable error before any pod is created.

## What's deliberately not here

- **A `git lfs` CLI backend.** Once `git lfs pull` runs, the blob is a real file
  handled by `Local`, so a CLI backend would be redundant. The batch-API path is
  the one that works where a checkout doesn't exist.
- **In-cluster fetch (F6).** Today the orchestrator does the GET and streams into
  the uploader pod. A future cluster-resident cache would have the uploader pod
  GET from rudolfs directly (new uploader image + href plumbing); the trait seam
  is ready for it as another backend.
- **Preflight pre-warm banner classification.** `pipeline/archives.rs` still only
  *observes* seed PVCs for the banner; wiring declared-seed source classification
  (`storage::source_kind`) into a truthful per-archive `DownloadSource` needs the
  dumped `SeedEntry` list threaded through `cli::run`, a separate change.
```
