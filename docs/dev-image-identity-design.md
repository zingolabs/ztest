# Dev-image identity: snapshot, key, reference

## The failure this exists to eliminate

On 2026-08-11 a 123-test run failed in its entirety. Every test pod reported
`ImagePullBackOff`. The referenced image, `zainod:dev-2ead673eeb59`, had never
existed in the registry; the run that scheduled those pods had built and pushed
`dev-d9608e5daa57` and `dev-af3ed3760a07`. Four distinct hashes, no overlap.

The mechanism, confirmed by reconstructing `bundle::pack` byte-for-byte against
the live checkout: the `<repo>:dev-<hash>` tag is a SHA-256 of a tar of the
zaino working tree, and it is computed independently **three to four times per
run**, minutes apart —

| site                                                               | purpose                                          |
| ------------------------------------------------------------------ | ------------------------------------------------ |
| `resource/impls/image.rs` (`ImageNode::new`)                       | names the image that is built and pushed         |
| `resource/entry.rs` (`image_node_id`), called from `cli/run.rs` ×2 | keys dependency edges and the failed-node lookup |
| `resource/entry.rs` (`dev_image_refs`)                             | names the image **pods pull**                    |

Nothing pins the tree between the first and the last. The window is the whole
build phase. A 22-minute observation of the zaino checkout recorded the packed
context changing **18 times** — `git rebase` starts, branch checkouts with
autostash, files appearing and vanishing, the digest returning to an exact
earlier value twice. That is ordinary development, and it is sufficient to split
the key.

The build tag and the pod-facing tag are therefore two *predictions* of the same
value, made at different times against a moving input. When they disagree, every
test in the run dies with an error that names neither the cause nor the culprit.

Two further defects share this root cause:

- `image_node_id` re-hashes, so the `NodeState::Failed` lookup in
  `dev_image_refs` can miss its own node — **a failed build can be published to
  the manifest as if it had succeeded**.
- `if let Ok(tag) = …` in the same function silently swallows a hashing error,
  omitting the image with no diagnostic.

## Invariants

The design is the minimum machinery that makes these true by construction.

1. **One snapshot per run.** The input set is materialized once, as an immutable
   object. Every downstream value derives from that object, never from the live
   filesystem.
1. **The key is a function of declared inputs.** Everything that can change the
   built artifact is folded in; nothing else is.
1. **The reference is an observation, never a prediction.** What pods pull is
   derived from the artifact that was built or verified to exist.
1. **A failed build is never referenced.** Absence of a successful provision
   must make the reference unavailable, not stale.
1. **No harness-specific config file.** Exclusion rides files developers already
   maintain.

Invariants 1 and 3 are independent, and both are required. Snapshotting fixes
the *key*; observation fixes the *reference*. Only the second makes the reported
failure unrepresentable rather than merely unlikely.

## Three concepts, currently conflated into one string

| concept       | question                      | wrong answer costs                 | source of truth         |
| ------------- | ----------------------------- | ---------------------------------- | ----------------------- |
| **snapshot**  | what exactly are we building? | irreproducibility                  | a git tree object       |
| **key**       | can we skip work?             | rebuild (cheap) or staleness (bad) | hash of declared inputs |
| **reference** | what do pods pull?            | **total run failure**              | the push / the registry |

Today `<repo>:dev-<hash>` is all three. A heuristic-grade computation is
load-bearing for a correctness-grade job.

## Design

### 1. Snapshot — a git tree object

Per source repo, once per run:

```sh
TMPIDX="$XDG_CACHE_HOME/ztest/index-<repo-id>"   # MUST be outside the worktree
cp "$GIT_DIR/index" "$TMPIDX"
GIT_INDEX_FILE="$TMPIDX" git add -A -- <pathspecs>
TREE=$(GIT_INDEX_FILE="$TMPIDX" git write-tree)
```

Measured ~20 ms on the zaino checkout, and verified across nine worktree states:
unstaged edits, untracked files, untracked *directories*, deletions, and
staged-plus-further-unstaged (distinct from staged-only) all change the tree id;
a modified **gitignored** file does not; restoring to clean returns the original
id.

The temp index must not live inside the worktree, or `git add -A` adds it to
itself and the id never stabilizes.

For a context that is a subdirectory of the repo, address the subtree directly:
`git rev-parse "$TREE^{tree}:<subdir>"`.

For a build whose source spans repos (the runner image), the snapshot is the
sorted list of `(repo_rel_path, tree_id)`.

**Fallback.** A context outside any git checkout falls back to the current
filesystem walk, under a distinct key schema tag so the two can never collide.
This path must be exercised in tests, not left to discover itself in the field.

### 2. Enumeration and materialization

Replace only the `walkdir` traversal in `bundle::collect` with

```sh
git ls-tree -r -z <TREE>   |   git cat-file --batch
```

**Keep `bundle::write_tar` exactly as it is** — sorted paths, zeroed
mtime/uid/gid, normalized mode — and keep `digest = sha256(tar)`. This preserves
the module's existing and correct contract: the digest names the tar that was
actually staged, so the tag can never name a context the archive didn't ship.

Two properties fall out for free:

- **Large payloads stay out.** Enumerating from git is what makes a context
  sitting next to multi-GB chain archives tractable: git lists what is tracked,
  not what is on disk. `bundle::pack` reads every file and the whole tar into
  memory, so a `dev!(…, context = ".")` over a directory holding a materialized
  archive is an out-of-memory failure, not merely a slow one. (Written when
  ztest's own `fixtures/` was Git LFS-tracked — 14 GB smudged, a few KB as
  pointers; the archives are now gitignored and bucket-hosted, see
  [design-snapshots.md](design-snapshots.md). The property is unchanged, and
  still applies to a consuming repo that does use LFS.)
- **BuildKit's incremental context sync.** Its differ compares size + mtime, and
  our zeroed mtimes mean an unchanged tree transfers ~zero bytes even though the
  build pod's context dir is a fresh `emptyDir` each run.

**Do not use `git archive`.** Measured non-deterministic from a *tree* — it
stamps entry mtime with the current time, so two runs seconds apart produce
different digests — and `--mtime=@0` had no observable effect on git 2.55. It
also smudges LFS by default (14.1 GB in 31 s, versus 3.33 MB in 20 ms with the
filters disabled).

### 3. Exclusion: git first, `.dockerignore` as a filter on top

The file set becomes `tracked ∪ (untracked − ignored)`, which is exactly what
`pipeline/remote_compile.rs` already runs:

```
git ls-files -z --cached --others --exclude-standard
```

Adopting the rule the compile path already uses ends the current situation where
one run has two incompatible definitions of "the source".

This rule, not git's tracked-only rule, is deliberate. Tracked-only (Nix flakes)
**silently omits new untracked files**, which is the single most-reported UX
failure in this design space and is still open upstream. `--exclude-standard`
also honours `.git/info/exclude` and `core.excludesFile`, so per-developer local
exclusions work without touching a tracked file.

**Measured against zaino, git enumeration is not equivalent to today's context:**

|                                                                                | count |
| ------------------------------------------------------------------------------ | ----- |
| currently packed                                                               | 280   |
| git-enumerated                                                                 | 298   |
| dropped (`scripts/produce-chain-fixture.sh`, excluded via `.git/info/exclude`) | 1     |
| added (`.github/**`, `.gitignore`)                                             | 19    |

The additions are the problem: `.github/` is tracked, irrelevant to the image,
and edited often enough to churn the cache. Two ways to keep it out:

- **(A) `.gitattributes export-ignore`** — the git-native answer. Requires adding
  a `.gitattributes` to each component repo (zaino has none today). Note that
  `export-ignore` does **not** change the tree id, so we must keep keying on our
  own tar digest — which we already do.
- **(B) Apply the existing `.dockerignore` filter to the git-enumerated set** —
  no component-repo changes, and zaino's `.dockerignore` already excludes
  `.github/` and `.gitignore`.

**Adopt (B).** It requires no change to any component repo, preserves the image
contents we ship today, and the "chasing Docker's ignore semantics is a losing
battle" argument is much weaker once git has already removed build outputs, tool
caches, and VCS internals: the residual patterns are plain directory names, far
from the negation and `**`-positioning edge cases where `moby/patternmatcher` is
known-divergent (it deprecates its own `Matches()` as buggy, and a negation bug
has been open since 2019).

Document the `.dockerignore` support as approximate. (A) remains the migration
path if we later want the emulation deleted; it is not a prerequisite.

**No new config file is introduced.** `.gitignore`, `.git/info/exclude`,
`core.excludesFile`, and `.dockerignore` are all pre-existing and
developer-maintained. Explicitly rejected for violating this constraint:
`.ztestignore`, and Bazel/Buck-style explicit source lists.

### 4. Key composition

```
key = sha256( SCHEMA_VERSION
            ⊕ sha256(deterministic_tar)          // §1–3
            ⊕ dockerfile_bytes
            ⊕ sorted[(stage, base_ref@sha256)]   // §5
            ⊕ sorted[(arg, value)]               // args DECLARED in the Dockerfile
            ⊕ target_platform
            ⊕ rust_toolchain_id
            ⊕ features )
```

`SCHEMA_VERSION` is bumped to invalidate globally when the composition changes;
it also distinguishes the git path from the filesystem fallback.

Build args are folded from the set the **Dockerfile declares**, parsed, not from
whatever the caller passed — otherwise an unused arg fragments the cache while
the ones that matter go unnoticed.

The tag remains `<repo>:dev-<12 hex>`, unchanged in form. It stays the node
identity, the human-readable name, and the warm-cache probe target.

### 5. Base image digests — closing the stale-image hole

`dev-<hash>` currently folds the context, features, and rust_version but **not
the base image**. `zaino/Dockerfile` builds `FROM rust:${RUST_VERSION}-bookworm`
and `FROM debian:bookworm-slim` — mutable tags. When they move, the key does not,
and a stale image is silently reused. This is the one failure mode where a wrong
answer is *worse* than the reported bug, because it fails quietly.

For each `FROM` in the Dockerfile:

1. `HEAD /v2/<name>/manifests/<tag>` → `Docker-Content-Digest`. Send `Accept`
   for **both** the OCI index and the Docker manifest-list media types, or a
   registry may return a converted single-arch manifest with a different digest.
1. Fold `(stage, ref@sha256)` into the key.
1. Rewrite the `FROM` to `image:tag@sha256:…` so BuildKit resolves the same
   bytes we hashed.

Both steps are required: rewriting alone leaves our key blind; hashing alone
lets BuildKit re-resolve independently. Resolve per target platform — a
manifest-list digest and its per-arch children are different values.

`# syntax=` is a moving tag by design; pin it and own the upgrade.

### 6. Reference — derived from the artifact

Every topology now pushes to a registry (`from_env` returns one provider; the
kind sideload path is deleted, and kind is served by a registry plus the k3s
embedded mirror). **There is therefore no per-topology reference form** — one
mechanism, one string shape, no platform branch. This is strictly simpler than
the earlier draft of this section, and it is what `design-platform-collapse.md`'s
rule requires.

Pods get `repo:dev-<hash>@sha256:…`. Kubernetes uses **only the digest** to pull
when both are present, so the tag costs nothing at runtime and preserves every
debuggability property we have today.

The digest is captured at the `ImageProvider` seam — `build_image` and
`image_built` return it — so the capture survives the in-flight change of build
engine (see §12). Per engine:

| engine                                                       | cold build                                                                                                                                    | warm cache                                                     |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| local `docker` (today, `Docker` provider)                    | `docker buildx build --metadata-file` → `containerimage.digest`, or the `digest: sha256:…` line `docker push` already prints                  | `docker manifest inspect` stdout (`-v` → `.Descriptor.digest`) |
| on-cluster `buildctl` (runner today, `dev!` images intended) | `--metadata-file` → `containerimage.digest`; buildctl runs under `kubectl exec`, so the JSON must return over stdout behind a sentinel marker | as above                                                       |

**The warm path still costs zero additional round-trips**, but for a different
reason than in the earlier draft: `docker::exists_in_registry` already runs
`docker manifest inspect` and throws away stdout, keeping only the exit status.
The digest is in output we are already producing. (The in-process HTTPS `HEAD`
probe that the earlier draft relied on — `manifest_exists`, the `Session`/`Ref`
types, and the `openshift_manifest_digest` seam its module doc advertised — was
deleted with `openshift.rs`. So was the `Ref` tag parser that this document
previously listed as needing a digest-form fix. Both items are void.)

**Consumer side needs no change.** `ImageProvider::image_reference` is a lookup
in a `DevImageId → String` map and pods consume the string verbatim; nothing
downstream parses it.

**Retention note.** The tag portion of a pod reference is client-side decoration
— the registry never sees it, and it keeps nothing alive. What protects our
images from the OpenShift pruner is the **ImageStream tag created by the push**,
which `keepTagRevisions` honours per tag. Measured on this cluster: the pruner
runs nightly with `keepYoungerThan=1h keepTagRevisions=3 allImages=true` and has
deleted **0 objects** on three consecutive nights, because every tag we push has
exactly one revision (`rev 0 < 3`). The rule to preserve: **never publish a
reference to an image that is not also tagged.** A digest-only reference would
be genuinely exposed.

### 7. Threading — the change that actually fixes the bug

- `ImageNode::provision` currently calls `build_image`, **which already returns
  the resolved reference**, and discards it with a comment conceding that the
  image phase will recompute it. Surface it into node state instead.
- `ImageNode::probe` returns the reference it verified on a warm hit.
- `dev_image_refs` **reads provisioned node state**. It must not call `dev_tag`.
- `image_node_id` reads the snapshot-derived key, computed once, rather than
  re-deriving it.
- The `if let Ok(tag)` swallow becomes an explicit error surfaced into the run's
  failure reporting.

After this, a reference in the manifest can only originate from a node that
provisioned or verified successfully. Invariant 4 holds structurally.

### 8. Runner image — same mechanism

`ztest-runner:dev-ztest-elicb-<run-id>` is content-addressed by nothing: it is
rebuilt every run and never shared. It moves onto the same scheme, with the
snapshot being the multi-repo form from §1:

```
runner_key = sha256( SCHEMA_VERSION
                   ⊕ sorted[(repo_rel, tree_id)]   // the set remote_compile already enumerates
                   ⊕ RUNNER_DOCKERFILE bytes       // include_str!, so already in-process
                   ⊕ base_ref@sha256
                   ⊕ rust_toolchain_id )
```

`remote_compile` already computes this file set; it needs to hash it rather than
only ship it. This collapses three naming schemes (content-hash, pinned git rev,
per-run id) to two, and the survivors are both immutable-snapshot schemes.

### 9. Repo hygiene — the harness must not write into a hashed context

`cli/sync/perf.rs` defaults `--out` to `ztest-perf-<id>`, a **CWD-relative**
path. Invoked from a component checkout, it writes artifacts into the build
context: `live-tests/ztest-perf-zaino-state-sync-56cb0345/zainod-profile.pb` was
inside the packed tar. The harness invalidates its own cache.

The default moves out of the CWD, under the ztest cache/state directory, keyed
by sync id. Explicit `--out` continues to honour whatever the user names.

Note this is *also* fixed incidentally by §3 for the common case (untracked
output under a `.gitignore`d path is excluded), but relying on that would leave
the harness writing into a repo it does not own. Both changes land.

### 10. Attribution — stop discarding the diagnosis

`pod_status::image_error` matches on `waiting.reason` and **discards
`waiting.message`**. The kubelet reported
`reading manifest dev-2ead673eeb59 … manifest unknown`; the operator saw
`ImagePullBackOff`. One discarded field turned a name-mismatch bug into an
afternoon.

- Capture and propagate `waiting.message`.
- Preflight each unique image once before scheduling any pod, so one failure
  aborts the run instead of 123 pods discovering it independently. The pull
  backoff reaches a compiled-in 5-minute ceiling and is not worth waiting out.
- Verify the **blobs**, not only the manifest: a manifest `HEAD` returning 200
  does not mean the image is pullable, and missing-blob-with-valid-manifest is a
  long-standing registry failure mode. Config + layer HEADs, once per unique
  image per run.
- Distinguish causes using OCI error codes — `NAME_UNKNOWN` (repository never
  existed), `MANIFEST_UNKNOWN` (exists, this reference does not), `BLOB_UNKNOWN`
  (manifest fine, layers gone) — falling back to `GET /v2/<repo>/tags/list`,
  since registries often return 401 rather than 404 to avoid leaking existence.

### 11. Probe becomes a provisioning gate

Where BuildKit does the building, it should be the sole authority on *what* to
rebuild. Its cache lives on a persistent PVC (`ztest-buildkit-cache`, mounted at
`BUILDKIT_STATE_DIR` = `/home/user/.local/share/buildkit` under the rootless
posture) — the *pod* is ephemeral, the *state* is not — and its `COPY` cache key
is a content checksum that **excludes mtime**, which our deterministic tar
already satisfies.

The registry probe's remaining value is deciding **whether to stand up the build
pod at all** — `qos::build::BUILDKIT_BUILD` at Guaranteed QoS, with pod schedule
and image pull dominating the wall clock. That is worth a ~100 ms probe. What it
must stop doing is deciding *what to rebuild*, which is a second cache-key
implementation that must agree with the builder's and silently wins when it
doesn't.

Two caveats, both from the in-flight state described in §12:

- Today `dev!` images are built by the local-`docker` provider, not by BuildKit,
  so the "BuildKit is the authority" half currently applies only to the
  runner/compile path in `remote_compile`. It becomes general when `dev!` images
  move onto `buildctl`, which `capability::probe_builder` already anticipates.
- If the builder becomes an operator-provided, long-lived StatefulSet (which
  `probe_builder` looks for and `buildkit.rs` does not yet create), the
  provisioning-gate argument weakens considerably — there is no expensive pod to
  avoid standing up. Re-derive this section at that point rather than carrying
  the conclusion forward.

Measure before committing to the split: `buildctl debug histories` exposes
`numCachedSteps`/`numTotalSteps`, which gives the real warm no-op cost.

### 12. Status against `feat/platform-collapse`

This plan was written against `master` at `e96afec` and re-verified against
`feat/platform-collapse` at `9e372c6` (58 files, −7,486 lines). What follows is
the verification, so a later reader knows which claims were re-checked.

**Unchanged — the bug and everything the fix depends on:**

| fact                                                                  | location                            |
| --------------------------------------------------------------------- | ----------------------------------- |
| `dev_tag` recomputed at graph construction                            | `resource/impls/image.rs:41`        |
| `provision` discards `build_image`'s returned reference, same comment | `resource/impls/image.rs:80`        |
| `dev_image_refs` recomputes via `dev_tag`                             | `resource/entry.rs:206`             |
| `image_node_id` re-derives; `if let Ok(id)` swallows                  | `resource/entry.rs:170,199`         |
| two further re-derivations                                            | `cli/run.rs:1958,2004,2009`         |
| `bundle.rs` packer (+1 line since `e96afec`)                          | `backends/image/bundle.rs`          |
| `tag_suffix` / `dev_tag` / `fold_suffix`                              | `backends/image/mod.rs:160,600,692` |
| `image_error` discards `waiting.message`                              | `pod_status.rs:85`                  |
| `ztest sync --out` defaults CWD-relative                              | `cli/sync/perf.rs:471`              |

So §§1–5, 7–10 stand as written.

**Deleted, invalidating parts of the earlier draft:** `backends/image/openshift.rs`
(574 lines), `backends/image/kind.rs` (206), and `docker.rs`'s in-process OCI
probe layer (−345). `from_env` now returns a single `Docker` provider for every
topology. §6 is rewritten above; the `Ref`-parser work item is void.

**In flight, and not to be built on:**

- `ImageBackend` still has `Kind`/`Registry`/`OpenShift` variants and
  `builds_on_cluster()` still branches on `is_openshift()` — a platform-identity
  branch the collapse rule says must die, and which `design-platform-collapse.md`
  schedules for deletion. This design must not add a dependency on it.
- `capability::probe_builder` looks for a BuildKit **StatefulSet**;
  `buildkit.rs` still creates an **ephemeral pod**. The intended end state is a
  builder ztest probes rather than provisions.
- `dev!` images build via local `docker build`/`docker push` while the runner
  builds via on-cluster `buildctl` — **two build engines with different cache
  semantics** in one run. The zeroed-mtime/incremental-context-sync property
  §2 relies on is a property of BuildKit's context sync, and does not transfer to
  a local `docker build`. Capturing the digest at the `ImageProvider` seam (§6)
  is what keeps this design independent of which engine wins.
- Stale module docs now assert things that are false: `docker.rs`'s header still
  describes `super::openshift`, `openshift_manifest_present`, and
  `openshift_manifest_digest`; `pull_secret_env` still refers to `kind::Kind`.
  Worth correcting when this work touches those files — an earlier draft of this
  document cited one of them as evidence of an anticipated seam, and that
  evidence was void.

## Rejected alternatives

| option                                | why not                                                                                                                                                                                                                                 |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Compute the hash more carefully       | No hashing rigor survives a `git rebase` landing mid-build. Any design requiring a quiescent worktree fails during normal work.                                                                                                         |
| Snapshot only, keep the predicted tag | Narrows the window without closing it. The tag stays a *claim about* the build rather than a *fact from* it, and any future drift between the key and the build side silently reopens it.                                               |
| Digest only, drop the content key     | An image digest is not a stable function of inputs — layer `Created` timestamps come from wall-clock build time, compression is nondeterministic, and rootless workers produce different images. It is a reference, never a dedupe key. |
| Digest-only pod references            | Would make our images untagged manifests, which is the one thing the pruner reaps.                                                                                                                                                      |
| `imagePullPolicy: Always`             | Buys nothing with a correct digest, and makes 123 pods each hit a registry behind a kubelet that serializes pulls by default.                                                                                                           |
| Nix flakes' tracked-only rule         | Silently omits untracked files. Most-reported papercut in this space.                                                                                                                                                                   |
| A third-party `.dockerignore` crate   | The only Rust option is 55 lines, v0.1.0, from an abandoned repo — worse than the current globset code.                                                                                                                                 |
| `.ztestignore`                        | Explicitly out of bounds: another file for developers to maintain.                                                                                                                                                                      |

## Status

Landed on `feat/platform-collapse`; 667 lib tests pass, clippy clean.

| §   | Change                                                                                                                                                      | Where                                                                           |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| 1–3 | Git-tree snapshot, `ls-tree`-equivalent enumeration via `git archive`, `.dockerignore` as a filter over it, filesystem fallback                             | `backends/image/bundle.rs`                                                      |
| 4–5 | `Resolved` + `resolve_dev_image`: one pack per image per run; `FROM` digests pinned into the staged Dockerfile, so the base image is in the content address | `backends/image/mod.rs`                                                         |
| 6   | `image_built` returns the observed reference; `build_image` returns the digest read back after push. Both are `<pull>/<repo>:dev-<hash>@sha256:…`           | `backends/image/{mod,docker}.rs`                                                |
| 7   | `ImagePlan` owns resolution and the manifest; nodes record only on success; `dev_tag`, `image_node_id`, `dev_image_refs` and `pod_reference` deleted        | `resource/entry.rs`, `resource/impls/image.rs`, `cli/run.rs`, `cli/sync/mod.rs` |
| 9   | `ztest sync --out` defaults to ztest's cache dir, not the CWD                                                                                               | `cli/sync/perf.rs`                                                              |
| 10  | `image_error` carries the kubelet's message                                                                                                                 | `pod_status.rs`                                                                 |

Fixed on the way through: the `Docker` provider had one registry address for
both push and pull. Now that the pod reference comes from the backend rather
than from a separate `pod_reference()` join, that would have published the
*push* address to pods — wrong wherever the two differ (the in-cluster registry
reached by route from the builder and by service from a pod). `Docker` now
carries both, builds and probes against `push`, and hands pods `pull` plus the
digest observed at push. The digest is the same content either way.

### Review pass

Five defects found auditing the landed code, all fixed:

- **The base digest was the wrong digest.** `docker manifest inspect -v` returns
  one entry *per platform* (12 for `rust:1.90-bookworm`, including attestation
  entries) and no index digest at all, so taking the first entry pinned whichever
  platform the registry happened to list first. Base pinning now uses `docker
  buildx imagetools inspect --format '{{.Manifest.Digest}}'`, which reports the
  index digest — the one a pull of that tag resolves to. `manifest_digest` stays
  for probing images ztest itself pushed, which are single-manifest.
- **Every run paid for the pin.** Resolution is a network round trip per `FROM`
  per image (measured: 1.5s warm DNS, 9.3s cold), on the critical path of every
  run, and a hard failure offline or against a rate-limited Docker Hub. Digests
  are now cached under `~/.cache/ztest/base-digest` with a 12h TTL, published by
  rename. Measured: 1.49s cold, 57µs warm. The staleness bound is far tighter
  than the unpinned base it replaced.
- **The rewrite dropped `FROM` flags.** The line was rebuilt as `FROM <ref>@<digest>
  [AS <stage>]`, discarding `--platform` and anything else on it. Only the image
  token is substituted now. No Dockerfile in the tree uses such a flag today, so
  this was latent.
- **A symlink packed differently inside a worktree.** `git archive` emits links
  verbatim, and the reader treated any non-directory entry as a file — so a link
  became a *regular file whose contents are the target's path*, while the
  filesystem walk dereferenced it. Links are now resolved within the snapshot and
  dropped if they escape it, matching the walk. The repo's one tracked symlink
  (`result`) is `.dockerignore`d, so this too was latent.
- **Two concurrency hazards.** The scratch index was keyed only by worktree, so
  two concurrent runs in one checkout collided on `<index>.lock` and one failed
  outright; and the staging directory was named after the tag, so two runs
  building the same tag shared it and either one's `Drop` deleted the other's
  context mid-build. Both are now per-process.

### Not done, and why

**The runner image keeps its per-run `dev-<run-id>` tag (§8).** Its inputs are
the shipped source tar, the embedded `RUNNER_DOCKERFILE`, the base image, *and
the nextest args baked into the build* — which change with the test filter on
almost every invocation. A key folding only the first three would be a false
*hit*: two runs with different filters would share a tag. That is a worse
failure than the one this document fixes, because it is silent. Doing it
correctly means threading a digest out of `ship_source` and folding the args,
and it should be its own change.

**No blob-level verification (§10).** The manifest digest is now observed rather
than predicted, which closes the failure we actually hit. Layer-blob checking
addresses a different failure (registry GC racing a push) that has not been seen
on this cluster, where the pruner has deleted nothing in three consecutive
nightly runs.

## Sequencing

**Phase 0 — attribution (independent, permanent, land first).**
Propagate `waiting.message`; preflight-verify each unique image before
scheduling. Turns any recurrence — including ones this design does not
anticipate — into a named failure. No stopgap is proposed: the pre-manifest
re-check considered earlier is subsumed by Phase 2 and would be temporary code on
a real path.

**Phase 1 — threading.** Surface `build_image`'s reference through node state;
`dev_image_refs` reads it; remove the re-derivations; surface the swallowed
error. This alone eliminates every observed failure, and is independent of how
the key is computed.

**Phase 2 — snapshot and enumeration.** Git tree snapshot, `ls-tree`/`cat-file`
enumeration, `.dockerignore` as a filter, schema version, filesystem fallback.
One-time rebuild of every dev image as keys change.

**Phase 3 — reference form.** Digest capture at the `ImageProvider` seam — cold
path from the build engine's metadata, warm path from the manifest-inspect output
we already produce — and `repo:dev-<hash>@sha256:…` in every pod spec. Engine-
agnostic by construction (§6), so it does not block on the build-engine question
in §12.

**Phase 4 — base image digests.** Dockerfile `FROM` resolution and rewriting,
declared-ARG folding. Second one-time rebuild.

**Phase 5 — runner image and hygiene.** Runner onto the shared scheme; `ztest sync --out` default moves out of the CWD.

Phases 1 and 2 are the correctness core. 3 and 4 are what make it stay correct.

## Testing

- **Key stability matrix** — the nine worktree states from §1, asserted as
  properties, including the negative case (modified gitignored file ⇒ unchanged
  key) and the staged-vs-staged-plus-unstaged distinction.
- **Atomicity** — mutate the worktree between snapshot and manifest and assert
  the key does not move. This is the regression test for the reported bug.
- **Structural** — assert the manifest reference cannot be produced without a
  successful provision or probe (a failed node must yield no reference).
- **Large payloads** — a context directory holding an untracked multi-GB archive
  packs neither it nor an entry for it.
- **Reference forms** — `repo:dev-<hash>@sha256:…` round-trips through the pod
  spec, and a reference is never produced without a successful build or probe.
- **Fallback** — a non-git context still builds, under a distinct key schema.
- **Base image drift** — a changed base digest changes the key.

## Open items

- Whether the pruner credits the tag portion of a `tag@digest` pod reference as
  an ImageStreamTag reference or only the digest as an ImageStreamImage
  reference. Does not affect us while every push creates an istag, but it is the
  assumption behind the retention note in §6.
- `name:tag@digest` acceptance is containerd/CRI-O reference-parsing behaviour;
  Kubernetes documents `name@digest`. Widely relied on, not a documented
  contract. Set `imagePullPolicy` explicitly rather than depending on the
  digest-implies-`IfNotPresent` default.
- Unbounded istag growth (~40 per stream and rising) is ours to own precisely
  because unique tags never rotate out of tag history and the pruner will never
  reclaim them. A label-scoped reaper is a follow-up, not part of this work.
- Component repos other than zaino may have `.gitattributes` `text`/`eol`
  normalization, which would make tree content differ from worktree bytes.
  zaino has none. Check per repo as they are onboarded.
