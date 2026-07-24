# Remote execution — pod-per-test, on-cluster compile, on-cluster image build

On a remote cluster a `ztest` test runs inside a sibling pod rather than as a
local child process; the cluster compiles the test binaries and builds every
image.

## Executor seam

`Executor` has two implementations, selected by cluster profile
(`engine/mod.rs::select_executor`):

| Executor | Target | Mechanism |
| --- | --- | --- |
| `LocalExecutor` (`engine/exec.rs`) | local / kind | local child process per test |
| `PodExecutor` (`engine/pod_exec.rs`) | remote cluster | runner pod per test |

`PodExecutor` is auto-selected (`ZTEST_RUNNER_IMAGE`-gated) when the
distribution is remote. Per `WorkItem` it:

- creates a runner pod in the test's own per-test namespace (see
  [design-architecture.md](design-architecture.md) for namespace lifecycle and
  teardown),
- mounts the test binary (`baked` remote image, or `hostpath` on kind),
- runs `<binary> --exact <test_name> --nocapture`,
- sets `LD_LIBRARY_PATH` (from `engine/dylib.rs`, remapped to pod mount paths)
  plus the ztest env: `NEXTEST_*`, in-cluster SA token, and `ZTEST_ENGINE=1`
  (marks the child orchestrated so `TestEnv::build` proceeds; the parent
  scheduler owns capacity admission),
- streams pod logs into the reporter, maps pod exit code → test result,
- relies on `qos::LABEL_RUN_ID` on the pod for label-selector teardown
  (see [design-resources.md](design-resources.md)).

The runner pod's ServiceAccount has RBAC to create its sibling component pods.

**In-cluster networking.** `env.rs`'s `in_cluster` branch of `resolve_port`
returns a direct pod-IP (no port-forward); a test reaches the validator/indexer
pods it creates natively.

**The wallet is the real in-process library** (`librustzcash` / `zingo`),
running in the runner pod with its own `TempDir`. No RPC, no daemon, no facade.

### ErrImagePull grace window

Launching a run's worth of test pods together has them pull the same runner
image at once; a single node's kubelet throttles concurrent pulls
(`registryPullQPS`) and rejects the excess as `ErrImagePull`. This self-heals —
the kubelet retries with backoff, and once the first pod warms the node cache
`imagePullPolicy: IfNotPresent` stops the rest. The executor therefore treats
`ErrImagePull`/`ImagePullBackOff` as terminal only after `IMAGE_PULL_GRACE`.
`InvalidImageName` is immediately terminal.

### Component image references

A `dev!` component image is normally resolved by hashing its Dockerfile +
context to `<repo>:dev-<hash>`. The baked runner image carries no source tree,
so an in-pod test cannot recompute that hash, and `Distribution::from_env` is
unset in-pod. Instead the laptop preflight (which built and pushed every
component image) serializes a `spec_key → pull reference` map into each runner
pod as `ZTEST_IMAGE_REFS`, and `image::resolve` returns the pre-resolved
reference before touching the source. `spec_key` is a file-free hash over
repo/features/toolchain/source-origin, so laptop and pod derive the same key
from the same `dev!` declaration. (Image distribution env vars:
[ops-clusters.md](ops-clusters.md).)

## Storage constraint

All cluster storage is RWO Ceph RBD block (`rook-ceph-block`) plus CSI
VolumeSnapshots; there is no RWX/NFS/CephFS (`mounts.rs` hardcodes
`ReadWriteOnce`; kind uses the hostpath CSI driver). A live "laptop writes, pod
reads" mount is unavailable. Seed data is delivered via
archive → snapshot → CoW-clone-per-test: materialize once, clone per test in
milliseconds. See [design-architecture.md](design-architecture.md).

## Linking: glibc-dynamic only

`zingo` links `libstdc++` at runtime, so static musl is a non-starter; the
glibc-dynamic path is mandatory. The compile stage (`rust:1.95.0-bookworm`) and
the runtime stage (`debian:bookworm-slim`) both pin Debian `bookworm`, so a
binary compiled in the toolchain image links the identical glibc the runtime
image ships — no version matching to get wrong.

## On-cluster compilation

On an OpenShift target the laptop ships **source** and the cluster produces the
binaries; no compile runs on the laptop and no runner image is pushed per edit.
The whole compile + assemble is one multi-stage `buildctl` build of
`docker/runner.Dockerfile` in the ephemeral BuildKit pod `ztest run` creates for
the run (see [the image-build section](#openshift-on-cluster-image-build-canonical)).

**The drive** (`pipeline/remote_compile.rs`, selected by
`image::builds_on_cluster()`):

1. **Ship source.** `cargo metadata` finds the workspace root and every local
   (path) package; the git repos backing them are enumerated with `git ls-files`
   (so each repo's `.gitignore` prunes `target/` and VCS metadata, no
   hand-maintained exclude list) and streamed as a local `tar` into the pod's
   `tar -x` under `/build/ctx`, at their ancestor-relative paths. `oc rsync` is
   avoided: the BuildKit image ships no `rsync`, and its tar fallback walks the
   excluded `target/` trees instead of pruning them.
2. **Build + push the runner image.** A `buildctl build --opt target=runner`
   compiles the selected binaries (`cargo nextest run --no-run`, `compile`
   stage), assembles the runtime image (`runner` stage), and pushes it — one
   build, authenticated with the pod SA token. Cargo's registry/git and target
   dirs are `--mount=type=cache` mounts persisted in the BuildKit content store
   on the cache PVC, so recompiles are incremental across runs. The tag is
   content-addressed on the run id.
3. **Export the inventory.** A second `buildctl build --opt
   target=inventory-export` (`FROM scratch`, carrying only `/out`) reuses the
   first build's layer/mount cache — so the compile does not re-run — and emits
   `list.json` (`cargo nextest list --json`) plus a framed `inventory.jsonl`
   (each binary run under `ZTEST_DUMP_INVENTORY=1`). `--output type=local`
   exports just those two files, `oc cp`'d back and parsed by the same
   `build::parse_list_summary` / `images::parse_inventory` + `images::assemble`
   the local path uses.

Component `dev!` images and data seeds still provision through the resource
graph (built on-cluster too); only the runner image comes from this build.
`ztest run` streams BuildKit's live progress and per-phase notes into scrollback.

> **exec transport gotcha.** On the non-PTY path stdout and stderr are
> multiplexed over one websocket, so `exec_streamed` must drain both
> concurrently. Reading stdout to EOF then stderr deadlocks once a chatty compile
> fills the stderr channel: stdout can't reach EOF until the stderr-blocked
> stream closes.

## OpenShift on-cluster image build (canonical)

For a profile with distinct `push`/`pull` addresses (OpenShift), ztest builds
every image on the cluster in an ephemeral, ztest-owned privileged-in-userns
BuildKit pod (`ztest-buildkit`), not through OpenShift's Build subsystem. This
covers `dev!` component images and the runner image alike.

Per image, during preflight:

1. **Pack the context.** `bundle::pack` walks the build context once into a
   deterministic, `.dockerignore`-aware tar with the chosen Dockerfile at the
   root — the same bytes the `dev-<hash>` tag is content-addressed on.
2. **Build in the pod.** The tar is `oc cp`'d into the BuildKit pod and built by
   `oc exec`ing `buildctl build` against the in-pod `buildkitd`. On a PTY
   (`oc exec -t`) `buildctl` renders its own collapsing progress UI live into
   the console.
3. **Push over the in-cluster service.** `buildctl` pushes to the `pull` address
   via `--output type=image,push=true`, authenticating with a docker
   `config.json` written in-pod from the SA token. The registry's
   service-ca-signed serving cert is verified via the auto-injected
   `openshift-service-ca.crt` bundle, which the pod entrypoint folds into the
   container's system trust before starting `buildkitd` (the push's OAuth token
   fetch honours only system roots, not `buildkitd.toml`'s per-registry
   `ca`/`insecure`). The first push auto-creates the imagestream.
4. **Pods pull via the service.** Pod specs reference the `pull` address
   (`image-registry.openshift-image-registry.svc:5000/…`); the kubelet pulls
   in-cluster using the pod SA's auto-injected registry credentials — no pull
   secret, no route cert on nodes. The laptop probes image presence via the
   `push` route (same registry storage).

### Security posture

`buildkitd` runs as in-pod-root (uid 0) with `privileged: true`, confined
inside a Kubernetes pod user namespace (`hostUsers: false`) under ztest's own
`ztest-buildkit` SCC. The userns maps that root to a kubelet-assigned
unprivileged host uid, so `privileged` grants no authority over the host. It is
required: on OKD/CRI-O each `RUN` step's runc container needs `CAP_SYS_ADMIN` in
the userns owning its mount ns and an unmasked `/proc`; the kernel's
`mount_capable` gate plus the API's `procMount: Unmasked` rule (permitted only
under `hostUsers: false`) make the pod userns the only working path. This buys
overlayfs layer caching and DAG-parallel builds.

### Cluster-side prerequisites

`ztest setup --target okd` (run once, with an admin kubeconfig — needed to
create the SCC) provisions everything; there are no cluster operators to
install.

- **BuildKit build server** (`resource::impls::buildkit`, `NodeId::Buildkit`):
  the `ztest-buildkit` Deployment running `buildkitd`
  (`moby/buildkit:v0.18.2`, `--oci-worker-snapshotter=overlayfs`) as in-pod-root
  under `hostUsers: false` + `privileged: true`; its `ztest-buildkit`
  ServiceAccount; a `buildkitd.toml` ConfigMap; and a cache PVC at BuildKit's
  state dir (content store + overlayfs snapshots, persisting the layer cache
  across builds). Context is staged in a per-build `emptyDir`. `buildkitd.toml`
  routes `docker.io` through the `mirror.gcr.io` pull-through cache (BuildKit's
  content store is separate from CRI-O's, so cold base `FROM` pulls would
  otherwise re-resolve against Docker Hub and hit its per-IP anonymous rate
  limit; the resolver tries the mirror first and keeps `registry-1.docker.io`
  as automatic fallback), and marks the integrated registry insecure
  (self-signed TLS).
- **Custom SCC `ztest-buildkit`**: OKD's built-in `nested-container` SCC
  (`SETUID`/`SETGID`, `seccompProfiles ['*']`, SELinux `container_engine_t`,
  `userNamespaceLevel: RequirePodLevel`) plus `allowPrivilegedContainer`. Its
  `runAsUser` range is pinned to `0-65534` so in-pod-root (uid 0) admits without
  patching the namespace's billion-based uid-range annotation.
- **Registry push authz**: the `ztest-image-push` role on `ztest-images`
  (`policy::IMAGES_NAMESPACE`), bound to the run SA `ztest/ztest` **and**
  `ztest/ztest-buildkit`. It grants `imagestreams: create` plus
  `imagestreams/layers: get,update` — plain `system:image-pusher` lacks
  imagestream **create**, so the first push of a never-seen image is denied.
- **Pull authz**: `system:image-puller` on `ztest-images` for
  `system:serviceaccounts`, so every pod SA can pull (why no pull secret is
  needed).
- **Per-test pod SCC grant.**

The run SA's cluster read permissions (`nodes` for the QoS probe,
`volumesnapshotclasses`/`storageclasses` for seeding) come from the same
`ztest-remote` ClusterRole, sourced from `policy::RUN_RULES`, which also drives
a run-start `SelfSubjectAccessReview` self-check: a stale grant makes
`ztest run` fail fast naming the missing permission. The build path needs no
`build.openshift.io` grants — it only `exec`s into the BuildKit pod
(`pods/exec`).

### Runtime images

The runner build pulls its stage base images directly from upstream — the
compile stage `FROM rust:1.95.0-bookworm`, the runtime stage
`FROM debian:bookworm-slim` — so there are no ztest-built base images to
provision at `ztest setup`. The runtime closure is just glibc + CA roots: the
workspace links no rocksdb and no OpenSSL (rustls everywhere), only
statically-linked C (ring, aws-lc-sys, zstd-sys). See
`docker/runner.Dockerfile`.

No fallback: the cluster profile names the backend, and if the on-cluster build
fails the run fails — it never degrades to another build path.

See [ops-openshift-setup.md](ops-openshift-setup.md) for cluster bring-up and
[ops-clusters.md](ops-clusters.md) for the `ztest cluster` profile model.
