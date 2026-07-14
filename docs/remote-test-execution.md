# Remote test execution — hermetic pod-per-test on remote clusters

## The goal, restated

On a remote cluster, a `ztest` test should run **inside a sibling pod**, not as a
local child process, so that:

1. **Compute leaves the laptop** — chain sync, trial-decryption, transaction
   building, and Sapling/Orchard proving all run in-cluster (the original
   motivator).
2. **Isolation is hermetic** — a running test cannot be poisoned by the laptop's
   ambient environment, by sibling tests, or by shared state. Its inputs are only
   the components it declares in its own per-test namespace.
3. **Fidelity is preserved** — the wallet stays the **real in-process library**
   (`librustzcash` / `zingo`), exercised exactly as a real consumer uses it. No
   RPC facade.

This supersedes the earlier "RPC wallet daemon" idea. That design fractured a test
across the laptop/cluster boundary (assertions stayed local and poisonable) and
replaced the real wallet with an ztest-invented gRPC facade that mirrors no real
wallet UX. Running the whole test in a pod gives all three properties above at
once; the wallet needs no special handling because it simply runs wherever the
test runs.

## Why this fits the existing machinery

- **The executor is one seam.** `engine/exec.rs::spawn_test` spawns a local OS
  process per test (`WorkItem { binary_path, cwd, test_name }`, `engine/plan.rs`).
  A remote executor slots in beside it.
- **In-cluster networking already exists.** `env.rs`'s `in_cluster` branch of
  `resolve_port` returns a **direct pod-IP** (no port-forward). A test running in
  a pod reaches the validator/indexer pods it creates natively.
- **Per-test isolation already exists.** Per-test namespace, QoS lease,
  label-selector teardown, shadow VSCs (`docs/resource-graph-design.md`). The
  runner pod just joins that same namespace, closing the last gap (the test
  process itself).
- **A content-addressed delivery primitive already exists.** `Mount::archive()`
  → `ubuntu:24.04` uploader pod streams a tar into a `seed-<hash>` PVC → CSI
  snapshot → **CoW-clone per test** (`materialize.rs`, `seeds.rs`). Ceph RBD CoW
  clones are near-instant.
- **The dylib search path is already computed.** `engine/dylib.rs` reproduces
  nextest's `LD_LIBRARY_PATH` (deps dirs + base output dirs + rustc host/target
  libdirs). Reused and remapped to pod mount paths.

## The storage reality (constraint, not choice)

All cluster storage is **RWO Ceph RBD block** (`rook-ceph-block`) plus CSI
VolumeSnapshots. There is **no RWX / NFS / CephFS** anywhere (`mounts.rs` hardcodes
`ReadWriteOnce`; kind uses the hostpath CSI driver). So a *live* "laptop writes,
pod reads" network mount is not available. The efficient equivalent that fits this
substrate is the existing **archive → snapshot → CoW-clone-per-test** path:
materialize once, clone per test in milliseconds.

## Architecture

### 1. Executor seam (`engine/`)

Add a `PodExecutor` alongside the local `spawn_test`, selected by cluster profile:

- **local / kind →** local child process (today's fast edit-run loop).
- **remote cluster →** pod-per-test.

Per `WorkItem`, the pod executor:
- creates a **runner pod in the test's own per-test namespace**,
- mounts the CoW-cloned build artifact read-only (see delivery),
- runs `<mounted binary> --exact <test_name> --nocapture`,
- sets `LD_LIBRARY_PATH` (from `dylib.rs`, remapped to mount paths) plus the
  ztest env (`NEXTEST_*`, in-cluster SA token, `ZTEST_ENGINE=1` — marks the child
  as orchestrated so `TestEnv::build` proceeds; the parent's scheduler owns
  capacity admission),
- streams pod logs into the existing reporter (replacing the local-child stdout
  read),
- maps the pod's exit code → test result,
- relies on the existing label-selector teardown for cleanup (the pod is in the
  reaped namespace).

The runner pod's ServiceAccount needs RBAC to create its sibling component pods
(the `crc-remote` run-only SA / ARC model already exists).

**Image-pull errors are transient by default.** Launching a run's worth of test
pods at once has them all pull the *same* runner image simultaneously; on a
single node the kubelet throttles concurrent registry pulls (`registryPullQPS`),
rejecting the excess with `pull QPS exceeded` → `ErrImagePull`. This self-heals —
the kubelet retries with backoff, and once the first pod warms the node's cache
`imagePullPolicy: IfNotPresent` stops the rest from pulling — so the executor
treats `ErrImagePull`/`ImagePullBackOff` as terminal only after a grace window
(`IMAGE_PULL_GRACE`), not on first sight. `InvalidImageName` (never resolvable)
stays immediately terminal.

### 2. Build-artifact delivery — nix-built layered image

The runtime closure is nontrivial and **nix-pinned**: `flake.nix:92-94,109-112`
shows the test binaries dynamically link `libstdc++`/`libgcc_s` (via rocksdb's
C++) and the dev shell hand-sets `LD_LIBRARY_PATH` to the nix `rocksdb_8_11` and
`stdenv.cc.cc.lib` store paths, plus the rustc/std libdirs. Reproducing that on a
hand-rolled ubuntu image is the parity headache; nix removes it **by
construction**.

**Mechanism:** a flake output builds the runner image with
`pkgs.dockerTools.streamLayeredImage` from the *same* `flake.lock` as the dev
shell, so the image's glibc / `libstdc++` / `rocksdb_8_11` / rustc-std are the
identical store paths the `cargo` build linked against. `streamLayeredImage`
auto-splits the closure into content-addressed layers, so the large stable base
(toolchain + C/C++ runtime + third-party closure) is pushed **once** and cached in
the registry; only the small changed layer moves per edit. The pod's env bakes the
same `LD_LIBRARY_PATH` the dev shell computes.

The k8s nodes need **no nix** — they pull a standard OCI image. Delivery reuses the
existing image-dist registry push/pull path (`backends/image.rs`); no
archive/PVC/CoW machinery is needed for the binaries.

**Test binaries — two options for what rides on top of the nix base:**

- *(baseline, hybrid)* keep the `cargo` build in the dev shell (as today) and add
  the compiled outputs as thin top layer(s) over the nix base — order the copy so a
  rarely-changing third-party-`deps/` layer stays cached and only a small
  workspace-outputs + test-binaries layer re-pushes per edit. Their `LD_LIBRARY_PATH`
  resolves against the nix store paths present in the base image. Least build-system
  change.
- *(later, full reproducibility)* build the test binaries themselves in nix (crane
  / `buildRustPackage`) and put them straight in the image. Fully hermetic, but
  moves the Rust build into nix and must vendor the git deps (zingolib /
  librustzcash revs, with `cargoLock.outputHashes`) — real work, deferred.

Rejected: *nix on the remote nodes* (running nix or restoring a store closure
pod-side). Possible via a trusted binary cache, but it needs cluster-side nix infra
and buys nothing over shipping a plain OCI image.

### 3. In-pod test → cluster

Unchanged test code. In-cluster the ztest library resolves its components by
pod-IP (already supported). The test's wallet is the real in-process library,
running in the pod.

**Component image references.** A `dev!` component image (zainod, zebrad) is
normally resolved by hashing its Dockerfile + context to the content-addressed
`<repo>:dev-<hash>` tag. The baked runner image carries **no** source tree, so an
in-pod test cannot recompute that hash — and `Distribution::from_env` is unset
in-pod, so it couldn't qualify the pull address either. Instead the laptop
preflight (which built and pushed every component image) serializes a
`spec_key → pull reference` map into each runner pod as `ZTEST_IMAGE_REFS`, and
`image::resolve` returns the pre-resolved reference before touching the source.
`spec_key` is a file-free hash over repo/features/toolchain/source-origin, so the
laptop and the pod derive the same key from the same `dev!` declaration.

### 4. The wallet

No RPC, no daemon, no facade. The real `librustzcash` / `zingo` backend runs
in-process inside the runner pod — faithful UX, and hermetically isolated (its own
`TempDir` in its own pod).

## Linking: glibc-dynamic only, via nix

`zingo` (C++/`libstdc++`) is required on remote, so the **glibc-dynamic** path is
mandatory. Independently, `flake.nix:92-94` shows *every* test binary already links
`libstdc++`/`libgcc_s` (via rocksdb's C++) at runtime — so **static musl is a
non-starter even for the default backend**, not merely deferred. Drop it.

Parity is handled by the nix-built image (§2): the runtime closure is the same
store paths at build and run time, so there is no glibc/`libstdc++`/rocksdb version
matching to get wrong.

## Open spikes / risks

- **nix image build** — a `streamLayeredImage` flake output whose closure equals
  the dev shell's, that a cargo-built test binary actually runs from. Confirms
  parity and the layer split. This replaces the old glibc-parity spike.
- **Volatile-layer push time** over the remote mesh (Nebula/Tailscale) — layering
  keeps it to workspace outputs, but measure on a real edit.
- **Log streaming + exit-code fidelity** through the unified-console reporter,
  which currently owns the terminal and reads a local child's stdout.
- **Per-test pod scheduling latency** vs. a local fork — mitigable later with a
  warm runner pool.
- **RBAC** for the runner SA to create sibling component pods.

## Phasing / status

1. **Nix runner image** — ✅ done. `packages.runner-image`
   (`dockerTools.streamLayeredImage`) whose closure matches the dev shell. Verified
   by running a real dev-shell-built nextest binary inside it (`docker run`); its
   `/nix/store/…/ld-linux` interpreter + `libstdc++`/`librocksdb` resolve.
2. **Executor seam + `PodExecutor`** — ✅ done. `Executor` trait + `LocalExecutor`
   (`engine/exec.rs`); `PodExecutor` (`engine/pod_exec.rs`) with the kube
   create/poll/logs/delete lifecycle, verdict mapping (incl. image-error
   fast-fail), and `ZTEST_RUNNER_IMAGE`-gated selection (`engine/mod.rs`
   `select_executor`). Proven green on `kind`: a real test ran in a pod via
   hostPath and exited 0.
3. **Delivery** — ✅ done and verified green on crc. Modes: `hostpath` (kind) and
   `baked` (remote). Preflight builds the runner image (`prepare_runner_image`),
   injects it into the image graph, resolves its pull tag, and auto-selects
   `PodExecutor(baked, sa/ns=ztest)` when the distribution is remote. Two issues
   the crc run surfaced and fixed:
   - **buildx can't see local images.** The OpenShift `Internal` distribution
     builds in an isolated `docker-container` buildx driver, so `FROM
     ztest-runner:dev` (a local `docker load`) failed. Fix: skopeo the base to a
     local OCI layout and pin `FROM` via buildx `--build-context` — no registry
     pull, no auth, no self-signed-CA TLS.
   - **Image was multi-GB.** Staging copied all `deps/*.so` (~3 GB of build-time
     proc-macro cdylibs) + unstripped debug binaries. Fix: stage only the
     `ldd`-NEEDED runtime `.so` closure and `strip --strip-debug` — `api_surface`
     3.4 GB → 34 MB. Base trimmed 115 MB → 103 MB (busybox instead of
     bash+coreutils).
   Result: `ztest run --cluster crc endpoint_url_format` → `PASS [9.064s]` in a
   pod, exit 0, no leaks. Finer per-crate layering (crane) is a later optimization.
4. **Harden** — teardown ✅ (runner pods carry `qos::LABEL_RUN_ID`, so the existing
   `reap_run` Ctrl-C path deletes them by construction); logs captured into the
   outcome. *Remaining:* RBAC (runner SA + Role/RoleBinding in `ztest setup` so
   component-spawning tests can create sibling pods), live log streaming during the
   run, warm runner-pod pool.
5. (Optional, later) full-nix test-binary build via crane for end-to-end
   reproducibility.
