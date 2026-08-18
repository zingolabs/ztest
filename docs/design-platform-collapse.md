# Platform collapse: ztest as a Kubernetes client

ztest is today two products welded together: a **test harness** and a **cluster
installer**. Every `--target` branch, every embedded CSI manifest, every SCC
belongs to the installer half. This document plans its removal, and the
replacement of the image and observability planes with upstream-supported
mechanisms.

## The rule

**ztest asserts capabilities; operators provide them.**

Never branch on *"is this OpenShift"*. Ask *"does this cluster have a
snapshot-capable StorageClass / the `PodMonitor` CRD / a reachable builder"*.

The pattern already exists in two places, and both are the good kind:

- `metrics.rs` — without Prometheus-operator CRDs, emitting a `PodMonitor` is
  meaningless and must not be done.
- `storage.rs` — `discover()` + `snapshot_capable(&classes, provisioners)`, with
  unit tests.

Both survive. It is the *installers* around them that go. If a platform-identity
branch ever reappears, the collapse has failed.

## Findings that drive the design

| # | Finding | Consequence |
|---|---|---|
| 1 | Upstream ships a **rootless BuildKit k8s manifest** with no `privileged`, no capabilities, no `hostUsers`, no `procMount` — using `--oci-worker-no-process-sandbox` | the custom SCC + privileged-in-userns posture is avoidable |
| 2 | BuildKit **cache mounts are builder-local and deliberately not exported** by any cache backend | an ephemeral builder throws away the cargo cache every run |
| 3 | k3s has an **embedded registry mirror (Spegel)** behind one flag | `mirror.rs` (ITMS + MCO node reboot) is replaced by config |
| 4 | **zot** is OCI-native, has an official Helm chart, and its sync extension does on-demand pull-through caching | one component is both push target and Docker Hub cache |
| 5 | **Kaniko was archived** (June 2025, read-only) | BuildKit is the mainstream answer, not a legacy choice |
| 6 | `buildctl --addr kube-pod://` dials via `kubectl exec` | no Service, no mTLS, no port-forward |
| 7 | Pyroscope ingests pushed pprof and serves merged pprof back | the artifact PVC + collector pod plane disappears |

### On finding 1

`buildkit.rs` documents the fork and takes the branch upstream does not:

> No `--oci-worker-no-process-sandbox`: the process sandbox is what mounts the
> [/proc for RUN steps]

The upstream rootless doc reaches the same dead-end and answers it differently:

> Kubernetes lacks the equivalent of `systempaths=unconfined`.
> (`securityContext.procMount=Unmasked` is similar, but different in the sense
> that it depends on `hostUsers: false`)

The trade is real and bounded: `--oci-worker-no-process-sandbox` lets build steps
kill or ptrace other processes **inside the BuildKit container**. `privileged` is
a *host* boundary; this is an *intra-pod* boundary, in a single-tenant pod ztest
owns. Cluster-side cost drops to one namespace label
(`pod-security.kubernetes.io/enforce: privileged`, needed because
`seccompProfile: Unconfined` exceeds PSA baseline) instead of a custom SCC.

#### Measured: what admission actually says (CRC, k8s v1.35)

Server-side dry-run of the upstream rootless pod, impersonating the ztest run
ServiceAccount, is **rejected**:

```
restricted-v2: .containers[0].runAsUser: Invalid value: 1000: must be in the
               ranges: [1000680000, 1000689999]
               seccomp: unconfined is not an allowed seccomp profile.
               Valid values are [runtime/default]
restricted-v3: .spec.hostUsers: Invalid value: null: Host Users must be set to false
```

As cluster-admin the same pod is admitted — by the `privileged` SCC, which is
not available to a workload SA. So on OpenShift the upstream recipe **still
needs a custom SCC**; the simplification is that it becomes a much smaller one:

| | today | upstream rootless |
|---|---|---|
| `allowPrivilegedContainer` | yes | **no** |
| `hostUsers: false` | yes | **no** |
| `procMount: Unmasked` | yes | **no** |
| `runAsUser` | `MustRunAsRange 0–65534` | `MustRunAs 1000` |
| seccomp | `["*"]` | `Unconfined` only |

**The "one namespace label" outcome is k3s-only.** That is now a measured
argument for the platform decision rather than an inferred one.

With that SCC in place the posture was run for real on CRC: the daemon starts,
`buildctl debug workers` reports `process-mode: no-sandbox`, the auto snapshotter
selects `overlayfs` unaided, and a `RUN` step executes.

This **disproves** `buildkit.rs`'s standing claim that *"Rootless BuildKit
therefore cannot run `RUN` steps on OpenShift/CRI-O at all (verified on CRC)"*.
That held for the `procMount`/`systempaths` route; it does not hold for
`--oci-worker-no-process-sandbox`. The privileged posture is retired.

### On finding 2

Layer cache and cache-mount cache are different mechanisms. `--mount=type=cache`
— the cargo registry and `target/` dir, which is *the* cost in a Rust image build
— is scratch space keyed by location, not by cache key, and is intentionally
excluded from registry/inline cache exporters.

**Therefore the ephemeral per-run build pod is the wrong shape.** It discards the
only cache that matters. The builder must persist.

This also *fixes* a known bug: a build pod created after the capacity probe is
invisible to admission. A permanent builder is a static, knowable reservation
subtracted from `ClusterCapacity` once.

## Target architecture

### Cluster requirements (operator-provided)

Published as `docs/ops-cluster-requirements.md` plus a plain `deploy/` manifest
bundle applied with `kubectl` — cluster-admin work, documented as such, not
embedded in a test harness.

| Capability | Provided by |
|---|---|
| snapshot-capable StorageClass + VolumeSnapshot v1 | Rook-Ceph RBD (prod), any CSI with an external-snapshotter |
| `PodMonitor` CRD | prometheus-operator |
| OCI registry (push + pull) | zot in-cluster; ghcr.io for published images |
| BuildKit endpoint | rootless StatefulSet |
| Pyroscope | monolithic, single replica |

### Capability preflight

One code path. On connect ztest probes and reports:

```
cluster  k3s-prod (context: prod)
  ✓ snapshot-capable storage    ceph-rbd (rbd.csi.ceph.com)
  ✓ PodMonitor CRD              prometheus-operator
  ✓ builder                     buildkit-0 (ztest-meta)
  ✗ Pyroscope                   no Service in ztest-meta
      → profiling unavailable; see docs/ops-cluster-requirements.md
```

A missing capability yields a named cause and a doc pointer, never a silent
degrade. `ztest cluster setup` survives as *provision ztest's own namespaced resources*,
already driven by the same `resource::Graph` as `ztest run`.

### Image plane

Three concerns, three upstream mechanisms, no ztest-specific machinery:

**Build** — BuildKit as a **StatefulSet**, upstream rootless manifest verbatim
(`runAsUser: 1000`, `seccompProfile`/`appArmorProfile: Unconfined`,
`--oci-worker-no-process-sandbox`), with `/home/user/.local/share/buildkit` on a
PVC so cache mounts survive. Reached with `buildctl --addr
kube-pod://<pod>?namespace=&container=`, which needs only `pods/exec` — already
granted and already used by `exec_tar`.

**Store** — zot in-cluster as the push target for `dev-<hash>` images, with the
sync extension configured for on-demand pull-through of `docker.io`. Its online
GC and retention policies suit high-churn content-addressed tags; Distribution's
stop-the-world GC does not, and Harbor's Postgres + Redis + Trivy is
disproportionate. Published images continue to ghcr.io.

**Distribute** — k3s `embedded-registry: true` (Spegel) plus per-node
`registries.yaml` mirrors. Peer-to-peer node sharing and Docker Hub rate-limit
relief, with no MCO drain/reboot cycle.

`ImageBackend` collapses. `push` and `pull` remain as *config* (an in-cluster
registry has different addresses inside and out); `--local` is a preset that sets
them and nothing else.

The one surviving switch is *where a builder is*, resolved by probing for a
BuildKit endpoint — capability, not identity.

### Observability plane

**Metrics** — unchanged CRD (`PodMonitor`); only the consumer changes from
OpenShift UWM to prometheus-operator.

> **Superseded.** The in-process push described below was replaced by an out-of-process
> eBPF collector: no cargo feature, no `ZTEST_PROFILE_*` variables, no component contract.
> See [how-to-profile.md](how-to-profile.md) for what shipped.

**Profiles** — components push to Pyroscope. Contract collapses to one runtime
switch:

| Var | Fate |
|---|---|
| `profile` cargo feature | stays (linking is build-time) |
| `ZTEST_PROFILE`, `ZTEST_PROFILE_OUT`, `ZTEST_PROFILE_INTERVAL` | removed |
| `ZTEST_PROFILE_HZ` | stays |
| `ZTEST_PROFILE_URL`, `ZTEST_PROFILE_TAGS` | new — URL presence is the gate |

Mid-run profiling becomes the default rather than a feature, profiles outlive the
namespace, and a component killed by OOM no longer loses everything. `perf.rs`
keeps its reader half — viewer discovery and the `Segment` throughput table — and
swaps only its retrieval backend. Post-`go`-removal it applies no pprof transform
at all: what Pyroscope serves for the window is what lands on disk. `--window 11h..12h` keeps its elapsed-since-run-start framing
and stops being cadence-limited.

pprof-rs remains the sampler, so the C/C++ blind spot (RocksDB, LMDB) is
unchanged. Closing it needs eBPF and is out of scope here.

## Deletion inventory

Estimates from line counts and platform-token density; needs one precise pass
before being quoted in a commit.

| Target | Lines | Disposition |
|---|---|---|
| `backends/image/openshift.rs` | 574 | delete |
| `backends/image/kind.rs` | 206 | delete |
| `resource/impls/monitoring.rs` | 206 | delete (OpenShift UWM) |
| `resource/impls/mirror.rs` | 163 | delete (ITMS + MCO reboot) |
| `resource/impls/storage.rs` | 814 | keep `discover`/`snapshot_capable`; delete installers (~600) |
| `cli/setup.rs` | 528 | delete target/bring-up phase (~250) |
| `cli/cluster_tools.rs` | 84 | delete |
| `fixtures/kind/*.yaml` | 2,752 | delete |
| `cluster_config.rs` | 775 | delete `ImageBackend`, `backend`, `kind_cluster` |
| `resource/impls/buildkit.rs` | 605 | keep; strip SCC/userns/privileged/service-CA |
| `resource/impls/policy.rs` | 774 | strip SCC portions |
| `profiling.rs` | 427 | → ~50 |
| `cli/sync/perf.rs` | 1,237 | strip retrieval/windowing (~400) |

**~3,000 Rust lines + ~2,750 YAML**, plus `ops-openshift-setup.md` and most of
`how-to-profile.md` (~240 → ~90).

## Why this is the safe direction

Every installer needs cluster-admin — CRDs, ClusterRoles, `CSIDriver`, SCCs,
MachineConfig. Removing them means ztest performs **no cluster-scoped mutations**
and needs only namespaced RBAC plus a few cluster-scoped reads
(`VolumeSnapshotContent`, StorageClasses).

1. ztest becomes structurally incapable of breaking the cluster.
2. CI credentials shrink from cluster-admin to a namespaced ServiceAccount.
3. The historical failure class disappears — the scos-content pruning saga, the
   external-snapshotter/CSI version matrix, MCO reboots were all installer bugs.

For developer-facing infrastructure powering CI, a smaller blast radius and
narrower credentials are a reliability improvement, not a trade.

## Sequencing

Each phase is gated on the previous being observable.

| Phase | Work | Gate |
|---|---|---|
| 0 | Spikes: Connect wire format for `SelectMergeProfile`; does Pyroscope's merge emit a mapping table; `pyroscope-rs` 2.x signatures; agent behaviour on unreachable server; rootless BuildKit on the target cluster | answers recorded |
| 1 | Capability preflight (**additive**, deletes nothing) | reports correctly against both a k3s cluster and CRC |
| 2 | Builder: upstream rootless StatefulSet + PVC cache + `kube-pod://` | a full image build succeeds; second build hits cache |
| 3 | Registry + distribution — **ztest side is a probe only** (see below) | `cluster check` reports the configured registry |
| 4 | Delete installers | preflight still green |
| 5 | Collapse image path; add `--local` | one path, all suites pass |
| 6 | Pyroscope contract, then `profiling.rs` + `perf.rs` | mid-sync `ztest sync perf` opens a live profile in flameshow |

Phases 0–3 are additive or self-contained; the irreversible half begins at 4.

## Correction: phase 3 is smaller than it was written

Phase 3 was originally scoped as *"ztest cluster setup installs zot, the embedded
registry mirror, and the mirror config"*. That contradicts the rule this document
opens with. Installing a registry is cluster-admin work with exactly the
properties that motivated removing the other installers.

The phase therefore splits, and only the first part is ztest's:

| Part | Owner |
|---|---|
| report the configured push/pull registry | ztest (`cluster check`) |
| run a registry (zot, ghcr.io, …) | operator — `ops-cluster-requirements.md#registry` |
| `embedded-registry: true`, `registries.yaml` | operator — k3s/NixOS node config, not Kubernetes objects at all |

The last row was never ztest-shaped: it is host configuration in the cluster
flake, which is where `mirror.rs`'s replacement lives. `mirror.rs` is not
reimplemented anywhere in ztest; it is deleted and its job moves to the node.

## Open questions

1. **The platform contradiction.** `ops-production-cluster.md` says k3s + Rook-Ceph
   + Flux + Prometheus/Grafana/Loki. `ops-openshift-setup.md` and `setup.rs` say
   prod OpenShift. This plan assumes k3s. If prod is OpenShift, `monitoring.rs`
   stays (UWM is the supported scrape path there) and the SCC work keeps its
   justification.
2. **Pyroscope label schema** — what identifies a run's profiles (`service_name`
   per component? `sync_id`? topology? cluster?). Determines whether cross-run
   `--base` is queryable; Prometheus-shaped labels mean cardinality discipline
   applies.
3. **Builder sizing and lifetime** — a persistent StatefulSet is a standing
   reservation the QoS scheduler must account for. Fixed size, or resized between
   waves?
4. **Registry retention** — `dev-<hash>` tags churn per build. zot retention
   policy needs a rule (age? count? last-pulled?).

## Sources

- [BuildKit rootless docs](https://github.com/moby/buildkit/blob/master/docs/rootless.md)
- [BuildKit Kubernetes examples](https://github.com/moby/buildkit/tree/master/examples/kubernetes)
- [kube-pod connection helper](https://github.com/moby/buildkit/blob/master/client/connhelper/kubepod/kubepod.go)
- [Cache storage backends](https://docs.docker.com/build/cache/backends/) · [cache mount location issue](https://github.com/moby/buildkit/issues/1512)
- [k3s embedded registry mirror](https://docs.k3s.io/installation/registry-mirror) · [strategies for large images](https://docs.k3s.io/blog/2025/11/11/strategies-for-large-images) · [Spegel](https://github.com/spegel-org/spegel)
- [zot](https://zotregistry.dev/) · [Helm chart](https://artifacthub.io/packages/helm/zot/zot) · [Giant Swarm: caching registry with zot](https://docs.giantswarm.io/tutorials/registry/zot/)
- [Kaniko archived](https://github.com/kubernetes-sigs/kernel-module-management/issues/1244)
- [Pyroscope server API](https://grafana.com/docs/pyroscope/latest/reference-server-api/) · [profilecli](https://grafana.com/docs/pyroscope/latest/view-and-analyze-profile-data/profile-cli/) · [pyroscope-rs](https://github.com/grafana/pyroscope-rs)
