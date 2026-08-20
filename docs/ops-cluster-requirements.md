# Cluster requirements

What a cluster and a workstation must provide before ztest runs.

```bash
ztest cluster check [--cluster <profile>]     # non-zero on any capability a run needs
```

**The contract: a green `check` means `ztest run` and `ztest sync` will work here.** Every precondition
either has a probe below or is named under [residual gaps](#residual-gaps) — there is no third category.

| Capability                                  | Need                             | Provided by                            |
| ------------------------------------------- | -------------------------------- | -------------------------------------- |
| cluster reachable                           | **required**                     | operator                               |
| snapshot-capable StorageClass               | **required**                     | operator (local: `--install-storage`)  |
| `snapshot.storage.k8s.io/v1` + a controller | **required**                     | operator (local: `--install-storage`)  |
| host toolchain                              | **required**                     | you — `cargo git tar` + `oc` / `kind`  |
| node capacity                               | required on a remote cluster     | operator                               |
| image side-load (`kind load` chain)         | required on a local cluster      | you — a working `kind` + engine pair   |
| image registry                              | required on a remote cluster     | operator                               |
| ztest namespaces, identity, BuildKit cache  | **required**                     | `ztest cluster setup`                  |
| run ServiceAccount permissions              | **required**                     | `ztest cluster setup`                  |
| build pod admission (PSA / SCC)             | **required**                     | `ztest cluster setup` + operator's SCC |
| volume expansion                            | optional — BuildKit cache growth | operator                               |
| `metrics.k8s.io` API                        | optional — `kubectl top` / k9s   | `ztest cluster setup`                  |
| metrics stack                               | optional — metrics & profiling   | `ztest cluster setup`                  |
| profile collector                           | optional — CPU profiles          | your workstation                       |
| snapshot bucket (public read)               | optional — chain fixtures        | none — published, no credentials       |

Three outcomes, and the third matters:

| Mark                | Meaning                                           |
| ------------------- | ------------------------------------------------- |
| `✓`                 | present, with what was found                      |
| `✗`                 | absent — the cluster genuinely lacks it           |
| `✗` with "cannot …" | **unknown** — *your credential* could not read it |

An unknown is not an absence: a least-privilege run ServiceAccount cannot list cluster-scoped objects an
admin context reads fine, so "could not determine storage" usually means the kubeconfig is wrong, not the
cluster. Unknown blocks a run for the same reason absent does — a clear refusal now beats an obscure
failure twenty minutes in.

`ztest cluster setup` is gated on the **required** rows it cannot fix and does not itself need — never on
the rows it provisions, which would deadlock every fresh cluster, and never on host tooling it never
spawns. It re-runs the whole probe after provisioning, so "setup succeeded" and "a run will work" are one
answer.

## how a probe answers

Every probe is a read, and each reuses a signal the cluster already publishes rather than inventing one:

- **Ready pods, not Services** — the kubelet flips `Ready` only after the container's own endpoint
  answered, so `/-/ready` (Prometheus), `/ready` (Pyroscope), `/api/health` (Grafana) and
  `buildctl debug workers` (BuildKit) are read through the API server for free. A Service in front of a
  `CrashLoopBackOff` Deployment cannot pass for a working one.
- **Discovery, not listing** — "is this API served" is asked of `/apis`, which separates *unserved* from
  *empty* from *forbidden*; a `list` conflates all three.
- **`SelfSubjectAccessReview` over the whole role** — every `(resource, verb)` pair the `ztest-remote`
  ClusterRole grants is probed, not a sample. Partial grants are the failure that happens (a rule naming
  `jobs` read-only while the seed puller needs `create`), and a sample never sees them.
- **The role's revision, not its existence** — an admin caller is allowed everything, so their own
  access review passes over a stale role that would 403 the run ServiceAccount. The applied role carries
  a hash of the rules it was rendered from, and the probe compares it.
- **`dryRun` create for admission** — the real BuildKit pod spec is submitted with `dryRun: true`, so PSA
  level, SCC selection and every admission webhook vote for real and nothing is persisted.
- **A real read for reachability, not `/version`** — the version endpoint answers off memory, so a
  cluster whose etcd is down passes it and then fails every remaining row with the same buried error. A
  `get` of the `default` namespace touches storage; one line replaces fourteen. A 401/403 is not an
  outage and falls through to the table, where `run permissions` names the gap.
- **The whole side-load chain, not `kind` on PATH** — a local cluster's only image path runs four tools
  deep (ztest → `kind` → engine → the node's containerd), and each pair can be healthy alone while the
  chain is broken. The probe walks it read-only: the cluster's nodes exist, `kind` itself resolves them,
  and the node's `crictl` answers.
- **Per-node capacity, not the cluster sum** — a pod lands on one node, so 4×4c promises 16 cores nothing
  can hold. Measured against `allocatable`, not free: transient load queues a pod, it does not make the
  cluster unusable.

## residual gaps

Two preconditions no read-only, workstation-side probe reaches. Both are cluster egress:

| Gap                          | Fails as                                           |
| ---------------------------- | -------------------------------------------------- |
| cluster → snapshot bucket    | seed puller Job exhausts `backoffLimit: 2` mid-run |
| BuildKit pod → registry push | `buildctl` push error at the end of the compile    |

`check` probes the bucket from *your* machine, which catches a wrong endpoint or a withdrawn blob but not
an egress proxy or a `NetworkPolicy` on the cluster side. Registry push authentication is the registry's
own concern — the build pod presents its ServiceAccount token, and what that buys differs per registry.

## storage

**Required.** A `StorageClass` whose provisioner is backed by a `VolumeSnapshotClass` — ztest seeds tests
by cloning a content-addressed PVC copy-on-write from a `VolumeSnapshot`, and without it most suites
cannot run.

| Cluster    | Provide with                                                                                                                             |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| bare-metal | Rook-Ceph RBD (`rbd.csi.ceph.com`)                                                                                                       |
| kind       | CSI hostpath + external-snapshotter (`ztest cluster setup --install-storage`), or TopoLVM ([ops-local-cluster.md](ops-local-cluster.md)) |
| k3s        | an external-snapshotter + a snapshot-capable CSI — **k3s ships neither**                                                                 |

CRDs come from [kubernetes-csi/external-snapshotter](https://github.com/kubernetes-csi/external-snapshotter);
match its release to the CSI driver's compatibility matrix.

## registry

**Required for `dev!` images on a remote cluster.** A local cluster side-loads into its kind node and
needs none.

- Content-addressed `dev-<hash>` images need a writable target, configured per profile as `push`
  (reachable from the builder) and `pull` (reachable from inside the cluster) — an in-cluster registry
  has different addresses inside and out
- [zot](https://zotregistry.dev/) suits it: OCI-native, official Helm chart, online GC with retention
  policies, which `dev-<hash>` churn needs

## metrics

**Optional**, and ztest owns it — `ztest cluster setup` provisions Prometheus, Pyroscope and Grafana as
plain Deployments in `ztest-obs`. Nothing for an operator to install, nothing to configure (Prometheus
discovers ztest pods from labels they already carry).

- `--no-observability` declines it, together with the `metrics.k8s.io` API (metrics-server, into
  `kube-system`) setup provisions alongside it — one switch for everything metrics-related; a
  cluster already serving that API is left untouched either way
- An existing Pyroscope is adopted by its `app.kubernetes.io/name` label rather than duplicated
- Without the stack: components are not scraped and `ztest sync perf` is unavailable. Nothing else changes

## builder

**Not a prerequisite** — ztest owns it end to end: setup provisions the BuildKit ServiceAccount,
`buildkitd.toml` ConfigMap and cache PVC; each build creates a rootless BuildKit pod on demand and tears
it down after.

- Cache PVC outlives the pod deliberately: BuildKit `--mount=type=cache` state is builder-local and no
  cache backend exports it, so a pod without it rebuilds everything from scratch
- The one operator concession is the pod's security context — rootless BuildKit runs `runAsUser: 1000`
  with `seccompProfile: Unconfined`, exceeding Pod Security Admission *baseline*, so the `ztest`
  namespace needs `pod-security.kubernetes.io/enforce: privileged`, which `ztest cluster setup` applies

## container engine

Host-side only: `dev!` builds, the local runner bake, kind side-loads, and the host-placed
profiler. A remote cluster building on-cluster with BuildKit needs no engine on your machine.

`ztest cluster add` records whichever engine owns the cluster's node container — exact, since a
node belongs to one engine and never both. Override with `--runtime docker|podman`, which is
only needed where there is no node to observe (a `--kubeconfig` profile on a machine running
both). `ztest cluster check` prints the resolved engine on its header line.

Podman carries two host requirements ztest cannot satisfy for you:

- **Creating** a kind cluster under rootless podman needs cgroup delegation *and* a process
  cgroup that actually carries the controllers. A desktop terminal usually sits in
  `user@.service/session.slice/<term>.scope`, which holds only `memory pids` even once
  `Delegate=yes` is configured, so kind refuses. Create it from a delegated scope:

  ```sh
  systemd-run --user --scope -p Delegate=yes -- \
    env KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name <name>
  ```

  ztest never creates clusters, and every ztest operation works from a plain shell. This
  affects only the operator bootstrapping one.

- **Host-side profiling needs rootful podman.** Under rootless podman `--pid=host` shows only
  the invoking user's processes, so the collector resolves no pod pids. `ztest cluster check`
  reports the collector absent rather than producing an empty profile.
