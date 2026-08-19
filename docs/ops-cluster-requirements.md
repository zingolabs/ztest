# Cluster requirements

What a cluster must provide before ztest runs on it.

```bash
ztest cluster check [--cluster <profile>]     # non-zero only on a missing *required* capability
```

| Capability                                                   | Need                                           | Provided by                                  |
| ------------------------------------------------------------ | ---------------------------------------------- | -------------------------------------------- |
| snapshot-capable StorageClass + `snapshot.storage.k8s.io/v1` | **required**                                   | operator (local: `ztest cluster setup`)      |
| image registry                                               | required for `dev!` images on a remote cluster | operator                                     |
| metrics stack                                                | optional — metrics & profiling                 | `ztest cluster setup`                        |
| snapshot bucket (public read)                                 | optional — chain fixtures                      | none — published bucket, no credentials      |

Three outcomes, and the third matters:

| Mark                | Meaning                                           |
| ------------------- | ------------------------------------------------- |
| `✓`                 | present, with what was found                      |
| `✗`                 | absent — the cluster genuinely lacks it           |
| `✗` with "cannot …" | **unknown** — *your credential* could not read it |

An unknown is not an absence: a least-privilege run ServiceAccount cannot list cluster-scoped objects an
admin context reads fine, so "could not determine storage" usually means the kubeconfig is wrong, not the
cluster.

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

- `--no-observability` declines it
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
