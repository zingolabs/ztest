# Cluster requirements

What a cluster must provide before ztest can run on it. Check any cluster with:

```bash
ztest cluster check [--cluster <profile>]
```

It exits non-zero only when a **required** capability is missing.

| Capability | Need | Provided by |
| --- | --- | --- |
| snapshot-capable StorageClass + `snapshot.storage.k8s.io/v1` | **required** | operator |
| image registry | required for `dev!` images on a remote cluster | operator |
| metrics stack | optional — metrics & profiling | `ztest cluster setup` |

`ztest cluster check` distinguishes three outcomes, and the third matters:

| Mark | Meaning |
| --- | --- |
| `✓` | present, with what was found |
| `!` | absent — the cluster genuinely lacks it |
| `!` with "cannot …" | **unknown** — *your credential* could not read it |

An unknown is not an absence. The least-privilege run ServiceAccount cannot list
cluster-scoped objects an admin context sees fine, so "could not determine
storage" usually means the kubeconfig is wrong, not the cluster.

## storage

**Required.** A `StorageClass` whose provisioner is backed by a
`VolumeSnapshotClass`. ztest seeds tests by cloning a content-addressed PVC
copy-on-write from a `VolumeSnapshot`; without it most suites cannot run.

| Cluster | Provide with |
| --- | --- |
| bare-metal | Rook-Ceph RBD (`rbd.csi.ceph.com`) |
| kind | CSI hostpath driver + external-snapshotter (`scripts/kind-storage.sh`) |
| k3s | an external-snapshotter + a snapshot-capable CSI — **k3s ships neither** |

CRDs come from [kubernetes-csi/external-snapshotter](https://github.com/kubernetes-csi/external-snapshotter);
match its release to your CSI driver's compatibility matrix.

## registry

**Required for `dev!` images on a remote cluster.** A local cluster side-loads
into its kind node and needs none.

Content-addressed `dev-<hash>` images need a writable target, configured per
profile as `push` (reachable from the builder) and `pull` (reachable from inside
the cluster) — an in-cluster registry has different addresses inside and out.
[zot](https://zotregistry.dev/) suits this: OCI-native, an official Helm chart,
and online GC with retention policies, which `dev-<hash>` churn needs.

## metrics

**Optional**, and **ztest owns it** — `ztest cluster setup` provisions Prometheus,
Pyroscope, and Grafana as plain Deployments in the `ztest-obs` namespace. There
is nothing for an operator to install, and nothing to configure: Prometheus
discovers ztest pods from the labels they already carry.

Pass `ztest cluster setup --no-observability` to decline. If the cluster already runs
Pyroscope, ztest adopts it by its `app.kubernetes.io/name` label rather than
standing up a second one.

Without the stack, components are not scraped and `ztest sync perf` is
unavailable. Nothing else changes.

## builder

**Not a prerequisite.** ztest owns it end to end: `ztest cluster setup` provisions the
BuildKit ServiceAccount, `buildkitd.toml` ConfigMap, and cache PVC, and each
build creates a rootless BuildKit pod on demand and tears it down after.

The cache PVC outlives the pod deliberately — BuildKit `--mount=type=cache`
state is builder-local and is not exported by any cache backend, so a pod
without it re-does every build from scratch.

The one thing an operator must allow is the pod's security context: rootless
BuildKit runs `runAsUser: 1000` with `seccompProfile: Unconfined`, which exceeds
Pod Security Admission *baseline*. The `ztest` namespace therefore needs
`pod-security.kubernetes.io/enforce: privileged`, which `ztest cluster setup` applies.
