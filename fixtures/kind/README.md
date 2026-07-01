# Kind snapshot-support bundle

Vendored, version-pinned Kubernetes manifests that give a local **kind**
cluster the CSI VolumeSnapshot + clone capability ztest's seed/archive
machinery depends on (`materialize.rs`, `seeds.rs`, `mounts.rs`). On a
production Ceph cluster Rook provides this; kind ships only the
`standard` local-path provisioner, which has no snapshot support — so
`ztest setup` applies this bundle to bring kind to parity.

`ztest setup` `kubectl apply`s these in lexical order; the files are
embedded in the `ztest` binary via `include_str!`, so a `cargo install`ed
ztest carries them (no network fetch for the manifests themselves — kind
still pulls the container images at apply time).

The full ztest clone path (source PVC → VolumeSnapshot → cross-namespace
shadow VolumeSnapshotContent → cloned PVC in another namespace) is
validated against this bundle before vendoring.

## Files and provenance

| File                          | Upstream                                                                                                                                                       | Pin                                                                                                        |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `00-snapshot-crds.yaml`       | kubernetes-csi/external-snapshotter `client/config/crd/` (3 snapshot + 3 group-snapshot CRDs)                                                                  | `v8.2.0`                                                                                                   |
| `10-snapshot-controller.yaml` | external-snapshotter `deploy/kubernetes/snapshot-controller/` (rbac + setup)                                                                                   | `v8.2.0`                                                                                                   |
| `20-csi-hostpath-rbac.yaml`   | sidecar `rbac.yaml` from external-{provisioner,attacher,resizer,snapshotter,health-monitor} — the `external-*-runner`/`-cfg` roles the driver binds            | provisioner `v5.2.0`, attacher `v4.8.0`, resizer `v1.13.1`, snapshotter `v8.2.0`, health-monitor `v0.14.0` |
| `30-csi-hostpath-driver.yaml` | kubernetes-csi/csi-driver-host-path `deploy/kubernetes-1.30/hostpath/` (driverinfo + plugin StatefulSet)                                                       | `v1.17.1`                                                                                                  |
| `40-ztest-classes.yaml`       | ztest-authored: StorageClasses `rook-ceph-block` + `rook-ceph-block-archive` and VolumeSnapshotClass `ceph-rbd-snapclass`, all backed by `hostpath.csi.k8s.io` | —                                                                                                          |

The class names in `40-ztest-classes.yaml` deliberately match the
production Ceph names that `materialize.rs`/`mounts.rs` default to, so the
*same* code path runs on kind and Ceph with no env overrides. The only
value that can't be name-aliased — the CSI `driver` string in the shadow
VolumeSnapshotContent — is resolved from the live VolumeSnapshotClass at
runtime (`seeds::detect_driver`).

## Regenerating / bumping versions

Re-fetch the upstream files at the new tags, concatenate per the table
above, re-run the validated smoke test (source PVC → snapshot → shadow
clone in a second namespace → read back the marker), then replace these
files. Bump the pins in this table in the same commit.
