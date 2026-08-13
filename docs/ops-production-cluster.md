# Production cluster runbook

Bare-metal NixOS/k3s/Ceph cluster running test workloads. CI runners are
GitHub-hosted and reach it over kubeconfig + registry. Data plane in-cluster,
management plane over Tailscale, no public ingress.

## Hardware

Minimum 3 mixed-role nodes — the etcd HA quorum floor. Split control plane from
workers beyond 5.

| Resource | Per-node min | Recommended |
| --- | --- | --- |
| CPU | 8 cores | 16+ (validators are CPU-heavy) |
| RAM | 32 GiB | 64 GiB |
| Disk (OS) | 64 GiB SSD | 256 GiB NVMe |
| Disk (data) | 512 GiB NVMe | 1–2 TiB NVMe (Ceph OSD) |
| Network | 1 GbE | 10 GbE |
| BMC/IPMI | required | required |

## Stack

| Layer | Component |
| --- | --- |
| Host OS / Kubernetes | NixOS + k3s |
| CNI | Cilium |
| Storage CSI | Rook + Ceph (RBD) |
| GitOps | FluxCD |
| Admin plane | Tailscale |
| Secrets | sops-nix (host) + External Secrets (cluster) |

The host stack is one flake under `infrastructure/cluster/nix/`, tested by
`nixosTest` in `nix flake check`. Helm releases route through Flux, never k3s
auto-deploy (`services.k3s` warts: nixpkgs
[#436224](https://github.com/nixos/nixpkgs/issues/436224),
[#308201](https://github.com/NixOS/nixpkgs/issues/308201)).

Kube API is reachable **only** over Tailscale — no public TLS, no `LoadBalancer`
for the API.

## Storage

`rbd.csi.ceph.com` provides the VolumeSnapshot + `dataSource` cloning that seeds
every test ([design-architecture.md](design-architecture.md)).

- 1 OSD per node on a dedicated data disk, 3 MONs, 2 MGRs.
- Default RBD pool `size=3`; archive pool `size=1` (recreatable from LFS).
- `rbd-csi-snapshotter` enabled.
- Host side: `boot.kernelModules = [ "rbd" "ceph" ];`

Cluster and pools live in `infrastructure/cluster/apps/rook-ceph/`.

## Provisioning boundary

- **Substrate** — operators, storage engine, snapshot controller: installed by
  Flux. ztest consumes it and refuses to install operators.
- **ztest's own** — the `ztest`/`ztest-seeds`/`ztest-meta`/`ztest-obs`
  namespaces, run identity + RBAC, node labels, BuildKit scaffolding, and the
  metrics stack. All owned by `ztest cluster setup`, run once as admin.

## Bootstrap

```bash
# 1. Install NixOS minimal on each node, then apply the flake.
for n in node-1 node-2 node-3; do
  nixos-rebuild switch --flake ./infrastructure/cluster/nix#$n \
    --target-host nixos@$n.zaino-cluster.ts.net --use-remote-sudo
done

# 2. Pull kubeconfig, point it at the tailnet hostname.
scp nixos@node-1.zaino-cluster.ts.net:/etc/rancher/k3s/k3s.yaml ~/.kube/zaino
sed -i 's|https://127.0.0.1:6443|https://node-1.zaino-cluster.ts.net:6443|' ~/.kube/zaino
export KUBECONFIG=~/.kube/zaino && kubectl get nodes

# 3. Cilium (Flannel disabled in the flake).
helm install cilium cilium/cilium -n kube-system \
    -f infrastructure/cluster/apps/cilium/values.yaml

# 4. Rook operator, then the Ceph cluster (5–10 min).
helm install rook-ceph rook-release/rook-ceph -n rook-ceph --create-namespace \
    -f infrastructure/cluster/apps/rook-ceph/operator-values.yaml
kubectl apply -k infrastructure/cluster/apps/rook-ceph/cluster/
kubectl -n rook-ceph wait cephcluster/zaino --for=condition=Ready --timeout=15m

# 5. Flux brings up everything else from git.
flux bootstrap github --owner=zingolabs --repository=infrastructure \
    --branch=dev --path=cluster/flux

# 6. ztest's own objects.
ztest cluster setup --cluster prod
```

## Verify before declaring production

1. `nix flake check` — boots the flake in a VM, brings k3s up, asserts Ceph health.
2. Snapshot+clone regression: PVC → VolumeSnapshot → new PVC via `dataSource` →
   attach → byte-compare. Re-run on every Rook/Ceph bump.
3. Cross-namespace shadow-VSC: confirm Rook's csi-snapshotter handles
   pre-provisioned VSCs sharing a `snapshotHandle`.
4. Disk-dies drill: wipe a node's data disk, reinstall, rejoin, confirm
   re-replication.
5. `ztest cluster check --cluster prod` reports every capability present.

## Day-2

**Adding or replacing a node** — add `machines/node-N.nix`, install NixOS, push
the flake as in bootstrap step 1. Tailscale joins, k3s registers, Rook adds an
OSD and rebalances over ~1 hour. For a replacement, drain and delete the old
node first and set `removeOSDsIfOutAndSafeToRemove` on the `CephCluster`. The
`size=3` pool re-replicates; the `size=1` archive pool re-materializes from LFS.

**Disaster recovery** — k3s ships etcd snapshots off-cluster, 24h × 30
retention. Re-provision 3 nodes, then:

```bash
ssh nixos@node-1.zaino-cluster.ts.net sudo k3s server \
    --cluster-reset --cluster-reset-restore-path=/path/to/latest.snapshot
```

Rejoin nodes 2 and 3; Flux reconciles from git; Rook re-creates Ceph. If disks
were wiped, archives re-materialize from LFS and ephemerals were transient.
Rehearse annually against a deliberately destroyed staging cluster.

## Onboarding

1. Tailscale invite into the `engineers` group (gates API reachability).
2. Pull the kubeconfig via External Secrets / 1Password.
3. `ztest cluster add prod --kubeconfig ~/.kube/zaino`
