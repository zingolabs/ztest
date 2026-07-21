# Production Cluster Runbook

Bare-metal NixOS/k3s/Ceph cluster that runs test workloads (validators, indexers, snapshots); CI runners are GitHub-hosted and reach it over kubeconfig + registry.

Data plane runs in the cluster; the management plane rides Tailscale. No node has a public port. For a single-node OpenShift stand-in, see [ops-openshift-setup.md](ops-openshift-setup.md).

## Hardware

Minimum 3 mixed-role nodes (control plane + worker + storage) — the etcd HA quorum floor. Split control plane from workers beyond 5 nodes.

| Resource    | Per-node min | Recommended  | Notes                               |
| ----------- | ------------ | ------------ | ----------------------------------- |
| CPU         | 8 cores      | 16+          | Validators are CPU-heavy at runtime |
| RAM         | 32 GiB       | 64 GiB       | Runners + validators + Ceph OSD/MON |
| Disk (OS)   | 64 GiB SSD   | 256 GiB NVMe |                                     |
| Disk (data) | 512 GiB NVMe | 1–2 TiB NVMe | Ceph OSD; archives + ephemerals     |
| Network     | 1 GbE        | 10 GbE       | 10 GbE if archive seeds are large   |
| BMC/IPMI    | required     | required     | Out-of-band power                   |

## Network

```
   engineer ──► Tailscale tailnet ─► node-{1,2,3}   (SSH, kube API)
   GitHub Actions ── HTTPS ──► runners poll outbound only
   Cluster LAN (private, no public IPs): k3s + Rook-Ceph; Cilium eBPF east-west; MetalLB L2 if needed
```

Zero public ingress. Kube API reachable **only** over Tailscale — no public TLS, no `LoadBalancer` for the API. MetalLB is provisioned but used only if an in-cluster Service needs an external IP.

## Stack

| Layer           | Component                                       |
| --------------- | ----------------------------------------------- |
| Host OS         | NixOS                                           |
| Kubernetes      | k3s (`services.k3s`)                            |
| CNI             | Cilium                                          |
| Storage CSI     | Rook + Ceph (RBD)                               |
| LB (optional)   | MetalLB, L2 mode                                |
| CI runners      | GitHub-hosted (kubeconfig + registry)           |
| Image registry  | `ghcr.io` (runner pushes, cluster pulls)        |
| GitOps          | FluxCD                                          |
| Observability   | Prometheus + Grafana + Loki                     |
| Secrets         | sops-nix (host) + External Secrets (cluster)    |
| Cert mgmt       | cert-manager                                    |
| Admin plane     | Tailscale                                       |

The host stack is one flake under `infrastructure/cluster/nix/`, tested via `nixosTest` in `nix flake check`. Disk dies → PXE/USB-install, `nixos-install --flake .#node-X`, rejoin. OS+k8s upgrades are `nixos-rebuild boot --target-host` + reboot; rollback via grub generations.

Helm releases route through Flux, never k3s auto-deploy (`services.k3s` warts: nixpkgs [#436224](https://github.com/nixos/nixpkgs/issues/436224), [#308201](https://github.com/NixOS/nixpkgs/issues/308201), [#425460](https://github.com/nixos/nixpkgs/issues/425460)).

## Storage — Rook + Ceph (RBD)

VolumeSnapshot + `dataSource` cloning via `rbd.csi.ceph.com` backs the cross-namespace shadow-VSC clone pattern ([design-architecture.md](design-architecture.md)).

NixOS host side:

```nix
boot.kernelModules = [ "rbd" "ceph" ];
# Rook CSI via Helm values (not host config):
#   csi.csiCephFSPluginVolume: /run/current-system/kernel-modules/lib/modules
#   spec.dataDirHostPath:      /var/lib/rook
```

Cluster shape:

- 1 OSD per node on a dedicated data disk (not the OS disk).
- 3 MONs (per-node on a 3-node cluster), 2 MGRs.
- Default RBD pool `size=3`; archive pool `size=1` (recreatable from LFS in minutes).
- `rbd-csi-snapshotter` enabled.

Cluster + pools live in `infrastructure/cluster/apps/rook-ceph/` as `CephCluster` + `CephBlockPool` + `StorageClass` + `VolumeSnapshotClass`.

## GitOps layout — FluxCD

```
infrastructure/cluster/
├── nix/                    # NixOS flake; per-node host config
│   ├── flake.nix
│   ├── machines/node-{1,2,3}.nix
│   ├── modules/
│   └── tests/              # nixosTest fixtures
├── flux-system/
├── apps/                   # rook-ceph, cilium, observability, seeds-reconciler
└── infrastructure/         # cert-manager, external-secrets, metallb
```

Add/remove apps by PR; drift surfaces as Flux alerts.

## Runners & cluster access

CI runs on stock GitHub-hosted runners (2-core/7-GiB): the job builds and pushes the dev image, then drives the test binary over kubeconfig while expensive work runs in-cluster. No self-hosted/ARC controller in the cluster.

The runner reaches the cluster two ways, both repo secrets:

- **Kube API — ServiceAccount token.** Mint a token for the run SA (below), embed in a kubeconfig, store base64 as `KUBECONFIG_B64`; `ztest` picks it up via `kube::Config::infer()`. Add the `tailscale/github-action` step if the API is Tailscale-only.
- **Images — registry.** Runner pushes `ghcr.io/<owner>/<repo>:dev-<hash>`; cluster pulls over egress. Registry mode is set with `ZTEST_IMAGE_REGISTRY`. See the image-distribution table in [ops-clusters.md](ops-clusters.md).

Registry pull auth: public `ghcr.io` packages need no creds; for private packages either give the pod SA an `imagePullSecrets` entry (reflected into ephemeral namespaces) or set `ZTEST_IMAGE_PULL_SECRET`. On-cluster image builds are covered in [design-remote-execution.md](design-remote-execution.md).

### Run RBAC

The run ServiceAccount needs: create/delete namespaces; CRUD pods/services/configmaps/PVCs/leases within them; create `VolumeSnapshot`s and cluster-scoped `VolumeSnapshotContent`s; read nodes / `CSIDriver`s. `ztest setup` provisions this SA plus its `ztest-remote` ClusterRole and a token (`src/resource/impls/policy.rs`); rotate the token via External Secrets.

### Provisioning boundary

- **Substrate** (operators, storage engine, snapshot controller): installed by Flux. ztest consumes it and refuses to install operators.
- **ztest's contract**: `ztest-seeds`/`ztest-qos` namespaces, QoS RBAC + per-tier SAs, node labels, `StorageClass` objects, the run identity, and on OpenShift the `nonroot-v2` SCC grant + `ztest-images` registry project. Owned by `ztest setup` (run once as admin); shapes in `qos.rs`, `storage.rs`, `resource/impls/policy.rs`.

## Observability

- Prometheus, 30 days local.
- Grafana dashboards in git (`apps/observability/dashboards/`).
- Loki + Promtail, 7 days.
- AlertManager → `#cluster-alerts` on Slack.

Key dashboards: Ceph OSD/MON/PG health, per-test pod startup latency, per-test artifact size, seed reconciler success rate.

## Bootstrap

Steps 1–7 are one-shot per cluster; step 8 tags the handover to Flux.

```bash
# 1. Provision hardware + BMC creds. Net-boot or USB-install NixOS minimal.

# 2. Apply per-node flake from workstation.
for n in node-1 node-2 node-3; do
  nixos-rebuild switch \
    --flake ./infrastructure/cluster/nix#$n \
    --target-host nixos@$n.zaino-cluster.ts.net \
    --use-remote-sudo
done
#   Flake sets services.k3s.{enable,role,extraFlags=[--flannel-backend=none …]},
#   services.tailscale.enable, boot.kernelModules=[rbd ceph]. Node-1 bootstraps
#   the control plane; 2/3 join via sops-provided token.

# 3. Pull kubeconfig, rewrite server URL to tailnet hostname.
scp nixos@node-1.zaino-cluster.ts.net:/etc/rancher/k3s/k3s.yaml ~/.kube/zaino
sed -i 's|server: https://127.0.0.1:6443|server: https://node-1.zaino-cluster.ts.net:6443|' ~/.kube/zaino
export KUBECONFIG=~/.kube/zaino
kubectl get nodes

# 4. Cilium (Flannel disabled above).
helm install cilium cilium/cilium --version 1.16.5 -n kube-system \
    -f infrastructure/cluster/apps/cilium/values.yaml

# 5. Rook operator.
helm install rook-ceph rook-release/rook-ceph --version v1.15.5 \
    -n rook-ceph --create-namespace \
    -f infrastructure/cluster/apps/rook-ceph/operator-values.yaml

# 6. Ceph cluster + pools (5–10 min first install).
kubectl apply -k infrastructure/cluster/apps/rook-ceph/cluster/
kubectl -n rook-ceph wait cephcluster/zaino --for=condition=Ready --timeout=15m

# 7. Flux picks up everything else from the repo.
flux bootstrap github --owner=zingolabs --repository=infrastructure \
    --branch=dev --path=cluster/flux

# 8. Tag the bootstrap commit.
git tag cluster-bootstrap-$(date +%Y%m%d)
```

Step 7 also brings up MetalLB, cert-manager, External Secrets, observability, and the seed reconciler.

## Verify before declaring production

1. **`nixosTest`** — `nix flake check` boots the flake in a VM, brings k3s up, deploys minimal Rook-Ceph, asserts health.
2. **Snapshot+clone regression** — PVC → VolumeSnapshot → new PVC via `dataSource` → attach → byte-compare. Re-run on Rook/Ceph bumps.
3. **Cross-ns shadow-VSC** — confirm Rook's csi-snapshotter handles pre-provisioned VSCs sharing a `snapshotHandle`.
4. **Disk-dies drill** — wipe a node's data disk, reinstall, rejoin, confirm Rook re-replicates.
5. **Bus-factor dry run** — a second engineer rebuilds a node from this doc + flake alone.

## Day-2 ops

- **Weekly:** Grafana scan for slow leaks (disk trend, pod startup).
- **Monthly:** bump Rook / Cilium chart versions; Flux applies on merge.
- **Quarterly:** Kubernetes minor upgrade, one node at a time.

### Adding a node

```bash
# Add infrastructure/cluster/nix/machines/node-4.nix, install NixOS, then:
nixos-rebuild switch \
    --flake ./infrastructure/cluster/nix#node-4 \
    --target-host nixos@node-4.zaino-cluster.ts.net \
    --use-remote-sudo
```

Tailscale joins, k3s registers, Rook adds an OSD, Ceph rebalances over ~1 hour.

### Replacing a failed node

```bash
kubectl drain --ignore-daemonsets --delete-emptydir-data <node>   # if reachable
kubectl delete node <node>
kubectl -n rook-ceph patch cephcluster zaino --type=merge \
    -p '{"spec":{"removeOSDsIfOutAndSafeToRemove":true}}'
# Provision replacement hardware, same hostname; push flake as in "Adding a node".
```

The `size=3` pool re-replicates automatically; the `size=1` archive pool re-materializes from LFS on next reconcile.

### Disaster recovery (full cluster loss)

k3s writes etcd snapshots (via `services.k3s.extraFlags`), shipped off-cluster by a sidecar, 24h × 30 retention.

```bash
# 1. Re-provision 3 nodes through bootstrap step 3.
# 2. Restore etcd:
ssh nixos@node-1.zaino-cluster.ts.net sudo k3s server \
    --cluster-reset --cluster-reset-restore-path=/path/to/latest.snapshot
#    Then rejoin nodes 2 and 3.
# 3. Flux reconciles from git.
# 4. Rook re-creates Ceph. If disks were wiped, RBD data is gone — archives
#    re-materialize from LFS; ephemerals were transient.
# 5. GitHub-hosted runners reconnect on the next CI run.
```

Rehearse DR annually against a deliberately destroyed staging cluster.

## Onboarding

One-time per engineer:

1. Tailscale invite into the `engineers` group (gates API-server reachability).
2. Pull kubeconfig via External Secrets / 1Password (`~/.kube/zaino`, pointing at `https://node-1.zaino-cluster.ts.net:6443`).
3. `export KUBECONFIG=~/.kube/zaino` in shell rc; `kubectl get nodes`.
4. Read `infrastructure/cluster/` and this doc.

SSH by tailnet hostname (`ssh nixos@node-1.zaino-cluster.ts.net`). CI service accounts use OAuth clients with scoped tags, not engineer credentials.
