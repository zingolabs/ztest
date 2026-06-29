# Cluster Administration

Bare-metal Kubernetes cluster hosting self-hosted GH Actions runners +
the `zcash_kube_net` orchestrator. Optimized for low-maintenance on a
small team.

Data plane in the cluster; management plane over Tailscale. No public
ports on any node.

## Hardware

Minimum: 3 nodes, mixed roles (control plane + worker + storage +
runner). Three is the floor for etcd HA quorum.

| Resource    | Per-node min | Recommended      | Notes                               |
| ----------- | ------------ | ---------------- | ----------------------------------- |
| CPU         | 8 cores      | 16+ (Ryzen/Xeon) | Validators CPU-heavy at runtime     |
| RAM         | 32 GiB       | 64 GiB           | Runners + validators + Ceph OSD/MON |
| Disk (OS)   | 64 GiB SSD   | 256 GiB NVMe     |                                     |
| Disk (data) | 512 GiB NVMe | 1–2 TiB NVMe     | Ceph OSD; archives + ephemerals     |
| Network     | 1 GbE        | 10 GbE           | 10 GbE if archive seeds are large   |
| BMC/IPMI    | required     | required         | Out-of-band power, vendor-agnostic  |

Beyond 5 nodes: split control plane from workers.

## Network

```
   engineer ───► Tailscale tailnet ─► node-{1,2,3}
                 (admin plane: SSH, kube API)

   GitHub Actions ── HTTPS ──► runners poll out (outbound only)

   Cluster LAN (private, no public IPs)
     k3s + Rook-Ceph; Cilium (eBPF) for east-west; MetalLB L2 if needed
```

Public attack surface: zero ingress. MetalLB only if anything in-cluster
needs an external Service (typically nothing).

## Stack

| Layer             | Component                                    |
| ----------------- | -------------------------------------------- |
| Host OS           | NixOS                                        |
| Kubernetes        | k3s (via `services.k3s`)                     |
| CNI               | Cilium                                       |
| Storage CSI       | Rook + Ceph (RBD)                            |
| LB (optional)     | MetalLB, L2 mode                             |
| Runner controller | ARC Scale Sets (`kubernetes` mode)           |
| GitOps            | FluxCD                                       |
| Observability     | Prometheus + Grafana + Loki                  |
| Secrets           | sops-nix (host) + External Secrets (cluster) |
| Cert mgmt         | cert-manager                                 |
| Admin plane       | Tailscale                                    |

## OS — NixOS + k3s

Entire host stack expressed in one flake under
`infrastructure/cluster/nix/`. Disk dies → PXE/USB-install,
`nixos-install --flake .#node-X`, rejoin.

The flake is testable end-to-end via `nixosTest` in `nix flake check`,
so host config regressions surface in CI before touching hardware.

### Trade-offs

- OS+k8s upgrades are `nixos-rebuild boot --target-host` + reboot;
  rollback is via grub generations (reliable, not automatic).
- Known `services.k3s` warts:
  [#436224](https://github.com/nixos/nixpkgs/issues/436224) (systemd
  override via `lib.mkForce`),
  [#308201](https://github.com/NixOS/nixpkgs/issues/308201) (token
  immutable after init),
  [#425460](https://github.com/nixos/nixpkgs/issues/425460)
  (`autoDeployCharts` path-vs-string). All Helm releases route via Flux,
  never the k3s auto-deploy.

### Why not Talos / Flatcar / Ubuntu

- Talos: smaller surface, atomic upgrades — rejected, team has Nix
  fluency. Engineer-who-left risk lower in our primary language.
- Flatcar + k3s: honest fallback if Nix expertise evaporates.
- Ubuntu LTS + k3s: maximum drift; rejected.

### Tailscale

`services.tailscale.enable = true;` plus an ephemeral auth-key via
sops-nix. Nodes join `zaino-cluster` at boot. Engineers reach nodes by
Tailscale hostname; ACLs gate which engineer tags reach which node tags.

**Developer onboarding** (one-time per engineer):

1. Tailscale invite into `engineers` group (gates network reachability
   of the API server).
1. Pull kubeconfig — distributed via External Secrets / 1Password CLI;
   one command writes `~/.kube/zaino`. Points at
   `https://node-1.zaino-cluster.ts.net:6443`.
1. `export KUBECONFIG=~/.kube/zaino` in shell rc.

## Storage — Rook + Ceph (RBD)

VolumeSnapshot + `dataSource` cloning via `rbd.csi.ceph.com` —
load-bearing for the shadow-VSC pattern (see
[architecture-overview.md](architecture-overview.md#cross-namespace-clone-shadow-vsc)). Ceph RBD
snapshot/clone has shipped since 2007.

Why not the alternatives:

- **Longhorn**: officially unsupported on NixOS
  ([#2166](https://github.com/longhorn/longhorn/issues/2166)) —
  `iscsiadm` discovery breaks on non-FHS paths. Working around it
  requires a forked container image. Disqualifying.
- **Mayastor**: faster IOPS, but our workload is sequential reads of
  regtest chain dirs. Younger CSI, smaller community. Skip.

The usual "Ceph is too heavy" critique applies to multi-PB
multi-tenant clusters. For 3–5 NVMe nodes, Rook handles MON quorum, OSD
lifecycle, and recovery for a reasonable operational tax.

### NixOS-side

```nix
boot.kernelModules = [ "rbd" "ceph" ];

# Rook CSI plugins — via Helm values, not host config:
#   csi.csiCephFSPluginVolume: /run/current-system/kernel-modules/lib/modules
#   spec.dataDirHostPath:      /var/lib/rook
```

### Cluster shape

- 1 OSD per node, dedicated data disk (not the OS disk).
- 3 MONs (per-node on 3-node cluster).
- 2 MGRs.
- Default RBD pool: `size=3`. Archive pool: `size=1` (recreatable from
  LFS in minutes; 3× would burn disk).
- `rbd-csi-snapshotter` enabled.

Cluster + pools live in `infrastructure/cluster/apps/rook-ceph/`
as `CephCluster` + `CephBlockPool` + `StorageClass` +
`VolumeSnapshotClass`. Flux reconciles.

## Admin — Tailscale

Each node enables `services.tailscale`; auth-key via sops-nix, rotated
independently. Engineers SSH by tailnet hostname:

```bash
ssh nixos@node-1.zaino-cluster.ts.net
export KUBECONFIG=~/.kube/zaino   # from sops-encrypted secret
kubectl get nodes
```

Kube API is reachable **only** over Tailscale. No public TLS, no
`LoadBalancer` for the API. CI service accounts use OAuth clients with
scoped tags, not engineer credentials.

## Runners — ARC Scale Sets

GitHub-supported successor to ARC v1. Auto-scales 0 → N based on the
GitHub-side queue.

```bash
helm install arc \
    --namespace arc-systems --create-namespace \
    oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set-controller

helm install zaino-runners \
    --namespace arc-runners --create-namespace \
    --set githubConfigUrl="https://github.com/zingolabs/infrastructure" \
    --set githubConfigSecret.github_token="<PAT or App token>" \
    --set containerMode.type="kubernetes" \
    --set minRunners=0 --set maxRunners=8 \
    oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set
```

`containerMode.type=kubernetes` runs jobs as sibling pods via the kube
API — not DinD. This is the model `zcash_kube_net` is built against.

Token: GitHub App with `Actions: read+write` scoped to infrastructure +
consuming repos. Rotated via External Secrets.

## GitOps — FluxCD

Cluster state in `infrastructure/cluster/`:

```
infrastructure/cluster/
├── nix/                    # NixOS flake; per-node host config
│   ├── flake.nix
│   ├── machines/node-{1,2,3}.nix
│   ├── modules/
│   └── tests/              # nixosTest fixtures
├── flux-system/
├── apps/                   # rook-ceph, cilium, arc, observability, seeds-reconciler
└── infrastructure/         # cert-manager, external-secrets, metallb
```

Flux watches and reconciles. Add/remove apps by PR. Drift surfaces as
Flux alerts.

## Observability

Standard stack:

- Prometheus, 30 days local.
- Grafana dashboards in git (`apps/observability/dashboards/`).
- Loki + Promtail, 7 days.
- AlertManager → `#cluster-alerts` on Slack.

Dashboards that matter: Ceph OSD/MON/PG health, ARC queue depth + pod
startup latency, per-test artifact size, seed reconciler success rate.

## Bootstrap

Steps 1–7 are one-shot per cluster; step 8 hands over to Flux.

```bash
# 1. Provision hardware; BMC creds. Net-boot or USB-install NixOS minimal.

# 2. Apply per-node flake from workstation.
for n in node-1 node-2 node-3; do
  nixos-rebuild switch \
    --flake ./infrastructure/cluster/nix#$n \
    --target-host nixos@$n.zaino-cluster.ts.net \
    --use-remote-sudo
done
#   Per-node flake sets: services.k3s.{enable,role,extraFlags=[--flannel-backend=none …]},
#   services.tailscale.enable, boot.kernelModules=[rbd ceph]. Node-1 bootstraps
#   control plane; 2/3 join via sops-provided token.

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

# 7. Flux. Picks up everything else from the repo.
flux bootstrap github --owner=zingolabs --repository=infrastructure \
    --branch=dev --path=cluster/flux

# 8. Tag the bootstrap commit.
git tag cluster-bootstrap-$(date +%Y%m%d)
```

After step 7, Flux installs ARC, MetalLB, cert-manager, External
Secrets, observability, and the seed reconciler.

## Verify before declaring production

1. **`nixosTest`.** `nix flake check` boots the flake config in a VM,
   brings k3s up, deploys a minimal Rook-Ceph, asserts cluster health.
   Catches host-side regressions before nodes.
1. **Snapshot+clone regression.** PVC → VolumeSnapshot → new PVC via
   `dataSource` → attach → byte-compare. Re-run on Rook/Ceph bumps.
1. **Cross-ns shadow-VSC end-to-end.** Confirm Rook's csi-snapshotter
   handles pre-provisioned VSCs sharing a `snapshotHandle`.
1. **Disk-dies drill.** Wipe one node's data disk; reinstall; rejoin;
   confirm Rook re-replicates. Time and document.
1. **Bus-factor dry run.** A second engineer rebuilds a node from this
   doc + flake alone.

## Day-2 ops

- **Weekly:** Grafana scan for slow leaks (disk trend, runner startup).
- **Monthly:** bump Rook / Cilium / ARC chart versions; Flux applies on
  merge.
- **Quarterly:** Kubernetes minor upgrade. One node at a time, 4–6 h.

### Adding a node

```bash
# Add infrastructure/cluster/nix/machines/node-4.nix, install NixOS, then:
nixos-rebuild switch \
    --flake ./infrastructure/cluster/nix#node-4 \
    --target-host nixos@node-4.zaino-cluster.ts.net \
    --use-remote-sudo
```

Tailscale joins; k3s registers; Rook adds an OSD; Ceph rebalances over
the next hour.

### Replacing a failed node

```bash
kubectl drain --ignore-daemonsets --delete-emptydir-data <node>   # if reachable
kubectl delete node <node>

kubectl -n rook-ceph patch cephcluster zaino --type=merge \
    -p '{"spec":{"removeOSDsIfOutAndSafeToRemove":true}}'

# Provision replacement hardware, same hostname; push flake as in "Adding a node".
```

Archives on the failed OSD: `size=3` pool re-replicates automatically;
`size=1` archive pool re-materializes from LFS on next reconcile.

### Disaster recovery (full cluster loss)

k3s writes etcd snapshots (enabled via `services.k3s.extraFlags`),
shipped off-cluster via sidecar, 24h × 30 retention.

```bash
# 1. Re-provision 3 nodes through bootstrap step 3.
# 2. Restore etcd:
ssh nixos@node-1.zaino-cluster.ts.net sudo k3s server \
    --cluster-reset --cluster-reset-restore-path=/path/to/latest.snapshot
#    Then rejoin nodes 2 and 3.
# 3. Flux reconciles from git.
# 4. Rook re-creates Ceph. If local disks were wiped, RBD data is gone —
#    archives re-materialize from LFS; ephemerals were transient.
# 5. ARC reconnects to GitHub.
```

DR is rehearsed annually against a deliberately destroyed staging
cluster.

### Onboarding

1. Tailscale invite (`engineers` group).
1. SSO access to kubeconfig (one External Secrets pull).
1. Read on `infrastructure/cluster/`.
1. This doc.

First day to debugging a node: under an hour.

## Open

1. **etcd snapshot off-site target.** MinIO on a separate box, or a
   cloud bucket via Tailscale Funnel. Pick before production.
1. **In-cluster secrets backend.** External Secrets Operator backed by
   1Password Connect or Vault — match what the wider team uses.
1. **GPU nodes (future).** Add `nvidia-container-toolkit` overlay +
   kernel modules to the relevant flake module if any test ever needs
   GPU.
1. **Ceph pool sizing.** Start `size=3` default / `size=1` archive.
   Revisit if disk pressure forces EC (needs ≥5 nodes).

# Xeon LGA 3647 Shopping List Priced 2026-06-19

SuperMicro Dual Socket LGA 3647 Motherboard ~$700
Xeon Gold 6258R (28 cores, 2.7GHz) $875
128GB DDR4 ECC 2666MHz (4x32GB) $1200
WD-Black 2TB NVMe SSD $250
Pcie to 4x Nvme Adapter $100
12TB Seagate Ironwolf Pro Sata HDD $460

# AMD Milan Epyc 7xx3 Shopping List Priced 2026-06-24

SuperMicro Dual Socket Epyc 7003 Motherboard $850 (Link)[https://www.newegg.com/p/1JW-0006-00R41]
AMD Epyc 7453 (28 cores, 2.75Ghz) $500
128GB DDR4 ECC 3200MHz (4x32GB) $1250
WD-Black 2TB NVMe SSD $250
Pcie to 4x Nvme Adapter $100
12TB Seagate Ironwolf Pro Sata HDD $460
