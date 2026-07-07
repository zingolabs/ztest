# Local OpenShift (crc) Cluster Setup

Single-node OKD cluster via **crc** (OpenShift Local), brought to the state `ztest
run` needs. Local rehearsal for the prod OpenShift target
([cluster-administration.md](cluster-administration.md)) — not byte-identical
(single node, community bundle, SCOS CoreOS) but it reproduces the two things that
break ztest only on OpenShift: the `restricted-v2` **SCC** admission model and the
`topolvm.io` **CSI + snapshot** path.

> **Targets.** crc → `--target okd`; prod (bare-metal OpenShift/Rook-Ceph) →
> `--target remote`.

## TL;DR

You bring crc up; ztest only *connects* (§ztest's role). The non-obvious work is
storage — crc ships **no snapshot substrate**, so install the LVM Storage operator
from upstream manifests and give the VM a spare disk. Then:

```bash
ztest setup --target okd --storage-device /dev/vdb
```

## Contents

- [Prerequisites](#prerequisites)
- [Invoking `oc` (NixOS)](#invoking-oc-nixos)
- [Bringing up crc](#bringing-up-crc)
- [ztest's role: connect, don't drive](#ztests-role-connect-dont-drive)
- [Storage: LVMS from scratch](#storage-lvms-from-scratch)
- [Running `ztest setup`](#running-ztest-setup)
- [Remote access over Nebula](#remote-access-over-nebula)
- [Verification](#verification)
- [Known issues](#known-issues)
- [Teardown / rebuild](#teardown--rebuild)
- [SCC admission](#scc-admission)

## Prerequisites

| Need              | Detail                                                             |
| ----------------- | ------------------------------------------------------------------ |
| `crc` binary      | OpenShift Local, from <https://developers.redhat.com/products/openshift-local/overview>. ztest only checks it's on `PATH`. |
| `oc` / `kubectl`  | OpenShift CLI. On NixOS via `nix shell nixpkgs#openshift` (below).  |
| Virtualization    | libvirt/KVM on the host. On NixOS, declarative — see NixOS note.    |
| Disk              | ~120 GiB free for the VM after growth; a spare virtual disk for LVMS. |
| RAM               | crc default ~10.9 GiB; `crc config set memory` to bump.            |

**No Red Hat pull secret needed** — OKD uses community images (the reason we pick
it over the `openshift` preset).

## Invoking `oc` (NixOS)

Not permanently installed on NixOS; run ad-hoc:

```bash
nix shell nixpkgs#openshift -c oc <args…>
```

> **Gotcha.** A shell alias/function named `oc` (e.g. an `opencode` launcher)
> shadows the CLI. Use the `nix shell … -c oc` form. `nix run … -c` does **not**
> work — `-c` is a `nix shell` flag, not `nix run`.

All `oc` commands below assume this wrapper.

## Bringing up crc

ztest never drives crc (§ztest's role). Minimal sequence:

```bash
crc config set preset okd                # community bundle; no pull secret
crc config set consent-telemetry no
crc config set enable-shared-dirs false  # default mounts host $HOME over virtiofs; unused
crc config set disk-size 100             # 31 GiB default is too small — see below
crc setup
crc start
```

crc writes the `crc-admin` context into `~/.kube/config`; `oc`/`ztest` pick it up.

### disk-size: 100+ GiB up front

The 31 GiB default overflows once LVMS + ztest's zebra/zcashd/zaino images land:
the kubelet crosses its ephemeral-storage eviction threshold
(`NodeHasDiskPressure`) and evicts pods including `vg-manager`, so the VG never
builds. Growing an existing VM is non-destructive (crc runs `xfs_growfs` on start):

```bash
crc stop; crc config set disk-size 100; crc start
```

### NixOS host note

`crc setup` assumes Fedora/RHEL and shells out to `dnf`, which fails on NixOS.
Instead run crc in **system** network mode and provide libvirt + the NetworkManager
dnsmasq split-DNS declaratively, then skip only the two `libvirt-group` preflight
checks (NixOS manages group membership, so crc's `usermod` fix can't run).

Reference specialization: <https://github.com/elicbarbieri/nixos-config/blob/master/modules/specializations/kubernetes.nix>

## ztest's role: connect, don't drive

`ztest setup --target okd` connects to a cluster you already brought up — it never
runs `crc config`/`setup`/`start` (that imperatively mutates the host: a poor fit
for declarative hosts and not ztest's to own). Lifecycle matches `--target remote`:
preflight only checks the `crc` binary exists, then it talks to the API server
(`https://api.crc.testing:6443`) via the kubeconfig cert. It never uses
routes/ingress or oauth — so a degraded `ingress` operator (§Known issues) is
irrelevant.

## Storage: LVMS from scratch

crc ships no snapshot substrate:

```
oc get sc                  → only crc-csi-hostpath-provisioner (no snapshots)
oc get volumesnapshotclass → server doesn't have that resource type
```

ztest's seed cache clones PVCs from **CSI VolumeSnapshots**, so hostpath is a dead
end. Use **LVMS** (`topolvm.io`, thin-pool snapshots) — same driver family as prod:

1. give the VM a spare block device (LVMS carves from *node* disks),
2. install the LVM Storage operator (not in OKD's catalog — from upstream),
3. `ztest setup` applies the `LVMCluster` + storage classes.

> No existing snapshot-capable driver to point `--storage-provisioner` at — crc's
> only StorageClass is hostpath.

### 1. Attach a spare disk

LVMS consumes unused devices as seen **inside the VM** (`/dev/vdb`), not host
`lsblk` paths. The VM has only `/dev/vda` by default. Hypervisor op, needs `sudo`
(system libvirt, root-owned disk files):

```bash
sudo qemu-img create -f qcow2 ~/.crc/machines/crc/lvms.qcow2 50G
sudo virsh -c qemu:///system attach-disk crc \
  ~/.crc/machines/crc/lvms.qcow2 vdb \
  --driver qemu --subdriver qcow2 --targetbus virtio --persistent --live
```

Verify inside the VM (empty, no filesystem):

```bash
oc debug node/crc -- chroot /host lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT
# want: vdb  50G  disk   (no children, no FSTYPE)
```

> crc can regenerate the domain on restart. Re-verify after any restart; re-run
> `attach-disk` if `vdb` is gone.

### 2. Install the LVM Storage operator

LVMS is in the `redhat-operators` catalog, absent on OKD/crc (community-operators
only). Install from upstream manifests:

```bash
oc get packagemanifests -n openshift-marketplace | grep -i lvm   # → empty on OKD
```

```bash
# release branch matching the cluster (e.g. OKD 4.20)
git clone --depth 1 --branch release-4.20 https://github.com/openshift/lvm-operator
cd lvm-operator
oc apply -k config/default --server-side   # image quay.io/lvms_dev/lvms-operator:latest, ns openshift-lvm-storage
```

`config/default` is OLM-oriented and omits two things a plain apply needs — both
pods hang without these:

**(a) Metrics-cert Services.** The operator and `vg-manager` mount service-ca
metrics certs that nothing creates. Apply both, rewriting the namespace:

```bash
for f in config/prometheus/metrics_service.yaml \
         config/prometheus/vgmanager_metrics_service.yaml; do
  sed 's/namespace: system/namespace: openshift-lvm-storage/' "$f" | oc apply -f -
done
```

**(b) `apiservers` RBAC.** The `:latest` binary reads the cluster TLS profile
(`apiservers.config.openshift.io/cluster`), which `release-4.20`'s `config/rbac`
omits — both SAs crash-loop with `apiservers … is forbidden`:

```bash
oc apply -f - <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: lvms-operator-tlsprofile-extra
rules:
- apiGroups: ["config.openshift.io"]
  resources: ["apiservers"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: lvms-operator-tlsprofile-extra
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: lvms-operator-tlsprofile-extra
subjects:
- kind: ServiceAccount
  name: lvms-operator
  namespace: openshift-lvm-storage
- kind: ServiceAccount
  name: vg-manager
  namespace: openshift-lvm-storage
EOF
```

Wait for health + the CRD ztest probes:

```bash
oc rollout status deploy/lvms-operator -n openshift-lvm-storage --timeout=180s
oc get crd lvmclusters.lvm.topolvm.io
```

> Host-cluster patches, not tracked by ztest — re-apply on rebuild. A newer release
> branch may fold (a)/(b) in; check `config/rbac/role.yaml` for `apiservers` first.

### 3. Namespace

The operator is namespace-scoped and watches **`openshift-lvm-storage`** (older
ODF-LVM used `openshift-storage`). ztest's `LVMS_NAMESPACE` matches; the
`LVMCluster` must live there.

## Running `ztest setup`

With crc up, `vdb` attached, and the operator healthy, ztest applies its
`LVMCluster` (`ztest-lvmcluster`), the external-snapshotter CRDs + controller (crc
lacks them), and its StorageClasses / VolumeSnapshotClass on `topolvm.io`:

```bash
ztest setup --target okd --storage-device /dev/vdb
```

Idempotent — skips anything already Ready.

> `--storage-device` is only for **building** a fresh LVMS pool. On a cluster
> that already has snapshot-capable storage (operator + `LVMCluster` up, or any
> CSI driver with a `VolumeSnapshotClass`), run plain `ztest setup --target okd`:
> it scans the cluster, lists what it finds, and you pick the class — no flag
> needed. `--storage-provisioner topolvm.io` still works as a non-interactive
> override.

## Remote access over Nebula

Local use talks to `https://api.crc.testing:6443` over libvirt. To drive the
cluster from another machine (workstation/CI), crc's API + ingress are bridged
onto a [Nebula](https://github.com/slackhq/nebula) mesh — no SSH tunnel, no
public ports.

**Cluster host (server side).** A specialization DNATs mesh traffic on
`6443,80,443` to the crc node (`192.168.130.11`), so peers reach the API and the
ingress routes (registry, oauth) at the host's mesh IP. See
`modules/specializations/crc-nebula-exposure.nix` in the nixos-config.

**Peer (client side).** Resolve the crc hostnames to the host's mesh IP so TLS
verifies against crc's certs and `oc login`'s OAuth redirect resolves — the
mirror of the host's split-DNS, pointed at the mesh IP:

```
address=/crc.testing/<mesh-ip>
address=/apps-crc.testing/<mesh-ip>
```

Gate it behind a `kubernetes` specialisation (`crc-nebula-client.nix`) so it's
active only during cluster-dev, not every boot:

```bash
sudo nixos-rebuild switch --flake <cfg>#<host> --specialisation kubernetes
getent hosts api.crc.testing          # → <mesh-ip>
```

**Cluster policy** is provisioned by `ztest setup` itself (admin, once): on an
OpenShift target it creates the run identity (`ztest` SA + `ztest-remote` RBAC +
token), the `nonroot-v2` SCC grant, and the `ztest-images` registry project +
pull/push RBAC (`src/resource/impls/policy.rs`). Nothing to apply by hand.

**Run credential.** Build a kubeconfig from the SA token that `ztest setup`
minted:

```bash
oc --kubeconfig=~/.kube/config-crc-remote config set-cluster crc \
  --server=https://api.crc.testing:6443 \
  --certificate-authority=<crc-ca.pem> --embed-certs
oc --kubeconfig=~/.kube/config-crc-remote config set-credentials ztest \
  --token="$(oc -n ztest get secret ztest-token -o jsonpath='{.data.token}' | base64 -d)"
oc --kubeconfig=~/.kube/config-crc-remote config set-context crc \
  --cluster=crc --user=ztest --namespace=ztest
oc --kubeconfig=~/.kube/config-crc-remote config use-context crc
```

Without the client split-DNS active, target `--server=https://<mesh-ip>:6443`
`--tls-server-name=api.crc.testing` (the SNI still selects the api cert). This is
DNS-independent, so `ztest run`'s API access works regardless of the
specialisation; only image push (below) needs the ingress hostname.

**Image distribution** (the `ztest-images` project + RBAC are created by `ztest setup`):

```bash
docker login -u ztest -p "$(KUBECONFIG=~/.kube/config-crc-remote oc whoami -t)" \
  default-route-openshift-image-registry.apps-crc.testing
export ZTEST_IMAGE_REGISTRY=default-route-openshift-image-registry.apps-crc.testing/ztest-images
KUBECONFIG=~/.kube/config-crc-remote ztest run -p <package>
```

The client module also marks that registry host `insecure` for the local Docker
daemon (its Route cert is from the ingress CA, which Docker doesn't trust; the
mesh already encrypts the hop).

> **nushell:** `$HOME` is not expanded in external-command args — use `~` (or
> `$env.HOME`). `--kubeconfig=$HOME/...` silently writes a literal `$HOME/` dir.

## Verification

```bash
# LVMCluster reconciled and VG built on vdb
oc get lvmcluster ztest-lvmcluster -n openshift-lvm-storage -o jsonpath='{.status.state}{"\n"}'
# want: Ready

# operator + vg-manager both running
oc get pods -n openshift-lvm-storage

# storage classes present (operator's + ztest's)
oc get sc          # lvms-vg1 (topolvm.io) + ztest's rook-ceph-block*
oc get volumesnapshotclass

# node has headroom + no disk pressure
oc get node crc -o jsonpath='{range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}{"\n"}'
```

## Known issues

- **`ingress` Degraded (canary).** `Degraded=True` / `CanaryChecksSucceeding=False`
  — the canary route to `*.apps-crc.testing` times out. Long-standing crc cosmetic
  issue; still `Available=True` and doesn't affect ztest (API only). If `crc start`
  blocks on "ingress is degraded", the API is already up and `~/.kube/config`
  written — Ctrl-C the wait.
- **Kubeconfig missing mid-restart.** During `crc stop`/`start`, `~/.kube/config`
  may be absent until start completes. Meanwhile:
  `export KUBECONFIG=~/.crc/machines/crc/kubeconfig`.
- **`crc status` fails from a subprocess** (`dial tcp: missing address`) even when
  up — it needs the crc daemon socket. Don't gate automation on it; use the API.

## Teardown / rebuild

- Stop (keep state): `crc stop`. Start again: `crc start`.
- **Full reset**: `crc delete` wipes the VM — you lose the LVMS install, the `vdb`
  attach, and all ztest resources. Re-do §Storage afterward.
- ztest-created resources: `ztest cleanup` (see
  [running-tests.md](running-tests.md)); the LVMS operator + `LVMCluster` persist
  independently in `openshift-lvm-storage`.

## SCC admission

`ztest setup` resources pass admission, but **per-test pods** (`ztest run`) set
explicit `runAsUser: 1000` / `fsGroup: 1000/2001` (`manifest.rs`), which the
default `restricted-v2` SCC (`MustRunAsRange`) rejects — they fail admission on
OpenShift (crc *and* prod).

**Fix:** grant `nonroot-v2` (explicit non-root UID/GID — not `anyuid`, which
permits root). `ztest setup` provisions this on OpenShift targets
(`SccGrantProvider`, `src/resource/impls/policy.rs`), bound to the
`system:serviceaccounts` group because test namespaces are created dynamically
and the run identity is rbac-less (can't self-bind SCCs). A future refinement
could move it into `cluster::ensure_namespace` as a per-namespace grant, but
that requires giving the run identity rbac-write, so the group grant is the
current answer.
