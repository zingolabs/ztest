# OpenShift (crc) Setup & Troubleshooting

Bringing a single-node OKD cluster (via crc / OpenShift Local) to the state `ztest
run` needs. crc reproduces the two things that break ztest only on OpenShift: the
`restricted-v2` **SCC** admission model and the `topolvm.io` **CSI + snapshot** path.

**Targets:** crc → `--target okd`; prod bare-metal OpenShift/Rook-Ceph →
`--target remote` (see [ops-production-cluster.md](ops-production-cluster.md)).

## TL;DR

You start crc; ztest only connects. The work is storage — crc ships no snapshot
substrate, so install the LVM Storage operator and give the VM a spare disk. Then:

```bash
ztest setup --target okd --storage-device /dev/vdb
```

## Prerequisites

| Need           | Detail |
| -------------- | ------ |
| `crc` binary   | OpenShift Local; ztest only checks it's on `PATH`. |
| `oc`/`kubectl` | OpenShift CLI. On NixOS: `nix shell nixpkgs#openshift -c oc <args>`. |
| Virtualization | libvirt/KVM on the host. |
| Disk           | ~120 GiB free after VM growth; plus a spare virtual disk for LVMS. |
| RAM            | crc default ~10.9 GiB; bump with `crc config set memory`. |

No Red Hat pull secret needed — OKD uses community images.

**`oc` on NixOS.** Run ad-hoc via `nix shell nixpkgs#openshift -c oc <args>`. A shell
alias/function named `oc` shadows the CLI — use the full form. `nix run … -c` does
**not** work (`-c` is a `nix shell` flag). All `oc` commands below assume this wrapper.

## Bringing up crc

```bash
crc config set preset okd                # community bundle; no pull secret
crc config set consent-telemetry no
crc config set enable-shared-dirs false
crc config set disk-size 100             # 31 GiB default is too small
crc setup
crc start
```

crc writes the `crc-admin` context into `~/.kube/config`.

**disk-size 100+ GiB up front.** The 31 GiB default overflows once LVMS + ztest's
zebra/zcashd/zaino images land: the kubelet crosses its ephemeral-storage eviction
threshold (`NodeHasDiskPressure`) and evicts pods including `vg-manager`, so the VG
never builds. Growing an existing VM is non-destructive (`crc stop; crc config set
disk-size 100; crc start` runs `xfs_growfs`).

**NixOS host.** `crc setup` assumes Fedora/RHEL and shells out to `dnf`. Instead run
crc in **system** network mode, provide libvirt + NetworkManager dnsmasq split-DNS
declaratively, and skip the two `libvirt-group` preflight checks (NixOS manages group
membership). Reference specialization:
<https://github.com/elicbarbieri/nixos-config/blob/master/modules/specializations/kubernetes.nix>

## ztest's role: connect, don't drive

`ztest setup --target okd` connects to a running cluster; it never runs `crc
config`/`setup`/`start`. Preflight checks the `crc` binary exists, then talks to the
API server (`https://api.crc.testing:6443`) via the kubeconfig cert. It uses no
routes/ingress/oauth for the run itself — a degraded `ingress` operator is irrelevant.

## Storage: LVMS from scratch

crc's only StorageClass is `crc-csi-hostpath-provisioner` (no snapshots), and it has
no `volumesnapshotclass` resource type. ztest's seed cache clones PVCs from **CSI
VolumeSnapshots**, so use **LVMS** (`topolvm.io`, thin-pool snapshots — same driver
family as prod):

### 1. Attach a spare disk

LVMS carves from unused *node* devices, seen as `/dev/vdb` **inside the VM** (the VM
has only `/dev/vda` by default). No `sudo` if you're in the `libvirt` group (the disk
file lives under your user-owned `~/.crc`). Hot-plugged live — no restart:

```bash
qemu-img create -f qcow2 ~/.crc/machines/crc/lvms.qcow2 150G
virsh -c qemu:///system attach-disk crc \
  ~/.crc/machines/crc/lvms.qcow2 vdb \
  --driver qemu --subdriver qcow2 --targetbus virtio --persistent --live

# verify empty, no filesystem:
oc debug node/crc -- chroot /host lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT
# want: vdb  disk  (no children, no FSTYPE)
```

crc can regenerate the domain on restart — re-verify and re-run `attach-disk` if
`vdb` is gone.

### 2. Install the LVM Storage operator

LVMS is in `redhat-operators`, absent on OKD/crc. Install from upstream, using the
release branch matching the cluster:

```bash
git clone --depth 1 --branch release-4.20 https://github.com/openshift/lvm-operator
cd lvm-operator
oc apply -k config/default --server-side   # ns openshift-lvm-storage
```

`config/default` is OLM-oriented and omits two things a plain apply needs — both pods
hang without them:

**(a) Metrics-cert Services.** The operator and `vg-manager` mount service-ca metrics
certs that nothing creates:

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

oc rollout status deploy/lvms-operator -n openshift-lvm-storage --timeout=180s
oc get crd lvmclusters.lvm.topolvm.io
```

These are host-cluster patches, not tracked by ztest — re-apply on rebuild. A newer
release branch may fold (a)/(b) in; check `config/rbac/role.yaml` for `apiservers` first.

### 3. Namespace

The operator is namespace-scoped and watches **`openshift-lvm-storage`**. ztest's
`LVMS_NAMESPACE` matches; the `LVMCluster` must live there.

## Running `ztest setup`

With crc up, `vdb` attached, and the operator healthy, ztest applies its `LVMCluster`
(`ztest-lvmcluster`), the external-snapshotter CRDs + controller (crc lacks them), and
its StorageClasses / VolumeSnapshotClass on `topolvm.io`:

```bash
ztest setup --target okd --storage-device /dev/vdb
```

Idempotent — skips anything already Ready.

`--storage-device` is only for **building** a fresh LVMS pool. On a cluster that
already has snapshot-capable storage, run plain `ztest setup --target okd`: it scans,
lists what it finds, and you pick the class. `--storage-provisioner topolvm.io` is a
non-interactive override.

### Setup kubeconfig must carry a bearer token, not only a cert

On an OpenShift target, setup builds the base images and each build pushes a source
bundle to the integrated registry authenticated with the kubeconfig's **token**. crc's
default `kubeadmin` context is **cert-only**, so it fails with `the kubeconfig has no
bearer token for the registry push`. Mint a non-expiring token from a cluster-admin SA
and point your setup context at it:

```bash
oc create sa ztest-admin -n kube-system
oc create clusterrolebinding ztest-admin --clusterrole=cluster-admin \
  --serviceaccount=kube-system:ztest-admin
oc apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: ztest-admin-token
  namespace: kube-system
  annotations: { kubernetes.io/service-account.name: ztest-admin }
type: kubernetes.io/service-account-token
EOF
TOKEN=$(oc get secret ztest-admin-token -n kube-system -o jsonpath='{.data.token}' | base64 -d)
oc --kubeconfig=~/.kube/config-crc-admin config set-credentials ztest-admin --token="$TOKEN"
oc --kubeconfig=~/.kube/config-crc-admin config set-context <your-ctx> --user=ztest-admin
```

The `ztest run` credential (`config-crc-remote`, below) is already a token-based SA.

## Remote access over Nebula

Local use talks to `https://api.crc.testing:6443` over libvirt. To drive the cluster
from another machine, crc's API + ingress are bridged onto a
[Nebula](https://github.com/slackhq/nebula) mesh — no SSH tunnel, no public ports.

**Cluster host.** A specialization DNATs mesh traffic on `6443,80,443` to the crc node
(`192.168.130.11`) so peers reach the API and ingress routes at the host's mesh IP
(`crc-nebula-exposure.nix`).

**Peer.** Resolve crc hostnames to the host's mesh IP (mirror of the host split-DNS)
so TLS verifies and `oc login`'s OAuth redirect resolves. Gate behind a `kubernetes`
specialisation (`crc-nebula-client.nix`) so it's active only during cluster-dev:

```
address=/crc.testing/<mesh-ip>
address=/apps-crc.testing/<mesh-ip>
```

```bash
sudo nixos-rebuild switch --flake <cfg>#<host> --specialisation kubernetes
getent hosts api.crc.testing          # → <mesh-ip>
```

**Cluster policy** is provisioned by `ztest setup` (admin, once): the run identity
(`ztest` SA + `ztest-remote` RBAC + token), the `nonroot-v2` SCC grant, and the
`ztest-images` registry project + pull/push RBAC (`src/resource/impls/policy.rs`).

**Run credential** — built from the SA token `ztest setup` minted:

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

Without the client split-DNS active, target `--server=https://<mesh-ip>:6443
--tls-server-name=api.crc.testing` (the SNI still selects the api cert). This is
DNS-independent — only image push needs the ingress hostname.

**Image distribution** (see the `ZTEST_IMAGE_*` table in
[ops-clusters.md](ops-clusters.md)):

```bash
docker login -u ztest -p "$(KUBECONFIG=~/.kube/config-crc-remote oc whoami -t)" \
  default-route-openshift-image-registry.apps-crc.testing
export ZTEST_IMAGE_REGISTRY=default-route-openshift-image-registry.apps-crc.testing/ztest-images
KUBECONFIG=~/.kube/config-crc-remote ztest run -p <package>
```

The client module marks that registry host `insecure` for the local Docker daemon
(its Route cert is from the ingress CA; the mesh already encrypts the hop).

> **nushell:** `$HOME` is not expanded in external-command args — use `~` (or
> `$env.HOME`), else `--kubeconfig=$HOME/...` writes a literal `$HOME/` dir.

## Verification

```bash
oc get lvmcluster ztest-lvmcluster -n openshift-lvm-storage -o jsonpath='{.status.state}{"\n"}'  # Ready
oc get pods -n openshift-lvm-storage                 # operator + vg-manager Running
oc get sc                                            # lvms-vg1 (topolvm.io) + ztest's
oc get volumesnapshotclass
oc get node crc -o jsonpath='{range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}{"\n"}'
```

## Known issues

- **`ingress` Degraded (canary).** `Degraded=True` / `CanaryChecksSucceeding=False` —
  the canary route to `*.apps-crc.testing` times out. Cosmetic; still `Available=True`
  and doesn't affect ztest (API only). If `crc start` blocks on "ingress is degraded",
  the API is already up and `~/.kube/config` written — Ctrl-C the wait.
- **Kubeconfig missing mid-restart.** During `crc stop`/`start`, `~/.kube/config` may
  be absent until start completes; meanwhile
  `export KUBECONFIG=~/.crc/machines/crc/kubeconfig`.
- **`401 Unauthorized` after a crc restart/recreate.** A restart that renews expired
  cluster certs rotates the SA-token signing key (and a fresh `ztest setup` mints a new
  run SA), so token-based kubeconfigs go stale — `ztest run` fails `cluster probe
  failed — ApiError: Unauthorized … 401`. Rebuild from the *current* SA tokens via a
  working admin context (e.g. `crc oc-env`'s):
  ```bash
  oc -n ztest get secret ztest-token -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/crc-ca.crt
  TOKEN=$(oc -n ztest get secret ztest-token -o jsonpath='{.data.token}' | base64 -d)
  oc --kubeconfig=~/.kube/config-crc-remote config set-cluster crc \
    --server=https://<mesh-ip>:6443 --tls-server-name=api.crc.testing \
    --certificate-authority=/tmp/crc-ca.crt --embed-certs
  oc --kubeconfig=~/.kube/config-crc-remote config set-credentials ztest --token="$TOKEN"
  oc --kubeconfig=~/.kube/config-crc-remote config set-context crc-remote \
    --cluster=crc --user=ztest --namespace=ztest
  oc --kubeconfig=~/.kube/config-crc-remote config use-context crc-remote
  ```
  Refresh the setup credential (`config-crc-admin`) the same way against its
  cluster-admin SA token (must be token-based, not cert-only).
- **`crc status` misreports** even when the cluster is up (`dial tcp: missing address`,
  or `crc does not seem to be setup correctly…`). Don't gate automation on it; confirm
  via the API: `eval "$(crc oc-env)"; oc get nodes`.
- **`oc` not on `PATH` in a fresh/non-interactive shell** — run `eval "$(crc oc-env)"`
  first (puts crc's bundled `oc` on `PATH`).

## Teardown / rebuild

- Stop (keep state): `crc stop`; resume with `crc start`.
- **Full reset:** `crc delete` wipes the VM — you lose the LVMS install, the `vdb`
  attach, and all ztest resources. Re-do § Storage afterward.
- ztest-created resources: `ztest cleanup` (see
  [guide-running-tests.md](guide-running-tests.md)); the LVMS operator + `LVMCluster`
  persist independently in `openshift-lvm-storage`.

## SCC admission

`ztest setup` resources pass admission, but **per-test pods** (`ztest run`) set
explicit `runAsUser: 1000` / `fsGroup: 1000/2001` (`manifest.rs`), which the default
`restricted-v2` SCC (`MustRunAsRange`) rejects on OpenShift (crc and prod).

**Fix:** grant `nonroot-v2`. `ztest setup` provisions this on OpenShift targets
(`SccGrantProvider`, `src/resource/impls/policy.rs`), bound to the
`system:serviceaccounts` group because test namespaces are created dynamically and the
run identity is rbac-less.

## On-cluster image builds

On OpenShift, ztest builds every image on the cluster in a ztest-owned BuildKit pod
provisioned by `ztest setup`. See
[design-remote-execution.md](design-remote-execution.md) for the full flow.
