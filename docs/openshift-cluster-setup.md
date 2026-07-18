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
`lsblk` paths. The VM has only `/dev/vda` by default. No `sudo` needed if you're in
the `libvirt` group (on NixOS the `kubernetes` specialization adds you) — the disk
file lives under your user-owned `~/.crc`, and `qemu:///system` is group-accessible.
The disk is hot-plugged live, so the node sees `/dev/vdb` without a restart:

```bash
qemu-img create -f qcow2 ~/.crc/machines/crc/lvms.qcow2 150G
virsh -c qemu:///system attach-disk crc \
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

> **The setup kubeconfig must carry a bearer token, not only a client
> certificate.** On an OpenShift target, setup builds the base images, and each
> build pushes a source bundle to the integrated registry authenticated with the
> kubeconfig's **token** (`internal_push_target` reads it). crc's default
> `kubeadmin` context is **cert-only**, so it fails with `the kubeconfig has no
> bearer token for the registry push`. Use a token-bearing admin context — mint a
> non-expiring token from a cluster-admin ServiceAccount and point your setup
> context's user at it:
>
> ```bash
> oc create sa ztest-admin -n kube-system
> oc create clusterrolebinding ztest-admin --clusterrole=cluster-admin \
>   --serviceaccount=kube-system:ztest-admin
> oc apply -f - <<'EOF'
> apiVersion: v1
> kind: Secret
> metadata:
>   name: ztest-admin-token
>   namespace: kube-system
>   annotations: { kubernetes.io/service-account.name: ztest-admin }
> type: kubernetes.io/service-account-token
> EOF
> TOKEN=$(oc get secret ztest-admin-token -n kube-system -o jsonpath='{.data.token}' | base64 -d)
> oc --kubeconfig=~/.kube/config-crc-admin config set-credentials ztest-admin --token="$TOKEN"
> oc --kubeconfig=~/.kube/config-crc-admin config set-context <your-ctx> --user=ztest-admin
> ```
>
> (The `ztest run` credential — [Remote access](#remote-access-over-nebula)'s
> `config-crc-remote` — is already a token-based SA, so runs that build `dev!`
> component images push their bundles fine.)

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
- **`401 Unauthorized` after a crc restart/recreate.** A restart that renews
  expired cluster certs rotates the SA-token signing key (and a fresh `ztest setup`
  mints a new run SA), so the token-based kubeconfigs go stale — `ztest run` fails
  `cluster probe failed — ApiError: Unauthorized … 401`. Rebuild them from the
  *current* SA tokens (via a working admin context, e.g. `crc oc-env`'s):
  ```bash
  # run credential (config-crc-remote → the `crc` profile)
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
  The setup credential (`config-crc-admin`) is refreshed the same way, pointed at
  its cluster-admin SA token (see [Running `ztest setup`](#running-ztest-setup) — it
  must be token-based, not cert-only).
- **`crc status` misreports** even when the cluster is up — `dial tcp: missing
  address` from a subprocess, or `crc does not seem to be setup correctly, have you
  run 'crc setup'?` (a daemon-state quirk; `crc start` will still say the cluster is
  running and hand you the console URL). Don't gate automation on `crc status`;
  confirm with the API instead: `eval "$(crc oc-env)"; oc get nodes`.
- **`oc` not on `PATH` in a fresh/non-interactive shell** — run `eval "$(crc
  oc-env)"` first (it puts crc's bundled `oc` at `~/.crc/bin/oc/oc` on `PATH`).

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

## On-cluster builds (default on OpenShift)

On an OpenShift registry (distinct push route / pull service) ztest builds every
image **on the cluster** in a long-lived, **ztest-owned rootless buildah pod**
(`src/backends/image/openshift.rs`, `src/resource/impls/buildah.rs`). Per image it
packs the Dockerfile + context into a deterministic, content-addressed tar
(`bundle::pack`), `oc cp`s it into the buildah pod, runs `buildah bud`, and
`buildah push`es the result to the integrated registry over the in-cluster
service (authenticating with the pod SA's token). The build log is streamed live
through the console PTY (`oc exec -t`), and success is the exec's exit status.

Why not `oc start-build`: the native Build subsystem runs the build in the
cluster's **release-managed** `docker-builder` image (`quay.io/okd/scos-content`),
which on OKD-SCOS/CRC is pruned from quay when the release tag moves — breaking
every build (`Init:ImagePullBackOff: manifest unknown`). Buildah builds from a
**pinned, retained public** image (`quay.io/buildah/stable`), so a pruned release
image can't break builds.

Why `chroot` isolation (not `oci`/user namespaces): buildah runs each `RUN` via
`chroot` — no per-step OCI runtime, no new namespaces — needing only
`SETUID`/`SETGID`/`SYS_CHROOT`. Full `oci`/userns isolation can't run unprivileged
on this OpenShift/CRI-O/kernel stack: a locked-down pod has masked `/proc`
submounts, and the kernel's "procfs must be fully visible" rule makes it need
`procMount: Unmasked` + an unconfined seccomp profile — strictly *more* privilege.
`chroot` builds identically with less, under ztest's own narrow `ztest-buildah`
SCC (not privileged, no host access; runs as uid 1000).

**No fallback.** The cluster profile names the backend (an OpenShift registry ⇒
this on-cluster builder); if it fails, the run fails — it never silently
degrades to another build path.

**Requirements.** No cluster operators to install — ztest owns the whole build
path. `ztest setup` (with an admin kubeconfig, needed to create the SCC) provisions
everything (`src/resource/impls/buildah.rs`, `policy.rs`, `base_images.rs`):
- the **buildah build server** (`BuildahProvider`, `NodeId::Buildah`): the
  `ztest-buildah` Deployment running `quay.io/buildah/stable` idle, its
  `ztest-buildah` ServiceAccount, and a storage PVC (buildah's `vfs` graphroot +
  the staged context; persists cached base layers across builds);
- a **custom non-privileged SCC `ztest-buildah`**: allows `SETUID`/`SETGID`/
  `SYS_CHROOT`, `allowPrivilegeEscalation`, and `RunAsAny` — no privileged
  container, no host access. The pod runs as uid 1000 with SELinux type
  `container_engine_t` (the domain that permits buildah's nested-container fs
  setup);
- **registry push authz**: the `ztest-image-push` role on `ztest-images` bound to
  the `ztest-buildah` SA (`imagestreams: create` + `imagestreams/layers:
  get,update`) — the push auto-creates the imagestream on first push. The buildah
  pod pushes over the in-cluster service with its SA token
  (`--tls-verify=false`, the registry's service-ca cert; pulls are unaffected —
  the kubelet trusts the service-ca).

**Base images — nothing to seed.** The compile **builder** and the test-runner
**base** are built on the cluster by `ztest setup` from `docker/builder.Dockerfile`
and `docker/runner-base.Dockerfile` (`src/resource/impls/base_images.rs`), through
the same buildah-pod path as component images. They are **stock Debian**
(`rust:1.95.0-bookworm` / `debian:bookworm-slim`) — the workspace links no
rocksdb / no OpenSSL (rustls everywhere) and only statically-linked C (ring,
aws-lc-sys, zstd-sys), so the runner base is just glibc + CA roots and both images
pin `bookworm` for an identical glibc at compile and run time. No `nix build`, no
`docker load`, no local image build — only the ~1 KB source bundle leaves the laptop.

Each is content-addressed on its Dockerfile bytes (`<repo>:d-<hash>`): edit a
Dockerfile and the tag forks, so the next `ztest setup` rebuilds it and the digest
pin re-rolls the builder Deployment; leave it unchanged and setup finds the tag
present and skips the build. The whole flow is `ztest setup` → `ztest run`.
