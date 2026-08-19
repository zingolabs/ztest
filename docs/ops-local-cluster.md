# Local cluster storage

Seeding clones a PVC copy-on-write from a `VolumeSnapshot` → a local cluster needs a CSI driver that can
snapshot. `ztest cluster setup` offers to install csi-hostpath here; anywhere else the driver is the
operator's call ([ops-cluster-requirements.md](ops-cluster-requirements.md)). Name the driver, ztest
resolves the rest:

```sh
ztest cluster add kind --kind kind --storage-driver topolvm.io --set-default
ztest cluster check
```

- A *driver*, not class names → StorageClass and VolumeSnapshotClass can never come from different providers
- Omitted, ztest follows the cluster's default StorageClass
- `check` resolves storage exactly as a run does, so green means seeding works

## Which driver

|                                                            | setup       | 32 GiB snapshot                                       |
| ---------------------------------------------------------- | ----------- | ----------------------------------------------------- |
| **CSI hostpath** (`ztest cluster setup --install-storage`) | one command | **~10 min** — `tar`, and it blocks every other volume |
| **TopoLVM thin pool**                                      | below       | **~5 s** — O(1) copy-on-write                         |

hostpath is fine for small volumes and needs nothing from the host. TopoLVM for real seeds.

## TopoLVM

### 1. Kernel modules

Containers cannot load modules; the kind node inherits the host's.

```nix
boot.kernelModules = [ "dm-thin-pool" "dm-snapshot" ];   # NixOS
```

- No `dm-thin-pool` → pool cannot be created
- No `dm-snapshot` → provisions fine, every snapshot fails

### 2. A thin pool

Volume group `ztest` containing thin pool `thin`. Back it however suits the machine — an existing VG with
free extents, a loopback file, or a dedicated disk (fastest, erases the device).

Loopback needs no spare disk and works anywhere. Put it **inside the kind node**: host and node have
separate `/dev` trees, and two LVM instances managing one VG deactivate each other's devices.

```sh
docker exec <cluster>-control-plane sh -c '
  apt-get update -qq && apt-get install -y -qq lvm2
  mkdir -p /etc/lvm && cat > /etc/lvm/lvm.conf <<EOF
devices    { obtain_device_list_from_udev = 0 }
activation { udev_sync = 0
             udev_rules = 0 }
EOF
  mkdir -p /var/lib/ztest-storage
  truncate -s 500G /var/lib/ztest-storage/pool.img
  LOOP=$(losetup -f --show /var/lib/ztest-storage/pool.img)
  lvm pvcreate -y "$LOOP" && lvm vgcreate ztest "$LOOP"
  lvm lvcreate --type thin-pool -n thin -l 95%FREE --poolmetadatasize 2G --zero n ztest'
```

- `lvm.conf` block is load-bearing: no `udev` in a kind node, so without it LVM creates the
  device-mapper device in the kernel then blocks forever on a `/dev` entry nobody will make
- `--zero n` skips zeroing new blocks → a fresh volume can expose blocks freed by another (fine for
  single-tenant fixtures, not a shared cluster)
- Existing VG on a real disk: create the pool there, skip the loopback — still need the `lvm2` install +
  `lvm.conf` in the node, plus `lvm vgchange -ay ztest` from inside it. **The host must never activate
  the VG** (same two-instance conflict)

### 3. TopoLVM

Its webhook needs cert-manager.

```sh
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.1/cert-manager.yaml
kubectl -n cert-manager rollout status deploy/cert-manager-webhook

helm repo add topolvm https://topolvm.github.io/topolvm && helm repo update
kubectl create ns topolvm-system
kubectl label ns topolvm-system pod-security.kubernetes.io/enforce=privileged
helm install topolvm topolvm/topolvm -n topolvm-system --wait -f - <<'YAML'
controller: { replicaCount: 1 }   # 2 replicas anti-affine; one never schedules on 1 node
lvmd:
  deviceClasses:
    - name: thin
      volume-group: ztest
      default: true
      spare-gb: 10
      type: thin
      thin-pool: { name: thin, overprovision-ratio: 10 }
storageClasses:
  - name: topolvm-thin
    storageClass:
      fsType: ext4                # the only mkfs in the kind node image
      isDefaultClass: true
      volumeBindingMode: WaitForFirstConsumer
      allowVolumeExpansion: true
      additionalParameters: { "topolvm.io/device-class": "thin" }
YAML
```

Chart ships no `VolumeSnapshotClass`; without one ztest sees no snapshot-capable storage:

```sh
kubectl apply -f - <<'YAML'
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata: { name: topolvm-thin-snapclass }
driver: topolvm.io
deletionPolicy: Delete
YAML
```

csi-hostpath also installed → demote it; two default StorageClasses is invalid and k8s picks neither:

```sh
kubectl patch sc csi-hostpath-sc \
  -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
```

## Verify

```sh
ztest cluster check      # snapshot-capable storage → topolvm-thin (topolvm.io)
```

Health of the pool itself:

```sh
docker exec <cluster>-control-plane lvm lvs ztest    # expect twi-aot---
```

`a` active, `o` open, `t` thin target. A pool that is not `a` fails every PVC at once — usually a reboot
without the kernel modules.

## Recreating the node

`lvm2` + `/etc/lvm/lvm.conf` live in the node's container filesystem → lost on `kind delete cluster`.
Re-run step 2 after recreating; the volume group is on disk and survives.
