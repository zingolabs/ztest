#!/usr/bin/env bash
#
# Give a local kind cluster the snapshot-capable storage ztest requires:
# the external-snapshotter CRDs + controller, the CSI hostpath driver, and a
# StorageClass/VolumeSnapshotClass pair backed by it.
#
# ztest installs no cluster storage itself (docs/ops-cluster-requirements.md#storage):
# it is cluster-admin work whose blast radius is the whole cluster. On a throwaway
# kind cluster the admin is you, so this scripts the operator side — and nothing
# here runs as part of `ztest cluster setup` or `ztest run`.
#
# Verify the result with: ztest cluster check --cluster <profile>
#
# Usage: scripts/kind-storage.sh [kube-context]        (default: kind-kind)

set -euo pipefail

CONTEXT="${1:-kind-kind}"
SNAPSHOTTER_VERSION="${SNAPSHOTTER_VERSION:-v8.6.0}"
HOSTPATH_VERSION="${HOSTPATH_VERSION:-v1.18.0}"
# Named by the driver's own examples/csi-storageclass.yaml, applied below.
STORAGE_CLASS="csi-hostpath-sc"
DEFAULT_CLASS_ANNOTATION="storageclass.kubernetes.io/is-default-class"

log() { printf '\n\033[1m• %s\033[0m\n' "$*"; }

for bin in kubectl git; do
	command -v "$bin" >/dev/null || { echo "need $bin on PATH" >&2; exit 1; }
done

kubectl config get-contexts -o name | grep -qx "$CONTEXT" || {
	echo "no kube-context '$CONTEXT'. Available:" >&2
	kubectl config get-contexts -o name >&2
	exit 1
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# csi-driver-host-path's deploy.sh calls `kubectl` with no context flag and no
# override hook, so pin the target by handing it a single-context kubeconfig
# rather than mutating the caller's current-context.
kubectl config view --raw --minify --context "$CONTEXT" >"$WORK/kubeconfig"
export KUBECONFIG="$WORK/kubeconfig"

SERVER_MINOR="$(kubectl version -o json | sed -n 's/.*"minor": *"\([0-9]*\)".*/\1/p' | tail -1)"
# One deploy dir per Kubernetes minor. Match the server instead of taking
# `kubernetes-latest`, whose sidecars may assume APIs this server does not serve.
HOSTPATH_DEPLOY="${HOSTPATH_DEPLOY:-kubernetes-1.${SERVER_MINOR}}"

log "target: $CONTEXT (v1.${SERVER_MINOR}) — snapshotter $SNAPSHOTTER_VERSION, hostpath $HOSTPATH_VERSION/$HOSTPATH_DEPLOY"

log "external-snapshotter CRDs"
kubectl apply -k "https://github.com/kubernetes-csi/external-snapshotter//client/config/crd?ref=${SNAPSHOTTER_VERSION}"
# The controller's RBAC references these kinds; applying it against unestablished
# CRDs races into a NotFound.
kubectl wait --for=condition=Established --timeout=90s \
	crd/volumesnapshots.snapshot.storage.k8s.io \
	crd/volumesnapshotcontents.snapshot.storage.k8s.io \
	crd/volumesnapshotclasses.snapshot.storage.k8s.io

log "snapshot-controller"
kubectl apply -k "https://github.com/kubernetes-csi/external-snapshotter//deploy/kubernetes/snapshot-controller?ref=${SNAPSHOTTER_VERSION}"
kubectl -n kube-system rollout status deploy/snapshot-controller --timeout=180s

log "CSI hostpath driver"
git clone --quiet --depth 1 --branch "$HOSTPATH_VERSION" \
	https://github.com/kubernetes-csi/csi-driver-host-path.git "$WORK/csi-hostpath"
DEPLOY_DIR="$WORK/csi-hostpath/deploy/$HOSTPATH_DEPLOY"
[ -d "$DEPLOY_DIR" ] || { echo "no deploy dir $HOSTPATH_DEPLOY in $HOSTPATH_VERSION" >&2; exit 1; }
"$DEPLOY_DIR/deploy.sh"
kubectl rollout status statefulset/csi-hostpathplugin --timeout=300s

log "StorageClass + VolumeSnapshotClass"
kubectl apply -f "$WORK/csi-hostpath/examples/csi-storageclass.yaml"
kubectl apply -f "$WORK/csi-hostpath/examples/csi-volumesnapshotclass.yaml"

# Seeding needs snapshots and the BuildKit cache needs volume expansion; kind's
# built-in `standard` (local-path) offers neither, so leaving it default sends
# every PVC that names no class to a class ztest cannot use. Demote the others
# rather than naming this one at each call site.
log "make $STORAGE_CLASS the default StorageClass"
for sc in $(kubectl get storageclass -o name); do
	default=false
	[ "$sc" = "storageclass.storage.k8s.io/$STORAGE_CLASS" ] && default=true
	kubectl patch "$sc" -p \
		"{\"metadata\":{\"annotations\":{\"$DEFAULT_CLASS_ANNOTATION\":\"$default\"}}}"
done

log "result"
kubectl get storageclass
kubectl get volumesnapshotclass
