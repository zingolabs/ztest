#!/usr/bin/env bash
# Connection-count sweep for loadgen: run one Job per concurrency level,
# SEQUENTIALLY (parallel levels would load the server at once and confound the
# measurement), and collect one JSON line per level.
#
#   scripts/sweep.sh <namespace> [connections] [duration_s] [rpc]
#   scripts/sweep.sh preview-070-rc1c "1 4 8 16 32 64 128" 20 block-range
#
# Requires: kubectl context already pointed at the target cluster, and a zaino
# service reachable at zaino.<namespace>.svc.cluster.local:8137.
set -euo pipefail

NS="${1:?usage: sweep.sh <namespace> [connections] [duration_s] [rpc]}"
CONNS="${2:-1 4 8 16 32 64 128}"
DUR="${3:-20}"
RPC="${4:-block-range}"
IMG="${LOADGEN_IMAGE:-docker.io/zingodevops/loadgen:dev}"
OUT="${OUT:-sweep-${NS}-${RPC}.jsonl}"

: > "$OUT"
for c in $CONNS; do
  name="loadgen-c${c}"
  kubectl -n "$NS" delete job "$name" --ignore-not-found >/dev/null 2>&1 || true
  cat <<EOF | kubectl -n "$NS" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: { name: $name, labels: { app: loadgen } }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 1800
  template:
    metadata: { labels: { app: loadgen } }
    spec:
      restartPolicy: Never
      containers:
      - name: loadgen
        image: $IMG
        imagePullPolicy: Always
        env:
        - { name: POD_NAMESPACE, valueFrom: { fieldRef: { fieldPath: metadata.namespace } } }
        - { name: ZTEST_LOG, value: "loadgen=info,ztest=info" }
        args:
        - --target=http://zaino.$NS.svc.cluster.local:8137
        - --rpc=$RPC
        - --connections=$c
        - --tip-window=50000
        - --blocks=100
        - --conn-mode=per-task
        - --duration=$DUR
        - --json
EOF
  kubectl -n "$NS" wait --for=condition=complete "job/$name" --timeout=300s >/dev/null 2>&1 \
    || kubectl -n "$NS" wait --for=condition=failed "job/$name" --timeout=5s >/dev/null 2>&1 || true
  json=$(kubectl -n "$NS" logs "job/$name" 2>/dev/null | grep '^{' | head -1 || true)
  if [ -n "$json" ]; then
    echo "$json" >> "$OUT"
    echo "conns=$c OK"
  else
    echo "conns=$c FAILED"
    kubectl -n "$NS" logs "job/$name" 2>/dev/null | tail -3 || true
  fi
  kubectl -n "$NS" delete job "$name" --ignore-not-found >/dev/null 2>&1 || true
done

echo "=== results in $OUT ==="
cat "$OUT"
