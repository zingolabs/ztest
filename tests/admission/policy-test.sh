#!/usr/bin/env bash
# Checks the ValidatingAdmissionPolicy that bounds ztest's identities. The policy itself is
# GitOps-managed in zingolabs/devops (platform/ztest-ci/policy.yaml); this only exercises it.
#
# Usage: ./policy-test.sh [policy.yaml] [kube-context]
#   defaults: ../../../devops/platform/ztest-ci/policy.yaml, kind-kind
#
# Applies the policy as [Deny] for the duration — never aim it at production.
#
# Every case asserts both directions and checks the policy *named* the denial: under
# failurePolicy: Fail a broken policy also denies everything, and kubectl fails for reasons that
# are not a verdict (protected namespace, missing SA, absent pod).
set -uo pipefail
POLICY_FILE="${1:-$(dirname "$0")/../../../devops/platform/ztest-ci/policy.yaml}"
K="kubectl --context=${2:-kind-kind}"
AS="--as=ci --as-group=ztest-ci --as-group=system:authenticated"
POLICY="ValidatingAdmissionPolicy 'ztest'"
failed=0 case_name="" bad=""

PLAIN='  containers: [{name: c, image: busybox}]'   # a `}` inside ${2:-...} would end the expansion
pod() { printf 'apiVersion: v1\nkind: Pod\nmetadata: {name: p, namespace: %s}\nspec:\n%s\n' "$1" "${2:-$PLAIN}"; }
ns()  { printf 'apiVersion: v1\nkind: Namespace\nmetadata: {name: %s, labels: {%s}}\n' "$1" "${2:+ztest.io/role: $2}"; }
rb()  { printf 'apiVersion: rbac.authorization.k8s.io/v1\nkind: RoleBinding\nmetadata: {name: p, namespace: ztest-fixture}\nroleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: %s}\nsubjects: [{kind: ServiceAccount, name: %s, namespace: ztest}]\n' "$1" "$2"; }

assert() { # assert deny|allow <what> <cmd...>
  local want="$1" what="$2"; shift 2
  local out; out=$("$@" 2>&1)
  if grep -qF "$POLICY" <<<"$out"; then [ "$want" = deny ] || bad+="    $what — denied"$'\n'
  else [ "$want" = allow ] || bad+="    $what — not denied by the policy"$'\n'; fi
}
deny()  { assert deny  "$1" $K $AS apply --dry-run=server -f /dev/stdin; }
allow() { assert allow "$1" $K $AS apply --dry-run=server -f /dev/stdin; }
begin() { case_name="$1"; bad=""; }
end()   { [ -z "$bad" ] && printf '  ok    %s\n' "$case_name" || { printf '  FAIL  %s\n%s' "$case_name" "$bad"; failed=$((failed+1)); }; }

$K delete ns ztest-fixture ztest-outsider --ignore-not-found >/dev/null 2>&1
{ ns ztest-fixture test-env; echo ---; ns ztest; echo ---; ns ztest-outsider; } | $K apply -f - >/dev/null
$K create sa intruder -n ztest-fixture >/dev/null 2>&1
$K create sa ztest-driver -n ztest-fixture >/dev/null 2>&1
$K create clusterrolebinding ztest-policy-test --clusterrole=cluster-admin --group=ztest-ci >/dev/null 2>&1
[ -f "$POLICY_FILE" ] || { echo "no policy at $POLICY_FILE" >&2; exit 2; }
sed 's/\[Audit, Warn\]/[Deny]/' "$POLICY_FILE" | $K apply -f - >/dev/null
sleep 2
trap '$K delete clusterrolebinding ztest-policy-test ns/ztest-fixture ns/ztest-outsider --ignore-not-found --wait=false >/dev/null 2>&1
      $K delete validatingadmissionpolicybinding/ztest validatingadmissionpolicy/ztest --ignore-not-found >/dev/null 2>&1' EXIT

begin "reach stops at the ztest namespaces"
  deny  "pod in kube-system"   < <(pod kube-system)
  allow "pod in ztest"         < <(pod ztest)
  allow "pod in a test env"    < <(pod ztest-fixture)
  # pods/exec is a subresource — `resources: ["*"]` would match neither of these
  assert deny  "exec into kube-system" $K $AS -n kube-system exec "$($K -n kube-system get pod -o name | head -1 | cut -d/ -f2)" -- true
  assert allow "exec in ztest"         $K $AS -n ztest exec no-such-pod -- true
end

begin "namespaces: test envs only, never relabelled"
  deny "create unlabelled"     < <(ns grabby)
  assert deny  "relabel one we did not create" $K $AS label ns ztest-outsider ztest.io/role=test-env --dry-run=server
  assert deny  "delete one we did not create"  $K $AS delete ns ztest-outsider --dry-run=server
  allow "create a test env"    < <(ns ztest-new test-env)
  assert allow "delete a test env" $K $AS delete ns ztest-fixture --dry-run=server
end

begin "pods reach neither the host nor root"
  deny  "hostPath"        < <(pod ztest-fixture "$PLAIN
  volumes: [{name: h, hostPath: {path: /}}]")
  deny  "hostNetwork"     < <(pod ztest-fixture "$PLAIN
  hostNetwork: true")
  deny  "hostPID"         < <(pod ztest-fixture "$PLAIN
  hostPID: true")
  deny  "hostPort"        < <(pod ztest-fixture '  containers: [{name: c, image: b, ports: [{containerPort: 1, hostPort: 1}]}]')
  deny  "privileged"      < <(pod ztest-fixture '  containers: [{name: c, image: b, securityContext: {privileged: true}}]')
  deny  "privileged init" < <(pod ztest-fixture "$PLAIN
  initContainers: [{name: i, image: b, securityContext: {privileged: true}}]")
  deny  "added caps"      < <(pod ztest-fixture '  containers: [{name: c, image: b, securityContext: {capabilities: {add: [SYS_ADMIN]}}}]')
  allow "a plain pod"     < <(pod ztest-fixture)
end

begin "a pod may only name a ztest ServiceAccount"
  deny  "someone else's SA" < <(pod ztest-fixture "$PLAIN
  serviceAccountName: intruder")
  allow "the driver's SA"   < <(pod ztest-fixture "$PLAIN
  serviceAccountName: ztest-driver")
end

begin "RBAC writes are pinned to the driver binding"
  deny  "binding cluster-admin"  < <(rb cluster-admin ztest-driver)
  deny  "binding another SA"     < <(rb ztest-driver ztest-orchestrator)
  assert deny "writing a Role"   $K $AS -n ztest-fixture create role p --verb=get --resource=secrets --dry-run=server
  allow "the driver binding"     < <(rb ztest-driver ztest-driver)
end

begin "other identities are untouched"
  assert allow "an admin writes to kube-system" $K apply --dry-run=server -f /dev/stdin < <(pod kube-system)
end

[ "$failed" -eq 0 ] && echo "all cases passed" || echo "$failed case(s) failed"
exit "$failed"
