# Cluster profiles & the image registry

A **cluster profile** makes `ztest run` agree on three things at once: which **kube-context** API calls target, how **images** reach the cluster (`kind load` vs registry push), and whether the target is **OpenShift**. `ztest cluster` manages profiles; `ztest run --cluster <name>` (or a persisted default) selects one.

## `ztest cluster`

```
ztest cluster list                 # profiles, * marks the active default
ztest cluster current              # the active default
ztest cluster add <name> …         # create/update a profile
ztest cluster set <name>           # make <name> the default
ztest cluster remove <name>        # delete (clears default if it pointed here)
```

A profile has exactly one source — `--kind` or `--kubeconfig` (mutually exclusive). Add `--set-default` to also make it the default (the first profile becomes default automatically).

```
# local kind, addressed by name: context derived as kind-<cluster>
ztest cluster add zkn --kind                # kind cluster name defaults to <name>
ztest cluster add local --kind zkn          # profile name ≠ kind cluster name

# remote: context = the file's current-context; registry config comes from a
# ztest.io/registry kubeconfig extension (see "One kubeconfig")
ztest cluster add crc --kubeconfig ~/.kube/crc.yaml
```

Profiles live in `$XDG_CONFIG_HOME/ztest/clusters.toml` (else `~/.config/ztest/clusters.toml`). Fields:

| field | meaning |
|-------|---------|
| `context` | kube-context to target — resolved in-memory; your kubeconfig is never modified |
| `kubeconfig` | file holding that context when not the default `~/.kube/config`; sets `KUBECONFIG` for the run |
| `backend` | image distribution: `kind` (`kind load`), `registry` (build + push to a generic registry), or `openshift` (on-cluster build into the integrated registry). Read by both `ztest setup` and `ztest run`. |
| `push` | registry base images are pushed to (route, or e.g. `ghcr.io/zingolabs`); required for `registry`/`openshift` |
| `pull` | in-cluster pull address, `openshift` backend only (pods reference this, not `push`) |
| `kind_cluster` | kind cluster name (`kind` backend only) — `kind load` into `<name>-control-plane` |

### Selection precedence

```
--cluster <name>  >  env vars already set  >  persisted current  >  built-in kind defaults
```

The persisted default defers to env already set, so CI (which exports `ZTEST_IMAGE_REGISTRY`) is unaffected; an explicit `--cluster` overrides both. `--cluster` must appear **before** the nextest args (`ztest run --cluster crc -p mytests`). The profile's context is verified against the kubeconfig at run start; a stale name fails fast, listing available contexts.

## One kubeconfig = everything

A shared cluster is onboarded with a single kubeconfig file. Beyond server + SA token + CA, it carries registry config as a standard kubeconfig **extension** on the cluster:

```yaml
clusters:
- name: crc
  cluster:
    server: https://100.64.0.3:6443
    certificate-authority-data: <base64 CA — validates the API and the registry route>
    extensions:
    - name: ztest.io/registry
      extension:
        push: default-route-openshift-image-registry.apps-crc.testing/ztest-images
        pull: image-registry.openshift-image-registry.svc:5000/ztest-images
        openshift: true
contexts:
- name: crc-remote
  context: { cluster: crc, user: ztest-sa }
users:
- name: ztest-sa
  user: { token: sha256~… }
```

`ztest cluster add crc --kubeconfig ~/.kube/crc.yaml` reads the `ztest.io/registry` extension, derives `backend` (`openshift: true` → `openshift`, else `registry`) and the addresses, and records the file's `current-context`. A generic registry uses the same extension with `openshift: false`.

One file suffices because the same SA **token** authenticates both the kube client and the registry push, and the same **CA** validates both the API server and the registry route (on CRC both are signed by the ingress CA).

## Backends

- **kind** (no `push`/`pull`): `docker build` + `kind load` into `<kind_cluster>-control-plane`. The local-dev default.
- **Generic registry** (`push` only): `docker build` + `docker push` via the ambient `docker` credentials; pods pull the same address, optionally with `ZTEST_IMAGE_PULL_SECRET`.
- **OpenShift** (distinct `push`/`pull`): images are built **on the cluster** in a ztest-owned BuildKit pod and pushed to the integrated registry; pods pull in-cluster via the `pull` service address with no pull secret. See [design-remote-execution.md](design-remote-execution.md) for the build flow.

### Cluster-side prerequisites (OpenShift)

`ztest setup --target okd` (run once, admin kubeconfig) provisions:

- the `ztest-images` project (`policy::IMAGES_NAMESPACE`);
- the BuildKit build server (`resource::impls::buildkit`): custom SCC, ServiceAccount, `buildkitd.toml` ConfigMap, cache PVC, and Deployment — see [design-remote-execution.md](design-remote-execution.md);
- the `ztest-image-push` role on `ztest-images`, bound to `ztest/ztest` and `ztest/ztest-buildkit`, granting `imagestreams: create` + `imagestreams/layers: get,update` (plain `system:image-pusher` lacks imagestream **create**, so a never-seen image's first push is denied);
- `system:image-puller` on `ztest-images` for `system:serviceaccounts` (why no pull secret is needed);
- the SCC grant for per-test pods.

The run SA's cluster read permissions (`nodes` for the QoS probe, `volumesnapshotclasses`/`storageclasses` for seeding) come from the same `ztest-remote` ClusterRole, sourced from `policy::RUN_RULES`, which also drives a run-start `SelfSubjectAccessReview` self-check that fails fast naming any missing permission. The build path needs only `pods/exec`, no `build.openshift.io` grants.

See [ops-openshift-setup.md](ops-openshift-setup.md) for bringing up CRC/OKD and [ops-production-cluster.md](ops-production-cluster.md) for the production target.

## Environment variables

Activation sets these from the profile; setting them directly still works (env beats the persisted default).

| var | meaning |
|-----|---------|
| `ZTEST_KUBE_CONTEXT` | kube-context to target in-memory |
| `KUBECONFIG` | kubeconfig file (also the token+CA source for the push) |
| `ZTEST_IMAGE_BACKEND` | `kind` / `registry` / `openshift`; with no profile, inferred from the registry vars below |
| `ZTEST_IMAGE_REGISTRY` | pull base (what pods reference) |
| `ZTEST_IMAGE_PUSH_REGISTRY` | distinct push base → OpenShift integrated-registry mode |
| `KIND_CLUSTER` | kind cluster name |
| `ZTEST_IMAGE_PULL_SECRET` | pod `imagePullSecrets` name (ignored in OpenShift internal mode) |
