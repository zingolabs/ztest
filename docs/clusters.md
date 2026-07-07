# Cluster profiles & the image registry

`ztest run` has to agree on three otherwise-independent things before it can do
anything: which **kube-context** the API calls go to, how **images** reach the
cluster (`kind load` vs a registry push), and whether the target is
**OpenShift** (needs the SCC grant + integrated-registry project). Left to the
ambient environment these drift apart — the classic failure is building images
into a local kind node while the kube-context points at a remote cluster, or
vice versa, with nothing warning you.

A **cluster profile** binds all three under one name. `ztest cluster` manages
them; `ztest run --cluster <name>` (or a persisted default) selects a whole
target at once.

## `ztest cluster`

```
ztest cluster list                 # profiles, * marks the active default
ztest cluster current              # the active default
ztest cluster add <name> …         # create/update a profile (see below)
ztest cluster set <name>           # make <name> the default
ztest cluster remove <name>        # delete (clears the default if it pointed here)
```

A profile has **one of two sources**, matching how each cluster type is actually
addressed — a local kind cluster by name, a remote cluster by file:

```
# local kind, addressed by name: context derived as kind-<cluster>
ztest cluster add zkn --kind                # kind cluster name defaults to <name>
ztest cluster add local --kind zkn          # profile name ≠ kind cluster name

# remote, described by a kubeconfig: context is the file's current-context and a
# `ztest.io/registry` extension supplies the registry config (OpenShift or
# generic) — see "One kubeconfig = everything" below
ztest cluster add crc --kubeconfig ~/.kube/crc.yaml
```

`--kind` and `--kubeconfig` are the two sources and are mutually exclusive —
nothing about the context or the registry is typed on the command line. A kind
context is derived (`kind-<cluster>`); a remote's context and registry both come
from the kubeconfig. Add `--set-default` to also make it the default (the first
profile becomes the default automatically).

Profiles live in `$XDG_CONFIG_HOME/ztest/clusters.toml` (else
`~/.config/ztest/clusters.toml`). A profile records:

| field | meaning |
|-------|---------|
| `context` | kube-context to target — resolved **in-memory**; your kubeconfig is never modified |
| `kubeconfig` | the file holding that context, when it isn't the default `~/.kube/config`; sets `KUBECONFIG` for the run |
| `push` | registry base images are pushed to (a route, or e.g. `ghcr.io/zingolabs`) |
| `pull` | in-cluster pull address, **only** for the OpenShift integrated registry (pods reference this, not `push`) |
| `kind_cluster` | kind cluster name (mutually exclusive with `push`) — `kind load` into `<name>-control-plane` |
| `openshift` | provision/expect the OpenShift-only policy (SCC grant, registry project) |

### Selection precedence

At run time the active profile is resolved as:

```
--cluster <name>  >  environment variables already set  >  persisted `current`  >  built-in kind defaults
```

An explicit `--cluster` flag overrides any pre-set env; the persisted default
defers to env that's already set, so CI (which exports `ZTEST_IMAGE_REGISTRY`)
is unaffected. The `--cluster` flag must appear **before** the nextest args
(`ztest run --cluster crc -p mytests`), the same constraint nextest's own global
flags have.

The profile's context is verified against the kubeconfig at run start, so a
stale name fails fast — listing the available contexts — rather than silently
falling through to the current context or dying on a cryptic auth error later.

## One kubeconfig = everything

The best way to onboard a developer to a shared cluster is to hand them **one
kubeconfig file**. Beyond the usual server + SA token + CA, a ztest kubeconfig
carries the registry configuration as a standard kubeconfig **extension** on the
cluster:

```yaml
clusters:
- name: crc
  cluster:
    server: https://100.64.0.3:6443
    certificate-authority-data: <base64 CA — validates the API *and* the registry route>
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

The developer then runs, with no other flags:

```
ztest cluster add crc --kubeconfig ~/.kube/crc.yaml
```

`cluster add` reads the `ztest.io/registry` extension and derives `push`, `pull`,
and `openshift`; it records the file's `current-context` so the profile is
self-describing. A **generic** registry lives in the same extension — set
`push` == `pull` and `openshift: false` — so registry config always has one home,
the kubeconfig, never a command-line flag.

Why one file suffices: the same SA **token** authenticates both the kube client
and the registry push, and the same **CA** in `certificate-authority-data`
validates both the API server and the registry route (on CRC both are signed by
the ingress CA). Nothing else is needed on the developer's machine — see the
push mechanics below.

## The OpenShift integrated-registry push

For a profile with a distinct `push`/`pull` (OpenShift), ztest pushes images
**itself**, over HTTPS, rather than shelling out to `docker push`. This is what
makes the one-file model real: no `docker login`, no `oc`, no
`/etc/docker/certs.d`, no per-developer `sudo`.

The flow, per image, during preflight (`ImageProvider` →
`backends::oci`):

1. **Build to an OCI layout.** `docker buildx build --output type=oci` produces
   registry-ready blobs (correct gzip + digests, unlike `docker save`). The
   default `docker` buildx driver can't export OCI, so ztest ensures a
   `docker-container` driver builder named `ztest` exists (created once,
   idempotently).
2. **Push in-process.** ztest reads the OCI layout and uploads each blob +
   the manifest with `reqwest`, authenticating via OpenShift's standard
   `Basic(sa:token) → Bearer` registry token handshake. The **token and CA come
   straight from the kubeconfig** (`KUBECONFIG` / `ZTEST_KUBE_CONTEXT`); the CA
   is added as a trusted root for the route's TLS. Blobs already in the registry
   are skipped (content-address dedup), and per-blob progress is reported to the
   run's transfer panel.
3. **Pods pull via the service.** Pod specs reference the `pull` address
   (`image-registry.openshift-image-registry.svc:5000/…`), so the kubelet pulls
   in-cluster using the pod SA's auto-injected registry credentials — **no pull
   secret, no route cert on nodes.**

### Cluster-side prerequisites

`ztest setup --target okd` (run once, with an admin kubeconfig) provisions the
pieces the push and pull rely on:

- the `ztest-images` project (`policy::IMAGES_NAMESPACE`);
- the `ztest-image-push` role on `ztest-images` for the run SA `ztest/ztest`,
  bound as `ztest-image-builder` — it grants `imagestreams: create` plus
  `imagestreams/layers: get,update`. Plain `system:image-pusher` is *not* enough:
  it lacks imagestream **create**, so the first push of a never-seen image is
  denied (the registry must create the imagestream on first push);
- `system:image-puller` on `ztest-images` for `system:serviceaccounts` (so every
  pod SA can pull — this is why no pull secret is needed);
- the SCC grant.

The run SA's cluster read permissions (`nodes` for the QoS probe,
`volumesnapshotclasses`/`storageclasses` for seeding) are part of the same
`ztest-remote` ClusterRole `setup` provisions — there is nothing to grant
out-of-band. They come from a single source (`policy::RUN_RULES`) that also
drives a run-start `SelfSubjectAccessReview` self-check: a stale grant makes
`ztest run` fail fast naming the exact missing permission, rather than 403-ing
deep in a run.

See **[Local OpenShift (crc) setup](openshift-cluster-setup.md)** for bringing
up the cluster itself, and **[Cluster administration](cluster-administration.md)**
for the production target.

## Generic registry & kind — unchanged

- **kind** (no `push`/`pull`): `docker build` + `kind load` into
  `<kind_cluster>-control-plane`. The local-dev default.
- **Generic registry** (`push` only, e.g. `ghcr.io/zingolabs`): `docker build` +
  `docker push`; pods pull the same address, optionally with
  `ZTEST_IMAGE_PULL_SECRET`. Uses the ambient `docker` credentials — the
  in-process push is OpenShift-only for now.

## Environment variables

Activation sets these from the profile; setting them directly still works (env
beats the persisted default), so CI and one-off overrides are unaffected:

| var | meaning |
|-----|---------|
| `ZTEST_KUBE_CONTEXT` | kube-context to target in-memory |
| `KUBECONFIG` | kubeconfig file (also the token+CA source for the push) |
| `ZTEST_IMAGE_REGISTRY` | pull base (what pods reference) |
| `ZTEST_IMAGE_PUSH_REGISTRY` | distinct push base → OpenShift integrated-registry mode |
| `KIND_CLUSTER` | kind cluster name |
| `ZTEST_IMAGE_PULL_SECRET` | pod `imagePullSecrets` name (ignored in OpenShift internal mode) |
