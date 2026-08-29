# Cluster profiles

A profile binds kube-context + cluster class + registry addresses under one name, so
`ztest run --cluster <name>` selects them together instead of from independent ambient signals.

```
ztest cluster list                 # profiles, * marks the active default
ztest cluster current
ztest cluster add <name> …         # create/update
ztest cluster set <name>           # make it the default
ztest cluster remove <name>
ztest cluster check                # what this cluster can do
```

```bash
ztest cluster add dev --kind                          # local kind cluster
ztest cluster add prod --kube-context admin@prod \
  --extra-config ./cluster.toml                       # remote: identity + cluster facts
```

First profile added becomes the default; `--set-default` on any later one.

## The two classes

|            | `local`                                 | `remote`                             |
| ---------- | --------------------------------------- | ------------------------------------ |
| what it is | a kind cluster on this machine          | any cluster reached over the network |
| images     | built here, `kind load`ed into the node | built in the on-cluster BuildKit pod |
| registry   | none                                    | required                             |

No third axis — where the build happens follows from the class, because a remote cluster is precisely one
this machine is not part of.

## `clusters.toml`

At `$XDG_CONFIG_HOME/ztest/clusters.toml`, else `~/.config/ztest/clusters.toml`.

| field            | meaning                                                                             |
| ---------------- | ----------------------------------------------------------------------------------- |
| `class`          | `local` or `remote`                                                                 |
| `context`        | kube-context to target — resolved in-memory; your kubeconfig is never modified      |
| `push`           | registry base images are pushed to (`remote` only, required)                        |
| `pull`           | in-cluster pull address, when it differs from `push`                                |
| `push_secret`    | dockerconfigjson Secret the build pod pushes with; unset = anonymous                |
| `storage_driver` | snapshot-capable provisioner to resolve against (probed at `add` when unambiguous)  |
| `storage_class`  | StorageClass named outright; set with `snapshot_class` or not at all                |
| `snapshot_class` | VolumeSnapshotClass seed clones bind through, where one driver serves several       |
| `runtime`        | host container engine, `docker` or `podman` (probed at `add` from the node's owner) |

Everything but `context` and `runtime` can be supplied by `--extra-config`.

Selection precedence:

```
--cluster <name>  >  env already set  >  persisted default  >  ambient env
```

- Persisted default defers to env already set → CI exporting `ZTEST_IMAGE_REGISTRY` is unaffected
- `--cluster` overrides both and must appear **before** the nextest args
- Context is verified at run start; a stale name fails fast, listing available contexts

## Identity and facts arrive separately

A profile answers two questions with different lifetimes, so they onboard from different places.

**Who you are** is a kube-context, named with `--kube-context`. Selection happens inside whichever
kubeconfig the run already reads — the ambient `KUBECONFIG`, else `~/.kube/config` — and ztest never
modifies it. A cluster you have been handed a kubeconfig for is merged in with `kubectl config` first.

**What the cluster is** — registry, storage — is public, identical for everyone, and changes when the
cluster changes. It arrives as `--extra-config`, a TOML document from a path or an `https://` URL:

```toml
[ztest]
class          = "remote"
push           = "registry.example.com/ztest-images"
pull           = "registry.internal.svc:5000/ztest-images"
push_secret    = "ztest-registry-creds"
storage_class  = "topolvm-thin"
snapshot_class = "ztest-snapshot"
```

```bash
ztest cluster add prod \
  --kube-context admin@prod \
  --extra-config https://raw.githubusercontent.com/<org>/<repo>/main/cluster.toml
```

Keeping them apart is what lets the facts live in the git repo that establishes them, reviewed in the
same change that moves a storage class — instead of travelling through the secret channel a credential
needs, and being reissued to everyone whenever the cluster moves.

Sections other than `[ztest]` are ignored, so one file can describe a cluster to several tools.

### What a fetched config may do

`push` and `pull` decide which registry ztest pulls a runner image from and executes in the cluster.
Whoever serves the file chooses them, so:

- `https://` only — a custom redirect policy re-checks the scheme on every hop, and `http://` is refused
  outright rather than falling through to a filename
- Body is capped as it arrives, never trusted from `Content-Length`
- `context` is not a field; naming it fails, as does any unknown key
- Every value is rejected outright if it carries a control character — the echo below is rendered
  verbatim, so ESC or `\r` could paint a benign registry over the real one
- Every value is echoed before anything is written, and a fetched one is confirmed. Off a terminal there
  is nobody to read the echo, so `--yes` is required

TLS answers a network attacker, not a compromised source. The echo is what makes the registry visible at
the one moment a person is present.

## Environment variables

Activation sets these from the profile; setting them directly still works.

| var                         | meaning                                             |
| --------------------------- | --------------------------------------------------- |
| `ZTEST_CLUSTER_CLASS`       | `local` / `remote`                                  |
| `ZTEST_KUBE_CONTEXT`        | kube-context to target in-memory                    |
| `KUBECONFIG`                | kubeconfig file                                     |
| `ZTEST_IMAGE_REGISTRY`      | pull base (what pods reference)                     |
| `ZTEST_IMAGE_PUSH_REGISTRY` | push base, when it differs                          |
| `ZTEST_IMAGE_PULL_SECRET`   | pod `imagePullSecrets` name, for a private registry |
| `ZTEST_IMAGE_PUSH_SECRET`   | dockerconfigjson Secret the builder pushes with     |
| `ZTEST_STORAGE_CLASS`       | StorageClass override, with the next one or neither |
| `ZTEST_VOLUMESNAPSHOT_CLASS`| VolumeSnapshotClass override                        |
| `ZTEST_CONTAINER_RUNTIME`   | host container engine (`docker` / `podman`)         |

What a cluster must provide: [ops-cluster-requirements.md](ops-cluster-requirements.md).
