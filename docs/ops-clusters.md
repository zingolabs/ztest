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
ztest cluster add dev --kind                      # local kind cluster
ztest cluster add prod --kubeconfig ~/.kube/prod  # remote, config from the file
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
| `kubeconfig`     | file holding that context, when not `~/.kube/config`                                |
| `kind_cluster`   | kind cluster name (`local` only)                                                    |
| `push`           | registry base images are pushed to (`remote` only, required)                        |
| `pull`           | in-cluster pull address, when it differs from `push`                                |
| `storage_driver` | snapshot-capable provisioner to resolve against (probed at `add` when unambiguous)  |
| `runtime`        | host container engine, `docker` or `podman` (probed at `add` from the node's owner) |

Selection precedence:

```
--cluster <name>  >  env already set  >  persisted default  >  ambient env
```

- Persisted default defers to env already set → CI exporting `ZTEST_IMAGE_REGISTRY` is unaffected
- `--cluster` overrides both and must appear **before** the nextest args
- Context is verified at run start; a stale name fails fast, listing available contexts

## One kubeconfig = everything

A shared cluster onboards from a single file — beyond server + token + CA it carries registry config as a
standard kubeconfig extension:

```yaml
clusters:
- name: prod
  cluster:
    server: https://cluster.internal:6443
    certificate-authority-data: <base64 CA>
    extensions:
    - name: ztest.io/registry
      extension:
        push: registry.example.com/ztest-images
        pull: registry.internal.svc:5000/ztest-images
```

`ztest cluster add prod --kubeconfig <file>` reads that extension and records the file's
`current-context`. `pull` is stored only when it differs from `push`.

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
| `ZTEST_CONTAINER_RUNTIME`   | host container engine (`docker` / `podman`)         |

What a cluster must provide: [ops-cluster-requirements.md](ops-cluster-requirements.md).
