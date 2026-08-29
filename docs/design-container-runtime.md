# Container runtime selection (Docker / Podman)

How ztest chooses between Docker and Podman for every host-side container
operation, where that choice is configured, how it is detected, and how the
pieces interact.

Today every host-side container operation shells out to the literal string
`docker` across 19 call sites, with no override anywhere. This describes making
Podman a first-class peer without adding an abstraction layer.

All findings below were measured on Docker 29.6.2, Podman 5.8.4 (rootless),
kind 0.30.0, against live kind clusters under both engines.

## Scope

Only **host-side** paths are affected: `dev!` image builds, the `local_bake`
runner bake, kind side-loading, and the host-placed profiler. The remote
BuildKit path builds on-cluster and spawns no CLI at all, so the runtime is
irrelevant there. Nothing in-pod resolves a runtime — the driver pod
has neither engine installed.

## Layout

```
                          ztest cluster add zkn --kind [--runtime podman]
                                          │
                                          ▼
                             ┌────────────────────────┐
                             │   adopt_runtime()      │  observes, does not guess
                             │   cli/src/cluster/     │
                             └───────────┬────────────┘
              docker ps --filter label=io.x-k8s.kind.role=control-plane
              podman ps --filter label=io.x-k8s.kind.role=control-plane
                                          │  exactly one answers
                                          ▼
                         ~/.config/ztest/clusters.toml
                         [clusters.zkn]  runtime = "podman"
                                          │
   ztest run --cluster zkn                │
            │                             ▼
            │        ┌───────────────────────────────────────┐
            └───────▶│ cluster_config::activate()  (pre-spawn)│
                     │  KUBECONFIG / ZTEST_KUBE_CONTEXT / …    │
                     │  ZTEST_CONTAINER_RUNTIME = "podman"     │
                     └───────────────────┬───────────────────┘
                                         │ process env
                                         ▼
                     ┌───────────────────────────────────────┐
                     │  runtime::active() -> ContainerRuntime │  OnceLock
                     │           src/runtime.rs               │  memoized
                     └───────────────────┬───────────────────┘
                                         │
        ┌────────────────────────────────┼───────────────────────────────┐
        ▼                                ▼                               ▼
  runtime::program()             rt.build_envs()                  rt.kind_envs()
  probe spawns                   build spawns                     kind spawns
  Command::new(..)               run_streamed / proc::run         proc::run_checked
        │                                │                               │
        │              DOCKER_BUILDKIT (docker)         KIND_EXPERIMENTAL_PROVIDER
        │              CONTAINERS_REGISTRIES_CONF (podman)               │
        ▼                                ▼                               ▼
   docker | podman                  docker | podman                     kind
```

Who reads what — no new spawn layer, the engine rides existing seams:

```
  backends/image/docker.rs  ─ build+push, manifest probe ─┐
  backends/image/kind.rs    ─ build, side_load, crictl   ─┤
  pipeline/local_bake.rs    ─ bake, export, publish      ─┼─▶ runtime::active()
  profiling/host.rs         ─ collector container (×9)   ─┤        │
  capability.rs             ─ daemon + rootless probes   ─┘        │
                                                                   ▼
                              ┌──────────────────────────────────────┐
                              │           ContainerRuntime           │
                              ├──────────────────────────────────────┤
                              │ as_str()          │ docker | podman  │
                              │ build_envs()      │ BUILDKIT | CONF  │
                              │ kind_envs()       │ — | PROVIDER     │
                              │ local_tag_prefix()│ "" | localhost/  │
                              │ node_repo_forms() │ crictl repo cols │
                              │ usable()          │ daemon answering │
                              └──────────────────────────────────────┘

  kind::side_load(host, rt, reference)   ← strategy lives with kind, not on the enum
     docker → kind load docker-image
     podman → podman save → kind load image-archive → rm
```

## What actually differs

| Divergence        | Docker                   | Podman                              |
| ----------------- | ------------------------ | ----------------------------------- |
| CLI name          | `docker`                 | `podman`                            |
| BuildKit opt-in   | `DOCKER_BUILDKIT=1`      | not needed                          |
| kind provider     | default                  | `KIND_EXPERIMENTAL_PROVIDER=podman` |
| Bare-tag storage  | `<repo>:<tag>`           | `localhost/<repo>:<tag>`            |
| Short-name `FROM` | resolves to Docker Hub   | **refuses**                         |
| kind side-load    | `kind load docker-image` | `save` + `kind load image-archive`  |

Verified **not** to differ, and so costing no code:

- `manifest inspect` on a remote-only reference contacts the registry under both
  engines (exit 0 present, non-zero absent). No `skopeo` dependency.
- `build` accepts `--target`, `--build-arg`, `RUN --mount=type=cache`, heredoc
  `RUN <<EOF`, and `--output type=local,dest=` identically. The `local_bake`
  recipe needs no Podman variant.
- `ps --filter label=`, `inspect -f`, `port`, `exec … crictl images`, `rm -f`
  are argv-compatible.
- `kind get clusters` prints its provider banner to **stderr**; stdout stays
  clean, so `kind_clusters()` parses correctly as written.

### The `localhost/` prefix reaches further than the existence probe

Podman normalizes locally-built bare names to `localhost/<repo>`. This
propagates all the way into the pod spec — measured end to end:

```
podman build -t ztest-probe:dev-cafe      → localhost/ztest-probe:dev-cafe
kind load image-archive                    → node containerd:
                                             localhost/ztest-probe   dev-cafe
pod image: localhost/ztest-probe:dev-cafe  → Running (imagePullPolicy: Never)
```

So under Podman `Kind::reference()` must return the prefixed form, not just
`exists_in_kind`'s parse — `local_tag_prefix()` feeds the build tag, the load
argument and the pod reference alike, while `node_repo_forms()` covers what
`crictl images` prints.

### `kind load docker-image` is unusable under Podman

kind's own source hardcodes the lookup:

```go
// pkg/cmd/kind/load/docker-image/docker-image.go
cmd := exec.Command("docker", "image", "inspect", "-f", "{{ .Id }}", containerNameOrID)
```

It never consults the podman provider, so the command fails with
`image: "…" not present locally` regardless of the tag form. The error swallows
the cause: it reports the same string whether `docker` is absent from `PATH` or
the image genuinely is not there.

The supported path is the archive form, which takes a tarball and performs no
engine lookup. Verified working from a plain shell:

```
podman save -o <tmp>.tar localhost/<repo>:<tag>
kind load image-archive <tmp>.tar --name <cluster>
```

So side-loading is a strategy, not a constant. It lives beside kind as
`kind::side_load(host, rt, reference)` rather than on the enum: Docker keeps the
one-shot `kind load docker-image`; Podman saves to a scratch tarball, loads the
archive, and removes the tarball on both the success and failure paths. Taking
`rt` explicitly keeps both branches testable against a recording `ChildHost`.

### Short-name resolution

Podman ships no `unqualified-search-registries`, so `FROM debian:bookworm-slim`
fails with *"short-name … did not resolve to an alias"*. This is a deliberate
Podman stance against short-name ambiguity, not an oversight.

ztest owns exactly one Dockerfile (`docker/runner.Dockerfile`, two external
`FROM`s) which is fully qualified at the source. But `dev!` images point at
**user-supplied** Dockerfiles in other repos that ztest cannot edit — and a
Dockerfile building under Docker must build under Podman, or the engines are not
peers.

Resolved per-invocation by pointing Podman at a ztest-owned registries file via
`CONTAINERS_REGISTRIES_CONF`, carried in `build_envs()`. Measured: a bare `FROM`
fails without it and succeeds with it, against a base image proven absent from
local storage first. The user's global `registries.conf` is untouched.

## The type

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContainerRuntime {
    #[default]
    Docker,
    Podman,
}
```

Five members, existing precisely because they are the five places the engines
disagree:

```rust
fn as_str(self) -> &'static str              // config token, env value, spawn program
fn build_envs(self) -> Vec<(&'static str, String)>
fn kind_envs(self) -> Vec<(&'static str, String)>
fn local_tag_prefix(self) -> &'static str    // "" | "localhost/"
fn node_repo_forms(self, repo: &str) -> Vec<String>
fn usable(self) -> bool
```

The side-load *strategy* is not on the enum: it belongs to kind, and lives beside
it as `kind::side_load(host, rt, reference)`, taking the engine explicitly so
both branches stay testable against a recording `ChildHost`.

Deliberately **not** a trait. `ImageProvider` abstracts behaviour — kind
side-loads, Docker pushes, genuinely different algorithms. Docker and Podman run
the same algorithm and differ in four constants and one strategy. A `Copy` enum
serializes into the cluster profile for free, allocates nothing, and gives
exhaustiveness checking if a third engine appears.

## The spawn funnel

No new spawn wrapper — the engine rides the seams that already exist. Probe-style
spawns take the program name; build and `kind` spawns pass the engine's env into
`run_streamed` / `proc::run`, which already accept one:

```rust
- Command::new("docker").args(["exec", &node, "crictl", "images"])
+ Command::new(runtime::program()).args(["exec", &node, "crictl", "images"])

- let envs = [("DOCKER_BUILDKIT", "1".to_string())];
- run_streamed(cx, tag, "docker", &argv, &envs, "docker build").await?;
+ let rt = runtime::active();
+ run_streamed(cx, tag, rt.as_str(), &argv, &rt.build_envs(), "build").await?;
```

`local_bake`'s private `docker()`/`run()` helpers collapse into
`proc::run_checked`, shared with `kind::side_load` so one side-load serves both
the `Kind` provider and the local bake. A test forbids
`Command::new("docker"|"podman")` outside `src/runtime.rs`.

## Where the choice lives

**On the cluster profile**, as `--runtime` on `ztest cluster add`. Not
machine-global, and not a prompt during `ztest cluster setup`.

A kind cluster's nodes *are* containers owned by one engine — measured, with a
Docker `kind` and a Podman `ztest-podman` coexisting on one machine, each
resolving to exactly one owner. A machine-global setting cannot express that.
For a `Remote` profile the engine is only used for build-and-push and is a
machine property, which a per-profile field with detection fallback covers
anyway. Per-profile strictly dominates.

`Profile.storage_driver` is the precedent to copy:

| `storage_driver` (exists)                | `runtime` (new)                             |
| ---------------------------------------- | ------------------------------------------- |
| `--storage-driver` on `cluster add`      | `--runtime` on `cluster add`                |
| `adopt_storage_driver()` probes on add   | `adopt_runtime()` probes on add             |
| `STORAGE_DRIVER_ENV` set by `activate()` | `CONTAINER_RUNTIME_ENV` set by `activate()` |
| `active_storage_driver()` sole read      | `runtime::active()` sole read               |

`runtime` takes `activate()`'s existing `force` rule verbatim — no special case,
so it resolves exactly like `KUBECONFIG` and the registry addresses:

```
explicit --cluster   →  profile field wins over an ambient ZTEST_CONTAINER_RUNTIME
persisted default    →  ambient ZTEST_CONTAINER_RUNTIME wins (CI keeps control)
no profile at all    →  sole live daemon, else Docker
```

`--runtime` is a `cluster add` flag, not a run-time one: it edits the profile,
which is then the thing every command reads.

`active()` memoizes in a `OnceLock` — detection shells out and must not run 19
times. Safe because `activate()` runs pre-spawn, before any read.

No prompt in `ztest cluster setup`: setup provisions cluster-side resources, and
by then the engine is already decided. Unlike the csi-hostpath prompt, which
offers a real trade-off, this question has one correct answer.

## Detection

For a local profile the answer is an observation, not a heuristic, and stays
unambiguous even with both engines running clusters:

```
cluster kind          → docker:owns  podman:—
cluster ztest-podman  → docker:—     podman:owns
```

`kind get clusters` likewise partitions cleanly per provider. **A Local profile
therefore never hits the ambiguous case.**

For a remote profile, or before any cluster exists, there is no node to observe.
Falls back to a daemon-reachability probe — a CLI on `PATH` *with a live daemon
behind it*, generalizing today's `docker_usable()`. A bare `which docker`
reintroduces the failure that check exists to prevent: a client with no daemon
passes `check` and fails much later at `sync start`.

## Capability surface

`ztest cluster check` reports the resolved runtime on its header line, beside
the context and cluster class.

Rootless Podman cannot host the eBPF collector: `--pid=host` in a rootless
container shows only the invoking user's processes, and the collector's premise
is sharing the pid namespace eBPF measures in. Reported as `Finding::Absent`
with a remedy naming rootful Podman — never attempted. A silently-empty
flamegraph is worse than a declared missing capability.

## Host requirements (Podman only)

Neither is fixable from ztest; both belong in `ops-local-cluster.md`.

- **Creating** a kind cluster under rootless Podman needs cgroup delegation
  (`Delegate=yes` on `user@.service`) *and* a process cgroup that actually
  carries the controllers. A desktop terminal typically sits in
  `user@.service/session.slice/<term>.scope`, which carries only `memory pids`
  even when delegation is configured, so kind refuses. Creating the cluster
  from a delegated transient scope works:

  ```
  systemd-run --user --scope -p Delegate=yes -- \
    env KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name <name>
  ```

  ztest never creates clusters, and every ztest operation — including
  `kind load image-archive` — was verified to work from a plain shell. This
  affects only the operator bootstrapping a cluster.

- **Rootful Podman** is required for host-side profiling, per above.

## Diff budget

| File                         | Change                                                   |
| ---------------------------- | -------------------------------------------------------- |
| `src/runtime.rs`             | new, ~150 lines incl. tests                              |
| `src/proc.rs`                | `run_checked` — net removal, two private copies collapse |
| `src/cluster_config.rs`      | +1 field, +1 `apply()` line, +1 const                    |
| `cli/src/cluster/mod.rs`     | +1 flag, +~20 line `adopt_runtime()`                     |
| 19 spawn sites               | 1 line each, net negative                                |
| `src/backends/image/kind.rs` | prefix + side-load through the enum                      |
| `src/capability.rs`          | `docker_usable` → `runtime_usable`; +1 rootless arm      |
| `docker/runner.Dockerfile`   | 2 `FROM`s fully qualified                                |

No new trait, no dyn dispatch, no new config file, no new command, no prompt.
