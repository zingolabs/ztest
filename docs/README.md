# ztest documentation

`ztest` is a Rust library that boots isolated Zcash topologies on Kubernetes.
Test binaries link it as a dev-dependency and run under `cargo nextest`; each
test gets a fresh, peerable set of `zebrad`/`zaino`/wallet pods and tears them
down on exit.

## Guides — writing and running tests

| Doc                                              | Read it to                                                                                         |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| [guide-writing-tests.md](guide-writing-tests.md) | Write a test: `TestEnv` builder, components, handles, peering, `dev!`, multi-Rust-version matrices |
| [guide-running-tests.md](guide-running-tests.md) | Invoke the suite in dev and CI, slots, filtering, failure modes                                    |

## Operations — running clusters

| Doc                                                        | Read it to                                                                                       |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| [ops-cluster-requirements.md](ops-cluster-requirements.md) | What a cluster must provide before ztest can run on it, and how `ztest cluster check` reports it |
| [ops-local-cluster.md](ops-local-cluster.md)               | Give a local kind cluster snapshot-capable storage (CSI hostpath, or TopoLVM for fast seeds)     |
| [ops-clusters.md](ops-clusters.md)                         | Bind a kube-context + cluster class + registry under a named `ztest cluster` profile             |
| [seed-cdn runbook](../workers/seed-cdn/README.md)          | Operate the read-only Worker that is ztest's seed read path: deploy, verify, repoint `base_uri`  |

## Design — how it works

| Doc                                                      | Covers                                                                                     |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| [design-architecture.md](design-architecture.md)         | K8s substrate: namespaces, ownership/cleanup, networking, seed→snapshot→CoW, observability |
| [design-execution-engine.md](design-execution-engine.md) | Run-loop scheduler and console render thread                                               |
| [design-resources.md](design-resources.md)               | Provider DAG, resource lifetimes, teardown, storage byte-source                            |
| [design-qos.md](design-qos.md)                           | Tiers, capacity model, scheduler, cross-run ledger, calibration                            |
| [design-status.md](design-status.md)                     | `ztest status`: the live cluster view, lease beacons, claim leases, gantt layout           |
| [design-remote-execution.md](design-remote-execution.md) | Pod-per-test, on-cluster compilation, on-cluster image builds                              |
| [design-observability.md](design-observability.md)       | Metrics & profiling planes, Prometheus discovery, what a run records                       |
| [design-sync.md](design-sync.md)                         | Observable chain-sync harness, probe taxonomy, nemesis/chaos layer                         |
| [design-snapshots.md](design-snapshots.md)               | Chain fixtures as pinned build inputs: manifest-as-lockfile, content-addressed bucket      |
| [design-describe.md](design-describe.md)                 | One planner behind `run` and `sync describe`: what a selection actually pulls              |

## How-to

| Doc                                    | Read it to                                                |
| -------------------------------------- | --------------------------------------------------------- |
| [how-to-profile.md](how-to-profile.md) | Collect and read a flamegraph from a component under test |
