# ztest documentation

`ztest` is a Rust library that boots isolated Zcash topologies on Kubernetes.
Test binaries link it as a dev-dependency and run under `cargo nextest`; each
test gets a fresh, peerable set of `zebrad`/`zaino`/wallet pods and tears them
down on exit.

## Guides — writing and running tests

| Doc | Read it to |
|-----|------------|
| [guide-writing-tests.md](guide-writing-tests.md) | Write a test: `TestEnv` builder, components, handles, peering, `dev!`, multi-Rust-version matrices |
| [guide-running-tests.md](guide-running-tests.md) | Invoke the suite in dev and CI, slots, filtering, failure modes |

## Operations — running clusters

| Doc | Read it to |
|-----|------------|
| [ops-clusters.md](ops-clusters.md) | Bind a kube-context + image backend under a named `ztest cluster` profile; image distribution |
| [ops-production-cluster.md](ops-production-cluster.md) | Stand up and operate the bare-metal NixOS/k3s/Ceph cluster |
| [ops-openshift-setup.md](ops-openshift-setup.md) | Bring up a local CRC/OKD rehearsal cluster; troubleshooting |

## Design — how it works

| Doc | Covers |
|-----|--------|
| [design-architecture.md](design-architecture.md) | K8s substrate: namespaces, ownership/cleanup, networking, seed→snapshot→CoW, observability |
| [design-execution-engine.md](design-execution-engine.md) | Run-loop scheduler and console render thread |
| [design-resources.md](design-resources.md) | Provider DAG, resource lifetimes, teardown, storage byte-source |
| [design-qos.md](design-qos.md) | Tiers, capacity model, scheduler, cross-run ledger, calibration |
| [design-remote-execution.md](design-remote-execution.md) | Pod-per-test, on-cluster compilation, on-cluster image builds |
