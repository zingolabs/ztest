# `ztest` — Design Docs

A Rust library that boots Zcash network topologies (validators, indexers,
wallets) on Kubernetes and hands typed RPC handles back to test code.
Linked into test binaries as a `dev-dependency` and driven by `cargo nextest`; there is no daemon, no CLI, no IDL. Sibling to
`infrastructure/zcash_local_net/`, which runs the same shape in-process —
tests written against one port to the other with cosmetic changes.

The library is the integration-test backend for `zaino` and related
crates today. It is not bound to that role: anything wanting a fresh,
isolated, peerable Zcash topology in CI can depend on it.

1. **[Test-author API](test-author-api.md)** — the Rust surface: how to
   declare components (`Validator::zebrad(...)`), attach configs and
   seeded data dirs (`mount_config!` / `mount_archive!`), wire peers,
   and dial endpoints. Start here if you're writing a test.

1. **[Running tests](running-tests.md)** — invocation: `cargo nextest`
   in dev and CI, slot semantics, namespace naming, the `hash:N/M`
   partitioning pattern. Read second if you're debugging *why* your
   test ran (or didn't) the way it did.

1. **[Architecture](architecture-overview.md)** — what happens between
   the test calling `TestEnv::build()` and a pod being dialable: the
   per-slot namespace model, sentinel-ConfigMap ownership cascade,
   content-addressed archive PVCs, the cross-namespace shadow-VSC
   clone, in-cluster-direct vs port-forward endpoint routing. Read
   when a test breaks in a way the API docs don't explain.

1. **[Cluster administration](cluster-administration.md)** — the
   Kubernetes cluster the library targets: NixOS + k3s + Cilium +
   Rook-Ceph + ARC Scale Sets, on bare metal behind Tailscale. Read if
   you operate the cluster or are bootstrapping a new one.

TODO: Add quality of service annotations to tests. ServiceAccounts will have the authorized level of service it can provide

All pods should have requests and limits. Limits will be tiered based on QOS classes
