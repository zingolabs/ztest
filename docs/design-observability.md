# Observability: metrics & profiling

Two planes, deliberately different postures:

- **Metrics** — cheap, low-cardinality, on for every test. *"Is it keeping up?"*
- **Profiling** — expensive, opt-in per component. *"Where is the CPU going?"*

Both served by the stack `ztest cluster setup` provisions into `ztest-obs`: Prometheus, Pyroscope,
Grafana, as plain Deployments.

## The rule that shapes everything

**No test verdict may depend on the collect plane.** Oracles read components directly
(`SyncSubject::progress` scrapes the subject itself), the load-test gate takes latency from a
client-side hdrhistogram. Prometheus holds the *record*, never the *oracle* — otherwise a scrape outage
becomes a test failure.

## Metrics

### What exists to scrape

Both binaries emit through the `metrics` crate behind a build feature plus one config key, on ports
declared in `src/handles/mod.rs`:

| Binary | Build                   | Config key                | Port | Signals                                                                                                                                   |
| ------ | ----------------------- | ------------------------- | ---- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| zebrad | `--features prometheus` | `[metrics] endpoint_addr` | 9999 | chain tip, verification, peers                                                                                                            |
| zaino  | `--features prometheus` | `metrics_endpoint`        | 9998 | gRPC rate/latency/errors by method, outbound JSON-RPC to the validator, sync lag, block build/write histograms, reorg count/depth, DB tip |

- Zaino's by-method gRPC histograms + outbound-JSON-RPC panel show whether zaino or zebrad is the
  bottleneck directly, no inference
- No LMDB monitoring: zaino's store is a memory-mapped C library with no metrics surface. Monitor zaino,
  whose application-level DB tip and write timing beat raw page counts

### Discovery: no operator, no per-run object

Prometheus' own `kubernetes_sd_configs` keeps a pod iff it carries `ztest.io/component-name` **and**
declares a container port named `metrics`, then promotes ztest's pod labels (`component`,
`component_category`, `test`, `run_id`, `namespace`) to series labels.

- No `PodMonitor`, no prometheus-operator, nothing emitted per component at materialize time — ztest
  creates every pod it wants scraped and already labels it; a CR round-trip would only add a CRD set and
  a controller
- Promoting `run_id` + `test` is what keeps a run's series selectable after its namespace is gone, which
  the report reader depends on

### The contract a component implements

- Declare a container port named `metrics`, serve Prometheus text at `/metrics`
- `impl metrics::Exporter` — `endpoint()` + `rows()`, the component's own table of
  `(label, family, reduction, live?)`

`metrics` names no component; which backend publishes which families is the backends' knowledge
(`backends::metrics_rows`), so a new component joins without editing the metrics module.

### Three readers

| Reader                  | Path                                                    | Load-bearing |
| ----------------------- | ------------------------------------------------------- | ------------ |
| `ztest sync watch`      | scrapes the component directly, 1 s, over a portforward | display only |
| `SyncSubject::progress` | the subject scrapes itself each tick                    | **yes**      |
| `ztest sync status`     | `record::summarize` — `query_range` against Prometheus  | no           |

- Live reader is direct because a display refreshed at the scrape interval lags what it describes
- Record reader runs at report time, reusing the same `Row`/`Reduce` table so a metric cannot mean one
  thing live and another in the report, and omits its whole section on any failure

## Profiling

An eBPF collector pushes CPU and off-CPU profiles to Pyroscope; ztest queries the merged result back and
folds it to collapsed stacks. No component contract — see [how-to-profile.md](how-to-profile.md).

- **No build gate** — out-of-process, so nothing is linked into the component and no image builds twice
- **Run-time gate** — `--profile` (default on), `--profile-hz` (19), `--profile-off-cpu` (0.05)
- **Placement** — sidecar on the driver pod, or a docker container beside the kubelet when the kubelet is
  nested (eBPF reports initial-namespace pids, unresolvable from a pod under kind's node container)
- **ztest owns** placement choice, collector config, Pyroscope Service discovery, and querying
  `SelectMergeStacktraces` with `PROFILE_FORMAT_FLAMEGRAPH` for `ztest sync perf`. Pyroscope's pprof
  encoder returns an empty sample list for these profiles → the flamegraph folds to collapsed stacks
  client-side
- **Push, not file** — makes a profile readable *during* a run and survivable across an OOM kill: no
  flush-on-shutdown step a `SIGKILL` can skip
- Reads resolve the Pyroscope **Service**, then a ready pod behind its selector — not a pod matching the
  chart label. The two differ under microservices mode, where the label also matches components that
  cannot answer a query
- Components push over the legacy `/ingest` endpoint, still served, so the contract is unchanged by v2
  storage. ztest runs Pyroscope v2 single-binary (`target: all`), `filesystem` object-store backend;
  profiles *and* metastore state share one PVC (`ReadWriteMany` would only matter to scale the read path
  across pods, which this install does not do)

### Inherent limitation

`pprof-rs` samples via `SIGPROF` + backtrace with a mandatory `blocklist(["libc","libgcc","pthread", "vdso"])` → **native frames are opaque**: zaino's LMDB C and zebrad's RocksDB C++ appear as leaf nodes.

The result is a faithful *Rust-level* flamegraph — which RPC handler, which codepath, which task burns
CPU — which answers "why is zaino slow" ~95% of the time. Seeing inside the embedded databases needs
host-level eBPF sampling and elevated privileges; out of scope by design.

## Dependencies

`prometheus-parse` for exposition text. `pprof`/`pyroscope` go into **component images only**, never ztest.
