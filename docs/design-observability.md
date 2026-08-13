# Observability: metrics & profiling

Two planes with deliberately different postures:

- **Metrics** — cheap, low-cardinality, on for every test. *"Is it keeping up?"*
- **Profiling** — expensive, opt-in per component. *"Where is the CPU going?"*

Both are served by the stack `ztest cluster setup` provisions into `ztest-obs`:
Prometheus, Pyroscope, and Grafana, as plain Deployments.

## The rule that shapes everything

**No test verdict may depend on the collect plane.** Oracles read components
directly (`SyncSubject::progress` scrapes the subject itself); the load-test gate
takes its latency from a client-side hdrhistogram. Prometheus holds the
*record*, never the *oracle* — otherwise a scrape outage becomes a test failure.

## Metrics

### What exists to scrape

Both binaries emit through the `metrics` crate behind a build feature plus one
config key, on ports declared in `src/handles/mod.rs`:

| Binary | Build | Config key | Port | Signals |
| --- | --- | --- | --- | --- |
| zebrad | `--features prometheus` | `[metrics] endpoint_addr` | 9999 | chain tip, verification, peers |
| zaino | `--features prometheus` | `metrics_endpoint` | 9998 | gRPC rate/latency/errors by method, outbound JSON-RPC to the validator, sync lag, block build/write histograms, reorg count/depth, DB tip |

Zaino's by-method gRPC histograms and its outbound-JSON-RPC panel show *whether
zaino or zebrad is the bottleneck* directly, without inference.

> You do not monitor LMDB. Zaino's store is a memory-mapped C library with no
> metrics surface. You monitor zaino, whose application-level metrics cover the
> DB tip and write timing more usefully than raw page counts would.

### Discovery: no operator, no per-run object

Prometheus' own `kubernetes_sd_configs` keeps a pod iff it carries
`ztest.io/component-name` **and** declares a container port named `metrics`, then
promotes ztest's pod labels (`component`, `component_category`, `test`,
`run_id`, `namespace`) to series labels.

That is the whole mechanism. There is no `PodMonitor`, no prometheus-operator,
and nothing emitted per component at materialize time — ztest creates every pod
it wants scraped and already labels it, so translating that into a CR and back
would only add a CRD set and a controller.

Promoting `run_id` and `test` is what makes a run's series selectable after its
namespace is gone, which is what the report reader below depends on.

### The contract a component implements

One port name and one trait:

- declare a container port named `metrics`, serve Prometheus text at `/metrics`;
- `impl metrics::Exporter` — `endpoint()` and `rows()`, the component's own table
  of `(label, family, reduction, live?)`.

`metrics` names no component; which backend publishes which families is the
backends' knowledge (`backends::metrics_rows`), so a new component joins without
editing the metrics module.

### Three readers

| Reader | Path | Load-bearing |
| --- | --- | --- |
| `ztest sync watch` | scrapes the component directly, 1 s, over a portforward | display only |
| `SyncSubject::progress` | the subject scrapes itself each tick | **yes** |
| `ztest sync status` | `record::summarize` — `query_range` against Prometheus | no |

The live reader is direct because a display refreshed at the scrape interval
lags the thing it describes. The record reader runs at report time, reuses the
same `Row`/`Reduce` table so a metric cannot mean one thing live and another in
the report, and omits its whole section on any failure.

## Profiling

Components push CPU profiles to Pyroscope; ztest queries the merged result back
as pprof. The component contract is [how-to-profile.md](how-to-profile.md).

- **Build gate**: a cargo `profile` feature, flipped by a Docker build `ARG`.
  Linking is a build decision — a runtime env cannot pull in a crate.
- **Run-time gate**: `ZTEST_PROFILE_URL`. Its presence is the switch.
  `ZTEST_PROFILE_TAGS` carries the labels ztest scopes queries by,
  `ZTEST_PROFILE_HZ` the sample rate (default 100).
- **ztest owns**: discovering the Pyroscope Service, injecting those variables at
  materialize, and querying `SelectMergeStacktraces` with `PROFILE_FORMAT_PPROF`
  for `ztest sync perf`. (The older `SelectMergeProfile` is deprecated upstream.)

Pushing rather than writing a file is what makes a profile readable *during* a
run and what makes one survive an OOM kill: there is no flush-on-shutdown step a
`SIGKILL` can skip.

Reads resolve the Pyroscope **Service**, then a ready pod behind its selector —
not a pod matching the chart label. The two differ under microservices mode,
where the label also matches components that cannot answer a query.

Components push over the legacy `/ingest` endpoint, which Pyroscope still
serves, so the contract above is unchanged by the v2 storage architecture. ztest
runs Pyroscope v2 single-binary (`target: all`) with the `filesystem` object-store
backend; profiles *and* metastore state share one PVC. `ReadWriteMany` would only
be needed to scale the read path across pods, which this install does not do.

### Inherent limitation

`pprof-rs` samples via `SIGPROF` + backtrace with a mandatory
`blocklist(["libc","libgcc","pthread","vdso"])`, so **native frames are opaque**:
zaino's LMDB C and zebrad's RocksDB C++ appear as leaf nodes. The result is a
faithful *Rust-level* flamegraph — which RPC handler, which codepath, which task
burns CPU — which answers "why is zaino slow" ~95% of the time. Seeing inside
the embedded databases needs host-level eBPF sampling and elevated privileges;
out of scope by design.

## Dependencies

`prometheus-parse` for exposition text. `pprof`/`pyroscope` are added to
**component images only**, never to ztest.
