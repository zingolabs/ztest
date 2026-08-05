# Observability: metrics & profiling

Two independent planes for looking inside a running test, with deliberately
different postures:

- **Metrics** — cheap, low-cardinality, safe to leave on for every test. Answers
  *"is the system healthy and keeping up?"* Default-on for testnet/sync tests.
- **Profiling** — expensive, opt-in, per-component. Answers *"where is the CPU
  going?"* Off unless a test asks for it.

The load/perf *gate* itself lives in [design-load-testing.md](design-load-testing.md)
(client-side hdrhistogram); this doc is the *server-side* observability that
sits underneath and beside it.

## The two premises this corrects

1. **You do not monitor LMDB.** Zaino's store is LMDB (`lmdb`/`lmdb-sys`), a
   memory-mapped C library with zero network or metrics surface — there is
   nothing to "turn on." You monitor **zaino** and **zebrad**, both of which
   expose a Prometheus `/metrics` endpoint behind a compile-time feature
   (zaino added this 2026-06-24; zebra has had `[metrics]` for years). Zaino's
   own metrics already cover the DB tip and write timing at the application
   level — richer and more relevant than raw LMDB page counts.
2. **You do not inject a profiler into third-party pods.** Profiling zebra/zaino
   is a *contract the component image opts into* ([how-to-profile.md](how-to-profile.md)),
   not a sidecar ztest forces in. This keeps the whole profiling plane at zero
   privilege — no custom SCC, no `hostPID`, no `shareProcessNamespace`.

## Metrics plane

### What exists to scrape

Both binaries emit through the `metrics` crate; enabling is a build feature plus
one config key, bound to ports ztest already declares in
`src/handles/mod.rs` (`ZEBRAD_METRICS=9999`, `ZAINO_METRICS=9998`):

| Binary | Build | Config key | Signals |
| --- | --- | --- | --- |
| zebrad | `--features prometheus` | `[metrics] endpoint_addr` | chain tip, verification, peers, `zebrad_build_info` |
| zaino | `--features prometheus` (or `no_tls_with_prometheus`) | `metrics_endpoint` | **inbound gRPC rate/latency/errors by method**, **outbound JSON-RPC to the validator** (rate/latency/errors/retries), sync lag + block build/write histograms, tx/output/action counters, reorg count/depth, mempool size, DB tip height, `zainod.build_info` |

Zaino's by-method gRPC histograms and its outbound-JSON-RPC panel are the
load-test signal that matters: they show *whether zaino or zebrad is the
bottleneck* directly, without inference.

Gaps that would need bespoke pollers (deferred — not needed for the health
picture): raw LMDB `mdb_env_stat` internals, zebrad RocksDB `Statistics`, and
per-process CPU/RSS/fds (add a standard process collector only if a test needs
it).

### Discovery & scraping: cluster-native, no ztest-owned Prometheus

The mechanism differs by cluster, because the discovery contract does:

- **OpenShift/OKD — User Workload Monitoring (UWM).** The supported path for
  scraping *user* workloads. UWM **ignores `prometheus.io/scrape` annotations**;
  it discovers targets via `ServiceMonitor`/`PodMonitor` CRs
  (`monitoring.coreos.com`). ztest emits one **`PodMonitor` per profiled
  component**, carrying the run-id label, and reads back from the
  **thanos-querier** endpoint keyed on that label. No ztest-owned Prometheus,
  no Grafana (OKD ships neither for user workloads by default — query via the
  console or thanos-querier).
  - **Precondition:** UWM must be enabled (`enableUserWorkload: true` in the
    `cluster-monitoring-config` ConfigMap). Platform monitoring being on does
    **not** imply UWM is on. Confirm before relying on this plane.
  - **Cadence caveat:** UWM enforces a scrape-interval floor (~5–15 s;
    default 30 s). Perfect for **sync/testnet** tests (minutes-to-hours), but
    too coarse to compute a p99 over a 30 s load burst. That is fine, because
    the load-test *gate* takes its latency from the client-side hdrhistogram
    (design-load-testing.md); UWM carries the durable server-side record, not
    the sub-second gate.
- **kind / plain Prometheus (dev/local).** The `prometheus.io/scrape=true` +
  `prometheus.io/port=<n>` annotation scheme from
  [design-architecture.md](design-architecture.md) remains valid for a
  self-hosted Prometheus. `PodSpec::render` must gain an annotations field to
  emit it (unimplemented today).

This supersedes the assumption in design-architecture.md that a single
ztest-provisioned `observability` Prometheus scrapes annotations everywhere:
on OKD the cluster owns Prometheus and the annotation scheme does not apply.

### Division of labor

- **Server-side durable record** (all testnet/sync tests): UWM, queried by
  run-id. This is the "track zaino/zebrad for every testnet and sync test" ask.
- **Load-test perf gate** (30 s bursts): client-side hdrhistogram in the
  `LoadReport`. Independent of scrape cadence.

## Profiling plane

**A contract, not an injection.** ztest publishes how a component image makes
itself profileable; the component team implements it in their own source and
Dockerfile ([how-to-profile.md](how-to-profile.md)). ztest only flips the switch
and collects the artifact. Zero privilege, zero SCC, zero sidecar.

The full contract lives in [how-to-profile.md](how-to-profile.md); the shape:

- **Build gate** (links `pprof-rs`): a cargo `profile` feature, flipped by a
  Docker build `ARG`. Linking is a build decision — a runtime env cannot pull
  in a crate.
- **Run-time gate** (samples + dumps): `ZTEST_PROFILE` opens a `ProfilerGuard`
  for the whole process lifetime; `ZTEST_PROFILE_OUT` is the dump directory. On
  graceful `SIGTERM` (pod teardown) the component builds the report *off the
  signal handler* (pprof report-building is not async-signal-safe) and writes
  `profile.pb` — the pprof protobuf, from which speedscope / pprof.me /
  `go tool pprof` render the flamegraph on demand (source-of-truth, ~10× smaller
  than a stored SVG, and interactive/diffable). Sample rate is
  `ZTEST_PROFILE_HZ` (default 100 Hz), the real lever on overhead + size over a
  10–600 min run.
- **ztest owns**: setting the two env vars on the component pods, pointing
  `ZTEST_PROFILE_OUT` at a writable volume, collecting the artifact after the
  test, and surfacing the flamegraph in the report. A `#[ztest::profile]`
  attribute + `ZTEST_PROFILE` is the test-facing switch.

Because the `ProfilerGuard` spans the process lifetime, the flamegraph covers
the entirety of the test.

**Artifact collection is new subsystem work, not a wiring job.** There is no
per-test artifact collection today — only runner-pod stdout is captured. Two
facts force the shape:

- The component writes `profile.pb` **only on `SIGTERM`** (pod teardown), so
  an `emptyDir` is destroyed before the file can be read. `ZTEST_PROFILE_OUT`
  must be a **per-test PVC** that outlives the component pod and is reclaimed at
  namespace teardown.
- Component pods are owned by the *test* process, not the parent, so collection
  runs **test-side in `TestEnv::drop`**: delete each component pod with a grace
  period (so the profiler flushes), then a collector pod tars the PVC out. The
  parent then does a second hop (runner pod → laptop). Both hops reuse the
  existing `materialize.rs` attach-tar pattern.

### Inherent limitation, stated up front

pprof-rs samples via `SIGPROF` + backtrace with a mandatory
`blocklist(["libc","libgcc","pthread","vdso"])`, so **native frames are
opaque**: zaino's LMDB C and zebrad's RocksDB C++ appear as leaf nodes, not
walked. The contract yields a faithful **Rust-level** flamegraph — which RPC
handler / codepath / async task burns CPU — which is the "why is zaino slow
under load" question ~95% of the time. It does **not** show time *inside* the
embedded databases.

### Escalation (designed, deferred)

If native-DB time ever becomes the bottleneck under investigation, the only
mechanism that walks kernel + native stacks is host-level sampling (eBPF or
`perf`), which on OKD's `restricted-v2` SCC requires a custom SCC in the
buildkit tier (`privileged`/`hostPID` for a Parca-agent DaemonSet, or
`CAP_PERFMON` + `shareProcessNamespace` for an ephemeral `perf` sidecar). This
is a deliberate, reviewer-gated escalation reached only when the opaque-DB-leaf
is proven to be the hot spot — not part of the default plane.

## Build order

1. **Exporters on** — `prometheus`-feature builds + `[metrics]`/`metrics_endpoint`
   config render for zaino (9998) and zebrad (9999). Smallest step; unblocks the
   whole metrics plane.
2. **`PodMonitor` resource + RBAC + run-id label** — wire components into OKD
   UWM; add the thanos-querier read path keyed by run-id. (Annotations field on
   `PodSpec` for the kind/plain-Prometheus path.)
3. **Profiling contract** — publish [how-to-profile.md](how-to-profile.md);
   `#[ztest::profile]`/`ZTEST_PROFILE` env wiring + artifact collection on the
   ztest side. Component-side integration is owned by each component team.

## Open items to confirm on the target cluster

- UWM enabled (`enableUserWorkload: true`) and its minimum enforced scrape
  interval.
- The build pipeline actually flips `--features prometheus` for zaino/zebrad
  (ports are declared today but no listener is enabled).

## Dependencies

- No new ztest runtime crate for the metrics plane (query is HTTP against
  thanos-querier / Prometheus).
- Profiling adds `pprof` **to the component images only**, never to ztest.
