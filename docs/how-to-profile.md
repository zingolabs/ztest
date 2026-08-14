# How to profile a component under ztest

ztest profiles components **from outside the process**, with an eBPF collector. There is
no contract for component authors to implement: no dependency, no cargo feature, no code
in `main`. A component is profileable because it is a process.

## The switch

| Switch | Kind | Controls |
| --- | --- | --- |
| `ZTEST_PROFILE` | run-time | whether a run deploys a collector at all |

Set it and ztest creates one collector per run; leave it unset and nothing is collected.
Off by default — a collector is a privileged pod charged against the run's capacity.

## What you get

- A flame graph spanning **Rust, C, C++ and the kernel** in one stack: `zainod` frames
  run through LMDB into `mdb_page_search_root`, and `writev` is no longer a leaf.
- **Off-CPU time**, merged with on-CPU. Blocked I/O, lock contention and major page
  faults become visible; a CPU-time profiler cannot see any of them.
- **Every process in the run**, including components built from published images
  (`zebrad`), which an in-process profiler could never reach.
- Unbiased sampling. The collector samples per-CPU; a signal-based in-process profiler
  samples whichever thread happens to receive `SIGPROF`, which is not proportional to
  CPU consumption.

## Getting good symbols

The collector unwinds via frame pointers, falling back to `.eh_frame`. A component built
with neither yields truncated stacks — silently, since a partial stack is still a stack.
For a Rust component:

```toml
# .cargo/config.toml
[build]
rustflags = ["-C", "force-frame-pointers=yes"]
```

```toml
# Cargo.toml — line numbers and inlined frames
[profile.release]
debug = "line-tables-only"
```

Cargo strips debuginfo from release builds when `debug` is unset (since 1.77), and every
inlined function folds into its caller.

> A `RUSTFLAGS` environment variable **replaces** `[build] rustflags` rather than
> appending to it. A build environment that sets `RUSTFLAGS` must carry the
> frame-pointer flag itself or it ships an unprofileable binary.

## How it works

One Alloy DaemonSet per run, in the run's namespace:

- **Per run, not per node.** `pyroscope.write` sends static headers only
  ([grafana/alloy#259](https://github.com/grafana/alloy/issues/259)), so one collector
  pushes to exactly one Pyroscope tenant. ztest retires a sync's profiles *by tenant*, so
  a shared node-wide collector would forfeit deletion outright.
- **DaemonSet, not Pod.** `pyroscope.ebpf` only sees processes on its own node, and a
  run's pods are not co-scheduled.
- **Namespaced Role**, never a ClusterRole — discovery is scoped to the run's namespace
  and the RBAC is garbage-collected with it.
- Profiles carry `component` / `namespace` / `run_id` / `sync_id`, so `ztest sync perf`
  queries them through the same selector as before.

The pod runs `privileged` with `hostPID` and an `Unconfined` AppArmor profile: BPF
program load is blocked by the default profile, and sampled PIDs resolve only against the
host namespace.

## Reading a profile

`ztest sync perf` fetches a sync's merged pprof. Grafana has the Pyroscope datasource
wired at first boot; its comparison view diffs two label selectors, which is how a change
gets measured — baseline, one change, diff. Reading a single flame graph in isolation is
how people convince themselves of things that are not true.
