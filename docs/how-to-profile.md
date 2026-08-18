# How to profile a component under ztest

ztest profiles components **from outside the process**, with an eBPF collector. There is
no contract for component authors to implement: no dependency, no cargo feature, no code
in `main`. A component is profileable because it is a process.

## The switches

| Flag                | Default | Controls                                    |
| ------------------- | ------- | ------------------------------------------- |
| `--profile`         | on      | whether the run collects profiles at all    |
| `--profile-hz`      | 19      | on-CPU sample rate (upstream's default)     |
| `--profile-off-cpu` | 0.05    | fraction of scheduler-switch events sampled |

Off-CPU sampling is thinned hard on purpose: one trace event per *scheduler switch* into a
fixed-size per-CPU ring. At `1.0` a busy sync overruns it and every trace is dropped. Raise
it only while `ztest sync perf` still reports 0 dropped events.

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

## Symbols: nothing to rebuild

The collector unwinds with **`.eh_frame`**, the CFI table every optimized binary already
carries for exception handling. Frame pointers are *not* required — `-C force-frame-pointers` buys nothing and its absence costs nothing. Stacks reach ~50 frames
through Rust, tokio, glibc and RocksDB on stock release builds.

Worth adding for line numbers and inlined frames, which unwinding alone cannot recover:

```toml
# Cargo.toml
[profile.release]
debug = "line-tables-only"
```

Cargo strips debuginfo from release builds when `debug` is unset (since 1.77), and every
inlined function folds into its caller.

## Where the collector runs

One collector per run, pushing to one Pyroscope tenant — `pyroscope.write` sends static
headers only ([grafana/alloy#259](https://github.com/grafana/alloy/issues/259)), and ztest
retires a sync's profiles *by tenant*, so a shared node-wide collector would forfeit
deletion outright.

Placement is a property of the cluster, not a preference:

| Cluster               | Placement                           | Why                                                                 |
| --------------------- | ----------------------------------- | ------------------------------------------------------------------- |
| Real node             | sidecar on the driver pod           | `hostPID` reaches the initial pid namespace                         |
| Nested kubelet (kind) | docker container beside the kubelet | a pod under kind's node container cannot name the pids eBPF reports |

eBPF reports pids in the *initial* namespace. kind runs its node as a container, so
`hostPID` there reaches the node's namespace — one level below — and every `/proc/<pid>`
lookup misses: targets match, processes are counted, and nothing unwinds, with no error
([kind#3182](https://github.com/kubernetes-sigs/kind/issues/3182) is closed as not
planned). The host-placed collector joins the cluster's docker network instead, reaching
the apiserver and Pyroscope by node IP; container attribution still works because nested
containerd IDs appear verbatim in the host cgroup path.

The sidecar runs `privileged` with `hostPID` and an `Unconfined` AppArmor profile: BPF
program load is blocked by the default profile. It is charged against the run's capacity;
a host-placed collector is not, being outside the cluster.

`ztest cluster check` reports which placement applies and whether its prerequisites hold.

## Reading a profile

`ztest sync perf <sync> --component <name>` writes **collapsed stacks**
(`frame;frame <self>`) and opens them in `flameshow`.

Collapsed rather than pprof because Pyroscope's pprof encoder returns an empty sample list
for these profiles — locations and functions survive, every sample is dropped — while the
flamegraph encoder carries the same query's data whole. ztest folds that to collapsed,
which `flameshow` reads directly and `--base` can diff line-by-line.

Grafana has the Pyroscope datasource wired at first boot; its comparison view diffs two
label selectors, which is how a change gets measured — baseline, one change, diff. Reading
a single flame graph in isolation is how people convince themselves of things that are not
true.
