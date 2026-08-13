# How to make a component profileable under ztest

A contract for **component authors** (zaino, zebra, any binary ztest runs).
Implement it once and ztest can produce a CPU flame graph of your service — with
no elevated privileges, no sidecar, and no change to how it runs in production.

ztest does not inject a profiler. You expose one; ztest points it at a
collector.

## The two switches

| Switch | Kind | Controls |
| --- | --- | --- |
| `profile` cargo feature | build-time | whether the profiler is **linked** |
| `ZTEST_PROFILE_URL` | run-time | where profiles are **pushed** — and whether they are |
| `ZTEST_PROFILE_TAGS` | run-time | `key=value,…` labels ztest attaches |
| `ZTEST_PROFILE_HZ` | run-time | sample rate; default 100 |

A single image built `--features profile` runs unprofiled when
`ZTEST_PROFILE_URL` is unset and profiled when it is set. One image, one switch.

> A runtime env var cannot pull a crate into the binary — linking is decided at
> build time. That is why the build feature exists separately.

## Cargo

```toml
[dependencies]
pyroscope = { version = "2", features = ["backend-pprof-rs"], optional = true }

[features]
profile = ["dep:pyroscope"]
```

## Dockerfile

```dockerfile
ARG ZTEST_PROFILE_BUILD=""
RUN cargo build --release ${ZTEST_PROFILE_BUILD:+--features profile}
```

## Wiring in `main`

```rust
#[cfg(feature = "profile")]
fn start_profiler(app: &str) -> Option<pyroscope::PyroscopeAgent<pyroscope::pyroscope::PyroscopeAgentRunning>> {
    use pyroscope::backend::{pprof_backend, BackendConfig, PprofConfig};
    use pyroscope::pyroscope::PyroscopeAgentBuilder;

    let url = std::env::var("ZTEST_PROFILE_URL").ok()?;
    let hz: u32 = std::env::var("ZTEST_PROFILE_HZ")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&h| h > 0)
        .unwrap_or(100);
    let tags = std::env::var("ZTEST_PROFILE_TAGS").unwrap_or_default();
    let tags: Vec<(&str, &str)> = tags
        .split(',')
        .filter_map(|pair| pair.split_once('='))
        .collect();

    let agent = PyroscopeAgentBuilder::new(
        url,
        app,
        hz,
        "pyroscope-rs",
        env!("CARGO_PKG_VERSION"),
        pprof_backend(PprofConfig::new().sample_rate(hz), BackendConfig::default()),
    )
    .tags(tags)
    .build()
    .ok()?;
    agent.start().ok()
}
```

Start it before your runtime, and on your existing graceful-shutdown path:

```rust
if let Some(agent) = agent {
    if let Ok(ready) = agent.stop() {
        ready.shutdown();
    }
}
```

`stop()` then `shutdown()`, in that order — the agent needs a moment (usually
well under 10 s) to drain its threads. Skipping it loses at most the last push
interval, not the run.

**Use your component's own name as `app`.** ztest selects profiles by the
`component` tag it sets, and a service name that collides across components
makes two processes' samples indistinguishable.

## What you get, and what you don't

- **You get** a Rust-level flame graph — which RPC handler, which codepath,
  which async task is spending CPU — queryable *while the run is still going*,
  surviving the pod, its namespace, and an OOM kill.
- **You do not get** time inside native libraries. `pyroscope-rs` samples via
  pprof-rs, so LMDB (C) and RocksDB (C++) frames are opaque leaves. Seeing
  inside them needs host-level eBPF sampling with elevated privileges — out of
  scope for this contract by design.
- **Async note:** the graph shows where CPU is spent (poll stacks on worker
  threads), not logical await chains. Use `tokio-console`/`tracing` for
  scheduling analysis; a profiler is the wrong tool for it.

## Why push, and not a file

The earlier contract wrote `profile.pb` to a volume on graceful shutdown, and
ztest collected it afterwards. That made a profile exist only if the process
exited cleanly — an OOM kill produced nothing — and made mid-run reading
impossible, which is the question a multi-hour sync actually raises.

Pushing inverts both: samples are queryable seconds after they are taken, and
whatever was pushed before a crash survives it.

## Checklist

- [ ] `pyroscope` optional dependency behind a `profile` feature.
- [ ] Dockerfile builds `--features profile` under a build `ARG`.
- [ ] Agent started only when `ZTEST_PROFILE_URL` is set.
- [ ] `ZTEST_PROFILE_TAGS` parsed and passed through as tags.
- [ ] `ZTEST_PROFILE_HZ` honoured, defaulting to 100.
- [ ] `stop()` + `shutdown()` on the graceful-shutdown path.
- [ ] Production image (no build arg) links no profiler.
