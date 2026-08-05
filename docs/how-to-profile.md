# How to make a component profileable under ztest

This is a contract for **component authors** (zaino, zebra, and any binary ztest
runs as a validator/indexer). Implement it once in your image and ztest can
produce a CPU flame graph covering the entire duration of a test — with **no
elevated privileges**, no sidecar, and no change to how your service runs in
production.

ztest does not inject a profiler. You expose one; ztest flips a switch and
collects the output.

## The two switches

Profiling is gated twice, because linking a crate and running a profiler are
different decisions:

| Switch | Kind | Controls | Set by |
| --- | --- | --- | --- |
| `profile` cargo feature | build-time | whether `pprof-rs` is **linked** | Docker build `ARG` |
| `ZTEST_PROFILE` | run-time | whether the profiler **samples** | ztest, per test |
| `ZTEST_PROFILE_OUT` | run-time | **where** `profile.pb` is written | ztest (an artifact dir) |
| `ZTEST_PROFILE_HZ` | run-time | **sample rate** (Hz); default 100 | ztest (optional, per run length) |

A single image built `--features profile` runs **unprofiled** when `ZTEST_PROFILE`
is unset (no `ProfilerGuard` is created, so there is zero overhead) and
**profiled** when it is set. You do not need two images.

> A runtime env var cannot pull a crate into the binary — linking is decided at
> build time. That is why the build feature exists separately from the runtime
> switch.

## Cargo

```toml
[dependencies]
pprof = { version = "0.13", features = ["protobuf-codec"], optional = true }

[features]
profile = ["dep:pprof"]
```

`protobuf-codec` only: you emit the pprof `profile.pb` (the source-of-truth
artifact), not a rendered SVG, so the `flamegraph`/inferno backend is not needed.

Keeping `pprof` optional and off by default means production builds never carry
it. (Making it an unconditional dependency gated only by `ZTEST_PROFILE` at
runtime is also acceptable — zero overhead when off — at the cost of the dep in
every build. Your call.)

## Dockerfile

```dockerfile
ARG ZTEST_PROFILE_BUILD=""
RUN cargo build --release ${ZTEST_PROFILE_BUILD:+--features profile}
```

Build a profileable image with `--build-arg ZTEST_PROFILE_BUILD=1`.

## Wiring in `main`

Open the guard for the whole process lifetime; write the report on graceful
shutdown. The critical correctness rule: **build the report off the signal
handler.** pprof-rs report-building is not async-signal-safe, so a `SIGTERM`
handler must only *signal* your shutdown path — the report is built on a normal
thread after the runtime begins winding down.

```rust
#[cfg(feature = "profile")]
fn start_profiler() -> Option<pprof::ProfilerGuard<'static>> {
    if std::env::var_os("ZTEST_PROFILE").is_none() {
        return None;
    }
    // The sample RATE is the lever on overhead and artifact size over a long run
    // — not any after-the-fact compression. 100 Hz resolves the hot Rust paths at
    // ~1% overhead over a multi-hour test; `ZTEST_PROFILE_HZ` overrides it.
    // (pprof's own default is 99 Hz; 1000 Hz is wasteful for a 10–600 min run.)
    let hz = std::env::var("ZTEST_PROFILE_HZ")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&h| h > 0)
        .unwrap_or(100);
    pprof::ProfilerGuardBuilder::default()
        .frequency(hz)
        // Mandatory: SIGPROF-unwind is not safe through these; omitting risks deadlock.
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .ok()
}

#[cfg(feature = "profile")]
fn write_profile(guard: pprof::ProfilerGuard<'_>) {
    let Ok(report) = guard.report().build() else { return };
    let dir = std::env::var("ZTEST_PROFILE_OUT").unwrap_or_else(|_| ".".into());
    // Write only the pprof protobuf. It is the source-of-truth artifact —
    // string-interned and ~10× smaller than a rendered SVG — and speedscope.app,
    // pprof.me, and `go tool pprof` all render the flamegraph from it on demand,
    // interactively and diffably. `write_to_bytes()` is the rust-protobuf
    // serializer the `protobuf-codec` feature provides; `.encode()` is prost's
    // API and does not exist under this feature. The bytes are raw protobuf —
    // pprof consumers sniff the format, so no gzip layer is needed.
    if let Ok(profile) = report.pprof() {
        use pprof::protos::Message;
        if let Ok(buf) = profile.write_to_bytes() {
            let _ = std::fs::write(format!("{dir}/profile.pb"), &buf);
        }
    }
}
```

Call `start_profiler()` before your runtime starts and hold the guard until your
existing graceful-shutdown path (the one that already handles `SIGTERM` for a
clean DB close) calls `write_profile(guard)`. Pod teardown sends `SIGTERM`, so a
component that already shuts down cleanly needs only to add the
`write_profile` call at the end of that path.

## What you get, and what you don't

- **You get** a faithful **Rust-level** flame graph: which RPC handler, which
  codepath, which async task is spending CPU across the whole test. This is the
  answer to "why is my service slow under load" the overwhelming majority of the
  time.
- **You do not get** time *inside* native libraries: the `blocklist` means LMDB
  (C) and RocksDB (C++) frames appear as opaque leaves, not walked. Seeing
  inside the embedded databases requires host-level sampling (eBPF/`perf`) with
  elevated privileges — out of scope for this contract by design; see the
  escalation note in [design-observability.md](design-observability.md).
- **Async note:** the flame graph shows *where CPU is spent* (poll stacks on
  worker threads), not logical await chains. Use `tokio-console`/`tracing` for
  scheduling/stall analysis — a profiler is the wrong tool for that.

## Checklist

- [ ] `pprof` optional dependency + `profile` feature.
- [ ] Dockerfile builds `--features profile` under a build `ARG`.
- [ ] Guard opened only when `ZTEST_PROFILE` is set; `blocklist` applied.
- [ ] Report written to `$ZTEST_PROFILE_OUT` from the graceful-shutdown path,
      never inside the signal handler.
- [ ] Production image (no build arg) links no profiler.
