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
| `ZTEST_PROFILE_INTERVAL` | run-time | **snapshot period** (s); unset ⇒ shutdown only | ztest (optional, long runs) |

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
fn write_pprof(guard: &pprof::ProfilerGuard<'_>, name: &str) {
    let Ok(report) = guard.report().build() else { return };
    let dir = std::env::var("ZTEST_PROFILE_OUT").unwrap_or_else(|_| ".".into());
    // Write only the pprof protobuf. It is the source-of-truth artifact —
    // string-interned and ~10× smaller than a rendered SVG — and speedscope.app,
    // pprof.me, and `go tool pprof` all render the flamegraph from it on demand,
    // interactively and diffably. `write_to_bytes()` is the rust-protobuf
    // serializer the `protobuf-codec` feature provides; `.encode()` is prost's
    // API and does not exist under this feature. The bytes are raw protobuf —
    // pprof consumers sniff the format, so no gzip layer is needed.
    let Ok(profile) = report.pprof() else { return };
    use pprof::protos::Message;
    let Ok(buf) = profile.write_to_bytes() else { return };
    // Temp file in the same directory, then rename. ztest reads this directory
    // out of a *running* pod, so a reader can arrive mid-write; a same-filesystem
    // rename is atomic and makes a partial profile unobservable. Writing in place
    // would hand out truncated protobuf.
    let tmp = format!("{dir}/.{name}.tmp");
    if std::fs::write(&tmp, &buf).is_ok() {
        let _ = std::fs::rename(&tmp, format!("{dir}/{name}"));
    }
}
```

Call `start_profiler()` before your runtime starts and hold the guard until your
existing graceful-shutdown path (the one that already handles `SIGTERM` for a
clean DB close) calls `write_pprof(&guard, "profile.pb")`. Pod teardown sends
`SIGTERM`, so a component that already shuts down cleanly needs only to add that
call at the end of that path.

`write_pprof` is the **only** writer in this contract — the periodic snapshots
below reuse it verbatim, so the atomicity rule cannot be satisfied in one path and
missed in the other.

## Long runs: periodic snapshots

A single profile covering a multi-hour run is a **time-average**. If your service
is healthy for forty hours and pathological for eight, one profile shows the blend
and the transition is invisible — which is exactly the question you opened a
profiler to answer.

The fix is to snapshot on an interval. `ProfilerGuard::report()` takes `&self`,
does not consume the guard, and does not reset the accumulator, so it can be
called as often as you like on a *running* profiler. Each snapshot is therefore
**cumulative** — all work since the profiler started — which makes the series a
set of monotone counters:

```
snapshot(t₂) − snapshot(t₁)  ==  exactly the work done in [t₁, t₂]
```

That subtraction is what `go tool pprof -base` does, and it is how `ztest sync
perf --window` serves an arbitrary slice of a long run. It is only valid because
the sample rate is constant (see below), which keeps `period` identical across
the series.

Implement it in the same task that owns the guard — `ProfilerGuard` borrows a
process-global, so moving it to another thread is not the shape you want:

```rust
#[cfg(feature = "profile")]
async fn profile_until_shutdown(
    guard: pprof::ProfilerGuard<'static>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let secs = std::env::var("ZTEST_PROFILE_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&s| s > 0);
    let started = std::time::Instant::now();
    if let Some(secs) = secs {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        tick.tick().await; // the first tick is immediate; a 0-length profile is noise
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tick.tick() => {
                    let elapsed = started.elapsed().as_secs();
                    write_pprof(&guard, &format!("profile-{elapsed:08}.pb"));
                }
            }
        }
    } else {
        let _ = shutdown.changed().await;
    }
    // The whole-run profile keeps its stable name, so a consumer that knows
    // nothing about snapshots still finds it.
    write_pprof(&guard, "profile.pb");
}
```

This reuses the same `write_pprof` defined above — including its temp-file +
`rename`, which is what makes a snapshot safe to read while the process is still
running. The one rule specific to snapshots is the **name**: elapsed seconds,
zero-padded, so `profile-00003600.pb` is one hour in. That makes the series
self-describing and lexically sortable, so a reader needs no sidecar index and no
assumption that every tick fired on time.

**Sizing.** A pprof profile is keyed by *stack*, not by sample, so its size tracks
the number of distinct stacks a process reaches — which saturates within minutes
and then stays roughly flat however long the run continues. Budget a few hundred
KB per snapshot against ztest's 2 GiB artifact PVC: a 5-minute interval costs
~150 MB/day, which is the right default for a multi-day sync. Sub-minute
intervals are for short runs.

### Why the sample rate is fixed for the process lifetime

It is deliberate that there is no way to change `ZTEST_PROFILE_HZ` on a running
process, and none should be added:

- **It saves nothing.** Size tracks unique stacks, not sample count, and pprof's
  overhead at 100 Hz is ~1% of CPU time and constant — neither grows with run
  length, so lowering the rate later has almost nothing to reclaim.
- **It breaks the arithmetic.** `frequency` is a parameter of
  `ProfilerGuardBuilder`, not a setter on a live profiler, so changing it means
  dropping and rebuilding the guard: the accumulator resets and samples are lost
  in the gap. Worse, the series then spans two `period` values, and subtracting
  across that boundary is silently wrong.
- **It makes the real problem worse.** Time-blending is the defect; fewer samples
  is a blurrier blend, not a sharper one. Snapshot cadence is the lever, not rate.

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
- [ ] Whole-run profile named exactly `profile.pb`.
- [ ] `ZTEST_PROFILE_INTERVAL` honoured; snapshots named
      `profile-<elapsed_seconds:08>.pb` and written via temp-file + `rename`.
- [ ] `tar` present in the profiled image — ztest retrieves snapshots from the
      *running* pod over `exec`, and there is no fallback while the pod holds its
      artifact volume.
- [ ] Production image (no build arg) links no profiler.
