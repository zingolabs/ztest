# The ztest engine — design

Status: draft / RFC. Supersedes the nextest-as-opaque-subprocess parts of
`qos-design.md` (§5.2, §6, §12) — see §9 below for what changes.

## 1. Mandate

Today `ztest run` is a *wrapper* around `cargo nextest`: it shells out to
`cargo nextest list` for inventory (`pipeline/build.rs`) and to
`cargo nextest run` for execution (`cli/run.rs`, `cli/pty_run.rs`), parsing
neither's live output because the **cargo nextest binary exposes no live
event stream** (`qos-design.md` §12). All live coordination is bolted on via
a side-channel: the QoS broker / k8s-Lease `Allocator`, threaded through the
test processes nextest happens to spawn.

This forces an awkward **two-scheduler architecture** (`qos-design.md` §5.2):

- **Layer 1** — `qos/lower.rs` generates a nextest `--tool-config-file` so
  nextest's *internal* scheduler provides coarse backpressure (`threads-
  required`), priority ordering, and a loose `slow-timeout`. To fit nextest's
  scalar knobs, `lower.rs` folds each test's 2-D footprint onto a 1-D
  "thread-unit" axis (`max(cpu, ceil(mem/2GiB))`). This is a lossy
  approximation of the real policy.
- **Layer 2** — the broker (`qos/scheduler.rs`, `qos/allocator.rs`,
  `env.rs::admit`) does the precise 2-D / budget / NVMe decision, but only
  *after* nextest has already forked the test process. The process then
  blocks in `TestEnv::build()` waiting for a capacity lease.

The two layers must be carefully tuned to *agree* rather than fight, and the
`slow-timeout`-measures-from-spawn hazard (§5.2) exists only because tests are
spawned before they're admitted.

**Goal of the engine:** own test execution end-to-end so that *the scheduler
decides which test process to spawn and when*. This collapses the two layers
into one dynamic scheduler, removes the 2-D→1-D fold, eliminates the
spawn-before-admit hazard, and — per the finding in §3 — gives us a native
live event stream we currently believe doesn't exist.

Non-goals: replacing the k8s-Lease capacity ledger (`Allocator`/`kube_store`)
— that stays. The engine *consumes* it as the capacity oracle.

## 2. What nextest gives us, and where the seam is

`cargo nextest` is itself just a **process-per-test runner over standard
libtest binaries**. Verified against the nextest source (v0.119, crates:
`cargo-nextest` = CLI shell, `nextest-runner` = engine, `nextest-filtering` =
filterset DSL, `nextest-metadata` = the JSON types ztest already depends on):

| Capability | nextest mechanism | Reusable as a library? |
|---|---|---|
| Compile test binaries | `cargo test --no-run` / cargo metadata | n/a — cargo does it |
| List tests | `<bin> --list --format terse [--ignored]` → parsed into `list::TestList` | `list` is `pub`; `TestList::from_summary` builds it from the `TestListSummary` JSON we already parse |
| Filter selection | `nextest-filtering` + `test_filter.rs` | `pub` crate; the DSL we partially use |
| Run one test | `<bin> --exact <name> --nocapture [--ignored]`, one OS process per test, `NEXTEST_EXECUTION_MODE=process-per-test` | command builder `test_command` is **private** (`mod`, not `pub mod`) |
| Capture/split output | `test_output.rs` (`CaptureStrategy::{Split,Combined}`) | `pub` |
| Schedule/order | `runner/executor.rs` over `future_queue::FutureQueueContext` | **not pluggable** — see §2.1 |
| Report (human/JUnit/libtest-json) | `reporter/{displayer,aggregator,structured}`, driven by a `ReporterEvent` callback | `pub`; fully decoupled — see §3 |

Exact libtest args confirmed in source: list at `list/test_list.rs:1375`
(`--list --format terse`), run at `list/test_list.rs:1675`
(`--exact <name> --nocapture`).

### 2.1 The scheduler is not pluggable — this is the crux

`runner/executor.rs` schedules tests by feeding a **pre-sorted stream**
(priority, then test-list order) into `future_queue`, a weighted-concurrency
stream combinator (weight = `threads-required`), with per-group semaphores
(`test_group` / `global_slot`). The ordering is **frozen when the stream is
constructed.** There is no scheduler trait, no "what runs next" hook, no way
to reorder based on live cluster capacity.

This is the structural reason our dynamic broker cannot live *inside*
nextest, and why "use nextest's runner but swap its scheduler" is not a
small patch — the scheduler is welded into the executor's actor loop
(`runner/dispatcher.rs` + `runner/executor.rs`, ~3400 LOC combined). The
runner is simultaneously (a) the piece most coupled to the scheduling we want
to replace and (b) the least stable API (`nextest-runner` is `0.x`, no semver
guarantee, designed as cargo-nextest's private engine).

## 3. Finding that changes the existing design

`qos-design.md` §12 states, as a researched fact: *"nextest has no
bidirectional API, no callback, no live event stream … the broker is the only
live UI source — not a preference, a necessity."*

**That is true only of the `cargo nextest` binary's external interface. The
`nextest-runner` library is fully event-streamed.** `TestRunner::execute`
takes a callback `F: FnMut(ReporterEvent<'a>)` (`runner/imp.rs:478`); the
runner emits a live `ReporterEvent` stream and the `reporter` module is a pure
*consumer* of it. The relevant variants for a live UI:
`RunStarted`, `TestStarted`, `TestSlow`, `TestFinished`, `TestSkipped`,
`RunFinished` (`reporter/events.rs`).

Consequence: once ztest *is* the engine, the broker stops being the sole live
event source. Per-test start/slow/finish are first-class engine events,
emitted by our own run loop, consumed by our own renderer (and/or nextest's
`displayer`). The pinned-panel-over-scrolling-nextest-output split (§12
"Screen layout") becomes one unified renderer fed by one event stream.

## 4. Boundary decision

Three reuse boundaries were considered (full analysis in commit thread):

- **B1 — reuse both ends, write the middle.** Keep `list` + `test_output` +
  `reporter`; write our run loop, *emitting* `ReporterEvent`. Maximal reuse,
  but `ReporterEvent<'a>` / `TestEventKind<'a>` is a borrow-and-config-heavy
  ~3400-LOC type designed to be emitted *by nextest's runner*. Synthesizing it
  externally (holding the `TestList` alive, threading lifetimes, importing
  nextest's `config`/`TestGroup` model) is a large, churn-exposed surface. We
  also lose the private `test_command`.
- **B2 — reuse the runner whole, gate inside the test.** The status quo
  (broker socket in `TestEnv::build`). Rejected — it *is* the two-layer
  problem.
- **B3 — reuse only the stable leaves; own list+exec+reporter. ✅ chosen.**

### 4.1 Chosen: B3

Depend only on the genuinely-stable, genuinely-decoupled pieces and own the
loop:

- **Inventory**: keep shelling to `cargo nextest list --message-format=json`
  (zero new code; `pipeline/build.rs` already does this and parses
  `nextest_metadata::TestListSummary`). This stays our compile+list step.
- **Filtering**: optionally depend on `nextest-filtering` for
  `s/cargo nextest/ztest/` filterset compatibility; otherwise reimplement the
  subset we use (`binary_id(=…) & test(=…)`, substring, `/regex/`).
- **Scheduler**: `qos/scheduler.rs` (`Scheduler`) — already exists, pure,
  unit-tested, priority + backfill + 2-D capacity + FIFO tiebreak. Promote it
  from "Layer 2 among already-spawned tests" to "owner of the spawn decision."
- **Exec**: our own process-per-test spawner. One test per process via
  `<bin> --exact <name> --nocapture` makes exec and result-parsing trivial:
  **exit code is the verdict**, output captured verbatim. No libtest text
  parsing, no interleaving (we copy nextest's own process-per-test model;
  `qos/mod.rs:426` already relies on its no-cross-test-leakage property).
- **Reporter**: our own, building on `preflight/render.rs` + `theme.rs`, which
  already mimic nextest's reporter conventions.

Rationale: the scheduler exists, the reporter mostly exists, the unstable
runner is the part we must replace regardless, and B3 severs us from
`nextest-runner` 0.x churn. We treat `nextest-runner` as a **reference
implementation to read** (signal handling, leak detection, double-spawn,
grace-period termination — `runner/unix.rs`, `runner/AGENTS.md`), not a
dependency.

One spike (§10) may revisit reusing nextest's `displayer` specifically.

## 5. Architecture

```text
  ztest run args
        │
        ├─► Phase A  cluster probe ──────────────► ClusterCapacity ──┐
        │   (pipeline/cluster.rs, unchanged)                         │
        │                                                            ▼
        ├─► Phase B  inventory ─────────► TestPlan ──────────►  ┌──────────────┐
        │   cargo nextest list --json     (Vec<PlannedTest>,    │  Scheduler   │
        │   (pipeline/build.rs, kept)       footprint+tier      │ (qos/        │
        │                                   per test)           │  scheduler)  │
        │                                                       └──────┬───────┘
        │                                          grant: "spawn test T" │
        │                                                                ▼
        └─► Phase D  ENGINE run loop  ◄───── release/finish ──── ┌──────────────┐
            (src/engine/*)                                       │ Exec workers │
              • pull grants from Scheduler                       │ process-per- │
              • spawn <bin> --exact <name>                       │ test         │
              • capacity oracle = k8s Allocator + probe          └──────┬───────┘
              • emit EngineEvent stream ─────────────────────────┐      │
                                                                 ▼      ▼
                                                          ┌──────────────────┐
                                                          │ Reporter         │
                                                          │ (preflight/render│
                                                          │  + theme)        │
                                                          └──────────────────┘
```

### 5.1 New module: `src/engine/`

```
engine/
├── mod.rs        # EngineConfig, run(), public surface
├── plan.rs       # TestPlan / PlannedTest: TestListSummary → scheduler inputs
├── exec.rs       # process-per-test spawn, capture, timeout/term, verdict
├── loop.rs       # the run loop: Scheduler ↔ exec workers ↔ events
├── event.rs      # EngineEvent (our live stream; the §3 events)
└── report.rs     # EngineEvent → render (built on preflight/render.rs)
```

The pure scheduling core stays in `qos/scheduler.rs`. The engine is the I/O
shell around it — analogous to how nextest splits pure `executor` policy from
the `dispatcher` actor.

### 5.2 `TestPlan` — the bridge

`plan.rs` turns the existing `TestListSummary` (from `pipeline/build.rs`) plus
the per-binary QoS dump (`ZTEST_DUMP_INVENTORY`, `pipeline/images.rs`) into:

```rust
struct PlannedTest {
    binary_path: PathBuf,    // from SelectedBinary
    cwd: PathBuf,            // from SelectedBinary (libtest cwd contract)
    binary_id: String,       // NEXTEST_BINARY_ID we now set ourselves
    test_name: String,       // NEXTEST_TEST_NAME we now set ourselves
    tier: QosClass,          // from the dump; None ⇒ default/basic
    footprint: Resources,    // tier.profile().footprint — full 2-D, no fold
    priority: u8,
    hard_cap: Duration,
}
```

This replaces `qos/lower.rs` entirely: the footprint goes *straight* into
`Scheduler::request` as a `qos::scheduler::Request`. No tool-config TOML, no
1-D fold, no `threads-required`/`test-group` translation.

### 5.3 The run loop (`loop.rs`)

```
1. For each PlannedTest, Scheduler::request(). Granted → spawn now;
   Queued → wait; Rejected → fail fast (unschedulable / over budget).
2. On a Grant: exec::spawn(test) — set NEXTEST_BINARY_ID / NEXTEST_TEST_NAME /
   NEXTEST_EXECUTION_MODE=process-per-test + the dylib path + cwd ourselves;
   run <bin> --exact <name> --nocapture; arm the hard-cap timer AT SPAWN
   (== admission now, since we spawn only after grant — the §5.2 hazard is
   gone by construction).
3. On test exit: parse exit code (0 = pass), capture output; emit
   EngineEvent::TestFinished; Scheduler::release(lease) → backfill grants →
   spawn the freed-up tests.
4. Capacity oracle: the Scheduler's free-capacity model is reconciled from
   the k8s Allocator / probe (qos/scheduler.rs::reconcile), exactly as the
   broker does today. The engine loop owns the reconcile cadence.
5. Concurrency: a bounded set of exec workers; the Scheduler — not a fixed
   --test-threads pool — decides how many run, by 2-D capacity.
6. fail-fast / --no-fail-fast, retries: implemented here (re-request on fail
   with backoff; stop admitting on first failure unless --no-fail-fast).
```

Note the inversion: the queue of not-yet-started tests lives in the
`Scheduler`, **not** as forked-but-blocked OS processes. This is strictly
better than today's "nextest fork-bombs ≈capacity-worth, broker gates them"
(§5.2) — no blocked processes, exact 2-D enforcement, dynamic ordering at the
real spawn boundary.

### 5.4 Reporter (`report.rs`)

Consumes `EngineEvent`; renders via `preflight/render.rs` + `theme.rs`. One
unified view (capacity gauges + per-tier running/queued from the Scheduler,
*and* per-test pass/fail from the engine) — the §12 "pinned panel over
scrolling nextest output" two-source split collapses into one. JUnit XML
output (if CI needs it) is a second `EngineEvent` consumer.

## 6. Interaction with the existing QoS / k8s admission

The k8s-Lease `Allocator` (`qos/allocator.rs`, `qos/kube_store.rs`) and the
`TestEnv::build()` reservation stay — they remain the authoritative,
crash-safe, decentralized capacity ledger for a *shared* cluster. Two changes:

- The engine `Scheduler` becomes the **local pre-gate**: it won't spawn a test
  it doesn't expect to fit, so `TestEnv::build()`'s admit call almost always
  succeeds immediately (the cluster-wide ledger still arbitrates between
  concurrent `ztest run` invocations / external load).
- Identity (`NEXTEST_BINARY_ID` / `NEXTEST_TEST_NAME`) is now set **by the
  engine** when it spawns each test, instead of being read from nextest. The
  `env.rs` admit path consuming those env vars is unchanged — the contract is
  the same, just sourced from us. Stronger, not weaker.

## 7. What gets deleted / collapsed

- `qos/lower.rs` — the entire 2-D→1-D fold + tool-config TOML generator.
- The tool-config plumbing in `cli/run.rs` (`--tool-config-file ztest:…`,
  the `qos-sync` `@tool:ztest:` namespacing).
- `cli/pty_run.rs` — spawning `cargo nextest run` under a PTY to relay output.
- The nextest-specific arg peeking in `cli/args_peek.rs` (kept only insofar as
  we honor a compatible flag surface).
- The `nextest-metadata` dependency *may* stay (it's the stable JSON contract
  for the `list` step) or be replaced by our own inventory types — TBD by §10.
- In `qos-design.md`: §5.2's two-layer split, §6's lowering, and §12's "no
  live stream / broker-is-sole-source" framing are rewritten — the engine is
  one layer with a native event stream.

## 8. Risks

- **libtest output edge cases** — mitigated by one-test-per-process: we rely on
  exit code + verbatim capture, not parsing the `test … ok/FAILED` stream.
- **Process management parity** — nextest handles double-spawn (SIGTSTP race),
  process groups, grace-period SIGTERM→SIGKILL, leak detection
  (`runner/unix.rs`, `runner/AGENTS.md`). We must port the ones we need. The
  hard-cap timer + SIGTERM→SIGKILL we already model (`hard_cap`); leak
  detection and double-spawn are the notable gaps to scope.
- **Migration promise** — `cli/mod.rs` documents `s/cargo nextest/ztest/` with
  args forwarded verbatim. B3 means reimplementing a compatible-enough flag
  surface or consciously narrowing it. Product decision.
- **Feature parity to reclaim incrementally**: `--retries`, fail-fast,
  `--partition` sharding, `--ignored`, JUnit XML, doctests (nextest doesn't run
  these either — not a regression).

## 9. Supersedes

This document supersedes, once implemented: `qos-design.md` §5.2 (two-layer
split), §6 (nextest config lowering), and the §12 claims that nextest has no
live event stream and that the broker must be the sole live UI source. Those
sections should be marked "historical — see engine-design.md" at merge time.

## 10. Phased plan

1. **Spike — displayer reuse (decides B1-vs-B3 for the reporter only).**
   Throwaway crate against `nextest-runner = "0.119"`: construct one
   `TestEventKind::TestFinished` and feed `DisplayReporter`. If the
   lifetime/config scaffolding is small, reuse the displayer; else own the
   reporter. ~1 day.
2. **`engine` skeleton behind `ZTEST_ENGINE=native`.** `plan.rs` (TestPlan
   from existing `TestListSummary`) + `exec.rs` (spawn one test, capture, exit
   code) + `loop.rs` wired to the existing `Scheduler`. Default path stays
   nextest. A/B both engines against the wallet suite — same scheduler core.
3. **Reporter parity.** `EngineEvent` → `preflight/render.rs`; match today's
   output. Validate on the real suite.
4. **Collapse Layer 1.** Delete `qos/lower.rs`, the tool-config plumbing,
   `cli/pty_run.rs`; rewrite `qos-design.md` §5.2/§6/§12. Footprint flows
   straight into the Scheduler.
5. **Reclaim deferred features** as needed: retries, fail-fast, JUnit,
   partitioning, leak detection, double-spawn.

## 11. Open questions

- Keep `nextest-metadata` for the `list` JSON, or define our own inventory
  types and parse `<bin> --list --format terse` directly (dropping the nextest
  binary entirely from inventory too)?
- Adopt `nextest-filtering` for filterset compatibility, or a minimal own DSL?
- Reconcile cadence for the engine `Scheduler` vs the k8s ledger — who polls
  the cluster and how often (today: `QUEUE_POLL` / `RENEW_INTERVAL`)?
- Do we still want the cargo-nextest CLI flag-compatibility promise, or is
  `ztest`'s own flag surface acceptable post-engine?
