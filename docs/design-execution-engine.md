# Execution engine and console

How `ztest run` executes tests and renders their progress: the engine
schedules and spawns one process per test and emits an event stream; a
dedicated render thread consumes it and paints the terminal.

The engine is the **producer**, the console is the **consumer**. They share
nothing but the event stream — the engine speaks in ANSI strings and scene
snapshots; the render thread owns all terminal bridging.

## Engine

### Module layout (`src/engine/`)

| Module | Responsibility |
|---|---|
| `mod.rs` | `EngineConfig`, `run()`, public surface |
| `plan.rs` | `TestPlan` / `PlannedTest` — turn inventory into scheduler inputs |
| `exec.rs` | process-per-test spawn, capture, timeout/termination, verdict |
| `loop.rs` | the run loop: `Scheduler` ↔ exec workers ↔ events |
| `event.rs` | `EngineEvent` — the live stream |
| `report.rs` | `EngineEvent` → render (built on `preflight/render.rs` + `theme.rs`) |

The pure scheduling core lives in `qos/scheduler.rs` (`Scheduler`); the engine
is the I/O shell around it. See [design-qos.md](design-qos.md) for the
scheduler, capacity model, and cross-run ledger.

### Phases

```
ztest run args
  ├─ Phase A  cluster probe (pipeline/cluster.rs) → ClusterCapacity
  ├─ Phase B  inventory: cargo nextest list --message-format=json
  │            (pipeline/build.rs) → TestPlan
  └─ Phase D  engine run loop (src/engine/): Scheduler grants spawns,
             exec workers run one process per test, EngineEvent stream out
```

Inventory is a shell-out to `cargo nextest list --message-format=json`, parsed
via `nextest_metadata::TestListSummary` — the only place nextest is invoked.

### TestPlan

`plan.rs` combines the `TestListSummary` with the per-binary QoS dump
(`ZTEST_DUMP_INVENTORY`, `pipeline/images.rs`) into a `Vec<PlannedTest>`:

```rust
struct PlannedTest {
    binary_path: PathBuf,   // from SelectedBinary
    cwd: PathBuf,           // libtest cwd contract
    binary_id: String,      // engine sets NEXTEST_BINARY_ID itself
    test_name: String,      // engine sets NEXTEST_TEST_NAME itself
    tier: QosClass,         // from the dump; None ⇒ default/basic
    footprint: Resources,   // tier.profile().footprint — full 2-D
    priority: u8,
    hard_cap: Duration,
}
```

The 2-D `footprint` feeds straight into `Scheduler::request` as a
`qos::scheduler::Request`.

### Run loop (`loop.rs`)

1. For each `PlannedTest`, call `Scheduler::request()`. `Granted` → spawn now;
   `Queued` → wait; `Rejected` → fail fast (unschedulable / over budget).
2. On a grant, `exec::spawn` sets `NEXTEST_BINARY_ID`, `NEXTEST_TEST_NAME`,
   `NEXTEST_EXECUTION_MODE=process-per-test`, the dylib path, and cwd, then runs
   `<bin> --exact <name> --nocapture`. The hard-cap timer arms at spawn — which
   is also admission, since tests spawn only after a grant.
3. On exit, the **exit code is the verdict** (0 = pass); output is captured
   verbatim. Emit `EngineEvent::TestFinished`; `Scheduler::release(lease)`
   backfills grants and spawns the freed-up tests.
4. The scheduler's free-capacity model is reconciled from the k8s `Allocator` /
   probe (`Scheduler::reconcile`); the loop owns the reconcile cadence.
5. A bounded set of exec workers; the `Scheduler` — not a fixed thread pool —
   decides how many run, by 2-D capacity.
6. Retries and fail-fast (`--no-fail-fast`) are handled here.

The queue of not-yet-started tests lives in the `Scheduler`, not as
forked-but-blocked OS processes.

### Capacity oracle

The engine `Scheduler` is a local pre-gate: it won't spawn a test it doesn't
expect to fit, so the `TestEnv::build()` admit usually succeeds immediately. The
authoritative cross-run ledger (the k8s-Lease `Allocator`, `qos/allocator.rs` /
`qos/kube_store.rs`) still arbitrates between concurrent `ztest run` invocations
— see [design-qos.md](design-qos.md). Test identity (`NEXTEST_BINARY_ID` /
`NEXTEST_TEST_NAME`) is set by the engine at spawn; `env.rs` consumes those vars.

### EngineEvent

The live stream the reporter consumes: `RunStarted`, `TestStarted`, `TestSlow`,
`TestFinished`, `TestSkipped`, `RunFinished`. JUnit XML (for CI) is a second
consumer of the same stream.

### Reporter output (`reporter.rs`)

`StyledReporter` formats the event stream into `cargo nextest run`'s default
human output: verdict lines (`PASS`/`FAIL`/`SLOW`/`TRY n …`), the `output ───`
block, the `Summary` line, the failure recap. No `nextest-runner` dependency —
we generate every line, and the invariant is that each one is byte-identical to
nextest's. Change a status or summary line only against a diff with nextest.

`Verdict` models pass/fail/timeout/spawn-error only, so nextest's
leak/flaky/slow-pass/abort words are unreachable by construction.

#### Divergence: the captured-output block is de-framed

The one place we diverge, and it's confined to the bytes *inside* the
`output ───` block — never a line we generate.

Tests run as `<binary> --exact <name> --nocapture` (`local_runner.rs`,
`pod_runner.rs`), so the captured stream is wrapped in libtest's per-run framing:

```
running 1 test
test <name> ... <first log glued here — --nocapture holds the line open>
<logs>
FAILED

failures:

failures:
    <name>

test result: FAILED. 0 passed; 1 failed; …
```

nextest replays that verbatim. For a one-test process it's all noise we already
render better elsewhere: `running 1 test` is a constant, `test <name> ... `
restates our `FAIL […] <name>` line (and is what the first log glues onto), and
the footer restates our `Summary` and recap. So we strip it and show only the
test's own stdout/stderr:

```
        FAIL [  83.222s] clientless::… value_pools_respect_the_boundary_on_the_pub_testnet
  output ───
    2026-07-27T20:56:31 INFO ztest::env: starting test run …
    2026-07-27T20:56:32 INFO ztest::env: provisioning validator …
    Error: archive materialize failed for …/zebra.tar.xz: No such file or directory
```

`strip_libtest_frame(output, test_name)` does this before replay: drop through
the `test <name> ... ` marker (which also un-glues the first log line), then peel
the footer from the trailing `test result: ` line, consuming exactly one verdict
token so a log line that merely reads `FAILED` survives. Panic output
(`thread … panicked`, backtrace notes) precedes the verdict on stderr and is
kept. Each cut is anchored on a marker; a stream missing either anchor is left
uncut on that side, falling back to verbatim replay rather than risk eating
output.

New divergences go in this section with their rationale — the point is that
byte-identity stays the default and every exception is on the record.

## Console (render thread)

The bottom status panel is live for the whole session — spinner and clocks keep
ticking even while the work side is CPU-bound or blocked on a silent subprocess
(the inventory index pass, the image-inventory dump).

### Actor model

```
   work side (sequential phases)         render side (persistent)
 ┌──────────────────────────────┐      ┌────────────────────────────┐
 │ probe → build → image → run  │ Msg  │ DEDICATED OS THREAD         │
 │  pushes Scene snapshots       ├─────▶│ owns Surface, avt::Vt,      │
 │  spawns PTY children          │(mpsc)│ FrameClock, scrollback      │
 │  awaits futures, reads cancel │◀─────┤ loop { select! {            │
 └──────────────────────────────┘cancel│  msg=>mutate,mark dirty     │
                                  /size │  tick=>paint if dirty|spin  │
                                        │  signal=>forward/escalate }}│
                                        └────────────────────────────┘
```

- **Dedicated OS thread, not a tokio task.** Kernel-scheduled independently, so
  the 33 ms redraw tick fires regardless of what the work side does. The render
  thread runs its own current-thread tokio runtime for the timer, signals, and
  channel `select!`.
- **Actor, not `Arc<Mutex<State>>`.** The render thread owns display state
  (`Surface`, `avt` grid, `FrameClock`) exclusively; the work side owns its
  domain state exclusively. They communicate by value over one mpsc channel.

### Scene

The render thread is domain-agnostic — it knows nothing of test verdicts,
themes, or QoS. It only calls a closure:

```rust
type SceneFn = Box<dyn Fn(Duration) -> SceneFrame + Send>;

struct SceneFrame {
    left: String,          // pinned panel, left column (phase status)
    right: String,         // pinned panel, right column (transfer tracker)
    live: Option<String>,  // None => paint the emulated PTY grid (compile/build)
                           // Some => explicit ANSI lines (engine run phase)
}
```

The viewport splits into a live region above and a two-column panel below (each
column a constant `PANEL_ROWS` lines). `live: None` mirrors the child's `avt`
grid; the run phase sets `Some(...)` because no child PTY feeds the grid then.

When domain state changes, the work side mutates its own state and pushes a
fresh scene closure capturing a snapshot. The render thread holds the latest
scene and calls it every tick: the spinner/clocks animate because `elapsed`
advances, and data updates when a new scene arrives.

### Ownership

| Resource | Owner |
|---|---|
| `Surface` / inline viewport (the terminal) | render thread |
| `avt::Vt` live grid | render thread |
| `FrameClock` (dirty + spinner gate) | render thread |
| `BannerState`, run progress | work side |
| per-child PTY master + reader thread | work side (`child::run_child`) |

### Message protocol

A single `enum Msg` into one mpsc gives total ordering of all display events:

```rust
enum Msg {
    Scene(SceneFn),            // swap the active render recipe
    Output(Vec<u8>),           // raw PTY bytes from the current child → avt
    Scrollback(String),        // pre-formatted completed lines (verdicts), ANSI
    FlushLive,                 // commit avt grid to scrollback, reset (between children)
    ChildStarted(Option<i32>), // child's pgid (== pid, setsid'd), for Ctrl-C
    ChildExited,
    Shutdown,
}
```

The work side holds a cheap clonable `Console` handle (senders + a `size` watch
+ a `cancel` flag) and a `ConsoleGuard` (owns the thread join; `finish()` sends
`Shutdown` and tears the viewport down).

### Correctness rules

1. **Reader-join before `FlushLive`.** Two producers feed the channel: the PTY
   reader thread (`Output`) and the work thread (`FlushLive`/`Scene`). mpsc only
   guarantees per-producer FIFO, so a `FlushLive` could overtake a child's last
   `Output`. After `child.wait()`, `run_child` joins the reader thread (which
   sends `Output` until PTY EOF) before the caller sends `FlushLive`. The join
   is the happens-before that orders all output ahead of the flush. (A
   grandchild holding the PTY slave open past its parent would block the reader
   in `read()` and hang the join.)

2. **A plain `block_on(future)` keeps the panel live**, because the render
   thread ticks independently. Running a future concurrently with an update
   drain is only for folding concurrent data updates into fresh scenes, not for
   liveness.

3. **`scrolling-regions` must stay OFF.** Completed lines reach native
   scrollback via `ratatui`'s `insert_before`, which only forwards to native
   scrollback while its `scrolling-regions` feature is disabled; with it on,
   `insert_before` scrolls through a DECSTBM margin region most emulators
   exclude from scrollback, silently breaking the design. A `compile_error!` in
   `cli/console/mod.rs` (wired to a guard feature in `Cargo.toml`) fails the
   build if anything enables it.

### Signals and cancellation

A TUI must not run under cooked mode, or the kernel echoes `^C` onto the panel.
On startup `TtyGuard` puts the controlling terminal into a mode with `ECHO` and
`ICANON` off but `ISIG` kept: no keystroke echo, but Ctrl-C still raises
`SIGINT`. `TtyGuard` restores the original attributes on teardown and on `Drop`
(panic/`exit` backstop).

Cancellation is a cooperative state machine keyed on one `watch`-backed `Cancel`
token (`crate::cancel`) the render thread fires and every phase observes.

On the **first Ctrl-C** the render thread: (1) flips to the `cancel_panel`
overlay (shows **Cancelling…** instantly, even if the work side is mid-syscall);
(2) forwards `SIGINT` to the current PTY child's process group; (3) fires the
`Cancel` token.

**Signal routing:**

| Subprocess | How it dies |
|---|---|
| index / dump children (ztest's own group) | receive the terminal's SIGINT directly |
| PTY children — compile / docker / kind (`setsid`) | render thread forwards SIGINT to their pgid |
| engine test processes (`setsid`, own group) | `run_loop`'s cancel arm drops in-flight futures → `kill_on_drop` reaps them |

Each phase watches the token: the engine `run_loop` has a `select!` cancel arm
(stop admitting, drop in-flight); `run_inner` checks `Console::cancelled()`
after every phase and short-circuits to exit **130**. Once the work side
unwinds, `guard.finish()` tears down and the process exits.

**Escalation:** the 2nd Ctrl-C re-sends SIGINT; the **3rd** sends SIGKILL,
restores the terminal in place, and hard-`exit`s.
