# Execution engine and console

How `ztest run` executes tests and renders progress.

- Engine = **producer**: schedules, spawns one process per test, emits an event stream
- Console = **consumer**: a dedicated render thread owning all terminal bridging
- Shared: the event stream and nothing else (engine speaks ANSI strings + scene snapshots)

## Engine

### Module layout (`src/engine/`)

| Module      | Responsibility                                                    |
| ----------- | ----------------------------------------------------------------- |
| `mod.rs`    | `EngineConfig`, `run()`, public surface                           |
| `plan.rs`   | `TestPlan` / `PlannedTest` — turn inventory into scheduler inputs |
| `exec.rs`   | process-per-test spawn, capture, timeout/termination, verdict     |
| `loop.rs`   | the run loop: `Scheduler` ↔ exec workers ↔ events                 |
| `event.rs`  | `EngineEvent` — the live stream                                   |
| `report.rs` | `EngineEvent` → render (on `ui/render.rs` + `ui/theme.rs`)        |

Pure scheduling core = `qos/scheduler.rs`; the engine is the I/O shell around it
([design-qos.md](design-qos.md)).

### Phases

```
ztest run args
  ├─ Phase A  cluster probe (pipeline/cluster.rs) → ClusterCapacity
  ├─ Phase B  inventory: cargo nextest list --message-format=json
  │            (pipeline/build.rs) → TestPlan
  └─ Phase D  engine run loop (src/engine/): Scheduler grants spawns,
             exec workers run one process per test, EngineEvent stream out
```

Inventory shells out to `cargo nextest list --message-format=json`, parsed via
`nextest_metadata::TestListSummary` — the only place nextest is invoked.

### TestPlan

`plan.rs` combines `TestListSummary` with the per-binary QoS dump (`ZTEST_DUMP_INVENTORY`,
`pipeline/images.rs`):

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

`footprint` feeds straight into `Scheduler::request` as a `qos::scheduler::Request`.

### Run loop (`loop.rs`)

1. `Scheduler::request()` per `PlannedTest` — `Granted` → spawn, `Queued` → wait, `Rejected` → fail fast
   (unschedulable / over budget)
1. On grant, `exec::spawn` sets `NEXTEST_BINARY_ID`, `NEXTEST_TEST_NAME`,
   `NEXTEST_EXECUTION_MODE=process-per-test`, dylib path, cwd → `<bin> --exact <name> --nocapture`.
   Hard-cap timer arms at spawn, which is also admission (tests spawn only after a grant)
1. On exit the **exit code is the verdict** (0 = pass), output captured verbatim → `TestFinished`;
   `Scheduler::release(lease)` backfills grants
1. Free capacity reconciled from the k8s `Allocator`/probe (`Scheduler::reconcile`); the loop owns cadence
1. Bounded exec workers, but the `Scheduler` — not a fixed pool — decides how many run, by 2-D capacity
1. Retries + `--no-fail-fast` handled here

Not-yet-started tests queue in the `Scheduler`, never as forked-but-blocked OS processes.

### Capacity oracle

- Engine `Scheduler` = local pre-gate; it won't spawn what it doesn't expect to fit, so `TestEnv::build()`
  admission usually succeeds immediately
- Authoritative cross-run arbitration stays with the k8s-Lease ledger (`qos/ledger.rs`)
- Test identity (`NEXTEST_BINARY_ID`/`NEXTEST_TEST_NAME`) set by the engine at spawn, consumed by `env.rs`

### EngineEvent

`RunStarted`, `TestStarted`, `TestSlow`, `TestFinished`, `TestSkipped`, `RunFinished`. JUnit XML (CI) is a
second consumer of the same stream.

### Reporter output (`reporter.rs`)

`StyledReporter` formats the stream into `cargo nextest run`'s default human output: verdict lines
(`PASS`/`FAIL`/`SLOW`/`TRY n …`), the `output ───` block, `Summary`, failure recap.

- No `nextest-runner` dependency — every line generated here, and the invariant is byte-identity with
  nextest. Change a status or summary line only against a diff with nextest
- `Verdict` models pass/fail/timeout/spawn-error only → nextest's leak/flaky/slow-pass/abort words are
  unreachable by construction

#### Divergence: the captured-output block is de-framed

The one divergence, confined to bytes *inside* the `output ───` block — never a line we generate.

Tests run `<binary> --exact <name> --nocapture` (`local_runner.rs`, `pod_runner.rs`), so the stream
carries libtest's per-run framing:

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

nextest replays that verbatim; for a one-test process it is all noise rendered better elsewhere —
`running 1 test` is a constant, `test <name> ... ` restates our `FAIL […] <name>` line (and is what the
first log glues onto), the footer restates `Summary` + recap. So:

```
        FAIL [  83.222s] clientless::… value_pools_respect_the_boundary_on_the_pub_testnet
  output ───
    2026-07-27T20:56:31 INFO ztest::env: starting test run …
    2026-07-27T20:56:32 INFO ztest::env: provisioning validator …
    Error: archive materialize failed for …/zebra.tar.xz: No such file or directory
```

`strip_libtest_frame(output, test_name)`, before replay:

- Drops through the `test <name> ... ` marker (which also un-glues the first log line)
- Peels the footer from the trailing `test result: ` line, consuming exactly one verdict token so a log
  line that merely reads `FAILED` survives
- Keeps panic output (`thread … panicked`, backtrace notes) — it precedes the verdict on stderr
- Anchors each cut on a marker; a stream missing an anchor is left uncut on that side (verbatim replay
  beats risking eaten output)

New divergences go in this section with their rationale — byte-identity stays the default, every
exception on the record.

## Console (render thread)

Bottom status panel stays live all session — spinner and clocks tick even while the work side is
CPU-bound or blocked on a silent subprocess (inventory index pass, image-inventory dump).

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

- **Dedicated OS thread, not a tokio task** — kernel-scheduled independently, so the 33 ms redraw tick
  fires regardless of the work side. It runs its own current-thread runtime for timer, signals, `select!`
- **Actor, not `Arc<Mutex<State>>`** — render thread exclusively owns display state (`Surface`, `avt`
  grid, `FrameClock`), work side exclusively owns domain state; they communicate by value over one mpsc

### Scene

Render thread is domain-agnostic (knows nothing of verdicts, themes, QoS) — it only calls a closure:

```rust
type SceneFn = Box<dyn Fn(Duration) -> SceneFrame + Send>;

struct SceneFrame {
    left: String,          // pinned panel, left column (phase status)
    right: String,         // pinned panel, right column (transfer tracker)
    live: Option<String>,  // None => paint the emulated PTY grid (compile/build)
                           // Some => explicit ANSI lines (engine run phase)
}
```

- Viewport = live region above, two-column panel below (each column a constant `PANEL_ROWS`)
- `live: None` mirrors the child's `avt` grid; the run phase sets `Some(...)` because no child PTY feeds
  the grid then
- Work side mutates its own state, then pushes a fresh closure capturing a snapshot; the render thread
  calls the latest one every tick → spinner/clocks animate off `elapsed`, data updates on arrival

### Ownership

| Resource                                   | Owner                          |
| ------------------------------------------ | ------------------------------ |
| `Surface` / inline viewport (the terminal) | render thread                  |
| `avt::Vt` live grid                        | render thread                  |
| `FrameClock` (dirty + spinner gate)        | render thread                  |
| `BannerState`, run progress                | work side                      |
| per-child PTY master + reader thread       | work side (`child::run_child`) |

### Message protocol

One `enum Msg` into one mpsc = total ordering of all display events:

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

Work side holds a cheap clonable `Console` (senders + `size` watch + `cancel` flag) and a `ConsoleGuard`
(owns the thread join; `finish()` sends `Shutdown` and tears the viewport down).

### Correctness rules

1. **Reader-join before `FlushLive`.** Two producers feed the channel — the PTY reader thread (`Output`)
   and the work thread (`FlushLive`/`Scene`) — and mpsc guarantees only per-producer FIFO, so a
   `FlushLive` could overtake a child's last `Output`. After `child.wait()`, `run_child` joins the reader
   (which sends until PTY EOF) before the caller sends `FlushLive`; the join is the happens-before.
   (A grandchild holding the PTY slave open past its parent blocks the reader in `read()` → join hangs.)
1. **A plain `block_on(future)` keeps the panel live** — the render thread ticks independently. Running a
   future concurrently with an update drain is for folding concurrent data into fresh scenes, not liveness
1. **`scrolling-regions` must stay OFF.** Completed lines reach native scrollback via ratatui's
   `insert_before`, which forwards there only while that feature is disabled; enabled, it scrolls through
   a DECSTBM margin region most emulators exclude from scrollback, silently breaking the design. A
   `compile_error!` in `cli/console/mod.rs` (wired to a guard feature) fails the build if anything enables it

### Signals and cancellation

A TUI must not run in cooked mode (the kernel echoes `^C` onto the panel). `TtyGuard` sets `ECHO` and
`ICANON` off, `ISIG` kept — no echo, Ctrl-C still raises `SIGINT` — and restores the original attributes on teardown
and on `Drop` (panic/`exit` backstop).

Cancellation = a cooperative state machine on one `watch`-backed `Cancel` token (`crate::cancel`), fired
by the render thread and observed by every phase. First Ctrl-C, render thread:

1. Flips to the `cancel_panel` overlay (**Cancelling…** instantly, even mid-syscall on the work side)
1. Forwards `SIGINT` to the current PTY child's process group
1. Fires the `Cancel` token

| Subprocess                                        | How it dies                                                                 |
| ------------------------------------------------- | --------------------------------------------------------------------------- |
| index / dump children (ztest's own group)         | receive the terminal's SIGINT directly                                      |
| PTY children — compile / docker / kind (`setsid`) | render thread forwards SIGINT to their pgid                                 |
| engine test processes (`setsid`, own group)       | `run_loop`'s cancel arm drops in-flight futures → `kill_on_drop` reaps them |

- Engine `run_loop` has a `select!` cancel arm (stop admitting, drop in-flight)
- `run_inner` checks `Console::cancelled()` after every phase → short-circuits to exit **130**
- Once the work side unwinds, `guard.finish()` tears down and the process exits
- Escalation: 2nd Ctrl-C re-sends SIGINT; **3rd** sends SIGKILL, restores the terminal in place, hard-exits
