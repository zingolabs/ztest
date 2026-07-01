# Console architecture — the persistent render thread

## The problem this solves

The bottom status panel must be **live at all times** for the whole `ztest run`
session — its spinner and clocks must keep ticking no matter what any phase is
doing, even while the work side is fully CPU-bound or blocked on a silent
multi-second subprocess (the `cargo nextest list --message-format=json` index
pass, the image-inventory dump).

The previous design coupled liveness to control flow: the panel only animated
while execution happened to be inside one specific function's `select!` loop
(`run_child`). Every other moment — `paint_panel` one-shots, `block_on(index)`,
`block_on(discover)` — froze the panel by construction. Adding a second animated
loop (`drive`) didn't fix that; it just added a second place where liveness
*happened* to hold.

## The model: one render thread as an actor

```
        work side (sequential phases)              render side (persistent)
   ┌────────────────────────────────┐         ┌──────────────────────────────┐
   │ probe → build → image → run     │         │  DEDICATED OS THREAD          │
   │                                 │  Msg    │  owns: Surface, avt::Vt,      │
   │  pushes Scene snapshots         ├────────▶│        FrameClock, scrollback │
   │  spawns PTY children            │ (mpsc)  │  loop { select! {             │
   │  awaits futures (index, dump)   │         │    msg  => mutate, mark dirty │
   │  reads cancel flag              │◀────────┤    tick => paint if dirty|spin│
   └────────────────────────────────┘ cancel/ │    signal => forward/escalate │
                                       size    │  }}                           │
                                               └──────────────────────────────┘
```

Two decisive choices:

- **Dedicated OS thread, not a tokio task.** If the render loop were a task on
  the work runtime, a phase that saturates the worker threads (the image dump
  spawning subprocesses) could starve it — liveness would again depend on
  scheduler fairness. An OS thread is scheduled by the kernel independently, so
  the 33 ms redraw tick fires regardless of what the work side does. This is the
  strongest form of "live at all times." The render thread runs its own
  current-thread tokio runtime for the timer + signals + channel `select!`.

- **Actor (message passing), not `Arc<Mutex<State>>`.** The render thread owns
  its display state (`Surface`, the `avt` grid, the `FrameClock`) exclusively;
  the work side owns its domain state (`BannerState`, run progress) exclusively.
  Neither reaches into the other's. They communicate by **value** over a single
  mpsc channel. No lock, and `render` stays a pure borrow of single-threaded
  state on each side — the property that made the original design clean is
  preserved, not sacrificed.

## The Scene: an immutable render recipe

The render thread is **domain-agnostic** — it knows nothing of `BannerState`,
`QosPlan`, themes, or test verdicts. It only knows how to call a closure:

```rust
type SceneFn = Box<dyn Fn(Duration) -> SceneFrame + Send>;

struct SceneFrame {
    live: Live,      // Child  => use the emulated PTY grid
                     // Lines  => explicit ANSI lines (the engine run phase)
    panel: String,   // the pinned bottom panel, as an ANSI string
}
```

A `Scene` is an **immutable, self-contained recipe for rendering the current
data at any instant.** Whenever domain state changes, the work side mutates its
own state with plain function calls (exactly as before — `apply_update` et al.
are untouched) and then pushes a *fresh* scene closure capturing a snapshot. The
render thread holds the latest scene and calls it every 33 ms tick:

- the **spinner / clocks animate** because `elapsed` advances each tick, and
- the **data updates** when a new scene arrives.

This is what fully decouples the render thread from every domain type. The work
side speaks only in ANSI strings (which `render_preflight_panel` /
`render_live_panel` / the engine reporter already produce); the render thread
owns *all* `ratatui`/`avt` bridging.

## Ownership map

| Resource                                   | Owner                          | Why                                     |
| ------------------------------------------ | ------------------------------ | --------------------------------------- |
| `Surface` / inline viewport (the terminal) | render thread                  | single writer — no tears                |
| `avt::Vt` live grid                        | render thread                  | display state, fed by `Msg::Output`     |
| `FrameClock` (dirty + spinner gate)        | render thread                  | the one place "paint now" is decided    |
| `BannerState`, run progress                | work side                      | single-threaded, mutated by plain calls |
| per-child PTY master + reader thread       | work side (`child::run_child`) | lives and dies with the child           |

## Message protocol

A single `enum Msg` into one mpsc gives **total ordering** of all display
events — output, scenes, scrollback, and flushes interleave in exactly the
order produced:

```rust
enum Msg {
    Scene(SceneFn),       // swap the active render recipe
    Output(Vec<u8>),      // raw PTY bytes from the current child → fed to avt
    Scrollback(String),   // pre-formatted completed lines (engine verdicts), ANSI
    FlushLive,            // commit the avt grid to scrollback, reset it (between children)
    ChildStarted(Option<i32>), // foreground pgid, so the render thread forwards Ctrl-C
    ChildExited,
    Shutdown,
}
```

The work side holds a cheap, clonable `Console` handle (senders + a `size`
watch + a `cancel` flag) and a `ConsoleGuard` (owns the thread join; `finish()`
sends `Shutdown` and tears the viewport down).

## Two correctness subtleties

1. **Cross-producer ordering at phase boundaries.** Two producers feed the
   channel: the PTY reader thread (`Output`) and the work thread
   (`FlushLive`/`Scene`). mpsc only guarantees *per-producer* FIFO. So a
   `FlushLive` could overtake a child's last `Output`. **Rule:** after
   `child.wait()`, `run_child` **joins the reader thread** (which sends `Output`
   until PTY EOF, then exits) *before* the caller sends `FlushLive`. The join is
   the happens-before that puts all output ahead of the flush.

1. **Liveness no longer needs "draining."** Because the render thread ticks
   independently, a plain `block_on(future)` on the work side keeps the panel
   live. The only reason a phase runs a future *concurrently with an update
   drain* is to fold concurrent **data** updates (the probe landing during the
   compile) into fresh scenes — a data concern, not a liveness one.

## Signals & cancellation (Ctrl-C)

A TUI must not run under cooked mode: the kernel would echo the user's keystrokes
— most visibly the `^C` — straight onto the drawn panel and corrupt it. On
startup the render thread puts the controlling terminal into a mode with `ECHO`
and `ICANON` **off** but `ISIG` **kept** (`TtyGuard`), so:

- no keystroke echo → the panel is never corrupted, and
- Ctrl-C still raises `SIGINT` (rather than arriving as a raw byte we'd have to
  read), which the render thread turns into cooperative cancellation.

`TtyGuard` restores the original attributes on teardown **and** on `Drop`
(panic/`exit` backstop).

Cancellation is a cooperative state machine keyed on one primitive
(`crate::cancel`): a `watch`-backed `Cancel` token the render thread fires and
every phase observes.

**On the first Ctrl-C**, the render thread:

1. flips to the `cancel_panel` overlay → the panel shows **Cancelling…**
   instantly, even if the work side is mid-syscall (the render thread is
   independent);
1. forwards `SIGINT` to the current PTY child's process group (`child_started`
   registered the pgid); and
1. fires the `Cancel` token.

**Signal routing** — every live subprocess is reached:

| Subprocess                                           | How it dies                                                                     |
| ---------------------------------------------------- | ------------------------------------------------------------------------------- |
| index / dump children (spawned in ztest's own group) | receive the terminal's SIGINT directly                                          |
| PTY children — compile / docker / kind (`setsid`)    | render thread forwards SIGINT to their pgid                                     |
| engine test processes (`setsid`, own group)          | `run_loop`'s cancel arm drops the in-flight futures → `kill_on_drop` reaps them |

**Work-side observation** — each phase watches the token: the engine `run_loop`
has a `select!` cancel arm (stop admitting, drop in-flight); `run_inner` checks
`Console::cancelled()` after every phase and short-circuits to exit **130**
rather than misreporting the interrupted phase as a build/setup failure. Once the
work side unwinds, `guard.finish()` tears down and the process exits.

**Escalation** — 2nd Ctrl-C re-sends SIGINT; the **3rd** sends SIGKILL, restores
the terminal in place, and hard-`exit`s (the backstop when a graceful unwind
stalls).

## What this deleted

`run_child`'s and `drive`'s duplicated `select!` loops collapse into one render
loop; `paint_panel` one-shots, the `runtime()` escape hatch, and the engine's
`into_surface` / `commit_live` handoff dance are all gone — the render thread
owns one bottom-anchored viewport for the entire session, so the run phase is
"just another scene producer."

## Known POC limitations (follow-ups)

- **Per-child PTY resize on SIGWINCH is not propagated to the child.** The
  render side resizes its `avt` grid + viewport correctly (so our layout stays
  right), but the in-flight child keeps its old width until it next repaints.
  Wiring `master.resize()` from the size watch is a clean follow-up.
- The run-phase spinner advances at the engine's tick rate (it ships a fresh
  scene per tick) rather than the render thread's; preflight scenes animate at
  the render rate. Unifying on render-rate animation would mean having the engine
  scene recompute clocks from captured `Instant`s.
- On Ctrl-C, engine test processes die via `kill_on_drop` (SIGKILL to the direct
  child); a test's *own* spawned helpers (pods, port-forwards in its process
  group) are left to the 1 h namespace janitor rather than group-killed inline.
  Threading each inflight test's pgid up to a `kill_group` on cancel is a clean
  follow-up.
