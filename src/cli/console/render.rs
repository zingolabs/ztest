//! The persistent render thread and the `Console` handle the work side talks to.
//!
//! A single dedicated OS thread owns the terminal (`Surface`), the `avt` virtual
//! terminal that emulates the current child, the `FrameClock` that decides when
//! to paint, and the accumulated native-scrollback buffer. It runs its own
//! current-thread tokio runtime so its 33 ms redraw tick fires independently of
//! whatever the work side is doing. That independence is the whole point: the
//! bottom panel stays live (spinner, clocks) even while the work side is blocked
//! on a silent multi-second subprocess.
//!
//! The work side never touches the terminal. It holds a cheap, clonable
//! [`Console`] and communicates by value over one mpsc channel ([`Msg`]),
//! pushing immutable [`SceneFn`] render-recipes whenever its own domain state
//! changes. See `docs/console-architecture.md` for the full rationale.

use std::io;
use std::time::{Duration, Instant};

use portable_pty::PtySize;
use tokio::signal::unix::{Signal, SignalKind};
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

use avt::Vt;

use crate::cancel::{Cancel, CancelSource};
// Milliseconds per spinner frame — shared with `preflight`'s `spinner_glyph` so
// the console redraw gate and the glyph table can't drift apart.
use crate::preflight::SPINNER_STEP_MS;

use super::{Surface, bridge};

/// Target frame interval (~30 fps). State changes are folded into the model as
/// they arrive, but the terminal repaints at most once per interval, so an
/// output flood collapses to ~30 clear+repaint cycles a second.
const REDRAW_INTERVAL: Duration = Duration::from_millis(33);

/// One frame's worth of content, produced on demand by a [`SceneFn`]: the two
/// columns of the pinned bottom panel as ANSI strings, which the render thread
/// composes into the side-by-side panel of the sticky footer. `left` is the
/// phase-scoped status (cluster / build / run); `right` is the session-long
/// transfer tracker. Both are up to `PANEL_ROWS` lines. Everything else —
/// subprocess output and the engine's reporter verdicts — reaches the terminal as
/// native scrollback, not through the scene.
pub(crate) struct SceneFrame {
    pub left: String,
    pub right: String,
    /// Explicit live-region content (the run phase's running-tests block). `None`
    /// means "use the child's `avt` grid" — the default for the compile/build
    /// phases, where the live region mirrors the subprocess's own output.
    pub live: Option<String>,
}

/// An immutable recipe for rendering the current data at any instant. `elapsed`
/// is the session-wide clock (drives spinner phase). The work side ships a fresh
/// one whenever its domain state changes; the render thread calls the latest one
/// every tick.
pub(crate) type SceneFn = Box<dyn Fn(Duration) -> SceneFrame + Send>;

/// Renders the pinned panel while cancelling, given the session clock. Provided
/// at [`Console::start`] so the render thread can show "Cancelling…" the instant
/// Ctrl-C arrives, even if the work side is momentarily blocked in a subprocess
/// and hasn't pushed a fresh scene yet. Keeps the render thread domain-agnostic:
/// it swaps to this opaque closure rather than knowing anything about the panel.
pub(crate) type CancelPanelFn = Box<dyn Fn(Duration) -> String + Send>;

/// Messages from the work side to the render thread. A single channel ⇒ total
/// ordering of all display events.
enum Msg {
    Scene(SceneFn),
    Output(Vec<u8>),
    Scrollback(String),
    FlushLive,
    ChildStarted(Option<i32>),
    ChildExited,
    Shutdown,
}

/// The work-side handle: senders plus the shared size/cancel state. Cheap to
/// clone; every clone talks to the same render thread.
#[derive(Clone, Debug)]
pub(crate) struct Console {
    tx: mpsc::UnboundedSender<Msg>,
    size: watch::Receiver<PtySize>,
    cancel: Cancel,
}

/// Owns the render thread's join handle. Kept by the session's top-level flow;
/// [`finish`](ConsoleGuard::finish) shuts the thread down and restores the
/// terminal. Dropping it without `finish` still tears down (best effort).
pub(crate) struct ConsoleGuard {
    tx: mpsc::UnboundedSender<Msg>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Console {
    /// Spawn the render thread and return the handle + its guard. `session_start`
    /// anchors the session-wide spinner clock; `cancel_panel` renders the pinned
    /// panel while cancelling. Fails if the terminal can't be put into an inline
    /// viewport (the caller then falls back to non-TTY mode).
    pub fn start(
        session_start: Instant,
        cancel_panel: CancelPanelFn,
    ) -> io::Result<(Console, ConsoleGuard)> {
        let (tx, rx) = mpsc::unbounded_channel::<Msg>();
        let initial = current_pty_size();
        let (size_tx, size_rx) = watch::channel(initial);
        let (cancel_src, cancel) = CancelSource::new();

        // The render thread reports startup success/failure here so `start`
        // can surface a terminal-setup error synchronously.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<io::Result<()>>();

        let join = std::thread::Builder::new()
            .name("ztest-render".to_string())
            .spawn(move || {
                render_thread(
                    rx,
                    size_tx,
                    cancel_src,
                    cancel_panel,
                    session_start,
                    ready_tx,
                )
            })
            .map_err(io::Error::other)?;

        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let _ = join.join();
                return Err(e);
            }
            Err(_) => {
                let _ = join.join();
                return Err(io::Error::other("render thread died during startup"));
            }
        }

        let console = Console {
            tx: tx.clone(),
            size: size_rx,
            cancel,
        };
        let guard = ConsoleGuard {
            tx,
            join: Some(join),
        };
        Ok((console, guard))
    }

    /// Push a fresh render recipe. Called whenever the work side's domain state
    /// changes; the render thread keeps painting the latest one with an advancing
    /// clock so spinners/timers animate between updates.
    ///
    /// Fire-and-forget: if the render thread is already gone the scene is silently
    /// dropped. That's intentional — a dead render thread means teardown is
    /// underway, and there's nothing left to paint. The same holds for
    /// [`scrollback`](Self::scrollback), [`flush_live`](Self::flush_live), and the
    /// child lifecycle sends; only [`output`](Self::output) reports the loss, so
    /// the PTY reader thread knows to stop.
    pub fn scene(&self, f: impl Fn(Duration) -> SceneFrame + Send + 'static) {
        let _ = self.tx.send(Msg::Scene(Box::new(f)));
    }

    /// Forward raw PTY bytes from a child's reader thread into the `avt` grid.
    /// Returns `false` once the render thread is gone (the reader should stop).
    pub fn output(&self, bytes: Vec<u8>) -> bool {
        self.tx.send(Msg::Output(bytes)).is_ok()
    }

    /// Push already-formatted ANSI lines straight into native scrollback (the
    /// engine's verdict + summary lines).
    pub fn scrollback(&self, ansi: String) {
        let _ = self.tx.send(Msg::Scrollback(ansi));
    }

    /// Commit the current `avt` grid into native scrollback and reset it, so the
    /// next child starts on a clean live region. Used at phase boundaries.
    pub fn flush_live(&self) {
        let _ = self.tx.send(Msg::FlushLive);
    }

    /// Announce the foreground process group of the child now running, so the
    /// render thread can forward Ctrl-C to it.
    pub fn child_started(&self, pgid: Option<i32>) {
        let _ = self.tx.send(Msg::ChildStarted(pgid));
    }

    /// Announce that the current child has exited (clears the Ctrl-C target).
    pub fn child_exited(&self) {
        let _ = self.tx.send(Msg::ChildExited);
    }

    /// The current terminal size, read by `child::run_child` when it opens a PTY.
    pub fn size(&self) -> PtySize {
        *self.size.borrow()
    }

    /// A clone of the size watch, so `child::run_child` can await SIGWINCH-driven
    /// resizes and forward the new width to its PTY child (`master.resize`).
    pub fn size_watch(&self) -> watch::Receiver<PtySize> {
        self.size.clone()
    }

    /// Rows available for the live region above the pinned panel — the size of the
    /// `avt` grid, each child PTY (build/compile), and the ceiling the engine's
    /// running block grows to (see [`super::live_rows_for`]). Read fresh from the
    /// size watch so a SIGWINCH resize reaches the child's PTY, not just the grid.
    pub fn live_rows(&self) -> u16 {
        super::live_rows_for(self.size.borrow().rows)
    }

    /// Whether the user has asked to abort (Ctrl-C). Phases check this between
    /// blocking steps to stop early.
    pub fn cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// A clone of the cancellation observer, for phases that `select!` on it (the
    /// engine run loop) rather than polling.
    pub fn cancel(&self) -> Cancel {
        self.cancel.clone()
    }
}

impl ConsoleGuard {
    /// Shut the render thread down and restore the terminal.
    pub fn finish(mut self) {
        let _ = self.tx.send(Msg::Shutdown);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for ConsoleGuard {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Shutdown);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// The render thread entry point: build the `Surface` on this thread (it owns
/// stdout), report readiness, then run the loop on a current-thread runtime.
fn render_thread(
    rx: mpsc::UnboundedReceiver<Msg>,
    size_tx: watch::Sender<PtySize>,
    cancel: CancelSource,
    cancel_panel: CancelPanelFn,
    session_start: Instant,
    ready_tx: std::sync::mpsc::Sender<io::Result<()>>,
) {
    let surface = match Surface::bottom_panel() {
        Ok(s) => s,
        Err(e) => {
            let _ = ready_tx.send(Err(e));
            return;
        }
    };
    let _ = ready_tx.send(Ok(()));

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return,
    };
    rt.block_on(render_loop(
        rx,
        surface,
        size_tx,
        cancel,
        cancel_panel,
        session_start,
    ));
}

/// The single render loop: the one and only place a frame is painted.
async fn render_loop(
    mut rx: mpsc::UnboundedReceiver<Msg>,
    mut surface: Surface,
    size_tx: watch::Sender<PtySize>,
    cancel: CancelSource,
    cancel_panel: CancelPanelFn,
    session_start: Instant,
) {
    // The `avt` grid's row count: the full height above the pinned panel, so a
    // child's in-place progress block up to this tall stays live and repaints in
    // place rather than being sliced into scrollback. The footer is drawn only as
    // tall as the live content actually is (`trimmed_view`). Recomputed on resize.
    let mut live_rows = surface.live_rows();
    let mut vt = new_vt(surface.cols(), live_rows);
    let mut carry: Vec<u8> = Vec::new();
    // Committed lines awaiting the next atomic present (avt scroll-off + engine
    // lines), as ready-to-write ANSI strings.
    let mut pending: Vec<String> = Vec::new();
    let mut scene: Option<SceneFn> = None;
    let mut clock = FrameClock::new();
    let mut pgid: Option<i32> = None;
    let mut interrupts: u32 = 0;
    // Set on the first Ctrl-C: the panel switches to the `cancel_panel` overlay so
    // the user sees "Cancelling…" instantly, even before the work side reacts.
    let mut cancelling = false;

    let mut sigint = signal(SignalKind::interrupt());
    let mut sigwinch = signal(SignalKind::window_change());
    let mut redraw = tokio::time::interval(REDRAW_INTERVAL);
    redraw.set_missed_tick_behavior(MissedTickBehavior::Delay);
    redraw.tick().await; // drop the immediate first tick

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(Msg::Scene(f)) => {
                    scene = Some(f);
                    clock.mark_dirty();
                }
                Some(Msg::Output(bytes)) => {
                    // Fold the chunk into the grid and mark dirty; the redraw tick
                    // coalesces a burst into one paint. Don't drain the channel
                    // here with a typed `while let Ok(Msg::Output(_))`: on this
                    // unified channel that would consume and discard a non-Output
                    // message queued mid-burst, e.g. a `Scene` or `Scrollback`.
                    feed(&mut vt, &mut carry, &mut pending, &bytes);
                    clock.mark_dirty();
                }
                Some(Msg::Scrollback(ansi)) => {
                    // Already terminal-ready ANSI (engine verdicts / phase lines);
                    // one committed line per text line.
                    pending.extend(ansi.lines().map(str::to_string));
                    clock.mark_dirty();
                }
                Some(Msg::FlushLive) => {
                    pending.extend(bridged(&trimmed_view(&vt)));
                    vt = new_vt(surface.cols(), live_rows);
                    clock.mark_dirty();
                }
                Some(Msg::ChildStarted(p)) => pgid = p,
                Some(Msg::ChildExited) => pgid = None,
                Some(Msg::Shutdown) | None => break,
            },

            _ = recv_signal(&mut sigint) => {
                interrupts += 1;
                // Escalate signal to the current PTY child's group (SIGINT for the
                // first two, SIGKILL from the third). Children spawned in ztest's
                // own group get the terminal's SIGINT directly; this reaches the
                // `setsid`-detached PTY children (cargo/docker/kind).
                forward_interrupt(pgid, interrupts);
                if !cancelling {
                    // First Ctrl-C: fire cooperative cancellation and flip the
                    // panel to the Cancelling overlay.
                    cancel.cancel();
                    cancelling = true;
                }
                if interrupts >= 3 {
                    // Backstop: the user is mashing Ctrl-C and the graceful unwind
                    // isn't happening fast enough. Restore the terminal in place
                    // (Drop is skipped by `exit`) and hard-quit.
                    let _ = surface.finish(&std::mem::take(&mut pending));
                    std::process::exit(130);
                }
                clock.mark_dirty();
            }

            _ = recv_signal(&mut sigwinch) => {
                let size = current_pty_size();
                surface.set_size(size.cols, size.rows);
                // The grid follows the new terminal height; resize it to match.
                // Floor to 1: a 0 dimension underflow-panics inside `avt::resize`
                // (see `new_vt`); terminals can briefly report 0 during a resize.
                live_rows = surface.live_rows();
                let sb: Vec<avt::Line> = vt
                    .resize((size.cols.max(1)) as usize, (live_rows.max(1)) as usize)
                    .scrollback
                    .collect();
                pending.extend(bridged(&sb));
                let _ = size_tx.send(size);
                clock.mark_dirty();
            }

            _ = redraw.tick() => {
                let elapsed = session_start.elapsed();
                if !clock.should_paint(elapsed) {
                    continue;
                }
                // The Cancelling overlay replaces the left column (and clears
                // transfers + live region) once cancellation is in progress. All
                // completed output is already queued in `pending` for native
                // scrollback.
                let (left, right, live_src) = match scene.as_ref() {
                    _ if cancelling => (cancel_panel(elapsed), String::new(), Some(String::new())),
                    Some(scene) => {
                        let f = scene(elapsed);
                        (f.left, f.right, f.live)
                    }
                    // No scene yet: still flush any queued scrollback so early
                    // output isn't withheld, painting an empty panel.
                    None if !pending.is_empty() => (String::new(), String::new(), None),
                    None => continue,
                };
                // Live region: the scene's explicit content (the run phase's
                // running-tests block) when present, else the child's live `avt`
                // grid trimmed to its used rows (compile/build phases) — so the
                // footer is exactly as tall as the child's live output, no blanks.
                let live_lines: Vec<String> = match &live_src {
                    Some(s) => s.lines().map(str::to_string).collect(),
                    None => bridged(&trimmed_view(&vt)),
                };
                surface.present(&pending, &live_lines, &left, &right);
                pending.clear();
            }
        }
    }

    // Teardown: commit any pending scrollback plus the emulator grid's leftover
    // (the last subprocess's final output, not yet scrolled off). Empty during
    // the run phase — the pre-run FlushLive reset the grid and no child feeds it.
    let mut final_lines = std::mem::take(&mut pending);
    final_lines.extend(bridged(&trimmed_view(&vt)));
    let _ = surface.finish(&final_lines);
}

/// The redraw decision, factored out so it's unit-testable: repaint when state
/// changed (`dirty`) or the spinner frame advanced. Starts dirty so the first
/// tick always paints.
struct FrameClock {
    last_spin: u128,
    dirty: bool,
}

impl FrameClock {
    fn new() -> Self {
        FrameClock {
            last_spin: u128::MAX,
            dirty: true,
        }
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Whether to paint now, consuming the dirty flag and latching the spinner
    /// frame for `elapsed`.
    fn should_paint(&mut self, elapsed: Duration) -> bool {
        let spin = elapsed.as_millis() / SPINNER_STEP_MS;
        let paint = self.dirty || spin != self.last_spin;
        if paint {
            self.dirty = false;
            self.last_spin = spin;
        }
        paint
    }
}

/// A fresh `avt` grid of the given size, retaining no scrollback of its own.
/// `scrollback_limit(0)` yields each line the moment it scrolls past the grid,
/// which is our feed into native scrollback.
///
/// Dimensions are floored to 1: `avt` computes `rows - 1` / `cols - 1` internally
/// and underflow-panics on a zero dimension, and some terminals momentarily
/// report a 0 width/height during a resize.
fn new_vt(cols: u16, rows: u16) -> Vt {
    Vt::builder()
        .size(cols.max(1) as usize, rows.max(1) as usize)
        .scrollback_limit(0)
        .build()
}

/// Fold a PTY chunk into the emulator: decode, feed `avt`, and accumulate
/// scrolled-off lines (as ANSI strings) into `pending` for the next present.
fn feed(vt: &mut Vt, carry: &mut Vec<u8>, pending: &mut Vec<String>, bytes: &[u8]) {
    let text = decode(carry, bytes);
    if text.is_empty() {
        return;
    }
    for line in vt.feed_str(&text).scrollback {
        pending.push(bridge::avt_line_to_ansi(&line));
    }
}

/// Convert owned `avt` lines to ready-to-write ANSI strings.
fn bridged(lines: &[avt::Line]) -> Vec<String> {
    lines.iter().map(bridge::avt_line_to_ansi).collect()
}

/// The live grid trimmed of trailing blank rows (avt pads to full height).
fn trimmed_view(vt: &Vt) -> Vec<avt::Line> {
    let mut lines: Vec<avt::Line> = vt.view().cloned().collect();
    while lines
        .last()
        .is_some_and(|l| l.cells().iter().all(|c| c.is_default()))
    {
        lines.pop();
    }
    lines
}

/// Current terminal size as a `PtySize` (80×40 fallback if the query fails).
fn current_pty_size() -> PtySize {
    let (cols, rows) = terminal_size::terminal_size()
        .map(|(w, h)| (w.0, h.0))
        .unwrap_or((80, 40));
    PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    }
}

/// Register a unix signal, or `None` if registration fails.
fn signal(kind: SignalKind) -> Option<Signal> {
    tokio::signal::unix::signal(kind).ok()
}

/// Await the next delivery of an optional signal stream; pends forever when the
/// stream is absent so its `select!` arm simply never fires.
async fn recv_signal(sig: &mut Option<Signal>) {
    match sig {
        Some(s) => {
            s.recv().await;
        }
        None => std::future::pending().await,
    }
}

/// Forward a Ctrl-C to the child's process group, escalating with the count:
/// `1|2` → `SIGINT`, `>=3` → `SIGKILL`. No-op if no child is running.
fn forward_interrupt(pgid: Option<i32>, count: u32) {
    let Some(pgid) = pgid else { return };
    let sig = if count >= 3 {
        libc::SIGKILL
    } else {
        libc::SIGINT
    };
    unsafe { libc::killpg(pgid, sig) };
}

/// Incrementally decode a PTY byte chunk into UTF-8, buffering a trailing
/// incomplete multi-byte sequence in `carry` and substituting the replacement
/// char for a genuinely invalid byte so a stray byte can't wedge the stream.
fn decode(carry: &mut Vec<u8>, chunk: &[u8]) -> String {
    carry.extend_from_slice(chunk);
    let mut out = String::with_capacity(carry.len());
    loop {
        match std::str::from_utf8(carry) {
            Ok(s) => {
                out.push_str(s);
                carry.clear();
                break;
            }
            Err(e) => {
                let valid = e.valid_up_to();
                // SAFETY: `valid` is a UTF-8 boundary by `valid_up_to`'s contract.
                out.push_str(unsafe { std::str::from_utf8_unchecked(&carry[..valid]) });
                match e.error_len() {
                    Some(bad) => {
                        out.push('\u{FFFD}');
                        carry.drain(..valid + bad);
                    }
                    None => {
                        carry.drain(..valid);
                        break;
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_clock_paints_when_dirty_then_quiesces() {
        let mut c = FrameClock::new();
        // Starts dirty → first paint regardless of elapsed.
        assert!(c.should_paint(Duration::ZERO));
        // Same spinner frame, no new state → no paint.
        assert!(!c.should_paint(Duration::from_millis(50)));
        // Spinner frame advanced (100ms step) → paint.
        assert!(c.should_paint(Duration::from_millis(100)));
        // Quiesces again.
        assert!(!c.should_paint(Duration::from_millis(150)));
        // A state change forces a paint within the same frame.
        c.mark_dirty();
        assert!(c.should_paint(Duration::from_millis(150)));
    }

    #[test]
    fn decode_buffers_split_multibyte_across_chunks() {
        let mut carry = Vec::new();
        assert_eq!(decode(&mut carry, &[0xC3]), "");
        assert_eq!(carry, vec![0xC3]);
        assert_eq!(decode(&mut carry, &[0xA9]), "é");
        assert!(carry.is_empty());
    }

    #[test]
    fn decode_substitutes_invalid_bytes_without_wedging() {
        let mut carry = Vec::new();
        let out = decode(&mut carry, &[b'a', 0xFF, b'b']);
        assert_eq!(out, "a\u{FFFD}b");
        assert!(carry.is_empty());
    }

    #[test]
    fn trimmed_view_drops_trailing_blanks_but_keeps_interior_ones() {
        // A 4-row grid: content on row 0 and row 2, an interior blank on row 1,
        // and a trailing blank on row 3. Only the trailing blank should go.
        let mut vt = new_vt(10, 4);
        let _ = vt.feed_str("top\r\n\r\nmid\r\n");
        let texts = bridged(&trimmed_view(&vt));
        assert_eq!(
            texts,
            vec!["top".to_string(), String::new(), "mid".to_string()]
        );
    }

    #[test]
    fn trimmed_view_of_a_blank_grid_is_empty() {
        let vt = new_vt(10, 4);
        assert!(trimmed_view(&vt).is_empty());
    }

    #[test]
    fn feed_forwards_scrolled_off_lines_oldest_first() {
        // A 2-row grid: feeding three newline-terminated lines scrolls the first
        // two off the top (oldest first) into `pending`; the third stays live.
        let mut vt = new_vt(10, 2);
        let mut carry = Vec::new();
        let mut pending: Vec<String> = Vec::new();
        feed(&mut vt, &mut carry, &mut pending, b"a\r\nb\r\nc\r\n");

        assert_eq!(pending, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(bridged(&trimmed_view(&vt)), vec!["c".to_string()]);
    }

    #[test]
    fn feed_holds_a_split_multibyte_char_in_carry() {
        // A UTF-8 sequence split across two PTY reads must not corrupt the grid:
        // the first (partial) feed produces nothing, the second completes it.
        let mut vt = new_vt(10, 1);
        let mut carry = Vec::new();
        let mut pending: Vec<String> = Vec::new();
        feed(&mut vt, &mut carry, &mut pending, &[0xC3]); // lead byte of 'é'
        assert_eq!(carry, vec![0xC3], "partial char buffered, not fed");
        feed(&mut vt, &mut carry, &mut pending, &[0xA9]); // continuation
        assert!(carry.is_empty());
        let live = bridge::avt_line_to_ansi(&vt.view().next().unwrap().clone());
        assert_eq!(live, "é");
    }
}
