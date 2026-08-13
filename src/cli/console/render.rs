//! Persistent render thread + the `Console` handle the work side talks to.
//!
//! - One OS thread owns `Surface` + `avt` + [`FrameClock`] + scrollback, on its own
//!   current-thread runtime (33 ms tick fires independently of the work side)
//! - Work side talks by value over one mpsc channel ([`Msg`]) = total ordering of
//!   every display event
//! - See `docs/design-execution-engine.md`

use std::io;
use std::time::{Duration, Instant};

use portable_pty::PtySize;
use tokio::signal::unix::{Signal, SignalKind};
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

use avt::Vt;

use crate::cancel::{Cancel, CancelSource};
// Shared with `preflight`'s `spinner_glyph` (redraw gate & glyph table cannot drift)
use crate::ui::SPINNER_STEP_MS;

use super::{Surface, bridge};

/// Target frame interval (~30 fps) → an output flood collapses to ~30 repaints/s
const REDRAW_INTERVAL: Duration = Duration::from_millis(33);

/// One frame of panel content, produced on demand by a [`SceneFn`].
///
/// - `left` = phase status, `mid` = third column (wide terminals only), `right` =
///   session-long transfer tracker; at two columns `mid` survives, `right` drops
/// - `live` = explicit live-region content, `None` = mirror the child's `avt` grid
pub(crate) struct SceneFrame {
    pub left: String,
    pub mid: Option<String>,
    pub right: String,
    pub live: Option<String>,
}

/// Immutable recipe for rendering current data at any instant, called every tick.
/// `elapsed` = session-wide clock (drives spinner phase)
pub(crate) type SceneFn = Box<dyn Fn(Duration) -> SceneFrame + Send>;

/// Renders the pinned panel while cancelling. Handed in at [`Console::start`] so
/// "Cancelling…" shows on the Ctrl-C itself, before any fresh scene arrives
pub(crate) type CancelPanelFn = Box<dyn Fn(Duration) -> String + Send>;

/// Work side → render thread
enum Msg {
    Scene(SceneFn),
    Output(Vec<u8>),
    Scrollback(String),
    FlushLive,
    ChildStarted(Option<i32>),
    ChildExited,
    Fatal(String),
    Shutdown,
}

/// Work-side handle: senders + shared size/cancel state. Cheap to clone (every clone
/// talks to the same render thread)
#[derive(Clone, Debug)]
pub(crate) struct Console {
    tx: mpsc::UnboundedSender<Msg>,
    size: watch::Receiver<PtySize>,
    cancel: Cancel,
}

/// Owns the render thread's join handle, held by the session's top-level flow.
/// [`finish`](ConsoleGuard::finish) restores the terminal; plain drop tears down best-effort
pub(crate) struct ConsoleGuard {
    tx: mpsc::UnboundedSender<Msg>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Console {
    /// Spawn the render thread → handle + guard. `session_start` anchors the spinner
    /// clock. Fails when the terminal takes no inline viewport (caller → non-TTY mode)
    pub fn start(
        session_start: Instant,
        cancel_panel: CancelPanelFn,
    ) -> io::Result<(Console, ConsoleGuard)> {
        let (tx, rx) = mpsc::unbounded_channel::<Msg>();
        let initial = current_pty_size();
        let (size_tx, size_rx) = watch::channel(initial);
        let (cancel_src, cancel) = CancelSource::new();

        // Startup result, so `start` surfaces a terminal-setup error synchronously
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<io::Result<()>>();

        let join = std::thread::Builder::new()
            .name("ztest-render".to_string())
            .spawn(move || {
                render_thread(rx, size_tx, cancel_src, cancel_panel, session_start, ready_tx)
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

        let console = Console { tx: tx.clone(), size: size_rx, cancel };
        let guard = ConsoleGuard { tx, join: Some(join) };
        Ok((console, guard))
    }

    /// Push a fresh render recipe. Latest one keeps painting with an advancing clock
    /// (spinners/timers animate between updates).
    ///
    /// Fire-and-forget, as are [`scrollback`](Self::scrollback),
    /// [`flush_live`](Self::flush_live) and the child-lifecycle sends; only
    /// [`output`](Self::output) reports the loss (its reader thread must stop)
    pub fn scene(&self, f: impl Fn(Duration) -> SceneFrame + Send + 'static) {
        let _ = self.tx.send(Msg::Scene(Box::new(f)));
    }

    /// Raw PTY bytes from a child's reader thread → the `avt` grid. `false` once the
    /// render thread is gone (reader should stop)
    pub fn output(&self, bytes: Vec<u8>) -> bool {
        self.tx.send(Msg::Output(bytes)).is_ok()
    }

    /// Pre-formatted ANSI straight into native scrollback (engine verdict + summary)
    pub fn scrollback(&self, ansi: String) {
        let _ = self.tx.send(Msg::Scrollback(ansi));
    }

    /// Commit the `avt` grid into native scrollback and reset it, at a phase boundary
    /// (next child starts on a clean live region)
    pub fn flush_live(&self) {
        let _ = self.tx.send(Msg::FlushLive);
    }

    /// Foreground pgid of the running child, for Ctrl-C forwarding
    pub fn child_started(&self, pgid: Option<i32>) {
        let _ = self.tx.send(Msg::ChildStarted(pgid));
    }

    pub fn child_exited(&self) {
        let _ = self.tx.send(Msg::ChildExited);
    }

    /// Record a fatal message, printed once on the clean line after teardown.
    ///
    /// - Durable where a mid-run `eprintln!` is not (the footer repaints in place over it)
    /// - First message only (later ones are cascade)
    pub fn fatal(&self, msg: String) {
        let _ = self.tx.send(Msg::Fatal(msg));
    }

    /// Current terminal size, read by `child::run_child` when it opens a PTY
    pub fn size(&self) -> PtySize {
        *self.size.borrow()
    }

    /// Size watch clone, so `child::run_child` awaits SIGWINCH and forwards the new
    /// width to its PTY child (`master.resize`)
    pub fn size_watch(&self) -> watch::Receiver<PtySize> {
        self.size.clone()
    }

    /// Rows for the live region above the panel (see [`super::live_rows_for`]). Read
    /// fresh from the size watch → a SIGWINCH reaches the child's PTY, not just the grid
    pub fn live_rows(&self) -> u16 {
        super::live_rows_for(self.size.borrow().rows)
    }

    /// Abort requested (Ctrl-C). Phases poll between blocking steps to stop early
    pub fn cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Cancellation observer clone, for phases that `select!` rather than poll
    pub fn cancel(&self) -> Cancel {
        self.cancel.clone()
    }
}

impl ConsoleGuard {
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

/// Render thread entry: build `Surface` here (this thread owns stdout), report
/// readiness, then run the loop on a current-thread runtime
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

    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(_) => return,
    };
    rt.block_on(render_loop(rx, surface, size_tx, cancel, cancel_panel, session_start));
}

/// Sole place a frame is painted
async fn render_loop(
    mut rx: mpsc::UnboundedReceiver<Msg>,
    mut surface: Surface,
    size_tx: watch::Sender<PtySize>,
    cancel: CancelSource,
    cancel_panel: CancelPanelFn,
    session_start: Instant,
) {
    // Grid rows = full height above the panel (an in-place progress block stays live
    // instead of being sliced into scrollback); recomputed on resize
    let mut live_rows = surface.live_rows();
    let mut vt = new_vt(surface.cols(), live_rows);
    let mut carry: Vec<u8> = Vec::new();
    // Committed ANSI lines awaiting the next atomic present
    let mut pending: Vec<String> = Vec::new();
    let mut scene: Option<SceneFn> = None;
    let mut clock = FrameClock::new();
    let mut pgid: Option<i32> = None;
    let mut interrupts: u32 = 0;
    // First Ctrl-C switches the panel to the `cancel_panel` overlay
    let mut cancelling = false;
    // Printed after teardown (see `Console::fatal`); held to the loop's end so it
    // survives the footer's in-place repaints
    let mut fatal: Option<String> = None;

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
                    // Redraw tick already coalesces a burst. Never drain here with a
                    // typed `while let Ok(Msg::Output(_))`: on one unified channel that
                    // discards a `Scene`/`Scrollback` queued mid-burst
                    feed(&mut vt, &mut carry, &mut pending, &bytes);
                    clock.mark_dirty();
                }
                Some(Msg::Scrollback(ansi)) => {
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
                Some(Msg::Fatal(m)) => {
                    if fatal.is_none() {
                        fatal = Some(m);
                    }
                }
                Some(Msg::Shutdown) | None => break,
            },

            _ = recv_signal(&mut sigint) => {
                interrupts += 1;
                // `setsid`-detached PTY children never get the terminal's SIGINT
                // directly (third interrupt escalates to SIGKILL)
                forward_interrupt(pgid, interrupts);
                if !cancelling {
                    cancel.cancel();
                    cancelling = true;
                }
                if interrupts >= 3 {
                    // Ctrl-C mashed: restore in place (`exit` skips Drop), hard-quit
                    surface.finish(&std::mem::take(&mut pending));
                    std::process::exit(130);
                }
                clock.mark_dirty();
            }

            _ = recv_signal(&mut sigwinch) => {
                let size = current_pty_size();
                surface.set_size(size.cols, size.rows);
                // Floored to 1 (a 0 dimension underflow-panics inside `avt::resize`)
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
                // Cancelling: overlay replaces the left column, clears transfers + live
                // region (completed output already sits in `pending`)
                let (left, mid, right, live_src) = match scene.as_ref() {
                    _ if cancelling => (
                        cancel_panel(elapsed),
                        None,
                        String::new(),
                        Some(String::new()),
                    ),
                    Some(scene) => {
                        let f = scene(elapsed);
                        (f.left, f.mid, f.right, f.live)
                    }
                    // No scene yet: flush queued scrollback against an empty panel
                    None if !pending.is_empty() => (String::new(), None, String::new(), None),
                    None => continue,
                };
                // Scene's explicit live content, else the `avt` grid trimmed to used
                // rows (no blanks above the footer)
                let live_lines: Vec<String> = match &live_src {
                    Some(s) => s.lines().map(str::to_string).collect(),
                    None => bridged(&trimmed_view(&vt)),
                };
                surface.present(&pending, &live_lines, &left, mid.as_deref(), &right);
                pending.clear();
            }
        }
    }

    // Teardown: pending scrollback + the grid's leftover (last subprocess's output,
    // never scrolled off)
    let mut final_lines = std::mem::take(&mut pending);
    final_lines.extend(bridged(&trimmed_view(&vt)));
    surface.finish(&final_lines);

    // Footer erased, cursor on a fresh line → the write lands durably.
    // Sole place a fatal error/panic reaches the user
    if let Some(msg) = fatal {
        eprintln!("{msg}");
    }
}

/// Redraw decision: repaint on changed state (`dirty`) or an advanced spinner frame.
/// Starts dirty (first tick always paints)
struct FrameClock {
    last_spin: u128,
    dirty: bool,
}

impl FrameClock {
    fn new() -> Self {
        FrameClock { last_spin: u128::MAX, dirty: true }
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

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

/// Fresh `avt` grid.
///
/// - `scrollback_limit(0)` yields each line as it scrolls off = our native-scrollback feed
/// - Dimensions floored to 1 (`avt` underflow-panics on 0, which terminals report mid-resize)
fn new_vt(cols: u16, rows: u16) -> Vt {
    Vt::builder().size(cols.max(1) as usize, rows.max(1) as usize).scrollback_limit(0).build()
}

/// Fold a PTY chunk into the emulator, scrolled-off lines → `pending` for the next present
fn feed(vt: &mut Vt, carry: &mut Vec<u8>, pending: &mut Vec<String>, bytes: &[u8]) {
    let text = decode(carry, bytes);
    if text.is_empty() {
        return;
    }
    for line in vt.feed_str(&text).scrollback {
        pending.push(bridge::avt_line_to_ansi(&line));
    }
}

fn bridged(lines: &[avt::Line]) -> Vec<String> {
    lines.iter().map(bridge::avt_line_to_ansi).collect()
}

/// Live grid minus trailing blank rows (avt pads to full height)
fn trimmed_view(vt: &Vt) -> Vec<avt::Line> {
    let mut lines: Vec<avt::Line> = vt.view().cloned().collect();
    while lines.last().is_some_and(|l| l.cells().iter().all(|c| c.is_default())) {
        lines.pop();
    }
    lines
}

/// Current terminal size as a `PtySize` (80×40 fallback on a failed query)
fn current_pty_size() -> PtySize {
    let (cols, rows) = terminal_size::terminal_size().map(|(w, h)| (w.0, h.0)).unwrap_or((80, 40));
    PtySize { rows, cols, pixel_width: 0, pixel_height: 0 }
}

fn signal(kind: SignalKind) -> Option<Signal> {
    tokio::signal::unix::signal(kind).ok()
}

/// Await the next delivery of an optional signal stream. Absent stream → pends forever,
/// so its `select!` arm never fires
async fn recv_signal(sig: &mut Option<Signal>) {
    match sig {
        Some(s) => {
            s.recv().await;
        }
        None => std::future::pending().await,
    }
}

/// Ctrl-C → the child's process group, escalating: `1|2` → `SIGINT`, `>=3` → `SIGKILL`.
/// No-op with no child running
fn forward_interrupt(pgid: Option<i32>, count: u32) {
    let Some(pgid) = pgid else { return };
    let sig = if count >= 3 { libc::SIGKILL } else { libc::SIGINT };
    unsafe { libc::killpg(pgid, sig) };
}

/// Incrementally decode a PTY chunk to UTF-8: trailing incomplete sequence → `carry`,
/// invalid byte → U+FFFD (a stray byte cannot wedge the stream)
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
        // Starts dirty → paints whatever `elapsed`
        assert!(c.should_paint(Duration::ZERO));
        // Same spinner frame, no new state
        assert!(!c.should_paint(Duration::from_millis(50)));
        // Spinner frame advanced (100ms step)
        assert!(c.should_paint(Duration::from_millis(100)));
        assert!(!c.should_paint(Duration::from_millis(150)));
        // State change repaints within one frame
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
        // 4 rows: content on 0 and 2, interior blank on 1, trailing blank on 3
        let mut vt = new_vt(10, 4);
        let _ = vt.feed_str("top\r\n\r\nmid\r\n");
        let texts = bridged(&trimmed_view(&vt));
        assert_eq!(texts, vec!["top".to_string(), String::new(), "mid".to_string()]);
    }

    #[test]
    fn trimmed_view_of_a_blank_grid_is_empty() {
        let vt = new_vt(10, 4);
        assert!(trimmed_view(&vt).is_empty());
    }

    #[test]
    fn feed_forwards_scrolled_off_lines_oldest_first() {
        // 2 rows, 3 lines → first two scroll off oldest-first, third stays live
        let mut vt = new_vt(10, 2);
        let mut carry = Vec::new();
        let mut pending: Vec<String> = Vec::new();
        feed(&mut vt, &mut carry, &mut pending, b"a\r\nb\r\nc\r\n");

        assert_eq!(pending, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(bridged(&trimmed_view(&vt)), vec!["c".to_string()]);
    }

    #[test]
    fn feed_holds_a_split_multibyte_char_in_carry() {
        // Split across two PTY reads: partial feed produces nothing, second completes
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
