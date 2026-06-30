//! The PTY console driver — a single subprocess emulated under a PTY through
//! `avt`, rendered via the shared [`Surface`](super::Surface) primitive.
//!
//! ## Data flow
//!
//! ```text
//!   child (cargo nextest list / docker build / cargo nextest run, on a PTY)
//!         │  raw bytes (colours, cursor moves, its own progress bar)
//!         ▼
//!   PTY master ──reader thread──▶ mpsc<Vec<u8>>
//!         │
//!         ▼
//!   avt::Vt  (in-memory VT: owns the child's grid)
//!         │            │
//!         │            └── Changes::scrollback (lines scrolled off the top)
//!         │                ──▶ Surface::insert_scrollback ──▶ NATIVE scrollback
//!         ▼
//!   Vt::view()  (the live visible grid) ──▶ live region of the inline viewport
//!                                            status panel ──▶ bottom of viewport
//! ```
//!
//! Running the child under a **PTY** (rather than a pipe) is what keeps its
//! native output intact: cargo and docker only emit colour and their in-place
//! progress bars when they detect a TTY. `avt` interprets those cursor-addressed
//! updates into a grid we bridge to `ratatui` lines and hand to the
//! [`Surface`](super::Surface), which owns the terminal and the atomic present.
//! One [`Console`] is created per session and reused for every preflight/build
//! phase via [`Console::run_child`]; only the panel renderer and concurrent
//! state differ. (The test-run phase is the engine's job now — see
//! [`crate::engine`].)
//!
//! ## Why no raw mode
//!
//! The parent reads no keystrokes, and cooked mode keeps the kernel delivering
//! `SIGINT` to us on Ctrl-C, which we forward to the child's process group (see
//! [`forward_interrupt`]).
//!
//! ## Signals
//!
//! The child is a session leader on its own controlling terminal
//! ([`portable_pty`] does `setsid` + `TIOCSCTTY`), so the real terminal's
//! signals don't reach it. We bridge two: **SIGWINCH** (re-query width, push to
//! the PTY + `avt` so the child reflows) and **SIGINT** (forward to the child's
//! process group; the third Ctrl-C escalates to `SIGKILL`).

use std::io::{self, Read};
use std::time::{Duration, Instant};

use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use ratatui::text::Line;
use tokio::signal::unix::{Signal, SignalKind};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::MissedTickBehavior;

use avt::Vt;

use super::{Surface, bridge};

/// Target frame interval for the live console (~30 fps). The child's bytes are
/// folded into the `avt` grid as they arrive, but the terminal is repainted at
/// most once per interval — so a flood of output (nextest's progress churn)
/// drives at most ~30 clear+repaint cycles a second instead of one per PTY
/// read. This is the real, terminal-independent fix for the flicker: bounding
/// the *amount* of clear+repaint work. Synchronized output ([`Surface::present`])
/// then makes each of these bounded frames tear-free where 2026 is supported.
const REDRAW_INTERVAL: Duration = Duration::from_millis(33);

/// Milliseconds per spinner frame (matches [`crate::preflight`]'s
/// `spinner_glyph`). The redraw loop forces a repaint when the derived frame
/// index changes, so the "is the cluster still alive?" heartbeat keeps
/// animating even when the child has produced no new output.
const SPINNER_STEP_MS: u128 = 100;

/// The session-long bottom console: the shared [`Surface`](super::Surface) (an
/// inline viewport), an `avt` virtual terminal that every child is emulated
/// through, and a reused current-thread runtime for the per-child event loop.
pub(crate) struct Console {
    surface: Surface,
    vt: Vt,
    rt: tokio::runtime::Runtime,
    /// Buffers UTF-8 bytes split across PTY read boundaries (see [`decode`]).
    carry: Vec<u8>,
}

impl Console {
    /// Open the console: a full-height [`Surface`](super::Surface) with a live
    /// region, an `avt` grid sized to match, and the runtime.
    pub fn new() -> io::Result<Console> {
        let surface = Surface::with_live_region()?;
        // `scrollback_limit(0)` makes avt yield every line the moment it scrolls
        // past the visible grid (retaining none itself) — our feed into native
        // scrollback.
        let vt = Vt::builder()
            .size(surface.cols() as usize, surface.live_rows() as usize)
            .scrollback_limit(0)
            .build();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        Ok(Console {
            surface,
            vt,
            rt,
            carry: Vec::new(),
        })
    }

    /// The runtime, so callers can spawn concurrent background work (a cluster
    /// probe, a reservation poll) that feeds the panel via an updates channel.
    pub fn runtime(&self) -> &tokio::runtime::Runtime {
        &self.rt
    }

    /// Paint a frame with no child running (the current live grid + `panel`) —
    /// an initial frame, or a panel refresh between phases.
    pub fn paint_panel(&mut self, panel: &str) -> io::Result<()> {
        let live = view_lines(&self.vt);
        self.surface.paint_panel(&live, panel)
    }

    /// Commit the current live grid (trimmed of trailing blank rows) into native
    /// scrollback and reset the emulator to a blank grid, so the next phase
    /// starts on a clean live region with nothing lost. Callers should
    /// [`paint_panel`](Self::paint_panel) afterward to repaint the now-blank
    /// live region.
    pub fn flush_live(&mut self) -> io::Result<()> {
        let lines = bridged(&trimmed_view(&self.vt));
        self.surface.insert_scrollback(&lines)?;
        self.vt = Vt::builder()
            .size(self.surface.cols() as usize, self.surface.live_rows() as usize)
            .scrollback_limit(0)
            .build();
        Ok(())
    }

    /// Hand the live [`Surface`](super::Surface) and the runtime to the test
    /// engine, which drives the run phase itself (process-per-test, no PTY child)
    /// — see the module docs. The preflight phase's final live grid is committed
    /// to native scrollback first, so the handoff is seamless (one viewport, no
    /// flash) and nothing is lost. The `avt` emulator is dropped — the engine
    /// renders formatted lines directly, with no child grid to interpret.
    pub fn into_surface(
        self,
        live_rows: u16,
    ) -> io::Result<(Surface, tokio::runtime::Runtime)> {
        let Console {
            surface,
            vt,
            rt,
            carry: _,
        } = self;
        let lines = bridged(&trimmed_view(&vt));
        // Collapse the full-height preflight viewport into the compact run
        // viewport (a small `live_rows` running region + the pinned panel),
        // committing the final preflight grid to scrollback en route (see
        // `Surface::into_run_surface`).
        let surface = surface.into_run_surface(&lines, live_rows)?;
        Ok((surface, rt))
    }

    /// Run `program args` under the PTY until it exits, emulating it in the live
    /// region and forwarding scrolled-off lines into native scrollback. Each
    /// repaint renders the panel from `render(state, elapsed)`; `apply` folds in
    /// messages from concurrent background work on `updates`; `on_line` sees
    /// each completed (scrolled-off) line — used by the run phase to tally
    /// verdicts. Returns the child's exit code (`130` on Ctrl-C).
    #[allow(clippy::too_many_arguments)]
    pub fn run_child<S, U>(
        &mut self,
        program: &str,
        args: &[String],
        envs: &[(&str, String)],
        started: Instant,
        state: &mut S,
        updates: &mut UnboundedReceiver<U>,
        mut apply: impl FnMut(&mut S, U),
        mut on_line: impl FnMut(&mut S, &str),
        render: impl Fn(&S, Duration) -> String,
    ) -> io::Result<i32> {
        let cols = self.surface.cols();
        let live_rows = self.surface.live_rows();

        let pty = native_pty_system();
        let pair = pty
            .openpty(PtySize {
                rows: live_rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| io::Error::other(format!("openpty: {e}")))?;

        let mut cmd = CommandBuilder::new(program);
        for a in args {
            cmd.arg(a);
        }
        for (k, v) in envs {
            cmd.env(k, v);
        }
        if std::env::var_os("TERM").is_none() {
            cmd.env("TERM", "xterm-256color");
        }
        if let Ok(cwd) = std::env::current_dir() {
            cmd.cwd(cwd);
        }

        let mut child = pair
            .slave
            .spawn_command(cmd)
            .map_err(|e| io::Error::other(format!("spawn {program}: {e}")))?;
        drop(pair.slave);

        let mut reader = pair
            .master
            .try_clone_reader()
            .map_err(|e| io::Error::other(format!("pty reader: {e}")))?;
        let master = pair.master;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let reader_thread = std::thread::spawn(move || {
            let mut buf = [0u8; 8192];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tx.send(buf[..n].to_vec()).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        // Split borrows so the async loop can hold the runtime (shared) and the
        // surface/vt/carry (mutable) at once.
        let Console {
            surface, vt, rt, carry, ..
        } = self;

        let exit = rt.block_on(async {
            let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt()).ok();
            let mut sigwinch = tokio::signal::unix::signal(SignalKind::window_change()).ok();
            // The render heartbeat. `Delay` (not the default `Burst`) means that
            // if the loop is busy feeding for longer than one interval, the next
            // tick fires one full interval later rather than a catch-up burst.
            let mut redraw = tokio::time::interval(REDRAW_INTERVAL);
            redraw.set_missed_tick_behavior(MissedTickBehavior::Delay);
            redraw.tick().await; // drop the immediate first tick

            let mut interrupts: u32 = 0;
            let mut updates_open = true;

            // Lines scrolled off the top of the `avt` grid, awaiting a flush into
            // native scrollback on the next present. Accumulating them (rather
            // than inserting per chunk) is what lets a burst become one
            // clear+repaint.
            let mut pending: Vec<avt::Line> = Vec::new();

            // Initial frame. Seed the redraw state from it: `dirty` stays false
            // and `last_spin` records the drawn spinner index, so the first tick
            // only repaints once something actually moves.
            let initial = started.elapsed();
            present(surface, vt, &mut pending, &render(state, initial));
            let mut dirty = false;
            let mut last_spin = initial.as_millis() / SPINNER_STEP_MS;

            // NOTE: deliberately NOT `biased` — under an output flood `rx` is
            // ready on nearly every poll; fair polling keeps the redraw tick
            // (the sole draw site) from being starved.
            loop {
                tokio::select! {
                    _ = next_signal(&mut sigint) => {
                        interrupts += 1;
                        forward_interrupt(master.as_ref(), child.as_ref(), interrupts);
                    }

                    _ = next_signal(&mut sigwinch) => {
                        if let Some((w, _h)) = terminal_size::terminal_size() {
                            let _ = master.resize(PtySize {
                                rows: live_rows,
                                cols: w.0,
                                pixel_width: 0,
                                pixel_height: 0,
                            });
                            let mut sb: Vec<avt::Line> =
                                vt.resize(w.0 as usize, live_rows as usize).scrollback.collect();
                            pending.append(&mut sb);
                            surface.set_cols(w.0);
                        }
                        dirty = true;
                    }

                    chunk = rx.recv() => match chunk {
                        Some(bytes) => {
                            feed(vt, carry, &mut pending, &bytes, state, &mut on_line);
                            // Drain anything already queued so a burst is one model
                            // update, not one per PTY read.
                            while let Ok(more) = rx.try_recv() {
                                feed(vt, carry, &mut pending, &more, state, &mut on_line);
                            }
                            dirty = true;
                        }
                        None => {
                            // PTY EOF — fold any partial trailing bytes into the
                            // grid, present one final frame, then reap.
                            if !carry.is_empty() {
                                let tail = String::from_utf8_lossy(carry).into_owned();
                                carry.clear();
                                for line in vt.feed_str(&tail).scrollback {
                                    on_line(state, &line.text());
                                    pending.push(line);
                                }
                            }
                            present(surface, vt, &mut pending, &render(state, started.elapsed()));
                            break exit_code_from(child.wait(), interrupts > 0);
                        }
                    },

                    u = updates.recv(), if updates_open => match u {
                        Some(u) => {
                            apply(state, u);
                            dirty = true;
                        }
                        None => updates_open = false,
                    },

                    _ = redraw.tick() => {
                        let elapsed = started.elapsed();
                        let spin = elapsed.as_millis() / SPINNER_STEP_MS;
                        if dirty || spin != last_spin {
                            present(surface, vt, &mut pending, &render(state, elapsed));
                            dirty = false;
                            last_spin = spin;
                        }
                    }
                }
            }
        });

        drop(master);
        let _ = reader_thread.join();
        Ok(exit)
    }

    /// Tear down the viewport: commit the final visible child grid (trimmed of
    /// trailing blank rows) into native scrollback, then blank the viewport and
    /// park the cursor on a clean line below it.
    pub fn finish(self) -> io::Result<()> {
        let Console { surface, vt, .. } = self;
        surface.finish(&bridged(&trimmed_view(&vt)))
    }
}

/// Bridge the live `avt` grid to `ratatui` lines for the [`Surface`].
fn view_lines(vt: &Vt) -> Vec<Line<'static>> {
    vt.view().map(bridge::avt_line).collect()
}

/// Bridge owned `avt` scrollback lines to `ratatui` lines.
fn bridged(lines: &[avt::Line]) -> Vec<Line<'static>> {
    lines.iter().map(bridge::avt_line).collect()
}

/// Present one frame via the [`Surface`]: bridge the live grid and the
/// accumulated scrollback, hand them over, and drain `pending`.
fn present(surface: &mut Surface, vt: &Vt, pending: &mut Vec<avt::Line>, panel: &str) {
    let live = view_lines(vt);
    let scrollback = bridged(pending);
    pending.clear();
    surface.present(&live, &scrollback, panel);
}

/// Fold a PTY chunk into the emulator: decode the bytes, feed them to the `avt`
/// grid, hand each completed line to `on_line`, and accumulate whatever scrolled
/// off the top into `pending` for the next [`present`]. Pure model update.
fn feed<S>(
    vt: &mut Vt,
    carry: &mut Vec<u8>,
    pending: &mut Vec<avt::Line>,
    bytes: &[u8],
    state: &mut S,
    on_line: &mut impl FnMut(&mut S, &str),
) {
    let text = decode(carry, bytes);
    if text.is_empty() {
        return;
    }
    for line in vt.feed_str(&text).scrollback {
        on_line(state, &line.text());
        pending.push(line);
    }
}

/// The live grid (`Vt::view`) as owned lines, trimmed of trailing blank rows.
///
/// `avt` pads its view to the full grid height, so the visible slice almost
/// always ends in empty rows. Committing those into native scrollback would
/// leave a slab of blank lines between phases (and below the final frame), so
/// both [`Console::flush_live`] and [`Console::finish`] trim them first.
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

/// Incrementally decode a PTY byte chunk into UTF-8.
///
/// `carry` accumulates bytes that couldn't yet be decoded — a multi-byte
/// sequence cut by a read boundary. We decode the longest valid prefix, keep a
/// *trailing incomplete* sequence for next time, and substitute the replacement
/// char for a *genuinely invalid* byte so a stray byte can't wedge the stream.
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

/// Await the next delivery of an optional signal stream. When `None`
/// (registration failed) the future never resolves, so the `select!` arm never
/// fires.
async fn next_signal(sig: &mut Option<Signal>) {
    match sig {
        Some(s) => {
            s.recv().await;
        }
        None => std::future::pending().await,
    }
}

/// Forward a Ctrl-C to the child, escalating with the count of Ctrl-Cs so far.
///
/// `count == 1|2`: `SIGINT` to the child's **foreground process group** (the
/// target a terminal's line discipline signals on Ctrl-C). `count >= 3`:
/// escalate to `SIGKILL` as a backstop. The group is read from the PTY master;
/// on failure we fall back to the child's pid (a session leader's pgid == pid).
fn forward_interrupt(master: &dyn MasterPty, child: &dyn Child, count: u32) {
    let sig = if count >= 3 {
        libc::SIGKILL
    } else {
        libc::SIGINT
    };
    if let Some(pgid) = master.process_group_leader()
        && unsafe { libc::killpg(pgid, sig) } == 0
    {
        return;
    }
    if let Some(pid) = child.process_id() {
        unsafe { libc::kill(pid as libc::pid_t, sig) };
    }
}

/// Map a finished PTY child status to an exit code. Clean exits propagate their
/// code; a signal death reports `130` when we forwarded a Ctrl-C (`portable_pty`
/// keeps the signal as a name, not a number), else `1`.
fn exit_code_from(status: io::Result<portable_pty::ExitStatus>, interrupted: bool) -> i32 {
    match status {
        Ok(s) => code_for(&s, interrupted) as i32,
        Err(err) => {
            eprintln!("ztest run: error waiting on child: {err}");
            127
        }
    }
}

/// Pure numeric exit-code decision (see [`exit_code_from`]).
fn code_for(status: &portable_pty::ExitStatus, interrupted: bool) -> u8 {
    if status.signal().is_some() {
        if interrupted { 130 } else { 1 }
    } else {
        (status.exit_code() & 0xff) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_for_maps_clean_and_signal_deaths() {
        use portable_pty::ExitStatus;
        assert_eq!(code_for(&ExitStatus::with_exit_code(0), false), 0);
        assert_eq!(code_for(&ExitStatus::with_exit_code(101), false), 101);
        assert_eq!(code_for(&ExitStatus::with_exit_code(7), true), 7);
        assert_eq!(code_for(&ExitStatus::with_signal("Killed"), true), 130);
        assert_eq!(code_for(&ExitStatus::with_signal("Hangup"), false), 1);
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
}
