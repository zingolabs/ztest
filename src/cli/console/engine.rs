//! The unified bottom console — every `ztest run` subprocess emulated under a
//! PTY through `avt`, rendered into a `ratatui` inline viewport.
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
//!         │                ──▶ Terminal::insert_before ──▶ NATIVE scrollback
//!         ▼
//!   Vt::view()  (the live visible grid) ──▶ live region of the inline viewport
//!                                            status panel ──▶ bottom of viewport
//! ```
//!
//! Running the child under a **PTY** (rather than a pipe) is what keeps its
//! native output intact: cargo and docker only emit colour and their in-place
//! progress bars when they detect a TTY. `avt` interprets those cursor-addressed
//! updates into a grid we render ourselves, and forwards completed lines into
//! the terminal's real scrollback. One [`Console`] is created per session and
//! reused for every phase via [`Console::run_child`]; only the panel renderer
//! and concurrent state differ.
//!
//! ## What writes to the real terminal
//!
//! Exactly one thing: the `ratatui` [`Terminal`]. The children's bytes never
//! reach the real stdout — they're consumed by `avt` — so there's no two-writer
//! race.
//!
//! ## Why no raw mode
//!
//! The parent reads no keystrokes, and cooked mode keeps the kernel delivering
//! `SIGINT` to us on Ctrl-C, which we forward to the child's process group (see
//! [`forward_interrupt`]). An inline viewport needs only cursor positioning,
//! which works in cooked mode.
//!
//! ## Signals
//!
//! The child is a session leader on its own controlling terminal
//! ([`portable_pty`] does `setsid` + `TIOCSCTTY`), so the real terminal's
//! signals don't reach it. We bridge two: **SIGWINCH** (re-query width, push to
//! the PTY + `avt` so the child reflows) and **SIGINT** (forward to the child's
//! process group; the third Ctrl-C escalates to `SIGKILL`).

use std::io::{self, Read};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use crossterm::terminal::{BeginSynchronizedUpdate, EndSynchronizedUpdate};
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
// `Backend` (the trait) is imported anonymously so its methods are callable on
// `backend_mut()` without clashing with the `Backend` type alias below.
use ratatui::backend::{Backend as _, ClearType, CrosstermBackend};
use ratatui::layout::{Position, Rect};
use ratatui::widgets::{Paragraph, Widget};
use ratatui::{Terminal, TerminalOptions, Viewport};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::signal::unix::{Signal, SignalKind};
use tokio::time::MissedTickBehavior;

use avt::Vt;

use super::{Backend, PANEL_ROWS, bridge};
use crate::preflight::{RunProgress, Theme, render_live_panel};
use crate::qos::Resources;
use crate::qos::live::{self, LiveSnapshot};
use crate::qos::schedule::QosPlan;

/// At least this many rows for the live (emulated child) region, so a short
/// terminal still shows a usable slice of the child's output.
const MIN_LIVE_ROWS: u16 = 4;

/// Target frame interval for the live console (~30 fps). The child's bytes are
/// folded into the `avt` grid as they arrive, but the terminal is repainted at
/// most once per interval — so a flood of output (nextest's progress churn)
/// drives at most ~30 clear+repaint cycles a second instead of one per PTY
/// read. This is the real, terminal-independent fix for the flicker: bounding
/// the *amount* of clear+repaint work. 30 fps reads as smooth on modern
/// terminals and stays gentle on older ones; when idle the loop repaints far
/// less (only when the spinner advances). Synchronized output ([`begin_sync`])
/// then makes each of these bounded frames tear-free where 2026 is supported.
const REDRAW_INTERVAL: Duration = Duration::from_millis(33);

/// Milliseconds per spinner frame (matches [`crate::preflight`]'s
/// `spinner_glyph`). The redraw loop forces a repaint when the derived frame
/// index changes, so the "is the cluster still alive?" heartbeat keeps
/// animating even when the child has produced no new output.
const SPINNER_STEP_MS: u128 = 100;

/// The session-long bottom console: a `ratatui` inline viewport (live region +
/// status panel), an `avt` virtual terminal that every child is emulated
/// through, and a reused current-thread runtime for the per-child event loop.
pub struct Console {
    term: Terminal<Backend>,
    vt: Vt,
    rt: tokio::runtime::Runtime,
    cols: u16,
    live_rows: u16,
    /// Buffers UTF-8 bytes split across PTY read boundaries (see [`decode`]).
    carry: Vec<u8>,
}

impl Console {
    /// Open the console: size the live region and `avt` grid to the terminal,
    /// reserve the inline viewport at the bottom, and build the runtime.
    pub fn new() -> io::Result<Console> {
        let (cols, rows) = terminal_size::terminal_size()
            .map(|(w, h)| (w.0, h.0))
            .unwrap_or((80, 40));
        let live_rows = rows.saturating_sub(PANEL_ROWS).max(MIN_LIVE_ROWS);
        let viewport_rows = live_rows + PANEL_ROWS;

        let term = Terminal::with_options(
            CrosstermBackend::new(std::io::stdout()),
            TerminalOptions {
                viewport: Viewport::Inline(viewport_rows),
            },
        )?;
        // `scrollback_limit(0)` makes avt yield every line the moment it scrolls
        // past the visible grid (retaining none itself) — our feed into native
        // scrollback.
        let vt = Vt::builder()
            .size(cols as usize, live_rows as usize)
            .scrollback_limit(0)
            .build();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        Ok(Console {
            term,
            vt,
            rt,
            cols,
            live_rows,
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
        draw_frame(&mut self.term, &self.vt, panel)
    }

    /// Relay one (possibly ANSI-coloured) line directly into native scrollback
    /// above the viewport, without an emulated child. Used by the brief image
    /// phase, whose `docker`/`kind` output is captured as plain lines. The live
    /// region is left as-is (blank after [`flush_live`]).
    pub fn log_line(&mut self, ansi: &str) -> io::Result<()> {
        let line = ansi.trim_end_matches(['\r', '\n']);
        let cols = self.cols as usize;
        self.term.insert_before(1, |buf| {
            let text = bridge::text_from_ansi(line, cols, 1);
            Paragraph::new(text).render(buf.area, buf);
        })
    }

    /// Commit the current live grid (trimmed of trailing blank rows) into native
    /// scrollback and reset the emulator to a blank grid, so the next phase
    /// starts on a clean live region with nothing lost. Callers should
    /// [`paint_panel`](Self::paint_panel) afterward to repaint the now-blank
    /// live region.
    pub fn flush_live(&mut self) -> io::Result<()> {
        insert_scrollback(&mut self.term, &trimmed_view(&self.vt))?;
        self.vt = Vt::builder()
            .size(self.cols as usize, self.live_rows as usize)
            .scrollback_limit(0)
            .build();
        Ok(())
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
        let pty = native_pty_system();
        let pair = pty
            .openpty(PtySize {
                rows: self.live_rows,
                cols: self.cols,
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
        // terminal/vt/carry (mutable) at once. `cols` is intentionally not bound:
        // the live width only changes via SIGWINCH, which re-queries it fresh.
        let Console {
            term,
            vt,
            rt,
            live_rows,
            carry,
            ..
        } = self;
        let live_rows = *live_rows;

        let exit = rt.block_on(async {
            let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt()).ok();
            let mut sigwinch = tokio::signal::unix::signal(SignalKind::window_change()).ok();
            // The render heartbeat. `Delay` (not the default `Burst`) means that
            // if the loop is busy feeding for longer than one interval, the next
            // tick fires one full interval later rather than a catch-up burst of
            // immediate ticks — so the cadence stays an upper bound on the redraw
            // rate even after a stall.
            let mut redraw = tokio::time::interval(REDRAW_INTERVAL);
            redraw.set_missed_tick_behavior(MissedTickBehavior::Delay);
            redraw.tick().await; // drop the immediate first tick

            let mut interrupts: u32 = 0;
            let mut updates_open = true;

            // Lines scrolled off the top of the `avt` grid, awaiting a flush into
            // native scrollback on the next present. Accumulating them here (rather
            // than calling `insert_before` per chunk) is what lets a burst become a
            // single clear+repaint.
            let mut pending: Vec<avt::Line> = Vec::new();

            // Initial frame, so the panel appears immediately rather than after the
            // first tick. Seed the redraw state from this frame: `dirty` stays false
            // (nothing has changed since we just drew) and `last_spin` records the
            // spinner index drawn, so the first tick only repaints once something
            // actually moves.
            let initial = started.elapsed();
            present(term, vt, &mut pending, &render(state, initial));
            // `true` when the model (grid, panel state, pending scrollback) changed
            // since the last present, so the next tick repaints.
            let mut dirty = false;
            // Spinner frame index at the last present; a change forces a repaint so
            // the heartbeat animates even with no child output.
            let mut last_spin = initial.as_millis() / SPINNER_STEP_MS;

            // NOTE: this `select!` is deliberately NOT `biased`. Under a flood of
            // child output `rx` is ready on nearly every poll; with biased priority
            // the lower `redraw` arm could be starved and the display would freeze
            // until the output paused. Fair (random) polling guarantees the tick
            // gets its turn — and since draws happen ONLY on the tick, that turn is
            // what bounds the repaint rate. The other arms just update the model.
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
                        }
                        dirty = true;
                    }

                    chunk = rx.recv() => match chunk {
                        Some(bytes) => {
                            feed(vt, carry, &mut pending, &bytes, state, &mut on_line);
                            // Drain anything already queued so a burst is one model
                            // update, not one per PTY read (fewer loop wakeups; the
                            // tick still owns the actual repaint).
                            while let Ok(more) = rx.try_recv() {
                                feed(vt, carry, &mut pending, &more, state, &mut on_line);
                            }
                            dirty = true;
                        }
                        None => {
                            // PTY EOF — fold any partial trailing bytes into the grid,
                            // present one final frame (the burst since the last tick may
                            // not have been drawn yet), then reap.
                            if !carry.is_empty() {
                                let tail = String::from_utf8_lossy(carry).into_owned();
                                carry.clear();
                                for line in vt.feed_str(&tail).scrollback {
                                    on_line(state, &line.text());
                                    pending.push(line);
                                }
                            }
                            present(term, vt, &mut pending, &render(state, started.elapsed()));
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
                            present(term, vt, &mut pending, &render(state, elapsed));
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

    /// Run the `cargo nextest run` phase: emulate it under the PTY with the live
    /// QoS panel pinned beneath, a 1s cluster-reservation poll feeding the
    /// panel, and per-test verdict tallying. Returns the process exit code.
    pub fn run_tests(
        &mut self,
        effective_args: Vec<String>,
        envs: Vec<(&'static str, String)>,
        plan: &QosPlan,
        free: &Resources,
        total_tests: u32,
        theme: &Theme,
    ) -> ExitCode {
        struct RunState {
            progress: RunProgress,
            snapshot: LiveSnapshot,
        }
        let mut state = RunState {
            progress: RunProgress {
                total: total_tests,
                ..RunProgress::default()
            },
            snapshot: LiveSnapshot::default(),
        };

        // 1s reservation poll → panel snapshots, as a background task on the
        // console's runtime. It owns the sender, so the updates channel stays
        // open for the whole run; we abort it once nextest exits.
        let (snap_tx, mut snap_rx) = tokio::sync::mpsc::unbounded_channel::<LiveSnapshot>();
        let poll = self.rt.spawn(async move {
            let Ok(client) = crate::cluster::client().await else {
                return;
            };
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                if let Some(snap) = poll_reservations(&client).await
                    && snap_tx.send(snap).is_err()
                {
                    break;
                }
            }
        });

        let mut args = vec!["nextest".to_string(), "run".to_string()];
        args.extend(effective_args);

        let started = Instant::now();
        let code = self.run_child(
            "cargo",
            &args,
            &envs,
            started,
            &mut state,
            &mut snap_rx,
            |s, snap| s.snapshot = snap,
            |s, line| tally(&mut s.progress, line),
            |s, elapsed| {
                let mut p = s.progress.clone();
                p.elapsed = elapsed;
                render_live_panel(&s.snapshot, plan, free, &p, theme)
            },
        );
        poll.abort();

        match code {
            Ok(c) => ExitCode::from(c.clamp(0, 255) as u8),
            Err(e) => {
                eprintln!("ztest run: {e}");
                ExitCode::from(127)
            }
        }
    }

    /// Tear down the viewport: commit the final visible child grid (trimmed of
    /// trailing blank rows) into native scrollback so the run's tail stays on
    /// screen as ordinary output, then blank the viewport region and park the
    /// cursor on a clean line below it.
    pub fn finish(mut self) -> io::Result<()> {
        insert_scrollback(&mut self.term, &trimmed_view(&self.vt))?;

        let mut origin = Position::new(0, 0);
        self.term.draw(|f| {
            let a = f.area();
            origin = Position::new(a.x, a.y);
        })?;
        let backend = self.term.backend_mut();
        backend.set_cursor_position(origin)?;
        backend.clear_region(ClearType::AfterCursor)?;
        backend.show_cursor()?;
        backend.flush()?;
        Ok(())
    }
}

/// Fold a PTY chunk into the emulator: decode the bytes, feed them to the `avt`
/// grid, hand each completed line to `on_line`, and accumulate whatever scrolled
/// off the top into `pending` for the next [`present`]. Pure model update — it
/// performs no terminal I/O, so it can run many times between repaints.
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

/// Present one frame to the real terminal, atomically: flush accumulated
/// scrollback above the viewport, then repaint the live grid + panel, wrapped in
/// a synchronized-update frame ([`begin_sync`]) so the viewport clear that
/// `insert_before` performs and the repaint that follows never show as a blank
/// flash. `pending` is drained on success.
fn present(term: &mut Terminal<Backend>, vt: &Vt, pending: &mut Vec<avt::Line>, panel: &str) {
    begin_sync(term);
    if !pending.is_empty() {
        let _ = insert_scrollback(term, pending);
        pending.clear();
    }
    let _ = draw_frame(term, vt, panel);
    end_sync(term);
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

/// Forward emulator scrollback lines into the terminal's native scrollback via
/// [`Terminal::insert_before`], in chunks so an enormous burst can't allocate a
/// single giant buffer.
fn insert_scrollback(term: &mut Terminal<Backend>, lines: &[avt::Line]) -> io::Result<()> {
    const CHUNK: usize = 256;
    for batch in lines.chunks(CHUNK) {
        let n = batch.len() as u16;
        term.insert_before(n, |buf| {
            let rendered: Vec<_> = batch.iter().map(bridge::avt_line).collect();
            Paragraph::new(rendered).render(buf.area, buf);
        })?;
    }
    Ok(())
}

/// Open a synchronized-update frame (DEC private mode 2026): the terminal
/// buffers every following write and repaints them in a single atomic present
/// at [`end_sync`]. This is what stops the viewport clear that `insert_before`
/// performs (it blanks the region and resets the diff buffer, forcing a full
/// repaint on the next `draw`) from flashing blank before the repaint lands —
/// the dominant source of the run panel's flicker. Terminals that don't
/// implement 2026 ignore the escape, so this is purely additive.
fn begin_sync(term: &mut Terminal<Backend>) {
    let _ = crossterm::queue!(term.backend_mut(), BeginSynchronizedUpdate);
}

/// Close the frame opened by [`begin_sync`] and flush, so the terminal presents
/// the buffered frame. The flush is unconditional (even if the enclosed draw
/// was a no-op) so 2026 mode is always left — a missed `EndSynchronizedUpdate`
/// would freeze the display.
fn end_sync(term: &mut Terminal<Backend>) {
    let _ = crossterm::queue!(term.backend_mut(), EndSynchronizedUpdate);
    let _ = std::io::Write::flush(term.backend_mut());
}

/// Render one frame: the emulated child grid in the live region, the status
/// panel pinned beneath it.
///
/// The panel region is sized to the panel's *actual* line count (the trailing
/// `writeln!` newline trimmed first) rather than a fixed [`PANEL_ROWS`], and
/// bottom-anchored with the remaining rows handed to the live region. Two
/// reasons: (1) feeding a trailing-newline string to a `Vt` sized to exactly
/// its line count overflows by one row and scrolls the top rule off, leaving a
/// stray blank at the bottom — trimming avoids that; (2) a panel shorter than
/// the reservation (the preflight panel is one line shorter than the run panel)
/// then leaves no blank filler row.
fn draw_frame(term: &mut Terminal<Backend>, vt: &Vt, panel: &str) -> io::Result<()> {
    let panel = panel.trim_end_matches('\n');
    term.draw(|f| {
        let area = f.area();
        let panel_lines = panel.lines().count() as u16;
        let panel_h = panel_lines.min(area.height);
        let live_h = area.height - panel_h;
        let live = Rect::new(area.x, area.y, area.width, live_h);
        let panel_area = Rect::new(area.x, area.y + live_h, area.width, panel_h);

        let live_lines: Vec<_> = vt.view().map(bridge::avt_line).collect();
        f.render_widget(Paragraph::new(live_lines), live);

        let text = bridge::text_from_ansi(panel, area.width as usize, panel_h as usize);
        f.render_widget(Paragraph::new(text), panel_area);
    })?;
    Ok(())
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

/// Poll the cluster's reservation Leases into a [`LiveSnapshot`]. `None` on any
/// list error — the caller keeps the previous snapshot.
async fn poll_reservations(client: &kube::Client) -> Option<LiveSnapshot> {
    use crate::qos::kube_store::KubeStore;
    use crate::qos::store::{Kind, LabelSelector, ObjectStore};
    use crate::qos::{LABEL_ROLE, ROLE_RESERVATION};

    let store = KubeStore::with_default_namespace(client.clone());
    let objs = store
        .list(
            Kind::Reservation,
            &LabelSelector::eq(LABEL_ROLE, ROLE_RESERVATION),
            None,
        )
        .await
        .ok()?;
    Some(live::summarize(&objs, crate::qos::now_secs(), crate::qos::GRACE))
}

/// Tally a relayed nextest output line into the run progress counters.
///
/// Best-effort: strips ANSI, takes the leading status token. nextest prints a
/// terminal `PASS`/`FAIL`/`TIMEOUT` token per test; non-terminal markers
/// (`SLOW`, `START`, `TRY n …`, `LEAK`) are ignored so retries don't double
/// count.
fn tally(progress: &mut RunProgress, line: &str) {
    let clean = strip_ansi(line);
    match clean.split_whitespace().next().unwrap_or("") {
        "PASS" => progress.passed += 1,
        "FAIL" | "TIMEOUT" => progress.failed += 1,
        _ => {}
    }
}

/// Remove ANSI escape sequences so the leading status token can be matched.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'[') {
            chars.next();
            while let Some(&n) = chars.peek() {
                chars.next();
                if ('@'..='~').contains(&n) {
                    break;
                }
            }
        } else {
            chars.next();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_ansi_removes_color_codes() {
        assert_eq!(strip_ansi("\x1b[32mPASS\x1b[0m test::foo"), "PASS test::foo");
        assert_eq!(strip_ansi("plain"), "plain");
    }

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
    fn tally_counts_terminal_verdicts_only() {
        let mut p = RunProgress::default();
        tally(&mut p, "        \x1b[32m    PASS\x1b[0m [   0.1s] crate test::a");
        tally(&mut p, "        \x1b[31m    FAIL\x1b[0m [   0.2s] crate test::b");
        tally(&mut p, "        TIMEOUT [  60.0s] crate test::c");
        tally(&mut p, "        SLOW [  10.0s] crate test::d");
        tally(&mut p, "   TRY 1 FAIL [  0.0s]  crate test::f");
        assert_eq!(p.passed, 1, "passed");
        assert_eq!(p.failed, 2, "failed (FAIL + TIMEOUT, not TRY/SLOW)");
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
