//! `Surface`: the reusable inline-viewport render primitive.
//!
//! The render thread (`super::render`) owns the single instance: it bridges both
//! the `avt` child grid (preflight/build phases) and the engine's formatted
//! verdict + panel lines (run phase) into this one viewport. `Surface` is
//! content-agnostic: it speaks only in already-bridged `ratatui` [`Line`]s and
//! a panel string; the render thread owns the avt-vs-string bridging.
//!
//! Two properties matter: the `scrolling-regions` feature stays off, so
//! [`Terminal::insert_before`] forwards lines into the terminal's native
//! scrollback; and each frame is wrapped in a DEC private mode 2026 synchronized
//! update (`begin_sync`/`end_sync`) so the viewport clear that `insert_before`
//! performs and the repaint that follows present atomically.

use std::io;

use ratatui::backend::{Backend as _, ClearType, CrosstermBackend};
use ratatui::layout::Position;
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Widget};
use ratatui::{Terminal, TerminalOptions, Viewport};

use crossterm::terminal::{BeginSynchronizedUpdate, EndSynchronizedUpdate};

use super::{Backend, PANEL_ROWS, bridge};

/// Restores the controlling terminal's line discipline on drop.
///
/// A TUI must not run under cooked mode: the kernel would echo the user's
/// keystrokes (most visibly the `^C` from Ctrl-C) straight onto the drawn panel,
/// corrupting it. We disable `ECHO` + `ICANON` (no echo, no line buffering) but
/// keep `ISIG`, so Ctrl-C still raises `SIGINT` (which the render thread turns
/// into cooperative cancellation) rather than arriving as a raw byte we'd have
/// to read. The original attributes are restored by [`Surface::finish`] and, as
/// a panic/`exit` backstop, by this guard's `Drop`.
struct TtyGuard {
    fd: std::os::fd::RawFd,
    original: Option<libc::termios>,
}

impl TtyGuard {
    /// Enter no-echo / no-canonical mode on stdin's tty, remembering the prior
    /// attributes. A no-op (`original: None`) if stdin isn't a tty.
    fn enter() -> TtyGuard {
        let fd = libc::STDIN_FILENO;
        let mut term: libc::termios = unsafe { std::mem::zeroed() };
        let original = if unsafe { libc::tcgetattr(fd, &mut term) } == 0 {
            let saved = term;
            term.c_lflag &= !(libc::ECHO | libc::ICANON);
            unsafe { libc::tcsetattr(fd, libc::TCSANOW, &term) };
            Some(saved)
        } else {
            None
        };
        TtyGuard { fd, original }
    }

    /// Restore the saved attributes. Idempotent: called by `finish` (so a
    /// `process::exit` that skips `Drop` still restores) and again by `Drop`.
    fn restore(&self) {
        if let Some(orig) = self.original.as_ref() {
            unsafe { libc::tcsetattr(self.fd, libc::TCSANOW, orig) };
        }
    }
}

impl Drop for TtyGuard {
    fn drop(&mut self) {
        self.restore();
    }
}

/// An inline `ratatui` viewport that is *only* the pinned panel: a fixed
/// [`PANEL_ROWS`]-tall region at the bottom, with `insert_before` forwarding
/// everything else into native scrollback above. Owns the terminal and the
/// current width; rendering is synchronous (no runtime).
pub(crate) struct Surface {
    term: Terminal<Backend>,
    cols: u16,
    tty: TtyGuard,
}

impl Surface {
    /// A panel-only surface used for the whole session (preflight build, image
    /// phases, and the test run). The inline viewport is exactly [`PANEL_ROWS`]
    /// tall and holds nothing but the panel; every subprocess and reporter line
    /// scrolls above it into native scrollback via [`Surface::insert_scrollback`].
    ///
    /// The cursor is left wherever the shell put it. `ratatui` anchors the
    /// inline viewport to the cursor row at creation and reserves the panel's
    /// rows with `append_lines` — but because the panel is small and always
    /// painted in full on the first frame, no blank band appears: on a fresh
    /// screen it opens right under the prompt and crawls to the bottom as
    /// scrollback accumulates; on a full screen `ratatui` scrolls a few *real*
    /// prior lines up (as any status widget must) and the panel paints over the
    /// reserved rows immediately. No cursor-parking, so no reserved gap.
    pub fn bottom_panel() -> io::Result<Surface> {
        let cols = terminal_size::terminal_size()
            .map(|(w, _)| w.0)
            .unwrap_or(80);
        Surface::build(cols, PANEL_ROWS)
    }

    fn build(cols: u16, viewport_rows: u16) -> io::Result<Surface> {
        let term = Terminal::with_options(
            CrosstermBackend::new(std::io::stdout()),
            TerminalOptions {
                viewport: Viewport::Inline(viewport_rows),
            },
        )?;
        Ok(Surface {
            term,
            cols,
            tty: TtyGuard::enter(),
        })
    }

    pub fn cols(&self) -> u16 {
        self.cols
    }

    /// Re-query and record the terminal width (after SIGWINCH).
    pub fn set_cols(&mut self, cols: u16) {
        self.cols = cols;
    }

    /// Present one frame atomically: flush `scrollback` above the viewport, then
    /// repaint the `panel`, all inside one synchronized update.
    pub fn present(&mut self, scrollback: &[Line<'static>], panel: &str) {
        self.begin_sync();
        if !scrollback.is_empty() {
            let _ = self.insert_scrollback(scrollback);
        }
        let _ = self.draw_frame(panel);
        self.end_sync();
    }

    /// Forward already-bridged lines into native scrollback via
    /// [`Terminal::insert_before`], chunked so a huge burst can't allocate one
    /// giant buffer.
    pub fn insert_scrollback(&mut self, lines: &[Line<'static>]) -> io::Result<()> {
        const CHUNK: usize = 256;
        for batch in lines.chunks(CHUNK) {
            let n = batch.len() as u16;
            self.term.insert_before(n, |buf| {
                Paragraph::new(batch.to_vec()).render(buf.area, buf);
            })?;
        }
        Ok(())
    }

    /// Convert an ANSI string (the engine reporter's scroll-lines) into ratatui
    /// `Line`s via the same `avt` bridge the panel uses: one `Line` per text
    /// line, colours preserved. `s.lines()` keeps interior blank lines and drops
    /// the trailing newline's empty, so the run-phase verdict lines bridge
    /// faithfully into native scrollback.
    pub fn scrollback_from_ansi(&self, s: &str) -> Vec<Line<'static>> {
        s.lines()
            .map(|line| {
                bridge::text_from_ansi(line, self.cols as usize, 1)
                    .lines
                    .into_iter()
                    .next()
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Tear down: restore the terminal's line discipline, commit the final live
    /// region into native scrollback so it stays on screen as ordinary output,
    /// then blank the viewport and park the cursor on a clean line below it.
    ///
    /// Takes `&mut self` (not `self`) so the render thread's hard-exit backstop
    /// can restore the terminal in place before `process::exit` (which would
    /// otherwise skip `Drop`). Restoring the tty mode here, rather than relying
    /// only on the guard's `Drop`, is what makes that path safe.
    pub fn finish(&mut self, final_live: &[Line<'static>]) -> io::Result<()> {
        self.tty.restore();
        self.insert_scrollback(final_live)?;

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

    /// Render one frame: the `panel` filling the whole (panel-only) viewport,
    /// top-aligned. The panel is a fixed [`PANEL_ROWS`] lines, so it fills the
    /// region exactly; a shorter panel (the cancel overlay) leaves the trailing
    /// rows blank. The trailing newline is trimmed so a Vt sized to the line
    /// count wouldn't scroll the top rule off.
    fn draw_frame(&mut self, panel: &str) -> io::Result<()> {
        let panel = panel.trim_end_matches('\n');
        self.term.draw(|f| {
            let area = f.area();
            let text = bridge::text_from_ansi(panel, area.width as usize, area.height as usize);
            f.render_widget(Paragraph::new(text), area);
        })?;
        Ok(())
    }

    /// Open a synchronized-update frame (DEC private mode 2026): the terminal
    /// buffers writes and presents them atomically at [`Surface::end_sync`],
    /// so the `insert_before` viewport clear never flashes blank before the
    /// repaint. Terminals without 2026 ignore it.
    fn begin_sync(&mut self) {
        let _ = crossterm::queue!(self.term.backend_mut(), BeginSynchronizedUpdate);
    }

    /// Close the synchronized-update frame and flush unconditionally, so 2026
    /// mode is always left even if the enclosed draw was a no-op.
    fn end_sync(&mut self) {
        let _ = crossterm::queue!(self.term.backend_mut(), EndSynchronizedUpdate);
        let _ = io::Write::flush(self.term.backend_mut());
    }
}
