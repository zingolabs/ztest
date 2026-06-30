//! `Surface` — the reusable inline-viewport render primitive.
//!
//! Both console drivers share this: the PTY driver (`super::pty`) emulates a
//! single child through `avt` and bridges its grid here; the engine
//! (`crate::engine`) has no child grid and instead scrolls formatted verdict
//! lines through here with a pinned status panel. `Surface` is therefore
//! content-agnostic — it speaks only in already-bridged `ratatui` [`Line`]s and
//! a panel string; the *drivers* own the avt-vs-string bridging.
//!
//! It keeps the load-bearing properties: the `scrolling-regions` feature stays
//! OFF, so [`Terminal::insert_before`] forwards lines into the terminal's
//! **native scrollback**; and each frame is wrapped in a DEC private mode 2026
//! synchronized update (`begin_sync`/`end_sync`) so the viewport clear that
//! `insert_before` performs and the repaint that follows present atomically.

use std::io;

use ratatui::backend::{Backend as _, ClearType, CrosstermBackend};
use ratatui::layout::{Position, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Widget};
use ratatui::{Terminal, TerminalOptions, Viewport};

use crossterm::terminal::{BeginSynchronizedUpdate, EndSynchronizedUpdate};

use super::{Backend, PANEL_ROWS, bridge};

/// At least this many rows for the live (emulated child) region, so a short
/// terminal still shows a usable slice of a PTY child's output.
const MIN_LIVE_ROWS: u16 = 4;

/// An inline `ratatui` viewport: a live region on top, a pinned panel beneath,
/// and `insert_before` into native scrollback above. Owns the terminal and the
/// current width/live-height; rendering is synchronous (no runtime).
pub(crate) struct Surface {
    term: Terminal<Backend>,
    cols: u16,
    live_rows: u16,
}

impl Surface {
    /// A full-height surface with a live region — the PTY driver renders a
    /// child's emulated grid into it.
    pub fn with_live_region() -> io::Result<Surface> {
        let (cols, rows) = terminal_size::terminal_size()
            .map(|(w, h)| (w.0, h.0))
            .unwrap_or((80, 40));
        let live_rows = rows.saturating_sub(PANEL_ROWS).max(MIN_LIVE_ROWS);
        Surface::build(cols, live_rows + PANEL_ROWS, live_rows)
    }

    fn build(cols: u16, viewport_rows: u16, live_rows: u16) -> io::Result<Surface> {
        let term = Terminal::with_options(
            CrosstermBackend::new(std::io::stdout()),
            TerminalOptions {
                viewport: Viewport::Inline(viewport_rows),
            },
        )?;
        Ok(Surface {
            term,
            cols,
            live_rows,
        })
    }

    pub fn cols(&self) -> u16 {
        self.cols
    }

    pub fn live_rows(&self) -> u16 {
        self.live_rows
    }

    /// Re-query and record the terminal width (after SIGWINCH).
    pub fn set_cols(&mut self, cols: u16) {
        self.cols = cols;
    }

    /// Present one frame atomically: flush `scrollback` above the viewport, then
    /// repaint the `live` region + `panel`, all inside one synchronized update.
    pub fn present(&mut self, live: &[Line<'static>], scrollback: &[Line<'static>], panel: &str) {
        self.begin_sync();
        if !scrollback.is_empty() {
            let _ = self.insert_scrollback(scrollback);
        }
        let _ = self.draw_frame(live, panel);
        self.end_sync();
    }

    /// Repaint the live region + panel with nothing new for scrollback (an
    /// initial frame or a between-phase refresh).
    pub fn paint_panel(&mut self, live: &[Line<'static>], panel: &str) -> io::Result<()> {
        self.draw_frame(live, panel)
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
    /// `Line`s via the same `avt` bridge the panel uses — one `Line` per text
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

    /// Hand off from the full-height preflight viewport to a compact **run
    /// viewport** for the engine: commit `final_live` into native scrollback,
    /// tear down the tall viewport, and return a fresh `Surface` whose viewport
    /// is `live_rows` (the live running region) plus [`PANEL_ROWS`] (the pinned
    /// QoS panel), bottom-anchored on the terminal.
    ///
    /// `live_rows` is small and bounded (the running-tests region), unlike the
    /// preflight viewport's full-screen live grid — so the engine's verdict
    /// lines land just above the running region and panel, with at most a few
    /// blank rows when fewer tests are in flight than the region holds.
    pub fn into_run_surface(mut self, final_live: &[Line<'static>], live_rows: u16) -> io::Result<Surface> {
        // 1. Commit the preflight frame into history (above the viewport).
        self.insert_scrollback(final_live)?;

        // 2. Blank the full-height viewport region so no stale preflight frame
        //    lingers, and leave the cursor at its origin — i.e. directly below
        //    the just-committed content. The new short viewport is built *here*,
        //    NOT bottom-anchored: a bottom-anchored viewport on an otherwise
        //    blank screen makes every later `insert_before` scroll that blank
        //    into scrollback (a screenful of dead space between the preflight and
        //    engine output). Built at the origin, `insert_before` instead pushes
        //    the panel *down* toward the bottom as verdict lines accumulate — no
        //    blank ever enters scrollback (ratatui-core `insert_before` docs).
        let mut origin = Position::new(0, 0);
        self.term.draw(|f| {
            let a = f.area();
            origin = Position::new(a.x, a.y);
        })?;
        {
            let backend = self.term.backend_mut();
            backend.set_cursor_position(origin)?;
            backend.clear_region(ClearType::AfterCursor)?;
            backend.flush()?;
        }

        let cols = self.cols;
        // 3. Drop the tall viewport (releases its inline reservation) and build
        //    the compact run viewport (live region + panel) at the (origin)
        //    cursor.
        drop(self.term);
        Surface::build(cols, live_rows + PANEL_ROWS, live_rows)
    }

    /// Tear down: commit the final live region into native scrollback so it
    /// stays on screen as ordinary output, then blank the viewport and park the
    /// cursor on a clean line below it.
    pub fn finish(mut self, final_live: &[Line<'static>]) -> io::Result<()> {
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

    /// Render one frame: the `live` lines in the live region, the `panel` pinned
    /// beneath. The panel region is sized to the panel's actual line count
    /// (trailing newline trimmed) and bottom-anchored; remaining rows go to the
    /// live region (see the history in the panel/console docs — a trailing
    /// newline fed to a Vt sized to its line count would scroll the top rule
    /// off, and a short panel would otherwise leave a blank filler row).
    fn draw_frame(&mut self, live: &[Line<'static>], panel: &str) -> io::Result<()> {
        let panel = panel.trim_end_matches('\n');
        self.term.draw(|f| {
            let area = f.area();
            let panel_h = (panel.lines().count() as u16).min(area.height);
            let live_h = area.height - panel_h;
            let live_rect = Rect::new(area.x, area.y, area.width, live_h);
            let panel_area = Rect::new(area.x, area.y + live_h, area.width, panel_h);

            f.render_widget(Paragraph::new(live.to_vec()), live_rect);

            let text = bridge::text_from_ansi(panel, area.width as usize, panel_h as usize);
            f.render_widget(Paragraph::new(text), panel_area);
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

    /// Close the synchronized-update frame and flush — unconditionally, so 2026
    /// mode is always left even if the enclosed draw was a no-op.
    fn end_sync(&mut self) {
        let _ = crossterm::queue!(self.term.backend_mut(), EndSynchronizedUpdate);
        let _ = io::Write::flush(self.term.backend_mut());
    }
}
