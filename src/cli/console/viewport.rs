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
use ratatui::layout::{Position, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Widget};
use ratatui::{Terminal, TerminalOptions, Viewport};

use crossterm::terminal::{BeginSynchronizedUpdate, EndSynchronizedUpdate};

use super::{Backend, LIVE_ROWS, PANEL_ROWS, bridge};

/// Minimum terminal width for the two-column panel. Below this the right
/// (transfer) column is dropped and the left column spans the full width — a
/// narrow terminal can't fit both without mangling either.
const TWO_COL_MIN: u16 = 90;

/// The left column's target width. Its content is five fixed status lines whose
/// widest (the capacity gauge) needs ~56 cols; 80 leaves headroom for a long
/// kube-context. Held constant so every extra terminal column flows to the
/// right (transfer) column, which is the one that wants the room — a full
/// `⇡⠹ label · bar % · done / total` line runs ~60 cols.
const LEFT_COL_TARGET: u16 = 80;
/// Floor for the right column when the terminal is too narrow to grant the left
/// its target: the left yields down to this so the transfer column stays legible.
const RIGHT_COL_MIN: u16 = 30;

/// Emulator width used to bridge each panel column: wide enough that no real
/// panel line wraps, so each logical line stays one grid row. `ratatui` clips the
/// resulting lines to the actual (narrower) column width when it renders them.
const NOWRAP_COLS: usize = 512;

/// Paint the two panel columns into the frame's (viewport) area. Extracted from
/// [`Surface::draw_frame`] so it can be exercised against a `TestBackend`.
///
/// At or above [`TWO_COL_MIN`] the area is split into a left region pinned to
/// [`LEFT_COL_TARGET`] (yielding toward [`RIGHT_COL_MIN`] on a tight terminal)
/// with every remaining column given to the right region; below it, a single
/// full-width left column. Each block
/// is bridged through a *wide* emulator ([`NOWRAP_COLS`]) so a line longer than
/// its column is never wrapped (which would consume rows and break the
/// fixed-height grid); `ratatui`'s `Paragraph` then clips it to the column,
/// keeping the labelled left side and dropping trailing detail. Trailing newlines
/// are trimmed so a Vt sized to the line count wouldn't scroll the top row off.
fn paint_panel(f: &mut ratatui::Frame, area: Rect, left: &str, right: &str) {
    let left = left.trim_end_matches('\n');
    let right = right.trim_end_matches('\n');
    let rows = |s: &str, h: u16| bridge::text_from_ansi(s, NOWRAP_COLS, h as usize);
    if area.width < TWO_COL_MIN {
        f.render_widget(Paragraph::new(rows(left, area.height)), area);
        return;
    }
    let (left_w, right_w) = two_col_split(area.width);
    let left_area = Rect::new(area.x, area.y, left_w, area.height);
    let right_area = Rect::new(area.x + left_w, area.y, right_w, area.height);
    f.render_widget(Paragraph::new(rows(left, area.height)), left_area);
    if !right.is_empty() {
        f.render_widget(Paragraph::new(rows(right, area.height)), right_area);
    }
}

/// Split a two-column width into `(left, right)`: the left is pinned to
/// [`LEFT_COL_TARGET`] but yields toward [`RIGHT_COL_MIN`] on a tight terminal;
/// the right takes everything left over (uncapped, so it grows with the screen).
/// Only called for `width >= TWO_COL_MIN`, so the subtraction can't underflow.
fn two_col_split(width: u16) -> (u16, u16) {
    let left = LEFT_COL_TARGET.min(width - RIGHT_COL_MIN);
    (left, width - left)
}

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

/// An inline `ratatui` viewport holding a live region atop the pinned panel: a
/// fixed [`LIVE_ROWS`]-tall live region (the child's current output / the run's
/// running-tests block) above a [`PANEL_ROWS`]-tall two-column status panel, with
/// `insert_before` forwarding completed lines into native scrollback above. Owns
/// the terminal and the current width; rendering is synchronous (no runtime).
pub(crate) struct Surface {
    term: Terminal<Backend>,
    cols: u16,
    /// The live-region rows this surface actually reserved (`viewport height −
    /// PANEL_ROWS`). The single derived value the render thread sizes the `avt`
    /// grid / child PTY / engine running-block from, so none of them restate
    /// [`LIVE_ROWS`].
    live_rows: u16,
    tty: TtyGuard,
}

impl Surface {
    /// The session surface (preflight build, image phases, and the test run). The
    /// inline viewport is [`LIVE_ROWS`] live rows + [`PANEL_ROWS`] panel rows;
    /// completed subprocess/reporter lines scroll above it into native scrollback
    /// via [`Surface::insert_scrollback`].
    ///
    /// The cursor is left wherever the shell put it. `ratatui` anchors the inline
    /// viewport to the cursor row at creation and reserves its rows with
    /// `append_lines`. No cursor-parking. The live region sits blank until the
    /// child produces output (the accepted startup-gap tradeoff of showing live
    /// build/run activity), which is why [`LIVE_ROWS`] is kept modest.
    pub fn bottom_panel() -> io::Result<Surface> {
        let cols = terminal_size::terminal_size()
            .map(|(w, _)| w.0)
            .unwrap_or(80);
        Surface::build(cols, LIVE_ROWS + PANEL_ROWS)
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
            // Derive the live-region height from the rows we actually reserved,
            // so the grid/PTY/engine follow the viewport rather than restating
            // LIVE_ROWS. Saturating so a (nonsensical) sub-panel viewport is 0.
            live_rows: viewport_rows.saturating_sub(PANEL_ROWS),
            tty: TtyGuard::enter(),
        })
    }

    pub fn cols(&self) -> u16 {
        self.cols
    }

    /// The live-region height this surface reserved (`viewport height −
    /// PANEL_ROWS`): the rows the render thread sizes the `avt` grid and child PTY
    /// to, and that the engine fills with its running-tests block.
    pub fn live_rows(&self) -> u16 {
        self.live_rows
    }

    /// Re-query and record the terminal width (after SIGWINCH).
    pub fn set_cols(&mut self, cols: u16) {
        self.cols = cols;
    }

    /// Present one frame atomically: flush `scrollback` above the viewport, then
    /// repaint the live region plus the two panel columns, all inside one
    /// synchronized update.
    pub fn present(
        &mut self,
        scrollback: &[Line<'static>],
        live: &[Line<'static>],
        left: &str,
        right: &str,
    ) {
        self.begin_sync();
        if !scrollback.is_empty() {
            let _ = self.insert_scrollback(scrollback);
        }
        let _ = self.draw_frame(live, left, right);
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
        // Best-effort: if the final scrollback insert fails (e.g. the terminal is
        // already closing / broken pipe) we must STILL fall through to restore the
        // cursor below — leaving it hidden and mispositioned is the worse outcome.
        let _ = self.insert_scrollback(final_live);

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

    /// Render one frame: the `live` region across the top rows of the viewport,
    /// then the two-column status panel across the bottom [`PANEL_ROWS`]. The live
    /// lines are already bridged (the child's `avt` grid, or the run's
    /// running-tests block); more lines than fit are clipped, fewer are blank-
    /// padded, so the split stays fixed. The panel columns themselves are laid out
    /// by [`paint_panel`] (which owns the width-driven two-column split); this
    /// method only carves the viewport into live-region and panel rects.
    fn draw_frame(&mut self, live: &[Line<'static>], left: &str, right: &str) -> io::Result<()> {
        self.term.draw(|f| {
            let area = f.area();
            let panel_h = PANEL_ROWS.min(area.height);
            let live_h = area.height - panel_h;
            let live_area = Rect::new(area.x, area.y, area.width, live_h);
            let panel_area = Rect::new(area.x, area.y + live_h, area.width, panel_h);
            if live_h > 0 {
                f.render_widget(Paragraph::new(live.to_vec()), live_area);
            }
            paint_panel(f, panel_area, left, right);
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

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    /// Render a panel through `paint_panel` on a TestBackend and return the buffer
    /// rows as trimmed strings.
    fn render(width: u16, height: u16, left: &str, right: &str) -> Vec<String> {
        let mut term = Terminal::new(TestBackend::new(width, height)).unwrap();
        term.draw(|f| {
            let area = f.area();
            paint_panel(f, area, left, right);
        })
        .unwrap();
        let buf = term.backend().buffer().clone();
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect()
    }

    const PREFLIGHT: &str = "\
────────────
   Preflight kind-zaino-local · 3 ready · 12/16 slots
    capacity [██████░░░░░░] 50% · 6/12c · 28/48Gi free
   Inventory compiling test binaries… · 34s
  Scheduling 8 tests · 1 waves";

    const TRANSFERS: &str = "
dev-zainod · building
dev-zebrad · load→kind
testnet-3.1m · 63%";

    #[test]
    fn wide_terminal_shows_both_columns() {
        let out = render(120, 5, PREFLIGHT, TRANSFERS);
        let joined = out.join("\n");
        // Left column content present.
        assert!(joined.contains("Preflight"), "left col missing:\n{joined}");
        assert!(joined.contains("Inventory"), "left col missing:\n{joined}");
        assert!(joined.contains("Scheduling"), "left col missing:\n{joined}");
        // Right column content present, to the right of the left column.
        assert!(
            joined.contains("dev-zainod"),
            "right col missing:\n{joined}"
        );
        assert!(
            joined.contains("testnet-3.1m"),
            "right col missing:\n{joined}"
        );
        // The branded rule and the first transfer share the top row.
        assert!(out[0].contains("────"), "top rule row:\n{joined}");
    }

    #[test]
    fn split_pins_left_and_gives_the_rest_to_the_right() {
        // Roomy terminal: left pinned to target, right absorbs everything extra.
        assert_eq!(two_col_split(160), (LEFT_COL_TARGET, 160 - LEFT_COL_TARGET));
        assert_eq!(two_col_split(240), (LEFT_COL_TARGET, 240 - LEFT_COL_TARGET));
        // Right grows without bound as the terminal widens.
        assert!(two_col_split(300).1 > two_col_split(200).1);
        // Tight terminal: left yields so the right keeps its floor.
        assert_eq!(two_col_split(TWO_COL_MIN), (TWO_COL_MIN - RIGHT_COL_MIN, RIGHT_COL_MIN));
    }

    #[test]
    fn narrow_terminal_shows_left_column_full_width() {
        let out = render(80, 5, PREFLIGHT, TRANSFERS);
        let joined = out.join("\n");
        assert!(joined.contains("Preflight"), "left col missing:\n{joined}");
        // Right column dropped on a narrow terminal.
        assert!(
            !joined.contains("dev-zainod"),
            "right col leaked:\n{joined}"
        );
    }

    #[test]
    fn empty_right_column_still_renders_left() {
        let out = render(120, 5, PREFLIGHT, "");
        let joined = out.join("\n");
        assert!(joined.contains("Preflight"), "left col missing:\n{joined}");
        assert!(joined.contains("Scheduling"), "left col missing:\n{joined}");
    }

    #[test]
    fn overlong_left_line_is_clipped_not_wrapped() {
        // A left line far wider than its column must stay on one row (clipping),
        // never wrap and push later rows off the fixed-height grid.
        let long = format!("   Preflight {}", "x".repeat(400));
        let panel = format!("────────────\n{long}\nline3\nline4\nline5");
        let out = render(120, 5, &panel, TRANSFERS);
        assert_eq!(out.len(), 5, "grid height preserved");
        assert!(out[2].contains("line3"), "row 3 not pushed off:\n{out:?}");
        assert!(out[4].contains("line5"), "row 5 not pushed off:\n{out:?}");
    }
}
