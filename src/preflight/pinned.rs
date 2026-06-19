//! Pinned-header banner — banner stays anchored at a fixed row of the
//! main screen while child-process output (`cargo nextest list`
//! compile lines, etc.) scrolls beneath it.
//!
//! ## Mechanics
//!
//! 1. **No screen clear.** Banner is drawn at `start_row` via absolute
//!    cursor positioning (`\x1b[<row>;1H`). Content above `start_row`
//!    (the user's shell prompt, the `ztest run` command, prior output)
//!    is left untouched.
//! 2. **DECSTBM scroll region.** `\x1b[<start_row+H>;<bottom> r` pins
//!    rows `start_row..start_row+H` (the banner). Writes that exceed
//!    `bottom` scroll content *within* the region, leaving the banner
//!    untouched.
//! 3. **Park cursor.** Cursor moves to `(start_row+H, 1)` — the top
//!    of the scroll region — so child writes land there.
//! 4. **Redraw on state change.** [`PinnedHeader::redraw`] does
//!    DEC save (`\x1b[s`) → absolute move to `start_row` → write
//!    banner lines with line-clears (`\x1b[K`) → DEC restore
//!    (`\x1b[u`). The cursor returns to wherever the child's writes
//!    left it.
//! 5. **Fallback when banner doesn't fit.** If `start_row + H > rows`
//!    the banner can't fit below the cursor. We then emit
//!    `\x1b[<rows>S` (Scroll Up) to push everything visible into the
//!    terminal's native scrollback ring, draw the banner at row 1,
//!    and set DECSTBM normally.
//! 6. **Release on drop.** `\x1b[r` resets the scroll region. The
//!    banner and child output stay on screen; subsequent writes
//!    scroll the whole screen naturally so everything ends up in
//!    the terminal's native scrollback.

use std::io::{self, Write};

/// One column of the right edge gets reserved for terminals that
/// soft-wrap their last column; banners never exceed this width
/// minus one.
#[allow(dead_code)]
pub const RIGHT_MARGIN: u16 = 1;

/// Whole-screen banner pinned at a fixed row via DECSTBM.
///
/// Owns a writer (typically [`std::io::Stdout`]). On construction it
/// draws the banner at `start_row`, sets the scroll region, and parks
/// the cursor below the banner. On [`Drop`] it resets the scroll
/// region; remaining screen content stays intact.
#[derive(Debug)]
pub struct PinnedHeader<W: Write> {
    writer: W,
    rows: u16,
    banner_height: u16,
    /// Absolute 1-indexed row where the banner's top line begins.
    /// Either the caller-provided value, or `1` if the banner didn't
    /// fit and we fell back to "scroll into scrollback, banner at top."
    start_row: u16,
}

impl<W: Write> PinnedHeader<W> {
    /// Enter pinned-header mode.
    ///
    /// `rows` is the terminal height. `banner` is the rendered banner
    /// frame. `start_row` is the absolute 1-indexed row where the
    /// caller wants the banner to begin — typically the row of the
    /// cursor right after the `ztest run` command line, so the banner
    /// renders *below* the command without overwriting it.
    ///
    /// If the banner would extend past the bottom of the screen
    /// (`start_row + banner_height > rows`), the entire visible screen
    /// is scrolled into scrollback via SU and the banner is placed at
    /// row 1 instead.
    pub fn enter(mut writer: W, rows: u16, banner: &str, start_row: u16) -> io::Result<Self> {
        let h = count_lines(banner) as u16;

        // Fall back to top-of-screen if the banner doesn't fit below
        // the requested start row.
        let effective_start = if start_row + h > rows {
            write!(writer, "\x1b[{rows}S")?;
            1
        } else {
            start_row.max(1)
        };

        // Move cursor to banner start, write banner, set DECSTBM,
        // park cursor at the top of the scroll region.
        write!(writer, "\x1b[{};1H", effective_start)?;
        writer.write_all(banner.as_bytes())?;
        write!(writer, "\x1b[{};{}r", effective_start + h, rows)?;
        write!(writer, "\x1b[{};1H", effective_start + h)?;
        writer.flush()?;

        Ok(Self {
            writer,
            rows,
            banner_height: h,
            start_row: effective_start,
        })
    }

    /// Redraw the banner with a freshly-rendered frame. Cursor is
    /// saved, the banner area is overwritten, cursor is restored —
    /// so any in-flight child output stream beneath the banner is
    /// unaffected.
    ///
    /// If the new banner has a different height than the last one,
    /// the scroll region is re-emitted so the region top tracks the
    /// banner bottom.
    pub fn redraw(&mut self, banner: &str) -> io::Result<()> {
        let h = count_lines(banner) as u16;
        // DEC save cursor (terminal-internal, position + attrs).
        write!(self.writer, "\x1b[s")?;
        // Move to banner's top row (absolute).
        write!(self.writer, "\x1b[{};1H", self.start_row)?;
        // Write each banner line with a leading line-clear so a
        // shorter new value cleanly replaces a longer old one.
        for line in banner.lines() {
            write!(self.writer, "\x1b[K{line}\r\n")?;
        }
        if h != self.banner_height {
            // Banner grew or shrank — move the DECSTBM top so the
            // region tracks the banner bottom.
            write!(self.writer, "\x1b[{};{}r", self.start_row + h, self.rows)?;
            self.banner_height = h;
        }
        // DEC restore cursor.
        write!(self.writer, "\x1b[u")?;
        self.writer.flush()
    }
}

impl<W: Write> Drop for PinnedHeader<W> {
    fn drop(&mut self) {
        // Reset scroll region. Banner + child output stay on screen
        // and behave as normal scrollback from this point on.
        let _ = write!(self.writer, "\x1b[r");
        let _ = self.writer.flush();
    }
}

/// Line count for a banner frame. Matches `frame.lines().count()`
/// except that a trailing newline does not produce a phantom blank
/// row (mirroring how the terminal counts cursor positions).
fn count_lines(frame: &str) -> usize {
    if frame.is_empty() {
        return 0;
    }
    let n = frame.bytes().filter(|&b| b == b'\n').count();
    if frame.ends_with('\n') { n } else { n + 1 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_lines_matches_terminal_row_count() {
        assert_eq!(count_lines(""), 0);
        assert_eq!(count_lines("one"), 1);
        assert_eq!(count_lines("one\n"), 1);
        assert_eq!(count_lines("one\ntwo"), 2);
        assert_eq!(count_lines("one\ntwo\n"), 2);
        assert_eq!(count_lines("one\ntwo\nthree\n"), 3);
    }

    #[test]
    fn enter_below_cursor_no_clear_sets_decstbm() {
        let mut buf = Vec::new();
        {
            let h = PinnedHeader::enter(&mut buf, 24, "row1\nrow2\nrow3\n", 10).unwrap();
            drop(h);
        }
        let s = String::from_utf8(buf).unwrap();
        // No full-screen clear — banner sits below the cursor row.
        assert!(!s.contains("\x1b[2J"), "should not clear screen: {s:?}");
        // Banner positioned at start_row 10.
        assert!(s.contains("\x1b[10;1H"), "no cursor at start_row: {s:?}");
        assert!(s.contains("row1\nrow2\nrow3\n"), "banner missing: {s:?}");
        // DECSTBM region begins at row 13 (10 + banner_height 3).
        assert!(s.contains("\x1b[13;24r"), "no DECSTBM(13,24): {s:?}");
        // Cursor parked at the top of the scroll region.
        assert!(s.contains("\x1b[13;1H"), "no cursor park: {s:?}");
        assert!(s.ends_with("\x1b[r"), "no DECSTBM reset on drop: {s:?}");
    }

    #[test]
    fn enter_falls_back_to_top_when_banner_overflows() {
        let mut buf = Vec::new();
        {
            // 24-row terminal, banner is 10 rows tall, start_row=20 →
            // banner would extend to row 29, past the screen. Fall
            // back to scroll-into-scrollback + banner at row 1.
            let banner = "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\n";
            let h = PinnedHeader::enter(&mut buf, 24, banner, 20).unwrap();
            drop(h);
        }
        let s = String::from_utf8(buf).unwrap();
        // SU pushed the visible screen into scrollback.
        assert!(s.contains("\x1b[24S"), "no SU(24): {s:?}");
        // Banner placed at top.
        assert!(
            s.contains("\x1b[1;1H"),
            "no top-of-screen positioning: {s:?}"
        );
        // DECSTBM at the right offset for top-of-screen banner.
        assert!(
            s.contains("\x1b[11;24r"),
            "no DECSTBM(11,24) for top: {s:?}"
        );
    }

    #[test]
    fn redraw_saves_restores_and_clears_lines() {
        let mut buf = Vec::new();
        {
            let mut h = PinnedHeader::enter(&mut buf, 24, "a\nb\n", 5).unwrap();
            h.redraw("c\nd\n").unwrap();
        }
        let s = String::from_utf8(buf).unwrap();
        assert!(
            s.contains("\x1b[s\x1b[5;1H"),
            "no save+absolute-move: {s:?}"
        );
        assert!(s.contains("\x1b[Kc\r\n"), "no line-clear+content: {s:?}");
        assert!(s.contains("\x1b[u"), "no restore: {s:?}");
    }

    #[test]
    fn redraw_with_changed_height_reemits_decstbm() {
        let mut buf = Vec::new();
        {
            let mut h = PinnedHeader::enter(&mut buf, 24, "a\nb\n", 5).unwrap();
            // banner grows from 2 to 3 lines
            h.redraw("a\nb\nc\n").unwrap();
        }
        let s = String::from_utf8(buf).unwrap();
        // Two DECSTBMs: 7..24 from enter (5+2), then 8..24 from height change (5+3).
        assert!(s.contains("\x1b[7;24r"), "missing initial DECSTBM: {s:?}");
        assert!(s.contains("\x1b[8;24r"), "missing resized DECSTBM: {s:?}");
    }
}
