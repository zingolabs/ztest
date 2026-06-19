//! In-place multi-line redraw — the production analogue of
//! [`indicatif::MultiProgress`] for our whole-block banner rendering.
//!
//! `indicatif` redraws per-row using individual `ProgressBar` widgets;
//! our renderer produces the full block as one `String` so the layout
//! decisions stay in one place. [`LiveRender`] does the same kind of
//! cursor-up + clear that `indicatif` does internally, but at the
//! granularity of the whole banner.
//!
//! Usage:
//!
//! ```no_run
//! use std::io::stdout;
//! use ztest::preflight::{render, LiveRender, Theme};
//! # fn build_state() -> ztest::preflight::BannerState { todo!() }
//!
//! let theme = Theme::detect();
//! let mut live = LiveRender::new(stdout());
//! loop {
//!     let frame = render(&build_state(), &theme);
//!     live.draw(&frame).unwrap();
//!     if /* done */ false { break }
//! }
//! live.finish(&render(&build_state(), &theme)).unwrap();
//! ```
//!
//! TTY-less callers should set `interactive = false` on construction;
//! `draw` then writes nothing until [`LiveRender::finish`] commits the
//! final frame, so CI log capture isn't polluted with redraws.

use std::io::{self, IsTerminal, Write};

/// In-place renderer for whole-block frames.
///
/// Owns its writer. `W` is typically `std::io::Stdout` in production
/// and `&mut Vec<u8>` in tests — anything implementing [`Write`].
#[derive(Debug)]
pub struct LiveRender<W: Write> {
    writer: W,
    last_frame_lines: usize,
    interactive: bool,
}

impl<W: Write> LiveRender<W> {
    /// Construct with automatic TTY detection — interactive iff the
    /// writer is a terminal. For non-terminal writers (`Vec<u8>`, a
    /// file, a pipe) this defaults to non-interactive mode; intermediate
    /// `draw` calls become no-ops and only [`finish`](Self::finish)
    /// commits.
    ///
    /// `W: IsTerminal` is satisfied by `Stdout`, `Stderr`, files, and
    /// `&mut` references to any of them.
    pub fn new(writer: W) -> Self
    where
        W: IsTerminal,
    {
        let interactive = writer.is_terminal();
        Self::with_mode(writer, interactive)
    }

    /// Explicit mode constructor — for tests and for callers that want
    /// to override the TTY heuristic.
    pub fn with_mode(writer: W, interactive: bool) -> Self {
        Self {
            writer,
            last_frame_lines: 0,
            interactive,
        }
    }

    /// Draw an intermediate frame. No-op when non-interactive.
    ///
    /// `frame` is expected to be the full multi-line banner; its line
    /// count is used to position the cursor for the next redraw.
    pub fn draw(&mut self, frame: &str) -> io::Result<()> {
        if !self.interactive {
            return Ok(());
        }
        self.clear_previous()?;
        self.writer.write_all(frame.as_bytes())?;
        self.last_frame_lines = count_lines(frame);
        self.writer.flush()
    }

    /// Commit a final frame and forget any redraw state. Always writes,
    /// even when non-interactive — callers should use this for the
    /// terminal state of the banner so CI logs capture the result.
    pub fn finish(&mut self, frame: &str) -> io::Result<()> {
        if self.interactive {
            self.clear_previous()?;
        }
        self.writer.write_all(frame.as_bytes())?;
        self.last_frame_lines = 0;
        self.writer.flush()
    }

    fn clear_previous(&mut self) -> io::Result<()> {
        if self.last_frame_lines == 0 {
            return Ok(());
        }
        // `\x1b[<N>A` — cursor up N lines.
        // `\r`        — cursor to column 0 (defensive, in case the user
        //               started typing).
        // `\x1b[J`    — erase from cursor to end of screen.
        write!(self.writer, "\x1b[{}A\r\x1b[J", self.last_frame_lines)
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
    fn non_interactive_draw_is_noop_finish_writes() {
        let mut buf = Vec::new();
        {
            let mut live = LiveRender::with_mode(&mut buf, false);
            live.draw("frame-1\n").unwrap();
            live.draw("frame-2\n").unwrap();
            live.finish("final\n").unwrap();
        }
        assert_eq!(buf, b"final\n");
    }

    #[test]
    fn interactive_draw_emits_cursor_controls_between_frames() {
        let mut buf = Vec::new();
        {
            let mut live = LiveRender::with_mode(&mut buf, true);
            live.draw("a\nb\n").unwrap();
            live.draw("c\nd\n").unwrap();
            live.finish("e\nf\n").unwrap();
        }
        let s = String::from_utf8(buf).unwrap();
        // First frame writes plainly.
        assert!(s.starts_with("a\nb\n"), "got: {s:?}");
        // Second frame is preceded by cursor-up-2 + clear.
        assert!(s.contains("\x1b[2A\r\x1b[J"), "missing redraw codes: {s:?}");
        // Final frame ends with the committed final content.
        assert!(s.ends_with("e\nf\n"), "got: {s:?}");
    }

    #[test]
    fn count_lines_matches_terminal_row_count() {
        assert_eq!(count_lines(""), 0);
        assert_eq!(count_lines("one"), 1);
        assert_eq!(count_lines("one\n"), 1);
        assert_eq!(count_lines("one\ntwo"), 2);
        assert_eq!(count_lines("one\ntwo\n"), 2);
        assert_eq!(count_lines("one\ntwo\nthree\n"), 3);
    }
}
