//! `Surface`: the terminal-owning render primitive.
//!
//! The render thread (`super::render`) owns the single instance. `Surface` speaks
//! only in already-composed ANSI strings — the lines to commit into scrollback,
//! the live rows, and the two panel column strings — and drives the terminal with
//! a manual **sticky footer** ([`super::footer`]): completed lines are printed
//! normally so the terminal scrolls them into its own native scrollback, and only
//! the footer (live region + pinned panel) is repainted in place. There is no
//! reserved region and no fixed height — the footer is exactly as tall as its
//! content each frame.
//!
//! Each frame is wrapped in a DEC private mode 2026 synchronized update so the
//! cursor-up + repaint present atomically (terminals without 2026 ignore it), and
//! the cursor is hidden for the session so it never blinks inside the panel.

use std::io::{self, Write as _};

use super::{bridge, footer};

/// Minimum terminal width for the two-column panel. Below this the right
/// (transfer) column is dropped and the left column spans the full width — a
/// narrow terminal can't fit both without mangling either.
const TWO_COL_MIN: u16 = 90;

/// The left column's target width. Its content is fixed status lines whose widest
/// (the capacity gauge) needs ~56 cols; 80 leaves headroom for a long
/// kube-context. Held constant so every extra terminal column flows to the right
/// (transfer) column, which is the one that wants the room.
const LEFT_COL_TARGET: u16 = 80;
/// Floor for the right column when the terminal is too narrow to grant the left
/// its target: the left yields down to this so the transfer column stays legible.
const RIGHT_COL_MIN: u16 = 30;

/// Compose the two-column panel into ANSI rows, each clipped to `cols` so it
/// stays exactly one physical row.
///
/// At or above [`TWO_COL_MIN`] (and with a non-empty right column) the width is
/// split into a left column pinned to [`LEFT_COL_TARGET`] (yielding toward
/// [`RIGHT_COL_MIN`] on a tight terminal) and a right column taking the rest;
/// each left row is padded to the left width and the right row appended. Below
/// that, a single full-width left column. A line longer than its column is
/// clipped (keeping the labelled head), never wrapped.
fn compose_panel(left: &str, right: &str, cols: u16) -> Vec<String> {
    let right = right.trim_end_matches('\n');
    if cols < TWO_COL_MIN || right.is_empty() {
        return bridge::ansi_rows(left, cols as usize)
            .into_iter()
            .map(|(s, _)| s)
            .collect();
    }
    let (left_w, right_w) = two_col_split(cols);
    let lrows = bridge::ansi_rows(left, left_w as usize);
    let rrows = bridge::ansi_rows(right, right_w as usize);
    let n = lrows.len().max(rrows.len());
    (0..n)
        .map(|i| {
            let (l, used) = lrows.get(i).cloned().unwrap_or_default();
            let r = rrows.get(i).map(|(s, _)| s.clone()).unwrap_or_default();
            let pad = (left_w as usize).saturating_sub(used);
            format!("{l}{}{r}", " ".repeat(pad))
        })
        .collect()
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
/// into cooperative cancellation) rather than arriving as a raw byte we'd have to
/// read. The original attributes are restored by [`Surface::finish`] and, as a
/// panic/`exit` backstop, by this guard's `Drop`.
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

/// Synchronized-update + cursor-visibility control sequences (DEC private modes).
const SYNC_BEGIN: &str = "\x1b[?2026h";
const SYNC_END: &str = "\x1b[?2026l";
const CURSOR_HIDE: &str = "\x1b[?25l";
const CURSOR_SHOW: &str = "\x1b[?25h";

/// The terminal owner: drives a manual sticky footer over real stdout. Holds the
/// current width/height and the footer's last height (for the in-place repaint).
/// Rendering is synchronous — no ratatui, no reserved viewport, no runtime.
pub(crate) struct Surface {
    stdout: io::Stdout,
    cols: u16,
    rows: u16,
    /// Footer physical-row count from the last [`present`](Self::present), threaded
    /// back into [`footer::render`] to walk the cursor up for the next repaint.
    prev_footer_rows: usize,
    tty: TtyGuard,
}

impl Surface {
    /// The session surface. Hides the cursor for the session (shown again by
    /// [`finish`](Self::finish)) and enters no-echo mode. Nothing is reserved: the
    /// first [`present`](Self::present) draws the footer at the current cursor row
    /// and every completed line above it scrolls into native scrollback.
    pub fn bottom_panel() -> io::Result<Surface> {
        let (cols, rows) = terminal_size::terminal_size()
            .map(|(w, h)| (w.0, h.0))
            .unwrap_or((80, 40));
        let mut stdout = io::stdout();
        stdout.write_all(CURSOR_HIDE.as_bytes())?;
        stdout.flush()?;
        Ok(Surface {
            stdout,
            cols,
            rows,
            prev_footer_rows: 0,
            tty: TtyGuard::enter(),
        })
    }

    pub fn cols(&self) -> u16 {
        self.cols
    }

    /// Rows available for the live region above the pinned panel — the height the
    /// render thread sizes the `avt` grid and each child PTY to, and the ceiling the
    /// engine's running block grows to (see [`super::live_rows_for`]).
    pub fn live_rows(&self) -> u16 {
        super::live_rows_for(self.rows)
    }

    /// Re-query and record the terminal size (after SIGWINCH).
    pub fn set_size(&mut self, cols: u16, rows: u16) {
        self.cols = cols;
        self.rows = rows;
    }

    /// Present one frame: commit `committed` into native scrollback, then repaint
    /// the footer (the `live` rows above the two-column panel) in place. The whole
    /// frame is one synchronized-update write.
    pub fn present(&mut self, committed: &[String], live: &[String], left: &str, right: &str) {
        let mut footer_lines: Vec<String> = live.to_vec();
        footer_lines.extend(compose_panel(left, right, self.cols));
        // Never let the footer exceed the screen: the cursor can't be walked up
        // into scrollback, so drop from the top (oldest live rows) if it would.
        let max = self.rows as usize;
        if footer_lines.len() > max {
            footer_lines.drain(0..footer_lines.len() - max);
        }

        let mut frame = String::new();
        frame.push_str(SYNC_BEGIN);
        self.prev_footer_rows =
            footer::render(&mut frame, committed, &footer_lines, self.prev_footer_rows);
        frame.push_str(SYNC_END);
        let _ = self.stdout.write_all(frame.as_bytes());
        let _ = self.stdout.flush();
    }

    /// Tear down: commit `final_live` into native scrollback, erase the footer,
    /// restore the terminal's line discipline, and show the cursor on a clean line.
    ///
    /// Takes `&mut self` (not `self`) so the render thread's hard-exit backstop can
    /// restore in place before `process::exit` (which would skip `Drop`).
    pub fn finish(&mut self, final_live: &[String]) {
        self.tty.restore();
        let mut frame = String::new();
        // Commit the final live rows as scrollback and draw an empty footer, which
        // erases the panel and leaves the cursor on a fresh line below.
        footer::render(&mut frame, final_live, &[], self.prev_footer_rows);
        self.prev_footer_rows = 0;
        frame.push_str(CURSOR_SHOW);
        let _ = self.stdout.write_all(frame.as_bytes());
        let _ = self.stdout.flush();
    }
}

impl Drop for Surface {
    fn drop(&mut self) {
        // Backstop for a normal drop that skipped `finish` (the hard-exit path
        // calls `finish` explicitly): make sure the cursor comes back.
        let _ = self.stdout.write_all(CURSOR_SHOW.as_bytes());
        let _ = self.stdout.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn joined(lines: &[String]) -> String {
        lines.join("\n")
    }

    #[test]
    fn wide_terminal_shows_both_columns() {
        let out = compose_panel(PREFLIGHT, TRANSFERS, 120);
        let j = joined(&out);
        assert!(j.contains("Preflight"), "left col missing:\n{j}");
        assert!(j.contains("Inventory"), "left col missing:\n{j}");
        assert!(j.contains("Scheduling"), "left col missing:\n{j}");
        assert!(j.contains("dev-zainod"), "right col missing:\n{j}");
        assert!(j.contains("testnet-3.1m"), "right col missing:\n{j}");
        // The branded rule and the first transfer share the top row.
        assert!(out[0].contains("────"), "top rule row:\n{j}");
    }

    #[test]
    fn split_pins_left_and_gives_the_rest_to_the_right() {
        assert_eq!(two_col_split(160), (LEFT_COL_TARGET, 160 - LEFT_COL_TARGET));
        assert_eq!(two_col_split(240), (LEFT_COL_TARGET, 240 - LEFT_COL_TARGET));
        assert!(two_col_split(300).1 > two_col_split(200).1);
        assert_eq!(
            two_col_split(TWO_COL_MIN),
            (TWO_COL_MIN - RIGHT_COL_MIN, RIGHT_COL_MIN)
        );
    }

    #[test]
    fn narrow_terminal_shows_left_column_full_width() {
        let out = compose_panel(PREFLIGHT, TRANSFERS, 80);
        let j = joined(&out);
        assert!(j.contains("Preflight"), "left col missing:\n{j}");
        assert!(!j.contains("dev-zainod"), "right col leaked:\n{j}");
    }

    #[test]
    fn empty_right_column_still_renders_left() {
        let out = compose_panel(PREFLIGHT, "", 120);
        let j = joined(&out);
        assert!(j.contains("Preflight"), "left col missing:\n{j}");
        assert!(j.contains("Scheduling"), "left col missing:\n{j}");
    }

    #[test]
    fn every_panel_row_fits_within_cols() {
        // The one invariant the footer's cursor math depends on: no composed row
        // exceeds the terminal width (which would wrap into a second physical row).
        for cols in [80u16, 90, 120, 200] {
            for row in compose_panel(PREFLIGHT, TRANSFERS, cols) {
                let w = bridge::ansi_rows(&row, usize::MAX)[0].1;
                assert!(w <= cols as usize, "row {w} > {cols}: {row:?}");
            }
        }
    }

    #[test]
    fn overlong_left_line_is_clipped_to_one_row() {
        let long = format!("   Preflight {}", "x".repeat(400));
        let panel = format!("────────────\n{long}\nline3\nline4\nline5");
        let out = compose_panel(&panel, TRANSFERS, 120);
        assert_eq!(out.len(), 5, "one row per logical line");
        let w = bridge::ansi_rows(&out[1], usize::MAX)[0].1;
        assert!(w <= 120, "clipped row width {w} <= 120");
    }
}
