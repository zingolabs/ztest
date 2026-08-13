//! `Surface` = terminal-owning render primitive, one instance, owned by the render thread.
//!
//! - Composed ANSI strings in, manual sticky footer out ([`super::footer`])
//! - Completed lines → native scrollback, only the footer repaints in place
//! - Frame wrapped in DEC 2026 synchronized update → cursor-up + repaint land atomically
//!   (terminals without 2026 ignore it)
//! - Cursor hidden for the session (never blinks inside the panel)

use std::io::{self, Write as _};

use super::{bridge, footer};

/// Minimum width for the two-column panel. Below it the right (transfer) column drops
/// and the left spans the full width
const TWO_COL_MIN: u16 = 90;

/// Left column target, held constant → every extra terminal column flows right
const LEFT_COL_TARGET: u16 = 80;
/// Right-column floor on a terminal too narrow for the left's target (the left yields
/// down to this, keeping transfers legible)
const RIGHT_COL_MIN: u16 = 30;

/// Minimum width for three columns = left at target + middle + right, each at
/// [`RIGHT_COL_MIN`]. Below it a scene offering a middle composes as left + *middle*
/// (a phase offers a middle because that is its live content)
const THREE_COL_MIN: u16 = LEFT_COL_TARGET + RIGHT_COL_MIN * 2;

/// Middle-column width at three columns. Fixed, so extra width still flows right
const MID_COL_WIDTH: u16 = 34;

/// Footer's physical rows: live region stacked above the panel.
///
/// - **Every** line clipped to `cols` = 1 logical line → 1 physical row, the invariant
///   [`super::footer::render`]'s `prev_rows` cursor arithmetic rests on
/// - Live lines arrive un-clipped (an over-wide name would wrap and leak stale rows)
fn compose_footer(
    live: &[String],
    left: &str,
    mid: Option<&str>,
    right: &str,
    cols: u16,
) -> Vec<String> {
    let mut lines: Vec<String> = live
        .iter()
        .flat_map(|l| bridge::ansi_rows(l, cols as usize).into_iter().map(|(s, _)| s))
        .collect();
    lines.extend(compose_panel(left, mid, right, cols));
    lines
}

fn compose_panel(left: &str, mid: Option<&str>, right: &str, cols: u16) -> Vec<String> {
    let right = right.trim_end_matches('\n');
    let mid = mid.map(|m| m.trim_end_matches('\n')).filter(|m| !m.is_empty());

    // Three when offered and seatable, else two keeping middle over right
    // (see `THREE_COL_MIN`), else one
    let columns: Vec<(&str, u16)> = match mid {
        Some(mid) if cols >= THREE_COL_MIN => {
            let right_w = cols - LEFT_COL_TARGET - MID_COL_WIDTH;
            vec![(left, LEFT_COL_TARGET), (mid, MID_COL_WIDTH), (right, right_w)]
        }
        Some(mid) if cols >= TWO_COL_MIN => {
            let (left_w, mid_w) = two_col_split(cols);
            vec![(left, left_w), (mid, mid_w)]
        }
        None if cols >= TWO_COL_MIN && !right.is_empty() => {
            let (left_w, right_w) = two_col_split(cols);
            vec![(left, left_w), (right, right_w)]
        }
        _ => {
            return bridge::ansi_rows(left, cols as usize).into_iter().map(|(s, _)| s).collect();
        }
    };

    let rows: Vec<Vec<(String, usize)>> =
        columns.iter().map(|(text, w)| bridge::ansi_rows(text, *w as usize)).collect();
    let n = rows.iter().map(Vec::len).max().unwrap_or(0);
    let last = rows.len() - 1;
    (0..n)
        .map(|i| {
            let mut line = String::new();
            for (col_ix, (col, (_, w))) in rows.iter().zip(columns.iter()).enumerate() {
                let (text, used) = col.get(i).cloned().unwrap_or_default();
                line.push_str(&text);
                // Padded to full width → next column at a fixed offset, no shear on a
                // short row. Last needs none (already clipped, and trailing spaces to
                // the screen edge only risk a wrap)
                if col_ix != last {
                    line.push_str(&" ".repeat((*w as usize).saturating_sub(used)));
                }
            }
            line
        })
        .collect()
}

/// Two-column width → `(left, right)`: left pinned to [`LEFT_COL_TARGET`], yielding
/// toward [`RIGHT_COL_MIN`] when tight; right takes the remainder, uncapped.
/// Callers must pass `width >= TWO_COL_MIN` (else the subtraction underflows)
fn two_col_split(width: u16) -> (u16, u16) {
    let left = LEFT_COL_TARGET.min(width - RIGHT_COL_MIN);
    (left, width - left)
}

/// Restores the controlling terminal's line discipline on drop.
///
/// - `ECHO` + `ICANON` off (cooked mode echoes keystrokes, `^C` worst, onto the panel)
/// - `ISIG` kept → Ctrl-C still raises `SIGINT` instead of arriving as a raw byte
/// - Restored by [`Surface::finish`], with `Drop` as the panic/`exit` backstop
struct TtyGuard {
    fd: std::os::fd::RawFd,
    original: Option<libc::termios>,
}

impl TtyGuard {
    /// Enter no-echo / no-canonical mode on stdin's tty, saving the prior attributes.
    /// No-op (`original: None`) off a tty
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

    /// Restore the saved attributes. Idempotent — `finish` calls it (covering a
    /// `Drop`-skipping `process::exit`) and `Drop` calls it again
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

/// Synchronized-update + cursor-visibility sequences (DEC private modes)
const SYNC_BEGIN: &str = "\x1b[?2026h";
const SYNC_END: &str = "\x1b[?2026l";
const CURSOR_HIDE: &str = "\x1b[?25l";
const CURSOR_SHOW: &str = "\x1b[?25h";

/// Terminal owner: manual sticky footer over real stdout, synchronous (no ratatui,
/// no reserved viewport, no runtime). `prev_footer_rows` = last present's physical row
/// count, threaded into [`footer::render`] to walk the cursor up
pub(crate) struct Surface {
    stdout: io::Stdout,
    cols: u16,
    rows: u16,
    prev_footer_rows: usize,
    tty: TtyGuard,
}

impl Surface {
    /// Session surface: cursor hidden until [`finish`](Self::finish), no-echo entered.
    /// Nothing reserved — the first [`present`](Self::present) draws the footer at the
    /// current cursor row, completed lines scroll above it
    pub fn bottom_panel() -> io::Result<Surface> {
        let (cols, rows) =
            terminal_size::terminal_size().map(|(w, h)| (w.0, h.0)).unwrap_or((80, 40));
        let mut stdout = io::stdout();
        stdout.write_all(CURSOR_HIDE.as_bytes())?;
        stdout.flush()?;
        Ok(Surface { stdout, cols, rows, prev_footer_rows: 0, tty: TtyGuard::enter() })
    }

    pub fn cols(&self) -> u16 {
        self.cols
    }

    /// Rows for the live region above the panel (see [`super::live_rows_for`])
    pub fn live_rows(&self) -> u16 {
        super::live_rows_for(self.rows)
    }

    pub fn set_size(&mut self, cols: u16, rows: u16) {
        self.cols = cols;
        self.rows = rows;
    }

    /// One frame: `committed` → native scrollback, then repaint the footer (`live`
    /// rows above the panel) in place. One synchronized-update write
    pub fn present(
        &mut self,
        committed: &[String],
        live: &[String],
        left: &str,
        mid: Option<&str>,
        right: &str,
    ) {
        let mut footer_lines = compose_footer(live, left, mid, right, self.cols);
        // Cursor cannot walk up into scrollback → an over-tall footer sheds its
        // oldest live rows
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

    /// Tear down: `final_live` → scrollback, footer erased, line discipline restored,
    /// cursor shown on a clean line.
    ///
    /// `&mut self` not `self`, so the hard-exit backstop restores in place before a
    /// `Drop`-skipping `process::exit`
    pub fn finish(&mut self, final_live: &[String]) {
        self.tty.restore();
        let mut frame = String::new();
        // Empty footer erases the panel, cursor left on a fresh line
        footer::render(&mut frame, final_live, &[], self.prev_footer_rows);
        self.prev_footer_rows = 0;
        frame.push_str(CURSOR_SHOW);
        let _ = self.stdout.write_all(frame.as_bytes());
        let _ = self.stdout.flush();
    }
}

impl Drop for Surface {
    fn drop(&mut self) {
        // Backstop for a drop that skipped `finish`
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
        let out = compose_panel(PREFLIGHT, None, TRANSFERS, 120);
        let j = joined(&out);
        assert!(j.contains("Preflight"), "left col missing:\n{j}");
        assert!(j.contains("Inventory"), "left col missing:\n{j}");
        assert!(j.contains("Scheduling"), "left col missing:\n{j}");
        assert!(j.contains("dev-zainod"), "right col missing:\n{j}");
        assert!(j.contains("testnet-3.1m"), "right col missing:\n{j}");
        // Branded rule and first transfer share the top row
        assert!(out[0].contains("────"), "top rule row:\n{j}");
    }

    #[test]
    fn split_pins_left_and_gives_the_rest_to_the_right() {
        assert_eq!(two_col_split(160), (LEFT_COL_TARGET, 160 - LEFT_COL_TARGET));
        assert_eq!(two_col_split(240), (LEFT_COL_TARGET, 240 - LEFT_COL_TARGET));
        assert!(two_col_split(300).1 > two_col_split(200).1);
        assert_eq!(two_col_split(TWO_COL_MIN), (TWO_COL_MIN - RIGHT_COL_MIN, RIGHT_COL_MIN));
    }

    const WORK: &str = "
    sap  12.1k/s ⣠⣤⣶⣿⣷⣤
   orch   8.4k/s ⣶⣿⣷⣤⣀⣠
  total  23.6k/s 30m";

    #[test]
    fn a_wide_terminal_seats_all_three_columns() {
        let out = compose_panel(PREFLIGHT, Some(WORK), TRANSFERS, 200);
        let j = joined(&out);
        assert!(j.contains("Preflight"), "left col missing:\n{j}");
        assert!(j.contains("total"), "mid col missing:\n{j}");
        assert!(j.contains("dev-zainod"), "right col missing:\n{j}");
    }

    /// Which column goes when only two fit: the static right one, never the middle
    /// (dropping the middle leaves a watcher on counters with the rates off-screen)
    #[test]
    fn a_two_column_terminal_keeps_the_middle_over_the_right() {
        let out = compose_panel(PREFLIGHT, Some(WORK), TRANSFERS, THREE_COL_MIN - 1);
        let j = joined(&out);
        assert!(j.contains("Preflight"), "left col missing:\n{j}");
        assert!(j.contains("total"), "mid col dropped:\n{j}");
        assert!(!j.contains("dev-zainod"), "right col should have gone:\n{j}");
    }

    #[test]
    fn three_columns_never_exceed_the_terminal_width() {
        for cols in [THREE_COL_MIN, 160u16, 200, 300] {
            for row in compose_panel(PREFLIGHT, Some(WORK), TRANSFERS, cols) {
                let w = bridge::ansi_rows(&row, usize::MAX)[0].1;
                assert!(w <= cols as usize, "row {w} > {cols}: {row:?}");
            }
        }
    }

    #[test]
    fn narrow_terminal_shows_left_column_full_width() {
        let out = compose_panel(PREFLIGHT, None, TRANSFERS, 80);
        let j = joined(&out);
        assert!(j.contains("Preflight"), "left col missing:\n{j}");
        assert!(!j.contains("dev-zainod"), "right col leaked:\n{j}");
    }

    #[test]
    fn empty_right_column_still_renders_left() {
        let out = compose_panel(PREFLIGHT, None, "", 120);
        let j = joined(&out);
        assert!(j.contains("Preflight"), "left col missing:\n{j}");
        assert!(j.contains("Scheduling"), "left col missing:\n{j}");
    }

    #[test]
    fn every_panel_row_fits_within_cols() {
        // The invariant the footer's cursor math rests on: no composed row exceeds
        // the terminal width (a wrap = a second physical row)
        for cols in [80u16, 90, 120, 200] {
            for row in compose_panel(PREFLIGHT, None, TRANSFERS, cols) {
                let w = bridge::ansi_rows(&row, usize::MAX)[0].1;
                assert!(w <= cols as usize, "row {w} > {cols}: {row:?}");
            }
        }
    }

    #[test]
    fn overlong_left_line_is_clipped_to_one_row() {
        let long = format!("   Preflight {}", "x".repeat(400));
        let panel = format!("────────────\n{long}\nline3\nline4\nline5");
        let out = compose_panel(&panel, None, TRANSFERS, 120);
        assert_eq!(out.len(), 5, "one row per logical line");
        let w = bridge::ansi_rows(&out[1], usize::MAX)[0].1;
        assert!(w <= 120, "clipped row width {w} <= 120");
    }

    #[test]
    fn overlong_live_line_stays_one_physical_row() {
        // Over-wide running-test line must clip, not wrap (a second physical row
        // desyncs `footer::render`'s `prev_rows` and leaks stale rows into scrollback)
        let live = vec![
            "PASS [  24.882s] clientless::chain_cache chain_query_interface::\
             ephemeral_serves_finalised_blocks_zebrad"
                .to_string(),
            "     [ 00:00:41] clientless::chain_cache chain_query_interface::\
             get_mempool_stream_fresh_snapshot_repeated_zebrad"
                .to_string(),
        ];
        for cols in [80u16, 100, 120] {
            let out = compose_footer(&live, PREFLIGHT, None, TRANSFERS, cols);
            // Two live lines + panel, each in one physical row
            for row in &out {
                let w = bridge::ansi_rows(row, usize::MAX)[0].1;
                assert!(w <= cols as usize, "row {w} > {cols}: {row:?}");
            }
            // Two live lines = exactly two rows (clipped, not wrapped)
            let panel_rows = compose_panel(PREFLIGHT, None, TRANSFERS, cols).len();
            assert_eq!(
                out.len(),
                2 + panel_rows,
                "live lines wrapped into extra rows at cols={cols}"
            );
        }
    }
}
