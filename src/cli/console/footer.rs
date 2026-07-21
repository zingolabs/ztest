//! The pure sticky-footer renderer: given the lines to commit into scrollback and
//! the footer to repaint, produce the exact terminal byte sequence and the
//! footer's new height. No I/O; unit-tested against an in-memory terminal model.
//!
//! Completed lines are printed normally so the terminal scrolls them into its own
//! scrollback; only the live footer is repainted in place via cursor-up +
//! overwrite. A DECSTBM scroll region is never used — it discards lines scrolled
//! past an interior top margin instead of saving them.
//!
//! Invariants:
//! - The cursor rests at column 0 of the footer's last row on entry and exit
//!   (`prev_rows` is 0 on the first call, cursor on a fresh line).
//! - Every `footer` line is at most `cols` columns, so it occupies exactly one
//!   physical row (caller-guaranteed) — that keeps the `prev_rows` cursor
//!   arithmetic in logical == physical rows.

use std::fmt::Write as _;

/// Emit the frame that commits `committed` into scrollback above the footer and
/// repaints `footer` as the new pinned block. `prev_rows` is the value returned
/// by the previous call (0 initially). Returns the new footer height.
pub(super) fn render(
    out: &mut String,
    committed: &[String],
    footer: &[String],
    prev_rows: usize,
) -> usize {
    // Move to the top of the old footer. The cursor sits at its *last* row.
    if prev_rows > 1 {
        let _ = write!(out, "\x1b[{}A", prev_rows - 1);
    }
    if prev_rows > 0 {
        out.push('\r');
    }

    // Committed lines overwrite the old footer top and push it down with each
    // `\r\n`, becoming ordinary scrollback content. `\x1b[K` wipes any longer
    // old-footer tail.
    for line in committed {
        out.push_str(line);
        out.push_str("\x1b[K\r\n");
    }

    let f = footer.len();
    for (i, line) in footer.iter().enumerate() {
        out.push_str(line);
        out.push_str("\x1b[K");
        if i + 1 < f {
            out.push_str("\r\n");
        }
    }

    // Wipe leftover old rows below the cursor when the new block is shorter.
    if committed.len() + f < prev_rows {
        out.push_str("\x1b[J");
    }

    out.push('\r');
    f
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal VT good enough to prove the renderer: printable glyphs, `\r`,
    /// `\n`, `CUU` (`ESC[nA`), `EL` (`ESC[K`), `ED` (`ESC[J`), SGR (ignored).
    struct Term {
        cols: usize,
        rows: usize,
        grid: Vec<Vec<char>>,
        scrollback: Vec<String>,
        cx: usize,
        cy: usize,
    }

    impl Term {
        fn new(cols: usize, rows: usize) -> Term {
            Term {
                cols,
                rows,
                grid: vec![vec![' '; cols]; rows],
                scrollback: Vec::new(),
                cx: 0,
                cy: 0,
            }
        }

        fn scroll_up(&mut self) {
            let top = self.grid.remove(0);
            self.scrollback.push(trim(&top));
            self.grid.push(vec![' '; self.cols]);
        }

        fn newline(&mut self) {
            if self.cy + 1 == self.rows {
                self.scroll_up();
            } else {
                self.cy += 1;
            }
        }

        fn put(&mut self, c: char) {
            if self.cx < self.cols {
                self.grid[self.cy][self.cx] = c;
                self.cx += 1;
            }
        }

        fn apply(&mut self, s: &str) {
            let b: Vec<char> = s.chars().collect();
            let mut i = 0;
            while i < b.len() {
                match b[i] {
                    '\r' => {
                        self.cx = 0;
                        i += 1;
                    }
                    '\n' => {
                        self.newline();
                        i += 1;
                    }
                    '\x1b' if i + 1 < b.len() && b[i + 1] == '[' => {
                        // Parse CSI: optional '?', numeric params, final byte.
                        let mut j = i + 2;
                        let private = j < b.len() && b[j] == '?';
                        if private {
                            j += 1;
                        }
                        let start = j;
                        while j < b.len() && (b[j].is_ascii_digit() || b[j] == ';') {
                            j += 1;
                        }
                        let params: String = b[start..j].iter().collect();
                        let final_byte = if j < b.len() { b[j] } else { '\0' };
                        let n: usize = params.split(';').next().unwrap_or("").parse().unwrap_or(1);
                        if !private {
                            match final_byte {
                                'A' => self.cy = self.cy.saturating_sub(n.max(1)),
                                'K' => {
                                    for x in self.cx..self.cols {
                                        self.grid[self.cy][x] = ' ';
                                    }
                                }
                                'J' => {
                                    for x in self.cx..self.cols {
                                        self.grid[self.cy][x] = ' ';
                                    }
                                    for y in (self.cy + 1)..self.rows {
                                        self.grid[y] = vec![' '; self.cols];
                                    }
                                }
                                _ => {} // SGR ('m') and anything else: no geometry effect
                            }
                        }
                        i = j + 1;
                    }
                    c => {
                        self.put(c);
                        i += 1;
                    }
                }
            }
        }

        /// Visible non-blank rows, trimmed.
        fn visible(&self) -> Vec<String> {
            self.grid
                .iter()
                .map(|r| trim(r))
                .filter(|s| !s.is_empty())
                .collect()
        }
    }

    fn trim(row: &[char]) -> String {
        let s: String = row.iter().collect();
        s.trim_end().to_string()
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn first_frame_prints_committed_then_footer() {
        let mut t = Term::new(40, 10);
        let mut out = String::new();
        let rows = render(&mut out, &strings(&["A", "B"]), &strings(&["P1", "P2"]), 0);
        t.apply(&out);
        assert_eq!(rows, 2);
        assert_eq!(t.visible(), strings(&["A", "B", "P1", "P2"]));
    }

    #[test]
    fn committed_line_inserts_above_footer_preserving_order() {
        let mut t = Term::new(40, 10);
        let mut out = String::new();
        let mut rows = render(&mut out, &strings(&["A", "B"]), &strings(&["P1", "P2"]), 0);
        t.apply(&out);

        out.clear();
        rows = render(&mut out, &strings(&["C"]), &strings(&["P1'", "P2'"]), rows);
        t.apply(&out);

        assert_eq!(rows, 2);
        // C landed between the earlier committed lines and the footer.
        assert_eq!(t.visible(), strings(&["A", "B", "C", "P1'", "P2'"]));
    }

    #[test]
    fn shrinking_footer_clears_leftover_rows() {
        let mut t = Term::new(40, 10);
        let mut out = String::new();
        let mut rows = render(&mut out, &[], &strings(&["P1", "P2", "P3"]), 0);
        t.apply(&out);
        assert_eq!(rows, 3);

        out.clear();
        rows = render(&mut out, &[], &strings(&["ONLY"]), rows);
        t.apply(&out);
        assert_eq!(rows, 1);
        // The two extra old rows must be gone, not left as stale text.
        assert_eq!(t.visible(), strings(&["ONLY"]));
    }

    #[test]
    fn growing_footer_extends_downward() {
        let mut t = Term::new(40, 10);
        let mut out = String::new();
        let mut rows = render(&mut out, &[], &strings(&["ONE"]), 0);
        t.apply(&out);

        out.clear();
        rows = render(&mut out, &[], &strings(&["A", "B", "C"]), rows);
        t.apply(&out);
        assert_eq!(rows, 3);
        assert_eq!(t.visible(), strings(&["A", "B", "C"]));
    }

    #[test]
    fn shorter_footer_line_overwrites_old_longer_line() {
        let mut t = Term::new(40, 10);
        let mut out = String::new();
        let mut rows = render(&mut out, &[], &strings(&["LONGLINE"]), 0);
        t.apply(&out);

        out.clear();
        rows = render(&mut out, &[], &strings(&["hi"]), rows);
        t.apply(&out);
        assert_eq!(rows, 1);
        // No "LONGLINE" tail remains after "hi".
        assert_eq!(t.visible(), strings(&["hi"]));
    }

    #[test]
    fn sgr_in_footer_does_not_shift_columns() {
        let mut t = Term::new(40, 10);
        let mut out = String::new();
        // A styled line: the SGR codes must not count as visible columns.
        render(&mut out, &[], &strings(&["\x1b[1;32mOK\x1b[0m"]), 0);
        t.apply(&out);
        assert_eq!(t.visible(), strings(&["OK"]));
    }

    #[test]
    fn many_committed_lines_scroll_into_scrollback() {
        let mut t = Term::new(40, 4); // short screen so it must scroll
        let mut out = String::new();
        let mut rows = render(&mut out, &[], &strings(&["panel"]), 0);
        t.apply(&out);
        for i in 0..10 {
            out.clear();
            rows = render(&mut out, &strings(&[&format!("line{i}")]), &strings(&["panel"]), rows);
            t.apply(&out);
        }
        // Everything committed is preserved: earliest in scrollback, panel pinned.
        let all: Vec<String> = t.scrollback.iter().cloned().chain(t.visible()).collect();
        let expected: Vec<String> = (0..10)
            .map(|i| format!("line{i}"))
            .chain(std::iter::once("panel".to_string()))
            .collect();
        assert_eq!(all, expected);
    }

    #[test]
    fn empty_footer_after_content_clears_the_region() {
        let mut t = Term::new(40, 10);
        let mut out = String::new();
        let mut rows = render(&mut out, &[], &strings(&["P1", "P2"]), 0);
        t.apply(&out);

        out.clear();
        rows = render(&mut out, &[], &[], rows);
        t.apply(&out);
        assert_eq!(rows, 0);
        assert_eq!(t.visible(), Vec::<String>::new());
    }
}
