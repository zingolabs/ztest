//! Pure sticky-footer renderer: committed lines + footer in → terminal bytes +
//! new footer height out. No I/O; tested against an in-memory terminal model.
//!
//! - Committed lines printed normally (terminal owns the scrollback); only the
//!   footer repaints in place via cursor-up + overwrite
//! - Never DECSTBM (discards lines scrolled past an interior top margin)
//!
//! Invariants:
//! - Cursor at column 0 of the footer's last row on entry & exit (`prev_rows` = 0
//!   on the first call, cursor on a fresh line)
//! - Every `footer` line ≤ `cols` (caller-guaranteed) → `prev_rows` arithmetic
//!   stays logical == physical rows

use std::fmt::Write as _;

/// Commit `committed` into scrollback above the footer, repaint `footer` as the
/// pinned block. `prev_rows` = previous call's return (0 initially)
pub(super) fn render(
    out: &mut String,
    committed: &[String],
    footer: &[String],
    prev_rows: usize,
) -> usize {
    // To the top of the old footer (cursor sits at its *last* row)
    if prev_rows > 1 {
        let _ = write!(out, "\x1b[{}A", prev_rows - 1);
    }
    if prev_rows > 0 {
        out.push('\r');
    }

    // Each `\r\n` pushes the old footer down; committed text becomes scrollback.
    // `\x1b[K` wipes any longer old-footer tail
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

    // New block shorter → wipe the leftover rows below
    if committed.len() + f < prev_rows {
        out.push_str("\x1b[J");
    }

    out.push('\r');
    f
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal VT: printable glyphs, `\r`, `\n`, CUU, EL, ED, SGR (ignored)
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
                        // CSI: optional '?', numeric params, final byte
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

        fn visible(&self) -> Vec<String> {
            self.grid.iter().map(|r| trim(r)).filter(|s| !s.is_empty()).collect()
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
        // C lands between the earlier committed lines and the footer
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
        // Two extra old rows gone, not left as stale text
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
        // No "LONGLINE" tail after "hi"
        assert_eq!(t.visible(), strings(&["hi"]));
    }

    #[test]
    fn sgr_in_footer_does_not_shift_columns() {
        let mut t = Term::new(40, 10);
        let mut out = String::new();
        // SGR codes must not count as visible columns
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
        // All committed preserved: earliest in scrollback, panel pinned
        let all: Vec<String> = t.scrollback.iter().cloned().chain(t.visible()).collect();
        let expected: Vec<String> = (0..10)
            .map(|i| format!("line{i}"))
            .chain(std::iter::once("panel".to_string()))
            .collect();
        assert_eq!(all, expected);
    }

    #[test]
    fn live_running_lines_never_leak_into_scrollback() {
        // Run-panel shape: variable-height live "running" block over a fixed 5-line
        // panel. Only committed verdicts may reach scrollback, never a running line
        let panel = ["p1", "p2", "p3", "p4", "p5"].map(String::from).to_vec();
        let footer = |running: &[&str]| {
            let mut f: Vec<String> = running.iter().map(|s| s.to_string()).collect();
            f.extend(panel.clone());
            f
        };
        let mut t = Term::new(40, 10); // footer (2 running + 5 panel = 7) nearly fills 10 rows
        let mut out = String::new();
        let mut rows = render(&mut out, &[], &footer(&["run_A 1s", "run_B 1s"]), 0);
        t.apply(&out);

        // Frames: timer ticks, verdicts committing, running set churn
        let frames: &[(&[&str], &[&str])] = &[
            (&[], &["run_A 2s", "run_B 2s"]),             // tick
            (&["PASS run_B"], &["run_A 3s", "run_C 1s"]), // B done, C starts
            (&[], &["run_A 4s", "run_C 2s"]),             // tick
            (&["PASS run_C"], &["run_A 5s", "run_D 1s"]), // C done, D starts
            (&["PASS run_A"], &["run_D 2s"]),             // A done, footer shrinks
            (&["PASS run_D"], &[]),                       // D done, footer = panel only
        ];
        for (committed, running) in frames {
            out.clear();
            let c: Vec<String> = committed.iter().map(|s| s.to_string()).collect();
            rows = render(&mut out, &c, &footer(running), rows);
            t.apply(&out);
        }
        let _ = rows;

        // Everything that scrolled off + what is still visible
        let seen: Vec<String> = t.scrollback.iter().cloned().chain(t.visible()).collect();
        for line in &seen {
            assert!(
                !line.starts_with("run_"),
                "live running line leaked into output: {line:?}\nall:\n{}",
                seen.join("\n")
            );
        }
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
