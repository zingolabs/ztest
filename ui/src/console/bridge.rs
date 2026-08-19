//! Pure [`avt`] terminal cells → ANSI strings. No I/O; round-tripped through a real `avt`
//! parser so colour/attribute/wide-char handling cannot silently drift.

use avt::{Color as AvtColor, Line as AvtLine, Pen, Vt};

/// Emulator width for every grid whose rows are clipped at present time, never wrapped.
///
/// - Child output must reach native scrollback as the child laid it out (a terminal-width
///   grid re-wraps a long line into two, baking a newline mid-token)
/// - Ceiling, not a reservation — `avt` allocates per row, and >512-col lines are pathological
pub(crate) const NOWRAP_COLS: usize = 512;

/// SGR params for a [`Pen`] (`"1;31"` = bold + red); empty = terminal default
fn sgr_params(pen: &Pen) -> String {
    let mut p: Vec<String> = Vec::new();
    if pen.is_bold() {
        p.push("1".into());
    }
    if pen.is_faint() {
        p.push("2".into());
    }
    if pen.is_italic() {
        p.push("3".into());
    }
    if pen.is_underline() {
        p.push("4".into());
    }
    if pen.is_blink() {
        p.push("5".into());
    }
    if pen.is_inverse() {
        p.push("7".into());
    }
    if pen.is_strikethrough() {
        p.push("9".into());
    }
    if let Some(fg) = pen.foreground() {
        p.push(color_params(fg, 30));
    }
    if let Some(bg) = pen.background() {
        p.push(color_params(bg, 40));
    }
    p.join(";")
}

/// One colour → SGR params. `base` = 30 (foreground) or 40 (background).
///
/// - Indices 0-15 take their canonical short codes (`31`, `91`), the spelling every
///   producer emits and the only one a 16-colour terminal understands
/// - `38;5;n` reserved for the 216-cube + greyscale, where no short code exists
fn color_params(c: AvtColor, base: u8) -> String {
    match c {
        AvtColor::Indexed(i @ 0..=7) => format!("{}", base + i),
        AvtColor::Indexed(i @ 8..=15) => format!("{}", base + 60 + (i - 8)),
        AvtColor::Indexed(i) => format!("{};5;{i}", base + 8),
        AvtColor::RGB(c) => format!("{};2;{};{};{}", base + 8, c.r, c.g, c.b),
    }
}

/// One emulated line → self-contained ANSI rows of at most `width` display columns each.
///
/// - Same-pen runs coalesce into one SGR span, trailing default cells trimmed
/// - Every style change & every row end reset first (nothing bleeds across concatenation)
/// - Wide char never straddles a row edge — it opens the next one
pub(crate) fn avt_line_ansi_rows(line: &AvtLine, width: usize) -> Vec<(String, usize)> {
    let width = width.max(1);
    let cells = line.cells();
    let end = cells.iter().rposition(|c| !c.is_default()).map_or(0, |i| i + 1);

    let mut rows = Vec::new();
    let mut out = String::new();
    let mut cur = String::new(); // SGR params currently in effect ("" = default)
    let mut used = 0usize;
    for cell in &cells[..end] {
        let w = cell.width() as usize;
        if w == 0 {
            continue; // wide-char tail
        }
        if used + w > width {
            if !cur.is_empty() {
                out.push_str("\x1b[0m");
            }
            rows.push((std::mem::take(&mut out), used));
            cur.clear();
            used = 0;
        }
        let params = sgr_params(cell.pen());
        if params != cur {
            if params.is_empty() {
                out.push_str("\x1b[0m");
            } else {
                out.push_str("\x1b[0;");
                out.push_str(&params);
                out.push('m');
            }
            cur = params;
        }
        out.push(cell.char());
        used += w;
    }
    if !cur.is_empty() {
        out.push_str("\x1b[0m");
    }
    rows.push((out, used));
    rows
}

/// First [`avt_line_ansi_rows`] row: the line truncated at `max_cols`. `(ansi, width)`,
/// the width feeding side-by-side padding
pub(crate) fn avt_line_ansi_clipped(line: &AvtLine, max_cols: usize) -> (String, usize) {
    avt_line_ansi_rows(line, max_cols).swap_remove(0)
}

/// Unclipped [`avt_line_ansi_clipped`]
pub(crate) fn avt_line_to_ansi(line: &AvtLine) -> String {
    avt_line_ansi_clipped(line, usize::MAX).0
}

/// Replay ANSI through a wide (non-wrapping) emulator → one entry per logical row,
/// truncated to `width`. Panel columns sit side by side, so an over-wide one must not
/// steal rows from its neighbours
pub(crate) fn ansi_rows(s: &str, width: usize) -> Vec<(String, usize)> {
    replay(s).iter().map(|l| avt_line_ansi_clipped(l, width)).collect()
}

/// `\n` normalised to `\r\n` first (`avt` is a raw VT: lone LF moves down, keeps the column)
fn replay(s: &str) -> Vec<AvtLine> {
    let s = s.trim_end_matches('\n');
    let h = s.lines().count().max(1);
    let mut vt = Vt::new(NOWRAP_COLS, h);
    vt.feed_str(&s.replace('\n', "\r\n"));
    vt.view().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(cols: usize, input: &str) {
        let mut vt = Vt::new(cols, 1);
        vt.feed_str(input);
        let orig = avt_line_to_ansi(&vt.view().next().expect("one row").clone());

        let mut vt2 = Vt::new(cols, 1);
        vt2.feed_str(&orig);
        let round = avt_line_to_ansi(&vt2.view().next().expect("one row").clone());

        assert_eq!(orig, round, "roundtrip drifted for {input:?}");
    }

    #[test]
    fn ansi_roundtrips_preserve_text_and_style() {
        roundtrip(40, "Compiling ztest");
        roundtrip(40, "\x1b[1;31mERR\x1b[0m ok");
        roundtrip(40, "ok\x1b[32mgo\x1b[0m more");
        roundtrip(40, "\x1b[38;2;10;20;30mtrue\x1b[0mcolor");
        roundtrip(40, "\x1b[1mbold\x1b[0m \x1b[4munder\x1b[0m \x1b[7mrev\x1b[0m");
    }

    fn line_of(cols: usize, input: &str) -> AvtLine {
        let mut vt = Vt::new(cols, 1);
        vt.feed_str(input);
        vt.view().next().unwrap().clone()
    }

    #[test]
    fn plain_ansi_has_no_escape_codes() {
        assert_eq!(avt_line_to_ansi(&line_of(40, "plain text")), "plain text");
    }

    #[test]
    fn styled_ansi_resets_at_end_for_safe_concatenation() {
        let ansi = avt_line_to_ansi(&line_of(40, "\x1b[32mgreen\x1b[0m"));
        assert!(ansi.ends_with("\x1b[0m"), "must reset trailing style: {ansi:?}");
        assert!(ansi.contains("32"), "green keeps its short code: {ansi:?}");
    }

    #[test]
    fn clipped_ansi_stops_at_max_cols() {
        let (s, used) = avt_line_ansi_clipped(&line_of(40, "abcdefghij"), 4);
        assert_eq!(s, "abcd");
        assert_eq!(used, 4);
    }

    #[test]
    fn ansi_rows_clips_each_row_and_reports_width() {
        let rows = ansi_rows("short\nthis-one-is-long", 6);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], ("short".to_string(), 5));
        assert_eq!(rows[1].1, 6, "long row clipped to width");
        assert_eq!(rows[1].0, "this-o");
    }

    #[test]
    fn ansi_rows_splits_on_newline_without_column_drift() {
        // Bare-`\n` rows: row 1 must start at column 0, not shifted right
        let rows = ansi_rows("alpha\n\x1b[32mbeta\x1b[0m", 20);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "alpha");
        assert!(rows[1].0.contains("beta"), "row 1: {:?}", rows[1].0);
    }

    #[test]
    fn wrapped_rows_split_at_width_and_restyle_each_row() {
        // Each row self-contained: row 1 must re-open the pen, not inherit row 0's
        let rows = avt_line_ansi_rows(&line_of(40, "\x1b[31mabcdef\x1b[0m"), 4);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], ("\x1b[0;31mabcd\x1b[0m".to_string(), 4));
        assert_eq!(rows[1], ("\x1b[0;31mef\x1b[0m".to_string(), 2));
    }

    #[test]
    fn clipping_takes_only_the_first_wrapped_row() {
        // `ansi_rows` truncates where `ansi_rows_wrapped` continues (panel columns vs live)
        assert_eq!(ansi_rows("abcdefghij", 4), vec![("abcd".to_string(), 4)]);
        assert_eq!(avt_line_ansi_rows(&line_of(40, "abcdefghij"), 4).len(), 3);
    }

    #[test]
    fn wide_char_moves_to_the_next_row_rather_than_splitting() {
        // U+4E2D is 2 cells wide; at width 3 it cannot share row 0 with "ab"
        let rows = avt_line_ansi_rows(&line_of(40, "ab\u{4e2d}"), 3);
        assert_eq!(rows[0].0, "ab");
        assert_eq!(rows[1].0, "\u{4e2d}");
        assert_eq!(rows[1].1, 2);
    }

    #[test]
    fn standard_colors_keep_their_short_codes() {
        // `38;5;n` is invisible to a 16-colour terminal; the short code is what producers
        // (cargo, nextest) emit and what must survive the round trip
        assert!(avt_line_to_ansi(&line_of(40, "\x1b[31mred\x1b[0m")).starts_with("\x1b[0;31m"));
        assert!(avt_line_to_ansi(&line_of(40, "\x1b[91mbright\x1b[0m")).starts_with("\x1b[0;91m"));
        assert!(avt_line_to_ansi(&line_of(40, "\x1b[44mbg\x1b[0m")).starts_with("\x1b[0;44m"));
        assert!(avt_line_to_ansi(&line_of(40, "\x1b[102mbg\x1b[0m")).starts_with("\x1b[0;102m"));
    }

    #[test]
    fn cube_colors_stay_extended() {
        // No short code exists above 15 — the 256-colour form is the only spelling
        let ansi = avt_line_to_ansi(&line_of(40, "\x1b[38;5;208morange\x1b[0m"));
        assert!(ansi.starts_with("\x1b[0;38;5;208m"), "{ansi:?}");
    }

    #[test]
    fn blank_line_is_empty() {
        assert_eq!(avt_line_to_ansi(&line_of(20, "")), "");
    }
}
