//! Pure conversion from [`avt`]'s emulated terminal cells to ANSI strings.
//!
//! The console feeds every subprocess's output through an `avt` virtual terminal,
//! then serializes the result back to ANSI for direct terminal output: lines that
//! scroll off the top (`Changes::scrollback`) become committed scrollback, and the
//! live grid (`Vt::view`) becomes the footer's live rows. This module is that
//! serialization — pure, no I/O, unit-tested against a real `avt` parser (round-
//! tripping through the emulator) so colour/attribute/wide-char handling can't
//! silently drift.

use avt::{Color as AvtColor, Line as AvtLine, Pen, Vt};

/// The SGR parameter list for a [`Pen`] (e.g. `"1;38;5;1"` for bold + red), or an
/// empty string for the terminal default. Colours use the always-valid extended
/// forms (`38;5;i` / `38;2;r;g;b`); attributes precede them.
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
        p.push(match fg {
            AvtColor::Indexed(i) => format!("38;5;{i}"),
            AvtColor::RGB(c) => format!("38;2;{};{};{}", c.r, c.g, c.b),
        });
    }
    if let Some(bg) = pen.background() {
        p.push(match bg {
            AvtColor::Indexed(i) => format!("48;5;{i}"),
            AvtColor::RGB(c) => format!("48;2;{};{};{}", c.r, c.g, c.b),
        });
    }
    p.join(";")
}

/// Convert one emulated terminal line into a self-contained ANSI string, clipped
/// to at most `max_cols` display columns. Returns the string and the display
/// width it uses (for side-by-side padding in the two-column panel).
///
/// For fidelity and cost:
/// - Runs are coalesced: consecutive cells sharing a pen become one SGR span, so
///   a 200-column line of one colour is one span, not 200.
/// - Trailing blanks are trimmed: avt pads every line to full width with default
///   cells; we cut at the last non-default cell (a non-default background is
///   preserved).
/// - Wide-char tails (width 0) are skipped; a wide glyph that would straddle the
///   clip edge is dropped.
/// - Each style change resets first (`ESC[0;…m`) so no attribute leaks from the
///   previous run, and any trailing style is reset at line end so the line can be
///   concatenated beside another without bleeding.
pub(crate) fn avt_line_ansi_clipped(line: &AvtLine, max_cols: usize) -> (String, usize) {
    let cells = line.cells();
    let end = cells
        .iter()
        .rposition(|c| !c.is_default())
        .map_or(0, |i| i + 1);

    let mut out = String::new();
    let mut cur = String::new(); // SGR params currently in effect ("" = default)
    let mut used = 0usize;
    for cell in &cells[..end] {
        let w = cell.width() as usize;
        if w == 0 {
            continue; // wide-char tail
        }
        if used + w > max_cols {
            break;
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
    (out, used)
}

/// Unclipped [`avt_line_ansi_clipped`] — the whole line as an ANSI string.
pub(crate) fn avt_line_to_ansi(line: &AvtLine) -> String {
    avt_line_ansi_clipped(line, usize::MAX).0
}

/// Replay an owo-colors/ANSI string through a wide (non-wrapping) emulator and
/// return each logical row clipped to `width` display columns, as
/// `(ansi, display_width)`. Used to lay the two panel columns side by side: a
/// line longer than its column is clipped (keeping the labelled head, dropping
/// trailing detail) rather than wrapped, so it stays one physical row.
///
/// The panel is generated with bare `\n` line breaks (`writeln!`), but `avt` is a
/// raw VT: a lone line-feed moves down without returning to column 0, so we
/// normalise `\n` to `\r\n` first, exactly as a terminal's `ONLCR` discipline
/// would for a child's output.
pub(crate) fn ansi_rows(s: &str, width: usize) -> Vec<(String, usize)> {
    const NOWRAP: usize = 512;
    let s = s.trim_end_matches('\n');
    let h = s.lines().count().max(1);
    let mut vt = Vt::new(NOWRAP, h);
    vt.feed_str(&s.replace('\n', "\r\n"));
    vt.view()
        .map(|row| avt_line_ansi_clipped(&row, width))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// avt line → ANSI → avt line must preserve the serialized form (a stable
    /// round-trip proves text + per-cell style survive the emulator).
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
        assert!(ansi.contains("38;5;2"), "green as extended fg: {ansi:?}");
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
        // Two `writeln!`-style rows (bare `\n`): the second must start at column 0,
        // not be shifted right by the first row's width.
        let rows = ansi_rows("alpha\n\x1b[32mbeta\x1b[0m", 20);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "alpha");
        assert!(rows[1].0.contains("beta"), "row 1: {:?}", rows[1].0);
    }

    #[test]
    fn blank_line_is_empty() {
        assert_eq!(avt_line_to_ansi(&line_of(20, "")), "");
    }
}
