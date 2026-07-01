//! Pure conversion from [`avt`]'s emulated terminal cells to [`ratatui`]
//! styled text.
//!
//! The unified run console feeds every subprocess's output through an `avt`
//! virtual terminal, then renders the result two ways:
//!
//! - the live visible grid (`Vt::view`) into the inline viewport, and
//! - lines that scroll off the top (`Changes::scrollback`) forwarded into the
//!   terminal's native scrollback via `ratatui`'s `insert_before`.
//!
//! Both paths map avt cells (a glyph plus a [`Pen`]: fg/bg colour and SGR
//! attributes) onto a `ratatui` [`Style`]/[`Span`]/[`Line`]. That mapping is
//! the whole module: pure, no I/O, unit-tested against a real `avt` parser so
//! the colour/attribute/wide-char handling can't silently drift.

use avt::{Color as AvtColor, Line as AvtLine, Pen, Vt};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};

/// Map an avt colour to its `ratatui` equivalent. avt carries either a 256-
/// colour palette index or a 24-bit RGB triple; both have direct `ratatui`
/// counterparts.
fn color(c: AvtColor) -> Color {
    match c {
        AvtColor::Indexed(i) => Color::Indexed(i),
        AvtColor::RGB(rgb) => Color::Rgb(rgb.r, rgb.g, rgb.b),
    }
}

/// Map an avt cell's [`Pen`] (foreground/background + SGR attributes) onto a
/// `ratatui` [`Style`]. An unset fg/bg leaves the terminal default (we don't
/// force a colour), and each SGR flag maps to the matching `ratatui` modifier.
pub(crate) fn pen_style(pen: &Pen) -> Style {
    let mut style = Style::default();
    if let Some(fg) = pen.foreground() {
        style = style.fg(color(fg));
    }
    if let Some(bg) = pen.background() {
        style = style.bg(color(bg));
    }
    let mut m = Modifier::empty();
    if pen.is_bold() {
        m |= Modifier::BOLD;
    }
    if pen.is_faint() {
        m |= Modifier::DIM;
    }
    if pen.is_italic() {
        m |= Modifier::ITALIC;
    }
    if pen.is_underline() {
        m |= Modifier::UNDERLINED;
    }
    if pen.is_strikethrough() {
        m |= Modifier::CROSSED_OUT;
    }
    if pen.is_blink() {
        m |= Modifier::SLOW_BLINK;
    }
    if pen.is_inverse() {
        m |= Modifier::REVERSED;
    }
    style.add_modifier(m)
}

/// Convert one emulated terminal line into an owned `ratatui` [`Line`].
///
/// For fidelity and cost:
/// - Runs are coalesced: consecutive cells sharing a pen become a single
///   [`Span`], so a 200-column line of one colour is one span, not 200.
/// - Trailing blanks are trimmed: avt pads every line to full width with
///   default cells, and forwarding those into scrollback would tack ~180 spaces
///   onto a short `Compiling foo` line. We cut at the last non-default cell. A
///   cell with a non-default background (e.g. a highlighted row) is not
///   "default", so genuine trailing colour is preserved.
/// - Wide-char tails (width 0) are skipped: the head cell already carries the
///   full glyph; the tail is a placeholder.
pub(crate) fn avt_line(line: &AvtLine) -> Line<'static> {
    let cells = line.cells();
    let end = cells
        .iter()
        .rposition(|c| !c.is_default())
        .map_or(0, |i| i + 1);

    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut buf = String::new();
    let mut cur: Option<Style> = None;
    for cell in &cells[..end] {
        if cell.width() == 0 {
            continue; // wide-char tail: glyph already emitted by the head cell
        }
        let style = pen_style(cell.pen());
        match cur {
            Some(s) if s == style => buf.push(cell.char()),
            _ => {
                if let Some(s) = cur.take() {
                    spans.push(Span::styled(std::mem::take(&mut buf), s));
                }
                cur = Some(style);
                buf.push(cell.char());
            }
        }
    }
    if let Some(s) = cur {
        spans.push(Span::styled(buf, s));
    }
    Line::from(spans)
}

/// Render an owo-colors/ANSI string (e.g. the QoS status panel produced by
/// [`crate::preflight::render_live_panel`]) into `ratatui` styled [`Text`] by
/// replaying it through a throwaway `avt` screen of the given size and
/// converting each visible row with [`avt_line`].
///
/// This routes the panel through the same cell-to-span mapping the live child
/// grid uses, so the two halves of the viewport can't drift in colour/attribute
/// handling. The panel is generated with bare `\n` line breaks (`writeln!`), but
/// `avt` is a raw VT: a lone line-feed moves down a row without returning to
/// column 0, so we normalise `\n` to `\r\n` first, exactly as a terminal's
/// `ONLCR` discipline would for a child's output.
pub(crate) fn text_from_ansi(s: &str, cols: usize, rows: usize) -> Text<'static> {
    let mut vt = Vt::new(cols.max(1), rows.max(1));
    vt.feed_str(&s.replace('\n', "\r\n"));
    let lines: Vec<Line<'static>> = vt.view().map(avt_line).collect();
    Text::from(lines)
}

#[cfg(test)]
mod tests {
    use super::*;
    use avt::Vt;

    /// Feed bytes to a fresh emulator and return its single visible line,
    /// converted. `cols`/`rows` size the virtual screen.
    fn line_of(cols: usize, input: &str) -> Line<'static> {
        let mut vt = Vt::new(cols, 1);
        vt.feed_str(input);
        let row = vt.view().next().expect("one row").clone();
        avt_line(&row)
    }

    #[test]
    fn plain_text_is_one_span_with_no_style_and_trimmed() {
        let line = line_of(40, "Compiling ztest");
        assert_eq!(line.spans.len(), 1, "one run: {line:?}");
        assert_eq!(line.spans[0].content, "Compiling ztest");
        // Trailing padding to 40 cols is trimmed away, not carried as spaces.
        assert_eq!(line.spans[0].style, Style::default());
    }

    #[test]
    fn sgr_bold_red_maps_to_modifier_and_indexed_color() {
        // ESC[1;31m = bold + red(=indexed 1); then reset.
        let line = line_of(20, "\x1b[1;31mERR\x1b[0m");
        let span = &line.spans[0];
        assert_eq!(span.content, "ERR");
        assert!(span.style.add_modifier.contains(Modifier::BOLD), "{span:?}");
        assert_eq!(span.style.fg, Some(Color::Indexed(1)));
    }

    #[test]
    fn style_changes_split_into_separate_spans() {
        // "ok" default, then green "go".
        let line = line_of(20, "ok\x1b[32mgo\x1b[0m");
        assert_eq!(line.spans.len(), 2, "two runs: {line:?}");
        assert_eq!(line.spans[0].content, "ok");
        assert_eq!(line.spans[0].style.fg, None);
        assert_eq!(line.spans[1].content, "go");
        assert_eq!(line.spans[1].style.fg, Some(Color::Indexed(2)));
    }

    #[test]
    fn truecolor_maps_to_rgb() {
        // ESC[38;2;10;20;30m = 24-bit fg.
        let line = line_of(10, "\x1b[38;2;10;20;30mx\x1b[0m");
        assert_eq!(line.spans[0].style.fg, Some(Color::Rgb(10, 20, 30)));
    }

    #[test]
    fn blank_line_yields_no_spans() {
        let line = line_of(20, "");
        assert!(line.spans.is_empty(), "empty: {line:?}");
    }

    #[test]
    fn text_from_ansi_splits_rows_on_newline_without_column_drift() {
        // Two `writeln!`-style rows (bare `\n`): the second must start at
        // column 0, not be shifted right by the first row's width.
        let text = text_from_ansi("alpha\n\x1b[32mbeta\x1b[0m", 20, 3);
        assert_eq!(text.lines.len(), 3, "padded to `rows`: {text:?}");
        assert_eq!(text.lines[0].spans[0].content, "alpha");
        assert_eq!(text.lines[1].spans[0].content, "beta");
        assert_eq!(text.lines[1].spans[0].style.fg, Some(Color::Indexed(2)));
        assert!(text.lines[2].spans.is_empty(), "trailing blank row");
    }
}
