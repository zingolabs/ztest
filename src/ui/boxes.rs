//! Titled boxes and column composition, shared by every boxed surface.
//!
//! - Width is enforced here, never trusted from callers (one over-long line wraps and
//!   shears every box below it)
//! - Measured in display columns: frame glyphs and the title dot are multibyte, and
//!   byte-measuring shortens the rule per glyph

use owo_colors::OwoColorize as _;

use super::Theme;

/// Content width inside a `width`-column box: less two borders, two pad spaces
pub(super) fn interior(width: usize) -> usize {
    width.saturating_sub(4)
}

/// Frame `body` in a titled box of exactly `width` columns
pub(super) fn boxed(
    title: &str,
    right: &str,
    body: &[String],
    width: usize,
    theme: &Theme,
) -> Vec<String> {
    let [tl, tr, bl, br, h, v] = theme.chars.frame;
    let inner = interior(width);
    let dim = theme.styles.dim;

    let head = format!("{h} {title} ");
    let tail = match right.is_empty() {
        true => String::new(),
        false => format!(" {right} "),
    };
    let rule = width.saturating_sub(2 + display_width(&head) + display_width(&tail));

    let mut out = vec![format!(
        "{}{}{}{}{}",
        tl.style(dim),
        head.style(theme.styles.count),
        h.repeat(rule).style(dim),
        tail.style(dim),
        tr.style(dim),
    )];
    for line in body {
        let shown = truncate(line, inner);
        let pad = inner.saturating_sub(display_width(&shown));
        out.push(format!("{} {shown}{:pad$} {}", v.style(dim), "", v.style(dim)));
    }
    out.push(format!(
        "{}{}{}",
        bl.style(dim),
        h.repeat(width.saturating_sub(2)).style(dim),
        br.style(dim)
    ));
    out
}

/// Two boxes side by side, shorter padded with blank lines so both start level
pub(super) fn beside(out: &mut String, left: &[String], right: &[String], left_w: usize) {
    use std::fmt::Write as _;
    let rows = left.len().max(right.len());
    for i in 0..rows {
        let blank = String::new();
        let l = left.get(i).unwrap_or(&blank);
        let r = right.get(i).unwrap_or(&blank);
        let pad = left_w.saturating_sub(display_width(l));
        let line = format!("{l}{:pad$} {r}", "");
        let _ = writeln!(out, "{}", line.trim_end());
    }
}

/// `label   value` row, right-aligned to the box interior
pub(super) fn row(label: &str, value: &str, inner: usize, theme: &Theme) -> String {
    let pad = inner.saturating_sub(display_width(label) + display_width(value));
    format!("{}{:pad$}{}", label.style(theme.styles.dim), "", value.style(theme.styles.count))
}

pub(super) fn dim(text: &str, theme: &Theme) -> String {
    text.style(theme.styles.dim).to_string()
}

/// Printable width, ANSI escapes ignored (padding against byte length leaves every
/// coloured box ragged)
pub(super) fn display_width(s: &str) -> usize {
    console::measure_text_width(s)
}

/// Cut `s` to `width` printable columns, escapes preserved
pub(super) fn truncate(s: &str, width: usize) -> String {
    match display_width(s) <= width {
        true => s.to_string(),
        false => console::truncate_str(s, width, "").into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    /// Misalignment within a column shears the layout
    #[test]
    fn every_box_in_a_column_is_the_same_width() {
        let widths: Vec<usize> = boxed("t", "", &["a".into(), "bb".into()], 40, &theme())
            .iter()
            .map(|l| display_width(l))
            .collect();
        assert!(widths.iter().all(|&w| w == 40), "{widths:?}");
    }

    /// Over-long body line truncates; it never breaks the frame
    #[test]
    fn an_over_long_body_line_is_cut_to_fit() {
        let long = "x".repeat(200);
        for line in boxed("t", "", &[long], 40, &theme()) {
            assert_eq!(display_width(&line), 40);
        }
    }

    /// A short box beside a tall one must not drag the tall one's rows leftward
    #[test]
    fn beside_pads_the_shorter_column_to_keep_the_taller_one_aligned() {
        let mut out = String::new();
        beside(&mut out, &["aa".into()], &["r1".into(), "r2".into()], 10);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[1].starts_with(&" ".repeat(11)), "{:?}", lines[1]);
    }
}
