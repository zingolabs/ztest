//! [`Cell`]s + a [`Row`] -> a ratatui [`Line`].
//!
//! - Every getter returns `Option`: `None` = unmeasured, and a group holding one
//!   collapses (the `absent != zero` rule, held in one place instead of ~30)
//! - Widths resolve in two passes (fixed first, `*` takes the remainder) so one
//!   template serves a 60-column pipe and a 200-column panel

use std::borrow::Cow;
use std::time::Duration;

use ratatui::style::Style;
use ratatui::text::{Line, Span};

use super::{Align, Cell, Tone, Width};
use crate::Theme;

/// What a surface must answer to be drawn by a [`Template`](super::Template).
///
/// All defaults return `None` so a row implements only the kinds it actually has, and
/// asking for a key it never carries reads as unmeasured rather than panicking
pub trait Row {
    fn text(&self, _key: &str) -> Option<Cow<'_, str>> {
        None
    }
    fn value(&self, _key: &str) -> Option<f64> {
        None
    }
    /// `(done, total)` for a `{key%}` pair
    fn pair(&self, _key: &str) -> Option<(u64, u64)> {
        None
    }
    /// `0..=100` for a `{key:N#}` meter
    fn percent(&self, _key: &str) -> Option<u8> {
        None
    }
    /// Per-bucket `(min, max)` bands for a `{key:N~}` sparkline; `None` entries = gaps
    fn bands(&self, _key: &str) -> Option<Vec<crate::plot::Band>> {
        None
    }
}

pub(super) fn render(
    cells: &[Cell],
    row: &dyn Row,
    width: usize,
    elapsed: Duration,
    theme: &Theme,
) -> Line<'static> {
    // Star cells get whatever the fixed ones leave; measured against the *live* row so a
    // collapsed group hands its space back rather than stranding it
    let fixed = measure(cells, row, elapsed, theme, None);
    let star = width.saturating_sub(fixed);
    let mut spans = Vec::new();
    emit(cells, row, elapsed, theme, Some(star), &mut spans);
    Line::from(spans)
}

/// Width of everything except `*` cells
fn measure(
    cells: &[Cell],
    row: &dyn Row,
    elapsed: Duration,
    theme: &Theme,
    star: Option<usize>,
) -> usize {
    let mut spans = Vec::new();
    emit(cells, row, elapsed, theme, star, &mut spans);
    spans.iter().map(|s| crate::layout::display_width(&s.content)).sum()
}

/// `star` = resolved width for `*` cells; `None` while measuring them as zero
fn emit(
    cells: &[Cell],
    row: &dyn Row,
    elapsed: Duration,
    theme: &Theme,
    star: Option<usize>,
    out: &mut Vec<Span<'static>>,
) {
    for cell in cells {
        match cell {
            Cell::Lit(s) => out.push(Span::raw(s.clone())),
            Cell::Group(inner) => {
                if group_is_present(inner, row) {
                    emit(inner, row, elapsed, theme, star, out);
                }
            }
            Cell::Glyph { name, tone } => {
                // `parse` rejects an unknown name against this same table, so the `None`
                // arm is unreachable rather than a silent substitution
                if let Some(g) = glyph_of(name, &theme.chars) {
                    out.push(styled(g.to_string(), *tone, theme));
                }
            }
            Cell::Spinner { tone } => {
                out.push(styled(
                    crate::layout::spinner_glyph(elapsed, theme).to_string(),
                    *tone,
                    theme,
                ));
            }
            Cell::Text { key, width, align, tone } => {
                if let Some(v) = row.text(key) {
                    out.push(styled(fit(&v, *width, *align, star), *tone, theme));
                }
            }
            Cell::Value { key, width, align, unit, prec, thousands, tone } => {
                // Tone and unit names share a namespace, so `{k|count}` reads as `Unit::Count`
                // and yields a Value cell; a row binding it as text then renders nothing
                debug_assert!(
                    !(row.value(key).is_none() && row.text(key).is_some()),
                    "template key `{key}` is a value cell but the row bound it as text \
                     (option name collides with a unit?)"
                );
                if let Some(v) = row.value(key) {
                    let text = match (prec, unit, thousands) {
                        // Explicit precision wins: the unit rides the template as a literal
                        (Some(p), _, _) => format!("{v:.p$}"),
                        (None, _, true) => ztest::api::thousands(v.max(0.0) as u64),
                        (None, Some(u), _) => ztest::api::unit_value(*u, v),
                        (None, None, _) => ztest::api::compact(v),
                    };
                    out.push(styled(fit(&text, *width, *align, star), *tone, theme));
                }
            }
            Cell::Pair { key, tone } => {
                if let Some((done, total)) = row.pair(key) {
                    out.push(styled(ztest::api::byte_pair(done, total), *tone, theme));
                }
            }
            Cell::Bar { key, width } => {
                if let Some(pct) = row.percent(key) {
                    // Three spans, not one pre-styled string: a `Span` carrying its own
                    // ANSI would defeat every width calculation downstream of it
                    let filled = (pct.min(100) as usize) * width / 100;
                    out.push(styled("[".into(), Tone::Dim, theme));
                    out.push(styled(theme.chars.bar_fill.repeat(filled), Tone::Count, theme));
                    out.push(styled(
                        theme.chars.bar_empty.repeat(width - filled),
                        Tone::Dim,
                        theme,
                    ));
                    out.push(styled("]".into(), Tone::Dim, theme));
                }
            }
            Cell::Spark { key, width } => {
                if let Some(bands) = row.bands(key) {
                    let opts = crate::plot::PlotOpts::new(*width, 1, theme.chars.graph);
                    let palette = crate::plot::Palette::pools(theme.is_colorized());
                    let drawn =
                        crate::plot::plot_stacked(&[(key.as_str(), bands)], &opts, &palette);
                    out.push(Span::raw(drawn.into_iter().next().unwrap_or_default()));
                }
            }
        }
    }
}

/// A group survives only if every data cell inside it reads measured.
///
/// Nested groups do not veto their parent: an inner optional that collapses is exactly
/// what it is for, so only *this* level's cells are consulted
fn group_is_present(cells: &[Cell], row: &dyn Row) -> bool {
    cells.iter().all(|c| match c {
        Cell::Lit(_) | Cell::Spinner { .. } | Cell::Glyph { .. } | Cell::Group(_) => true,
        Cell::Text { key, .. } => row.text(key).is_some(),
        Cell::Value { key, .. } => row.value(key).is_some(),
        Cell::Pair { key, .. } => row.pair(key).is_some(),
        Cell::Bar { key, .. } => row.percent(key).is_some(),
        Cell::Spark { key, .. } => row.bands(key).is_some(),
    })
}

fn fit(s: &str, width: Option<Width>, align: Align, star: Option<usize>) -> String {
    let have = crate::layout::display_width(s);
    let target = match width {
        None => return s.to_string(),
        Some(Width::Fixed(n)) => n,
        // Measuring (`None`) contributes nothing, so the fixed total excludes this cell.
        // Rendering fills, but never below natural width — the same template then serves a
        // padded panel column and an unpadded single line
        Some(Width::Star) => match star {
            None => return String::new(),
            Some(n) => n.max(have),
        },
    };
    if have >= target {
        return crate::layout::truncate(s, target);
    }
    let pad = target - have;
    match align {
        Align::Left => format!("{s}{}", " ".repeat(pad)),
        Align::Right => format!("{}{s}", " ".repeat(pad)),
        Align::Center => {
            let l = pad / 2;
            format!("{}{s}{}", " ".repeat(l), " ".repeat(pad - l))
        }
    }
}

fn styled(text: String, tone: Tone, theme: &Theme) -> Span<'static> {
    Span::styled(text, style_for(tone, theme))
}

/// [`Tone`] -> a ratatui [`Style`]. The one place a named tone becomes colour, so
/// NO_COLOR and the ASCII theme fall out of `Theme` rather than each call site
fn style_for(tone: Tone, theme: &Theme) -> Style {
    use ratatui::style::{Color, Modifier};
    if !theme.is_colorized() {
        return Style::default();
    }
    match tone {
        Tone::Plain => Style::default(),
        Tone::Dim => Style::default().fg(Color::DarkGray),
        Tone::Count => Style::default().add_modifier(Modifier::BOLD),
        Tone::Pass => Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
        Tone::Fail => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        Tone::Skip => Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        Tone::ScriptId => Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD),
    }
}

/// [`Line`] -> ANSI, the form every existing surface already emits.
///
/// - Per-span SGR, reset after each styled span (a span never leaks into its neighbour)
/// - Unstyled spans emit no escapes at all, so a NO_COLOR render is byte-clean
pub(super) fn to_ansi(line: &Line<'_>) -> String {
    let mut out = String::new();
    for span in &line.spans {
        match sgr(span.style) {
            Some(codes) => {
                out.push_str("\x1b[");
                out.push_str(&codes);
                out.push('m');
                out.push_str(&span.content);
                out.push_str("\x1b[0m");
            }
            None => out.push_str(&span.content),
        }
    }
    out
}

/// `None` = nothing to set, so the span writes bare
fn sgr(style: Style) -> Option<String> {
    use ratatui::style::{Color, Modifier};
    let mut parts: Vec<String> = Vec::new();
    if style.add_modifier.contains(Modifier::BOLD) {
        parts.push("1".into());
    }
    if style.add_modifier.contains(Modifier::DIM) {
        parts.push("2".into());
    }
    if let Some(c) = style.fg {
        parts.push(
            match c {
                Color::Black => "30",
                Color::Red => "31",
                Color::Green => "32",
                Color::Yellow => "33",
                Color::Blue => "34",
                Color::Magenta => "35",
                Color::Cyan => "36",
                Color::Gray => "37",
                Color::DarkGray => "90",
                Color::LightRed => "91",
                Color::LightGreen => "92",
                Color::LightYellow => "93",
                Color::LightBlue => "94",
                Color::LightMagenta => "95",
                Color::LightCyan => "96",
                Color::White => "97",
                _ => "39",
            }
            .to_string(),
        );
    }
    (!parts.is_empty()).then(|| parts.join(";"))
}

/// Key -> value, the binder every surface needs.
///
/// - Unset key reads absent, which is what collapses a `[..]` group
/// - `maybe_*` binds nothing for `None`, so an unmeasured figure never becomes an empty
///   string that would render as a gap
/// - One copy: `render.rs`, `report.rs` and `status.rs` each grew their own before this
#[derive(Debug, Default)]
pub struct Fields<'a> {
    texts: Vec<(&'static str, Cow<'a, str>)>,
    values: Vec<(&'static str, f64)>,
    pairs: Vec<(&'static str, (u64, u64))>,
    percents: Vec<(&'static str, u8)>,
    bands: Vec<(&'static str, Vec<crate::plot::Band>)>,
}

impl<'a> Fields<'a> {
    pub fn new() -> Fields<'a> {
        Fields::default()
    }

    pub fn text(mut self, key: &'static str, v: impl Into<Cow<'a, str>>) -> Self {
        self.texts.push((key, v.into()));
        self
    }

    pub fn maybe_text(self, key: &'static str, v: Option<impl Into<Cow<'a, str>>>) -> Self {
        match v {
            Some(v) => self.text(key, v),
            None => self,
        }
    }

    pub fn value(mut self, key: &'static str, v: f64) -> Self {
        self.values.push((key, v));
        self
    }

    pub fn maybe_value(self, key: &'static str, v: Option<f64>) -> Self {
        match v {
            Some(v) => self.value(key, v),
            None => self,
        }
    }

    pub fn pair(mut self, key: &'static str, done: u64, total: u64) -> Self {
        self.pairs.push((key, (done, total)));
        self
    }

    pub fn percent(mut self, key: &'static str, v: u8) -> Self {
        self.percents.push((key, v));
        self
    }

    pub fn bands(mut self, key: &'static str, v: Vec<crate::plot::Band>) -> Self {
        self.bands.push((key, v));
        self
    }
}

fn lookup<T: Copy>(v: &[(&'static str, T)], key: &str) -> Option<T> {
    v.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
}

impl Row for Fields<'_> {
    fn text(&self, key: &str) -> Option<Cow<'_, str>> {
        self.texts.iter().find(|(k, _)| *k == key).map(|(_, v)| Cow::Borrowed(v.as_ref()))
    }
    fn value(&self, key: &str) -> Option<f64> {
        lookup(&self.values, key)
    }
    fn pair(&self, key: &str) -> Option<(u64, u64)> {
        lookup(&self.pairs, key)
    }
    fn percent(&self, key: &str) -> Option<u8> {
        lookup(&self.percents, key)
    }
    fn bands(&self, key: &str) -> Option<Vec<crate::plot::Band>> {
        self.bands.iter().find(|(k, _)| *k == key).map(|(_, v)| v.clone())
    }
}

// ─────────────────────────────── test row ─────────────────────────────

/// Hand-built [`Row`] for the template tests; `None` for anything not set, which is what
/// makes the absent-vs-zero cases expressible
/// `{@name}` -> themed glyph. One table, consulted by [`emit`] to draw and by
/// [`parse`](super::parse) to reject an unknown name — so a role added here is spellable
/// in a template immediately, and a typo can never fall through to some default glyph
pub(super) fn glyph_of(name: &str, c: &crate::theme::ThemeChars) -> Option<&'static str> {
    Some(match name {
        "ok" => c.ok,
        "fail" => c.fail,
        "warn" => c.warn,
        "dot" => c.dot,
        "bullet" => c.bullet,
        "arrow" => c.arrow,
        "progress" => c.progress,
        "up" => c.up,
        "ellipsis" => c.ellipsis,
        "vbar" => c.vbar,
        "na" => c.na,
        "dash" => c.dash,
        "mine" => c.mine,
        "blocked" => c.blocked,
        "pending" => c.pending,
        "stem_mid" => c.stem_mid,
        "stem_last" => c.stem_last,
        _ => return None,
    })
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct TestRow {
    texts: std::collections::BTreeMap<String, String>,
    values: std::collections::BTreeMap<String, f64>,
    pairs: std::collections::BTreeMap<String, (u64, u64)>,
    percents: std::collections::BTreeMap<String, u8>,
}

#[cfg(test)]
impl TestRow {
    pub(super) fn new() -> TestRow {
        TestRow::default()
    }
    pub(super) fn text(mut self, k: &str, v: &str) -> Self {
        self.texts.insert(k.into(), v.into());
        self
    }
    pub(super) fn value(mut self, k: &str, v: f64) -> Self {
        self.values.insert(k.into(), v);
        self
    }
    pub(super) fn pair(mut self, k: &str, d: u64, t: u64) -> Self {
        self.pairs.insert(k.into(), (d, t));
        self
    }
    pub(super) fn percent(mut self, k: &str, v: u8) -> Self {
        self.percents.insert(k.into(), v);
        self
    }
}

#[cfg(test)]
impl Row for TestRow {
    fn text(&self, key: &str) -> Option<Cow<'_, str>> {
        self.texts.get(key).map(|s| Cow::Borrowed(s.as_str()))
    }
    fn value(&self, key: &str) -> Option<f64> {
        self.values.get(key).copied()
    }
    fn pair(&self, key: &str) -> Option<(u64, u64)> {
        self.pairs.get(key).copied()
    }
    fn percent(&self, key: &str) -> Option<u8> {
        self.percents.get(key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template::Template;

    fn theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    /// Regression: a collapsed group used to leave its width reserved, so the `*` cell
    /// came up short and the row ended ragged
    #[test]
    fn a_collapsed_group_hands_its_width_back_to_the_star_cell() {
        let t = Template::parse("{name:<*}|[ {rate|per_sec}]");
        let row = TestRow::new().text("name", "ab");
        assert_eq!(t.render(&row, 10, Duration::ZERO, &theme()).to_string(), "ab       |");
    }

    #[test]
    fn a_pair_shares_one_magnitude() {
        let t = Template::parse("{d%}");
        let row = TestRow::new().pair("d", 512, 1024);
        assert_eq!(t.render(&row, 40, Duration::ZERO, &theme()).to_string(), "512 B / 1.0 KiB");
    }

    #[test]
    fn a_bar_fills_to_its_percent() {
        let t = Template::parse("{p:12#}");
        let row = TestRow::new().percent("p", 50);
        assert_eq!(t.render(&row, 40, Duration::ZERO, &theme()).to_string(), "[██████░░░░░░]");
    }

    /// An inner optional collapsing must not take the outer group with it — that is the
    /// difference between "no ETA yet" and "no transfer running"
    #[test]
    fn a_nested_group_does_not_veto_its_parent() {
        let t = Template::parse("{name}[ · {rate|per_sec}[ · {eta}]]");
        let row = TestRow::new().text("name", "seed").value("rate", 8.0);
        assert_eq!(t.render(&row, 40, Duration::ZERO, &theme()).to_string(), "seed · 8/s");
    }

    #[test]
    fn an_over_long_cell_truncates_to_its_width() {
        let t = Template::parse("{name:<6}|");
        let row = TestRow::new().text("name", "a-very-long-name");
        assert_eq!(t.render(&row, 40, Duration::ZERO, &theme()).to_string(), "a-very|");
    }
}
