//! Row templates: the row *shape* declared as text, bound to data at render time.
//!
//! - Surface declares a template; this module owns every decision about how it draws
//! - `[..]` group = omitted whole when any cell inside is unmeasured (absent != zero)
//! - Output is [`Line`], so a template composes into any ratatui widget/`Buffer`
//! - Units resolve through [`unit_value`](ztest::api::unit_value): one vocabulary, not five

mod bind;
mod parse;

pub use bind::{Fields, Row};

use ratatui::text::Line;

use crate::Theme;

/// Horizontal placement inside a cell's declared width
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Align {
    Left,
    Right,
    Center,
}

/// Cell width. `Star` takes the row's remaining space, resolved against `total_width`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Width {
    Fixed(usize),
    Star,
}

/// Named theme style, resolved late so one template serves colour and NO_COLOR alike
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Tone {
    #[default]
    Plain,
    Dim,
    Count,
    Pass,
    Fail,
    Skip,
    ScriptId,
}

/// One drawable piece of a row.
///
/// Every data-bearing variant reads through [`Row`], which answers `None` for anything
/// unmeasured — the single rule that keeps a blank column from rendering as `0`
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cell {
    /// Separator/punctuation between placeholders; never absent
    Lit(String),
    /// `{key}` — a string
    Text { key: String, width: Option<Width>, align: Align, tone: Tone },
    /// `{key|unit}` — a number, formatted by its [`Unit`](ztest::api::Unit)
    Value {
        key: String,
        width: Option<Width>,
        align: Align,
        unit: Option<ztest::api::Unit>,
        prec: Option<usize>,
        thousands: bool,
        tone: Tone,
    },
    /// `{key%}` — a `done/total` pair sharing one magnitude
    Pair { key: String, tone: Tone },
    /// `{key:N#}` — a filled meter
    Bar { key: String, width: usize },
    /// `{key:N~}` — a stacked sparkline, drawn by [`plot`](crate::plot)
    Spark { key: String, width: usize },
    /// `{@spin}` — the shared frame-clock spinner
    Spinner { tone: Tone },
    /// `{@ok}` `{@fail}` `{@warn}` `{@dot}` `{@na}` `{@blocked}` … — a themed glyph, in
    /// whichever encoding the terminal supports. Every name in
    /// [`glyph_of`](bind::glyph_of) is spellable here, and only those
    Glyph { name: String, tone: Tone },
    /// `[..]` — dropped entirely when any cell inside reads absent
    Group(Vec<Cell>),
}

/// A parsed row shape. Parse once (`LazyLock`), bind per frame
#[derive(Debug, Clone, PartialEq)]
pub struct Template {
    pub(crate) cells: Vec<Cell>,
}

impl Template {
    /// Parse a row template.
    ///
    /// Panics on a malformed template: templates are `const`-declared literals, so a bad
    /// one is a build-time mistake that must not reach a running panel as a blank row
    pub fn parse(src: &str) -> Template {
        match parse::parse(src) {
            Ok(cells) => Template { cells },
            Err(e) => panic!("malformed row template {src:?}: {e}"),
        }
    }

    /// Bind and draw to an ANSI string.
    ///
    /// The string form, not the [`Line`], is what every current surface consumes — and a
    /// piped report has no `Buffer` to render into. `Line` stays the internal shape so a
    /// panel can compose these through ratatui widgets instead
    pub fn render_str(
        &self,
        row: &dyn Row,
        width: usize,
        elapsed: std::time::Duration,
        theme: &Theme,
    ) -> String {
        bind::to_ansi(&self.render(row, width, elapsed, theme))
    }

    /// Bind `row` and draw. `width` sizes `*` cells; `elapsed` drives the spinner
    pub fn render(
        &self,
        row: &dyn Row,
        width: usize,
        elapsed: std::time::Duration,
        theme: &Theme,
    ) -> Line<'static> {
        bind::render(&self.cells, row, width, elapsed, theme)
    }
}

/// Parse and draw in one call.
///
/// The common case for a *printed* line: no `*` cell to size and no spinner to phase, so
/// neither a width nor a frame clock has anything to contribute. Eight surfaces each kept
/// a private three-line copy of this, in two different argument orders
pub fn draw(src: &str, row: &dyn Row, theme: &Theme) -> String {
    Template::parse(src).render_str(row, 0, std::time::Duration::ZERO, theme)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rule the whole layer exists to hold: an unmeasured cell takes its separator
    /// with it, rather than leaving a dangling `·` or rendering `0`
    #[test]
    fn an_absent_group_takes_its_separator_with_it() {
        let t = Template::parse("{name}[ · {rate|per_sec}]");
        let theme = Theme::for_capabilities(false, true);
        let with = bind::TestRow::new().text("name", "zebrad").value("rate", 12.0);
        let without = bind::TestRow::new().text("name", "zebrad");
        assert_eq!(
            t.render(&with, 40, std::time::Duration::ZERO, &theme).to_string(),
            "zebrad · 12/s"
        );
        assert_eq!(t.render(&without, 40, std::time::Duration::ZERO, &theme).to_string(), "zebrad");
    }

    /// Regression: a zero that *was* measured must still render, or a genuinely idle
    /// pool disappears from the panel instead of reporting zero
    #[test]
    fn a_measured_zero_still_renders() {
        let t = Template::parse("{name}[ · {rate|per_sec}]");
        let theme = Theme::for_capabilities(false, true);
        let row = bind::TestRow::new().text("name", "orchard").value("rate", 0.0);
        assert_eq!(
            t.render(&row, 40, std::time::Duration::ZERO, &theme).to_string(),
            "orchard · 0/s"
        );
    }

    #[test]
    fn width_and_alignment_are_honoured() {
        let t = Template::parse("{name:>7|dim}|{n:<5}");
        let theme = Theme::for_capabilities(false, true);
        let row = bind::TestRow::new().text("name", "orc").text("n", "12");
        assert_eq!(
            t.render(&row, 40, std::time::Duration::ZERO, &theme).to_string(),
            "    orc|12   "
        );
    }

    /// `*` is what lets one template serve a 60-column pipe and a 200-column terminal
    #[test]
    fn a_star_cell_absorbs_the_remaining_width() {
        let t = Template::parse("{name:<*}|");
        let theme = Theme::for_capabilities(false, true);
        let row = bind::TestRow::new().text("name", "ab");
        assert_eq!(t.render(&row, 10, std::time::Duration::ZERO, &theme).to_string(), "ab       |");
    }

    /// Regression: routing a scan rate through `Unit::PerSec` reduced 12.4 to `12`
    /// (`compact` is integer past 1.0), and a load reading 1.5 cores to `2c`. Precision is
    /// declared per cell; the unit then rides the template as a literal, which also keeps
    /// `blk` and its `/s` adjacent
    #[test]
    fn a_precision_cell_keeps_the_decimal_a_rate_needs() {
        let theme = Theme::for_capabilities(false, true);
        let row = bind::TestRow::new().value("blk", 12.4).value("cpu", 1.5);
        let draw = |src| {
            Template::parse(src).render(&row, 40, std::time::Duration::ZERO, &theme).to_string()
        };
        assert_eq!(draw("{blk|per_sec}"), "12/s", "compact drops the tenth");
        assert_eq!(draw("{blk:.1} blk/s"), "12.4 blk/s");
        assert_eq!(draw("{cpu:.1}c"), "1.5c");
        assert_eq!(draw("{blk:.0} blk/s"), "12 blk/s", "precision 0 still bypasses the unit");
    }

    /// Units are the reason this layer exists: one vocabulary, not the five spellings
    /// the hand-rolled renderers grew
    #[test]
    fn units_resolve_through_one_vocabulary() {
        let theme = Theme::for_capabilities(false, true);
        let row = bind::TestRow::new().value("b", 1536.0).value("r", 94.0 * 1024.0 * 1024.0);
        assert_eq!(
            Template::parse("{b|bytes}")
                .render(&row, 40, std::time::Duration::ZERO, &theme)
                .to_string(),
            "1.5 KiB"
        );
        assert_eq!(
            Template::parse("{r|bytes_per_sec}")
                .render(&row, 40, std::time::Duration::ZERO, &theme)
                .to_string(),
            "94.0 MiB/s"
        );
    }
}
