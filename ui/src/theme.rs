//! Colour palette + glyph table for the preflight banner.
//!
//! - Matches nextest's reporter conventions: same `owo_colors` roles,
//!   [`supports_color`]/[`supports_unicode`] gating, same rule glyph
//! - Built once via [`Theme::detect`], passed by ref into [`super::render`]
//! - Every colour/glyph decision lives here; renderers ask by role, never emit ANSI

use owo_colors::Style;
use supports_color::Stream as ColorStream;
use supports_unicode::Stream as UnicodeStream;

#[derive(Debug, Clone)]
pub struct Theme {
    pub styles: Styles,
    pub chars: ThemeChars,
}

impl Theme {
    /// Same environment matrix as nextest.
    ///
    /// - colour: `NO_COLOR`/`CLICOLOR`/`CLICOLOR_FORCE`/`TERM=dumb` + TTY detection
    /// - unicode: `supports-unicode` sniff (locale, `WT_SESSION`, …)
    pub fn detect() -> Self {
        let colorize =
            supports_color::on(ColorStream::Stdout).map(|level| level.has_basic).unwrap_or(false);
        let unicode = supports_unicode::on(UnicodeStream::Stdout);
        Self::for_capabilities(colorize, unicode)
    }

    /// For tests and for callers that resolved capabilities elsewhere (e.g. an
    /// inherited `--color`)
    pub fn for_capabilities(colorize: bool, unicode: bool) -> Self {
        Self {
            styles: if colorize { Styles::colorized() } else { Styles::plain() },
            chars: if unicode { ThemeChars::unicode() } else { ThemeChars::ascii() },
        }
    }

    pub fn is_colorized(&self) -> bool {
        self.styles.colorized
    }
}

// ─────────────────────────── Styles ───────────────────────────────────

/// `owo_colors::Style` per semantic role, mirroring nextest's `helpers::Styles`
/// (banner reads as a continuation of nextest's own output)
#[derive(Debug, Clone, Default)]
pub struct Styles {
    pub colorized: bool,
    pub pass: Style,
    pub fail: Style,
    pub skip: Style,
    pub count: Style,
    pub dim: Style,
    pub script_id: Style,
}

impl Styles {
    pub fn plain() -> Self {
        Self::default()
    }

    pub fn colorized() -> Self {
        Self {
            colorized: true,
            pass: Style::new().green().bold(),
            fail: Style::new().red().bold(),
            skip: Style::new().yellow().bold(),
            count: Style::new().bold(),
            dim: Style::new().bright_black(),
            script_id: Style::new().blue().bold(),
        }
    }
}

// ─────────────────────────── ThemeChars ───────────────────────────────

/// Glyph table, Unicode or ASCII fallback (a CI logfile diffs cleanly).
///
/// `frame` = corners then edges: top-left, top-right, bottom-left, bottom-right,
/// horizontal, vertical
#[derive(Debug, Clone)]
pub struct ThemeChars {
    /// Which set this is, so a consumer picks a variant instead of sniffing a glyph
    pub unicode: bool,
    pub ok: &'static str,
    pub arrow: &'static str,
    pub bullet: &'static str,
    pub progress: &'static str,
    pub up: &'static str,
    pub warn: &'static str,
    pub fail: &'static str,
    pub hbar_char: char,
    pub dot: &'static str,
    pub entry: &'static str, // listing marker, coloured by kind (≠ [`Self::dot`] — ASCII `*` collides on the same line)
    pub ellipsis: &'static str,
    pub vbar: &'static str,
    pub na: &'static str, // placeholder where a figure was not measured — never `0`, which is a measurement
    pub dash: &'static str, // prose separator, a wider pause than [`Self::dot`]. Spells the same as [`Self::na`]
    pub mine: &'static str, // "this row is yours" pointer in a listing gutter
    pub blocked: &'static str, // out of service (a cordoned node), distinct from [`Self::fail`] (a thing that ran)
    pub pending: &'static str, // not started yet — a projection, not an outcome
    pub stem_mid: &'static str, // tree corners; the surface appends its own tail (`── ` in a plan, `→` in a gantt)
    pub stem_last: &'static str,
    pub bar_fill: &'static str,
    pub bar_empty: &'static str,
    pub bar_light: char, // Gantt cells. `char`, not `&str`: the bar is a column grid, so one glyph is one cell
    pub bar_end: char,
    pub clip_start: char,
    pub clip_end: char,
    pub tick_now: char,
    pub graph: super::plot::GraphMode,
    pub frame: [&'static str; 6],
}

impl ThemeChars {
    pub fn unicode() -> Self {
        Self {
            unicode: true,
            ok: "✓",
            arrow: "→",
            bullet: "•",
            progress: "⇣",
            up: "⇡",
            warn: "!",
            fail: "✗",
            hbar_char: '─',
            dot: "·",
            entry: "●",
            ellipsis: "…",
            vbar: "│",
            na: "—",
            dash: "—",
            mine: "▸",
            blocked: "⊘",
            pending: "◇",
            stem_mid: "├",
            stem_last: "└",
            bar_fill: "█",
            bar_empty: "░",
            bar_light: '▒',
            bar_end: '┤',
            clip_start: '◀',
            clip_end: '▶',
            tick_now: '▼',
            graph: super::plot::GraphMode::Braille,
            frame: ["╭", "╮", "╰", "╯", "─", "│"],
        }
    }

    pub fn ascii() -> Self {
        Self {
            unicode: false,
            ok: "OK",
            arrow: "->",
            bullet: "+",
            progress: "..",
            up: "^^",
            warn: "WARN",
            fail: "FAIL",
            hbar_char: '-',
            dot: "*",
            entry: "o",
            ellipsis: "...",
            vbar: "|",
            na: "-",
            dash: "-",
            mine: ">",
            blocked: "X",
            pending: "<>",
            stem_mid: "|",
            stem_last: "`",
            bar_fill: "#",
            bar_empty: "-",
            bar_light: ':',
            bar_end: ']',
            clip_start: '<',
            clip_end: '>',
            tick_now: 'v',
            graph: super::plot::GraphMode::Ascii,
            frame: ["+", "+", "+", "+", "-", "|"],
        }
    }

    pub fn hbar(&self, n: usize) -> String {
        std::iter::repeat_n(self.hbar_char, n).collect()
    }
}
