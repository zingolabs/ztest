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
    pub ok: &'static str,
    pub progress: &'static str,
    pub up: &'static str,
    pub warn: &'static str,
    pub fail: &'static str,
    pub hbar_char: char,
    pub dot: &'static str,
    /// listing marker, coloured by kind (≠ [`Self::dot`] — ASCII `*` collides on the same line)
    pub entry: &'static str,
    pub ellipsis: &'static str,
    pub vbar: &'static str,
    pub bar_fill: &'static str,
    pub bar_empty: &'static str,
    pub graph: super::plot::GraphMode,
    pub frame: [&'static str; 6],
}

impl ThemeChars {
    pub fn unicode() -> Self {
        Self {
            ok: "✓",
            progress: "⇣",
            up: "⇡",
            warn: "!",
            fail: "✗",
            hbar_char: '─',
            dot: "·",
            entry: "●",
            ellipsis: "…",
            vbar: "│",
            bar_fill: "█",
            bar_empty: "░",
            graph: super::plot::GraphMode::Braille,
            frame: ["╭", "╮", "╰", "╯", "─", "│"],
        }
    }

    pub fn ascii() -> Self {
        Self {
            ok: "OK",
            progress: "..",
            up: "^^",
            warn: "WARN",
            fail: "FAIL",
            hbar_char: '-',
            dot: "*",
            entry: "o",
            ellipsis: "...",
            vbar: "|",
            bar_fill: "#",
            bar_empty: "-",
            graph: super::plot::GraphMode::Ascii,
            frame: ["+", "+", "+", "+", "-", "|"],
        }
    }

    pub fn hbar(&self, n: usize) -> String {
        std::iter::repeat_n(self.hbar_char, n).collect()
    }
}
