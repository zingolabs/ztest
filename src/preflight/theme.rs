//! Colour palette and glyph table for the preflight banner.
//!
//! Matches `cargo nextest`'s reporter conventions: same `owo_colors` style
//! choices (`green().bold()` for ok actions, `red().bold()` for fails,
//! `bright_black()` for dim separators), same [`supports_color`] /
//! [`supports_unicode`] capability gating, same horizontal-rule glyph.
//!
//! Callers construct a [`Theme`] once via [`Theme::detect`] and pass it by
//! reference into [`super::render`]. All colour / glyph decisions live here; the
//! renderer reaches for them by semantic role and never invokes ANSI directly.

use owo_colors::Style;
use supports_color::Stream as ColorStream;
use supports_unicode::Stream as UnicodeStream;

/// Top-level theme: palette, glyph table, and a colorize flag for callers that
/// want to bypass `style.style(s)` no-ops.
#[derive(Debug, Clone)]
pub struct Theme {
    pub styles: Styles,
    pub chars: ThemeChars,
}

impl Theme {
    /// Detect terminal capabilities and build a theme matching the
    /// runtime context.
    ///
    /// Honors the same environment matrix nextest does:
    /// - colour: `NO_COLOR`, `CLICOLOR`, `CLICOLOR_FORCE`,
    ///   `TERM=dumb`, and TTY detection (`supports-color`).
    /// - unicode: terminal capability sniff via `supports-unicode`
    ///   (which considers locale and `WT_SESSION` etc.).
    pub fn detect() -> Self {
        let colorize = supports_color::on(ColorStream::Stdout)
            .map(|level| level.has_basic)
            .unwrap_or(false);
        let unicode = supports_unicode::on(UnicodeStream::Stdout);
        Self::for_capabilities(colorize, unicode)
    }

    /// Explicit construction for tests and for code that already resolved
    /// capability flags another way (e.g. an inherited `--color` flag).
    pub fn for_capabilities(colorize: bool, unicode: bool) -> Self {
        Self {
            styles: if colorize {
                Styles::colorized()
            } else {
                Styles::plain()
            },
            chars: if unicode {
                ThemeChars::unicode()
            } else {
                ThemeChars::ascii()
            },
        }
    }

    /// True iff this theme renders ANSI escape codes. Useful for callers that
    /// early-return a captured rendering without an escape strip.
    pub fn is_colorized(&self) -> bool {
        self.styles.colorized
    }
}

// ─────────────────────────── Styles ───────────────────────────────────

/// `owo_colors::Style` per semantic role, mirroring nextest's `helpers::Styles`:
/// - `pass`: successful state / started action labels.
/// - `fail`: hard failures.
/// - `skip`: soft failures (the `!` warn marker; nextest uses skip similarly).
/// - `count`: numeric counts; bold, no colour, so emphasis carries without
///   fighting the palette.
/// - `dim`: separators, secondary metadata (matches nextest's `run_id_rest`).
/// - `script_id`: banner header label; matches nextest's setup-script identifier
///   colour, since the preflight banner is itself setup-script output.
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

/// Glyph table: Unicode when the terminal can render it, ASCII fallback
/// otherwise. The renderer only knows the semantic names (`ok`, `progress`,
/// `warn`, `hbar`, `dot`); the glyph choice happens here so a CI logfile gets a
/// deterministic ASCII rendering that diffs cleanly.
#[derive(Debug, Clone)]
pub struct ThemeChars {
    pub ok: &'static str,
    pub progress: &'static str,
    pub warn: &'static str,
    pub fail: &'static str,
    /// Horizontal rule character.
    pub hbar_char: char,
    /// Separator dot (between metadata fields on a line).
    pub dot: &'static str,
    /// Progress-bar fill and empty cells. ASCII fallback uses `#` / `-`
    /// so a `[####------] 40%` reads cleanly in any terminal.
    pub bar_fill: &'static str,
    pub bar_empty: &'static str,
}

impl ThemeChars {
    pub fn unicode() -> Self {
        Self {
            ok: "✓",
            progress: "⇣",
            warn: "!",
            fail: "✗",
            hbar_char: '─',
            dot: "·",
            bar_fill: "█",
            bar_empty: "░",
        }
    }

    pub fn ascii() -> Self {
        Self {
            ok: "OK",
            progress: "..",
            warn: "WARN",
            fail: "FAIL",
            hbar_char: '-',
            dot: "*",
            bar_fill: "#",
            bar_empty: "-",
        }
    }

    /// `n`-wide horizontal rule. Matches nextest's `theme_chars.hbar(n)`.
    pub fn hbar(&self, n: usize) -> String {
        std::iter::repeat_n(self.hbar_char, n).collect()
    }
}
