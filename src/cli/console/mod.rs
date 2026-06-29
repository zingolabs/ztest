//! Unified bottom-pinned run console.
//!
//! A single compact status panel stays pinned to the bottom of the terminal for
//! the **entire** `ztest run` session — preflight, image build, and the test
//! run — while every subprocess's output scrolls *above* it into the terminal's
//! **native scrollback**. This replaces the old split UI (a top `PinnedHeader`
//! DECSTBM banner for preflight + a separate run-phase panel): there is now one
//! panel, always at the bottom.
//!
//! All output reaches the real terminal through a [`ratatui`] **inline
//! viewport** (`Viewport::Inline`), whose `insert_before` forwards completed
//! lines into native scrollback — the load-bearing property a top DECSTBM
//! region can't provide (lines scrolled off a margin region are discarded, not
//! retained). The `scrolling-regions` cargo feature MUST stay off, or
//! `insert_before` switches to a DECSTBM region most emulators exclude from
//! scrollback.
//!
//! One [`Console`] is created per session ([`Console::new`]) and reused for
//! every phase via [`Console::run_child`]: `cargo nextest list` (preflight
//! build), `docker build` / `kind load` (images), and `cargo nextest run` (the
//! run phase, via [`Console::run_tests`]). Every child runs under a PTY so its
//! native colour + in-place progress survive; [`avt`] interprets the result
//! into a grid we render in the viewport's live region, with the appropriate
//! panel ([`crate::preflight::render_preflight_panel`] or
//! [`crate::preflight::render_live_panel`]) pinned beneath, and completed lines
//! forwarded to native scrollback via `insert_before`.

use std::io::Stdout;

use ratatui::backend::CrosstermBackend;

mod bridge;
mod engine;

pub use engine::Console;

/// Rows reserved at the bottom for the status panel — the tallest a panel ever
/// renders: a top rule plus up to three content lines (the run panel's running
/// / progress / per-tier rows). Neither panel draws a bottom rule, so the
/// separator only ever appears at the *top* of the live display. `draw_frame`
/// sizes the panel region to each frame's actual line count (bottom-anchored,
/// remaining rows go to the live region), so a shorter panel — the preflight
/// panel is three lines — leaves no blank filler row. Shared so the reservation
/// (and thus the `avt` grid height) stays constant across the session.
pub(super) const PANEL_ROWS: u16 = 4;

/// The concrete `ratatui` backend both consoles drive — crossterm over the real
/// stdout. Aliased so the loop signatures stay readable.
pub(super) type Backend = CrosstermBackend<Stdout>;
