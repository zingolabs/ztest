//! Unified bottom-pinned run console: a persistent render thread.
//!
//! A single compact status panel stays pinned to the bottom of the terminal for
//! the entire `ztest run` session (preflight, image build, and the test run)
//! while every subprocess's output scrolls above it into the terminal's native
//! scrollback.
//!
//! The split: a dedicated render thread ([`render`]) owns the terminal, the
//! `avt` virtual terminal, and the frame clock, and runs its own current-thread
//! tokio runtime so its 33 ms redraw tick fires independently of the work side.
//! That independence is the point: the panel stays live (spinner, clocks) even
//! while a phase blocks on a silent multi-second subprocess.
//!
//! The work side never touches the terminal. It holds a cheap, clonable
//! [`Console`] handle and talks to the render thread purely by value: pushing
//! immutable [`SceneFrame`] render-recipes on every domain-state change, and
//! streaming a child's PTY bytes via [`run_child`] ([`child`]). See
//! `docs/console-architecture.md` for the full rationale.
//!
//! All output reaches the real terminal through a [`ratatui`] inline viewport
//! ([`Surface`]), whose `insert_before` forwards completed lines into native
//! scrollback. A top DECSTBM region can't do this: lines scrolled off a margin
//! region are discarded, not retained. The `scrolling-regions` cargo feature
//! MUST stay off, or `insert_before` switches to a DECSTBM region most emulators
//! exclude from scrollback.
//!
//! The test run is just another scene producer: the engine ([`crate::engine`])
//! owns process-per-test execution and ships a fresh [`SceneFrame`] panel +
//! scrollback through the same [`Console`]. No special handoff; the one panel
//! viewport persists across every phase.

use std::io::Stdout;

use ratatui::backend::CrosstermBackend;

// The console's whole scrollback mechanism depends on ratatui's `insert_before`
// forwarding completed lines into the terminal's NATIVE scrollback. That only
// holds while ratatui's `scrolling-regions` feature is OFF; with it on,
// `insert_before` scrolls content through a DECSTBM margin region most emulators
// exclude from scrollback, silently breaking the design (verified against
// ratatui-core 0.1.2 `terminal/inline.rs`). Fail the build loudly rather than
// regress at runtime. See the `scrolling-regions` guard-feature in Cargo.toml.
#[cfg(feature = "scrolling-regions")]
compile_error!(
    "ztest's console requires ratatui's `scrolling-regions` feature to stay OFF \
     (it routes insert_before through a DECSTBM region and breaks native-scrollback \
     forwarding). Remove whatever enabled `scrolling-regions`."
);

mod bridge;
mod child;
mod render;
mod viewport;

pub(crate) use child::run_child;
pub(crate) use render::{Console, SceneFrame};
pub(crate) use viewport::Surface;

/// The pinned status panel's fixed height in rows: a branded rule plus four
/// content lines (the two-column cluster/build/run status + transfer tracker).
/// Held constant across every phase so the panel is a stable, always-fully-
/// painted block pinned to the bottom of the inline viewport.
pub(super) const PANEL_ROWS: u16 = 5;

/// The on-screen **live region** height: the rows between native scrollback and
/// the pinned panel. During the compile phase it shows the child's live output
/// (cargo's `Compiling …` lines + progress bar); during the run the engine feeds
/// it the live running-tests block instead. Completed lines scroll off the top
/// into native scrollback above the live region, seamlessly. The inline viewport
/// is therefore `LIVE_ROWS + PANEL_ROWS` tall for the whole session.
///
/// This is the **single source of truth** for the live-region size. It's fixed
/// because the inline viewport height is immutable once created (ratatui #984),
/// so it must be chosen up front. Everything downstream that needs the live
/// height — the `avt` grid, the child PTY, the engine's running-rows count — does
/// not restate this number: it derives from [`Surface::live_rows`], which reports
/// the rows the surface actually reserved (`viewport height − PANEL_ROWS`).
///
/// Modest by design: a taller live region reserves more rows that sit blank until
/// the child produces output (the startup-gap tradeoff), so this is kept small.
pub(super) const LIVE_ROWS: u16 = 8;

/// The concrete `ratatui` backend both consoles drive: crossterm over the real
/// stdout. Aliased so the loop signatures stay readable.
pub(super) type Backend = CrosstermBackend<Stdout>;
