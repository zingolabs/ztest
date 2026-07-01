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

mod bridge;
mod child;
mod render;
mod viewport;

pub(crate) use child::run_child;
pub(crate) use render::{Console, SceneFrame};
pub(crate) use viewport::Surface;

/// The pinned status panel's fixed height in rows — and thus the inline
/// viewport's total height for the entire session: a top rule plus four content
/// lines. Held constant across every phase (preflight, image, run) so the panel
/// is a stable, always-fully-painted block. That constancy is what keeps
/// startup gap-free: a small viewport that's immediately filled with real panel
/// content reserves no *visible* blank rows (`ratatui` scrolls at most
/// `PANEL_ROWS - 1` real prior lines up to make room, then paints over them on
/// the first frame), unlike a tall live region that sits empty during a slow
/// compile. Every subprocess and reporter line scrolls above it into native
/// scrollback.
pub(super) const PANEL_ROWS: u16 = 5;

/// Height of the `avt` emulator grid (and the child PTY), decoupled from the
/// panel now that the child's output is never pinned on screen — it only feeds
/// native scrollback. Sized so a subprocess's in-place progress churn (cargo's
/// bar, docker BuildKit's multi-line block) stays contained in the grid and
/// never pollutes scrollback; completed lines scroll off into scrollback as the
/// child produces them, and the phase boundary flushes the final grid.
pub(super) const EMU_ROWS: u16 = 11;

/// The concrete `ratatui` backend both consoles drive: crossterm over the real
/// stdout. Aliased so the loop signatures stay readable.
pub(super) type Backend = CrosstermBackend<Stdout>;
