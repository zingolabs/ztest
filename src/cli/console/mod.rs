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
//! All output reaches the real terminal through a manual **sticky footer**
//! ([`Surface`] + [`footer`]): completed lines are printed normally so the
//! terminal scrolls them into its own native scrollback, and only the footer
//! (live region + pinned panel) is repainted in place each frame. No reserved
//! region, no DECSTBM scroll region (which would discard lines scrolled off an
//! interior margin instead of saving them), no fixed height — the footer is
//! exactly as tall as its content.
//!
//! The test run is just another scene producer: the engine ([`crate::engine`])
//! owns process-per-test execution and ships a fresh [`SceneFrame`] panel +
//! scrollback through the same [`Console`]. No special handoff; the pinned panel
//! persists across every phase.

mod bridge;
mod child;
mod footer;
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

/// Rows available for the live region above the pinned panel: the full terminal
/// height minus [`PANEL_ROWS`]. **The single source** every live-region consumer
/// derives from — the `avt` grid, each child PTY (build/compile), and the engine's
/// running-tests block — so the live region is exactly as tall as the terminal
/// allows and a child renders as it would in a bare shell, never clipped to a
/// hard-coded row count. The footer is still drawn only as tall as its live content
/// actually is (`trimmed_view` for the grid, the running list for the engine); this
/// is the ceiling, not a reservation.
///
/// Floored to 1: a terminal can momentarily report a height at or below the panel
/// during a resize, and `avt` underflow-panics on a 0 dimension.
pub(super) fn live_rows_for(total_rows: u16) -> u16 {
    total_rows.saturating_sub(PANEL_ROWS).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_region_fills_the_terminal_above_the_panel() {
        // No hard-coded cap: the live region is the whole terminal minus the panel,
        // so a tall in-place progress block renders at real height.
        assert_eq!(live_rows_for(50), 50 - PANEL_ROWS);
        assert_eq!(live_rows_for(24), 24 - PANEL_ROWS);
    }

    #[test]
    fn live_region_never_underflows_to_zero() {
        // A terminal at or below the panel height (or a momentary 0 during resize)
        // must floor to 1: `avt` underflow-panics on a 0 dimension.
        assert_eq!(live_rows_for(PANEL_ROWS), 1);
        assert_eq!(live_rows_for(0), 1);
    }
}
