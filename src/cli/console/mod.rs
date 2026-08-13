//! Unified bottom-pinned run console: a compact status panel stays pinned for the whole
//! `ztest run` session while subprocess output scrolls above into native scrollback.
//!
//! - Render thread ([`render`]) owns the terminal, the `avt` emulator and the frame clock
//!   on its own runtime → the 33 ms tick fires even while a phase blocks on a silent child
//! - Work side never touches the terminal: a clonable [`Console`], talking by value in
//!   immutable [`SceneFrame`] recipes + child PTY bytes via [`run_child`]
//! - Manual sticky footer ([`Surface`] + [`footer`]): completed lines print into native
//!   scrollback, only the footer repaints in place (`docs/design-execution-engine.md`)

mod bridge;
mod buildphase;
mod child;
mod footer;
mod render;
mod viewport;

pub(crate) use buildphase::{CapRx, commit_phase, provision_with_tracker};
pub(crate) use child::run_child;
pub(crate) use render::{Console, SceneFrame};
pub(crate) use viewport::Surface;

/// Pinned panel height in rows: a branded rule + four content lines. Constant across every
/// phase → a stable, always-fully-painted block
pub(super) const PANEL_ROWS: u16 = 5;

/// Rows available above the pinned panel; the single source every live-region consumer
/// derives from. A ceiling, not a reservation (the footer is drawn only as tall as its
/// content). Floored to 1 — a resize can report a height at or below the panel, and `avt`
/// underflow-panics on a 0 dimension
pub(super) fn live_rows_for(total_rows: u16) -> u16 {
    total_rows.saturating_sub(PANEL_ROWS).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_region_fills_the_terminal_above_the_panel() {
        // No hard cap: live region = whole terminal minus the panel, so a tall in-place
        // progress block renders at real height
        assert_eq!(live_rows_for(50), 50 - PANEL_ROWS);
        assert_eq!(live_rows_for(24), 24 - PANEL_ROWS);
    }

    #[test]
    fn live_region_never_underflows_to_zero() {
        // Terminal at or below the panel height (or a momentary 0 mid-resize) floors to 1;
        // `avt` underflow-panics on a 0 dimension
        assert_eq!(live_rows_for(PANEL_ROWS), 1);
        assert_eq!(live_rows_for(0), 1);
    }
}
