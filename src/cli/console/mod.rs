//! Unified bottom-pinned run console. A compact status panel stays pinned to the
//! bottom of the terminal for the whole `ztest run` session while subprocess
//! output scrolls above it into native scrollback.
//!
//! A dedicated render thread ([`render`]) owns the terminal, the `avt` emulator,
//! and the frame clock on its own tokio runtime, so its 33 ms redraw tick fires
//! independently of the work side — the panel stays live even while a phase
//! blocks on a silent subprocess. The work side never touches the terminal: it
//! holds a cheap clonable [`Console`] and talks by value, pushing immutable
//! [`SceneFrame`] recipes and streaming child PTY bytes via [`run_child`].
//!
//! Output reaches the terminal through a manual sticky footer ([`Surface`] +
//! [`footer`]): completed lines print normally into native scrollback, only the
//! footer repaints in place. See `docs/console-architecture.md` for the rationale.

mod bridge;
mod buildphase;
mod child;
mod footer;
mod render;
mod viewport;

pub(crate) use buildphase::{commit_phase, provision_with_tracker, CapRx};
pub(crate) use child::run_child;
pub(crate) use render::{Console, SceneFrame};
pub(crate) use viewport::Surface;

/// The pinned status panel's fixed height in rows: a branded rule plus four
/// content lines. Held constant across every phase so the panel is a stable,
/// always-fully-painted block.
pub(super) const PANEL_ROWS: u16 = 5;

/// Rows available for the live region above the pinned panel: terminal height
/// minus [`PANEL_ROWS`]. The single source every live-region consumer derives
/// from, so a child renders at real height rather than a hard-coded row count.
/// This is a ceiling, not a reservation — the footer is drawn only as tall as the
/// live content actually is.
///
/// Floored to 1: a resize can momentarily report a height at or below the panel,
/// and `avt` underflow-panics on a 0 dimension.
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
