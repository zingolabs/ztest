//! `ztest sync watch` — `status`, redrawn each scrape until the run's report lands.
//!
//! - Same view, same source, same renderer as `status` (a live view drawing anything else
//!   would disagree with the report the run ends on)
//! - Read-only: Ctrl-C detaches, never stops the sync (only `ztest sync stop` does)

use std::io::{IsTerminal as _, Write as _, stdout};

use anyhow::Result;
use ztest::sync::SyncStatus;
use ztest_ui::Theme;

use super::{render, report_view};

/// How an attach ended: the sync's own standing, or the user leaving it running. `watch`
/// reports; the *caller* maps it to an exit status (`ztest sync watch` always succeeds;
/// `ztest sync start --watch` stands in for a foreground run and must fail its pipeline on
/// a failing verdict)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WatchEnd {
    Detached,
    Settled(SyncStatus),
}

/// Cursor home + clear to end → each frame replaces the last in place
const CLEAR: &str = "\x1b[H\x1b[J";

pub(super) async fn watch(id: &str) -> Result<WatchEnd> {
    let client = super::client().await?;
    let theme = Theme::detect();
    // Off a terminal a redraw is noise: one report, once the run settles
    let redraw = stdout().is_terminal();
    loop {
        let (view, status) = report_view(&client, id).await?;
        let settled = !status.is_live();
        if redraw || settled {
            let frame = ztest_ui::render_sync_report(&view, &theme, render::width());
            let mut out = stdout().lock();
            if redraw {
                write!(out, "{CLEAR}")?;
            }
            write!(out, "{frame}")?;
            out.flush()?;
        }
        if settled {
            return Ok(WatchEnd::Settled(status));
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("sync {id}: detached — still running (`ztest sync status {id}`)");
                return Ok(WatchEnd::Detached);
            }
            _ = tokio::time::sleep(ztest::api::metrics::SCRAPE_INTERVAL) => {}
        }
    }
}
