//! [`RunView`] impl: paints the engine's per-tick state through the pinned console.

use crate::console::{Console, SceneFrame};
use ztest::api::Cancel;
use ztest::api::PanelFrame;
use ztest::api::QosPlan;
use ztest::api::RunView;

use super::{Theme, render_live_panel};

pub struct ConsoleView<'a> {
    console: &'a Console,
    theme: &'a Theme,
}

impl<'a> ConsoleView<'a> {
    pub fn new(console: &'a Console, theme: &'a Theme) -> Self {
        Self { console, theme }
    }
}

impl RunView for ConsoleView<'_> {
    fn cancel(&self) -> Cancel {
        self.console.cancel()
    }

    fn live_rows(&self) -> usize {
        self.console.live_rows() as usize
    }

    fn flush_live(&self) {
        self.console.flush_live();
    }

    fn scrollback(&self, text: String) {
        self.console.scrollback(text);
    }

    fn tick(&self, frame: &PanelFrame, plan: &QosPlan, live: String) {
        // Snapshotted into an immutable scene the render thread re-paints (spinner animated)
        // until the next tick
        let left =
            render_live_panel(&frame.snapshot, plan, &frame.free, &frame.progress, self.theme);
        // Provisioning = pre-run barrier → right column blank (width-driven split holds, no
        // reflow here)
        self.console.scene(move |_elapsed| SceneFrame {
            left: left.clone(),
            mid: None,
            right: String::new(),
            live: Some(live.clone()),
        });
    }
}
