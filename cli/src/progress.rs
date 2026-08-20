//! One-shot [`StepProgress`] sink for the cluster-free subcommands.
//!
//! - `run` paints transfers into the pinned console; `snapshot push`/`warm` have neither
//!   console nor frame clock → single row repainted in place here
//! - Fold + rate window = [`TransferState`]; this module owns *painting policy* only
//! - Non-TTY → one line per [`LOG_STEP`] percent (CI logs must not carry repaint frames)

use std::io::{IsTerminal as _, Write as _};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ztest::api::Progress;
use ztest::api::progress::StepProgress;
use ztest_ui::{Theme, TransferKind, TransferProgress, TransferRow, TransferState};

/// Repaint cadence (9,000-part upload must not spend its time formatting)
const REPAINT: Duration = Duration::from_millis(100);

/// Non-TTY log granularity, in percent
const LOG_STEP: u64 = 5;

struct State {
    transfer: TransferState,
    last_fold: Option<Instant>,
    logged_step: u64,
    dirty: bool,
    finished: bool,
}

/// One live transfer row on **stderr** (stdout carries the manifest/result, often piped)
pub struct LiveStep {
    row_label: String,
    kind: TransferKind,
    theme: Theme,
    started: Instant,
    tty: bool,
    state: Mutex<State>,
}

impl LiveStep {
    pub fn new(label: impl Into<String>, kind: TransferKind) -> LiveStep {
        LiveStep {
            row_label: label.into(),
            kind,
            theme: Theme::detect(),
            started: Instant::now(),
            tty: std::io::stderr().is_terminal(),
            state: Mutex::new(State {
                transfer: TransferState::new("starting"),
                last_fold: None,
                logged_step: 0,
                dirty: false,
                finished: false,
            }),
        }
    }

    /// Close the row: unthrottled repaint, then newline.
    ///
    /// - Idempotent (`push` finishes before its result line, caller finishes again on exit)
    /// - Latch also deadens a late report → row never repaints below its own result
    pub fn finish(&self) {
        let mut st = self.state.lock().expect("LiveStep state mutex poisoned");
        if st.finished {
            return;
        }
        st.finished = true;
        if self.tty {
            self.paint(&st);
            if st.dirty {
                let mut err = std::io::stderr();
                let _ = writeln!(err);
                let _ = err.flush();
            }
        }
        st.dirty = false;
    }

    fn paint(&self, st: &State) {
        let row = TransferRow {
            label: self.row_label.clone(),
            kind: self.kind,
            progress: st.transfer.progress().clone(),
        };
        let line = ztest_ui::render_transfer_line(&row, self.started.elapsed(), &self.theme);
        let mut err = std::io::stderr();
        // `\r` + erase-to-end, never clear-screen (line above = whatever already printed)
        let _ = write!(err, "\r\x1b[2K{line}");
        let _ = err.flush();
    }

    /// Sole write path: fold, then paint under this sink's policy.
    ///
    /// - Bytes sampled at [`REPAINT`] (arrive per read ~60k/s; `pace_by` allocates per
    ///   fold → 30% of a 245 GiB hash spent on frames nobody paints)
    /// - Note/Finalizing always fold (phase changes are the row's only structure)
    fn report(&self, ev: Progress) {
        let now = Instant::now();
        let mut st = self.state.lock().expect("LiveStep state mutex poisoned");
        if st.finished {
            return;
        }
        let due = !st.last_fold.is_some_and(|last| now.duration_since(last) < REPAINT);
        if matches!(ev, Progress::Bytes { .. }) && !due {
            return;
        }
        st.last_fold = Some(now);
        st.transfer.apply(ev, now);
        match self.tty {
            true => {
                self.paint(&st);
                st.dirty = true;
            }
            false => self.log(&mut st),
        }
    }

    fn log(&self, st: &mut State) {
        match st.transfer.progress() {
            TransferProgress::Bytes { done, total, .. } => {
                let step = match total {
                    0 => 0,
                    t => done * 100 / t / LOG_STEP,
                };
                if step > st.logged_step {
                    st.logged_step = step;
                    let pair = ztest::api::byte_pair(*done, *total);
                    eprintln!("  {}: {}% ({pair})", self.row_label, step * LOG_STEP);
                }
            }
            TransferProgress::Stage(note) => {
                st.logged_step = 0;
                eprintln!("  {}: {note}", self.row_label);
            }
            TransferProgress::Failed { detail } => eprintln!("  {}: {detail}", self.row_label),
        }
    }
}

impl StepProgress for LiveStep {
    fn note(&self, note: &str) {
        self.report(Progress::Note(note.to_string()));
    }

    fn bytes(&self, done: u64, total: u64) {
        self.report(Progress::Bytes { done, total });
    }

    fn finalizing(&self) {
        self.report(Progress::Finalizing);
    }
}
