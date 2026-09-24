//! Component restart window, folded from per-tick container samples.
//!
//! - Restart = kubelet count past the phase's first sample (killed by a fault, a handle, or a
//!   crash under `restartPolicy: OnFailure`)
//! - Window held from first sign (count moved / container down / kill issued) until every
//!   container is Ready AND the subject answered again → liveness never clocks the outage

use crate::handles::ContainerSample;

#[derive(Debug, Default)]
pub(super) struct RestartWatch {
    baseline: Vec<Option<u32>>,
    seen: u32,
    holding: bool,
}

/// This tick's read of the window
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RestartView {
    pub restarts: u32,
    pub holding: bool,
}

impl RestartWatch {
    /// Kill issued by the runner itself → hold before kubelet shows anything
    pub(super) fn open(&mut self) {
        self.holding = true;
    }

    /// `samples[i]` = watched container `i` this tick (`None` = unreadable, never "down")
    pub(super) fn observe(
        &mut self,
        samples: &[Option<ContainerSample>],
        subject_answered: bool,
    ) -> RestartView {
        self.baseline.resize(samples.len(), None);
        let mut restarts = 0;
        let mut down = false;
        for (base, sample) in self.baseline.iter_mut().zip(samples) {
            let Some(s) = sample else { continue };
            let base = *base.get_or_insert(s.restarts);
            restarts += s.restarts.saturating_sub(base);
            down |= !s.ready;
        }
        if restarts > self.seen || down {
            self.holding = true;
        }
        self.seen = self.seen.max(restarts);
        if self.holding && !down && subject_answered {
            self.holding = false;
        }
        RestartView { restarts: self.seen, holding: self.holding }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn up(restarts: u32) -> Option<ContainerSample> {
        Some(ContainerSample { restarts, ready: true })
    }
    fn down(restarts: u32) -> Option<ContainerSample> {
        Some(ContainerSample { restarts, ready: false })
    }

    /// Pre-existing restarts = baseline; hold spans kill → Ready → first answered read
    #[test]
    fn window_holds_from_kill_until_ready_and_answering() {
        let mut w = RestartWatch::default();
        let view = |restarts, holding| RestartView { restarts, holding };
        let timeline: [(&str, Vec<Option<ContainerSample>>, bool, RestartView); 7] = [
            ("baseline", vec![up(3), up(0)], true, view(0, false)),
            ("unreadable pod", vec![None, up(0)], true, view(0, false)),
            ("killed, not yet counted", vec![down(3), up(0)], false, view(0, true)),
            ("backoff", vec![down(4), up(0)], false, view(1, true)),
            ("ready, subject still reopening", vec![up(4), up(0)], false, view(1, true)),
            ("answering again", vec![up(4), up(0)], true, view(1, false)),
            ("restart already over when seen", vec![up(4), up(1)], true, view(2, false)),
        ];
        for (step, samples, answered, want) in timeline {
            assert_eq!(w.observe(&samples, answered), want, "{step}");
        }

        w.open();
        assert_eq!(w.observe(&[up(4), up(1)], false), view(2, true), "runner kill opens at once");
    }
}
