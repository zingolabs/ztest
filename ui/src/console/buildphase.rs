//! On-cluster **build + provision** phase, rendered through the unified [`Console`].
//!
//! - `ztest run` and `ztest sync start` do identical work here (build the runner image
//!   = compile the selected tests, then provision `dev!` images + data seeds), both
//!   through this module → one pinned-panel UX, no drift
//! - Console-coupled machinery only: phase boundary lines ([`commit_phase`]) + the
//!   right-column transfer tracker ([`provision_with_tracker`])
//! - Left panel stays with each caller (`run` = preflight banner, `sync` = sync
//!   context), over the shared [`crate::ui`] primitives

use std::collections::HashMap;
use std::time::Instant;

use kube::Client;

use crate::console::Console;
use crate::template::{Fields, Template};
use crate::{Theme, TransferKind, TransferRow, Transfers};
use ztest::api::CompilePhase as Phase;
use ztest::api::{Cx, Graph, NodeId, NodeState, ProgressSink};

/// Read per frame by a scene closure to paint live capacity; `None` pins the panel
/// to its last static figure
pub type CapRx = tokio::sync::watch::Receiver<ztest::api::ClusterCapacity>;

/// Scrollback lines this module commits. A phase start and a note share a glyph on
/// purpose — both are "something is happening", and the indent tells the closing line apart
mod row {
    pub(super) const START: &str = "{@bullet|dim} {label}{@ellipsis}";
    /// `elapsed` arrives parenthesised: the template cannot tone a literal, and a
    /// bright `(` around a dim `12s` reads as two things
    pub(super) const DONE: &str = "  {@ok|pass} {label} {elapsed|dim}";
    pub(super) const NOTE: &str = "{@bullet|dim} {note}";
}

/// [`Phase`] transition → colored scrollback line, committed through the console
/// (stderr when there is none). `Some(label)` on a phase *start*, for the caller's
/// live row; `None` for `Done`/`Note`, which only land in scrollback
pub fn commit_phase(console: Option<&Console>, theme: &Theme, ev: Phase<'_>) -> Option<String> {
    let draw = |src: &str, f: Fields<'_>| {
        Template::parse(src).render_str(&f, 0, std::time::Duration::ZERO, theme)
    };
    let (line, new_phase) = match ev {
        Phase::Start(label) => {
            (draw(row::START, Fields::new().text("label", label)), Some(label.to_string()))
        }
        Phase::Done { label, dur } => (
            draw(
                row::DONE,
                Fields::new()
                    .text("label", label)
                    .text("elapsed", format!("({})", ztest::api::format_elapsed(dur))),
            ),
            None,
        ),
        Phase::Note(text) => (draw(row::NOTE, Fields::new().text("note", text)), None),
    };
    match console {
        Some(c) => {
            // Live grid before the boundary line, keeping history ordered:
            // build output, then the `✓ …` closing it
            c.flush_live();
            c.scrollback(format!("{line}\n"));
        }
        None => eprintln!("{line}"),
    }
    new_phase
}

/// Label + kind for a node's right-column row: image tags → `dev-<repo-leaf>`, seeds
/// keep `seed-<sha8>`. Runtime graph emits only those two; anything else falls back
/// to an image row rather than panicking
fn describe_node(id: &NodeId) -> (String, TransferKind) {
    match id {
        NodeId::Image(tag) => {
            let repo = tag.split(':').next().unwrap_or(tag);
            let leaf = repo.rsplit('/').next().unwrap_or(repo);
            (format!("dev-{leaf}"), TransferKind::Image)
        }
        NodeId::Seed(name) => (name.clone(), TransferKind::Seed),
        other => (other.display_label(), TransferKind::Image),
    }
}

/// Right-column state change. Graph lifecycle (`on_change`) + per-provider sub-phase
/// notes on one channel → the work side folds both into [`TransferRegistry`] in order
enum TransferEvent {
    State(NodeId, NodeState),
    Progress(NodeId, ztest::api::Progress),
}

/// Work-side model behind the right column: in-flight and failed acquisitions, by node
#[derive(Default)]
struct TransferRegistry {
    rows: std::collections::BTreeMap<NodeId, TrackedRow>,
}

/// [`TransferRow`] before its per-frame snapshot — carries the series the snapshot cannot
struct TrackedRow {
    label: String,
    kind: TransferKind,
    state: crate::TransferState,
}

impl TransferRegistry {
    /// `at` = arrival, passed in rather than read here (a fold reading the clock cannot be
    /// driven over a scripted timeline)
    fn apply(&mut self, ev: TransferEvent, at: Instant) {
        match ev {
            TransferEvent::State(id, NodeState::Acquiring) => {
                let (label, kind) = describe_node(&id);
                self.rows.entry(id).or_insert_with(|| TrackedRow {
                    label,
                    kind,
                    state: crate::TransferState::new("acquiring"),
                });
            }
            TransferEvent::State(id, NodeState::Ready) => {
                self.rows.remove(&id);
            }
            TransferEvent::State(id, NodeState::Failed(detail)) => {
                if let Some(row) = self.rows.get_mut(&id) {
                    row.state.fail(detail);
                }
            }
            // Pending/Blocked never surface: nothing started, and a blocked node's
            // failed dependency is the signal shown instead
            TransferEvent::State(_, NodeState::Pending | NodeState::Blocked) => {}
            TransferEvent::Progress(id, progress) => {
                if let Some(row) = self.rows.get_mut(&id) {
                    row.state.apply(progress, at);
                }
            }
        }
    }

    fn snapshot(&self) -> Transfers {
        let rows = self
            .rows
            .values()
            .map(|r| TransferRow {
                label: r.label.clone(),
                kind: r.kind,
                progress: r.state.progress().clone(),
            })
            .collect();
        Transfers { rows }
    }
}

/// Build-side [`Cx`]: console as the child host + live-region sink, and the pod an
/// on-cluster build `exec`s into. No progress sink — callers with graph nodes add one
pub fn build_cx(client: Client, console: Option<&Console>, build_pod: Option<String>) -> Cx {
    let mut builder = Cx::builder(client);
    if let Some(c) = console.cloned() {
        builder = builder.host(std::sync::Arc::new(c));
    }
    if let Some(pod) = build_pod {
        builder = builder.build_pod(pod);
    }
    builder.build()
}

/// Provision a planned `graph` → terminal node states, `repaint`ing a fresh
/// [`Transfers`] snapshot per lifecycle/sub-phase change.
///
/// Sequential (cap 1): image builds are already disk/CPU/network bound, and serial
/// order streams each one's native BuildKit/kind output uninterleaved
pub async fn provision_with_tracker(
    graph: &Graph,
    client: Client,
    build_pod: Option<String>,
    console: Option<&Console>,
    mut repaint: impl FnMut(&Transfers),
) -> HashMap<NodeId, NodeState> {
    let mut registry = TransferRegistry::default();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<TransferEvent>();

    let progress = console.map(|_| {
        let tx = tx.clone();
        ProgressSink::new(move |id, progress| {
            let _ = tx.send(TransferEvent::Progress(id, progress));
        })
    });
    let mut cx = build_cx(client, console, build_pod);
    cx.progress = progress;

    let on_change = {
        let tx = tx.clone();
        move |id: &NodeId, st: &NodeState| {
            let _ = tx.send(TransferEvent::State(id.clone(), st.clone()));
        }
    };
    // `on_change` + sink clones are now the only holders → closes when provisioning ends
    drop(tx);

    let prov = graph.provision(&cx, 1, on_change);
    tokio::pin!(prov);
    loop {
        tokio::select! {
            states = &mut prov => {
                // Drain post-transition stragglers, then paint the final column
                while let Ok(ev) = rx.try_recv() {
                    registry.apply(ev, Instant::now());
                }
                repaint(&registry.snapshot());
                break states;
            }
            Some(ev) = rx.recv() => {
                registry.apply(ev, Instant::now());
                repaint(&registry.snapshot());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransferProgress;
    use std::time::Duration;
    use ztest::api::Progress;

    fn seed() -> NodeId {
        NodeId::Seed("seed-a1b2c3d4".to_string())
    }

    /// `(second, bytes-done)` against a 4 GiB pull, folded as the graph would deliver it
    fn pull(samples: &[(u64, u64)]) -> TransferRow {
        const TOTAL: u64 = 4 * 1024 * 1024 * 1024;
        let origin = Instant::now();
        let mut registry = TransferRegistry::default();
        registry.apply(TransferEvent::State(seed(), NodeState::Acquiring), origin);
        for &(secs, done) in samples {
            registry.apply(
                TransferEvent::Progress(seed(), Progress::Bytes { done, total: TOTAL }),
                origin + Duration::from_secs(secs),
            );
        }
        registry.snapshot().rows.into_iter().next().expect("the seed row")
    }

    #[test]
    fn a_node_starts_as_a_stage_row_with_no_bar() {
        let row = pull(&[]);
        assert!(matches!(row.progress, TransferProgress::Stage(note) if note == "acquiring"));
    }

    /// Rate at two reports, countdown only once the window agrees with itself — the
    /// same gate the sync panel's blocks/sec runs through
    #[test]
    fn the_bar_measures_before_it_projects() {
        let TransferProgress::Bytes { pace, .. } = pull(&[(0, 0), (1, 1_000_000)]).progress else {
            panic!("a byte report lights the bar");
        };
        let pace = pace.expect("two reports measure");
        assert_eq!(pace.per_sec, 1_000_000.0);
        assert_eq!(pace.eta, None, "one interval is not a trend");

        let steady = (0..5).map(|s| (s, s * 1_000_000)).collect::<Vec<_>>();
        let TransferProgress::Bytes { pace, .. } = pull(&steady).progress else {
            panic!("a byte report lights the bar");
        };
        assert!(pace.expect("still measuring").eta.is_some(), "a steady pull projects");
    }

    /// Puller Job retried: the counter restarts, and no rate may survive the reset
    #[test]
    fn a_restarted_pull_reports_no_rate_until_the_window_clears() {
        let TransferProgress::Bytes { pace, .. } =
            pull(&[(0, 3_000_000), (1, 6_000_000), (2, 0)]).progress
        else {
            panic!("a byte report lights the bar");
        };
        assert_eq!(pace, None);
    }

    /// Leaving byte mode drops the window: a resumed bar must not date its rate to
    /// whatever the node was doing while it showed a stage
    #[test]
    fn a_stage_between_byte_reports_restarts_the_measurement() {
        let origin = Instant::now();
        let mut registry = TransferRegistry::default();
        registry.apply(TransferEvent::State(seed(), NodeState::Acquiring), origin);
        let bytes = |done| Progress::Bytes { done, total: 4_000_000 };
        registry.apply(TransferEvent::Progress(seed(), bytes(1_000_000)), origin);
        registry.apply(
            TransferEvent::Progress(seed(), Progress::Note("retrying pull".into())),
            origin + Duration::from_secs(1),
        );
        registry.apply(
            TransferEvent::Progress(seed(), bytes(2_000_000)),
            origin + Duration::from_secs(2),
        );

        let row = registry.snapshot().rows.into_iter().next().expect("the seed row");
        let TransferProgress::Bytes { pace, .. } = row.progress else {
            panic!("back on the bar");
        };
        assert_eq!(pace, None, "the pre-stage samples are gone");
    }

    /// A failed node keeps its failure until the phase ends, whatever still arrives
    #[test]
    fn a_failure_is_not_overwritten_by_a_late_report() {
        let origin = Instant::now();
        let mut registry = TransferRegistry::default();
        registry.apply(TransferEvent::State(seed(), NodeState::Acquiring), origin);
        registry
            .apply(TransferEvent::State(seed(), NodeState::Failed("PVC timed out".into())), origin);
        registry.apply(
            TransferEvent::Progress(seed(), Progress::Bytes { done: 5, total: 9 }),
            origin + Duration::from_secs(1),
        );

        let row = registry.snapshot().rows.into_iter().next().expect("the seed row");
        assert!(matches!(row.progress, TransferProgress::Failed { .. }));
    }

    /// Row owns its window, so departing takes it — the parallel-map leak this once
    /// guarded is no longer expressible
    #[test]
    fn a_ready_node_leaves_the_column_and_takes_its_window() {
        let origin = Instant::now();
        let mut registry = TransferRegistry::default();
        registry.apply(TransferEvent::State(seed(), NodeState::Acquiring), origin);
        registry
            .apply(TransferEvent::Progress(seed(), Progress::Bytes { done: 5, total: 9 }), origin);
        registry.apply(TransferEvent::State(seed(), NodeState::Ready), origin);
        assert!(registry.snapshot().rows.is_empty());
        assert!(registry.rows.is_empty(), "a departed row must not leak its window");
    }
}
