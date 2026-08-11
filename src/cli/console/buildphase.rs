//! The on-cluster **build + provision** phase, rendered through the unified
//! [`Console`]. Both `ztest run` and `ztest sync start` do the identical work
//! here — build the runner image on the cluster (compiling the selected tests),
//! then provision the profile's `dev!` component images and data seeds — and
//! both drive it through this module so the two entry points render the same
//! pinned-panel UX and can never drift.
//!
//! What lives here is exactly the console-coupled machinery: the colored phase
//! boundary lines ([`commit_phase`]) and the right-column background-transfer
//! tracker ([`provision_with_tracker`]). The *left* panel content stays with
//! each caller (its scene differs: `run` shows the preflight banner, `sync`
//! shows the sync context), reusing the shared [`crate::ui`] primitives.

use std::collections::HashMap;
use std::time::Duration;

use kube::Client;
use owo_colors::OwoColorize as _;

use crate::cli::console::Console;
use crate::pipeline::remote_compile::Phase;
use crate::resource::{self, Graph, NodeId, NodeState};
use crate::ui::{Theme, TransferKind, TransferProgress, TransferRow, Transfers};

/// A scene closure reads this each frame to paint live cluster capacity; `None`
/// pins the panel to its last static figure.
pub(crate) type CapRx = tokio::sync::watch::Receiver<crate::qos::ClusterCapacity>;

/// Compact human duration: `12.3s` under a minute, `1m05s` above.
pub(crate) fn fmt_dur(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s < 60.0 {
        format!("{s:.1}s")
    } else {
        let t = d.as_secs();
        format!("{}m{:02}s", t / 60, t % 60)
    }
}

/// Format a [`Phase`] transition as a colored scrollback line and commit it
/// through the console (or stderr when there is none). Returns the new
/// sub-phase label when a phase *started*, so the caller can repaint its panel's
/// live row; [`None`] for the `Done`/`Step`/`Note` lines that only land in
/// scrollback.
pub(crate) fn commit_phase(
    console: Option<&Console>,
    theme: &Theme,
    ev: Phase<'_>,
) -> Option<String> {
    let (line, new_phase) = match ev {
        Phase::Start(label) => (
            format!("{} {label}…", "•".style(theme.styles.dim)),
            Some(label.to_string()),
        ),
        Phase::Done { label, dur } => (
            format!(
                "  {} {label} {}",
                theme.chars.ok.style(theme.styles.pass),
                format!("({})", fmt_dur(dur)).style(theme.styles.dim),
            ),
            None,
        ),
        Phase::Note(text) => (format!("{} {text}", "•".style(theme.styles.dim)), None),
    };
    match console {
        Some(c) => {
            // Commit live-grid content before this boundary line so history stays
            // in order: build output, then the `✓ …` that closes it.
            c.flush_live();
            c.scrollback(format!("{line}\n"));
        }
        None => eprintln!("{line}"),
    }
    new_phase
}

/// A short label + kind for a resource node's right-column row. Image tags
/// collapse to `dev-<repo-leaf>`; seeds keep their `seed-<sha8>` id.
///
/// The runtime graph emits only [`NodeId::Image`] and [`NodeId::Seed`]; any
/// other variant falls back to `display_label()` as an image row rather than
/// panic (the label carries its own kind tag).
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

/// A background-transfer state change for the right column: the graph's coarse
/// lifecycle (`on_change`) and each provider's finer sub-phase notes merged onto
/// one channel so the work side folds both into the [`TransferRegistry`] in order.
enum TransferEvent {
    /// A node's lifecycle transition from the graph executor.
    State(NodeId, NodeState),
    /// A provider's sub-phase report (a note, a byte count, or finalizing).
    Progress(NodeId, crate::resource::Progress),
}

/// The work-side model behind the right column: in-flight (and failed)
/// background acquisitions, keyed by resource node.
#[derive(Default)]
struct TransferRegistry {
    rows: std::collections::BTreeMap<NodeId, TransferRow>,
}

impl TransferRegistry {
    /// Fold one event into the registry: a node starts (Acquiring) as an active
    /// row, updates its note, drops out on Ready, or is marked failed.
    fn apply(&mut self, ev: TransferEvent) {
        match ev {
            TransferEvent::State(id, NodeState::Acquiring) => {
                let (label, kind) = describe_node(&id);
                self.rows.entry(id).or_insert_with(|| TransferRow {
                    label,
                    kind,
                    progress: TransferProgress::Active {
                        note: "acquiring".to_string(),
                        bytes: None,
                    },
                });
            }
            TransferEvent::State(id, NodeState::Ready) => {
                self.rows.remove(&id);
            }
            TransferEvent::State(id, NodeState::Failed(detail)) => {
                if let Some(row) = self.rows.get_mut(&id) {
                    row.progress = TransferProgress::Failed { detail };
                }
            }
            // Pending/Blocked never surface: nothing started, and a blocked node's
            // failed dependency is the signal shown instead.
            TransferEvent::State(_, NodeState::Pending | NodeState::Blocked) => {}
            // A sub-phase report only updates a still-active row; a Failed row
            // keeps its failure until the phase ends.
            TransferEvent::Progress(id, progress) => {
                if let Some(TransferRow {
                    progress: TransferProgress::Active { note, bytes },
                    ..
                }) = self.rows.get_mut(&id)
                {
                    match progress {
                        crate::resource::Progress::Note(n) => {
                            *note = n;
                            *bytes = None;
                        }
                        crate::resource::Progress::Bytes {
                            done,
                            total,
                            note: n,
                        } => {
                            *note = n;
                            *bytes = Some((done, total));
                        }
                        // Bytes complete, manifest still in flight: drop the bar so
                        // the row spins instead of parking at 100% until Ready.
                        crate::resource::Progress::Finalizing => {
                            "finalizing…".clone_into(note);
                            *bytes = None;
                        }
                    }
                }
            }
        }
    }

    /// An immutable snapshot for the render thread.
    fn snapshot(&self) -> Transfers {
        Transfers {
            rows: self.rows.values().cloned().collect(),
        }
    }
}

/// Provision a planned resource `graph` (component images + data seeds),
/// maintaining the right-column transfer tracker and calling `repaint` with a
/// fresh [`Transfers`] snapshot on every lifecycle/sub-phase change. Returns the
/// terminal node states so the caller can resolve refs / dependency edges.
///
/// Provisioning runs sequentially (cap 1): a topological `build && load && …`
/// walk. Image builds are already disk/CPU/network bound, so building two at
/// once mostly contends; serial order also lets each stream its native
/// BuildKit/kind output through the single emulator grid with no interleaving.
/// On a TTY the child streams to the grid; off it, it inherits stdio.
pub(crate) async fn provision_with_tracker(
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
        resource::ProgressSink::new(move |id, progress| {
            let _ = tx.send(TransferEvent::Progress(id, progress));
        })
    });
    let mut builder = resource::Cx::builder(client);
    if let Some(c) = console.cloned() {
        builder = builder.console(c);
    }
    if let Some(p) = progress {
        builder = builder.progress(p);
    }
    if let Some(pod) = build_pod {
        builder = builder.build_pod(pod);
    }
    let cx = builder.build();

    let on_change = {
        let tx = tx.clone();
        move |id: &NodeId, st: &NodeState| {
            let _ = tx.send(TransferEvent::State(id.clone(), st.clone()));
        }
    };
    // Only the `on_change` + sink clones keep the channel open, so it closes when
    // provisioning finishes.
    drop(tx);

    let prov = graph.provision(&cx, 1, on_change);
    tokio::pin!(prov);
    loop {
        tokio::select! {
            states = &mut prov => {
                // Drain events queued after the last transition, then paint the
                // final (usually empty) right column.
                while let Ok(ev) = rx.try_recv() {
                    registry.apply(ev);
                }
                repaint(&registry.snapshot());
                break states;
            }
            Some(ev) = rx.recv() => {
                registry.apply(ev);
                repaint(&registry.snapshot());
            }
        }
    }
}
