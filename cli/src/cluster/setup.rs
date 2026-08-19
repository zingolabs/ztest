//! `ztest cluster setup`: provision the namespaced resources ztest owns. Idempotent.
//!
//! - Not a cluster installer (storage / monitoring CRDs / registry = operator's,
//!   see `docs/ops-cluster-requirements.md`, probe with `ztest cluster check`)
//! - Same dependency-ordered [`Graph`](ztest::api::resource::Graph) `ztest run` uses

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use clap::Args as ClapArgs;
use dialoguer::Confirm;

use super::csi_hostpath::{self, Offer};
use crate::CliError;
use ztest::api::capability::Report;
use ztest::api::resource::{self, InitializeOpts, NodeId, NodeState};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Named cluster profile (`ztest cluster list`) to provision against: binds
    /// its kube-context + kubeconfig before connecting, exactly as `ztest run
    /// --cluster` does. Omitted, the persisted default (if any) is used, else
    /// the ambient kube-context.
    #[arg(long, value_name = "NAME")]
    cluster: Option<String>,

    /// Never prompt; require every choice to come from a flag. For CI and
    /// scripted cluster bootstrap.
    #[arg(long)]
    non_interactive: bool,

    /// Skip waiting for Deployments/StatefulSets to become Ready. Faster
    /// setup, but the first test run then blocks on their rollout instead.
    #[arg(long)]
    no_wait: bool,

    /// Don't provision anything metrics-related: neither the metrics stack (Prometheus
    /// + Pyroscope + Grafana, into `ztest-obs`) nor the `metrics.k8s.io` API
    /// (metrics-server, into `kube-system`). For a cluster whose operator owns these —
    /// configure the stack's endpoints on the cluster profile instead, and ztest uses
    /// those. A cluster already serving `metrics.k8s.io` is left untouched regardless.
    #[arg(long)]
    no_observability: bool,

    /// Install csi-hostpath on a snapshot-less local cluster instead of prompting.
    /// Copies each seed whole; far slower than a TopoLVM thin pool
    /// (docs/ops-local-cluster.md).
    #[arg(long)]
    install_storage: bool,
}

pub fn execute(args: Args) -> ExitCode {
    // Bind kube-context + kubeconfig pre-runtime → the resolution below targets it.
    // Precedence: --cluster > ambient env > default.
    //
    // SAFETY: still single-threaded; `set_var` must precede thread creation.
    if let Err(detail) = unsafe { ztest::api::cluster_config::activate(args.cluster.as_deref()) } {
        eprintln!("ztest cluster setup: {detail}");
        return ExitCode::FAILURE;
    }

    crate::block_on("cluster setup", crate::Rt::Multi, run(&args))
}

async fn run(args: &Args) -> Result<(), CliError> {
    // Echo the target pre-write (wrong-context provisioning = the irreversible footgun)
    let cfg =
        ztest::api::cluster::config().await.map_err(|e| format!("resolve kube config: {e}"))?;
    eprintln!("• target cluster: {}", cfg.cluster_url);
    let client = kube::Client::try_from(cfg).map_err(|e| format!("connect to cluster: {e}"))?;

    // Refuse a cluster that cannot run tests: setup cannot fix a missing capability
    // (operator's) → name it before writing anything. Local-cluster storage = the one
    // exception, so it speaks first and in its own shape
    let mut report = ztest::api::capability::probe(&client).await;
    match csi_hostpath::offer(&report, args.non_interactive, args.install_storage)? {
        Offer::Install => {
            csi_hostpath::install()?;
            report = ztest::api::capability::probe(&client).await;
        }
        Offer::Explained => return Err(CliError::Reported),
        Offer::Unrelated => {}
    }
    if !report.is_runnable() {
        eprint!("{}", blocking_summary(&report));
        return Err(CliError::Reported);
    }

    // First write gated on assent (remote cluster ztest did not create)
    if !args.non_interactive && std::io::stdin().is_terminal() {
        let ok = Confirm::new()
            .with_prompt("Provision ztest infrastructure into this cluster?")
            .default(false)
            .interact()
            .map_err(|e| format!("confirmation prompt: {e}"))?;
        if !ok {
            return Err(CliError::Message("aborted: no changes made".into()));
        }
    }

    eprintln!("• provisioning cluster infrastructure");

    // Transitions in insertion order, keyed by node id (providers emit several per node,
    // `Acquiring` → `Ready`/`Failed`; the summary wants one line each)
    let seen: Arc<Mutex<Vec<(NodeId, NodeState)>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_capture = Arc::clone(&seen);
    let on_change = move |id: &NodeId, state: &NodeState| {
        let mut s = seen_capture.lock().expect("progress mutex poisoned");
        match state {
            NodeState::Acquiring => {
                eprintln!("  • {}", id.display_label());
            }
            NodeState::Ready => {
                eprintln!("  ✓ {}", id.display_label());
                upsert(&mut s, id, state);
            }
            NodeState::Failed(msg) => {
                eprintln!("  ✗ {}: {}", id.display_label(), msg);
                upsert(&mut s, id, state);
            }
            NodeState::Blocked => {
                eprintln!("  · {} (blocked by failed dep)", id.display_label());
                upsert(&mut s, id, state);
            }
            NodeState::Pending => {} // never surfaced to `on_change`
        }
    };

    let mut opts = InitializeOpts::default();
    opts.no_wait = args.no_wait;
    opts.observability = !args.no_observability;

    let states = resource::initialize(client, opts, on_change)
        .await
        .map_err(|e| format!("graph shape: {e}"))?;

    // Any Failed/Blocked *required* node ⇒ non-zero, scanned from the final map (the graph
    // never aborts early: one stuck subtree must not strand the rest). Optional nodes
    // report and stand down (a slow metrics stack != broken cluster).
    let (optional, required): (Vec<_>, Vec<_>) = states
        .iter()
        .filter(|(_, s)| !matches!(s, NodeState::Ready))
        .partition(|(id, _)| id.is_optional());

    for (id, _) in &optional {
        eprintln!(
            "\n! {} did not come up. Everything `ztest run` needs is ready; \
             metrics and profiling are not.\n  Re-check with `ztest cluster check` — \
             it may simply still be settling.",
            id.display_label(),
        );
    }

    let (failed, blocked): (Vec<_>, Vec<_>) =
        required.into_iter().partition(|(_, s)| matches!(s, NodeState::Failed(_)));
    if !failed.is_empty() || !blocked.is_empty() {
        // Counts only; each node already printed its own ✗ / · line
        return Err(CliError::Message(format!(
            "{} failed, {} blocked",
            failed.len(),
            blocked.len(),
        )));
    }

    eprintln!("\n✓ ztest resources ready. Run tests with: ztest run");
    Ok(())
}

/// Blocking capabilities, one line each, remedies deduped below (same doc anchor twice = noise)
fn blocking_summary(report: &Report) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let mut remedies: Vec<&str> = Vec::new();
    for cap in report.blocking() {
        let _ = writeln!(out, "  ✗ {}: {}", cap.name, cap.finding.detail());
        if !remedies.contains(&cap.remedy) {
            remedies.push(cap.remedy);
        }
    }
    for remedy in remedies {
        let _ = writeln!(out, "  → {remedy}");
    }
    out
}

/// Insert-or-replace, vec kept in insertion order
fn upsert(v: &mut Vec<(NodeId, NodeState)>, id: &NodeId, state: &NodeState) {
    if let Some(existing) = v.iter_mut().find(|(k, _)| k == id) {
        existing.1 = state.clone();
    } else {
        v.push((id.clone(), state.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ztest::api::capability::{Capability, Finding, Need};

    fn blocked(name: &'static str, remedy: &'static str) -> Capability {
        Capability { name, need: Need::Required, finding: Finding::Absent("gone".into()), remedy }
    }

    /// Storage + snapshot API share a doc anchor: the setup screen it replaced spent
    /// two of its four lines printing that anchor twice
    #[test]
    fn one_remedy_is_printed_once_however_many_capabilities_cite_it() {
        let report = Report {
            capabilities: vec![
                blocked("snapshot-capable storage", "docs/ops-cluster-requirements.md#storage"),
                blocked("VolumeSnapshot v1 API", "docs/ops-cluster-requirements.md#storage"),
            ],
        };
        let summary = blocking_summary(&report);
        assert_eq!(summary.lines().count(), 3, "{summary}");
        assert_eq!(summary.matches("docs/ops-cluster-requirements.md#storage").count(), 1);
    }

    #[test]
    fn distinct_remedies_all_survive() {
        let report = Report {
            capabilities: vec![
                blocked("snapshot-capable storage", "docs/storage.md"),
                blocked("image registry", "docs/registry.md"),
            ],
        };
        let summary = blocking_summary(&report);
        assert!(summary.contains("→ docs/storage.md"), "{summary}");
        assert!(summary.contains("→ docs/registry.md"), "{summary}");
    }
}
