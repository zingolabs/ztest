//! `ztest cluster setup`: provision the namespaced resources ztest owns. Idempotent.
//!
//! - Not a cluster installer (storage / monitoring CRDs / registry = operator's,
//!   see `docs/ops-cluster-requirements.md`, probe with `ztest cluster check`)
//! - Same dependency-ordered [`Graph`](crate::resource::Graph) `ztest run` uses

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use clap::Args as ClapArgs;
use dialoguer::Confirm;

use crate::resource::{self, InitializeOpts, NodeId, NodeState};

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

    /// Don't provision the metrics stack (Prometheus + Pyroscope + Grafana).
    /// For a cluster that already runs its own — configure the endpoints on the
    /// cluster profile instead, and ztest uses those.
    #[arg(long)]
    no_observability: bool,

    /// Don't provision the `metrics.k8s.io` API (metrics-server, into `kube-system`).
    /// For a cluster whose operator owns that API — a cluster already serving it is
    /// left untouched regardless.
    #[arg(long)]
    no_metrics_api: bool,
}

pub fn execute(args: Args) -> ExitCode {
    // Bind kube-context + kubeconfig pre-runtime → the resolution below targets it.
    // Precedence: --cluster > ambient env > default.
    //
    // SAFETY: still single-threaded; `set_var` must precede thread creation.
    if let Err(detail) = unsafe { crate::cluster_config::activate(args.cluster.as_deref()) } {
        eprintln!("ztest cluster setup: {detail}");
        return ExitCode::FAILURE;
    }

    crate::cli::block_on("cluster setup", crate::cli::Rt::Multi, run(&args))
}

async fn run(args: &Args) -> Result<(), String> {
    // Echo the target pre-write (wrong-context provisioning = the irreversible footgun)
    let cfg = crate::cluster::config().await.map_err(|e| format!("resolve kube config: {e}"))?;
    eprintln!("• target cluster: {}", cfg.cluster_url);
    let client = kube::Client::try_from(cfg).map_err(|e| format!("connect to cluster: {e}"))?;

    // Refuse a cluster that cannot run tests: setup cannot fix a missing capability
    // (operator's) → name it before writing anything
    let report = crate::capability::probe(&client).await;
    for cap in report.blocking() {
        eprintln!("  ✗ {}: {}", cap.name, cap.finding.detail());
        eprintln!("      see {}", cap.remedy);
    }
    if !report.is_runnable() {
        return Err("cluster is missing a required capability; `ztest cluster check` for the \
                    full report"
            .to_string());
    }

    // First write gated on assent (remote cluster ztest did not create)
    if !args.non_interactive && std::io::stdin().is_terminal() {
        let ok = Confirm::new()
            .with_prompt("Provision ztest infrastructure into this cluster?")
            .default(false)
            .interact()
            .map_err(|e| format!("confirmation prompt: {e}"))?;
        if !ok {
            return Err("aborted: no changes made".to_string());
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

    let states = resource::initialize(
        client,
        InitializeOpts {
            no_wait: args.no_wait,
            observability: !args.no_observability,
            metrics_api: !args.no_metrics_api,
            ..Default::default()
        },
        on_change,
    )
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
        return Err(format!(
            "{} node(s) failed, {} node(s) blocked. See `  ✗ … / · …` lines above.",
            failed.len(),
            blocked.len(),
        ));
    }

    eprintln!("\n✓ ztest resources ready. Run tests with: ztest run");
    Ok(())
}

/// Insert-or-replace, vec kept in insertion order
fn upsert(v: &mut Vec<(NodeId, NodeState)>, id: &NodeId, state: &NodeState) {
    if let Some(existing) = v.iter_mut().find(|(k, _)| k == id) {
        existing.1 = state.clone();
    } else {
        v.push((id.clone(), state.clone()));
    }
}
