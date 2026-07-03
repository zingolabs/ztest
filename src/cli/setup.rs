//! `ztest setup`: bring up a local kind cluster ready to run the ztest
//! integration suites, in one command.
//!
//! Two phases:
//! 1. Create the kind cluster (subprocess to `kind`, idempotent — skipped
//!    if the cluster already exists). `kind`'s own progress UI is
//!    inherited.
//! 2. Provision every K8s resource ztest needs — CSI snapshot support,
//!    ztest StorageClasses, QoS RBAC + per-tier ServiceAccounts, the
//!    `zaino-{seeds,qos}` namespaces, the NVMe node label — through
//!    [`resource::initialize`], which drives them through the same
//!    dependency-ordered [`Graph`](crate::resource::Graph) `ztest run`
//!    uses for runtime test resources.
//!
//! Zero `kubectl` subprocess: every K8s call goes through `kube-rs`.

use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use clap::Parser;

use crate::resource::{self, InitializeOpts, NodeId, NodeState};

use super::cluster_tools::{
    kind_cluster_exists, kind_context, kind_create, require_kind, require_tool,
};

#[derive(Debug, Parser)]
pub struct Args {
    /// Name of the kind cluster to create / target.
    #[arg(long, default_value = "zkn")]
    name: String,

    /// Skip waiting for Deployments/StatefulSets to become Ready. Faster
    /// setup, but the first test run then blocks on their rollout instead.
    #[arg(long)]
    no_wait: bool,
}

pub fn execute(args: Args) -> ExitCode {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ztest setup: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(&args)) {
        Ok(()) => {
            eprintln!(
                "\n✓ cluster `{}` ready. Context: {}\n  Run tests with: ztest run",
                args.name,
                kind_context(&args.name),
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("\nztest setup: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> Result<(), String> {
    // Tool + cluster prerequisites.
    require_kind()?;
    // `docker` is what `kind` uses to run the node container. Fail up-front
    // with a clean message rather than surfacing kind's own error.
    require_tool(
        "docker",
        &["version"],
        "install Docker Desktop or your distro's docker package; it's what `kind` uses to run its node container.",
    )?;

    // 1. Cluster.
    if kind_cluster_exists(&args.name)? {
        eprintln!("• kind cluster `{}` already exists — reusing", args.name);
    } else {
        eprintln!("• creating kind cluster `{}`", args.name);
        kind_create(&args.name)?;
    }

    // 2. Provision the K8s infrastructure graph. Every provider is
    //    idempotent, so this is safe to re-run against a partially-set-up
    //    cluster.
    eprintln!("• provisioning cluster infrastructure");
    let client = crate::cluster::client()
        .await
        .map_err(|e| format!("connect to cluster: {e}"))?;

    // Track lifecycle transitions in insertion order for a compact
    // human-readable summary at the end. Providers may emit multiple
    // transitions per node (`Acquiring` → `Ready` / `Failed`); we key by
    // node id so the final render shows one line per node.
    let seen: Arc<Mutex<Vec<(NodeId, NodeState)>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_capture = Arc::clone(&seen);
    let on_change = move |id: &NodeId, state: &NodeState| {
        let mut s = seen_capture.lock().expect("progress mutex poisoned");
        // Terminal states get printed live; interim `Acquiring` is quiet
        // to keep the setup output tight.
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
            ..Default::default()
        },
        on_change,
    )
    .await
    .map_err(|e| format!("graph shape: {e}"))?;

    // Determine outcome: any Failed/Blocked node ⇒ non-zero exit. The
    // graph doesn't abort on a single failure (that's the point — one
    // stuck subtree shouldn't strand the rest), so we scan the final
    // state map here.
    let (failed, blocked): (Vec<_>, Vec<_>) = states
        .iter()
        .filter(|(_, s)| !matches!(s, NodeState::Ready))
        .partition(|(_, s)| matches!(s, NodeState::Failed(_)));

    if !failed.is_empty() || !blocked.is_empty() {
        return Err(format!(
            "{} node(s) failed, {} node(s) blocked. See `  ✗ … / · …` lines above.",
            failed.len(),
            blocked.len(),
        ));
    }

    Ok(())
}

/// Insert-or-replace, keeping the vec in insertion order.
fn upsert(v: &mut Vec<(NodeId, NodeState)>, id: &NodeId, state: &NodeState) {
    if let Some(existing) = v.iter_mut().find(|(k, _)| k == id) {
        existing.1 = state.clone();
    } else {
        v.push((id.clone(), state.clone()));
    }
}
