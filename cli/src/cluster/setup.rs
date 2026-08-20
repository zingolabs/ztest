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
use super::{draw, row};
use crate::CliError;
use ztest::api::capability::Report;
use ztest::api::resource::{self, InitializeOpts, NodeId, NodeState};
use ztest_ui::Theme;
use ztest_ui::template::Fields;

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

    /// Don't provision anything metrics-related: neither the metrics stack
    /// (Prometheus + Pyroscope + Grafana, into `ztest-obs`) nor the
    /// `metrics.k8s.io` API (metrics-server, into `kube-system`). For a cluster
    /// whose operator owns these — configure the stack's endpoints on the cluster
    /// profile instead, and ztest uses those. A cluster already serving
    /// `metrics.k8s.io` is left untouched regardless.
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

/// Every setup line goes to stderr (stdout stays the command's own output)
fn note(src: &str, fields: &Fields<'_>, theme: &Theme) {
    eprintln!("{}", draw(src, fields, theme));
}

async fn run(args: &Args) -> Result<(), CliError> {
    let theme = Theme::detect();

    // Echo the target pre-write (wrong-context provisioning = the irreversible footgun)
    let cfg =
        ztest::api::cluster::config().await.map_err(|e| format!("resolve kube config: {e}"))?;
    let target = Fields::new().text("url", cfg.cluster_url.to_string());
    note(row::TARGET, &target, &theme);
    let client = kube::Client::try_from(cfg).map_err(|e| format!("connect to cluster: {e}"))?;

    // Refuse a cluster that cannot run tests: setup cannot fix a missing capability
    // (operator's) → name it before writing anything. Local-cluster storage = the one
    // exception, so it speaks first and in its own shape
    let mut report = ztest::api::capability::probe(&client).await;
    match csi_hostpath::offer(&report, args.non_interactive, args.install_storage)? {
        Offer::Install => {
            csi_hostpath::install(&client).await?;
            report = ztest::api::capability::probe(&client).await;
        }
        Offer::Explained => return Err(CliError::Reported),
        Offer::Unrelated => {}
    }
    // Substrate only: `Need::Provisioned` gaps are what the graph below is about to fill,
    // and gating on them would deadlock every fresh cluster
    if !report.is_setupable() {
        eprint!("{}", blocking_summary(&report, &theme));
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

    let provisioning = Fields::new().text("text", "provisioning cluster infrastructure");
    note(&row::marked(row::BULLET), &provisioning, &theme);

    // Transitions in insertion order, keyed by node id (providers emit several per node,
    // `Acquiring` → `Ready`/`Failed`; the summary wants one line each)
    let seen: Arc<Mutex<Vec<(NodeId, NodeState)>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_capture = Arc::clone(&seen);
    let progress_theme = theme.clone();
    let on_change = move |id: &NodeId, state: &NodeState| {
        let mut s = seen_capture.lock().expect("progress mutex poisoned");
        let data = Fields::new().text("text", id.display_label());
        let (mark, data) = match state {
            NodeState::Acquiring => (row::BULLET, data),
            NodeState::Ready => (row::OK, data),
            NodeState::Failed(msg) => (row::FAIL, data.text("detail", msg)),
            NodeState::Blocked => (row::BLOCKED, data.text("note", "(blocked by failed dep)")),
            NodeState::Pending => return, // never surfaced to `on_change`
        };
        note(&row::step(mark), &data, &progress_theme);
        if !matches!(state, NodeState::Acquiring) {
            upsert(&mut s, id, state);
        }
    };

    let mut opts = InitializeOpts::default();
    opts.no_wait = args.no_wait;
    opts.observability = !args.no_observability;

    let states = resource::initialize(client.clone(), opts, on_change)
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
        eprintln!();
        note(row::OPTIONAL_DOWN, &Fields::new().text("name", id.display_label()), &theme);
        let hint = "Re-check with `ztest cluster check` — it may simply still be settling.";
        note(row::HINT, &Fields::new().text("hint", hint), &theme);
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

    // Close the loop the graph cannot: re-probe and hold the same contract `ztest cluster
    // check` publishes, so "setup succeeded" and "a run will work" are one answer
    let after = ztest::api::capability::probe(&client).await;
    if !after.is_runnable() {
        eprint!("\n{}", run_summary(&after, &theme));
        return Err(CliError::Reported);
    }
    eprintln!();
    note(row::READY, &Fields::new().text("cmd", "ztest run"), &theme);
    Ok(())
}

/// What still blocks a run after provisioning — a graph that reported Ready over a
/// capability the cluster does not actually offer
fn run_summary(report: &Report, theme: &Theme) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let head = Fields::new().text("note", "provisioned, but `ztest run` is still blocked:");
    let _ = writeln!(out, "{}", draw(row::NOTE, &head, theme));
    for cap in report.blocking() {
        out.push_str(&blocker(cap, theme));
    }
    out
}

/// What stops `setup` before its first write, one line each, remedies deduped below
/// (same doc anchor twice = noise)
fn blocking_summary(report: &Report, theme: &Theme) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let mut remedies: Vec<&str> = Vec::new();
    for cap in report.setup_blockers() {
        out.push_str(&blocker(cap, theme));
        if !remedies.contains(&cap.remedy) {
            remedies.push(cap.remedy);
        }
    }
    for remedy in remedies {
        let data = Fields::new().text("mark", theme.chars.arrow).text("text", remedy);
        let _ = writeln!(out, "{}", draw(&row::step(row::BOUND), &data, theme));
    }
    out
}

/// One blocked capability, named with its finding
fn blocker(cap: &ztest::api::capability::Capability, theme: &Theme) -> String {
    let data = Fields::new().text("text", cap.name).text("detail", cap.finding.detail());
    format!("{}\n", draw(&row::step(row::FAIL), &data, theme))
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

    /// Unicode + no colour: the glyphs under test, none of the escapes
    fn theme() -> Theme {
        Theme::for_capabilities(false, true)
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
        let summary = blocking_summary(&report, &theme());
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
        let summary = blocking_summary(&report, &theme());
        assert!(summary.contains("→ docs/storage.md"), "{summary}");
        assert!(summary.contains("→ docs/registry.md"), "{summary}");
    }
}
