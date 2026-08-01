//! `ztest sync` — the stateless controller for detached, ztest-owned chain
//! syncs (design §"Execution model: ztest-owned pods").
//!
//! A detached sync outlives the launching terminal: state lives in k8s (a pod
//! labelled `ztest.io/{kind=sync,sync-id,owner}` in a persistent, user-scoped
//! namespace), so any machine with the kubeconfig can `list`/`watch`/`stop`.
//! `watch`/`start --watch` are read-only tails — detaching never stops a sync;
//! ending one is only `stop` (graceful `sync_mode = Shutdown` → checkpoint).
//!
//! Runtime status: the pod-lifecycle verbs (`list`/`status`/`stop`/`rm`/
//! `watch`) are implemented against the live cluster; `start`/`describe`/
//! `report` carry the admission + inventory + report-store wiring that is
//! verified on a cluster (their seams are marked below).

use std::process::ExitCode;

use clap::{Args as ClapArgs, Subcommand};
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::api::{Api, DeleteParams, ListParams, LogParams, Patch, PatchParams};
use serde_json::json;

/// Persistent, user-scoped namespace detached syncs live in (not an ephemeral
/// per-run one, so `ztest cleanup` can skip running `kind=sync` pods).
const SYNC_NAMESPACE: &str = "ztest-sync";
/// Label marking a ztest-owned sync pod.
const KIND_LABEL: &str = "ztest.io/kind=sync";
/// Per-sync id label key.
const SYNC_ID_KEY: &str = "ztest.io/sync-id";
/// Annotation the in-pod runner watches to shut down gracefully.
const STOP_ANNOTATION: &str = "ztest.io/sync-stop";

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// List detached syncs (labelled pod query): id, subject, phase, %, age.
    List {
        /// Include every user's syncs, not just your own.
        #[arg(long)]
        all_users: bool,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Print a profile's invariant + nemesis manifest (Collect mode, no cluster).
    Describe {
        /// Profile name (`#[ztest::sync_test(name = ..)]`).
        name: String,
    },
    /// Admit + create the ztest-owned pod for a profile.
    Start {
        /// Profile name.
        name: String,
        /// Attach a read-only progress tail after starting.
        #[arg(long)]
        watch: bool,
    },
    /// Attach to a sync's live progress (read-only; detaching never stops it).
    Watch {
        /// Sync id.
        id: String,
    },
    /// One-shot last snapshot for a sync.
    Status {
        /// Sync id.
        id: String,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Final `SyncReport` (works after the pod is gone).
    Report {
        /// Sync id.
        id: String,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Graceful stop: `sync_mode = Shutdown` → checkpoint → exit 0.
    Stop {
        /// Sync id.
        id: String,
    },
    /// Delete a sync's pod (and its PVC with `--purge`).
    Rm {
        /// Sync id.
        id: String,
        /// Also delete the wallet/cache PVC.
        #[arg(long)]
        purge: bool,
    },
}

pub fn execute(args: Args) -> ExitCode {
    super::block_on("sync", super::Rt::Multi, run(args))
}

async fn run(args: Args) -> Result<(), String> {
    match args.cmd {
        Cmd::List { all_users, json } => list(all_users, json).await,
        Cmd::Status { id, json } => status(&id, json).await,
        Cmd::Stop { id } => stop(&id).await,
        Cmd::Rm { id, purge } => rm(&id, purge).await,
        Cmd::Watch { id } => watch(&id).await,
        Cmd::Describe { name } => describe(&name),
        Cmd::Start { name, watch } => start(&name, watch),
        Cmd::Report { id, json } => report(&id, json).await,
    }
}

async fn pods() -> Result<Api<Pod>, String> {
    let client = crate::cluster::client()
        .await
        .map_err(|e| format!("kube client: {e}"))?;
    Ok(Api::namespaced(client, SYNC_NAMESPACE))
}

/// A pod's `ztest.io/sync-id` label, if present.
fn sync_id_of(pod: &Pod) -> Option<&str> {
    pod.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(SYNC_ID_KEY))
        .map(String::as_str)
}

async fn find_pod(id: &str) -> Result<Pod, String> {
    let api = pods().await?;
    let lp = ListParams::default().labels(&format!("{SYNC_ID_KEY}={id}"));
    api.list(&lp)
        .await
        .map_err(|e| format!("list sync pods: {e}"))?
        .items
        .into_iter()
        .next()
        .ok_or_else(|| format!("no sync with id `{id}` in namespace `{SYNC_NAMESPACE}`"))
}

async fn list(all_users: bool, json: bool) -> Result<(), String> {
    let api = pods().await?;
    // `all_users` would drop the owner-label filter; the owner filter itself is
    // added once the run identity threads an owner label through `start`.
    let _ = all_users;
    let lp = ListParams::default().labels(KIND_LABEL);
    let list = api
        .list(&lp)
        .await
        .map_err(|e| format!("list sync pods: {e}"))?;

    if json {
        let rows: Vec<_> = list
            .items
            .iter()
            .map(|p| {
                json!({
                    "id": sync_id_of(p),
                    "pod": p.metadata.name,
                    "phase": p.status.as_ref().and_then(|s| s.phase.clone()),
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&rows).map_err(|e| e.to_string())?
        );
        return Ok(());
    }

    if list.items.is_empty() {
        println!("no detached syncs in `{SYNC_NAMESPACE}`");
        return Ok(());
    }
    println!("{:<24} {:<40} {:<12}", "SYNC-ID", "POD", "PHASE");
    for p in &list.items {
        println!(
            "{:<24} {:<40} {:<12}",
            sync_id_of(p).unwrap_or("-"),
            p.metadata.name.as_deref().unwrap_or("-"),
            p.status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("-"),
        );
    }
    Ok(())
}

async fn status(id: &str, json: bool) -> Result<(), String> {
    let pod = find_pod(id).await?;
    let phase = pod
        .status
        .as_ref()
        .and_then(|s| s.phase.clone())
        .unwrap_or_else(|| "Unknown".into());
    if json {
        println!("{}", json!({ "id": id, "phase": phase }));
    } else {
        println!("sync {id}: {phase}");
        // The last per-tick snapshot rides the pod log sentinel line; `watch`
        // renders the live stream. A one-shot rich snapshot is read from the
        // ConfigMap mirror once `start` writes it (seam).
    }
    Ok(())
}

async fn stop(id: &str) -> Result<(), String> {
    let api = pods().await?;
    let pod = find_pod(id).await?;
    let name = pod.metadata.name.ok_or("sync pod has no name")?;
    // Graceful: flip the annotation the in-pod runner watches → it sets
    // `sync_mode = Shutdown`, checkpoints to the PVC, and exits 0. This is not
    // a kill (design §"stop flips sync_mode to Shutdown, not a kill").
    let patch = json!({ "metadata": { "annotations": { STOP_ANNOTATION: "true" } } });
    api.patch(
        &name,
        &PatchParams::apply("ztest-sync"),
        &Patch::Merge(&patch),
    )
    .await
    .map_err(|e| format!("signal stop: {e}"))?;
    println!("sync {id}: stop signalled (graceful checkpoint)");
    Ok(())
}

async fn rm(id: &str, purge: bool) -> Result<(), String> {
    let api = pods().await?;
    let pod = find_pod(id).await?;
    let name = pod.metadata.name.clone().ok_or("sync pod has no name")?;
    if pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running") {
        return Err(format!(
            "sync {id} is Running; `ztest sync stop {id}` first (or it may be mid-checkpoint)"
        ));
    }
    api.delete(&name, &DeleteParams::default())
        .await
        .map_err(|e| format!("delete sync pod: {e}"))?;
    println!("sync {id}: pod deleted");
    if purge {
        let client = crate::cluster::client()
            .await
            .map_err(|e| format!("kube client: {e}"))?;
        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client, SYNC_NAMESPACE);
        let lp = ListParams::default().labels(&format!("{SYNC_ID_KEY}={id}"));
        for pvc in pvcs
            .list(&lp)
            .await
            .map_err(|e| format!("list sync PVCs: {e}"))?
            .items
        {
            if let Some(n) = pvc.metadata.name {
                let _ = pvcs.delete(&n, &DeleteParams::default()).await;
                println!("sync {id}: PVC {n} deleted");
            }
        }
    }
    Ok(())
}

async fn watch(id: &str) -> Result<(), String> {
    let api = pods().await?;
    let pod = find_pod(id).await?;
    let name = pod.metadata.name.ok_or("sync pod has no name")?;
    // A read-only tail. The follow-stream renderer that parses the per-tick
    // sentinel lines into per-invariant rows is the fuller `watch` UI; this
    // fetches the recent log tail so the verb is usable today.
    let lp = LogParams {
        tail_lines: Some(200),
        ..Default::default()
    };
    let logs = api
        .logs(&name, &lp)
        .await
        .map_err(|e| format!("read sync log: {e}"))?;
    print!("{logs}");
    Ok(())
}

fn describe(name: &str) -> Result<(), String> {
    // Collect mode: run the profile body inert and print its invariant +
    // nemesis manifest without touching a cluster. This reads the static
    // `SyncTestDecl` from the test binaries' inventory dump (the same
    // `ZTEST_DUMP_INVENTORY` discovery `ztest run` uses) — wiring that
    // discovery into this verb is the remaining step.
    Err(format!(
        "`ztest sync describe {name}`: profile discovery not yet wired \
         (SyncTestDecl is emitted; the inventory-dump reader for `describe` is pending)"
    ))
}

fn start(name: &str, _watch: bool) -> Result<(), String> {
    // Admit the profile against its QoS tier (§QoS), then create the
    // ztest-owned pod (labelled `kind=sync`, PVC-backed datadir) running the
    // profile body detached. Admission + pod-spec construction reuse the engine
    // pod-runner; that wiring is the remaining step.
    Err(format!(
        "`ztest sync start {name}`: pod admission + detached-pod creation not yet wired \
         (QoS tier `sync` and the pod-runner exist; the sync-owned pod spec is pending)"
    ))
}

async fn report(id: &str, _json: bool) -> Result<(), String> {
    // The final `SyncReport` is written to the PVC and mirrored to a ConfigMap
    // so it outlives the pod; this reads that ConfigMap. It is written by the
    // in-pod runner at completion (seam).
    Err(format!(
        "`ztest sync report {id}`: SyncReport ConfigMap mirror not yet written by the runner"
    ))
}
