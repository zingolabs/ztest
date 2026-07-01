//! `ztest snapshot {list,prune,warm}`: manage the content-addressed seed cache
//! in the `zaino-seeds` namespace.
//!
//! A "seed" is a `seed-<sha8>` PVC populated once from a local archive and
//! paired with a `VolumeSnapshot`; tests clone it copy-on-write (see
//! `materialize.rs` / `seeds.rs`). These subcommands inspect the cache
//! (`list`), reclaim it (`prune`), and pre-populate it without running a test
//! (`warm`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use kube::api::{Api, ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams};
use kube::{Client, ResourceExt};

use crate::materialize::Payload;
use crate::seeds::{SEEDS_NAMESPACE, sha8};
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};

const READY_LABEL: &str = "seeds.zaino.io/ready";
const SEED_PREFIX: &str = "seed-";

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    cmd: SnapshotCmd,
}

#[derive(Debug, Subcommand)]
enum SnapshotCmd {
    /// List the seed PVCs and their snapshot/ready state.
    List,

    /// Delete cached seeds (PVCs + paired VolumeSnapshots) and any
    /// orphaned cluster-scoped shadow VolumeSnapshotContents.
    Prune(PruneArgs),

    /// Pre-materialize one or more local archives into seeds without
    /// running a test.
    Warm(WarmArgs),
}

#[derive(Debug, Parser)]
struct PruneArgs {
    /// Delete every seed in the cache.
    #[arg(long)]
    all: bool,

    /// Specific seed sha8 prefixes to delete (e.g. `4c86ea3c`). The
    /// `seed-` prefix is optional.
    shas: Vec<String>,
}

#[derive(Debug, Parser)]
struct WarmArgs {
    /// Archive file(s) to materialize. Treated as compressed tar
    /// archives (extracted into the seed); content-addressed by hash.
    #[arg(required = true)]
    archives: Vec<PathBuf>,
}

pub fn execute(args: Args) -> ExitCode {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ztest snapshot: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let result = rt.block_on(async {
        let client = crate::cluster::client()
            .await
            .map_err(|e| format!("connecting to cluster: {e}"))?;
        match args.cmd {
            SnapshotCmd::List => list(&client).await,
            SnapshotCmd::Prune(p) => prune(&client, &p).await,
            SnapshotCmd::Warm(w) => warm(&client, &w).await,
        }
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ztest snapshot: {e}");
            ExitCode::FAILURE
        }
    }
}

fn volume_snapshot_ar() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshot".into(),
    })
}

fn volume_snapshot_content_ar() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshotContent".into(),
    })
}

/// Seed PVCs in the namespace, by name (`seed-<sha8>`).
async fn seed_pvcs(client: &Client) -> Result<Vec<PersistentVolumeClaim>, String> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let list = api
        .list(&ListParams::default())
        .await
        .map_err(|e| format!("listing seed PVCs: {e}"))?;
    Ok(list
        .items
        .into_iter()
        .filter(|p| p.name_any().starts_with(SEED_PREFIX))
        .collect())
}

async fn list(client: &Client) -> Result<(), String> {
    let pvcs = seed_pvcs(client).await?;
    if pvcs.is_empty() {
        println!("No seeds in {SEEDS_NAMESPACE}.");
        return Ok(());
    }
    let snap_api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &volume_snapshot_ar());
    println!("{:<20} {:<8} {:<10} SNAPSHOT", "SEED", "READY", "SIZE");
    for pvc in &pvcs {
        let name = pvc.name_any();
        let ready = pvc
            .labels()
            .get(READY_LABEL)
            .map(|v| v == "true")
            .unwrap_or(false);
        let size = pvc
            .spec
            .as_ref()
            .and_then(|s| s.resources.as_ref())
            .and_then(|r| r.requests.as_ref())
            .and_then(|m| m.get("storage"))
            .map(|q| q.0.clone())
            .unwrap_or_else(|| "?".into());
        let snap = match snap_api.get_opt(&name).await {
            Ok(Some(s)) => {
                let bound = s.data["status"]["readyToUse"].as_bool().unwrap_or(false);
                if bound { "ready" } else { "pending" }
            }
            Ok(None) => "missing",
            Err(_) => "?",
        };
        println!(
            "{:<20} {:<8} {:<10} {}",
            name,
            if ready { "yes" } else { "no" },
            size,
            snap
        );
    }
    Ok(())
}

async fn prune(client: &Client, args: &PruneArgs) -> Result<(), String> {
    if !args.all && args.shas.is_empty() {
        return Err("nothing selected — pass `--all` or one or more seed sha8 prefixes.".into());
    }
    let pvcs = seed_pvcs(client).await?;
    let targets: Vec<String> = pvcs
        .iter()
        .map(|p| p.name_any())
        .filter(|name| {
            args.all
                || args.shas.iter().any(|s| {
                    let want = s.trim_start_matches(SEED_PREFIX);
                    name.trim_start_matches(SEED_PREFIX).starts_with(want)
                })
        })
        .collect();

    if targets.is_empty() {
        println!("No matching seeds to prune.");
        return Ok(());
    }

    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let snap_api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &volume_snapshot_ar());
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let dp = DeleteParams::default();
    for name in &targets {
        // Drop any leftover uploader pod first: a crashed/stale materialization
        // can leave one mounting the PVC, which would otherwise block the PVC
        // delete on its mount finalizer.
        let uploader = name.replace(SEED_PREFIX, "uploader-");
        match pod_api.delete(&uploader, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => eprintln!("  ! deleting uploader pod {uploader}: {e}"),
        }
        // Snapshot next so its content can be released before the PVC.
        match snap_api.delete(name, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => return Err(format!("deleting VolumeSnapshot {name}: {e}")),
        }
        match pvc_api.delete(name, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => return Err(format!("deleting PVC {name}: {e}")),
        }
        println!("pruned {name}");
    }

    // Sweep orphaned cluster-scoped shadow VolumeSnapshotContents: their
    // `Retain` policy means a crashed test can leave them behind.
    let vsc_api: Api<DynamicObject> = Api::all_with(client.clone(), &volume_snapshot_content_ar());
    if let Ok(vscs) = vsc_api.list(&ListParams::default()).await {
        for vsc in vscs.items {
            let n = vsc.name_any();
            if n.starts_with("shadow-vsc-") {
                match vsc_api.delete(&n, &dp).await {
                    Ok(_) => println!("pruned orphan {n}"),
                    Err(kube::Error::Api(e)) if e.code == 404 => {}
                    Err(e) => eprintln!("  ! deleting {n}: {e}"),
                }
            }
        }
    }
    Ok(())
}

async fn warm(client: &Client, args: &WarmArgs) -> Result<(), String> {
    for archive in &args.archives {
        if !archive.exists() {
            return Err(format!("archive not found: {}", archive.display()));
        }
        let sha = sha8(archive).map_err(|e| format!("hashing {}: {e}", archive.display()))?;
        eprintln!("• warming seed-{sha} from {}", archive.display());
        crate::materialize::ensure_seed(client, archive, Payload::Archive)
            .await
            .map_err(|e| format!("materializing {}: {e}", archive.display()))?;
        println!("ready seed-{sha}");
    }
    Ok(())
}
