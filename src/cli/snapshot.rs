//! `ztest snapshot {list,prune,warm}`: the seed cache in `ztest-seeds`.
//!
//! - Seed = `seed-<sha8>-<driver>` PVC populated once from a local archive + paired
//!   `VolumeSnapshot`; tests clone it copy-on-write (`materialize.rs` / `seeds.rs`)
//! - Keyed on content *and* driver → `list` reports `DRIVER this|other` and seeds
//!   for a driver this cluster no longer uses are inert, never selected
//! - `list` inspects, `prune` reclaims, `warm` pre-populates without a test run

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use kube::api::{Api, ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams};
use kube::{Client, ResourceExt};

use crate::seeds::SEEDS_NAMESPACE;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};

const READY_LABEL: &str = "seeds.ztest.io/ready";
const DRIVER_LABEL: &str = "seeds.ztest.io/driver";
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
    /// orphaned cluster-scoped seed-binding VolumeSnapshotContents.
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
    super::block_on("snapshot", super::Rt::Current, async {
        let client =
            crate::cluster::client().await.map_err(|e| format!("connecting to cluster: {e}"))?;
        match args.cmd {
            SnapshotCmd::List => list(&client).await,
            SnapshotCmd::Prune(p) => prune(&client, &p).await,
            SnapshotCmd::Warm(w) => warm(&client, &w).await,
        }
    })
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

/// Seed PVCs in the namespace, by `seed-<sha8>` name
async fn seed_pvcs(client: &Client) -> Result<Vec<PersistentVolumeClaim>, String> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let list =
        api.list(&ListParams::default()).await.map_err(|e| format!("listing seed PVCs: {e}"))?;
    Ok(list.items.into_iter().filter(|p| p.name_any().starts_with(SEED_PREFIX)).collect())
}

async fn list(client: &Client) -> Result<(), String> {
    let pvcs = seed_pvcs(client).await?;
    if pvcs.is_empty() {
        println!("No seeds in {SEEDS_NAMESPACE}.");
        return Ok(());
    }
    let snap_api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &volume_snapshot_ar());
    // Seeds published on another driver still list: they are inert here, not broken,
    // and a run switched back to that driver reuses them
    let ours = crate::resource::selected_storage(client)
        .await
        .map(|s| crate::naming::slug(&s.provisioner, crate::naming::DNS_LABEL_MAX))
        .unwrap_or_default();
    println!("{:<38} {:<8} {:<10} {:<9} SNAPSHOT", "SEED", "READY", "SIZE", "DRIVER");
    for pvc in &pvcs {
        let name = pvc.name_any();
        let ready = pvc.labels().get(READY_LABEL).map(|v| v == "true").unwrap_or(false);
        // Pre-`driver`-label seeds carry no driver → unknown, not "other"
        let driver = match pvc.labels().get(DRIVER_LABEL) {
            None => "?",
            Some(d) if *d == ours => "this",
            Some(_) => "other",
        };
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
            "{:<38} {:<8} {:<10} {:<9} {}",
            name,
            if ready { "yes" } else { "no" },
            size,
            driver,
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
        // Leftover uploader pod first: a crashed materialization leaves one mounting
        // the PVC, blocking its delete on the mount finalizer
        let uploader = name.replace(SEED_PREFIX, "uploader-");
        match pod_api.delete(&uploader, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => eprintln!("  ! deleting uploader pod {uploader}: {e}"),
        }
        // Snapshot next → its content releases before the PVC
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

    // Orphaned cluster-scoped seed-binding contents (`Retain` → a crashed test leaves
    // them). Matched by name prefix, not label: sweep of last resort, must catch a
    // content whose labels never landed. Always safe — `Retain` means the backend
    // snapshot belongs to the seed, not the binding
    let vsc_api: Api<DynamicObject> = Api::all_with(client.clone(), &volume_snapshot_content_ar());
    if let Ok(vscs) = vsc_api.list(&ListParams::default()).await {
        for vsc in vscs.items {
            let n = vsc.name_any();
            if n.starts_with(crate::seeds::BINDING_PREFIX) {
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

/// Pre-provision seeds for the named archives.
///
/// Archive file never opened: identity from the sidecar manifest, bytes from the
/// bucket → works in a checkout that never ran `git lfs pull` (same property that
/// lets a build pod declare a seed it cannot read)
async fn warm(client: &Client, args: &WarmArgs) -> Result<(), String> {
    for archive in &args.archives {
        let (name, oid, size) = crate::archive::identity_from_manifest(archive)?;
        let entry = crate::inventory::SeedEntry {
            name,
            oid,
            size,
            payload: crate::inventory::SeedPayload::Archive,
        };
        eprintln!("• warming seed {} from {}", crate::storage::seed_sha8(&entry.oid), entry.name);
        // No panel to paint: the pull's sub-phases have nowhere to land, and `warm`
        // already brackets each seed
        // Name comes back on the handle: only `provision_seed` knows the driver half
        let handle = crate::materialize::provision_seed(client, &entry, &Default::default())
            .await
            .map_err(|e| format!("materializing {}: {e}", entry.name))?;
        println!("ready {}", handle.seed_pvc);
    }
    Ok(())
}
