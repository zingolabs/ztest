//! `ztest cleanup`: reclaim the test resources a developer's runs left behind.
//!
//! Scoped to *test resources*, never cluster lifecycle — deleting the cluster
//! itself is `kind delete cluster`, run by hand. Two scopes:
//!
//! - **Default** — reclaim only the caller's resources: every per-test
//!   namespace, shadow `VolumeSnapshotContent`, and QoS reservation Lease
//!   labelled `ztest.io/user=$USER`.
//! - **`--all-users`** — reclaim every developer's resources. Needs an admin
//!   ServiceAccount able to list/delete cluster-wide; without it the deletes
//!   come back as RBAC errors.
//!
//! Either way the shared data plane (CSI, snapshot classes, QoS namespace,
//! RBAC) and content-addressed caches (dev images, seed PVCs) stay put. Every
//! K8s operation goes through `kube-rs`.

use std::process::ExitCode;

use clap::Parser;

#[derive(Debug, Parser)]
pub struct Args {
    /// Reclaim every developer's test resources, not just your own. Requires an
    /// admin ServiceAccount with cluster-wide list/delete.
    #[arg(long)]
    all_users: bool,
}

pub fn execute(args: Args) -> ExitCode {
    super::block_on("cleanup", super::Rt::Multi, run(&args))
}

async fn run(args: &Args) -> Result<(), String> {
    let client = crate::cluster::client()
        .await
        .map_err(|e| format!("connect to cluster: {e}"))?;

    let errors = if args.all_users {
        eprintln!("• reclaiming test resources for all users");
        crate::resource::reap_all(&client).await
    } else {
        let user = crate::naming::RunCoords::from_env()
            .map(|c| c.user)
            .map_err(|e| format!("resolve invoking user: {e}"))?;
        eprintln!("• reclaiming test resources owned by `{user}`");
        crate::resource::reap_user(&client, &user).await
    };

    if errors.is_empty() {
        eprintln!("✓ test resources reclaimed (cluster + shared infrastructure kept)");
        return Ok(());
    }
    for e in &errors {
        eprintln!("  ✗ {e}");
    }
    Err(format!(
        "{} resource(s) failed to reclaim; see `✗ …` lines above",
        errors.len()
    ))
}
