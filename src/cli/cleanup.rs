//! `ztest cleanup`: tear down the local kind cluster, or just the per-test
//! namespaces it accumulated.
//!
//! Two modes:
//! - **Default** — delete the whole kind cluster via `kind delete cluster`.
//!   The cheapest way to reclaim CSI driver, seeds, and every test
//!   namespace in one operation.
//! - **`--namespaces-only`** — keep the cluster and its CSI + QoS
//!   infrastructure standing; delete only the ztest per-test namespaces
//!   (those labelled `zaino.io/role=test-env`). Fast reset between runs
//!   without re-installing the data plane.
//!
//! Every K8s operation goes through `kube-rs`; only `kind` remains as a
//! subprocess (for full-cluster teardown).

use std::process::ExitCode;

use clap::Parser;
use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, DeleteParams, ListParams};

use crate::qos;

use super::cluster_tools::{kind_cluster_exists, kind_delete, require_kind};

#[derive(Debug, Parser)]
pub struct Args {
    /// Name of the kind cluster to tear down / target.
    #[arg(long, default_value = "zkn")]
    name: String,

    /// Keep the cluster + CSI/QoS infrastructure; delete only the ztest
    /// per-test namespaces (labelled `zaino.io/role=test-env`).
    #[arg(long)]
    namespaces_only: bool,
}

pub fn execute(args: Args) -> ExitCode {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ztest cleanup: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(&args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ztest cleanup: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> Result<(), String> {
    if args.namespaces_only {
        return delete_test_namespaces(&args.name).await;
    }

    require_kind()?;
    if !kind_cluster_exists(&args.name)? {
        eprintln!(
            "• kind cluster `{}` does not exist — nothing to do",
            args.name
        );
        return Ok(());
    }
    eprintln!("• deleting kind cluster `{}`", args.name);
    kind_delete(&args.name)?;
    eprintln!("✓ cluster `{}` deleted", args.name);
    Ok(())
}

/// Delete every ztest per-test namespace via kube-rs. Namespace deletion
/// cascades every namespaced object (pods, PVCs, services, ConfigMaps, the
/// namespaced `VolumeSnapshot`s) — the same cascade the test-env `Drop`
/// path relies on.
///
/// Cluster-scoped shadow `VolumeSnapshotContent`s do NOT cascade with
/// namespaces; we scope by the same test-env role label, then invoke a
/// dynamic delete-collection for VSCs. Together this reproduces exactly
/// what `resource::reap_run` does for a single `run-id`, but across every
/// test-env namespace in the cluster.
async fn delete_test_namespaces(cluster: &str) -> Result<(), String> {
    eprintln!(
        "• deleting ztest test-env namespaces in `{}` (label {}={})",
        cluster,
        qos::LABEL_ROLE,
        qos::ROLE_TEST_ENV,
    );

    let client = crate::cluster::client()
        .await
        .map_err(|e| format!("connect to cluster: {e}"))?;

    let selector = format!("{}={}", qos::LABEL_ROLE, qos::ROLE_TEST_ENV);
    let lp = ListParams::default().labels(&selector);
    let dp = DeleteParams::default();

    // Namespaces advertise only the `delete` verb, never `deletecollection`
    // (see `resource::reap_run` for the same treatment) — list by label
    // and delete each individually.
    let ns_api: Api<Namespace> = Api::all(client.clone());
    let list = ns_api
        .list(&lp)
        .await
        .map_err(|e| format!("list test-env namespaces: {e}"))?;

    if list.items.is_empty() {
        eprintln!("  · no test-env namespaces to delete");
        eprintln!("✓ per-test namespaces cleared (cluster + infrastructure kept)");
        return Ok(());
    }

    let mut errors = Vec::new();
    for ns in list.items {
        let Some(name) = ns.metadata.name.as_deref() else {
            continue;
        };
        match ns_api.delete(name, &dp).await {
            Ok(_) => eprintln!("  ✓ deleted namespace {name}"),
            Err(e) if is_not_found(&e) => {
                eprintln!("  · namespace {name} already gone");
            }
            Err(e) => {
                let msg = format!("delete namespace {name}: {e}");
                eprintln!("  ✗ {msg}");
                errors.push(msg);
            }
        }
    }

    if !errors.is_empty() {
        return Err(format!(
            "{} namespace(s) failed to delete; see `✗ …` lines above",
            errors.len()
        ));
    }

    eprintln!("✓ per-test namespaces cleared (cluster + infrastructure kept)");
    Ok(())
}

/// A 404 counts as success for idempotent teardown. Mirrors the same helper
/// used by [`crate::resource::reap_run`] — repeated here to keep
/// `resource::kube::is_not_found` `pub(crate)` inside `resource/`.
fn is_not_found(err: &kube::Error) -> bool {
    match err {
        kube::Error::Api(resp) => resp.code == 404,
        other => {
            let s = other.to_string();
            s.contains("not found") || s.contains("404")
        }
    }
}
