//! `ztest setup`: bring up a local kind cluster ready to run the ztest
//! integration suites, in one command.
//!
//! A production Ceph cluster gives ztest's seed/archive machinery
//! (`materialize.rs` -> `seeds.rs` -> `mounts.rs`) CSI VolumeSnapshot + clone
//! support. A stock kind cluster has only the `standard` local-path
//! provisioner, which can't snapshot, so archive-backed tests hang and time
//! out. `ztest setup` closes that gap:
//!
//! 1. Create the kind cluster (idempotent; skipped if it exists).
//! 2. Install the vendored, version-pinned snapshot bundle (`fixtures/kind/`,
//!    embedded via `include_str!`): external-snapshotter CRDs + controller,
//!    the csi-driver-host-path driver + RBAC, and the StorageClasses /
//!    VolumeSnapshotClass ztest's defaults expect.
//! 3. Label the node for QoS NVMe placement and create the `zaino-seeds` /
//!    `zaino-qos` namespaces.
//!
//! The StorageClass / VolumeSnapshotClass names match the production Ceph
//! names, so the identical materialization code path runs on kind and Ceph
//! (see `fixtures/kind/README.md`).

use std::process::ExitCode;

use clap::Parser;

use super::cluster_tools::{
    kind_cluster_exists, kind_context, kubectl, kubectl_apply_stdin, require_cluster_tools,
};

/// Apply order matters: CRDs must be Established before the VolumeSnapshotClass
/// in `40` and before the driver's snapshotter sidecar starts.
const SNAPSHOT_CRDS: &str = include_str!("../../fixtures/kind/00-snapshot-crds.yaml");
const SNAPSHOT_CONTROLLER: &str = include_str!("../../fixtures/kind/10-snapshot-controller.yaml");
const CSI_RBAC: &str = include_str!("../../fixtures/kind/20-csi-hostpath-rbac.yaml");
const CSI_DRIVER: &str = include_str!("../../fixtures/kind/30-csi-hostpath-driver.yaml");
const ZTEST_CLASSES: &str = include_str!("../../fixtures/kind/40-ztest-classes.yaml");

/// `zaino-seeds` holds the content-addressed seed PVCs; `zaino-qos` holds the
/// QoS reservation/job bookkeeping. Both created up front so the first test
/// run doesn't race their creation.
const NAMESPACES: &str = "\
apiVersion: v1
kind: Namespace
metadata:
  name: zaino-seeds
---
apiVersion: v1
kind: Namespace
metadata:
  name: zaino-qos
";

#[derive(Debug, Parser)]
pub struct Args {
    /// Name of the kind cluster to create / target.
    #[arg(long, default_value = "zkn")]
    name: String,

    /// Skip waiting for the CSI driver + snapshot controller to roll out. The
    /// first archive-backed test then blocks on them instead.
    #[arg(long)]
    no_wait: bool,
}

pub fn execute(args: Args) -> ExitCode {
    match run(&args) {
        Ok(()) => {
            eprintln!(
                "\n✓ cluster `{}` ready. Context: {}\n  Run tests with: ztest run -E 'test(...)'",
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

fn run(args: &Args) -> Result<(), String> {
    require_cluster_tools()?;

    // 1. Cluster.
    if kind_cluster_exists(&args.name)? {
        eprintln!("• kind cluster `{}` already exists — reusing", args.name);
    } else {
        eprintln!("• creating kind cluster `{}`", args.name);
        let status = std::process::Command::new("kind")
            .args(["create", "cluster", "--name", &args.name])
            .status()
            .map_err(|e| format!("`kind create cluster` failed to start: {e}"))?;
        if !status.success() {
            return Err(format!(
                "`kind create cluster --name {}` exited with {}",
                args.name,
                status.code().unwrap_or(-1)
            ));
        }
    }

    // 2. Snapshot bundle, in dependency order.
    eprintln!("• installing CSI snapshot support");
    kubectl_apply_stdin(&args.name, "snapshot CRDs", SNAPSHOT_CRDS)?;
    // The VolumeSnapshotClass in `40` and the driver's snapshotter sidecar
    // both need the snapshot CRDs Established first.
    kubectl(
        &args.name,
        &[
            "wait",
            "--for=condition=established",
            "--timeout=60s",
            "crd/volumesnapshots.snapshot.storage.k8s.io",
            "crd/volumesnapshotcontents.snapshot.storage.k8s.io",
            "crd/volumesnapshotclasses.snapshot.storage.k8s.io",
        ],
    )?;
    kubectl_apply_stdin(&args.name, "snapshot controller", SNAPSHOT_CONTROLLER)?;
    kubectl_apply_stdin(&args.name, "CSI driver RBAC", CSI_RBAC)?;
    kubectl_apply_stdin(&args.name, "CSI hostpath driver", CSI_DRIVER)?;
    kubectl_apply_stdin(&args.name, "storage + snapshot classes", ZTEST_CLASSES)?;

    // 3. QoS node label + namespaces.
    eprintln!("• labelling node + creating namespaces");
    kubectl(
        &args.name,
        &[
            "label",
            "nodes",
            "--all",
            &format!(
                "{}={}",
                crate::qos::NVME_NODE_LABEL_KEY,
                crate::qos::NVME_NODE_LABEL_VALUE
            ),
            "--overwrite",
        ],
    )?;
    kubectl_apply_stdin(&args.name, "ztest namespaces", NAMESPACES)?;

    // 4. Wait for the data plane so the first test doesn't race it.
    if !args.no_wait {
        eprintln!("• waiting for CSI driver + snapshot controller to be ready");
        // Best-effort: surface a timeout, but the cluster is still usable once
        // the pods settle, so don't hard-fail.
        if let Err(e) = kubectl(
            &args.name,
            &[
                "rollout",
                "status",
                "deploy/snapshot-controller",
                "-n",
                "kube-system",
                "--timeout=180s",
            ],
        ) {
            eprintln!("  ! {e} (continuing — it may still settle)");
        }
        if let Err(e) = kubectl(
            &args.name,
            &[
                "rollout",
                "status",
                "statefulset/csi-hostpathplugin",
                "-n",
                "default",
                "--timeout=180s",
            ],
        ) {
            eprintln!("  ! {e} (continuing — it may still settle)");
        }
    }

    Ok(())
}
