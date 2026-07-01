//! `ztest cleanup`: tear down the local kind cluster, or just the per-test
//! namespaces it accumulated.
//!
//! Default: delete the whole kind cluster (the cheapest way to reclaim CSI
//! driver, seeds, and test namespaces). `--namespaces-only` keeps the cluster
//! and its CSI bundle standing and deletes only the ztest test-env namespaces,
//! for a fast reset between runs without re-installing the data plane.

use std::process::{Command, ExitCode};

use clap::Parser;

use super::cluster_tools::{kind_cluster_exists, kubectl, require_tool};

#[derive(Debug, Parser)]
pub struct Args {
    /// Name of the kind cluster to tear down / target.
    #[arg(long, default_value = "zkn")]
    name: String,

    /// Keep the cluster + CSI bundle; delete only the ztest per-test
    /// namespaces (those labelled `zaino.io/role=test-env`).
    #[arg(long)]
    namespaces_only: bool,
}

pub fn execute(args: Args) -> ExitCode {
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ztest cleanup: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<(), String> {
    if args.namespaces_only {
        require_tool(
            "kubectl",
            &["version", "--client"],
            "install it from https://kubernetes.io/docs/tasks/tools/.",
        )?;
        eprintln!("• deleting ztest test-env namespaces in `{}`", args.name);
        kubectl(
            &args.name,
            &[
                "delete",
                "namespace",
                "-l",
                &format!("{}={}", crate::qos::LABEL_ROLE, crate::qos::ROLE_TEST_ENV),
                "--ignore-not-found",
            ],
        )?;
        eprintln!("✓ per-test namespaces cleared (cluster + CSI bundle kept)");
        return Ok(());
    }

    require_tool(
        "kind",
        &["version"],
        "install it from https://kind.sigs.k8s.io.",
    )?;
    if !kind_cluster_exists(&args.name)? {
        eprintln!(
            "• kind cluster `{}` does not exist — nothing to do",
            args.name
        );
        return Ok(());
    }
    eprintln!("• deleting kind cluster `{}`", args.name);
    let status = Command::new("kind")
        .args(["delete", "cluster", "--name", &args.name])
        .status()
        .map_err(|e| format!("`kind delete cluster` failed to start: {e}"))?;
    if !status.success() {
        return Err(format!(
            "`kind delete cluster --name {}` exited with {}",
            args.name,
            status.code().unwrap_or(-1)
        ));
    }
    eprintln!("✓ cluster `{}` deleted", args.name);
    Ok(())
}
