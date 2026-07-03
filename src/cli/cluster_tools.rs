//! Host-tool plumbing for the cluster-lifecycle subcommands
//! (`ztest setup` / `ztest cleanup`).
//!
//! Down to just `kind`: create / delete / exists. Every K8s operation
//! goes through [`crate::resource`] and `kube-rs`; there is no subprocess
//! kubectl anywhere in the tree.

use std::process::{Command, Stdio};

/// Verify a host tool is on `PATH`, returning an install hint when it
/// isn't. ztest depends on the developer having these installed — we
/// don't bundle them.
pub(super) fn require_tool(bin: &str, probe: &[&str], hint: &str) -> Result<(), String> {
    match Command::new(bin)
        .args(probe)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        // Present: even a non-zero exit means the binary ran.
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(format!("ztest needs `{bin}` on your PATH — {hint}"))
        }
        Err(e) => Err(format!("could not run `{bin}`: {e}")),
    }
}

/// Require `kind` for cluster create/delete. `kubectl` is not required:
/// every K8s operation ztest performs goes through `kube-rs`.
pub(super) fn require_kind() -> Result<(), String> {
    require_tool(
        "kind",
        &["version"],
        "install it from https://kind.sigs.k8s.io (e.g. `go install sigs.k8s.io/kind@latest`, `brew install kind`, or your distro package).",
    )
}

/// Conventional kube context for a kind cluster named `<name>`. Used in
/// user-facing messages ("kubectl config use-context ..."); ztest itself
/// never selects contexts.
pub(super) fn kind_context(cluster: &str) -> String {
    format!("kind-{cluster}")
}

/// `true` if a kind cluster named `cluster` already exists.
pub(super) fn kind_cluster_exists(cluster: &str) -> Result<bool, String> {
    let out = Command::new("kind")
        .args(["get", "clusters"])
        .output()
        .map_err(|e| format!("`kind get clusters` failed: {e}"))?;
    let list = String::from_utf8_lossy(&out.stdout);
    Ok(list.lines().any(|l| l.trim() == cluster))
}

/// Create a kind cluster (inherits stdio so the user sees `kind`'s own
/// progress UI, which is already excellent).
pub(super) fn kind_create(cluster: &str) -> Result<(), String> {
    let status = Command::new("kind")
        .args(["create", "cluster", "--name", cluster])
        .status()
        .map_err(|e| format!("`kind create cluster` failed to start: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "`kind create cluster --name {cluster}` exited with {}",
            status.code().unwrap_or(-1)
        ))
    }
}

/// Delete a kind cluster (inherits stdio so the user sees `kind`'s
/// progress).
pub(super) fn kind_delete(cluster: &str) -> Result<(), String> {
    let status = Command::new("kind")
        .args(["delete", "cluster", "--name", cluster])
        .status()
        .map_err(|e| format!("`kind delete cluster` failed to start: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "`kind delete cluster --name {cluster}` exited with {}",
            status.code().unwrap_or(-1)
        ))
    }
}
