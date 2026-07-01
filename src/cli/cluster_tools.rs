//! Shared host-tool plumbing for the cluster-lifecycle subcommands
//! (`ztest setup` / `ztest cleanup`).
//!
//! These commands drive `kind` and `kubectl` as child processes rather than
//! the kube client: cluster creation has no client, and `kubectl apply` is
//! the canonical way to install the vendored CSI bundle. Every `kubectl` call
//! is pinned to an explicit `--context kind-<name>` so the commands can never
//! mutate whatever production cluster the developer has as their current
//! context.

use std::io::Write;
use std::process::{Command, Stdio};

/// Conventional kube context for a kind cluster named `<name>`.
pub(super) fn kind_context(cluster: &str) -> String {
    format!("kind-{cluster}")
}

/// Verify a host tool is on `PATH`, returning an install hint when it isn't.
/// ztest depends on the developer having these installed; we don't bundle them.
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

/// Require both tools the cluster commands depend on. `helm` is intentionally
/// not required: the CSI bundle is applied with `kubectl`, not Helm.
pub(super) fn require_cluster_tools() -> Result<(), String> {
    require_tool(
        "kind",
        &["version"],
        "install it from https://kind.sigs.k8s.io (e.g. `go install sigs.k8s.io/kind@latest`, `brew install kind`, or your distro package).",
    )?;
    require_tool(
        "kubectl",
        &["version", "--client"],
        "install it from https://kubernetes.io/docs/tasks/tools/ (e.g. `brew install kubectl` or your distro package).",
    )?;
    Ok(())
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

/// Run a `kubectl` subcommand against the kind context, inheriting stdio so
/// the user sees progress. Errors carry the joined argv for context.
pub(super) fn kubectl(cluster: &str, args: &[&str]) -> Result<(), String> {
    let ctx = kind_context(cluster);
    let status = Command::new("kubectl")
        .args(["--context", &ctx])
        .args(args)
        .status()
        .map_err(|e| format!("kubectl {}: {e}", args.join(" ")))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "kubectl --context {ctx} {} exited with {}",
            args.join(" "),
            status.code().unwrap_or(-1)
        ))
    }
}

/// `kubectl apply -f -` against the kind context, piping `manifest` in on
/// stdin. Installs the embedded CSI bundle without writing temp files.
pub(super) fn kubectl_apply_stdin(
    cluster: &str,
    label: &str,
    manifest: &str,
) -> Result<(), String> {
    let ctx = kind_context(cluster);
    eprintln!("  • applying {label}");
    let mut child = Command::new("kubectl")
        .args(["--context", &ctx, "apply", "-f", "-"])
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning kubectl apply ({label}): {e}"))?;
    child
        .stdin
        .take()
        .ok_or_else(|| "kubectl apply did not expose stdin".to_string())?
        .write_all(manifest.as_bytes())
        .map_err(|e| format!("writing manifest to kubectl ({label}): {e}"))?;
    let status = child
        .wait()
        .map_err(|e| format!("waiting on kubectl apply ({label}): {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "kubectl apply ({label}) exited with {}",
            status.code().unwrap_or(-1)
        ))
    }
}
