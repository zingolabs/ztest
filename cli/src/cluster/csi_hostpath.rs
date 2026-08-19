//! Optional csi-hostpath install — `ztest cluster setup`, snapshot-less local cluster.
//!
//! - Local only (shared-cluster storage = operator's)
//! - Assent-gated (cluster-scoped writes + steals the default StorageClass)
//! - Upstream manifests @ pinned refs, deploy dir = server minor

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::Command;

use dialoguer::Confirm;

use ztest::api::capability::Report;

const SNAPSHOTTER_REF: &str = "v8.6.0";
const HOSTPATH_REF: &str = "v1.18.0";
const STORAGE_CLASS: &str = "csi-hostpath-sc";
const DEFAULT_CLASS_ANNOTATION: &str = "storageclass.kubernetes.io/is-default-class";

/// Gaps this install closes
const FIXABLE: [&str; 2] = ["snapshot-capable storage", "VolumeSnapshot v1 API"];

/// FIXABLE pair = one gap to a reader (probe names report the harness, not the cluster)
const GAP: &str = "no volume snapshots";

/// Subprocesses [`install`] drives
const TOOLS: [&str; 2] = ["kubectl", "git"];

/// How the local snapshot gap resolved, ahead of any capability report.
///
/// - `Explained` = gap + its fix already on stderr (caller exits without restating)
/// - `Unrelated` = not this gap (caller's own report)
pub enum Offer {
    Install,
    Explained,
    Unrelated,
}

pub fn offer(report: &Report, non_interactive: bool, install: bool) -> Result<Offer, String> {
    if !fixable_here(report) {
        return Ok(Offer::Unrelated);
    }
    // Before assent, not inside install() (assent then a tooling error = the offer was a lie)
    let missing: Vec<&str> = TOOLS.into_iter().filter(|b| which(b).is_err()).collect();
    if !missing.is_empty() {
        eprintln!("✗ {GAP}; install {} to fix it here", missing.join(" + "));
        return Ok(Offer::Explained);
    }
    if install {
        return Ok(Offer::Install);
    }
    // Unattended = never mutate cluster-scoped state by default
    if non_interactive || !std::io::stdin().is_terminal() {
        eprintln!("✗ {GAP}; rerun with --install-storage");
        return Ok(Offer::Explained);
    }
    // One line, no wrap (dialoguer re-renders on answer → a wrapped prompt prints twice)
    let yes = Confirm::new()
        .with_prompt(format!("{GAP} — install csi-hostpath? (seed copies, no CoW)"))
        .default(false)
        .interact()
        .map_err(|e| format!("confirmation prompt: {e}"))?;
    if !yes {
        eprintln!("  → nothing installed; `ztest run` needs snapshots");
        return Ok(Offer::Explained);
    }
    Ok(Offer::Install)
}

/// Storage = sole blocker, cluster = local
fn fixable_here(report: &Report) -> bool {
    if ztest::backends::image::selected_class() != ztest::api::cluster_config::ClusterClass::Local {
        return false;
    }
    let mut blocking = report.blocking().peekable();
    blocking.peek().is_some() && report.blocking().all(|c| FIXABLE.contains(&c.name))
}

/// external-snapshotter (CRDs + controller) → csi-hostpath → class pair → default class.
///
/// - Blocking (setup CLI, nothing else in flight; every step = subprocess)
pub fn install() -> Result<(), String> {
    for bin in TOOLS {
        which(bin)?;
    }
    let work = WorkDir::new()?;
    // deploy.sh = kubectl with no --context, no override hook → pin via single-context kubeconfig
    let kubeconfig = minified_kubeconfig(work.path())?;

    eprintln!("  • external-snapshotter CRDs");
    kubectl(&kubeconfig, &["apply", "-k", &crd_url()])?;
    // Controller RBAC references these kinds (unestablished CRDs → NotFound race)
    kubectl(
        &kubeconfig,
        &[
            "wait",
            "--for=condition=Established",
            "--timeout=90s",
            "crd/volumesnapshots.snapshot.storage.k8s.io",
            "crd/volumesnapshotcontents.snapshot.storage.k8s.io",
            "crd/volumesnapshotclasses.snapshot.storage.k8s.io",
        ],
    )?;

    eprintln!("  • snapshot-controller");
    kubectl(&kubeconfig, &["apply", "-k", &controller_url()])?;
    kubectl(
        &kubeconfig,
        &["-n", "kube-system", "rollout", "status", "deploy/snapshot-controller", "--timeout=180s"],
    )?;

    eprintln!("  • csi-hostpath driver ({HOSTPATH_REF})");
    let checkout = work.path().join("csi-hostpath");
    run(Command::new("git").args([
        "clone",
        "--quiet",
        "--depth",
        "1",
        "--branch",
        HOSTPATH_REF,
        "https://github.com/kubernetes-csi/csi-driver-host-path.git",
        &checkout.display().to_string(),
    ]))?;

    let minor = server_minor(&kubeconfig)?;
    let deploy = checkout.join("deploy").join(format!("kubernetes-1.{minor}"));
    if !deploy.is_dir() {
        return Err(format!(
            "csi-driver-host-path {HOSTPATH_REF} ships no deploy dir for Kubernetes 1.{minor}"
        ));
    }
    run(Command::new(deploy.join("deploy.sh")).env("KUBECONFIG", &kubeconfig))?;
    kubectl(
        &kubeconfig,
        &["rollout", "status", "statefulset/csi-hostpathplugin", "--timeout=300s"],
    )?;

    eprintln!("  • StorageClass + VolumeSnapshotClass");
    let examples = checkout.join("examples");
    kubectl(
        &kubeconfig,
        &["apply", "-f", &examples.join("csi-storageclass.yaml").display().to_string()],
    )?;
    kubectl(
        &kubeconfig,
        &["apply", "-f", &examples.join("csi-volumesnapshotclass.yaml").display().to_string()],
    )?;

    make_default(&kubeconfig)?;
    eprintln!("  ✓ csi-hostpath installed ({STORAGE_CLASS} is now the default StorageClass)");
    Ok(())
}

/// kind's default `standard` (local-path) = no snapshots, no expansion → unnamed PVCs unusable
fn make_default(kubeconfig: &Path) -> Result<(), String> {
    let listed = output(Command::new("kubectl").env("KUBECONFIG", kubeconfig).args([
        "get",
        "storageclass",
        "-o",
        "name",
    ]))?;
    for sc in listed.lines().map(str::trim).filter(|s| !s.is_empty()) {
        let default = sc == format!("storageclass.storage.k8s.io/{STORAGE_CLASS}");
        let patch = format!(
            "{{\"metadata\":{{\"annotations\":{{\"{DEFAULT_CLASS_ANNOTATION}\":\"{default}\"}}}}}}"
        );
        kubectl(kubeconfig, &["patch", sc, "-p", &patch])?;
    }
    Ok(())
}

fn crd_url() -> String {
    format!(
        "https://github.com/kubernetes-csi/external-snapshotter//client/config/crd?ref={SNAPSHOTTER_REF}"
    )
}

fn controller_url() -> String {
    format!(
        "https://github.com/kubernetes-csi/external-snapshotter//deploy/kubernetes/snapshot-controller?ref={SNAPSHOTTER_REF}"
    )
}

/// One deploy dir per k8s minor (`latest` sidecars assume APIs an old server lacks)
fn server_minor(kubeconfig: &Path) -> Result<u32, String> {
    let raw = output(
        Command::new("kubectl").env("KUBECONFIG", kubeconfig).args(["version", "-o", "json"]),
    )?;
    let json: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| format!("parse `kubectl version`: {e}"))?;
    let minor = json["serverVersion"]["minor"]
        .as_str()
        .ok_or_else(|| "`kubectl version` reported no server minor".to_string())?;
    // Managed distributions suffix it ("29+")
    minor
        .trim_end_matches(|c: char| !c.is_ascii_digit())
        .parse()
        .map_err(|_| format!("unparsable server minor `{minor}`"))
}

fn minified_kubeconfig(work: &Path) -> Result<PathBuf, String> {
    let mut cmd = Command::new("kubectl");
    cmd.args(["config", "view", "--raw", "--minify"]);
    if let Some(ctx) = ztest::api::cluster_config::active_context() {
        cmd.args(["--context", &ctx]);
    }
    let path = work.join("kubeconfig");
    std::fs::write(&path, output(&mut cmd)?)
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(path)
}

fn kubectl(kubeconfig: &Path, args: &[&str]) -> Result<(), String> {
    run(Command::new("kubectl").env("KUBECONFIG", kubeconfig).args(args))
}

fn run(cmd: &mut Command) -> Result<(), String> {
    let status = cmd.status().map_err(|e| format!("spawn {:?}: {e}", cmd.get_program()))?;
    if !status.success() {
        return Err(format!("{:?} failed ({status})", cmd.get_program()));
    }
    Ok(())
}

fn output(cmd: &mut Command) -> Result<String, String> {
    let out = cmd.output().map_err(|e| format!("spawn {:?}: {e}", cmd.get_program()))?;
    if !out.status.success() {
        return Err(format!(
            "{:?} failed ({}): {}",
            cmd.get_program(),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    String::from_utf8(out.stdout).map_err(|e| format!("non-UTF-8 output: {e}"))
}

/// PATH scan, nothing executed (`git --help` → pager, `kubectl --version` → not a flag)
fn which(bin: &str) -> Result<(), String> {
    let missing = || format!("`{bin}` not on PATH; needed to install csi-hostpath");
    let path = std::env::var_os("PATH").ok_or_else(missing)?;
    std::env::split_paths(&path)
        .any(|dir| dir.join(bin).is_file())
        .then_some(())
        .ok_or_else(missing)
}

/// Scratch dir, removed on drop (`tempfile` = wallet-feature-gated)
struct WorkDir(PathBuf);

impl WorkDir {
    fn new() -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!("ztest-csi-hostpath-{}", std::process::id()));
        std::fs::create_dir_all(&path).map_err(|e| format!("create {}: {e}", path.display()))?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for WorkDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
