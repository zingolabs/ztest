//! Host-placed collector: Alloy in a docker container beside the kubelet, not inside it.
//!
//! - Nested kubelet (kind) numbers pods below the initial pid namespace; eBPF reports initial
//!   pids, so an in-cluster collector resolves none of them (see [`super::ebpf::Placement`])
//! - `--pid=host` puts it in the namespace eBPF measures in; it joins the cluster's docker
//!   network so the apiserver and the Pyroscope NodePort resolve by node IP
//! - Container ids still match: nested containerd ids appear verbatim in the host cgroup path
//! - Lifetime = the run's, enforced by reaping against the driver pod (the CLI is detached, so
//!   nothing stays resident to stop it)

use std::path::{Path, PathBuf};
use tokio::process::Command;

use super::ebpf::HTTP_PORT;

/// Container path for the generated config + kubeconfig (mounted read-only)
pub(crate) const HOST_CONFIG: &str = "/etc/alloy/config.alloy";
pub(crate) const HOST_KUBECONFIG: &str = "/etc/alloy/kubeconfig";

/// Marks a container as ours *and* whose: reaping needs the sync id without a pid file
const SYNC_LABEL: &str = "ztest.io/sync-id";

/// kind labels its node containers; the cluster's docker network is read off one of them so
/// the collector joins it rather than guessing a name
const KIND_ROLE_LABEL: &str = "io.x-k8s.kind.role=control-plane";

/// Docker network the cluster's nodes sit on.
///
/// - Joining it is what makes the apiserver and the Pyroscope NodePort reachable by node IP
///   (user-defined bridges are isolated from the default one)
/// - Node IP over loopback on purpose: `--network host` would work too, but then the metrics
///   port lives in the host's port space, where anything may already hold it
pub(crate) async fn cluster_network() -> Option<String> {
    let node = Command::new("docker")
        .args(["ps", "--filter", &format!("label={KIND_ROLE_LABEL}"), "--format", "{{.Names}}"])
        .output()
        .await
        .ok()?;
    let node = String::from_utf8_lossy(&node.stdout).lines().next()?.trim().to_string();
    let net = Command::new("docker")
        .args([
            "inspect",
            "-f",
            "{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}",
            &node,
        ])
        .output()
        .await
        .ok()?;
    String::from_utf8_lossy(&net.stdout).split_whitespace().next().map(str::to_string)
}

/// Host port docker bound the collector's `/metrics` to. Docker owns the mapping, so this is
/// a lookup rather than a convention both sides must keep in step
pub(crate) async fn metrics_port(sync_id: &str) -> Option<u16> {
    let out = Command::new("docker")
        .args(["port", &container_name(sync_id), &HTTP_PORT.to_string()])
        .output()
        .await
        .ok()?;
    // `127.0.0.1:49154` (one line per protocol)
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .find_map(|l| l.trim().rsplit_once(':'))
        .and_then(|(_, port)| port.trim().parse().ok())
}

pub(crate) fn container_name(sync_id: &str) -> String {
    format!("ztest-profiler-{sync_id}")
}

/// Per-run scratch holding the rendered config + a single-context kubeconfig
fn run_dir(sync_id: &str) -> PathBuf {
    std::env::temp_dir().join(container_name(sync_id))
}

/// Single-context kubeconfig for the collector.
///
/// - Alloy's `discovery.kubernetes` has no context selector: it takes the file's
///   `current-context`, which is whatever the *user's* shell last set
/// - Written from the same config ztest resolved its own client from, so the collector cannot
///   discover against a different cluster than the run
fn write_kubeconfig(dir: &Path, api_server: &str) -> Result<PathBuf, String> {
    let source = std::env::var("KUBECONFIG")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| dirs_home().map(|h| h.join(".kube/config")))
        .ok_or("no kubeconfig: set KUBECONFIG or $HOME/.kube/config")?;
    let raw = std::fs::read_to_string(&source)
        .map_err(|e| format!("read kubeconfig {}: {e}", source.display()))?;
    let mut doc: serde_yaml::Value =
        serde_yaml::from_str(&raw).map_err(|e| format!("parse kubeconfig: {e}"))?;
    // Every cluster's address rewritten, not just the current one: the loopback address the
    // file carries is unreachable once the collector leaves the host network
    if let Some(clusters) = doc.get_mut("clusters").and_then(|c| c.as_sequence_mut()) {
        for entry in clusters {
            if let Some(cluster) = entry.get_mut("cluster").and_then(|c| c.as_mapping_mut()) {
                cluster.insert("server".into(), api_server.into());
            }
        }
    }
    let path = dir.join("kubeconfig");
    let rendered = serde_yaml::to_string(&doc).map_err(|e| format!("render kubeconfig: {e}"))?;
    std::fs::write(&path, rendered).map_err(|e| format!("write kubeconfig: {e}"))?;
    Ok(path)
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var("HOME").ok().filter(|s| !s.trim().is_empty()).map(PathBuf::from)
}

/// Start the collector for a run. Idempotent: an existing container for this id is replaced,
/// so a relaunch cannot leave two collectors pushing the same tenant.
pub(crate) async fn start(sync_id: &str, config: &str, api_server: &str) -> Result<(), String> {
    let dir = run_dir(sync_id);
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let config_path = dir.join("config.alloy");
    std::fs::write(&config_path, config).map_err(|e| format!("write collector config: {e}"))?;
    let kubeconfig = write_kubeconfig(&dir, api_server)?;

    let name = container_name(sync_id);
    let _ = Command::new("docker").args(["rm", "-f", &name]).output().await;

    let network = cluster_network().await.ok_or("no kind docker network found for this cluster")?;
    let http = format!("--server.http.listen-addr=0.0.0.0:{HTTP_PORT}");
    let publish = format!("127.0.0.1::{HTTP_PORT}");
    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &name,
            "--label",
            &format!("{SYNC_LABEL}={sync_id}"),
            "--privileged",
            "--pid=host",
            "--network",
            &network,
            "-p",
            &publish,
            "-v",
            &format!("{}:{HOST_CONFIG}:ro", config_path.display()),
            "-v",
            &format!("{}:{HOST_KUBECONFIG}:ro", kubeconfig.display()),
            "-v",
            "/sys/kernel/debug:/sys/kernel/debug",
            "-v",
            "/sys/fs/bpf:/sys/fs/bpf",
            super::ebpf::ALLOY_IMAGE,
            "run",
            &http,
            HOST_CONFIG,
        ])
        .output()
        .await
        .map_err(|e| format!("spawn docker: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "start host collector: {}",
            String::from_utf8_lossy(&out.stderr).trim().lines().next_back().unwrap_or_default()
        ));
    }
    settled(&name).await
}

/// `docker run -d` returning 0 means *spawned*, not *serving*: a bind clash or a rejected
/// config exits a second later, and reporting success there is how a run reaches `sync perf`
/// before anyone learns the collector died
async fn settled(name: &str) -> Result<(), String> {
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let Ok(out) = Command::new("docker")
            .args(["inspect", "-f", "{{.State.Running}} {{.State.ExitCode}}", name])
            .output()
            .await
        else {
            return Ok(());
        };
        let state = String::from_utf8_lossy(&out.stdout);
        let mut fields = state.split_whitespace();
        match (fields.next(), fields.next()) {
            (Some("true"), _) => return Ok(()),
            (Some("false"), Some(code)) if code != "0" => {
                return Err(format!("collector exited ({code}): {}", last_error(name).await));
            }
            _ => continue,
        }
    }
    Ok(())
}

/// Alloy's own last complaint, which names the cause far better than an exit code
async fn last_error(name: &str) -> String {
    let Ok(out) = Command::new("docker").args(["logs", "--tail", "40", name]).output().await else {
        return "see `docker logs`".to_string();
    };
    let text =
        format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    text.lines()
        .filter(|l| l.contains("level=error") || l.contains("level=ERROR"))
        .next_back()
        .map(|l| l.rsplit_once("err=").map_or(l, |(_, tail)| tail))
        .unwrap_or("see `docker logs`")
        .trim()
        .to_string()
}

pub(crate) async fn stop(sync_id: &str) {
    let _ = Command::new("docker").args(["rm", "-f", &container_name(sync_id)]).output().await;
    let _ = std::fs::remove_dir_all(run_dir(sync_id));
}

/// Sync ids with a host collector container, running *or* exited.
///
/// - `ps -a`, not `ps`: a collector that died (bind clash, rejected config) still owns a
///   container name and a scratch dir, and only this sweep frees them
pub(crate) async fn collectors() -> Vec<String> {
    let Ok(out) = Command::new("docker")
        .args([
            "ps",
            "-a",
            "--filter",
            &format!("label={SYNC_LABEL}"),
            "--format",
            "{{.Label \"ztest.io/sync-id\"}}",
        ])
        .output()
        .await
    else {
        return Vec::new();
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// Reap collectors whose run is over. The CLI is detached, so this is the only thing that
/// stops one — called wherever a sync's liveness is already being read.
pub(crate) async fn reap_finished(client: &kube::Client) {
    for id in collectors().await {
        let live = crate::cli::sync::driver_is_live(client, &id).await;
        if !live {
            stop(&id).await;
        }
    }
}
