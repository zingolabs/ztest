//! Host-placed collector: Alloy in a host-engine container beside the kubelet, not inside it.
//!
//! - Nested kubelet (kind) numbers pods below the initial pid namespace; eBPF reports initial
//!   pids, so an in-cluster collector resolves none of them (see [`super::ebpf::Placement`])
//! - `--pid=host` puts it in the namespace eBPF measures in; it joins the cluster's engine
//!   network so the apiserver and the Pyroscope NodePort resolve by node IP
//! - Container ids still match: nested containerd ids appear verbatim in the host cgroup path
//! - Lifetime = the run's, enforced by reaping against the driver pod (the CLI is detached, so
//!   nothing stays resident to stop it)

use std::path::{Path, PathBuf};
use tokio::process::Command;

use super::ebpf::HTTP_PORT;
use crate::runtime;

/// Container path for the generated config + kubeconfig (mounted read-only)
pub const HOST_CONFIG: &str = "/etc/alloy/config.alloy";
pub const HOST_KUBECONFIG: &str = "/etc/alloy/kubeconfig";

/// Marks a container as ours *and* whose: reaping needs the sync id without a pid file
const SYNC_LABEL: &str = "ztest.io/sync-id";

/// kind labels its node containers; the cluster's engine network is read off one of them so
/// the collector joins it rather than guessing a name
const KIND_ROLE_LABEL: &str = "io.x-k8s.kind.role=control-plane";

/// Engine network the cluster's nodes sit on.
///
/// - Joining it is what makes the apiserver and the Pyroscope NodePort reachable by node IP
///   (user-defined bridges are isolated from the default one)
/// - Node IP over loopback on purpose: `--network host` would work too, but then the metrics
///   port lives in the host's port space, where anything may already hold it
pub async fn cluster_network() -> Option<String> {
    let node = Command::new(runtime::program())
        .args(["ps", "--filter", &format!("label={KIND_ROLE_LABEL}"), "--format", "{{.Names}}"])
        .output()
        .await
        .ok()?;
    let node = String::from_utf8_lossy(&node.stdout).lines().next()?.trim().to_string();
    let net = Command::new(runtime::program())
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

/// Host port the engine bound the collector's `/metrics` to. Engine owns the mapping, so this
/// is a lookup rather than a convention both sides must keep in step
pub async fn metrics_port(sync_id: &str) -> Option<u16> {
    let out = Command::new(runtime::program())
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

pub fn container_name(sync_id: &str) -> String {
    format!("ztest-profiler-{sync_id}")
}

/// Per-run scratch holding the rendered config + a single-context kubeconfig
fn run_dir(sync_id: &str) -> PathBuf {
    std::env::temp_dir().join(container_name(sync_id))
}

/// Self-contained, single-context kubeconfig for the collector.
///
/// - Alloy's `discovery.kubernetes` takes no context: it uses the file's `current-context` =
///   whatever the user's shell last set, which is not the context the run bound to
/// - Pinned to [`active_context`](crate::cluster_config::active_context) — same resolution
///   [`cluster::config`](crate::cluster::config) binds the run's own client by
/// - Every other cluster/user/context dropped (nothing left to pick up)
/// - Only this cluster's `server` rewritten: the file's address is loopback or a host-side
///   name, dead once the collector leaves the host network
/// - Path credentials inlined: the container has neither those paths nor the files
pub fn kubeconfig(api_server: &str) -> Result<String, crate::error::PipelineError> {
    let source = kube::config::Kubeconfig::read().map_err(|e| format!("read kubeconfig: {e}"))?;
    let wanted = crate::cluster_config::active_context()
        .ok_or("no kube-context bound: set one with `ztest cluster set <profile>`")?;
    let pinned = pin(source, &wanted, api_server)?;
    serde_yaml::to_string(&pinned).map_err(|e| format!("render kubeconfig: {e}").into())
}

/// [`kubeconfig`]'s reduction, over a kubeconfig already read
fn pin(
    source: kube::config::Kubeconfig,
    wanted: &str,
    api_server: &str,
) -> Result<kube::config::Kubeconfig, crate::error::PipelineError> {
    use kube::config::{Kubeconfig, NamedAuthInfo, NamedCluster};

    let named = source
        .contexts
        .iter()
        .find(|c| c.name == wanted)
        .cloned()
        .ok_or_else(|| format!("kube-context `{wanted}` is not in the kubeconfig"))?;
    let context = named.context.clone().ok_or_else(|| format!("context `{wanted}` is empty"))?;

    let mut cluster = source
        .clusters
        .iter()
        .find(|c| c.name == context.cluster)
        .and_then(|c| c.cluster.clone())
        .ok_or_else(|| format!("context `{wanted}` names cluster `{}`, absent", context.cluster))?;
    cluster.server = Some(api_server.to_string());
    inline(&mut cluster.certificate_authority, &mut cluster.certificate_authority_data)?;

    let user = context.user.clone().unwrap_or_default();
    let mut auth = source
        .auth_infos
        .iter()
        .find(|a| a.name == user)
        .and_then(|a| a.auth_info.clone())
        .unwrap_or_default();
    portable(&auth, wanted)?;
    inline(&mut auth.client_certificate, &mut auth.client_certificate_data)?;
    inline_secret(&mut auth.client_key, &mut auth.client_key_data)?;

    Ok(Kubeconfig {
        clusters: vec![NamedCluster { name: context.cluster.clone(), cluster: Some(cluster) }],
        auth_infos: vec![NamedAuthInfo { name: user, auth_info: Some(auth) }],
        contexts: vec![named],
        current_context: Some(wanted.to_string()),
        kind: Some("Config".to_string()),
        api_version: Some("v1".to_string()),
        preferences: None,
        extensions: None,
    })
}

/// Credentials the collector cannot reproduce: the plugin binary is not in its image, and a
/// cached token would expire mid-sync with no way to refresh
fn portable(
    auth: &kube::config::AuthInfo,
    context: &str,
) -> Result<(), crate::error::PipelineError> {
    let plugin = match (&auth.exec, &auth.auth_provider) {
        (Some(_), _) => "an exec credential plugin",
        (_, Some(p)) => return Err(format!("context `{context}` authenticates through the `{}` auth-provider, which the collector image cannot run", p.name).into()),
        _ => return Ok(()),
    };
    Err(format!(
        "context `{context}` authenticates through {plugin}, absent from the collector image"
    )
    .into())
}

/// Referenced file → embedded `-data`, base64 as the kubeconfig schema wants it
fn inline(
    path: &mut Option<String>,
    data: &mut Option<String>,
) -> Result<(), crate::error::PipelineError> {
    if let Some(pem) = read_pem(path, data.is_some())? {
        *data = Some(pem);
    }
    Ok(())
}

/// Generic over the secret wrapper: naming `kube`'s own would pin this file to whichever
/// `secrecy` major it tracks, and two live in the tree
fn inline_secret<T: From<String>>(
    path: &mut Option<String>,
    data: &mut Option<T>,
) -> Result<(), crate::error::PipelineError> {
    if let Some(pem) = read_pem(path, data.is_some())? {
        *data = Some(pem.into());
    }
    Ok(())
}

/// `None` = nothing to inline (already embedded, or never set). Takes the path either way:
/// a stale path beside the data reads as a file the container will look for
fn read_pem(
    path: &mut Option<String>,
    embedded: bool,
) -> Result<Option<String>, crate::error::PipelineError> {
    use base64::Engine as _;
    let Some(file) = path.take() else {
        return Ok(None);
    };
    if embedded {
        return Ok(None);
    }
    let bytes = std::fs::read(&file).map_err(|e| format!("read {file} for the collector: {e}"))?;
    Ok(Some(base64::engine::general_purpose::STANDARD.encode(bytes)))
}

fn write_kubeconfig(dir: &Path, api_server: &str) -> Result<PathBuf, crate::error::PipelineError> {
    let path = dir.join("kubeconfig");
    std::fs::write(&path, kubeconfig(api_server)?).map_err(|e| format!("write kubeconfig: {e}"))?;
    Ok(path)
}

/// Start the collector for a run. Idempotent: an existing container for this id is replaced,
/// so a relaunch cannot leave two collectors pushing the same tenant.
pub async fn start(
    sync_id: &str,
    config: &str,
    api_server: &str,
) -> Result<(), crate::error::PipelineError> {
    let dir = run_dir(sync_id);
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let config_path = dir.join("config.alloy");
    std::fs::write(&config_path, config).map_err(|e| format!("write collector config: {e}"))?;
    let kubeconfig = write_kubeconfig(&dir, api_server)?;

    let name = container_name(sync_id);
    let _ = Command::new(runtime::program()).args(["rm", "-f", &name]).output().await;

    let network = cluster_network().await.ok_or("no kind docker network found for this cluster")?;
    let http = format!("--server.http.listen-addr=0.0.0.0:{HTTP_PORT}");
    let publish = format!("127.0.0.1::{HTTP_PORT}");
    let out = Command::new(runtime::program())
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
        )
        .into());
    }
    settled(&name).await
}

/// `docker run -d` returning 0 means *spawned*, not *serving*: a bind clash or a rejected
/// config exits a second later, and reporting success there is how a run reaches `sync perf`
/// before anyone learns the collector died
async fn settled(name: &str) -> Result<(), crate::error::PipelineError> {
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let Ok(out) = Command::new(runtime::program())
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
                return Err(format!("collector exited ({code}): {}", last_error(name).await).into());
            }
            _ => continue,
        }
    }
    Ok(())
}

/// Alloy's own last complaint, which names the cause far better than an exit code
async fn last_error(name: &str) -> String {
    let Ok(out) =
        Command::new(runtime::program()).args(["logs", "--tail", "40", name]).output().await
    else {
        return "see `docker logs`".to_string();
    };
    let text =
        format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    text.lines()
        .rfind(|l| l.contains("level=error") || l.contains("level=ERROR"))
        .map(|l| l.rsplit_once("err=").map_or(l, |(_, tail)| tail))
        .unwrap_or("see `docker logs`")
        .trim()
        .to_string()
}

pub async fn stop(sync_id: &str) {
    let _ = Command::new(runtime::program())
        .args(["rm", "-f", &container_name(sync_id)])
        .output()
        .await;
    let _ = std::fs::remove_dir_all(run_dir(sync_id));
}

/// Sync ids with a host collector container, running *or* exited.
///
/// - `ps -a`, not `ps`: a collector that died (bind clash, rejected config) still owns a
///   container name and a scratch dir, and only this sweep frees them
pub async fn collectors() -> Vec<String> {
    let Ok(out) = Command::new(runtime::program())
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
pub async fn reap_finished(client: &kube::Client) {
    for id in collectors().await {
        let live = crate::sync::driver_is_live(client, &id).await;
        if !live {
            stop(&id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::config::Kubeconfig;

    /// Two clusters, current-context on the one the run did *not* bind — the shape that
    /// pointed a collector at the wrong apiserver for six hours
    fn two_clusters() -> Kubeconfig {
        Kubeconfig::from_yaml(
            r#"
apiVersion: v1
kind: Config
current-context: kind-other
clusters:
- name: kind-run
  cluster:
    server: https://127.0.0.1:36591
    certificate-authority-data: cnVuLWNh
- name: kind-other
  cluster:
    server: https://127.0.0.1:44444
    certificate-authority-data: b3RoZXItY2E=
users:
- name: kind-run
  user:
    client-certificate-data: cnVuLWNlcnQ=
- name: kind-other
  user:
    client-certificate-data: b3RoZXItY2VydA==
contexts:
- name: kind-run
  context: { cluster: kind-run, user: kind-run }
- name: kind-other
  context: { cluster: kind-other, user: kind-other }
"#,
        )
        .expect("fixture parses")
    }

    const API: &str = "https://172.24.0.2:6443";

    /// Alloy has no context selector, so `current-context` *is* the selection — inheriting
    /// the shell's authenticates against the run's cluster with another one's CA
    #[test]
    fn pin_takes_the_bound_context_over_the_files_current_one() {
        let pinned = pin(two_clusters(), "kind-run", API).expect("pins");
        assert_eq!(pinned.current_context.as_deref(), Some("kind-run"));
        let ca = pinned.clusters[0].cluster.as_ref().unwrap().certificate_authority_data.as_deref();
        assert_eq!(ca, Some("cnVuLWNh"));
    }

    /// One entry each: an unreferenced cluster left in the file is one a future
    /// `current-context` can still select
    #[test]
    fn pin_drops_every_entry_the_context_does_not_name() {
        let pinned = pin(two_clusters(), "kind-run", API).expect("pins");
        assert_eq!(pinned.clusters.len(), 1);
        assert_eq!(pinned.auth_infos.len(), 1);
        assert_eq!(pinned.contexts.len(), 1);
        assert_eq!(pinned.clusters[0].name, "kind-run");
        assert_eq!(
            pinned.auth_infos[0].auth_info.as_ref().unwrap().client_certificate_data.as_deref(),
            Some("cnVuLWNlcnQ=")
        );
    }

    /// The file's address is loopback, which is dead once the collector leaves the host network
    #[test]
    fn pin_dials_the_address_the_collector_can_reach() {
        let pinned = pin(two_clusters(), "kind-run", API).expect("pins");
        assert_eq!(pinned.clusters[0].cluster.as_ref().unwrap().server.as_deref(), Some(API));
    }

    /// A stale profile outlives the kubeconfig it names; silently falling back to the file's
    /// current-context is the bug, so this must fail and say which context is missing
    #[test]
    fn pin_refuses_a_context_the_kubeconfig_lacks() {
        let err = pin(two_clusters(), "kind-gone", API).expect_err("refused");
        assert!(err.0.contains("kind-gone"), "{}", err.0);
    }

    /// The plugin binary is not in the collector image: rendered anyway, discovery fails at
    /// runtime with an error only the container's log carries
    #[test]
    fn pin_refuses_credentials_the_collector_image_cannot_produce() {
        let exec = Kubeconfig::from_yaml(
            r#"
apiVersion: v1
kind: Config
current-context: oidc
clusters:
- name: oidc
  cluster: { server: https://example:6443 }
users:
- name: oidc
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubelogin
contexts:
- name: oidc
  context: { cluster: oidc, user: oidc }
"#,
        )
        .expect("fixture parses");
        let err = pin(exec, "oidc", API).expect_err("refused");
        assert!(err.0.contains("exec credential plugin"), "{}", err.0);
    }
}
