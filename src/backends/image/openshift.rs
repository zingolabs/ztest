//! OpenShift integrated-registry topology: `docker buildx` exports an OCI
//! layout which ztest pushes in-process ([`crate::backends::oci`]) to the
//! external route, while pods reference the in-cluster service (`pull`) so the
//! kubelet pulls over cluster DNS with the pod SA's auto-injected creds — no
//! route cert on nodes, no pull secret. See `docs/openshift-registry.md`.

use std::path::Path;

use async_trait::async_trait;

use super::{ImageError, ImageProvider, RunnerBase, build_arg_rust_version, join, run_streamed};
use crate::backends::oci;
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};

/// Push and pull addresses differ: build output is pushed to the external route
/// (`push`), pods reference the in-cluster service (`pull`).
#[derive(Debug)]
pub(crate) struct OpenShift {
    pub(crate) push: String,
    pub(crate) pull: String,
}

/// Buildx builder name ztest owns for OCI-layout exports. The default `docker`
/// driver can't `--output type=oci`; a `docker-container` driver builder can, so
/// provisioning ensures one of these exists.
pub(crate) const BUILDX_BUILDER: &str = "ztest";

#[async_trait]
impl ImageProvider for OpenShift {
    fn reference(&self, tag: &str) -> String {
        join(&self.pull, tag)
    }

    fn pull_secret(&self) -> Option<String> {
        // Pods reference the in-cluster service and pull with the pod SA's
        // auto-injected registry creds (the `system:image-puller` grant), so a
        // pull secret is never needed and injecting one meant for the external
        // route would be wrong.
        None
    }

    fn dev_present(&self, _tag: &str) -> Result<bool, ImageError> {
        // The preflight push (run process) already ensured presence; a per-test
        // re-probe would need the registry token+CA in every test binary. A
        // genuine miss surfaces as the pod's ImagePullBackOff, which the run
        // reports.
        Ok(true)
    }

    async fn probe(&self, _cx: &Cx, tag: &str, _entry: &DevImageEntry) -> Readiness {
        // In-process manifest HEAD against the push registry, using the token+CA
        // from the kubeconfig. A query error means `Absent` so we (re)build.
        let present = match self.push_target(tag) {
            Ok(target) => oci::manifest_exists(&target).await.unwrap_or(false),
            Err(_) => false,
        };
        if present {
            Readiness::Ready
        } else {
            Readiness::Absent
        }
    }

    /// Build an OCI layout with buildx and push it in-process, streaming
    /// per-blob progress to the right-hand transfer panel. Self-contained: the
    /// push reuses the kubeconfig's SA token + cluster CA, so it needs no
    /// `docker login`, no `/etc/docker/certs.d`, and no `sudo`.
    async fn build(&self, cx: &Cx, entry: &DevImageEntry, tag: &str) -> Result<(), ResourceError> {
        let id = NodeId::Image(tag.to_string());
        let (dockerfile, context) = entry
            .source
            .materialize()
            .map_err(|e| ResourceError::Provision(format!("resolve image source {tag}: {e}")))?;
        let target = self
            .push_target(tag)
            .map_err(|e| ResourceError::Provision(format!("registry credentials: {e}")))?;

        // Buildx's OCI exporter needs a `docker-container` driver builder; the
        // default `docker` driver can't `--output type=oci`. Idempotent create.
        ensure_buildx_builder(cx, tag).await?;

        let work = std::env::temp_dir().join(format!("ztest-oci-{}", slug(tag)));
        let _ = std::fs::remove_dir_all(&work);
        std::fs::create_dir_all(&work)
            .map_err(|e| ResourceError::Provision(format!("scratch dir: {e}")))?;
        let tar = work.join("image.tar");
        let layout = work.join("layout");

        if let Some(sink) = &cx.progress {
            sink.note(&id, "building");
        }
        let build_contexts = match runner_base_context(cx, tag, entry, &work).await {
            Ok(bc) => bc,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&work);
                return Err(e);
            }
        };
        let argv = buildx_oci_argv(
            &dockerfile,
            &context,
            &entry.features,
            tag,
            &tar,
            entry.rust_version.as_deref(),
            &build_contexts,
        );
        let envs = [("DOCKER_BUILDKIT", "1".to_string())];
        let build = run_streamed(cx, tag, "docker", &argv, &envs, "buildx build").await;
        if let Err(e) = build {
            let _ = std::fs::remove_dir_all(&work);
            return Err(e);
        }

        // Extract the OCI tarball to a directory so the push reads blobs as files.
        if let Err(e) = extract_tar(&tar, &layout).await {
            let _ = std::fs::remove_dir_all(&work);
            return Err(ResourceError::Provision(e));
        }

        let sink = cx.progress.clone();
        let report_id = id.clone();
        let report = move |ev: oci::PushProgress| {
            let Some(sink) = &sink else { return };
            match ev {
                oci::PushProgress::Blob {
                    n,
                    total,
                    pushed_bytes,
                    total_bytes,
                } => sink.bytes(
                    &report_id,
                    pushed_bytes,
                    total_bytes,
                    format!("layer {n}/{total}"),
                ),
                oci::PushProgress::Manifest => sink.finalizing(&report_id),
            }
        };
        let result = oci::push_layout(&layout, &target, &report).await;
        let _ = std::fs::remove_dir_all(&work);
        result.map_err(|e| ResourceError::Provision(format!("push {tag}: {e}")))
    }

    fn prepare_runner_base(&self, workspace: &Path) -> Result<RunnerBase, ResourceError> {
        // The base is pinned into the buildx build via a skopeo→OCI layout in
        // `build`, so Phase 1 loads it locally just like kind/docker.
        super::nix_runner_base(workspace)
    }
}

impl OpenShift {
    /// The in-process push target: the push (route) reference plus the token+CA
    /// read from the active kubeconfig.
    fn push_target(&self, tag: &str) -> Result<oci::Target, String> {
        internal_push_target(join(&self.push, tag))
    }
}

/// Build-contexts pinning `FROM` sources for this image. The baked tests
/// image ([`RUNNER_REPO`](crate::engine::pod_runner::RUNNER_REPO)) does
/// `FROM ztest-runner:dev`, a nix base that lives only in the local docker
/// daemon — invisible to the isolated `docker-container` buildx builder. Turn
/// it into a local OCI layout with skopeo and pin the `FROM` to that layout,
/// so buildx resolves it with no registry round-trip (which on OpenShift would
/// need docker-login auth + the self-signed-CA TLS the in-process push avoids).
/// Other images add nothing.
///
/// skopeo is piped (not run under the console PTY): off a TTY it emits plain
/// `Copying blob … done` lines, which we parse into transfer-panel notes so
/// this shows up beside the push progress instead of fighting the live grid.
async fn runner_base_context(
    cx: &Cx,
    tag: &str,
    entry: &DevImageEntry,
    work: &Path,
) -> Result<Vec<(String, String)>, ResourceError> {
    if entry.repo != crate::engine::pod_runner::RUNNER_REPO {
        return Ok(Vec::new());
    }
    let id = NodeId::Image(tag.to_string());
    let base_tag = crate::engine::pod_runner::RUNNER_BASE_LOCAL_TAG;
    let layout = work.join("base-layout");

    if let Some(sink) = &cx.progress {
        sink.note(&id, "runner base → oci");
    }
    let mut child = tokio::process::Command::new("skopeo")
        .args([
            "copy".to_string(),
            "--insecure-policy".to_string(),
            format!("docker-daemon:{base_tag}"),
            format!("oci:{}:base", layout.display()),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ResourceError::Provision(format!("skopeo (runner base → oci): {e}")))?;

    // skopeo writes copy progress to stderr; drain stdout concurrently so a
    // full pipe buffer can't deadlock the child.
    if let Some(out) = child.stdout.take() {
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt as _;
            let mut out = out;
            let mut sink = Vec::new();
            let _ = out.read_to_end(&mut sink).await;
        });
    }
    if let (Some(err), Some(sink)) = (child.stderr.take(), cx.progress.clone()) {
        use tokio::io::{AsyncBufReadExt as _, BufReader};
        let mut lines = BufReader::new(err).lines();
        let mut blobs = 0usize;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains("Copying blob") {
                blobs += 1;
                sink.note(&id, format!("runner base → oci · blob {blobs}"));
            } else if line.contains("Copying config") {
                sink.note(&id, "runner base → oci · config");
            } else if line.contains("Writing manifest") {
                sink.note(&id, "runner base → oci · manifest");
            }
        }
    }

    let status = child
        .wait()
        .await
        .map_err(|e| ResourceError::Provision(format!("skopeo (runner base → oci): {e}")))?;
    if !status.success() {
        return Err(ResourceError::Provision(
            "skopeo copy of the nix runner base to an OCI layout failed".to_string(),
        ));
    }
    Ok(vec![(
        base_tag.to_string(),
        format!("oci-layout://{}", layout.display()),
    )])
}

/// Ensure the `docker-container` buildx builder ztest uses for OCI exports
/// exists. `docker buildx inspect` succeeds → reuse; else create it.
async fn ensure_buildx_builder(cx: &Cx, tag: &str) -> Result<(), ResourceError> {
    let name = BUILDX_BUILDER;
    let present = tokio::process::Command::new("docker")
        .args(["buildx", "inspect", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false);
    if present {
        return Ok(());
    }
    if let Some(sink) = &cx.progress {
        sink.note(&NodeId::Image(tag.to_string()), "buildx builder");
    }
    let argv = vec![
        "buildx".to_string(),
        "create".to_string(),
        "--name".to_string(),
        name.to_string(),
        "--driver".to_string(),
        "docker-container".to_string(),
    ];
    run_streamed(cx, tag, "docker", &argv, &[], "buildx create").await
}

/// The `docker buildx build` argv that exports a registry-ready OCI layout
/// (correct gzip + digests, unlike `docker save`) to `dest_tar`, for the
/// in-process push. Runs through the console PTY like
/// [`docker_build_argv`](super::docker_build_argv).
pub(crate) fn buildx_oci_argv(
    dockerfile: &Path,
    context: &Path,
    features: &[String],
    tag: &str,
    dest_tar: &Path,
    rust_version: Option<&str>,
    build_contexts: &[(String, String)],
) -> Vec<String> {
    let mut argv = vec![
        "buildx".to_string(),
        "build".to_string(),
        "--builder".to_string(),
        BUILDX_BUILDER.to_string(),
        "-f".to_string(),
        dockerfile.display().to_string(),
        "-t".to_string(),
        tag.to_string(),
        "--output".to_string(),
        format!("type=oci,dest={}", dest_tar.display()),
    ];
    // Pin a `FROM <name>` to a locally-supplied source (e.g. an oci-layout of the
    // nix runner base) so buildx resolves it without a registry pull — the
    // isolated `docker-container` builder can't see the local docker daemon.
    for (name, source) in build_contexts {
        argv.push("--build-context".to_string());
        argv.push(format!("{name}={source}"));
    }
    if let Some(rv) = build_arg_rust_version(rust_version, context) {
        argv.push("--build-arg".to_string());
        argv.push(format!("RUST_VERSION={rv}"));
    }
    // Pass features under both the ztest convention (`CARGO_FEATURES`, read by
    // our own Dockerfiles) and the upstream zcash convention (`FEATURES`, read
    // by e.g. zebra's in-tree `docker/Dockerfile`). An undeclared build-arg is
    // only a warning, so whichever the target Dockerfile reads gets set and the
    // other is ignored.
    if !features.is_empty() {
        let joined = features.join(",");
        argv.push("--build-arg".to_string());
        argv.push(format!("CARGO_FEATURES={joined}"));
        argv.push("--build-arg".to_string());
        argv.push(format!("FEATURES={joined}"));
    }
    argv.push(context.display().to_string());
    argv
}

/// Assemble the in-process push [`Target`](crate::backends::oci::Target) for an
/// OpenShift integrated-registry push reference, reading the bearer token and CA
/// from the same kubeconfig (`KUBECONFIG` / `ZTEST_KUBE_CONTEXT`) that
/// authenticates the kube client — the "one file has everything" path.
pub(crate) fn internal_push_target(push_reference: String) -> Result<oci::Target, String> {
    use crate::backends::oci::{Auth, Target};
    let context = std::env::var("ZTEST_KUBE_CONTEXT")
        .ok()
        .filter(|s| !s.is_empty());
    let kubeconfig = std::env::var_os("KUBECONFIG").map(std::path::PathBuf::from);
    let material = crate::cluster_config::read_material(kubeconfig.as_deref(), context.as_deref())?;
    let token = material
        .token
        .ok_or("the kubeconfig has no bearer token for the registry push")?;
    Ok(Target {
        reference: push_reference,
        // OpenShift's token handshake ignores the username but requires one.
        auth: Auth {
            username: "ztest".to_string(),
            token,
        },
        ca_pem: material.ca_pem,
    })
}

/// `docker`-tag → filesystem-safe scratch name.
fn slug(tag: &str) -> String {
    tag.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Extract an OCI image tarball to `dest` via the `tar` CLI (avoids a tar-crate
/// dependency and lets the push stream blobs from disk).
async fn extract_tar(tar: &Path, dest: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dest).map_err(|e| format!("layout dir: {e}"))?;
    let status = tokio::process::Command::new("tar")
        .arg("-xf")
        .arg(tar)
        .arg("-C")
        .arg(dest)
        .status()
        .await
        .map_err(|e| format!("spawn tar: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("tar exited {status}"))
    }
}
