//! [`ImageProvider`] — a dev image (`<repo>:dev-<hash>`) as a resource-graph
//! node.
//!
//! `probe` asks whether the content-addressed image is already present (a warm
//! cache skips the build); `provision` runs `docker build` then a distribution
//! step through the console PTY so BuildKit's live layer progress renders in the
//! panel. Which distribution step depends on [`image::Distribution`]:
//!
//!   - **Kind** (local dev): probe queries the kind node's containerd, provision
//!     `kind load`s the built tag into it.
//!   - **Registry** (remote/CI): probe queries the registry manifest, provision
//!     `docker push`es the registry-qualified reference. This is the path a
//!     kubeconfig-only runner (no local kind node to `docker exec` or
//!     `kind load` into) uses against a shared cluster.
//!   - **Internal** (OpenShift integrated registry): probe HEADs the manifest
//!     in-process; provision `docker buildx`-exports an OCI layout and pushes it
//!     with [`crate::backends::oci`] (token + CA from the kubeconfig), streaming
//!     per-blob progress. Pods pull the in-cluster service address, so no pull
//!     secret is needed. See `docs/clusters.md`.
//!
//! Dev images are [`Lifetime::Cached`] — reused across runs and never reaped on
//! cancel, so [`teardown`](Provider::teardown) is the trait's default no-op
//! (eviction is a separate, explicit prune).

use std::path::Path;

use async_trait::async_trait;

use crate::backends::image::{self, Distribution};
use crate::backends::oci;
use crate::cli::console::run_child;
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// One dev image to ensure present in the cluster.
///
/// The content-addressed tag is computed at construction (fallibly) so
/// [`Provider::id`] is infallible and stable across the provider's
/// lifetime.
#[derive(Debug)]
pub(crate) struct ImageProvider {
    entry: DevImageEntry,
    /// `<repo>:dev-<hash>` — the content-addressed local tag and this node's
    /// identity. Registry-independent, so the node id (and thus the graph's
    /// dedup + dependency edges) is the same whether or not a registry is set.
    tag: String,
    /// Distribution mode for this invocation (kind vs registry), read once from
    /// the environment at construction.
    dist: Distribution,
}

impl ImageProvider {
    /// Build a provider for `entry`, computing its `<repo>:dev-<hash>` tag
    /// now. Fails if the build context can't be hashed (missing Dockerfile /
    /// context tree, IO error while walking).
    pub(crate) fn new(entry: DevImageEntry) -> Result<Self, String> {
        let tag = image::dev_tag(
            &entry.source,
            &entry.features,
            &entry.repo,
            entry.rust_version.as_deref(),
        )
        .map_err(|e| e.to_string())?;
        Ok(Self {
            entry,
            tag,
            dist: Distribution::from_env(),
        })
    }

    /// The pod-manifest / build reference for this image under the active
    /// distribution mode: the bare tag for kind, the registry-qualified tag for
    /// registry mode.
    fn reference(&self) -> String {
        self.dist.reference(&self.tag)
    }

    /// The content-addressed [`NodeId`] this entry resolves to. Public so
    /// `cli::run` can key per-binary image dependency edges to the
    /// provisioned node id without re-derivation.
    pub(crate) fn node_id(entry: &DevImageEntry) -> Result<NodeId, String> {
        Self::new(entry.clone()).map(|p| p.id())
    }

    /// Run one build/load step, streaming its output live through the console
    /// PTY so BuildKit / kind progress renders in the panel. Provisioning runs
    /// at cap 1 (see `cli::run`), so at most one such stream drives the emulator
    /// grid at a time — no interleaving, no lock. Off a TTY `run_child` inherits
    /// stdio for the plain CI log.
    async fn run_streamed(
        &self,
        cx: &Cx,
        program: &str,
        argv: &[String],
        envs: &[(&str, String)],
        step: &str,
    ) -> Result<(), ResourceError> {
        let code = run_child(cx.console.as_ref(), program, argv, envs)
            .await
            .map_err(|e| ResourceError::Provision(format!("{step} {}: {e}", self.tag)))?;
        if code != 0 {
            return Err(ResourceError::Provision(format!(
                "{step} {} exited {code}",
                self.tag
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl Provider for ImageProvider {
    fn id(&self) -> NodeId {
        NodeId::Image(self.tag.clone())
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, _cx: &Cx) -> Readiness {
        // A query error (node/registry unreachable) means `Absent` so we attempt
        // a (re)build rather than silently treating the image as present — the
        // build/push will surface the real error.
        let present = match &self.dist {
            // In-process manifest HEAD against the push registry, using the
            // token+CA from the kubeconfig.
            Distribution::Internal { .. } => match self.internal_target() {
                Ok(target) => oci::manifest_exists(&target).await.unwrap_or(false),
                Err(_) => false,
            },
            // kind / generic registry shell out to `docker` (a
            // `docker exec … crictl images`, or `docker manifest inspect`); keep
            // them off the async worker.
            _ => {
                let dist = self.dist.clone();
                let tag = self.tag.clone();
                let reference = self.reference();
                matches!(
                    tokio::task::spawn_blocking(move || match dist {
                        Distribution::Kind => image::exists_in_kind(&tag),
                        Distribution::Registry { .. } => image::exists_in_registry(&reference),
                        Distribution::Internal { .. } => unreachable!("handled above"),
                    })
                    .await,
                    Ok(Ok(true))
                )
            }
        };
        if present {
            Readiness::Ready
        } else {
            Readiness::Absent
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        // The OpenShift integrated registry builds to an OCI layout and pushes
        // in-process (own auth/TLS/progress); kind and generic registries build
        // into the docker daemon and `kind load` / `docker push`.
        if matches!(self.dist, Distribution::Internal { .. }) {
            return self.provision_internal(cx).await;
        }

        let (dockerfile, context) = self.entry.source.materialize().map_err(|e| {
            ResourceError::Provision(format!("resolve image source {}: {e}", self.tag))
        })?;
        let id = self.id();
        let reference = self.reference();

        // Fail fast on a missing kind cluster: cheaper to report before the
        // multi-minute build than to let `kind load` error afterwards.
        if matches!(self.dist, Distribution::Kind) {
            tokio::task::spawn_blocking(image::ensure_kind_cluster)
                .await
                .map_err(|e| ResourceError::Provision(format!("kind preflight: {e}")))?
                .map_err(|e| ResourceError::Provision(e.to_string()))?;
        }

        // Provisioning runs at cap 1 (see `cli::run`), so image nodes build one
        // at a time — each step streams its native `docker` / `kind` output live
        // through the emulator grid with nothing else contending for it. The
        // right-column tracker mirrors the current sub-phase via `progress`
        // notes; off a TTY `run_child` inherits stdio for the plain CI log.
        //
        // The build tags directly with `reference` (the registry-qualified tag
        // in registry mode) so the distribution step is a plain `kind load` /
        // `docker push` with no intermediate re-tag.
        if let Some(sink) = &cx.progress {
            sink.note(&id, "building");
        }
        let argv = image::docker_build_argv(
            &dockerfile,
            &context,
            &self.entry.features,
            &reference,
            self.entry.rust_version.as_deref(),
        );
        let envs = [("DOCKER_BUILDKIT", "1".to_string())];
        self.run_streamed(cx, "docker", &argv, &envs, "docker build")
            .await?;

        match &self.dist {
            Distribution::Kind => {
                if let Some(sink) = &cx.progress {
                    sink.note(&id, format!("load → kind {}", image::kind_cluster_name()));
                }
                let argv = image::kind_load_argv(&reference);
                self.run_streamed(cx, "kind", &argv, &[], "kind load")
                    .await?;
            }
            Distribution::Registry { .. } => {
                if let Some(sink) = &cx.progress {
                    sink.note(&id, "push→registry");
                }
                let argv = image::docker_push_argv(&reference);
                self.run_streamed(cx, "docker", &argv, &[], "docker push")
                    .await?;
            }
            // Handled by provision_internal above.
            Distribution::Internal { .. } => unreachable!(),
        }

        Ok(())
    }
}

impl ImageProvider {
    /// The in-process push target for [`Distribution::Internal`]: the push
    /// (route) reference plus the token+CA read from the active kubeconfig.
    fn internal_target(&self) -> Result<oci::Target, String> {
        let push_reference = self
            .dist
            .push_reference(&self.tag)
            .ok_or("internal distribution has no push reference")?;
        image::internal_push_target(push_reference)
    }

    /// Build an OCI layout with buildx and push it in-process, streaming
    /// per-blob progress to the right-hand transfer panel. Self-contained: the
    /// push reuses the kubeconfig's SA token + cluster CA, so it needs no
    /// `docker login`, no `/etc/docker/certs.d`, and no `sudo`.
    async fn provision_internal(&self, cx: &Cx) -> Result<(), ResourceError> {
        let id = self.id();
        let (dockerfile, context) = self.entry.source.materialize().map_err(|e| {
            ResourceError::Provision(format!("resolve image source {}: {e}", self.tag))
        })?;
        let target = self
            .internal_target()
            .map_err(|e| ResourceError::Provision(format!("registry credentials: {e}")))?;

        // Buildx's OCI exporter needs a `docker-container` driver builder; the
        // default `docker` driver can't `--output type=oci`. Idempotent create.
        self.ensure_buildx_builder(cx).await?;

        let work = std::env::temp_dir().join(format!("ztest-oci-{}", slug(&self.tag)));
        let _ = std::fs::remove_dir_all(&work);
        std::fs::create_dir_all(&work)
            .map_err(|e| ResourceError::Provision(format!("scratch dir: {e}")))?;
        let tar = work.join("image.tar");
        let layout = work.join("layout");

        if let Some(sink) = &cx.progress {
            sink.note(&id, "building");
        }
        let build_contexts = match self.runner_base_context(cx, &work).await {
            Ok(bc) => bc,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&work);
                return Err(e);
            }
        };
        let argv = image::buildx_oci_argv(
            &dockerfile,
            &context,
            &self.entry.features,
            &self.tag,
            &tar,
            self.entry.rust_version.as_deref(),
            &build_contexts,
        );
        let envs = [("DOCKER_BUILDKIT", "1".to_string())];
        let build = self
            .run_streamed(cx, "docker", &argv, &envs, "buildx build")
            .await;
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
                } => sink.bytes(&report_id, pushed_bytes, total_bytes, format!("layer {n}/{total}")),
                oci::PushProgress::Manifest => sink.finalizing(&report_id),
            }
        };
        let result = oci::push_layout(&layout, &target, &report).await;
        let _ = std::fs::remove_dir_all(&work);
        result.map_err(|e| ResourceError::Provision(format!("push {}: {e}", self.tag)))
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
        &self,
        cx: &Cx,
        work: &std::path::Path,
    ) -> Result<Vec<(String, String)>, ResourceError> {
        if self.entry.repo != crate::engine::pod_runner::RUNNER_REPO {
            return Ok(Vec::new());
        }
        let base_tag = crate::engine::pod_runner::RUNNER_BASE_LOCAL_TAG;
        let layout = work.join("base-layout");

        if let Some(sink) = &cx.progress {
            sink.note(&self.id(), "runner base → oci");
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
            let id = self.id();
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
    async fn ensure_buildx_builder(&self, cx: &Cx) -> Result<(), ResourceError> {
        let name = image::BUILDX_BUILDER;
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
            sink.note(&self.id(), "buildx builder");
        }
        let argv = vec![
            "buildx".to_string(),
            "create".to_string(),
            "--name".to_string(),
            name.to_string(),
            "--driver".to_string(),
            "docker-container".to_string(),
        ];
        self.run_streamed(cx, "docker", &argv, &[], "buildx create")
            .await
    }
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
