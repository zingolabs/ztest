//! OpenShift backend: on-cluster image builds via a ztest-owned **rootless
//! buildah pod** ([`crate::resource::impls::buildah`]).
//!
//! The only backend that builds *on the cluster* (kind loads into nodes; the
//! generic/`docker` backend builds locally and pushes). Nothing is built on the
//! laptop; images are built in the long-lived `ztest-buildah` pod with `buildah
//! bud` and pushed to the integrated registry over the in-cluster service —
//! authenticating with the pod SA's token exactly like the runner-image `crane`
//! bake ([`crate::pipeline::remote_compile`]).
//!
//! Two source shapes ([`DevSource`]):
//!   - **Git** (`run_build_git`): the pod shallow-fetches the pinned rev straight
//!     from the upstream repo (it has `git` + egress) and builds the checkout — no
//!     laptop clone, no context upload. Only the pinned build-args cross the wire.
//!   - **Local** (`run_build_local`, also the base images): the working tree is
//!     packed into a deterministic archive and `oc cp`'d into the pod — there is
//!     no ref to clone, the point being to test local changes.
//!
//! ## Why not OpenShift's Build subsystem
//!
//! The native docker-strategy `BuildConfig` (`oc start-build`) pins its build
//! pod's init containers to `quay.io/okd/scos-content` by digest, and OKD prunes
//! those digests from quay within ~72h on pre-release streams — so a day-old
//! cluster's first build dies `ImagePullBackOff: manifest unknown`. Building with
//! a pinned, retained public buildah image ([`buildah::BUILDAH_IMAGE`]) removes
//! that dependency on upstream registry retention entirely.
//!
//! The build context is serialized by [`bundle::pack`](super::bundle), the same
//! deterministic, symlink-safe, `.dockerignore`-aware packer that content-
//! addresses the image tag — so the archive is exactly the bytes the tag names,
//! and the chosen Dockerfile is staged at the archive root as `Dockerfile`.
//! `oc exec -t` streams `buildah`'s build log through the console PTY exactly like
//! a local `docker build`, so progress renders live in the grid.
//!
//! This backend also builds the base images themselves ([`build_base_image`],
//! driven by [`base_images`](crate::resource::impls::base_images) at `ztest
//! setup`) — the compile builder and the runner base, from `docker/*.Dockerfile`
//! — through the identical path.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};

use super::{DevSource, ImageProvider, bundle, docker, join};
use crate::inventory::DevImageEntry;
use crate::resource::impls::buildah::{BUILDAH_CONTAINER, BUILDAH_DEPLOYMENT, WORK_MOUNT};
use crate::resource::impls::policy;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};

/// How long to wait for the buildah build pod to become Ready before giving up.
const BUILDAH_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5 * 60);
const POLL: std::time::Duration = std::time::Duration::from_secs(2);

/// Push and pull addresses differ: probes use the external `push` route (where
/// the laptop authenticates), while the buildah pod pushes — and pods pull —
/// through the in-cluster `pull` service. Both front the same registry storage.
#[derive(Debug)]
pub(crate) struct OpenShift {
    pub(crate) push: String,
    pub(crate) pull: String,
}

#[async_trait]
impl ImageProvider for OpenShift {
    fn pull_secret(&self) -> Option<String> {
        // Pods pull the in-cluster service with the SA's auto-injected creds
        // (the `system:image-puller` grant); a pull secret is never needed.
        None
    }

    async fn image_built(&self, _cx: &Cx, _entry: &DevImageEntry, tag: &str) -> Readiness {
        if docker::openshift_manifest_present(join(&self.push, tag)).await {
            Readiness::Ready
        } else {
            Readiness::Absent
        }
    }

    async fn build_image(
        &self,
        cx: &Cx,
        entry: &DevImageEntry,
        tag: &str,
    ) -> Result<String, ResourceError> {
        match &entry.source {
            // A git dev image clones ON the cluster — the build pod has `git` and
            // egress, so nothing but the pinned build-args crosses the wire. No
            // laptop clone, no context upload (the vestige of `oc start-build`,
            // whose binary source *required* uploading an archive).
            DevSource::Git {
                url,
                rev,
                dockerfile,
                context,
            } => run_build_git(cx, tag, url, rev, dockerfile, context, &build_args_git(entry)).await?,
            // A local dev image is the working tree: pack it and upload it (there
            // is no ref to clone — the point is testing local changes).
            DevSource::Local {
                dockerfile,
                context,
            } => run_build_local(cx, tag, dockerfile, context, &build_args(entry, context)).await?,
        }
        Ok(self.reference(tag))
    }
}

impl OpenShift {
    /// The in-cluster pull reference pods use and the buildah pod pushes to — the
    /// same string the build manifest records.
    pub(super) fn reference(&self, tag: &str) -> String {
        join(&self.pull, tag)
    }
}

/// Build a base image (the compile **builder** or the runner **base**) from an
/// embedded Dockerfile via the same buildah-pod path component images use. A base
/// image is pure `FROM` + `RUN`, so its context is just the staged Dockerfile.
/// `tag` is content-addressed (`<repo>:d-<hash>`), so the caller's probe skips
/// this when the Dockerfile is unchanged. Reused by
/// [`base_images`](crate::resource::impls::base_images) at `ztest setup`.
pub(crate) async fn build_base_image(
    cx: &Cx,
    tag: &str,
    dockerfile: &str,
) -> Result<(), ResourceError> {
    let work = std::env::temp_dir().join(format!("ztest-baseimg-{}", slug(tag)));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work)
        .map_err(|e| ResourceError::Provision(format!("scratch dir: {e}")))?;
    let df = work.join("Dockerfile");
    std::fs::write(&df, dockerfile)
        .map_err(|e| ResourceError::Provision(format!("stage Dockerfile: {e}")))?;

    let result = run_build_local(cx, tag, &df, &work, &[]).await;
    let _ = std::fs::remove_dir_all(&work);
    result
}

/// The in-cluster push target for `tag`: the `pull` service address (the buildah
/// pod pushes from inside the cluster; the same storage the external probe route
/// reads).
fn reference_for(tag: &str) -> Result<String, ResourceError> {
    Ok(join(
        &super::pull_base().ok_or_else(|| {
            ResourceError::Provision(
                "no in-cluster registry (ZTEST_IMAGE_REGISTRY unset) for the buildah push".into(),
            )
        })?,
        tag,
    ))
}

/// Build a **local** context: pack the working tree into a deterministic tar,
/// `oc cp` it into a fresh per-build dir in the buildah pod, and build+push from
/// there. Used by local `dev!` images and the base images.
async fn run_build_local(
    cx: &Cx,
    tag: &str,
    dockerfile: &Path,
    context: &Path,
    build_args: &[(String, String)],
) -> Result<(), ResourceError> {
    let id = NodeId::Image(tag.to_string());
    let reference = reference_for(tag)?;
    let build_dir = format!("{WORK_MOUNT}/ctx-{}", slug(tag));

    let work = std::env::temp_dir().join(format!("ztest-build-{}", slug(tag)));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work)
        .map_err(|e| ResourceError::Provision(format!("scratch dir: {e}")))?;

    let result = async {
        if let Some(sink) = &cx.progress {
            sink.note(&id, "staging context");
        }
        let tar = stage_archive(dockerfile, context, &work)?;

        let client = crate::cluster::client()
            .await
            .map_err(|e| ResourceError::Provision(format!("kube client: {e}")))?;
        if let Some(sink) = &cx.progress {
            sink.note(&id, "waiting for the buildah build server");
        }
        let pod = wait_for_buildah(&client).await?;

        if let Some(sink) = &cx.progress {
            sink.note(&id, "on-cluster build (buildah)");
        }
        let mut mkdir = exec_argv(&pod, false);
        mkdir.extend(["--", "mkdir", "-p", &build_dir].map(String::from));
        run_oc_quiet(&mkdir)?;
        let mut cp = oc_base("cp");
        cp.extend([
            tar.display().to_string(),
            format!("{}/{pod}:{build_dir}/ctx.tar", policy::RUN_NAMESPACE),
            "-c".to_string(),
            BUILDAH_CONTAINER.to_string(),
        ]);
        run_oc_quiet(&cp)?;

        let prep = format!(
            "cd {dir}\ntar -xf ctx.tar && rm -f ctx.tar",
            dir = shell_quote(&build_dir),
        );
        build_and_push(cx, tag, &pod, &reference, &build_dir, &prep, "Dockerfile", ".", build_args)
            .await
    }
    .await;

    let _ = std::fs::remove_dir_all(&work);
    result
}

/// Build a **git** context entirely on the cluster: the build pod shallow-fetches
/// the exact `rev` from `url` (it has `git` + egress) and builds from the checkout
/// — no laptop clone, no upload. `dockerfile`/`context` are repo-relative.
async fn run_build_git(
    cx: &Cx,
    tag: &str,
    url: &str,
    rev: &str,
    dockerfile: &str,
    context: &str,
    build_args: &[(String, String)],
) -> Result<(), ResourceError> {
    let id = NodeId::Image(tag.to_string());
    let reference = reference_for(tag)?;
    let build_dir = format!("{WORK_MOUNT}/git-{}", slug(tag));

    let client = crate::cluster::client()
        .await
        .map_err(|e| ResourceError::Provision(format!("kube client: {e}")))?;
    if let Some(sink) = &cx.progress {
        sink.note(&id, "waiting for the buildah build server");
    }
    let pod = wait_for_buildah(&client).await?;
    if let Some(sink) = &cx.progress {
        sink.note(&id, "on-cluster build (buildah, git clone)");
    }

    // Shallow single-rev fetch — the same init/fetch/checkout the laptop cache used
    // (`fetch_git_rev`), now run in the pod. Fetching a bare SHA works on GitHub.
    let prep = format!(
        "rm -rf {dir}\n\
         mkdir -p {dir}\n\
         cd {dir}\n\
         git init -q\n\
         git remote add origin {url}\n\
         git fetch -q --depth 1 origin {rev}\n\
         git checkout -q FETCH_HEAD",
        dir = shell_quote(&build_dir),
        url = shell_quote(url),
        rev = shell_quote(rev),
    );
    build_and_push(
        cx, tag, &pod, &reference, &build_dir, &prep, dockerfile, context, build_args,
    )
    .await
}

/// Find the `ztest-buildah` pod and wait until it reports Ready, returning its
/// name. Mirrors the compile builder's wait: the Deployment is applied at setup
/// and its image pull/rollout is asynchronous, so the first build blocks here.
async fn wait_for_buildah(client: &kube::Client) -> Result<String, ResourceError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), policy::RUN_NAMESPACE);
    let selector = "ztest.io/component=buildah";
    let start = std::time::Instant::now();
    loop {
        if let Ok(list) = api.list(&ListParams::default().labels(selector)).await {
            for p in &list.items {
                if pod_ready(p) {
                    return Ok(p.metadata.name.clone().expect("a listed pod has a name"));
                }
            }
        }
        if start.elapsed() >= BUILDAH_READY_TIMEOUT {
            return Err(ResourceError::Provision(format!(
                "buildah pod ({BUILDAH_DEPLOYMENT}) not Ready within {}s — is it provisioned \
                 (`ztest setup`) and its image pulled? Check `oc -n {} get pods -l ztest.io/component=buildah`",
                BUILDAH_READY_TIMEOUT.as_secs(),
                policy::RUN_NAMESPACE,
            )));
        }
        tokio::time::sleep(POLL).await;
    }
}

fn pod_ready(p: &Pod) -> bool {
    // A terminating pod (Recreate rollout after a spec change) keeps its
    // `Ready=True` condition for a moment after deletion starts; exec'ing into
    // it races the teardown and fails "container is not created or running".
    // Skip it so the wait blocks for the replacement pod.
    if p.metadata.deletion_timestamp.is_some() {
        return false;
    }
    p.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// `buildah bud` the prepared build dir (streaming the log through the console
/// PTY) and push to `reference`. `prep` is the shell that populates and `cd`s into
/// `build_dir` (untar an uploaded context, or a git clone); `dockerfile`/`context`
/// are the `-f` and context args relative to it. The per-build dir + the built
/// image are reaped afterward so the `vfs` graphroot keeps only cached base layers.
async fn build_and_push(
    cx: &Cx,
    tag: &str,
    pod: &str,
    reference: &str,
    build_dir: &str,
    prep: &str,
    dockerfile: &str,
    context: &str,
    build_args: &[(String, String)],
) -> Result<(), ResourceError> {
    // The SA token is read in-pod; `--tls-verify=false` accepts the registry
    // service's self-signed serving cert (in-cluster, single-tenant), matching the
    // crane bake's `--insecure`. `chroot` isolation needs no network flag — it
    // shares the pod netns, so RUN steps reach cluster DNS + egress to fetch
    // crates/packages (and the git clone in `prep` does too).
    let ba: String = build_args
        .iter()
        .map(|(k, v)| format!(" --build-arg {}", shell_quote(&format!("{k}={v}"))))
        .collect();
    let script = format!(
        "set -eu\n\
         {prep}\n\
         buildah bud --isolation chroot --storage-driver vfs{ba} -f {df} -t {ref_} {ctx}\n\
         TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)\n\
         buildah push --tls-verify=false --creds ztest:$TOKEN {ref_}\n",
        df = shell_quote(dockerfile),
        ref_ = shell_quote(reference),
        ctx = shell_quote(context),
    );
    let mut argv = exec_argv(pod, true);
    argv.extend(["--".to_string(), "sh".to_string(), "-c".to_string(), script]);
    let result = super::run_streamed(cx, tag, "oc", &argv, &[], "on-cluster buildah build").await;

    // Reap the build dir and the built image (best-effort). Cached base layers
    // (the `FROM` images) stay, so a rebuild reuses them.
    let cleanup = format!(
        "rm -rf {dir}; buildah rmi {ref_} >/dev/null 2>&1 || true",
        dir = shell_quote(build_dir),
        ref_ = shell_quote(reference),
    );
    let mut cargv = exec_argv(pod, false);
    cargv.extend(["--".to_string(), "sh".to_string(), "-c".to_string(), cleanup]);
    let _ = std::process::Command::new("oc")
        .args(&cargv)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    result
}

/// `oc <sub> [--context <ctx>]` — the base argv every `oc` subcommand starts
/// with. The kube *client* targets the profile's context in-memory, but `oc` is a
/// separate process honouring only the kubeconfig's current-context — without
/// pinning it here `oc` could act against a stale local context (the same footgun
/// `oc rsync` guards in [`remote_compile`](crate::pipeline::remote_compile)).
fn oc_base(sub: &str) -> Vec<String> {
    let mut argv = vec![sub.to_string()];
    if let Ok(ctx) = std::env::var(crate::cluster_config::KUBE_CONTEXT_ENV)
        && !ctx.is_empty()
    {
        argv.push("--context".to_string());
        argv.push(ctx);
    }
    argv
}

/// `oc exec [-t] [--context <ctx>] <pod> -c buildah -n ztest` — the prefix a
/// command appends its `-- <argv>` to. All oc-level flags precede the `--` the
/// caller adds, so they reach `oc`, not the exec'd command. `tty` allocates a PTY
/// so `buildah` streams its live build output into the console emulator.
fn exec_argv(pod: &str, tty: bool) -> Vec<String> {
    let mut argv = oc_base("exec");
    if tty {
        argv.push("-t".to_string());
    }
    argv.extend([
        pod.to_string(),
        "-c".to_string(),
        BUILDAH_CONTAINER.to_string(),
        "-n".to_string(),
        policy::RUN_NAMESPACE.to_string(),
    ]);
    argv
}

/// Run a quiet `oc` invocation (fully-formed argv), capturing output and erroring
/// on non-zero. For the non-streamed staging steps (mkdir, cp).
fn run_oc_quiet(argv: &[String]) -> Result<(), ResourceError> {
    let out = std::process::Command::new("oc")
        .args(argv)
        .output()
        .map_err(|e| ResourceError::Provision(format!("spawn `oc` (is `oc` on PATH?): {e}")))?;
    if !out.status.success() {
        return Err(ResourceError::Provision(format!(
            "`oc {}` failed: {}",
            argv.first().map(String::as_str).unwrap_or(""),
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(())
}

/// Write the deterministic source-bundle tar (via [`bundle::pack`]) for the
/// buildah build. Reusing the same packer that content-addresses the image tag
/// guarantees the archive is exactly the bytes the tag names; its symlink-safe,
/// `.dockerignore`-aware walk removes the `tar -h` dangling-symlink break. The
/// Dockerfile is staged at the archive root as `Dockerfile`.
fn stage_archive(dockerfile: &Path, context: &Path, work: &Path) -> Result<PathBuf, ResourceError> {
    let bundle = bundle::pack(context, dockerfile)
        .map_err(|e| ResourceError::Provision(format!("pack build context: {e}")))?;
    let tar = work.join("ctx.tar");
    std::fs::write(&tar, &bundle.tar)
        .map_err(|e| ResourceError::Provision(format!("write source archive: {e}")))?;
    Ok(tar)
}

/// Build-arg env for the Dockerfile compile: features under both the ztest
/// (`CARGO_FEATURES`) and upstream zcash (`FEATURES`) names, plus `RUST_VERSION`
/// resolved the same way the local docker path resolves it — the pinned version,
/// else the context's `rust-toolchain.toml` channel. Omitting it when unresolved
/// lets a Dockerfile's own `ARG RUST_VERSION` default stand; passing an empty
/// string is what produced the `rust:-bookworm` invalid-reference break.
fn build_args(entry: &DevImageEntry, context: &Path) -> Vec<(String, String)> {
    let mut env = Vec::new();
    if !entry.features.is_empty() {
        let joined = entry.features.join(",");
        env.push(("CARGO_FEATURES".to_string(), joined.clone()));
        env.push(("FEATURES".to_string(), joined));
    }
    if let Some(rv) = super::build_arg_rust_version(entry.rust_version.as_deref(), context) {
        env.push(("RUST_VERSION".to_string(), rv));
    }
    env
}

/// Build-arg env for a **git** source. Same features, but `RUST_VERSION` comes
/// only from the entry's *pinned* value — there is no local checkout to read a
/// `rust-toolchain.toml` from (that lives in the repo the pod clones), and an
/// unpinned version correctly falls through to the Dockerfile's own `ARG` default.
fn build_args_git(entry: &DevImageEntry) -> Vec<(String, String)> {
    let mut env = Vec::new();
    if !entry.features.is_empty() {
        let joined = entry.features.join(",");
        env.push(("CARGO_FEATURES".to_string(), joined.clone()));
        env.push(("FEATURES".to_string(), joined));
    }
    if let Some(rv) = entry.rust_version.as_deref() {
        env.push(("RUST_VERSION".to_string(), rv.to_string()));
    }
    env
}

/// Single-quote a value for `/bin/sh`, escaping embedded single quotes.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// `docker`-tag → filesystem/DNS-safe slug (scratch dir + in-pod build dir).
fn slug(tag: &str) -> String {
    tag.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::image::DevSource;

    fn entry(features: Vec<String>, rust_version: Option<String>) -> DevImageEntry {
        DevImageEntry {
            repo: "zainod".to_string(),
            source: DevSource::Local {
                dockerfile: std::path::PathBuf::from("/x/Dockerfile"),
                context: std::path::PathBuf::from("/x"),
            },
            features,
            rust_version,
        }
    }

    /// The regression that produced `FROM rust:-bookworm`: a context carrying a
    /// concrete `rust-toolchain.toml` channel must yield a `RUST_VERSION`
    /// build-arg even when the entry pins nothing.
    #[test]
    fn build_args_resolve_rust_version_from_toolchain_file() {
        let dir = std::env::temp_dir().join(format!("ztest-ba-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("rust-toolchain.toml"),
            "[toolchain]\nchannel = \"1.96.0\"\n",
        )
        .unwrap();
        let args = build_args(&entry(vec![], None), &dir);
        std::fs::remove_dir_all(&dir).ok();
        assert!(
            args.iter()
                .any(|(k, v)| k == "RUST_VERSION" && v == "1.96.0"),
            "toolchain channel must become a RUST_VERSION build-arg: {args:?}"
        );
    }

    #[test]
    fn features_set_both_arg_names() {
        let dir = std::env::temp_dir();
        let args = build_args(&entry(vec!["a".into(), "b".into()], None), &dir);
        assert!(
            args.iter()
                .any(|(k, v)| k == "CARGO_FEATURES" && v == "a,b")
        );
        assert!(args.iter().any(|(k, v)| k == "FEATURES" && v == "a,b"));
    }

    #[test]
    fn slug_is_filesystem_safe() {
        let s = slug("ztest-images/Zainod:dev-ABC123");
        assert!(s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
    }

    #[test]
    fn shell_quote_escapes_single_quotes() {
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
        assert_eq!(shell_quote("plain"), "'plain'");
    }
}
