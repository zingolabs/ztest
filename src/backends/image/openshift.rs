//! OpenShift backend: the only backend that builds *on the cluster* — in the
//! ephemeral ztest-owned BuildKit pod ([`crate::resource::impls::buildkit`]),
//! pushing to the integrated registry over the in-cluster service and
//! authenticating with the pod SA's token. Nothing is built on the laptop.
//!
//! Two source shapes ([`DevSource`]): **Git** (`run_build_git`) shallow-fetches
//! the pinned rev in the pod (it has `git` + egress), so only the build-args
//! cross the wire; **Local** (`run_build_local`, and the base images) packs the
//! working tree and `oc cp`s it in, to test local changes.
//!
//! The context is serialized by [`bundle::pack`](super::bundle), the same packer
//! that content-addresses the tag, so the archive is exactly the bytes the tag
//! names. `oc exec -t` gives `buildctl` a PTY so its `--progress=auto` UI renders
//! through the console emulator.
//!
//! Not OpenShift's own Build subsystem: its docker-strategy `BuildConfig` pins
//! init containers to `quay.io/okd/scos-content` digests that OKD prunes from
//! quay on pre-release streams, so a day-old cluster's first build dies
//! `ImagePullBackOff`. A pinned public BuildKit image removes that dependency.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;

use super::{DevSource, ImageProvider, bundle, docker, join};
use crate::inventory::DevImageEntry;
use crate::resource::impls::buildkit::{BUILDKIT_CONTAINER, WORK_MOUNT};
use crate::resource::impls::policy;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};

/// Push and pull addresses differ: probes use the external `push` route (where
/// the laptop authenticates), while the BuildKit pod pushes — and pods pull —
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
            // Git clones ON the cluster (the pod has `git` + egress), so only the
            // pinned build-args cross the wire — no laptop clone, no upload.
            DevSource::Git {
                url,
                rev,
                dockerfile,
                context,
            } => {
                run_build_git(
                    cx,
                    tag,
                    url,
                    rev,
                    dockerfile,
                    context,
                    &build_args_git(entry),
                )
                .await?
            }
            // Local is the working tree: pack and upload it (no ref to clone).
            DevSource::Local {
                dockerfile,
                context,
            } => run_build_local(cx, tag, dockerfile, context, &build_args(entry, context)).await?,
        }
        Ok(self.reference(tag))
    }
}

impl OpenShift {
    /// The in-cluster pull reference pods use and the BuildKit pod pushes to — the
    /// same string the build manifest records.
    pub(super) fn reference(&self, tag: &str) -> String {
        join(&self.pull, tag)
    }
}

/// Mirror one published Hub image into the internal registry with a buildkit-native
/// `FROM <hub>` build in the ephemeral build pod (see the body — no `crane`). A
/// re-run is cheap: an unchanged image no-ops in the content store. `dest_ref` is
/// the full path-preserving internal ref so an [`ImageTagMirrorSet`]
/// prefix-substitution resolves to it.
pub(crate) async fn mirror_image(
    cx: &Cx,
    hub_ref: &str,
    dest_ref: &str,
) -> Result<(), ResourceError> {
    let pod = build_pod_name(cx)?;
    let registry = dest_ref.split('/').next().unwrap_or_default();
    let build_dir = format!("{WORK_MOUNT}/mirror-{}", slug(dest_ref));
    // buildkit-native mirror (no crane): build a one-line `FROM <hub>` image and
    // push it to `dest`. Pull of the public `hub_ref` is anonymous; push to the
    // internal registry authenticates via the pod SA token in a docker
    // config.json (its service-ca TLS is trusted through the system store).
    let script = format!(
        "set -eu\n\
         export DOCKER_CONFIG=/tmp/.docker\n\
         mkdir -p {dir} \"$DOCKER_CONFIG\"\n\
         cd {dir}\n\
         printf 'FROM %s\\n' {hub} > Dockerfile\n\
         TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)\n\
         AUTH=$(printf 'ztest:%s' \"$TOKEN\" | base64 | tr -d '\\n')\n\
         printf '{{\"auths\":{{\"%s\":{{\"auth\":\"%s\"}}}}}}' {reg} \"$AUTH\" > \"$DOCKER_CONFIG/config.json\"\n\
         buildctl build --frontend dockerfile.v0 --local context=. --local dockerfile=. \
           --opt filename=Dockerfile --output type=image,name={dst},push=true,{comp} --progress=auto\n\
         rm -rf {dir}\n",
        dir = shell_quote(&build_dir),
        hub = shell_quote(hub_ref),
        reg = shell_quote(registry),
        dst = shell_quote(dest_ref),
        comp = super::IMAGE_OUTPUT_COMPRESSION,
    );
    let mut argv = exec_argv(&pod, true);
    argv.extend(["--".to_string(), "sh".to_string(), "-c".to_string(), script]);
    super::run_streamed(cx, hub_ref, "oc", &argv, &[], "mirror component image").await
}

/// The in-cluster push target for `tag`: the `pull` service address (the BuildKit
/// pod pushes from inside the cluster; the same storage the external probe route
/// reads).
fn reference_for(tag: &str) -> Result<String, ResourceError> {
    Ok(join(
        &super::pull_base().ok_or_else(|| {
            ResourceError::Provision(
                "no in-cluster registry (ZTEST_IMAGE_REGISTRY unset) for the buildkit push".into(),
            )
        })?,
        tag,
    ))
}

/// Build a **local** context: pack the working tree into a deterministic tar,
/// `oc cp` it into a fresh per-build dir in the BuildKit pod, and build+push from
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

        let pod = build_pod_name(cx)?;
        if let Some(sink) = &cx.progress {
            sink.note(&id, "on-cluster build (buildkit)");
        }
        let mut mkdir = exec_argv(&pod, false);
        mkdir.extend(["--", "mkdir", "-p", &build_dir].map(String::from));
        run_oc_quiet(&mkdir)?;
        let mut cp = oc_base("cp");
        cp.extend([
            tar.display().to_string(),
            format!("{}/{pod}:{build_dir}/ctx.tar", policy::RUN_NAMESPACE),
            "-c".to_string(),
            BUILDKIT_CONTAINER.to_string(),
        ]);
        run_oc_quiet(&cp)?;

        let prep = format!(
            "cd {dir}\ntar -xf ctx.tar && rm -f ctx.tar",
            dir = shell_quote(&build_dir),
        );
        build_and_push(
            cx,
            BuildJob {
                tag,
                pod: &pod,
                reference: &reference,
                build_dir: &build_dir,
                prep: &prep,
                dockerfile: "Dockerfile",
                context: ".",
                build_args,
            },
        )
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

    let pod = build_pod_name(cx)?;
    if let Some(sink) = &cx.progress {
        sink.note(&id, "on-cluster build (buildkit, git clone)");
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
        cx,
        BuildJob {
            tag,
            pod: &pod,
            reference: &reference,
            build_dir: &build_dir,
            prep: &prep,
            dockerfile,
            context,
            build_args,
        },
    )
    .await
}

/// The ephemeral BuildKit pod this invocation stood up (`cx.build_pod`), which
/// every on-cluster build `exec`s `buildctl` against. `None` means the build
/// phase ran without provisioning a pod — a harness bug on the build path.
fn build_pod_name(cx: &Cx) -> Result<String, ResourceError> {
    cx.build_pod.clone().ok_or_else(|| {
        ResourceError::Provision(
            "no ephemeral BuildKit pod for this invocation (cx.build_pod unset) — the build \
             phase must create one before any image build"
                .into(),
        )
    })
}

/// `buildctl build` the prepared build dir (streaming BuildKit's progress through
/// the console PTY) and push to `reference`. `prep` is the shell that populates
/// and `cd`s into `build_dir` (untar an uploaded context, or a git clone);
/// `dockerfile`/`context` are relative to it. Push authenticates via a docker
/// `config.json` written from the pod SA's token. Only the per-build context dir
/// is reaped afterward — the layer cache on the state PVC stays.
/// One on-cluster image build, beyond the ambient [`Cx`]. A struct so the call
/// sites name each field — `dockerfile` and `context` are both bare relative
/// paths, trivial to transpose as positional args.
struct BuildJob<'a> {
    tag: &'a str,
    pod: &'a str,
    reference: &'a str,
    /// Per-build context dir in the pod; reaped after the build.
    build_dir: &'a str,
    /// Shell that populates and `cd`s into `build_dir` (untar, or git clone).
    prep: &'a str,
    /// Dockerfile path, relative to `build_dir`.
    dockerfile: &'a str,
    /// Build-context path, relative to `build_dir`.
    context: &'a str,
    build_args: &'a [(String, String)],
}

async fn build_and_push(cx: &Cx, job: BuildJob<'_>) -> Result<(), ResourceError> {
    let BuildJob {
        tag,
        pod,
        reference,
        build_dir,
        prep,
        dockerfile,
        context,
        build_args,
    } = job;
    let df = Path::new(dockerfile);
    let df_name = df
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("Dockerfile");
    let df_dir = df
        .parent()
        .and_then(|p| p.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(".");
    // The registry host (no path) the push target names — the docker config.json
    // auth key below must match it. Its service-ca-signed serving cert is verified
    // (pull and push) via the `ca` entry for this host in the daemon's buildkitd.toml.
    let host = reference.split('/').next().unwrap_or(reference);

    let opts: String = build_args
        .iter()
        .map(|(k, v)| format!(" --opt {}", shell_quote(&format!("build-arg:{k}={v}"))))
        .collect();
    // BuildKit reads registry creds from a docker config.json written from the
    // in-pod SA token. `DOCKER_CONFIG` pins the dir explicitly — the daemon runs
    // as uid 0 with no guaranteed `$HOME`. `base64 | tr -d '\n'` keeps the JSON on
    // one line. RUN steps share the pod netns, reaching cluster DNS + egress.
    let script = format!(
        "set -eu\n\
         export DOCKER_CONFIG=/tmp/.docker\n\
         {prep}\n\
         mkdir -p \"$DOCKER_CONFIG\"\n\
         TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)\n\
         AUTH=$(printf 'ztest:%s' \"$TOKEN\" | base64 | tr -d '\\n')\n\
         printf '{{\"auths\":{{\"%s\":{{\"auth\":\"%s\"}}}}}}' {host} \"$AUTH\" > \"$DOCKER_CONFIG/config.json\"\n\
         buildctl build \
           --frontend dockerfile.v0 \
           --local context={ctx} \
           --local dockerfile={df_dir} \
           --opt filename={df_name}{opts} \
           --output type=image,name={ref_},push=true,{comp} \
           --progress=auto\n",
        host = shell_quote(host),
        ctx = shell_quote(context),
        df_dir = shell_quote(df_dir),
        df_name = shell_quote(df_name),
        ref_ = shell_quote(reference),
        comp = super::IMAGE_OUTPUT_COMPRESSION,
    );
    let mut argv = exec_argv(pod, true);
    argv.extend(["--".to_string(), "sh".to_string(), "-c".to_string(), script]);

    // The pod is already at its Guaranteed build footprint (created ephemeral at
    // that size), so the build runs directly — no in-place resize.
    let result = super::run_streamed(cx, tag, "oc", &argv, &[], "on-cluster buildkit build").await;

    // Reap the per-build context dir (best-effort). The layer cache in the state
    // PVC stays, so a rebuild reuses it.
    let cleanup = format!("rm -rf {dir}", dir = shell_quote(build_dir));
    let mut cargv = exec_argv(pod, false);
    cargv.extend([
        "--".to_string(),
        "sh".to_string(),
        "-c".to_string(),
        cleanup,
    ]);
    let _ = std::process::Command::new("oc")
        .args(&cargv)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    result
}

/// `oc <sub> [--context <ctx>]` — the base argv every `oc` subcommand starts
/// with. `oc` is a separate process honouring only the kubeconfig's
/// current-context, so without pinning `--context` here it could act against a
/// stale local context (unlike the in-memory kube client).
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

/// `oc exec [-i -t] [--context <ctx>] <pod> -c buildkit -n ztest` — the prefix a
/// command appends its `-- <argv>` to. All oc-level flags precede the `--` the
/// caller adds, so they reach `oc`, not the exec'd command. `tty` allocates a PTY
/// so `buildctl` renders its collapsing progress UI (`--progress=auto`) into the
/// console emulator instead of degrading to plain line output.
///
/// `-t` alone is not enough: kubectl's `SetupTTY` early-returns with `TTY=false`
/// (silently, no warning) unless stdin is also attached, so the container process
/// never sees a PTY. Passing `-i` alongside `-t` — with `oc` running under the
/// console's own PTY, so its stdin *is* a terminal — is what makes kubectl set
/// `t.Raw`, allocate the container TTY, and open the resize stream that forwards
/// SIGWINCH so `buildctl` re-wraps its UI when the terminal resizes. Nothing ever
/// writes to that stdin; it exists only to unlock the interactive TTY path.
fn exec_argv(pod: &str, tty: bool) -> Vec<String> {
    let mut argv = oc_base("exec");
    if tty {
        argv.push("-i".to_string());
        argv.push("-t".to_string());
    }
    argv.extend([
        pod.to_string(),
        "-c".to_string(),
        BUILDKIT_CONTAINER.to_string(),
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
/// BuildKit build. Reusing the same packer that content-addresses the image tag
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
