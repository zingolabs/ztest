//! `dev!` images built by `buildctl` in the run's BuildKit pod.
//!
//! - Selected on [`builds_on_cluster`]; [`Docker`] = registry, no builder; [`Kind`] = no registry
//! - Build context crosses the workstation link, image never does
//! - Same `dev-<hash>` tag & build-args as [`Docker`] → one cache either way
//!
//! [`builds_on_cluster`]: super::builds_on_cluster
//! [`Docker`]: super::docker::Docker
//! [`Kind`]: super::kind::Kind

use std::path::Path;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;

use super::ImageProvider;
use crate::inventory::DevImageEntry;
use crate::naming::RUN_NAMESPACE;
use crate::pipeline::remote_compile::{exec_streamed, shell_quote, ship_context, stage_file, tail};
use crate::resource::impls::buildkit::WORK_MOUNT;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};

/// Stateless — push target = [`pod_reference`](super::pod_reference) (a second copy is what drifts)
#[derive(Debug)]
pub struct ClusterBuild;

#[async_trait]
impl ImageProvider for ClusterBuild {
    fn pull_secret(&self) -> Option<String> {
        super::pull_secret_env()
    }

    /// Unprobed — BuildKit's cache is the skip (a registry round trip the workstation
    /// cannot make to an in-cluster address, to spare a build that is already cached)
    async fn image_built(&self, _cx: &Cx, _entry: &DevImageEntry, _tag: &str) -> Readiness {
        Readiness::Absent
    }

    async fn build_image(
        &self,
        cx: &Cx,
        entry: &DevImageEntry,
        tag: &str,
    ) -> Result<String, ResourceError> {
        let pod = cx.build_pod.as_deref().ok_or_else(|| {
            ResourceError::Provision(format!(
                "no build pod for {tag}: an on-cluster image build needs the BuildKit pod \
                 `ztest run` creates per build"
            ))
        })?;
        let (dockerfile, context) = entry
            .source
            .materialize()
            .map_err(|e| ResourceError::Provision(format!("resolve image source {tag}: {e}")))?;
        let dockerfile_text = std::fs::read_to_string(&dockerfile)
            .map_err(|e| ResourceError::Provision(format!("read {}: {e}", dockerfile.display())))?;

        let id = NodeId::Image(tag.to_string());
        let api: Api<Pod> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let base = format!("{WORK_MOUNT}/dev/{}", crate::naming::slug(tag, 63));
        let reference = super::pod_reference(tag);

        if let Some(sink) = &cx.progress {
            sink.note(&id, "shipping context");
        }
        ship_context(&api, &context, pod, &format!("{base}/ctx"))
            .await
            .map_err(|e| ResourceError::Provision(format!("ship context for {tag}:\n{e}")))?;
        stage_file(&api, pod, &format!("{base}/df"), "Dockerfile", &dockerfile_text)
            .await
            .map_err(|e| ResourceError::Provision(format!("stage Dockerfile for {tag}:\n{e}")))?;

        if let Some(sink) = &cx.progress {
            sink.note(&id, "building");
        }
        // build-args off the *local* context (toolchain file read here, not pod-side → same
        // RUST_VERSION either backend)
        let cmd = buildctl_cmd(
            &base,
            &reference,
            &build_args(entry, &context),
            &super::image_output_attrs(super::registry_plaintext()),
        );
        let note = |line: &str| {
            if let Some(sink) = &cx.progress {
                sink.note(&id, line);
            }
        };
        let (_out, err, code) = exec_streamed(&api, pod, &cmd, Some(&note))
            .await
            .map_err(|e| ResourceError::Provision(format!("build {tag} in the build pod:\n{e}")))?;
        if code != 0 {
            return Err(ResourceError::Provision(format!(
                "build {tag} failed (exit {code}):\n{}",
                tail(&err, 40)
            )));
        }
        Ok(reference)
    }
}

/// Pair mirrors [`docker_build_argv`](super::docker_build_argv) — ztest `CARGO_FEATURES` +
/// upstream zcash `FEATURES`
fn build_args(entry: &DevImageEntry, context: &Path) -> Vec<String> {
    let mut args = Vec::new();
    if let Some(rv) = super::build_arg_rust_version(entry.rust_version.as_deref(), context) {
        args.push(format!("build-arg:RUST_VERSION={rv}"));
    }
    if !entry.features.is_empty() {
        let joined = entry.features.join(",");
        args.push(format!("build-arg:CARGO_FEATURES={joined}"));
        args.push(format!("build-arg:FEATURES={joined}"));
    }
    args
}

/// - `--progress=plain` (no PTY on this exec → live renderer has nothing to drive)
/// - push Secret held by the build pod ([`REGISTRY_MOUNT`](crate::resource::impls::buildkit::REGISTRY_MOUNT))
fn buildctl_cmd(base: &str, reference: &str, args: &[String], attrs: &str) -> String {
    let auth = match super::push_secret() {
        Some(_) => {
            format!("export DOCKER_CONFIG={}\n", crate::resource::impls::buildkit::REGISTRY_MOUNT)
        }
        None => String::new(),
    };
    let opts: String = args.iter().map(|a| format!(" --opt {}", shell_quote(a))).collect();
    format!(
        "set -eu\n\
         {auth}\
         buildctl build --frontend dockerfile.v0 \
           --local context={ctx} --local dockerfile={df} --opt filename=Dockerfile\
           {opts} \
           --output type=image,name={name},push=true,{attrs} --progress=plain\n",
        ctx = shell_quote(&format!("{base}/ctx")),
        df = shell_quote(&format!("{base}/df")),
        name = shell_quote(reference),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(features: &[&str]) -> DevImageEntry {
        DevImageEntry {
            repo: "zainod".into(),
            source: super::super::DevSource::Local {
                dockerfile: "Dockerfile".into(),
                context: ".".into(),
            },
            features: features.iter().map(|s| (*s).to_string()).collect(),
            rust_version: None,
        }
    }

    #[test]
    fn features_reach_both_build_arg_conventions() {
        let args = build_args(&entry(&["a", "b"]), Path::new("/nonexistent"));
        assert!(args.contains(&"build-arg:CARGO_FEATURES=a,b".to_string()), "{args:?}");
        assert!(args.contains(&"build-arg:FEATURES=a,b".to_string()), "{args:?}");
    }

    #[test]
    fn no_features_emits_no_build_args() {
        assert!(build_args(&entry(&[]), Path::new("/nonexistent")).is_empty());
    }

    #[test]
    fn a_scheme_states_the_transport_and_never_reaches_the_reference() {
        use super::super::scheme_of;

        assert_eq!(
            scheme_of("http://zot.zot.svc.cluster.local:5000/ztest"),
            (true, "zot.zot.svc.cluster.local:5000/ztest")
        );
        assert_eq!(scheme_of("https://ghcr.io/zingolabs"), (false, "ghcr.io/zingolabs"));
        assert_eq!(scheme_of("ghcr.io/zingolabs"), (false, "ghcr.io/zingolabs"));
    }

    /// `dev!` Dockerfile routinely sits outside the context it builds
    #[test]
    fn buildctl_separates_context_and_dockerfile_roots() {
        let cmd = buildctl_cmd("/build/dev/x", "reg/zainod:dev-abc", &[], "compression=zstd");
        assert!(cmd.contains("--local context='/build/dev/x/ctx'"), "{cmd}");
        assert!(cmd.contains("--local dockerfile='/build/dev/x/df'"), "{cmd}");
        assert!(cmd.contains("--opt filename=Dockerfile"), "{cmd}");
        assert!(cmd.contains("push=true"), "{cmd}");
    }

    /// Plaintext registry: push fails HTTPS-against-HTTP without the per-export attr
    #[test]
    fn plaintext_registry_marks_the_export_insecure() {
        use super::super::image_output_attrs;

        assert!(image_output_attrs(true).contains(",registry.insecure=true"));
        assert!(!image_output_attrs(false).contains("registry.insecure"));

        let cmd =
            buildctl_cmd("/build/dev/x", "zot:5000/z:dev-abc", &[], &image_output_attrs(true));
        assert!(cmd.contains(",registry.insecure=true"), "{cmd}");
    }
}
