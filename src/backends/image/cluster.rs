//! `buildctl` in the run's BuildKit pod — every image the remote path builds.
//!
//! - Selected on [`builds_on_cluster`]; the workstation engine is out of the path entirely
//! - Build context crosses the workstation link, image never does
//! - Same `dev-<hash>` tag & build-args as [`LocalEngine`] → one cache either way
//!
//! [`builds_on_cluster`]: super::builds_on_cluster
//! [`LocalEngine`]: super::local::LocalEngine

use std::path::Path;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;

use super::buildpod::{self, Route, shell_quote, tail};
use super::{BuildRequest, Built, Dockerfile, ImageProvider, Output, note};
use crate::naming::RUN_NAMESPACE;
use crate::resource::impls::buildkit::WORK_MOUNT;
use crate::resource::{Cx, Readiness, ResourceError};

/// Stateless — push target = [`pod_reference`](super::pod_reference) (a second copy is what drifts)
#[derive(Debug)]
pub(crate) struct RemoteBuildkit;

#[async_trait]
impl ImageProvider for RemoteBuildkit {
    /// Unprobed — BuildKit's cache is the skip (a registry round trip the workstation
    /// cannot make to an in-cluster address, to spare a build that is already cached)
    async fn exists(&self, _cx: &Cx, _tag: &str) -> Readiness {
        Readiness::Absent
    }

    async fn build(&self, cx: &Cx, req: &BuildRequest) -> Result<Built, ResourceError> {
        let label = req.label();
        let pod = cx.build_pod.as_deref().ok_or_else(|| {
            ResourceError::Provision(format!(
                "no build pod for {label}: an on-cluster image build needs the BuildKit pod \
                 `ztest run` creates per build"
            ))
        })?;
        let api: Api<Pod> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);

        // Content-keyed, so the two builds of one recipe (image + inventory export) share
        // one shipped tree. Sound because the build pod is created per run: nothing under
        // a key changes while the pod that holds it lives
        let ctx_dir = format!("{WORK_MOUNT}/ctx/{}", req.context.key());
        if !buildpod::dir_exists(&api, pod, &ctx_dir).await {
            note(cx, req, "shipping context");
            buildpod::ship_context(&api, &req.context, pod, &ctx_dir)
                .await
                .map_err(|e| ResourceError::Provision(format!("ship context for {label}:\n{e}")))?;
        }

        // Staged beside the context, never into it: an in-context Dockerfile would change
        // the shipped tree, hence the content hash the tag is built from
        let text = dockerfile_text(&req.dockerfile)?;
        let df_dir = format!("{WORK_MOUNT}/df/{}", super::short_hash(text.as_bytes()));
        if !buildpod::dir_exists(&api, pod, &df_dir).await {
            buildpod::stage_file(&api, pod, &df_dir, "Dockerfile", &text).await.map_err(|e| {
                ResourceError::Provision(format!("stage Dockerfile for {label}:\n{e}"))
            })?;
        }

        note(cx, req, "building");
        let route = Route::of(cx.host.as_deref());
        let export = Export::of(req, &label);
        let cmd = buildctl_cmd(req, &ctx_dir, &df_dir, &export, &route);
        let to_stderr = |line: &str| eprintln!("{line}");
        let (tail_out, code) =
            buildpod::exec_build(&api, pod, &cmd, &route, Some(&to_stderr)).await.map_err(|e| {
                ResourceError::Provision(format!("build {label} in the build pod:\n{e}"))
            })?;
        if code != 0 {
            return Err(ResourceError::Provision(format!(
                "build {label} failed (exit {code}):\n{}",
                tail(&tail_out, 40)
            )));
        }

        match export {
            Export::Image { reference } => Ok(Built::Image(reference)),
            Export::Files { pod_dir, dest } => {
                buildpod::fetch_into(&api, pod, &pod_dir, dest).await.map_err(|e| {
                    ResourceError::Provision(format!("fetch exported files for {label}:\n{e}"))
                })?;
                Ok(Built::Files)
            }
        }
    }
}

/// [`Output`] resolved against the pod: the pull reference to publish under, or where the
/// export lands on both sides. Held so the command, the auth prelude and the post-build
/// step read one value rather than re-deriving it
enum Export<'a> {
    Image { reference: String },
    Files { pod_dir: String, dest: &'a Path },
}

impl<'a> Export<'a> {
    fn of(req: &'a BuildRequest, label: &str) -> Export<'a> {
        match &req.output {
            Output::Image { tag } => Export::Image { reference: super::pod_reference(tag) },
            Output::Files { dest } => Export::Files {
                pod_dir: format!("{WORK_MOUNT}/out/{}", super::short_hash(label.as_bytes())),
                dest,
            },
        }
    }
}

/// `buildctl build` shell for one request.
///
/// - Pushing build + a configured push Secret → point `DOCKER_CONFIG` at its mount;
///   unset = anonymous push, which is every registry reachable only in-cluster
/// - Credentials are the Secret's to hold: no token is minted, printed or logged here
fn buildctl_cmd(
    req: &BuildRequest,
    ctx_dir: &str,
    df_dir: &str,
    export: &Export<'_>,
    route: &Route<'_>,
) -> String {
    let auth = match (export, super::push_secret()) {
        (Export::Image { .. }, Some(_)) => {
            format!("export DOCKER_CONFIG={}\n", crate::resource::impls::buildkit::REGISTRY_MOUNT)
        }
        _ => String::new(),
    };
    let target = req
        .target
        .as_ref()
        .map(|t| format!(" --opt target={}", shell_quote(t)))
        .unwrap_or_default();
    let opts: String = req
        .build_args
        .iter()
        .map(|(k, v)| format!(" --opt {}", shell_quote(&format!("build-arg:{k}={v}"))))
        .collect();
    let output = match export {
        Export::Image { reference } => format!(
            "--output type=image,name={},push=true,{}",
            shell_quote(reference),
            super::image_output_attrs(super::registry_plaintext()),
        ),
        Export::Files { pod_dir, .. } => {
            format!("--output type=local,dest={}", shell_quote(pod_dir))
        }
    };
    format!(
        "set -eu\n\
         {auth}\
         buildctl build --frontend dockerfile.v0 \
           --local context={ctx} --local dockerfile={df} --opt filename=Dockerfile\
           {target}{opts} \
           {output} --progress={progress}\n",
        ctx = shell_quote(ctx_dir),
        df = shell_quote(df_dir),
        progress = route.progress(),
    )
}

fn dockerfile_text(df: &Dockerfile) -> Result<String, ResourceError> {
    match df {
        Dockerfile::Text(text) => Ok(text.clone()),
        Dockerfile::Path(path) => std::fs::read_to_string(path)
            .map_err(|e| ResourceError::Provision(format!("read {}: {e}", path.display()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn req(output: Output) -> BuildRequest {
        BuildRequest {
            context: super::super::Context::dir(PathBuf::from("/ctx")),
            dockerfile: Dockerfile::Path(PathBuf::from("/ctx/Dockerfile")),
            target: None,
            build_args: vec![("FEATURES".into(), "a,b".into())],
            output,
        }
    }

    fn cmd(r: &BuildRequest) -> String {
        buildctl_cmd(r, "/w/ctx", "/w/df", &Export::of(r, &r.label()), &Route::Lines)
    }

    /// Context and Dockerfile are separate `--local` roots: staging the Dockerfile into the
    /// context would change the tree the tag hashes
    #[test]
    fn buildctl_separates_context_and_dockerfile_roots() {
        let cmd = cmd(&req(Output::Image { tag: "zainod:dev-abc".into() }));
        assert!(cmd.contains("--local context='/w/ctx'"), "{cmd}");
        assert!(cmd.contains("--local dockerfile='/w/df'"), "{cmd}");
        assert!(cmd.contains("--opt 'build-arg:FEATURES=a,b'"), "{cmd}");
    }

    /// No emulator on the host → BuildKit's TTY UI would render as escape soup
    #[test]
    fn progress_follows_the_output_route() {
        assert!(
            cmd(&req(Output::Image { tag: "zainod:dev-abc".into() })).contains("--progress=plain")
        );
    }

    /// An export neither tags nor pushes, so it must not reach for the push Secret
    #[test]
    fn a_files_output_exports_locally_and_never_authenticates() {
        let cmd = cmd(&req(Output::Files { dest: PathBuf::from("/out") }));
        assert!(cmd.contains(&format!("--output type=local,dest='{WORK_MOUNT}/out/")), "{cmd}");
        assert!(!cmd.contains("push=true"), "{cmd}");
        assert!(!cmd.contains("DOCKER_CONFIG"), "{cmd}");
    }

    #[test]
    fn a_target_selects_one_stage() {
        let mut r = req(Output::Image { tag: "z:dev-x".into() });
        r.target = Some("runner".into());
        assert!(cmd(&r).contains("--opt target='runner'"));
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
}
