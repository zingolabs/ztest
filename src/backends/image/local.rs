//! Host container engine (docker/podman) build, published by registry push or kind side-load.
//!
//! - One builder, two [`Publish`] strategies: *how the cluster is given the result*, never
//!   how it is produced
//! - Same strategy answers [`exists`](ImageProvider::exists) — where the image lives is
//!   also where its presence is asked

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::buildpod::TempDir;
use super::kind;
use super::{BuildRequest, Built, Dockerfile, ImageError, ImageProvider, Output, join, note};
use crate::resource::{Cx, Readiness, ResourceError};
use crate::runtime;

/// How a locally built image reaches the kubelet
#[derive(Debug, Clone)]
pub(crate) enum Publish {
    /// Registry serving both push and pull, e.g. `ghcr.io/zingolabs`
    Push(String),
    /// kind node's containerd; no registry in the path, no pull secret
    SideLoad,
}

/// `staged` holds each git-selected context for this engine's life, so the two builds of
/// one recipe (image + inventory export) extract the tree once, not twice
#[derive(Debug)]
pub(crate) struct LocalEngine {
    publish: Publish,
    staged: Mutex<HashMap<String, Arc<TempDir>>>,
}

impl LocalEngine {
    pub(crate) fn push(registry: String) -> LocalEngine {
        LocalEngine::new(Publish::Push(registry))
    }

    pub(crate) fn side_load() -> LocalEngine {
        LocalEngine::new(Publish::SideLoad)
    }

    fn new(publish: Publish) -> LocalEngine {
        LocalEngine { publish, staged: Mutex::new(HashMap::new()) }
    }

    fn build_argv(&self, req: &BuildRequest, dockerfile: &Path, context: &Path) -> Vec<String> {
        let mut argv =
            vec!["build".to_string(), "-f".to_string(), dockerfile.display().to_string()];
        if let Some(target) = &req.target {
            argv.push("--target".to_string());
            argv.push(target.clone());
        }
        for (k, v) in &req.build_args {
            argv.push("--build-arg".to_string());
            argv.push(format!("{k}={v}"));
        }
        match &req.output {
            Output::Image { tag } => {
                argv.push("-t".to_string());
                argv.push(self.reference(tag));
            }
            Output::Files { dest } => {
                argv.push("--output".to_string());
                argv.push(format!("type=local,dest={}", dest.display()));
            }
        }
        argv.push(context.display().to_string());
        argv
    }

    /// Git-selected into a temp dir, always — a raw root ships `target/`, `.git` and
    /// gitignored archives past the [`CONTEXT_MAX_BYTES`](super::buildpod::CONTEXT_MAX_BYTES)
    /// ceiling, and the tag hashes the *selection*, so a wider build moves bytes under a
    /// fixed tag. Staged copies are keyed, so one recipe stages once per run
    fn stage_context(
        &self,
        cx: &Cx,
        req: &BuildRequest,
    ) -> Result<(PathBuf, Arc<TempDir>), ResourceError> {
        let key = req.context.key();
        let mut staged = self.staged.lock().expect("staged context mutex poisoned");
        if let Some(tmp) = staged.get(&key) {
            return Ok((tmp.path().join("ctx"), Arc::clone(tmp)));
        }
        note(cx, req, "staging the build context");
        let tmp = Arc::new(
            TempDir::new("ztest-ctx").map_err(|e| ResourceError::Provision(e.to_string()))?,
        );
        let dir = tmp.path().join("ctx");
        std::fs::create_dir_all(&dir)
            .map_err(|e| ResourceError::Provision(format!("create build context dir: {e}")))?;
        super::buildpod::extract_context(&req.context, &dir)
            .map_err(|e| ResourceError::Provision(e.to_string()))?;
        staged.insert(key, Arc::clone(&tmp));
        Ok((dir, tmp))
    }

    async fn publish_image(
        &self,
        cx: &Cx,
        req: &BuildRequest,
        tag: &str,
    ) -> Result<(), ResourceError> {
        let rt = runtime::active();
        let reference = self.reference(tag);
        match &self.publish {
            Publish::Push(_) => {
                note(cx, req, "push→registry");
                let argv = vec!["push".to_string(), reference];
                super::run_streamed(cx, tag, rt.as_str(), &argv, &[], "push").await
            }
            Publish::SideLoad => {
                note(cx, req, format!("load → kind {}", kind::kind_cluster_name()));
                kind::side_load(cx.host.as_deref(), rt, tag)
                    .await
                    .map_err(|e| ResourceError::Provision(format!("side-load {reference}: {e}")))?;
                kind::confirm_loaded(tag).await.map_err(|e| ResourceError::Provision(e.to_string()))
            }
        }
    }
}

#[async_trait]
impl ImageProvider for LocalEngine {
    /// What the build tags *and* what the pod pulls — one derivation, so a push needs no re-tag
    fn reference(&self, tag: &str) -> String {
        match &self.publish {
            Publish::Push(registry) => join(registry, tag),
            Publish::SideLoad => runtime::active().local_reference(tag),
        }
    }

    async fn exists(&self, _cx: &Cx, tag: &str) -> Readiness {
        // Shell-out kept off the async worker. Query error (registry or node unreachable)
        // → `Absent`, so (re)build rather than assume present
        let (publish, reference, tag) =
            (self.publish.clone(), self.reference(tag), tag.to_string());
        let read = tokio::task::spawn_blocking(move || match publish {
            Publish::Push(_) => exists_in_registry(&reference),
            Publish::SideLoad => kind::exists_in_kind(&tag),
        })
        .await;
        if matches!(read, Ok(Ok(true))) { Readiness::Ready } else { Readiness::Absent }
    }

    async fn build(&self, cx: &Cx, req: &BuildRequest) -> Result<Built, ResourceError> {
        // Missing kind cluster reported before the multi-minute build, not after it
        if matches!(self.publish, Publish::SideLoad) {
            tokio::task::spawn_blocking(kind::ensure_kind_cluster)
                .await
                .map_err(|e| ResourceError::Provision(format!("kind preflight: {e}")))?
                .map_err(|e| ResourceError::Provision(e.to_string()))?;
        }

        let (dockerfile, _df_tmp) = materialize_dockerfile(&req.dockerfile)?;
        let (context, _ctx_tmp) = self.stage_context(cx, req)?;

        note(cx, req, "building");
        let rt = runtime::active();
        let argv = self.build_argv(req, &dockerfile, &context);
        super::run_streamed(cx, &req.label(), rt.as_str(), &argv, &rt.build_envs(), "build")
            .await?;

        match &req.output {
            Output::Image { tag } => {
                self.publish_image(cx, req, tag).await?;
                Ok(Built::Image(self.reference(tag)))
            }
            Output::Files { .. } => Ok(Built::Files),
        }
    }
}

/// `Text` → a real file (the engine takes only `-f <path>`); the temp dir rides back so it
/// outlives the build
fn materialize_dockerfile(df: &Dockerfile) -> Result<(PathBuf, Option<TempDir>), ResourceError> {
    match df {
        Dockerfile::Path(p) => Ok((p.clone(), None)),
        Dockerfile::Text(text) => {
            let tmp =
                TempDir::new("ztest-df").map_err(|e| ResourceError::Provision(e.to_string()))?;
            let path = tmp.path().join("Dockerfile");
            std::fs::write(&path, text)
                .map_err(|e| ResourceError::Provision(format!("write Dockerfile: {e}")))?;
            Ok((path, Some(tmp)))
        }
    }
}

/// `manifest inspect`: exit 0 = present, anything else = `false` (both engines reach the
/// remote registry; false negative → rebuild+push, whose own failure carries the real error)
pub(crate) fn exists_in_registry(reference: &str) -> Result<bool, ImageError> {
    let engine = runtime::program();
    let out = Command::new(engine)
        .args(["manifest", "inspect", reference])
        .output()
        .map_err(|err| ImageError::spawn(format!("{engine} manifest inspect {reference}"), err))?;
    Ok(out.status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn req(output: Output) -> BuildRequest {
        BuildRequest {
            context: super::super::Context::dir(PathBuf::from("/ctx")),
            dockerfile: Dockerfile::Path(PathBuf::from("/ctx/Dockerfile")),
            target: Some("runner".into()),
            build_args: vec![("FEATURES".into(), "a,b".into())],
            output,
        }
    }

    fn argv(e: &LocalEngine, output: Output) -> Vec<String> {
        e.build_argv(&req(output), Path::new("D"), Path::new("/ctx"))
    }

    /// Tag carries the registry already — the push address and the build tag are one string
    #[test]
    fn an_image_build_tags_with_the_reference_the_pod_will_pull() {
        let e = LocalEngine::push("ghcr.io/zingolabs".into());
        let argv = argv(&e, Output::Image { tag: "zainod:dev-abc".into() });
        let t = argv.iter().position(|a| a == "-t").expect("-t");
        assert_eq!(argv[t + 1], "ghcr.io/zingolabs/zainod:dev-abc");
        assert_eq!(argv[t + 1], e.reference("zainod:dev-abc"));
    }

    /// A `Files` output never tags and never pushes — it is the inventory dump, not an image
    #[test]
    fn a_files_output_exports_locally_instead_of_tagging() {
        let argv = argv(&LocalEngine::side_load(), Output::Files { dest: PathBuf::from("/out") });
        assert!(!argv.iter().any(|a| a == "-t"), "{argv:?}");
        let o = argv.iter().position(|a| a == "--output").expect("--output");
        assert_eq!(argv[o + 1], "type=local,dest=/out");
    }

    #[test]
    fn target_and_build_args_reach_the_engine() {
        let argv = argv(&LocalEngine::side_load(), Output::Image { tag: "z:dev-x".into() });
        let t = argv.iter().position(|a| a == "--target").expect("--target");
        assert_eq!(argv[t + 1], "runner");
        let b = argv.iter().position(|a| a == "--build-arg").expect("--build-arg");
        assert_eq!(argv[b + 1], "FEATURES=a,b");
        assert_eq!(argv.last().unwrap(), "/ctx", "context is the trailing positional");
    }
}
