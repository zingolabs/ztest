//! On-cluster base images: the compile **builder** and the test-runner **base**.
//!
//! `ztest setup` builds both from repo Dockerfiles (`docker/*.Dockerfile`) in
//! the ztest-owned buildah pod — the same path component images use
//! ([`crate::backends::image::openshift::build_base_image`]). This replaces the
//! hand-seeded nix images: nothing is built or pushed from the laptop.
//!
//! Both are content-addressed on their Dockerfile bytes (`<repo>:d-<hash>`), so
//! an edit forks the tag and setup rebuilds; an unchanged Dockerfile resolves the
//! existing tag and `probe` reports it present, skipping the build. The builder
//! image is a dependency of the builder Deployment
//! ([`super::builder::BuilderDeploymentProvider`]); the runner base has no
//! in-graph dependents — it must exist before a run's `crane` bake appends the
//! compiled binaries onto it.

use async_trait::async_trait;

use crate::backends::image;
use crate::backends::image::{docker, openshift};
use crate::resource::impls::policy::IMAGES_NAMESPACE;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

fn deps() -> Vec<NodeId> {
    vec![
        NodeId::Namespace(IMAGES_NAMESPACE.to_string()),
        NodeId::RegistryProject,
        // The buildah build server does the build (`build_base_image` execs into
        // it) and pushes as its SA; both must exist first.
        NodeId::Buildah,
    ]
}

async fn present(push_ref: Option<String>) -> Readiness {
    match push_ref {
        Some(r) if docker::openshift_manifest_present(r.clone()).await => Readiness::Ready,
        _ => Readiness::Absent,
    }
}

#[derive(Debug)]
pub(crate) struct RunnerBaseImageProvider;

#[async_trait]
impl Provider for RunnerBaseImageProvider {
    fn id(&self) -> NodeId {
        NodeId::Image(image::runner_base_tag())
    }

    fn deps(&self) -> Vec<NodeId> {
        deps()
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, _cx: &Cx) -> Readiness {
        present(image::runner_base_push_ref()).await
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        openshift::build_base_image(cx, &image::runner_base_tag(), image::RUNNER_BASE_DOCKERFILE)
            .await
    }
}

#[derive(Debug)]
pub(crate) struct BuilderImageProvider;

#[async_trait]
impl Provider for BuilderImageProvider {
    fn id(&self) -> NodeId {
        NodeId::Image(image::builder_image_tag())
    }

    fn deps(&self) -> Vec<NodeId> {
        // Not a real content dependency — this serializes the two base-image
        // builds so their live buildah logs stream one at a time onto the grid
        // instead of interleaving. Runner base first (it's small and quick),
        // then the builder.
        let mut d = deps();
        d.push(NodeId::Image(image::runner_base_tag()));
        d
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, _cx: &Cx) -> Readiness {
        present(image::builder_push_ref()).await
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        openshift::build_base_image(cx, &image::builder_image_tag(), image::BUILDER_DOCKERFILE).await
    }
}
