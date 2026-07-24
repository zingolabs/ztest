//! Core-component image mirror (`ztest setup`, OpenShift targets).
//!
//! A wide test wave means many concurrent cold Docker Hub pulls of component
//! images, which stall (bandwidth + anonymous rate limits) and time tests out.
//! This provider mirrors those images into the in-cluster registry and redirects
//! the node's CRI-O at it, so the pulls are LAN-local.
//!
//! Two parts:
//! 1. An [`ImageTagMirrorSet`] rewriting `docker.io/<repo>` → the internal
//!    registry with Hub fallback (`AllowContactingSource`). CRI-O redirects
//!    transparently, so pod specs are untouched. Applying an ITMS drains +
//!    reboots the node (MCO) — a one-time setup cost; the config is stable.
//! 2. A path-preserving buildkit-native `FROM <hub> + push` of each curated image
//!    (from [`image::mirror_set`]) into the registry, so the mirror resolves.
//!
//! Curated (not auto-discovered) because each downstream suite pins its own
//! component versions imperatively. An image absent from the set still works via
//! the ITMS Hub fallback.

use std::collections::BTreeMap;

use async_trait::async_trait;
use kube::api::{Patch, PatchParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use kube::{Api, Client};
use serde_json::json;

use crate::backends::image::{self, docker, openshift};
use crate::resource::impls::policy::RUN_NAMESPACE;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// The singleton ITMS name.
const ITMS_NAME: &str = "ztest-component-images";
const FIELD_MANAGER: &str = "ztest";

#[derive(Debug)]
pub(crate) struct ImageMirrorProvider;

#[async_trait]
impl Provider for ImageMirrorProvider {
    fn id(&self) -> NodeId {
        NodeId::ImageMirror
    }

    fn deps(&self) -> Vec<NodeId> {
        // The registry project must exist to push into; the mirror is a
        // buildkit-native `FROM <hub> + push` (see `openshift::mirror_image`), so
        // it needs the BuildKit scaffolding (SCC/SA/config/cache), not the retired
        // builder pod. The ephemeral build pod itself is created by the setup
        // entrypoint, not a graph node.
        vec![
            NodeId::Namespace(RUN_NAMESPACE.to_string()),
            NodeId::RegistryProject,
            NodeId::Buildkit,
        ]
    }

    fn lifetime(&self) -> Lifetime {
        // Never torn down: deleting the ITMS reboots the node, and the mirrored
        // blobs are a cross-run cache — the whole point of the internal registry.
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let set = image::mirror_set();
        if set.is_empty() {
            return Readiness::Ready; // nothing configured to mirror
        }
        if itms_api(&cx.client).get(ITMS_NAME).await.is_err() {
            return Readiness::Absent;
        }
        let Some(base) = image::push_base().or_else(image::pull_base) else {
            return Readiness::Absent;
        };
        for hub in &set {
            if !docker::openshift_manifest_present(image::mirror_dest(&base, hub)).await {
                return Readiness::Absent;
            }
        }
        Readiness::Ready
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let set = image::mirror_set();
        if set.is_empty() {
            return Ok(());
        }
        apply_itms(&cx.client, &set).await?;

        let base = image::push_base()
            .or_else(image::pull_base)
            .ok_or_else(|| {
                ResourceError::Provision(
                    "no registry (ZTEST_IMAGE_REGISTRY unset) for the component-image mirror"
                        .into(),
                )
            })?;
        for hub in &set {
            let dest = image::mirror_dest(&base, hub);
            // Idempotent: skip an image already in the mirror (the buildkit push
            // would no-op anyway, but the probe avoids the exec round-trip entirely).
            if docker::openshift_manifest_present(dest.clone()).await {
                continue;
            }
            openshift::mirror_image(cx, hub, &dest).await?;
        }
        Ok(())
    }
}

/// Cluster-scoped `Api` for the `ImageTagMirrorSet` CRD.
fn itms_api(client: &Client) -> Api<DynamicObject> {
    let ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "config.openshift.io",
        "v1",
        "ImageTagMirrorSet",
    ));
    Api::all_with(client.clone(), &ar)
}

/// Server-side-apply the ITMS: one rule per unique source repo, each mirrored to
/// its path-preserving location under the internal registry's service address,
/// with Hub fallback. Re-applying identical content is a no-op; a *change*
/// reboots the node, so the rule set is derived deterministically.
async fn apply_itms(client: &Client, set: &[String]) -> Result<(), ResourceError> {
    let pull = image::pull_base().ok_or_else(|| {
        ResourceError::Provision("ZTEST_IMAGE_REGISTRY unset — cannot build the ITMS".into())
    })?;
    // One rule per source repo; BTreeMap keeps the order stable so the applied
    // spec doesn't churn (a spurious change would reboot the node).
    let mut by_source: BTreeMap<String, String> = BTreeMap::new();
    for hub in set {
        by_source.insert(image::mirror_source(hub), image::mirror_repo(&pull, hub));
    }
    let rules: Vec<_> = by_source
        .iter()
        .map(|(source, mirror)| {
            json!({
                "source": source,
                "mirrors": [mirror],
                "mirrorSourcePolicy": "AllowContactingSource",
            })
        })
        .collect();

    let obj: DynamicObject = serde_json::from_value(json!({
        "apiVersion": "config.openshift.io/v1",
        "kind": "ImageTagMirrorSet",
        "metadata": { "name": ITMS_NAME },
        "spec": { "imageTagMirrors": rules },
    }))
    .expect("static ITMS manifest is valid");

    itms_api(client)
        .patch(
            ITMS_NAME,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&obj),
        )
        .await
        .map(|_| ())
        .map_err(|e| ResourceError::Provision(format!("apply ImageTagMirrorSet {ITMS_NAME}: {e}")))
}
