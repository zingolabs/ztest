//! Generic Kubernetes scaffolding providers: namespaces and cluster-wide
//! node labels.
//!
//! These are the primitives every setup call needs — `zaino-seeds` and
//! `zaino-qos` namespaces, the NVMe node label — and are cleanly
//! parameterized so they don't grow one file per constant. Both are
//! [`Lifetime::Cached`]: created once by `ztest setup`, kept for the life
//! of the cluster.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Namespace, Node};
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use serde_json::json;

use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// A Kubernetes Namespace, created if absent. Idempotent; 409 counts as
/// success (a namespace that already exists satisfies the invariant).
#[derive(Debug)]
pub(crate) struct NamespaceProvider {
    name: String,
}

impl NamespaceProvider {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[async_trait]
impl Provider for NamespaceProvider {
    fn id(&self) -> NodeId {
        NodeId::Namespace(self.name.clone())
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<Namespace> = Api::all(cx.client.clone());
        match api.get(&self.name).await {
            Ok(_) => Readiness::Ready,
            Err(_) => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let api: Api<Namespace> = Api::all(cx.client.clone());
        let ns: Namespace = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": self.name },
        }))
        .expect("static namespace manifest is valid");
        match api.create(&PostParams::default(), &ns).await {
            Ok(_) => Ok(()),
            Err(e) if e_is_conflict_or_exists(&e) => Ok(()),
            Err(e) => Err(ResourceError::Provision(format!(
                "create namespace {}: {e}",
                self.name
            ))),
        }
    }
}

/// A cluster-wide node label applied to all schedulable nodes.
///
/// One provider owns one label key (the [`NodeId::NodeLabel`] carries the
/// key), so two independent providers can't fight over the same label.
/// The value is a construction-time parameter — most calls use the
/// `zaino.io/pool=nvme` label defined in [`crate::qos`].
#[derive(Debug)]
pub(crate) struct NodeLabelProvider {
    key: String,
    value: String,
}

impl NodeLabelProvider {
    pub(crate) fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[async_trait]
impl Provider for NodeLabelProvider {
    fn id(&self) -> NodeId {
        NodeId::NodeLabel(self.key.clone())
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        // Ready when every node already carries the label with the desired
        // value. One drift → Absent (re-apply).
        let api: Api<Node> = Api::all(cx.client.clone());
        let list = match api.list(&ListParams::default()).await {
            Ok(l) => l,
            Err(_) => return Readiness::Absent,
        };
        if list.items.is_empty() {
            return Readiness::Absent;
        }
        for node in &list.items {
            let matches = node
                .metadata
                .labels
                .as_ref()
                .and_then(|m| m.get(&self.key))
                .map(String::as_str)
                == Some(self.value.as_str());
            if !matches {
                return Readiness::Absent;
            }
        }
        Readiness::Ready
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let api: Api<Node> = Api::all(cx.client.clone());
        let list = api
            .list(&ListParams::default())
            .await
            .map_err(|e| ResourceError::Provision(format!("list nodes: {e}")))?;
        // Merge-patch each node individually. `kubectl label nodes --all`
        // does the same under the hood; server-side apply would over-claim
        // ownership of node labels we don't manage.
        let patch = json!({ "metadata": { "labels": { &self.key: &self.value } } });
        let params = PatchParams::default();
        for node in list.items {
            let Some(name) = node.metadata.name else {
                continue;
            };
            api.patch(&name, &params, &Patch::Merge(&patch))
                .await
                .map_err(|e| {
                    ResourceError::Provision(format!(
                        "label node {name} with {}={}: {e}",
                        self.key, self.value
                    ))
                })?;
        }
        Ok(())
    }
}

/// A 409 (AlreadyExists) on create-only paths counts as success — the
/// invariant a create-then-noop guarantees is already met.
fn e_is_conflict_or_exists(e: &kube::Error) -> bool {
    match e {
        kube::Error::Api(resp) => resp.code == 409,
        // Some wrapper variants stringify these differently across kube
        // versions; fall back to a substring check for portability.
        other => {
            let s = other.to_string();
            s.contains("AlreadyExists") || s.contains("409")
        }
    }
}
