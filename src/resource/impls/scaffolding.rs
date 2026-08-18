//! Generic K8s scaffolding providers — namespaces + cluster-wide node labels.
//! Both [`Lifetime::Cached`] for the life of the cluster.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Namespace, Node};
use kube::api::{Api, ListParams, Patch, PatchParams};
use serde_json::json;

use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

#[derive(Debug)]
pub struct NamespaceProvider {
    name: String,
    psa_privileged: bool,
}

impl NamespaceProvider {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), psa_privileged: false }
    }

    /// Required by the BuildKit pod (unconfined seccomp/AppArmor exceeds
    /// *baseline*); without it the pod dies at admission, far from BuildKit
    pub fn pod_security_privileged(mut self) -> Self {
        self.psa_privileged = true;
        self
    }

    fn labels(&self) -> serde_json::Value {
        if self.psa_privileged { json!({ PSA_ENFORCE_LABEL: PSA_PRIVILEGED }) } else { json!({}) }
    }
}

const PSA_ENFORCE_LABEL: &str = "pod-security.kubernetes.io/enforce";
const PSA_PRIVILEGED: &str = "privileged";

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
        let Ok(ns) = api.get(&self.name).await else {
            return Readiness::Absent;
        };
        // Existence != readiness under a required PSA level (a pre-existing ns would
        // report Ready, then reject the BuildKit pod at admission)
        if self.psa_privileged
            && ns
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(PSA_ENFORCE_LABEL))
                .map(String::as_str)
                != Some(PSA_PRIVILEGED)
        {
            return Readiness::Absent;
        }
        Readiness::Ready
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let api: Api<Namespace> = Api::all(cx.client.clone());
        let ns: Namespace = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": self.name, "labels": self.labels() },
        }))
        .expect("static namespace manifest is valid");
        // SSA, not create: an existing ns may lack the PSA label, and 409-is-success
        // would leave it that way forever
        api.patch(
            &self.name,
            &PatchParams::apply(crate::resource::kube::FIELD_MANAGER).force(),
            &Patch::Apply(&ns),
        )
        .await
        .map(|_| ())
        .map_err(|e| ResourceError::Provision(format!("apply namespace {}: {e}", self.name)))
    }
}

/// One cluster-wide label over all schedulable nodes. One provider per label key
/// ([`NodeId::NodeLabel`]) → no two can fight over it
#[derive(Debug)]
pub struct NodeLabelProvider {
    key: String,
    value: String,
}

impl NodeLabelProvider {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self { key: key.into(), value: value.into() }
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
        // One drifted node → Absent (re-apply across all)
        let api: Api<Node> = Api::all(cx.client.clone());
        let list = match api.list(&ListParams::default()).await {
            Ok(l) => l,
            Err(_) => return Readiness::Absent,
        };
        if list.items.is_empty() {
            return Readiness::Absent;
        }
        for node in &list.items {
            let matches =
                node.metadata.labels.as_ref().and_then(|m| m.get(&self.key)).map(String::as_str)
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
        // Merge-patch per node (as `kubectl label nodes --all` does); SSA would
        // over-claim ownership of labels ztest does not manage
        let patch = json!({ "metadata": { "labels": { &self.key: &self.value } } });
        let params = PatchParams::default();
        for node in list.items {
            let Some(name) = node.metadata.name else {
                continue;
            };
            api.patch(&name, &params, &Patch::Merge(&patch)).await.map_err(|e| {
                ResourceError::Provision(format!(
                    "label node {name} with {}={}: {e}",
                    self.key, self.value
                ))
            })?;
        }
        Ok(())
    }
}

/// 409 AlreadyExists on a create-only path = success (invariant already met)
fn e_is_conflict_or_exists(e: &kube::Error) -> bool {
    match e {
        kube::Error::Api(resp) => resp.code == 409,
        // Wrapper variants stringify differently across kube versions
        other => {
            let s = other.to_string();
            s.contains("AlreadyExists") || s.contains("409")
        }
    }
}
