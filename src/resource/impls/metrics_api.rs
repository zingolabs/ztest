//! `metrics.k8s.io` resource-metrics API (upstream metrics-server), as typed objects.
//!
//! - Distinct plane from [`observability`](super::observability): live per-pod CPU/mem for
//!   `kubectl top` / k9s / HPA, NOT the Prometheus TSDB (neither substitutes for the other)
//! - Exactly one `v1beta1.metrics.k8s.io` APIService per cluster → [`probe`] treats *any*
//!   serving provider as Ready (OKD's prometheus-adapter must never be hijacked)
//!
//! [`probe`]: MetricsApiProvider::probe

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::{
    Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment,
};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, ContainerPort, EmptyDirVolumeSource, HTTPGetAction, PodSpec,
    PodTemplateSpec, Probe, ResourceRequirements, SeccompProfile, SecurityContext, Service,
    ServiceAccount, ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::api::rbac::v1::{
    ClusterRole, ClusterRoleBinding, PolicyRule, RoleBinding, RoleRef, Subject,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use k8s_openapi::kube_aggregator::pkg::apis::apiregistration::v1::{
    APIService, APIServiceSpec, ServiceReference,
};
use kube::api::Api;

use crate::resource::kube::{apply, wait_api_service_available, wait_deployment_available};
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

const NAMESPACE: &str = "kube-system";
const NAME: &str = "metrics-server";
const APP_LABEL: &str = "k8s-app";

/// Aggregation-layer registration; name is `<version>.<group>`, fixed by the API
const API_SERVICE: &str = "v1beta1.metrics.k8s.io";

/// Pinned, not floating (a moved tag's `ImagePullBackOff` surfaces as "setup hung",
/// three layers from its cause). Override to test a bump
const IMAGE: &str = "registry.k8s.io/metrics-server/metrics-server:v0.9.0";

const UPSTREAM_ARGS: [&str; 5] = [
    "--cert-dir=/tmp",
    "--secure-port=10250",
    "--kubelet-preferred-address-types=InternalIP,ExternalIP,Hostname",
    "--kubelet-use-node-status-port",
    "--metric-resolution=15s",
];

/// - kind kubelets serve self-signed certs → verification fails every scrape
/// - Dev clusters only (ztest provisions no other kind) → no MITM surface worth the arg
const EXTRA_ARGS: [&str; 1] = ["--kubelet-insecure-tls"];

const PORT: i32 = 10250;
const PORT_NAME: &str = "https";

/// Rollout + first scrape + aggregation-layer health propagation
const ROLLOUT_TIMEOUT: Duration = Duration::from_secs(180);

fn image() -> String {
    std::env::var("ZTEST_METRICS_SERVER_IMAGE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| IMAGE.to_string())
}

fn labels() -> BTreeMap<String, String> {
    BTreeMap::from([(APP_LABEL.to_string(), NAME.to_string())])
}

fn meta(name: &str, namespaced: bool, extra_labels: &[(&str, &str)]) -> ObjectMeta {
    let mut l = labels();
    l.extend(extra_labels.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: namespaced.then(|| NAMESPACE.to_string()),
        labels: Some(l),
        ..Default::default()
    }
}

fn rule(api_groups: &[&str], resources: &[&str], verbs: &[&str]) -> PolicyRule {
    PolicyRule {
        api_groups: Some(api_groups.iter().map(|s| s.to_string()).collect()),
        resources: Some(resources.iter().map(|s| s.to_string()).collect()),
        verbs: verbs.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

fn service_account_subject() -> Subject {
    Subject {
        kind: "ServiceAccount".to_string(),
        name: NAME.to_string(),
        namespace: Some(NAMESPACE.to_string()),
        ..Default::default()
    }
}

fn role_ref(kind: &str, name: &str) -> RoleRef {
    RoleRef {
        api_group: "rbac.authorization.k8s.io".to_string(),
        kind: kind.to_string(),
        name: name.to_string(),
    }
}

fn probe(path: &str, initial_delay: Option<i32>) -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            path: Some(path.to_string()),
            port: IntOrString::String(PORT_NAME.to_string()),
            scheme: Some("HTTPS".to_string()),
            ..Default::default()
        }),
        initial_delay_seconds: initial_delay,
        period_seconds: Some(10),
        failure_threshold: Some(3),
        ..Default::default()
    }
}

fn service_account() -> ServiceAccount {
    ServiceAccount { metadata: meta(NAME, true, &[]), ..Default::default() }
}

/// Aggregates into the built-in admin/edit/view roles → `kubectl top` works for any
/// principal already holding one
fn aggregated_metrics_reader() -> ClusterRole {
    ClusterRole {
        metadata: meta(
            "system:aggregated-metrics-reader",
            false,
            &[
                ("rbac.authorization.k8s.io/aggregate-to-admin", "true"),
                ("rbac.authorization.k8s.io/aggregate-to-edit", "true"),
                ("rbac.authorization.k8s.io/aggregate-to-view", "true"),
            ],
        ),
        rules: Some(vec![rule(&["metrics.k8s.io"], &["pods", "nodes"], &["get", "list", "watch"])]),
        ..Default::default()
    }
}

fn metrics_server_role() -> ClusterRole {
    ClusterRole {
        metadata: meta("system:metrics-server", false, &[]),
        rules: Some(vec![
            rule(&[""], &["nodes/metrics"], &["get"]),
            rule(&[""], &["pods", "nodes"], &["get", "list", "watch"]),
        ]),
        ..Default::default()
    }
}

/// Reads `extension-apiserver-authentication` → without it the aggregated server cannot
/// validate the front-proxy client cert, and every request 401s
fn auth_reader_binding() -> RoleBinding {
    RoleBinding {
        metadata: meta("metrics-server-auth-reader", true, &[]),
        role_ref: role_ref("Role", "extension-apiserver-authentication-reader"),
        subjects: Some(vec![service_account_subject()]),
    }
}

fn auth_delegator_binding() -> ClusterRoleBinding {
    ClusterRoleBinding {
        metadata: meta("metrics-server:system:auth-delegator", false, &[]),
        role_ref: role_ref("ClusterRole", "system:auth-delegator"),
        subjects: Some(vec![service_account_subject()]),
    }
}

fn metrics_server_binding() -> ClusterRoleBinding {
    ClusterRoleBinding {
        metadata: meta("system:metrics-server", false, &[]),
        role_ref: role_ref("ClusterRole", "system:metrics-server"),
        subjects: Some(vec![service_account_subject()]),
    }
}

fn service() -> Service {
    Service {
        metadata: meta(NAME, true, &[]),
        spec: Some(ServiceSpec {
            selector: Some(labels()),
            ports: Some(vec![ServicePort {
                name: Some(PORT_NAME.to_string()),
                port: 443,
                protocol: Some("TCP".to_string()),
                target_port: Some(IntOrString::String(PORT_NAME.to_string())),
                app_protocol: Some("https".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn container() -> Container {
    Container {
        name: NAME.to_string(),
        image: Some(image()),
        image_pull_policy: Some("IfNotPresent".to_string()),
        args: Some(UPSTREAM_ARGS.iter().chain(EXTRA_ARGS.iter()).map(|s| s.to_string()).collect()),
        ports: Some(vec![ContainerPort {
            name: Some(PORT_NAME.to_string()),
            container_port: PORT,
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }]),
        liveness_probe: Some(probe("/livez", None)),
        readiness_probe: Some(probe("/readyz", Some(20))),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("100m".to_string())),
                ("memory".to_string(), Quantity("200Mi".to_string())),
            ])),
            ..Default::default()
        }),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Default::default()
            }),
            read_only_root_filesystem: Some(true),
            run_as_non_root: Some(true),
            run_as_user: Some(1000),
            seccomp_profile: Some(SeccompProfile {
                type_: "RuntimeDefault".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        // `--cert-dir` target; root fs is read-only
        volume_mounts: Some(vec![VolumeMount {
            name: "tmp-dir".to_string(),
            mount_path: "/tmp".to_string(),
            ..Default::default()
        }]),
        ..Default::default()
    }
}

fn deployment() -> Deployment {
    Deployment {
        metadata: meta(NAME, true, &[]),
        spec: Some(DeploymentSpec {
            selector: LabelSelector { match_labels: Some(labels()), ..Default::default() },
            strategy: Some(DeploymentStrategy {
                rolling_update: Some(RollingUpdateDeployment {
                    max_unavailable: Some(IntOrString::Int(0)),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta { labels: Some(labels()), ..Default::default() }),
                spec: Some(PodSpec {
                    service_account_name: Some(NAME.to_string()),
                    priority_class_name: Some("system-cluster-critical".to_string()),
                    node_selector: Some(BTreeMap::from([(
                        "kubernetes.io/os".to_string(),
                        "linux".to_string(),
                    )])),
                    containers: vec![container()],
                    volumes: Some(vec![Volume {
                        name: "tmp-dir".to_string(),
                        empty_dir: Some(EmptyDirVolumeSource::default()),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// `insecure_skip_tls_verify` = apiserver→metrics-server hop (serving cert is self-signed
/// at `--cert-dir`); unrelated to [`EXTRA_ARGS`]' metrics-server→kubelet hop
fn api_service() -> APIService {
    APIService {
        metadata: meta(API_SERVICE, false, &[]),
        spec: Some(APIServiceSpec {
            group: Some("metrics.k8s.io".to_string()),
            version: Some("v1beta1".to_string()),
            group_priority_minimum: 100,
            version_priority: 100,
            insecure_skip_tls_verify: Some(true),
            service: Some(ServiceReference {
                name: Some(NAME.to_string()),
                namespace: Some(NAMESPACE.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Resource-metrics API as one node. Optional: absence costs `kubectl top`/k9s columns,
/// blocks no test
#[derive(Debug)]
pub struct MetricsApiProvider;

#[async_trait]
impl Provider for MetricsApiProvider {
    fn id(&self) -> NodeId {
        NodeId::MetricsApi
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    /// Serving APIService = Ready, whoever installed it (OKD serves this from
    /// prometheus-adapter; re-applying would repoint the group at our Service)
    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<APIService> = Api::all(cx.client.clone());
        match api.get(API_SERVICE).await {
            Ok(svc) if is_available(&svc) => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    /// RBAC before the Deployment: a pod that starts without its bindings crash-loops on
    /// 401 and burns the backoff clock
    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let ctx = "metrics-server";
        let c = &cx.client;

        let applied: Result<(), String> = async {
            apply(&Api::namespaced(c.clone(), NAMESPACE), &service_account(), ctx).await?;
            apply(&Api::<ClusterRole>::all(c.clone()), &aggregated_metrics_reader(), ctx).await?;
            apply(&Api::<ClusterRole>::all(c.clone()), &metrics_server_role(), ctx).await?;
            apply(&Api::namespaced(c.clone(), NAMESPACE), &auth_reader_binding(), ctx).await?;
            apply(&Api::<ClusterRoleBinding>::all(c.clone()), &auth_delegator_binding(), ctx)
                .await?;
            apply(&Api::<ClusterRoleBinding>::all(c.clone()), &metrics_server_binding(), ctx)
                .await?;
            apply(&Api::namespaced(c.clone(), NAMESPACE), &service(), ctx).await?;
            apply(&Api::namespaced(c.clone(), NAMESPACE), &deployment(), ctx).await?;
            apply(&Api::<APIService>::all(c.clone()), &api_service(), ctx).await
        }
        .await;
        applied.map_err(ResourceError::Provision)?;

        wait_deployment_available(c, NAMESPACE, NAME, ROLLOUT_TIMEOUT, cx.no_wait)
            .await
            .map_err(ResourceError::Provision)?;
        wait_api_service_available(c, API_SERVICE, ROLLOUT_TIMEOUT, cx.no_wait)
            .await
            .map_err(ResourceError::Provision)?;
        Ok(())
    }
}

fn is_available(svc: &APIService) -> bool {
    svc.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Available" && c.status == "True"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_container_carries_every_arg() {
        let args = container().args.expect("args");
        for want in UPSTREAM_ARGS.iter().chain(EXTRA_ARGS.iter()) {
            assert!(args.iter().any(|a| a == want), "dropped {want}");
        }
        assert_eq!(args.len(), UPSTREAM_ARGS.len() + EXTRA_ARGS.len());
    }

    /// Selector must match the template's labels, else the ReplicaSet adopts nothing
    #[test]
    fn the_deployment_selector_matches_its_template() {
        let spec = deployment().spec.expect("spec");
        let selector = spec.selector.match_labels.expect("match_labels");
        let template = spec.template.metadata.expect("template metadata").labels.expect("labels");
        assert_eq!(selector, template);
        assert_eq!(spec.template.spec.expect("pod spec").containers.len(), 1);
    }

    /// Service routes by port *name*; a rename on one side alone silently blackholes it
    #[test]
    fn the_service_target_port_names_the_container_port() {
        let ports = service().spec.expect("spec").ports.expect("ports");
        assert_eq!(ports[0].target_port, Some(IntOrString::String(PORT_NAME.to_string())));
        let c = container();
        assert_eq!(c.ports.expect("ports")[0].name.as_deref(), Some(PORT_NAME));
    }

    /// Registration name is `<version>.<group>`; drift = a second, competing APIService
    #[test]
    fn the_api_service_name_matches_its_group_and_version() {
        let svc = api_service();
        let spec = svc.spec.expect("spec");
        assert_eq!(
            svc.metadata.name.expect("name"),
            format!("{}.{}", spec.version.expect("version"), spec.group.expect("group"))
        );
    }

    /// Every binding must name the SA actually created (a typo 401s at runtime only)
    #[test]
    fn every_binding_targets_the_service_account() {
        let sa = service_account().metadata.name.expect("sa name");
        let subjects = [
            auth_reader_binding().subjects.expect("subjects"),
            auth_delegator_binding().subjects.expect("subjects"),
            metrics_server_binding().subjects.expect("subjects"),
        ];
        for s in subjects.iter().flatten() {
            assert_eq!(s.name, sa);
            assert_eq!(s.namespace.as_deref(), Some(NAMESPACE));
        }
    }
}
