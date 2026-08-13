//! ztest-owned metrics stack: Prometheus + Pyroscope + Grafana, plain Kubernetes
//! objects in one namespace.
//!
//! - No prometheus-operator: `PodMonitor`/`ServiceMonitor` serve many teams on a
//!   cluster none of them owns; ztest creates and labels every pod it scrapes, so
//!   `kubernetes_sd_configs` reads those labels directly (one ConfigMap replaces a
//!   CRD set + operator Deployment + a per-component CR per run)
//! - To be scraped: carry `ztest.io/component-name` (every ztest pod does) and declare
//!   a port named [`crate::metrics::PORT_NAME`]; [`SCRAPE_CONFIG`] drops everything else

use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::Deployment;
use kube::api::Api;

use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// Namespace for the whole stack: fixed, cluster-lifetime, owned by `ztest cluster setup`.
/// Never per-run (the record must outlive the run that produced it)
pub const OBS_NAMESPACE: &str = "ztest-obs";

pub const PROMETHEUS_SERVICE: &str = "ztest-prometheus";
pub const PROMETHEUS_PORT: u16 = 9090;
pub const PYROSCOPE_SERVICE: &str = "ztest-pyroscope";
pub const PYROSCOPE_PORT: u16 = 4040;
pub const GRAFANA_SERVICE: &str = "ztest-grafana";
pub const GRAFANA_PORT: u16 = 3000;

/// The three Deployments, in [`ObservabilityProvider`]'s wait order
const DEPLOYMENTS: [&str; 3] = [PROMETHEUS_SERVICE, PYROSCOPE_SERVICE, GRAFANA_SERVICE];

/// Pinned, not floating (a moved tag's `ImagePullBackOff` surfaces as "setup hung",
/// three layers from its cause). Override to test a bump
fn image(component: &str, default: &str) -> String {
    std::env::var(format!("ZTEST_OBS_{}_IMAGE", component.to_uppercase().replace('-', "_")))
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

const PYROSCOPE_IMAGE: &str = "grafana/pyroscope:2.2.1";
const PROMETHEUS_IMAGE: &str = "prom/prometheus:v3.13.2";
const GRAFANA_IMAGE: &str = "grafana/grafana:13.0.6";

fn storage_class_line() -> String {
    match std::env::var("ZTEST_OBS_STORAGE_CLASS").ok().filter(|s| !s.trim().is_empty()) {
        // Omitted, not defaulted (a named class strands the PVC `Pending` on every
        // cluster spelling its default differently)
        None => String::new(),
        Some(class) => format!("\n    storageClassName: {class}"),
    }
}

fn volume_size(key: &str, default: &str) -> String {
    std::env::var(key).ok().filter(|s| !s.trim().is_empty()).unwrap_or_else(|| default.to_string())
}

/// Rollout wait. Generous — a cluster's first `setup` pulls three images cold
const ROLLOUT_TIMEOUT: Duration = Duration::from_secs(300);

/// Scrape ztest component pods, nothing else.
///
/// - Two `keep` rules = the whole discovery contract (`ztest.io/component-name` *and*
///   a port named `metrics`)
/// - Rest promote pod labels to series labels → a run stays selectable once its
///   namespace is gone
const SCRAPE_CONFIG: &str = r#"global:
  scrape_interval: 5s
  scrape_timeout: 4s
scrape_configs:
  - job_name: ztest-components
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_ztest_io_component_name]
        regex: .+
        action: keep
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: metrics
        action: keep
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_pod_label_ztest_io_component_name]
        target_label: component_name
      - source_labels: [__meta_kubernetes_pod_label_ztest_io_component]
        target_label: component
      - source_labels: [__meta_kubernetes_pod_label_ztest_io_component_category]
        target_label: component_category
      - source_labels: [__meta_kubernetes_pod_label_ztest_io_test]
        target_label: test
      - source_labels: [__meta_kubernetes_pod_label_ztest_io_run_id]
        target_label: run_id
"#;

/// Pyroscope v2 writes profiles *and* metastore state to object storage; `filesystem`
/// = a directory, all one replica needs. Both must land on the same volume (else a
/// restart comes back with data it cannot index)
const PYROSCOPE_CONFIG: &str = r#"target: all
server:
  http_listen_port: 4040
storage:
  backend: filesystem
  filesystem:
    dir: /data/shared
metastore:
  data_dir: /data/metastore/data
  raft:
    dir: /data/metastore/raft
"#;

/// Grafana datasource provisioning: both stores wired at first boot (a fresh install
/// is queryable without anyone opening the UI)
const GRAFANA_DATASOURCES: &str = r#"apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    isDefault: true
    url: http://ztest-prometheus:9090
  - name: Pyroscope
    type: grafana-pyroscope-datasource
    access: proxy
    url: http://ztest-pyroscope:4040
"#;

fn indent(text: &str, spaces: usize) -> String {
    let pad = " ".repeat(spaces);
    text.lines()
        .map(|l| if l.is_empty() { String::new() } else { format!("{pad}{l}") })
        .collect::<Vec<_>>()
        .join("\n")
}

fn manifest() -> String {
    let sc = storage_class_line();
    let prom_size = volume_size("ZTEST_OBS_PROMETHEUS_SIZE", "20Gi");
    let pyro_size = volume_size("ZTEST_OBS_PYROSCOPE_SIZE", "20Gi");
    let prom_image = image("prometheus", PROMETHEUS_IMAGE);
    let pyro_image = image("pyroscope", PYROSCOPE_IMAGE);
    let grafana_image = image("grafana", GRAFANA_IMAGE);
    let scrape = indent(SCRAPE_CONFIG, 4);
    let pyroscope_config = indent(PYROSCOPE_CONFIG, 4);
    let datasources = indent(GRAFANA_DATASOURCES, 4);

    format!(
        r#"---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {PROMETHEUS_SERVICE}
  namespace: {OBS_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {PROMETHEUS_SERVICE}
rules:
  - apiGroups: [""]
    resources: [nodes, services, endpoints, pods]
    verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {PROMETHEUS_SERVICE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {PROMETHEUS_SERVICE}
subjects:
  - kind: ServiceAccount
    name: {PROMETHEUS_SERVICE}
    namespace: {OBS_NAMESPACE}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {PROMETHEUS_SERVICE}-config
  namespace: {OBS_NAMESPACE}
data:
  prometheus.yml: |
{scrape}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {PROMETHEUS_SERVICE}-data
  namespace: {OBS_NAMESPACE}
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: {prom_size}{sc}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {PROMETHEUS_SERVICE}
  namespace: {OBS_NAMESPACE}
  labels:
    app.kubernetes.io/name: prometheus
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: prometheus
  template:
    metadata:
      labels:
        app.kubernetes.io/name: prometheus
    spec:
      serviceAccountName: {PROMETHEUS_SERVICE}
      securityContext:
        fsGroup: 65534
        runAsUser: 65534
        runAsNonRoot: true
      containers:
        - name: prometheus
          image: {prom_image}
          args:
            - --config.file=/etc/prometheus/prometheus.yml
            - --storage.tsdb.path=/prometheus
            - --storage.tsdb.retention.time=30d
            - --web.enable-lifecycle
          ports:
            - name: http
              containerPort: {PROMETHEUS_PORT}
          readinessProbe:
            httpGet:
              path: /-/ready
              port: http
            initialDelaySeconds: 5
            periodSeconds: 5
          resources:
            requests:
              cpu: 200m
              memory: 512Mi
            limits:
              cpu: "2"
              memory: 4Gi
          volumeMounts:
            - name: config
              mountPath: /etc/prometheus
            - name: data
              mountPath: /prometheus
      volumes:
        - name: config
          configMap:
            name: {PROMETHEUS_SERVICE}-config
        - name: data
          persistentVolumeClaim:
            claimName: {PROMETHEUS_SERVICE}-data
---
apiVersion: v1
kind: Service
metadata:
  name: {PROMETHEUS_SERVICE}
  namespace: {OBS_NAMESPACE}
  labels:
    app.kubernetes.io/name: prometheus
spec:
  selector:
    app.kubernetes.io/name: prometheus
  ports:
    - name: http
      port: {PROMETHEUS_PORT}
      targetPort: http
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {PYROSCOPE_SERVICE}-config
  namespace: {OBS_NAMESPACE}
data:
  config.yaml: |
{pyroscope_config}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {PYROSCOPE_SERVICE}-data
  namespace: {OBS_NAMESPACE}
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: {pyro_size}{sc}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {PYROSCOPE_SERVICE}
  namespace: {OBS_NAMESPACE}
  labels:
    app.kubernetes.io/name: pyroscope
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: pyroscope
  template:
    metadata:
      labels:
        app.kubernetes.io/name: pyroscope
    spec:
      securityContext:
        fsGroup: 10001
        runAsUser: 10001
        runAsNonRoot: true
      containers:
        - name: pyroscope
          image: {pyro_image}
          args:
            - -config.file=/etc/pyroscope/config.yaml
          ports:
            - name: http
              containerPort: {PYROSCOPE_PORT}
          readinessProbe:
            httpGet:
              path: /ready
              port: http
            initialDelaySeconds: 10
            periodSeconds: 5
          resources:
            requests:
              cpu: 200m
              memory: 512Mi
            limits:
              cpu: "2"
              memory: 4Gi
          volumeMounts:
            - name: config
              mountPath: /etc/pyroscope
            - name: data
              mountPath: /data
      volumes:
        - name: config
          configMap:
            name: {PYROSCOPE_SERVICE}-config
        - name: data
          persistentVolumeClaim:
            claimName: {PYROSCOPE_SERVICE}-data
---
apiVersion: v1
kind: Service
metadata:
  name: {PYROSCOPE_SERVICE}
  namespace: {OBS_NAMESPACE}
  labels:
    app.kubernetes.io/name: pyroscope
spec:
  selector:
    app.kubernetes.io/name: pyroscope
  ports:
    - name: http
      port: {PYROSCOPE_PORT}
      targetPort: http
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {GRAFANA_SERVICE}-datasources
  namespace: {OBS_NAMESPACE}
data:
  datasources.yaml: |
{datasources}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {GRAFANA_SERVICE}
  namespace: {OBS_NAMESPACE}
  labels:
    app.kubernetes.io/name: grafana
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: grafana
  template:
    metadata:
      labels:
        app.kubernetes.io/name: grafana
    spec:
      securityContext:
        fsGroup: 472
        runAsUser: 472
        runAsNonRoot: true
      containers:
        - name: grafana
          image: {grafana_image}
          env:
            # Anonymous admin: port-forward-only test cluster, never behind an
            # Ingress — a login wall here is just a credential to lose.
            - name: GF_AUTH_ANONYMOUS_ENABLED
              value: "true"
            - name: GF_AUTH_ANONYMOUS_ORG_ROLE
              value: Admin
            - name: GF_AUTH_BASIC_ENABLED
              value: "false"
            - name: GF_FEATURE_TOGGLES_ENABLE
              value: flameGraph
          ports:
            - name: http
              containerPort: {GRAFANA_PORT}
          readinessProbe:
            httpGet:
              path: /api/health
              port: http
            initialDelaySeconds: 10
            periodSeconds: 5
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: "1"
              memory: 1Gi
          volumeMounts:
            - name: datasources
              mountPath: /etc/grafana/provisioning/datasources
            - name: data
              mountPath: /var/lib/grafana
      volumes:
        - name: datasources
          configMap:
            name: {GRAFANA_SERVICE}-datasources
        # emptyDir, unlike the two stores: provisioning rebuilds all of this at boot
        - name: data
          emptyDir: {{}}
---
apiVersion: v1
kind: Service
metadata:
  name: {GRAFANA_SERVICE}
  namespace: {OBS_NAMESPACE}
  labels:
    app.kubernetes.io/name: grafana
spec:
  selector:
    app.kubernetes.io/name: grafana
  ports:
    - name: http
      port: {GRAFANA_PORT}
      targetPort: http
"#
    )
}

/// Prometheus + Pyroscope + Grafana as one node: one capability to the reader, one
/// namespace/storage class/install step, and they fail together
#[derive(Debug)]
pub(crate) struct ObservabilityProvider;

#[async_trait]
impl Provider for ObservabilityProvider {
    fn id(&self) -> NodeId {
        NodeId::Observability
    }

    fn deps(&self) -> Vec<NodeId> {
        vec![NodeId::Namespace(OBS_NAMESPACE.to_string())]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<Deployment> = Api::namespaced(cx.client.clone(), OBS_NAMESPACE);
        for name in DEPLOYMENTS {
            match api.get(name).await {
                Ok(d) if deployment_is_available(&d) => {}
                _ => return Readiness::Absent,
            }
        }
        Readiness::Ready
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        crate::resource::kube::apply_yaml_bundle(&cx.client, &manifest(), "observability stack")
            .await
            .map_err(ResourceError::Provision)?;

        for name in DEPLOYMENTS {
            if let Err(timeout) = crate::resource::kube::wait_deployment_available(
                &cx.client,
                OBS_NAMESPACE,
                name,
                ROLLOUT_TIMEOUT,
                cx.no_wait,
            )
            .await
            {
                // The kubelet already recorded a reason; a bare "timeout" sends the
                // reader to hand-inspect what the cluster would have told them
                let why = stalled_because(cx, name).await;
                return Err(ResourceError::Provision(match why {
                    Some(detail) => format!("{timeout}: {detail}"),
                    None => timeout,
                }));
            }
        }
        Ok(())
    }
}

/// Why a Deployment's pods are not running yet, in the cluster's own words.
///
/// Conditions, not container statuses (a pod stuck on an unbound PVC never reaches a
/// container → `PodScheduled=False` is the only record)
async fn stalled_because(cx: &Cx, deployment: &str) -> Option<String> {
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::ListParams;

    let pods: Api<Pod> = Api::namespaced(cx.client.clone(), OBS_NAMESPACE);
    let selector = format!("app.kubernetes.io/name={}", deployment.trim_start_matches("ztest-"));
    let list = pods.list(&ListParams::default().labels(&selector)).await.ok()?;
    list.items.iter().find_map(|p| {
        let status = p.status.as_ref()?;
        let phase = status.phase.as_deref().unwrap_or("Unknown");
        let reason = status
            .conditions
            .as_ref()?
            .iter()
            .find(|c| c.status == "False")
            .and_then(|c| c.message.clone())?;
        Some(format!("pod is {phase} — {reason}"))
    })
}

fn deployment_is_available(d: &Deployment) -> bool {
    let desired = d.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
    let ready = d.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0);
    ready >= desired && desired > 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_manifest_is_valid_multi_document_yaml() {
        let rendered = manifest();
        let docs: Vec<serde_yaml::Value> = serde_yaml::Deserializer::from_str(&rendered)
            .map(|d| serde::Deserialize::deserialize(d).expect("each document parses"))
            .collect();
        let named: Vec<(String, String)> = docs
            .iter()
            .filter(|d| !d.is_null())
            .map(|d| {
                (
                    d["kind"].as_str().expect("kind").to_string(),
                    d["metadata"]["name"].as_str().expect("name").to_string(),
                )
            })
            .collect();

        for want in [
            ("Deployment", PROMETHEUS_SERVICE),
            ("Deployment", PYROSCOPE_SERVICE),
            ("Deployment", GRAFANA_SERVICE),
            ("Service", PROMETHEUS_SERVICE),
            ("Service", PYROSCOPE_SERVICE),
            ("Service", GRAFANA_SERVICE),
        ] {
            assert!(
                named.iter().any(|(k, n)| k == want.0 && n == want.1),
                "missing {} {}",
                want.0,
                want.1
            );
        }
    }

    /// Config rides a YAML block scalar → a wrong indent applies cleanly and scrapes nothing
    #[test]
    fn the_scrape_config_survives_embedding() {
        let rendered = manifest();
        let docs: Vec<serde_yaml::Value> = serde_yaml::Deserializer::from_str(&rendered)
            .map(|d| serde::Deserialize::deserialize(d).expect("parses"))
            .collect();
        let cm = docs
            .iter()
            .find(|d| {
                d["kind"].as_str() == Some("ConfigMap")
                    && d["metadata"]["name"].as_str() == Some("ztest-prometheus-config")
            })
            .expect("prometheus ConfigMap");
        let inner: serde_yaml::Value =
            serde_yaml::from_str(cm["data"]["prometheus.yml"].as_str().expect("yml key"))
                .expect("embedded config is itself valid YAML");
        assert_eq!(inner["scrape_configs"][0]["job_name"].as_str(), Some("ztest-components"));
    }

    /// Dropping either `keep` rule scrapes every pod in the cluster
    #[test]
    fn discovery_keeps_only_ztest_pods_with_a_metrics_port() {
        let keeps: Vec<&str> =
            SCRAPE_CONFIG.lines().filter(|l| l.contains("action: keep")).collect();
        assert_eq!(keeps.len(), 2);
        assert!(SCRAPE_CONFIG.contains("__meta_kubernetes_pod_label_ztest_io_component_name"));
        assert!(SCRAPE_CONFIG.contains("__meta_kubernetes_pod_container_port_name"));
    }

    /// Filtered port name = the exporter contract's (drift here silently scrapes nothing)
    #[test]
    fn the_kept_port_name_is_the_exporter_contract() {
        assert!(SCRAPE_CONFIG.contains(&format!("regex: {}", crate::metrics::PORT_NAME)));
    }

    /// Profiles *and* metastore state on the one PVC (else a restart cannot index them)
    #[test]
    fn pyroscope_stores_everything_on_its_volume() {
        let config: serde_yaml::Value = serde_yaml::from_str(PYROSCOPE_CONFIG).expect("valid YAML");
        assert_eq!(config["storage"]["backend"].as_str(), Some("filesystem"));
        for path in [
            config["storage"]["filesystem"]["dir"].as_str(),
            config["metastore"]["data_dir"].as_str(),
            config["metastore"]["raft"]["dir"].as_str(),
        ] {
            assert!(
                path.expect("path set").starts_with("/data/"),
                "every store must be under the mounted volume, got {path:?}"
            );
        }
    }

    #[test]
    fn an_unset_storage_class_omits_the_field() {
        // SAFETY: single-threaded section, no other thread reads env here
        unsafe { std::env::remove_var("ZTEST_OBS_STORAGE_CLASS") };
        assert!(!manifest().contains("storageClassName"));
    }
}
