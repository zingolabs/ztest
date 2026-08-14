//! ztest-owned metrics stack: Prometheus + Pyroscope + Grafana, plain Kubernetes
//! objects in one namespace.
//!
//! - No prometheus-operator: `PodMonitor`/`ServiceMonitor` serve many teams on a
//!   cluster none of them owns; ztest creates and labels every pod it scrapes, so
//!   `kubernetes_sd_configs` reads those labels directly (one ConfigMap replaces a
//!   CRD set + operator Deployment + a per-component CR per run)
//! - To be scraped: carry `ztest.io/component-name` (every ztest pod does) and declare
//!   a port named [`crate::metrics::PORT_NAME`]; [`SCRAPE_CONFIG`] drops everything else

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, EmptyDirVolumeSource, EnvVar,
    HTTPGetAction, PersistentVolumeClaim, PersistentVolumeClaimSpec,
    PersistentVolumeClaimVolumeSource, PodSecurityContext, PodSpec, PodTemplateSpec, Probe,
    ResourceRequirements, Service, ServiceAccount, ServicePort, ServiceSpec, Volume, VolumeMount,
    VolumeResourceRequirements,
};
use k8s_openapi::api::rbac::v1::{ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::Api;
use serde::{Deserialize, Serialize};

use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// Container-port name every Service and probe targets
const HTTP_PORT: &str = "http";

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

/// `None`, never a default (a named class strands the PVC `Pending` on every cluster
/// spelling its default differently)
fn storage_class() -> Option<String> {
    std::env::var("ZTEST_OBS_STORAGE_CLASS").ok().filter(|s| !s.trim().is_empty())
}

fn volume_size(key: &str, default: &str) -> String {
    std::env::var(key).ok().filter(|s| !s.trim().is_empty()).unwrap_or_else(|| default.to_string())
}

/// Rollout wait. Generous — a cluster's first `setup` pulls three images cold
const ROLLOUT_TIMEOUT: Duration = Duration::from_secs(300);

/// TSDB retention. Named because a reader hitting an empty report needs to be told the
/// horizon, and a number in two places drifts
pub const RETENTION_DAYS: u32 = 30;

/// Profile retention; < [`RETENTION_DAYS`] (bulkier store, same PVC size).
///
/// - Backstop only (`ztest cleanup` retires a sync's tenant directly)
/// - Covers tenants no pass reaches (Ctrl-C, RBAC) — no tenant-listing API to sweep by
pub const PROFILE_RETENTION_HOURS: u32 = 120;

/// `-metastore.index.cleanup-grace-period` (upstream 6h).
///
/// - Cleaner inspects a partition at `partition.end + grace`; partition = 6h fixed, so
///   that term dominates, not this one
/// - Safe to shorten (deletion gated on `shard.MaxTime < marker` — grace moves when the
///   cleaner *looks*, never what it may delete)
const PROFILE_CLEANUP_GRACE: &str = "15m";

/// `ztest cleanup` → profiles gone.
///
/// - Worst case = 6h partition + [`PROFILE_CLEANUP_GRACE`] + one 15m cleanup interval
/// - Best case ~30m (partition already closed)
pub const PROFILE_RETIREMENT_LAG: &str = "~6h";

/// Pod-template stamp; [`probe`](ObservabilityProvider::probe) re-provisions on drift
const CONFIG_HASH_ANNOTATION: &str = "ztest.io/config-hash";

/// Prometheus flags.
///
/// - `--web.enable-admin-api` = the `delete_series`/`clean_tombstones` endpoints
///   [`purge`](crate::metrics::query::purge) drives (ClusterIP, no ingress)
/// - Feeds [`config_hash`] alongside [`SCRAPE_CONFIG`]: a flag change touches no
///   ConfigMap, so nothing else would notice it
fn prometheus_args() -> Vec<String> {
    vec![
        "--config.file=/etc/prometheus/prometheus.yml".into(),
        "--storage.tsdb.path=/prometheus".into(),
        format!("--storage.tsdb.retention.time={RETENTION_DAYS}d"),
        "--web.enable-lifecycle".into(),
        "--web.enable-admin-api".into(),
    ]
}

/// Two jobs: what ztest's components publish, and what the kubelet observes of their
/// containers.
///
/// - `ztest-components` — two `keep` rules = the whole discovery contract
///   (`ztest.io/component-name` *and* a port named `metrics`)
/// - Rest promote pod labels to series labels → a run stays selectable once its
///   namespace is gone
/// - `kubelet-cadvisor` — cpu/mem/io-stall a component cannot publish about itself; via
///   the apiserver proxy, not node:10250 (kubelet serving certs are per-cluster and CRC's
///   are not in any CA the pod trusts)
/// - PSI pair kept together: `stalled` (cgroup `full`) = every task blocked, so throughput
///   lost; `waiting` (`some`) = one task blocked, near-always nonzero on a threaded
///   process. Either alone misleads — `full` under-reports a partial stall, `some`
///   over-reports an idle worker
/// - `blkio_device_usage_total` = the only per-cgroup disk figure cAdvisor actually fills;
///   the `container_fs_*` family reads 0 throughout under cgroup v2 + containerd
/// - Kept to 5 families × ztest namespaces: unfiltered cAdvisor is ~40 families over
///   every container on the node, which is the run's own TSDB budget spent on `kube-system`
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
      - source_labels: [__meta_kubernetes_pod_label_ztest_io_user]
        target_label: user
      - source_labels: [__meta_kubernetes_pod_label_ztest_io_sync_id]
        target_label: sync_id
  - job_name: kubelet-cadvisor
    scheme: https
    tls_config:
      ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
    bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
    kubernetes_sd_configs:
      - role: node
    relabel_configs:
      - target_label: __address__
        replacement: kubernetes.default.svc:443
      - source_labels: [__meta_kubernetes_node_name]
        regex: (.+)
        target_label: __metrics_path__
        replacement: /api/v1/nodes/$1/proxy/metrics/cadvisor
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: container_(cpu_usage_seconds_total|memory_working_set_bytes|pressure_io_stalled_seconds_total|pressure_io_waiting_seconds_total|blkio_device_usage_total)
        action: keep
      - source_labels: [namespace]
        regex: ztest.*
        action: keep
      - source_labels: [container]
        regex: (|POD)
        action: drop
"#;

/// Pyroscope v2 writes profiles *and* metastore state to object storage; `filesystem`
/// = a directory, all one replica needs. Both must land on the same volume (else a
/// restart comes back with data it cannot index)
///
/// - `architecture_storage` pinned (v1/v2 delete by different machinery; default
///   `v1-v2-dual` not ours to inherit across an image bump)
/// - `multitenancy_enabled` = delete handle (off → all pushes = tenant `anonymous`,
///   retirement silently no-ops)
/// - `retention_period` = hidden limit, absent from the published reference; v1's
///   `compactor_blocks_retention_period` inert here
/// - `runtime_config` re-reads [`PYROSCOPE_OVERRIDES_CONFIGMAP`] @10s = cleanup → live
///   server, no roll
#[derive(Debug, Serialize)]
struct PyroscopeConfig {
    target: &'static str,
    architecture_storage: &'static str,
    multitenancy_enabled: bool,
    server: PyroscopeServer,
    storage: PyroscopeStorage,
    metastore: PyroscopeMetastore,
    limits: PyroscopeLimits,
    runtime_config: PyroscopeRuntimeConfig,
}

#[derive(Debug, Serialize)]
struct PyroscopeServer {
    http_listen_port: u16,
}

#[derive(Debug, Serialize)]
struct PyroscopeStorage {
    backend: &'static str,
    filesystem: PyroscopeFilesystem,
}

#[derive(Debug, Serialize)]
struct PyroscopeFilesystem {
    dir: &'static str,
}

#[derive(Debug, Serialize)]
struct PyroscopeMetastore {
    data_dir: &'static str,
    raft: PyroscopeRaft,
}

#[derive(Debug, Serialize)]
struct PyroscopeRaft {
    dir: &'static str,
}

#[derive(Debug, Serialize)]
struct PyroscopeLimits {
    retention_period: String,
}

#[derive(Debug, Serialize)]
struct PyroscopeRuntimeConfig {
    file: String,
    period: &'static str,
}

fn pyroscope_config() -> PyroscopeConfig {
    PyroscopeConfig {
        target: "all",
        architecture_storage: "v2",
        multitenancy_enabled: true,
        server: PyroscopeServer { http_listen_port: PYROSCOPE_PORT },
        storage: PyroscopeStorage {
            backend: "filesystem",
            filesystem: PyroscopeFilesystem { dir: "/data/shared" },
        },
        metastore: PyroscopeMetastore {
            data_dir: "/data/metastore/data",
            raft: PyroscopeRaft { dir: "/data/metastore/raft" },
        },
        limits: PyroscopeLimits { retention_period: format!("{PROFILE_RETENTION_HOURS}h") },
        runtime_config: PyroscopeRuntimeConfig {
            file: format!("{PYROSCOPE_OVERRIDES_DIR}/{PYROSCOPE_OVERRIDES_KEY}"),
            period: "10s",
        },
    }
}

fn pyroscope_args() -> Vec<String> {
    vec![
        "-config.file=/etc/pyroscope/config.yaml".into(),
        format!("-metastore.index.cleanup-grace-period={PROFILE_CLEANUP_GRACE}"),
    ]
}

/// Per-tenant retention overrides: `ztest cleanup` writes, metastore cleaner reads.
///
/// - Seeded empty here, not owned declaratively after (re-provision resets it)
/// - Reset costs a delay to the [`PROFILE_RETENTION_HOURS`] floor, never resurrection
pub const PYROSCOPE_OVERRIDES_CONFIGMAP: &str = "ztest-pyroscope-overrides";
pub const PYROSCOPE_OVERRIDES_KEY: &str = "overrides.yaml";
pub const PYROSCOPE_OVERRIDES_DIR: &str = "/etc/pyroscope-overrides";

/// When each tenant was retired, epoch seconds. ztest's own bookkeeping — Pyroscope
/// never reads this key, it only bounds [`PYROSCOPE_OVERRIDES_KEY`]'s growth
pub const PYROSCOPE_RETIRED_KEY: &str = "ztest-retired-at.yaml";

/// Retirement lifetime; > [`PROFILE_RETIREMENT_LAG`] with margin, so an entry only
/// expires once its data is long gone
pub const RETIREMENT_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Runtime-config document: tenant → limit overrides, merged over [`PyroscopeConfig`]'s
/// `limits` by the running server
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Overrides {
    pub overrides: BTreeMap<String, TenantLimits>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TenantLimits {
    pub retention_period: String,
}

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

/// Shared `app.kubernetes.io/name`, the selector every Deployment/Service pairs on
fn app_labels(app: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("app.kubernetes.io/name".to_string(), app.to_string())])
}

fn meta(name: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(OBS_NAMESPACE.to_string()),
        ..Default::default()
    }
}

fn config_map(name: &str, key: &str, contents: String) -> ConfigMap {
    ConfigMap {
        metadata: meta(name),
        data: Some(BTreeMap::from([(key.to_string(), contents)])),
        ..Default::default()
    }
}

fn pvc(name: &str, size: &str) -> PersistentVolumeClaim {
    PersistentVolumeClaim {
        metadata: meta(name),
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".into()]),
            resources: Some(VolumeResourceRequirements {
                requests: Some(BTreeMap::from([("storage".into(), Quantity(size.into()))])),
                ..Default::default()
            }),
            storage_class_name: storage_class(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Every Service here is the same shape: ClusterIP onto the one named container port
fn service(name: &str, app: &str, port: u16) -> Service {
    Service {
        metadata: ObjectMeta { labels: Some(app_labels(app)), ..meta(name) },
        spec: Some(ServiceSpec {
            selector: Some(app_labels(app)),
            ports: Some(vec![ServicePort {
                name: Some(HTTP_PORT.into()),
                port: port as i32,
                target_port: Some(IntOrString::String(HTTP_PORT.into())),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Single-replica store: `Recreate`, never `RollingUpdate` (RWO volume — a second pod
/// blocks on the first's mount and the rollout deadlocks)
fn deployment(
    name: &str,
    app: &str,
    annotations: Option<BTreeMap<String, String>>,
    pod: PodSpec,
) -> Deployment {
    Deployment {
        metadata: ObjectMeta { labels: Some(app_labels(app)), ..meta(name) },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".into()),
                ..Default::default()
            }),
            selector: LabelSelector { match_labels: Some(app_labels(app)), ..Default::default() },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(app_labels(app)),
                    annotations,
                    ..Default::default()
                }),
                spec: Some(pod),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Same uid for `runAsUser` and `fsGroup`: the image's own, and the volume must be
/// writable by it
fn security_context(uid: i64) -> PodSecurityContext {
    PodSecurityContext {
        fs_group: Some(uid),
        run_as_user: Some(uid),
        run_as_non_root: Some(true),
        ..Default::default()
    }
}

fn probe(path: &str, initial_delay: i32) -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            path: Some(path.to_string()),
            port: IntOrString::String(HTTP_PORT.into()),
            ..Default::default()
        }),
        initial_delay_seconds: Some(initial_delay),
        period_seconds: Some(5),
        ..Default::default()
    }
}

fn resources(cpu: (&str, &str), memory: (&str, &str)) -> ResourceRequirements {
    ResourceRequirements {
        requests: Some(BTreeMap::from([
            ("cpu".into(), Quantity(cpu.0.into())),
            ("memory".into(), Quantity(memory.0.into())),
        ])),
        limits: Some(BTreeMap::from([
            ("cpu".into(), Quantity(cpu.1.into())),
            ("memory".into(), Quantity(memory.1.into())),
        ])),
        ..Default::default()
    }
}

fn mount(name: &str, path: &str) -> VolumeMount {
    VolumeMount { name: name.into(), mount_path: path.into(), ..Default::default() }
}

fn config_map_volume(name: &str, config_map: &str) -> Volume {
    Volume {
        name: name.into(),
        config_map: Some(ConfigMapVolumeSource {
            name: config_map.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn pvc_volume(name: &str, claim: &str) -> Volume {
    Volume {
        name: name.into(),
        persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
            claim_name: claim.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn port(number: u16) -> ContainerPort {
    ContainerPort {
        name: Some(HTTP_PORT.into()),
        container_port: number as i32,
        ..Default::default()
    }
}

// ──────────────────────────── the stack ────────────────────────────

fn prometheus_service_account() -> ServiceAccount {
    ServiceAccount { metadata: meta(PROMETHEUS_SERVICE), ..Default::default() }
}

/// Scrape discovery + the node proxy.
///
/// - Cluster-scoped: discovery lists across every namespace a run may land in
/// - `nodes/proxy` = what cAdvisor-through-the-apiserver authorizes as
fn prometheus_cluster_role() -> ClusterRole {
    ClusterRole {
        metadata: ObjectMeta { name: Some(PROMETHEUS_SERVICE.to_string()), ..Default::default() },
        rules: Some(vec![
            PolicyRule {
                api_groups: Some(vec![String::new()]),
                resources: Some(
                    ["nodes", "services", "endpoints", "pods"].map(String::from).to_vec(),
                ),
                verbs: ["get", "list", "watch"].map(String::from).to_vec(),
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec![String::new()]),
                resources: Some(["nodes/proxy", "nodes/metrics"].map(String::from).to_vec()),
                verbs: vec!["get".into()],
                ..Default::default()
            },
        ]),
        ..Default::default()
    }
}

fn prometheus_cluster_role_binding() -> ClusterRoleBinding {
    ClusterRoleBinding {
        metadata: ObjectMeta { name: Some(PROMETHEUS_SERVICE.to_string()), ..Default::default() },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "ClusterRole".into(),
            name: PROMETHEUS_SERVICE.into(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".into(),
            name: PROMETHEUS_SERVICE.into(),
            namespace: Some(OBS_NAMESPACE.into()),
            ..Default::default()
        }]),
    }
}

fn prometheus_deployment() -> Deployment {
    let pod = PodSpec {
        service_account_name: Some(PROMETHEUS_SERVICE.into()),
        security_context: Some(security_context(65534)),
        containers: vec![Container {
            name: "prometheus".into(),
            image: Some(image("prometheus", PROMETHEUS_IMAGE)),
            args: Some(prometheus_args()),
            ports: Some(vec![port(PROMETHEUS_PORT)]),
            readiness_probe: Some(probe("/-/ready", 5)),
            resources: Some(resources(("200m", "2"), ("512Mi", "4Gi"))),
            volume_mounts: Some(vec![
                mount("config", "/etc/prometheus"),
                mount("data", "/prometheus"),
            ]),
            ..Default::default()
        }],
        volumes: Some(vec![
            config_map_volume("config", &format!("{PROMETHEUS_SERVICE}-config")),
            pvc_volume("data", &format!("{PROMETHEUS_SERVICE}-data")),
        ]),
        ..Default::default()
    };
    let stamp = BTreeMap::from([(CONFIG_HASH_ANNOTATION.to_string(), config_hash())]);
    deployment(PROMETHEUS_SERVICE, "prometheus", Some(stamp), pod)
}

fn pyroscope_deployment() -> Deployment {
    let pod = PodSpec {
        security_context: Some(security_context(10001)),
        containers: vec![Container {
            name: "pyroscope".into(),
            image: Some(image("pyroscope", PYROSCOPE_IMAGE)),
            args: Some(pyroscope_args()),
            ports: Some(vec![port(PYROSCOPE_PORT)]),
            readiness_probe: Some(probe("/ready", 10)),
            resources: Some(resources(("200m", "2"), ("512Mi", "4Gi"))),
            volume_mounts: Some(vec![
                mount("config", "/etc/pyroscope"),
                mount("overrides", PYROSCOPE_OVERRIDES_DIR),
                mount("data", "/data"),
            ]),
            ..Default::default()
        }],
        volumes: Some(vec![
            config_map_volume("config", &format!("{PYROSCOPE_SERVICE}-config")),
            config_map_volume("overrides", PYROSCOPE_OVERRIDES_CONFIGMAP),
            pvc_volume("data", &format!("{PYROSCOPE_SERVICE}-data")),
        ]),
        ..Default::default()
    };
    deployment(PYROSCOPE_SERVICE, "pyroscope", None, pod)
}

fn prometheus_config_map() -> ConfigMap {
    config_map(&format!("{PROMETHEUS_SERVICE}-config"), "prometheus.yml", SCRAPE_CONFIG.into())
}

fn pyroscope_config_map() -> Result<ConfigMap, String> {
    let rendered = serde_yaml::to_string(&pyroscope_config())
        .map_err(|e| format!("render Pyroscope config: {e}"))?;
    Ok(config_map(&format!("{PYROSCOPE_SERVICE}-config"), "config.yaml", rendered))
}

fn pyroscope_overrides_config_map() -> Result<ConfigMap, String> {
    let seed = serde_yaml::to_string(&Overrides::default())
        .map_err(|e| format!("render Pyroscope overrides: {e}"))?;
    Ok(config_map(PYROSCOPE_OVERRIDES_CONFIGMAP, PYROSCOPE_OVERRIDES_KEY, seed))
}

fn grafana_config_map() -> ConfigMap {
    config_map(
        &format!("{GRAFANA_SERVICE}-datasources"),
        "datasources.yaml",
        GRAFANA_DATASOURCES.into(),
    )
}

/// Anonymous admin: port-forward-only test cluster, never behind an Ingress — a login
/// wall here is just a credential to lose
fn grafana_deployment() -> Deployment {
    let env = [
        ("GF_AUTH_ANONYMOUS_ENABLED", "true"),
        ("GF_AUTH_ANONYMOUS_ORG_ROLE", "Admin"),
        ("GF_AUTH_BASIC_ENABLED", "false"),
        ("GF_FEATURE_TOGGLES_ENABLE", "flameGraph"),
    ]
    .map(|(name, value)| EnvVar {
        name: name.into(),
        value: Some(value.into()),
        ..Default::default()
    })
    .to_vec();

    let pod = PodSpec {
        security_context: Some(security_context(472)),
        containers: vec![Container {
            name: "grafana".into(),
            image: Some(image("grafana", GRAFANA_IMAGE)),
            env: Some(env),
            ports: Some(vec![port(GRAFANA_PORT)]),
            readiness_probe: Some(probe("/api/health", 10)),
            resources: Some(resources(("100m", "1"), ("256Mi", "1Gi"))),
            volume_mounts: Some(vec![
                mount("datasources", "/etc/grafana/provisioning/datasources"),
                mount("data", "/var/lib/grafana"),
            ]),
            ..Default::default()
        }],
        volumes: Some(vec![
            config_map_volume("datasources", &format!("{GRAFANA_SERVICE}-datasources")),
            // emptyDir, unlike the two stores: provisioning rebuilds all of this at boot
            Volume {
                name: "data".into(),
                empty_dir: Some(EmptyDirVolumeSource::default()),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    deployment(GRAFANA_SERVICE, "grafana", None, pod)
}

/// Applied in dependency order: RBAC before the pod that binds it, ConfigMaps and PVCs
/// before the pod that mounts them (a Deployment whose volume is missing sits
/// `ContainerCreating` past the rollout wait, reporting nothing)
async fn apply_stack(cx: &Cx) -> Result<(), String> {
    const WHAT: &str = "observability stack";
    let client = &cx.client;
    let ns = |c: &kube::Client| -> (Api<ConfigMap>, Api<Service>, Api<Deployment>) {
        (
            Api::namespaced(c.clone(), OBS_NAMESPACE),
            Api::namespaced(c.clone(), OBS_NAMESPACE),
            Api::namespaced(c.clone(), OBS_NAMESPACE),
        )
    };
    let (config_maps, services, deployments) = ns(client);
    let accounts: Api<ServiceAccount> = Api::namespaced(client.clone(), OBS_NAMESPACE);
    let claims: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), OBS_NAMESPACE);
    let roles: Api<ClusterRole> = Api::all(client.clone());
    let bindings: Api<ClusterRoleBinding> = Api::all(client.clone());

    use crate::resource::kube::apply;
    apply(&accounts, &prometheus_service_account(), WHAT).await?;
    apply(&roles, &prometheus_cluster_role(), WHAT).await?;
    apply(&bindings, &prometheus_cluster_role_binding(), WHAT).await?;

    apply(&config_maps, &prometheus_config_map(), WHAT).await?;
    apply(&config_maps, &pyroscope_config_map()?, WHAT).await?;
    apply(&config_maps, &grafana_config_map(), WHAT).await?;
    // Seeded only when absent: `ztest cleanup` owns the contents, and an apply here
    // would drop every retirement it has written
    seed_overrides(&config_maps).await?;

    apply(&claims, &pvc(&format!("{PROMETHEUS_SERVICE}-data"), &prometheus_size()), WHAT).await?;
    apply(&claims, &pvc(&format!("{PYROSCOPE_SERVICE}-data"), &pyroscope_size()), WHAT).await?;

    apply(&deployments, &prometheus_deployment(), WHAT).await?;
    apply(&deployments, &pyroscope_deployment(), WHAT).await?;
    apply(&deployments, &grafana_deployment(), WHAT).await?;

    apply(&services, &service(PROMETHEUS_SERVICE, "prometheus", PROMETHEUS_PORT), WHAT).await?;
    apply(&services, &service(PYROSCOPE_SERVICE, "pyroscope", PYROSCOPE_PORT), WHAT).await?;
    apply(&services, &service(GRAFANA_SERVICE, "grafana", GRAFANA_PORT), WHAT).await
}

/// Create-if-absent, never apply — a re-provision must not clobber the retirements
/// `ztest cleanup` has written into it
async fn seed_overrides(api: &Api<ConfigMap>) -> Result<(), String> {
    use kube::api::PostParams;

    if api.get_opt(PYROSCOPE_OVERRIDES_CONFIGMAP).await.map_err(|e| e.to_string())?.is_some() {
        return Ok(());
    }
    match api.create(&PostParams::default(), &pyroscope_overrides_config_map()?).await {
        Ok(_) => Ok(()),
        // Lost the race with a concurrent setup; its seed is as good as ours
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(format!("seed {PYROSCOPE_OVERRIDES_CONFIGMAP}: {e}")),
    }
}

fn prometheus_size() -> String {
    volume_size("ZTEST_OBS_PROMETHEUS_SIZE", "20Gi")
}

fn pyroscope_size() -> String {
    volume_size("ZTEST_OBS_PYROSCOPE_SIZE", "20Gi")
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

    /// Running *and* configured as this build would configure it.
    ///
    /// Deployment availability alone would pin every cluster to whatever
    /// [`SCRAPE_CONFIG`] it was first set up with: `Lifetime::Cached` skips
    /// [`provision`](Self::provision) once ready, so a new scrape job would never reach
    /// a cluster that already has the stack — and would look installed
    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<Deployment> = Api::namespaced(cx.client.clone(), OBS_NAMESPACE);
        let want = config_hash();
        for name in DEPLOYMENTS {
            let Ok(deployment) = api.get(name).await else {
                return Readiness::Absent;
            };
            if !deployment_is_available(&deployment) {
                return Readiness::Absent;
            }
            // Prometheus alone carries the stamp; drifted config/flags must re-provision
            if name == PROMETHEUS_SERVICE && deployed_hash(&deployment) != Some(want.as_str()) {
                return Readiness::Absent;
            }
        }
        Readiness::Ready
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        apply_stack(cx).await.map_err(ResourceError::Provision)?;

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

/// Stamp the live Prometheus is running under.
///
/// Absent → treated as drift: re-applying is idempotent, and reading an unknown config
/// as current is the failure that hides a stale one
fn deployed_hash(deployment: &Deployment) -> Option<&str> {
    deployment
        .spec
        .as_ref()?
        .template
        .metadata
        .as_ref()?
        .annotations
        .as_ref()?
        .get(CONFIG_HASH_ANNOTATION)
        .map(String::as_str)
}

/// Config digest, stamped on the pod template so a changed [`SCRAPE_CONFIG`] or
/// [`prometheus_args`] rolls Prometheus.
///
/// A ConfigMap edit alone changes nothing running: the projected volume updates on the
/// kubelet's own sync period and Prometheus never re-reads the file. Rolling the pod is
/// cheaper than it sounds — `Recreate` over a PVC, so the TSDB survives
fn config_hash() -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(SCRAPE_CONFIG.as_bytes());
    for arg in prometheus_args() {
        hasher.update(arg.as_bytes());
    }
    // Pyroscope's too (pre-multitenancy cluster keeps pushing to `anonymous` → every
    // retirement matches nothing, reports success)
    hasher.update(serde_yaml::to_string(&pyroscope_config()).unwrap_or_default().as_bytes());
    for arg in pyroscope_args() {
        hasher.update(arg.as_bytes());
    }
    hasher.finalize().to_hex()[..16].to_string()
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

    /// Volume-bearing objects must exist before the pod that mounts them, RBAC before
    /// the pod that binds it
    #[test]
    fn the_stack_declares_every_object_its_pods_mount() {
        let mounted: Vec<String> = [prometheus_deployment(), pyroscope_deployment()]
            .iter()
            .flat_map(|d| {
                d.spec.as_ref().unwrap().template.spec.as_ref().unwrap().volumes.clone().unwrap()
            })
            .filter_map(|v| {
                v.config_map.map(|c| c.name).or(v.persistent_volume_claim.map(|p| p.claim_name))
            })
            .collect();

        let declared: Vec<String> = [
            prometheus_config_map(),
            pyroscope_config_map().expect("renders"),
            pyroscope_overrides_config_map().expect("renders"),
            grafana_config_map(),
        ]
        .iter()
        .filter_map(|c| c.metadata.name.clone())
        .chain([format!("{PROMETHEUS_SERVICE}-data"), format!("{PYROSCOPE_SERVICE}-data")])
        .collect();

        for name in mounted {
            assert!(declared.contains(&name), "{name} is mounted but never created");
        }
    }

    /// Selector and pod labels come from one place; a mismatch leaves the Service
    /// endpoint-less and every query silently empty
    #[test]
    fn every_service_selects_its_own_deployments_pods() {
        for (svc, deployment) in [
            (service(PROMETHEUS_SERVICE, "prometheus", PROMETHEUS_PORT), prometheus_deployment()),
            (service(PYROSCOPE_SERVICE, "pyroscope", PYROSCOPE_PORT), pyroscope_deployment()),
            (service(GRAFANA_SERVICE, "grafana", GRAFANA_PORT), grafana_deployment()),
        ] {
            let selector = svc.spec.expect("spec").selector.expect("selector");
            let pod_labels = deployment.spec.expect("spec").template.metadata.expect("meta").labels;
            assert_eq!(Some(&selector), pod_labels.as_ref());
        }
    }

    fn job(name: &str) -> serde_yaml::Value {
        let config: serde_yaml::Value = serde_yaml::from_str(SCRAPE_CONFIG).expect("valid YAML");
        config["scrape_configs"]
            .as_sequence()
            .expect("jobs")
            .iter()
            .find(|j| j["job_name"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("no {name} job"))
            .clone()
    }

    fn actions(job: &serde_yaml::Value, key: &str, action: &str) -> usize {
        job[key]
            .as_sequence()
            .map(|rules| rules.iter().filter(|r| r["action"].as_str() == Some(action)).count())
            .unwrap_or(0)
    }

    /// Dropping either `keep` rule scrapes every pod in the cluster
    #[test]
    fn discovery_keeps_only_ztest_pods_with_a_metrics_port() {
        let components = job("ztest-components");
        assert_eq!(actions(&components, "relabel_configs", "keep"), 2);
        assert!(SCRAPE_CONFIG.contains("__meta_kubernetes_pod_label_ztest_io_component_name"));
        assert!(SCRAPE_CONFIG.contains("__meta_kubernetes_pod_container_port_name"));
    }

    /// Ownership must ride the *series*, not the pod: `ztest cleanup` scopes by user,
    /// and by report time every pod it named is gone
    #[test]
    fn ownership_labels_are_promoted_onto_the_series() {
        let components = job("ztest-components");
        let promoted: Vec<(&str, &str)> = components["relabel_configs"]
            .as_sequence()
            .expect("rules")
            .iter()
            .filter_map(|r| {
                let source = r["source_labels"][0].as_str()?;
                Some((source, r["target_label"].as_str()?))
            })
            .collect();

        for (source, target) in [
            ("__meta_kubernetes_pod_label_ztest_io_run_id", "run_id"),
            ("__meta_kubernetes_pod_label_ztest_io_user", "user"),
            ("__meta_kubernetes_pod_label_ztest_io_sync_id", "sync_id"),
        ] {
            assert!(promoted.contains(&(source, target)), "{source} is not promoted to {target}");
        }
    }

    /// `delete_series`/`clean_tombstones` 404 without it, so `--purge-metrics` fails shut
    #[test]
    fn the_admin_api_the_purge_needs_is_enabled() {
        let deployed =
            prometheus_deployment().spec.expect("spec").template.spec.expect("pod").containers[0]
                .args
                .clone()
                .expect("args");
        assert!(deployed.iter().any(|a| a == "--web.enable-admin-api"), "{deployed:?}");
    }

    /// Flags touch no ConfigMap; hashing only [`SCRAPE_CONFIG`] would leave an existing
    /// cluster un-rolled and the admin API off, while `probe` reported it installed
    #[test]
    fn the_stamp_covers_the_flags_not_just_the_scrape_config() {
        let baseline = config_hash();
        assert_eq!(baseline.len(), 16);
        assert_eq!(deployed_hash(&prometheus_deployment()), Some(baseline.as_str()));

        let mut hasher = blake3::Hasher::new();
        hasher.update(SCRAPE_CONFIG.as_bytes());
        assert_ne!(baseline, hasher.finalize().to_hex()[..16].to_string());
    }

    /// Unfiltered cAdvisor is every container on every node — the run's TSDB budget
    /// spent on `kube-system`
    #[test]
    fn cadvisor_keeps_only_the_families_the_report_reads() {
        let cadvisor = job("kubelet-cadvisor");
        assert_eq!(actions(&cadvisor, "metric_relabel_configs", "keep"), 2);
        let names = kept_families(&cadvisor);
        for family in [
            "container_cpu_usage_seconds_total",
            "container_memory_working_set_bytes",
            "container_pressure_io_stalled_seconds_total",
            "container_pressure_io_waiting_seconds_total",
        ] {
            let bare = family.trim_start_matches("container_");
            assert!(names.contains(bare), "{family} must survive the keep: {names}");
        }
    }

    /// Both halves of PSI or neither: `full` alone under-reports a partial stall, `some`
    /// alone over-reports a parked worker, and one number cannot be corrected into the other
    #[test]
    fn the_io_pressure_pair_is_kept_together() {
        let names = kept_families(&job("kubelet-cadvisor"));
        assert_eq!(
            names.contains("pressure_io_stalled"),
            names.contains("pressure_io_waiting"),
            "PSI keeps both or neither: {names}"
        );
    }

    /// The `__name__` keep's regex, verbatim
    fn kept_families(job: &serde_yaml::Value) -> String {
        job["metric_relabel_configs"]
            .as_sequence()
            .expect("rules")
            .iter()
            .find(|r| r["source_labels"][0].as_str() == Some("__name__"))
            .expect("a __name__ filter")["regex"]
            .as_str()
            .expect("regex")
            .to_string()
    }

    /// Node IPs need a kubelet serving cert no pod trusts; the apiserver proxy needs
    /// only the in-cluster CA already mounted
    #[test]
    fn cadvisor_scrapes_through_the_apiserver_proxy() {
        let cadvisor = job("kubelet-cadvisor");
        let rendered = serde_yaml::to_string(&cadvisor).expect("serializes");
        assert!(rendered.contains("kubernetes.default.svc:443"), "{rendered}");
        assert!(rendered.contains("/proxy/metrics/cadvisor"), "{rendered}");
        let granted: Vec<String> = prometheus_cluster_role()
            .rules
            .expect("rules")
            .into_iter()
            .flat_map(|r| r.resources.unwrap_or_default())
            .collect();
        assert!(granted.contains(&"nodes/proxy".to_string()), "{granted:?}");
    }

    /// Filtered port name = the exporter contract's (drift here silently scrapes nothing)
    #[test]
    fn the_kept_port_name_is_the_exporter_contract() {
        assert!(SCRAPE_CONFIG.contains(&format!("regex: {}", crate::metrics::PORT_NAME)));
    }

    /// Every input the pods read must move the stamp; one left out leaves an existing
    /// cluster un-rolled while `probe` reports it installed
    #[test]
    fn the_stamp_moves_with_every_config_the_pods_read() {
        let baseline = config_hash();
        for ingredient in [
            SCRAPE_CONFIG.to_string(),
            prometheus_args().join(" "),
            serde_yaml::to_string(&pyroscope_config()).expect("renders"),
            pyroscope_args().join(" "),
        ] {
            let mut hasher = blake3::Hasher::new();
            hasher.update(ingredient.as_bytes());
            assert_ne!(baseline, hasher.finalize().to_hex()[..16].to_string());
        }
    }

    /// Drop any one → `ztest cleanup` reports a retirement that deletes nothing
    #[test]
    fn pyroscope_is_configured_for_per_tenant_deletion() {
        let config = pyroscope_config();
        assert!(config.multitenancy_enabled);
        // default `v1-v2-dual` routes deletion via the v1 compactor = a different limit
        assert_eq!(config.architecture_storage, "v2");
        // upstream reads 0 as never-delete, not delete-now
        assert!(PROFILE_RETENTION_HOURS > 0, "a zero retention disables deletion outright");
        assert_eq!(config.limits.retention_period, format!("{PROFILE_RETENTION_HOURS}h"));

        // runtime-config path must be the mount, else the retirements are never read
        let mounts = pyroscope_deployment().spec.unwrap().template.spec.unwrap().containers[0]
            .volume_mounts
            .clone()
            .unwrap();
        assert!(config.runtime_config.file.starts_with(PYROSCOPE_OVERRIDES_DIR));
        assert!(mounts.iter().any(|m| m.mount_path == PYROSCOPE_OVERRIDES_DIR), "{mounts:?}");
    }

    /// Upstream drops an override equal to the default (`retention.go`), so a retirement
    /// valued like [`PROFILE_RETENTION_HOURS`] would report success and delete nothing
    #[test]
    fn the_retirement_value_differs_from_the_default_retention() {
        assert_ne!(format!("{PROFILE_RETENTION_HOURS}h"), crate::profiling::RETIRED_RETENTION);
    }

    /// Seed must round-trip: `ztest cleanup` reads it back before adding a tenant
    #[test]
    fn the_seeded_overrides_document_parses_as_an_empty_override_set() {
        let cm = pyroscope_overrides_config_map().expect("renders");
        let seeded = &cm.data.expect("data")[PYROSCOPE_OVERRIDES_KEY];
        let parsed: Overrides = serde_yaml::from_str(seeded).expect("round-trips");
        assert!(parsed.overrides.is_empty());
    }

    /// Profiles *and* metastore state on the one PVC (else a restart cannot index them)
    #[test]
    fn pyroscope_stores_everything_on_its_volume() {
        let config = pyroscope_config();
        assert_eq!(config.storage.backend, "filesystem");
        for path in
            [config.storage.filesystem.dir, config.metastore.data_dir, config.metastore.raft.dir]
        {
            assert!(path.starts_with("/data/"), "every store must be under the volume: {path}");
        }
    }

    #[test]
    fn an_unset_storage_class_omits_the_field() {
        // SAFETY: single-threaded section, no other thread reads env here
        unsafe { std::env::remove_var("ZTEST_OBS_STORAGE_CLASS") };
        assert_eq!(pvc("any", "20Gi").spec.expect("spec").storage_class_name, None);
    }
}
