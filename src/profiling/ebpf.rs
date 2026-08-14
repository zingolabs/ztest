//! Per-run eBPF profile collector: privileged Alloy DaemonSet, one per profiled run.
//!
//! - One collector per *run*, not per node: `pyroscope.write` headers are static, so an
//!   Alloy instance pushes to exactly one tenant (grafana/alloy#259) — and ztest retires a
//!   sync's profiles by tenant, so a node-wide collector would forfeit deletion entirely
//! - DaemonSet, not Pod: `pyroscope.ebpf` only sees processes on its own node, and a run's
//!   pods are not co-scheduled
//! - Discovery scoped to the run's namespace → namespaced Role (GC'd with the namespace),
//!   never a ClusterRole
//! - Emits `component`/`namespace` matching [`super::selector`], so `ztest sync perf` reads
//!   an eBPF profile and an in-process one through the same query

use k8s_openapi::api::apps::v1::{DaemonSet, DaemonSetSpec};
use k8s_openapi::api::core::v1::{
    AppArmorProfile, ConfigMap, ConfigMapVolumeSource, Container, EnvVar, EnvVarSource,
    HostPathVolumeSource, ObjectFieldSelector, PodSpec, PodTemplateSpec, ResourceRequirements,
    SecurityContext, ServiceAccount, Volume, VolumeMount,
};
use k8s_openapi::api::rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::Client;
use kube::api::Api;
use std::collections::BTreeMap;

use crate::resource::kube::apply;

/// Every object the collector owns; one name, so a run's leftovers are greppable
const NAME: &str = "ztest-ebpf-profiler";

/// Pinned to the version validated on-cluster (a moved tag's `ImagePullBackOff` surfaces
/// as "the run produced no profile", three layers from its cause)
const ALLOY_IMAGE: &str = "grafana/alloy:v1.18.1";

/// Guaranteed QoS is a hard invariant for ztest pods (see `manifest::pod_is_guaranteed`);
/// requests are omitted so Kubernetes derives them from these
const CPU_LIMIT: &str = "500m";
const MEMORY_LIMIT: &str = "512Mi";

/// Off-CPU sampling probability. Non-zero = blocked time captured (`0` = on-CPU only, the
/// upstream default), which is the whole reason a profile can attribute I/O stall at all
const OFF_CPU_THRESHOLD: f64 = 1.0;

/// `pyroscope.ebpf` default is 19; matched to the in-process contract instead so a profile
/// taken before and after the collector swap carries comparable sample density
const DEFAULT_HZ: u32 = 100;

/// Sample-rate override, Hz. `rate × cores` over the kernel tick under-samples, which
/// `ztest sync perf` reports as lost fidelity
pub(crate) const HZ_ENV: &str = "ZTEST_PROFILE_HZ";

pub(crate) fn hz() -> Option<u32> {
    std::env::var(HZ_ENV).ok()?.parse().ok().filter(|&h| h > 0)
}

/// Opt-in switch, run-level.
///
/// - Not the image's `profile` feature: that gated *linking* an in-process profiler, a
///   constraint eBPF does not have (any pod profiles, published images included)
/// - Off by default — a collector = one more privileged pod against the run's capacity
const REQUEST_ENV: &str = "ZTEST_PROFILE";

pub(crate) fn requested() -> bool {
    std::env::var(REQUEST_ENV).is_ok_and(|v| !matches!(v.trim(), "" | "0" | "false"))
}

/// Collector for one run's namespace, pushing under one tenant.
pub(crate) struct Collector<'a> {
    pub namespace: &'a str,
    pub tenant: &'a str,
    pub push_url: &'a str,
    pub hz: Option<u32>,
}

impl Collector<'_> {
    /// Alloy river config.
    ///
    /// - `NODE_NAME` from the downward API, never `HOSTNAME` (= the *pod* name unless
    ///   `hostNetwork`; upstream's own example gets this wrong and silently matches 0 pods)
    /// - `demangle` defaults to `none` upstream → C++ frames arrive as raw `_ZN…` symbols
    fn config(&self) -> String {
        let hz = self.hz.unwrap_or(DEFAULT_HZ);
        let namespace = &self.namespace;
        let url = &self.push_url;
        let tenant = &self.tenant;
        let sync_id_label = label_var(crate::sync::SYNC_ID_KEY);
        format!(
            r#"
discovery.kubernetes "run_pods" {{
  role = "pod"
  namespaces {{
    names = ["{namespace}"]
  }}
  selectors {{
    role  = "pod"
    field = "spec.nodeName=" + sys.env("NODE_NAME")
  }}
}}

discovery.relabel "targets" {{
  targets = discovery.kubernetes.run_pods.targets

  rule {{
    action        = "drop"
    source_labels = ["__meta_kubernetes_pod_phase"]
    regex         = "Succeeded|Failed|Pending"
  }}
  rule {{
    action        = "replace"
    source_labels = ["__meta_kubernetes_pod_label_ztest_io_component_name"]
    target_label  = "component"
  }}
  rule {{
    action        = "replace"
    source_labels = ["__meta_kubernetes_pod_label_ztest_io_component_name"]
    target_label  = "service_name"
  }}
  rule {{
    action        = "replace"
    source_labels = ["__meta_kubernetes_namespace"]
    target_label  = "namespace"
  }}
  rule {{
    action        = "replace"
    source_labels = ["__meta_kubernetes_pod_label_ztest_io_run_id"]
    target_label  = "run_id"
  }}
  rule {{
    action        = "replace"
    source_labels = ["{sync_id_label}"]
    target_label  = "sync_id"
  }}
  rule {{
    action        = "replace"
    source_labels = ["__meta_kubernetes_pod_container_id"]
    target_label  = "__container_id__"
  }}
}}

pyroscope.ebpf "run" {{
  targets           = discovery.relabel.targets.output
  forward_to        = [pyroscope.write.store.receiver]
  demangle          = "full"
  sample_rate       = {hz}
  off_cpu_threshold = {OFF_CPU_THRESHOLD}
}}

pyroscope.write "store" {{
  endpoint {{
    url = "{url}"
    headers = {{
      "X-Scope-OrgID" = "{tenant}",
    }}
  }}
}}
"#
        )
    }

    fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            name: Some(NAME.to_string()),
            namespace: Some(self.namespace.to_string()),
            labels: Some(BTreeMap::from([(
                "ztest.io/component-name".to_string(),
                NAME.to_string(),
            )])),
            ..Default::default()
        }
    }

    fn service_account(&self) -> ServiceAccount {
        ServiceAccount { metadata: self.meta(), ..Default::default() }
    }

    fn role(&self) -> Role {
        Role {
            metadata: self.meta(),
            rules: Some(vec![PolicyRule {
                api_groups: Some(vec![String::new()]),
                resources: Some(vec!["pods".to_string()]),
                verbs: ["get", "list", "watch"].iter().map(|v| v.to_string()).collect(),
                ..Default::default()
            }]),
        }
    }

    fn role_binding(&self) -> RoleBinding {
        RoleBinding {
            metadata: self.meta(),
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".to_string(),
                kind: "Role".to_string(),
                name: NAME.to_string(),
            },
            subjects: Some(vec![Subject {
                kind: "ServiceAccount".to_string(),
                name: NAME.to_string(),
                namespace: Some(self.namespace.to_string()),
                ..Default::default()
            }]),
        }
    }

    fn config_map(&self) -> ConfigMap {
        ConfigMap {
            metadata: self.meta(),
            data: Some(BTreeMap::from([("config.alloy".to_string(), self.config())])),
            ..Default::default()
        }
    }

    /// - `privileged` + `Unconfined`: BPF program load is blocked by the default AppArmor
    ///   profile, and the loader raises `RLIMIT_MEMLOCK`
    /// - `host_pid`: sampled PIDs resolve against the host namespace, not the pod's
    fn daemon_set(&self) -> DaemonSet {
        let selector = BTreeMap::from([("ztest.io/component-name".to_string(), NAME.to_string())]);
        DaemonSet {
            metadata: self.meta(),
            spec: Some(DaemonSetSpec {
                selector: LabelSelector {
                    match_labels: Some(selector.clone()),
                    ..Default::default()
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta { labels: Some(selector), ..Default::default() }),
                    spec: Some(PodSpec {
                        service_account_name: Some(NAME.to_string()),
                        host_pid: Some(true),
                        containers: vec![self.container()],
                        volumes: Some(vec![
                            Volume {
                                name: "config".to_string(),
                                config_map: Some(ConfigMapVolumeSource {
                                    name: NAME.to_string(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            host_path("sys-fs-bpf", "/sys/fs/bpf"),
                            host_path("sys-kernel-debug", "/sys/kernel/debug"),
                        ]),
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn container(&self) -> Container {
        let limits = BTreeMap::from([
            ("cpu".to_string(), Quantity(CPU_LIMIT.to_string())),
            ("memory".to_string(), Quantity(MEMORY_LIMIT.to_string())),
        ]);
        Container {
            name: "alloy".to_string(),
            image: Some(ALLOY_IMAGE.to_string()),
            args: Some(
                [
                    "run",
                    "/etc/alloy/config.alloy",
                    "--storage.path=/tmp/alloy",
                    "--server.http.listen-addr=0.0.0.0:12345",
                ]
                .iter()
                .map(|a| a.to_string())
                .collect(),
            ),
            env: Some(vec![EnvVar {
                name: "NODE_NAME".to_string(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "spec.nodeName".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            security_context: Some(SecurityContext {
                privileged: Some(true),
                run_as_user: Some(0),
                run_as_group: Some(0),
                app_armor_profile: Some(AppArmorProfile {
                    type_: "Unconfined".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            resources: Some(ResourceRequirements {
                limits: Some(limits.clone()),
                requests: Some(limits),
                ..Default::default()
            }),
            volume_mounts: Some(vec![
                mount("config", "/etc/alloy"),
                mount("sys-fs-bpf", "/sys/fs/bpf"),
                mount("sys-kernel-debug", "/sys/kernel/debug"),
            ]),
            ..Default::default()
        }
    }
}

/// Prometheus SD exposes a pod label as `__meta_kubernetes_pod_label_<key>`, non-alphanumerics
/// mapped to `_` — derived here so a renamed label key cannot silently stop matching
fn label_var(key: &str) -> String {
    let sanitised: String =
        key.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '_' }).collect();
    format!("__meta_kubernetes_pod_label_{sanitised}")
}

fn host_path(name: &str, path: &str) -> Volume {
    Volume {
        name: name.to_string(),
        host_path: Some(HostPathVolumeSource {
            path: path.to_string(),
            type_: Some("DirectoryOrCreate".to_string()),
        }),
        ..Default::default()
    }
}

fn mount(name: &str, path: &str) -> VolumeMount {
    VolumeMount { name: name.to_string(), mount_path: path.to_string(), ..Default::default() }
}

/// Create the collector in the run's namespace.
///
/// - Failure never fails the run: a missing profile costs a diagnostic, a failed setup costs
///   the run (same contract as the in-process pusher)
pub(crate) async fn deploy(client: &Client, collector: &Collector<'_>) -> Result<(), String> {
    let ns = collector.namespace;
    apply(&Api::namespaced(client.clone(), ns), &collector.service_account(), NAME).await?;
    apply(&Api::namespaced(client.clone(), ns), &collector.role(), NAME).await?;
    apply(&Api::namespaced(client.clone(), ns), &collector.role_binding(), NAME).await?;
    apply(&Api::namespaced(client.clone(), ns), &collector.config_map(), NAME).await?;
    apply(&Api::namespaced(client.clone(), ns), &collector.daemon_set(), NAME).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collector() -> Collector<'static> {
        Collector {
            namespace: "ztest-run-abc",
            tenant: "ztest.eli.sync-123",
            push_url: "http://ztest-pyroscope.ztest-obs.svc:4040",
            hz: None,
        }
    }

    #[test]
    fn config_scopes_discovery_to_the_run_namespace() {
        let config = collector().config();
        assert!(config.contains(r#"names = ["ztest-run-abc"]"#), "{config}");
    }

    /// Static header = the whole reason this is per-run; a lost tenant makes the run's
    /// profiles undeletable (`schedule_purge` retires by tenant)
    #[test]
    fn config_pushes_under_the_run_tenant() {
        assert!(collector().config().contains(r#""X-Scope-OrgID" = "ztest.eli.sync-123""#));
    }

    /// `HOSTNAME` = the pod name, so the selector would match zero pods and the collector
    /// would run silently against nothing
    #[test]
    fn config_takes_the_node_from_the_downward_api() {
        let config = collector().config();
        assert!(config.contains(r#"sys.env("NODE_NAME")"#), "{config}");
        assert!(!config.contains("HOSTNAME"), "{config}");
    }

    #[test]
    fn config_emits_the_labels_ztest_sync_perf_queries_by() {
        let config = collector().config();
        for label in ["component", "namespace", "sync_id", "run_id"] {
            assert!(config.contains(&format!(r#"target_label  = "{label}""#)), "{label}: {config}");
        }
    }

    #[test]
    fn config_captures_off_cpu_time() {
        assert!(!collector().config().contains("off_cpu_threshold = 0\n"));
    }

    #[test]
    fn label_var_matches_prometheus_sd_mangling() {
        assert_eq!(label_var("ztest.io/sync-id"), "__meta_kubernetes_pod_label_ztest_io_sync_id");
    }

    #[test]
    fn container_is_guaranteed() {
        let container = collector().container();
        let resources = container.resources.expect("resources set");
        assert_eq!(resources.limits, resources.requests);
    }

    #[test]
    fn daemon_set_can_load_bpf_programs() {
        let ds = collector().daemon_set();
        let spec = ds.spec.expect("spec").template.spec.expect("pod spec");
        assert_eq!(spec.host_pid, Some(true));
        let ctx = spec.containers[0].security_context.clone().expect("security context");
        assert_eq!(ctx.privileged, Some(true));
        assert_eq!(ctx.app_armor_profile.expect("apparmor").type_, "Unconfined");
    }
}
