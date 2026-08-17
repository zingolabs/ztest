//! Per-run eBPF profile collector: a privileged Alloy sidecar on the sync's driver pod.
//!
//! - Sidecar, not a DaemonSet: `pyroscope.ebpf` only sees its own node, and the driver pod
//!   is the one pod guaranteed to exist for the run's whole life
//! - Rides the driver → outside the sync namespace's `ResourceQuota` (which sizes to the
//!   component footprint alone and has no slot for a collector), and dies with it
//! - Costs the driver `hostPID` (pod-level, so the driver container shares it): eBPF
//!   resolves sampled PIDs against the host namespace
//! - Discovery hits the *sync* namespace over the API; the driver SA already lists pods
//!   there, so no per-run RBAC
//! - Covers what shares the driver's node — `ztest sync perf` names any component that
//!   did not, rather than reporting an empty profile
//! - Emits `component`/`namespace` matching [`super::selector`], so `ztest sync perf` reads
//!   an eBPF profile and an in-process one through the same query

use k8s_openapi::api::core::v1::{
    AppArmorProfile, ConfigMap, ConfigMapVolumeSource, Container, EnvVar, EnvVarSource,
    HostPathVolumeSource, ObjectFieldSelector, ResourceRequirements, SecurityContext, Volume,
    VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::BTreeMap;

/// Sidecar container name; also what `ztest sync perf` reads back to prove profiling was on
pub(crate) const CONTAINER: &str = "ebpf-profiler";

/// Pinned to the version validated on-cluster (a moved tag's `ImagePullBackOff` surfaces
/// as "the run produced no profile", three layers from its cause)
const ALLOY_IMAGE: &str = "grafana/alloy:v1.18.1";

/// Guaranteed QoS is a hard invariant for ztest pods (see `manifest::pod_is_guaranteed`);
/// added to the tier's `runner` reserve so the driver's total stays covered
pub(crate) const CPU_MILLI: u64 = 500;
pub(crate) const MEM_BYTES: u64 = 512 * 1024 * 1024;

pub(crate) fn resources() -> crate::qos::Resources {
    crate::qos::Resources::new(CPU_MILLI, MEM_BYTES, 0, 0)
}

/// Off-CPU sampling probability. Non-zero = blocked time captured (`0` = on-CPU only, the
/// upstream default), which is the whole reason a profile can attribute I/O stall at all
const OFF_CPU_THRESHOLD: f64 = 1.0;

/// `pyroscope.ebpf` default is 19; matched to the in-process contract instead so a profile
/// taken before and after the collector swap carries comparable sample density
pub(crate) const DEFAULT_HZ: u32 = 100;

/// Collector for one run's namespace, pushing under one tenant.
pub(crate) struct Collector {
    pub namespace: String,
    pub tenant: String,
    pub push_url: String,
    pub hz: u32,
    pub config_map: String,
}

impl Collector {
    /// `None` = no Pyroscope on this cluster; caller reports it rather than launching a
    /// collector that would push into nothing
    pub(crate) async fn for_sync(
        client: &kube::Client,
        sync_id: &str,
        namespace: &str,
        hz: u32,
    ) -> Option<Collector> {
        Some(Collector {
            namespace: namespace.to_string(),
            tenant: super::tenant(&crate::naming::current_user(), sync_id),
            push_url: super::push_url(client).await?,
            hz,
            config_map: crate::cli::sync::profiler_config_name(sync_id),
        })
    }

    /// Alloy river config.
    ///
    /// - `NODE_NAME` from the downward API, never `HOSTNAME` (= the *pod* name unless
    ///   `hostNetwork`; upstream's own example gets this wrong and silently matches 0 pods)
    /// - Node field selector = the co-location constraint, made explicit
    /// - `__container_id__` strips the `containerd://` scheme: matching is against the raw
    ///   id a sampled PID's cgroup resolves to, so the prefix silently attributes nothing
    /// - `demangle` defaults to `none` upstream → C++ frames arrive as raw `_ZN…` symbols
    fn config(&self) -> String {
        let hz = self.hz;
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
    regex         = "^[a-z0-9-]+://(.+)$"
    replacement   = "$1"
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

    /// Lives beside the driver in [`RUN_NAMESPACE`](crate::resource::impls::policy::RUN_NAMESPACE);
    /// caller owner-references it to the driver pod so it is collected with it
    pub(crate) fn config_map(&self) -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(self.config_map.to_string()),
                namespace: Some(crate::resource::impls::policy::RUN_NAMESPACE.to_string()),
                ..Default::default()
            },
            data: Some(BTreeMap::from([("config.alloy".to_string(), self.config())])),
            ..Default::default()
        }
    }

    /// Native sidecar — belongs in `initContainers`, never `containers`.
    ///
    /// - Alloy never exits, so as a regular container it would hold a `restartPolicy: Never`
    ///   pod at `Running` forever: the sync would never settle and `ztest cleanup` would
    ///   read it as live
    /// - `restartPolicy: Always` on an init container = kubelet stops it once the driver
    ///   container terminates (k8s ≥ 1.29)
    /// - `privileged` + `Unconfined`: BPF program load is blocked by the default AppArmor
    ///   profile, and the loader raises `RLIMIT_MEMLOCK`
    /// - Pod must also set `hostPID` — sampled PIDs resolve against the host namespace
    pub(crate) fn container(&self) -> Container {
        let limits = BTreeMap::from([
            ("cpu".to_string(), Quantity(format!("{CPU_MILLI}m"))),
            ("memory".to_string(), Quantity(format!("{MEM_BYTES}"))),
        ]);
        Container {
            name: CONTAINER.to_string(),
            image: Some(ALLOY_IMAGE.to_string()),
            restart_policy: Some("Always".to_string()),
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
                mount("ebpf-config", "/etc/alloy"),
                mount("sys-fs-bpf", "/sys/fs/bpf"),
                mount("sys-kernel-debug", "/sys/kernel/debug"),
            ]),
            ..Default::default()
        }
    }

    pub(crate) fn volumes(&self) -> Vec<Volume> {
        vec![
            Volume {
                name: "ebpf-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: self.config_map.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            host_path("sys-fs-bpf", "/sys/fs/bpf"),
            host_path("sys-kernel-debug", "/sys/kernel/debug"),
        ]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn collector() -> Collector {
        Collector {
            namespace: "ztest-sync-abc".into(),
            tenant: "ztest.eli.sync-123".into(),
            push_url: "http://ztest-pyroscope.ztest-obs.svc:4040".into(),
            hz: DEFAULT_HZ,
            config_map: "ztest-sync-abc-profiler".into(),
        }
    }

    #[test]
    fn config_scopes_discovery_to_the_sync_namespace() {
        let config = collector().config();
        assert!(config.contains(r#"names = ["ztest-sync-abc"]"#), "{config}");
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

    /// Targets carry `containerd://<id>`; matching is against the bare id, so an unstripped
    /// scheme profiles nothing while every metric still reads healthy
    #[test]
    fn container_id_is_stripped_of_its_scheme() {
        let config = collector().config();
        let rule = config
            .split("rule {")
            .find(|r| r.contains(r#"target_label  = "__container_id__""#))
            .expect("container-id rule");
        assert!(rule.contains(r#"regex         = "^[a-z0-9-]+://(.+)$""#), "{rule}");
        assert!(rule.contains(r#"replacement   = "$1""#), "{rule}");
    }

    #[test]
    fn config_captures_off_cpu_time() {
        assert!(!collector().config().contains("off_cpu_threshold = 0\n"));
    }

    #[test]
    fn label_var_matches_prometheus_sd_mangling() {
        assert_eq!(label_var("ztest.io/sync-id"), "__meta_kubernetes_pod_label_ztest_io_sync_id");
    }

    /// Sidecar rides a Guaranteed pod; unequal requests/limits would demote the *driver*
    #[test]
    fn container_is_guaranteed_and_matches_the_reserved_amount() {
        let container = collector().container();
        let r = container.resources.expect("resources set");
        assert_eq!(r.limits, r.requests);
        let limits = r.limits.expect("limits");
        assert_eq!(limits["cpu"].0, format!("{CPU_MILLI}m"));
        assert_eq!(limits["memory"].0, format!("{MEM_BYTES}"));
        assert_eq!(resources(), crate::qos::Resources::new(CPU_MILLI, MEM_BYTES, 0, 0));
    }

    /// Alloy never exits: as a regular container it holds the `restartPolicy: Never`
    /// driver pod at `Running` forever, so the sync never settles and cleanup reads it live
    #[test]
    fn container_is_a_native_sidecar() {
        assert_eq!(collector().container().restart_policy.as_deref(), Some("Always"));
    }

    #[test]
    fn container_can_load_bpf_programs() {
        let ctx = collector().container().security_context.expect("security context");
        assert_eq!(ctx.privileged, Some(true));
        assert_eq!(ctx.app_armor_profile.expect("apparmor").type_, "Unconfined");
    }

    /// Every mount the container declares must have a backing volume, else the pod is
    /// rejected at admission with a message naming neither
    #[test]
    fn every_mount_has_a_volume() {
        let collector = collector();
        let volumes: Vec<String> = collector.volumes().into_iter().map(|v| v.name).collect();
        for mount in collector.container().volume_mounts.expect("mounts") {
            assert!(volumes.contains(&mount.name), "{} unbacked: {volumes:?}", mount.name);
        }
    }

    #[test]
    fn config_map_lives_beside_the_driver() {
        let cm = collector().config_map();
        assert_eq!(cm.metadata.namespace.as_deref(), Some("ztest"));
        assert_eq!(cm.metadata.name.as_deref(), Some("ztest-sync-abc-profiler"));
        assert!(cm.data.expect("data").contains_key("config.alloy"));
    }
}
