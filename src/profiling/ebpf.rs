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
//! - [`Placement`] tracks kubelet nesting: eBPF's initial-namespace pids are unresolvable from
//!   a pod under kind's node container, so there the collector runs host-side instead
//! - Emits `component`/`namespace` matching [`super::selector`], so `ztest sync perf` reads
//!   an eBPF profile and an in-process one through the same query

use super::host::HOST_KUBECONFIG;
use k8s_openapi::api::core::v1::{
    AppArmorProfile, ConfigMap, ConfigMapVolumeSource, Container, EnvVar, EnvVarSource,
    HostPathVolumeSource, ObjectFieldSelector, ResourceRequirements, SecurityContext, Volume,
    VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::BTreeMap;

/// Sidecar container name; also what `ztest sync perf` reads back to prove profiling was on
pub const CONTAINER: &str = "ebpf-profiler";

/// Port Alloy serves `/metrics` on, both placements. Never published to a fixed *host* port:
/// docker allocates one per host collector ([`super::host::metrics_port`] reads it back), so
/// two syncs cannot collide
pub const HTTP_PORT: u16 = 12345;

/// Pinned to the version validated on-cluster (a moved tag's `ImagePullBackOff` surfaces
/// as "the run produced no profile", three layers from its cause)
pub const ALLOY_IMAGE: &str = "grafana/alloy:v1.18.1";

/// Which pid namespace the collector observes from. Same image, same `.eh_frame` unwinder
/// either way — placement is the only variable.
///
/// - eBPF reports *initial*-namespace pids; resolving them needs a `/proc` that numbers them
/// - `Sidecar`: `hostPID` on the driver = the node's namespace = the initial one
/// - `Host`: a nested kubelet (kind's node is a container) puts every pod one level below the
///   initial namespace, where no privilege can name those pids → collector runs beside dockerd
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Placement {
    Sidecar,
    Host,
}

/// `Host` when the kubelet is nested, else `Sidecar` (an unreadable node = `Sidecar`: a probe
/// failure should not silently move every run's collector off-cluster)
pub async fn placement_for(client: &kube::Client) -> Placement {
    match crate::cluster_config::kubelet_is_nested(client).await {
        true => Placement::Host,
        false => Placement::Sidecar,
    }
}

/// Guaranteed QoS is a hard invariant for ztest pods (see `manifest::pod_is_guaranteed`);
/// added to the tier's `runner` reserve so the driver's total stays covered
pub const CPU_MILLI: u64 = 500;
pub const MEM_BYTES: u64 = 512 * 1024 * 1024;

pub fn resources() -> crate::qos::Resources {
    crate::qos::Resources::new(CPU_MILLI, MEM_BYTES, 0, 0)
}

/// Off-CPU sample probability (the configured value *is* `p`; upstream default `0` =
/// on-CPU only). Non-zero buys blocked-time attribution, the half of a sync profile that
/// explains I/O stall.
///
/// Thinned hard on purpose: one trace event per *scheduler switch*, into a fixed-size
/// per-CPU perf ring. At `1.0` a busy sync overran it by ~12k events/s — every trace
/// dropped, profile empty, collector idle at 14% of its CPU limit. Raise only while
/// `ztest sync perf` still reports 0 dropped
pub const DEFAULT_OFF_CPU: f64 = 0.05;

/// Upstream default. Deviating costs ring headroom that off-CPU events also draw on
pub const DEFAULT_HZ: u32 = 19;

/// Collector for one run's namespace, pushing under one tenant.
#[derive(Debug)]
pub struct Collector {
    pub namespace: String,
    pub tenant: String,
    pub push_url: String,
    pub hz: u32,
    pub off_cpu: f64,
    pub config_map: String,
    pub placement: Placement,
    /// `Host` only: discovery talks to the apiserver over the network, not a mounted SA token
    pub api_server: Option<String>,
}

impl Collector {
    /// `None` = no Pyroscope on this cluster; caller reports it rather than launching a
    /// collector that would push into nothing
    pub async fn for_sync(
        client: &kube::Client,
        sync_id: &str,
        namespace: &str,
        hz: u32,
        off_cpu: f64,
    ) -> Option<Collector> {
        let placement = placement_for(client).await;
        // Push target follows placement: a host collector cannot resolve a ClusterIP
        let (push_url, api_server) = match placement {
            Placement::Sidecar => (super::push_url(client).await?, None),
            Placement::Host => {
                (super::node_push_url(client).await?, Some(super::node_api_server(client).await?))
            }
        };
        Some(Collector {
            namespace: namespace.to_string(),
            tenant: crate::naming::profile_tenant(&crate::naming::current_user(), sync_id),
            push_url,
            hz,
            off_cpu,
            config_map: crate::sync::profiler_config_name(sync_id),
            placement,
            api_server,
        })
    }

    /// Pod discovery, scoped to the run either way.
    ///
    /// - `Sidecar`: in-cluster (SA token), pinned to its own node — a sidecar sees no other
    /// - `Host`: apiserver over the network + mounted kubeconfig; no node selector (the
    ///   collector is off-cluster, so `NODE_NAME` has nothing to resolve against)
    fn discovery(&self) -> String {
        let namespace = &self.namespace;
        let scope =
            format!("  role = \"pod\"\n  namespaces {{\n    names = [\"{namespace}\"]\n  }}");
        match (&self.placement, &self.api_server) {
            (Placement::Host, Some(api)) => format!(
                "discovery.kubernetes \"run_pods\" {{\n{scope}\n  \
                 api_server      = \"{api}\"\n  \
                 kubeconfig_file = \"{HOST_KUBECONFIG}\"\n}}"
            ),
            _ => format!(
                "discovery.kubernetes \"run_pods\" {{\n{scope}\n  selectors {{\n    \
                 role  = \"pod\"\n    field = \"spec.nodeName=\" + sys.env(\"NODE_NAME\")\n  }}\n}}"
            ),
        }
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
        let off_cpu = self.off_cpu;
        let url = &self.push_url;
        let tenant = &self.tenant;
        let sync_id_label = label_var(crate::sync::SYNC_ID_KEY);
        let discovery = self.discovery();
        format!(
            r#"
{discovery}

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
  off_cpu_threshold = {off_cpu}
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

    /// Rendered config for a host-placed collector, which reads a file rather than a mount
    pub fn host_config(&self) -> String {
        self.config()
    }

    /// Lives beside the driver in [`RUN_NAMESPACE`](crate::naming::RUN_NAMESPACE);
    /// caller owner-references it to the driver pod so it is collected with it
    pub fn config_map(&self) -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(self.config_map.to_string()),
                namespace: Some(crate::naming::RUN_NAMESPACE.to_string()),
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
    pub fn container(&self) -> Container {
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
                    &format!("--server.http.listen-addr=0.0.0.0:{HTTP_PORT}"),
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

    pub fn volumes(&self) -> Vec<Volume> {
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

    fn collector_on(placement: Placement) -> Collector {
        Collector {
            namespace: "ztest-sync-abc".into(),
            tenant: "ztest.eli.sync-123".into(),
            push_url: "http://ztest-pyroscope.ztest-obs.svc:4040".into(),
            hz: DEFAULT_HZ,
            off_cpu: DEFAULT_OFF_CPU,
            config_map: "ztest-sync-abc-profiler".into(),
            placement,
            api_server: match placement {
                Placement::Host => Some("https://127.0.0.1:6443".to_string()),
                Placement::Sidecar => None,
            },
        }
    }

    fn collector() -> Collector {
        collector_on(Placement::Sidecar)
    }

    /// Host placement must not emit a node selector: `NODE_NAME` resolves to nothing
    /// off-cluster, and the selector would match zero pods rather than error
    #[test]
    fn host_placement_discovers_over_the_apiserver() {
        let host = collector_on(Placement::Host).config();
        assert!(host.contains("api_server"), "{host}");
        assert!(host.contains("kubeconfig_file"), "{host}");
        assert!(!host.contains("NODE_NAME"), "{host}");

        let sidecar = collector_on(Placement::Sidecar).config();
        assert!(sidecar.contains("NODE_NAME"), "{sidecar}");
        assert!(!sidecar.contains("api_server"), "{sidecar}");
    }

    /// One image either way: placement moves the collector, it does not change the engine
    #[test]
    fn both_placements_share_one_image() {
        assert_eq!(collector().container().image.unwrap(), ALLOY_IMAGE);
        assert!(collector_on(Placement::Host).config().contains("off_cpu_threshold"));
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

    /// Off-CPU stays on (it is the half that explains I/O stall) but well under `1.0`,
    /// which overran the perf ring and dropped every trace
    #[test]
    fn config_captures_off_cpu_time_without_saturating_the_ring() {
        assert!(collector().config().contains(&format!("off_cpu_threshold = {DEFAULT_OFF_CPU}")));
        const { assert!(DEFAULT_OFF_CPU > 0.0 && DEFAULT_OFF_CPU <= 0.1) };
    }

    /// Upstream default; raising it draws on the same ring the off-CPU events fill
    #[test]
    fn sample_rate_matches_the_documented_default() {
        assert_eq!(DEFAULT_HZ, 19);
        assert!(collector().config().contains("sample_rate       = 19"));
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
