//! Manifest builders for each component variant.

use k8s_openapi::api::core::v1::Pod;
use serde_json::{Value, json};

use crate::EnvError;
use crate::backends;
use crate::component::{ComponentCategory, Resources};
use crate::mounts::ResolvedMount;
use crate::naming::RunCoords;

/// Rendered pod spec. Constraints the types don't show:
///
/// - `resources` (explicit) wins over `guaranteed` (QoS per-pod reserve); both
///   render `requests == limits`
/// - `run_as_user` + `supplemental_groups` for volumes `fsGroup` can't reconcile
///   (hostPath/local-path ignore it — see
///   [`SEED_GID`](crate::materialize::SEED_GID))
#[derive(Debug, Clone)]
pub struct PodSpec {
    pub pod_name: String,
    pub category: ComponentCategory,
    pub label: &'static str,
    pub image: String,
    pub ports: Vec<(String, u16)>,
    pub ready_port: u16,
    pub command: Option<Vec<String>>,
    pub args: Option<Vec<String>>,
    pub resources: Option<Resources>,
    pub guaranteed: Option<Resources>,
    pub env: Vec<(String, String)>,
    pub fs_group: Option<i64>,
    pub run_as_user: Option<i64>,
    pub supplemental_groups: Vec<i64>,
    pub placement: Option<crate::qos::Pool>,
    pub image_pull_secret: Option<String>,
    pub termination_grace_period: Option<i64>,
}

impl PodSpec {
    pub fn render(
        &self,
        coords: &RunCoords,
        test_name: &str,
        mounts: &[ResolvedMount],
    ) -> Result<Pod, EnvError> {
        let ports_json: Vec<_> =
            self.ports.iter().map(|(n, p)| json!({ "name": n, "containerPort": p })).collect();
        let volumes: Vec<Value> = mounts.iter().map(|m| m.volume.clone()).collect();
        let volume_mounts: Vec<Value> = mounts.iter().map(|m| m.volume_mount.clone()).collect();
        let component_label = self.label;

        let mut container = json!({
            "name": component_label,
            "image": self.image,
            "ports": ports_json,
            "volumeMounts": volume_mounts,
            "readinessProbe": {
                "tcpSocket": { "port": self.ready_port },
                "initialDelaySeconds": 1,
                "periodSeconds": 2,
                "failureThreshold": 60,
            },
        });
        if let Some(cmd) = &self.command {
            container["command"] = json!(cmd);
        }
        if let Some(args) = &self.args {
            container["args"] = json!(args);
        }
        // Guaranteed QoS (§7) → requests == limits; explicit wins over the QoS reserve
        let sizing = self.resources.as_ref().or(self.guaranteed.as_ref());
        if let Some(res) = sizing {
            container["resources"] = json!({
                "requests": { "cpu": res.cpu, "memory": res.memory },
                "limits": { "cpu": res.cpu, "memory": res.memory },
            });
        }
        if !self.env.is_empty() {
            let env: Vec<Value> = self
                .env
                .iter()
                .map(|(name, value)| json!({ "name": name, "value": value }))
                .collect();
            container["env"] = json!(env);
        }

        let mut spec = json!({
            "enableServiceLinks": false,
            "restartPolicy": "Never",
            "volumes": volumes,
            "containers": [container],
        });
        if let Some(grace) = self.termination_grace_period {
            spec["terminationGracePeriodSeconds"] = json!(grace);
        }
        let mut security_context = serde_json::Map::new();
        if let Some(fs_group) = self.fs_group {
            security_context.insert("fsGroup".into(), json!(fs_group));
        }
        if let Some(uid) = self.run_as_user {
            security_context.insert("runAsUser".into(), json!(uid));
        }
        if !self.supplemental_groups.is_empty() {
            security_context.insert("supplementalGroups".into(), json!(self.supplemental_groups));
        }
        if !security_context.is_empty() {
            spec["securityContext"] = Value::Object(security_context);
        }

        // Pod-level pull creds (cluster wants them here, not on the SA); the secret
        // must already exist in the test namespace
        if let Some(secret) = &self.image_pull_secret {
            spec["imagePullSecrets"] = json!([{ "name": secret }]);
        }

        let mut tolerations: Vec<Value> = Vec::new();

        // `sync` tier: nodeSelector keeps the pod off general nodes, the toleration
        // gets it past the NVMe taint (General/None = default scheduling)
        if let Some(crate::qos::Pool::Nvme) = self.placement {
            spec["nodeSelector"] =
                json!({ crate::qos::NVME_NODE_LABEL_KEY: crate::qos::NVME_NODE_LABEL_VALUE });
            tolerations.push(json!({
                "key": crate::qos::NVME_TAINT_KEY,
                "operator": "Exists",
                "effect": "NoSchedule",
            }));
        }

        // Lost node must kill, not migrate (a moved pod loses pinned CPUs + node-local
        // state); overrides the auto-added 300s not-ready/unreachable tolerations
        if sizing.is_some() {
            for cond in ["node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"] {
                tolerations.push(json!({
                    "key": cond,
                    "operator": "Exists",
                    "effect": "NoExecute",
                    "tolerationSeconds": 0,
                }));
            }
        }

        if !tolerations.is_empty() {
            spec["tolerations"] = Value::Array(tolerations);
        }

        // Hard invariant (§7): non-Guaranteed forfeits static-policy CPU pinning.
        // `assert!`, not `debug_assert!` — must fire in release too
        assert!(
            sizing.is_some(),
            "ztest pod {} has no resource sizing — would be BestEffort QoS; \
             every ztest pod must be Guaranteed",
            self.pod_name,
        );
        assert!(
            pod_is_guaranteed(&spec),
            "ztest pod {} is not Guaranteed (a container lacks cpu+memory limits) — \
             would downgrade to Burstable and lose CPU pinning",
            self.pod_name,
        );

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": self.pod_name,
                "labels": {
                    "ztest.io/run-id": coords.run_id,
                    "ztest.io/component": component_label,
                    // Backend-independent role: select "the indexer" without knowing
                    // zainod vs lightwalletd (`ztest sync watch` follows it)
                    "ztest.io/component-category": self.category.as_str(),
                    "ztest.io/test": test_name,
                    "ztest.io/component-name": self.pod_name,
                },
            },
            "spec": spec,
        });
        serde_json::from_value(pod)
            .map_err(|e| EnvError::Manifest { reason: format!("pod {}: {e}", self.pod_name) })
    }
}

/// Every container (regular + init) carries cpu & memory `limits` = necessary
/// condition for Guaranteed QoS and `static` CPU-manager pinning
fn pod_is_guaranteed(spec: &Value) -> bool {
    let has_cpu_mem_limits = |c: &Value| {
        let limits = &c["resources"]["limits"];
        limits.get("cpu").is_some() && limits.get("memory").is_some()
    };
    [spec.get("containers"), spec.get("initContainers")]
        .into_iter()
        .flatten()
        .all(|list| list.as_array().map(|cs| cs.iter().all(has_cpu_mem_limits)).unwrap_or(true))
}

pub(crate) fn merge_ports(defaults: &[(&str, u16)], extra: &[(String, u16)]) -> Vec<(String, u16)> {
    let mut out: Vec<(String, u16)> =
        defaults.iter().map(|(n, p)| ((*n).to_string(), *p)).collect();
    for (n, p) in extra {
        if out.iter().all(|(en, _)| en != n) {
            out.push((n.clone(), *p));
        }
    }
    out
}

/// Resolved-image result → pod image string, tagging any failure with the
/// component name for the surfaced [`EnvError`]
pub(crate) fn resolve_image(
    resolved: Result<backends::image::ResolvedImage, backends::image::ImageError>,
    component: &'static str,
) -> Result<String, EnvError> {
    resolved
        .map(|r| r.image)
        .map_err(|source| EnvError::ImageBuild { component: component.into(), source })
}

// No wallet pod spec: wallets run in-process (`crate::backends::zingo`)

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::{Cpu, Mem};

    fn coords() -> RunCoords {
        RunCoords { run_id: "run".into(), user: "user".into() }
    }

    fn base_spec() -> PodSpec {
        PodSpec {
            pod_name: "zebrad".into(),
            category: ComponentCategory::Validator,
            label: "zebrad",
            image: "zfnd/zebra:1.9.1".into(),
            ports: vec![("rpc".into(), 28232)],
            ready_port: 28232,
            command: None,
            args: None,
            resources: None,
            env: Vec::new(),
            fs_group: None,
            run_as_user: None,
            supplemental_groups: Vec::new(),
            placement: None,
            // render() rejects non-Guaranteed, so the fixture carries a reserve
            guaranteed: Some(Resources { cpu: Cpu::cores(1), memory: Mem::mib(512) }),
            image_pull_secret: None,
            termination_grace_period: None,
        }
    }

    fn container(pod: &Pod) -> Value {
        // Round-trip through JSON = the shape the API server would receive
        let v = serde_json::to_value(pod).unwrap();
        v["spec"]["containers"][0].clone()
    }

    #[test]
    fn explicit_resources_render_guaranteed_requests_equal_limits() {
        // Explicit override is Guaranteed too, never requests-only Burstable
        let spec = PodSpec {
            resources: Some(Resources { cpu: Cpu::millis(500), memory: Mem::mib(512) }),
            ..base_spec()
        };
        let pod = spec.render(&coords(), "t", &[]).unwrap();
        let c = container(&pod);
        assert_eq!(c["resources"]["requests"]["cpu"], "500m");
        assert_eq!(c["resources"]["requests"]["memory"], "512Mi");
        assert_eq!(c["resources"]["limits"]["cpu"], "500m");
        assert_eq!(c["resources"]["limits"]["memory"], "512Mi");
    }

    #[test]
    fn pod_is_guaranteed_requires_limits_on_every_container() {
        let ok = json!({
            "containers": [{ "resources": { "limits": { "cpu": "2", "memory": "1" } } }],
        });
        assert!(pod_is_guaranteed(&ok));
        // Requests-only → Burstable
        let burstable = json!({
            "containers": [{ "resources": { "requests": { "cpu": "2", "memory": "1" } } }],
        });
        assert!(!pod_is_guaranteed(&burstable));
        // Init container missing limits drops the whole pod to Burstable
        let downgraded = json!({
            "containers": [{ "resources": { "limits": { "cpu": "2", "memory": "1" } } }],
            "initContainers": [{ "resources": { "requests": { "cpu": "1", "memory": "1" } } }],
        });
        assert!(!pod_is_guaranteed(&downgraded));
    }

    #[test]
    fn qos_guaranteed_renders_requests_equal_limits_with_integer_cpu_and_fast_evict() {
        let spec = PodSpec {
            guaranteed: Some(Resources {
                cpu: Cpu::cores(2), // whole cores → eligible for static CPU pinning
                memory: Mem::gib(2),
            }),
            ..base_spec()
        };
        let pod = spec.render(&coords(), "t", &[]).unwrap();
        let c = container(&pod);
        assert_eq!(c["resources"]["requests"]["cpu"], "2");
        assert_eq!(c["resources"]["limits"]["cpu"], "2");
        assert_eq!(c["resources"]["requests"]["memory"], "2Gi");
        assert_eq!(c["resources"]["limits"]["memory"], "2Gi");

        // Fast-eviction: lost node kills immediately, not after 300s
        let s = pod_spec_json(&pod);
        let tols = s["tolerations"].as_array().unwrap();
        let nr = tols
            .iter()
            .find(|t| t["key"] == "node.kubernetes.io/not-ready")
            .expect("not-ready toleration");
        assert_eq!(nr["effect"], "NoExecute");
        assert_eq!(nr["tolerationSeconds"], 0);
        assert!(
            tols.iter().any(|t| t["key"] == "node.kubernetes.io/unreachable"),
            "unreachable toleration present: {s}"
        );
        // Bare pod → never rescheduled
        assert_eq!(s["restartPolicy"], "Never");
    }

    #[test]
    fn explicit_resources_take_precedence_over_guaranteed() {
        // Override wins, still Guaranteed at the override's values
        let spec = PodSpec {
            resources: Some(Resources { cpu: Cpu::millis(750), memory: Mem::gib(1) }),
            guaranteed: Some(Resources { cpu: Cpu::cores(4), memory: Mem::gib(8) }),
            ..base_spec()
        };
        let c = container(&spec.render(&coords(), "t", &[]).unwrap());
        assert_eq!(c["resources"]["requests"]["cpu"], "750m");
        assert_eq!(c["resources"]["limits"]["cpu"], "750m");
        assert_eq!(c["resources"]["requests"]["memory"], "1Gi");
        assert_eq!(c["resources"]["limits"]["memory"], "1Gi");
    }

    #[test]
    #[should_panic(expected = "must be Guaranteed")]
    fn render_panics_on_a_besteffort_pod() {
        // Neither override nor reserve = BestEffort; the guard must reject it in
        // every build
        let spec = PodSpec { guaranteed: None, resources: None, ..base_spec() };
        let _ = spec.render(&coords(), "t", &[]);
    }

    #[test]
    fn supplemental_groups_render_alongside_the_pinned_uid() {
        let spec = PodSpec { run_as_user: Some(1000), supplemental_groups: vec![0], ..base_spec() };
        let pod = spec.render(&coords(), "t", &[]).unwrap();
        let sc = &pod_spec_json(&pod)["securityContext"];
        // Both, not either: the pinned uid makes the extra group the only way into
        // a seed the pod does not own
        assert_eq!(sc["runAsUser"], 1000);
        assert_eq!(sc["supplementalGroups"], serde_json::json!([0]));
    }

    #[test]
    fn an_empty_group_set_renders_no_field() {
        let pod = base_spec().render(&coords(), "t", &[]).unwrap();
        assert!(pod_spec_json(&pod)["securityContext"]["supplementalGroups"].is_null());
    }

    #[test]
    fn env_renders_in_declaration_order() {
        let spec = PodSpec {
            env: vec![("RUST_LOG".into(), "debug".into()), ("FOO".into(), "bar".into())],
            ..base_spec()
        };
        let pod = spec.render(&coords(), "t", &[]).unwrap();
        let c = container(&pod);
        let env = c["env"].as_array().unwrap();
        assert_eq!(env[0]["name"], "RUST_LOG");
        assert_eq!(env[0]["value"], "debug");
        assert_eq!(env[1]["name"], "FOO");
    }

    #[test]
    fn no_env_omits_the_env_key() {
        // `resources` always present (every pod Guaranteed), `env` only when non-empty
        let pod = base_spec().render(&coords(), "t", &[]).unwrap();
        let c = container(&pod);
        assert!(c.get("env").is_none());
        assert!(c.get("resources").is_some());
    }

    #[test]
    fn image_pull_secret_renders_only_when_set() {
        // Default (kind / public registry): no key
        let s = pod_spec_json(&base_spec().render(&coords(), "t", &[]).unwrap());
        assert!(s.get("imagePullSecrets").is_none());
        // Private registry: named secret injected pod-level
        let spec = PodSpec {
            image_pull_secret: Some("ghcr-pull".into()),
            termination_grace_period: None,
            ..base_spec()
        };
        let s = pod_spec_json(&spec.render(&coords(), "t", &[]).unwrap());
        assert_eq!(s["imagePullSecrets"][0]["name"], "ghcr-pull");
    }

    fn pod_spec_json(pod: &Pod) -> Value {
        serde_json::to_value(pod).unwrap()["spec"].clone()
    }

    #[test]
    fn nvme_placement_renders_node_selector_and_toleration() {
        let spec = PodSpec { placement: Some(crate::qos::Pool::Nvme), ..base_spec() };
        let pod = spec.render(&coords(), "t", &[]).unwrap();
        let s = pod_spec_json(&pod);
        assert_eq!(
            s["nodeSelector"][crate::qos::NVME_NODE_LABEL_KEY],
            crate::qos::NVME_NODE_LABEL_VALUE
        );
        let tol = &s["tolerations"][0];
        assert_eq!(tol["key"], crate::qos::NVME_TAINT_KEY);
        assert_eq!(tol["operator"], "Exists");
        assert_eq!(tol["effect"], "NoSchedule");
    }

    #[test]
    fn general_and_none_placement_omit_nvme_scheduling_keys() {
        // base_spec is Guaranteed → always carries the fast-eviction NoExecute
        // tolerations; General/None must add neither NVMe nodeSelector nor NoSchedule
        let no_nvme = |s: &Value| {
            assert!(s.get("nodeSelector").is_none());
            let has_nvme_tol = s["tolerations"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|t| t["effect"] == "NoSchedule" || t["key"] == crate::qos::NVME_TAINT_KEY);
            assert!(!has_nvme_tol, "no NVMe toleration for general/none: {s}");
        };
        no_nvme(&pod_spec_json(&base_spec().render(&coords(), "t", &[]).unwrap()));
        // Explicit General also a no-op (schedules anywhere)
        let spec = PodSpec { placement: Some(crate::qos::Pool::General), ..base_spec() };
        no_nvme(&pod_spec_json(&spec.render(&coords(), "t", &[]).unwrap()));
    }
}
