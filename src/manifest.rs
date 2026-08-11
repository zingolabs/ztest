//! Manifest builders for each component variant.

use k8s_openapi::api::core::v1::Pod;
use serde_json::{Value, json};

use crate::EnvError;
use crate::backends;
use crate::component::{ComponentCategory, Resources};
use crate::mounts::ResolvedMount;
use crate::naming::RunCoords;

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
    /// Container resource requests, rendered into `resources.requests`. Set by
    /// an explicit `.resources()`; takes precedence over [`Self::guaranteed`].
    pub resources: Option<Resources>,
    /// QoS-default per-pod reserve (the tier footprint split across the env's
    /// pods, §7), injected by `TestEnv::build` when the test didn't call
    /// `.resources()`. Rendered as `requests == limits` (Guaranteed QoS).
    pub guaranteed: Option<Resources>,
    /// Container environment variables, in declaration order.
    pub env: Vec<(String, String)>,
    pub fs_group: Option<i64>,
    /// `securityContext.runAsUser` override. Set when a pod must read another
    /// pod's files on a shared volume whose ownership can't be reconciled via
    /// `fsGroup` (hostPath/local-path volumes ignore it), e.g. a zaino
    /// StateService reading the root-owned zebra-state DB the validator wrote.
    /// `None` keeps the image default user.
    pub run_as_user: Option<i64>,
    /// `securityContext.supplementalGroups`: extra gids added to the container's
    /// group set, on top of its primary gid.
    ///
    /// The reason this exists is the seed group. A pod's primary gid comes from
    /// the image's `USER` when nothing pins `runAsGroup`, so it says nothing
    /// about the volumes the pod mounts; a group it must hold to read shared
    /// storage has to be named here. See
    /// [`SEED_GID`](crate::materialize::SEED_GID) and
    /// [`seed_groups`](crate::backends::seed_groups). Empty renders no field.
    pub supplemental_groups: Vec<i64>,
    /// The QoS tier's node placement target. `Some(Pool::Nvme)` renders the
    /// NVMe nodeSelector and toleration so a `sync` pod lands on the dedicated
    /// NVMe pool; `General` and `None` schedule anywhere. Placement, not sizing;
    /// see [`crate::qos::Pool`].
    pub placement: Option<crate::qos::Pool>,
    /// Optional `imagePullSecrets` entry name, for a private registry whose pull
    /// credentials aren't already on the pods' ServiceAccount or node config.
    /// `None` (kind mode, public registries) omits the field and relies on
    /// SA-/node-level auth. See [`crate::backends::image::pull_secret`].
    pub image_pull_secret: Option<String>,
    /// `terminationGracePeriodSeconds` for the pod. `None` uses the k8s default
    /// (30 s). Raised for a profiled component so its `SIGTERM` shutdown has
    /// time to flush the flamegraph before the kubelet `SIGKILL`s it.
    pub termination_grace_period: Option<i64>,
}

impl PodSpec {
    pub fn render(
        &self,
        coords: &RunCoords,
        test_name: &str,
        mounts: &[ResolvedMount],
    ) -> Result<Pod, EnvError> {
        let ports_json: Vec<_> = self
            .ports
            .iter()
            .map(|(n, p)| json!({ "name": n, "containerPort": p }))
            .collect();
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
        // Every ztest pod is Guaranteed QoS (§7): render `requests == limits`,
        // explicit `.resources()` winning over the QoS per-pod reserve. The
        // completeness guard below fails the render if neither is present.
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

        // Pod-level pull creds, when the cluster expects them there rather than
        // on the ServiceAccount. The named secret must already exist in the test
        // namespace.
        if let Some(secret) = &self.image_pull_secret {
            spec["imagePullSecrets"] = json!([{ "name": secret }]);
        }

        // Accumulate tolerations (NVMe placement + QoS fast-eviction).
        let mut tolerations: Vec<Value> = Vec::new();

        // NVMe placement (the `sync` tier): the `nodeSelector` keeps the pod off
        // general nodes, the matching toleration lets it past the NVMe taint.
        // `General`/`None` add nothing (default scheduling).
        if let Some(crate::qos::Pool::Nvme) = self.placement {
            spec["nodeSelector"] =
                json!({ crate::qos::NVME_NODE_LABEL_KEY: crate::qos::NVME_NODE_LABEL_VALUE });
            tolerations.push(json!({
                "key": crate::qos::NVME_TAINT_KEY,
                "operator": "Exists",
                "effect": "NoSchedule",
            }));
        }

        // A lost node must kill these pods, not migrate them: a moved pod loses
        // its pinned CPUs and node-local state. Override the auto-added
        // not-ready/unreachable tolerations (default `tolerationSeconds: 300`)
        // to `0` so a lost node deletes the pod immediately.
        if sizing.is_some() {
            for cond in [
                "node.kubernetes.io/not-ready",
                "node.kubernetes.io/unreachable",
            ] {
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

        // Guaranteed-QoS completeness guard (§7), a hard invariant: EVERY
        // ztest-spawned pod must be Guaranteed, else it forfeits static-policy
        // CPU pinning. A `panic!` (not `debug_assert!`) so a violation surfaces
        // in release builds too — never shipped silently.
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
                    // The backend-independent role, so a consumer can select
                    // "the indexer" without knowing whether the profile runs
                    // zainod or lightwalletd (`ztest sync watch` follows it).
                    "ztest.io/component-category": self.category.as_str(),
                    "ztest.io/test": test_name,
                    "ztest.io/component-name": self.pod_name,
                },
            },
            "spec": spec,
        });
        serde_json::from_value(pod).map_err(|e| EnvError::Manifest {
            reason: format!("pod {}: {e}", self.pod_name),
        })
    }
}

/// `true` if every container (regular and init) carries both cpu and memory
/// `limits`: the necessary condition for the whole pod to be Guaranteed QoS and
/// eligible for `static` CPU-manager pinning.
fn pod_is_guaranteed(spec: &Value) -> bool {
    let has_cpu_mem_limits = |c: &Value| {
        let limits = &c["resources"]["limits"];
        limits.get("cpu").is_some() && limits.get("memory").is_some()
    };
    [spec.get("containers"), spec.get("initContainers")]
        .into_iter()
        .flatten()
        .all(|list| {
            list.as_array()
                .map(|cs| cs.iter().all(has_cpu_mem_limits))
                .unwrap_or(true)
        })
}

pub(crate) fn merge_ports(defaults: &[(&str, u16)], extra: &[(String, u16)]) -> Vec<(String, u16)> {
    let mut out: Vec<(String, u16)> = defaults
        .iter()
        .map(|(n, p)| ((*n).to_string(), *p))
        .collect();
    for (n, p) in extra {
        if out.iter().all(|(en, _)| en != n) {
            out.push((n.clone(), *p));
        }
    }
    out
}

/// Map a backend's resolved-image result into the pod image string, tagging any
/// build/resolution failure with the component name for the surfaced
/// [`EnvError`]. Shared by the per-backend `pod_spec` impls, each of which owns
/// its image, ports, ready port, and security context in `backends/*.rs`.
pub(crate) fn resolve_image(
    resolved: Result<backends::image::ResolvedImage, backends::image::ImageError>,
    component: &'static str,
) -> Result<String, EnvError> {
    resolved
        .map(|r| r.image)
        .map_err(|source| EnvError::ImageBuild {
            component: component.into(),
            source,
        })
}

// No pod spec for wallets: wallets run in-process (no pod). See
// `crate::backends::zingo`.

#[cfg(test)]
mod tests {
    use super::*;

    fn coords() -> RunCoords {
        RunCoords {
            run_id: "run".into(),
            user: "user".into(),
        }
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
            // render() requires every pod to be Guaranteed, so the shared
            // fixture carries a reserve; explicit-`.resources()` tests override.
            guaranteed: Some(Resources {
                cpu: "1".into(),
                memory: "536870912".into(),
            }),
            image_pull_secret: None,
            termination_grace_period: None,
        }
    }

    fn container(pod: &Pod) -> Value {
        // Round-trip through JSON so the test reads the shape the API server
        // would receive.
        let v = serde_json::to_value(pod).unwrap();
        v["spec"]["containers"][0].clone()
    }

    #[test]
    fn explicit_resources_render_guaranteed_requests_equal_limits() {
        // An explicit `.resources()` override is Guaranteed too (requests ==
        // limits), never requests-only Burstable.
        let spec = PodSpec {
            resources: Some(Resources {
                cpu: "500m".into(),
                memory: "512Mi".into(),
            }),
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
        // Single guaranteed container → Guaranteed.
        let ok = json!({
            "containers": [{ "resources": { "limits": { "cpu": "2", "memory": "1" } } }],
        });
        assert!(pod_is_guaranteed(&ok));
        // A requests-only container → not Guaranteed (Burstable).
        let burstable = json!({
            "containers": [{ "resources": { "requests": { "cpu": "2", "memory": "1" } } }],
        });
        assert!(!pod_is_guaranteed(&burstable));
        // A guaranteed main container but an init container missing limits:
        // the whole pod drops to Burstable, the regression the render-time
        // guard catches.
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
                cpu: "2".into(), // whole cores → eligible for static CPU pinning
                memory: "2147483648".into(),
            }),
            ..base_spec()
        };
        let pod = spec.render(&coords(), "t", &[]).unwrap();
        let c = container(&pod);
        // requests == limits ⇒ Guaranteed QoS; CPU is an integer core count.
        assert_eq!(c["resources"]["requests"]["cpu"], "2");
        assert_eq!(c["resources"]["limits"]["cpu"], "2");
        assert_eq!(c["resources"]["requests"]["memory"], "2147483648");
        assert_eq!(c["resources"]["limits"]["memory"], "2147483648");

        // Fast-eviction: lost node kills the pod immediately (not after 300s).
        let s = pod_spec_json(&pod);
        let tols = s["tolerations"].as_array().unwrap();
        let nr = tols
            .iter()
            .find(|t| t["key"] == "node.kubernetes.io/not-ready")
            .expect("not-ready toleration");
        assert_eq!(nr["effect"], "NoExecute");
        assert_eq!(nr["tolerationSeconds"], 0);
        assert!(
            tols.iter()
                .any(|t| t["key"] == "node.kubernetes.io/unreachable"),
            "unreachable toleration present: {s}"
        );
        // restartPolicy Never (bare pod ⇒ never rescheduled).
        assert_eq!(s["restartPolicy"], "Never");
    }

    #[test]
    fn explicit_resources_take_precedence_over_guaranteed() {
        // When both are set the explicit `.resources()` override wins; it still
        // renders Guaranteed (requests == limits at the override's values).
        let spec = PodSpec {
            resources: Some(Resources {
                cpu: "750m".into(),
                memory: "1Gi".into(),
            }),
            guaranteed: Some(Resources {
                cpu: "4".into(),
                memory: "8Gi".into(),
            }),
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
        // A pod with neither an explicit override nor a QoS reserve is
        // BestEffort — the render-time guard must reject it in every build.
        let spec = PodSpec {
            guaranteed: None,
            resources: None,
            ..base_spec()
        };
        let _ = spec.render(&coords(), "t", &[]);
    }

    #[test]
    fn supplemental_groups_render_alongside_the_pinned_uid() {
        let spec = PodSpec {
            run_as_user: Some(1000),
            supplemental_groups: vec![0],
            ..base_spec()
        };
        let pod = spec.render(&coords(), "t", &[]).unwrap();
        let sc = &pod_spec_json(&pod)["securityContext"];
        // Both, not either: a pinned uid is what makes the extra group the only
        // way into a seed the pod does not own.
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
            env: vec![
                ("RUST_LOG".into(), "debug".into()),
                ("FOO".into(), "bar".into()),
            ],
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
        // With no env vars the `env` key is omitted. (Resources is always
        // present now — every pod is Guaranteed.)
        let pod = base_spec().render(&coords(), "t", &[]).unwrap();
        let c = container(&pod);
        assert!(c.get("env").is_none());
        assert!(c.get("resources").is_some());
    }

    #[test]
    fn image_pull_secret_renders_only_when_set() {
        // Default (kind mode / public registry): no imagePullSecrets key.
        let s = pod_spec_json(&base_spec().render(&coords(), "t", &[]).unwrap());
        assert!(s.get("imagePullSecrets").is_none());
        // Private-registry mode: the named secret is injected pod-level.
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
        let spec = PodSpec {
            placement: Some(crate::qos::Pool::Nvme),
            ..base_spec()
        };
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
        // A Guaranteed pod (base_spec is one) always carries the fast-eviction
        // NoExecute tolerations; General/None placement must add NEITHER the NVMe
        // nodeSelector NOR the NVMe NoSchedule toleration.
        let no_nvme = |s: &Value| {
            assert!(s.get("nodeSelector").is_none());
            let has_nvme_tol = s["tolerations"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|t| t["effect"] == "NoSchedule" || t["key"] == crate::qos::NVME_TAINT_KEY);
            assert!(!has_nvme_tol, "no NVMe toleration for general/none: {s}");
        };
        // None (default).
        no_nvme(&pod_spec_json(
            &base_spec().render(&coords(), "t", &[]).unwrap(),
        ));
        // Explicit General is also a no-op (schedules anywhere).
        let spec = PodSpec {
            placement: Some(crate::qos::Pool::General),
            ..base_spec()
        };
        no_nvme(&pod_spec_json(&spec.render(&coords(), "t", &[]).unwrap()));
    }
}
