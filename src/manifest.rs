//! Manifest builders for each component variant.

use k8s_openapi::api::core::v1::Pod;
use serde_json::{Value, json};

use crate::EnvError;
use crate::backends;
use crate::component::{ComponentCategory, ComponentOpts, Resources};
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
    /// `.resources()`. Rendered as `requests == limits`, giving k8s
    /// "Guaranteed" QoS.
    pub guaranteed: Option<Resources>,
    /// Container environment variables, in declaration order.
    pub env: Vec<(String, String)>,
    pub fs_group: Option<i64>,
    /// `securityContext.runAsUser` override. Set when a pod must read
    /// another pod's files on a shared volume whose ownership can't be
    /// reconciled via `fsGroup` (hostPath/local-path volumes ignore
    /// `fsGroup`), e.g. a zaino StateService reading the root-owned
    /// zebra-state DB written by the validator. `None` keeps the image
    /// default user.
    pub run_as_user: Option<i64>,
    /// The QoS tier's node placement target, injected by `TestEnv::build()`
    /// when QoS is enabled. `Some(Pool::Nvme)` renders the NVMe nodeSelector
    /// and toleration so a `sync` pod lands on the dedicated NVMe pool;
    /// `General` and `None` schedule anywhere (the default). This is
    /// placement, not sizing; see [`crate::qos::Pool`].
    pub placement: Option<crate::qos::Pool>,
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
        // Explicit `.resources()` (requests only) wins; otherwise the QoS
        // per-pod reserve renders as requests==limits (Guaranteed QoS, §7).
        if let Some(res) = &self.resources {
            container["resources"] = json!({
                "requests": { "cpu": res.cpu, "memory": res.memory },
            });
        } else if let Some(res) = &self.guaranteed {
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
        let mut security_context = serde_json::Map::new();
        if let Some(fs_group) = self.fs_group {
            security_context.insert("fsGroup".into(), json!(fs_group));
        }
        if let Some(uid) = self.run_as_user {
            security_context.insert("runAsUser".into(), json!(uid));
        }
        if !security_context.is_empty() {
            spec["securityContext"] = Value::Object(security_context);
        }

        // Accumulate tolerations (NVMe placement + QoS fast-eviction).
        let mut tolerations: Vec<Value> = Vec::new();

        // NVMe placement (the `sync` tier): pin the pod to the tainted NVMe
        // node pool. `nodeSelector` keeps it off general nodes; the matching
        // toleration lets it past the taint that keeps other tiers off NVMe.
        // `General`/`None` add nothing (default scheduling), so dev/kind
        // clusters (no NVMe pool, QoS disabled) are unaffected.
        if let Some(crate::qos::Pool::Nvme) = self.placement {
            spec["nodeSelector"] =
                json!({ crate::qos::NVME_NODE_LABEL_KEY: crate::qos::NVME_NODE_LABEL_VALUE });
            tolerations.push(json!({
                "key": crate::qos::NVME_TAINT_KEY,
                "operator": "Exists",
                "effect": "NoSchedule",
            }));
        }

        // QoS performance pods (those given a Guaranteed reserve) must be
        // killed, not migrated, when their node goes offline: a moved pod
        // loses its pinned CPUs and any node-local state, so we never want a
        // reschedule. These are bare `Pod`s with `restartPolicy: Never`, so
        // k8s never recreates them; we additionally override the auto-added
        // not-ready/unreachable tolerations (default `tolerationSeconds: 300`)
        // to `0` so a lost node deletes the pod immediately rather than after
        // the 5-minute grace. (Exclusive-CPU pinning itself is the kubelet's
        // `static` CPU-manager policy, which this pod is eligible for by being
        // Guaranteed with integer-core CPU; see `env::even_share`.)
        if self.guaranteed.is_some() {
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

        // Guaranteed-QoS completeness guard (§7). Exclusive static-policy CPU
        // pinning requires the whole pod to be Guaranteed: every container
        // (regular and any init/sidecar) must carry cpu+memory limits; one
        // missing limit silently downgrades the pod to Burstable and forfeits
        // pinning. This pod is single-container today, so the guard holds; it
        // fences a future init/sidecar addition that forgets its limits. Skip
        // it when an explicit `.resources()` override is in effect (that path
        // is intentionally requests-only / Burstable, see above).
        debug_assert!(
            self.guaranteed.is_none() || self.resources.is_some() || pod_is_guaranteed(&spec),
            "QoS Guaranteed pod {} has a container without cpu+memory limits — \
             would downgrade to Burstable and lose CPU pinning",
            self.pod_name,
        );

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": self.pod_name,
                "labels": {
                    "zaino.io/run-id": coords.run_id,
                    "zaino.io/component": component_label,
                    "zaino.io/test": test_name,
                    "zaino.io/component-name": self.pod_name,
                },
            },
            "spec": spec,
        });
        serde_json::from_value(pod).map_err(|e| EnvError::Manifest {
            reason: format!("pod {}: {e}", self.pod_name),
        })
    }
}

/// `true` if every container (regular and init) in a pod spec carries both
/// cpu and memory `limits`: the necessary condition for the whole pod to be
/// Guaranteed QoS and thus eligible for the kubelet's `static` CPU-manager
/// pinning. Containers without a resources block fail the check.
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

fn merge_ports(defaults: &[(&str, u16)], extra: &[(String, u16)]) -> Vec<(String, u16)> {
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

pub fn pod_spec_for_validator(
    label: &'static str,
    opts: &ComponentOpts,
    pod_name: String,
) -> PodSpec {
    match label {
        "zebrad" => PodSpec {
            pod_name,
            category: ComponentCategory::Validator,
            label,
            image: backends::zebra::image_uri(&opts.version),
            ports: merge_ports(
                &[
                    ("rpc", crate::handles::ports::ZEBRAD_RPC),
                    ("metrics", crate::handles::ports::ZEBRAD_METRICS),
                    ("p2p", crate::handles::ports::ZEBRAD_P2P),
                ],
                &opts.extra_ports,
            ),
            ready_port: crate::handles::ports::ZEBRAD_RPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources.clone(),
            env: opts.env.clone(),
            // When sharing its zebra-state DB, run zebrad as the same uid
            // (1000) the zaino reader uses, so the DB files it writes
            // (including the mode-0600 `version` file) are owned by 1000
            // and readable by the colocated StateService that opens them
            // as a secondary. fsGroup is ineffective here: hostPath /
            // local-path volumes ignore it, and the zainod image refuses
            // to run as root, so matching uids is the portable fix.
            fs_group: opts.shared_state.as_ref().map(|_| 1000),
            run_as_user: opts.shared_state.as_ref().map(|_| 1000),
            placement: None,
            guaranteed: None,
        },
        "zcashd" => PodSpec {
            pod_name,
            category: ComponentCategory::Validator,
            label,
            image: backends::zcashd::image_uri(&opts.version),
            ports: merge_ports(
                &[("rpc", crate::handles::ports::ZCASHD_RPC)],
                &opts.extra_ports,
            ),
            ready_port: crate::handles::ports::ZCASHD_RPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources.clone(),
            env: opts.env.clone(),
            fs_group: Some(2001),
            run_as_user: None,
            placement: None,
            guaranteed: None,
        },
        other => panic!("pod_spec_for_validator: unknown validator backend label {other:?}"),
    }
}

pub fn pod_spec_for_indexer(
    label: &'static str,
    opts: &ComponentOpts,
    pod_name: String,
) -> Result<PodSpec, EnvError> {
    match label {
        "zainod" => {
            let resolved =
                backends::zainod::image_uri(opts).map_err(|source| EnvError::ImageBuild {
                    component: "zainod".into(),
                    source,
                })?;
            Ok(PodSpec {
                pod_name,
                category: ComponentCategory::Indexer,
                label,
                image: resolved.image,
                ports: merge_ports(
                    &[
                        ("grpc", crate::handles::ports::ZAINO_GRPC),
                        ("jsonrpc", crate::handles::ports::ZAINO_JSONRPC),
                        ("metrics", crate::handles::ports::ZAINO_METRICS),
                    ],
                    &opts.extra_ports,
                ),
                ready_port: crate::handles::ports::ZAINO_GRPC,
                command: opts.command.clone(),
                args: opts.args.clone(),
                resources: opts.resources.clone(),
                env: opts.env.clone(),
                fs_group: Some(1000),
                // The zainod image refuses to run as root and defaults to
                // uid 1000. For the shared-DB case the validator is pinned
                // to the same uid (see the zebrad arm) so this reader owns
                // the files it must read.
                run_as_user: None,
                placement: None,
                guaranteed: None,
            })
        }
        "lightwalletd" => Ok(PodSpec {
            pod_name,
            category: ComponentCategory::Indexer,
            label,
            image: backends::lightwalletd::image_uri(&opts.version),
            ports: merge_ports(
                &[("grpc", crate::handles::ports::LIGHTWALLETD_GRPC)],
                &opts.extra_ports,
            ),
            ready_port: crate::handles::ports::LIGHTWALLETD_GRPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources.clone(),
            env: opts.env.clone(),
            fs_group: Some(1000),
            run_as_user: None,
            placement: None,
            guaranteed: None,
        }),
        other => panic!("pod_spec_for_indexer: unknown indexer backend label {other:?}"),
    }
}

// No `pod_spec_for_wallet`: wallets run in-process (no pod). See
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
            placement: None,
            guaranteed: None,
        }
    }

    fn container(pod: &Pod) -> Value {
        // Round-trip through JSON so the test reads the rendered shape
        // exactly as the API server would receive it.
        let v = serde_json::to_value(pod).unwrap();
        v["spec"]["containers"][0].clone()
    }

    #[test]
    fn resources_render_into_requests() {
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
        // A test that called `.resources()` keeps requests-only (no limits);
        // env.rs leaves `guaranteed` unset in that case, but assert the render
        // precedence directly too.
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
        assert!(
            c["resources"].get("limits").is_none(),
            "explicit ⇒ requests only"
        );
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
    fn no_resources_or_env_omits_the_keys() {
        let pod = base_spec().render(&coords(), "t", &[]).unwrap();
        let c = container(&pod);
        assert!(c.get("resources").is_none());
        assert!(c.get("env").is_none());
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
    fn general_and_none_placement_omit_scheduling_keys() {
        // None (default).
        let s = pod_spec_json(&base_spec().render(&coords(), "t", &[]).unwrap());
        assert!(s.get("nodeSelector").is_none());
        assert!(s.get("tolerations").is_none());
        // Explicit General is also a no-op (schedules anywhere).
        let spec = PodSpec {
            placement: Some(crate::qos::Pool::General),
            ..base_spec()
        };
        let s = pod_spec_json(&spec.render(&coords(), "t", &[]).unwrap());
        assert!(s.get("nodeSelector").is_none());
        assert!(s.get("tolerations").is_none());
    }
}
