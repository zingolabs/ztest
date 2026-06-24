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
    /// Container resource requests, rendered into `resources.requests`.
    pub resources: Option<Resources>,
    /// Container environment variables, in declaration order.
    pub env: Vec<(String, String)>,
    pub fs_group: Option<i64>,
    /// `securityContext.runAsUser` override. Set when a pod must read
    /// another pod's files on a shared volume whose ownership can't be
    /// reconciled via `fsGroup` (hostPath/local-path volumes ignore
    /// `fsGroup`) — e.g. a zaino StateService reading the root-owned
    /// zebra-state DB written by the validator. `None` keeps the image
    /// default user.
    pub run_as_user: Option<i64>,
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
        if let Some(res) = &self.resources {
            container["resources"] = json!({
                "requests": { "cpu": res.cpu, "memory": res.memory },
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
            // (1000) the zaino reader uses, so the DB files it writes —
            // including the mode-0600 `version` file — are owned by 1000
            // and readable by the colocated StateService that opens them
            // as a secondary. fsGroup is ineffective here: hostPath /
            // local-path volumes ignore it, and the zainod image refuses
            // to run as root, so matching uids is the portable fix.
            fs_group: opts.shared_state.as_ref().map(|_| 1000),
            run_as_user: opts.shared_state.as_ref().map(|_| 1000),
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
}
