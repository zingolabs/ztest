//! Manifest builders for each component variant.
//!
//! Everything we hand to Kubernetes is plain JSON via `serde_json::json!`
//! — readable, and we lean on the API server to validate it. Reasoning
//! about pod state lives in k8s, not here: `restartPolicy: Never` plus a
//! `readinessProbe` is enough to express "boot it; tell me when it's
//! ready; if it dies, surface the exit".

use k8s_openapi::api::core::v1::Pod;
use serde_json::{Value, json};

use crate::component::ComponentKind;
use crate::handles::backends;
use crate::handles::indexer::IndexerKind;
use crate::handles::validator::ValidatorKind;
use crate::handles::wallet::WalletKind;
use crate::mounts::ResolvedMount;
use crate::naming::RunCoords;
use crate::{ComponentOpts, EnvError, Indexer, Validator, Wallet};

/// What we need to apply a pod for a single component.
///
/// The pod name doubles as the DNS short-name inside the test's
/// namespace — `create_pod_service` makes a same-named ClusterIP
/// Service that routes to this pod via the `zaino.io/component-name`
/// label.
#[derive(Debug, Clone)]
pub struct PodSpec {
    pub pod_name: String,
    /// Typed backend kind. Also the source of the k8s component label
    /// (via `ComponentKind::as_label`).
    pub kind: ComponentKind,
    pub image: String,
    pub ports: Vec<(String, u16)>, // named ports
    pub ready_port: u16,           // the port the TCP readinessProbe hits
    /// Container `command` override. `None` lets the upstream image's
    /// ENTRYPOINT run.
    pub command: Option<Vec<String>>,
    /// Container `args` override.
    pub args: Option<Vec<String>>,
    /// Pod-level `securityContext.fsGroup`. Required when the pod
    /// carries a [`crate::MountKind::Scratch`] mount: kubelet `chown`s
    /// the emptyDir's mount root to this gid and sets `g+rwxs` so the
    /// container's uid can write under it. The image's expected
    /// runtime gid (e.g. 1000 for zaino).
    pub fs_group: Option<i64>,
}

impl PodSpec {
    /// Render the Pod manifest as JSON. Returns `EnvError::Manifest` if
    /// the synthesized JSON ever fails to deserialize — in practice this
    /// is unreachable for a well-formed `PodSpec`, but it's typed input
    /// (mount volume/volumeMount JSON is built from user-controlled
    /// paths) so we propagate rather than panic.
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
        let component_label = self.kind.as_label();

        let mut container = json!({
            "name": component_label,
            "image": self.image,
            "ports": ports_json,
            "volumeMounts": volume_mounts,
            "readinessProbe": {
                // Let k8s decide "ready" via TCP — no logic on our side.
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

        let mut spec = json!({
            // Disable legacy Docker-link env var injection.
            "enableServiceLinks": false,
            "restartPolicy": "Never",
            "volumes": volumes,
            "containers": [container],
        });
        if let Some(fs_group) = self.fs_group {
            spec["securityContext"] = json!({ "fsGroup": fs_group });
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
                    // Same-named ClusterIP Service selects pods by this
                    // label — see `cluster::create_pod_service`.
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

// ───────── per-variant tables ─────────

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

pub fn pod_spec_for_validator(v: &Validator, pod_name: String) -> PodSpec {
    let opts: &ComponentOpts = match v {
        Validator::Zebrad(o) => &o.0,
        Validator::Zcashd(o) => &o.0,
    };
    match v {
        Validator::Zebrad(_) => PodSpec {
            pod_name,
            kind: ComponentKind::Validator(ValidatorKind::Zebrad),
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
            fs_group: None,
        },
        Validator::Zcashd(_) => PodSpec {
            pod_name,
            kind: ComponentKind::Validator(ValidatorKind::Zcashd),
            image: backends::zcashd::image_uri(&opts.version),
            ports: merge_ports(
                &[("rpc", crate::handles::ports::ZCASHD_RPC)],
                &opts.extra_ports,
            ),
            ready_port: crate::handles::ports::ZCASHD_RPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            // `electriccoinco/zcashd` runs as the unprivileged `zcash`
            // user (uid 2001 in historical builds). Setting `fsGroup`
            // makes the scratch emptyDir at `-datadir` writable to that
            // user without requiring `runAsUser` overrides.
            fs_group: Some(2001),
        },
    }
}

pub fn pod_spec_for_indexer(i: &Indexer, pod_name: String) -> Result<PodSpec, EnvError> {
    let Indexer::Zainod(o) = i;
    let resolved = backends::zainod::image_uri(&o.opts).map_err(|source| EnvError::ImageBuild {
        component: "zaino".into(),
        source,
    })?;
    Ok(PodSpec {
        pod_name,
        kind: ComponentKind::Indexer(IndexerKind::Zainod),
        image: resolved.image,
        ports: merge_ports(
            &[
                ("grpc", crate::handles::ports::ZAINO_GRPC),
                ("jsonrpc", crate::handles::ports::ZAINO_JSONRPC),
                ("metrics", crate::handles::ports::ZAINO_METRICS),
            ],
            &o.opts.extra_ports,
        ),
        ready_port: crate::handles::ports::ZAINO_GRPC,
        command: o.opts.command.clone(),
        args: o.opts.args.clone(),
        // The zaino image runs as uid/gid 1000. Any scratch mount on
        // this pod (e.g. the regtest `/var/lib/zaino` emptyDir) needs
        // `fsGroup=1000` so the container can write to the volume root.
        fs_group: Some(1000),
    })
}

pub fn pod_spec_for_wallet(w: &Wallet, pod_name: String) -> PodSpec {
    let Wallet::Zingo(o) = w;
    PodSpec {
        pod_name,
        kind: ComponentKind::Wallet(WalletKind::Zingo),
        image: backends::zingo::image_uri(&o.0.version),
        ports: merge_ports(
            &[("grpc", crate::handles::ports::ZINGO_GRPC)],
            &o.0.extra_ports,
        ),
        ready_port: crate::handles::ports::ZINGO_GRPC,
        command: o.0.command.clone(),
        args: o.0.args.clone(),
        fs_group: None,
    }
}
