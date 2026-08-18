//! What a cluster must provide for ztest, and whether it does.
//!
//! - Every probe = a read: missing → reported with a remedy, never repaired
//!   (installing infra from a harness needs cluster-admin and puts a CI job's
//!   blast radius around the whole cluster)
//! - Probe capabilities, never platforms — brand is not a capability, and same-brand
//!   clusters differ in what they can do
//! - Operator-provided only; the BuildKit builder is ztest's own ephemeral pod, so
//!   its absence means "run `ztest cluster setup`", not "unsuitable cluster"

use k8s_openapi::api::core::v1::Service;
use kube::Client;
use kube::api::{Api, ListParams};

/// Set by the Pyroscope Helm chart = the only name-independent way to find an
/// operator-installed service
const NAME_LABEL: &str = "app.kubernetes.io/name";

/// Bucket round trip's budget. Generous enough for a cold TLS handshake to R2, short
/// enough that a wrong endpoint reports rather than hangs `check`
const BUCKET_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// What a missing capability costs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    Required,
    Enables(&'static str),
}

/// One probe's outcome. `Unknown` stays out of `Absent` — a forbidden read means
/// *this caller* cannot see it, and folding them blames a healthy cluster
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    Present(String),
    Absent(String),
    Unknown(String),
}

impl Finding {
    pub fn is_present(&self) -> bool {
        matches!(self, Finding::Present(_))
    }

    pub fn detail(&self) -> &str {
        match self {
            Finding::Present(d) | Finding::Absent(d) | Finding::Unknown(d) => d,
        }
    }
}

/// One cluster facility ztest depends on but does not provide. `remedy` shows only
/// when absent
#[derive(Debug, Clone)]
pub struct Capability {
    pub name: &'static str,
    pub need: Need,
    pub finding: Finding,
    pub remedy: &'static str,
}

impl Capability {
    /// Blocks `ztest run` outright. `Unknown` blocks a *required* capability too
    /// (refusing now beats an obscure failure twenty minutes in)
    pub fn is_blocking(&self) -> bool {
        self.need == Need::Required && !self.finding.is_present()
    }
}

/// Everything probed, in report order
#[derive(Debug, Clone)]
pub struct Report {
    pub capabilities: Vec<Capability>,
}

impl Report {
    pub fn blocking(&self) -> impl Iterator<Item = &Capability> {
        self.capabilities.iter().filter(|c| c.is_blocking())
    }

    pub fn is_runnable(&self) -> bool {
        self.blocking().next().is_none()
    }
}

/// Probe every capability, concurrently (independent reads; a preflight costing
/// the sum of its round trips gets skipped)
pub async fn probe(client: &Client) -> Report {
    let (storage, snapshots, metrics, bucket, profiling) = tokio::join!(
        probe_storage(client),
        probe_api(client, "snapshot.storage.k8s.io/v1", "VolumeSnapshot"),
        probe_metrics(client),
        probe_bucket(),
        probe_profiling(client),
    );

    Report {
        capabilities: vec![
            Capability {
                name: "snapshot-capable storage",
                need: Need::Required,
                finding: storage,
                remedy: "docs/ops-cluster-requirements.md#storage",
            },
            Capability {
                name: "VolumeSnapshot v1 API",
                need: Need::Required,
                finding: snapshots,
                remedy: "docs/ops-cluster-requirements.md#storage",
            },
            Capability {
                name: "metrics",
                need: Need::Enables("metrics & profiling"),
                finding: metrics,
                remedy: "run `ztest cluster setup` (docs/ops-cluster-requirements.md#metrics)",
            },
            Capability {
                name: "profile collector",
                need: Need::Enables("CPU profiles (ztest sync perf)"),
                finding: profiling,
                remedy: "nested kubelet profiles host-side: needs a reachable docker daemon \
                         (`--profile=false` runs without it)",
            },
            Capability {
                name: "image registry",
                need: Need::Enables("dev! images"),
                finding: probe_registry(),
                remedy: "docs/ops-cluster-requirements.md#registry",
            },
            Capability {
                name: "snapshot bucket",
                need: Need::Enables("chain fixtures (.mainnet/.testnet)"),
                finding: bucket,
                remedy: "export AWS_* or write ~/.config/ztest/bucket.toml \
                         (fixtures/chains/README.md#environment)",
            },
        ],
    }
}

/// The bucket every chain fixture's bytes come from, resolved *and* reached.
///
/// - Not a cluster facility, like [`probe_registry`] — but a green cluster with an
///   unreachable bucket still fails every fixture-mounting profile, from a subsystem
///   `check` would otherwise never mention
/// - Round trip, not just credential resolution: a stale key resolves fine and fails at
///   the first seed, half an hour into a run
/// - `Unknown` on a reachable-but-refused read: the credentials may be scoped to `GET`
///   on `lfs/*`, which is enough for seeding and not enough to list
async fn probe_bucket() -> Finding {
    let bucket = match crate::storage::r2::Bucket::resolve() {
        Ok(b) => b,
        Err(why) => return Finding::Absent(why.to_string()),
    };
    let source = crate::storage::r2::credentials_source();
    match bucket.reachable(BUCKET_PROBE_TIMEOUT).await {
        Ok(()) => Finding::Present(format!("reachable · {source}")),
        Err(why) => Finding::Unknown(format!("{source}: {why}")),
    }
}

/// StorageClass whose provisioner has a VolumeSnapshotClass = precondition for
/// every seeded test's CoW clone
async fn probe_storage(client: &Client) -> Finding {
    let driver = crate::cluster_config::active_storage_driver();
    match crate::resource::discover_storage(client).await {
        // Resolved exactly as a run resolves it, driver and all — reporting the
        // cluster default under a profile-named driver = green check, failing run.
        Ok(options) => match crate::resource::select_storage(&options, driver.as_deref()) {
            Ok(chosen) => {
                Finding::Present(format!("{} ({})", chosen.class_name, chosen.provisioner))
            }
            Err(why) => Finding::Absent(why),
        },
        // `discover_storage` fails only on a failed list, indistinguishable from
        // a cluster that has nothing.
        Err(why) => Finding::Unknown(why),
    }
}

/// Does the API server serve `kind` at `api_version`? Asked of discovery, not by
/// listing (which conflates empty, unserved and forbidden)
async fn probe_api(client: &Client, api_version: &str, kind: &str) -> Finding {
    match client.list_api_group_resources(api_version).await {
        Ok(list) if list.resources.iter().any(|r| r.kind == kind) => {
            Finding::Present(api_version.to_string())
        }
        Ok(_) => Finding::Absent(format!("{api_version} is served but has no {kind}")),
        Err(e) if is_not_found(&e) => Finding::Absent(format!("{api_version} is not served")),
        Err(e) => Finding::Unknown(format!("querying {api_version}: {e}")),
    }
}

/// Where `dev!` images are pushed to and pulled from.
///
/// - Config, not a cluster read (push reachability says nothing about pull, which
///   only the kubelet resolves; an unauthed probe of a private registry fails anyway)
/// - Catches the failure that does happen: a registry-less profile whose builds die
///   at push time
fn probe_registry() -> Finding {
    // `kind load` side-loads into the node — no registry in the path, so "none
    // configured" is correct rather than missing.
    if crate::backends::image::selected_class() == crate::cluster_config::ClusterClass::Local {
        return Finding::Present("kind load (no registry)".to_string());
    }
    match crate::cluster_config::active_registry() {
        (Some(push), Some(pull)) if push == pull => Finding::Present(push),
        (Some(push), Some(pull)) => Finding::Present(format!("push {push} / pull {pull}")),
        // Pull-only = published images only: nothing builds, pods still start.
        (None, Some(pull)) => Finding::Absent(format!("pull-only ({pull}); no push address")),
        (Some(push), None) => Finding::Absent(format!("push-only ({push}); no pull address")),
        (None, None) => Finding::Absent("no registry configured".to_string()),
    }
}

/// Where a collector can run for this cluster, and whether that placement's prerequisites hold.
///
/// - Placement is not a preference: a nested kubelet numbers pods below the pid namespace eBPF
///   reports in, so kind can only be profiled from the host
/// - Host placement leans on the *workstation* — docker, a kubeconfig, a routable node — none
///   of which the cluster can vouch for, so they are probed here and not discovered at launch
/// - Read-only: promoting the Pyroscope Service to NodePort is a launch-time act, not a check
async fn probe_profiling(client: &Client) -> Finding {
    use crate::profiling::ebpf::{Placement, placement_for};

    if placement_for(client).await == Placement::Sidecar {
        return Finding::Present("driver-pod sidecar".to_string());
    }
    let node_ip = crate::profiling::node_internal_ip(client).await;
    let mut missing = Vec::new();
    if !docker_usable() {
        missing.push("a reachable docker daemon");
    }
    // The address the collector actually dials, not the kubeconfig's: a nested cluster's
    // kubeconfig points at loopback, which is dead once the collector leaves the host network
    if crate::profiling::node_api_server(client).await.is_none() {
        missing.push("an apiserver address on the cluster network");
    }
    if crate::profiling::host::cluster_network().await.is_none() {
        missing.push("the cluster's docker network");
    }
    if node_ip.is_none() {
        missing.push("a node InternalIP to push to");
    }
    match missing.as_slice() {
        [] => Finding::Present(format!(
            "host-side, nested kubelet · pushes to {}",
            node_ip.unwrap_or_default()
        )),
        _ => Finding::Absent(format!("host-side profiling needs {}", missing.join(", "))),
    }
}

/// Daemon reachable, not just a client on `PATH` (a `docker` binary with no daemon behind it
/// fails at `sync start`, long after `check` said yes)
fn docker_usable() -> bool {
    std::process::Command::new("docker")
        .args(["version", "--format", "{{.Server.Version}}"])
        .output()
        .is_ok_and(|out| out.status.success())
}

/// Metrics stack as one capability — provisioned as one, and a partial install has
/// the same fix as a missing one.
///
/// Found by `app.kubernetes.io/name`, not a fixed address, so an operator's own
/// install (foreign namespace, foreign release name) is adopted not duplicated
async fn probe_metrics(client: &Client) -> Finding {
    let api: Api<Service> = Api::all(client.clone());
    let mut found = Vec::new();
    let mut missing = Vec::new();
    for component in ["prometheus", "pyroscope", "grafana"] {
        let selector = format!("{NAME_LABEL}={component}");
        match api.list(&ListParams::default().labels(&selector)).await {
            Ok(list) => match list.items.first() {
                Some(svc) => {
                    let ns = svc.metadata.namespace.as_deref().unwrap_or("?");
                    let name = svc.metadata.name.as_deref().unwrap_or("?");
                    found.push(format!("{name}.{ns}"));
                }
                None => missing.push(component),
            },
            // One unreadable list ⇒ whole answer unknown, else an operator is sent
            // to install what is already there.
            Err(e) => return Finding::Unknown(format!("listing Services: {e}")),
        }
    }
    match missing.as_slice() {
        [] => Finding::Present(found.join(", ")),
        _ if found.is_empty() => Finding::Absent("not installed".to_string()),
        _ => Finding::Absent(format!("missing {}", missing.join(", "))),
    }
}

/// Discovery 404 = group/version not served
fn is_not_found(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(e) if e.code == 404)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap(name: &'static str, need: Need, finding: Finding) -> Capability {
        Capability { name, need, finding, remedy: "docs/ops-cluster-requirements.md" }
    }

    /// Profiling is a diagnostic: a workstation without docker must still be able to run
    /// tests, so an absent collector reports and never blocks
    #[test]
    fn an_unprofilable_cluster_is_still_runnable() {
        let report = Report {
            capabilities: vec![
                cap("snapshot-capable storage", Need::Required, Finding::Present("csi".into())),
                cap(
                    "profile collector",
                    Need::Enables("CPU profiles (ztest sync perf)"),
                    Finding::Absent("host-side profiling needs a reachable docker daemon".into()),
                ),
            ],
        };
        assert!(report.is_runnable());
        assert_eq!(report.blocking().count(), 0);
    }

    /// Module's founding distinction — forbidden read != absent capability, and the
    /// two send an operator to different places
    #[test]
    fn an_unreadable_capability_is_not_an_absent_one() {
        let unknown = Finding::Unknown("forbidden".into());
        let absent = Finding::Absent("nothing installed".into());
        assert_ne!(unknown, absent);
        assert!(!unknown.is_present() && !absent.is_present());
    }

    /// Unreadable required capability blocks (else a clear refusal now becomes an
    /// obscure failure deep into a test wave)
    #[test]
    fn an_unreadable_required_capability_blocks() {
        let c =
            cap("snapshot-capable storage", Need::Required, Finding::Unknown("forbidden".into()));
        assert!(c.is_blocking());
    }

    /// Optional never blocks, however it failed (no-metrics run must still run)
    #[test]
    fn a_missing_optional_capability_degrades_rather_than_blocks() {
        for finding in
            [Finding::Absent("not installed".into()), Finding::Unknown("forbidden".into())]
        {
            let c = cap("PodMonitor CRD", Need::Enables("metrics"), finding);
            assert!(!c.is_blocking());
        }
    }

    #[test]
    fn a_report_names_what_blocks_and_what_is_merely_degraded() {
        let report = Report {
            capabilities: vec![
                cap("snapshot-capable storage", Need::Required, Finding::Absent("none".into())),
                cap("PodMonitor CRD", Need::Enables("metrics"), Finding::Absent("none".into())),
                cap(
                    "image registry",
                    Need::Enables("dev! images"),
                    Finding::Present("registry.example/ztest".into()),
                ),
            ],
        };

        assert!(!report.is_runnable());
        let blocking: Vec<_> = report.blocking().map(|c| c.name).collect();
        assert_eq!(blocking, vec!["snapshot-capable storage"]);
    }

    #[test]
    fn a_capable_cluster_is_runnable() {
        let report = Report {
            capabilities: vec![
                cap(
                    "snapshot-capable storage",
                    Need::Required,
                    Finding::Present("ceph-rbd".into()),
                ),
                cap(
                    "PodMonitor CRD",
                    Need::Enables("metrics"),
                    Finding::Present("monitoring.coreos.com/v1".into()),
                ),
            ],
        };
        assert!(report.is_runnable());
    }
}
