//! CPU profiles, via Pyroscope. Component contract in `docs/how-to-profile.md`.
//!
//! - Components push; ztest queries the merged result back as pprof
//! - No volume, no pod collection → a profile outlives the component, its namespace
//!   and an OOM kill, and reads mid-run

use std::time::{SystemTime, UNIX_EPOCH};

use k8s_openapi::api::core::v1::{Pod, Service};
use kube::Client;
use kube::api::{Api, ListParams};
use prost::Message as _;

use crate::portforward::Forwarder;

/// Push endpoint for a profiled component; presence = the runtime switch
pub(crate) const PROFILE_URL_ENV: &str = "ZTEST_PROFILE_URL";
/// Comma-separated `key=value`, attached to every pushed profile
pub(crate) const PROFILE_TAGS_ENV: &str = "ZTEST_PROFILE_TAGS";
/// Sample rate, Hz
pub(crate) const PROFILE_HZ_ENV: &str = "ZTEST_PROFILE_HZ";

/// Locates an install ztest did not create (operator's lives wherever they put it;
/// every Pyroscope deployment carries this label)
const PYROSCOPE_LABEL: &str = "app.kubernetes.io/name=pyroscope";

/// Profile type `pyroscope-rs` pushes CPU samples under
const CPU_PROFILE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

const SELECT_MERGE_STACKTRACES: &str = "/querier.v1.QuerierService/SelectMergeStacktraces";

/// `ProfileFormat::ProfileFormatPprof`. Mandatory in effect — unspecified yields
/// a flamegraph, not a pprof
const PROFILE_FORMAT_PPROF: i32 = 4;

/// Hand-declared, not generated (a protoc pipeline for three messages costs more
/// than it saves). `SelectMergeProfile` is upstream-deprecated in favour of this.
#[derive(Clone, PartialEq, prost::Message)]
struct SelectMergeStacktracesRequest {
    #[prost(string, tag = "1")]
    profile_type_id: String,
    #[prost(string, tag = "2")]
    label_selector: String,
    #[prost(int64, tag = "3")]
    start: i64,
    #[prost(int64, tag = "4")]
    end: i64,
    #[prost(int32, tag = "6")]
    format: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SelectMergeStacktracesResponse {
    #[prost(message, optional, tag = "5")]
    pprof: Option<PprofProfile>,
}

/// Upstream `google.v1.Profile`, taken as `bytes` — its raw encoding *is* pprof,
/// and both share a wire type, so the decode is faithful and the schema stays out
#[derive(Clone, PartialEq, prost::Message)]
struct PprofProfile {
    #[prost(bytes = "vec", tag = "1")]
    profile: Vec<u8>,
}

pub(crate) fn pod_env(
    url: &str,
    tags: &[(&str, String)],
    hz: Option<u32>,
) -> Vec<(String, String)> {
    let mut env = vec![(PROFILE_URL_ENV.to_string(), url.to_string())];
    if !tags.is_empty() {
        let rendered = tags.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(",");
        env.push((PROFILE_TAGS_ENV.to_string(), rendered));
    }
    if let Some(hz) = hz {
        env.push((PROFILE_HZ_ENV.to_string(), hz.to_string()));
    }
    env
}

/// `ztest cluster setup`'s Pyroscope Service, else an operator's. Known address first
/// (skips a cluster-wide list, and stays deterministic where two exist)
async fn pyroscope_service(client: &Client) -> Option<Service> {
    let owned: Api<Service> = Api::namespaced(client.clone(), crate::resource::OBS_NAMESPACE);
    if let Ok(svc) = owned.get(crate::resource::PYROSCOPE_SERVICE).await {
        return Some(svc);
    }
    let all: Api<Service> = Api::all(client.clone());
    all.list(&ListParams::default().labels(PYROSCOPE_LABEL)).await.ok()?.items.into_iter().next()
}

/// In-cluster (`.svc`) address — the pushers are pods
pub(crate) async fn push_url(client: &Client) -> Option<String> {
    let svc = pyroscope_service(client).await?;
    let port = service_port(&svc);
    let name = svc.metadata.name?;
    let namespace = svc.metadata.namespace?;
    Some(format!("http://{name}.{namespace}.svc:{port}"))
}

/// Service's first declared port, else Pyroscope's usual one
fn service_port(svc: &Service) -> u16 {
    svc.spec
        .as_ref()
        .and_then(|s| s.ports.as_ref())
        .and_then(|ports| ports.first())
        .map(|p| p.port as u16)
        .unwrap_or(crate::resource::PYROSCOPE_PORT)
}

/// Merged CPU profile for `selector` over `[from, to]`, as pprof bytes
pub(crate) async fn fetch(
    client: &Client,
    selector: &str,
    from: SystemTime,
    to: SystemTime,
) -> Result<Vec<u8>, String> {
    let (namespace, pod, port) = pyroscope_backend(client).await?;
    let fwd = Forwarder::start(client.clone(), namespace, pod, port)
        .await
        .map_err(|e| format!("port-forward to Pyroscope: {e}"))?;

    let body = SelectMergeStacktracesRequest {
        profile_type_id: CPU_PROFILE.to_string(),
        label_selector: selector.to_string(),
        start: epoch_millis(from),
        end: epoch_millis(to),
        format: PROFILE_FORMAT_PPROF,
    }
    .encode_to_vec();

    let url = format!("http://127.0.0.1:{}{SELECT_MERGE_STACKTRACES}", fwd.local_port);
    let response = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/proto")
        .header("Connect-Protocol-Version", "1")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("querying Pyroscope: {e}"))?;

    let status = response.status();
    let bytes = response.bytes().await.map_err(|e| format!("reading profile: {e}"))?;
    if !status.is_success() {
        return Err(format!(
            "Pyroscope returned {status}: {}",
            String::from_utf8_lossy(&bytes).trim()
        ));
    }

    let profile = SelectMergeStacktracesResponse::decode(&bytes[..])
        .map_err(|e| format!("decoding Pyroscope response: {e}"))?
        .pprof
        .map(|p| p.profile)
        .unwrap_or_default();
    // Empty = a successful query that matched nothing, a different problem from
    // a failed one.
    if profile.is_empty() {
        return Err(format!("no profile matched {selector} in this window"));
    }
    Ok(profile)
}

/// A pod backing the Pyroscope Service + its port.
///
/// - Resolved through the Service's own selector, never the chart label on pods
/// - Microservices mode: that label also matches ingesters/distributors, and a
///   query landing on one never reaches the querier
async fn pyroscope_backend(client: &Client) -> Result<(String, String, u16), String> {
    let svc = pyroscope_service(client)
        .await
        .ok_or_else(|| "no Pyroscope Service in this cluster".to_string())?;
    let namespace = svc
        .metadata
        .namespace
        .clone()
        .ok_or_else(|| "Pyroscope Service has no namespace".to_string())?;
    let port = service_port(&svc);
    let selector = svc
        .spec
        .as_ref()
        .and_then(|s| s.selector.as_ref())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Pyroscope Service selects no pods".to_string())?
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");

    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let list = pods
        .list(&ListParams::default().labels(&selector))
        .await
        .map_err(|e| format!("listing Pyroscope pods: {e}"))?;
    list.items
        .into_iter()
        .find(pod_is_ready)
        .and_then(|p| p.metadata.name)
        .map(|name| (namespace, name, port))
        .ok_or_else(|| "no ready Pyroscope pod".to_string())
}

/// `Ready`, not `Running` — a started-but-unprobed pod refuses queries, turning a
/// working install into a connection error
fn pod_is_ready(p: &Pod) -> bool {
    p.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
}

fn epoch_millis(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Pyroscope selector for one component of one run. Namespace-scoped, not run-id
/// (what the component was tagged with, derivable from a sync id without a lookup)
pub(crate) fn selector(component: &str, namespace: &str) -> String {
    format!(r#"{{component="{component}",namespace="{namespace}"}}"#)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_render_as_comma_separated_pairs() {
        let env = pod_env(
            "http://pyroscope.obs.svc:4040",
            &[("sync_id", "abc".into()), ("component", "zainod".into())],
            Some(100),
        );
        let get = |k: &str| env.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str()).unwrap();
        assert_eq!(get(PROFILE_URL_ENV), "http://pyroscope.obs.svc:4040");
        assert_eq!(get(PROFILE_TAGS_ENV), "sync_id=abc,component=zainod");
        assert_eq!(get(PROFILE_HZ_ENV), "100");
    }

    /// URL alone = the switch; no tags, no rate override, still profiles
    #[test]
    fn the_url_is_the_only_required_variable() {
        let env = pod_env("http://p:4040", &[], None);
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].0, PROFILE_URL_ENV);
    }

    /// Both pinned, else two concurrent runs merge samples into one meaningless graph
    #[test]
    fn a_selector_scopes_to_one_component_of_one_run() {
        assert_eq!(
            selector("zainod", "ztest-sync-abc"),
            r#"{component="zainod",namespace="ztest-sync-abc"}"#
        );
    }

    /// Protobuf-encoded request required — a JSON one gets a non-pprof response
    #[test]
    fn the_request_encodes_every_field() {
        let encoded = SelectMergeStacktracesRequest {
            profile_type_id: CPU_PROFILE.to_string(),
            label_selector: r#"{service_name="zainod"}"#.to_string(),
            start: 1_700_000_000_000,
            end: 1_700_000_060_000,
            format: PROFILE_FORMAT_PPROF,
        }
        .encode_to_vec();
        let decoded = SelectMergeStacktracesRequest::decode(&encoded[..]).expect("round trip");
        assert_eq!(decoded.profile_type_id, CPU_PROFILE);
        assert_eq!(decoded.end - decoded.start, 60_000);
    }

    /// Format must reach the wire (default-valued enums are elided, and unspecified
    /// yields a flamegraph not a pprof)
    #[test]
    fn the_request_asks_for_pprof_explicitly() {
        let encoded =
            SelectMergeStacktracesRequest { format: PROFILE_FORMAT_PPROF, ..Default::default() }
                .encode_to_vec();
        assert!(!encoded.is_empty(), "a non-default format is encoded");
        assert_eq!(
            SelectMergeStacktracesResponse::decode(&[] as &[u8]).expect("empty decodes").pprof,
            None
        );
    }

    /// Embedded `google.v1.Profile` decodes to opaque bytes = the pprof payload
    /// (round-trips a nested length-delimited message through both declarations)
    #[test]
    fn the_pprof_payload_survives_as_raw_bytes() {
        let pprof_bytes = vec![0x0au8, 0x03, b'a', b'b', b'c'];
        let response = SelectMergeStacktracesResponse {
            pprof: Some(PprofProfile { profile: pprof_bytes.clone() }),
        };
        let decoded = SelectMergeStacktracesResponse::decode(&response.encode_to_vec()[..])
            .expect("round trip");
        assert_eq!(decoded.pprof.expect("pprof present").profile, pprof_bytes);
    }
}
