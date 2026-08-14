//! CPU profiles, via Pyroscope. Component contract in `docs/how-to-profile.md`.
//!
//! - Components push; ztest queries the merged result back as pprof
//! - No volume, no pod collection → a profile outlives the component, its namespace
//!   and an OOM kill, and reads mid-run
//! - [`ebpf`] collects the same profiles out-of-process (native + kernel frames, off-CPU)

pub(crate) mod ebpf;

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use k8s_openapi::api::core::v1::{Pod, Service};
use kube::Client;
use kube::api::{Api, ListParams};
use prost::Message as _;

use crate::portforward::Forwarder;

/// Tenant header.
///
/// - Sole delete handle (no delete-by-selector API — per-tenant retention = only way to
///   retire one sync's profiles)
/// - Mandatory under `multitenancy_enabled`: absent → 401, not a default tenant
const TENANT_HEADER: &str = "X-Scope-OrgID";

/// Marks ztest's tenants apart in a Pyroscope an operator may share
const TENANT_PREFIX: &str = "ztest";

/// Upstream tenant-id ceiling, bytes
const TENANT_MAX: usize = 150;

/// Retention stamped on a retired tenant.
///
/// - Never `0` (upstream: zero override = never delete → outlives every other tenant)
/// - Any positive duration < data age
pub(crate) const RETIRED_RETENTION: &str = "1s";

/// Pyroscope tenant: `ztest.<user>.<id>`, id = sync id else run id.
///
/// - Derived, never looked up (retirement outlives the namespace)
/// - `.` = separator → escaped out of both parts
/// - Charset ≤150 bytes, alphanumeric + `!-_.*'()`, no `/`, no whitespace
pub(crate) fn tenant(user: &str, sync_id: &str) -> String {
    let part = |s: &str| -> String {
        s.chars()
            .map(|c| match c {
                c if c.is_ascii_alphanumeric() => c,
                '-' | '_' => c,
                _ => '_',
            })
            .collect()
    };
    let tenant = format!("{TENANT_PREFIX}.{}.{}", part(user), part(sync_id));
    // collision-safe: sync id carries its own random suffix, far inside the cap
    tenant.chars().take(TENANT_MAX).collect()
}

/// Locates an install ztest did not create (operator's lives wherever they put it;
/// every Pyroscope deployment carries this label)
const PYROSCOPE_LABEL: &str = "app.kubernetes.io/name=pyroscope";

/// Profile type the collector pushes CPU samples under
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

/// Merged CPU profile for `selector` over `[from, to]`, as pprof bytes.
///
/// `tenant` required, not optional — `multitenancy_enabled` makes a header-less request
/// a 401, never a query of some default tenant
pub(crate) async fn fetch(
    client: &Client,
    selector: &str,
    from: SystemTime,
    to: SystemTime,
    tenant: &str,
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
        .header(TENANT_HEADER, tenant)
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

/// Retire `tenants`; Pyroscope's cleaner deletes within [`PROFILE_RETIREMENT_LAG`].
///
/// - Scheduled, never immediate (no delete API) — 1s retention → cleaner tombstones →
///   compaction frees objects
/// - Read-modify-write, not an apply (document accumulates across passes)
/// - Entries expire after [`RETIREMENT_TTL`] (else unbounded growth)
///
/// [`PROFILE_RETIREMENT_LAG`]: crate::resource::PROFILE_RETIREMENT_LAG
pub(crate) async fn schedule_purge(client: &Client, tenants: &[String]) -> Result<(), String> {
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::api::{Patch, PatchParams};

    use crate::resource::impls::observability::{
        Overrides, PYROSCOPE_OVERRIDES_CONFIGMAP, PYROSCOPE_OVERRIDES_KEY, PYROSCOPE_RETIRED_KEY,
        RETIREMENT_TTL, TenantLimits,
    };

    if tenants.is_empty() {
        return Ok(());
    }
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), crate::resource::OBS_NAMESPACE);
    let existing = api
        .get_opt(PYROSCOPE_OVERRIDES_CONFIGMAP)
        .await
        .map_err(|e| format!("read {PYROSCOPE_OVERRIDES_CONFIGMAP}: {e}"))?
        .ok_or("no Pyroscope overrides ConfigMap; re-run `ztest cluster setup`")?;

    // Ledger = sole source, overrides derived from it each pass (hand-edits & an older
    // build's leftovers cannot drift)
    let mut retired: BTreeMap<String, u64> = existing
        .data
        .as_ref()
        .and_then(|d| d.get(PYROSCOPE_RETIRED_KEY))
        .map(|doc| serde_yaml::from_str(doc))
        .transpose()
        .map_err(|e| format!("parse {PYROSCOPE_RETIRED_KEY}: {e}"))?
        .unwrap_or_default();

    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    retired.extend(tenants.iter().map(|t| (t.clone(), now)));
    retired.retain(|_, at| now.saturating_sub(*at) < RETIREMENT_TTL.as_secs());

    let overrides = Overrides {
        overrides: retired
            .keys()
            .map(|t| (t.clone(), TenantLimits { retention_period: RETIRED_RETENTION.into() }))
            .collect(),
    };

    let patch = serde_json::json!({
        "data": {
            PYROSCOPE_OVERRIDES_KEY: serde_yaml::to_string(&overrides)
                .map_err(|e| format!("render {PYROSCOPE_OVERRIDES_KEY}: {e}"))?,
            PYROSCOPE_RETIRED_KEY: serde_yaml::to_string(&retired)
                .map_err(|e| format!("render {PYROSCOPE_RETIRED_KEY}: {e}"))?,
        }
    });
    api.patch(PYROSCOPE_OVERRIDES_CONFIGMAP, &PatchParams::default(), &Patch::Merge(patch))
        .await
        .map_err(|e| format!("write {PYROSCOPE_OVERRIDES_CONFIGMAP}: {e}"))?;
    Ok(())
}

/// ztest's *own* Pyroscope exists.
///
/// - Absent = `--no-observability` → nothing recorded, nothing left un-reclaimed
/// - Not [`pyroscope_service`] (operator's install carries no ztest overrides ConfigMap
///   → retirement would error, not no-op)
pub(crate) async fn is_deployed(client: &Client) -> bool {
    let api: Api<Service> = Api::namespaced(client.clone(), crate::resource::OBS_NAMESPACE);
    api.get_opt(crate::resource::PYROSCOPE_SERVICE).await.ok().flatten().is_some()
}

/// Tenant a sync's profiles were pushed under.
///
/// - Owner from the namespace label, not [`current_user`](crate::naming) (a named target
///   may be another dev's)
/// - `None` = namespace gone → tenant unrecoverable, profiles left to the default clock
pub(crate) async fn tenant_for_sync(client: &Client, sync_id: &str) -> Option<String> {
    use k8s_openapi::api::core::v1::Namespace;

    let api: Api<Namespace> = Api::all(client.clone());
    let ns = api.get_opt(&crate::sync::namespace_for(sync_id)).await.ok()??;
    let owner = ns.metadata.labels?.get(crate::qos::LABEL_USER)?.clone();
    Some(tenant(&owner, sync_id))
}

/// Pyroscope selector for one component of one run. Namespace-scoped, not run-id
/// (what the component was tagged with, derivable from a sync id without a lookup)
pub(crate) fn selector(component: &str, namespace: &str) -> String {
    format!(r#"{{component="{component}",namespace="{namespace}"}}"#)
}

/// CPU the profile *claims*, summed over every sample.
///
/// - Half of [`fidelity`](crate::cli::sync::perf); the kernel's own figure is the other
/// - `sample.value[0]` = ns under the sole `cpu/nanoseconds` type ztest ever requests
///   ([`CPU_PROFILE`]) — a second type would need an index, not a `[0]`
/// - Merged profiles carry no `duration_nanos`, so utilisation is not derivable here;
///   the caller supplies the window it asked Pyroscope for
/// - `None` = unparseable, never 0 (a zero total reads as an idle process)
pub(crate) fn cpu_nanos(profile: &[u8]) -> Option<u64> {
    use prost::bytes::Buf as _;
    use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};

    const SAMPLE: u32 = 2;
    const VALUE: u32 = 2;

    fn first_value(sample: &[u8]) -> Option<u64> {
        let mut rest = sample;
        while rest.has_remaining() {
            let (tag, wire) = decode_key(&mut rest).ok()?;
            if tag == VALUE {
                // Packed by every encoder in practice, but a conformant writer may emit
                // one varint per field, and the first value is all this needs either way
                return match wire {
                    WireType::LengthDelimited => {
                        let len = decode_varint(&mut rest).ok()? as usize;
                        decode_varint(&mut rest.get(..len)?).ok()
                    }
                    WireType::Varint => decode_varint(&mut rest).ok(),
                    _ => None,
                };
            }
            skip_field(wire, tag, &mut rest, DecodeContext::default()).ok()?;
        }
        None
    }

    let mut total: u64 = 0;
    let mut rest = profile;
    while rest.has_remaining() {
        let (tag, wire) = decode_key(&mut rest).ok()?;
        if tag == SAMPLE && wire == WireType::LengthDelimited {
            let len = decode_varint(&mut rest).ok()? as usize;
            let sample = rest.get(..len)?;
            total = total.saturating_add(first_value(sample).unwrap_or(0));
            rest.advance(len);
            continue;
        }
        skip_field(wire, tag, &mut rest, DecodeContext::default()).ok()?;
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `.` = separator → neither part may contribute one (else two syncs collide)
    #[test]
    fn a_tenant_escapes_the_separator_out_of_both_parts() {
        assert_eq!(tenant("eli.b", "zaino.a52f"), "ztest.eli_b.zaino_a52f");
        assert_eq!(tenant("elicb", "zaino-a52f"), "ztest.elicb.zaino-a52f");
    }

    /// Charset is upstream-enforced: `/` and whitespace are rejected outright
    #[test]
    fn a_tenant_carries_no_character_pyroscope_rejects() {
        let t = tenant("dept/eng team", "sync id");
        assert!(t.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)), "got {t}");
        assert!(tenant(&"x".repeat(400), "abc").len() <= TENANT_MAX);
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

    /// Hand-built two-sample profile: `sample.value[0]` is what a CPU total sums, and
    /// the second value of a multi-value sample must not join it
    #[test]
    fn the_cpu_total_sums_the_first_value_of_every_sample() {
        // sample{value=[7]}, sample{value=[5,99]} — packed, as every encoder writes them
        let profile = [0x12, 0x03, 0x12, 0x01, 0x07, 0x12, 0x05, 0x12, 0x03, 0x05, 0x63, 0x00];
        assert_eq!(cpu_nanos(&profile), Some(12));
    }

    /// A field this build does not know must be stepped over, not abandoned
    #[test]
    fn an_unknown_field_does_not_stop_the_sum() {
        // string_table{""} ahead of sample{value=[7]}
        let profile = [0x32, 0x00, 0x12, 0x03, 0x12, 0x01, 0x07];
        assert_eq!(cpu_nanos(&profile), Some(7));
    }

    /// Unparseable yields `None`, never `Some(0)` — a zero total reads as an idle process
    #[test]
    fn a_truncated_profile_reports_no_total() {
        assert_eq!(cpu_nanos(&[0x12, 0xff, 0xff]), None);
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
