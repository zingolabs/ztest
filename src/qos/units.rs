//! k8s quantity parsing + pod resource accounting.
//!
//! - Probe reserves each pod at `max(effective_request, observed_usage)`
//! - Here: the spec-derived `pod_effective_request` half (probe adds the usage term)
//! - Scalar quantity parsing delegates to `ztest_attr::footprint`, which the
//!   `footprint = ".."` attribute grammar also reads — one accepted syntax whether
//!   a quantity came from an annotation or the apiserver

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Container, PersistentVolumeClaim, PodSpec};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use super::{ANNOTATION_IO_BPS, ANNOTATION_IO_IOPS, Resources};

/// k8s CPU quantity → millicores. `"500m"`/`"2"`/`"1.5"`/`"2500000n"` (rounds to nearest).
/// Unrecognized → 0, which under-counts (unsafe direction — leave no real unit unhandled)
pub(crate) fn parse_cpu_milli(s: &str) -> u64 {
    parse_cpu_milli_opt(s).unwrap_or(0)
}

/// [`parse_cpu_milli`] with `None` for unparseable, for callers separating
/// garbage from an intended `0` (typo'd SA budget must be rejected, not zeroed)
pub(crate) fn parse_cpu_milli_opt(s: &str) -> Option<u64> {
    ztest_attr::footprint::parse_cpu_milli(s)
}

/// k8s memory/byte quantity → bytes: binary, decimal SI, exponent, raw.
/// Overflow saturates, unparseable → 0
pub(crate) fn parse_mem_bytes(s: &str) -> u64 {
    parse_mem_bytes_opt(s).unwrap_or(0)
}

/// [`parse_mem_bytes`] with `None` for unparseable, see [`parse_cpu_milli_opt`]
pub(crate) fn parse_mem_bytes_opt(s: &str) -> Option<u64> {
    ztest_attr::footprint::parse_mem_bytes(s)
}

/// Absent `requests` → [`Resources::ZERO`].
pub(crate) fn container_requests(c: &Container) -> Resources {
    container_amount(c.resources.as_ref().and_then(|r| r.requests.as_ref()))
}

/// Absent `limits` → [`Resources::ZERO`], i.e. uncapped, not zero-capped. Callers
/// wanting a denominator must reject `ZERO` rather than divide by it
pub(crate) fn container_limits(c: &Container) -> Resources {
    container_amount(c.resources.as_ref().and_then(|r| r.limits.as_ref()))
}

/// I/O dimensions always zero (no k8s I/O field; harness caps via cgroup `io.max`,
/// see `docs/design-qos.md`)
fn container_amount(map: Option<&BTreeMap<String, Quantity>>) -> Resources {
    let Some(map) = map else {
        return Resources::ZERO;
    };
    let cpu = map.get("cpu").map(|q| parse_cpu_milli(&q.0)).unwrap_or(0);
    let mem = map.get("memory").map(|q| parse_mem_bytes(&q.0)).unwrap_or(0);
    Resources::new(cpu, mem, 0, 0)
}

/// Pod-level footprint on k8s's effective-request model, not a naive sum:
/// native sidecars (init + `restartPolicy: Always`) run pod-lifetime, plain init
/// containers only peak.
///
/// ```text
/// running   = Σ regular containers + Σ native-sidecar init containers
/// init_peak = max over plain init containers of (its amount + sidecars-so-far)
/// effective = max(running, init_peak)          // per dimension
/// ```
fn pod_effective(pod: &PodSpec, per_container: impl Fn(&Container) -> Resources) -> Resources {
    let mut running =
        pod.containers.iter().fold(Resources::ZERO, |acc, c| acc.saturating_add(&per_container(c)));

    // In order: sidecars add permanently, plain init peaks at own amount + sidecars so far
    let mut sidecars = Resources::ZERO;
    let mut init_peak = Resources::ZERO;
    for c in pod.init_containers.iter().flatten() {
        let amt = per_container(c);
        if c.restart_policy.as_deref() == Some("Always") {
            running = running.saturating_add(&amt);
            sidecars = sidecars.saturating_add(&amt);
        } else {
            init_peak = init_peak.max(&amt.saturating_add(&sidecars));
        }
    }

    running.max(&init_peak)
}

/// Per-pod reservation floor, later `max`'d with usage (bounds a bursting co-tenant).
/// CPU+memory only, disk I/O rides the PVC ([`pvc_io_reservation`])
pub(crate) fn pod_effective_request(pod: &PodSpec) -> Resources {
    pod_effective(pod, container_requests)
}

/// Ceiling the pod may draw, on the same effective model as
/// [`pod_effective_request`]. `ZERO` in a dimension = uncapped there
pub(crate) fn pod_effective_limit(pod: &PodSpec) -> Resources {
    pod_effective(pod, container_limits)
}

/// From [`ANNOTATION_IO_BPS`]/[`ANNOTATION_IO_IOPS`], CPU/memory always zero.
/// Neither annotation = nothing reserved (uncapped volume, unbounded by the probe)
pub(crate) fn pvc_io_reservation(pvc: &PersistentVolumeClaim) -> Resources {
    let Some(a) = pvc.metadata.annotations.as_ref() else {
        return Resources::ZERO;
    };
    let io_bps = a.get(ANNOTATION_IO_BPS).map(|s| parse_mem_bytes(s)).unwrap_or(0);
    let io_iops = a.get(ANNOTATION_IO_IOPS).and_then(|s| s.trim().parse::<u64>().ok()).unwrap_or(0);
    Resources::new(0, 0, io_bps, io_iops)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::GIB;
    use k8s_openapi::api::core::v1::{Container, PodSpec, ResourceRequirements};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use std::collections::BTreeMap;

    #[test]
    fn parse_cpu_milli_handles_milli_integer_fractional_and_subunits() {
        assert_eq!(parse_cpu_milli("500m"), 500);
        assert_eq!(parse_cpu_milli("250m"), 250);
        assert_eq!(parse_cpu_milli("2"), 2000);
        assert_eq!(parse_cpu_milli("1.5"), 1500);
        assert_eq!(parse_cpu_milli("0.1"), 100);
        assert_eq!(parse_cpu_milli("1000000u"), 1000);
        assert_eq!(parse_cpu_milli("2500000n"), 3); // 2.5 milli, rounded up
        assert_eq!(parse_cpu_milli(""), 0);
        assert_eq!(parse_cpu_milli("garbage"), 0);
    }

    #[test]
    fn parse_mem_bytes_handles_binary_decimal_exponent_and_raw() {
        assert_eq!(parse_mem_bytes("2Gi"), 2 * GIB);
        assert_eq!(parse_mem_bytes("512Mi"), 512 * 1024 * 1024);
        assert_eq!(parse_mem_bytes("64Ki"), 64 * 1024);
        assert_eq!(parse_mem_bytes("1Pi"), 1u64 << 50);
        assert_eq!(parse_mem_bytes("1G"), 1_000_000_000);
        assert_eq!(parse_mem_bytes("1T"), 1_000_000_000_000);
        assert_eq!(parse_mem_bytes("1k"), 1_000);
        assert_eq!(parse_mem_bytes("129e6"), 129_000_000);
        assert_eq!(parse_mem_bytes("1.5Gi"), 1024 * 1024 * 1024 + 512 * 1024 * 1024);
        assert_eq!(parse_mem_bytes("1048576"), 1_048_576);
        assert_eq!(parse_mem_bytes("nope"), 0);
    }

    fn container(cpu: &str, mem: &str) -> Container {
        Container {
            name: "c".into(),
            resources: Some(ResourceRequirements {
                requests: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity(cpu.to_string())),
                    ("memory".to_string(), Quantity(mem.to_string())),
                ])),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn sidecar(cpu: &str, mem: &str) -> Container {
        Container { restart_policy: Some("Always".into()), ..container(cpu, mem) }
    }

    /// Burstable: `requests` < `limits`. Reserved at *request*, headroom above it
    /// accounted separately via observed usage
    fn burstable(cpu_req: &str, mem_req: &str, cpu_lim: &str, mem_lim: &str) -> Container {
        Container {
            name: "c".into(),
            resources: Some(ResourceRequirements {
                requests: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity(cpu_req.to_string())),
                    ("memory".to_string(), Quantity(mem_req.to_string())),
                ])),
                limits: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity(cpu_lim.to_string())),
                    ("memory".to_string(), Quantity(mem_lim.to_string())),
                ])),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn pod(containers: Vec<Container>, init: Vec<Container>) -> PodSpec {
        PodSpec {
            containers,
            init_containers: if init.is_empty() { None } else { Some(init) },
            ..Default::default()
        }
    }

    // Effective-model composition exercised through `pod_effective_request`

    #[test]
    fn request_sums_regular_containers() {
        let p = pod(vec![container("500m", "512Mi"), container("1", "1Gi")], vec![]);
        let fp = pod_effective_request(&p);
        assert_eq!(fp.cpu_milli, 1500);
        assert_eq!(fp.mem_bytes, 512 * 1024 * 1024 + GIB);
    }

    #[test]
    fn request_counts_native_sidecars_permanently() {
        let p = pod(vec![container("1", "1Gi")], vec![sidecar("1", "1Gi")]);
        let fp = pod_effective_request(&p);
        assert_eq!(fp.cpu_milli, 2000, "sidecar added, not ignored");
        assert_eq!(fp.mem_bytes, 2 * GIB);
    }

    #[test]
    fn request_takes_plain_init_peak_when_larger() {
        // Transient init 4 CPU vs steady 1 → peak 4
        let p = pod(vec![container("1", "1Gi")], vec![container("4", "1Gi")]);
        assert_eq!(pod_effective_request(&p).cpu_milli, 4000);
        // Small init under the running total = no change
        let p = pod(vec![container("2", "1Gi")], vec![container("1", "512Mi")]);
        assert_eq!(pod_effective_request(&p).cpu_milli, 2000);
    }

    #[test]
    fn request_of_empty_and_resourceless_pod_is_zero() {
        assert_eq!(pod_effective_request(&PodSpec::default()), Resources::ZERO);
        let bare = pod(vec![Container { name: "c".into(), ..Default::default() }], vec![]);
        assert_eq!(pod_effective_request(&bare), Resources::ZERO);
    }

    // Reserve at request (scheduler floor), never the limit (burst caught via usage)

    #[test]
    fn request_ignores_the_limit() {
        // Limit must not sterilize the node
        let p = pod(vec![burstable("8", "4Gi", "24", "16Gi")], vec![]);
        let fp = pod_effective_request(&p);
        assert_eq!(fp.cpu_milli, 8_000);
        assert_eq!(fp.mem_bytes, 4 * GIB);
        // Guaranteed (requests == limits) reserves exactly that
        let g = pod(vec![burstable("2", "2Gi", "2", "2Gi")], vec![]);
        assert_eq!(pod_effective_request(&g), Resources::new(2_000, 2 * GIB, 0, 0));
    }

    #[test]
    fn pod_request_covers_cpu_and_memory_only() {
        // Disk I/O declared on the PVC, not the pod → summed separately
        let p = pod(vec![burstable("8", "4Gi", "24", "16Gi")], vec![]);
        let fp = pod_effective_request(&p);
        assert_eq!(fp.io_bps, 0);
        assert_eq!(fp.io_iops, 0);
    }

    // I/O reservation = a property of the PVC

    fn pvc(annotations: &[(&str, &str)]) -> PersistentVolumeClaim {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        PersistentVolumeClaim {
            metadata: ObjectMeta {
                annotations: (!annotations.is_empty()).then(|| {
                    annotations.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
                }),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn pvc_io_reservation_reads_the_storage_request_annotations() {
        let p = pvc(&[(ANNOTATION_IO_BPS, "100Mi"), (ANNOTATION_IO_IOPS, "5000")]);
        let fp = pvc_io_reservation(&p);
        assert_eq!(fp.io_bps, 100 * crate::qos::MIB);
        assert_eq!(fp.io_iops, 5_000);
        // Never CPU/memory (those come from the pod)
        assert_eq!(fp.cpu_milli, 0);
        assert_eq!(fp.mem_bytes, 0);
    }

    #[test]
    fn pvc_io_reservation_is_zero_for_an_uncapped_volume() {
        // No annotation → no reservation (uncapped volume; why co-tenants must be capped)
        assert_eq!(pvc_io_reservation(&pvc(&[])), Resources::ZERO);
        // Partial annotation still yields the dimension it declares
        let bps_only = pvc(&[(ANNOTATION_IO_BPS, "50Mi")]);
        assert_eq!(pvc_io_reservation(&bps_only).io_bps, 50 * crate::qos::MIB);
        assert_eq!(pvc_io_reservation(&bps_only).io_iops, 0);
    }
}

/// Still holding capacity: anything not settled into `Succeeded`/`Failed`.
///
/// - Sole definition; the ledger's headroom subtraction, `assert_invariant` and the probe's
///   `ClusterCapacity` must agree or admission double-counts a pod one of them cannot see
/// - Unscheduled `Pending` counts: it is capacity already promised to a created pod, and
///   under-counting it is the direction that overcommits a node
pub fn pod_holds_capacity(pod: &k8s_openapi::api::core::v1::Pod) -> bool {
    !matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some("Succeeded") | Some("Failed")
    )
}
