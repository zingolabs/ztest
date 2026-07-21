//! Kubernetes quantity parsing and pod resource accounting.
//!
//! The cluster probe reserves each scheduled pod at `max(effective_request,
//! observed_usage)`. This module supplies the spec-derived request footprint
//! ([`pod_effective_request`]); the probe adds the metrics-derived usage term.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Container, PersistentVolumeClaim, PodSpec};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use super::{ANNOTATION_IO_BPS, ANNOTATION_IO_IOPS, Resources};

/// Parse a k8s CPU quantity to millicores. Handles `"500m"` = 500, `"2"` =
/// 2000, `"1.5"` = 1500, and the rarer micro/nano suffixes (`"2500000n"` = 3,
/// rounded up, conservative). An unrecognized form parses as 0; since 0
/// under-counts (the unsafe direction for capacity), the goal is to leave no
/// realistic k8s unit unhandled.
pub(crate) fn parse_cpu_milli(s: &str) -> u64 {
    parse_cpu_milli_opt(s).unwrap_or(0)
}

/// Like [`parse_cpu_milli`] but yields `None` on an unparseable quantity
/// instead of `0`. Callers that must distinguish "absent or garbage" from a
/// deliberate `0` (e.g. SA-budget validation, where a typo'd budget must be
/// rejected loudly rather than silently become a zero budget that fails every
/// request) use this; the probe path that under-counts safely uses the `u64`
/// form.
pub(crate) fn parse_cpu_milli_opt(s: &str) -> Option<u64> {
    let s = s.trim();
    // Float-to-int casts saturate in Rust (>=1.45): NaN and negatives to 0,
    // huge to u64::MAX. No panics.
    let scaled = |body: &str, per_milli: f64| -> Option<u64> {
        body.trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v / per_milli).round() as u64)
    };
    if let Some(n) = s.strip_suffix('m') {
        // Millicores are integers in valid k8s quantities.
        n.trim().parse::<u64>().ok()
    } else if let Some(n) = s.strip_suffix('u') {
        scaled(n, 1_000.0) // microcores → millicores
    } else if let Some(n) = s.strip_suffix('n') {
        scaled(n, 1_000_000.0) // nanocores → millicores
    } else {
        // Bare cores, possibly fractional or exponent ("1.5", "2e0").
        s.parse::<f64>()
            .ok()
            .map(|cores| (cores * 1000.0).round() as u64)
    }
}

/// Parse a k8s memory (or generic byte) quantity to bytes. Covers binary
/// (`Ki/Mi/Gi/Ti/Pi/Ei`), decimal SI (`k/K, M, G, T, P, E`), exponent
/// (`129e6`), and raw bytes. Overflow saturates; unparseable → 0.
pub(crate) fn parse_mem_bytes(s: &str) -> u64 {
    parse_mem_bytes_opt(s).unwrap_or(0)
}

/// Like [`parse_mem_bytes`] but yields `None` on an unparseable quantity
/// instead of `0`; see [`parse_cpu_milli_opt`] for the absent-vs-garbage
/// rationale.
pub(crate) fn parse_mem_bytes_opt(s: &str) -> Option<u64> {
    let s = s.trim();
    // Order matters: two-char binary suffixes are checked before the
    // single-char decimal ones they'd otherwise shadow.
    let (num, mult) = if let Some(n) = s.strip_suffix("Ki") {
        (n, 1u64 << 10)
    } else if let Some(n) = s.strip_suffix("Mi") {
        (n, 1u64 << 20)
    } else if let Some(n) = s.strip_suffix("Gi") {
        (n, 1u64 << 30)
    } else if let Some(n) = s.strip_suffix("Ti") {
        (n, 1u64 << 40)
    } else if let Some(n) = s.strip_suffix("Pi") {
        (n, 1u64 << 50)
    } else if let Some(n) = s.strip_suffix("Ei") {
        (n, 1u64 << 60)
    } else if let Some(n) = s.strip_suffix('k').or_else(|| s.strip_suffix('K')) {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix('M') {
        (n, 1_000_000)
    } else if let Some(n) = s.strip_suffix('G') {
        (n, 1_000_000_000)
    } else if let Some(n) = s.strip_suffix('T') {
        (n, 1_000_000_000_000)
    } else if let Some(n) = s.strip_suffix('P') {
        (n, 1_000_000_000_000_000)
    } else if let Some(n) = s.strip_suffix('E') {
        (n, 1_000_000_000_000_000_000)
    } else {
        (s, 1)
    };
    let num = num.trim();
    if let Ok(v) = num.parse::<u64>() {
        Some(v.saturating_mul(mult))
    } else if let Ok(f) = num.parse::<f64>() {
        // Fractional/exponent forms ("1.5Gi", "129e6").
        Some((f.max(0.0) * mult as f64).round() as u64)
    } else {
        None
    }
}

/// One container's `requests` as [`Resources`] (cpu millicores + memory bytes).
/// Absent → zero.
pub(crate) fn container_requests(c: &Container) -> Resources {
    container_amount(c.resources.as_ref().and_then(|r| r.requests.as_ref()))
}

/// Parse a `requests`/`limits` quantity map into [`Resources`]. I/O has no k8s
/// request/limit field, so the I/O dimensions are always zero here — the harness
/// enforces I/O per pod via cgroup `io.max` (see `docs/qos-io-dimension-design.md`),
/// not through the k8s resource model.
fn container_amount(map: Option<&BTreeMap<String, Quantity>>) -> Resources {
    let Some(map) = map else {
        return Resources::ZERO;
    };
    let cpu = map.get("cpu").map(|q| parse_cpu_milli(&q.0)).unwrap_or(0);
    let mem = map
        .get("memory")
        .map(|q| parse_mem_bytes(&q.0))
        .unwrap_or(0);
    Resources::new(cpu, mem, 0, 0)
}

/// The pod-level effective footprint for a per-container amount, following the
/// Kubernetes effective-request model rather than a naive sum: native sidecars
/// (init containers with `restartPolicy: Always`, k8s >=1.28) run for the whole
/// pod lifetime, while plain init containers contribute only a transient peak.
///
/// ```text
/// running   = Σ regular containers + Σ native-sidecar init containers
/// init_peak = max over plain init containers of (its amount + sidecars-so-far)
/// effective = max(running, init_peak)          // per dimension
/// ```
fn pod_effective(pod: &PodSpec, per_container: impl Fn(&Container) -> Resources) -> Resources {
    // Regular containers always run.
    let mut running = pod.containers.iter().fold(Resources::ZERO, |acc, c| {
        acc.saturating_add(&per_container(c))
    });

    // Init containers, in order: native sidecars add permanently; plain init
    // containers contribute their transient peak (their own amount plus the
    // sidecars that started before them).
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

/// The effective CPU + memory request of a pod, composed by the
/// effective-request model above. The floor the probe reserves per pod; it then
/// takes `max(request, usage)` so a Burstable co-tenant running above its
/// request is still bounded. CPU and memory only — disk I/O is a property of the
/// volume, declared on the PVC (see [`pvc_io_reservation`]).
pub(crate) fn pod_effective_request(pod: &PodSpec) -> Resources {
    pod_effective(pod, container_requests)
}

/// A PVC's disk-I/O reservation, read from
/// [`ANNOTATION_IO_BPS`]/[`ANNOTATION_IO_IOPS`]. I/O lives on the volume, not the
/// pod. CPU/memory are always zero here; a PVC with neither annotation reserves
/// nothing — an uncapped volume the probe cannot bound.
pub(crate) fn pvc_io_reservation(pvc: &PersistentVolumeClaim) -> Resources {
    let Some(a) = pvc.metadata.annotations.as_ref() else {
        return Resources::ZERO;
    };
    let io_bps = a.get(ANNOTATION_IO_BPS).map(|s| parse_mem_bytes(s)).unwrap_or(0);
    let io_iops = a
        .get(ANNOTATION_IO_IOPS)
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);
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
        assert_eq!(
            parse_mem_bytes("1.5Gi"),
            1024 * 1024 * 1024 + 512 * 1024 * 1024
        );
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
        Container {
            restart_policy: Some("Always".into()),
            ..container(cpu, mem)
        }
    }

    /// A Burstable container: `requests` below `limits`. The probe reserves it at
    /// its *request* (the scheduler floor); the burst headroom above request is
    /// accounted separately, via observed usage.
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

    // The effective-model composition (sums, sidecars, init peak) is exercised
    // through `pod_effective_request`, which reserves the pod at its request.

    #[test]
    fn request_sums_regular_containers() {
        let p = pod(
            vec![container("500m", "512Mi"), container("1", "1Gi")],
            vec![],
        );
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
        // Transient init needs 4 CPU; steady state only 1 → request-peak 4.
        let p = pod(vec![container("1", "1Gi")], vec![container("4", "1Gi")]);
        assert_eq!(pod_effective_request(&p).cpu_milli, 4000);
        // Small init under the running total changes nothing.
        let p = pod(vec![container("2", "1Gi")], vec![container("1", "512Mi")]);
        assert_eq!(pod_effective_request(&p).cpu_milli, 2000);
    }

    #[test]
    fn request_of_empty_and_resourceless_pod_is_zero() {
        assert_eq!(pod_effective_request(&PodSpec::default()), Resources::ZERO);
        let bare = pod(
            vec![Container {
                name: "c".into(),
                ..Default::default()
            }],
            vec![],
        );
        assert_eq!(pod_effective_request(&bare), Resources::ZERO);
    }

    // The request rule: the probe reserves a pod at its request (the scheduler
    // floor), ignoring the limit — the burst above request is caught separately
    // by observed usage, not by reserving the whole limit.

    #[test]
    fn request_ignores_the_limit() {
        // A Burstable pod is reserved at its request; its limit must not
        // sterilize the node.
        let p = pod(vec![burstable("8", "4Gi", "24", "16Gi")], vec![]);
        let fp = pod_effective_request(&p);
        assert_eq!(fp.cpu_milli, 8_000);
        assert_eq!(fp.mem_bytes, 4 * GIB);
        // A Guaranteed pod (requests == limits) reserves exactly that.
        let g = pod(vec![burstable("2", "2Gi", "2", "2Gi")], vec![]);
        assert_eq!(pod_effective_request(&g), Resources::new(2_000, 2 * GIB, 0, 0));
    }

    #[test]
    fn pod_request_covers_cpu_and_memory_only() {
        // Disk I/O is not a pod-`resources` field in Kubernetes — it is declared
        // on the volume (PVC), not the pod. So the pod request carries no I/O
        // dimension; the probe sums that separately from the pod's PVCs (see
        // `pvc_io_reservation`).
        let p = pod(vec![burstable("8", "4Gi", "24", "16Gi")], vec![]);
        let fp = pod_effective_request(&p);
        assert_eq!(fp.io_bps, 0);
        assert_eq!(fp.io_iops, 0);
    }

    // I/O reservation is a property of the storage request (the PVC).

    fn pvc(annotations: &[(&str, &str)]) -> PersistentVolumeClaim {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        PersistentVolumeClaim {
            metadata: ObjectMeta {
                annotations: (!annotations.is_empty()).then(|| {
                    annotations
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect()
                }),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn pvc_io_reservation_reads_the_storage_request_annotations() {
        let p = pvc(&[
            (ANNOTATION_IO_BPS, "100Mi"),
            (ANNOTATION_IO_IOPS, "5000"),
        ]);
        let fp = pvc_io_reservation(&p);
        assert_eq!(fp.io_bps, 100 * crate::qos::MIB);
        assert_eq!(fp.io_iops, 5_000);
        // Never CPU/memory — those come from the pod.
        assert_eq!(fp.cpu_milli, 0);
        assert_eq!(fp.mem_bytes, 0);
    }

    #[test]
    fn pvc_io_reservation_is_zero_for_an_uncapped_volume() {
        // No annotation → no reservation. Such a volume is uncapped; the probe
        // cannot bound its I/O (why co-tenant volumes must be capped).
        assert_eq!(pvc_io_reservation(&pvc(&[])), Resources::ZERO);
        // A partial annotation still yields the dimension it declares.
        let bps_only = pvc(&[(ANNOTATION_IO_BPS, "50Mi")]);
        assert_eq!(pvc_io_reservation(&bps_only).io_bps, 50 * crate::qos::MIB);
        assert_eq!(pvc_io_reservation(&bps_only).io_iops, 0);
    }
}
