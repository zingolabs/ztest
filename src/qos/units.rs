//! Kubernetes quantity parsing and pod resource accounting.
//!
//! Shared by the real store adapter ([`crate::qos::kube_store`], which
//! synthesizes a Job's footprint) and the cluster probe
//! ([`crate::pipeline::cluster`], which sums pod requests against node
//! allocatable). One implementation, one set of tests — no per-consumer
//! drift in how a `"500m"` / `"2Gi"` quantity or a pod's effective
//! request is computed.

use k8s_openapi::api::core::v1::{Container, PodSpec};

use super::Resources;

/// Parse a k8s CPU quantity to millicores. Handles `"500m"` → 500, `"2"` →
/// 2000, `"1.5"` → 1500, and the rarer micro/nano suffixes (`"2500000n"` →
/// 3, rounded up — conservative). An unrecognized form parses as 0; since
/// 0 *under*-counts (the unsafe direction for capacity), the goal is to leave
/// no realistic k8s unit unhandled.
pub(crate) fn parse_cpu_milli(s: &str) -> u64 {
    let s = s.trim();
    // Float→int casts saturate in Rust (≥1.45): NaN→0, negatives→0,
    // huge→u64::MAX — all safe, no panics.
    let scaled = |body: &str, per_milli: f64| -> u64 {
        body.trim()
            .parse::<f64>()
            .map(|v| (v / per_milli).round() as u64)
            .unwrap_or(0)
    };
    if let Some(n) = s.strip_suffix('m') {
        // Millicores are integers in valid k8s quantities.
        n.trim().parse::<u64>().unwrap_or(0)
    } else if let Some(n) = s.strip_suffix('u') {
        scaled(n, 1_000.0) // microcores → millicores
    } else if let Some(n) = s.strip_suffix('n') {
        scaled(n, 1_000_000.0) // nanocores → millicores
    } else {
        // Bare cores, possibly fractional or exponent ("1.5", "2e0").
        s.parse::<f64>()
            .map(|cores| (cores * 1000.0).round() as u64)
            .unwrap_or(0)
    }
}

/// Parse a k8s memory (or generic byte) quantity to bytes. Covers binary
/// (`Ki/Mi/Gi/Ti/Pi/Ei`), decimal SI (`k/K, M, G, T, P, E`), exponent
/// (`129e6`), and raw bytes. Overflow saturates; unparseable → 0.
pub(crate) fn parse_mem_bytes(s: &str) -> u64 {
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
        v.saturating_mul(mult)
    } else if let Ok(f) = num.parse::<f64>() {
        // Fractional/exponent forms ("1.5Gi", "129e6").
        (f.max(0.0) * mult as f64).round() as u64
    } else {
        0
    }
}

/// The `requests` of a single container as [`Resources`] (cpu millicores +
/// memory bytes). Absent requests → zero.
pub(crate) fn container_requests(c: &Container) -> Resources {
    let Some(reqs) = c.resources.as_ref().and_then(|r| r.requests.as_ref()) else {
        return Resources::ZERO;
    };
    let cpu = reqs.get("cpu").map(|q| parse_cpu_milli(&q.0)).unwrap_or(0);
    let mem = reqs.get("memory").map(|q| parse_mem_bytes(&q.0)).unwrap_or(0);
    Resources::new(cpu, mem)
}

/// The effective resource *request* of a pod — the amount the k8s scheduler
/// actually reserves (requests, not limits).
///
/// Follows the Kubernetes effective-request model rather than a naive sum,
/// because **native sidecars** (init containers with `restartPolicy: Always`,
/// k8s ≥1.28) run for the whole pod lifetime and are resource-significant;
/// ignoring them would under-count and risk overcommit:
///
/// ```text
/// running   = Σ regular containers + Σ native-sidecar init containers
/// init_peak = max over plain init containers of (its request + sidecars-so-far)
/// effective = max(running, init_peak)          // per dimension
/// ```
pub(crate) fn pod_effective_requests(pod: &PodSpec) -> Resources {
    // Regular containers always run.
    let mut running = pod
        .containers
        .iter()
        .fold(Resources::ZERO, |acc, c| acc.saturating_add(&container_requests(c)));

    // Init containers, in order: native sidecars add permanently; plain init
    // containers contribute their transient peak (their own request plus the
    // sidecars that started before them).
    let mut sidecars = Resources::ZERO;
    let mut init_peak = Resources::ZERO;
    for c in pod.init_containers.iter().flatten() {
        let req = container_requests(c);
        if c.restart_policy.as_deref() == Some("Always") {
            running = running.saturating_add(&req);
            sidecars = sidecars.saturating_add(&req);
        } else {
            init_peak = init_peak.max(&req.saturating_add(&sidecars));
        }
    }

    running.max(&init_peak)
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
        Container {
            restart_policy: Some("Always".into()),
            ..container(cpu, mem)
        }
    }

    fn pod(containers: Vec<Container>, init: Vec<Container>) -> PodSpec {
        PodSpec {
            containers,
            init_containers: if init.is_empty() { None } else { Some(init) },
            ..Default::default()
        }
    }

    #[test]
    fn effective_request_sums_regular_containers() {
        let p = pod(vec![container("500m", "512Mi"), container("1", "1Gi")], vec![]);
        let fp = pod_effective_requests(&p);
        assert_eq!(fp.cpu_milli, 1500);
        assert_eq!(fp.mem_bytes, 512 * 1024 * 1024 + GIB);
    }

    #[test]
    fn native_sidecars_count_permanently() {
        let p = pod(vec![container("1", "1Gi")], vec![sidecar("1", "1Gi")]);
        let fp = pod_effective_requests(&p);
        assert_eq!(fp.cpu_milli, 2000, "sidecar added, not ignored");
        assert_eq!(fp.mem_bytes, 2 * GIB);
    }

    #[test]
    fn plain_init_peak_dominates_when_larger() {
        // Transient init needs 4 CPU; steady state only 1 → effective 4.
        let p = pod(vec![container("1", "1Gi")], vec![container("4", "1Gi")]);
        assert_eq!(pod_effective_requests(&p).cpu_milli, 4000);
        // Small init under the running total changes nothing.
        let p = pod(vec![container("2", "1Gi")], vec![container("1", "512Mi")]);
        assert_eq!(pod_effective_requests(&p).cpu_milli, 2000);
    }

    #[test]
    fn empty_pod_and_requestless_containers_are_zero() {
        assert_eq!(pod_effective_requests(&PodSpec::default()), Resources::ZERO);
        let bare = pod(vec![Container { name: "c".into(), ..Default::default() }], vec![]);
        assert_eq!(pod_effective_requests(&bare), Resources::ZERO);
    }
}
