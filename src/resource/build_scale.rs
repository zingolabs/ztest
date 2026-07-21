//! In-place vertical resize of the long-lived build pods (buildkit, builder):
//! grown to a build size before a build and shrunk back after, via the `resize`
//! subresource (KEP-1287) — no restart/reschedule/PVC-remount.
//!
//! `requests == limits` and integer CPU throughout: a pod holding exclusive
//! cores under the static CPU-manager policy cannot be resized in place, so
//! these pods are never statically pinned. Grow/shrink is phase-driven, correct
//! for sequential runs; a concurrent run on the same shared pod could see it
//! shrunk from under it (cross-run refcount deliberately deferred). All
//! operations are best-effort; a resize failure surfaces as an ordinary build error.

use k8s_openapi::api::core::v1::Pod;
use std::time::{Duration, Instant};

use kube::api::{AttachParams, Patch, PatchParams};
use kube::{Api, Client};
use serde_json::json;

use crate::resource::impls::policy::RUN_NAMESPACE;

/// Resize `container` in `pod` (namespace [`RUN_NAMESPACE`]) to `cpu`/`mem` as a
/// Guaranteed reservation (`requests == limits`), in place via the `resize`
/// subresource. Strategic-merge patch: the container is matched by name, other
/// pod fields untouched. `cpu` is a whole-core integer, never fractional.
pub(crate) async fn resize_to(
    client: &Client,
    pod: &str,
    container: &str,
    cpu: &str,
    mem: &str,
) -> Result<(), String> {
    // Fractional CPU makes the pod ineligible for the static CPU-manager
    // policy and KEP-1287 forbids in-place resize once a pod holds exclusive
    // cores; an empty quantity would clear the field and downgrade QoS. A bad
    // value here is a harness bug, so panic.
    assert!(!cpu.is_empty() && !mem.is_empty(), "resize {pod}/{container}: empty cpu/mem");
    assert!(
        cpu.chars().all(|c| c.is_ascii_digit()),
        "resize {pod}/{container}: cpu {cpu:?} must be a whole-core integer (never fractional)",
    );

    let api: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    let patch = json!({
        "spec": { "containers": [{
            "name": container,
            "resources": {
                "requests": { "cpu": cpu, "memory": mem },
                "limits":   { "cpu": cpu, "memory": mem },
            },
        }]},
    });
    api.patch_subresource(
        "resize",
        pod,
        &PatchParams::default(),
        &Patch::Strategic(&patch),
    )
    .await
    .map(|_| ())
    .map_err(|e| format!("resize {pod}/{container} to {cpu}c/{mem}: {e}"))
}

/// Shrink a build pod back to its rest size. The kubelet refuses an in-place
/// resize-*down* whenever `memory.current` exceeds the new limit, and KEP-1287
/// counts reclaimable page cache in `memory.current` — every build touches GiBs
/// of files, so a plain shrink fails and the pod stays stuck grown. So first
/// drop the page cache with a cgroup-v2 targeted reclaim (`memory.reclaim`,
/// scoped to this pod's cgroup), then resize down. Best-effort: on a kernel
/// without `memory.reclaim` the reclaim is a silent no-op.
pub(crate) async fn shrink_to(
    client: &Client,
    pod: &str,
    container: &str,
    cpu: &str,
    mem: &str,
) -> Result<(), String> {
    let target = parse_mem_bytes(mem).unwrap_or(0);
    reclaim_page_cache(client, pod, container, target).await;
    resize_to(client, pod, container, cpu, mem).await
}

/// `exec` a targeted cgroup-v2 reclaim into `pod`, freeing page cache down
/// toward `target_bytes` so a subsequent shrink is accepted. The leaf cgroup is
/// resolved from `/proc/self/cgroup` so it works whether the container is in a
/// private cgroup namespace (path `/`) or a host one (a privileged pod seeing
/// the full `/kubepods.slice/…/crio-….scope` path). Best-effort; failure is
/// swallowed and the shrink still attempted.
async fn reclaim_page_cache(client: &Client, pod: &str, container: &str, target_bytes: u64) {
    // `{{` / `}}` are literal braces for awk; `{target_bytes}` is interpolated.
    // Writing more than is reclaimable returns -EAGAIN (harmless), so `|| true`.
    let script = format!(
        "CG=$(awk -F: '/^0::/{{print $3}}' /proc/self/cgroup); \
         B=/sys/fs/cgroup; [ \"$CG\" != / ] && B=/sys/fs/cgroup$CG; \
         CUR=$(cat \"$B/memory.current\" 2>/dev/null || echo 0); \
         if [ \"$CUR\" -gt {target_bytes} ]; then \
           echo $((CUR - {target_bytes})) > \"$B/memory.reclaim\" 2>/dev/null || true; \
         fi; true"
    );
    let api: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    let ap = AttachParams::default()
        .container(container)
        .stdin(false)
        .stdout(false)
        .stderr(false);
    if let Ok(proc) = api.exec(pod, ["sh", "-c", &script], &ap).await {
        // Await exit so the reclaim completes before the resize is issued.
        let _ = proc.join().await;
    }
}

/// How long a grow may wait for the kubelet to actuate the resize, including
/// time spent `Deferred` behind another run's build holding capacity. Generous:
/// waiting out a concurrent build beats failing a build that would have fit.
const GROW_TIMEOUT: Duration = Duration::from_secs(600);

/// Resize a build pod *up* and block until the kubelet has actuated it — the
/// load-bearing difference from [`resize_to`], which only submits the request.
/// A KEP-1287 resize is asynchronous; if the caller builds the moment the patch
/// returns, the heavy layer runs at the rest limit and is OOM-killed (exit 137).
/// So this polls the actuated limits and resize conditions: target reached →
/// `Ok`; `Deferred` → keep waiting up to [`GROW_TIMEOUT`] (the kubelet retries
/// as capacity frees); `Infeasible` → error immediately; timeout → error. On
/// any error the caller must NOT build.
pub(crate) async fn grow_to(
    client: &Client,
    pod: &str,
    container: &str,
    cpu: &str,
    mem: &str,
) -> Result<(), String> {
    let target_cpu = parse_cpu_milli(cpu)
        .ok_or_else(|| format!("grow {pod}/{container}: unparseable cpu {cpu:?}"))?;
    let target_mem = parse_mem_bytes(mem)
        .ok_or_else(|| format!("grow {pod}/{container}: unparseable mem {mem:?}"))?;

    resize_to(client, pod, container, cpu, mem).await?;

    let api: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    let deadline = Instant::now() + GROW_TIMEOUT;
    let mut noted_deferred = false;
    loop {
        let observed = api
            .get(pod)
            .await
            .map_err(|e| format!("grow {pod}/{container}: poll status: {e}"))?;
        match resize_state(&observed, container, target_cpu, target_mem) {
            ResizeState::Actuated => return Ok(()),
            ResizeState::Infeasible(why) => {
                return Err(format!(
                    "grow {pod}/{container} to {cpu}c/{mem} is infeasible on this node: {why}"
                ));
            }
            ResizeState::Deferred => {
                if !noted_deferred {
                    tracing::info!(
                        target: "ztest::build",
                        pod, cpu, mem,
                        "build-pod grow deferred — waiting for cluster capacity to free"
                    );
                    noted_deferred = true;
                }
            }
            ResizeState::Pending => {}
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "grow {pod}/{container} to {cpu}c/{mem}: resize not actuated within {}s",
                GROW_TIMEOUT.as_secs()
            ));
        }
        tokio::time::sleep(crate::pod_status::POLL_INTERVAL).await;
    }
}

/// Where an in-place resize stands, from an observed pod (see [`grow_to`]).
#[derive(Debug, PartialEq, Eq)]
enum ResizeState {
    /// The container's actuated limits have reached the target.
    Actuated,
    /// Waiting on cluster capacity; the kubelet will retry (`PodResizePending`
    /// reason `Deferred`).
    Deferred,
    /// The node can never satisfy the request (`PodResizePending` reason
    /// `Infeasible`); the string is the kubelet's message.
    Infeasible(String),
    /// Requested but not yet actuated and not blocked — actuation in flight.
    Pending,
}

/// Classify a resize from an observed pod: terminal `Infeasible` wins, then a
/// `Deferred` wait, then the actuated-limits check, else `Pending`. Pure, so the
/// state machine is unit-tested without a cluster. `target_*` are desired cpu
/// (millicores) and memory (bytes); `Actuated` once actuated limits meet both.
fn resize_state(pod: &Pod, container: &str, target_cpu: u64, target_mem: u64) -> ResizeState {
    // k8s 1.33+ dropped `status.resize`; a pending resize now surfaces as a
    // `PodResizePending` condition with reason `Deferred`/`Infeasible`.
    if let Some(cond) = pod
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.type_ == "PodResizePending"))
    {
        if cond.reason.as_deref() == Some("Infeasible") {
            return ResizeState::Infeasible(
                cond.message.clone().unwrap_or_else(|| "infeasible".into()),
            );
        }
        return ResizeState::Deferred;
    }
    let limits = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.name == container))
        .and_then(|c| c.resources.as_ref())
        .and_then(|r| r.limits.as_ref());
    if let Some(limits) = limits {
        let cpu_ok = limits
            .get("cpu")
            .and_then(|q| parse_cpu_milli(&q.0))
            .is_some_and(|c| c >= target_cpu);
        let mem_ok = limits
            .get("memory")
            .and_then(|q| parse_mem_bytes(&q.0))
            .is_some_and(|m| m >= target_mem);
        if cpu_ok && mem_ok {
            return ResizeState::Actuated;
        }
    }
    ResizeState::Pending
}

/// Parse a k8s CPU quantity to millicores: a bare number is whole cores, an `m`
/// suffix is millicores (`"32"`→32000, `"500m"`→500).
fn parse_cpu_milli(s: &str) -> Option<u64> {
    let s = s.trim();
    match s.strip_suffix('m') {
        Some(milli) => milli.trim().parse().ok(),
        None => s.parse::<f64>().ok().map(|cores| (cores * 1000.0).round() as u64),
    }
}

/// Parse a k8s memory quantity to bytes: binary SI (`Ki`/`Mi`/`Gi`/`Ti`/`Pi`/`Ei`,
/// 1024-based), decimal SI (`k`/`M`/`G`/`T`/`P`/`E`, 1000-based), or a bare byte
/// count. Binary suffixes are matched first so `Gi` never falls through to `G`.
fn parse_mem_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    const BIN: &[(&str, u64)] = &[
        ("Ki", 1 << 10),
        ("Mi", 1 << 20),
        ("Gi", 1 << 30),
        ("Ti", 1 << 40),
        ("Pi", 1 << 50),
        ("Ei", 1 << 60),
    ];
    for (suf, mul) in BIN {
        if let Some(n) = s.strip_suffix(suf) {
            return n.trim().parse::<f64>().ok().map(|v| (v * *mul as f64) as u64);
        }
    }
    const DEC: &[(&str, f64)] = &[
        ("E", 1e18),
        ("P", 1e15),
        ("T", 1e12),
        ("G", 1e9),
        ("M", 1e6),
        ("k", 1e3),
    ];
    for (suf, mul) in DEC {
        if let Some(n) = s.strip_suffix(suf) {
            return n.trim().parse::<f64>().ok().map(|v| (v * *mul) as u64);
        }
    }
    s.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        ContainerStatus, PodCondition, PodStatus, ResourceRequirements,
    };
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

    #[test]
    fn cpu_parses_cores_and_millicores() {
        assert_eq!(parse_cpu_milli("32"), Some(32_000));
        assert_eq!(parse_cpu_milli("1"), Some(1_000));
        assert_eq!(parse_cpu_milli("500m"), Some(500));
        assert_eq!(parse_cpu_milli(" 16 "), Some(16_000));
    }

    #[test]
    fn mem_parses_binary_decimal_and_bytes() {
        assert_eq!(parse_mem_bytes("24Gi"), Some(24 * (1 << 30)));
        assert_eq!(parse_mem_bytes("500Mi"), Some(500 * (1 << 20)));
        // 500Mi expressed as decimal kilobytes, as the kubelet often reports it.
        assert_eq!(parse_mem_bytes("524288k"), Some(524_288_000));
        assert_eq!(parse_mem_bytes("25769803776"), Some(24 * (1 << 30)));
        // A `Gi` value must not be mis-parsed by the `G` decimal rule.
        assert_ne!(parse_mem_bytes("24Gi"), parse_mem_bytes("24G"));
    }

    fn pod_with(limits: Option<(&str, &str)>, pending: Option<&str>) -> Pod {
        let mut status = PodStatus::default();
        if let Some(reason) = pending {
            status.conditions = Some(vec![PodCondition {
                type_: "PodResizePending".into(),
                status: "True".into(),
                reason: Some(reason.into()),
                message: Some(format!("{reason} detail")),
                ..Default::default()
            }]);
        }
        if let Some((cpu, mem)) = limits {
            status.container_statuses = Some(vec![ContainerStatus {
                name: "buildkit".into(),
                image: "img".into(),
                image_id: String::new(),
                ready: true,
                restart_count: 0,
                resources: Some(ResourceRequirements {
                    limits: Some(
                        [
                            ("cpu".to_string(), Quantity(cpu.to_string())),
                            ("memory".to_string(), Quantity(mem.to_string())),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }]);
        }
        Pod {
            status: Some(status),
            ..Default::default()
        }
    }

    // Target: 32 cores / 24 GiB.
    const TCPU: u64 = 32_000;
    const TMEM: u64 = 24 * (1 << 30);

    #[test]
    fn actuated_when_limits_reach_target() {
        let p = pod_with(Some(("32", "25769803776")), None);
        assert_eq!(resize_state(&p, "buildkit", TCPU, TMEM), ResizeState::Actuated);
    }

    #[test]
    fn pending_while_limits_still_at_rest() {
        // Rest limits, no condition yet: requested but not actuated.
        let p = pod_with(Some(("1", "524288k")), None);
        assert_eq!(resize_state(&p, "buildkit", TCPU, TMEM), ResizeState::Pending);
    }

    #[test]
    fn deferred_and_infeasible_from_conditions() {
        let d = pod_with(Some(("1", "524288k")), Some("Deferred"));
        assert_eq!(resize_state(&d, "buildkit", TCPU, TMEM), ResizeState::Deferred);
        let i = pod_with(Some(("1", "524288k")), Some("Infeasible"));
        assert!(matches!(
            resize_state(&i, "buildkit", TCPU, TMEM),
            ResizeState::Infeasible(_)
        ));
    }
}
