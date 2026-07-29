//! Execute one test in a sibling runner pod instead of a local child process, so
//! the heavy wallet compute runs in-cluster and the test is hermetic (it sees
//! only its own per-test namespace). Delivery is decoupled: this module takes
//! ready volumes/mounts plus a local→pod [`PodRunConfig::path_map`], so the same
//! code serves a `kind` hostPath mount and a remote image-layer/PVC unchanged.

use std::collections::BTreeMap;
use std::time::Instant;

use k8s_openapi::api::core::v1 as corev1;
use kube::api::{Api, DeleteParams, LogParams, ObjectMeta, PostParams};

use crate::cancel::Cancel;
use crate::engine::events::Verdict;
use crate::engine::local_runner::{EngineEnv, Executor, OutcomeFuture, TestOutcome};
use crate::engine::plan::WorkItem;

use crate::pod_status::{
    IMAGE_PULL_GRACE, POLL_INTERVAL, PodPhases, exit_code, image_error, pod_phases,
    pull_error_is_terminal,
};

/// Image repo of the baked tests image (`docs/design-remote-execution.md`).
pub const RUNNER_REPO: &str = "ztest-runner";

/// Everything the pod executor needs that isn't per-test: which cluster/namespace
/// to run in, the runner image, and how build-output paths map from the laptop to
/// the pod.
#[derive(Debug, Clone)]
pub struct PodRunConfig {
    /// Namespace runner pods are created in (the per-test/per-run namespace).
    pub namespace: String,
    /// Runner image reference (the buildkit-built `ztest-runner` image; see
    /// `docker/runner.Dockerfile`).
    pub image: String,
    /// `imagePullPolicy` — `"Never"` for a `kind`-loaded image, `"IfNotPresent"`
    /// for a registry-hosted one. `None` leaves the cluster default.
    pub image_pull_policy: Option<String>,
    /// ServiceAccount the runner pod runs as; it needs RBAC to create the test's
    /// sibling component pods. `None` uses the namespace default.
    pub service_account: Option<String>,
    /// Volumes delivering the build outputs (and the test cwd) into the pod.
    pub volumes: Vec<corev1::Volume>,
    /// Mounts pairing [`Self::volumes`] to their in-pod paths.
    pub volume_mounts: Vec<corev1::VolumeMount>,
    /// Local-path-prefix → pod-path-prefix rewrites, applied to the binary path,
    /// the cwd, and each `LD_LIBRARY_PATH` entry. Longest prefix wins; unmatched
    /// paths (e.g. `/nix/store/…`) pass through unchanged. Order does not matter.
    pub path_map: Vec<(String, String)>,
    /// Resolved `DevImageId → pull reference` (the build manifest) for the run's
    /// dev component images, serialized into the pod as [`image::IMAGE_REFS_ENV`]
    /// so an in-pod test resolves them by their path-free id without touching a
    /// Dockerfile the baked image doesn't carry.
    pub image_refs: BTreeMap<String, String>,
    /// Shared per-run env (dylib search path, run id, SA, no-cleanup).
    pub env: EngineEnv,
}

/// Executes each test in its own runner pod.
pub struct PodExecutor {
    client: kube::Client,
    cfg: PodRunConfig,
}

// `kube::Client` is not `Debug`; the config carries the identifying detail.
impl std::fmt::Debug for PodExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PodExecutor")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl PodRunConfig {
    /// A hostPath-delivery config for local (`kind`) runs: the node's workspace at
    /// `node_workspace` is mounted read-only into the pod at the *same* absolute
    /// path it has on the laptop (`local_workspace`), so binary/cwd/search paths
    /// resolve unchanged (empty `path_map`) and `/nix/store` comes from the image.
    #[allow(clippy::too_many_arguments)]
    pub fn hostpath(
        env: EngineEnv,
        image: String,
        namespace: String,
        local_workspace: String,
        node_workspace: String,
        service_account: Option<String>,
        image_refs: BTreeMap<String, String>,
    ) -> Self {
        let volume = corev1::Volume {
            name: "workspace".to_string(),
            host_path: Some(corev1::HostPathVolumeSource {
                path: node_workspace,
                type_: Some("Directory".to_string()),
            }),
            ..Default::default()
        };
        let mount = corev1::VolumeMount {
            name: "workspace".to_string(),
            mount_path: local_workspace,
            read_only: Some(true),
            ..Default::default()
        };
        Self {
            namespace,
            image,
            image_pull_policy: Some("Never".to_string()),
            service_account,
            volumes: vec![volume],
            volume_mounts: vec![mount],
            path_map: Vec::new(),
            image_refs,
            env,
        }
    }

    /// A baked-delivery config for remote runs: the build outputs are already in
    /// `image` at their original absolute paths (`docs/design-remote-execution.md`
    /// §2), so no volume is mounted and paths resolve unchanged.
    pub fn baked(
        env: EngineEnv,
        image: String,
        namespace: String,
        service_account: Option<String>,
        image_refs: BTreeMap<String, String>,
    ) -> Self {
        Self {
            namespace,
            image,
            image_pull_policy: Some("IfNotPresent".to_string()),
            service_account,
            volumes: Vec::new(),
            volume_mounts: Vec::new(),
            path_map: Vec::new(),
            image_refs,
            env,
        }
    }
}

impl PodExecutor {
    pub fn new(client: kube::Client, cfg: PodRunConfig) -> Self {
        Self { client, cfg }
    }
}

impl Executor for PodExecutor {
    fn run(&self, item: WorkItem, cancel: Cancel) -> OutcomeFuture {
        let client = self.client.clone();
        let cfg = self.cfg.clone();
        Box::pin(async move { run_in_pod(client, cfg, item, cancel).await })
    }
}

async fn run_in_pod(
    client: kube::Client,
    cfg: PodRunConfig,
    item: WorkItem,
    cancel: Cancel,
) -> TestOutcome {
    let started = Instant::now();
    let name = pod_name(&item);

    // The laptop owns the per-test namespace on the pod path: it picks the name,
    // creates it here, fetches every pod's logs at the test's terminal, and tears
    // it down after — so a definitive `api.logs` fetch (no live follow) can't race
    // an in-pod delete. The in-pod `TestEnv::build` reads `ZTEST_TEST_NAMESPACE`
    // (injected below) and skips its own namespace create + teardown.
    let coords = match crate::naming::RunCoords::from_env() {
        Ok(c) => c,
        Err(e) => {
            return TestOutcome {
                verdict: Verdict::SpawnError,
                output: format!("resolve run coords: {e}").into_bytes(),
                duration: started.elapsed(),
            };
        }
    };
    let test_ns = crate::naming::namespace_for(
        &item.binary_id,
        &item.test_name,
        &crate::naming::test_suffix(),
    );

    let runner_api: Api<corev1::Pod> = Api::namespaced(client.clone(), &cfg.namespace);

    if let Err(e) =
        crate::cluster::ensure_namespace(&client, &test_ns, &coords, &item.binary_id, &item.test_name)
            .await
    {
        return TestOutcome {
            verdict: Verdict::SpawnError,
            output: format!("create test namespace {test_ns}: {e}").into_bytes(),
            duration: started.elapsed(),
        };
    }

    let pod = build_pod(&name, &cfg, &item, &test_ns);
    if let Err(e) = runner_api.create(&PostParams::default(), &pod).await {
        teardown(&client, &cfg, &test_ns, &name).await;
        return TestOutcome {
            verdict: Verdict::SpawnError,
            output: format!("create runner pod {name}: {e}").into_bytes(),
            duration: started.elapsed(),
        };
    }

    let hard_cap = item.hard_cap;
    // When this pod's containers first entered an image-pull error, so a transient
    // storm can be waited out for `IMAGE_PULL_GRACE` before it's declared terminal.
    let mut pull_error_since: Option<Instant> = None;
    // The most recent full pod observation, retained so the terminal timing
    // breakdown can read the kube-server phase timestamps (`pod_phases`).
    let mut last_pod: Option<corev1::Pod> = None;
    let done = loop {
        tokio::select! {
            _ = tokio::time::sleep(POLL_INTERVAL) => {
                if let Ok(p) = runner_api.get(&name).await {
                    let terminal = terminal_state(&p);
                    last_pod = Some(p);
                    if let Some(st) = terminal {
                        break Done::Reached(st);
                    }
                    let status = last_pod.as_ref().and_then(|p| p.status.as_ref());
                    match status.and_then(image_error) {
                        Some(reason) => {
                            let first = *pull_error_since.get_or_insert_with(Instant::now);
                            if pull_error_is_terminal(&reason, first, Instant::now(), IMAGE_PULL_GRACE) {
                                break Done::Reached(TerminalState::ImageError(reason));
                            }
                        }
                        // Recovered (pull finally progressed): reset the window.
                        None => pull_error_since = None,
                    }
                }
                if started.elapsed() >= hard_cap {
                    break Done::Timeout;
                }
            }
            _ = cancel.cancelled() => break Done::Cancelled,
        }
    };
    let total = started.elapsed();
    emit_timing(&item.test_name, last_pod.as_ref(), total);

    // Fetch every log definitively, before anything is deleted, while the
    // namespace and its pods still exist: the runner pod's own libtest-framed
    // stdout+stderr (a running pod on the timeout/cancel path still serves it),
    // the component-pod lines (timestamped), and any dead component pod's terminal
    // reason (OOMKilled/Evicted vs a panic). `unified_output` weaves them into one
    // timeline with the assertion pinned at the end; teardown follows.
    let runner_raw = runner_api
        .logs(&name, &LogParams::default())
        .await
        .unwrap_or_default()
        .into_bytes();
    let dead = crate::cluster::dead_pod_report(&client, &test_ns).await;
    let components = crate::logstream::fetch_component_lines(&client, &test_ns).await;
    let unified = crate::logstream::unified_output(
        &runner_raw,
        &item.test_name,
        components,
        &dead,
        cfg.env.color,
    );

    if !cfg.env.no_cleanup {
        teardown(&client, &cfg, &test_ns, &name).await;
    } else {
        // Mirror the local path's `--no-cleanup` behaviour: leave the namespace,
        // its pods, and the runner pod for a post-mortem. The 1h `janitor/ttl`
        // annotation still reaps the namespace, so this never leaks permanently.
        tracing::warn!(
            target: "ztest::pod",
            namespace = %test_ns,
            runner_pod = %name,
            "--no-cleanup: preserving per-test namespace and runner pod for inspection (janitor reaps in ~1h)"
        );
    }

    let (verdict, output) = match done {
        Done::Reached(TerminalState::Passed) => (Verdict::Pass, unified),
        Done::Reached(TerminalState::Failed(code)) => (Verdict::Fail(code), unified),
        Done::Reached(TerminalState::ImageError(reason)) => {
            // The pod produced no logs; surface the pull failure as the output so
            // the reporter shows why, not a blank SpawnError.
            (
                Verdict::SpawnError,
                format!("runner image error: {reason}").into_bytes(),
            )
        }
        Done::Timeout => (Verdict::Timeout, unified),
        Done::Cancelled => (Verdict::Terminated, unified),
    };

    TestOutcome {
        verdict,
        output,
        duration: started.elapsed(),
    }
}

/// Tear down one test's cluster footprint (pod path): the cluster-scoped shadow
/// VSCs it minted (by the per-test-ns label — they don't cascade with the
/// namespace), then the per-test namespace (cascading its component pods, PVCs,
/// and quota), then the runner pod itself (it lives in the run namespace, not the
/// per-test one, so the namespace delete doesn't reach it). All best-effort;
/// `reap_run` by `run-id` is the crash-safety net if this process dies first.
async fn teardown(client: &kube::Client, cfg: &PodRunConfig, test_ns: &str, runner_pod: &str) {
    crate::cluster::delete_shadow_vscs_for_ns(client, test_ns).await;
    let _ = crate::cluster::delete_namespace(client, test_ns).await;
    let api: Api<corev1::Pod> = Api::namespaced(client.clone(), &cfg.namespace);
    let _ = api.delete(runner_pod, &DeleteParams::default()).await;
}

/// How the pod-await loop finished.
enum Done {
    Reached(TerminalState),
    Timeout,
    Cancelled,
}

/// A pod's terminal observation. Distinct from [`Verdict`] so the image-error
/// reason survives to the outcome's output.
#[derive(Debug, PartialEq, Eq)]
enum TerminalState {
    Passed,
    Failed(i32),
    ImageError(String),
}

/// Map a pod's *settled* state (Succeeded/Failed) to a terminal state, or `None`
/// while pending/running. Image-pull errors are handled separately with a grace
/// window (see [`pull_error_is_terminal`](crate::pod_status::pull_error_is_terminal)):
/// they are frequently transient, so treating them as terminal here would fail a
/// test on a recoverable pull-throttle storm.
fn terminal_state(pod: &corev1::Pod) -> Option<TerminalState> {
    let status = pod.status.as_ref()?;
    match status.phase.as_deref() {
        Some("Succeeded") => Some(TerminalState::Passed),
        Some("Failed") => Some(TerminalState::Failed(exit_code(status).unwrap_or(-1))),
        _ => None,
    }
}

/// A DNS-safe runner-pod name, unique per *creation*. libtest names contain `::`
/// and mixed case, neither DNS-label-legal, so slugify the test name for a
/// human-readable prefix and suffix with a random token for uniqueness.
///
/// The suffix is random (not a hash of the test identity) on purpose: runner
/// pods share one namespace and are reaped by `LABEL_RUN_ID`, not by name, so the
/// name only needs to never collide. A deterministic name 409s whenever another
/// pod of the same test already exists — a concurrent run, a retry whose prior
/// attempt is still terminating, or a run killed mid-flight that leaked a pod its
/// deferred delete never removed. A fresh token per creation rules all three out.
/// This mirrors the per-test *namespace*, which is likewise randomized per
/// `TestEnv` (`naming::test_suffix`).
fn pod_name(item: &WorkItem) -> String {
    let mut slug = String::new();
    for c in item.test_name.chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug: String = slug.trim_matches('-').chars().take(40).collect();
    let uniq: u32 = rand::random();
    format!(
        "ztest-run-{}-{uniq:08x}",
        if slug.is_empty() { "t" } else { &slug }
    )
}

/// Generate the Dockerfile for a baked tests image: the nix runner `base` (its
/// closure gives glibc/libstdc++/rocksdb/rustc-std at the store paths the binary
/// references) plus one `COPY` layer per selected test binary, landing at the
/// absolute path it occupies on the laptop so [`PodRunConfig::baked`] needs no
/// volume and no path rewriting.
///
/// One layer per binary (not a single `COPY .`) so `docker push` uploads only
/// the blobs whose binary changed — the registry already has the rest. Each
/// `binaries` entry is a filename staged under the context's `deps/`; it lands
/// at `<dest_abs>/deps/<name>`, matching `WorkItem::binary_path`.
///
/// Rewrite `path` by the longest matching prefix in `map`; unmatched paths (e.g.
/// `/nix/store/…`, present in the image) pass through unchanged.
fn remap(path: &str, map: &[(String, String)]) -> String {
    map.iter()
        .filter(|(from, _)| path == from.as_str() || path.starts_with(&format!("{from}/")))
        .max_by_key(|(from, _)| from.len())
        .map(|(from, to)| format!("{to}{}", &path[from.len()..]))
        .unwrap_or_else(|| path.to_string())
}

/// Remap a `:`-separated search path (the dylib env value) entry by entry.
fn remap_search_path(value: &str, map: &[(String, String)]) -> String {
    value
        .split(':')
        .filter(|s| !s.is_empty())
        .map(|entry| remap(entry, map))
        .collect::<Vec<_>>()
        .join(":")
}

fn build_pod(name: &str, cfg: &PodRunConfig, item: &WorkItem, test_ns: &str) -> corev1::Pod {
    let bin = remap(&item.binary_path.to_string_lossy(), &cfg.path_map);
    let cwd = remap(&item.cwd.to_string_lossy(), &cfg.path_map);
    let ld = remap_search_path(&cfg.env.dylib_path.to_string_lossy(), &cfg.path_map);

    let mut env = vec![
        env_var(crate::engine::dylib::dylib_path_envvar(), &ld),
        env_var("NEXTEST", "1"),
        env_var("NEXTEST_EXECUTION_MODE", "process-per-test"),
        env_var("NEXTEST_RUN_ID", &cfg.env.run_id),
        env_var("CARGO_MANIFEST_DIR", &cwd),
        // Mark the child as orchestrated (parent owns capacity admission);
        // a `TestEnv` refuses to provision outside a `ztest run`.
        env_var("ZTEST_ENGINE", "1"),
        env_var("ZTEST_SA", &cfg.env.sa),
        // The laptop-created per-test namespace this pod's `TestEnv` provisions
        // into; it reads this instead of inventing a name, and skips namespace
        // create + teardown (the laptop owns both). See `naming::TEST_NAMESPACE_ENV`.
        env_var(crate::naming::TEST_NAMESPACE_ENV, test_ns),
    ];
    if cfg.env.no_cleanup {
        env.push(env_var("ZTEST_NO_CLEANUP", "1"));
    }
    // Forward the laptop's diagnostics filter so the in-pod subscriber
    // (`observ::init_in_pod`) honours the same `ZTEST_LOG` the operator set,
    // mirroring nextest's `NEXTEST_LOG`. Unset → the pod keeps its own default.
    if let Some(filter) = &cfg.env.ztest_log {
        env.push(env_var("ZTEST_LOG", filter));
    }
    // Hand the in-pod test the laptop's resolved component-image references so it
    // resolves them without a Dockerfile it doesn't have (see `image::resolve`).
    if !cfg.image_refs.is_empty()
        && let Ok(json) = serde_json::to_string(&cfg.image_refs)
    {
        env.push(env_var(crate::backends::image::IMAGE_REFS_ENV, &json));
    }

    // The run-id label is load-bearing: the parent's `reap_run` teardown
    // (`cli/run.rs` Ctrl-C path) deletes every resource matching it, so a runner
    // pod is cleaned up even if this process is killed mid-run.
    let labels = BTreeMap::from([(crate::qos::LABEL_RUN_ID.to_string(), cfg.env.run_id.clone())]);

    // Guaranteed QoS: the runner pod (the test binary + any in-process wallet)
    // is sized at its tier's runner footprint, rendered `requests == limits`
    // with whole-core CPU — never BestEffort. See `qos::QosProfile::runner`.
    let resources = guaranteed_resources(item.class.profile().runner);

    let container = corev1::Container {
        name: "test".to_string(),
        image: Some(cfg.image.clone()),
        image_pull_policy: cfg.image_pull_policy.clone(),
        command: Some(vec![
            bin,
            "--exact".to_string(),
            item.test_name.clone(),
            "--nocapture".to_string(),
        ]),
        working_dir: Some(cwd),
        env: Some(env),
        volume_mounts: Some(cfg.volume_mounts.clone()),
        resources: Some(resources),
        ..Default::default()
    };

    corev1::Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(cfg.namespace.clone()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(corev1::PodSpec {
            restart_policy: Some("Never".to_string()),
            service_account_name: cfg.service_account.clone(),
            containers: vec![container],
            volumes: Some(cfg.volumes.clone()),
            // Pinned Guaranteed pod on a bare `restartPolicy: Never` Pod: a lost
            // node must delete it immediately (it can't migrate without losing
            // its pinned CPUs), not sit through the default 300 s grace.
            tolerations: Some(fast_evict_tolerations()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// A Guaranteed (`requests == limits`) container `resources` block sized at
/// `footprint`, via the single QoS lowering ([`Resources::guaranteed_cpu_mem`]).
/// Panics on a degenerate footprint (that lowering's guard).
fn guaranteed_resources(footprint: crate::qos::Resources) -> corev1::ResourceRequirements {
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    let (cpu, mem) = footprint.guaranteed_cpu_mem("runner pod footprint");
    let amounts = BTreeMap::from([
        ("cpu".to_string(), Quantity(cpu)),
        ("memory".to_string(), Quantity(mem)),
    ]);
    corev1::ResourceRequirements {
        requests: Some(amounts.clone()),
        limits: Some(amounts),
        ..Default::default()
    }
}

/// The immediate-eviction tolerations a pinned Guaranteed pod carries so a lost
/// node deletes it at once (`tolerationSeconds: 0`) rather than after the 300 s
/// default. Mirrors [`crate::manifest::PodSpec::render`]'s component-pod path.
fn fast_evict_tolerations() -> Vec<corev1::Toleration> {
    [
        "node.kubernetes.io/not-ready",
        "node.kubernetes.io/unreachable",
    ]
    .into_iter()
    .map(|key| corev1::Toleration {
        key: Some(key.to_string()),
        operator: Some("Exists".to_string()),
        effect: Some("NoExecute".to_string()),
        toleration_seconds: Some(0),
        ..Default::default()
    })
    .collect()
}

/// Emit the runner pod's lifecycle latency breakdown on the `ztest::pod`
/// diagnostics target (see [`observ`](crate::observ)). The kube-server phase
/// timestamps isolate scheduler-queue wait, image pull + container init, and the
/// test body itself; `overhead_ms` is the remainder of the laptop-observed wall
/// — create-call latency plus the ≤`POLL_INTERVAL` lag before a settled state is
/// noticed. This is the signal for "test slow but cluster idle": a large
/// `pull_init_ms`/`schedule_ms` with a small `body_ms` is time spent waiting on
/// the cluster, not computing.
fn emit_timing(test: &str, pod: Option<&corev1::Pod>, total: std::time::Duration) {
    let phases = pod.map(pod_phases).unwrap_or(PodPhases {
        created: None,
        scheduled: None,
        container_started: None,
        container_finished: None,
    });
    let ms = |d: Option<std::time::Duration>| d.unwrap_or_default().as_millis() as u64;
    let accounted: std::time::Duration = [phases.schedule(), phases.pull_init(), phases.body()]
        .into_iter()
        .flatten()
        .sum();
    tracing::debug!(
        target: "ztest::pod",
        test = %test,
        total_ms = total.as_millis() as u64,
        schedule_ms = ms(phases.schedule()),
        pull_init_ms = ms(phases.pull_init()),
        body_ms = ms(phases.body()),
        overhead_ms = total.saturating_sub(accounted).as_millis() as u64,
        "runner pod lifecycle"
    );
}

fn env_var(name: &str, value: &str) -> corev1::EnvVar {
    corev1::EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    fn map() -> Vec<(String, String)> {
        vec![("/home/u/proj/target".into(), "/work/target".into())]
    }

    #[test]
    fn remap_rewrites_under_prefix_and_passes_through_nix() {
        assert_eq!(
            remap("/home/u/proj/target/debug/deps/foo-abc", &map()),
            "/work/target/debug/deps/foo-abc"
        );
        // /nix/store paths are present in the image; never rewritten.
        assert_eq!(
            remap("/nix/store/abc-glibc/lib", &map()),
            "/nix/store/abc-glibc/lib"
        );
    }

    #[test]
    fn remap_longest_prefix_wins() {
        let m = vec![("/a".into(), "/x".into()), ("/a/b".into(), "/y".into())];
        assert_eq!(remap("/a/b/c", &m), "/y/c");
        assert_eq!(remap("/a/z", &m), "/x/z");
    }

    #[test]
    fn remap_does_not_match_partial_component() {
        // "/home/u/proj/targetx" must not match the "/home/u/proj/target" prefix.
        assert_eq!(
            remap("/home/u/proj/targetx/y", &map()),
            "/home/u/proj/targetx/y"
        );
    }

    #[test]
    fn search_path_remaps_each_entry_and_keeps_nix() {
        let v = "/home/u/proj/target/debug/deps:/nix/store/g/lib:/home/u/proj/target/debug";
        assert_eq!(
            remap_search_path(v, &map()),
            "/work/target/debug/deps:/nix/store/g/lib:/work/target/debug"
        );
    }

    fn work(bin: &str, test: &str) -> WorkItem {
        WorkItem {
            binary_id: bin.to_string(),
            test_name: test.to_string(),
            binary_path: PathBuf::new(),
            cwd: PathBuf::new(),
            class: crate::qos::QosClass::Basic,
            footprint: crate::qos::Resources::ZERO,
            priority: 0,
            hard_cap: Duration::from_secs(1),
            retries: 0,
            deps: Vec::new(),
        }
    }

    fn work_in_tier(class: crate::qos::QosClass) -> WorkItem {
        WorkItem {
            class,
            ..work("crate::b", "t")
        }
    }

    #[test]
    fn runner_pod_is_guaranteed_and_sized_from_the_tier_runner_footprint() {
        use crate::qos::QosClass;
        let env = EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run_id: "r".into(),
            sa: "ztest".into(),
            no_cleanup: false,
            capture: true,
            color: false,
            ztest_log: None,
        };
        let cfg = PodRunConfig::baked(
            env,
            "runner:dev".into(),
            "ztest".into(),
            None,
            BTreeMap::new(),
        );

        // A regtest/integration tier runner: one whole core (orchestration).
        let pod = build_pod("p", &cfg, &work_in_tier(QosClass::Integration), "ztest-test-ns");
        let c = &pod.spec.as_ref().unwrap().containers[0];
        let res = c.resources.as_ref().expect("runner pod must be sized");
        let req = res.requests.as_ref().unwrap();
        let lim = res.limits.as_ref().unwrap();
        // Guaranteed: requests == limits, in every dimension present.
        assert_eq!(
            req, lim,
            "runner pod must be Guaranteed (requests == limits)"
        );
        assert_eq!(req["cpu"].0, "1");

        // A wallet tier keeps the in-process wallet's compute here (≥4 cores).
        let pod = build_pod("p", &cfg, &work_in_tier(QosClass::Wallet), "ztest-test-ns");
        let c = &pod.spec.as_ref().unwrap().containers[0];
        let req = c.resources.as_ref().unwrap().requests.as_ref().unwrap();
        assert_eq!(req["cpu"].0, "4");
    }

    #[test]
    fn runner_pod_evicts_immediately_on_node_loss() {
        let env = EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run_id: "r".into(),
            sa: "ztest".into(),
            no_cleanup: false,
            capture: true,
            color: false,
            ztest_log: None,
        };
        let cfg = PodRunConfig::baked(
            env,
            "runner:dev".into(),
            "ztest".into(),
            None,
            BTreeMap::new(),
        );
        let pod = build_pod("p", &cfg, &work("crate::b", "t"), "ztest-test-ns");
        let tols = pod.spec.unwrap().tolerations.unwrap();
        let nr = tols
            .iter()
            .find(|t| t.key.as_deref() == Some("node.kubernetes.io/not-ready"))
            .expect("not-ready toleration");
        assert_eq!(nr.effect.as_deref(), Some("NoExecute"));
        assert_eq!(nr.toleration_seconds, Some(0));
    }

    #[test]
    fn pod_name_is_dns_safe_and_readable() {
        let a = pod_name(&work("crate::b", "mod::Test_Case"));
        assert!(a.starts_with("ztest-run-mod-test-case-"));
        assert!(
            a.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        );
    }

    #[test]
    fn pod_name_is_unique_per_creation() {
        // The *same* test names distinctly on each call — the property that keeps
        // concurrent runs, retries, and crash-leftovers from 409-colliding.
        let item = work("crate::b", "mod::same_test");
        assert_ne!(pod_name(&item), pod_name(&item));
    }

    fn pod_with(phase: Option<&str>, exit: Option<i32>, waiting: Option<&str>) -> corev1::Pod {
        let cs = corev1::ContainerStatus {
            name: "test".into(),
            image: "img".into(),
            image_id: String::new(),
            ready: false,
            restart_count: 0,
            state: Some(corev1::ContainerState {
                terminated: exit.map(|code| corev1::ContainerStateTerminated {
                    exit_code: code,
                    ..Default::default()
                }),
                waiting: waiting.map(|r| corev1::ContainerStateWaiting {
                    reason: Some(r.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        corev1::Pod {
            status: Some(corev1::PodStatus {
                phase: phase.map(String::from),
                container_statuses: Some(vec![cs]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn state_pending_is_none() {
        assert!(terminal_state(&pod_with(Some("Pending"), None, None)).is_none());
        assert!(terminal_state(&pod_with(Some("Running"), None, None)).is_none());
    }

    #[test]
    fn state_success_and_failure() {
        assert_eq!(
            terminal_state(&pod_with(Some("Succeeded"), Some(0), None)),
            Some(TerminalState::Passed)
        );
        assert_eq!(
            terminal_state(&pod_with(Some("Failed"), Some(101), None)),
            Some(TerminalState::Failed(101))
        );
    }

    #[test]
    fn pull_error_is_not_settled_state() {
        // A pull error is no longer folded into `terminal_state`; the run loop
        // handles it with a grace window instead.
        let p = pod_with(Some("Pending"), None, Some("ImagePullBackOff"));
        assert!(terminal_state(&p).is_none());
        assert_eq!(
            image_error(p.status.as_ref().unwrap()).as_deref(),
            Some("ImagePullBackOff")
        );
    }

    #[test]
    fn build_pod_carries_image_refs_env() {
        let env = EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run_id: "r".into(),
            sa: "ztest".into(),
            no_cleanup: false,
            capture: true,
            color: false,
            ztest_log: None,
        };
        let mut refs = BTreeMap::new();
        refs.insert(
            "k".to_string(),
            "reg.svc:5000/ns/zainod:dev-abc".to_string(),
        );
        let cfg = PodRunConfig::baked(env, "runner:dev".into(), "ztest".into(), None, refs);
        let pod = build_pod("p", &cfg, &work("crate::b", "t"), "ztest-test-ns");
        let vars = pod.spec.unwrap().containers[0].env.clone().unwrap();
        let refs_var = vars
            .iter()
            .find(|v| v.name == crate::backends::image::IMAGE_REFS_ENV)
            .expect("IMAGE_REFS_ENV set");
        assert!(
            refs_var
                .value
                .as_deref()
                .unwrap()
                .contains("zainod:dev-abc")
        );
    }

    #[test]
    fn build_pod_omits_image_refs_env_when_empty() {
        let env = EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run_id: "r".into(),
            sa: "ztest".into(),
            no_cleanup: false,
            capture: true,
            color: false,
            ztest_log: None,
        };
        let cfg = PodRunConfig::baked(
            env,
            "runner:dev".into(),
            "ztest".into(),
            None,
            BTreeMap::new(),
        );
        let pod = build_pod("p", &cfg, &work("crate::b", "t"), "ztest-test-ns");
        let vars = pod.spec.unwrap().containers[0].env.clone().unwrap();
        assert!(
            !vars
                .iter()
                .any(|v| v.name == crate::backends::image::IMAGE_REFS_ENV)
        );
    }

    #[test]
    fn build_pod_forwards_ztest_log_when_set() {
        let env = EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run_id: "r".into(),
            sa: "ztest".into(),
            no_cleanup: false,
            capture: true,
            color: false,
            ztest_log: Some("ztest::build=debug".into()),
        };
        let cfg = PodRunConfig::baked(
            env,
            "runner:dev".into(),
            "ztest".into(),
            None,
            BTreeMap::new(),
        );
        let pod = build_pod("p", &cfg, &work("crate::b", "t"), "ztest-test-ns");
        let vars = pod.spec.unwrap().containers[0].env.clone().unwrap();
        let log = vars
            .iter()
            .find(|v| v.name == "ZTEST_LOG")
            .expect("ZTEST_LOG forwarded");
        assert_eq!(log.value.as_deref(), Some("ztest::build=debug"));
    }

    #[test]
    fn build_pod_omits_ztest_log_when_unset() {
        let env = EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run_id: "r".into(),
            sa: "ztest".into(),
            no_cleanup: false,
            capture: true,
            color: false,
            ztest_log: None,
        };
        let cfg = PodRunConfig::baked(
            env,
            "runner:dev".into(),
            "ztest".into(),
            None,
            BTreeMap::new(),
        );
        let pod = build_pod("p", &cfg, &work("crate::b", "t"), "ztest-test-ns");
        let vars = pod.spec.unwrap().containers[0].env.clone().unwrap();
        assert!(!vars.iter().any(|v| v.name == "ZTEST_LOG"));
    }

    #[test]
    fn build_pod_injects_the_laptop_chosen_test_namespace() {
        // The parent picks the per-test namespace name and injects it; the in-pod
        // `TestEnv` reads exactly this instead of inventing its own, so the pod
        // provisions into the same namespace the parent follows and tears down.
        let env = EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run_id: "r".into(),
            sa: "ztest".into(),
            no_cleanup: false,
            capture: true,
            color: false,
            ztest_log: None,
        };
        let cfg = PodRunConfig::baked(env, "runner:dev".into(), "ztest".into(), None, BTreeMap::new());
        let pod = build_pod("p", &cfg, &work("crate::b", "t"), "ztest-pkg-t-abcd1234");
        let vars = pod.spec.unwrap().containers[0].env.clone().unwrap();
        let ns = vars
            .iter()
            .find(|v| v.name == crate::naming::TEST_NAMESPACE_ENV)
            .expect("ZTEST_TEST_NAMESPACE injected");
        assert_eq!(ns.value.as_deref(), Some("ztest-pkg-t-abcd1234"));
    }
}
