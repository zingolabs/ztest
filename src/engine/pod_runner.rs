//! Execute one test in a sibling runner pod instead of a local child process.
//!
//! The runner pod runs the on-cluster-baked runner image (the compiled test
//! binaries `crane`-appended onto the Debian runner base,
//! `docker/runner-base.Dockerfile`), with the build outputs delivered via
//! [`PodRunConfig::volumes`] and the binary launched exactly as the local
//! executor launches it (`--exact <name> --nocapture`). This gives the two
//! properties the local executor can't on a remote cluster: the heavy wallet
//! compute runs in-cluster, and the whole test is hermetic — it can only see its
//! own per-test namespace, not the laptop's environment.
//!
//! Delivery is decoupled: this module takes ready volumes/mounts plus a
//! local→pod [`PodRunConfig::path_map`], so the same code serves a `kind`
//! hostPath mount and a remote image-layer/PVC without change. Paths under a
//! mapped prefix are rewritten; `/nix/store` paths pass through (present in the
//! image).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1 as corev1;
use kube::api::{Api, DeleteParams, LogParams, ObjectMeta, PostParams};

use crate::cancel::Cancel;
use crate::engine::events::Verdict;
use crate::engine::local_runner::{EngineEnv, Executor, OutcomeFuture, TestOutcome};
use crate::engine::plan::WorkItem;

/// How often the pod's phase is polled while awaiting a terminal state.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How long a pod may sit in a *transient* image-pull error before it's declared
/// terminal. The kubelet retries pulls with backoff, so a transient cause — most
/// visibly a `pull QPS exceeded` storm when many pods launch at once on a single
/// node, which clears the moment the first pod warms the node's image cache
/// (`imagePullPolicy: IfNotPresent`) — self-heals well within this window.
/// `InvalidImageName` is never transient and ignores the grace (see
/// [`pull_error_is_terminal`]); a genuinely absent image fails after the grace.
const IMAGE_PULL_GRACE: Duration = Duration::from_secs(90);

/// Image repo of the baked tests image (`docs/remote-test-execution.md`). The
/// on-cluster `crane` bake appends the compiled binaries onto the runner base and
/// pushes under this repo.
pub const RUNNER_REPO: &str = "ztest-runner";

/// Everything the pod executor needs that isn't per-test: which cluster/namespace
/// to run in, the runner image, and how build-output paths map from the laptop to
/// the pod.
#[derive(Debug, Clone)]
pub struct PodRunConfig {
    /// Namespace runner pods are created in (the per-test/per-run namespace).
    pub namespace: String,
    /// Runner image reference (the nix `ztest-runner` image).
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
    /// A hostPath-delivery config for local (`kind`) runs. The workspace tree is
    /// visible on the node at `node_workspace` (for `kind`, via the cluster's
    /// `extraMounts`) and mounted into the pod at the *same* absolute path it has
    /// on the laptop (`local_workspace`) — so the binary, cwd, and build-output
    /// search paths all resolve unchanged (empty `path_map`), and `/nix/store`
    /// comes from the image. Read-only: tests write to their own `TempDir`s, not
    /// the source tree.
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

    /// A baked-delivery config for remote runs: the build outputs are already
    /// inside `image` at their original absolute paths (a thin layer over the nix
    /// base — see `docs/remote-test-execution.md` §2), so no volume is mounted and
    /// paths resolve unchanged. This is the delivery the remote cluster uses;
    /// [`Self::hostpath`] is the local-`kind` shortcut.
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
    let api: Api<corev1::Pod> = Api::namespaced(client, &cfg.namespace);
    let pod = build_pod(&name, &cfg, &item);

    if let Err(e) = api.create(&PostParams::default(), &pod).await {
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
    let done = loop {
        tokio::select! {
            _ = tokio::time::sleep(POLL_INTERVAL) => {
                if let Ok(p) = api.get(&name).await {
                    if let Some(st) = terminal_state(&p) {
                        break Done::Reached(st);
                    }
                    match p.status.as_ref().and_then(image_error) {
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

    // Fetch logs before deleting; a running pod (timeout/cancel) still serves
    // them. Best-effort — a pod that never started has none.
    let mut output = api
        .logs(&name, &LogParams::default())
        .await
        .unwrap_or_default()
        .into_bytes();

    if !cfg.env.no_cleanup {
        let _ = api.delete(&name, &DeleteParams::default()).await;
    }

    let verdict = match done {
        Done::Reached(TerminalState::Passed) => Verdict::Pass,
        Done::Reached(TerminalState::Failed(code)) => Verdict::Fail(code),
        Done::Reached(TerminalState::ImageError(reason)) => {
            // The pod produced no logs; surface the pull failure as the output so
            // the reporter shows why, not a blank SpawnError.
            output = format!("runner image error: {reason}").into_bytes();
            Verdict::SpawnError
        }
        Done::Timeout => Verdict::Timeout,
        Done::Cancelled => Verdict::Terminated,
    };

    TestOutcome {
        verdict,
        output,
        duration: started.elapsed(),
    }
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

/// Map a pod's observed *settled* state (Succeeded/Failed) to a terminal state,
/// or `None` while it is still pending/running. Image-pull errors are handled
/// separately with a grace window (see [`pull_error_is_terminal`]) because they
/// are frequently transient — treating them as terminal here would fail a test on
/// a recoverable pull-throttle storm.
fn terminal_state(pod: &corev1::Pod) -> Option<TerminalState> {
    let status = pod.status.as_ref()?;
    match status.phase.as_deref() {
        Some("Succeeded") => Some(TerminalState::Passed),
        Some("Failed") => Some(TerminalState::Failed(exit_code(status).unwrap_or(-1))),
        _ => None,
    }
}

/// Whether an observed image-pull error should end the run. `InvalidImageName`
/// can never resolve, so it's terminal immediately; a pull *failure*
/// (`ErrImagePull`/`ImagePullBackOff`) is transient until it has persisted for
/// `grace`, giving the kubelet's backoff time to clear a `pull QPS exceeded`
/// storm before the test is failed. `first_seen` is when this pod's pull error
/// was first observed.
fn pull_error_is_terminal(
    reason: &str,
    first_seen: Instant,
    now: Instant,
    grace: Duration,
) -> bool {
    reason == "InvalidImageName" || now.duration_since(first_seen) >= grace
}

/// The container's terminated exit code, if it has one.
fn exit_code(status: &corev1::PodStatus) -> Option<i32> {
    status
        .container_statuses
        .as_ref()?
        .iter()
        .find_map(|cs| cs.state.as_ref()?.terminated.as_ref().map(|t| t.exit_code))
}

/// An unrecoverable image-pull waiting reason, if any container is stuck on one.
fn image_error(status: &corev1::PodStatus) -> Option<String> {
    let stuck = ["ImagePullBackOff", "ErrImagePull", "InvalidImageName"];
    status.container_statuses.as_ref()?.iter().find_map(|cs| {
        let w = cs.state.as_ref()?.waiting.as_ref()?;
        let reason = w.reason.as_deref()?;
        stuck.contains(&reason).then(|| reason.to_string())
    })
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

fn build_pod(name: &str, cfg: &PodRunConfig, item: &WorkItem) -> corev1::Pod {
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
    ];
    if cfg.env.no_cleanup {
        env.push(env_var("ZTEST_NO_CLEANUP", "1"));
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
            ..Default::default()
        }),
        ..Default::default()
    }
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
    fn transient_pull_error_terminal_only_after_grace() {
        let t0 = Instant::now();
        let grace = Duration::from_secs(90);
        // Within the window a pull failure is transient — keep waiting.
        assert!(!pull_error_is_terminal(
            "ErrImagePull",
            t0,
            t0 + Duration::from_secs(30),
            grace
        ));
        assert!(!pull_error_is_terminal(
            "ImagePullBackOff",
            t0,
            t0 + Duration::from_secs(89),
            grace
        ));
        // Past the window it is terminal.
        assert!(pull_error_is_terminal(
            "ErrImagePull",
            t0,
            t0 + Duration::from_secs(91),
            grace
        ));
        // A malformed reference can never resolve — terminal at once.
        assert!(pull_error_is_terminal("InvalidImageName", t0, t0, grace));
    }

    #[test]
    fn build_pod_carries_image_refs_env() {
        let env = EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run_id: "r".into(),
            sa: "ztest".into(),
            no_cleanup: false,
        };
        let mut refs = BTreeMap::new();
        refs.insert(
            "k".to_string(),
            "reg.svc:5000/ns/zainod:dev-abc".to_string(),
        );
        let cfg = PodRunConfig::baked(env, "runner:dev".into(), "ztest".into(), None, refs);
        let pod = build_pod("p", &cfg, &work("crate::b", "t"));
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
        };
        let cfg = PodRunConfig::baked(
            env,
            "runner:dev".into(),
            "ztest".into(),
            None,
            BTreeMap::new(),
        );
        let pod = build_pod("p", &cfg, &work("crate::b", "t"));
        let vars = pod.spec.unwrap().containers[0].env.clone().unwrap();
        assert!(
            !vars
                .iter()
                .any(|v| v.name == crate::backends::image::IMAGE_REFS_ENV)
        );
    }
}
