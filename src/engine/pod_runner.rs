//! Run one test in a sibling runner pod, not a local child: heavy wallet compute goes
//! in-cluster, the test stays hermetic (sees only its own per-test namespace).
//!
//! Delivery decoupled — ready volumes/mounts + a local→pod [`PodRunConfig::path_map`] →
//! `kind` hostPath and remote image-layer/PVC share this code unchanged

use std::collections::BTreeMap;
use std::time::Instant;

use k8s_openapi::api::core::v1 as corev1;
use kube::api::{Api, DeleteParams, LogParams, ObjectMeta, PostParams};

use crate::cancel::Cancel;
use crate::engine::events::Verdict;
use crate::engine::local_runner::{EngineEnv, Executor, OutcomeFuture, TestOutcome};
use crate::engine::plan::WorkItem;
use crate::engine::test_ns;

use crate::pod_status::{
    IMAGE_PULL_GRACE, POLL_INTERVAL, PodPhases, exit_code, image_error, pod_phases,
    pull_error_is_terminal,
};

/// Everything the pod executor needs that isn't per-test.
///
/// - `image_pull_policy`/`service_account` `None` → cluster default; that SA needs RBAC
///   to create the test's sibling component pods
/// - `path_map` rewrites local→pod prefixes (longest wins, unmatched pass through) over
///   the binary path, the cwd and each `LD_LIBRARY_PATH` entry
#[derive(Debug, Clone)]
pub struct PodRunConfig {
    pub namespace: String,
    pub image: String,
    pub image_pull_policy: Option<String>,
    pub service_account: Option<String>,
    pub volumes: Vec<corev1::Volume>,
    pub volume_mounts: Vec<corev1::VolumeMount>,
    pub path_map: Vec<(String, String)>,
    pub env: EngineEnv,
}

pub struct PodExecutor {
    client: kube::Client,
    cfg: PodRunConfig,
}

// `kube::Client` is not `Debug`; the config carries the identifying detail
impl std::fmt::Debug for PodExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PodExecutor").field("cfg", &self.cfg).finish_non_exhaustive()
    }
}

impl PodRunConfig {
    /// hostPath delivery for local (`kind`) runs: `node_workspace` mounted read-only at
    /// the *same* absolute path the laptop uses (`local_workspace`) → binary/cwd/search
    /// paths resolve unchanged (empty `path_map`), `/nix/store` comes from the image
    #[allow(clippy::too_many_arguments)]
    pub fn hostpath(
        env: EngineEnv,
        image: String,
        namespace: String,
        local_workspace: String,
        node_workspace: String,
        service_account: Option<String>,
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
            env,
        }
    }

    /// Baked delivery for remote runs: build outputs already sit in `image` at their
    /// original absolute paths (`docs/design-remote-execution.md` §2) → no volume, paths
    /// resolve unchanged
    pub fn baked(
        env: EngineEnv,
        image: String,
        namespace: String,
        service_account: Option<String>,
    ) -> Self {
        Self {
            namespace,
            image,
            image_pull_policy: Some("IfNotPresent".to_string()),
            service_account,
            volumes: Vec::new(),
            volume_mounts: Vec::new(),
            path_map: Vec::new(),
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
    let spawn_error = |output: String| TestOutcome {
        verdict: Verdict::SpawnError,
        output: output.into_bytes(),
        components: Vec::new(),
        duration: started.elapsed(),
    };

    let test_ns = match test_ns::open(&client, &cfg.env.run, &item).await {
        Ok(ns) => ns,
        Err(e) => return spawn_error(e),
    };
    let runner_api: Api<corev1::Pod> = Api::namespaced(client.clone(), &cfg.namespace);
    let pod = build_pod(&name, &cfg, &item, &test_ns);
    if let Err(e) = runner_api.create(&PostParams::default(), &pod).await {
        test_ns::close(&client, &test_ns, cfg.env.no_cleanup).await;
        return spawn_error(format!("create runner pod {name}: {e}"));
    }

    let hard_cap = item.hard_cap;
    // First entry into an image-pull error → a transient storm is waited out for
    // `IMAGE_PULL_GRACE` before turning terminal
    let mut pull_error_since: Option<Instant> = None;
    // Most recent full pod observation, so the terminal timing breakdown can read the
    // kube-server phase timestamps (`pod_phases`)
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
                        // Pull progressed → reset the window
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

    // Every log fetched before anything is deleted (pods must still exist)
    let runner_raw =
        runner_api.logs(&name, &LogParams::default()).await.unwrap_or_default().into_bytes();
    let harvest = test_ns::close(&client, &test_ns, cfg.env.no_cleanup).await;
    if !cfg.env.no_cleanup
        && let Err(e) =
            crate::cluster::delete_and_await(&runner_api, &name, &DeleteParams::default()).await
    {
        tracing::warn!(target: "ztest::pod", runner_pod = %name, error = %e, "runner pod delete failed");
    }
    let runner = crate::logstream::runner_output(&runner_raw, &item.test_name, &harvest.dead);
    let components = harvest.components;

    let (verdict, output) = match done {
        Done::Reached(TerminalState::Passed) => (Verdict::Pass, runner),
        Done::Reached(TerminalState::Failed(code)) => (Verdict::Fail(code), runner),
        Done::Reached(TerminalState::ImageError(reason)) => {
            // No logs → surface the pull failure as the output, not a blank SpawnError
            (Verdict::SpawnError, format!("runner image error: {reason}").into_bytes())
        }
        Done::Timeout => (Verdict::Timeout, runner),
        Done::Cancelled => (Verdict::Terminated, runner),
    };

    TestOutcome { verdict, output, components, duration: started.elapsed() }
}

/// How the pod-await loop finished
enum Done {
    Reached(TerminalState),
    Timeout,
    Cancelled,
}

/// Pod's terminal observation. Distinct from [`Verdict`] so the image-error reason
/// survives into the outcome's output
#[derive(Debug, PartialEq, Eq)]
enum TerminalState {
    Passed,
    Failed(i32),
    ImageError(String),
}

/// Map a pod's *settled* state (Succeeded/Failed) to a terminal state; `None` while
/// pending/running. Image-pull errors get a grace window instead
/// ([`pull_error_is_terminal`](crate::pod_status::pull_error_is_terminal)) — usually
/// transient, so failing here would fail a test on a recoverable pull-throttle storm
fn terminal_state(pod: &corev1::Pod) -> Option<TerminalState> {
    let status = pod.status.as_ref()?;
    match status.phase.as_deref() {
        Some("Succeeded") => Some(TerminalState::Passed),
        Some("Failed") => Some(TerminalState::Failed(exit_code(status).unwrap_or(-1))),
        _ => None,
    }
}

/// DNS-safe runner-pod name, unique per *creation*: libtest names carry `::` and mixed
/// case, so the test name is slugified for a readable prefix + a random token.
///
/// Random, not a hash of the test identity: pods are reaped by `LABEL_RUN_ID`, so the
/// suffix only has to never collide — a deterministic name 409s against a concurrent run,
/// a still-terminating retry, or a pod leaked by a killed run (`naming::test_suffix`)
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
    format!("ztest-run-{}-{uniq:08x}", if slug.is_empty() { "t" } else { &slug })
}

/// Rewrite `path` by the longest matching prefix in `map`; unmatched (`/nix/store/…`,
/// present in the image) pass through unchanged
fn remap(path: &str, map: &[(String, String)]) -> String {
    map.iter()
        .filter(|(from, _)| path == from.as_str() || path.starts_with(&format!("{from}/")))
        .max_by_key(|(from, _)| from.len())
        .map(|(from, to)| format!("{to}{}", &path[from.len()..]))
        .unwrap_or_else(|| path.to_string())
}

/// Remap a `:`-separated search path (the dylib env value) entry by entry
fn remap_search_path(value: &str, map: &[(String, String)]) -> String {
    value
        .split(':')
        .filter(|s| !s.is_empty())
        .map(|entry| remap(entry, map))
        .collect::<Vec<_>>()
        .join(":")
}

/// Runner container env: [`EngineEnv::test_vars`] + the two paths remapped into the pod
fn runner_env(cfg: &PodRunConfig, item: &WorkItem, test_ns: &str) -> BTreeMap<String, String> {
    let cwd = remap(&item.cwd.to_string_lossy(), &cfg.path_map);
    let ld = remap_search_path(&cfg.env.dylib_path.to_string_lossy(), &cfg.path_map);
    let mut env: BTreeMap<String, String> =
        cfg.env.test_vars(test_ns).into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    env.insert(crate::engine::dylib::dylib_path_envvar().to_string(), ld);
    env.insert("CARGO_MANIFEST_DIR".to_string(), cwd);
    env
}

fn build_pod(name: &str, cfg: &PodRunConfig, item: &WorkItem, test_ns: &str) -> corev1::Pod {
    let bin = remap(&item.binary_path.to_string_lossy(), &cfg.path_map);
    let cwd = remap(&item.cwd.to_string_lossy(), &cfg.path_map);
    let env: Vec<corev1::EnvVar> = runner_env(cfg, item, test_ns)
        .into_iter()
        .map(|(name, value)| corev1::EnvVar { name, value: Some(value), ..Default::default() })
        .collect();
    // - run-id → parent's `reap_run` (Ctrl-C) + ledger attribution
    // - user → `ztest cleanup --mine` when that teardown never ran
    let labels = BTreeMap::from([
        (crate::qos::LABEL_RUN_ID.to_string(), cfg.env.run.run_id.clone()),
        (crate::qos::LABEL_USER.to_string(), cfg.env.run.user.clone()),
    ]);

    // Guaranteed QoS: sized at its tier's runner footprint, `requests == limits` with
    // whole-core CPU — never BestEffort (`qos::QosProfile::runner`)
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
        security_context: Some(untrusted_security_context()),
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
            // Explicit: the driver does need a token (`TestEnv::build` runs in-pod), and the
            // safety is that DRIVER_SERVICE_ACCOUNT reaches exactly one namespace
            automount_service_account_token: Some(true),
            // Pinned Guaranteed pod on `restartPolicy: Never`: a lost node must delete
            // it at once (no migration without losing its pinned CPUs), not after 300 s
            tolerations: Some(fast_evict_tolerations()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Container hardening for a pod running untrusted test code.
///
/// - Within PSA `baseline`, which the driver namespace enforces (`resource::entry`)
/// - No `runAsNonRoot`: the baked runner image builds and runs as root, and flipping that is
///   an image change, not a pod change
fn untrusted_security_context() -> corev1::SecurityContext {
    corev1::SecurityContext {
        allow_privilege_escalation: Some(false),
        privileged: Some(false),
        capabilities: Some(corev1::Capabilities {
            drop: Some(vec!["ALL".to_string()]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Guaranteed (`requests == limits`) container `resources` sized at `footprint`, via the
/// single QoS lowering [`Resources::guaranteed_cpu_mem`]. Panics on a degenerate footprint
fn guaranteed_resources(footprint: crate::qos::Resources) -> corev1::ResourceRequirements {
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    let (cpu, mem) = footprint.guaranteed_cpu_mem("runner pod footprint");
    let amounts =
        BTreeMap::from([("cpu".to_string(), Quantity(cpu)), ("memory".to_string(), Quantity(mem))]);
    corev1::ResourceRequirements {
        requests: Some(amounts.clone()),
        limits: Some(amounts),
        ..Default::default()
    }
}

/// Immediate-eviction tolerations (`tolerationSeconds: 0`) a pinned Guaranteed pod carries
/// so a lost node deletes it at once. Mirrors [`crate::manifest::PodSpec::render`]
fn fast_evict_tolerations() -> Vec<corev1::Toleration> {
    ["node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"]
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

/// Emit the runner pod's lifecycle latency breakdown on the `ztest::pod` diagnostics
/// target ([`observ`](crate::observ)) — the signal for "test slow but cluster idle", where
/// large `pull_init_ms`/`schedule_ms` against a small `body_ms` is cluster wait.
/// `overhead_ms` = the rest of the laptop-observed wall (create-call latency + the
/// ≤`POLL_INTERVAL` lag before a settled state is noticed)
fn emit_timing(test: &str, pod: Option<&corev1::Pod>, total: std::time::Duration) {
    let phases = pod.map(pod_phases).unwrap_or(PodPhases {
        created: None,
        scheduled: None,
        container_started: None,
        container_finished: None,
    });
    let ms = |d: Option<std::time::Duration>| d.unwrap_or_default().as_millis() as u64;
    let accounted: std::time::Duration =
        [phases.schedule(), phases.pull_init(), phases.body()].into_iter().flatten().sum();
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
        // /nix/store paths come from the image; never rewritten
        assert_eq!(remap("/nix/store/abc-glibc/lib", &map()), "/nix/store/abc-glibc/lib");
    }

    #[test]
    fn remap_longest_prefix_wins() {
        let m = vec![("/a".into(), "/x".into()), ("/a/b".into(), "/y".into())];
        assert_eq!(remap("/a/b/c", &m), "/y/c");
        assert_eq!(remap("/a/z", &m), "/x/z");
    }

    #[test]
    fn remap_does_not_match_partial_component() {
        // "/home/u/proj/targetx" must not match the "/home/u/proj/target" prefix
        assert_eq!(remap("/home/u/proj/targetx/y", &map()), "/home/u/proj/targetx/y");
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
            class: crate::qos::QosClass::Integration,
            footprint: crate::qos::Resources::ZERO,
            hard_cap: Duration::from_secs(1),
            retries: 0,
            deps: Vec::new(),
        }
    }

    fn work_in_tier(class: crate::qos::QosClass) -> WorkItem {
        WorkItem { class, ..work("crate::b", "t") }
    }

    /// Infrastructure only: tests below assert pod shape, never these values
    fn env() -> EngineEnv {
        EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run: crate::naming::RunCoords { run_id: "r".into(), user: "u".into() },
            no_cleanup: false,
            capture: true,
            ztest_log: None,
            image_refs: BTreeMap::new(),
            storage: None,
        }
    }

    #[test]
    fn runner_pod_is_guaranteed_and_sized_from_the_tier_runner_footprint() {
        use crate::qos::QosClass;
        let cfg = PodRunConfig::baked(env(), "runner:dev".into(), "ztest".into(), None);

        // Regtest/integration tier runner: one whole core (orchestration)
        let pod = build_pod("p", &cfg, &work_in_tier(QosClass::Integration), "ztest-test-ns");
        let c = &pod.spec.as_ref().unwrap().containers[0];
        let res = c.resources.as_ref().expect("runner pod must be sized");
        let req = res.requests.as_ref().unwrap();
        let lim = res.limits.as_ref().unwrap();
        // Guaranteed: requests == limits, in every dimension present
        assert_eq!(req, lim, "runner pod must be Guaranteed (requests == limits)");
        assert_eq!(req["cpu"].0, "1");

        // Wallet tier keeps the in-process wallet's compute here — more than orchestration
        let pod = build_pod("p", &cfg, &work_in_tier(QosClass::Wallet), "ztest-test-ns");
        let c = &pod.spec.as_ref().unwrap().containers[0];
        let req = c.resources.as_ref().unwrap().requests.as_ref().unwrap();
        assert_eq!(req["cpu"].0, "2");
    }

    /// In-pod `TestEnv` reads all of these and can derive none:
    /// - run id + user + namespace → `ztest.io/*` labels on every component pod (ledger, reaping)
    /// - storage classes → driver SA may not list cluster-scoped classes (403 in-pod)
    /// - unresolved optionals absent, never empty
    #[test]
    fn runner_env_carries_the_run_identity_and_every_orchestrator_resolved_value() {
        let full = EngineEnv {
            dylib_path: std::ffi::OsString::from("/x"),
            run: crate::naming::RunCoords { run_id: "eli-0a1b2c3d".into(), user: "eli".into() },
            no_cleanup: false,
            capture: true,
            ztest_log: Some("ztest::build=debug".into()),
            image_refs: BTreeMap::from([("k".into(), "reg:5000/zainod:dev-abc".into())]),
            storage: Some(("fast-ssd".into(), "csi-snapclass".into())),
        };
        let cfg = PodRunConfig::baked(full.clone(), "runner:dev".into(), "ztest".into(), None);
        let base = [
            (crate::engine::dylib::dylib_path_envvar(), "/x"),
            ("CARGO_MANIFEST_DIR", ""),
            ("NEXTEST", "1"),
            ("NEXTEST_EXECUTION_MODE", "process-per-test"),
            ("NEXTEST_RUN_ID", "eli-0a1b2c3d"),
            (crate::naming::RUN_ID_ENV, "eli-0a1b2c3d"),
            ("USER", "eli"),
            (crate::naming::TEST_NAMESPACE_ENV, "ztest-b-t-0a1b2c3d"),
        ];
        let resolved = [
            ("ZTEST_LOG", "ztest::build=debug"),
            (crate::backends::image::IMAGE_REFS_ENV, r#"{"k":"reg:5000/zainod:dev-abc"}"#),
            (crate::cluster_config::STORAGE_CLASS_ENV, "fast-ssd"),
            (crate::cluster_config::SNAPSHOT_CLASS_ENV, "csi-snapclass"),
        ];
        let map = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
        };
        assert_eq!(
            runner_env(&cfg, &work("crate::b", "t"), "ztest-b-t-0a1b2c3d"),
            map(&[&base[..], &resolved[..]].concat())
        );

        let bare =
            EngineEnv { ztest_log: None, image_refs: BTreeMap::new(), storage: None, ..full };
        let cfg = PodRunConfig::baked(bare, "runner:dev".into(), "ztest".into(), None);
        assert_eq!(runner_env(&cfg, &work("crate::b", "t"), "ztest-b-t-0a1b2c3d"), map(&base));

        let pod = build_pod("p", &cfg, &work("crate::b", "t"), "ztest-b-t-0a1b2c3d");
        assert_eq!(
            pod.metadata.labels.expect("runner pod labels"),
            BTreeMap::from([
                (crate::qos::LABEL_RUN_ID.to_string(), "eli-0a1b2c3d".to_string()),
                (crate::qos::LABEL_USER.to_string(), "eli".to_string()),
            ])
        );
    }

    #[test]
    fn runner_pod_evicts_immediately_on_node_loss() {
        let cfg = PodRunConfig::baked(env(), "runner:dev".into(), "ztest".into(), None);
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
        assert!(a.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
    }

    #[test]
    fn pod_name_is_unique_per_creation() {
        // The *same* test names distinctly per call → concurrent runs, retries and
        // crash-leftovers never 409-collide
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
        // Pull errors are not folded into `terminal_state`; the run loop grace-windows them
        let p = pod_with(Some("Pending"), None, Some("ImagePullBackOff"));
        assert!(terminal_state(&p).is_none());
        assert_eq!(image_error(p.status.as_ref().unwrap()).as_deref(), Some("ImagePullBackOff"));
    }
}
