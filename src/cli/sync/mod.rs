//! `ztest sync` — the stateless controller for detached, ztest-owned chain
//! syncs (design §"Execution model: ztest-owned pods").
//!
//! A detached sync outlives the launching terminal: state lives in k8s, not a
//! local daemon. Each sync gets a hermetic, persistent namespace
//! `ztest-sync-<id>` holding its driver pod (labelled `ztest.io/{kind=sync,
//! sync-id,user}`) and, once the driver's in-pod `TestEnv` provisions them, its
//! `zebrad`/`zaino` component pods. Any machine with the kubeconfig can
//! `list`/`watch`/`stop` — the controller is stateless, finding syncs by the
//! `kind=sync` label. `watch` is a read-only tail; ending a sync is only `stop`
//! (graceful `sync_mode = Shutdown` → checkpoint), never a detach.
//!
//! `start` reuses the exact `ztest run` on-cluster compile — the detached driver
//! runs the *same* compiled `#[ztest::sync_test]` body, distinguished only by
//! the `ZTEST_SYNC_ID` env it carries (see [`crate::sync::detached`]).

mod perf;
mod watch;

use std::collections::BTreeMap;
use std::io::{IsTerminal, stdout};
use std::process::ExitCode;
use std::time::Instant;

use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Pod, ServiceAccount};
use k8s_openapi::api::rbac::v1::RoleBinding;
use kube::Client;
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use serde_json::json;

use clap::{Args as ClapArgs, Subcommand};

use crate::cli::console::{Console, SceneFrame};
use crate::pipeline::build::BuildOutcome;
use crate::pipeline::images::DumpOutcome;
use crate::pipeline::remote_compile::{self, BakeRefs, RemoteCompileOutcome};
use crate::resource::impls::buildkit;
use crate::resource::impls::policy::RUN_CLUSTER_ROLE;
use crate::sync::{
    KIND_LABEL_KEY, KIND_LABEL_VALUE, POD_NAME_ENV, STOP_ANNOTATION, SYNC_ID_ENV, SYNC_ID_KEY,
    SYNC_PROFILE_ENV, SyncReportMirror, kind_selector, namespace_for, report_cm_name,
};
use crate::ui::{Theme, Transfers};

/// The single driver pod's name within a sync's namespace (one sync ⇒ one
/// namespace ⇒ one driver, so a fixed name is unambiguous).
const DRIVER_POD: &str = "ztest-sync-driver";
/// The driver pod's sole container. Named because a log request has to gate on
/// *that* container's state, not the pod's phase.
const DRIVER_CONTAINER: &str = "sync";
/// The ServiceAccount the driver runs as, created per-sync-namespace and bound
/// (RoleBinding → the `ztest-remote` ClusterRole) so it can provision its own
/// topology within its namespace.
const DRIVER_SA: &str = "ztest-sync";
/// Grace window for the driver's cooperative shutdown: `sync_mode = Shutdown` →
/// wallet checkpoint → report mirror, before the kubelet `SIGKILL`s it.
const STOP_GRACE_SECS: i64 = 120;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Named cluster profile (see `ztest cluster`) bundling the image backend,
    /// registry, and kube-context — the same selector as `ztest run --cluster`.
    /// Applies to every `sync` subcommand; overrides the persisted default.
    /// Precedence: `--cluster` > ambient env > persisted default.
    #[arg(long, global = true, value_name = "NAME")]
    cluster: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// List detached syncs (cluster-wide `kind=sync` query): id, namespace, phase, user.
    List {
        /// Include every user's syncs, not just your own.
        #[arg(long)]
        all_users: bool,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Print a profile's static manifest (name/subject/qos/timeout/tags) from a
    /// local inventory dump (no cluster).
    Describe {
        /// Profile name (`#[ztest::sync_test(name = ..)]`).
        name: String,
    },
    /// Build the runner image and create the detached driver pod for a profile.
    Start {
        /// Profile name.
        name: String,
        /// Attach a read-only progress tail after starting.
        #[arg(long)]
        watch: bool,
    },
    /// Attach to a sync's live progress (read-only; detaching never stops it).
    Watch {
        /// Sync id.
        id: String,
    },
    /// One-shot status: the final report if the run has finished (read from the
    /// mirror ConfigMap, so it works after the pod is gone), else live progress.
    Status {
        /// Sync id.
        id: String,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Retrieve a sync's CPU profiles and open them in a flame-graph viewer.
    Perf {
        /// Sync id.
        id: String,
        /// Directory to write the artifacts to (default `ztest-perf-<id>`).
        #[arg(long)]
        out: Option<std::path::PathBuf>,
        /// Retrieve only; do not launch a viewer.
        #[arg(long)]
        no_open: bool,
        /// List the periodic snapshots available, without opening one.
        #[arg(long)]
        list: bool,
        /// View one slice of a long run, e.g. `11h..12h` (needs periodic
        /// snapshots; see `ZTEST_PROFILE_INTERVAL` in docs/how-to-profile.md).
        #[arg(long, value_name = "FROM..TO")]
        window: Option<String>,
        /// Which profiled component to view. Required only when a topology
        /// profiled more than one — their profiles are not comparable.
        #[arg(long, value_name = "NAME")]
        component: Option<String>,
        /// Compare against an earlier sync: per-op rate deltas, and a
        /// differential flame graph of this run against that one. Both runs
        /// must have covered the same declared height segment.
        #[arg(long, value_name = "SYNC_ID")]
        base: Option<String>,
        /// Keep every frame, including the thread/tokio scaffolding that precedes
        /// the first line of application code in every stack. Off by default:
        /// that prefix is identical on every sample and is over half the graph.
        #[arg(long)]
        raw: bool,
    },
    /// Graceful stop: `sync_mode = Shutdown` → checkpoint → exit.
    Stop {
        /// Sync id.
        id: String,
    },
}

pub fn execute(args: Args) -> ExitCode {
    // Bind the selected cluster profile (image backend / registry / kube-context)
    // up front, exactly as `ztest run` does. Without this, `sync start` never sees
    // the on-cluster build target and every cluster-bound subcommand ignores the
    // active profile — falling back to the ambient env's inferred (kind) backend.
    //
    // `describe` is exempt: it is a purely local inventory dump, so it must keep
    // working with no cluster configured at all (activation would offline-verify
    // the profile's context and could reject a stale default).
    //
    // Must precede `block_on`: `activate` calls `set_var`, which is only sound
    // while single-threaded, and `block_on` spawns the async runtime's threads.
    // SAFETY: still single-threaded here.
    if !matches!(args.cmd, Cmd::Describe { .. })
        && let Err(detail) = unsafe { crate::cluster_config::activate(args.cluster.as_deref()) }
    {
        eprintln!("ztest sync: {detail}");
        return ExitCode::FAILURE;
    }
    super::block_on("sync", super::Rt::Multi, run(args))
}

async fn run(args: Args) -> Result<(), String> {
    match args.cmd {
        Cmd::List { all_users, json } => list(all_users, json).await,
        Cmd::Status { id, json } => status(&id, json).await,
        Cmd::Stop { id } => stop(&id).await,
        // Retrieval is inspection, like `report`: it reads artifacts the run left
        // behind and never touches the run itself.
        Cmd::Perf {
            id,
            out,
            no_open,
            list,
            window,
            component,
            base,
            raw,
        } => {
            perf::perf(perf::Request {
                id,
                out,
                open: !no_open,
                list,
                window,
                component,
                base,
                raw,
            })
            .await
        }
        // Attaching to a sync is inspection, like `status`/`report`: it succeeds
        // when it managed to observe, whatever the run's own verdict was.
        Cmd::Watch { id } => watch::watch(&id).await.map(drop),
        Cmd::Describe { name } => describe(&name).await,
        Cmd::Start { name, watch } => start(&name, watch).await,
    }
}

async fn client() -> Result<Client, String> {
    crate::cluster::client()
        .await
        .map_err(|e| format!("kube client: {e}"))
}

/// The ServiceAccount a detached sync charges its QoS budget against — the
/// *credential*, not the person. Ownership is [`naming::current_user`]
/// (`ztest.io/user`); keeping the two apart is what lets several developers
/// share one remote SA without inheriting each other's syncs.
///
/// [`naming::current_user`]: crate::naming::current_user
fn service_account() -> String {
    let raw = std::env::var("ZTEST_SA")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "unknown".into());
    crate::naming::slug(&raw, crate::naming::DNS_LABEL_MAX)
}

// ─────────────────────────────── start ────────────────────────────────

async fn start(name: &str, watch_after: bool) -> Result<(), String> {
    if !crate::backends::image::builds_on_cluster() {
        return Err(
            "`ztest sync start` needs an on-cluster build target — detached syncs are \
             k8s-owned pods built + run on the cluster. Select an OpenShift cluster \
             (`ztest cluster ...`) and re-run."
                .into(),
        );
    }
    let sa = service_account();
    let sync_id = new_sync_id(name);
    let ns = namespace_for(&sync_id);
    let client = client().await?;

    let runner_repo_ref = crate::backends::image::runner_repo_ref().ok_or(
        "runner registry unknown: set the cluster's in-cluster pull address \
         (ZTEST_IMAGE_REGISTRY) — see `ztest cluster`.",
    )?;
    let refs = BakeRefs { runner_repo_ref };

    // The detached build streams through the same pinned console `ztest run`
    // uses (build-pod → BuildKit compile → component provisioning), so the two
    // entry points render identically. On a TTY a render thread owns the panel;
    // off it, output is linear to stderr.
    let theme = Theme::detect();
    let session_start = Instant::now();
    let (console, guard) = if stdout().is_terminal() {
        let cancel_theme = theme.clone();
        let cancel_panel =
            Box::new(move |elapsed| crate::ui::render_cancel_panel(elapsed, &cancel_theme));
        match Console::start(session_start, cancel_panel) {
            Ok((c, g)) => (Some(c), Some(g)),
            Err(_) => (None, None),
        }
    } else {
        (None, None)
    };

    let built = build_and_provision(&client, &sync_id, name, &refs, console.as_ref(), &theme).await;

    // Tear the render thread down before anything else prints, so the handoff (or
    // an error) lands on a clean line rather than behind the pinned panel.
    if let Some(g) = guard {
        g.finish();
    }
    let (compiled, image_refs) = built?;

    // Resolve the profile name → its binary + libtest test (the driver's
    // `<bin> --exact <test>` command) from the inventory the compile dumped.
    let target = resolve_target(&compiled, name)?;

    // Provision the hermetic per-sync namespace + its driver RBAC, then create the
    // detached driver pod.
    ensure_sync_namespace(&client, &ns, &sync_id).await?;
    let pod = build_driver_pod(&sync_id, name, &ns, &sa, &compiled, &target, &image_refs);
    Api::<Pod>::namespaced(client.clone(), &ns)
        .create(&PostParams::default(), &pod)
        .await
        .map_err(|e| format!("create driver pod: {e}"))?;

    print_handoff(&theme, &sync_id, &ns);
    if watch_after {
        return attach(&theme, &sync_id).await;
    }
    Ok(())
}

/// `--watch`: attach to the sync just started, and translate how the attach ended
/// into this command's result.
///
/// The sync is a durable cluster object from the moment its pod is created, so
/// everything past that point is observation. A failed *attach* therefore must not
/// fail `start`: the remedy is to re-attach, whereas a non-zero `start` reads as
/// "it didn't launch" and invites a re-run — which would launch a second sync
/// holding a second footprint. A failed *verdict*, on the other hand, is the
/// answer the user stayed attached for, and `--watch` is the foreground stand-in
/// for `ztest run`, so it fails the pipeline exactly as a failing run would.
async fn attach(theme: &Theme, sync_id: &str) -> Result<(), String> {
    use owo_colors::OwoColorize as _;
    match watch::watch(sync_id).await {
        Ok(watch::WatchEnd::Finished { passed: false }) => {
            Err(format!("sync {sync_id} finished with violations"))
        }
        // The driver's log ended while the driver was gone: killed, evicted, or
        // crashed before it could mirror a report. Silence here would let a
        // pipeline pass on a sync that never reached a verdict.
        Ok(watch::WatchEnd::Unresolved) => Err(format!(
            "sync {sync_id} ended without a report (`ztest sync status {sync_id}`)"
        )),
        Ok(watch::WatchEnd::Finished { passed: true } | watch::WatchEnd::Detached) => Ok(()),
        Err(detail) => {
            eprintln!("ztest sync: attach to {sync_id} failed: {detail}");
            eprintln!(
                "  {} the sync is unaffected — re-attach with `ztest sync watch {sync_id}`",
                theme.chars.dot.style(theme.styles.dim),
            );
            Ok(())
        }
    }
}

/// Build the runner image (compiling the sync test) and provision the profile's
/// component `dev!` images on the same ephemeral BuildKit pod, rendering the
/// whole phase through `console` via the shared [`crate::cli::console`] driver —
/// the identical build UX `ztest run` presents. Returns the compile outcome and
/// the `DevImageId → pull-ref` map to forward to the driver. The build pod is
/// always torn down before returning, whatever the outcome.
async fn build_and_provision(
    client: &Client,
    sync_id: &str,
    profile: &str,
    refs: &BakeRefs,
    console: Option<&Console>,
    theme: &Theme,
) -> Result<(RemoteCompileOutcome, BTreeMap<String, String>), String> {
    use crate::cli::console::commit_phase;
    use crate::pipeline::remote_compile::Phase;
    use crate::ui::{BuildState, render_sync_build_panel, render_transfers};

    let context = crate::cluster_config::active_context().unwrap_or_else(|| "(cluster)".into());
    let started = Instant::now();

    // Repaint the sync `Building` panel: the left column is this sync's context +
    // the shared inventory build line, the right column the transfer tracker.
    let paint = |build: BuildState, phase: &'static str, transfers: &Transfers| {
        if let Some(c) = console {
            let (profile, sync_id, context, theme, transfers) = (
                profile.to_string(),
                sync_id.to_string(),
                context.clone(),
                theme.clone(),
                transfers.clone(),
            );
            c.scene(move |elapsed| SceneFrame {
                left: render_sync_build_panel(
                    &profile, &sync_id, &context, &build, phase, elapsed, &theme,
                ),
                right: render_transfers(&transfers, elapsed, &theme),
                live: None,
            });
        }
    };

    // Phase transitions from the shared driver: the colored `• …/✓ …` boundary
    // lines land in scrollback; a new sub-phase repaints the panel's build row.
    let mut on_phase = |ev: Phase<'_>| {
        if let Some(sub) = commit_phase(console, theme, ev) {
            paint(
                BuildState::Compiling {
                    started_at: Instant::now(),
                    phase: Some(sub),
                },
                "Building",
                &Transfers::default(),
            );
        }
    };

    // Create the ephemeral BuildKit pod (its own phase with a live timer) and wait
    // it Ready before compiling.
    on_phase(Phase::Start("startup builder"));
    let t_builder = Instant::now();
    let build_pod = {
        let p = buildkit::create_build_pod(client, sync_id, &crate::naming::current_user())
            .await
            .map_err(|e| format!("create build pod: {e}"))?;
        if let Err(e) = buildkit::wait_build_pod_ready(client, &p).await {
            buildkit::delete_build_pod(client, &p).await;
            return Err(format!("build pod not ready: {e}"));
        }
        p
    };
    on_phase(Phase::Done {
        label: "builder pod ready",
        dur: t_builder.elapsed(),
    });

    // Compile the runner image on the cluster. With a console the BuildKit
    // progress streams through a remote PTY into the emulator grid; off it, each
    // line goes to stderr. A virtual-workspace root rejects a bare `--features`
    // and the consumer crate already enables `ztest/zingo`, so no list args.
    let byte_sink = |bytes: &[u8]| {
        if let Some(c) = console {
            c.output(bytes.to_vec());
        }
    };
    let line_sink = |line: &str| eprintln!("{line}");
    let compile_out = match console {
        Some(c) => remote_compile::CompileOut::Pty {
            size: (c.size().cols, c.live_rows()),
            sink: &byte_sink,
        },
        None => remote_compile::CompileOut::Lines { sink: &line_sink },
    };
    let compiled = remote_compile::compile_on_cluster(
        client,
        &build_pod,
        &[],
        refs,
        sync_id,
        Some(compile_out),
        Some(&mut on_phase),
    )
    .await;
    let compiled = match compiled {
        Ok(c) => c,
        Err(e) => {
            if let Some(c) = console {
                c.flush_live();
            }
            buildkit::delete_build_pod(client, &build_pod).await;
            return Err(e);
        }
    };

    // Provision the selection's component `dev!` images on the same build pod,
    // tracked in the right column. The resulting `DevImageId → pull-ref` map is
    // what the driver's in-pod `TestEnv` resolves component images from.
    let image_refs = provision_components(
        client,
        &build_pod,
        &compiled,
        console,
        |transfers: &Transfers| {
            let (test_count, binary_count) = match &compiled.build {
                BuildOutcome::Ok {
                    test_count,
                    binary_count,
                    ..
                } => (*test_count, *binary_count),
                _ => (0, 0),
            };
            paint(
                BuildState::Ok {
                    test_count,
                    binary_count,
                    elapsed: started.elapsed(),
                },
                "Provisioning",
                transfers,
            );
        },
    )
    .await;

    buildkit::delete_build_pod(client, &build_pod).await;
    Ok((compiled, image_refs?))
}

/// Provision the compile's `dev!` component images through the shared
/// transfer-tracker driver, returning the `DevImageId → pull-ref` map.
async fn provision_components(
    client: &Client,
    build_pod: &str,
    compiled: &RemoteCompileOutcome,
    console: Option<&Console>,
    repaint: impl FnMut(&Transfers),
) -> Result<BTreeMap<String, String>, String> {
    let DumpOutcome::Discovered {
        images,
        seeds,
        images_by_binary,
        ..
    } = &compiled.dump
    else {
        return Err("inventory dump failed (no component images resolved)".into());
    };
    if images.is_empty() && seeds.is_empty() {
        return Ok(BTreeMap::new());
    }
    let graph = crate::resource::plan_runtime(images, seeds)
        .map_err(|e| format!("plan component images: {e}"))?;
    let states = crate::cli::console::provision_with_tracker(
        &graph,
        client.clone(),
        Some(build_pod.to_string()),
        console,
        repaint,
    )
    .await;
    Ok(crate::resource::dev_image_refs(images_by_binary, &states))
}

/// The post-build handoff: a themed, branded summary of the started sync plus
/// its follow-up commands, printed to clean stdout after the console tears down.
fn print_handoff(theme: &Theme, sync_id: &str, ns: &str) {
    use owo_colors::OwoColorize as _;
    let dot = theme.chars.dot.style(theme.styles.dim);
    println!(
        "{} sync {} started {dot} namespace {}",
        theme.chars.ok.style(theme.styles.pass),
        sync_id.style(theme.styles.count),
        ns.style(theme.styles.dim),
    );
    for (verb, rest) in [
        ("watch ", sync_id),
        ("status", sync_id),
        ("report", sync_id),
        ("stop  ", sync_id),
    ] {
        println!(
            "  {} ztest sync {verb} {rest}",
            theme.chars.dot.style(theme.styles.dim),
        );
    }
}

/// The driver's `<bin> --exact <test>` target for a profile, resolved from the
/// compile's inventory dump + binary selection.
struct Target {
    binary_path: String,
    test_name: String,
    cwd: String,
}

/// Resolve a profile `name` → its binary + libtest test name.
///
/// The dump carries each profile's `test_id` (`crate::…::fn`); the libtest name
/// a baked binary is invoked with is that path minus its crate segment. We then
/// find the compiled binary whose selection contains that test — deriving, never
/// guessing, the exact `<bin> --exact <test>` the driver runs.
fn resolve_target(compiled: &RemoteCompileOutcome, name: &str) -> Result<Target, String> {
    let DumpOutcome::Discovered { sync_tests, .. } = &compiled.dump else {
        return Err("inventory dump failed".into());
    };
    let entry = sync_tests.iter().find(|s| s.name == name).ok_or_else(|| {
        let have: Vec<&str> = sync_tests.iter().map(|s| s.name.as_str()).collect();
        if have.is_empty() {
            format!("no sync profiles found in the compiled selection (looked for `{name}`)")
        } else {
            format!(
                "no sync profile named `{name}`; available: {}",
                have.join(", ")
            )
        }
    })?;
    // `test_id` is `crate::module::fn`; the in-binary libtest name drops the
    // crate segment (module_path!() at an integration test's root is the crate).
    let libtest = entry
        .test_id
        .split_once("::")
        .map(|(_, rest)| rest)
        .unwrap_or(entry.test_id.as_str());

    let BuildOutcome::Ok {
        selected_binaries, ..
    } = &compiled.build
    else {
        return Err("on-cluster build produced no test selection".into());
    };
    let bin = selected_binaries
        .iter()
        .find(|b| b.selected_tests.iter().any(|t| t == libtest))
        .ok_or_else(|| {
            format!(
                "profile `{name}` (test `{libtest}`) is not in any compiled binary — \
                 was it built with the features it needs?"
            )
        })?;
    Ok(Target {
        binary_path: bin.binary_path.to_string_lossy().into_owned(),
        test_name: libtest.to_string(),
        cwd: bin.cwd.to_string_lossy().into_owned(),
    })
}

/// A short, DNS-safe, unique id: `<name-slug>-<rand8>`.
fn new_sync_id(name: &str) -> String {
    format!(
        "{}-{:08x}",
        crate::naming::slug(name, 24),
        rand::random::<u32>()
    )
}

/// Create (idempotently) the sync's persistent namespace, its driver SA, and the
/// RoleBinding granting that SA the `ztest-remote` ClusterRole *within* the
/// namespace — enough to provision its own topology. The namespace carries no
/// `janitor/ttl`: a sync outlives the 1h test-namespace TTL by design, and is
/// reclaimed by `ztest cleanup` once it finishes.
async fn ensure_sync_namespace(client: &Client, ns: &str, sync_id: &str) -> Result<(), String> {
    let params = PatchParams::apply("ztest-sync").force();

    let namespace: Namespace = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": ns,
            "labels": {
                KIND_LABEL_KEY: KIND_LABEL_VALUE,
                SYNC_ID_KEY: sync_id,
                crate::qos::LABEL_USER: crate::naming::current_user(),
            },
        },
    }))
    .expect("static Namespace manifest is valid");
    Api::<Namespace>::all(client.clone())
        .patch(ns, &params, &Patch::Apply(&namespace))
        .await
        .map_err(|e| format!("create sync namespace {ns}: {e}"))?;

    let sa: ServiceAccount = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": { "name": DRIVER_SA, "namespace": ns },
    }))
    .expect("static ServiceAccount manifest is valid");
    Api::<ServiceAccount>::namespaced(client.clone(), ns)
        .patch(DRIVER_SA, &params, &Patch::Apply(&sa))
        .await
        .map_err(|e| format!("create driver SA: {e}"))?;

    let rb: RoleBinding = serde_json::from_value(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": { "name": "ztest-sync-remote", "namespace": ns },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": RUN_CLUSTER_ROLE,
        },
        "subjects": [{ "kind": "ServiceAccount", "name": DRIVER_SA, "namespace": ns }],
    }))
    .expect("static RoleBinding manifest is valid");
    Api::<RoleBinding>::namespaced(client.clone(), ns)
        .patch("ztest-sync-remote", &params, &Patch::Apply(&rb))
        .await
        .map_err(|e| format!("bind driver SA: {e}"))?;

    wait_for_sa(client, ns, DRIVER_SA).await
}

/// Block until a ServiceAccount exists (the SA controller creates the token
/// asynchronously; a pod scheduled in that gap is rejected).
async fn wait_for_sa(client: &Client, ns: &str, sa: &str) -> Result<(), String> {
    let api: Api<ServiceAccount> = Api::namespaced(client.clone(), ns);
    for _ in 0..150 {
        if api
            .get_opt(sa)
            .await
            .map_err(|e| format!("poll SA {sa}: {e}"))?
            .is_some()
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    Err(format!(
        "ServiceAccount {sa} not provisioned in {ns} within 30s"
    ))
}

/// The detached driver pod: the baked runner image, running the profile's test
/// via `<bin> --exact <test>` exactly as a `ztest run` runner pod does, but in
/// the persistent per-sync namespace, `kind=sync`-labelled, sized at the sync
/// tier + placed on the NVMe pool, with the detached-mode env wired and no
/// reaper/timeout.
fn build_driver_pod(
    sync_id: &str,
    profile: &str,
    ns: &str,
    sa: &str,
    compiled: &RemoteCompileOutcome,
    target: &Target,
    image_refs: &BTreeMap<String, String>,
) -> Pod {
    let tier = crate::qos::QosClass::Sync.profile();
    let (cpu, mem) = tier.runner.guaranteed_cpu_mem("sync driver pod");

    // The dynamic-library search path the baked binary needs (libstd is linked
    // dynamically) — derived exactly as the engine derives it for a runner pod,
    // from the same on-cluster build meta.
    let BuildOutcome::Ok { summary, .. } = &compiled.build else {
        // resolve_target already proved this arm; unreachable in practice.
        unreachable!("build outcome validated in resolve_target");
    };
    let ld = crate::engine::dylib::dylib_path_value(&summary.rust_build_meta)
        .to_string_lossy()
        .into_owned();

    let image_refs_json = serde_json::to_string(image_refs).unwrap_or_else(|_| "{}".into());

    let mut env = vec![
        json!({ "name": "ZTEST_ENGINE", "value": "1" }),
        json!({ "name": "ZTEST_SA", "value": sa }),
        json!({ "name": crate::naming::TEST_NAMESPACE_ENV, "value": ns }),
        json!({ "name": "ZTEST_RUN_ID", "value": format!("sync-{sync_id}") }),
        // The launching *person*, not the SA: everything the in-pod `TestEnv`
        // creates derives `ztest.io/user` from this, so charging the sync to a
        // shared SA must not relabel its resources as that SA's.
        json!({ "name": "USER", "value": crate::naming::current_user() }),
        json!({ "name": SYNC_ID_ENV, "value": sync_id }),
        json!({ "name": SYNC_PROFILE_ENV, "value": profile }),
        json!({ "name": crate::backends::image::IMAGE_REFS_ENV, "value": image_refs_json }),
        json!({ "name": crate::engine::dylib::dylib_path_envvar(), "value": ld }),
        // The downward-API pod name, so the in-pod stop-watch can find itself.
        json!({
            "name": POD_NAME_ENV,
            "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } },
        }),
    ];
    if let Some(secret) = crate::backends::image::pull_secret() {
        env.push(json!({ "name": "ZTEST_PULL_SECRET", "value": secret }));
    }

    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": DRIVER_POD,
            "namespace": ns,
            "labels": {
                KIND_LABEL_KEY: KIND_LABEL_VALUE,
                SYNC_ID_KEY: sync_id,
                crate::qos::LABEL_USER: crate::naming::current_user(),
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "serviceAccountName": DRIVER_SA,
            "enableServiceLinks": false,
            "terminationGracePeriodSeconds": STOP_GRACE_SECS,
            "nodeSelector": { crate::qos::NVME_NODE_LABEL_KEY: crate::qos::NVME_NODE_LABEL_VALUE },
            "tolerations": [
                { "key": crate::qos::NVME_TAINT_KEY, "operator": "Exists", "effect": "NoSchedule" },
            ],
            "containers": [{
                "name": DRIVER_CONTAINER,
                "image": compiled.runner_image_ref,
                "imagePullPolicy": "IfNotPresent",
                "command": [target.binary_path, "--exact", target.test_name, "--nocapture"],
                "workingDir": target.cwd,
                "env": env,
                "resources": {
                    "requests": { "cpu": cpu, "memory": mem },
                    "limits": { "cpu": cpu, "memory": mem },
                },
            }],
        },
    }))
    .expect("driver pod manifest is valid")
}

// ─────────────────────────── list / status ────────────────────────────

/// A driver pod's `sync-id`, namespace, phase, and owning user.
fn row_of(p: &Pod) -> (String, String, String, String) {
    let label = |k: &str| {
        p.metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(k))
            .cloned()
            .unwrap_or_else(|| "-".into())
    };
    let phase = p
        .status
        .as_ref()
        .and_then(|s| s.phase.clone())
        .unwrap_or_else(|| "Unknown".into());
    (
        label(SYNC_ID_KEY),
        p.metadata.namespace.clone().unwrap_or_else(|| "-".into()),
        phase,
        label(crate::qos::LABEL_USER),
    )
}

async fn list(all_users: bool, json_out: bool) -> Result<(), String> {
    let client = client().await?;
    let api: Api<Pod> = Api::all(client);
    let lp = ListParams::default().labels(&kind_selector());
    let mut items = api
        .list(&lp)
        .await
        .map_err(|e| format!("list sync pods: {e}"))?
        .items;
    if !all_users {
        let me = crate::naming::current_user();
        items.retain(|p| row_of(p).3 == me);
    }

    if json_out {
        let rows: Vec<_> = items
            .iter()
            .map(|p| {
                let (id, ns, phase, user) = row_of(p);
                json!({ "id": id, "namespace": ns, "phase": phase, "user": user })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&rows).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    if items.is_empty() {
        println!(
            "no detached syncs{}",
            if all_users { "" } else { " (yours)" }
        );
        return Ok(());
    }
    println!(
        "{:<28} {:<24} {:<12} {:<12}",
        "SYNC-ID", "NAMESPACE", "PHASE", "USER"
    );
    for p in &items {
        let (id, ns, phase, user) = row_of(p);
        println!("{id:<28} {ns:<24} {phase:<12} {user:<12}");
    }
    Ok(())
}

async fn status(id: &str, json_out: bool) -> Result<(), String> {
    // Prefer the durable report (survives the pod); fall back to the live phase.
    let client = client().await?;
    let ns = namespace_for(id);
    if let Some(report) = read_report(&client, &ns, id).await? {
        if json_out {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?
            );
        } else {
            let theme = Theme::detect();
            println!("{}", report_headline(&theme, &report));
            // The violations and coverage gaps too: this is the only command that
            // reads a finished run, so a headline alone would leave the detail of
            // *why* it failed with nowhere to be printed.
            print_report_details(&theme, &report);
        }
        return Ok(());
    }
    let pod = find_driver(&client, &ns, id).await?;
    let (_, _, phase, _) = row_of(&pod);
    // No report yet, but a running engine publishes its state to the driver log —
    // so recover the newest tick rather than reporting only the pod phase.
    let api: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let live = watch::latest_progress(&api).await;
    let vitals = live.as_ref().and_then(|s| s.vitals.as_ref());

    if json_out {
        let progress = vitals.map(|v| {
            let (ok, total) = live.as_ref().expect("vitals imply state").probe_tally();
            json!({
                "height": v.height,
                "target": v.target,
                "pct": v.pct,
                "phase": v.phase,
                "reorg_depth": v.reorg_depth,
                "blocks_per_sec": v.blocks_per_sec,
                "eta_secs": v.eta.map(|d| d.as_secs()),
                "probes_ok": ok,
                "probes_total": total,
            })
        });
        println!(
            "{}",
            json!({ "id": id, "phase": phase, "progress": progress, "report": null })
        );
        return Ok(());
    }

    use owo_colors::OwoColorize as _;
    let theme = Theme::detect();
    let dot = theme.chars.dot.style(theme.styles.dim);
    let Some(mut state) = live else {
        println!(
            "{} sync {} {dot} {}",
            theme.chars.dot.style(theme.styles.count),
            id.style(theme.styles.count),
            format!("{phase} (starting; no tick published yet)").style(theme.styles.dim),
        );
        return Ok(());
    };
    // The log tail carries no pod phase of its own — it is a property of the
    // cluster, which only this side has read.
    state.pod_phase = phase;
    state.sync_id = id.to_string();
    print!(
        "{}",
        crate::ui::render_sync_status(&state, &theme, terminal_width())
    );
    Ok(())
}

/// Columns available for the status view.
///
/// A non-TTY (a pipe, a CI log) reports nothing, and the view's own default is
/// the right answer there: the layout is designed to fit a conventional
/// terminal, so a redirected `status` stays diffable rather than stretching to
/// whatever the last interactive session happened to be.
fn terminal_width() -> usize {
    console::Term::stdout()
        .size_checked()
        .map_or(80, |(_, cols)| usize::from(cols))
}

/// The themed one-line verdict headline shared by `status` and `watch`: a
/// pass/fail mark, the sync id + profile, and the tick/violation/gap counts,
/// coloured like `ztest run`'s end-of-run banner.
fn report_headline(theme: &Theme, r: &SyncReportMirror) -> String {
    use owo_colors::OwoColorize as _;
    let passed = r.passed();
    let (mark, style) = if passed {
        (theme.chars.ok, theme.styles.pass)
    } else {
        (theme.chars.warn, theme.styles.fail)
    };
    let dot = theme.chars.dot.style(theme.styles.dim);
    let gaps = if r.coverage_gaps.is_empty() {
        String::new()
    } else {
        format!(
            " {dot} {} gaps",
            r.coverage_gaps.len().style(theme.styles.count)
        )
    };
    format!(
        "{} {} {} {dot} {} {dot} {} ticks {dot} {} violations{gaps}",
        mark.style(style),
        r.sync_id.style(theme.styles.count),
        format!("[{}]", r.profile).style(theme.styles.dim),
        r.verdict.style(style),
        r.ticks.style(theme.styles.count),
        r.violations.len().style(theme.styles.count),
    )
}

/// The themed detail block under the headline (`report` only): each violation,
/// coverage gap, metric sample, and any fatal error, marked and coloured.
fn print_report_details(theme: &Theme, r: &SyncReportMirror) {
    use owo_colors::OwoColorize as _;
    let dot = theme.chars.dot.style(theme.styles.dim);
    // The span comes first because it is what decides whether this run's
    // numbers can be compared with any other's — a reader reaching for
    // `perf --base` needs to know before they try.
    if let Some(segment) = &r.segment {
        let work = segment
            .work
            .total()
            .map(|t| format!(" {dot} {} ops", crate::ui::text::thousands(t)))
            .unwrap_or_default();
        println!(
            "  {} segment {dot} {}{work}",
            theme.chars.dot.style(theme.styles.dim),
            segment.describe().style(theme.styles.count),
        );
    }
    for v in &r.violations {
        let at = v.height.map(|h| format!(" @{h}")).unwrap_or_default();
        println!(
            "  {} {}{at} {dot} {}",
            theme.chars.warn.style(theme.styles.fail),
            v.probe.style(theme.styles.fail),
            v.detail,
        );
    }
    for g in &r.coverage_gaps {
        println!(
            "  {} coverage gap {dot} {g}",
            theme.chars.dot.style(theme.styles.dim),
        );
    }
    if let Some(e) = &r.error {
        println!(
            "  {} error {dot} {e}",
            theme.chars.warn.style(theme.styles.fail),
        );
    }
}

/// Read + parse the report-mirror ConfigMap, if present.
async fn read_report(
    client: &Client,
    ns: &str,
    id: &str,
) -> Result<Option<SyncReportMirror>, String> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);
    let Some(cm) = api
        .get_opt(&report_cm_name(id))
        .await
        .map_err(|e| format!("read report: {e}"))?
    else {
        return Ok(None);
    };
    let Some(body) = cm.data.and_then(|d| d.get("report.json").cloned()) else {
        return Ok(None);
    };
    serde_json::from_str(&body)
        .map(Some)
        .map_err(|e| format!("parse report: {e}"))
}

// ─────────────────────────── stop / rm / watch ────────────────────────

async fn stop(id: &str) -> Result<(), String> {
    let client = client().await?;
    let ns = namespace_for(id);
    let _ = find_driver(&client, &ns, id).await?; // 404s clearly if unknown
    let api: Api<Pod> = Api::namespaced(client, &ns);
    let patch = json!({ "metadata": { "annotations": { STOP_ANNOTATION: "true" } } });
    api.patch(
        DRIVER_POD,
        &PatchParams::apply("ztest-sync"),
        &Patch::Merge(&patch),
    )
    .await
    .map_err(|e| format!("signal stop: {e}"))?;
    println!("sync {id}: stop signalled (graceful checkpoint → report → exit)");
    Ok(())
}

/// The profile name a driver pod was launched for, read back from its
/// `ZTEST_SYNC_PROFILE` env (the panel's label; falls back to the sync id).
fn driver_profile(pod: &Pod) -> Option<String> {
    pod.spec
        .as_ref()?
        .containers
        .first()?
        .env
        .as_ref()?
        .iter()
        .find(|e| e.name == SYNC_PROFILE_ENV)?
        .value
        .clone()
}

/// Find the driver pod for a sync id, with a clear not-found error.
async fn find_driver(client: &Client, ns: &str, id: &str) -> Result<Pod, String> {
    Api::<Pod>::namespaced(client.clone(), ns)
        .get_opt(DRIVER_POD)
        .await
        .map_err(|e| format!("get sync pod: {e}"))?
        .ok_or_else(|| format!("no sync with id `{id}` (namespace `{ns}` has no driver pod)"))
}

// ─────────────────────────────── describe ─────────────────────────────

async fn describe(name: &str) -> Result<(), String> {
    // Cluster-free: a local inventory dump of the zingo test binaries surfaces
    // each profile's static `SyncTestDecl`. (The running-body "Collect mode"
    // invariant/nemesis manifest is a follow-on; this prints what the annotation
    // declares without executing the body.)
    // No `--features` (the consumer crate enables `ztest/zingo` on its dep, and a
    // virtual-workspace root rejects a bare `--features`); the default selection
    // includes the sync suite.
    let build = crate::pipeline::build::index(&[])
        .await
        .map_err(|e| format!("local build/list for describe: {e}"))?;
    let BuildOutcome::Ok {
        selected_binaries, ..
    } = &build
    else {
        return Err("local build produced no test selection".into());
    };
    let (dump, _) = crate::pipeline::images::discover(selected_binaries).await;
    let DumpOutcome::Discovered { sync_tests, .. } = &dump else {
        return Err("local inventory dump failed".into());
    };
    let entry = sync_tests.iter().find(|s| s.name == name).ok_or_else(|| {
        let have: Vec<&str> = sync_tests.iter().map(|s| s.name.as_str()).collect();
        format!(
            "no sync profile named `{name}`{}",
            if have.is_empty() {
                String::new()
            } else {
                format!("; available: {}", have.join(", "))
            }
        )
    })?;
    println!("profile:     {}", entry.name);
    println!("test:        {}", entry.test_id);
    println!("subject:     {}", entry.subject);
    println!("qos tier:    {}", entry.qos);
    println!("timeout:     {}", entry.timeout);
    if !entry.tags.is_empty() {
        println!("tags:        {}", entry.tags.join(", "));
    }
    if !entry.description.is_empty() {
        println!("description: {}", entry.description);
    }
    Ok(())
}
