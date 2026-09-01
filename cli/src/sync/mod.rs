//! `ztest sync` — stateless controller for detached, ztest-owned chain syncs.
//!
//! - Sync outlives its terminal (state in k8s, no local daemon)
//! - Driver pod (`ztest.io/{kind=sync,sync-id,user}`) in the shared run namespace; the
//!   `zebrad`/`zaino` pods its in-pod `TestEnv` provisions in `ztest-sync-<id>`, which is
//!   deleted at run end (durable record lives in `ztest-obs`, see [`ztest::sync::SyncLaunch`])
//! - Any kubeconfig holder can `list`/`watch`/`stop` (syncs found by `kind=sync` label)
//! - `watch` = read-only tail; only `stop` ends a sync (`sync_mode = Shutdown` → checkpoint)
//! - `start` reuses `ztest run`'s on-cluster compile — same `#[ztest::sync_test]` body,
//!   distinguished only by `ZTEST_SYNC_ID` (see [`ztest::sync::detached`])

pub(crate) mod render;
mod watch;

mod perf;

use std::collections::{BTreeMap, HashMap};
use std::io::{IsTerminal, stdout};
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context as _, Result, anyhow};
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Pod};
use kube::Client;
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use serde_json::json;

use clap::{Args as ClapArgs, Subcommand};

use ztest::api::metrics::Series;
use ztest::api::naming::{RUN_NAMESPACE, RUN_SERVICE_ACCOUNT};
use ztest::api::pipeline::BuildOutcome;
use ztest::api::pipeline::DumpOutcome;
use ztest::api::pipeline::local_bake;
use ztest::api::pipeline::profiles::{self, ProfileStub};
use ztest::api::pipeline::remote_compile::{self, BakeRefs, RemoteCompileOutcome};
use ztest::api::profiling::Placement;
use ztest::api::resource::buildkit;
use ztest::api::{column_width, format_elapsed, format_span, thousands};
use ztest::sync::{
    KIND_LABEL_KEY, KIND_LABEL_VALUE, POD_NAME_ENV, POD_NAMESPACE_ENV, STOP_ANNOTATION,
    SYNC_ID_ENV, SYNC_ID_KEY, SYNC_PROFILE_ENV, SyncReportMirror, SyncStatus, driver_pod_for,
    find_driver, kind_selector, namespace_for, profiler_config_name, read_report,
    report_cm_namespace,
};
use ztest_ui::console::{Console, SceneFrame};
use ztest_ui::template::{Fields, draw};
use ztest_ui::{ComponentResources, ReportView, Theme, Transfers, pad};

/// Row shapes for what `sync` prints itself; the run view proper draws through
/// [`ztest_ui::render_sync_report`]
mod row {
    pub(super) const LIST_HEADER: &str = "  {id|dim} {ns|dim} {status|dim} {user|dim}";
    /// `mark` arrives pre-styled from [`ztest_ui::status_mark`] — sole verdict→ink map,
    /// shared with the run view
    /// Verdict ink rides the template (a tone is not bindable data); glyph + tone both
    /// come from `status_mark`, so they cannot disagree
    pub(super) fn list(tone: &str) -> String {
        format!("{{mark|{tone}}} {{id|bold}} {{ns|dim}} {{status}} {{user|dim}}")
    }

    pub(super) const HANDOFF: &str =
        "{@ok|pass} sync {id|bold} started {@dot|dim} namespace {ns|dim}";
    pub(super) const HANDOFF_CMD: &str = "  {@dot|dim} ztest sync {verb:<6} {id}";

    pub(super) const SCAN_NOTE: &str = "ztest sync: {note|dim}";
    /// Gutter = `ztest sync: `, so a blind spot hangs under the note it qualifies
    pub(super) const SCAN_BLIND_SPOT: &str = "            {file|dim}: {reason|dim}";
    pub(super) const ATTACH_INTACT: &str = "  {@dot|dim} {note}";
}

/// Bind and draw. No `*` cell and no spinner in any of these rows → zero width, zero
/// elapsed
/// Driver pod's sole container (named: a log request gates on the container's state,
/// not the pod phase)
const DRIVER_CONTAINER: &str = "sync";
/// Cooperative-shutdown grace: `sync_mode = Shutdown` → wallet checkpoint → report
/// mirror, before the kubelet `SIGKILL`s
const STOP_GRACE_SECS: i64 = 120;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Named cluster profile (see `ztest cluster`) bundling the image backend,
    /// registry, and kube-context — the same selector as `ztest run --cluster`.
    /// Applies to every `sync` subcommand; overrides the persisted default.
    /// Precedence: `--cluster` > ambient env > persisted default.
    #[arg(long, global = true, value_name = "NAME")]
    cluster: Option<String>,
    // absent = profile catalogue (the question asked before a name exists to type)
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

impl Args {
    /// `--cluster`, read back by `bind_cluster` in dispatch — the one place that binds a
    /// profile, for every subcommand
    pub(crate) fn cluster_profile(&self) -> Option<&str> {
        self.cluster.as_deref()
    }
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// List detached syncs (cluster-wide `kind=sync` query): id, namespace, status, user.
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
        /// Collect eBPF CPU/off-CPU profiles for the run's components, readable with
        /// `ztest sync perf`. Covers components sharing the driver's node.
        #[arg(
            long,
            default_value_t = true,
            num_args = 0..=1,
            default_missing_value = "true",
            action = clap::ArgAction::Set,
        )]
        profile: bool,
        /// Profiler sample rate, Hz.
        #[arg(long, value_name = "HZ", default_value_t = ztest::api::profiling::DEFAULT_HZ)]
        profile_hz: u32,
        /// Fraction of off-CPU (blocked) events sampled, 0..=1. `0` profiles on-CPU time
        /// only. Raising it costs perf-ring headroom — check `ztest sync perf` reports
        /// 0 dropped trace events before keeping a higher value.
        #[arg(
            long,
            value_name = "P",
            default_value_t = ztest::api::profiling::DEFAULT_OFF_CPU,
            value_parser = off_cpu_fraction,
        )]
        profile_off_cpu: f64,
        /// Leave the sync's namespace (component pods, their logs, PVCs) standing when the
        /// run finishes, for inspection. The verdict, metrics and profiles are kept either
        /// way — they live outside it.
        #[arg(long)]
        no_cleanup: bool,
    },
    /// Attach to a sync's live progress (read-only; detaching never stops it).
    Watch {
        /// Sync id.
        id: String,
    },
    /// One-shot status: the whole run in one screen, running or finished. A finished
    /// run reads from the mirror ConfigMap, so it works after the pod is gone.
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
        /// View one slice of the run, e.g. `11h..12h` — bounds are elapsed
        /// since the sync started.
        #[arg(long, value_name = "FROM..TO")]
        window: Option<String>,
        /// Which profiled component to view. Required: profiles from different
        /// processes are not comparable.
        #[arg(long, value_name = "NAME")]
        component: Option<String>,
        /// Compare against an earlier sync: per-op rate deltas, plus both runs'
        /// profiles retrieved side by side to diff in a viewer. Both runs must
        /// have covered the same declared height segment.
        #[arg(long, value_name = "SYNC_ID")]
        base: Option<String>,
        /// Profile blocked time instead of CPU time: where the component waited on disk,
        /// locks and timers. Read separately from the CPU profile, never merged — parked
        /// threads outweigh real work by volume and would bury it.
        #[arg(long)]
        off_cpu: bool,
    },
    /// Graceful stop: `sync_mode = Shutdown` → checkpoint → exit.
    Stop {
        /// Sync id.
        id: String,
    },
}

pub fn execute(args: Args) -> ExitCode {
    // Bind the cluster profile (image backend / registry / kube-context) up front, as
    // `ztest run` does — else `sync start` misses the on-cluster build target and every
    // cluster-bound subcommand falls back to the ambient env's inferred (kind) backend.
    //
    // `describe` exempt: purely local inventory dump, must work with no cluster at all
    // (activation offline-verifies the context and could reject a stale default).
    //
    // SAFETY: `activate` calls `set_var` → must precede `block_on`'s threads; still
    // single-threaded here.
    // bare catalogue exempt too: source-only, and requiring a cluster would fail hardest in the
    // checkout it exists to diagnose
    super::block_on("sync", super::Rt::Multi, run(args))
}

async fn run(args: Args) -> Result<()> {
    let Some(cmd) = args.cmd else {
        return catalogue();
    };
    match cmd {
        Cmd::List { all_users, json } => list(all_users, json).await,
        Cmd::Status { id, json } => status(&id, json).await,
        Cmd::Stop { id } => stop(&id).await,
        // Inspection, like `status`: reads artifacts, never touches the run
        Cmd::Perf { id, out, no_open, window, component, base, off_cpu } => {
            perf::perf(perf::Request {
                id,
                out,
                open: !no_open,
                window,
                component,
                base,
                profile: match off_cpu {
                    true => ztest::api::profiling::Profile::OffCpu,
                    false => ztest::api::profiling::Profile::OnCpu,
                },
            })
            .await
        }
        // Inspection: succeeds when it managed to observe, whatever the verdict
        Cmd::Watch { id } => watch::watch(&id).await.map(drop),
        Cmd::Describe { name } => describe(&name).await,
        Cmd::Start { name, watch, profile, profile_hz, profile_off_cpu, no_cleanup } => {
            start(&name, watch, profile.then_some((profile_hz, profile_off_cpu)), no_cleanup).await
        }
    }
}

async fn client() -> Result<Client> {
    ztest::api::cluster::client().await.context("kube client")
}

/// ServiceAccount a detached sync charges its QoS budget to = the *credential*, not the
/// person (ownership = [`naming::current_user`] / `ztest.io/user`, kept apart so several
/// developers can share one remote SA without inheriting each other's syncs)
///
/// [`naming::current_user`]: ztest::api::naming::current_user
fn service_account() -> String {
    let raw = std::env::var("ZTEST_SA")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "unknown".into());
    ztest::api::naming::slug(&raw, ztest::api::naming::DNS_LABEL_MAX)
}

// ───────────────────────── profile catalogue ──────────────────────────

/// `ztest sync` — profiles declared in the cwd's workspace.
///
/// - Source-derived + cluster-free (answers with no sync tests, no kubeconfig, no compile)
fn catalogue() -> Result<()> {
    let scan = profiles::scan()?;
    let theme = Theme::detect();
    print!("{}", render::listing(&scan, &theme, render::width()));
    Ok(())
}

/// `name` → declaration, pre-build (~150ms vs a full runner build).
///
/// - `Ok(None)` = scan uncertain (`cfg`-gated decl | unparsed file) → proceed, [`resolve_target`]
///   still rejects post-build
/// - Rejecting on uncertainty would block work the compiler accepts
fn preflight(name: &str) -> Result<Option<ProfileStub>> {
    let scan = profiles::scan()?;
    match scan.find(name) {
        Ok(found) => Ok(Some(found.clone())),
        Err(_) if scan.is_uncertain() => {
            let theme = Theme::detect();
            let note = format!("`{name}` not in source; scan incomplete, building anyway");
            eprintln!("{}", draw(row::SCAN_NOTE, &Fields::new().text("note", note), &theme));
            for spot in &scan.blind_spots {
                let f = Fields::new()
                    .text("file", spot.file.display().to_string())
                    .text("reason", &*spot.reason);
                eprintln!("{}", draw(row::SCAN_BLIND_SPOT, &f, &theme));
            }
            Ok(None)
        }
        Err(why) => {
            Err(anyhow!("{}", render::miss(name, &scan, &why, &Theme::detect(), render::width())))
        }
    }
}

// ─────────────────────────────── start ────────────────────────────────

/// `profiling` = `Some((hz, off_cpu))` when on (the default), `None` for `--profile false`
async fn start(
    name: &str,
    watch_after: bool,
    profiling: Option<(u32, f64)>,
    no_cleanup: bool,
) -> Result<()> {
    // pre-client, pre-build (wrong name answerable from source in ms)
    let stub = preflight(name)?;

    let sa = service_account();
    let sync_id = new_sync_id(name);
    let ns = namespace_for(&sync_id);
    let client = client().await?;

    // Same pinned console `ztest run` uses (build-pod → BuildKit compile → provisioning)
    // → both entry points render identically; off a TTY, linear to stderr
    let theme = Theme::detect();
    let session_start = Instant::now();
    let (console, guard) = if stdout().is_terminal() {
        let cancel_theme = theme.clone();
        let cancel_panel =
            Box::new(move |elapsed| ztest_ui::render_cancel_panel(elapsed, &cancel_theme));
        match Console::start(session_start, cancel_panel) {
            Ok((c, g)) => (Some(c), Some(g)),
            Err(_) => (None, None),
        }
    } else {
        (None, None)
    };

    let built =
        build_and_provision(&client, &sync_id, name, stub.as_ref(), console.as_ref(), &theme).await;

    // Teardown before anything else prints → handoff (or error) lands on a clean line
    if let Some(g) = guard {
        g.finish();
    }
    let (compiled, image_refs) = built?;

    // Profile name → binary + libtest test (the driver's `<bin> --exact <test>`), from the
    // inventory the compile dumped
    let target = resolve_target(&compiled, name)?;

    // Resolved before the reserve: the collector rides the driver pod, so its slice must be
    // in the amount admission holds. No Pyroscope = say so and run unprofiled, never launch
    // a collector pushing into nothing
    let collector = match profiling {
        Some((hz, off_cpu)) => {
            let c = ztest::api::profiling::Collector::for_sync(&client, &sync_id, &ns, hz, off_cpu)
                .await;
            match &c {
                // Never asserts *why*: an unready Pyroscope and an absent one look the
                // same here, and blaming setup for a broken pod sends the reader nowhere
                None => eprintln!(
                    "ztest sync: Pyroscope unreachable; starting unprofiled \
                     (`ztest sync perf` reports the reason)"
                ),
                // Placement is a property of the cluster, not a choice; say where it landed
                // only when that changes what the user must reach for (`perf` reads it back)
                Some(c) if c.placement == ztest::api::profiling::Placement::Host => eprintln!(
                    "ztest sync: nested kubelet — collector runs host-side (docker), not in-pod"
                ),
                Some(_) => {}
            }
            c
        }
        None => None,
    };

    // Reserve pre-create:
    // - Sync holds the tier for hours (unreserved = the ledger's biggest hole)
    // - CLI, not driver → admission can refuse while a terminal hears it
    // - `acquire` waits → a busy cluster queues the launch
    let reservation = reserve_sync_capacity(
        &client,
        &sync_id,
        &sa,
        &target,
        collector.as_ref().is_some_and(|c| c.placement == Placement::Sidecar),
    )
    .await?;

    // Pre-adoption: every failure path releases
    let launched = async {
        launch_driver(
            &client,
            &sync_id,
            name,
            &ns,
            &sa,
            &compiled,
            &target,
            &image_refs,
            collector.as_ref(),
            no_cleanup,
        )
        .await?;
        await_driver_running(&client, &sync_id).await
    }
    .await;
    if let Err(e) = launched {
        // Pod first, then the lease: capacity must not read free while a pod is
        // still terminating on it
        abandon_launch(&client, &sync_id, &ns).await;
        reservation.release().await;
        return Err(e);
    }
    // Driver renews from here; drop stops only *our* heartbeat
    drop(reservation);

    print_handoff(&theme, &sync_id, &ns);
    if watch_after {
        return attach(&theme, &sync_id).await;
    }
    Ok(())
}

/// Ledger lease id = the sync's `ZTEST_RUN_ID` → driver + every component pod
/// carry it as `ztest.io/run-id`, all attributed to this one reservation
pub(super) fn sync_lease_id(sync_id: &str) -> String {
    format!("sync-{sync_id}")
}

/// Reserve what this profile declared
///
/// - `Fixed` (footprint never changes) → peers keep the rest for the hours it runs
/// - `admitted` covers the driver pod as well as the components
/// - Amount = [`Target::profile`] → override held for the run, pods can grow into it
/// - Profiling adds a sidecar to the driver, so its slice is reserved here too (a pod is
///   charged its whole spec, and an unreserved container is invisible cluster load)
async fn reserve_sync_capacity(
    client: &Client,
    sync_id: &str,
    sa: &str,
    target: &Target,
    profiled: bool,
) -> Result<ztest::qos::ledger::Reservation> {
    let capacity = ztest::api::pipeline::probe_capacity(client).await?;
    let mut want = target.profile.admitted();
    if profiled {
        want = want.saturating_add(&ztest::api::profiling::resources());
    }
    // Fail fast on a reserve no cluster state can satisfy (`acquire` would poll 10 min,
    // then misreport it as peer contention)
    if !want.fits_within(&capacity.allocatable) {
        return Err(anyhow!("footprint {want} exceeds allocatable {}", capacity.allocatable));
    }
    ztest::qos::ledger::acquire(
        client,
        &sync_lease_id(sync_id),
        sa,
        &ztest::api::naming::current_user(),
        capacity,
        ztest::qos::ledger::Reserve::Fixed(want),
        ztest::qos::beacon::LeaseKind::Sync,
    )
    .await
    .context("reserve sync capacity")
}

/// Sync namespace + driver pod (split out → the reservation has one unwind path)
#[allow(clippy::too_many_arguments)]
async fn launch_driver(
    client: &Client,
    sync_id: &str,
    profile: &str,
    ns: &str,
    sa: &str,
    compiled: &RemoteCompileOutcome,
    target: &Target,
    image_refs: &BTreeMap<String, String>,
    collector: Option<&ztest::api::profiling::Collector>,
    no_cleanup: bool,
) -> Result<()> {
    let namespace = ensure_sync_namespace(client, ns, sync_id, &target.profile, no_cleanup).await?;
    let ns_uid = namespace
        .metadata
        .uid
        .clone()
        .with_context(|| format!("sync namespace {ns} has no uid to own the driver"))?;
    // Config before the pod that mounts it (a missing ConfigMap holds the driver in
    // `ContainerCreating`, not just the sidecar)
    if let Some(c) = collector.filter(|c| c.placement == Placement::Sidecar) {
        let cm = c.config_map();
        Api::<ConfigMap>::namespaced(client.clone(), RUN_NAMESPACE)
            .patch(&c.config_map, &PatchParams::apply("ztest-sync").force(), &Patch::Apply(&cm))
            .await
            .context("create profiler config")?;
    }
    let pod = build_driver_pod(
        sync_id, profile, ns, &ns_uid, sa, compiled, target, image_refs, collector, no_cleanup,
    );
    let created = Api::<Pod>::namespaced(client.clone(), RUN_NAMESPACE)
        .create(&PostParams::default(), &pod)
        .await
        .context("create driver pod")?;
    record_launch(client, sync_id, &created, collector).await?;
    match collector {
        Some(c) if c.placement == Placement::Sidecar => {
            adopt_profiler_config(client, &created, &c.config_map).await;
        }
        // Started after the pod exists: discovery matches on labels the pod carries, and a
        // collector racing ahead of it would idle on zero targets.
        //
        // Never fatal: a diagnostic that discards a driver already scheduled for a
        // twelve-hour sync is the wrong trade. `ztest cluster check` is the gate
        Some(c) => {
            if let Err(e) = ztest::api::profiling::start(
                sync_id,
                &c.host_config(),
                c.api_server.as_deref().unwrap_or_default(),
            )
            .await
            {
                eprintln!(
                    "ztest sync: profiler not started ({e}); running unprofiled — \
                     `ztest cluster check` names the fault"
                );
            }
        }
        None => {}
    }
    Ok(())
}

/// Owner-reference the config to the driver pod → collected with it, so no cleanup class
/// exists for a file that only ever serves one pod.
///
/// Best-effort: a lost ownerRef leaks one small ConfigMap, where failing the launch here
/// would discard a driver that is already running
async fn adopt_profiler_config(client: &Client, driver: &Pod, name: &str) {
    let Some(uid) = driver.metadata.uid.as_deref() else {
        return;
    };
    let owner = json!({ "metadata": { "ownerReferences": [{
        "apiVersion": "v1",
        "kind": "Pod",
        "name": driver.metadata.name,
        "uid": uid,
    }]}});
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    if let Err(e) = api.patch(name, &PatchParams::default(), &Patch::Merge(&owner)).await {
        eprintln!("ztest sync: profiler config {name} not owner-referenced: {e}");
    }
}

/// Tear down a launch that never reached `Running` (else `sync list` shows a zombie
/// holding no reservation). Best-effort — leftovers are `ztest cleanup`'s
async fn abandon_launch(client: &Client, sync_id: &str, ns: &str) {
    let pods: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    let _ = pods.delete(&driver_pod_for(sync_id), &Default::default()).await;
    // Explicit: the config outlives a launch that died before the pod existed to own it
    let configs: Api<ConfigMap> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    let _ = configs.delete(&profiler_config_name(sync_id), &Default::default()).await;
    let namespaces: Api<Namespace> = Api::all(client.clone());
    let _ = namespaces.delete(ns, &Default::default()).await;
    // Launch record outlives the namespace by design, so it needs deleting by name
    let reports: Api<ConfigMap> = Api::namespaced(client.clone(), report_cm_namespace());
    let _ = reports.delete(&ztest::sync::report_cm_name(sync_id), &Default::default()).await;
}

/// Stamp what only the controller knows, so no reader of a finished run needs a live object.
///
/// - Origin = pod creation, the clock `perf`/`status` already window against
/// - Fatal: an unrecorded launch is profiles that become unreadable at teardown, and the
///   caller's `abandon_launch` still has a pod to remove
async fn record_launch(
    client: &Client,
    sync_id: &str,
    driver: &Pod,
    collector: Option<&ztest::api::profiling::Collector>,
) -> Result<()> {
    let created = driver
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| SystemTime::from(t.0))
        .unwrap_or_else(SystemTime::now);
    let launch = ztest::sync::SyncLaunch {
        sync_id: sync_id.to_string(),
        started_ms: ztest::sync::epoch_millis(created),
        profiling: collector.map(|c| ztest::sync::LaunchProfiling {
            tenant: c.tenant.clone(),
            placement: c.placement,
            hz: c.hz,
            off_cpu: c.off_cpu,
        }),
    };
    ztest::sync::write_launch(client, &launch).await.context("record sync launch")
}

/// Wait for driver `Running` = where it takes over renewing (see `TestEnv::build`)
/// — caller renews meanwhile, bridging an image pull longer than the TTL
async fn await_driver_running(client: &Client, sync_id: &str) -> Result<()> {
    use ztest::api::pod_status as ps;

    let api: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    let name = driver_pod_for(sync_id);
    let mut unscheduled_since: Option<std::time::Instant> = None;
    let started = std::time::Instant::now();
    loop {
        if let Ok(pod) = api.get(&name).await
            && let Some(status) = pod.status.as_ref()
        {
            if ps::is_running(status) {
                return Ok(());
            }
            // Same deadline component pods get (waiting it out holds the whole tier for a
            // pod that never runs)
            if !ps::is_scheduled(status) {
                let since = *unscheduled_since.get_or_insert_with(std::time::Instant::now);
                if since.elapsed() >= ps::PENDING_TIMEOUT {
                    return Err(anyhow!(
                        "sync driver pod unschedulable after {}: {}",
                        format_span(since.elapsed()),
                        ps::schedule_blocker(status)
                            .unwrap_or_else(|| "no PodScheduled condition".to_string()),
                    ));
                }
            }
            if let Some(reason) = ps::fault(status) {
                return Err(anyhow!("sync driver pod failed to start: {reason}"));
            }
            if let Some(reason) = ps::image_error(status)
                && ps::pull_error_is_terminal(
                    &reason,
                    started,
                    std::time::Instant::now(),
                    ps::IMAGE_PULL_GRACE,
                )
            {
                return Err(anyhow!("sync driver pod image error: {reason}"));
            }
        }
        tokio::time::sleep(ps::POLL_INTERVAL).await;
    }
}

/// `--watch`: attach to the just-started sync, attach outcome → this command's result.
///
/// - Failed *attach* ≠ failed `start` (sync is durable from pod creation; a non-zero
///   `start` reads as "didn't launch" and invites a re-run = a second footprint)
/// - Failed *verdict* fails, as `ztest run` would (`--watch` = its foreground stand-in)
async fn attach(theme: &Theme, sync_id: &str) -> Result<()> {
    match watch::watch(sync_id).await {
        // Log ended with the driver gone: killed, evicted, or crashed pre-report. Silence
        // here would pass a pipeline on a sync that never reached a verdict.
        Ok(watch::WatchEnd::Settled(SyncStatus::Unresolved)) => {
            Err(anyhow!("sync {sync_id} ended without a report (`ztest sync status {sync_id}`)"))
        }
        Ok(watch::WatchEnd::Settled(status)) if !status.is_pass() => {
            Err(anyhow!("sync {sync_id} finished {status} (`ztest sync status {sync_id}`)"))
        }
        Ok(watch::WatchEnd::Settled(_) | watch::WatchEnd::Detached) => Ok(()),
        Err(detail) => {
            eprintln!("ztest sync: attach to {sync_id} failed: {detail}");
            let f = Fields::new().text(
                "note",
                format!("the sync is unaffected — re-attach with `ztest sync watch {sync_id}`"),
            );
            eprintln!("{}", draw(row::ATTACH_INTACT, &f, theme));
            Ok(())
        }
    }
}

/// Build the runner image + provision the profile's `dev!` component images, rendered
/// through the shared [`crate::console`] driver (identical UX to `ztest run`).
///
/// - Returns the compile outcome + the `DevImageId → pull-ref` map the driver gets
/// - On-cluster target → ephemeral BuildKit pod (torn down whatever the outcome); every
///   other target bakes on this machine
async fn build_and_provision(
    client: &Client,
    sync_id: &str,
    profile: &str,
    stub: Option<&ProfileStub>,
    console: Option<&Console>,
    theme: &Theme,
) -> Result<(RemoteCompileOutcome, BTreeMap<String, String>)> {
    use ztest::api::pipeline::Phase;
    use ztest_ui::console::commit_phase;
    use ztest_ui::{BuildState, render_sync_build_panel, render_transfers};

    let context =
        ztest::api::cluster_config::active_context().unwrap_or_else(|| "(cluster)".into());
    let started = Instant::now();

    // Repaint `Building`: left = this sync's context + the shared inventory build line,
    // right = the transfer tracker
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
                mid: None,
                right: render_transfers(&transfers, elapsed, &theme),
                live: None,
            });
        }
    };

    // Shared driver's `• …/✓ …` boundary lines land in scrollback; a new sub-phase
    // repaints the panel's build row
    let mut on_phase = |ev: Phase<'_>| {
        if let Some(sub) = commit_phase(console, theme, ev) {
            paint(
                BuildState::Compiling { started_at: Instant::now(), phase: Some(sub) },
                "Building",
                &Transfers::default(),
            );
        }
    };

    // `-p <pkg> --test <target>` from the scan → unrelated broken targets never compile
    // - empty when the scan could not identify the profile (= old whole-workspace bake)
    // - no `--features` (virtual-workspace root rejects a bare one)
    let list_args = stub.map(ProfileStub::cargo_args).unwrap_or_default();

    let (compiled, build_pod) = if ztest::backends::image::builds_on_cluster() {
        let refs = BakeRefs {
            runner_repo_ref: ztest::backends::image::runner_repo_ref()
                .context("no runner registry; set ZTEST_IMAGE_REGISTRY")?,
        };

        // Ephemeral BuildKit pod (own phase + live timer), Ready before compiling
        on_phase(Phase::Start("startup builder"));
        let t_builder = Instant::now();
        let build_pod = {
            let p =
                buildkit::create_build_pod(client, sync_id, &ztest::api::naming::current_user())
                    .await
                    .context("create build pod")?;
            if let Err(e) = buildkit::wait_build_pod_ready(client, &p).await {
                buildkit::delete_build_pod(client, &p).await;
                return Err(anyhow!("build pod not ready: {e}"));
            }
            p
        };
        on_phase(Phase::Done { label: "builder pod ready", dur: t_builder.elapsed() });

        // Console → BuildKit progress through a remote PTY into the emulator grid; off it,
        // line per line to stderr
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
        match remote_compile::compile_on_cluster(
            client,
            &build_pod,
            &list_args,
            &refs,
            sync_id,
            Some(compile_out),
            Some(&mut on_phase),
        )
        .await
        {
            Ok(c) => (c, Some(build_pod)),
            Err(e) => {
                if let Some(c) = console {
                    c.flush_live();
                }
                buildkit::delete_build_pod(client, &build_pod).await;
                return Err(e.into());
            }
        }
    } else {
        match local_bake::bake_locally(
            &list_args,
            sync_id,
            console.map(|c| c as &dyn ztest::proc::ChildHost),
            Some(&mut on_phase),
        )
        .await
        {
            Ok(c) => (c, None),
            Err(e) => {
                if let Some(c) = console {
                    c.flush_live();
                }
                return Err(e.into());
            }
        }
    };

    // Provision the selection's `dev!` images (on the build pod when there is one), tracked
    // in the right column → the map the driver's in-pod `TestEnv` resolves them from
    let image_refs = provision_components(
        client,
        build_pod.as_deref(),
        &compiled,
        profile,
        console,
        |transfers: &Transfers| {
            let (test_count, binary_count) = match &compiled.build {
                BuildOutcome::Ok { test_count, binary_count, .. } => (*test_count, *binary_count),
                _ => (0, 0),
            };
            paint(
                BuildState::Ok { test_count, binary_count, elapsed: started.elapsed() },
                "Provisioning",
                transfers,
            );
        },
    )
    .await;

    if let Some(pod) = &build_pod {
        buildkit::delete_build_pod(client, pod).await;
    }
    Ok((compiled, image_refs?))
}

/// Provision the compile's `dev!` images through the shared transfer-tracker driver →
/// `DevImageId → pull-ref` map
async fn provision_components(
    client: &Client,
    build_pod: Option<&str>,
    compiled: &RemoteCompileOutcome,
    profile: &str,
    console: Option<&Console>,
    repaint: impl FnMut(&Transfers),
) -> Result<BTreeMap<String, String>> {
    let DumpOutcome::Discovered {
        images, seeds, images_by_binary, deps_by_binary, sync_tests, ..
    } = &compiled.dump
    else {
        return Err(anyhow!("inventory dump failed (no component images resolved)"));
    };
    // Dump unions seeds across the compiled binaries; only this profile's are wanted.
    // `cargo_args` already narrows to `-p/--test`, but falls back to a whole-workspace bake
    // when the scan cannot identify the profile, and never splits tests within one binary
    let seeds = match (&compiled.build, sync_tests.iter().find(|s| s.name == profile)) {
        (BuildOutcome::Ok { selected_binaries, .. }, Some(entry)) => ztest::api::plan::for_sync(
            selected_binaries,
            entry,
            images_by_binary,
            deps_by_binary,
            seeds,
        )
        .roots
        .first()
        .map(|r| r.seeds.clone())
        .unwrap_or_default(),
        _ => seeds.clone(),
    };
    if images.is_empty() && seeds.is_empty() {
        return Ok(BTreeMap::new());
    }
    let graph =
        ztest::api::resource::plan_runtime(images, &seeds).context("plan component images")?;
    let states = ztest_ui::console::provision_with_tracker(
        &graph,
        client.clone(),
        build_pod.map(str::to_string),
        console,
        repaint,
    )
    .await;
    // `dev_image_refs` drops failed nodes → un-checked, a failed build reaches the driver as
    // "not in the build manifest", which names the wrong fix
    let failed: Vec<String> = states
        .iter()
        .filter(|(id, _)| !id.is_optional())
        .filter_map(|(id, state)| match state {
            ztest::api::resource::NodeState::Failed(why) => {
                Some(format!("{}: {why}", id.display_label()))
            }
            ztest::api::resource::NodeState::Blocked => {
                Some(format!("{} (blocked by failed dep)", id.display_label()))
            }
            _ => None,
        })
        .collect();
    if !failed.is_empty() {
        return Err(anyhow!("component provisioning failed:\n  {}", failed.join("\n  ")));
    }
    Ok(ztest::api::resource::dev_image_refs(images_by_binary, &states))
}

/// Post-build handoff: themed summary of the started sync + its follow-up commands, onto
/// clean stdout after the console tears down
fn print_handoff(theme: &Theme, sync_id: &str, ns: &str) {
    let started = Fields::new().text("id", sync_id).text("ns", ns);
    println!("{}", draw(row::HANDOFF, &started, theme));
    for verb in ["watch", "status", "report", "stop"] {
        let f = Fields::new().text("verb", verb).text("id", sync_id);
        println!("{}", draw(row::HANDOFF_CMD, &f, theme));
    }
}

/// Driver's `<bin> --exact <test>` target, from the compile's inventory dump + selection.
struct Target {
    binary_path: String,
    test_name: String,
    cwd: String,
    profile: ztest::qos::QosProfile,
}

/// Profile `name` → its binary + libtest test name + effective QoS profile.
///
/// - Dump's `test_id` (`crate::…::fn`) minus the crate segment = the libtest name
/// - Binary = whichever selection contains that test (derived, never guessed)
/// - `profile` = declared tier + declared override → one resolved value sizes the launch
fn resolve_target(compiled: &RemoteCompileOutcome, name: &str) -> Result<Target> {
    let DumpOutcome::Discovered { sync_tests, .. } = &compiled.dump else {
        return Err(anyhow!("inventory dump failed"));
    };
    let entry = sync_tests.iter().find(|s| s.name == name).ok_or_else(|| {
        let have: Vec<&str> = sync_tests.iter().map(|s| s.name.as_str()).collect();
        anyhow!("no sync profile `{name}`; have: {}", have.join(", "))
    })?;
    // `test_id` = `crate::module::fn`; in-binary libtest name drops the crate segment
    // (`module_path!()` at an integration test's root is the crate)
    let libtest =
        entry.test_id.split_once("::").map(|(_, rest)| rest).unwrap_or(entry.test_id.as_str());

    let BuildOutcome::Ok { selected_binaries, .. } = &compiled.build else {
        return Err(anyhow!("on-cluster build produced no test selection"));
    };
    let bin = selected_binaries
        .iter()
        .find(|b| b.selected_tests.iter().any(|t| t == libtest))
        .with_context(|| format!("profile `{name}`: test `{libtest}` in no compiled binary"))?;
    // Unknown tier label → refuse (never reserve at a default the profile did not ask for)
    let profile = entry
        .profile()
        .with_context(|| format!("profile `{name}`: qos `{}` is not a tier", entry.qos))?;
    Ok(Target {
        binary_path: bin.binary_path.to_string_lossy().into_owned(),
        test_name: libtest.to_string(),
        cwd: bin.cwd.to_string_lossy().into_owned(),
        profile,
    })
}

/// Short, DNS-safe, unique: `<name-slug>-<rand8>`
fn new_sync_id(name: &str) -> String {
    format!("{}-{:08x}", ztest::api::naming::slug(name, 24), rand::random::<u32>())
}

/// Idempotently create the sync's persistent namespace — the deletion unit for everything
/// this run creates in-cluster.
///
/// - TTL at creation, never at completion: a driver that is OOM-killed or evicted annotates
///   nothing, and that is exactly the run that would leak
/// - 2x the tier's hard cap → a live sync can never be reaped out from under itself;
///   `--no-cleanup` doubles it again rather than removing the bound
/// - Driver shortens it on a clean finish ([`mark_finished`](ztest::sync::mark_finished))
/// - RBAC-free: the run role grants no `rbac` verbs and no `serviceaccounts` write, so a
///   run cannot mint itself authority ([`policy`](ztest::api::resource::policy)) — a
///   per-sync SA + RoleBinding is uncreatable by this credential, and would drop the
///   role's cluster-scoped rules anyway. Driver runs as the run identity; see
///   [`build_driver_pod`].
async fn ensure_sync_namespace(
    client: &Client,
    ns: &str,
    sync_id: &str,
    profile: &ztest::qos::QosProfile,
    no_cleanup: bool,
) -> Result<Namespace> {
    let ttl = match no_cleanup {
        true => ztest::sync::held_ttl(profile),
        false => ztest::sync::birth_ttl(profile),
    };
    let namespace: Namespace = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": ns,
            "labels": {
                KIND_LABEL_KEY: KIND_LABEL_VALUE,
                SYNC_ID_KEY: sync_id,
                ztest::qos::LABEL_USER: ztest::api::naming::current_user(),
            },
            "annotations": { ztest::api::naming::TTL_ANNOTATION: ztest::api::naming::ttl_value(ttl) },
        },
    }))
    .expect("static Namespace manifest is valid");
    Api::<Namespace>::all(client.clone())
        .patch(ns, &PatchParams::apply("ztest-sync").force(), &Patch::Apply(&namespace))
        .await
        .with_context(|| format!("create sync namespace {ns}"))
}

/// Detached driver pod: baked runner image running `<bin> --exact <test>`, as a
/// `ztest run` runner pod does — same *where* and *as whom*.
///
/// - [`RUN_NAMESPACE`] as [`RUN_SERVICE_ACCOUNT`], the one identity `ztest cluster setup`
///   provisions with the RBAC a component-spawning test needs
/// - Provisions into the sync's namespace via
///   [`TEST_NAMESPACE_ENV`](ztest::api::naming::TEST_NAMESPACE_ENV) → needs no identity of its own
/// - `kind=sync`-labelled, sync tier, NVMe pool, detached env, no reaper/timeout
#[allow(clippy::too_many_arguments)]
fn build_driver_pod(
    sync_id: &str,
    profile: &str,
    ns: &str,
    ns_uid: &str,
    sa: &str,
    compiled: &RemoteCompileOutcome,
    target: &Target,
    image_refs: &BTreeMap<String, String>,
    collector: Option<&ztest::api::profiling::Collector>,
    no_cleanup: bool,
) -> Pod {
    // Declared profile's runner slice (already covered by `reserve_sync_capacity`)
    let (cpu, mem) = target.profile.runner.guaranteed_cpu_mem("sync driver pod");

    // Dynamic-library search path the baked binary needs (libstd links dynamically),
    // derived from the same on-cluster build meta the engine uses for a runner pod
    let BuildOutcome::Ok { summary, .. } = &compiled.build else {
        // `resolve_target` already proved this arm
        unreachable!("build outcome validated in resolve_target");
    };
    let ld = ztest::api::engine::dylib_path_value(&summary.rust_build_meta)
        .to_string_lossy()
        .into_owned();

    let image_refs_json = serde_json::to_string(image_refs).unwrap_or_else(|_| "{}".into());

    let mut env = vec![
        json!({ "name": "ZTEST_ENGINE", "value": "1" }),
        // *Billing* SA (`ztest.io/sa`, the ledger's cost centre), not the credential: the
        // driver authenticates as `RUN_SERVICE_ACCOUNT`, bills whoever launched it
        json!({ "name": "ZTEST_SA", "value": sa }),
        json!({ "name": ztest::api::naming::TEST_NAMESPACE_ENV, "value": ns }),
        // In-pod `RunCoords` derives from this → every component pod carries it
        // as `ztest.io/run-id`, same reservation
        json!({ "name": "ZTEST_RUN_ID", "value": sync_lease_id(sync_id) }),
        // Launching *person*, not the SA: the in-pod `TestEnv` derives `ztest.io/user`
        // from this, so a shared billing SA must not relabel its resources
        json!({ "name": "USER", "value": ztest::api::naming::current_user() }),
        json!({ "name": SYNC_ID_ENV, "value": sync_id }),
        json!({ "name": SYNC_PROFILE_ENV, "value": profile }),
        json!({ "name": ztest::backends::image::IMAGE_REFS_ENV, "value": image_refs_json }),
        json!({ "name": ztest::api::engine::dylib_path_envvar(), "value": ld }),
        // Downward-API pod identity → the in-pod stop-watch finds itself. Both halves:
        // the driver's namespace ≠ the sync namespace it provisions into.
        json!({
            "name": POD_NAME_ENV,
            "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } },
        }),
        json!({
            "name": POD_NAMESPACE_ENV,
            "valueFrom": { "fieldRef": { "fieldPath": "metadata.namespace" } },
        }),
    ];
    if no_cleanup {
        env.push(json!({ "name": ztest::api::cluster::NO_CLEANUP_ENV, "value": "1" }));
    }
    if let Some(secret) = ztest::backends::image::pull_secret() {
        env.push(json!({ "name": "ZTEST_PULL_SECRET", "value": secret }));
    }

    let driver_container = json!({
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
    });
    // Collector goes in `initContainers` as a native sidecar, never `containers`: Alloy
    // never exits, and a regular container would hold this `restartPolicy: Never` pod at
    // `Running` after the driver finishes — a sync that never settles
    // `hostPID` is pod-level, so the driver container shares it while profiling (the cost
    // of co-locating the collector rather than running a DaemonSet)
    let (sidecars, volumes, host_pid) = match collector {
        // Host placement costs the driver nothing: no sidecar, no `hostPID`, no volumes
        Some(c) if c.placement == Placement::Sidecar => (
            json!([serde_json::to_value(c.container()).expect("container serialises")]),
            serde_json::to_value(c.volumes()).expect("volumes serialise"),
            true,
        ),
        _ => (json!([]), json!([]), false),
    };

    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": driver_pod_for(sync_id),
            "namespace": RUN_NAMESPACE,
            // Cluster-scoped owner, so a namespaced dependent in *another* namespace is
            // legal (cross-*namespace* refs are not): deleting the sync namespace GCs the
            // driver with it, making that namespace the whole run's single deletion unit
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "Namespace",
                "name": ns,
                "uid": ns_uid,
            }],
            "labels": {
                KIND_LABEL_KEY: KIND_LABEL_VALUE,
                SYNC_ID_KEY: sync_id,
                ztest::qos::LABEL_USER: ztest::api::naming::current_user(),
                // Ledger attribution. Absent → the driver's footprint counts as
                // foreign load, subtracted twice, invisible to `assert_invariant`
                ztest::qos::LABEL_RUN_ID: sync_lease_id(sync_id),
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "serviceAccountName": RUN_SERVICE_ACCOUNT,
            "enableServiceLinks": false,
            "terminationGracePeriodSeconds": STOP_GRACE_SECS,
            "nodeSelector": { ztest::qos::NVME_NODE_LABEL_KEY: ztest::qos::NVME_NODE_LABEL_VALUE },
            "tolerations": [
                { "key": ztest::qos::NVME_TAINT_KEY, "operator": "Exists", "effect": "NoSchedule" },
            ],
            "hostPID": host_pid,
            "volumes": volumes,
            "initContainers": sidecars,
            "containers": [driver_container],
        },
    }))
    .expect("driver pod manifest is valid")
}

// ─────────────────────────── list / status ────────────────────────────

/// What a driver pod alone can say. `pod_phase` = kubelet's word, never rendered raw
/// ([`SyncStatus::observe`] turns it into the status every command prints)
pub(super) struct SyncRow {
    pub(super) id: String,
    pub(super) namespace: String,
    pub(super) user: String,
}

/// Kubelet's phase for a driver pod, the only thing a pod still answers for a sync
pub(super) fn pod_phase(p: &Pod) -> Option<String> {
    p.status.as_ref()?.phase.clone()
}

/// One row per sync id across both halves. Owner from whichever half carries the label —
/// a record written before owners were stamped on records still lists under its driver pod
fn merge_rows(records: &[ConfigMap], pods: &HashMap<String, Pod>) -> Vec<SyncRow> {
    let owner_of = |m: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta| {
        m.labels.as_ref().and_then(|l| l.get(ztest::qos::LABEL_USER)).cloned()
    };
    let mut owners: BTreeMap<String, Option<String>> = BTreeMap::new();
    for cm in records {
        let Some(id) = cm.metadata.labels.as_ref().and_then(|l| l.get(SYNC_ID_KEY)) else {
            continue;
        };
        let slot = owners.entry(id.clone()).or_default();
        *slot = slot.take().or_else(|| owner_of(&cm.metadata));
    }
    for (id, pod) in pods {
        let slot = owners.entry(id.clone()).or_default();
        *slot = slot.take().or_else(|| owner_of(&pod.metadata));
    }
    owners
        .into_iter()
        .map(|(id, owner)| SyncRow {
            // Topology's namespace, not the driver's: the one a reader would `kubectl` into,
            // and the key `status` windows its metrics by
            namespace: namespace_for(&id),
            user: owner.unwrap_or_else(|| "-".into()),
            id,
        })
        .collect()
}

/// Driver pods by sync id. Hard error, never an empty map — an unreadable list would
/// render every running sync `Unresolved`, which reads as "no verdict coming"
async fn driver_pods(client: &Client) -> Result<HashMap<String, Pod>> {
    let api: Api<Pod> = Api::all(client.clone());
    let list = api
        .list(&ListParams::default().labels(&kind_selector()))
        .await
        .context("list sync driver pods")?;
    Ok(list
        .items
        .into_iter()
        .filter_map(|p| {
            let id = p.metadata.labels.as_ref()?.get(SYNC_ID_KEY)?.clone();
            Some((id, p))
        })
        .collect())
}

/// Verdicts out of records already fetched — `list` reads that collection once
fn mirrors_of(records: &[ConfigMap]) -> HashMap<String, SyncReportMirror> {
    records
        .iter()
        .filter_map(|cm| {
            let body = cm.data.as_ref()?.get(ztest::sync::REPORT_KEY)?;
            let report: SyncReportMirror = serde_json::from_str(body).ok()?;
            Some((report.sync_id.clone(), report))
        })
        .collect()
}

async fn list(all_users: bool, json_out: bool) -> Result<()> {
    let client = client().await?;
    // Host-placed collectors outlive nothing but their driver, and no process is resident to
    // notice it ended — so every command that already reads sync liveness sweeps them
    ztest::api::profiling::reap_finished(&client).await;
    // Union, keyed by sync id: the record outlives the pod (reaped with its namespace), and
    // the pod pre-dates the record (a sync launched before records existed, or still running
    // under an older ztest). Either half alone drops syncs from the listing
    let records: Api<ConfigMap> = Api::namespaced(client.clone(), report_cm_namespace());
    let items = records
        .list(&ListParams::default().labels(&kind_selector()))
        .await
        .context("list sync records")?
        .items;
    let pods = driver_pods(&client).await?;
    let mirrors = mirrors_of(&items);
    let me = ztest::api::naming::current_user();
    let rows: Vec<(SyncRow, SyncStatus)> = merge_rows(&items, &pods)
        .into_iter()
        .filter(|r| all_users || r.user == me)
        .map(|r| {
            let phase = pods.get(&r.id).and_then(pod_phase);
            let status = SyncStatus::observe(phase.as_deref(), mirrors.get(&r.id));
            (r, status)
        })
        .collect();

    if json_out {
        let rows: Vec<_> = rows
            .iter()
            .map(|(r, status)| {
                json!({
                    "id": r.id,
                    "namespace": r.namespace,
                    "status": status.to_string(),
                    "user": r.user,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("no detached syncs{}", if all_users { "" } else { " (yours)" });
        return Ok(());
    }
    // Columns measured across the rows they hold: a fixed gutter either wastes the width
    // a short id leaves or lets a long one shear the table
    let theme = Theme::detect();
    let statuses: Vec<String> = rows.iter().map(|(_, s)| s.to_string()).collect();
    let id_col = column_width(rows.iter().map(|(r, _)| r.id.as_str()).chain(["SYNC-ID"]), 12, 40);
    let ns_col =
        column_width(rows.iter().map(|(r, _)| r.namespace.as_str()).chain(["NAMESPACE"]), 12, 36);
    let status_col = column_width(statuses.iter().map(String::as_str).chain(["STATUS"]), 8, 16);

    let head = Fields::new()
        .text("id", pad("SYNC-ID", id_col))
        .text("ns", pad("NAMESPACE", ns_col))
        .text("status", pad("STATUS", status_col))
        .text("user", "USER");
    println!("{}", draw(row::LIST_HEADER, &head, &theme));
    for ((r, status), text) in rows.iter().zip(&statuses) {
        let (mark, tone) = ztest_ui::status_mark(*status, &theme);
        let f = Fields::new()
            .text("mark", mark)
            .text("id", pad(&r.id, id_col))
            .text("ns", pad(&r.namespace, ns_col))
            .text("status", pad(text, status_col))
            .text("user", &*r.user);
        println!("{}", draw(&row::list(tone), &f, &theme));
    }
    Ok(())
}

/// One view, live or finished: the header's source differs, every panel below it does
/// not (Prometheus scrapes a running sync exactly as it scraped a finished one)
async fn status(id: &str, json_out: bool) -> Result<()> {
    // Durable report first (survives the pod), else the live tick stream
    let client = client().await?;
    let ns = namespace_for(id);
    if let Some(report) = read_report(&client, id).await? {
        if json_out {
            println!("{}", serde_json::to_string_pretty(&report)?);
            return Ok(());
        }
        let view = build_report_view(&client, id, &ns, mirror_header(&report)).await;
        print!("{}", ztest_ui::render_sync_report(&view, &Theme::detect(), render::width()));
        return Ok(());
    }

    let pod = find_driver(&client, id).await?;
    let status = SyncStatus::observe(pod_phase(&pod).as_deref(), None);
    // No report yet, but a running engine publishes state to the driver log → recover the
    // newest tick rather than reporting the pod phase alone
    let live = watch::latest_progress(&watch::DriverPod::new(&client, id)).await;

    if json_out {
        let vitals = live.as_ref().and_then(|s| s.vitals.as_ref());
        let progress = vitals.map(|v| {
            let (ok, total) = live.as_ref().expect("vitals imply state").probe_tally();
            json!({
                "height": v.height,
                "target": v.target,
                "pct": v.pct,
                "phase": v.phase,
                "phase_detail": v.phase_detail,
                "reorg_depth": v.reorg_depth,
                "blocks_per_sec": v.pace.map(|p| p.per_sec),
                "eta_secs": v.pace.and_then(|p| p.eta).map(|d| d.as_secs()),
                "probes_ok": ok,
                "probes_total": total,
            })
        });
        let status = status.to_string();
        println!("{}", json!({ "id": id, "status": status, "progress": progress, "report": null }));
        return Ok(());
    }

    let view = build_report_view(&client, id, &ns, live_header(id, status, live.as_ref())).await;
    print!("{}", ztest_ui::render_sync_report(&view, &Theme::detect(), render::width()));
    Ok(())
}

/// How long a violating probe has gone unsatisfied, against the deadline it has to
/// recover in. An `eventually` probe's countdown is what shows a stall coming, so it
/// stands where a finished run puts the violation's detail
fn unsatisfied_for(p: &ztest_ui::ProbeRow) -> String {
    match (p.since_satisfied, p.window) {
        (Some(since), Some(window)) => {
            format!("unsatisfied {} of {}", format_elapsed(since), format_elapsed(window))
        }
        (Some(since), None) => format!("unsatisfied {}", format_elapsed(since)),
        (None, Some(window)) => format!("never satisfied, {} allowed", format_elapsed(window)),
        (None, None) => "violating".to_string(),
    }
}

/// Blocks the last grid sample may honestly trail the record by: one step + the rate
/// window's lag, at the run's own pace.
///
/// Self-scaling — a regtest run moves thousands inside one step, a stalled mainnet one none
fn stale_scrape_slack(elapsed: Duration, blocks_per_sec: Option<f64>) -> u32 {
    let Some(pace) = blocks_per_sec.filter(|p| p.is_finite() && *p >= 0.0) else {
        // Unknown pace → no honest bound, so never flag
        return u32::MAX;
    };
    let grid = ztest::api::metrics::Grid::for_span(elapsed);
    let lag = (grid.step + grid.rate_window).as_secs_f64();
    ((pace * lag).ceil() as u32).max(1)
}

/// Header of a finished run: the mirror is the verdict, and outlives the pod
fn mirror_header(r: &SyncReportMirror) -> ReportView {
    ReportView {
        sync_id: r.sync_id.clone(),
        profile: r.profile.clone(),
        status: SyncStatus::Finished(r.verdict),
        // Ops ride the segment they were counted over: the number compares between runs
        // only across a shared span, which is the thing beside it
        segment: r.segment.as_ref().map(|s| match s.work.total() {
            Some(ops) => format!("{} ({} ops)", s.describe(), thousands(ops)),
            None => s.describe(),
        }),
        // Both durable — a height off the query grid lands up to one step early, printing
        // a shortfall that is scrape cadence rather than the run
        height: r.segment.as_ref().map(|s| s.to).zip(r.target),
        unpublished: r.unpublished.clone(),
        elapsed: r
            .segment
            .as_ref()
            .map(|s| Duration::from_millis(s.elapsed_ms))
            .unwrap_or_default(),
        violations: r
            .violations
            .iter()
            .map(|v| {
                let at = v.height.map(|h| format!("@{h} ")).unwrap_or_default();
                (v.probe.clone(), format!("{at}{}", v.detail))
            })
            .collect(),
        coverage_gaps: r.coverage_gaps.clone(),
        error: r.error.clone(),
        ticks: r.ticks,
        dropped_snapshots: r.dropped_snapshots,
        ..ReportView::default()
    }
}

/// Header of a run with no mirror yet.
///
/// - `status` comes from the driver pod, never from the tick stream: a subject at tip is a
///   chain fact, and only a mirrored report ends a run
/// - Height/eta seed the header from the tick stream, which leads the TSDB by a scrape;
///   [`build_report_view`] overrides them only where Prometheus has an answer
/// - Violating probes stand in for the mirror's violations — same question, asked of a
///   run that has not finished answering it
fn live_header(
    id: &str,
    status: SyncStatus,
    live: Option<&ztest_ui::SyncWatchState>,
) -> ReportView {
    let Some(state) = live else {
        return ReportView {
            sync_id: id.to_string(),
            status,
            note: Some("no tick published yet".into()),
            ..ReportView::default()
        };
    };
    let vitals = state.vitals.as_ref();
    ReportView {
        sync_id: id.to_string(),
        profile: state.profile.clone(),
        status,
        phase: vitals.and_then(|v| v.phase),
        phase_detail: vitals.and_then(|v| v.phase_detail.clone()),
        height: vitals.and_then(|v| Some((v.height, v.target?))),
        eta: vitals.and_then(|v| v.pace).and_then(|p| p.eta),
        probes: Some(state.probe_tally()),
        violations: state
            .probes
            .iter()
            .filter(|p| matches!(p.state, ztest::sync::ProbeState::Violating))
            .map(|p| (p.name.clone(), unsatisfied_for(p)))
            .collect(),
        note: state.metrics_note.clone(),
        ..ReportView::default()
    }
}

/// Settled verdict, `watch`-only: [`mirror_header`]'s [`ReportView`] drawn without the
/// panels (a live tail already scrolled them past, and `watch` queries no TSDB).
///
/// - Same view + renderer as `status`, so the two cannot disagree on a glyph, an ink,
///   a count or a violation's wording
fn report_verdict(theme: &Theme, r: &SyncReportMirror) -> String {
    ztest_ui::render_sync_verdict(&mirror_header(r), theme, render::width())
}

/// Fill a seeded header out from whatever Prometheus holds for the run's window.
///
/// - Seed = [`mirror_header`] or [`live_header`]; everything below the header is read
///   the same way either way (a running sync is scraped exactly as a finished one was)
/// - Record is best-effort (no metrics stack / unreachable / expired retention): the
///   panels then state why they are empty rather than failing the report
/// - Window = driver pod's creation → its exit, or → now while it still runs
async fn build_report_view(
    client: &Client,
    id: &str,
    ns: &str,
    mut view: ReportView,
) -> ReportView {
    use ztest::api::metrics::Facet;

    let Ok(driver) = find_driver(client, id).await else {
        view.note = Some("driver pod is gone; its window cannot be recovered".into());
        return view;
    };
    let Some(created) = driver.metadata.creation_timestamp.clone() else {
        return view;
    };
    let window = run_window(&driver, SystemTime::from(created.0), view.elapsed);
    view.grafana = grafana_explore_url(ns, window);
    if view.elapsed.is_zero() {
        view.elapsed = window.1.duration_since(window.0).unwrap_or_default();
    }

    // Analysis grid, not the terminal — the renderer decimates downstream, where a
    // min/max envelope keeps the peaks a viewport-sized query would have smoothed away
    let rows: Vec<_> = ztest::backends::metrics_components().copied().collect();

    match ztest::api::metrics::history(client, ns, &rows, window).await {
        // Empty from a run shorter than a few scrapes is arithmetic, not a broken obs
        // stack — pointing those at `cluster setup` sends the reader to fix what works
        None => {
            let too_short = ztest::api::metrics::SCRAPE_INTERVAL * 3;
            view.note = Some(if view.elapsed < too_short {
                format!(
                    "no metrics recorded: the run lasted {}, under the {} Prometheus \
                     needs to sample it",
                    format_elapsed(view.elapsed),
                    format_span(too_short),
                )
            } else {
                format!(
                    "no metrics recorded for this run (needs `ztest cluster setup`, and \
                     Prometheus keeps {} days)",
                    ztest::api::resource::RETENTION_DAYS,
                )
            })
        }
        Some(series) => {
            let of = |facet: Facet| -> Vec<_> {
                series.iter().filter(|s| s.facet == Some(facet)).cloned().collect()
            };
            view.transparent = of(Facet::Transparent);
            view.shielded = of(Facet::Shielded);
            view.blocks = of(Facet::Blocks);
            view.throughput = of(Facet::Throughput);
            view.write_path = of(Facet::WritePath);
            view.store = of(Facet::Store);

            let progress = |label: &str, pick: fn(&ztest::api::metrics::Series) -> Option<f64>| {
                series
                    .iter()
                    .find(|s| s.facet == Some(Facet::Progress) && s.label == label)
                    .and_then(pick)
                    .map(|v| v as u32)
            };
            // Record owns the verdict's numbers; the TSDB only confirms them, and a
            // disagreement is shown rather than resolved
            let observed = progress("finalized", ztest::api::metrics::Series::last);
            let pace = view.blocks.first().and_then(ztest::api::metrics::Series::mean);
            let tolerance = stale_scrape_slack(view.elapsed, pace);
            view.height_check = view.height.zip(observed).and_then(|((recorded, _), observed)| {
                (recorded.abs_diff(observed) > tolerance).then_some((recorded, observed))
            });
            // Record never landed → still a height, marked provisional by the absent check
            if view.height.is_none() {
                let objective = progress("target", ztest::api::metrics::Series::last)
                    .or_else(|| progress("chain tip", ztest::api::metrics::Series::last));
                view.height = observed.zip(objective);
            }
            view.tip = progress("chain tip", ztest::api::metrics::Series::last).or(view.tip);
        }
    }

    if let Some(history) = ztest::api::metrics::container_history(client, ns, window).await {
        view.resources = by_component(history);
    }
    view
}

/// Container-labelled readings → one [`ComponentResources`] per component.
///
/// Bundled backends lead, in the order they are declared (the subject ahead of what it
/// proxies); anything else the namespace ran follows, named but unranked
fn by_component(history: ztest::api::metrics::ContainerHistory) -> Vec<ComponentResources> {
    let ztest::api::metrics::ContainerHistory { cpu, mem, disk_read, disk_write, io_stall } =
        history;
    let observed = || cpu.iter().chain(&mem).chain(&disk_read).chain(&disk_write).chain(&io_stall);

    let mut names: Vec<String> = ztest::backends::metrics_component_labels()
        .map(str::to_string)
        .filter(|n| observed().any(|s| &s.label == n))
        .collect();
    let mut rest: Vec<String> =
        observed().map(|s| s.label.clone()).filter(|l| !names.contains(l)).collect();
    rest.sort();
    rest.dedup();
    names.append(&mut rest);

    let pick = |series: &[Series], name: &str| -> Vec<Series> {
        series.iter().filter(|s| s.label == name).cloned().collect()
    };
    // Read + write are disjoint halves of one device total, so they stack. Relabelled to
    // the operation: the panel title already names the container, and a legend repeating
    // it twice says nothing
    let disk = |name: &str| -> Vec<Series> {
        [(&disk_read, "read"), (&disk_write, "write")]
            .into_iter()
            .flat_map(|(series, op)| {
                pick(series, name).into_iter().map(|s| Series { label: op.to_string(), ..s })
            })
            .collect()
    };
    names
        .into_iter()
        .map(|component| ComponentResources {
            cpu: pick(&cpu, &component),
            mem: pick(&mem, &component),
            disk: disk(&component),
            io_stall: pick(&io_stall, &component),
            component,
        })
        .collect()
}

/// Grafana Explore deep link scoped to this run's namespace + window.
///
/// - String construction, not a client (cannot fail, slow the report, or need a credential)
/// - Names the required port-forward (a URL to an unreachable Service is worse than none)
fn grafana_explore_url(ns: &str, window: (SystemTime, SystemTime)) -> Option<String> {
    let millis =
        |t: SystemTime| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_millis());
    let (from, to) = (millis(window.0)?, millis(window.1)?);
    Some(format!(
        "grafana: kubectl -n {} port-forward svc/{} {}:{} \
         then http://localhost:{}/explore?left=%7B%22range%22:%7B%22from%22:%22{from}%22,%22to%22:%22{to}%22%7D,\
         %22datasource%22:%22Prometheus%22,%22queries%22:%5B%7B%22expr%22:%22%7Bnamespace%3D%5C%22{ns}%5C%22%7D%22%7D%5D%7D",
        ztest::api::naming::OBS_NAMESPACE,
        ztest::api::naming::GRAFANA_SERVICE,
        ztest::api::ports::GRAFANA_PORT,
        ztest::api::ports::GRAFANA_PORT,
        ztest::api::ports::GRAFANA_PORT,
    ))
}

/// Prometheus range covering the *run*, not the pod that hosted it.
///
/// - Ends at the driver's exit, never `now` (a detached sync's pod idles for hours
///   after its verdict; those flat scrapes dilute every average and squeeze the plotted
///   run into the left edge)
/// - Opens `elapsed` back from that end — `Segment::elapsed_ms` already excludes
///   provisioning, which is where a cold seed spends an hour publishing nothing
/// - Never before the pod existed, and falls back to its full span when either bound
///   is unknown
fn run_window(
    driver: &k8s_openapi::api::core::v1::Pod,
    created: SystemTime,
    elapsed: Duration,
) -> (SystemTime, SystemTime) {
    let ended = driver_finished_at(driver).unwrap_or_else(SystemTime::now);
    let started = match elapsed.is_zero() {
        true => created,
        false => ended.checked_sub(elapsed).unwrap_or(created).max(created),
    };
    (started, ended)
}

fn driver_finished_at(driver: &k8s_openapi::api::core::v1::Pod) -> Option<SystemTime> {
    let finished = driver
        .status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .first()?
        .state
        .as_ref()?
        .terminated
        .as_ref()?
        .finished_at
        .clone()?;
    Some(SystemTime::from(finished.0))
}

// ─────────────────────────── stop / rm / watch ────────────────────────

async fn stop(id: &str) -> Result<()> {
    let client = client().await?;
    ztest::api::profiling::reap_finished(&client).await;
    let _ = find_driver(&client, id).await?; // 404s clearly if unknown
    let api: Api<Pod> = Api::namespaced(client, RUN_NAMESPACE);
    let patch = json!({ "metadata": { "annotations": { STOP_ANNOTATION: "true" } } });
    api.patch(&driver_pod_for(id), &PatchParams::apply("ztest-sync"), &Patch::Merge(&patch))
        .await
        .context("signal stop")?;
    println!("sync {id}: stop signalled (graceful checkpoint, report, then exit)");
    Ok(())
}

// `stop` does NOT release the reservation:
// - Signals only; driver then checkpoints + tears down over `STOP_GRACE_SECS`
// - Pods still hold nodes throughout → deleting the lease would advertise held
//   capacity as free (the overcommit this ledger prevents)
// - Release rides on the driver ceasing to exist (TTL), correct on clean exit,
//   eviction and kill alike

/// Profile a driver pod was launched for, read back from its `ZTEST_SYNC_PROFILE` env
/// (the panel's label; falls back to the sync id)
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

// ─────────────────────────────── describe ─────────────────────────────

async fn describe(name: &str) -> Result<()> {
    // Cluster-free: local inventory dump → each profile's static `SyncTestDecl`, printed
    // without executing the body.
    // No `--features` (virtual-workspace root rejects a bare one); default selection
    // includes the sync suite.
    // Same `-p/--test` narrowing `start` compiles with — a describe that models a wider
    // selection than the run it describes reports a `pruned` set the run never had
    let list_args = preflight(name)?.map(|s| s.cargo_args()).unwrap_or_default();
    let build =
        ztest::api::pipeline::index(&list_args).await.context("local build/list for describe")?;
    let BuildOutcome::Ok { selected_binaries, .. } = &build else {
        return Err(anyhow!("local build produced no test selection"));
    };
    let (dump, _) = ztest::api::pipeline::discover(selected_binaries).await;
    let DumpOutcome::Discovered { sync_tests, images_by_binary, deps_by_binary, seeds, .. } = &dump
    else {
        return Err(anyhow!("local inventory dump failed"));
    };
    let entry = sync_tests.iter().find(|s| s.name == name).ok_or_else(|| {
        let have: Vec<&str> = sync_tests.iter().map(|s| s.name.as_str()).collect();
        anyhow!("no sync profile `{name}`; have: {}", have.join(", "))
    })?;
    let plan = ztest::api::plan::for_sync(
        selected_binaries,
        entry,
        images_by_binary,
        deps_by_binary,
        seeds,
    );
    print!("{}", ztest_ui::render_plan(&plan, &Theme::detect()));
    Ok(())
}

/// `--profile-off-cpu` is a probability; anything outside `0..=1` is rejected by Alloy at
/// load, which surfaces three layers away as "the run produced no profile"
fn off_cpu_fraction(raw: &str) -> Result<f64> {
    let p: f64 = raw.parse().with_context(|| format!("`{raw}`: not a number"))?;
    match (0.0..=1.0).contains(&p) {
        true => Ok(p),
        false => Err(anyhow!("{p}: outside 0..=1")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    /// Heading sits over the column it names, whatever width the rows measured to
    #[test]
    fn the_listing_columns_line_up_under_their_headings() {
        let theme = plain_theme();
        let head = Fields::new()
            .text("id", pad("SYNC-ID", 12))
            .text("ns", pad("NAMESPACE", 12))
            .text("status", pad("STATUS", 8))
            .text("user", "USER");
        let head = draw(row::LIST_HEADER, &head, &theme);
        let listed = Fields::new()
            .text("mark", "*")
            .text("id", pad("sync-aaa", 12))
            .text("ns", pad("ztest-sync-a", 12))
            .text("status", pad("passed", 8))
            .text("user", "eli");
        let listed = draw(&row::list("pass"), &listed, &theme);
        assert_eq!(listed, "* sync-aaa     ztest-sync-a passed   eli");
        assert_eq!(head.find("NAMESPACE"), listed.find("ztest-sync-a"));
        assert_eq!(head.find("STATUS"), listed.find("passed"));
        assert_eq!(head.find("USER"), listed.find("eli"));
    }

    /// A mistyped key binds nothing and renders blank, so the handoff is bound once here.
    ///
    /// Regression: the mark was bound as data with a literal `*` — the ascii spelling —
    /// so the Unicode handoff drew an asterisk where every other surface drew a tick
    #[test]
    fn the_handoff_names_the_sync_and_its_follow_ups() {
        let theme = plain_theme();
        let started = Fields::new().text("id", "sync-aaa").text("ns", "ztest-sync-aaa");
        assert_eq!(
            draw(row::HANDOFF, &started, &theme),
            "✓ sync sync-aaa started · namespace ztest-sync-aaa"
        );
        let cmd = Fields::new().text("verb", "stop").text("id", "sync-aaa");
        assert_eq!(draw(row::HANDOFF_CMD, &cmd, &theme), "  · ztest sync stop   sync-aaa");
    }

    fn labelled_cm(id: &str, user: Option<&str>) -> ConfigMap {
        let mut labels = BTreeMap::from([(SYNC_ID_KEY.to_string(), id.to_string())]);
        if let Some(u) = user {
            labels.insert(ztest::qos::LABEL_USER.to_string(), u.to_string());
        }
        ConfigMap {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                labels: Some(labels),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn labelled_pod(id: &str, user: &str) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": driver_pod_for(id), "labels": {
                SYNC_ID_KEY: id,
                ztest::qos::LABEL_USER: user,
            }},
        }))
        .expect("static Pod manifest is valid")
    }

    /// Record outlives the pod, pod pre-dates the record — listing either alone loses syncs
    #[test]
    fn a_listing_unions_records_and_driver_pods() {
        let records = vec![labelled_cm("reaped-pod", Some("elicb"))];
        let pods = HashMap::from([("no-record".to_string(), labelled_pod("no-record", "elicb"))]);
        let rows = merge_rows(&records, &pods);
        let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["no-record", "reaped-pod"]);
        assert!(rows.iter().all(|r| r.user == "elicb"), "owner from whichever half has it");
    }

    /// Records written before owners were stamped on them must still list for their owner
    #[test]
    fn an_unlabelled_record_takes_its_owner_from_the_driver_pod() {
        let records = vec![labelled_cm("legacy", None)];
        let pods = HashMap::from([("legacy".to_string(), labelled_pod("legacy", "elicb"))]);
        assert_eq!(merge_rows(&records, &pods)[0].user, "elicb");
    }

    /// Neither half labelled → listed, never silently dropped from its owner's view
    #[test]
    fn an_ownerless_sync_still_lists() {
        let rows = merge_rows(&[labelled_cm("orphan", None)], &HashMap::new());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].user, "-");
        assert_eq!(rows[0].namespace, namespace_for("orphan"));
    }
}
