//! `ztest sync watch` — the live view of a detached sync.
//!
//! - Vitals = SUT + driver `/metrics` scraped direct every [`LIVE_INTERVAL`] (`status` = the TSDB)
//! - Loads = cAdvisor off the TSDB (no component sees its own cgroup)
//! - Scrollback = driver-pod log + indexer-under-test log, verbatim
//! - All over the kube API (no `kubectl` on the laptop)
//! - Read-only: Ctrl-C detaches, never stops the sync (only `ztest sync stop` does)

use std::collections::HashMap;
use std::future::Future;
use std::io::{IsTerminal, stdout};
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use futures::{AsyncBufReadExt as _, StreamExt as _};
use k8s_openapi::api::core::v1::{ContainerState, Pod};
use kube::api::{Api, ListParams, LogParams};
use tokio::sync::watch;

use ztest::api::Heights;
use ztest::api::Resources;
use ztest::api::metrics::{Exposition, LIVE_INTERVAL, Live, PORT_NAME, SCRAPE_INTERVAL, Series};
use ztest::api::naming::RUN_NAMESPACE;
use ztest::api::portforward::Forwarder;
use ztest::api::ports::SYNC_DRIVER_METRICS;
use ztest::sync::{SyncStatus, driver_family, driver_pod_for, find_driver, namespace_for};
use ztest_ui::console::{Console, SceneFrame};
use ztest_ui::template::{Fields, draw};
use ztest_ui::{
    ComponentResources, ContainerLoad, ReportView, SyncVitals, SyncWatchState, Theme,
    render_sync_load, render_sync_watch_panel, render_sync_work,
};

use super::{DRIVER_CONTAINER, by_component, driver_profile, place_by_facet, render, report_view};

/// Driver-pod address: run-namespace API handle + pod name.
///
/// - Driver = a *runner* pod in [`RUN_NAMESPACE`], not the sync namespace it deploys into (a
///   sync-scoped `Api<Pod>` would silently read the wrong one)
struct DriverPod {
    api: Api<Pod>,
    name: String,
}

impl DriverPod {
    fn new(client: &kube::Client, sync_id: &str) -> Self {
        DriverPod {
            api: Api::namespaced(client.clone(), RUN_NAMESPACE),
            name: driver_pod_for(sync_id),
        }
    }

    async fn get(&self) -> Result<Option<Pod>, kube::Error> {
        self.api.get_opt(&self.name).await
    }
}

/// Driver-pod phase re-read interval; slow, since it only matters before the first commit
const POD_POLL: Duration = Duration::from_secs(2);

/// Per-stream backfill: deep enough to explain the attach, shallow enough not to flood
const TAIL_LINES: i64 = 200;

/// Component whose **log** rides alongside the driver's, by category (`lightwalletd` followed
/// like `zainod`)
const SUT_SELECTOR: &str = "ztest.io/component-category=indexer";

/// Backend a pod runs → the metric layout its `/metrics` is read with
const COMPONENT_LABEL: &str = "ztest.io/component";

type LineStream = std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<String>> + Send>>;

/// How an attach ended: the sync's own standing, or the user leaving it running. `watch`
/// reports; the *caller* maps it to an exit status (`ztest sync watch` always succeeds;
/// `ztest sync start --watch` stands in for a foreground run and must fail its pipeline on
/// a failing verdict)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WatchEnd {
    Detached,
    Settled(SyncStatus),
}

pub(super) async fn watch(id: &str) -> Result<WatchEnd> {
    let client = super::client().await?;
    let pod = find_driver(&client, id).await?;
    let profile =
        driver_profile(&pod).with_context(|| format!("sync {id}: driver pod names no profile"))?;
    let driver = DriverPod::new(&client, id);
    let sut: Api<Pod> = Api::namespaced(client.clone(), &namespace_for(id));
    let theme = Theme::detect();

    // Non-TTY: no panel to pin → the driver log alone, then the verdict
    if !stdout().is_terminal() {
        return linear(&driver, &client, id, &theme).await;
    }

    let cancel_theme = theme.clone();
    let cancel_panel =
        Box::new(move |elapsed| ztest_ui::render_cancel_panel(elapsed, &cancel_theme));
    // Shared with the fold so a read's `received_at` and each frame's `elapsed` share an origin
    let session_start = Instant::now();
    let (console, guard) = match Console::start(session_start, cancel_panel) {
        Ok(cg) => cg,
        Err(_) => return linear(&driver, &client, id, &theme).await,
    };

    let mut state = SyncWatchState {
        profile,
        sync_id: id.to_string(),
        context: ztest::api::cluster_config::active_context().unwrap_or_else(|| "(cluster)".into()),
        pod_phase: driver_phase(&pod),
        ..SyncWatchState::default()
    };
    // Position + pace / per-pool rates / per-container draw, all off one read
    let render = |state: &SyncWatchState| {
        let (state, theme) = (state.clone(), theme.clone());
        console.scene(move |elapsed| SceneFrame {
            left: render_sync_watch_panel(&state, elapsed, &theme),
            mid: Some(render_sync_work(&state, elapsed, &theme)),
            right: render_sync_load(&state, &theme),
            live: None,
        });
    };
    render(&state);

    let mut live = Poller::spawn(LIVE_INTERVAL, LiveSampler::new(client.clone(), id));
    let mut loads =
        Poller::spawn(SCRAPE_INTERVAL, LoadSampler { client: client.clone(), id: id.into() });
    let tail = tail_loop(
        (&driver, &sut),
        (&mut live, &mut loads),
        &console,
        session_start,
        &mut state,
        &theme,
        render,
    )
    .await;

    let detached = console.cancelled();
    guard.finish();
    tail?;

    if detached {
        println!("sync {id}: detached — still running (`ztest sync status {id}`)");
        return Ok(WatchEnd::Detached);
    }
    settled(&client, id, &theme).await
}

/// Panel-less attach: linear driver-log tail, then the settled verdict. Off a TTY, or when the
/// console cannot start
async fn linear(
    driver: &DriverPod,
    client: &kube::Client,
    id: &str,
    theme: &Theme,
) -> Result<WatchEnd> {
    plain_tail(driver).await?;
    settled(client, id, theme).await
}

/// Post-log verdict off the read `status` draws. No mirror = no verdict to print
async fn settled(client: &kube::Client, id: &str, theme: &Theme) -> Result<WatchEnd> {
    let (view, status) = report_view(client, id).await?;
    if !matches!(status, SyncStatus::Finished(_)) {
        println!("sync {id}: tail ended — no report yet (`ztest sync status {id}`)");
        return Ok(WatchEnd::Settled(SyncStatus::Unresolved));
    }
    print!("{}", ztest_ui::render_sync_verdict(&view, theme, render::width()));
    Ok(WatchEnd::Settled(status))
}

// ────────────────────────────── panel reads ──────────────────────────────

/// Vitals: SUT rows placed as `status` places them, heights + segment span off the same scrape
type LiveRead = Result<ReportView, String>;

/// Loads: per-component usage + each container's declared limit
type LoadRead = Result<(Vec<ComponentResources>, HashMap<String, Resources>), String>;

/// Under [`LIVE_INTERVAL`] → a slow target skips a beat, never queues
const LIVE_SCRAPE_TIMEOUT: Duration = Duration::from_millis(800);

/// Trailing load window (≥ the TSDB grid's rate window, else no point lands)
const LOAD_WINDOW: Duration = Duration::from_secs(120);

/// One panel source, read repeatedly on its own task
trait Sample: Send + 'static {
    type Out: Clone + Send + Sync + 'static;
    fn sample(&mut self) -> Pin<Box<dyn Future<Output = Self::Out> + Send + '_>>;
}

/// One read per interval on its own task (a slow read must not stall the log tail). Drop = stop
/// (a departed watcher must stop reading)
struct Poller<T> {
    rx: watch::Receiver<Option<T>>,
    task: tokio::task::JoinHandle<()>,
}

impl<T: Clone + Send + Sync + 'static> Poller<T> {
    fn spawn<S: Sample<Out = T>>(every: Duration, mut source: S) -> Poller<T> {
        let (tx, rx) = watch::channel(None);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if tx.send(Some(source.sample().await)).is_err() {
                    return;
                }
            }
        });
        Poller { rx, task }
    }

    /// Newest read, never a backlog. Cancel-safe for a `select!` arm; pends forever once the task
    /// is gone (an instantly-resolving arm spins its loop)
    async fn changed(&mut self) -> T {
        loop {
            if self.rx.changed().await.is_err() {
                return std::future::pending().await;
            }
            if let Some(read) = self.rx.borrow_and_update().clone() {
                return read;
            }
        }
    }
}

impl<T> Drop for Poller<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// SUT + driver `/metrics` over port-forwards
///
/// - Tunnel dropped when its own read fails → re-resolved next tick (pods get replaced mid-run)
struct LiveSampler {
    client: kube::Client,
    id: String,
    http: reqwest::Client,
    sut: Option<SutTap>,
    driver: Option<Forwarder>,
}

/// Tunnel to the SUT + its rolling rows. Replaced with the pod (a new pod = new counters)
struct SutTap {
    forwarder: Forwarder,
    live: Live,
    heights: Heights,
}

impl LiveSampler {
    fn new(client: kube::Client, id: &str) -> LiveSampler {
        LiveSampler { client, id: id.into(), http: reqwest::Client::new(), sut: None, driver: None }
    }

    /// Slot taken per read, restored only on success (a dropped tunnel = re-resolve)
    async fn read(&mut self) -> LiveRead {
        let driver = match self.driver.take() {
            Some(driver) => driver,
            None => {
                let pod = driver_pod_for(&self.id);
                forward(&self.client, RUN_NAMESPACE, &pod, SYNC_DRIVER_METRICS).await?
            }
        };
        let origin = scrape_local(&self.http, &driver)
            .await
            .map_err(|e| format!("driver /metrics: {e}"))?
            .level(driver_family::STARTED);
        self.driver = Some(driver);

        let mut sut = match self.sut.take() {
            Some(sut) => sut,
            None => open_sut(&self.client, &self.id).await?,
        };
        let exposition = scrape_local(&self.http, &sut.forwarder)
            .await
            .map_err(|e| format!("indexer /metrics: {e}"))?;
        let sampled = SystemTime::now();
        sut.live.push(Instant::now(), sampled, exposition);
        let view = live_view(&sut, origin, sampled);
        self.sut = Some(sut);
        Ok(view)
    }
}

impl Sample for LiveSampler {
    type Out = LiveRead;
    fn sample(&mut self) -> Pin<Box<dyn Future<Output = LiveRead> + Send + '_>> {
        Box::pin(self.read())
    }
}

/// `origin` = the driver's segment start (unix secs), the one uptime `status` also spans from
fn live_view(sut: &SutTap, origin: Option<f64>, sampled: SystemTime) -> ReportView {
    let mut view = ReportView::default();
    place_by_facet(&mut view, &sut.live.series());
    let height = |gauge| sut.live.latest().and_then(|e| e.height(gauge));
    let target = sut.heights.target.and_then(height).filter(|&t| t > 0);
    view.height = height(sut.heights.height).zip(target);
    view.span = origin.map(|o| (UNIX_EPOCH + Duration::from_secs_f64(o), sampled));
    view
}

async fn open_sut(client: &kube::Client, id: &str) -> Result<SutTap, String> {
    let ns = namespace_for(id);
    let pod =
        sut_pod(&Api::namespaced(client.clone(), &ns)).await.ok_or("no running indexer yet")?;
    let name = pod.metadata.name.clone().unwrap_or_default();
    let backend = pod
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(COMPONENT_LABEL))
        .ok_or_else(|| format!("{name} carries no {COMPONENT_LABEL} label"))?;
    let (rows, heights) = ztest::backends::observed_backend(backend)
        .ok_or_else(|| format!("{backend} declares no sync metrics layout"))?;
    let port =
        metrics_port(&pod).ok_or_else(|| format!("{name} declares no `{PORT_NAME}` port"))?;
    let forwarder = forward(client, &ns, &name, port).await?;
    Ok(SutTap { forwarder, live: Live::new(rows), heights })
}

fn metrics_port(pod: &Pod) -> Option<u16> {
    pod.spec
        .as_ref()?
        .containers
        .iter()
        .flat_map(|c| c.ports.iter().flatten())
        .find(|p| p.name.as_deref() == Some(PORT_NAME))
        .and_then(|p| u16::try_from(p.container_port).ok())
}

async fn forward(
    client: &kube::Client,
    ns: &str,
    pod: &str,
    port: u16,
) -> Result<Forwarder, String> {
    Forwarder::start(client.clone(), ns.to_string(), pod.to_string(), port)
        .await
        .map_err(|e| format!("port-forward {pod}:{port}: {e}"))
}

async fn scrape_local(http: &reqwest::Client, forwarder: &Forwarder) -> Result<Exposition, String> {
    let base = format!("http://127.0.0.1:{}", forwarder.local_port);
    ztest::api::metrics::scrape(http, &base, LIVE_SCRAPE_TIMEOUT).await.map_err(|e| e.to_string())
}

/// Container usage off the TSDB's trailing [`LOAD_WINDOW`] + declared limits off the pod specs
struct LoadSampler {
    client: kube::Client,
    id: String,
}

impl LoadSampler {
    async fn read(&self) -> LoadRead {
        let ns = namespace_for(&self.id);
        let now = SystemTime::now();
        let window = (now.checked_sub(LOAD_WINDOW).unwrap_or(UNIX_EPOCH), now);
        let history = ztest::api::metrics::container_history(&self.client, &ns, window)
            .await
            .map_err(|e| format!("prometheus unreadable · {e}"))?;
        let pods = Api::<Pod>::namespaced(self.client.clone(), &ns)
            .list(&ListParams::default())
            .await
            .map_err(|e| format!("read the sync's pods: {e}"))?;
        Ok((by_component(history), declared_limits(&pods.items)))
    }
}

impl Sample for LoadSampler {
    type Out = LoadRead;
    fn sample(&mut self) -> Pin<Box<dyn Future<Output = LoadRead> + Send + '_>> {
        Box::pin(self.read())
    }
}

/// Declared limit per container; none declared = no entry (Burstable → bare usage)
fn declared_limits(pods: &[Pod]) -> HashMap<String, Resources> {
    pods.iter()
        .filter_map(|p| p.spec.as_ref())
        .flat_map(|spec| &spec.containers)
        .map(|c| (c.name.clone(), ztest::qos::units::container_limits(c)))
        .filter(|(_, limit)| *limit != Resources::ZERO)
        .collect()
}

/// One live read → the vitals. Failed read keeps the last vitals, which then age out as stale
fn fold_live(state: &mut SyncWatchState, read: LiveRead, at: Duration) {
    match read {
        Ok(view) => {
            if let Some(vitals) = SyncVitals::of(&view, at) {
                state.vitals = Some(vitals);
            }
            state.metrics_note = state.vitals.is_none().then(|| "no height scraped yet".into());
        }
        Err(why) => state.metrics_note = Some(why),
    }
}

fn fold_loads(state: &mut SyncWatchState, read: LoadRead) {
    match read {
        Ok((resources, limits)) => {
            state.loads = loads(&resources, &limits);
            state.loads_note = None;
        }
        Err(why) => state.loads_note = Some(why),
    }
}

/// Newest cpu + memory per container, beside its declared limit
fn loads(
    resources: &[ComponentResources],
    limits: &HashMap<String, Resources>,
) -> Vec<ContainerLoad> {
    let newest = |series: &[Series]| series.iter().filter_map(Series::last).sum::<f64>();
    resources
        .iter()
        .filter(|r| !r.cpu.is_empty() || !r.mem.is_empty())
        .map(|r| ContainerLoad {
            container: r.component.clone(),
            usage: Resources::new(
                (newest(&r.cpu) * 1000.0).round() as u64,
                newest(&r.mem).round() as u64,
                0,
                0,
            ),
            limit: limits.get(&r.component).copied(),
        })
        .collect()
}

// ────────────────────────────── log tail ──────────────────────────────

/// Merge driver log + SUT log + live/load reads + pod-phase poll until the driver's stream ends or
/// the user detaches
async fn tail_loop(
    (driver, api): (&DriverPod, &Api<Pod>),
    (live, loads): (&mut Poller<LiveRead>, &mut Poller<LoadRead>),
    console: &Console,
    session_start: Instant,
    state: &mut SyncWatchState,
    theme: &Theme,
    render: impl Fn(&SyncWatchState),
) -> Result<()> {
    // Last cause shown → a standing condition is scrolled once, not once per read
    let mut last_note: Option<String> = None;

    let started = open_driver_log(driver, &|| console.cancelled(), |phase| {
        state.pod_phase = phase;
        render(state);
    })
    .await?;
    let Some(mut driver_log) = started else {
        return Ok(());
    };
    // SUT absent on an early attach (the driver provisions it) → opened lazily by the poll.
    // Name held apart from the stream so a `select!` arm replacing the stream can still prefix
    let mut sut: Option<LineStream> = None;
    let mut sut_name = String::new();
    // Last pod followed → a reopened stream tells a resumed follow from a replacement
    let mut sut_prev: Option<String> = None;
    let (mut driver_seen, mut sut_seen) = (Instant::now(), Instant::now());
    let mut ticker = tokio::time::interval(POD_POLL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if console.cancelled() {
            break;
        }
        // Applied after the `select!`: no handler may mutate what another arm borrows
        let (mut close_sut, mut lost_driver) = (false, false);
        let (mut next_live, mut next_loads): (Option<LiveRead>, Option<LoadRead>) = (None, None);
        tokio::select! {
            line = driver_log.next() => match line {
                Some(Ok(l)) => {
                    driver_seen = Instant::now();
                    console.scrollback(prefixed("driver", &l, theme));
                }
                Some(Err(_)) | None => lost_driver = true,
            },
            line = next_line(&mut sut) => match line {
                Some(Ok(l)) => {
                    sut_seen = Instant::now();
                    console.scrollback(prefixed(&sut_name, &l, theme));
                }
                // SUT stream ending != run ending (restarted/replaced pod): drop, let the poll reopen
                Some(Err(_)) | None => close_sut = true,
            },
            read = live.changed() => next_live = Some(read),
            read = loads.changed() => next_loads = Some(read),
            _ = ticker.tick() => {
                if let Ok(Some(pod)) = driver.get().await {
                    let phase = driver_phase(&pod);
                    if phase != state.pod_phase {
                        state.pod_phase = phase;
                        render(state);
                    }
                }
                if sut.is_none()
                    && let Some(name) = find_sut(api).await
                {
                    // Same pod → resumed follow, replay the gap only; a new pod gets the context tail
                    let backfill = match sut_prev.as_deref() == Some(name.as_str()) {
                        true => Backfill::Seconds(gap_since(sut_seen)),
                        false => Backfill::Lines(TAIL_LINES),
                    };
                    if let Ok(stream) = open_log(api, &name, None, backfill).await {
                        console.scrollback(prefixed("ztest", &format!("following {name}"), theme));
                        sut_prev = Some(name.clone());
                        sut_name = name;
                        sut = Some(stream);
                        sut_seen = Instant::now();
                    }
                }
            }
        }
        if let Some(read) = next_loads {
            fold_loads(state, read);
            render(state);
        }
        if let Some(read) = next_live {
            fold_live(state, read, session_start.elapsed());
            // Panel names the cause; scrollback echoes it once, so a cleared condition leaves a trace
            let note = state.metrics_note.clone();
            if note != last_note
                && let Some(note) = &note
            {
                console.scrollback(prefixed("ztest", &format!("metrics: {note}"), theme));
            }
            last_note = note;
            render(state);
        }
        if close_sut {
            sut = None;
        }
        if lost_driver {
            match reattach_driver(driver, driver_seen).await? {
                Some(stream) => {
                    driver_log = stream;
                    console.scrollback(prefixed("ztest", "reattached to the driver log", theme));
                }
                None => break,
            }
        }
    }
    Ok(())
}

/// Reopen the driver log when its stream ended but the run did not.
///
/// - Hours-long follow-streams are routinely cut (idle API-server timeout, proxy hop); reading
///   that as the run's end strands a live sync
/// - `None` = the driver really finished
async fn reattach_driver(driver: &DriverPod, last_seen: Instant) -> Result<Option<LineStream>> {
    // Backs off a stream that fails immediately; lets a driver mid-exit finish writing
    tokio::time::sleep(POD_POLL).await;
    let Some(pod) = driver.get().await.context("read driver pod")? else {
        return Ok(None);
    };
    if !running(&pod, DRIVER_CONTAINER) {
        return Ok(None);
    }
    open_log(
        &driver.api,
        &driver.name,
        Some(DRIVER_CONTAINER),
        Backfill::Seconds(gap_since(last_seen)),
    )
    .await
    .map(Some)
    .context("reattach to sync log")
}

/// Replay window leaving no gap: since the last line read, rounded up (API resolution = 1s)
fn gap_since(last_seen: Instant) -> i64 {
    last_seen.elapsed().as_secs().saturating_add(1) as i64
}

/// Await the driver container's start, then follow its log.
///
/// - Log request before start = a 400, and `--watch` attaches at pod creation
/// - Gating on container state makes the wait report itself (`ImagePullBackOff` reaches the panel
///   instead of reading as a hang)
/// - `Ok(None)` = caller asked to stop waiting
async fn open_driver_log(
    driver: &DriverPod,
    stop: &dyn Fn() -> bool,
    mut observe: impl FnMut(String),
) -> Result<Option<LineStream>> {
    loop {
        if stop() {
            return Ok(None);
        }
        let pod = driver
            .get()
            .await
            .context("read driver pod")?
            .with_context(|| format!("driver pod {} no longer exists", driver.name))?;
        observe(driver_phase(&pod));
        if logs_available(&pod, DRIVER_CONTAINER) {
            return open_log(
                &driver.api,
                &driver.name,
                Some(DRIVER_CONTAINER),
                Backfill::Lines(TAIL_LINES),
            )
            .await
            .map(Some)
            .context("stream sync log");
        }
        tokio::time::sleep(POD_POLL).await;
    }
}

/// Log readable? The kubelet keeps one from container start, and after exit
fn logs_available(pod: &Pod, container: &str) -> bool {
    container_state(pod, container).is_some_and(|s| s.running.is_some() || s.terminated.is_some())
}

/// Still executing? Separates a dropped connection from a finished run
fn running(pod: &Pod, container: &str) -> bool {
    container_state(pod, container).is_some_and(|s| s.running.is_some())
}

fn container_state(pod: &Pod, container: &str) -> Option<ContainerState> {
    pod.status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|c| c.name == container)?
        .state
        .clone()
}

/// Phase + the container's waiting reason (bare `Pending` cannot separate scheduling from
/// `ImagePullBackOff`)
fn driver_phase(pod: &Pod) -> String {
    let phase = super::pod_phase(pod).unwrap_or_else(|| "Unknown".into());
    let reason = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.name == DRIVER_CONTAINER))
        .and_then(|c| c.state.as_ref()?.waiting.as_ref()?.reason.clone());
    match reason {
        Some(reason) => format!("{phase} · {reason}"),
        None => phase,
    }
}

/// History a freshly-opened log stream replays. `Seconds` = a resumed attach (overlap over a gap;
/// a repeated line is only cosmetic)
#[derive(Debug, Clone, Copy)]
enum Backfill {
    Lines(i64),
    Seconds(i64),
}

/// `container`: `None` for a single-container pod, `Some` where the pod has more than one
/// (the driver carries a profiler sidecar — unnamed, the apiserver answers 400)
async fn open_log(
    api: &Api<Pod>,
    pod: &str,
    container: Option<&str>,
    backfill: Backfill,
) -> Result<LineStream, kube::Error> {
    let mut lp =
        LogParams { follow: true, container: container.map(str::to_string), ..Default::default() };
    match backfill {
        Backfill::Lines(n) => lp.tail_lines = Some(n),
        Backfill::Seconds(s) => lp.since_seconds = Some(s),
    }
    Ok(Box::pin(api.log_stream(pod, &lp).await?.lines()))
}

/// Running indexer pod; `None` until the driver provisions one (caller retries)
async fn sut_pod(api: &Api<Pod>) -> Option<Pod> {
    let pods = api.list(&ListParams::default().labels(SUT_SELECTOR)).await.ok()?;
    pods.items
        .into_iter()
        .find(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"))
}

async fn find_sut(api: &Api<Pod>) -> Option<String> {
    sut_pod(api).await?.metadata.name
}

/// Next line of an optional stream; `None` parks forever, so `select!` ignores it until one opens
async fn next_line(stream: &mut Option<LineStream>) -> Option<std::io::Result<String>> {
    match stream {
        Some(s) => s.next().await,
        None => std::future::pending().await,
    }
}

const SOURCE_ROW: &str = "{source:>8|dim} {stem|dim} {line}";

/// Tag a line with the pod it came from, so a merged stream stays attributable
fn prefixed(source: &str, line: &str, theme: &Theme) -> String {
    let f = Fields::new().text("source", source).text("stem", theme.chars.vbar).text("line", line);
    format!("{}\n", draw(SOURCE_ROW, &f, theme))
}

/// Linear follow-tail of the driver log (non-TTY, or the console won't start). Reattaches like the
/// panel path (a piped `--watch` in CI follows the same hours-long log)
async fn plain_tail(driver: &DriverPod) -> Result<()> {
    let mut lines = open_driver_log(driver, &|| false, |phase| {
        println!("ztest sync: driver {phase}");
    })
    .await?
    .expect("only a cancelling caller gets None");
    let mut seen = Instant::now();
    loop {
        while let Some(line) = lines.next().await {
            println!("{}", line.context("read sync log")?);
            seen = Instant::now();
        }
        match reattach_driver(driver, seen).await? {
            Some(stream) => {
                lines = stream;
                println!("ztest sync: reattached to the driver log");
            }
            None => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;
    use ztest::api::GIB;

    fn read(sampled_secs: u64, height: Option<u32>) -> LiveRead {
        Ok(ReportView {
            height: height.map(|h| (h, 1_000)),
            span: Some((UNIX_EPOCH, UNIX_EPOCH + Duration::from_secs(sampled_secs))),
            ..ReportView::default()
        })
    }

    #[test]
    fn each_live_read_dates_the_vitals_it_carries() {
        let mut state = SyncWatchState::default();
        fold_live(&mut state, read(60, Some(500)), Duration::from_secs(1));
        fold_live(&mut state, read(61, Some(510)), Duration::from_secs(2));
        let v = state.vitals.as_ref().expect("vitals");
        assert_eq!(
            (v.height, v.received_at, v.span),
            (510, Duration::from_secs(2), Duration::from_secs(61))
        );
    }

    #[test]
    fn a_failed_read_keeps_the_last_vitals_and_names_the_cause() {
        let mut state = SyncWatchState::default();
        fold_live(&mut state, read(60, Some(500)), Duration::ZERO);
        fold_live(
            &mut state,
            Err("indexer /metrics: connection refused".into()),
            Duration::from_secs(5),
        );
        assert_eq!(state.vitals.as_ref().map(|v| v.height), Some(500));
        let note = state.metrics_note.as_deref().unwrap_or_default();
        assert!(note.contains("connection refused"), "{note}");
    }

    #[test]
    fn before_the_first_fetch_the_panel_says_why_it_is_empty() {
        let mut state = SyncWatchState::default();
        fold_live(&mut state, read(5, None), Duration::ZERO);
        assert!(state.vitals.is_none());
        assert_eq!(state.metrics_note.as_deref(), Some("no height scraped yet"));
    }

    /// Load failure names itself on the load column, never on the vitals
    #[test]
    fn a_failed_load_read_leaves_the_vitals_note_alone() {
        let mut state = SyncWatchState::default();
        fold_live(&mut state, read(60, Some(500)), Duration::ZERO);
        fold_loads(&mut state, Err("prometheus unreadable · timeout".into()));
        assert_eq!(state.metrics_note, None);
        assert_eq!(state.loads_note.as_deref(), Some("prometheus unreadable · timeout"));
    }

    fn newest_at(value: f64) -> Series {
        Series {
            reading: None,
            label: String::new(),
            unit: ztest::api::Unit::Count,
            facet: None,
            channel: None,
            points: vec![(0.0, 0.0), (5.0, value)],
            total: None,
            coverage: None,
        }
    }

    #[test]
    fn a_load_pairs_newest_usage_with_its_containers_declared_limit() {
        let resources = [
            ComponentResources {
                component: "zainod".into(),
                cpu: vec![newest_at(0.593)],
                mem: vec![newest_at(2.0 * GIB as f64)],
                ..ComponentResources::default()
            },
            ComponentResources {
                component: "zebrad".into(),
                cpu: vec![newest_at(1.5)],
                ..ComponentResources::default()
            },
        ];
        let limit = Resources::new(9_000, 24 * GIB, 0, 0);
        let limits = HashMap::from([("zainod".to_string(), limit)]);
        assert_eq!(
            loads(&resources, &limits),
            [
                ContainerLoad {
                    container: "zainod".into(),
                    usage: Resources::new(593, 2 * GIB, 0, 0),
                    limit: Some(limit),
                },
                ContainerLoad {
                    container: "zebrad".into(),
                    usage: Resources::new(1_500, 0, 0, 0),
                    limit: None,
                },
            ]
        );
    }

    #[test]
    fn only_a_declared_limit_becomes_a_denominator() {
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "metadata": { "name": "zainod-0" },
            "spec": { "containers": [
                { "name": "zainod", "resources": { "limits": { "cpu": "9", "memory": "24Gi" } } },
                { "name": "sidecar" },
            ] },
        }))
        .expect("pod fixture is valid");
        let limits = declared_limits(&[pod]);
        assert_eq!(limits.get("zainod"), Some(&Resources::new(9_000, 24 * GIB, 0, 0)));
        assert!(!limits.contains_key("sidecar"));
    }

    fn driver_pod(phase: &str, state: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": driver_pod_for("a-sync-id") },
            "spec": { "containers": [{ "name": DRIVER_CONTAINER }] },
            "status": {
                "phase": phase,
                "containerStatuses": [{
                    "name": DRIVER_CONTAINER,
                    "ready": false,
                    "restartCount": 0,
                    "image": "runner",
                    "imageID": "",
                    "state": state,
                }],
            },
        }))
        .expect("driver pod fixture is valid")
    }

    /// Attaching right after `sync start` finds the container still creating; asking the kubelet
    /// for its log there is the 400 this gate avoids
    #[test]
    fn a_container_that_has_not_started_has_no_log_and_says_why() {
        let pod = driver_pod(
            "Pending",
            serde_json::json!({ "waiting": { "reason": "ContainerCreating" } }),
        );
        assert!(!logs_available(&pod, DRIVER_CONTAINER));
        assert_eq!(driver_phase(&pod), "Pending · ContainerCreating");
    }

    #[test]
    fn a_failing_image_pull_is_named_rather_than_looking_like_a_hang() {
        let pod = driver_pod(
            "Pending",
            serde_json::json!({ "waiting": { "reason": "ImagePullBackOff" } }),
        );
        assert!(!logs_available(&pod, DRIVER_CONTAINER));
        assert_eq!(driver_phase(&pod), "Pending · ImagePullBackOff");
    }

    /// Tail loop must tell a dropped connection (reattach) from a finished run (stop)
    #[test]
    fn a_terminated_container_has_a_log_but_is_not_running() {
        let exited = driver_pod(
            "Succeeded",
            serde_json::json!({
                "terminated": { "exitCode": 0, "finishedAt": "2026-08-04T00:01:00Z" },
            }),
        );
        assert!(logs_available(&exited, DRIVER_CONTAINER));
        assert!(!running(&exited, DRIVER_CONTAINER));

        let live = driver_pod(
            "Running",
            serde_json::json!({ "running": { "startedAt": "2026-08-04T00:00:00Z" } }),
        );
        assert!(logs_available(&live, DRIVER_CONTAINER));
        assert!(running(&live, DRIVER_CONTAINER));
        assert_eq!(driver_phase(&live), "Running");
    }

    #[test]
    fn a_gap_window_always_covers_at_least_one_second() {
        assert!(gap_since(Instant::now()) >= 1, "kube rejects a zero window");
    }
}
