//! `ztest sync watch` — the live view of a detached sync.
//!
//! - Two streams, one terminal: driver-pod log ([`SyncEvent`](ztest::sync::SyncEvent)
//!   lines lifted out to drive the pinned panel) + the indexer-under-test's log,
//!   scrolling above it verbatim
//! - Both read over the kube API (no `kubectl` on the laptop)
//! - Read-only: Ctrl-C detaches, never stops the sync (only `ztest sync stop` does)

use std::io::{IsTerminal, stdout};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use futures::{AsyncBufReadExt as _, StreamExt as _};
use k8s_openapi::api::core::v1::{ContainerState, Pod};
use kube::api::{Api, ListParams, LogParams};

use ztest::api::fmt::thousands;
use ztest::api::metrics::{PodExporter, Poller, Sample};
use ztest::api::naming::RUN_NAMESPACE;
use ztest::sync::{
    SyncEvent, SyncStatus, Window, decode_event, driver_pod_for, find_driver, namespace_for,
    plot_channels, read_report,
};
use ztest_ui::console::{Console, SceneFrame};
use ztest_ui::template::{Fields, draw};
use ztest_ui::{
    ProbeRow, SetupStep, SyncVitals, SyncWatchState, Theme, render_sync_load,
    render_sync_watch_panel, render_sync_work,
};

use super::{DRIVER_CONTAINER, driver_profile, report_verdict};

/// Driver-pod address: run-namespace API handle + pod name.
///
/// - Neither half implies the other, so they travel together
/// - Driver = a *runner* pod, in [`RUN_NAMESPACE`], not the sync namespace it
///   deploys into (a sync-scoped `Api<Pod>` would silently read the wrong one)
/// - Every driver-side read here goes through this; SUT reads keep their own handle
pub(super) struct DriverPod {
    api: Api<Pod>,
    name: String,
}

impl DriverPod {
    pub(super) fn new(client: &kube::Client, sync_id: &str) -> Self {
        DriverPod {
            api: Api::namespaced(client.clone(), RUN_NAMESPACE),
            name: driver_pod_for(sync_id),
        }
    }

    async fn get(&self) -> Result<Option<Pod>, kube::Error> {
        self.api.get_opt(&self.name).await
    }
}

/// What an attach reads from, bundled: driver log in [`RUN_NAMESPACE`], subject log +
/// pod-metrics sampling in the sync's namespace (a bundle is harder to mix up than
/// four bare handles, two of which are `Api<Pod>`)
pub(super) struct Followed<'a> {
    driver: &'a DriverPod,
    sut: &'a Api<Pod>,
    client: &'a kube::Client,
    /// Sync's namespace; `sut`'s, named separately for the metrics.k8s.io read
    namespace: &'a str,
}

/// Driver-pod phase re-read interval; slow, since it only matters before the
/// first event (and after the last)
const POD_POLL: Duration = Duration::from_secs(2);

/// Per-stream backfill: deep enough to explain the attach, shallow enough not to
/// flood on a days-old sync
const TAIL_LINES: i64 = 200;

/// Component whose log rides alongside the driver's = the indexer (a sync
/// profile's subject). By category, not backend name, so `lightwalletd` is
/// followed like `zainod`
const SUT_SELECTOR: &str = "ztest.io/component-category=indexer";

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
    let ns = namespace_for(id);
    let pod = find_driver(&client, id).await?;
    let profile = driver_profile(&pod).unwrap_or_else(|| id.to_string());
    // Two namespaces: driver in the run namespace, the topology it deploys (SUT +
    // metrics poller) in the sync's
    let driver = DriverPod::new(&client, id);
    let api: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let theme = Theme::detect();

    // Non-TTY: no panel to pin, so the events that would drive it render as lines
    if !stdout().is_terminal() {
        return linear(&driver, &client, id, &theme).await;
    }

    let cancel_theme = theme.clone();
    let cancel_panel =
        Box::new(move |elapsed| ztest_ui::render_cancel_panel(elapsed, &cancel_theme));
    // Shared with the feed so a tick's `received_at` and each frame's `elapsed`
    // share an origin
    let session_start = Instant::now();
    let (console, guard) = match Console::start(session_start, cancel_panel) {
        Ok(cg) => cg,
        Err(_) => return linear(&driver, &client, id, &theme).await,
    };

    let mut feed = Feed::new(
        profile,
        id.to_string(),
        ztest::api::cluster_config::active_context().unwrap_or_else(|| "(cluster)".into()),
        driver_phase(&pod),
    );

    // Three views: position+pace / per-pool rates / per-pod draw. Left and mid share
    // the 1s scrape; right is the 15s `metrics.k8s.io` sample, on its own clock and
    // labelled as a different quantity so the mismatch cannot read as a stall.
    // Too narrow for three → load is the column that goes
    let render = |feed: &Feed| {
        let (state, theme) = (feed.state.clone(), theme.clone());
        console.scene(move |elapsed| SceneFrame {
            left: render_sync_watch_panel(&state, elapsed, &theme),
            mid: Some(render_sync_work(&state, elapsed, &theme)),
            right: render_sync_load(&state, &theme),
            live: None,
        });
    };
    render(&feed);

    // Controller-side, only while this attach lasts (exporter = the only
    // sub-scrape-interval source; a departed watcher should stop dialing it).
    // Built here, not by the sync machinery: this command composes two independent
    // systems, and the poller needs no driver
    let mut metrics = Poller::spawn(
        PodExporter::new(
            client.clone(),
            ns.clone(),
            ztest::component::ComponentCategory::Indexer.as_str(),
            ztest::backends::metrics_rows,
        ),
        ztest::api::metrics::LIVE_PERIOD,
    );

    feed.observed = true;
    let tail = tail_loop(
        Followed { driver: &driver, sut: &api, client: &client, namespace: &ns },
        &mut metrics,
        &console,
        session_start,
        &mut feed,
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

/// Panel-less attach: linear driver-log tail, then the settled verdict. Off a TTY,
/// or when the console cannot start
async fn linear(
    driver: &DriverPod,
    client: &kube::Client,
    id: &str,
    theme: &Theme,
) -> Result<WatchEnd> {
    plain_tail(driver, theme).await?;
    settled(client, id, theme).await
}

/// Post-log verdict: the durable report if the driver mirrored one, else "no report"
async fn settled(client: &kube::Client, id: &str, theme: &Theme) -> Result<WatchEnd> {
    match read_report(client, id).await? {
        Some(report) => {
            print!("{}", report_verdict(theme, &report));
            Ok(WatchEnd::Settled(SyncStatus::Finished(report.verdict)))
        }
        None => {
            println!("sync {id}: tail ended — no report yet (`ztest sync status {id}`)");
            Ok(WatchEnd::Settled(SyncStatus::Unresolved))
        }
    }
}

/// Merge driver log + SUT log + pod-phase poll until the driver's stream ends or
/// the user detaches
async fn tail_loop(
    followed: Followed<'_>,
    metrics: &mut Poller,
    console: &Console,
    session_start: Instant,
    feed: &mut Feed,
    theme: &Theme,
    render: impl Fn(&Feed),
) -> Result<()> {
    let Followed { driver, sut: api, client, namespace } = followed;
    // Last cause shown → a standing condition is stated once, not once a second
    let mut last_note: Option<String> = None;

    let started = open_driver_log(driver, &|| console.cancelled(), |phase| {
        feed.state.pod_phase = phase;
        render(feed);
    })
    .await?;
    let Some(mut driver_log) = started else {
        return Ok(());
    };
    // SUT pod absent on an early attach (the driver provisions its own topology) →
    // stream opened lazily by the poll below. Name held apart from stream so a
    // line's prefix reads in a `select!` arm that may be replacing the stream
    let mut sut: Option<LineStream> = None;
    let mut sut_name = String::new();
    // Last pod followed → a reopened stream tells a resumed follow from a replacement
    let mut sut_prev: Option<String> = None;
    // Where a resumed stream must pick up
    let (mut driver_seen, mut sut_seen) = (Instant::now(), Instant::now());
    let mut ticker = tokio::time::interval(POD_POLL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Own cadence, matching metrics-server's resolution: a faster poll re-reads one
    // reading. First tick fires immediately, so the column fills on attach
    let mut load_ticker = tokio::time::interval(ztest::api::podmetrics::SAMPLE_PERIOD);
    load_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if console.cancelled() {
            break;
        }
        // Applied after the `select!`: no handler may mutate what another borrows
        let (mut close_sut, mut lost_driver) = (false, false);
        let mut next_sample: Option<Sample> = None;
        tokio::select! {
            line = driver_log.next() => match line {
                Some(Ok(l)) => {
                    driver_seen = Instant::now();
                    if let Some(text) = feed.absorb(&l, session_start.elapsed(), theme) {
                        console.scrollback(prefixed("driver", &text, theme));
                    }
                    render(feed);
                }
                Some(Err(_)) | None => lost_driver = true,
            },
            line = next_line(&mut sut) => match line {
                Some(Ok(l)) => {
                    sut_seen = Instant::now();
                    console.scrollback(prefixed(&sut_name, &l, theme));
                }
                // SUT stream ending != run ending (restarted/replaced pod): drop and
                // let the poll reopen
                Some(Err(_)) | None => close_sut = true,
            },
            sample = metrics.changed() => {
                // `metrics` borrowed by this arm's future → the fold consuming it
                // cannot run inside the arm
                next_sample = Some(sample);
            }
            _ = load_ticker.tick() => {
                match ztest::api::podmetrics::sample(client, namespace).await {
                    Ok(pods) => {
                        // Empty ≠ error: pods not created yet. Note stays, so the
                        // column keeps naming why it is blank
                        if !pods.is_empty() {
                            feed.state.pods_note = None;
                        }
                        feed.state.pods = pods;
                    }
                    // No metrics API is a standing condition, not a per-sample failure
                    // → recorded once, never scrolled
                    Err(why) => {
                        feed.state.pods.clear();
                        feed.state.pods_note = Some(load_note(&why.to_string()));
                    }
                }
                render(feed);
            }
            _ = ticker.tick() => {
                if let Ok(Some(pod)) = driver.get().await {
                    let phase = driver_phase(&pod);
                    if phase != feed.state.pod_phase {
                        feed.state.pod_phase = phase;
                        render(feed);
                    }
                }
                if sut.is_none()
                    && let Some(name) = find_sut(api).await
                {
                    // Same pod → resumed follow, replay the gap only; a different pod
                    // is a fresh subject and gets the context tail
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
        if let Some(sample) = next_sample {
            let component = metrics.component();
            feed.observe(&sample, component.as_deref(), session_start.elapsed());
            // Panel names the cause; scrollback echoes it once, so a condition that
            // later clears still left a trace
            let note = feed.state.metrics_note.clone();
            if note != last_note
                && let Some(note) = &note
            {
                console.scrollback(prefixed("ztest", &format!("live metrics: {note}"), theme));
            }
            last_note = note;
            render(feed);
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
/// - Hours-long follow-streams are routinely cut (idle API-server timeout, proxy
///   hop, rebalanced connection); reading that as the run's end strands a live sync
/// - `None` = the driver really finished
async fn reattach_driver(driver: &DriverPod, last_seen: Instant) -> Result<Option<LineStream>> {
    // Backs off a stream that fails immediately; also lets a driver mid-exit finish
    // writing
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

/// Sampling failure → the panel's one-line cause. A missing aggregated API is the
/// common case and names its own fix; anything else shows verbatim
fn load_note(why: &str) -> String {
    match why.contains("metrics.k8s.io") || why.contains("404") {
        true => "no metrics API (`ztest cluster setup`)".to_string(),
        false => why.to_string(),
    }
}

/// Replay window leaving no gap: since the last line read, rounded up (API
/// resolution = 1s)
fn gap_since(last_seen: Instant) -> i64 {
    last_seen.elapsed().as_secs().saturating_add(1) as i64
}

/// Await the driver container's start, then follow its log.
///
/// - Log request before start = a 400, and `--watch` attaches at pod creation (an
///   image pull holds `ContainerCreating` for minutes)
/// - Gating on container state, not retrying the error, makes the wait report
///   itself: `observe` sees every phase change, so `ImagePullBackOff` reaches the
///   panel instead of reading as a hang
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

/// Phase + the container's waiting reason (bare `Pending` cannot separate
/// scheduling from `ImagePullBackOff`)
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

/// History a freshly-opened log stream replays.
///
/// `Seconds` = a resumed attach, which takes overlap over a gap (driver events
/// de-dupe on their sequence; a repeated component line is only cosmetic)
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

/// Running indexer pod's name; `None` until the driver provisions one (caller retries)
async fn find_sut(api: &Api<Pod>) -> Option<String> {
    let pods = api.list(&ListParams::default().labels(SUT_SELECTOR)).await.ok()?;
    pods.items
        .into_iter()
        .find(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"))?
        .metadata
        .name
}

/// Next line of an optional stream; `None` parks forever, so `select!` ignores that
/// branch until one opens
async fn next_line(stream: &mut Option<LineStream>) -> Option<std::io::Result<String>> {
    match stream {
        Some(s) => s.next().await,
        None => std::future::pending().await,
    }
}

/// Tag a line with the pod it came from, so a merged stream stays attributable.
fn prefixed(source: &str, line: &str, theme: &Theme) -> String {
    let f = Fields::new().text("source", source).text("stem", theme.chars.vbar).text("line", line);
    format!("{}\n", draw(tmpl::SOURCE, &f, theme))
}

/// Linear follow-tail of the driver log (non-TTY, or console won't start). No
/// panel, so every event including ticks renders as a line = a progress log
async fn plain_tail(driver: &DriverPod, theme: &Theme) -> Result<()> {
    let mut lines = open_driver_log(driver, &|| false, |phase| {
        println!("ztest sync: driver {phase}");
    })
    .await?
    .expect("only a cancelling caller gets None");
    let mut feed = Feed::new(String::new(), String::new(), String::new(), String::new());
    let started = Instant::now();
    let mut seen = Instant::now();
    // Reattaches like the panel path: a piped `--watch` in CI follows the same
    // hours-long log
    loop {
        while let Some(line) = lines.next().await {
            let line = line.context("read sync log")?;
            seen = Instant::now();
            if let Some(text) = feed.absorb_verbose(&line, started.elapsed(), theme) {
                println!("{text}");
            }
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

/// Newest live progress recoverable one-shot, by folding the driver log's tail.
///
/// - Makes `ztest sync status` truthful mid-run (the durable report exists only
///   after the run ends; without this the only mid-run answer is the pod phase)
/// - `None` = no tick published yet (provisioning) or the log is unreadable
pub(super) async fn latest_progress(driver: &DriverPod) -> Option<SyncWatchState> {
    let lp = LogParams {
        tail_lines: Some(STATUS_TAIL_LINES),
        container: Some(DRIVER_CONTAINER.to_string()),
        ..Default::default()
    };
    let logs = driver.api.logs(&driver.name, &lp).await.ok()?;
    let theme = Theme::for_capabilities(false, true);
    let mut feed = Feed::new(String::new(), String::new(), String::new(), String::new());
    for line in logs.lines() {
        feed.absorb(line, Duration::ZERO, &theme);
    }
    feed.state.vitals.is_some().then_some(feed.state)
}

/// Deeper than [`TAIL_LINES`]: two events per tick, and the tail must still hold
/// one after a burst of component logging
const STATUS_TAIL_LINES: i64 = 400;

// ─────────────────────────── event folding ────────────────────────────

/// Scrollback vocabulary, matching the pinned panel's row templates (a piped watch and a
/// TTY one report the same figures the same way)
mod tmpl {
    pub(super) const SOURCE: &str = "{source:>8|dim} {stem|dim} {line}";
    pub(super) const SETUP: &str = "setup {@dot|dim} {subject} {@dot|dim} {detail}";
    pub(super) const STARTED: &str =
        "sync engine started {@dot|dim} {probes|bold} probes {@dot|dim} {tick|millis} tick";
    pub(super) const TICK: &str = concat!(
        "tick {seq|bold} {@dot|dim} height {height|bold}[ / {target|dim}]",
        " {@dot|dim} {pct|fraction} {@dot|dim} {phase}",
    );
    pub(super) const VIOLATION: &str = "{@fail|fail} {probe|fail}[ at {height|dim}]: {detail}";
    pub(super) const FINISHED: &str = concat!(
        "sync finished {@dot|dim} {verdict} {@dot|dim} {ticks|bold} ticks",
        " {@dot|dim} {violations|bold} violations {@dot|dim} {gaps|bold} coverage gaps",
    );
}

/// Scrollback lines flow, never column-align → no `*` cell to size
/// Folds the driver's event stream into the panel's view model, holding the
/// derived quantities (scan rate, ETA) no single event carries.
pub(super) struct Feed {
    pub(super) state: SyncWatchState,
    /// Live scrape feeding the panel → the 5s tick stops writing vitals and
    /// contributes only phase + reorg depth (two writers would flip the panel
    /// between a 1s and a 5s reading); `ztest sync status` keeps the tick-fed path
    pub(super) observed: bool,
    /// One estimator, two clocks: scrapes stamped as observed here, driver ticks on
    /// the driver's own elapsed (a resumed stream replays a minute in milliseconds)
    scraped: Window,
    ticked: Window<Duration>,
    /// Driver's cadence, sizing `ticked`. A window narrower than the gap between ticks
    /// clears on every push and can never measure
    tick: Duration,
    /// Second-by-second, not the driver's once-a-minute `Series` (a sparkline 60×
    /// coarser than the number beside it is a different measurement)
    series: ztest::sync::Timeline,
    /// Newest scrape folded; moves forward only (a failed scrape re-sends the last
    /// exposition, which differenced against itself reads as a phantom stall)
    observed_at: Option<Instant>,
    phase: Option<ztest::sync::Phase>,
    phase_detail: Option<String>,
    reorg_depth: u32,
    /// Newest event folded; keeps the fold exactly-once across a resumed stream's
    /// by-time replay
    folded_through: Option<u64>,
}

impl Feed {
    fn new(profile: String, sync_id: String, context: String, pod_phase: String) -> Feed {
        Feed {
            state: SyncWatchState { profile, sync_id, context, pod_phase, ..Default::default() },
            observed: false,
            scraped: Window::new(ztest::api::metrics::LIVE_PERIOD),
            ticked: Window::new(ztest::sync::DEFAULT_TICK),
            tick: ztest::sync::DEFAULT_TICK,
            series: ztest::sync::Timeline::new(plot_channels(), ztest::api::metrics::LIVE_PERIOD),
            observed_at: None,
            phase: None,
            phase_detail: None,
            reorg_depth: 0,
            folded_through: None,
        }
    }

    /// Fold one exporter scrape into the panel's vitals. Heights, pace, per-pool
    /// rates and per-block cost all come off one exposition → no two columns can
    /// describe different instants
    fn observe(&mut self, sample: &Sample, component: Option<&str>, elapsed: Duration) {
        let fresh = sample
            .at
            .filter(|at| self.observed_at.is_none_or(|folded| *at > folded))
            .zip(component)
            .and_then(|(at, name)| Some((at, ztest::backends::observe(name, &sample.exposition)?)));

        if let Some((at, observation)) = fresh {
            self.observed_at = Some(at);
            self.scraped.push(at, observation);
            self.state.vitals = self.vitals_of(&self.scraped, elapsed);
            self.record_series(elapsed);
        }
        self.state.metrics_note = sample.note(self.state.vitals.is_some());
    }

    /// One window → the panel's vitals. Same fold whichever clock fed it, so no column
    /// means one thing under a live scrape and another off the driver's ticks
    fn vitals_of<S: ztest::rate::Stamp>(
        &self,
        window: &Window<S>,
        elapsed: Duration,
    ) -> Option<SyncVitals> {
        let observation = window.latest()?;
        let work = window.work_rate();
        Some(SyncVitals {
            height: observation.height.unwrap_or(0),
            target: observation.target,
            pct: observation.pct(),
            phase: self.phase,
            phase_detail: self.phase_detail.clone(),
            reorg_depth: self.reorg_depth,
            pace: window.block_pace(),
            tx_rate: window.tx_rate(),
            work_rate: work.and_then(|r| r.total()),
            pool_rates: work.map(|r| r.channels().to_vec()).unwrap_or_default(),
            cost: observation.cost,
            received_at: elapsed,
        })
    }

    /// This second's rates → the sparkline series; unmeasured channels contribute
    /// a gap, never a zero (an uncounted pool must not draw a floor)
    fn record_series(&mut self, elapsed: Duration) {
        let Some(vitals) = &self.state.vitals else {
            return;
        };
        let mut values = vec![vitals.pace.map(|p| p.per_sec)];
        values.extend(vitals.pool_rates.iter().map(|(_, rate)| *rate));
        self.series.push(elapsed, &values);
        self.state.timeline = Some(self.series.clone());
    }

    /// One driver log line → text for scrollback; `None` for a panel-only event or
    /// one already folded
    fn absorb(&mut self, line: &str, elapsed: Duration, theme: &Theme) -> Option<String> {
        self.fold(line, elapsed, theme, false)
    }

    /// [`absorb`](Self::absorb) for the panel-less path: nowhere to pin live state,
    /// so per-tick progress renders as ordinary lines
    fn absorb_verbose(&mut self, line: &str, elapsed: Duration, theme: &Theme) -> Option<String> {
        self.fold(line, elapsed, theme, true)
    }

    fn fold(
        &mut self,
        line: &str,
        elapsed: Duration,
        theme: &Theme,
        verbose: bool,
    ) -> Option<String> {
        let Some(env) = decode_event(line) else {
            return Some(line.to_string());
        };
        if self.already_folded(env.n) {
            return None;
        }
        self.folded_through = env.n.or(self.folded_through);
        self.record(&env.event, elapsed, theme, verbose)
    }

    /// Already folded (as a resumed stream's replay is)? An unnumbered event (older
    /// driver) is folded — repeating an observation beats losing one
    fn already_folded(&self, n: Option<u64>) -> bool {
        match (n, self.folded_through) {
            (Some(n), Some(through)) => n <= through,
            _ => false,
        }
    }

    /// Fold one event, returning any line worth keeping in history. `verbose` (the
    /// panel-less path) also renders per-tick progress, redundant with a panel
    fn record(
        &mut self,
        event: &SyncEvent,
        elapsed: Duration,
        theme: &Theme,
        verbose: bool,
    ) -> Option<String> {
        match event {
            SyncEvent::Setup { phase, detail, component } => {
                let subject = component.clone().unwrap_or_else(|| phase.clone());
                self.state.setup = Some(SetupStep {
                    subject: subject.clone(),
                    detail: detail.clone(),
                    received_at: elapsed,
                });
                // Panel-only here: the driver's own `provisioning component` lines
                // already reach scrollback, so ztest lines would double every step
                verbose.then(|| {
                    let f = Fields::new().text("subject", subject).text("detail", detail.clone());
                    draw(tmpl::SETUP, &f, theme)
                })
            }
            SyncEvent::Started { profile, sync_id, tick_ms, probes } => {
                let tick = Duration::from_millis(*tick_ms);
                // Re-published on a resumed stream; resizing to the same width would
                // throw away the samples already folded
                if self.tick != tick {
                    self.tick = tick;
                    self.ticked = Window::new(tick);
                }
                if self.state.profile.is_empty() {
                    self.state.profile = profile.clone();
                    self.state.sync_id = sync_id.clone();
                }
                let f = Fields::new()
                    .text("probes", thousands(*probes as u64))
                    .value("tick", *tick_ms as f64);
                Some(draw(tmpl::STARTED, &f, theme))
            }
            SyncEvent::Tick(t) => {
                // Engine state; no exporter publishes it, so every path needs it here
                self.phase = Some(t.phase);
                self.phase_detail = t.detail.clone();
                self.reorg_depth = t.reorg_depth;
                if let Some(vitals) = &mut self.state.vitals {
                    vitals.phase = Some(t.phase);
                    vitals.phase_detail = t.detail.clone();
                    vitals.reorg_depth = t.reorg_depth;
                }
                self.ticked.push(t.at(), t.into());
                if !self.observed {
                    self.state.vitals = self.vitals_of(&self.ticked, elapsed);
                }
                verbose.then(|| {
                    let f = Fields::new()
                        .text("seq", thousands(t.seq))
                        .text("height", thousands(u64::from(t.height)))
                        .maybe_text("target", t.target.map(|h| thousands(u64::from(h))))
                        .value("pct", f64::from(t.pct) / 100.0)
                        .text("phase", t.phase.to_string());
                    draw(tmpl::TICK, &f, theme)
                })
            }
            // Replaced wholesale, never merged: the driver owns the bucketing, and a
            // later publication is always the more complete one.
            // Ignored under a live scrape, which builds its own second-by-second
            // series (taking turns would change sparkline resolution mid-read)
            SyncEvent::Series { timeline } => {
                if !self.observed {
                    self.state.timeline = Some(timeline.clone());
                }
                None
            }
            SyncEvent::Probes { board } => {
                self.state.probes = board
                    .iter()
                    .map(|p| ProbeRow {
                        name: p.name.clone(),
                        state: p.state,
                        since_satisfied: p.since_ms.map(Duration::from_millis),
                        window: p.window_ms.map(Duration::from_millis),
                    })
                    .collect();
                None
            }
            SyncEvent::Violation { probe, height, detail } => {
                self.state.violations += 1;
                let f = Fields::new()
                    .text("probe", probe.clone())
                    .maybe_text("height", height.map(|h| thousands(u64::from(h))))
                    .text("detail", detail.clone());
                Some(draw(tmpl::VIOLATION, &f, theme))
            }
            SyncEvent::Finished { verdict, violations, coverage_gaps, ticks } => {
                let f = Fields::new()
                    .text("verdict", verdict.to_string())
                    .text("ticks", thousands(*ticks))
                    .text("violations", thousands(*violations as u64))
                    .text("gaps", thousands(*coverage_gaps as u64));
                Some(draw(tmpl::FINISHED, &f, theme))
            }
            SyncEvent::Unknown => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ztest::sync::Op;

    fn feed() -> Feed {
        Feed::new(
            "zaino_state_sync".into(),
            "zaino-state-sync-a52f9ec9".into(),
            "zingo-infra".into(),
            "Running".into(),
        )
    }

    /// zaino exposition at `height`: `transparent` cumulative ops + per-block
    /// timing summary = the subset the panel resolves
    fn exposition(
        height: u32,
        transparent: u64,
    ) -> std::sync::Arc<ztest::api::metrics::Exposition> {
        let text = format!(
            "# TYPE zaino_sync_fetched_height gauge\n\
             zaino_sync_fetched_height {height}\n\
             # TYPE zaino_sync_target_height gauge\n\
             zaino_sync_target_height 1024\n\
             # TYPE zaino_sync_transparent_outputs_total counter\n\
             zaino_sync_transparent_outputs_total{{stage=\"finalised\"}} {transparent}\n\
             # TYPE zaino_sync_block_build_seconds summary\n\
             zaino_sync_block_build_seconds_sum 1.0\n\
             zaino_sync_block_build_seconds_count 100\n\
             # TYPE zaino_sync_block_fetch_seconds summary\n\
             zaino_sync_block_fetch_seconds_sum 0.6\n\
             zaino_sync_block_fetch_seconds_count 100\n\
             # TYPE zaino_sync_treestate_fetch_seconds summary\n\
             zaino_sync_treestate_fetch_seconds_sum 0.1\n\
             zaino_sync_treestate_fetch_seconds_count 100\n"
        );
        let mut e = ztest::api::metrics::Exposition::default();
        e.absorb(&text);
        std::sync::Arc::new(e)
    }

    fn sample(at: Instant, height: u32, transparent: u64) -> Sample {
        Sample { at: Some(at), exposition: exposition(height, transparent), error: None }
    }

    /// Whole live display in one path: two zaino scrapes → height, scan rate,
    /// per-pool rates, cost split, no metric name touched outside the backend
    #[test]
    fn two_scrapes_become_the_panels_vitals() {
        let mut f = feed();
        f.observed = true;
        let origin = Instant::now();

        f.observe(&sample(origin, 900, 1_000), Some("zainod"), Duration::ZERO);
        let first = f.state.vitals.as_ref().expect("a scrape is a reading");
        assert_eq!(first.height, 900);
        assert_eq!(first.target, Some(1024));
        assert_eq!(
            first.pace, None,
            "one scrape cannot be a rate, and must not be shown as a zero"
        );
        assert_eq!(first.cost.fetch_ms, Some(6.0));
        assert_eq!(first.cost.treestate_ms, Some(1.0));
        assert_eq!(
            first.cost.parse_ms,
            Some(3.0),
            "build 10ms minus both source reads (6ms + 1ms) is zaino's own per-block cost"
        );

        f.observe(
            &sample(origin + Duration::from_secs(2), 1_000, 3_000),
            Some("zainod"),
            Duration::from_secs(2),
        );
        let v = f.state.vitals.as_ref().expect("still reading");
        assert_eq!(v.height, 1_000);
        assert_eq!(v.pace.map(|p| p.per_sec), Some(50.0), "100 blocks over 2s");
        assert_eq!(
            v.pool_rates.iter().find(|(name, _)| *name == "transparent").map(|(_, r)| *r),
            Some(Some(1_000.0)),
            "2,000 transparent ops over 2s"
        );
        assert_eq!(
            v.pool_rates.iter().find(|(name, _)| *name == "orchard").map(|(_, r)| *r),
            Some(None),
            "a pool this exposition never published stays unmeasured"
        );
        assert!(f.state.timeline.is_some(), "the trend series is recorded");
        assert!(f.state.metrics_note.is_none(), "nothing to explain away");
    }

    /// Failed scrape re-sends the last exposition under its original timestamp;
    /// re-folding differences a reading against itself = a phantom stall
    #[test]
    fn a_repeated_sample_is_not_folded_twice() {
        let mut f = feed();
        f.observed = true;
        let origin = Instant::now();
        let first = sample(origin, 900, 1_000);
        f.observe(&first, Some("zainod"), Duration::ZERO);
        f.observe(
            &sample(origin + Duration::from_secs(1), 1_000, 2_000),
            Some("zainod"),
            Duration::from_secs(1),
        );
        let rate = f.state.vitals.as_ref().unwrap().pace;

        // Poller re-sends its newest sample unchanged
        f.observe(&first, Some("zainod"), Duration::from_secs(2));
        assert_eq!(
            f.state.vitals.as_ref().unwrap().pace,
            rate,
            "a re-sent sample must leave the window untouched"
        );
    }

    /// Driver tick must not write vitals out from under the owning scrape (measured
    /// 5s apart → alternating shows two heights per tick)
    #[test]
    fn a_tick_contributes_only_what_the_exporter_cannot_publish() {
        let mut f = feed();
        f.observed = true;
        f.observe(&sample(Instant::now(), 900, 1_000), Some("zainod"), Duration::ZERO);
        let tick = ztest::sync::encode_event(&tick_at(1, 5, 0));
        f.absorb(tick.trim_end(), Duration::ZERO, &theme());

        let v = f.state.vitals.as_ref().expect("vitals");
        assert_eq!(v.height, 900, "the scrape still owns the height");
        assert_eq!(v.phase, Some(ztest::sync::Phase::Syncing), "the tick still owns the phase");
    }

    fn theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn tick_event(seq: u64, height: u32) -> SyncEvent {
        tick_at(seq, height, 0)
    }

    /// Tick published `elapsed_ms` into the driver's run
    fn tick_at(seq: u64, height: u32, elapsed_ms: u64) -> SyncEvent {
        SyncEvent::Tick(ztest::sync::SyncTick {
            detail: None,
            seq,
            elapsed_ms,
            height,
            target: Some(1024),
            pct: 0.0,
            phase: ztest::sync::Phase::Syncing,
            reorg_depth: 0,
            work: ztest::sync::Work::ZERO,
        })
    }

    /// Tick carrying cumulative work at `elapsed_ms`, for rows that separate an
    /// unmeasured pool from an idle one
    fn tick_with_work(seq: u64, elapsed_ms: u64, sapling: u64, orchard: u64) -> SyncEvent {
        let SyncEvent::Tick(mut t) = tick_at(seq, 100, elapsed_ms) else {
            unreachable!("tick_at builds a tick")
        };
        t.work.set(Op::SaplingOutput, sapling).set(Op::OrchardAction, orchard);
        SyncEvent::Tick(t)
    }

    /// Wire → view model, the distinction the work map exists for: sapling/orchard
    /// counted, ironwood counted & idle, tier-B never measured
    #[test]
    fn an_unmeasured_pool_reaches_the_panel_as_absent_not_zero() {
        let mut f = feed();
        // Ten seconds apart, so a fractional rate survives `Work`'s integer counts
        for (seq, ms, sapling, orchard) in [(1, 0, 0, 0), (2, 10_000, 194, 42)] {
            f.absorb(
                &ztest::sync::encode_event(&tick_with_work(seq, ms, sapling, orchard)),
                Duration::ZERO,
                &theme(),
            );
        }
        let vitals = f.state.vitals.expect("a tick landed");
        let rate =
            |name: &str| vitals.pool_rates.iter().find(|(n, _)| *n == name).and_then(|(_, r)| *r);
        assert_eq!(rate("sapling"), Some(19.4));
        assert_eq!(rate("orchard"), Some(4.2));
        assert_eq!(rate("transparent"), None, "tier B was never counted");
        assert_eq!(rate("sprout"), None);
        let total = vitals.work_rate.expect("a measured total");
        assert!((total - 23.6).abs() < 1e-9, "{total}");
    }

    /// No published series = no graph; the panel must cope, not assume one
    #[test]
    fn a_series_event_becomes_the_panels_timeline() {
        let mut f = feed();
        assert!(f.state.timeline.is_none());

        let mut timeline = ztest::sync::Timeline::new(["work"], Duration::from_secs(5));
        timeline.push(Duration::ZERO, &[Some(100.0)]);
        f.absorb(
            &ztest::sync::encode_event(&SyncEvent::Series { timeline: timeline.clone() }),
            Duration::ZERO,
            &theme(),
        );
        assert_eq!(f.state.timeline, Some(timeline));
    }

    #[test]
    fn a_log_line_passes_through_and_an_event_does_not() {
        let mut f = feed();
        let theme = theme();
        assert_eq!(
            f.absorb("INFO ztest::env: provisioning", Duration::ZERO, &theme),
            Some("INFO ztest::env: provisioning".to_string()),
        );
        let line = ztest::sync::encode_event(&tick_event(0, 100));
        assert_eq!(f.absorb(line.trim_end(), Duration::ZERO, &theme), None);
        assert_eq!(f.state.vitals.as_ref().map(|v| v.height), Some(100));
    }

    /// A rate needs two ticks; the countdown off it waits for the window to agree
    /// with itself, so a burst mid-scan never publishes a number that walks backwards
    #[test]
    fn the_rate_appears_before_the_eta_it_supports() {
        let mut f = feed();
        let theme = theme();

        f.record(&tick_at(0, 100, 0), Duration::ZERO, &theme, false);
        assert!(f.state.vitals.as_ref().unwrap().pace.is_none(), "one tick is not a rate");

        f.record(&tick_at(1, 150, 5_000), Duration::ZERO, &theme, false);
        let pace = f.state.vitals.as_ref().unwrap().pace.expect("two ticks measure");
        assert_eq!(pace.per_sec, 10.0, "50 blocks in 5s");
        assert_eq!(pace.eta, None, "one interval is a measurement, not a trend");

        for (seq, height, ms) in [(2, 200, 10_000), (3, 250, 15_000)] {
            f.record(&tick_at(seq, height, ms), Duration::ZERO, &theme, false);
        }
        let pace = f.state.vitals.as_ref().unwrap().pace.expect("still measuring");
        assert_eq!(pace.per_sec, 10.0);
        // 774 blocks left at 10 blk/s
        assert_eq!(pace.eta, Some(Duration::from_secs_f64(77.4)));
    }

    /// Same total, delivered in one burst: the rate publishes, the countdown does not
    #[test]
    fn a_bursty_scan_publishes_a_rate_but_no_eta() {
        let mut f = feed();
        let theme = theme();
        for (seq, height, ms) in [(0, 100, 0), (1, 100, 5_000), (2, 100, 10_000), (3, 250, 15_000)]
        {
            f.record(&tick_at(seq, height, ms), Duration::ZERO, &theme, false);
        }
        let pace = f.state.vitals.as_ref().unwrap().pace.expect("a measured rate");
        assert_eq!(pace.per_sec, 10.0, "150 blocks over 15s");
        assert_eq!(pace.eta, None, "three idle ticks then a burst is no trend");
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

    /// Attaching right after `sync start` finds the container still creating;
    /// asking the kubelet for its log there is the 400 this gate avoids
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

    /// Finished driver still holds the whole event stream → `watch` on a completed
    /// sync must not wait for a restart
    #[test]
    fn a_running_or_exited_container_has_a_log() {
        let running = driver_pod(
            "Running",
            serde_json::json!({ "running": { "startedAt": "2026-08-04T00:00:00Z" } }),
        );
        assert!(logs_available(&running, DRIVER_CONTAINER));
        assert_eq!(driver_phase(&running), "Running");

        let exited = driver_pod(
            "Failed",
            serde_json::json!({
                "terminated": { "exitCode": 101, "finishedAt": "2026-08-04T00:01:00Z" },
            }),
        );
        assert!(logs_available(&exited, DRIVER_CONTAINER));
    }

    /// Provisioning = most of a sync's wall-clock, so the driver's gate must reach
    /// the panel, each replacing the last (a stale gate reads as false progress)
    #[test]
    fn a_setup_event_becomes_the_panels_current_gate() {
        let mut f = feed();
        let theme = theme();

        let creating = ztest::sync::encode_event(&SyncEvent::Setup {
            phase: "indexer".into(),
            detail: "creating pod".into(),
            component: Some("zainod".into()),
        });
        assert!(
            f.absorb(creating.trim_end(), Duration::from_secs(5), &theme).is_none(),
            "a setup event belongs to the panel, not scrollback"
        );
        let step = f.state.setup.as_ref().expect("a setup step");
        assert_eq!((&*step.subject, &*step.detail), ("zainod", "creating pod"));
        assert_eq!(step.received_at, Duration::from_secs(5));

        let waiting = ztest::sync::encode_event(&SyncEvent::Setup {
            phase: "indexer".into(),
            detail: "waiting for gRPC GetLightdInfo".into(),
            component: None,
        });
        f.absorb(waiting.trim_end(), Duration::from_secs(30), &theme);
        let step = f.state.setup.as_ref().expect("a setup step");
        assert_eq!(
            (&*step.subject, &*step.detail),
            ("indexer", "waiting for gRPC GetLightdInfo"),
            "a componentless step falls back to its phase"
        );
        assert_eq!(
            step.received_at,
            Duration::from_secs(30),
            "the age must restart with the gate, not run from launch"
        );
    }

    /// Resumed streams replay by time, delivering the overlap twice; a doubled
    /// violation tally is a wrong answer, not a cosmetic repeat
    #[test]
    fn a_replayed_event_is_folded_only_once() {
        let mut f = feed();
        let theme = theme();
        let violation = ztest::sync::encode_event(&SyncEvent::Violation {
            probe: "no_stall".into(),
            height: Some(901),
            detail: "liveness stall".into(),
        });

        let first = f.absorb(violation.trim_end(), Duration::ZERO, &theme);
        assert!(first.is_some(), "the first delivery must reach scrollback");
        assert_eq!(f.state.violations, 1);

        let replay = f.absorb(violation.trim_end(), Duration::ZERO, &theme);
        assert!(replay.is_none(), "a replay must not be printed again");
        assert_eq!(f.state.violations, 1, "a replay must not be counted again");
    }

    /// Sequences order events from one driver only; an ordinary log line carries
    /// none and must always pass through
    #[test]
    fn ordinary_log_lines_are_never_deduplicated() {
        let mut f = feed();
        let theme = theme();
        f.absorb(ztest::sync::encode_event(&tick_event(0, 100)).trim_end(), Duration::ZERO, &theme);
        for _ in 0..3 {
            assert_eq!(
                f.absorb("INFO zebrad: committed block", Duration::ZERO, &theme),
                Some("INFO zebrad: committed block".to_string()),
            );
        }
    }

    /// Guard is a watermark, not a one-shot: a newer event still folds after a replay
    #[test]
    fn folding_resumes_past_the_replayed_overlap() {
        let mut f = feed();
        let theme = theme();
        let lines: Vec<String> = (0..3)
            .map(|i| ztest::sync::encode_event(&tick_at(i, 100 + i as u32 * 50, i * 5_000)))
            .collect();

        for line in &lines {
            f.absorb(line.trim_end(), Duration::ZERO, &theme);
        }
        // Stream drops, resumes 1s back, replays the last tick before the next
        f.absorb(lines[2].trim_end(), Duration::ZERO, &theme);
        let resumed = ztest::sync::encode_event(&tick_at(3, 300, 15_000));
        f.absorb(resumed.trim_end(), Duration::ZERO, &theme);

        assert_eq!(f.state.vitals.as_ref().map(|v| v.height), Some(300));
    }

    #[test]
    fn a_gap_window_always_covers_at_least_one_second() {
        assert!(gap_since(Instant::now()) >= 1, "kube rejects a zero window");
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
        assert!(running(&live, DRIVER_CONTAINER));
    }
}
