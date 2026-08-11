//! `ztest sync watch` — the live view of a detached sync.
//!
//! Two streams merge into one terminal: the driver pod's log, from which the
//! [`SyncEvent`](crate::sync::SyncEvent) lines are lifted out to drive the pinned
//! panel, and the indexer-under-test's log, which scrolls above it verbatim. Both
//! are read over the kube API, so nothing here needs `kubectl` on the laptop.
//!
//! Read-only throughout: Ctrl-C detaches the terminal and never stops the sync
//! (only `ztest sync stop` does that).

use std::io::{IsTerminal, stdout};
use std::time::{Duration, Instant};

use futures::{AsyncBufReadExt as _, StreamExt as _};
use k8s_openapi::api::core::v1::{ContainerState, Pod};
use kube::api::{Api, ListParams, LogParams};
use owo_colors::OwoColorize as _;

use crate::cli::console::{Console, SceneFrame};
use crate::metrics::{PodExporter, Poller, Reading};
use crate::sync::{SyncEvent, decode_event, namespace_for};
use crate::ui::{
    ProbeRow, SetupStep, SyncVitals, SyncWatchState, Theme, render_sync_metrics,
    render_sync_watch_panel,
};

use super::{
    DRIVER_CONTAINER, DRIVER_POD, driver_profile, find_driver, print_report_details, read_report,
    report_headline, row_of,
};

/// How often the driver pod's phase is re-read. Only matters before the first
/// event lands (and after the last), so it stays slow.
const POD_POLL: Duration = Duration::from_secs(2);

/// Backfill requested from each stream. Deep enough to explain what just happened
/// on attach, shallow enough not to flood the terminal on a days-old sync.
const TAIL_LINES: i64 = 200;

/// The label of the component whose log rides alongside the driver's: the
/// indexer, which is the subject under test in a sync profile. Selected by
/// category rather than backend name so a profile running `lightwalletd` instead
/// of `zainod` is followed just the same.
const SUT_SELECTOR: &str = "ztest.io/component-category=indexer";

/// A boxed line stream over one pod's log.
type LineStream = std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<String>> + Send>>;

/// How an attach ended. `watch` reports; the *caller* decides what it means for
/// the process's exit status — `ztest sync watch` is an inspection command that
/// always succeeds, whereas `ztest sync start --watch` stands in for a foreground
/// run and must fail its pipeline on a failing verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WatchEnd {
    /// The user detached (Ctrl-C). The sync is still running.
    Detached,
    /// The driver's log ended and its durable report was read back.
    Finished { passed: bool },
    /// The log ended with no report: the driver was killed, evicted, or crashed
    /// before it could mirror one.
    Unresolved,
}

pub(super) async fn watch(id: &str) -> Result<WatchEnd, String> {
    let client = super::client().await?;
    let ns = namespace_for(id);
    let driver = find_driver(&client, &ns, id).await?;
    let profile = driver_profile(&driver).unwrap_or_else(|| id.to_string());
    let api: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let theme = Theme::detect();

    // Non-TTY (CI / piped): a plain linear tail — no panel to pin, so the events
    // that would have driven it are rendered as ordinary lines instead.
    if !stdout().is_terminal() {
        return linear(&api, &client, &ns, id, &theme).await;
    }

    let cancel_theme = theme.clone();
    let cancel_panel =
        Box::new(move |elapsed| crate::ui::render_cancel_panel(elapsed, &cancel_theme));
    // The console's session clock. Shared with the feed so a tick's `received_at`
    // is on the same origin as the `elapsed` each frame is rendered against.
    let session_start = Instant::now();
    let (console, guard) = match Console::start(session_start, cancel_panel) {
        Ok(cg) => cg,
        Err(_) => return linear(&api, &client, &ns, id, &theme).await,
    };

    let mut feed = Feed::new(
        profile,
        id.to_string(),
        crate::cluster_config::active_context().unwrap_or_else(|| "(cluster)".into()),
        row_of(&driver).2,
    );

    // Two independent views on one screen: the sync feed drives the left panel,
    // the metrics poller the right. Neither reads the other's state.
    let render = |feed: &Feed, metrics: &Reading| {
        let (state, theme, metrics) = (feed.state.clone(), theme.clone(), metrics.clone());
        console.scene(move |elapsed| SceneFrame {
            left: render_sync_watch_panel(&state, elapsed, &theme),
            right: render_sync_metrics(&metrics, &theme),
            live: None,
        });
    };
    render(&feed, &Reading::default());

    // Scraped controller-side, for as long as this attach lasts: the exporter is
    // the only source with sub-scrape-interval resolution, and a watcher that
    // walks away should stop dialing it.
    //
    // Built here and not by the sync machinery: metrics and sync are independent
    // systems that this command composes. The poller needs no driver, and would
    // work against any namespace with a metrics-exposing pod in it.
    let mut metrics = Poller::spawn(
        PodExporter::new(
            client.clone(),
            ns.clone(),
            crate::component::ComponentCategory::Indexer.as_str(),
            crate::backends::metrics_rows,
        ),
        crate::metrics::LIVE_PERIOD,
    );

    let tail = tail_loop(
        &api,
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
    settled(&client, &ns, id, &theme).await
}

/// The panel-less attach: tail the driver log linearly, then report what the sync
/// settled as. Used off a TTY and when the console cannot start.
async fn linear(
    api: &Api<Pod>,
    client: &kube::Client,
    ns: &str,
    id: &str,
    theme: &Theme,
) -> Result<WatchEnd, String> {
    plain_tail(api, theme).await?;
    settled(client, ns, id, theme).await
}

/// Report what the sync settled as, once its log has ended: the durable report if
/// the driver mirrored one, else an honest "no report".
async fn settled(
    client: &kube::Client,
    ns: &str,
    id: &str,
    theme: &Theme,
) -> Result<WatchEnd, String> {
    match read_report(client, ns, id).await? {
        Some(report) => {
            println!("{}", report_headline(theme, &report));
            print_report_details(theme, &report);
            Ok(WatchEnd::Finished {
                passed: report.passed(),
            })
        }
        None => {
            println!("sync {id}: tail ended — no report yet (`ztest sync status {id}`)");
            Ok(WatchEnd::Unresolved)
        }
    }
}

/// Merge the driver log, the SUT log, and the pod-phase poll until either the
/// driver's stream ends or the user detaches.
async fn tail_loop(
    api: &Api<Pod>,
    metrics: &mut Poller,
    console: &Console,
    session_start: Instant,
    feed: &mut Feed,
    theme: &Theme,
    render: impl Fn(&Feed, &Reading),
) -> Result<(), String> {
    // The metrics view, held beside the sync feed and folded independently of it.
    let mut metrics_view = Reading::default();
    // The last cause shown, so a standing condition is stated once rather than
    // once a second.
    let mut last_note: Option<String> = None;

    let started = open_driver_log(api, &|| console.cancelled(), |phase| {
        feed.state.pod_phase = phase;
        render(feed, &Reading::default());
    })
    .await?;
    let Some(mut driver) = started else {
        return Ok(());
    };
    // The SUT pod does not exist yet on an early attach — the driver provisions
    // its own topology — so its stream is opened lazily by the poll below. Name
    // and stream are held apart so a line's prefix can be read in a `select!`
    // handler that may also be replacing the stream.
    let mut sut: Option<LineStream> = None;
    let mut sut_name = String::new();
    // The pod last followed, so a reopened SUT stream can tell a resumed follow of
    // the same pod from a fresh one that replaced it.
    let mut sut_prev: Option<String> = None;
    // When each stream last yielded, which is where a resumed one must pick up.
    let (mut driver_seen, mut sut_seen) = (Instant::now(), Instant::now());
    let mut ticker = tokio::time::interval(POD_POLL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if console.cancelled() {
            break;
        }
        // Set by the stream branches; applied after the `select!` so no handler
        // mutates what another branch is borrowing.
        let (mut close_sut, mut lost_driver) = (false, false);
        let mut next_reading: Option<Reading> = None;
        tokio::select! {
            line = driver.next() => match line {
                Some(Ok(l)) => {
                    driver_seen = Instant::now();
                    if let Some(text) = feed.absorb(&l, session_start.elapsed(), theme) {
                        console.scrollback(prefixed("driver", &text, theme));
                    }
                    render(feed, &metrics_view);
                }
                Some(Err(_)) | None => lost_driver = true,
            },
            line = next_line(&mut sut) => match line {
                Some(Ok(l)) => {
                    sut_seen = Instant::now();
                    console.scrollback(prefixed(&sut_name, &l, theme));
                }
                // The SUT's stream ending is not the run ending (a restarted pod,
                // or one still being replaced): drop it and let the poll reopen.
                Some(Err(_)) | None => close_sut = true,
            },
            reading = metrics.changed() => {
                // Applied after the `select!` for the same reason the flags above
                // are: `metrics` is borrowed by this arm's future, so the render
                // that consumes it cannot run inside the arm.
                next_reading = Some(reading);
            }
            _ = ticker.tick() => {
                if let Ok(Some(pod)) = api.get_opt(DRIVER_POD).await {
                    let phase = driver_phase(&pod);
                    if phase != feed.state.pod_phase {
                        feed.state.pod_phase = phase;
                        render(feed, &metrics_view);
                    }
                }
                if sut.is_none()
                    && let Some(name) = find_sut(api).await
                {
                    // Same pod as before ⇒ a resumed follow, which replays only the
                    // gap; a different pod is a fresh subject and gets the usual
                    // context tail.
                    let backfill = match sut_prev.as_deref() == Some(name.as_str()) {
                        true => Backfill::Seconds(gap_since(sut_seen)),
                        false => Backfill::Lines(TAIL_LINES),
                    };
                    if let Ok(stream) = open_log(api, &name, backfill).await {
                        console.scrollback(prefixed("ztest", &format!("following {name}"), theme));
                        sut_prev = Some(name.clone());
                        sut_name = name;
                        sut = Some(stream);
                        sut_seen = Instant::now();
                    }
                }
            }
        }
        if let Some(reading) = next_reading {
            // The panel names the cause itself; scrollback carries it too, once,
            // so a condition that later clears still left a trace in the log.
            if reading.note != last_note
                && let Some(note) = &reading.note
            {
                console.scrollback(prefixed("ztest", &format!("live metrics: {note}"), theme));
            }
            last_note = reading.note.clone();
            metrics_view = reading;
            render(feed, &metrics_view);
        }
        if close_sut {
            sut = None;
        }
        if lost_driver {
            match reattach_driver(api, driver_seen).await? {
                Some(stream) => {
                    driver = stream;
                    console.scrollback(prefixed("ztest", "reattached to the driver log", theme));
                }
                None => break,
            }
        }
    }
    Ok(())
}

/// Reopen the driver log after its stream ended but the run did not.
///
/// A follow-stream over a log that lives for hours is routinely cut — an idle
/// API-server timeout, a proxy hop, a rebalanced connection — and taking that for
/// the end of the run leaves a live sync unwatched with no way back short of
/// re-running the command. `None` means the driver really has finished.
async fn reattach_driver(api: &Api<Pod>, last_seen: Instant) -> Result<Option<LineStream>, String> {
    // A stream that fails immediately would otherwise spin here; this also gives
    // a driver mid-exit time to write its last lines.
    tokio::time::sleep(POD_POLL).await;
    let Some(pod) = api
        .get_opt(DRIVER_POD)
        .await
        .map_err(|e| format!("read driver pod: {e}"))?
    else {
        return Ok(None);
    };
    if !running(&pod, DRIVER_CONTAINER) {
        return Ok(None);
    }
    open_log(api, DRIVER_POD, Backfill::Seconds(gap_since(last_seen)))
        .await
        .map(Some)
        .map_err(|e| format!("reattach to sync log: {e}"))
}

/// The window a resumed stream must replay to leave no gap: everything since the
/// last line read, rounded up, since the API's resolution is one second.
fn gap_since(last_seen: Instant) -> i64 {
    last_seen.elapsed().as_secs().saturating_add(1) as i64
}

/// Wait for the driver container to start, then open a follow-stream over its log.
///
/// The kube API answers a log request for a container that has not started with a
/// 400, and `--watch` attaches the instant the pod is created — an image pull
/// holds it in `ContainerCreating` for minutes. Gating on the container's own
/// state rather than retrying the error means the wait also *reports* itself:
/// `observe` receives every phase change, so `ImagePullBackOff` shows on the panel
/// instead of looking like a hang.
///
/// `Ok(None)` means the caller asked to stop waiting.
async fn open_driver_log(
    api: &Api<Pod>,
    stop: &dyn Fn() -> bool,
    mut observe: impl FnMut(String),
) -> Result<Option<LineStream>, String> {
    loop {
        if stop() {
            return Ok(None);
        }
        let pod = api
            .get_opt(DRIVER_POD)
            .await
            .map_err(|e| format!("read driver pod: {e}"))?
            .ok_or_else(|| format!("driver pod {DRIVER_POD} no longer exists"))?;
        observe(driver_phase(&pod));
        if logs_available(&pod, DRIVER_CONTAINER) {
            return open_log(api, DRIVER_POD, Backfill::Lines(TAIL_LINES))
                .await
                .map(Some)
                .map_err(|e| format!("stream sync log: {e}"));
        }
        tokio::time::sleep(POD_POLL).await;
    }
}

/// Whether `container`'s log can be read: the kubelet keeps one only once the
/// container has started, and keeps it after it exits.
fn logs_available(pod: &Pod, container: &str) -> bool {
    container_state(pod, container).is_some_and(|s| s.running.is_some() || s.terminated.is_some())
}

/// Whether `container` is still executing — the question that separates a dropped
/// connection from a finished run.
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

/// The driver's phase, refined by the container's waiting reason when it has one:
/// a bare `Pending` cannot distinguish scheduling from `ImagePullBackOff`.
fn driver_phase(pod: &Pod) -> String {
    let phase = row_of(pod).2;
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

/// How much history a freshly-opened log stream replays.
#[derive(Debug, Clone, Copy)]
enum Backfill {
    /// The last N lines — a first attach, which wants context.
    Lines(i64),
    /// Everything from N seconds ago — a resumed attach, which must not leave a
    /// gap and so accepts a small overlap instead. Driver events carry a sequence
    /// and are de-duplicated on the fold; a repeated component log line is only
    /// cosmetic, and preferable to a lost one.
    Seconds(i64),
}

/// Open a follow-stream over one pod's log.
async fn open_log(
    api: &Api<Pod>,
    pod: &str,
    backfill: Backfill,
) -> Result<LineStream, kube::Error> {
    let mut lp = LogParams {
        follow: true,
        ..Default::default()
    };
    match backfill {
        Backfill::Lines(n) => lp.tail_lines = Some(n),
        Backfill::Seconds(s) => lp.since_seconds = Some(s),
    }
    Ok(Box::pin(api.log_stream(pod, &lp).await?.lines()))
}

/// The name of the running indexer pod. `None` while the driver has yet to
/// provision one — the caller retries.
async fn find_sut(api: &Api<Pod>) -> Option<String> {
    let pods = api
        .list(&ListParams::default().labels(SUT_SELECTOR))
        .await
        .ok()?;
    pods.items
        .into_iter()
        .find(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"))?
        .metadata
        .name
}

/// Await the next line of an optional stream. A `None` stream parks forever, so
/// `select!` simply ignores that branch until one is opened.
async fn next_line(stream: &mut Option<LineStream>) -> Option<std::io::Result<String>> {
    match stream {
        Some(s) => s.next().await,
        None => std::future::pending().await,
    }
}

/// Tag a line with the pod it came from, so a merged stream stays attributable.
fn prefixed(source: &str, line: &str, theme: &Theme) -> String {
    format!(
        "{:>8} {} {}\n",
        source.style(theme.styles.dim),
        theme.chars.vbar.style(theme.styles.dim),
        line
    )
}

/// A plain, linear follow-tail of the driver log (non-TTY, or when the terminal
/// console can't start). No panel, so every event — ticks included — is rendered
/// as a line: a piped `watch` is a progress log.
async fn plain_tail(api: &Api<Pod>, theme: &Theme) -> Result<(), String> {
    let mut lines = open_driver_log(api, &|| false, |phase| {
        println!("ztest sync: driver {phase}");
    })
    .await?
    .expect("only a cancelling caller gets None");
    let mut feed = Feed::new(String::new(), String::new(), String::new(), String::new());
    let started = Instant::now();
    let mut seen = Instant::now();
    // Reattaches on a dropped connection exactly as the panel path does: a piped
    // `--watch` in CI follows the same hours-long log.
    loop {
        while let Some(line) = lines.next().await {
            let line = line.map_err(|e| format!("read sync log: {e}"))?;
            seen = Instant::now();
            if let Some(text) = feed.absorb_verbose(&line, started.elapsed(), theme) {
                println!("{text}");
            }
        }
        match reattach_driver(api, seen).await? {
            Some(stream) => {
                lines = stream;
                println!("ztest sync: reattached to the driver log");
            }
            None => return Ok(()),
        }
    }
}

/// The newest live progress a *one-shot* command can recover, by folding the
/// driver log's tail rather than following it. This is what makes `ztest sync
/// status` truthful mid-run: the durable report only exists once the run ends, so
/// without this the only answer between start and finish is the pod phase.
///
/// `None` when no tick has been published yet (still provisioning), or the log is
/// unreadable.
pub(super) async fn latest_progress(api: &Api<Pod>) -> Option<SyncWatchState> {
    let lp = LogParams {
        tail_lines: Some(STATUS_TAIL_LINES),
        ..Default::default()
    };
    let logs = api.logs(DRIVER_POD, &lp).await.ok()?;
    let theme = Theme::for_capabilities(false, true);
    let mut feed = Feed::new(String::new(), String::new(), String::new(), String::new());
    for line in logs.lines() {
        feed.absorb(line, Duration::ZERO, &theme);
    }
    feed.state.vitals.is_some().then_some(feed.state)
}

/// Log tail `status` folds. Deeper than [`TAIL_LINES`] because two events land per
/// tick and the tail must still contain one after a burst of component logging.
const STATUS_TAIL_LINES: i64 = 400;

// ─────────────────────────── event folding ────────────────────────────

/// Folds the driver's event stream into the panel's view model, keeping the
/// derived quantities (scan rate, ETA) that no single event carries.
pub(super) struct Feed {
    pub(super) state: SyncWatchState,
    rate: RateMeter,
    /// Sequence of the newest event folded. A stream resumed after a dropped
    /// connection replays by time, so it can hand back events already counted;
    /// this is what keeps the fold exactly-once across that overlap.
    folded_through: Option<u64>,
}

impl Feed {
    fn new(profile: String, sync_id: String, context: String, pod_phase: String) -> Feed {
        Feed {
            state: SyncWatchState {
                profile,
                sync_id,
                context,
                pod_phase,
                ..Default::default()
            },
            rate: RateMeter::default(),
            folded_through: None,
        }
    }

    /// Take one driver log line. Returns the text to commit to scrollback, or
    /// `None` when the line was an event that belongs only in the panel — or one
    /// this feed has already folded.
    fn absorb(&mut self, line: &str, elapsed: Duration, theme: &Theme) -> Option<String> {
        self.fold(line, elapsed, theme, false)
    }

    /// As [`absorb`](Self::absorb), for the panel-less path: with nowhere to pin
    /// live state, per-tick progress has to be rendered as ordinary lines.
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

    /// Whether this event has been folded before, as it has after a resumed
    /// stream replays it. An unnumbered event (an older driver) cannot be judged,
    /// so it is folded — losing an observation is worse than repeating one.
    fn already_folded(&self, n: Option<u64>) -> bool {
        match (n, self.folded_through) {
            (Some(n), Some(through)) => n <= through,
            _ => false,
        }
    }

    /// Fold one event into the view model, returning any line worth keeping in
    /// history. `verbose` (the panel-less path) also renders per-tick progress,
    /// which would otherwise be redundant with the panel.
    fn record(
        &mut self,
        event: &SyncEvent,
        elapsed: Duration,
        theme: &Theme,
        verbose: bool,
    ) -> Option<String> {
        match event {
            SyncEvent::Setup {
                phase,
                detail,
                component,
            } => {
                let subject = component.clone().unwrap_or_else(|| phase.clone());
                self.state.setup = Some(SetupStep {
                    subject: subject.clone(),
                    detail: detail.clone(),
                    received_at: elapsed,
                });
                // Panel-only on the panel path: the driver's own `provisioning
                // component` log lines already carry this through to scrollback,
                // and repeating them as ztest lines would double every step.
                verbose.then(|| format!("setup · {subject} · {detail}"))
            }
            SyncEvent::Started {
                profile,
                sync_id,
                tick_ms,
                probes,
            } => {
                if self.state.profile.is_empty() {
                    self.state.profile = profile.clone();
                    self.state.sync_id = sync_id.clone();
                }
                Some(format!(
                    "sync engine started · {probes} probes · {}s tick",
                    tick_ms / 1000
                ))
            }
            SyncEvent::Tick(t) => {
                // Rate is measured on the *driver's* clock, not arrival time: the
                // 200-line backfill on attach arrives in milliseconds, which as a
                // wall-clock delta would read as a preposterous scan rate.
                let rate = self
                    .rate
                    .sample(Duration::from_millis(t.elapsed_ms), t.height);
                self.state.vitals = Some(SyncVitals {
                    height: t.height,
                    target: t.target,
                    pct: t.pct,
                    phase: t.phase.clone(),
                    reorg_depth: t.reorg_depth,
                    blocks_per_sec: rate,
                    eta: eta(t.height, t.target, rate),
                    work_rate: t.rate.total(),
                    pool_rates: t.rate.channels().to_vec(),
                    received_at: elapsed,
                });
                verbose.then(|| {
                    let of = t.target.map(|t| format!("/{t}")).unwrap_or_default();
                    format!(
                        "tick {} · height {}{of} · {:.1}% · {}",
                        t.seq, t.height, t.pct, t.phase
                    )
                })
            }
            // Replaced wholesale rather than merged: the driver owns the
            // bucketing, and a controller stitching two publications together
            // would have to re-derive the coarsening it deliberately does not
            // own. A later publication is always the more complete one.
            SyncEvent::Series { timeline } => {
                self.state.timeline = Some(timeline.clone());
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
            SyncEvent::Violation {
                probe,
                height,
                detail,
            } => {
                self.state.violations += 1;
                let at = height.map(|h| format!(" at {h}")).unwrap_or_default();
                Some(format!(
                    "{} {}{at}: {detail}",
                    theme.chars.fail.style(theme.styles.fail),
                    probe.style(theme.styles.fail),
                ))
            }
            SyncEvent::Finished {
                verdict,
                violations,
                coverage_gaps,
                ticks,
            } => Some(format!(
                "sync finished · {verdict} · {ticks} ticks · {violations} violation(s) · \
                 {coverage_gaps} coverage gap(s)"
            )),
            SyncEvent::Unknown => None,
        }
    }
}

/// Projected time to `target` at `rate`. `None` without both, or when the rate is
/// too near zero to project honestly rather than as a wild number.
fn eta(height: u32, target: Option<u32>, rate: Option<f64>) -> Option<Duration> {
    let (target, rate) = (target?, rate?);
    let remaining = target.saturating_sub(height);
    (rate > 0.05 && remaining > 0).then(|| Duration::from_secs_f64(remaining as f64 / rate))
}

/// A smoothed blocks-per-second meter over successive ticks.
///
/// Exponentially smoothed rather than a raw per-tick delta: scanning is bursty
/// (batch boundaries, tree completions), and a rate that swings by 10× between
/// frames is unreadable and makes the ETA jump.
#[derive(Debug, Default)]
struct RateMeter {
    prev: Option<(Duration, u32)>,
    ema: Option<f64>,
}

/// Weight of the newest sample. Low enough to ride out one slow batch, high
/// enough to react within a few ticks when the pace genuinely changes.
const RATE_ALPHA: f64 = 0.3;

impl RateMeter {
    fn sample(&mut self, at: Duration, height: u32) -> Option<f64> {
        if let Some((prev_at, prev_height)) = self.prev {
            let dt = at.saturating_sub(prev_at).as_secs_f64();
            // A reorg (height going backwards) is not a negative scan rate, and a
            // zero interval is not an infinite one: hold the previous estimate.
            if dt > 0.0 && height >= prev_height {
                let instant = (height - prev_height) as f64 / dt;
                self.ema = Some(match self.ema {
                    Some(prev) => prev * (1.0 - RATE_ALPHA) + instant * RATE_ALPHA,
                    None => instant,
                });
            }
        }
        self.prev = Some((at, height));
        self.ema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::Op;
    use crate::sync::ProbeState;

    fn feed() -> Feed {
        Feed::new(
            "zaino_state_sync".into(),
            "zaino-state-sync-a52f9ec9".into(),
            "crc".into(),
            "Running".into(),
        )
    }

    fn theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn tick_event(seq: u64, height: u32) -> SyncEvent {
        tick_at(seq, height, 0)
    }

    /// A tick published `elapsed_ms` into the driver's run.
    fn tick_at(seq: u64, height: u32, elapsed_ms: u64) -> SyncEvent {
        SyncEvent::Tick(crate::sync::SyncTick {
            seq,
            elapsed_ms,
            height,
            target: Some(1024),
            pct: 0.0,
            phase: "Historic".into(),
            reorg_depth: 0,
            rate: Default::default(),
        })
    }

    /// A tick carrying measured work, for the rows that have to distinguish an
    /// unmeasured pool from an idle one.
    fn tick_with_work(seq: u64, sapling: f64, orchard: f64) -> SyncEvent {
        let SyncEvent::Tick(mut t) = tick_at(seq, 100, seq * 1000) else {
            unreachable!("tick_at builds a tick")
        };
        // Counted over ten seconds so a fractional rate survives the integer
        // counts a `Work` holds.
        let window = std::time::Duration::from_secs(10);
        let mut work = crate::sync::Work::ZERO;
        work.set(Op::SaplingOutput, (sapling * 10.0) as u64)
            .set(Op::OrchardAction, (orchard * 10.0) as u64);
        t.rate = work.rate(window);
        SyncEvent::Tick(t)
    }

    /// The distinction the work map exists to carry, all the way from the wire
    /// to the panel's view model: Sapling and Orchard were counted, Ironwood
    /// was counted and is idle, and the tier-B pools were never measured at all.
    #[test]
    fn an_unmeasured_pool_reaches_the_panel_as_absent_not_zero() {
        let mut f = feed();
        f.absorb(
            &crate::sync::encode_event(&tick_with_work(1, 19.4, 4.2)),
            Duration::ZERO,
            &theme(),
        );
        let vitals = f.state.vitals.expect("a tick landed");
        let rate = |name: &str| {
            vitals
                .pool_rates
                .iter()
                .find(|(n, _)| *n == name)
                .and_then(|(_, r)| *r)
        };
        assert_eq!(rate("sapling"), Some(19.4));
        assert_eq!(rate("orchard"), Some(4.2));
        assert_eq!(rate("transparent"), None, "tier B was never counted");
        assert_eq!(rate("sprout"), None);
        let total = vitals.work_rate.expect("a measured total");
        assert!((total - 23.6).abs() < 1e-9, "{total}");
    }

    /// A driver that never published a series leaves no graph, and the panel
    /// has to cope rather than assume one is always there.
    #[test]
    fn a_series_event_becomes_the_panels_timeline() {
        let mut f = feed();
        assert!(f.state.timeline.is_none());

        let mut timeline = crate::sync::Timeline::new(["work"], Duration::from_secs(5));
        timeline.push(Duration::ZERO, &[Some(100.0)]);
        f.absorb(
            &crate::sync::encode_event(&SyncEvent::Series {
                timeline: timeline.clone(),
            }),
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
        let line = crate::sync::encode_event(&tick_event(0, 100));
        assert_eq!(f.absorb(line.trim_end(), Duration::ZERO, &theme), None);
        assert_eq!(f.state.vitals.as_ref().map(|v| v.height), Some(100));
    }

    #[test]
    fn rate_and_eta_appear_once_two_ticks_have_landed() {
        let mut f = feed();
        let theme = theme();

        f.record(&tick_at(0, 100, 0), Duration::ZERO, &theme, false);
        assert!(
            f.state.vitals.as_ref().unwrap().blocks_per_sec.is_none(),
            "one tick cannot establish a rate"
        );

        f.record(&tick_at(1, 150, 5_000), Duration::ZERO, &theme, false);
        let v = f.state.vitals.as_ref().unwrap();
        assert_eq!(v.blocks_per_sec, Some(10.0), "50 blocks in 5s");
        // 874 blocks left at 10 blk/s.
        assert_eq!(v.eta, Some(Duration::from_secs_f64(87.4)));
    }

    /// Backfilled lines all arrive at once, so a rate measured on arrival time
    /// would be absurd; the driver's own tick clock is the only sound basis.
    #[test]
    fn rate_uses_the_drivers_clock_not_the_watchers() {
        let mut f = feed();
        let theme = theme();
        // Two ticks 10s apart on the driver, delivered in the same instant here.
        f.record(&tick_at(0, 100, 0), Duration::ZERO, &theme, false);
        f.record(&tick_at(1, 200, 10_000), Duration::ZERO, &theme, false);
        assert_eq!(
            f.state.vitals.as_ref().unwrap().blocks_per_sec,
            Some(10.0),
            "100 blocks over the driver's 10s"
        );
    }

    #[test]
    fn a_reorg_does_not_produce_a_negative_rate() {
        let mut f = feed();
        let theme = theme();
        f.record(&tick_at(0, 100, 0), Duration::ZERO, &theme, false);
        f.record(&tick_at(1, 200, 5_000), Duration::ZERO, &theme, false);
        f.record(&tick_at(2, 150, 10_000), Duration::ZERO, &theme, false);
        let rate = f.state.vitals.as_ref().unwrap().blocks_per_sec.unwrap();
        assert!(rate > 0.0, "rolled-back height yielded rate {rate}");
    }

    #[test]
    fn a_violation_is_counted_and_kept_in_history() {
        let mut f = feed();
        let theme = theme();
        let line = f.record(
            &SyncEvent::Violation {
                probe: "no_stall".into(),
                height: Some(901),
                detail: "liveness stall".into(),
            },
            Duration::ZERO,
            &theme,
            false,
        );
        assert_eq!(f.state.violations, 1);
        let line = line.expect("a violation must reach scrollback");
        assert!(line.contains("no_stall") && line.contains("901"), "{line}");
    }

    #[test]
    fn the_worst_probe_is_the_one_nearest_failing() {
        let mut f = feed();
        f.state.probes = vec![
            ProbeRow {
                name: "height_monotonic".into(),
                state: ProbeState::Ok,
                since_satisfied: None,
                window: None,
            },
            ProbeRow {
                name: "nearly_stalled".into(),
                state: ProbeState::Pending,
                since_satisfied: Some(Duration::from_secs(25)),
                window: Some(Duration::from_secs(30)),
            },
            ProbeRow {
                name: "barely_late".into(),
                state: ProbeState::Pending,
                since_satisfied: Some(Duration::from_secs(6)),
                window: Some(Duration::from_secs(300)),
            },
        ];
        assert_eq!(f.state.probe_tally(), (1, 3));
        assert_eq!(
            f.state.worst_probe().map(|p| p.name.as_str()),
            Some("nearly_stalled"),
        );
    }

    fn driver_pod(phase: &str, state: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": DRIVER_POD },
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

    /// Attaching on the heels of `sync start` finds the container still being
    /// created; asking the kubelet for its log there is the 400 this gate avoids.
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

    /// A finished driver still has the log that holds the whole event stream, so
    /// `watch` on a completed sync must not wait forever for a restart.
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

    /// Provisioning is most of a sync's wall-clock, so the gate the driver is in
    /// has to reach the panel — and each new gate has to replace the last, since a
    /// stale one reads as progress that isn't happening.
    #[test]
    fn a_setup_event_becomes_the_panels_current_gate() {
        let mut f = feed();
        let theme = theme();

        let creating = crate::sync::encode_event(&SyncEvent::Setup {
            phase: "indexer".into(),
            detail: "creating pod".into(),
            component: Some("zainod".into()),
        });
        assert!(
            f.absorb(creating.trim_end(), Duration::from_secs(5), &theme)
                .is_none(),
            "a setup event belongs to the panel, not scrollback"
        );
        let step = f.state.setup.as_ref().expect("a setup step");
        assert_eq!((&*step.subject, &*step.detail), ("zainod", "creating pod"));
        assert_eq!(step.received_at, Duration::from_secs(5));

        let waiting = crate::sync::encode_event(&SyncEvent::Setup {
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

    /// A resumed stream replays by time, so the overlap is delivered twice. The
    /// panel must not count those events twice — a doubled violation tally is a
    /// wrong answer, not a cosmetic repeat.
    #[test]
    fn a_replayed_event_is_folded_only_once() {
        let mut f = feed();
        let theme = theme();
        let violation = crate::sync::encode_event(&SyncEvent::Violation {
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

    /// Sequences only order events from one driver; an ordinary log line carries
    /// none and must always pass through, however many arrive.
    #[test]
    fn ordinary_log_lines_are_never_deduplicated() {
        let mut f = feed();
        let theme = theme();
        f.absorb(
            crate::sync::encode_event(&tick_event(0, 100)).trim_end(),
            Duration::ZERO,
            &theme,
        );
        for _ in 0..3 {
            assert_eq!(
                f.absorb("INFO zebrad: committed block", Duration::ZERO, &theme),
                Some("INFO zebrad: committed block".to_string()),
            );
        }
    }

    /// An event newer than everything folded so far is still folded after a
    /// replay: the guard is a watermark, not a one-shot.
    #[test]
    fn folding_resumes_past_the_replayed_overlap() {
        let mut f = feed();
        let theme = theme();
        let lines: Vec<String> = (0..3)
            .map(|i| crate::sync::encode_event(&tick_at(i, 100 + i as u32 * 50, i * 5_000)))
            .collect();

        for line in &lines {
            f.absorb(line.trim_end(), Duration::ZERO, &theme);
        }
        // The stream drops and resumes one second back, replaying the last tick
        // before delivering the next.
        f.absorb(lines[2].trim_end(), Duration::ZERO, &theme);
        let resumed = crate::sync::encode_event(&tick_at(3, 300, 15_000));
        f.absorb(resumed.trim_end(), Duration::ZERO, &theme);

        assert_eq!(f.state.vitals.as_ref().map(|v| v.height), Some(300));
    }

    #[test]
    fn a_gap_window_always_covers_at_least_one_second() {
        assert!(gap_since(Instant::now()) >= 1, "kube rejects a zero window");
    }

    /// The tail loop must be able to tell a dropped connection (reattach) from a
    /// finished run (stop), which is exactly this distinction.
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

    #[test]
    fn every_probe_satisfied_leaves_no_worst() {
        let mut f = feed();
        f.state.probes = vec![ProbeRow {
            name: "ok".into(),
            state: ProbeState::Ok,
            since_satisfied: None,
            window: None,
        }];
        assert!(f.state.worst_probe().is_none());
    }
}
