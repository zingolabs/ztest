//! `ztest sync perf` — query a sync's CPU profile and open it.
//!
//! - Components push to Pyroscope; this asks for a merged flamegraph over a window and
//!   folds it to collapsed stacks (`frame;frame <self>`), which every viewer here reads
//! - Store sits outside the run → readable mid-sync and after the namespace is gone
//! - Untransformed: Pyroscope's bytes for the window are the file (no repair, no frame
//!   filtering, no local diff — a viewer's job, and no external toolchain to hold)
//! - Strictly a reader: never drains, stops, or signals a component (a diagnostic
//!   that can end a twelve-hour sync is a footgun)

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

use owo_colors::OwoColorize as _;

use crate::sync::namespace_for;
use crate::ui::Theme;

/// Viewer override, honoured ahead of [`VIEWERS`] (taste + what a machine has
/// installed are not ztest's call). Program name or path; profile appended last
const VIEWER_ENV: &str = "ZTEST_PROFILE_VIEWER";

/// Tried in order absent [`VIEWER_ENV`]. Terminal-first — usually run beside
/// `ztest sync watch` over SSH, where a browser is impossible or lands elsewhere
const VIEWERS: [&str; 1] = ["flameshow"];

/// One `ztest sync perf` invocation, as asked for on the command line.
///
/// Mutually-constraining choices, not independent inputs: `window` xor `base`. Named
/// fields also stop four `Option<String>`/`bool` arguments transposing silently
pub(crate) struct Request {
    pub(crate) id: String,
    pub(crate) out: Option<PathBuf>,
    pub(crate) open: bool,
    pub(crate) window: Option<String>,
    pub(crate) component: Option<String>,
    pub(crate) base: Option<String>,
    pub(crate) profile: super::Profile,
}

pub(crate) async fn perf(request: Request) -> Result<(), String> {
    let Request { id, out, open, window, component, base, profile } = request;
    // Parsed before any cluster call — a `--window` typo must cost nothing, not a
    // full retrieval then thrown away.
    let requested = window.as_deref().map(parse_window).transpose()?;
    if requested.is_some() && base.is_some() {
        return Err("`--window` and `--base` are different subtractions — pick one".to_string());
    }
    // Never guessed: profiles key by component, and merged samples from two
    // processes are meaningless, not merely coarse.
    let subject = component.ok_or_else(|| {
        "name the component to profile with `--component` (e.g. `--component zainod`)".to_string()
    })?;
    let span = span_for(&id, requested).await?;

    if let Some(base_id) = base {
        return compare(&id, &base_id, &subject, span, open, profile).await;
    }

    let target = retrieve(&id, &subject, span, out, profile).await?;
    match open {
        true => launch(&target, &Theme::detect()),
        false => Ok(()),
    }
}

/// Requested window → absolute times.
///
/// - Bounds = elapsed since start ("hour eleven", how a forty-hour run is read)
/// - Origin = driver pod creation, so this answers mid-run too; no window = whole run
async fn span_for(
    id: &str,
    requested: Option<(std::time::Duration, std::time::Duration)>,
) -> Result<(SystemTime, SystemTime), String> {
    let client = crate::cluster::client().await.map_err(|e| format!("kube client: {e}"))?;
    let driver = crate::sync::find_driver(&client, id).await?;
    let started: SystemTime = driver
        .metadata
        .creation_timestamp
        .ok_or_else(|| format!("sync {id}: driver pod has no creation timestamp"))?
        .0
        .into();
    Ok(match requested {
        Some((from, to)) => (started + from, started + to),
        None => (started, SystemTime::now()),
    })
}

/// Query one component's merged profile over `window`, write to `out`
async fn retrieve(
    id: &str,
    component: &str,
    window: (SystemTime, SystemTime),
    out: Option<PathBuf>,
    profile: super::Profile,
) -> Result<PathBuf, String> {
    let theme = Theme::detect();
    let client = crate::cluster::client().await.map_err(|e| format!("kube client: {e}"))?;
    let selector = super::selector(component, &namespace_for(id));
    let tenant = super::tenant_for_sync(&client, id)
        .await
        .ok_or_else(|| format!("sync {id} is gone; its profiles are no longer addressable"))?;
    let bytes = match super::fetch(&client, &selector, window.0, window.1, &tenant, profile).await {
        Ok(profile) => profile,
        // An empty match has four causes and the cluster can still separate them; a bare
        // "matched nothing" sends the reader to the query, which is the one thing that is fine
        Err(e) => return Err(explain_empty(&client, id, component).await.unwrap_or(e)),
    };

    let dest = out.unwrap_or_else(|| default_dest(id));
    std::fs::create_dir_all(&dest).map_err(|e| format!("create {}: {e}", dest.display()))?;
    let path = dest.join(format!("{component}-{}.collapsed", profile.stem()));
    std::fs::write(&path, &bytes).map_err(|e| format!("write {}: {e}", path.display()))?;
    println!("{} {}", theme.chars.ok.style(theme.styles.pass), path.display());
    match profile {
        super::Profile::OnCpu => {
            report_fidelity(&client, id, component, &bytes, window, &theme).await
        }
        // Blocked time has no CPU denominator to be a fraction of; the dropped-event count is
        // the only fidelity signal that still means something
        super::Profile::OffCpu => report_dropped_events(&client, id, &theme).await,
    }
    Ok(path)
}

/// Alloy's own count of trace events the per-CPU perf ring could not hold.
///
/// - Non-zero = the sampling settings outrun the ring, and the profile is missing
///   *unattributed* work — invisible in the fidelity ratio, which only compares what
///   arrived against the kernel's CPU figure
/// - Silent on zero and on any failure (a footnote must not fail a profile that retrieved)
async fn report_dropped_events(client: &kube::Client, id: &str, theme: &Theme) {
    let Some(dropped) = collector_dropped(client, id).await.filter(|&d| d > 0) else {
        return;
    };
    println!(
        "{:>10}  {} trace events  {}",
        "dropped".style(theme.styles.dim),
        crate::ui::text::compact(dropped as f64).style(theme.styles.count),
        "lower --profile-off-cpu or --profile-hz".style(theme.styles.fail),
    );
}

async fn collector_dropped(client: &kube::Client, id: &str) -> Option<u64> {
    collector_metrics(client, id).await?.get("agent_errors_trace_event_lost_total").copied()
}

/// Sidecar's own `/metrics`, as `name -> value` (labels dropped; every counter read here
/// is process-wide and single-series)
async fn collector_metrics(
    client: &kube::Client,
    id: &str,
) -> Option<std::collections::HashMap<String, u64>> {
    let port = super::ebpf::HTTP_PORT;
    // Host-placed collectors listen on loopback already; only a sidecar needs a tunnel, and
    // starting one against a finished driver pod fails rather than falling through
    let (_fwd, local) = match super::host::metrics_port(id).await {
        // Host-placed: docker owns the mapping, so the port is looked up, never assumed
        Some(published) => (None, published),
        None => {
            let fwd = crate::portforward::Forwarder::start(
                client.clone(),
                crate::resource::impls::policy::RUN_NAMESPACE.to_string(),
                crate::sync::driver_pod_for(id),
                port,
            )
            .await
            .ok()?;
            let local = fwd.local_port;
            (Some(fwd), local)
        }
    };
    let body = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{local}/metrics"))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    // Summed, not collected: Alloy labels each family by `component_id`, so collecting would
    // keep one arbitrary series per family rather than the collector's total
    Some(
        body.lines()
            .filter(|l| !l.starts_with('#'))
            .filter_map(|l| {
                let (name, value) = l.rsplit_once(' ')?;
                let name = name.split_once('{').map_or(name, |(n, _)| n);
                Some((name.trim().to_string(), value.trim().parse::<f64>().ok()? as u64))
            })
            .fold(std::collections::HashMap::new(), |mut acc, (name, value)| {
                *acc.entry(name).or_default() += value;
                acc
            }),
    )
}

/// Collector's own pipeline counters, in order of flow.
///
/// - Facts, not a verdict: an empty profile has several causes, and where the numbers stop
///   separates them without this having to guess a threshold
/// - `unwound` = executables whose stack deltas reached eBPF; zero (or far under the
///   process count) means no stack can be walked, so nothing downstream can exist
async fn collector_pipeline(client: &kube::Client, id: &str) -> Option<String> {
    let metrics = collector_metrics(client, id).await?;
    let get = |k: &str| metrics.get(k).copied().unwrap_or_default();
    Some(format!(
        "collector: {} targets · {} processes seen · {} executables unwound · \
         {} samples forwarded · {} events dropped",
        get("pyroscope_ebpf_active_targets"),
        get("bpf_num_proc_new_total"),
        get("agent_num_exe_id_loaded_to_ebpf"),
        get("pyroscope_forwarded_entries_total"),
        get("agent_errors_trace_event_lost_total"),
    ))
}

/// Why the query matched nothing, when the cluster can still say. `None` = no structural
/// cause found, so the caller's "matched nothing in this window" stands.
///
/// Ordered by distance from a profile: never collected → collected on another node →
/// collector never came up
async fn explain_empty(client: &kube::Client, id: &str, component: &str) -> Option<String> {
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::Api;

    let run_ns = crate::resource::impls::policy::RUN_NAMESPACE;
    let driver: Pod =
        Api::namespaced(client.clone(), run_ns).get(&crate::sync::driver_pod_for(id)).await.ok()?;
    let spec = driver.spec.as_ref()?;

    // Two placements, two places to look: a sidecar lands in `initContainers` (native
    // sidecar, never `containers`), a host collector in docker. Neither = never profiled,
    // the one cause a re-query can never fix
    let has_sidecar =
        spec.init_containers.iter().flatten().any(|c| c.name == super::ebpf::CONTAINER);
    let has_host = super::host::metrics_port(id).await.is_some();
    if !has_sidecar && !has_host {
        return Some(format!(
            "sync {id} carries no profiler; nothing was ever collected — \
             restart it with `ztest sync start <profile>`"
        ));
    }

    let status = driver
        .status
        .as_ref()
        .and_then(|s| s.init_container_statuses.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.name == super::ebpf::CONTAINER));
    if let Some(waiting) = status.and_then(|s| s.state.as_ref()?.waiting.as_ref()) {
        let reason = waiting.reason.as_deref().unwrap_or("not running");
        return Some(format!(
            "sync {id}: the profiler never started ({reason}); \
             `kubectl -n {run_ns} logs {} -c {}`",
            crate::sync::driver_pod_for(id),
            super::ebpf::CONTAINER,
        ));
    }

    // eBPF sees one node; the collector rides the driver, so anything scheduled elsewhere
    // was never sampled
    let node = spec.node_name.as_deref()?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace_for(id));
    let subject = pods.get(component).await.ok()?;
    let subject_node = subject.spec.as_ref()?.node_name.as_deref()?;
    if subject_node != node {
        return Some(format!(
            "{component} ran on node {subject_node}, the profiler on {node} — \
             eBPF only samples its own node"
        ));
    }

    // Nothing structural left to blame → hand over the collector's own counters. Where they
    // stop is the answer, and it beats naming a cause this cannot actually establish
    collector_pipeline(client, id)
        .await
        .map(|counters| format!("no profile matched yet · {counters}"))
}

/// Below this the profile is missing enough CPU that its *shape* is suspect too, not
/// merely its totals. Chosen an order of magnitude above the noise a scrape boundary
/// leaves, and far under the 10× loss `ITIMER_PROF` reaches past the signal ceiling
const FIDELITY_FLOOR: f64 = 0.80;

/// Say what fraction of the kernel's CPU the profile actually caught.
///
/// - Shortfall has several causes and this ratio distinguishes none: a collector that started
///   late, a dropped-event backlog, or unwinding that resolved nothing all read alike
/// - So it points at the pipeline counters rather than prescribing a knob — the in-process path
///   is bounded by the kernel's signal ceiling (lower the rate), the eBPF path by ring headroom
///   (lower `--profile-off-cpu`), and the two pull in opposite directions
/// - Never fatal, never a verdict: a missing TSDB must not fail a profile that opened
async fn report_fidelity(
    client: &kube::Client,
    id: &str,
    component: &str,
    profile: &[u8],
    window: (SystemTime, SystemTime),
    theme: &Theme,
) {
    let (Some(sampled), Some(charged)) = (
        super::collapsed_nanos(profile).map(|ns| ns as f64 / 1e9),
        crate::metrics::query::container_cpu_seconds(client, &namespace_for(id), component, window)
            .await,
    ) else {
        return;
    };
    if charged <= 0.0 {
        return;
    }

    let ratio = sampled / charged;
    let style = match ratio < FIDELITY_FLOOR {
        true => theme.styles.fail,
        false => theme.styles.pass,
    };
    println!(
        "{:>10}  {} of {} cpu-seconds sampled  {}",
        "fidelity".style(theme.styles.dim),
        crate::ui::text::compact(sampled).style(theme.styles.count),
        crate::ui::text::compact(charged).style(theme.styles.dim),
        format!("{:.0}%", ratio * 100.0).style(style),
    );
    report_dropped_events(client, id, theme).await;
    if ratio < FIDELITY_FLOOR {
        eprintln!(
            "ztest sync perf: under-sampled — the collector counters above name the shortfall"
        );
    }
}

/// Compare against an earlier run: what changed (headline), then both profiles to see
/// where — the two halves of an optimisation loop, useless apart.
///
/// - Exactly one number to compare: over a shared span the work vector is a chain
///   constant, so `rate = work/elapsed` varies only through `elapsed` and every op's
///   ratio is that same ratio (a per-op table = one number in seven hats)
/// - Per-op *attribution* comes from the profiler, i.e. the flame graph
/// - Composition printed unpaired: it belongs to the shared span, and explains
///   whether an optimisation landed
async fn compare(
    id: &str,
    base_id: &str,
    component: &str,
    span: (SystemTime, SystemTime),
    open: bool,
    profile: super::Profile,
) -> Result<(), String> {
    let theme = Theme::detect();
    let head = read_report(id).await?;
    let base = read_report(base_id).await?;

    let (Some(head_seg), Some(base_seg)) = (&head.segment, &base.segment) else {
        return Err(format!(
            "sync {id} and {base_id}: no segment on one — give both a `run.until_height(..)`"
        ));
    };
    head_seg.comparable_with(base_seg).map_err(|why| {
        format!(
            "sync {id} and {base_id} cannot be compared: {why}\n  {id:>16}  {}\n  {base_id:>16}  {}",
            head_seg.describe(),
            base_seg.describe(),
        )
    })?;

    print!("{}", verdict(head_seg, base_seg, id, base_id, &theme));

    // Second: the table is the cheap half, and reaches a reader even when one run
    // has no profile stored.
    let head_profile = retrieve(id, component, span, None, profile).await?;
    let base_span = span_for(base_id, None).await?;
    let base_profile =
        retrieve(base_id, component, base_span, Some(default_dest(base_id)), profile).await?;

    // Both profiles ship as retrieved; differencing them is a viewer's job, not ztest's
    println!("{:>10}  {}", "base".style(theme.styles.dim), base_profile.display());
    match open {
        true => launch(&head_profile, &theme),
        false => Ok(()),
    }
}

/// Full comparison: shared span, a line per run, and the span's composition
fn verdict(
    head: &crate::sync::Segment,
    base: &crate::sync::Segment,
    head_id: &str,
    base_id: &str,
    theme: &Theme,
) -> String {
    let mut out = String::new();
    // Equal spans ⇒ equal work, so disagreement = untrustworthy measurement, not a
    // slower run. Say so rather than print a difference in what was counted.
    let (agree, note) = match head.work == base.work {
        true => (theme.styles.pass, "identical work"),
        false => (theme.styles.fail, "WORK DISAGREES — see below"),
    };
    let _ = writeln!(
        out,
        "{:>10}  {}  {} {}",
        "segment".style(theme.styles.dim),
        head.describe().style(theme.styles.count),
        theme.chars.ok.style(agree),
        note.style(agree),
    );

    let line =
        |out: &mut String, label: &str, id: &str, seg: &crate::sync::Segment, delta: &str| {
            let row = format!(
                "{:>10}  {:<12}  {:>9}  {:>12}  {:>8}",
                label.style(theme.styles.dim),
                id,
                crate::ui::text::format_elapsed(seg.elapsed()).style(theme.styles.count),
                ops_per_sec(seg).style(theme.styles.count),
                delta,
            );
            let _ = writeln!(out, "{}", row.trim_end());
        };
    line(&mut out, "base", base_id, base, "");
    line(&mut out, "head", head_id, head, &change(head, base, theme).unwrap_or_default());

    let content: Vec<String> = head
        .work
        .composition()
        .iter()
        .filter_map(|(name, share)| share.map(|s| format!("{name} {s:.0}%")))
        .collect();
    let unmeasured: Vec<&str> = head
        .work
        .composition()
        .iter()
        .filter(|(_, share)| share.is_none())
        .map(|(name, _)| *name)
        .collect();
    if !content.is_empty() {
        let mut row = content.join("  ");
        if !unmeasured.is_empty() {
            row = format!("{row}  · {} unmeasured", unmeasured.join(", "));
        }
        let _ = writeln!(
            out,
            "{:>10}  {}",
            "content".style(theme.styles.dim),
            row.style(theme.styles.dim),
        );
    }
    out
}

fn ops_per_sec(seg: &crate::sync::Segment) -> String {
    match seg.rate().total() {
        Some(r) => format!("{}/s", crate::ui::text::compact(r)),
        None => "—".to_string(),
    }
}

/// Head throughput vs base, `None` when either is unmeasured. Flagged below
/// [`REGRESSION_MARGIN`] — marks the eye, gates nothing (real regression detection
/// is change-point detection over a run history, a separate concern)
fn change(
    head: &crate::sync::Segment,
    base: &crate::sync::Segment,
    theme: &Theme,
) -> Option<String> {
    let (h, b) = (head.rate().total()?, base.rate().total()?);
    if b <= 0.0 {
        return None;
    }
    let style = match h < b * REGRESSION_MARGIN {
        true => theme.styles.fail,
        false => theme.styles.pass,
    };
    Some(format!("{:+.1}%", (h - b) / b * 100.0).style(style).to_string())
}

/// Fraction of baseline below which a change is flagged
const REGRESSION_MARGIN: f64 = 0.95;

async fn read_report(id: &str) -> Result<crate::sync::SyncReportMirror, String> {
    let client = crate::cluster::client().await.map_err(|e| format!("kube client: {e}"))?;
    crate::sync::read_report(&client, id)
        .await?
        .ok_or_else(|| format!("sync {id}: no report — it has not finished yet"))
}

/// Landing dir absent `--out`. Per-sync, so two syncs of one profile never
/// overwrite each other's artifacts
fn default_dest(id: &str) -> PathBuf {
    PathBuf::from(format!("ztest-perf-{id}"))
}

/// Parse a `FROM..TO` window: `11h..12h`, `30m..90m`, `0..1h`
fn parse_window(spec: &str) -> Result<(std::time::Duration, std::time::Duration), String> {
    let (from, to) = spec
        .split_once("..")
        .ok_or_else(|| format!("window {spec:?} is not `FROM..TO` (e.g. `11h..12h`)"))?;
    let from = parse_elapsed(from)?;
    let to = parse_elapsed(to)?;
    if from >= to {
        return Err(format!("window {spec:?} does not advance"));
    }
    Ok((from, to))
}

/// Parse an elapsed bound: bare seconds, or `s`/`m`/`h`-suffixed
fn parse_elapsed(text: &str) -> Result<std::time::Duration, String> {
    let text = text.trim();
    let (digits, scale) = match text.chars().last() {
        Some('s') => (&text[..text.len() - 1], 1),
        Some('m') => (&text[..text.len() - 1], 60),
        Some('h') => (&text[..text.len() - 1], 3600),
        _ => (text, 1),
    };
    digits
        .parse::<u64>()
        .map(|n| std::time::Duration::from_secs(n * scale))
        .map_err(|_| format!("{text:?} is not an elapsed time (try `90m` or `2h`)"))
}

/// Hand `profile` to a viewer, else explain how to get one. Missing viewer =
/// guidance, not error (the artifact — what was actually asked for — is on disk)
fn launch(profile: &Path, theme: &Theme) -> Result<(), String> {
    let Some(viewer) = choose_viewer() else {
        eprintln!(
            "ztest sync perf: no viewer — `uv tool install flameshow` or set {VIEWER_ENV} ({})",
            profile.display()
        );
        return Ok(());
    };

    println!(
        "{} {} {}",
        theme.chars.dot.style(theme.styles.dim),
        "opening".style(theme.styles.dim),
        viewer.style(theme.styles.count)
    );
    // Inherited stdio, joined not detached — full-screen viewers must own the tty,
    // and a shell prompt returning mid-paint corrupts both.
    match Command::new(&viewer).arg(profile).status() {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(format!("{viewer} exited with {status} ({})", profile.display())),
        Err(e) => Err(format!("launch {viewer}: {e}")),
    }
}

/// Override if set, else the first known viewer on `PATH`
fn choose_viewer() -> Option<String> {
    if let Some(explicit) = std::env::var_os(VIEWER_ENV).filter(|v| !v.is_empty()) {
        return explicit.into_string().ok();
    }
    VIEWERS.iter().find(|v| on_path(v)).map(|v| (*v).to_string())
}

/// `program` executable on `PATH`? Hand-rolled — `which` is itself not guaranteed present
fn on_path(program: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| dir.join(program).is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Artifacts are named per component → only the directory separates two syncs
    /// of one profile
    #[test]
    fn each_sync_gets_its_own_artifact_directory() {
        assert_ne!(default_dest("zaino-state-sync-aaa"), default_dest("zaino-state-sync-bbb"));
        assert!(
            default_dest("zaino-state-sync-aaa").to_string_lossy().contains("zaino-state-sync-aaa")
        );
    }

    /// Override must win over an installed viewer, else a developer cannot choose
    #[test]
    fn the_env_override_outranks_an_installed_viewer() {
        // SAFETY: single-threaded test, var restored before returning.
        let restore = std::env::var_os(VIEWER_ENV);
        unsafe { std::env::set_var(VIEWER_ENV, "my-viewer") };
        assert_eq!(choose_viewer().as_deref(), Some("my-viewer"));
        unsafe {
            match restore {
                Some(v) => std::env::set_var(VIEWER_ENV, v),
                None => std::env::remove_var(VIEWER_ENV),
            }
        }
    }

    /// Empty override = shell accident (`ZTEST_PROFILE_VIEWER=`), not a nameless program
    #[test]
    fn an_empty_override_falls_through_to_discovery() {
        let restore = std::env::var_os(VIEWER_ENV);
        unsafe { std::env::set_var(VIEWER_ENV, "") };
        assert_ne!(choose_viewer().as_deref(), Some(""));
        unsafe {
            match restore {
                Some(v) => std::env::set_var(VIEWER_ENV, v),
                None => std::env::remove_var(VIEWER_ENV),
            }
        }
    }

    /// Chain span traversed in `secs`. Work belongs to the span, so same heights
    /// ⇒ same vector; only `secs` varies
    fn segment(from: u32, to: u32, secs: u64) -> crate::sync::Segment {
        use crate::sync::{Op, Work};
        let mut work = Work::ZERO;
        work.set(Op::SaplingOutput, 10_000).set(Op::OrchardAction, 5_000);
        crate::sync::Segment {
            network: Some("regtest".into()),
            from,
            to,
            work,
            elapsed_ms: secs * 1000,
        }
    }

    fn plain_theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    /// Shared span ⇒ constant work ⇒ throughput change is the only reportable
    /// difference, and it comes entirely from elapsed time
    #[test]
    fn the_comparison_reports_the_change_in_throughput() {
        let out = verdict(
            &segment(840_000, 855_000, 50),
            &segment(840_000, 855_000, 100),
            "sync-head",
            "sync-base",
            &plain_theme(),
        );
        assert!(out.contains("identical work"), "{out}");
        let head = out.lines().find(|l| l.contains("head")).expect("head line");
        assert!(head.contains("+100.0%"), "{head}");
        assert!(
            out.lines().filter(|l| l.contains('%')).count() <= 2,
            "one throughput change and one composition row, not a per-op table:\n{out}"
        );
    }

    /// Equal spans ⇒ equal work; disagreement = untrustworthy measurement, said aloud
    #[test]
    fn work_that_disagrees_over_a_shared_span_is_called_out() {
        let mut head = segment(840_000, 855_000, 50);
        head.work.set(crate::sync::Op::SaplingOutput, 4);
        let out = verdict(
            &head,
            &segment(840_000, 855_000, 100),
            "sync-head",
            "sync-base",
            &plain_theme(),
        );
        assert!(out.contains("WORK DISAGREES"), "{out}");
    }

    /// Composition = what the shared work vector is for; unmeasured channels are
    /// named, never dropped
    #[test]
    fn the_composition_of_the_span_is_reported_once() {
        let out = verdict(
            &segment(840_000, 855_000, 50),
            &segment(840_000, 855_000, 100),
            "sync-head",
            "sync-base",
            &plain_theme(),
        );
        let row = out.lines().find(|l| l.contains("content")).expect("a content row");
        assert!(row.contains("sapling 67%") && row.contains("orchard 33%"), "{row}");
        assert!(row.contains("transparent, sprout, ironwood unmeasured"), "{row}");
    }

    /// Nothing measured = no throughput to compare; run lines must still render
    #[test]
    fn a_span_with_no_measured_work_reports_no_throughput() {
        let bare = crate::sync::Segment {
            network: Some("regtest".into()),
            from: 0,
            to: 10,
            work: crate::sync::Work::ZERO,
            elapsed_ms: 1000,
        };
        let out = verdict(&bare, &bare.clone(), "sync-head", "sync-base", &plain_theme());
        assert!(out.contains('—'), "{out}");
        assert!(!out.contains('%'), "no percentage without a rate:\n{out}");
    }

    #[test]
    fn a_program_that_does_not_exist_is_not_on_path() {
        assert!(!on_path("ztest-definitely-not-a-real-program"));
    }

    #[test]
    fn window_specs_accept_hours_minutes_and_bare_seconds() {
        assert_eq!(
            parse_window("11h..12h").unwrap(),
            (std::time::Duration::from_secs(39_600), std::time::Duration::from_secs(43_200))
        );
        // Bare bound = seconds, so `0..90` is the first minute and a half.
        assert_eq!(
            parse_window("0..90").unwrap(),
            (std::time::Duration::ZERO, std::time::Duration::from_secs(90))
        );
        assert_eq!(
            parse_window("30m..1h").unwrap(),
            (std::time::Duration::from_secs(1800), std::time::Duration::from_secs(3600))
        );
    }

    /// Non-advancing window = a typo, caught before the cluster call so it costs nothing
    #[test]
    fn a_window_that_does_not_advance_is_rejected() {
        assert!(parse_window("12h..11h").is_err());
        assert!(parse_window("1h..1h").is_err());
        assert!(parse_window("11h").is_err());
        assert!(parse_window("banana..12h").is_err());
    }
}
