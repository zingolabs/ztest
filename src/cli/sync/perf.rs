//! `ztest sync perf` — pull a detached sync's CPU profiles to the laptop and
//! open them.
//!
//! A profile is written inside the cluster, on a PVC, in a namespace that outlives
//! the run. Every step of getting it onto a developer's screen was missing: the
//! driver collected profiles into its *own* ephemeral filesystem and then exited,
//! so the only trace left of a run's profile was a log line naming a path that no
//! longer existed. This command is the retrieval half, plus the one-keystroke
//! viewer hand-off that makes it worth having.
//!
//! Strictly a reader. It never drains, stops, or signals a component — a
//! diagnostic that can end a twelve-hour sync is a footgun, not a convenience.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use owo_colors::OwoColorize as _;

use crate::sync::namespace_for;
use crate::ui::Theme;

/// Overridden viewer, honoured ahead of anything this module knows about.
///
/// A profile viewer is a matter of taste and of what a given machine has
/// installed, and ztest has no business being the authority on either. The value
/// is a program name or path; the profile is appended as its final argument.
const VIEWER_ENV: &str = "ZTEST_PROFILE_VIEWER";

/// Viewers tried in order when [`VIEWER_ENV`] is unset. Terminal-first: this
/// command is usually run in the same terminal as `ztest sync watch`, often over
/// SSH, where spawning a browser is either impossible or lands on the wrong
/// machine.
const VIEWERS: [&str; 1] = ["flameshow"];

/// The pprof artifact, which is the one worth opening: it is the source of truth
/// the SVG is a single rendering of, and the only one every viewer accepts.
const PPROF_ARTIFACT: &str = "profile.pb";

/// Frames elided from the viewed profile: the thread-spawn and tokio-runtime
/// prologue that every sample carries before reaching a single line of
/// application code.
///
/// This is not cosmetic. Measured on a nine-minute `zaino-state-sync` profile,
/// the fourteen-frame prologue — `__clone` → `thread_start` → `call_once` →
/// `__rust_begin_short_backtrace` → `pool::Inner::run` → `Harness::poll` →
/// `Core::poll` → `worker::run` → `enter_runtime` → `Scoped::set` →
/// `Context::run` → `Context::run_task` → `Harness::poll` → `Core::poll` —
/// accounted for 11,182 of 19,131 frame-lines. It is byte-identical on every
/// stack, so it conveys nothing and costs the reader more than half the graph.
///
/// `-hide` is the correct pprof primitive here: it drops matched *frames* while
/// keeping their samples, so the totals and the flame widths are untouched.
/// `-focus`/`-ignore` operate on whole samples and would change the numbers;
/// `-prune_from` cuts *leafward* of its match, which is the opposite end of the
/// stack (verified: `-prune_from=zaino_state` deletes the `reqwest`/`url`
/// callees and keeps the tokio tail).
///
/// The one cost is recorded by [`report_elision`]: a sample whose every frame
/// matches — a parked worker, a blocking-pool poll — has nothing left and is
/// dropped, so pure runtime overhead is under-reported in this view. `--raw`
/// exists for when that is the number under investigation.
const HIDE_SCAFFOLDING: &str = r"^(root$|__clone|<?std::sys|core::ops::function::FnOnce|<?tokio::runtime|<?core::future::poll_fn|<?tracing::instrument)";

/// One invocation of `ztest sync perf`, as asked for on the command line.
///
/// A struct rather than a parameter list because the fields are a *set of
/// mutually-constraining choices*, not independent inputs — `window` and `base`
/// are exclusive, `component` is only meaningful once more than one process was
/// profiled, and `raw` only matters when something is opened. Naming them at the
/// one call site also stops the four `Option<String>`/`bool` arguments from
/// being silently transposable.
pub(super) struct Request {
    pub(super) id: String,
    pub(super) out: Option<PathBuf>,
    pub(super) open: bool,
    pub(super) list: bool,
    pub(super) window: Option<String>,
    pub(super) component: Option<String>,
    pub(super) base: Option<String>,
    pub(super) raw: bool,
}

pub(super) async fn perf(request: Request) -> Result<(), String> {
    let Request {
        id,
        out,
        open,
        list,
        window,
        component,
        base,
        raw,
    } = request;
    // Parsed before a single cluster call: a typo in `--window` should cost
    // nothing, not a full retrieval that is then thrown away.
    let requested = window.as_deref().map(parse_window).transpose()?;
    if requested.is_some() && base.is_some() {
        return Err(
            "`--window` and `--base` are different subtractions and cannot be combined: \
             one cuts a slice out of this run, the other differences this run against \
             another. Pick one."
                .to_string(),
        );
    }
    let collected = retrieve_all(&id, out).await?;

    if list {
        return show_snapshots(&collected);
    }
    // Resolved before anything is opened or subtracted: every operation below is
    // only meaningful within a single process's series.
    let subject = select_component(&collected, component.as_deref())?;

    if let Some(base_id) = base {
        return compare(&id, &base_id, &subject, &collected, open, raw).await;
    }

    let target = match requested {
        Some((from, to)) => cut_window(&collected, &subject, from, to)?,
        // No window asked for: the newest cumulative profile is the whole run,
        // which is the question almost every session opens with.
        None => pick_profile(&collected, &subject).ok_or_else(|| {
            format!("sync {id}: no pprof profile for {subject} among the retrieved artifacts")
        })?,
    };
    match open {
        true => launch(&normalize_for_viewing(&target, raw), &Theme::detect()),
        false => Ok(()),
    }
}

/// Give `profile` a mapping table and strip its runtime scaffolding, returning
/// whatever the viewer should open.
///
/// pprof-rs writes every location with `mapping_id = 0` and an empty mapping
/// table. That is legal — 0 means "unmapped" — and `go tool pprof` renders it
/// happily, but flameshow indexes its mapping table unconditionally and dies on
/// the lookup. Rewriting through the reference implementation supplies the table:
/// it synthesizes one entry and repoints every location at it, leaving the
/// samples untouched.
///
/// The same pass also applies [`HIDE_SCAFFOLDING`] unless `raw` is set — one
/// `go tool pprof` invocation does both, so the trimmed view costs nothing that
/// the mandatory mapping-table repair was not already paying for.
///
/// Best-effort: without Go the original is still worth opening, since `go tool
/// pprof`, speedscope and pprof.me all accept it as-is. A viewer that then fails
/// is explained by [`launch`]. Note that this fallback also means a Go-less
/// machine sees the untrimmed graph — the frames are hidden by the reference
/// implementation, not by ztest.
fn normalize_for_viewing(profile: &Path, raw: bool) -> PathBuf {
    if !on_path("go") {
        return profile.to_path_buf();
    }
    let out = profile.with_extension("view.pb");
    match Command::new("go")
        .args(proto_args(raw))
        .arg(profile)
        .output()
    {
        Ok(o) if o.status.success() && !o.stdout.is_empty() => {
            match std::fs::write(&out, &o.stdout) {
                Ok(()) => {
                    if !raw {
                        report_elision(profile, &out);
                    }
                    out
                }
                Err(_) => profile.to_path_buf(),
            }
        }
        _ => profile.to_path_buf(),
    }
}

/// The `go tool pprof` flags that rewrite a profile for viewing.
///
/// Split out so the one thing that distinguishes `--raw` from the default is a
/// value a test can inspect, rather than a branch buried in a process spawn.
fn proto_args(raw: bool) -> Vec<String> {
    let mut args = vec![
        "tool".to_string(),
        "pprof".to_string(),
        "-proto".to_string(),
    ];
    if !raw {
        args.push(format!("-hide={HIDE_SCAFFOLDING}"));
    }
    args
}

/// Say so when hiding frames also cost samples.
///
/// `-hide` keeps a sample as long as one frame survives, so the totals normally
/// match to the last millisecond. They diverge in exactly one case: a sample
/// whose *every* frame is scaffolding — a parked worker, a blocking-pool poll —
/// has nothing left to attribute and disappears. That is a real quantity (a few
/// percent of a mostly-idle process) and a reader comparing this view's total
/// against `ztest sync status` deserves to be told where it went rather than
/// left to discover the discrepancy.
///
/// Silent on failure and on an exact match: this is a footnote to a diagnostic,
/// and a profile that opened fine must not be reported as a problem because a
/// second, purely informational pprof invocation did not.
fn report_elision(raw: &Path, viewed: &Path) {
    let (Some(before), Some(after)) = (total_samples(raw), total_samples(viewed)) else {
        return;
    };
    if before == after {
        return;
    }
    eprintln!(
        "ztest sync perf: runtime scaffolding hidden ({before} of samples in the profile, \
         {after} in this view). The difference is samples with no application frame at \
         all — parked workers and blocking-pool polls. Use `--raw` to keep every frame."
    );
}

/// The `Total samples` figure pprof prints for `profile`, verbatim (e.g.
/// `14.97s`).
///
/// Kept as the string pprof chose rather than parsed into a number: the only use
/// is to show two totals side by side, and re-deriving a unit and a precision
/// that pprof has already picked correctly would be a way to disagree with it.
fn total_samples(profile: &Path) -> Option<String> {
    let out = Command::new("go")
        .args(["tool", "pprof", "-top", "-nodecount=0"])
        .arg(profile)
        .output()
        .ok()?;
    parse_total_samples(&String::from_utf8_lossy(&out.stdout))
}

/// Pull the `Total samples` figure out of a `go tool pprof -top` header.
///
/// The header line reads `Duration: 561.01s, Total samples = 14.97s ( 2.67%)`,
/// on stdout. Parsed rather than computed because the total lives in the profile
/// header, and split from [`total_samples`] so this dependency on another tool's
/// output format is pinned by a test instead of only by a live run.
fn parse_total_samples(stdout: &str) -> Option<String> {
    stdout.lines().find_map(|line| {
        line.split_once("Total samples = ")
            .and_then(|(_, rest)| rest.split_whitespace().next())
            .map(str::to_string)
    })
}

/// Print the snapshot series, so a reader can see which windows are available
/// before asking for one.
fn show_snapshots(collected: &[PathBuf]) -> Result<(), String> {
    let snaps = snapshots(collected);
    if snaps.is_empty() {
        println!(
            "no periodic snapshots — this run has only a whole-run profile \
             (set ZTEST_PROFILE_INTERVAL on the component to record a series)"
        );
        return Ok(());
    }
    for s in &snaps {
        println!(
            "{:<16} {:>8}  {}",
            s.component,
            format_elapsed(s.elapsed),
            s.path.display()
        );
    }
    Ok(())
}

/// Resolve a requested window to a single profile by subtracting its bounds.
fn cut_window(
    collected: &[PathBuf],
    component: &str,
    from: std::time::Duration,
    to: std::time::Duration,
) -> Result<PathBuf, String> {
    let snaps: Vec<Snapshot> = snapshots(collected)
        .into_iter()
        .filter(|s| s.component == component)
        .collect();
    let (base, head) = window(&snaps, from, to).ok_or_else(|| {
        format!(
            "no {component} snapshot pair covers {}..{} — {} snapshot(s) recorded \
             (`--list` to see them)",
            format_elapsed(from),
            format_elapsed(to),
            snaps.len()
        )
    })?;
    // Named for the interval it actually covers, not the one requested: the cut
    // lands on real sample points, and a file claiming otherwise would misreport
    // its own contents.
    let out = base.path.with_file_name(format!(
        "{component}-window-{}-{}.pb",
        format_elapsed(base.elapsed),
        format_elapsed(head.elapsed)
    ));
    subtract(&base.path, &head.path, &out)?;
    println!(
        "window {} → {}  {}",
        format_elapsed(base.elapsed),
        format_elapsed(head.elapsed),
        out.display()
    );
    Ok(out)
}

/// Pull every profiled component's artifacts down, returning what landed.
async fn retrieve_all(id: &str, out: Option<PathBuf>) -> Result<Vec<PathBuf>, String> {
    let theme = Theme::detect();
    let client = super::client().await?;
    let ns = namespace_for(id);

    let components = crate::profiling::profiled_components(&client, &ns)
        .await
        .map_err(|e| format!("list profile artifacts for {id}: {e}"))?;
    if components.is_empty() {
        return Err(format!(
            "sync {id} has no profiled components — build the image with \
             `--build-arg ZTEST_PROFILE_BUILD=1` and enable profiling on the component"
        ));
    }

    let dest = out.unwrap_or_else(|| default_dest(id));
    let mut collected = Vec::new();
    let mut failed = Vec::new();
    for pod in &components {
        match crate::profiling::retrieve(&client, &ns, pod, &dest).await {
            Ok(paths) => {
                for path in &paths {
                    println!(
                        "{} {}",
                        theme.chars.ok.style(theme.styles.pass),
                        path.display()
                    );
                }
                collected.extend(paths);
            }
            // One unreachable component must not cost the others their profiles:
            // a topology can profile several, and a partial answer beats none.
            Err(e) => {
                eprintln!("ztest sync perf: {pod}: {e}");
                failed.push(pod.as_str());
            }
        }
    }
    if collected.is_empty() {
        // Each component's actual cause is already on stderr. Naming a single
        // presumed reason here would contradict them whenever it guessed wrong.
        return Err(format!(
            "sync {id}: no profile artifacts retrieved from {}",
            failed.join(", ")
        ));
    }
    Ok(collected)
}

/// Compare this run against an earlier one: what changed, and where.
///
/// Two answers, in that order, because they are the two halves of an
/// optimisation loop. The headline says *what* changed and by how much; the
/// differential flame graph says *where* the time went. Neither is useful alone
/// — a percentage with no location is a mystery, and a flame graph with no
/// baseline is just a profile.
///
/// **There is exactly one number to compare.** Once the two runs are known to
/// have covered the same span, their work vectors are the same constant — work
/// is a property of the chain, not of whoever processed it. So `rate =
/// work/elapsed` differs between them only through `elapsed`, and every op's
/// ratio is the same ratio. A per-op comparison table would be seven columns of
/// one number wearing different hats. Per-op *attribution* is a thing only a
/// profiler can give, which is what the flame graph below is for.
///
/// The composition is printed instead, unpaired: it belongs to the span both
/// runs share, and it is what explains why an optimisation landed or didn't.
async fn compare(
    id: &str,
    base_id: &str,
    component: &str,
    collected: &[PathBuf],
    open: bool,
    raw: bool,
) -> Result<(), String> {
    let theme = Theme::detect();
    let head = read_report(id).await?;
    let base = read_report(base_id).await?;

    let (Some(head_seg), Some(base_seg)) = (&head.segment, &base.segment) else {
        return Err(format!(
            "sync {} and {base_id} cannot be compared: one of them recorded no segment, \
             which is what a run to tip (or a driver predating segments) leaves behind. \
             Give both profiles a `run.until_height(..)` so they cover the same work.",
            id
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

    // Retrieved second: the table is the cheap half and a reader gets it even
    // when the baseline's artifacts have already been reclaimed.
    let base_dir = default_dest(base_id);
    let base_files = retrieve_all(base_id, Some(base_dir)).await?;
    let (Some(head_profile), Some(base_profile)) = (
        pick_profile(collected, component),
        pick_profile(&base_files, component),
    ) else {
        return Err(format!(
            "no {component} profile in both runs — the rate table above still holds; \
             a flame-graph diff additionally needs each component built with \
             `--build-arg ZTEST_PROFILE_BUILD=1`"
        ));
    };

    let out = head_profile.with_file_name(format!("{component}-vs-{base_id}.pb"));
    subtract(&base_profile, &head_profile, &out)?;
    println!(
        "{:>10}  {}",
        "profile".style(theme.styles.dim),
        out.display()
    );
    match open {
        true => launch(&normalize_for_viewing(&out, raw), &theme),
        false => Ok(()),
    }
}

/// The comparison, in full: the shared span, a line per run, and what the span
/// is made of.
fn verdict(
    head: &crate::sync::Segment,
    base: &crate::sync::Segment,
    head_id: &str,
    base_id: &str,
    theme: &Theme,
) -> String {
    let mut out = String::new();
    // Equal spans imply equal work, so a disagreement is not a slower run — it
    // is a measurement that cannot be trusted, and saying so beats printing a
    // throughput difference that is really a difference in what was counted.
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
    line(
        &mut out,
        "head",
        head_id,
        head,
        &change(head, base, theme).unwrap_or_default(),
    );

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

/// The one number: total measured ops per second over the span.
fn ops_per_sec(seg: &crate::sync::Segment) -> String {
    match seg.rate().total() {
        Some(r) => format!("{}/s", crate::ui::text::compact(r)),
        None => "—".to_string(),
    }
}

/// Head's throughput against base's, or `None` when either is unmeasured.
///
/// Flagged when it falls below [`REGRESSION_MARGIN`], which marks the eye and
/// gates nothing: `perf` is a diagnostic for someone already optimising, and a
/// threshold here would be a verdict this command has no standing to give.
/// Detecting regressions properly is change point detection over a run history,
/// which is a separate concern and deliberately not this one.
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
    Some(
        format!("{:+.1}%", (h - b) / b * 100.0)
            .style(style)
            .to_string(),
    )
}

/// Fraction of the baseline below which the change is flagged.
const REGRESSION_MARGIN: f64 = 0.95;

/// Read a finished sync's durable report.
async fn read_report(id: &str) -> Result<crate::sync::SyncReportMirror, String> {
    let client = super::client().await?;
    let ns = namespace_for(id);
    super::read_report(&client, &ns, id)
        .await?
        .ok_or_else(|| format!("sync {id}: no report — it has not finished yet"))
}

/// Where profiles land when `--out` is not given: a per-sync directory, so two
/// syncs of the same profile never overwrite each other's artifacts.
fn default_dest(id: &str) -> PathBuf {
    PathBuf::from(format!("ztest-perf-{id}"))
}

/// The whole-run pprof profile for one component — never the SVG, and never
/// another component's.
fn pick_profile(collected: &[PathBuf], component: &str) -> Option<PathBuf> {
    let wanted = format!("{component}-{PPROF_ARTIFACT}");
    collected
        .iter()
        .find(|p| p.file_name().and_then(|n| n.to_str()) == Some(wanted.as_str()))
        .cloned()
}

/// One periodic snapshot: which component produced it, and how far into the run
/// it was taken.
///
/// The elapsed seconds come from the filename rather than the profile's own
/// `time_nanos`, because the contract puts them there precisely so a reader needs
/// no protobuf parse to order the series or locate a window in it.
///
/// The component is carried for a load-bearing reason: two profiled components
/// produce two independent series, and subtracting across them is not a smaller
/// answer but a **meaningless** one — `go tool pprof -base` will cheerfully
/// difference two unrelated processes and render a plausible flame graph of
/// nothing. Every series operation here is therefore scoped to one component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Snapshot {
    pub(super) component: String,
    pub(super) elapsed: std::time::Duration,
    pub(super) path: PathBuf,
}

/// The periodic snapshots among `collected`, oldest first.
///
/// Named `<component>-profile-<elapsed_seconds>.pb` once ztest's unpack has
/// prefixed the component, so both fields come straight out of the filename.
fn snapshots(collected: &[PathBuf]) -> Vec<Snapshot> {
    let mut found: Vec<Snapshot> = collected
        .iter()
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?;
            let (component, tail) = name.rsplit_once("-profile-")?;
            let secs: u64 = tail.strip_suffix(".pb")?.parse().ok()?;
            Some(Snapshot {
                component: component.to_string(),
                elapsed: std::time::Duration::from_secs(secs),
                path: path.clone(),
            })
        })
        .collect();
    found.sort_by(|a, b| {
        a.component
            .cmp(&b.component)
            .then(a.elapsed.cmp(&b.elapsed))
    });
    found
}

/// Every component represented in `collected`, whether by a whole-run profile or
/// a snapshot series.
fn components_present(collected: &[PathBuf]) -> Vec<String> {
    let mut names: Vec<String> = collected
        .iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?;
            let stem = name.rsplit_once("-profile-").map(|(c, _)| c);
            stem.or_else(|| name.strip_suffix("-profile.pb"))
                .map(str::to_string)
        })
        .collect();
    names.sort();
    names.dedup();
    names
}

/// The component whose profiles a request refers to.
///
/// Refuses to guess when a topology profiled more than one: picking silently is
/// how a reader ends up studying the wrong process's flame graph and concluding
/// something false about the one they meant.
fn select_component(collected: &[PathBuf], requested: Option<&str>) -> Result<String, String> {
    let present = components_present(collected);
    match requested {
        Some(name) => present.iter().find(|c| *c == name).cloned().ok_or_else(|| {
            format!(
                "no profiles for component {name:?} (have: {})",
                present.join(", ")
            )
        }),
        None if present.len() == 1 => Ok(present[0].clone()),
        None => Err(format!(
            "this sync profiled {} components ({}) — name one with `--component`, \
             since profiles from different processes are not comparable",
            present.len(),
            present.join(", ")
        )),
    }
}

/// The snapshot pair bounding `[from, to]`, for a windowed view.
///
/// Each bound resolves to the *nearest snapshot at or before* it, which is the
/// only honest choice: a cumulative series can only be cut where it was actually
/// sampled, and rounding a bound forward would attribute work to a window it did
/// not happen in. `None` when the series cannot bound the request — with fewer
/// than two distinct snapshots there is no interval to subtract.
fn window(
    snaps: &[Snapshot],
    from: std::time::Duration,
    to: std::time::Duration,
) -> Option<(Snapshot, Snapshot)> {
    let at_or_before = |t: std::time::Duration| {
        snaps
            .iter()
            .rev()
            .find(|s| s.elapsed <= t)
            .or_else(|| snaps.first())
            .cloned()
    };
    let (base, head) = (at_or_before(from)?, at_or_before(to)?);
    (base.elapsed < head.elapsed).then_some((base, head))
}

/// Subtract `base` from `head`, writing the window profile to `out`.
///
/// Delegated to `go tool pprof -base`, the reference implementation of this
/// arithmetic. Subtracting two profiles by hand means re-keying samples across
/// two string tables and rebuilding the location, function, and mapping tables —
/// and a subtly wrong result is not an error, it is a *plausible flame graph that
/// lies*, which is the worst possible failure for a diagnostic. Composing with
/// the maintained implementation is the right trade even at the cost of a
/// toolchain lookup; the fallback below keeps a Go-less machine working.
fn subtract(base: &Path, head: &Path, out: &Path) -> Result<(), String> {
    if !on_path("go") {
        return Err(format!(
            "windowing needs `go tool pprof` (the pprof reference implementation) \
             and Go is not on PATH — the snapshots are on disk, so `go tool pprof \
             -proto -base {} {}` on any machine with Go produces the window",
            base.display(),
            head.display()
        ));
    }
    let produced = Command::new("go")
        .args(["tool", "pprof", "-proto", "-base"])
        .arg(base)
        .arg(head)
        .output()
        .map_err(|e| format!("run go tool pprof: {e}"))?;
    if !produced.status.success() {
        return Err(format!(
            "go tool pprof -base failed: {}",
            String::from_utf8_lossy(&produced.stderr).trim()
        ));
    }
    std::fs::write(out, &produced.stdout).map_err(|e| format!("write {}: {e}", out.display()))
}

/// Parse a `FROM..TO` window such as `11h..12h`, `30m..90m`, `0..1h`.
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

/// Parse an elapsed-time bound: a bare number of seconds, or one suffixed
/// `s`/`m`/`h`.
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

/// Human `1h30m` / `45m` / `20s`, for the snapshot listing.
fn format_elapsed(d: std::time::Duration) -> String {
    let (h, m, s) = (
        d.as_secs() / 3600,
        (d.as_secs() % 3600) / 60,
        d.as_secs() % 60,
    );
    match (h, m) {
        (0, 0) => format!("{s}s"),
        (0, _) => format!("{m}m{s:02}s"),
        _ => format!("{h}h{m:02}m"),
    }
}

/// Hand `profile` to a viewer, or explain precisely how to get one.
///
/// A missing viewer is reported as guidance rather than an error: the artifact is
/// already on disk and is the thing the user actually asked for, so failing the
/// command over the absence of an optional Python tool would be reporting success
/// as failure.
fn launch(profile: &Path, theme: &Theme) -> Result<(), String> {
    let Some(viewer) = choose_viewer() else {
        eprintln!(
            "ztest sync perf: no profile viewer found — install one with \
             `uv tool install flameshow` (or set {VIEWER_ENV}); the profile is at {}",
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
    // Inherited stdio, and joined rather than detached: these are full-screen
    // terminal viewers that must own the tty, and returning to a shell prompt
    // while one is painting would corrupt both.
    match Command::new(&viewer).arg(profile).status() {
        Ok(status) if status.success() => Ok(()),
        // The overwhelmingly common cause is a viewer that cannot read a profile
        // carrying no mapping table, which is what pprof-rs emits and what
        // `normalize_for_viewing` repairs when Go is available.
        Ok(status) if !on_path("go") => Err(format!(
            "{viewer} exited with {status} — some viewers reject a profile with no \
             mapping table, which Go repairs; install Go, or open {} with \
             `go tool pprof -http=: {}` elsewhere",
            profile.display(),
            profile.display()
        )),
        Ok(status) => Err(format!("{viewer} exited with {status}")),
        Err(e) => Err(format!("launch {viewer}: {e}")),
    }
}

/// The viewer to use: the override if set, else the first known one on `PATH`.
fn choose_viewer() -> Option<String> {
    if let Some(explicit) = std::env::var_os(VIEWER_ENV).filter(|v| !v.is_empty()) {
        return explicit.into_string().ok();
    }
    VIEWERS
        .iter()
        .find(|v| on_path(v))
        .map(|v| (*v).to_string())
}

/// Whether `program` is executable on `PATH`. Hand-rolled rather than shelling to
/// `which`, which is itself not guaranteed present.
fn on_path(program: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| dir.join(program).is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The SVG is a rendering; the pprof is the artifact every viewer reads and
    /// the only one flameshow accepts. Handing over the SVG would fail at the
    /// viewer with a parse error rather than here with a clear one.
    #[test]
    fn the_pprof_artifact_is_preferred_over_the_rendered_svg() {
        let collected = vec![
            PathBuf::from("out/zainod-flamegraph.svg"),
            PathBuf::from("out/zainod-profile.pb"),
        ];
        assert_eq!(
            pick_profile(&collected, "zainod"),
            Some(PathBuf::from("out/zainod-profile.pb"))
        );
    }

    /// A component whose image was built without the profiler still leaves an SVG
    /// from an earlier run, or nothing at all; either way there is no pprof to
    /// open and the caller must not be handed a wrong file.
    #[test]
    fn no_pprof_artifact_means_nothing_to_open() {
        assert_eq!(
            pick_profile(&[PathBuf::from("out/zainod-flamegraph.svg")], "zainod"),
            None
        );
        assert_eq!(pick_profile(&[], "zainod"), None);
    }

    /// Two syncs of one profile must not collide: the artifacts are named per
    /// component, so only the directory separates the runs.
    #[test]
    fn each_sync_gets_its_own_artifact_directory() {
        assert_ne!(
            default_dest("zaino-state-sync-aaa"),
            default_dest("zaino-state-sync-bbb")
        );
        assert!(
            default_dest("zaino-state-sync-aaa")
                .to_string_lossy()
                .contains("zaino-state-sync-aaa")
        );
    }

    /// The override is the whole point of the env var — it must win even when a
    /// known viewer is installed, or a developer cannot choose their own.
    #[test]
    fn the_env_override_outranks_an_installed_viewer() {
        // SAFETY: single-threaded test, and the var is restored before returning.
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

    /// An empty override is a shell accident (`ZTEST_PROFILE_VIEWER=`), not a
    /// request to launch a program with no name.
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

    /// A span of chain, traversed in `secs`. Work belongs to the *span*, so
    /// two segments over the same heights necessarily carry the same vector —
    /// which is why only `secs` varies here.
    fn segment(from: u32, to: u32, secs: u64) -> crate::sync::Segment {
        use crate::sync::{Op, Work};
        let mut work = Work::ZERO;
        work.set(Op::SaplingOutput, 10_000)
            .set(Op::OrchardAction, 5_000);
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

    /// The invariant the old per-op table violated: over a shared span the work
    /// is a constant, so the *only* thing a comparison can report is the change
    /// in throughput, and it comes entirely from elapsed time.
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

    /// Equal spans imply equal work. A disagreement is not a slower run, it is
    /// a measurement that cannot be trusted, and it has to say so.
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

    /// Composition is what the shared work vector is for, and an unmeasured
    /// channel is named as unmeasured rather than silently dropped.
    #[test]
    fn the_composition_of_the_span_is_reported_once() {
        let out = verdict(
            &segment(840_000, 855_000, 50),
            &segment(840_000, 855_000, 100),
            "sync-head",
            "sync-base",
            &plain_theme(),
        );
        let row = out
            .lines()
            .find(|l| l.contains("content"))
            .expect("a content row");
        assert!(
            row.contains("sapling 67%") && row.contains("orchard 33%"),
            "{row}"
        );
        assert!(
            row.contains("transparent, sprout, ironwood unmeasured"),
            "{row}"
        );
    }

    /// Nothing measured means no throughput to compare, and the run lines must
    /// still render rather than the whole command failing.
    #[test]
    fn a_span_with_no_measured_work_reports_no_throughput() {
        let bare = crate::sync::Segment {
            network: Some("regtest".into()),
            from: 0,
            to: 10,
            work: crate::sync::Work::ZERO,
            elapsed_ms: 1000,
        };
        let out = verdict(
            &bare,
            &bare.clone(),
            "sync-head",
            "sync-base",
            &plain_theme(),
        );
        assert!(out.contains('—'), "{out}");
        assert!(!out.contains('%'), "no percentage without a rate:\n{out}");
    }

    /// The default view hides the runtime prologue; `--raw` is the escape hatch
    /// for the case the prologue is itself the subject. Both directions are
    /// asserted because a flag that silently does nothing is worse than none.
    #[test]
    fn the_scaffolding_is_hidden_by_default_and_kept_for_raw() {
        let default = proto_args(false);
        assert!(
            default.iter().any(|a| a.starts_with("-hide=")),
            "{default:?}"
        );
        assert!(!proto_args(true).iter().any(|a| a.starts_with("-hide=")));
        // The mapping-table repair is the reason this pass exists at all and
        // must survive in both modes, or `--raw` hands flameshow a profile it
        // cannot open.
        for args in [proto_args(false), proto_args(true)] {
            assert!(args.contains(&"-proto".to_string()), "{args:?}");
        }
    }

    /// Verbatim `go tool pprof -top -nodecount=0` output. Pinned as a fixture so
    /// a change in that tool's header format fails here — with the format
    /// visible — rather than silently reducing the elision notice to nothing.
    #[test]
    fn the_total_samples_figure_is_read_from_the_pprof_header() {
        let header = "Main binary filename not available.\n\
                      Type: cpu\n\
                      Time: 2026-08-11 09:49:47 PDT\n\
                      Duration: 561.01s, Total samples = 14.97s ( 2.67%)\n\
                      Showing nodes accounting for 12.14s, 81.10% of 14.97s total\n";
        assert_eq!(parse_total_samples(header).as_deref(), Some("14.97s"));
    }

    /// No header means no figure — and the caller must fall silent rather than
    /// print a comparison against a value it invented.
    #[test]
    fn output_without_a_total_yields_no_figure() {
        assert_eq!(parse_total_samples(""), None);
        assert_eq!(parse_total_samples("Type: cpu\nDuration: 561.01s\n"), None);
    }

    #[test]
    fn a_program_that_does_not_exist_is_not_on_path() {
        assert!(!on_path("ztest-definitely-not-a-real-program"));
    }

    fn series() -> Vec<PathBuf> {
        // Deliberately out of order: a tar yields entries in whatever order the
        // filesystem gave them, so ordering must come from the parse.
        ["3600", "0000", "10800", "7200"]
            .iter()
            .map(|s| PathBuf::from(format!("out/zainod-profile-{s:0>8}.pb")))
            .collect()
    }

    /// The elapsed seconds in the filename are what order the series and locate a
    /// window in it — the whole reason the contract puts them there.
    #[test]
    fn snapshots_are_parsed_from_their_names_and_sorted_by_elapsed_time() {
        let snaps = snapshots(&series());
        assert_eq!(
            snaps
                .iter()
                .map(|s| s.elapsed.as_secs())
                .collect::<Vec<_>>(),
            vec![0, 3600, 7200, 10800]
        );
    }

    /// The whole-run `profile.pb` has no elapsed segment and is not a member of
    /// the series; counting it as one would put a duplicate of the total in the
    /// timeline.
    #[test]
    fn the_whole_run_profile_is_not_a_snapshot() {
        let mixed = vec![
            PathBuf::from("out/zainod-profile.pb"),
            PathBuf::from("out/zainod-flamegraph.svg"),
            PathBuf::from("out/zainod-profile-00003600.pb"),
        ];
        let snaps = snapshots(&mixed);
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].elapsed.as_secs(), 3600);
    }

    /// A cumulative series can only be cut where it was actually sampled, so a
    /// bound resolves *backwards*. Rounding forward would attribute work to a
    /// window in which it did not happen.
    #[test]
    fn a_window_resolves_each_bound_to_the_snapshot_at_or_before_it() {
        let snaps = snapshots(&series());
        let (base, head) = window(
            &snaps,
            std::time::Duration::from_secs(5000),
            std::time::Duration::from_secs(9000),
        )
        .expect("a bounding pair");
        assert_eq!(
            (base.elapsed.as_secs(), head.elapsed.as_secs()),
            (3600, 7200)
        );
    }

    /// Both bounds landing on one snapshot is a zero-length window: subtracting a
    /// profile from itself yields an empty flame graph, which reads as "nothing
    /// ran" rather than "you asked for no interval".
    #[test]
    fn a_window_that_spans_no_snapshot_interval_is_refused() {
        let snaps = snapshots(&series());
        assert!(
            window(
                &snaps,
                std::time::Duration::from_secs(3700),
                std::time::Duration::from_secs(3800),
            )
            .is_none()
        );
        assert!(
            window(
                &[],
                std::time::Duration::ZERO,
                std::time::Duration::from_secs(60)
            )
            .is_none()
        );
    }

    fn two_component_series() -> Vec<PathBuf> {
        ["zainod", "zebrad"]
            .iter()
            .flat_map(|c| {
                ["00003600", "00007200"]
                    .iter()
                    .map(move |t| PathBuf::from(format!("out/{c}-profile-{t}.pb")))
            })
            .chain([
                PathBuf::from("out/zainod-profile.pb"),
                PathBuf::from("out/zebrad-profile.pb"),
            ])
            .collect()
    }

    /// The regression that matters most here. Sorting a merged series by elapsed
    /// time alone let a window take its base from one component and its head from
    /// another — and `go tool pprof -base` will difference two unrelated processes
    /// without complaint, producing a plausible flame graph of nothing.
    #[test]
    fn a_window_never_spans_two_different_components() {
        let all = two_component_series();
        let (base, head) = {
            let scoped: Vec<Snapshot> = snapshots(&all)
                .into_iter()
                .filter(|s| s.component == "zainod")
                .collect();
            window(
                &scoped,
                std::time::Duration::ZERO,
                std::time::Duration::from_secs(7200),
            )
            .expect("a bounding pair")
        };
        assert_eq!(base.component, "zainod");
        assert_eq!(head.component, "zainod");
        assert!(base.path.to_string_lossy().contains("zainod"));
        assert!(head.path.to_string_lossy().contains("zainod"));
    }

    /// Guessing which component to show is how a reader ends up studying the
    /// wrong process and drawing a confident false conclusion.
    #[test]
    fn an_ambiguous_component_is_refused_rather_than_guessed() {
        let all = two_component_series();
        let err = select_component(&all, None).expect_err("must not guess");
        assert!(err.contains("zainod") && err.contains("zebrad"), "{err}");
        assert_eq!(select_component(&all, Some("zebrad")).unwrap(), "zebrad");
        assert!(select_component(&all, Some("nope")).is_err());
    }

    /// The common case must stay frictionless: one profiled component needs no
    /// flag at all.
    #[test]
    fn a_single_component_needs_no_disambiguation() {
        let one = vec![
            PathBuf::from("out/zainod-profile.pb"),
            PathBuf::from("out/zainod-profile-00003600.pb"),
        ];
        assert_eq!(select_component(&one, None).unwrap(), "zainod");
    }

    /// A whole-run profile must come from the named component, not merely be the
    /// first `*profile.pb` the tar happened to yield.
    #[test]
    fn the_whole_run_profile_is_scoped_to_its_component() {
        let all = two_component_series();
        assert_eq!(
            pick_profile(&all, "zebrad"),
            Some(PathBuf::from("out/zebrad-profile.pb"))
        );
    }

    #[test]
    fn window_specs_accept_hours_minutes_and_bare_seconds() {
        assert_eq!(
            parse_window("11h..12h").unwrap(),
            (
                std::time::Duration::from_secs(39_600),
                std::time::Duration::from_secs(43_200)
            )
        );
        // A bare bound is seconds, so `0..90` is the first minute and a half.
        assert_eq!(
            parse_window("0..90").unwrap(),
            (
                std::time::Duration::ZERO,
                std::time::Duration::from_secs(90)
            )
        );
        assert_eq!(
            parse_window("30m..1h").unwrap(),
            (
                std::time::Duration::from_secs(1800),
                std::time::Duration::from_secs(3600)
            )
        );
    }

    /// A window that does not advance is a typo, and catching it before the
    /// cluster call keeps a mistake free.
    #[test]
    fn a_window_that_does_not_advance_is_rejected() {
        assert!(parse_window("12h..11h").is_err());
        assert!(parse_window("1h..1h").is_err());
        assert!(parse_window("11h").is_err());
        assert!(parse_window("banana..12h").is_err());
    }
}
