//! `ztest sync perf` — query a sync's CPU profile and open it.
//!
//! - Components push to Pyroscope; this asks for a merged pprof over a window
//! - Store sits outside the run → readable mid-sync and after the namespace is gone
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

/// Elides the thread-spawn + tokio-runtime prologue every sample carries ahead of
/// application code.
///
/// - Byte-identical on every stack, and >half the graph (11,182 of 19,131
///   frame-lines on a nine-minute `zaino-state-sync` profile)
/// - `-hide` = the right primitive: drops *frames*, keeps samples, so totals and
///   flame widths hold (`-focus`/`-ignore` cut whole samples; `-prune_from` cuts
///   leafward, the opposite end)
/// - Cost, reported by [`report_elision`]: an all-scaffolding sample (parked worker)
///   vanishes entirely, under-reporting pure runtime overhead. `--raw` opts out
const HIDE_SCAFFOLDING: &str = r"^(root$|__clone|<?std::sys|core::ops::function::FnOnce|<?tokio::runtime|<?core::future::poll_fn|<?tracing::instrument)";

/// One `ztest sync perf` invocation, as asked for on the command line.
///
/// Mutually-constraining choices, not independent inputs: `window` xor `base`,
/// `raw` only under `open`. Named fields also stop four `Option<String>`/`bool`
/// arguments transposing silently
pub(super) struct Request {
    pub(super) id: String,
    pub(super) out: Option<PathBuf>,
    pub(super) open: bool,
    pub(super) window: Option<String>,
    pub(super) component: Option<String>,
    pub(super) base: Option<String>,
    pub(super) raw: bool,
}

pub(super) async fn perf(request: Request) -> Result<(), String> {
    let Request { id, out, open, window, component, base, raw } = request;
    // Parsed before any cluster call — a `--window` typo must cost nothing, not a
    // full retrieval then thrown away.
    let requested = window.as_deref().map(parse_window).transpose()?;
    if requested.is_some() && base.is_some() {
        return Err("`--window` and `--base` are different subtractions and cannot be combined: \
             one cuts a slice out of this run, the other differences this run against \
             another. Pick one."
            .to_string());
    }
    // Never guessed: profiles key by component, and merged samples from two
    // processes are meaningless, not merely coarse.
    let subject = component.ok_or_else(|| {
        "name the component to profile with `--component` (e.g. `--component zainod`)".to_string()
    })?;
    let span = span_for(&id, requested).await?;

    if let Some(base_id) = base {
        return compare(&id, &base_id, &subject, span, open, raw).await;
    }

    let target = retrieve(&id, &subject, span, out).await?;
    match open {
        true => launch(&normalize_for_viewing(&target, raw), &Theme::detect()),
        false => Ok(()),
    }
}

/// Give `profile` a mapping table + strip runtime scaffolding → what the viewer opens.
///
/// - pprof-rs emits `mapping_id = 0` with an empty table: legal, fine for `go tool
///   pprof`, fatal for flameshow, which indexes it unconditionally
/// - One `go tool pprof` pass synthesizes the table and applies
///   [`HIDE_SCAFFOLDING`] (unless `raw`), so trimming rides free on a mandatory repair
/// - Best-effort: Go-less machines open the original (accepted as-is by pprof,
///   speedscope, pprof.me) and see the untrimmed graph; [`launch`] explains a failure
fn normalize_for_viewing(profile: &Path, raw: bool) -> PathBuf {
    if !on_path("go") {
        return profile.to_path_buf();
    }
    let out = profile.with_extension("view.pb");
    match Command::new("go").args(proto_args(raw)).arg(profile).output() {
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

/// `go tool pprof` flags rewriting a profile for viewing. Split out so the whole
/// `--raw` difference is an inspectable value, not a branch inside a process spawn
fn proto_args(raw: bool) -> Vec<String> {
    let mut args = vec!["tool".to_string(), "pprof".to_string(), "-proto".to_string()];
    if !raw {
        args.push(format!("-hide={HIDE_SCAFFOLDING}"));
    }
    args
}

/// Say so when hiding frames also cost samples.
///
/// - `-hide` spares any sample with one surviving frame, so totals normally match
///   exactly; an all-scaffolding sample (parked worker) has nothing left and vanishes
/// - A few percent on a mostly-idle process → told, not left to be discovered
///   against `ztest sync status`
/// - Silent on failure and on an exact match (a footnote must not fail a profile
///   that opened fine)
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

/// pprof's verbatim `Total samples` figure (`14.97s`). Kept as pprof's own string
/// — the sole use is two totals side by side, and re-deriving unit + precision it
/// already picked can only disagree with it
fn total_samples(profile: &Path) -> Option<String> {
    let out = Command::new("go")
        .args(["tool", "pprof", "-top", "-nodecount=0"])
        .arg(profile)
        .output()
        .ok()?;
    parse_total_samples(&String::from_utf8_lossy(&out.stdout))
}

/// Lift `Total samples` out of a `go tool pprof -top` stdout header
/// (`Duration: 561.01s, Total samples = 14.97s ( 2.67%)`). Split from
/// [`total_samples`] so a test pins this dependency on another tool's format
fn parse_total_samples(stdout: &str) -> Option<String> {
    stdout.lines().find_map(|line| {
        line.split_once("Total samples = ")
            .and_then(|(_, rest)| rest.split_whitespace().next())
            .map(str::to_string)
    })
}

/// Requested window → absolute times.
///
/// - Bounds = elapsed since start ("hour eleven", how a forty-hour run is read)
/// - Origin = driver pod creation, so this answers mid-run too; no window = whole run
async fn span_for(
    id: &str,
    requested: Option<(std::time::Duration, std::time::Duration)>,
) -> Result<(SystemTime, SystemTime), String> {
    let client = super::client().await?;
    let driver = super::find_driver(&client, id).await?;
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
) -> Result<PathBuf, String> {
    let theme = Theme::detect();
    let client = super::client().await?;
    let selector = crate::profiling::selector(component, &namespace_for(id));
    let profile = crate::profiling::fetch(&client, &selector, window.0, window.1).await?;

    let dest = out.unwrap_or_else(|| default_dest(id));
    std::fs::create_dir_all(&dest).map_err(|e| format!("create {}: {e}", dest.display()))?;
    let path = dest.join(format!("{component}-profile.pb"));
    std::fs::write(&path, &profile).map_err(|e| format!("write {}: {e}", path.display()))?;
    println!("{} {}", theme.chars.ok.style(theme.styles.pass), path.display());
    Ok(path)
}

/// Compare against an earlier run: what changed (headline), then where (differential
/// flame graph) — the two halves of an optimisation loop, useless apart.
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

    // Second: the table is the cheap half, and reaches a reader even when one run
    // has no profile stored.
    let head_profile = retrieve(id, component, span, None).await?;
    let base_span = span_for(base_id, None).await?;
    let base_profile = retrieve(base_id, component, base_span, Some(default_dest(base_id))).await?;

    let out = head_profile.with_file_name(format!("{component}-vs-{base_id}.pb"));
    subtract(&base_profile, &head_profile, &out)?;
    println!("{:>10}  {}", "profile".style(theme.styles.dim), out.display());
    match open {
        true => launch(&normalize_for_viewing(&out, raw), &theme),
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
    let client = super::client().await?;
    let ns = namespace_for(id);
    super::read_report(&client, &ns, id)
        .await?
        .ok_or_else(|| format!("sync {id}: no report — it has not finished yet"))
}

/// Landing dir absent `--out`. Per-sync, so two syncs of one profile never
/// overwrite each other's artifacts
fn default_dest(id: &str) -> PathBuf {
    PathBuf::from(format!("ztest-perf-{id}"))
}

/// Subtract `base` from `head` → window profile at `out`.
///
/// Delegated to `go tool pprof -base`, the reference implementation. Hand-rolling
/// means re-keying samples across two string tables and rebuilding the location /
/// function / mapping tables, where a subtle error is not an error but a plausible
/// flame graph that lies. Go-less machines get the fallback below
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
    // Inherited stdio, joined not detached — full-screen viewers must own the tty,
    // and a shell prompt returning mid-paint corrupts both.
    match Command::new(&viewer).arg(profile).status() {
        Ok(status) if status.success() => Ok(()),
        // Dominant cause: a viewer that cannot read pprof-rs's mapping-table-less
        // profile, which `normalize_for_viewing` repairs given Go.
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

    /// Default hides the prologue, `--raw` keeps it (for when the prologue is the
    /// subject). Both directions asserted — a silently inert flag is worse than none
    #[test]
    fn the_scaffolding_is_hidden_by_default_and_kept_for_raw() {
        let default = proto_args(false);
        assert!(default.iter().any(|a| a.starts_with("-hide=")), "{default:?}");
        assert!(!proto_args(true).iter().any(|a| a.starts_with("-hide=")));
        // Mapping-table repair = why this pass exists; must survive both modes,
        // else `--raw` hands flameshow a profile it cannot open.
        for args in [proto_args(false), proto_args(true)] {
            assert!(args.contains(&"-proto".to_string()), "{args:?}");
        }
    }

    /// Verbatim `go tool pprof -top -nodecount=0` fixture, so a header-format change
    /// fails here with the format visible, not by silencing the elision notice
    #[test]
    fn the_total_samples_figure_is_read_from_the_pprof_header() {
        let header = "Main binary filename not available.\n\
                      Type: cpu\n\
                      Time: 2026-08-11 09:49:47 PDT\n\
                      Duration: 561.01s, Total samples = 14.97s ( 2.67%)\n\
                      Showing nodes accounting for 12.14s, 81.10% of 14.97s total\n";
        assert_eq!(parse_total_samples(header).as_deref(), Some("14.97s"));
    }

    /// No header ⇒ no figure ⇒ caller falls silent, never comparing against an
    /// invented value
    #[test]
    fn output_without_a_total_yields_no_figure() {
        assert_eq!(parse_total_samples(""), None);
        assert_eq!(parse_total_samples("Type: cpu\nDuration: 561.01s\n"), None);
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
