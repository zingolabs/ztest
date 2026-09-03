//! `ztest status` frame: text summary column beside a gantt (`docs/design-status.md`).
//!
//! - Bars run `started_at → now → projected end`; the solid/light transition lands on the
//!   NOW column in every row, so the seam self-aligns and needs no overlay glyph
//! - Connectors hang from that same column → running-test names left-align into a block
//! - Fixed window (`PAST`..`FUTURE`), clipped with `◀`/`▶`; auto-fitting would rescale
//!   every time a 48 h sync starts

use std::fmt::Write as _;

use owo_colors::OwoColorize as _;

use super::layout::{pad, truncate};
use super::template::{Fields, draw};
use super::theme::Theme;
use super::{ClaimRow, RunRow, StatusView};
use ztest::api::NodeSummary;
use ztest::api::{Beacon, LeaseKind};
use ztest::api::{QosClass, Resources};

/// Asymmetric: elapsed times cluster within the half hour, projections spread to days, and
/// the decision lives in the future half
const PAST_SECS: i64 = 20 * 60;
const FUTURE_SECS: i64 = 60 * 60;

/// Widest left-hand row = a meter (mem line carries two IEC figures)
const LEFT_W: usize = 30;
/// User cell + its leading `▸`
const GUTTER: usize = 15;
/// Below this the columns stop fitting and the frame stacks instead
const NARROW_COLS: u16 = 100;
/// Running tests shown per run; the rest fold into the last row's `+ N more`
const TEST_SLOTS: usize = 3;

// ─────────────────────────── row shapes ───────────────────────────────

/// Every row's shape. Column widths sum to under [`LEFT_W`] on the left-hand set — an
/// over-wide row shoves the separator right and shears every line below it
mod tmpl {
    pub(super) const VERDICT_OK: &str = "{word|pass} {@dash} {free|cores.pass} free";
    pub(super) const VERDICT_FULL: &str = "{word|fail} {@dash} {free|cores.fail} free";
    pub(super) const METER_CPU: &str =
        " {label:<4}{used:>9|cores} /{total:>9|cores}{percent:>4|count}%";
    pub(super) const METER_MEM: &str =
        " {label:<4}{used:>9|bytes} /{total:>9|bytes}{percent:>4|count}%";
    /// Wider label than a meter's, narrower cpu cell — both land on the meter's columns
    pub(super) const TOTALS: &str = " {label:<5}{cpu:>8|cores}  {mem:>9|bytes}";
    pub(super) const RUNS: &str = " runs   {runs:>2}  {@dot}  {users}";
    pub(super) const TESTS: &str = " tests  {running:>2} running {@dot} {queued} q";
    pub(super) const PENDING: &str = " pending {count}";
    pub(super) const TIER: &str = " {tier:<6} {count:>2} {@dot} {reserve}";
    pub(super) const CLUSTER_CONTEXT: &str = " {context}  {version}";
    pub(super) const CLUSTER_NODES: &str =
        " {nodes} {@dot} {control_plane} cp {@dot} {workers} wkr";
    pub(super) const CLUSTER_CORDONED: &str = " {@blocked} {name}";
    pub(super) const CLUSTER_CAPACITY: &str = " {cpu|cores} /{mem|bytes} capacity";

    /// `[  ~{eta}]` drops on an unprojectable run (bar's own `?` already reports it)
    pub(super) const RUN_BUILD: &str = "{user}{bar} build  {reserve}[  ~{eta}]";
    pub(super) const RUN_SYNC: &str = "{user}{bar} sync   {reserve}[  ~{eta}]";
    pub(super) const RUN_UNCOUNTED: &str = "{user}{bar} {reserve}[  ~{eta}]";
    pub(super) const RUN_TESTS: &str =
        "{user}{bar} {done}/{total}   {reserve}[  ~{eta}][  {fail|fail}]";
    pub(super) const CONNECTOR: &str = "{indent}{stem|dim} {name:<26|dim} {footprint:>9|dim}[  + {more|dim} more {@dot} {held|dim}]";
    pub(super) const CONNECTOR_NONE: &str = "{indent}{stem|dim} {note|dim}";
    pub(super) const CLAIM_PROJECTED: &str =
        "{cell}{pad}{@pending} ~+{eta}   needs {needs}   {position}";
    pub(super) const CLAIM_WAITING: &str = "{cell}waiting {@dash} needs {needs}   {position}";
    pub(super) const ANOMALY: &str = " {@warn|skip} {note|skip}";

    pub(super) const NARROW_RESOURCES: &str = concat!(
        " {word} {@dash} {free|cores} free {@dot} cpu {cpu_used|cores}/{cpu_all|cores}",
        " {@dot} mem {mem_used|bytes}/{mem_all|bytes}",
    );
    pub(super) const NARROW_RUNS: &str =
        " {runs} runs {@dot} {users} users {@dot} {running} running {@dot} {queued} queued";
    pub(super) const NARROW_CLUSTER: &str = " {context} {version} {@dot} {nodes} nodes {@dot} {control_plane} cp {@dot} {workers} wkr[{cordoned}]";
}

/// No `*` cell and no spinner in any of these rows → zero width, zero elapsed
pub fn render_status(v: &StatusView, cols: u16, theme: &Theme) -> String {
    let cols = cols.max(40) as usize;
    let mut out = String::new();
    header(&mut out, v, cols, theme);
    if (cols as u16) < NARROW_COLS {
        stacked(&mut out, v, cols, theme);
    } else {
        columns(&mut out, v, cols, theme);
    }
    out
}

// ─────────────────────────── geometry ─────────────────────────────────

/// Maps a signed offset from now onto a bar column
#[derive(Debug, Clone, Copy)]
struct Axis {
    width: usize,
}

impl Axis {
    /// Unclamped — callers need to know a bar fell off the window, not a pinned edge
    fn col(self, offset_secs: i64) -> i64 {
        let span = PAST_SECS + FUTURE_SECS;
        (offset_secs + PAST_SECS) * self.width as i64 / span
    }

    fn now(self) -> usize {
        self.col(0).max(0) as usize
    }
}

/// One run's bar: elapsed solid, projection light, [`bar_end`](super::theme::ThemeChars::bar_end)
/// at a projected end inside the window, `?` where there is no projection to draw
fn bar(axis: Axis, start: i64, end: Option<i64>, ch: &super::theme::ThemeChars) -> String {
    let w = axis.width;
    let mut cells = vec![' '; w];
    let now = axis.now().min(w);
    let s = axis.col(start);
    let from = s.clamp(0, w as i64) as usize;
    for c in cells.iter_mut().take(now).skip(from) {
        *c = ch.bar_fill.chars().next().unwrap_or('#');
    }
    match end.map(|e| axis.col(e)) {
        Some(e) if e < w as i64 => {
            let to = e.clamp(now as i64, w as i64) as usize;
            for c in cells.iter_mut().take(to).skip(now) {
                *c = ch.bar_light;
            }
            if to < w {
                cells[to] = ch.bar_end;
            }
        }
        Some(_) => {
            for c in cells.iter_mut().skip(now) {
                *c = ch.bar_light;
            }
            if w > 0 {
                cells[w - 1] = ch.clip_end;
            }
        }
        // Unprojectable: a short stub then `?` — never a right edge the data can't support
        None => {
            let stub = (now + 3).min(w);
            for c in cells.iter_mut().take(stub).skip(now) {
                *c = ch.bar_light;
            }
            if stub < w {
                cells[stub] = '?';
            }
        }
    }
    if s < 0 && w > 0 {
        cells[0] = ch.clip_start;
    }
    cells.into_iter().collect::<String>().trim_end().to_string()
}

/// Tick row and its labels, sharing one column arithmetic so they cannot drift apart
fn axis_rows(axis: Axis, ch: &super::theme::ThemeChars) -> (String, String) {
    // ASCII hyphen, not U+2212 MINUS: the label sits in a fixed-width grid, and the
    // typographic minus is the one glyph here with no ASCII spelling to fall back to
    let marks: [(i64, &str); 5] =
        [(-PAST_SECS, "-20m"), (0, "NOW"), (20 * 60, "+20m"), (40 * 60, "+40m"), (60 * 60, "+60m")];
    let mut labels = String::new();
    let mut ticks = String::new();
    for (offset, text) in marks {
        let col = axis.col(offset).clamp(0, axis.width as i64) as usize;
        pad_to(&mut labels, col);
        labels.push_str(text);
        pad_to(&mut ticks, col);
        ticks.push(match offset {
            0 => ch.tick_now,
            _ => ch.vbar.chars().next().unwrap_or('|'),
        });
    }
    (labels, ticks)
}

fn pad_to(s: &mut String, col: usize) {
    while s.chars().count() < col {
        s.push(' ');
    }
}

// ─────────────────────────── left column ──────────────────────────────

/// `OPEN`/`TIGHT`/`FULL` against the lightest tier's footprint — the same `min_viable`
/// threshold `ledger::acquire` blocks on, so the word predicts what a launch would do
fn verdict(free: Resources, allocatable: Resources) -> (&'static str, bool) {
    let floor = QosClass::default_footprint();
    if !floor.fits_within(&free) {
        return ("FULL", false);
    }
    let roomy = free.cpu_milli * 4 >= allocatable.cpu_milli;
    (if roomy { "OPEN" } else { "TIGHT" }, true)
}

fn left_column(v: &StatusView, theme: &Theme) -> Vec<String> {
    let reserved = v.runs.iter().fold(Resources::ZERO, |a, r| a.saturating_add(&r.beacon.reserve));
    let free = v.allocatable.saturating_sub(&reserved);
    let (word, ok) = verdict(free, v.allocatable);
    let yours = sum_where(&v.runs, true);
    let peers = sum_where(&v.runs, false);

    let mut rows = Vec::new();
    let shape = match ok {
        true => tmpl::VERDICT_OK,
        false => tmpl::VERDICT_FULL,
    };
    rows.push(draw(shape, &Fields::new().text("word", word).value("free", free.cores()), theme));
    rows.push(String::new());
    let (cpu_pct, mem_pct) = reserved.ratio_pct(&v.allocatable);
    let all = v.allocatable;
    rows.push(meter_row(tmpl::METER_CPU, "cpu", reserved.cores(), all.cores(), cpu_pct, theme));
    rows.push(meter_row(tmpl::METER_MEM, "mem", bytes(reserved), bytes(all), mem_pct, theme));
    if yours != Resources::ZERO {
        rows.push(totals_row("yours", yours, theme));
    }
    if peers != Resources::ZERO {
        rows.push(totals_row("peers", peers, theme));
    }
    rows.push(String::new());

    let users = distinct_users(v);
    rows.push(draw(
        tmpl::RUNS,
        &Fields::new().text("runs", v.runs.len().to_string()).text("users", plural(users, "user")),
        theme,
    ));
    let running: u32 = v.runs.iter().map(|r| r.beacon.running_count).sum();
    let queued: u32 = v.runs.iter().map(|r| r.beacon.queued).sum();
    if running + queued > 0 {
        rows.push(draw(
            tmpl::TESTS,
            &Fields::new().text("running", running.to_string()).text("queued", queued.to_string()),
            theme,
        ));
    }
    if !v.claims.is_empty() {
        rows.push(draw(
            tmpl::PENDING,
            &Fields::new().text("count", v.claims.len().to_string()),
            theme,
        ));
    }
    for (tier, live) in cluster_tiers(v) {
        rows.push(draw(
            tmpl::TIER,
            &Fields::new()
                .text("tier", truncate(tier.as_label(), 5))
                .text("count", live.count.to_string())
                .text("reserve", live.reserve.compact()),
            theme,
        ));
    }
    rows.push(String::new());
    rows.extend(cluster_block(&v.context, &v.nodes, v.capacity, theme));
    rows
}

/// Every run's in-flight tests folded by tier — the line that explains the cpu figure
fn cluster_tiers(v: &StatusView) -> std::collections::BTreeMap<QosClass, ztest::api::TierLive> {
    ztest::api::tier_tally(
        v.runs.iter().flat_map(|r| r.beacon.running.iter()).map(|t| (t.tier, t.footprint)),
    )
}

fn cluster_block(
    context: &str,
    n: &NodeSummary,
    capacity: Resources,
    theme: &Theme,
) -> Vec<String> {
    let mut rows = vec![
        draw(
            tmpl::CLUSTER_CONTEXT,
            &Fields::new().text("context", context).text("version", n.k8s_version.as_str()),
            theme,
        ),
        draw(
            tmpl::CLUSTER_NODES,
            &Fields::new()
                .text("nodes", plural(n.ready as usize, "node"))
                .text("control_plane", n.control_plane.to_string())
                .text("workers", n.workers.to_string()),
            theme,
        ),
    ];
    for name in &n.cordoned {
        rows.push(draw(tmpl::CLUSTER_CORDONED, &Fields::new().text("name", name.as_str()), theme));
    }
    let cap = Fields::new().value("cpu", capacity.cores()).value("mem", bytes(capacity));
    rows.push(draw(tmpl::CLUSTER_CAPACITY, &cap, theme));
    rows
}

fn meter_row(src: &str, label: &str, used: f64, total: f64, percent: u8, theme: &Theme) -> String {
    draw(
        src,
        &Fields::new()
            .text("label", label)
            .value("used", used)
            .value("total", total)
            .value("percent", percent as f64),
        theme,
    )
}

fn totals_row(label: &str, r: Resources, theme: &Theme) -> String {
    let f = Fields::new().text("label", label).value("cpu", r.cores()).value("mem", bytes(r));
    draw(tmpl::TOTALS, &f, theme)
}

// ─────────────────────────── right column ─────────────────────────────

fn right_column(v: &StatusView, width: usize, theme: &Theme) -> Vec<String> {
    let axis = Axis { width: width.saturating_sub(GUTTER).clamp(16, 48) };
    let (labels, ticks) = axis_rows(axis, &theme.chars);
    let mut rows = vec![format!("{:GUTTER$}{labels}", " USER"), format!("{:GUTTER$}{ticks}", "")];

    for row in &v.runs {
        rows.push(run_row(row, axis, v, theme));
        rows.extend(connector_rows(&row.beacon, axis, theme));
    }
    if !v.claims.is_empty() {
        rows.push(rule("PENDING", width, theme));
        for c in &v.claims {
            rows.push(claim_row(c, axis, theme));
        }
    }
    if let Some(note) = &v.anomaly {
        rows.push(String::new());
        rows.push(draw(tmpl::ANOMALY, &Fields::new().text("note", note.as_str()), theme));
    }
    rows
}

fn run_row(row: &RunRow, axis: Axis, v: &StatusView, theme: &Theme) -> String {
    let b = &row.beacon;
    let start = (b.started_at - v.now).num_seconds();
    let end = row.eta.map(|d| d.as_secs() as i64);
    let head = Fields::new()
        .text("user", user_cell(row, theme))
        .text("bar", bar(axis, start, end, &theme.chars))
        .text("reserve", b.reserve.compact())
        .maybe_text("eta", row.eta.map(ztest::api::format_span));
    match b.kind {
        LeaseKind::Build => draw(tmpl::RUN_BUILD, &head, theme),
        LeaseKind::Sync => draw(tmpl::RUN_SYNC, &head, theme),
        _ if b.total == 0 => draw(tmpl::RUN_UNCOUNTED, &head, theme),
        _ => draw(
            tmpl::RUN_TESTS,
            &head
                .text("done", b.completed().to_string())
                .text("total", b.total.to_string())
                .maybe_text("fail", fail_mark(b, theme)),
            theme,
        ),
    }
}

/// `None` = clean run, dropping the template's trailing group with it
fn fail_mark(b: &Beacon, theme: &Theme) -> Option<String> {
    (b.failed > 0).then(|| format!("{}{}", theme.chars.fail, b.failed))
}

/// [`mine`](super::theme::ThemeChars::mine) = yours. Run-id appended only where a user
/// holds more than one active run
fn user_cell(row: &RunRow, theme: &Theme) -> String {
    let name = match row.show_run_id {
        true => format!("{} {}", row.beacon.user, short_id(&row.beacon.run_id)),
        false => row.beacon.user.clone(),
    };
    let mark = match row.yours {
        true => theme.chars.mine,
        false => " ",
    };
    let cell = format!("{mark}{name}");
    let padded = format!("{cell:GUTTER$}");
    match row.yours {
        true => padded.style(theme.styles.count).to_string(),
        false => padded,
    }
}

/// Run-ids are `${user}-${pid}`; the user is already the column, so only the suffix
/// distinguishes two of one user's rows
fn short_id(run_id: &str) -> &str {
    run_id.rsplit_once('-').map(|(_, s)| s).unwrap_or(run_id)
}

fn connector_rows(b: &Beacon, axis: Axis, theme: &Theme) -> Vec<String> {
    if b.kind == LeaseKind::Build || b.kind == LeaseKind::Claim {
        return Vec::new();
    }
    let indent = " ".repeat(GUTTER + axis.now());
    if b.running.is_empty() {
        // A blank block under a stalled run reads as a rendering fault, not a diagnosis
        return match b.reserve == Resources::ZERO {
            true => Vec::new(),
            false => vec![draw(
                tmpl::CONNECTOR_NONE,
                &Fields::new()
                    .text("indent", indent)
                    .text("stem", stem(true, theme))
                    .text("note", "(none running)"),
                theme,
            )],
        };
    }
    let shown = b.running.len().min(TEST_SLOTS);
    let mut rows = Vec::with_capacity(shown);
    for (i, t) in b.running.iter().take(shown).enumerate() {
        let last = i + 1 == shown;
        let elided = last.then(|| b.elided()).flatten();
        rows.push(draw(
            tmpl::CONNECTOR,
            &Fields::new()
                .text("indent", indent.as_str())
                .text("stem", stem(last, theme))
                .text("name", truncate_tail(&t.name, 26, &theme.chars))
                .text("footprint", t.footprint.compact())
                .maybe_text("more", elided.map(|(n, _)| n.to_string()))
                .maybe_text("held", elided.map(|(_, held)| held.compact())),
            theme,
        ));
    }
    rows
}

fn claim_row(c: &ClaimRow, axis: Axis, theme: &Theme) -> String {
    let mark = match c.yours {
        true => theme.chars.mine,
        false => " ",
    };
    let needs = c.beacon.needs.unwrap_or(Resources::ZERO);
    let head = Fields::new()
        .text("cell", format!("{:GUTTER$}", format!("{mark}{}", c.beacon.user)))
        .text("needs", needs.compact())
        .text("position", ordinal(c.position));
    match c.projected_start {
        Some(d) => {
            let col = axis.col(d.as_secs() as i64).clamp(0, axis.width as i64) as usize;
            draw(
                tmpl::CLAIM_PROJECTED,
                &head.text("pad", " ".repeat(col)).text("eta", ztest::api::format_span(d)),
                theme,
            )
        }
        None => draw(tmpl::CLAIM_WAITING, &head, theme),
    }
}

fn rule(label: &str, width: usize, theme: &Theme) -> String {
    let bar = theme.chars.hbar(width.saturating_sub(label.len() + 4));
    format!("{} {label} {bar}", theme.chars.hbar(2))
}

// ─────────────────────────── assembly ─────────────────────────────────

fn header(out: &mut String, v: &StatusView, cols: usize, theme: &Theme) {
    let clock = v.now.format("%H:%M:%S").to_string();
    let head = format!("ztest v{}", env!("CARGO_PKG_VERSION"));
    let mid = format!("{} {} {}", v.context, theme.chars.dot, v.server);
    let used = head.len() + mid.len() + clock.len() + 12;
    let fill = theme.chars.hbar(cols.saturating_sub(used));
    let _ = writeln!(
        out,
        "{lead} {} {mid_rule} {} {fill} {clock} {lead}\n",
        head.style(theme.styles.script_id),
        mid.style(theme.styles.count),
        lead = theme.chars.hbar(2),
        mid_rule = theme.chars.hbar(3),
    );
}

fn columns(out: &mut String, v: &StatusView, cols: usize, theme: &Theme) {
    let left = left_column(v, theme);
    let right = right_column(v, cols - LEFT_W - 2, theme);
    for i in 0..left.len().max(right.len()) {
        let l = left.get(i).cloned().unwrap_or_default();
        let r = right.get(i).cloned().unwrap_or_default();
        let _ = writeln!(out, "{}{}{r}", pad(&l, LEFT_W), theme.chars.vbar);
    }
}

/// Two summary lines instead of a column; same gantt, same glyphs
fn stacked(out: &mut String, v: &StatusView, cols: usize, theme: &Theme) {
    let reserved = v.runs.iter().fold(Resources::ZERO, |a, r| a.saturating_add(&r.beacon.reserve));
    let free = v.allocatable.saturating_sub(&reserved);
    let (word, _) = verdict(free, v.allocatable);
    let running: u32 = v.runs.iter().map(|r| r.beacon.running_count).sum();
    let queued: u32 = v.runs.iter().map(|r| r.beacon.queued).sum();
    let _ = writeln!(
        out,
        "{}",
        draw(
            tmpl::NARROW_RESOURCES,
            &Fields::new()
                .text("word", word)
                .value("free", free.cores())
                .value("cpu_used", reserved.cores())
                .value("cpu_all", v.allocatable.cores())
                .value("mem_used", bytes(reserved))
                .value("mem_all", bytes(v.allocatable)),
            theme,
        )
    );
    let _ = writeln!(
        out,
        "{}\n",
        draw(
            tmpl::NARROW_RUNS,
            &Fields::new()
                .text("runs", v.runs.len().to_string())
                .text("users", distinct_users(v).to_string())
                .text("running", running.to_string())
                .text("queued", queued.to_string()),
            theme,
        )
    );
    for row in right_column(v, cols, theme) {
        let _ = writeln!(out, "{row}");
    }
    let n = &v.nodes;
    let cordoned = n
        .cordoned
        .iter()
        .map(|c| format!(" {} {} {c}", theme.chars.dot, theme.chars.blocked))
        .collect::<String>();
    let _ = writeln!(
        out,
        "\n{}",
        draw(
            tmpl::NARROW_CLUSTER,
            &Fields::new()
                .text("context", v.context.as_str())
                .text("version", n.k8s_version.as_str())
                .text("nodes", n.ready.to_string())
                .text("control_plane", n.control_plane.to_string())
                .text("workers", n.workers.to_string())
                .maybe_text("cordoned", (!cordoned.is_empty()).then_some(cordoned)),
            theme,
        )
    );
}

// ─────────────────────────── formatters ───────────────────────────────

/// Gantt connector: the theme owns the corner, this surface owns the tail. A plan tree
/// spells the same corner with `── `; sharing the glyph and not the tail is the point
fn stem(last: bool, theme: &Theme) -> String {
    let corner = match last {
        true => theme.chars.stem_last,
        false => theme.chars.stem_mid,
    };
    format!("{corner}{}", theme.chars.arrow)
}

/// Head-truncated: the tail of a Rust test path is the distinguishing half
fn truncate_tail(s: &str, w: usize, ch: &super::theme::ThemeChars) -> String {
    let n = s.chars().count();
    if n <= w {
        return s.to_string();
    }
    let mark = ch.ellipsis;
    let keep = w.saturating_sub(mark.chars().count());
    format!("{mark}{}", s.chars().skip(n - keep).collect::<String>())
}

/// `mem_bytes` as the `f64` a `{k|bytes}` cell binds
fn bytes(r: Resources) -> f64 {
    r.mem_bytes as f64
}

fn plural(n: usize, noun: &str) -> String {
    match n {
        1 => format!("1 {noun}"),
        n => format!("{n} {noun}s"),
    }
}

fn ordinal(n: usize) -> String {
    match n {
        1 => "1st".into(),
        2 => "2nd".into(),
        3 => "3rd".into(),
        n => format!("{n}th"),
    }
}

fn sum_where(runs: &[RunRow], yours: bool) -> Resources {
    runs.iter()
        .filter(|r| r.yours == yours)
        .fold(Resources::ZERO, |a, r| a.saturating_add(&r.beacon.reserve))
}

fn distinct_users(v: &StatusView) -> usize {
    v.runs.iter().map(|r| r.beacon.user.as_str()).collect::<std::collections::BTreeSet<_>>().len()
}

#[cfg(test)]
mod tests {
    use super::super::layout::display_width;
    use super::*;
    use std::time::Duration;

    fn ch() -> super::super::theme::ThemeChars {
        super::super::theme::ThemeChars::unicode()
    }

    /// The seam every connector hangs from: elapsed/projected transition == the NOW tick,
    /// in every row, whatever a run's own start and end are
    #[test]
    fn every_bars_transition_lands_on_the_now_column() {
        let axis = Axis { width: 48 };
        let now = axis.now();
        for start in [-1200, -600, -120, -5] {
            let b = bar(axis, start, Some(1800), &ch());
            let cells: Vec<char> = b.chars().collect();
            assert_eq!(cells[now - 1], '█', "start={start} elapsed runs up to NOW");
            assert_eq!(cells[now], '▒', "start={start} projection begins at NOW");
        }
    }

    #[test]
    fn a_run_starting_before_the_window_is_clipped_left() {
        let b = bar(Axis { width: 48 }, -2 * 24 * 3600, Some(600), &ch());
        assert!(b.starts_with('◀'), "off-window start marks the clip: {b}");
    }

    #[test]
    fn a_run_ending_past_the_window_is_clipped_right() {
        let b = bar(Axis { width: 48 }, -600, Some(31 * 3600), &ch());
        assert!(b.ends_with('▶'), "off-window end marks the clip: {b}");
        assert!(!b.contains('┤'), "no end cap the window cannot place");
    }

    /// A stalled run has no honest projection; the `?` is the stall detector, and an end
    /// cap there would be an invented completion time
    #[test]
    fn an_unprojectable_run_ends_in_a_question_mark() {
        let b = bar(Axis { width: 48 }, -600, None, &ch());
        assert!(b.contains('?'));
        assert!(!b.contains('┤'));
        assert!(!b.contains('▶'));
    }

    #[test]
    fn axis_labels_and_ticks_share_one_column_arithmetic() {
        let axis = Axis { width: 48 };
        let ch = crate::theme::ThemeChars::unicode();
        let (labels, ticks) = axis_rows(axis, &ch);
        let now_col = ticks.chars().position(|c| c == ch.tick_now).expect("NOW tick");
        assert_eq!(now_col, axis.now());
        assert_eq!(labels.chars().skip(now_col).take(3).collect::<String>(), "NOW");
    }

    #[test]
    fn test_names_truncate_from_the_head() {
        let uni = crate::theme::ThemeChars::unicode();
        assert_eq!(truncate_tail("sync::feat_nu6_3_topology", 16, &uni), "…_nu6_3_topology");
        // ASCII spends three columns on the mark, so it keeps three fewer characters —
        // the budget is the column count, not the character count
        let ascii = crate::theme::ThemeChars::ascii();
        let cut = truncate_tail("sync::feat_nu6_3_topology", 16, &ascii);
        assert_eq!(cut, "...u6_3_topology");
        assert_eq!(cut.chars().count(), 16, "the mark must fit inside the budget");
        assert_eq!(truncate_tail("short", 16, &uni), "short");
    }

    /// `FULL` is not "zero free" but "less free than the lightest tier needs" — the same
    /// threshold admission blocks on
    #[test]
    fn verdict_turns_full_once_the_smallest_tier_cannot_fit() {
        let alloc = Resources::new(96_000, 128 * ztest::api::GIB, 0, 0);
        let basic = QosClass::default_footprint();
        assert_eq!(verdict(basic, alloc).0, "TIGHT");
        let short = Resources::new(basic.cpu_milli - 1, basic.mem_bytes, 0, 0);
        assert_eq!(verdict(short, alloc).0, "FULL");
        assert_eq!(verdict(alloc, alloc).0, "OPEN");
    }

    #[test]
    fn run_ids_shorten_to_the_suffix_the_user_column_does_not_carry() {
        assert_eq!(short_id("elicb-47192"), "47192");
        assert_eq!(short_id("gh-8841029"), "8841029");
    }

    /// The left column is padded to a fixed width; counting a styled cell's escapes would
    /// shove the separator right and shear every line below it
    #[test]
    fn ansi_does_not_count_toward_the_left_columns_width() {
        let styled = "\u{1b}[1mFULL\u{1b}[0m";
        assert_eq!(display_width(styled), 4);
        assert_eq!(display_width(&pad(styled, LEFT_W)), LEFT_W);
    }

    /// Absent ETA drops its group where a measured zero still prints — "no projection"
    /// and "due now" stay two different readings
    #[test]
    fn an_absent_eta_drops_its_group_where_a_zero_prints() {
        let theme = Theme::for_capabilities(false, true);
        let head = || Fields::new().text("user", "").text("bar", "").text("reserve", "1c/2Gi");
        assert!(!draw(tmpl::RUN_SYNC, &head(), &theme).contains('~'));
        let zero = head().text("eta", ztest::api::format_span(Duration::ZERO));
        assert!(draw(tmpl::RUN_SYNC, &zero, &theme).ends_with("~0s"));
    }

    /// Both meters must land their `%` on one column, or the left block reads as two;
    /// the mem line's IEC pair is the widest thing in it
    #[test]
    fn both_meter_rows_fit_the_left_column() {
        let (t, g) = (Theme::for_capabilities(false, true), ztest::api::GIB as f64);
        let cpu = meter_row(tmpl::METER_CPU, "cpu", 16.0, 96.0, 17, &t);
        let mem = meter_row(tmpl::METER_MEM, "mem", 12.0 * g, 128.0 * g, 9, &t);
        assert_eq!(display_width(&cpu), display_width(&mem));
        assert!(display_width(&mem) <= LEFT_W, "{mem:?} overflows the left column");
    }

    /// Full-frame fixture: every branch this surface can draw — a run with connectors,
    /// a queued claim, a cordoned node — so a golden covers the whole frame, not a row
    fn full_view() -> StatusView {
        use chrono::{TimeZone as _, Utc};
        let at =
            |secs: i64| Utc.timestamp_opt(1_700_000_000 + secs, 0).single().unwrap_or_default();
        let test = |name: &str| ztest::api::RunningTest {
            name: name.to_string(),
            footprint: Resources::new(2_000, 4 * ztest::api::GIB, 0, 0),
            started_at: at(-300),
            tier: QosClass::Integration,
        };
        let beacon = |user: &str, kind: LeaseKind, running: Vec<ztest::api::RunningTest>| Beacon {
            run_id: format!("{user}-4711"),
            user: user.to_string(),
            kind,
            reserve: Resources::new(6_000, 12 * ztest::api::GIB, 0, 0),
            started_at: at(-600),
            total: 12,
            queued: 3,
            failed: 1,
            running_count: running.len() as u32,
            running_footprint: Resources::new(4_000, 8 * ztest::api::GIB, 0, 0),
            running,
            needs: Some(Resources::new(8_000, 16 * ztest::api::GIB, 0, 0)),
            eta_override: None,
        };
        StatusView {
            context: "zkn".into(),
            server: "v1.31.0".into(),
            nodes: NodeSummary {
                k8s_version: "v1.31.0".into(),
                ready: 3,
                control_plane: 1,
                workers: 2,
                cordoned: vec!["zkn-worker3".into()],
            },
            allocatable: Resources::new(96_000, 128 * ztest::api::GIB, 0, 0),
            capacity: Resources::new(128_000, 192 * ztest::api::GIB, 0, 0),
            runs: vec![
                RunRow {
                    beacon: beacon(
                        "eli",
                        LeaseKind::Run,
                        vec![
                            test("sync::feat_nu6_3_topology_and_a_long_tail"),
                            test("wallet::send_to_orchard"),
                            test("wallet::shield_transparent"),
                            test("wallet::elided_extra"),
                        ],
                    ),
                    yours: true,
                    show_run_id: true,
                    eta: Some(Duration::from_secs(900)),
                },
                RunRow {
                    beacon: beacon("dana", LeaseKind::Sync, Vec::new()),
                    yours: false,
                    show_run_id: false,
                    eta: None,
                },
            ],
            claims: vec![ClaimRow {
                beacon: beacon("rio", LeaseKind::Claim, Vec::new()),
                yours: false,
                projected_start: Some(Duration::from_secs(1_500)),
                position: 1,
            }],
            anomaly: None,
            now: at(0),
        }
    }

    /// The gate `status` most needed: its gantt vocabulary is a dozen glyphs no other
    /// surface draws, and a Unicode golden cannot tell a themed one from a hardcoded one
    #[test]
    fn the_whole_frame_falls_back_to_ascii() {
        let v = full_view();
        let ascii = Theme::for_capabilities(false, false);
        for cols in [80, 120] {
            crate::testing::assert_ascii_clean(
                &format!("render_status({cols})"),
                &render_status(&v, cols, &ascii),
            );
        }
    }

    /// Both encodings draw the same frame: same rows, same columns, only the glyphs differ
    #[test]
    fn ascii_and_unicode_frames_agree_on_shape() {
        let v = full_view();
        let uni = render_status(&v, 120, &Theme::for_capabilities(false, true));
        let ascii = render_status(&v, 120, &Theme::for_capabilities(false, false));
        assert_eq!(uni.lines().count(), ascii.lines().count(), "row count diverged");
    }
}
