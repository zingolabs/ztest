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
use super::theme::Theme;
use super::{ClaimRow, NodeSummary, RunRow, StatusView};
use crate::qos::beacon::{Beacon, LeaseKind};
use crate::qos::{QosClass, Resources};

/// Asymmetric: elapsed times cluster within the half hour, projections spread to days, and
/// the decision lives in the future half
const PAST_SECS: i64 = 20 * 60;
const FUTURE_SECS: i64 = 60 * 60;

const LEFT_W: usize = 25;
/// User cell + its leading `▸`
const GUTTER: usize = 15;
/// Below this the columns stop fitting and the frame stacks instead
const NARROW_COLS: u16 = 100;
/// Running tests shown per run; the rest fold into the last row's `+ N more`
const TEST_SLOTS: usize = 3;

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

/// One run's bar: elapsed solid, projection light, `┤` at a projected end inside the
/// window, `?` where there is no projection to draw
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
                *c = '▒';
            }
            if to < w {
                cells[to] = '┤';
            }
        }
        Some(_) => {
            for c in cells.iter_mut().skip(now) {
                *c = '▒';
            }
            if w > 0 {
                cells[w - 1] = '▶';
            }
        }
        // Unprojectable: a short stub then `?` — never a right edge the data can't support
        None => {
            let stub = (now + 3).min(w);
            for c in cells.iter_mut().take(stub).skip(now) {
                *c = '▒';
            }
            if stub < w {
                cells[stub] = '?';
            }
        }
    }
    if s < 0 && w > 0 {
        cells[0] = '◀';
    }
    cells.into_iter().collect::<String>().trim_end().to_string()
}

/// Tick row and its labels, sharing one column arithmetic so they cannot drift apart
fn axis_rows(axis: Axis) -> (String, String) {
    let marks: [(i64, &str); 5] =
        [(-PAST_SECS, "−20m"), (0, "NOW"), (20 * 60, "+20m"), (40 * 60, "+40m"), (60 * 60, "+60m")];
    let mut labels = String::new();
    let mut ticks = String::new();
    for (offset, text) in marks {
        let col = axis.col(offset).clamp(0, axis.width as i64) as usize;
        pad_to(&mut labels, col);
        labels.push_str(text);
        pad_to(&mut ticks, col);
        ticks.push(if offset == 0 { '▼' } else { '│' });
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
    let floor = QosClass::Basic.profile().footprint;
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
    let head = format!("{word} — {} free", cpu(free));
    rows.push(if ok {
        head.style(theme.styles.pass).to_string()
    } else {
        head.style(theme.styles.fail).to_string()
    });
    rows.push(String::new());
    let (cpu_pct, mem_pct) = reserved.ratio_pct(&v.allocatable);
    rows.push(meter_row("cpu", format!("{:.1}", reserved.cores()), cpu(v.allocatable), cpu_pct));
    rows.push(meter_row("mem", format!("{:.1}", reserved.gib()), mem(v.allocatable), mem_pct));
    if yours != Resources::ZERO {
        rows.push(totals_row("yours", yours));
    }
    if peers != Resources::ZERO {
        rows.push(totals_row("peers", peers));
    }
    rows.push(String::new());

    let users = distinct_users(v);
    rows.push(format!(" runs   {:>2}  ·  {}", v.runs.len(), plural(users, "user")));
    let running: u32 = v.runs.iter().map(|r| r.beacon.running_count).sum();
    let queued: u32 = v.runs.iter().map(|r| r.beacon.queued).sum();
    if running + queued > 0 {
        rows.push(format!(" tests  {running:>2} running · {queued} q"));
    }
    if !v.claims.is_empty() {
        rows.push(format!(" pending {}", v.claims.len()));
    }
    for (tier, live) in cluster_tiers(v) {
        rows.push(format!(
            " {:<6} {:>2} · {}",
            truncate(tier.as_label(), 5),
            live.count,
            live.reserve.compact()
        ));
    }
    rows.push(String::new());
    rows.extend(cluster_block(&v.context, &v.nodes, v.capacity));
    rows
}

/// Every run's in-flight tests folded by tier — the line that explains the cpu figure
fn cluster_tiers(
    v: &StatusView,
) -> std::collections::BTreeMap<QosClass, crate::qos::live::TierLive> {
    crate::qos::live::tier_tally(
        v.runs.iter().flat_map(|r| r.beacon.running.iter()).map(|t| (t.tier, t.footprint)),
    )
}

fn cluster_block(context: &str, n: &NodeSummary, capacity: Resources) -> Vec<String> {
    let mut rows = vec![format!(" {context}  {}", n.k8s_version)];
    rows.push(format!(
        " {} · {} cp · {} wkr",
        plural(n.ready as usize, "node"),
        n.control_plane,
        n.workers
    ));
    for name in &n.cordoned {
        rows.push(format!(" ⊘ {name}"));
    }
    rows.push(format!(" {} /{} capacity", cpu(capacity), mem(capacity)));
    rows
}

/// Widths sum to under [`LEFT_W`]; an over-wide row shoves the separator right and shears
/// every line below it
fn meter_row(label: &str, used: String, total: String, percent: u8) -> String {
    format!(" {label:<5}{used:>5} /{total:>6} {percent:>3}%")
}

fn totals_row(label: &str, r: Resources) -> String {
    format!(" {label:<6}{:>6}{:>8}", cpu(r), mem(r))
}

// ─────────────────────────── right column ─────────────────────────────

fn right_column(v: &StatusView, width: usize, theme: &Theme) -> Vec<String> {
    let axis = Axis { width: width.saturating_sub(GUTTER).clamp(16, 48) };
    let (labels, ticks) = axis_rows(axis);
    let mut rows = vec![format!("{:GUTTER$}{labels}", " USER"), format!("{:GUTTER$}{ticks}", "")];

    for row in &v.runs {
        rows.push(run_row(row, axis, v, theme));
        rows.extend(connector_rows(&row.beacon, axis, theme));
    }
    if !v.claims.is_empty() {
        rows.push(rule("PENDING", width, theme));
        for c in &v.claims {
            rows.push(claim_row(c, axis));
        }
    }
    if let Some(note) = &v.anomaly {
        rows.push(String::new());
        rows.push(format!(" {} {note}", theme.chars.warn).style(theme.styles.skip).to_string());
    }
    rows
}

fn run_row(row: &RunRow, axis: Axis, v: &StatusView, theme: &Theme) -> String {
    let b = &row.beacon;
    let start = (b.started_at - v.now).num_seconds();
    let end = row.eta.map(|d| d.as_secs() as i64);
    let cell = user_cell(row, theme);
    let trailing = match b.kind {
        LeaseKind::Build => format!(" build  {}  {}", b.reserve.compact(), eta_str(row.eta)),
        LeaseKind::Sync => format!(" sync   {}  {}", b.reserve.compact(), eta_str(row.eta)),
        _ if b.total == 0 => format!(" {}  {}", b.reserve.compact(), eta_str(row.eta)),
        _ => format!(
            " {}/{}   {}  {}{}",
            b.completed(),
            b.total,
            b.reserve.compact(),
            eta_str(row.eta),
            fail_suffix(b, theme),
        ),
    };
    format!("{cell}{}{trailing}", bar(axis, start, end, &theme.chars))
}

fn fail_suffix(b: &Beacon, theme: &Theme) -> String {
    if b.failed == 0 {
        return String::new();
    }
    format!("  {}", format!("{}{}", theme.chars.fail, b.failed).style(theme.styles.fail))
}

/// `▸` = yours. Run-id appended only where a user holds more than one active run
fn user_cell(row: &RunRow, theme: &Theme) -> String {
    let name = match row.show_run_id {
        true => format!("{} {}", row.beacon.user, short_id(&row.beacon.run_id)),
        false => row.beacon.user.clone(),
    };
    let mark = if row.yours { "▸" } else { " " };
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
            false => {
                vec![format!("{indent}└→ (none running)").style(theme.styles.dim).to_string()]
            }
        };
    }
    let shown = b.running.len().min(TEST_SLOTS);
    let mut rows = Vec::with_capacity(shown);
    for (i, t) in b.running.iter().take(shown).enumerate() {
        let last = i + 1 == shown;
        let stem = if last { "└→" } else { "├→" };
        let overflow = match (last, b.elided()) {
            (true, Some((n, held))) => format!("  + {n} more · {}", held.compact()),
            _ => String::new(),
        };
        rows.push(
            format!(
                "{indent}{stem} {:<26} {:>9}{overflow}",
                truncate_tail(&t.name, 26),
                t.footprint.compact()
            )
            .style(theme.styles.dim)
            .to_string(),
        );
    }
    rows
}

fn claim_row(c: &ClaimRow, axis: Axis) -> String {
    let mark = if c.yours { "▸" } else { " " };
    let cell = format!("{:GUTTER$}", format!("{mark}{}", c.beacon.user));
    let needs = c.beacon.needs.unwrap_or(Resources::ZERO);
    match c.projected_start {
        Some(d) => {
            let col = axis.col(d.as_secs() as i64).clamp(0, axis.width as i64) as usize;
            format!(
                "{cell}{}◇ ~+{}   needs {}   {}",
                " ".repeat(col),
                short_dur(d),
                needs.compact(),
                ordinal(c.position)
            )
        }
        None => format!("{cell}waiting — needs {}   {}", needs.compact(), ordinal(c.position)),
    }
}

fn rule(label: &str, width: usize, theme: &Theme) -> String {
    let bar = theme.chars.hbar(width.saturating_sub(label.len() + 4));
    format!("── {label} {bar}")
}

// ─────────────────────────── assembly ─────────────────────────────────

fn header(out: &mut String, v: &StatusView, cols: usize, theme: &Theme) {
    let clock = v.now.format("%H:%M:%S").to_string();
    let head = format!("ztest v{}", env!("CARGO_PKG_VERSION"));
    let mid = format!("{} · {}", v.context, v.server);
    let used = head.len() + mid.len() + clock.len() + 12;
    let fill = theme.chars.hbar(cols.saturating_sub(used));
    let _ = writeln!(
        out,
        "── {} ─── {} {fill} {clock} ──\n",
        head.style(theme.styles.script_id),
        mid.style(theme.styles.count),
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
        " {word} — {} free · cpu {}/{} · mem {}/{}",
        cpu(free),
        cpu(reserved),
        cpu(v.allocatable),
        mem(reserved),
        mem(v.allocatable),
    );
    let _ = writeln!(
        out,
        " {} runs · {} users · {running} running · {queued} queued\n",
        v.runs.len(),
        distinct_users(v),
    );
    for row in right_column(v, cols, theme) {
        let _ = writeln!(out, "{row}");
    }
    let n = &v.nodes;
    let _ = writeln!(
        out,
        "\n {} {} · {} nodes · {} cp · {} wkr{}",
        v.context,
        n.k8s_version,
        n.ready,
        n.control_plane,
        n.workers,
        n.cordoned.iter().map(|c| format!(" · ⊘ {c}")).collect::<String>(),
    );
}

// ─────────────────────────── formatters ───────────────────────────────

/// Head-truncated: the tail of a Rust test path is the distinguishing half
fn truncate_tail(s: &str, w: usize) -> String {
    let n = s.chars().count();
    if n <= w {
        return s.to_string();
    }
    format!("…{}", s.chars().skip(n - w + 1).collect::<String>())
}

/// One decimal, for the left column's totals; [`Resources::compact`] is the dense
/// per-row form beside a bar
fn cpu(r: Resources) -> String {
    format!("{:.1}c", r.cores())
}

fn mem(r: Resources) -> String {
    format!("{:.1}Gi", r.gib())
}

fn eta_str(d: Option<std::time::Duration>) -> String {
    d.map(|d| format!("~{}", short_dur(d))).unwrap_or_else(|| "?".into())
}

fn short_dur(d: std::time::Duration) -> String {
    let s = d.as_secs();
    match s {
        0..=59 => format!("{s}s"),
        60..=3599 => format!("{}m", s / 60),
        _ if s < 86_400 => format!("{}h{:02}", s / 3600, (s % 3600) / 60),
        _ => format!("{}d", s / 86_400),
    }
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
        let (labels, ticks) = axis_rows(axis);
        let now_col = ticks.chars().position(|c| c == '▼').expect("NOW tick");
        assert_eq!(now_col, axis.now());
        assert_eq!(labels.chars().skip(now_col).take(3).collect::<String>(), "NOW");
    }

    #[test]
    fn test_names_truncate_from_the_head() {
        assert_eq!(truncate_tail("sync::feat_nu6_3_topology", 16), "…_nu6_3_topology");
        assert_eq!(truncate_tail("short", 16), "short");
    }

    /// `FULL` is not "zero free" but "less free than the lightest tier needs" — the same
    /// threshold admission blocks on
    #[test]
    fn verdict_turns_full_once_the_smallest_tier_cannot_fit() {
        let alloc = Resources::new(96_000, 128 * crate::qos::GIB, 0, 0);
        let basic = QosClass::Basic.profile().footprint;
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
}
