//! Pure formatter for the preflight banner.
//!
//! Takes a [`BannerState`] and a [`Theme`], returns a `String`. No I/O,
//! no async. The renderer obeys nextest's reporter conventions:
//!
//! - A 12-column right-aligned **action label** + single space prefix
//!   on every primary line (`"{:>12} "`). Matches `cargo nextest`'s
//!   `Nextest run`, `Starting`, `Running`, `Skipped` cadence.
//! - 12-character horizontal rule (`theme.chars.hbar(12)`) opening and
//!   closing the block — same width and glyph nextest uses for
//!   `RunStarted` and `RunFinished`.
//! - `·` between metadata fields, styled with `dim`.
//! - Markers (`✓ ⇣ !`) styled with `pass / dim / skip` respectively;
//!   numeric counts with `count`.
//!
//! Reading the output, a developer sees the preflight as just another
//! group of action lines in nextest's banner, not a separate UI.

use std::fmt::Write as _;

use bytesize::ByteSize;
use owo_colors::OwoColorize;

use super::theme::Theme;
use super::{
    ArchiveRow, ArchiveStatus, BannerState, BuildStage, BuildState, DownloadSource, SnapshotRow,
    SnapshotStatus,
};

/// Width of the action-label column, matching nextest's `{:>12}`.
const LABEL_WIDTH: usize = 12;

/// Width of the bracketed progress bar in [`render_progress_bar`].
const PROGRESS_BAR_WIDTH: usize = 12;

/// Render one frame of the banner to a `String`.
///
/// The result ends in `\n`. ANSI escape codes are present iff
/// `theme.is_colorized()`. Callers performing in-place refresh are
/// responsible for emitting cursor-up sequences between successive
/// `render` calls; this function only knows how to produce one frame.
pub fn render(state: &BannerState, theme: &Theme) -> String {
    let mut out = String::with_capacity(2048);

    render_top_rule(&mut out, theme);
    render_header_line(&mut out, state, theme);
    blank_line(&mut out);
    render_cluster_block(&mut out, state, theme);
    blank_line(&mut out);
    render_inventory_block(&mut out, state, theme);
    blank_line(&mut out);
    render_archive_block(&mut out, state, theme);
    blank_line(&mut out);
    render_snapshot_block(&mut out, state, theme);
    if !state.future.is_empty() {
        blank_line(&mut out);
        render_future_block(&mut out, state, theme);
    }
    render_bottom_rule(&mut out, theme);

    out
}

/// One blank line — the section separator. Stays a single `\n` so the
/// live-renderer's line counter doesn't double-count.
fn blank_line(out: &mut String) {
    out.push('\n');
}

// ─────────────────────────── line writers ─────────────────────────────

/// Indent for `{:>12} ` action labels — used directly on lines that
/// are continuations of the previous label (e.g. the second line of
/// the cluster block describing nodes).
const INDENT: &str = "             "; // 12 spaces + 1 separator = label column width + 1

fn render_top_rule(out: &mut String, theme: &Theme) {
    writeln!(out, "{}", theme.chars.hbar(LABEL_WIDTH)).expect("write to string");
}

fn render_bottom_rule(out: &mut String, theme: &Theme) {
    writeln!(out, "{}", theme.chars.hbar(LABEL_WIDTH)).expect("write to string");
}

fn render_header_line(out: &mut String, _state: &BannerState, theme: &Theme) {
    let label = "Preflight";
    writeln!(
        out,
        "{:>width$} {}",
        label.style(theme.styles.pass),
        "ztest".style(theme.styles.script_id),
        width = LABEL_WIDTH,
    )
    .expect("write to string");
}

fn render_cluster_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let c = &state.cluster;
    let dot = theme.chars.dot.style(theme.styles.dim);

    writeln!(
        out,
        "{:>width$} context {} {dot} {} / {} slots used {dot} configured {} via --test-threads",
        "Cluster".style(theme.styles.pass),
        c.context,
        c.slots_used.style(theme.styles.count),
        c.slots_total.style(theme.styles.count),
        c.slots_configured.style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    writeln!(
        out,
        "{INDENT}{} ready {dot} {} cordoned {dot} {} cores {dot} {} GiB",
        c.nodes_ready.style(theme.styles.count),
        c.nodes_cordoned.style(theme.styles.count),
        c.cores.style(theme.styles.count),
        c.memory_gib.style(theme.styles.count),
    )
    .expect("write to string");
}

/// Spinner glyph table — same braille frames `indicatif` uses for its
/// default spinner. The frame index is derived from the elapsed time
/// so a ~200ms redraw cadence cycles smoothly.
const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Pick a spinner frame based on `elapsed`. 100ms per frame ⇒ at
/// 200ms redraw cadence we advance ~2 frames per tick.
fn spinner_glyph(elapsed: std::time::Duration) -> &'static str {
    let idx = (elapsed.as_millis() / 100) as usize % SPINNER_FRAMES.len();
    SPINNER_FRAMES[idx]
}

/// Format an elapsed duration as `12s` / `1m23s`. Matches the
/// vocabulary nextest uses for its `[NNs]` / `[Nm NNs]` timestamps.
fn format_elapsed(d: std::time::Duration) -> String {
    let total = d.as_secs();
    if total < 60 {
        format!("{total}s")
    } else {
        format!("{}m{:02}s", total / 60, total % 60)
    }
}

fn render_inventory_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let dot = theme.chars.dot.style(theme.styles.dim);
    match &state.build {
        BuildState::Pending => {
            writeln!(
                out,
                "{:>width$} {}",
                "Inventory".style(theme.styles.dim),
                "queued".style(theme.styles.dim),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
        BuildState::Compiling { started_at } => {
            let elapsed = started_at.elapsed();
            writeln!(
                out,
                "{:>width$} {} compiling test binaries… {dot} {}",
                "Inventory".style(theme.styles.pass),
                spinner_glyph(elapsed).style(theme.styles.count),
                format_elapsed(elapsed).style(theme.styles.count),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
        BuildState::Indexing { started_at } => {
            let elapsed = started_at.elapsed();
            writeln!(
                out,
                "{:>width$} {} indexing test selection… {dot} {}",
                "Inventory".style(theme.styles.pass),
                spinner_glyph(elapsed).style(theme.styles.count),
                format_elapsed(elapsed).style(theme.styles.count),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
        BuildState::Ok {
            test_count,
            binary_count,
            elapsed,
        } => {
            writeln!(
                out,
                "{:>width$} {} {} tests across {} binaries {dot} {}",
                "Inventory".style(theme.styles.pass),
                theme.chars.ok.style(theme.styles.pass),
                test_count.style(theme.styles.count),
                binary_count.style(theme.styles.count),
                format_elapsed(*elapsed).style(theme.styles.count),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
        BuildState::Failed {
            exit_code,
            stage,
            elapsed,
        } => {
            let stage_label = match stage {
                BuildStage::Compile => "compile",
                BuildStage::Index => "index",
            };
            writeln!(
                out,
                "{:>width$} {} {} failed (exit {exit_code}) {dot} {}",
                "Inventory".style(theme.styles.fail),
                theme.chars.warn.style(theme.styles.fail),
                stage_label,
                format_elapsed(*elapsed).style(theme.styles.count),
                width = LABEL_WIDTH,
            )
            .expect("write to string");
        }
    }
}

fn render_archive_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let archives = &state.archives;
    writeln!(
        out,
        "{:>width$} {} selected",
        "Archives".style(theme.styles.pass),
        archives.len().style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    let name_col = column_width(archives.iter().map(|r| r.name.as_str()), 18, 28);
    let dot = theme.chars.dot.style(theme.styles.dim);
    for row in archives {
        write_archive_row(out, row, name_col, &dot, theme);
    }
}

fn render_snapshot_block(out: &mut String, state: &BannerState, theme: &Theme) {
    let snapshots = &state.snapshots;
    writeln!(
        out,
        "{:>width$} {} selected",
        "Snapshots".style(theme.styles.pass),
        snapshots.len().style(theme.styles.count),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    let name_col = column_width(snapshots.iter().map(|r| r.pvc.as_str()), 24, 36);
    let dot = theme.chars.dot.style(theme.styles.dim);
    for row in snapshots {
        write_snapshot_row(out, row, name_col, &dot, theme);
    }
}

fn render_future_block(out: &mut String, state: &BannerState, theme: &Theme) {
    if state.future.is_empty() {
        return;
    }
    writeln!(
        out,
        "{:>width$} {} reserved",
        "Future".style(theme.styles.dim),
        state.future.len().style(theme.styles.dim),
        width = LABEL_WIDTH,
    )
    .expect("write to string");

    let name_col = column_width(state.future.iter().map(|r| r.label), 12, 16);
    let dot = theme.chars.dot.style(theme.styles.dim);
    for row in &state.future {
        writeln!(
            out,
            "{INDENT}{:<width$} {dot} {}",
            row.label.style(theme.styles.dim),
            "not yet implemented".style(theme.styles.dim),
            width = name_col,
        )
        .expect("write to string");
    }
}

// ─────────────────────────── per-row writers ──────────────────────────

fn write_archive_row(
    out: &mut String,
    row: &ArchiveRow,
    name_col: usize,
    dot: &impl std::fmt::Display,
    theme: &Theme,
) {
    let (marker, marker_style) = match &row.status {
        ArchiveStatus::Cached { .. } => (theme.chars.ok, theme.styles.pass),
        ArchiveStatus::Downloading { .. } => (theme.chars.progress, theme.styles.count),
        ArchiveStatus::Missing { .. } => (theme.chars.warn, theme.styles.skip),
    };
    write!(
        out,
        "{INDENT}{} {:<width$} {dot} ",
        marker.style(marker_style),
        row.name,
        width = name_col,
    )
    .expect("write to string");
    write_archive_detail(out, &row.status, theme);
    out.push('\n');
}

fn write_archive_detail(out: &mut String, status: &ArchiveStatus, theme: &Theme) {
    let dot = theme.chars.dot.style(theme.styles.dim);
    match status {
        ArchiveStatus::Cached { size_bytes } => {
            write!(
                out,
                "{} {dot} {}",
                "cached".style(theme.styles.pass),
                ByteSize::b(*size_bytes).display().iec().style(theme.styles.count),
            )
            .expect("write to string");
        }
        ArchiveStatus::Downloading {
            source,
            bytes_done,
            bytes_total,
        } => {
            let source_label = match source {
                DownloadSource::Lfs => "LFS",
                DownloadSource::ClusterCache => "cluster cache",
            };
            let percent = status
                .download_progress()
                .map(|(p, _, _)| p)
                .unwrap_or(0);
            let bar = render_progress_bar(percent, theme);
            write!(
                out,
                "downloading from {source_label} {bar} {} {dot} {} / {}",
                format_args!("{percent}%").style(theme.styles.count),
                ByteSize::b(*bytes_done).display().iec().style(theme.styles.count),
                ByteSize::b(*bytes_total).display().iec().style(theme.styles.count),
            )
            .expect("write to string");
        }
        ArchiveStatus::Missing { detail } => {
            write!(
                out,
                "missing {dot} {}",
                detail.style(theme.styles.dim),
            )
            .expect("write to string");
        }
    }
}

fn write_snapshot_row(
    out: &mut String,
    row: &SnapshotRow,
    name_col: usize,
    dot: &impl std::fmt::Display,
    theme: &Theme,
) {
    let (marker, marker_style) = match &row.status {
        SnapshotStatus::BoundReady => (theme.chars.ok, theme.styles.pass),
        SnapshotStatus::Provisioning { .. } => (theme.chars.progress, theme.styles.count),
    };
    write!(
        out,
        "{INDENT}{} {:<width$} {dot} ",
        marker.style(marker_style),
        row.pvc,
        width = name_col,
    )
    .expect("write to string");
    match &row.status {
        SnapshotStatus::BoundReady => {
            write!(
                out,
                "{} {dot} {}",
                "bound".style(theme.styles.pass),
                "ready".style(theme.styles.pass),
            )
            .expect("write to string");
        }
        SnapshotStatus::Provisioning { from_archive } => {
            write!(out, "provisioning from {from_archive}").expect("write to string");
        }
    }
    out.push('\n');
}

// ─────────────────────────── helpers ──────────────────────────────────

fn render_progress_bar(percent: u8, theme: &Theme) -> String {
    let pct = percent.min(100) as usize;
    let filled = pct * PROGRESS_BAR_WIDTH / 100;
    let empty = PROGRESS_BAR_WIDTH - filled;
    format!(
        "{}{}{}{}",
        "[".style(theme.styles.dim),
        theme.chars.bar_fill.repeat(filled).style(theme.styles.count),
        theme.chars.bar_empty.repeat(empty).style(theme.styles.dim),
        "]".style(theme.styles.dim),
    )
}

/// Column width for a name column: max(items) clamped to a sane range
/// so a single very-long name can't push detail off the right.
fn column_width<'a>(
    names: impl IntoIterator<Item = &'a str>,
    min: usize,
    max: usize,
) -> usize {
    names.into_iter().map(|n| n.len()).max().unwrap_or(0).clamp(min, max)
}

// ─────────────────────────── tests ────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;

    fn sample_state() -> BannerState {
        BannerState {
            cluster: ClusterState {
                context: "kind-zaino-local".to_string(),
                slots_used: 12,
                slots_total: 16,
                slots_configured: 6,
                nodes_ready: 3,
                nodes_cordoned: 0,
                cores: 12,
                memory_gib: 48,
            },
            build: BuildState::Ok {
                test_count: 47,
                binary_count: 8,
                elapsed: std::time::Duration::from_secs(18),
            },
            archives: vec![
                ArchiveRow {
                    name: "regtest-nu5-h128".to_string(),
                    status: ArchiveStatus::Cached { size_bytes: 432_013_312 },
                },
                ArchiveRow {
                    name: "testnet-2.6m".to_string(),
                    status: ArchiveStatus::Cached { size_bytes: 19_754_106_880 },
                },
                ArchiveRow {
                    name: "testnet-3.1m".to_string(),
                    status: ArchiveStatus::Downloading {
                        source: DownloadSource::Lfs,
                        bytes_done: 19_241_454_485,
                        bytes_total: 30_064_771_072,
                    },
                },
                ArchiveRow {
                    name: "mainnet-snapshot-9.0".to_string(),
                    status: ArchiveStatus::Missing {
                        detail: "LFS pointer present, blob absent".to_string(),
                    },
                },
            ],
            snapshots: vec![
                SnapshotRow {
                    pvc: "pvc/zebra-testnet-cache".to_string(),
                    status: SnapshotStatus::BoundReady,
                },
                SnapshotRow {
                    pvc: "pvc/zebra-mainnet-cache".to_string(),
                    status: SnapshotStatus::Provisioning {
                        from_archive: "testnet-3.1m".to_string(),
                    },
                },
            ],
            future: vec![],
        }
    }

    /// Theme with no colours and Unicode glyphs — what `Theme::detect()`
    /// returns under a UTF-8 locale with `NO_COLOR=1` and lets us
    /// snapshot byte-exact output.
    fn plain_unicode_theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn plain_ascii_theme() -> Theme {
        Theme::for_capabilities(false, false)
    }

    fn colorized_unicode_theme() -> Theme {
        Theme::for_capabilities(true, true)
    }

    #[test]
    fn plain_unicode_golden() {
        let s = render(&sample_state(), &plain_unicode_theme());
        let expected = "\
────────────
   Preflight ztest

     Cluster context kind-zaino-local · 12 / 16 slots used · configured 6 via --test-threads
             3 ready · 0 cordoned · 12 cores · 48 GiB

   Inventory ✓ 47 tests across 8 binaries · 18s

    Archives 4 selected
             ✓ regtest-nu5-h128     · cached · 412.0 MiB
             ✓ testnet-2.6m         · cached · 18.4 GiB
             ⇣ testnet-3.1m         · downloading from LFS [███████░░░░░] 64% · 17.9 GiB / 28.0 GiB
             ! mainnet-snapshot-9.0 · missing · LFS pointer present, blob absent

   Snapshots 2 selected
             ✓ pvc/zebra-testnet-cache  · bound · ready
             ⇣ pvc/zebra-mainnet-cache  · provisioning from testnet-3.1m
────────────
";
        assert_eq!(
            s, expected,
            "golden mismatch.\n--- got ---\n{s}\n--- want ---\n{expected}"
        );
    }

    #[test]
    fn ascii_fallback_strips_unicode_glyphs() {
        let s = render(&sample_state(), &plain_ascii_theme());
        assert!(!s.contains('─'), "ascii leaked hbar:\n{s}");
        assert!(!s.contains('│'), "ascii leaked vert rule:\n{s}");
        assert!(!s.contains('·'), "ascii leaked dot:\n{s}");
        assert!(s.contains("------------"), "ascii hbar missing:\n{s}");
        assert!(s.contains("OK regtest-nu5-h128"), "ascii ok marker:\n{s}");
        assert!(s.contains(".. testnet-3.1m"), "ascii progress marker:\n{s}");
        assert!(s.contains("WARN mainnet-snapshot-9.0"), "ascii warn marker:\n{s}");
    }

    #[test]
    fn colorized_render_contains_ansi_escapes() {
        let s = render(&sample_state(), &colorized_unicode_theme());
        assert!(s.contains("\x1b["), "colorized output missing ESC:\n{s}");
        // ANSI sequences should not appear in the action-label slot
        // count — the visible text "Preflight" must still be present
        // as a substring.
        assert!(s.contains("Preflight"), "Preflight label missing:\n{s}");
    }

    #[test]
    fn empty_lists_render_zero_count() {
        let mut state = sample_state();
        state.archives.clear();
        state.snapshots.clear();
        let s = render(&state, &plain_unicode_theme());
        assert!(s.contains("Archives 0 selected"), "got:\n{s}");
        assert!(s.contains("Snapshots 0 selected"), "got:\n{s}");
    }

    #[test]
    fn future_rows_render_as_a_labeled_section() {
        let mut state = sample_state();
        state.future = vec![
            FutureRow { label: "tier" },
            FutureRow { label: "queue" },
            FutureRow { label: "reservation" },
        ];
        let s = render(&state, &plain_unicode_theme());
        // Section header.
        assert!(s.contains("Future 3 reserved"), "missing future header:\n{s}");
        // Rows are dot-separated and tagged not-yet-implemented.
        assert!(s.contains("tier"), "got:\n{s}");
        assert!(s.contains("queue"), "got:\n{s}");
        assert!(s.contains("reservation"), "got:\n{s}");
        assert!(s.contains("not yet implemented"), "got:\n{s}");
        // Blank line separator landed before the Future block.
        assert!(s.contains("\n\n      Future"), "missing blank separator:\n{s}");
    }

    #[test]
    fn theme_detect_for_capabilities_truth_table() {
        assert!(!Theme::for_capabilities(false, false).is_colorized());
        assert!(!Theme::for_capabilities(false, true).is_colorized());
        assert!(Theme::for_capabilities(true, false).is_colorized());
        assert!(Theme::for_capabilities(true, true).is_colorized());
    }

    #[test]
    fn progress_bar_clamps_overflow() {
        let theme = plain_unicode_theme();
        let bar0 = render_progress_bar(0, &theme);
        assert_eq!(bar0, "[░░░░░░░░░░░░]");
        let bar100 = render_progress_bar(100, &theme);
        assert_eq!(bar100, "[████████████]");
        let bar250 = render_progress_bar(250, &theme);
        assert_eq!(bar250, "[████████████]", "should clamp at 100%");
    }
}
