//! Sync profile catalogue.
//!
//! ```text
//! sync profiles                                    live-tests · feat_add_sync · 2
//!
//!  ●  zaino_index_construction   zaino builds its chain index over the pinned…
//!  │  indexer · sync · 48h       mainnet, zaino, index, blossom
//! ```
//!
//! - Listing / empty / `start` miss = one entry block, 3 conditions (3 formats would drift)
//! - Marker coloured by subject, left column sized to longest name, right column = remainder
//! - No hardcoded terminal width

use owo_colors::Style;

use ztest::api::fmt::column_width;
use ztest::api::pipeline::profiles::{Miss, ProfileStub, Scan};
use ztest_ui::Theme;

/// ` ● ` + trailing space
const GUTTER: usize = 4;
const GAP: usize = 3;
/// floor = ragged left edge on 2-profile workspaces; ceiling = one long name starving the right
const NAME_MIN: usize = 20;
const NAME_MAX: usize = 34;
/// no terminal (pipe, CI log)
const DEFAULT_WIDTH: usize = 100;
/// two-column form unreadable below this
const MIN_WIDTH: usize = 60;
/// hanging indent = width of the `ztest sync: ` prefix `crate::block_on` prepends
const CONT: &str = "            ";

pub(super) fn width() -> usize {
    terminal_size::terminal_size()
        .map(|(w, _)| usize::from(w.0))
        .unwrap_or(DEFAULT_WIDTH)
        .max(MIN_WIDTH)
}

pub(super) fn listing(scan: &Scan, theme: &Theme, width: usize) -> String {
    let mut out = header(scan, theme, width);
    if scan.profiles.is_empty() {
        // no explainer (workspace + branch already in the header = the whole diagnosis)
        out.push_str(&format!("\n    {}\n", dim(theme, "none in this workspace")));
        return out;
    }
    let name_w = name_column(&scan.profiles);
    for p in &scan.profiles {
        out.push('\n');
        out.push_str(&entry(p, theme, width, name_w));
    }
    out.push_str(&format!("\n    {}\n", dim(theme, "ztest sync start <name>")));
    out
}

/// Shown relative to cwd where possible — the message is a `cd` instruction, and an
/// absolute path buries the one segment that matters
fn relative_to_cwd(dir: &std::path::Path) -> std::path::PathBuf {
    std::env::current_dir()
        .ok()
        .and_then(|cwd| dir.strip_prefix(&cwd).ok().map(std::path::Path::to_path_buf))
        .filter(|rel| !rel.as_os_str().is_empty())
        .unwrap_or_else(|| dir.to_path_buf())
}

/// One width for the whole listing (per-row sizing = ragged right column).
fn name_column(profiles: &[ProfileStub]) -> usize {
    column_width(profiles.iter().map(|p| p.name.as_str()), NAME_MIN, NAME_MAX)
}

/// `start` failure, then the catalogue.
///
/// - No `ztest sync:` prefix on line 1 (CLI error path adds it; [`CONT`] hangs the rest under it)
pub(super) fn miss(name: &str, scan: &Scan, why: &Miss, theme: &Theme, width: usize) -> String {
    let mut out = String::new();
    match why {
        Miss::Empty => {
            out.push_str("no sync profiles in this workspace\n");
            for dir in ztest::api::pipeline::workspaces_with_profiles(&scan.workspace_root) {
                let dir = relative_to_cwd(&dir);
                out.push_str(&format!(
                    "{CONT}{}\n",
                    dim(theme, &format!("declared in {} — run from there", dir.display())),
                ));
            }
            out.push('\n');
            out.push_str(&listing(scan, theme, width));
            return out;
        }
        Miss::Unknown { suggestions } => {
            out.push_str(&format!("no profile `{name}`\n"));
            if let Some(near) = suggestions.first() {
                out.push_str(&format!("{CONT}{}\n", dim(theme, &format!("did you mean {near}?"))));
            }
        }
        Miss::Ambiguous { found } => {
            out.push_str(&format!("`{name}` is declared {} times\n", found.len()));
            for p in found {
                out.push_str(&format!(
                    "{CONT}{}\n",
                    dim(theme, &format!("{}:{}", p.file.display(), p.line))
                ));
            }
            return out;
        }
    }
    out.push('\n');
    let name_w = name_column(&scan.profiles);
    for p in &scan.profiles {
        out.push_str(&entry(p, theme, width, name_w));
        out.push('\n');
    }
    out
}

/// `sync profiles` left, workspace identity right-flushed (branch carries the wrong-branch case).
fn header(scan: &Scan, theme: &Theme, width: usize) -> String {
    let ws = scan
        .workspace_root
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| scan.workspace_root.display().to_string());
    let sep = theme.chars.dot;
    let mut right = ws;
    if let Some(branch) = &scan.branch {
        right = format!("{right} {sep} {branch}");
    }
    right = format!("{right} {sep} {}", scan.profiles.len());

    const LEFT: &str = "sync profiles";
    let pad = width.saturating_sub(LEFT.chars().count() + right.chars().count());
    format!("{LEFT}{}{}\n", " ".repeat(pad), dim(theme, &right))
}

fn entry(p: &ProfileStub, theme: &Theme, width: usize, name_w: usize) -> String {
    let right_w = width.saturating_sub(GUTTER + name_w + GAP).max(1);
    let gap = " ".repeat(GAP);

    let meta = {
        let sep = theme.chars.dot;
        // Overridden reserve shown beside the tier (else the tier alone implies its default)
        let qos = match p.footprint {
            Some(f) => {
                format!("{} ({})", p.qos, ztest::qos::Resources::from_footprint(f).compact())
            }
            None => p.qos.clone(),
        };
        format!("{} {sep} {qos} {sep} {}", p.subject, p.timeout)
    };
    let tags = p.tags.join(", ");

    let marker = subject_style(theme, &p.subject).style(theme.chars.entry).to_string();
    let stem = dim(theme, theme.chars.vbar);

    let mut out = format!(
        " {marker}  {}{gap}{}\n",
        theme.styles.script_id.style(pad(&clip(&p.name, name_w, theme), name_w)),
        dim(theme, &clip(&p.description, right_w, theme)),
    );
    out.push_str(&format!(
        " {stem}  {}{gap}{}\n",
        pad(&clip(&meta, name_w, theme), name_w),
        dim(theme, &clip(&tags, right_w, theme)),
    ));
    out
}

/// Only variable colour here (existing palette slots, no new roles).
fn subject_style(theme: &Theme, subject: &str) -> Style {
    match subject {
        "indexer" => theme.styles.script_id,
        "wallet" => theme.styles.pass,
        "validator" => theme.styles.skip,
        _ => theme.styles.count,
    }
}

fn dim(theme: &Theme, s: &str) -> String {
    theme.styles.dim.style(s).to_string()
}

/// Never truncates ([`clip`] owns that → unclipped caller gets a ragged line, not lost text).
fn pad(s: &str, w: usize) -> String {
    let len = s.chars().count();
    if len >= w {
        return s.to_string();
    }
    format!("{s}{}", " ".repeat(w - len))
}

/// Widths in `char`s (fields = identifiers/tags/English → matches display width; not wcwidth).
fn clip(s: &str, w: usize, theme: &Theme) -> String {
    let len = s.chars().count();
    if len <= w {
        return s.to_string();
    }
    let ell = theme.chars.ellipsis;
    // column narrower than the marker → plain truncation (ASCII marker = 3 chars, so reachable)
    if w <= ell.chars().count() {
        return s.chars().take(w).collect();
    }
    let keep = w - ell.chars().count();
    let head: String = s.chars().take(keep).collect();
    format!("{head}{ell}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Uncolorized + Unicode (assertions read as glyphs, no escapes in the way).
    fn theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn ascii_theme() -> Theme {
        Theme::for_capabilities(false, false)
    }

    fn profile(name: &str, subject: &str) -> ProfileStub {
        ProfileStub {
            name: name.to_string(),
            description: "zaino builds its chain index over the pinned Blossom mainnet \
                          snapshot; zebrad is the authority"
                .to_string(),
            subject: subject.to_string(),
            qos: "sync".into(),
            footprint: None,
            timeout: "48h".into(),
            tags: vec!["mainnet".into(), "zaino".into(), "index".into()],
            package: "sync".into(),
            target: "zaino_sync".into(),
            file: PathBuf::from("/w/live-tests/sync/tests/zaino_sync.rs"),
            line: 91,
            gated: false,
        }
    }

    fn scan_of(profiles: Vec<ProfileStub>) -> Scan {
        Scan {
            profiles,
            workspace_root: PathBuf::from("/home/e/zaino/live-tests"),
            branch: Some("feat_add_sync".into()),
            blind_spots: Vec::new(),
        }
    }

    fn widths(s: &str) -> Vec<usize> {
        s.lines().map(|l| l.chars().count()).collect()
    }

    #[test]
    fn every_line_fits_the_terminal() {
        let scan = scan_of(vec![
            profile("zaino_index_construction", "indexer"),
            profile("zaino_state_fetch_parity", "indexer"),
        ]);
        for w in [60usize, 80, 100, 140] {
            let out = listing(&scan, &theme(), w);
            for (i, len) in widths(&out).into_iter().enumerate() {
                assert!(len <= w, "line {i} is {len} wide at terminal width {w}:\n{out}");
            }
        }
    }

    #[test]
    fn renders_two_lines_per_profile_separated_by_a_blank() {
        let scan = scan_of(vec![
            profile("zaino_index_construction", "indexer"),
            profile("zaino_state_fetch_parity", "indexer"),
        ]);
        let out = listing(&scan, &theme(), 100);
        let entries: Vec<&str> =
            out.lines().filter(|l| l.starts_with(" ●") || l.starts_with(" │")).collect();
        assert_eq!(entries.len(), 4, "two profiles = four entry lines:\n{out}");
        // blank precedes each entry block
        let lines: Vec<&str> = out.lines().collect();
        for (i, l) in lines.iter().enumerate() {
            if l.starts_with(" ●") {
                assert_eq!(lines[i - 1], "", "entry block must be preceded by a blank");
            }
        }
    }

    #[test]
    fn columns_align_across_profiles_of_different_name_lengths() {
        let scan =
            scan_of(vec![profile("short", "indexer"), profile("a_much_longer_name", "wallet")]);
        let out = listing(&scan, &theme(), 100);
        let starts: Vec<usize> = out
            .lines()
            .filter(|l| l.starts_with(" ●"))
            // description starts after gutter + name column + gap
            .map(|l| l.find("zaino builds").expect("description present"))
            .collect();
        assert_eq!(starts[0], starts[1], "right column must align:\n{out}");
    }

    #[test]
    fn clips_the_description_rather_than_wrapping() {
        let scan = scan_of(vec![profile("zaino_index_construction", "indexer")]);
        let out = listing(&scan, &theme(), 80);
        assert!(out.contains('…'), "a clipped description must be marked:\n{out}");
        assert!(!out.contains("the authority"), "tail must be clipped, not wrapped");
    }

    #[test]
    fn falls_back_to_ascii_glyphs() {
        let scan = scan_of(vec![profile("p", "indexer")]);
        let out = listing(&scan, &ascii_theme(), 100);
        assert!(out.contains(" o  "), "ascii bullet:\n{out}");
        assert!(out.contains(" |  "), "ascii stem:\n{out}");
        assert!(!out.contains('●'));
        assert!(!out.contains('…'));
    }

    #[test]
    fn header_names_the_workspace_and_branch() {
        let out = listing(&scan_of(vec![profile("p", "indexer")]), &theme(), 100);
        let first = out.lines().next().expect("header");
        assert!(first.starts_with("sync profiles"));
        assert!(first.contains("live-tests"));
        assert!(first.contains("feat_add_sync"));
        assert!(first.trim_end().ends_with('1'), "profile count: {first}");
    }

    #[test]
    fn empty_workspace_says_only_that() {
        let out = listing(&scan_of(Vec::new()), &theme(), 100);
        assert!(out.contains("none in this workspace"));
        // explainer sentence must not come back
        assert!(!out.contains("async fn"));
        assert!(!out.contains("sync_test"));
        assert!(!out.contains("scanned"));
        assert!(out.lines().count() <= 3, "empty state stays small:\n{out}");
    }

    #[test]
    fn empty_workspace_still_names_the_branch() {
        // wrong-branch must stay visible
        let out = listing(&scan_of(Vec::new()), &theme(), 100);
        assert!(out.contains("feat_add_sync"));
    }

    #[test]
    fn miss_suggests_the_near_name_then_lists() {
        let scan = scan_of(vec![profile("zaino_index_construction", "indexer")]);
        let why = Miss::Unknown { suggestions: vec!["zaino_index_construction".into()] };
        let out = miss("zaino_index_constructon", &scan, &why, &theme(), 100);
        // prefix owned by the CLI error path (twice = noise)
        assert!(out.starts_with("no profile `zaino_index_constructon`"));
        assert!(out.contains("did you mean zaino_index_construction?"));
        assert!(out.contains(" ●  zaino_index_construction"), "list follows:\n{out}");
    }

    #[test]
    fn miss_on_an_empty_workspace_does_not_suggest_anything() {
        let scan = scan_of(Vec::new());
        let out = miss("anything", &scan, &Miss::Empty, &theme(), 100);
        assert!(out.starts_with("no sync profiles in this workspace"));
        assert!(out.contains("feat_add_sync"), "branch is the diagnosis:\n{out}");
        assert!(!out.contains("did you mean"));
    }

    #[test]
    fn ambiguous_name_names_both_declaration_sites() {
        let scan = scan_of(Vec::new());
        let why =
            Miss::Ambiguous { found: vec![profile("dup", "indexer"), profile("dup", "wallet")] };
        let out = miss("dup", &scan, &why, &theme(), 100);
        assert!(out.starts_with("`dup` is declared 2 times"));
        assert_eq!(out.matches("zaino_sync.rs:91").count(), 2);
    }

    #[test]
    fn clip_is_a_no_op_when_it_fits() {
        assert_eq!(clip("abc", 3, &theme()), "abc");
        assert_eq!(clip("abc", 9, &theme()), "abc");
    }

    #[test]
    fn clip_never_exceeds_its_budget() {
        for w in 1..12usize {
            assert!(clip("abcdefghijkl", w, &theme()).chars().count() <= w);
            assert!(clip("abcdefghijkl", w, &ascii_theme()).chars().count() <= w);
        }
    }

    #[test]
    fn pad_reaches_the_column_and_never_truncates() {
        assert_eq!(pad("ab", 4), "ab  ");
        assert_eq!(pad("abcdef", 3), "abcdef");
    }
}
