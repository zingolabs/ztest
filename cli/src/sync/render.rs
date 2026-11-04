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

use std::time::Duration;

use ztest::api::fmt::column_width;
use ztest::api::pipeline::profiles::{Miss, ProfileStub, Scan};
use ztest_ui::template::{Fields, Template};
use ztest_ui::{Theme, truncate_with};

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

mod tmpl {
    use super::GAP;

    pub(super) const HEADER: &str = "sync profiles{right:>*|dim}";
    pub(super) const IDENTITY: &str = "{ws}[ {@dot} {branch}] {@dot} {count}";
    pub(super) const NOTE: &str = "    {note|dim}";
    pub(super) const HINT: &str = "{indent}{note|dim}";

    /// Marker tone tracks the subject, so the shape is built per row
    pub(super) fn head(tone: &str, name_w: usize) -> String {
        let gap = " ".repeat(GAP);
        format!(" {{marker|{tone}}}  {{name:<{name_w}|script_id}}{gap}{{desc|dim}}")
    }

    pub(super) fn stem(name_w: usize) -> String {
        let gap = " ".repeat(GAP);
        format!(" {{stem|dim}}  {{meta:<{name_w}}}{gap}{{tags|dim}}")
    }
}

fn draw(src: &str, data: &Fields<'_>, width: usize, theme: &Theme) -> String {
    Template::parse(src).render_str(data, width, Duration::ZERO, theme)
}

/// Width for every *printed* CLI surface (`status`, `sync`, catalogue).
///
/// - Fixed [`DEFAULT_WIDTH`] off a TTY → redirected output stays diffable, never stretched
/// - [`MIN_WIDTH`] floor over the raw report (a 40-column split shears every panel)
/// - Not the only probe in the tree, and deliberately so: `ztest_ui::console` queries rows
///   as well as columns and falls back to 80×40, because a pinned panel must reserve a
///   height even where a printed report only has to avoid wrapping
pub(crate) fn width() -> usize {
    terminal_size::terminal_size()
        .map(|(w, _)| usize::from(w.0))
        .unwrap_or(DEFAULT_WIDTH)
        .max(MIN_WIDTH)
}

/// Indented aside under the listing
fn note(text: &str, theme: &Theme) -> String {
    draw(tmpl::NOTE, &Fields::new().text("note", text), 0, theme)
}

/// Aside hung under an error line, aligned past the `ztest sync: ` prefix
fn hint(text: &str, theme: &Theme) -> String {
    draw(tmpl::HINT, &Fields::new().text("indent", CONT).text("note", text), 0, theme)
}

pub(super) fn listing(scan: &Scan, theme: &Theme, width: usize) -> String {
    let mut out = header(scan, theme, width);
    if scan.profiles.is_empty() {
        // no explainer (workspace + branch already in the header = the whole diagnosis)
        out.push_str(&format!("\n{}\n", note("none in this workspace", theme)));
        return out;
    }
    let name_w = name_column(&scan.profiles);
    for p in &scan.profiles {
        out.push('\n');
        out.push_str(&entry(p, theme, width, name_w));
    }
    out.push_str(&format!("\n{}\n", note("ztest sync start <name>", theme)));
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
                let text = format!("declared in {} — run from there", dir.display());
                out.push_str(&format!("{}\n", hint(&text, theme)));
            }
            out.push('\n');
            out.push_str(&listing(scan, theme, width));
            return out;
        }
        Miss::Unknown { suggestions } => {
            out.push_str(&format!("no profile `{name}`\n"));
            if let Some(near) = suggestions.first() {
                out.push_str(&format!("{}\n", hint(&format!("did you mean {near}?"), theme)));
            }
        }
        Miss::Ambiguous { found } => {
            out.push_str(&format!("`{name}` is declared {} times\n", found.len()));
            for p in found {
                let site = format!("{}:{}", p.file.display(), p.line);
                out.push_str(&format!("{}\n", hint(&site, theme)));
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
    let identity = Fields::new()
        .text("ws", ws)
        .maybe_text("branch", scan.branch.as_deref())
        .text("count", scan.profiles.len().to_string());
    let right = Fields::new().text("right", draw(tmpl::IDENTITY, &identity, 0, theme));
    format!("{}\n", draw(tmpl::HEADER, &right, width, theme))
}

fn entry(p: &ProfileStub, theme: &Theme, width: usize, name_w: usize) -> String {
    let right_w = width.saturating_sub(GUTTER + name_w + GAP).max(1);
    let clip = |s: &str, w: usize| truncate_with(s, w, theme.chars.ellipsis);
    let sep = theme.chars.dot;

    // Overridden reserves shown beside the tier (else the tier alone implies its default)
    let compact = |f| ztest::qos::Resources::from_footprint(f).compact();
    let qos = match (p.footprint, p.runner) {
        (Some(f), Some(r)) => format!("{} ({} + runner {})", p.qos, compact(f), compact(r)),
        (Some(f), None) => format!("{} ({})", p.qos, compact(f)),
        (None, Some(r)) => format!("{} (runner {})", p.qos, compact(r)),
        (None, None) => p.qos.clone(),
    };
    let meta = format!("{} {sep} {qos} {sep} {}", p.subject, p.timeout);

    let head = Fields::new()
        .text("marker", theme.chars.entry)
        .text("name", clip(&p.name, name_w))
        .text("desc", clip(&p.description, right_w));
    let stem = Fields::new()
        .text("stem", theme.chars.vbar)
        .text("meta", clip(&meta, name_w))
        .text("tags", clip(&p.tags.join(", "), right_w));

    format!(
        "{}\n{}\n",
        draw(&tmpl::head(subject_tone(&p.subject), name_w), &head, width, theme),
        draw(&tmpl::stem(name_w), &stem, width, theme),
    )
}

/// Only variable colour here (existing palette slots, no new roles).
fn subject_tone(subject: &str) -> &'static str {
    match subject {
        "indexer" => "script_id",
        "wallet" => "pass",
        "validator" => "skip",
        _ => "bold",
    }
}
