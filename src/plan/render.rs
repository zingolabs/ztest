//! `cargo tree` render of a [`Plan`].
//!
//! - Node grammar = `kind name facts`, uniform at every depth
//! - Repeat node = `(*)`, never re-expanded (one seed shared by N tests prints once)
//! - Colour from existing `ui::Theme` roles only (no new `Styles` field)

use std::collections::BTreeSet;
use std::fmt::Write as _;

use owo_colors::OwoColorize as _;

use super::{Plan, PlanRoot, QosNode};
use crate::inventory::{DevImageEntry, SeedEntry};
use crate::qos::Resources;
use crate::ui::Theme;

const GIB: u64 = 1024 * 1024 * 1024;

/// `└──`/`├──` + the continuation prefix children inherit
struct Branch {
    prefix: String,
    last: bool,
}

impl Branch {
    fn glyphs(&self, theme: &Theme) -> (String, String) {
        let unicode = theme.chars.vbar == "│";
        let (tee, elbow, pipe) =
            if unicode { ("├── ", "└── ", "│   ") } else { ("|-- ", "`-- ", "|   ") };
        let head = format!("{}{}", self.prefix, if self.last { elbow } else { tee });
        let next = format!("{}{}", self.prefix, if self.last { "    " } else { pipe });
        (head, next)
    }
}

pub(crate) fn render(plan: &Plan, theme: &Theme) -> String {
    let mut out = String::with_capacity(1024);
    let mut seen: BTreeSet<String> = BTreeSet::new();

    if plan.roots.len() > 1 {
        writeln!(out, "{}\n", summary(plan, theme)).expect("write to string");
    }
    for (i, root) in plan.roots.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        render_root(&mut out, root, &mut seen, theme);
    }
    if !plan.pruned.is_empty() {
        render_pruned(&mut out, plan, theme);
    }
    out
}

fn summary(plan: &Plan, theme: &Theme) -> String {
    let dot = theme.chars.dot.style(theme.styles.dim);
    let images: BTreeSet<&str> =
        plan.roots.iter().flat_map(|r| r.images.iter().map(|i| i.repo.as_str())).collect();
    let seeds: BTreeSet<&str> =
        plan.roots.iter().flat_map(|r| r.seeds.iter().map(|s| s.oid.as_str())).collect();
    let bytes: u64 = plan
        .roots
        .iter()
        .flat_map(|r| r.seeds.iter())
        .map(|s| (s.oid.as_str(), s.size))
        .collect::<std::collections::BTreeMap<_, _>>()
        .values()
        .sum();
    format!(
        "{} tests {dot} {} images {dot} {} seeds {dot} {} to pull",
        plan.roots.len().style(theme.styles.count),
        images.len().style(theme.styles.count),
        seeds.len().style(theme.styles.count),
        gib(bytes).style(theme.styles.count),
    )
}

fn render_root(out: &mut String, root: &PlanRoot, seen: &mut BTreeSet<String>, theme: &Theme) {
    writeln!(out, "{}", root.label.style(theme.styles.count)).expect("write to string");
    if !root.description.is_empty() {
        writeln!(out, "{}", root.description.style(theme.styles.dim)).expect("write to string");
    }

    let mut rows: Vec<Row> = vec![Row::Qos(&root.qos)];
    if !root.tags.is_empty() {
        rows.push(Row::Tags(&root.tags));
    }
    rows.extend(root.images.iter().map(Row::Image));
    rows.extend(root.seeds.iter().map(Row::Seed));

    let n = rows.len();
    for (i, row) in rows.into_iter().enumerate() {
        let b = Branch { prefix: String::new(), last: i + 1 == n };
        match row {
            Row::Qos(q) => qos_node(out, q, &b, theme),
            Row::Tags(t) => tags_node(out, t, &b, theme),
            Row::Image(img) => image_node(out, img, &b, seen, theme),
            Row::Seed(s) => seed_node(out, s, &b, seen, theme),
        }
    }
}

enum Row<'a> {
    Qos(&'a QosNode),
    Tags(&'a [String]),
    Image(&'a DevImageEntry),
    Seed(&'a SeedEntry),
}

fn qos_node(out: &mut String, q: &QosNode, b: &Branch, theme: &Theme) {
    let (head, _) = b.glyphs(theme);
    let dot = theme.chars.dot.style(theme.styles.dim);
    let cap = cap_duration(q.class.profile().hard_cap);
    let declared = match &q.declared_timeout {
        Some(t) => format!(" {dot} declared {}", t.style(theme.styles.count)),
        None => String::new(),
    };
    writeln!(
        out,
        "{}{} {} {dot} reserve {} {dot} hard cap {}{declared}",
        head.style(theme.styles.dim),
        "qos".style(theme.styles.script_id),
        q.class.as_label().style(theme.styles.count),
        res(&q.admitted).style(theme.styles.count),
        cap.style(theme.styles.count),
    )
    .expect("write to string");
}

fn tags_node(out: &mut String, tags: &[String], b: &Branch, theme: &Theme) {
    let (head, _) = b.glyphs(theme);
    writeln!(
        out,
        "{}{} {}",
        head.style(theme.styles.dim),
        "tags".style(theme.styles.script_id),
        tags.join(", ")
    )
    .expect("write to string");
}

fn image_node(
    out: &mut String,
    img: &DevImageEntry,
    b: &Branch,
    seen: &mut BTreeSet<String>,
    theme: &Theme,
) {
    let (head, next) = b.glyphs(theme);
    let key = format!("image:{}:{:?}", img.repo, img.source);
    if !seen.insert(key) {
        writeln!(
            out,
            "{}{} {} {}",
            head.style(theme.styles.dim),
            "image".style(theme.styles.script_id),
            img.repo.style(theme.styles.count),
            "(*)".style(theme.styles.dim)
        )
        .expect("write to string");
        return;
    }
    writeln!(
        out,
        "{}{} {} {}",
        head.style(theme.styles.dim),
        "image".style(theme.styles.script_id),
        img.repo.style(theme.styles.count),
        "BUILD".style(theme.styles.skip)
    )
    .expect("write to string");

    let source = describe_source(&img.source);
    let kids: Vec<(&str, String)> = match img.features.is_empty() {
        true => vec![source],
        false => vec![source, ("features", img.features.join(", "))],
    };
    leaves(out, &kids, &next, theme);
}

fn describe_source(src: &crate::backends::image::DevSource) -> (&'static str, String) {
    use crate::backends::image::DevSource;
    match src {
        DevSource::Local { dockerfile, context } => {
            ("dockerfile", format!("{} ctx {}", tidy(dockerfile), tidy(context)))
        }
        DevSource::Git { url, rev, dockerfile, .. } => ("git", format!("{url}@{rev} {dockerfile}")),
    }
}

/// `dev!` bakes compile-time absolute paths, so a relative arg arrives as
/// `/abs/live-tests/sync/../../Dockerfile`. Lexical only (no symlink resolution — the path
/// may not exist on the machine rendering it)
fn tidy(p: &std::path::Path) -> String {
    let mut out: Vec<std::ffi::OsString> = Vec::new();
    for part in p.components() {
        match part {
            std::path::Component::ParentDir if matches!(out.last(), Some(l) if l != "..") => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str().to_os_string()),
        }
    }
    let joined: std::path::PathBuf = out.iter().collect();
    let text = joined.display().to_string();
    match std::env::current_dir()
        .ok()
        .and_then(|cwd| joined.strip_prefix(&cwd).ok().map(|r| r.display().to_string()))
    {
        Some(rel) if !rel.is_empty() => rel,
        _ => text,
    }
}

fn seed_node(
    out: &mut String,
    s: &SeedEntry,
    b: &Branch,
    seen: &mut BTreeSet<String>,
    theme: &Theme,
) {
    let (head, next) = b.glyphs(theme);
    let sha8 = crate::storage::seed_sha8(&s.oid);
    if !seen.insert(format!("seed:{}", s.oid)) {
        writeln!(
            out,
            "{}{} {} {}",
            head.style(theme.styles.dim),
            "seed".style(theme.styles.script_id),
            sha8.style(theme.styles.count),
            "(*)".style(theme.styles.dim)
        )
        .expect("write to string");
        return;
    }
    writeln!(
        out,
        "{}{} {} {} {}",
        head.style(theme.styles.dim),
        "seed".style(theme.styles.script_id),
        s.name.style(theme.styles.count),
        sha8.style(theme.styles.dim),
        gib(s.size).style(theme.styles.count),
    )
    .expect("write to string");

    let pvc = format!("ztest-seeds/seed-{sha8}-<driver> 32Gi");
    leaves(out, &[("pvc", pvc)], &next, theme);
}

fn leaves(out: &mut String, kids: &[(&str, String)], prefix: &str, theme: &Theme) {
    let n = kids.len();
    for (i, (kind, text)) in kids.iter().enumerate() {
        let b = Branch { prefix: prefix.to_string(), last: i + 1 == n };
        let (head, _) = b.glyphs(theme);
        writeln!(
            out,
            "{}{} {}",
            head.style(theme.styles.dim),
            kind.style(theme.styles.script_id),
            text.style(theme.styles.dim)
        )
        .expect("write to string");
    }
}

fn render_pruned(out: &mut String, plan: &Plan, theme: &Theme) {
    writeln!(out, "\n{}", "pruned".style(theme.styles.skip)).expect("write to string");
    let n = plan.pruned.len();
    for (i, p) in plan.pruned.iter().enumerate() {
        let b = Branch { prefix: String::new(), last: i + 1 == n };
        let (head, next) = b.glyphs(theme);
        writeln!(
            out,
            "{}{} {} {} {}",
            head.style(theme.styles.dim),
            "seed".style(theme.styles.script_id),
            p.seed.name.style(theme.styles.skip),
            crate::storage::seed_sha8(&p.seed.oid).style(theme.styles.dim),
            gib(p.seed.size).style(theme.styles.count),
        )
        .expect("write to string");
        let by = match p.declared_by.is_empty() {
            true => "declaring test not in the dump".to_string(),
            false => p.declared_by.join(", "),
        };
        leaves(out, &[("declared by", by)], &next, theme);
    }
}

/// Tier caps run 60s → 48h; `format_elapsed`'s m/s vocabulary renders that as `2880m00s`
fn cap_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    match secs {
        s if s >= 3600 && s % 3600 == 0 => format!("{}h", s / 3600),
        s if s >= 3600 => format!("{}h{}m", s / 3600, (s % 3600) / 60),
        s if s >= 60 => format!("{}m", s / 60),
        s => format!("{s}s"),
    }
}

fn res(r: &Resources) -> String {
    format!("{}c / {} GiB", r.cpu_milli / 1000, r.mem_bytes / GIB)
}

fn gib(bytes: u64) -> String {
    format!("{:.2} GiB", bytes as f64 / GIB as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::SeedPayload;
    use crate::plan::{PlanRoot, PrunedSeed, QosNode};
    use crate::qos::QosClass;

    fn plain() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn seed(name: &str, oid: &str, size: u64) -> SeedEntry {
        SeedEntry {
            name: name.to_string(),
            oid: oid.to_string(),
            size,
            payload: SeedPayload::Archive,
        }
    }

    fn root(label: &str, seeds: Vec<SeedEntry>) -> PlanRoot {
        PlanRoot {
            label: label.to_string(),
            description: String::new(),
            qos: QosNode {
                class: QosClass::Sync,
                admitted: QosClass::Sync.profile().admitted(),
                declared_timeout: Some("48h".into()),
            },
            tags: vec!["mainnet".into(), "blossom".into()],
            images: Vec::new(),
            seeds,
        }
    }

    #[test]
    fn renders_the_sync_profile_shape() {
        let plan = Plan {
            roots: vec![root(
                "zaino_index_construction",
                vec![seed("zebra-v6.2.3-mainnet-659600.tar.zst", "1106bc19aa", 14_016_194_520)],
            )],
            pruned: vec![PrunedSeed {
                seed: seed("zebra-v6.2.3-testnet-4140000.tar.zst", "3545da25bb", 8_751_733_052),
                declared_by: vec!["clientless::the_pub_testnet_ironwood_boundary".into()],
            }],
        };
        let out = render(&plan, &plain());

        assert!(out.contains("zaino_index_construction"));
        assert!(out.contains("qos sync · reserve 16c / 16 GiB · hard cap 48h · declared 48h"));
        assert!(out.contains("13.05 GiB"));
        assert!(out.contains("pruned"));
        assert!(out.contains("declared by clientless::the_pub_testnet_ironwood_boundary"));
        // No `test`/`binary` rows: structure without information
        assert!(!out.contains("├── test "));
        assert!(!out.contains("binary"));
    }

    /// One seed shared by N roots expands once; the rest repeat as `(*)`
    #[test]
    fn a_repeated_seed_collapses_to_a_star() {
        let s = seed("shared.tar.zst", "abcd1234ff", GIB);
        let plan = Plan {
            roots: vec![root("first", vec![s.clone()]), root("second", vec![s])],
            pruned: Vec::new(),
        };
        let out = render(&plan, &plain());

        assert_eq!(out.matches("shared.tar.zst").count(), 1);
        assert!(out.contains("seed abcd1234 (*)"));
    }
}
