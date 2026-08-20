//! `cargo tree` render of a [`Plan`].
//!
//! - Node grammar = `kind name facts`, uniform at every depth
//! - Repeat node = `(*)`, never re-expanded (one seed shared by N tests prints once)
//! - Colour from existing `ui::Theme` roles only (no new `Styles` field)

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::time::Duration;

use owo_colors::OwoColorize as _;

use super::Theme;
use super::template::{Fields, Template};
use ztest::api::{DevImageEntry, SeedEntry};
use ztest::api::{Plan, PlanRoot, QosNode};

/// Node grammar as templates: `kind name facts`, one shape per node kind
mod tmpl {
    pub(super) const SUMMARY: &str = concat!(
        "{tests|bold} tests {@dot|dim} {images|bold} images {@dot|dim} ",
        "{seeds|bold} seeds {@dot|dim} {bytes|bytes.bold} to pull",
    );
    pub(super) const ROOT: &str = "{label|bold}";
    pub(super) const DESCRIPTION: &str = "{description|dim}";
    pub(super) const QOS: &str = concat!(
        "{head|dim}{kind|script_id} {class|bold} {@dot|dim} reserve {reserve|bold} ",
        "{@dot|dim} hard cap {cap|bold}[ {@dot|dim} declared {declared|bold}]",
    );
    pub(super) const TAGS: &str = "{head|dim}{kind|script_id} {tags}";
    pub(super) const IMAGE: &str = "{head|dim}{kind|script_id} {repo|bold} {mark|skip}";
    /// Image and seed alike: the name already printed once, so only `(*)` follows
    pub(super) const REPEAT: &str = "{head|dim}{kind|script_id} {name|bold} {mark|dim}";
    pub(super) const SEED: &str =
        "{head|dim}{kind|script_id} {name|bold} {sha8|dim} {size|bytes.bold}";
    pub(super) const LEAF: &str = "{head|dim}{kind|script_id} {text|dim}";
    pub(super) const PRUNED: &str =
        "{head|dim}{kind|script_id} {name|skip} {sha8|dim} {size|bytes.bold}";
}

/// Renders + appends. No `*` cell and no spinner in a tree row → zero width, zero elapsed
fn emit(out: &mut String, f: Fields<'_>, src: &str, theme: &Theme) {
    let _ = writeln!(out, "{}", Template::parse(src).render_str(&f, 0, Duration::ZERO, theme));
}

/// `└──`/`├──` + the continuation prefix children inherit
struct Branch {
    prefix: String,
    last: bool,
}

impl Branch {
    /// Corners come from the theme (a gantt connector spells them the same way); only the
    /// tail — three rules and a space — belongs to a plan tree
    fn glyphs(&self, theme: &Theme) -> (String, String) {
        let ch = &theme.chars;
        let corner = match self.last {
            true => ch.stem_last,
            false => ch.stem_mid,
        };
        let head = format!("{}{corner}{} ", self.prefix, ch.hbar(2));
        let next = match self.last {
            true => format!("{}    ", self.prefix),
            false => format!("{}{}   ", self.prefix, ch.vbar),
        };
        (head, next)
    }
}

pub fn render(plan: &Plan, theme: &Theme) -> String {
    let mut out = String::with_capacity(1024);
    let mut seen: BTreeSet<String> = BTreeSet::new();

    if plan.roots.len() > 1 {
        summary(&mut out, plan, theme);
        out.push('\n');
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

fn summary(out: &mut String, plan: &Plan, theme: &Theme) {
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
    emit(
        out,
        Fields::new()
            .text("tests", plan.roots.len().to_string())
            .text("images", images.len().to_string())
            .text("seeds", seeds.len().to_string())
            .value("bytes", bytes as f64),
        tmpl::SUMMARY,
        theme,
    );
}

fn render_root(out: &mut String, root: &PlanRoot, seen: &mut BTreeSet<String>, theme: &Theme) {
    emit(out, Fields::new().text("label", root.label.as_str()), tmpl::ROOT, theme);
    if !root.description.is_empty() {
        let d = Fields::new().text("description", root.description.as_str());
        emit(out, d, tmpl::DESCRIPTION, theme);
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
    emit(
        out,
        Fields::new()
            .text("head", head)
            .text("kind", "qos")
            .text("class", q.class.as_label())
            .text("reserve", q.admitted.compact())
            .text("cap", ztest::api::format_span(q.class.profile().hard_cap))
            .maybe_text("declared", q.declared_timeout.clone()),
        tmpl::QOS,
        theme,
    );
}

fn tags_node(out: &mut String, tags: &[String], b: &Branch, theme: &Theme) {
    let (head, _) = b.glyphs(theme);
    let f = Fields::new().text("head", head).text("kind", "tags").text("tags", tags.join(", "));
    emit(out, f, tmpl::TAGS, theme);
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
    let row = Fields::new().text("head", head).text("kind", "image");
    if !seen.insert(key) {
        emit(out, row.text("name", img.repo.as_str()).text("mark", "(*)"), tmpl::REPEAT, theme);
        return;
    }
    emit(out, row.text("repo", img.repo.as_str()).text("mark", "BUILD"), tmpl::IMAGE, theme);

    let source = describe_source(&img.source);
    let kids: Vec<(&str, String)> = match img.features.is_empty() {
        true => vec![source],
        false => vec![source, ("features", img.features.join(", "))],
    };
    leaves(out, &kids, &next, theme);
}

fn describe_source(src: &ztest::api::DevSource) -> (&'static str, String) {
    use ztest::api::DevSource;
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
    let sha8 = ztest::api::seed_sha8(&s.oid);
    let row = Fields::new().text("head", head).text("kind", "seed");
    if !seen.insert(format!("seed:{}", s.oid)) {
        emit(out, row.text("name", sha8).text("mark", "(*)"), tmpl::REPEAT, theme);
        return;
    }
    let full = row.text("name", s.name.as_str()).text("sha8", sha8).value("size", s.size as f64);
    emit(out, full, tmpl::SEED, theme);

    let pvc = format!(
        "ztest-seeds/seed-{sha8}-<driver> {}",
        ztest::api::seed_size_for(s.uncompressed_bytes)
    );
    leaves(out, &[("pvc", pvc)], &next, theme);
}

fn leaves(out: &mut String, kids: &[(&str, String)], prefix: &str, theme: &Theme) {
    let n = kids.len();
    for (i, (label, text)) in kids.iter().enumerate() {
        let b = Branch { prefix: prefix.to_string(), last: i + 1 == n };
        let (head, _) = b.glyphs(theme);
        let f = Fields::new().text("head", head).text("kind", *label).text("text", text.as_str());
        emit(out, f, tmpl::LEAF, theme);
    }
}

fn render_pruned(out: &mut String, plan: &Plan, theme: &Theme) {
    let _ = writeln!(out, "\n{}", "pruned".style(theme.styles.skip));
    let n = plan.pruned.len();
    for (i, p) in plan.pruned.iter().enumerate() {
        let b = Branch { prefix: String::new(), last: i + 1 == n };
        let (head, next) = b.glyphs(theme);
        emit(
            out,
            Fields::new()
                .text("head", head)
                .text("kind", "seed")
                .text("name", p.seed.name.as_str())
                .text("sha8", ztest::api::seed_sha8(&p.seed.oid))
                .value("size", p.seed.size as f64),
            tmpl::PRUNED,
            theme,
        );
        let by = match p.declared_by.is_empty() {
            true => "declaring test not in the dump".to_string(),
            false => p.declared_by.join(", "),
        };
        leaves(out, &[("declared by", by)], &next, theme);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ztest::api::GIB;
    use ztest::api::QosClass;
    use ztest::api::SeedPayload;
    use ztest::api::{PlanRoot, PrunedSeed, QosNode};

    fn plain() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn seed(name: &str, oid: &str, size: u64) -> SeedEntry {
        SeedEntry {
            name: name.to_string(),
            oid: oid.to_string(),
            size,
            uncompressed_bytes: 0,
            payload: SeedPayload::Archive,
            base_uri: ztest::api::storage::BASE_URI.to_string(),
            key_prefix: ztest::api::storage::KEY_PREFIX.to_string(),
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
        assert!(out.contains("qos sync · reserve 16c/16Gi · hard cap 48h · declared 48h"));
        assert!(out.contains("13.1 GiB"));
        assert!(out.contains("pruned"));
        assert!(out.contains("declared by clientless::the_pub_testnet_ironwood_boundary"));
        // No `test`/`binary` rows: structure without information
        assert!(!out.contains("├── test "));
        assert!(!out.contains("binary"));
    }

    /// A plan tree is drawn almost entirely from glyphs, so ASCII mode is the encoding
    /// most likely to shear it — and the branch corners were hardcoded until they moved
    /// into [`ThemeChars`](crate::Theme)
    #[test]
    fn the_plan_tree_falls_back_to_ascii() {
        let plan = Plan {
            roots: vec![
                root("first", vec![seed("a.tar.zst", "aaaa1111ff", GIB)]),
                root("second", vec![seed("b.tar.zst", "bbbb2222ff", 2 * GIB)]),
            ],
            pruned: vec![PrunedSeed {
                seed: seed("c.tar.zst", "cccc3333ff", GIB),
                declared_by: vec!["clientless::somewhere".into()],
            }],
        };
        let ascii = Theme::for_capabilities(false, false);
        crate::testing::assert_ascii_clean("render_plan", &render(&plan, &ascii));
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
