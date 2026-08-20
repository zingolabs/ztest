//! Repo-wide lint for the rendering architecture.
//!
//! Every drawn surface declares a row *template* and binds data to it; the glyph and
//! colour vocabulary lives in one `ThemeChars`/`Styles` pair. Both halves are easy to
//! bypass by accident — a hardcoded `·` renders correctly on the machine that added it,
//! and an `owo_colors` import in a CLI file compiles fine. Neither shows up in a golden
//! test written in the same encoding it broke.
//!
//! So they are checked here, over the source, once, for the whole tree:
//!
//! - a template source carries no glyph: those come from `{@name}` cells
//! - colour is `ztest_ui`'s business; nothing else reaches for `owo_colors`
//!
//! Failing this means the surface, not the lint, is wrong — add the role to
//! `ThemeChars` and spell it `{@role}`.

use std::path::{Path, PathBuf};

/// `src/engine/reporter.rs` reproduces `cargo nextest`'s own reporter byte for byte,
/// including its magenta/cyan/blue — nextest's roles, not ztest's. Deliberate, and the
/// one exception.
const COLOUR_ALLOWED: [&str; 1] = ["src/engine/reporter.rs"];

fn rust_sources() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for e in entries.flatten() {
            let p = e.path();
            match p.is_dir() {
                true => walk(&p, out),
                false if p.extension().is_some_and(|x| x == "rs") => out.push(p),
                false => {}
            }
        }
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for dir in ["src", "ui/src", "cli/src", "macros/src", "attr/src"] {
        walk(&root.join(dir), &mut out);
    }
    out.sort();
    out
}

fn rel(p: &Path) -> String {
    p.strip_prefix(env!("CARGO_MANIFEST_DIR")).unwrap_or(p).display().to_string()
}

/// Template modules, by the three names the tree uses. A surface that invents a fourth
/// escapes this lint — which is why the name is a convention worth keeping.
const TEMPLATE_MODS: [&str; 3] = ["mod row {", "mod tmpl {", "mod transfer_row {"];

/// Every string literal declared inside a template module, with its 1-based line.
///
/// Brace counting would have to understand `{key}` and `[group]`, so the block instead
/// ends at the first line that is exactly `}` at the module's own indentation — which is
/// what `rustfmt` guarantees for these modules.
fn template_literals(src: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut inside = false;
    for (i, line) in src.lines().enumerate() {
        let t = line.trim();
        if !inside {
            inside = TEMPLATE_MODS.iter().any(|m| t.starts_with(m));
            continue;
        }
        if t == "}" {
            inside = false;
            continue;
        }
        // Doc comments describe the row; they are not drawn
        if !t.starts_with("//") {
            out.extend(string_literals(line).into_iter().map(|l| (i + 1, l)));
        }
    }
    out
}

/// Contents of every double-quoted run on a line
fn string_literals(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur: Option<String> = None;
    let mut escaped = false;
    for c in line.chars() {
        match (&mut cur, c) {
            (Some(s), _) if escaped => {
                s.push(c);
                escaped = false;
            }
            (Some(_), '\\') => escaped = true,
            (Some(_), '"') => out.push(cur.take().unwrap_or_default()),
            (Some(s), _) => s.push(c),
            (None, '"') => cur = Some(String::new()),
            (None, _) => {}
        }
    }
    out
}

/// A template's glyphs come from `{@role}` cells, which resolve per encoding. A literal
/// one renders as itself on a terminal that cannot draw it.
#[test]
fn no_template_hardcodes_a_glyph() {
    let mut bad = Vec::new();
    for path in rust_sources() {
        let name = rel(&path);
        let Ok(src) = std::fs::read_to_string(&path) else { continue };
        for (line, lit) in template_literals(&src) {
            let glyphs: Vec<char> = lit.chars().filter(|c| !c.is_ascii()).collect();
            if !glyphs.is_empty() {
                bad.push(format!("  {name}:{line}  {glyphs:?} in {lit:?}"));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "template sources carry hardcoded glyphs — add the role to `ThemeChars` and spell \
         it `{{@role}}`:\n{}",
        bad.join("\n")
    );
}

/// The lint is only worth its lines if it is actually reading the templates it claims to.
/// A refactor that renames the module or reindents the block would otherwise turn it into
/// a test that passes by looking at nothing.
#[test]
fn the_glyph_lint_actually_finds_the_templates() {
    let found: usize = rust_sources()
        .iter()
        .filter_map(|p| std::fs::read_to_string(p).ok())
        .map(|src| template_literals(&src).len())
        .sum();
    assert!(found > 50, "only {found} template literals found — the block scan has drifted");
}

/// One palette. A renderer asks the theme for a role; it never names a colour.
#[test]
fn colour_stays_inside_ztest_ui() {
    let mut bad = Vec::new();
    for path in rust_sources() {
        let name = rel(&path);
        if name.starts_with("ui/src/") || COLOUR_ALLOWED.contains(&name.as_str()) {
            continue;
        }
        let Ok(src) = std::fs::read_to_string(&path) else { continue };
        if let Some(line) = src.lines().position(|l| l.trim_start().starts_with("use owo_colors")) {
            bad.push(format!("  {name}:{}", line + 1));
        }
    }
    assert!(
        bad.is_empty(),
        "colour belongs to `ztest_ui` — style through `Theme`, or draw with a `{{k|tone}}` \
         cell:\n{}",
        bad.join("\n")
    );
}
