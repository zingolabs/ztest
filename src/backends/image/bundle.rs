//! Canonical build-context serialization — the single source of truth for both
//! a dev image's identity and the artifact the build consumes.
//!
//! [`pack`] walks a build context once and produces the uncompressed tar that
//! the on-cluster buildah build consumes (unpacked into the build pod), with the
//! chosen Dockerfile staged at the root. A dev image's `dev-<hash>` tag derives
//! from this tar's digest, and the very same bytes are what the build reads — so
//! the tag can never name a context the archive didn't stage. That shared
//! identity is the whole reason both come from one function.
//!
//! Three rules shape the tar, each forced by a downstream requirement:
//!
//!   - **Directory and regular-file entries only.** A symlink is *dereferenced*
//!     to a regular file at its own path when its target is a regular file, and
//!     otherwise skipped — so a broken or non-file symlink (a dangling nix
//!     `result`) is dropped rather than aborting the pack, the exact break a raw
//!     `tar -h` used to cause.
//!   - **`.dockerignore` governs membership**, matched with Docker's own
//!     semantics, so the archive equals what the docker-strategy build sees.
//!     [`DEFAULT_IGNORES`] always applies on top, even with no `.dockerignore`.
//!   - **Deterministic bytes** — sorted paths, zeroed mtime/uid/gid, normalized
//!     mode — so identical trees yield an identical digest.

use std::path::Path;

use sha2::{Digest, Sha256};

use super::ImageError;

/// Names always excluded, even without a `.dockerignore`: build output and
/// VCS / JS-dependency trees that are huge and never part of a source build.
pub(crate) const DEFAULT_IGNORES: [&str; 3] = ["target", ".git", "node_modules"];

/// Where the Dockerfile is staged inside the archive — `buildah bud -f
/// Dockerfile`, and the one path that is never subject to `.dockerignore`.
const DOCKERFILE: &str = "Dockerfile";

/// A packed build context: the tar bundle and its content digest.
pub(crate) struct Bundle {
    /// Uncompressed tar of the `.dockerignore`-filtered context with the chosen
    /// Dockerfile staged at the root. Directory and regular-file entries only.
    pub tar: Vec<u8>,
    /// Lowercase-hex SHA-256 of [`tar`](Self::tar): the bundle's content address.
    pub digest: String,
}

/// Serialize `context` — with `dockerfile` staged at the root as `Dockerfile` —
/// into a deterministic source-bundle tar. See the module docs for the rules.
pub(crate) fn pack(context: &Path, dockerfile: &Path) -> Result<Bundle, ImageError> {
    let ignore = Ignore::load(context);
    let mut entries = collect(context, &ignore)?;

    // ztest's chosen Dockerfile always wins over any the context carries.
    let dockerfile = std::fs::read(dockerfile).map_err(|err| ImageError::ReadFile {
        path: dockerfile.to_path_buf(),
        err,
    })?;
    entries.retain(|e| e.path != DOCKERFILE);
    entries.push(Entry {
        path: DOCKERFILE.to_string(),
        kind: Kind::File(dockerfile),
    });
    entries.sort_by(|a, b| a.path.cmp(&b.path));

    let tar = write_tar(&entries)?;
    let digest = hex::encode(Sha256::digest(&tar));
    Ok(Bundle { tar, digest })
}

enum Kind {
    Dir,
    File(Vec<u8>),
}

struct Entry {
    /// Context-relative path with `/` separators; the entry's name in the tar.
    path: String,
    kind: Kind,
}

/// Walk `context`, applying `ignore` and the symlink policy, into unsorted
/// entries. Only the built-in heavy directories are pruned; everything else is
/// filtered per entry so a `.dockerignore` `!` line can re-include a file under
/// an excluded directory (Docker allows this — see [`Ignore`]).
fn collect(context: &Path, ignore: &Ignore) -> Result<Vec<Entry>, ImageError> {
    let mut entries = Vec::new();
    let walk = walkdir::WalkDir::new(context)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| match e.path().strip_prefix(context) {
            Ok(rel) => !(e.file_type().is_dir() && is_built_in_ignore(rel)),
            // The context root itself — always descend.
            Err(_) => true,
        });

    for entry in walk {
        let entry = entry.map_err(|e| ImageError::Walk(e.to_string()))?;
        let rel = match entry.path().strip_prefix(context) {
            Ok(rel) if !rel.as_os_str().is_empty() => rel,
            _ => continue,
        };
        if ignore.excludes(rel, entry.file_type().is_dir()) {
            continue;
        }
        let path = rel.to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/");
        let file_type = entry.file_type();

        if file_type.is_dir() {
            entries.push(Entry { path, kind: Kind::Dir });
        } else if file_type.is_file() {
            entries.push(Entry { path, kind: Kind::File(read(entry.path())?) });
        } else if file_type.is_symlink() {
            // Only a symlink to a regular file can become a bundle entry; its
            // target bytes are inlined at the link's own path. Anything else —
            // a broken link, or a link to a directory / device — is dropped, so
            // a stale symlink in the context can never abort the build.
            match std::fs::metadata(entry.path()) {
                Ok(target) if target.is_file() => {
                    entries.push(Entry { path, kind: Kind::File(read(entry.path())?) });
                }
                Ok(_) => tracing::debug!(link = %path, "bundle: skipping symlink to non-file"),
                Err(_) => tracing::debug!(link = %path, "bundle: skipping broken symlink"),
            }
        }
    }
    Ok(entries)
}

fn read(path: &Path) -> Result<Vec<u8>, ImageError> {
    std::fs::read(path).map_err(|err| ImageError::ReadFile {
        path: path.to_path_buf(),
        err,
    })
}

/// Emit `entries` (already sorted) as a reproducible tar: GNU headers with a
/// zeroed mtime/uid/gid and a normalized mode, so identical trees are identical
/// bytes regardless of who packed them or when.
fn write_tar(entries: &[Entry]) -> Result<Vec<u8>, ImageError> {
    let assemble = |e: &str| ImageError::Bundle(e.to_string());
    let mut builder = tar::Builder::new(Vec::new());
    for entry in entries {
        let mut header = tar::Header::new_gnu();
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        match &entry.kind {
            Kind::Dir => {
                header.set_entry_type(tar::EntryType::Directory);
                header.set_mode(0o755);
                header.set_size(0);
                builder
                    .append_data(&mut header, format!("{}/", entry.path), std::io::empty())
                    .map_err(|e| assemble(&e.to_string()))?;
            }
            Kind::File(bytes) => {
                header.set_entry_type(tar::EntryType::Regular);
                header.set_mode(0o644);
                header.set_size(bytes.len() as u64);
                builder
                    .append_data(&mut header, &entry.path, bytes.as_slice())
                    .map_err(|e| assemble(&e.to_string()))?;
            }
        }
    }
    builder.into_inner().map_err(|e| assemble(&e.to_string()))
}

/// Whether any component of `rel` is a [`DEFAULT_IGNORES`] name — ztest's
/// built-in safety net, excluded at *any* depth (a nested `target/` is still
/// build output). Distinct from `.dockerignore` patterns, which follow Docker's
/// root-anchored semantics.
fn is_built_in_ignore(rel: &Path) -> bool {
    rel.components().any(|c| {
        c.as_os_str()
            .to_str()
            .is_some_and(|s| DEFAULT_IGNORES.contains(&s))
    })
}

/// The context's `.dockerignore` patterns, matched with Docker's semantics:
/// anchored to the context root, `*` does not cross `/`, a trailing `/` matches
/// directories only, `!` re-includes, the last matching pattern wins, and a
/// match on an ancestor directory excludes its children. Layered on top of the
/// depth-agnostic [`is_built_in_ignore`] safety net.
struct Ignore {
    patterns: Vec<Pattern>,
}

struct Pattern {
    matcher: globset::GlobMatcher,
    /// A `!`-prefixed line: a match re-includes rather than excludes.
    negated: bool,
    /// A trailing-`/` line: matches directories only.
    dir_only: bool,
}

impl Ignore {
    fn load(context: &Path) -> Ignore {
        let patterns = std::fs::read_to_string(context.join(".dockerignore"))
            .map(|text| text.lines().filter_map(Pattern::parse).collect())
            .unwrap_or_default();
        Ignore { patterns }
    }

    /// Whether the context-relative path `rel` is excluded from the bundle.
    /// `is_dir` gates directory-only (`foo/`) patterns.
    fn excludes(&self, rel: &Path, is_dir: bool) -> bool {
        // The build always needs these two, whatever the patterns say.
        if rel == Path::new(DOCKERFILE) || rel == Path::new(".dockerignore") {
            return false;
        }
        if is_built_in_ignore(rel) {
            return true;
        }
        let mut excluded = false;
        for p in &self.patterns {
            if p.matches(rel, is_dir) {
                excluded = !p.negated;
            }
        }
        excluded
    }
}

impl Pattern {
    fn parse(line: &str) -> Option<Pattern> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (negated, body) = match line.strip_prefix('!') {
            Some(rest) => (true, rest.trim()),
            None => (false, line),
        };
        let dir_only = body.ends_with('/');
        // A leading `/` or `./` is a no-op — every pattern is root-anchored — and
        // a trailing `/` only marked dir-only. Strip them to the bare glob.
        let body = body.trim_matches('/');
        let body = body.strip_prefix("./").unwrap_or(body);
        if body.is_empty() {
            return None;
        }
        let matcher = globset::GlobBuilder::new(body)
            .literal_separator(true)
            .build()
            .ok()?
            .compile_matcher();
        Some(Pattern { matcher, negated, dir_only })
    }

    fn matches(&self, rel: &Path, is_dir: bool) -> bool {
        if self.matcher.is_match(rel) {
            return !self.dir_only || is_dir;
        }
        // A match on an ancestor directory excludes the entry beneath it.
        // Ancestors are directories, so dir-only patterns apply to them.
        rel.ancestors()
            .skip(1)
            .take_while(|a| !a.as_os_str().is_empty())
            .any(|a| self.matcher.is_match(a))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A throwaway build context on disk, cleaned up on drop.
    struct Ctx {
        dir: PathBuf,
    }

    impl Ctx {
        fn new() -> Ctx {
            static SEQ: AtomicU32 = AtomicU32::new(0);
            let n = SEQ.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("ztest-bundle-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("Dockerfile"), "FROM scratch\n").unwrap();
            Ctx { dir }
        }

        fn write(&self, rel: &str, bytes: &[u8]) {
            let path = self.dir.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }

        fn pack(&self) -> Bundle {
            pack(&self.dir, &self.dir.join("Dockerfile")).unwrap()
        }
    }

    impl Drop for Ctx {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Every bundle carries the staged Dockerfile at the root.
    #[test]
    fn stages_the_dockerfile() {
        let c = Ctx::new();
        assert!(tar_paths(&c.pack().tar).contains(&"Dockerfile".to_string()));
    }

    /// Identical trees pack to identical bytes — the property the content
    /// address (and registry dedup) rests on.
    #[test]
    fn identical_trees_are_byte_identical() {
        let a = Ctx::new();
        a.write("src/main.rs", b"fn main() {}");
        let b = Ctx::new();
        b.write("src/main.rs", b"fn main() {}");
        assert_eq!(a.pack().tar, b.pack().tar);
    }

    /// `DEFAULT_IGNORES` drops build output with no `.dockerignore` at all.
    #[test]
    fn default_ignores_exclude_build_output() {
        let c = Ctx::new();
        c.write("keep.rs", b"x");
        c.write("target/junk", b"huge");
        c.write("nested/target/junk", b"huge");
        let paths = tar_paths(&c.pack().tar);
        assert!(paths.contains(&"keep.rs".to_string()));
        assert!(!paths.iter().any(|p| p.contains("junk")), "{paths:?}");
    }

    /// A broken symlink is skipped, not fatal — the `result` regression.
    #[test]
    #[cfg(unix)]
    fn broken_symlink_is_skipped() {
        let c = Ctx::new();
        c.write("real.rs", b"x");
        std::os::unix::fs::symlink("/nonexistent/target", c.dir.join("dangling")).unwrap();
        let paths = tar_paths(&c.pack().tar);
        assert!(paths.contains(&"real.rs".to_string()));
        assert!(!paths.contains(&"dangling".to_string()), "{paths:?}");
    }

    /// A symlink to a regular file is materialized as that file's contents at
    /// the link's own path (the `CLAUDE.md -> AGENTS.md` case), and never as a
    /// symlink entry (which the build pod's unpack would mishandle).
    #[test]
    #[cfg(unix)]
    fn symlink_to_file_is_dereferenced() {
        let c = Ctx::new();
        c.write("AGENTS.md", b"the docs");
        std::os::unix::fs::symlink("AGENTS.md", c.dir.join("CLAUDE.md")).unwrap();
        let bytes = read_entry(&c.pack().tar, "CLAUDE.md").expect("CLAUDE.md present");
        assert_eq!(bytes, b"the docs");
    }

    /// `.dockerignore` semantics: root-anchored globs, `**/` for depth, `!`
    /// re-inclusion, and dir-only trailing `/`.
    #[test]
    fn dockerignore_semantics() {
        let c = Ctx::new();
        c.write(".dockerignore", b"*.log\n**/*.tmp\nbuild/\n!build/keep\nresult\n");
        c.write("app.log", b"x"); // *.log at root → excluded
        c.write("logs/deep.log", b"x"); // *.log does not cross `/` → kept
        c.write("a/b/c.tmp", b"x"); // **/*.tmp → excluded at depth
        c.write("build/out", b"x"); // build/ dir → excluded
        c.write("build/keep", b"x"); // re-included by negation
        c.write("result", b"x"); // nix artifact → excluded
        c.write("keep.rs", b"x");

        let paths = tar_paths(&c.pack().tar);
        let has = |p: &str| paths.contains(&p.to_string());
        assert!(has("keep.rs") && has("logs/deep.log"));
        assert!(has("build/keep"), "negation must re-include: {paths:?}");
        assert!(!has("app.log") && !has("a/b/c.tmp") && !has("build/out") && !has("result"),
            "{paths:?}");
    }

    /// The Dockerfile is immune to `.dockerignore`, and ztest's staged copy wins
    /// over one carried in the context.
    #[test]
    fn dockerfile_is_never_excluded_and_ztest_wins() {
        let c = Ctx::new();
        c.write(".dockerignore", b"Dockerfile\n");
        std::fs::write(c.dir.join("Dockerfile"), "FROM context-copy\n").unwrap();
        let df = c.dir.join("chosen.Dockerfile");
        std::fs::write(&df, "FROM ztest-chosen\n").unwrap();

        let bytes = read_entry(&pack(&c.dir, &df).unwrap().tar, "Dockerfile").expect("Dockerfile");
        assert_eq!(bytes, b"FROM ztest-chosen\n");
    }

    /// Read the entry names from a tar (regular files and directories).
    fn tar_paths(tar: &[u8]) -> Vec<String> {
        tar::Archive::new(tar)
            .entries()
            .unwrap()
            .map(|e| {
                e.unwrap()
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .trim_end_matches('/')
                    .to_string()
            })
            .collect()
    }

    fn read_entry(tar: &[u8], want: &str) -> Option<Vec<u8>> {
        use std::io::Read;
        for entry in tar::Archive::new(tar).entries().unwrap() {
            let mut entry = entry.unwrap();
            if entry.path().unwrap().to_string_lossy() == want {
                let mut buf = Vec::new();
                entry.read_to_end(&mut buf).unwrap();
                return Some(buf);
            }
        }
        None
    }
}
