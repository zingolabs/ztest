//! Dylib search path (`LD_LIBRARY_PATH`) every test child inherits, from `cargo
//! nextest list` build-meta.
//!
//! - Absent → dynamically-linked libstd binaries die "exit 127"
//! - Reproduces nextest's `RustBuildMeta::dylib_paths`, existence check injected (stays pure)

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use nextest_metadata::{PlatformLibdirSummary, RustBuildMetaSummary};

pub const fn dylib_path_envvar() -> &'static str {
    if cfg!(windows) {
        "PATH"
    } else if cfg!(target_os = "macos") {
        "DYLD_FALLBACK_LIBRARY_PATH"
    } else {
        "LD_LIBRARY_PATH"
    }
}

/// Computed dirs prepended to the live [`dylib_path_envvar`] value (hits the real fs)
pub fn dylib_path_value(meta: &RustBuildMetaSummary) -> OsString {
    let inherited = std::env::var_os(dylib_path_envvar());
    join(dylib_dirs(meta, &|p| p.exists()), inherited)
}

/// Ordered, deduped search dirs, no inherited value. `exists` gates only the
/// relative linked paths (nextest parity)
fn dylib_dirs(meta: &RustBuildMetaSummary, exists: &dyn Fn(&Path) -> bool) -> Vec<PathBuf> {
    // Build-relative join base; target dir = fallback for pre-0.9.131 summaries
    let build_dir = meta.build_directory.as_ref().unwrap_or(&meta.target_directory);
    let build_dir = PathBuf::from(build_dir.as_str());

    let mut dirs: Vec<PathBuf> = Vec::new();

    for rel in &meta.linked_paths {
        let p = build_dir.join(rel.as_str());
        if exists(&p) {
            dirs.push(p);
        }
    }

    // deps subdir first (Cargo's order)
    for base in &meta.base_output_directories {
        let abs = build_dir.join(base.as_str());
        dirs.push(abs.join("deps"));
        dirs.push(abs);
    }

    // rustc libdirs (libstd)
    if let Some(platforms) = &meta.platforms {
        if let Some(p) = libdir_path(&platforms.host.libdir) {
            dirs.push(p);
        }
        if let Some(p) = platforms.targets.first().and_then(|t| libdir_path(&t.libdir)) {
            dirs.push(p);
        }
    }

    dedup_preserving_order(dirs)
}

fn libdir_path(libdir: &PlatformLibdirSummary) -> Option<PathBuf> {
    match libdir {
        PlatformLibdirSummary::Available { path } => Some(PathBuf::from(path.as_str())),
        PlatformLibdirSummary::Unavailable { .. } => None,
    }
}

fn dedup_preserving_order(dirs: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = std::collections::HashSet::new();
    dirs.into_iter().filter(|p| seen.insert(p.clone())).collect()
}

/// `dirs` prepended to `inherited`, platform separator
fn join(dirs: Vec<PathBuf>, inherited: Option<OsString>) -> OsString {
    let mut all: Vec<PathBuf> = dirs;
    if let Some(inherited) = inherited {
        all.extend(std::env::split_paths(&inherited));
    }
    // `join_paths` errors only on a path containing the separator char
    std::env::join_paths(&all)
        .unwrap_or_else(|_| all.first().map(|p| p.as_os_str().to_os_string()).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn libdir(path: &str) -> PlatformLibdirSummary {
        PlatformLibdirSummary::Available { path: path.into() }
    }

    // `PlatformSummary` not constructible from nextest-metadata → `platforms = None`,
    // libdir extraction covered directly via `libdir_path`
    fn meta(build_dir: &str, base: &[&str], linked: &[&str]) -> RustBuildMetaSummary {
        RustBuildMetaSummary {
            target_directory: build_dir.into(),
            build_directory: Some(build_dir.into()),
            base_output_directories: base.iter().map(|s| (*s).into()).collect(),
            non_test_binaries: Default::default(),
            build_script_out_dirs: Default::default(),
            build_script_info: None,
            linked_paths: linked.iter().map(|s| (*s).into()).collect::<BTreeSet<_>>(),
            platforms: None,
            target_platforms: Vec::new(),
            target_platform: None,
        }
    }

    #[test]
    fn base_output_dirs_emit_deps_then_self_in_order() {
        let m = meta("/build", &["debug"], &[]);
        let dirs = dylib_dirs(&m, &|_| true);
        assert_eq!(dirs, vec![PathBuf::from("/build/debug/deps"), PathBuf::from("/build/debug")]);
    }

    #[test]
    fn linked_paths_filtered_by_existence_and_come_first() {
        let m = meta("/build", &["debug"], &["extra/a", "extra/b"]);
        let dirs = dylib_dirs(&m, &|p| p == Path::new("/build/extra/a"));
        assert_eq!(dirs[0], PathBuf::from("/build/extra/a"));
        assert!(!dirs.contains(&PathBuf::from("/build/extra/b")));
        assert!(dirs.contains(&PathBuf::from("/build/debug/deps")));
    }

    #[test]
    fn libdir_extraction_skips_unavailable() {
        assert_eq!(libdir_path(&libdir("/rustc/lib")), Some(PathBuf::from("/rustc/lib")));
        assert_eq!(
            libdir_path(&PlatformLibdirSummary::Unavailable {
                reason: nextest_metadata::PlatformLibdirUnavailable::RUSTC_FAILED,
            }),
            None
        );
    }

    #[test]
    fn join_prepends_computed_dirs_to_inherited() {
        let joined =
            join(vec![PathBuf::from("/a"), PathBuf::from("/b")], Some(OsString::from("/sys/lib")));
        let parts: Vec<_> = std::env::split_paths(&joined).collect();
        assert_eq!(
            parts,
            vec![PathBuf::from("/a"), PathBuf::from("/b"), PathBuf::from("/sys/lib")]
        );
    }
}
