//! Controllable child processes for the executor tests, played by *this* binary.
//!
//! - [`spawn_test`](super::local_runner::spawn_test) needs a child whose exit code, output
//!   and timing the test dictates. Writing a `#!/bin/sh` file and pointing `binary_path` at
//!   it cannot be made safe: `execve` refuses with `ETXTBSY` while any process holds a write
//!   handle to the inode, and a `fork` on *any* thread copies the writer's fd table. Closing
//!   our handle before the rename does not help — the inode survives the rename, and the
//!   inherited copy lives until that child execs
//! - So no file is written. `binary_path` is `current_exe()` and the behaviour is chosen by
//!   the test *name*, which is what [`build_command`](super::local_runner) already puts on
//!   argv. Go's `os/exec` suite settled on the same shape (`TestHelperProcess`)
//! - Second gain: the child is a real libtest binary, so `--exact <name> --nocapture` is
//!   exercised for real. Fixtures that ignore argv cover none of that protocol
//!
//! Every helper prints a marker before doing anything else. A mistyped `--exact` matches no
//! test and libtest exits 0 — indistinguishable from a passing child unless the caller
//! asserts the marker, so the callers do.

use std::path::PathBuf;
use std::process::exit;
use std::time::Duration;

/// Carried in `NEXTEST_RUN_ID`, which [`build_command`](super::local_runner) already
/// forwards. A real run's id is generated, so it can never collide — and outside a fixture
/// spawn (a plain `cargo test`, or this suite running under `ztest run`) the helpers below
/// see someone else's id and no-op instead of sleeping 30 s in an unrelated run
pub const RUN_ID: &str = "ztest-fixture-child";

/// Module path libtest matches `--exact` against
const PREFIX: &str = "engine::fixture_child";

/// True when this process is a fixture child rather than the suite itself
fn playing() -> bool {
    std::env::var("NEXTEST_RUN_ID").as_deref() == Ok(RUN_ID)
}

/// `--exact` argument selecting `name`
pub fn test_name(name: &str) -> String {
    format!("{PREFIX}::{name}")
}

/// The binary to spawn: the running test binary itself, never a file we wrote
pub fn exe() -> PathBuf {
    std::env::current_exe().expect("current_exe")
}

#[test]
fn exits_zero() {
    if !playing() {
        return;
    }
    println!("zero-ok");
    exit(0);
}

/// Distinct from libtest's own 101 and from a shell's 1, so `Verdict::Fail` is proved to
/// carry the child's real status rather than a constant
#[test]
fn exits_three() {
    if !playing() {
        return;
    }
    println!("three-ok");
    exit(3);
}

#[test]
fn prints_stdout() {
    if !playing() {
        return;
    }
    println!("hello-stdout");
    exit(0);
}

#[test]
fn prints_ztest_engine() {
    if !playing() {
        return;
    }
    println!("ENGINE=[{}]", std::env::var("ZTEST_ENGINE").unwrap_or_default());
    exit(0);
}

#[test]
fn prints_image_refs() {
    if !playing() {
        return;
    }
    println!("REFS={}", std::env::var(crate::backends::image::IMAGE_REFS_ENV).unwrap_or_default());
    exit(0);
}

/// Writes the `Error:` line to *stderr* and the rest to stdout, then fails for real, so the
/// frame `strip_libtest_frame` has to survive is libtest's own and not a hand-typed copy
#[test]
fn fails_with_an_error_line() {
    if !playing() {
        return;
    }
    println!("INFO provisioning component");
    eprintln!("Error: image build failed for zainod");
    panic!("image build failed");
}

#[test]
fn sleeps_past_any_cap() {
    if !playing() {
        return;
    }
    println!("entering-hang");
    std::thread::sleep(Duration::from_secs(30));
    exit(0);
}

#[test]
fn alpha() {
    if !playing() {
        return;
    }
    std::thread::sleep(Duration::from_millis(60));
    println!("alpha-ok");
    exit(0);
}

#[test]
fn beta() {
    if !playing() {
        return;
    }
    std::thread::sleep(Duration::from_millis(60));
    println!("beta-ok");
    exit(0);
}

#[test]
fn gamma() {
    if !playing() {
        return;
    }
    eprintln!("kaboom-from-gamma");
    exit(1);
}

#[test]
fn prints_small_ok() {
    if !playing() {
        return;
    }
    println!("small-ok");
    exit(0);
}

#[test]
fn never_admitted() {
    if !playing() {
        return;
    }
    println!("should-never-run");
    exit(0);
}

#[test]
fn prints_queued_ran() {
    if !playing() {
        return;
    }
    println!("queued-ran");
    exit(0);
}

/// `PREFIX` is hand-written because libtest names have no crate root while `module_path!`
/// does. If this module ever moves, every fixture spawn would select *no* test and libtest
/// would exit 0 — the marker assertions catch that, but this names the cause directly
#[test]
fn the_prefix_matches_this_modules_real_path() {
    let without_crate = module_path!().split_once("::").expect("crate-rooted").1;
    assert_eq!(PREFIX, without_crate);
}

/// Fails its first attempt and passes the second. The counter lives in the child's *cwd*,
/// which `build_command` sets from `WorkItem::cwd` — so the fixture's own scratch dir
/// carries it, with no path to smuggle through the environment
#[test]
fn flakes_once_then_recovers() {
    if !playing() {
        return;
    }
    let counter = std::path::Path::new("attempts");
    let n: u32 =
        std::fs::read_to_string(counter).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0) + 1;
    std::fs::write(counter, n.to_string()).expect("write attempt counter");
    if n < 2 {
        eprintln!("flaked-on-{n}");
        exit(1);
    }
    println!("recovered");
    exit(0);
}
