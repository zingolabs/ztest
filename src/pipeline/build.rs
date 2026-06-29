//! Phase B — `cargo nextest list`, two-step.
//!
//! Splitting the inventory work into two cargo invocations is the
//! pragmatic compromise between "show the user cargo's compile output"
//! and "parse a stable JSON test list":
//!
//! 1. **Compile pass**: `cargo nextest list [args]` with stderr
//!    inherited and stdout discarded. Cargo's stderr (`Fetching`,
//!    `Compiling foo v0.1.0`, warnings, errors) flows directly to
//!    the user's terminal — which is exactly what they want to see
//!    while the test binaries build. The stdout text listing is
//!    thrown away to keep the scroll region clean.
//!
//! 2. **Index pass**: `cargo nextest list --message-format=json [args]`
//!    with stdout piped and stderr discarded. The compile cache is
//!    warm from pass 1 so this is sub-second; cargo emits no progress
//!    in `--message-format=json` mode anyway. We parse the JSON to
//!    extract the resolved test selection.
//!
//! On warm caches pass 1 is silent too — cargo has nothing to compile
//! and metadata resolution doesn't produce stderr. The banner's
//! `Inventory` row carries the rolled-up status either way.
//!
//! ## Event timeline
//!
//! - [`Event::BuildStarted`] — emitted at function entry, before pass 1.
//! - [`Event::BuildIndexing`] — emitted between passes, after pass 1 OK.
//! - [`Event::BuildComplete`] — emitted on pass 2 OK with counts.
//! - [`Event::BuildFailed`] — emitted on either pass non-zero exit, with
//!   the [`BuildStage`] indicating which pass failed.

use std::process::Stdio;

use nextest_metadata::TestListSummary;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedSender;

use crate::preflight::BuildStage;

use super::events::{Event, EventTx};

/// Run the two-step `cargo nextest list` pipeline and emit lifecycle
/// [`Event`]s.
///
/// When `lines` is `Some`, pass 1's stderr is piped and each line is forwarded
/// there (so the unified bottom console can relay `Compiling …` into native
/// scrollback above its panel); when `None`, stderr is inherited as before
/// (the non-TTY / linear path). The JSON pass is always captured.
pub async fn run(
    nextest_args: &[String],
    tx: &EventTx,
    lines: Option<UnboundedSender<String>>,
) -> std::io::Result<BuildOutcome> {
    let _ = tx.send(Event::BuildStarted);

    // Strip run-only flags before forwarding to `cargo nextest list`.
    // The user types one arg vector aimed at `cargo nextest run`; the
    // `list` subcommand rejects unknown flags with a clap error and
    // would abort the whole preflight before any test compiles. We
    // drop only the well-known run-only flags — anything we don't
    // recognise is left in place, matching the conservative shape of
    // [`super::super::cli::args_peek`].
    let list_args = strip_run_only_flags(nextest_args);

    // ───── Pass 1: chatty compile ─────
    // Cargo's stderr (`Fetching`, `Compiling foo`, warnings, errors) is what the
    // user watches while test binaries build. stdout=null drops the
    // human-readable test listing. With `lines` set we pipe stderr and relay it
    // line-by-line into the console's scrollback; otherwise it's inherited.
    let pass1 = if let Some(lines) = lines {
        let mut child = Command::new("cargo")
            .arg("nextest")
            .arg("list")
            .args(&list_args)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        if let Some(stderr) = child.stderr.take() {
            let mut reader = BufReader::new(stderr).lines();
            while let Some(line) = reader.next_line().await? {
                let _ = lines.send(line);
            }
        }
        child.wait().await?
    } else {
        Command::new("cargo")
            .arg("nextest")
            .arg("list")
            .args(&list_args)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .await?
    };

    if !pass1.success() {
        let exit_code = pass1.code().unwrap_or(-1);
        let _ = tx.send(Event::BuildFailed {
            exit_code,
            stage: BuildStage::Compile,
        });
        return Ok(BuildOutcome::Failed {
            exit_code,
            stage: BuildStage::Compile,
        });
    }

    let _ = tx.send(Event::BuildIndexing);
    let outcome = index(&list_args).await?;
    match &outcome {
        BuildOutcome::Ok {
            test_count,
            binary_count,
            ..
        } => {
            let _ = tx.send(Event::BuildComplete {
                test_count: *test_count,
                binary_count: *binary_count,
            });
        }
        BuildOutcome::Failed { exit_code, stage } => {
            let _ = tx.send(Event::BuildFailed {
                exit_code: *exit_code,
                stage: *stage,
            });
        }
    }
    Ok(outcome)
}

/// The `cargo nextest list` arguments, with run-only flags stripped — the
/// caller (the unified console) runs pass 1 (`cargo nextest list <args>`) under
/// a PTY itself, then calls [`index`] for pass 2.
pub fn list_args(nextest_args: &[String]) -> Vec<String> {
    strip_run_only_flags(nextest_args)
}

/// Pass 2 — the silent JSON inventory parse. stdout is piped (we capture the
/// JSON); stderr is dropped (`--message-format=json` suppresses progress and the
/// compile cache is warm from pass 1). Returns [`BuildOutcome::Ok`] with the
/// resolved selection, [`BuildOutcome::Failed`] on a non-zero exit, or `Err` on
/// unparseable JSON.
pub async fn index(list_args: &[String]) -> std::io::Result<BuildOutcome> {
    let pass2 = Command::new("cargo")
        .arg("nextest")
        .arg("list")
        .arg("--message-format=json")
        .args(list_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await?;

    if !pass2.status.success() {
        return Ok(BuildOutcome::Failed {
            exit_code: pass2.status.code().unwrap_or(-1),
            stage: BuildStage::Index,
        });
    }

    let summary: TestListSummary = serde_json::from_slice(&pass2.stdout).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("failed to parse `cargo nextest list` JSON: {err}"),
        )
    })?;

    let (test_count, binary_count) = count_selected(&summary);
    let selected_binaries = collect_selected_binaries(&summary);
    Ok(BuildOutcome::Ok {
        test_count,
        binary_count,
        selected_binaries,
    })
}

/// Pick out the binaries that have ≥1 selected test, paired with their
/// nextest-reported `cwd` so the dump subprocess inherits the right
/// working directory.
fn collect_selected_binaries(summary: &TestListSummary) -> Vec<SelectedBinary> {
    let mut out = Vec::new();
    for (binary_id, suite) in &summary.rust_suites {
        let has_selected = suite.test_cases.values().any(|t| t.filter_match.is_match());
        if !has_selected {
            continue;
        }
        out.push(SelectedBinary {
            binary_path: suite.binary.binary_path.as_std_path().to_path_buf(),
            cwd: suite.cwd.as_std_path().to_path_buf(),
            binary_id: binary_id.to_string(),
        });
    }
    out
}

/// Outcome of one Phase-B run, used by `ztest run` to decide whether
/// to proceed to the cargo-nextest-run step.
#[derive(Debug, Clone)]
pub enum BuildOutcome {
    Ok {
        test_count: usize,
        binary_count: usize,
        /// Test binaries with at least one selected test, in the order
        /// nextest reported them. The image-inventory phase spawns each
        /// of these with `ZTEST_DUMP_INVENTORY=1`.
        selected_binaries: Vec<SelectedBinary>,
    },
    Failed {
        exit_code: i32,
        stage: BuildStage,
    },
}

/// One test binary with at least one selected test. Carries the
/// information the image-inventory phase needs to spawn the dump.
#[derive(Debug, Clone)]
pub struct SelectedBinary {
    /// Absolute path to the test binary on disk.
    pub binary_path: std::path::PathBuf,
    /// Working directory nextest would run this binary in. Used so
    /// the `dev!` macro's compile-time absolute paths and the
    /// dump-time `current_dir` agree.
    pub cwd: std::path::PathBuf,
    /// Nextest's binary identifier — `<package>::<bin>` shape. Just
    /// for logging; the path is the load-bearing field.
    pub binary_id: String,
}

/// Count tests in the resolved selection.
///
/// The JSON schema separates `rust_suites` (per binary) from the
/// `test_cases` map within each suite. A test case is *selected*
/// when its `filter_match` is `is_match: true`; tests skipped by the
/// filter are present in the JSON but flagged as non-matching.
///
/// Binary count is the number of suites with at least one selected
/// test — empty suites (every test filtered out) don't show up in
/// nextest's "binaries" tally.
/// Run-only `cargo nextest run` flags that `cargo nextest list`
/// rejects. Drop these from Phase B's invocation so the user can
/// still forward them to `cargo nextest run` via the normal
/// pass-through.
const RUN_ONLY_VALUE_FLAGS: &[&str] = &["--test-threads", "-j", "--retries"];
const RUN_ONLY_BOOL_FLAGS: &[&str] = &["--no-fail-fast", "--fail-fast"];

/// Return a copy of `args` with the known run-only flags removed.
/// Handles both `--flag value` and `--flag=value` shapes for the
/// value-bearing ones. Stops scanning at `--` — anything past it is
/// nextest filter positionals, never a flag.
fn strip_run_only_flags(args: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--" {
            // Pass through everything from `--` onwards verbatim.
            out.extend(args[i..].iter().cloned());
            break;
        }
        if RUN_ONLY_BOOL_FLAGS.contains(&arg.as_str()) {
            i += 1;
            continue;
        }
        if let Some((flag, _)) = arg.split_once('=')
            && (RUN_ONLY_VALUE_FLAGS.contains(&flag) || RUN_ONLY_BOOL_FLAGS.contains(&flag))
        {
            i += 1;
            continue;
        }
        if RUN_ONLY_VALUE_FLAGS.contains(&arg.as_str()) {
            // Skip the flag and the following value (if any).
            i += if i + 1 < args.len() { 2 } else { 1 };
            continue;
        }
        out.push(arg.clone());
        i += 1;
    }
    out
}

fn count_selected(summary: &TestListSummary) -> (usize, usize) {
    let mut tests = 0usize;
    let mut binaries = 0usize;
    for suite in summary.rust_suites.values() {
        let selected_in_suite = suite
            .test_cases
            .values()
            .filter(|t| t.filter_match.is_match())
            .count();
        if selected_in_suite > 0 {
            binaries += 1;
            tests += selected_in_suite;
        }
    }
    (tests, binaries)
}
