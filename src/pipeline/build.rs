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
    list_args: &[String],
    tx: &EventTx,
    lines: Option<UnboundedSender<String>>,
) -> std::io::Result<BuildOutcome> {
    let _ = tx.send(Event::BuildStarted);

    // `list_args` is already the `cargo nextest list` argv: the caller
    // (`cli::run::RunOptions`) extracted the engine-owned run-behavior flags
    // (`--retries`, `--no-fail-fast`, `--no-cleanup`, …) that `list` would reject
    // and left the selection / filter / build flags untouched.

    // ───── Pass 1: chatty compile ─────
    // Cargo's stderr (`Fetching`, `Compiling foo`, warnings, errors) is what the
    // user watches while test binaries build. stdout=null drops the
    // human-readable test listing. With `lines` set we pipe stderr and relay it
    // line-by-line into the console's scrollback; otherwise it's inherited.
    let pass1 = if let Some(lines) = lines {
        let mut child = Command::new("cargo")
            .arg("nextest")
            .arg("list")
            .args(list_args)
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
            .args(list_args)
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
    let outcome = index(list_args).await?;
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

    let (test_count, selected_binaries) = summarize_selection(&summary);
    Ok(BuildOutcome::Ok {
        test_count,
        binary_count: selected_binaries.len(),
        selected_binaries,
        summary: Box::new(summary),
    })
}

/// Walk the resolved test list once, picking out the binaries with ≥1 selected
/// test (and their selected test names) plus the total selected-test count.
///
/// A test case is *selected* when its `filter_match` `is_match`; tests filtered
/// out are present in the JSON but flagged non-matching. The binary count is
/// simply the number of returned binaries (empty suites are dropped), so the
/// caller derives it from the vec rather than a second pass. Each binary carries
/// the `cwd` nextest reports so the inventory-dump subprocess inherits the right
/// working directory, and the `<bin> --exact <name>` targets the engine runs.
fn summarize_selection(summary: &TestListSummary) -> (usize, Vec<SelectedBinary>) {
    let mut test_count = 0;
    let mut binaries = Vec::new();
    for (binary_id, suite) in &summary.rust_suites {
        let selected_tests: Vec<String> = suite
            .test_cases
            .iter()
            .filter(|(_, t)| t.filter_match.is_match())
            .map(|(name, _)| name.as_str().to_string())
            .collect();
        if selected_tests.is_empty() {
            continue;
        }
        test_count += selected_tests.len();
        binaries.push(SelectedBinary {
            binary_path: suite.binary.binary_path.as_std_path().to_path_buf(),
            cwd: suite.cwd.as_std_path().to_path_buf(),
            binary_id: binary_id.to_string(),
            selected_tests,
        });
    }
    (test_count, binaries)
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
        /// The full parsed `cargo nextest list` summary. The engine
        /// (`engine::nextest`) reconstructs an owned `TestList` from it
        /// (`TestList::from_summary`) to resolve per-test nextest config
        /// (retries, slow-timeout) and to drive nextest's reporter. Boxed
        /// because the summary dwarfs the other fields.
        summary: Box<TestListSummary>,
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
    /// Nextest's binary identifier — `<package>::<bin>` shape. Also the key
    /// the QoS dump (`pipeline::images::discover`) is grouped by, so the engine
    /// can match a test's tier to its binary.
    pub binary_id: String,
    /// Names of the selected tests in this binary (`filter_match` true) — the
    /// `<bin> --exact <name>` targets. Populated for the engine
    /// (`src/engine`); the nextest path ignores it.
    pub selected_tests: Vec<String>,
}

