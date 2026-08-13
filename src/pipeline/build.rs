//! Phase B: `cargo nextest list` in two passes — a chatty compile pass (stderr
//! inherited/relayed, stdout dropped), then a silent JSON index pass (stdout captured) →
//! the user sees compile output and the test list still parses off a warm cache

use std::process::Stdio;

use nextest_metadata::TestListSummary;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedSender;

use crate::ui::BuildStage;

use super::events::{Event, EventTx};

/// `lines` set → pass 1's stderr is piped and forwarded there, so the bottom console can
/// relay `Compiling …` into the scrollback above its panel; `None` → inherited (non-TTY).
/// The JSON pass is always captured
pub async fn run(
    list_args: &[String],
    tx: &EventTx,
    lines: Option<UnboundedSender<String>>,
) -> std::io::Result<BuildOutcome> {
    let _ = tx.send(Event::BuildStarted);

    // `list_args` is already the `cargo nextest list` argv — the caller stripped the
    // run-behavior flags (`--retries`, …) `list` would reject

    // Pass 1: chatty compile. `lines` set → stderr piped and relayed into scrollback
    // line-by-line; else inherited
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
        let _ = tx.send(Event::BuildFailed { exit_code, stage: BuildStage::Compile });
        return Ok(BuildOutcome::Failed { exit_code, stage: BuildStage::Compile });
    }

    let _ = tx.send(Event::BuildIndexing);
    let outcome = index(list_args).await?;
    match &outcome {
        BuildOutcome::Ok { test_count, binary_count, .. } => {
            let _ = tx.send(Event::BuildComplete {
                test_count: *test_count,
                binary_count: *binary_count,
            });
        }
        BuildOutcome::Failed { exit_code, stage } => {
            let _ = tx.send(Event::BuildFailed { exit_code: *exit_code, stage: *stage });
        }
    }
    Ok(outcome)
}

/// Pass 2. stderr dropped (`--message-format=json` suppresses progress, cache warm from
/// pass 1). `Err` only for unparseable JSON
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

    parse_list_summary(&pass2.stdout)
}

/// Split from [`index`] so the on-cluster path ([`crate::pipeline::remote_compile`]) reuses
/// this parse on a builder pod's JSON, where `binary_path`/`cwd` are the pod's paths
pub fn parse_list_summary(stdout: &[u8]) -> std::io::Result<BuildOutcome> {
    let summary: TestListSummary = serde_json::from_slice(stdout).map_err(|err| {
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

/// Suites with no selected test (`filter_match.is_match`) are dropped
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

/// `summary` boxed (it dwarfs the other fields); the engine rebuilds an owned `TestList`
/// from it to resolve per-test config and drive the reporter
#[derive(Debug, Clone)]
pub enum BuildOutcome {
    Ok {
        test_count: usize,
        binary_count: usize,
        selected_binaries: Vec<SelectedBinary>,
        summary: Box<TestListSummary>,
    },
    Failed {
        exit_code: i32,
        stage: BuildStage,
    },
}

/// A binary with at least one selected test.
///
/// - `cwd` = where nextest runs it, keeping `dev!`'s compile-time absolute paths and the
///   dump-time `current_dir` in agreement
/// - `binary_id` = nextest's `<package>::<bin>`, also the QoS dump's grouping key → the
///   engine can match a test's tier to its binary
#[derive(Debug, Clone)]
pub struct SelectedBinary {
    pub binary_path: std::path::PathBuf,
    pub cwd: std::path::PathBuf,
    pub binary_id: String,
    pub selected_tests: Vec<String>,
}
