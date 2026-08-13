//! `ztest store`: list / inspect / export-`.zip` / prune recorded runs. Mirrors
//! `cargo nextest store`.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::engine::record::{self, RunSelector};

/// `ztest store` arguments.
#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    cmd: StoreCmd,
}

#[derive(Debug, Subcommand)]
enum StoreCmd {
    /// List recorded runs for this workspace, newest first.
    List,

    /// Show one recorded run's metadata and summary.
    Info {
        /// Run ID, `latest`, or recording path.
        #[arg(value_name = "RUN_ID_OR_RECORDING", default_value = "latest")]
        run_id: RunSelector,
    },

    /// Export a recording as a portable `.zip` (for archiving / CI upload).
    Export {
        /// Run ID, `latest`, or recording path to export.
        #[arg(value_name = "RUN_ID_OR_RECORDING")]
        run_id: RunSelector,
        /// Destination `.zip` path.
        #[arg(value_name = "OUT.zip")]
        out: PathBuf,
    },

    /// Prune recordings beyond the retention budget (30 days / 100 runs / 1 GiB).
    Prune,
}

pub fn execute(args: Args) -> ExitCode {
    let result = match args.cmd {
        StoreCmd::List => list(),
        StoreCmd::Info { run_id } => info(&run_id),
        StoreCmd::Export { run_id, out } => export(&run_id, &out),
        StoreCmd::Prune => prune(),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ztest store: {e}");
            ExitCode::FAILURE
        }
    }
}

fn workspace() -> Result<PathBuf, String> {
    record::locate::current_workspace().map_err(|e| e.to_string())
}

fn list() -> Result<(), String> {
    let ws = workspace()?;
    let runs = record::locate::list_runs(&ws).map_err(|e| e.to_string())?;
    if runs.is_empty() {
        println!("no recorded runs for this workspace");
        return Ok(());
    }
    for run in runs {
        let started =
            record::read_meta(&run.dir).map(|m| m.started_at).unwrap_or_else(|_| "?".to_string());
        let summary = match record::final_stats(&run.dir) {
            Ok(Some(s)) => plain_tally(&s),
            Ok(None) => "incomplete".to_string(),
            Err(_) => "unreadable".to_string(),
        };
        println!("{}  {started}  {summary}", run.run_id);
    }
    Ok(())
}

fn info(sel: &RunSelector) -> Result<(), String> {
    let ws = workspace()?;
    let dir = record::locate::resolve(&ws, sel).map_err(|e| e.to_string())?;
    let meta = record::read_meta(&dir).map_err(|e| e.to_string())?;
    let size = record::retention::dir_size(&dir);

    println!("run id:     {}", meta.run_id);
    println!("started:    {}", meta.started_at);
    println!("captured:   {}", meta.captured);
    println!("args:       {}", meta.args.join(" "));
    println!("size:       {size} bytes");
    println!("path:       {}", dir.display());
    match record::final_stats(&dir).map_err(|e| e.to_string())? {
        Some(s) => println!("result:     {} ({} of {} run)", plain_tally(&s), s.ran(), s.total),
        None => println!("result:     incomplete (no RunFinished recorded)"),
    }
    Ok(())
}

/// Un-styled tally, same terms + omit-rule as the live summary line ([`RunStats::tally`])
fn plain_tally(s: &crate::engine::events::RunStats) -> String {
    s.tally().into_iter().map(|(n, word)| format!("{n} {word}")).collect::<Vec<_>>().join(", ")
}

fn export(sel: &RunSelector, out: &std::path::Path) -> Result<(), String> {
    let ws = workspace()?;
    let dir = record::locate::resolve(&ws, sel).map_err(|e| e.to_string())?;
    record::portable::export(&dir, out).map_err(|e| e.to_string())?;
    println!("exported {} to {}", dir.display(), out.display());
    Ok(())
}

fn prune() -> Result<(), String> {
    let ws = workspace()?;
    let deleted = record::retention::gc(&ws, record::retention::RetentionPolicy::default());
    println!("pruned {deleted} recording(s)");
    Ok(())
}
