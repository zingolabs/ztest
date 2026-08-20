//! `ztest store`: list / inspect / export-`.zip` / prune recorded runs. Mirrors
//! `cargo nextest store`.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use ztest::api::engine::record::{self, RunSelector};
use ztest::api::thousands;
use ztest_ui::Theme;
use ztest_ui::template::{Fields, draw};

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
    let theme = Theme::detect();
    let result = match args.cmd {
        StoreCmd::List => list(&theme),
        StoreCmd::Info { run_id } => info(&run_id, &theme),
        StoreCmd::Export { run_id, out } => export(&run_id, &out, &theme),
        StoreCmd::Prune => prune(&theme),
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

/// Label column is 12 wide in every `info` row, so the values land on one gutter
mod row {
    pub(super) const EMPTY: &str = "{note|dim}";
    pub(super) const RUN: &str = "{id|bold}  {started|dim}  {summary}";
    pub(super) const FIELD: &str = "{label:<12|dim}{value}";
    pub(super) const SIZE: &str = "{label:<12|dim}{bytes|bytes}";
    pub(super) const RESULT: &str = "{label:<12|dim}{summary}[ ({ran|bold} of {total|bold} run)]";
    pub(super) const EXPORTED: &str = "exported {src|dim} to {out|bold}";
    pub(super) const PRUNED: &str = "pruned {n|bold} recording(s)";

    pub(super) fn tally_term(tone: &str) -> String {
        format!("{{n|bold}} {{word|{tone}}}")
    }
}

/// No `*` cell and no spinner in any of these rows → zero width, zero elapsed
fn print(src: &str, f: Fields<'_>, theme: &Theme) {
    println!("{}", draw(src, &f, theme));
}

/// Same terms, omit-rule and ink as the live summary line ([`RunStats::tally`])
fn tally(s: &ztest::api::engine::RunStats, theme: &Theme) -> String {
    let term = |(n, word): (u32, &str)| {
        let tone = match word {
            "passed" => "pass",
            "failed" | "sigkilled" => "fail",
            _ => "skip",
        };
        let f = Fields::new().text("n", thousands(u64::from(n))).text("word", word.to_string());
        draw(&row::tally_term(tone), &f, theme)
    };
    s.tally().into_iter().map(term).collect::<Vec<_>>().join(", ")
}

fn list(theme: &Theme) -> Result<(), String> {
    let ws = workspace()?;
    let runs = record::locate::list_runs(&ws).map_err(|e| e.to_string())?;
    if runs.is_empty() {
        print(row::EMPTY, Fields::new().text("note", "no recorded runs for this workspace"), theme);
        return Ok(());
    }
    for run in runs {
        let started =
            record::read_meta(&run.dir).map(|m| m.started_at).unwrap_or_else(|_| "?".to_string());
        // Pre-inked (term count varies), so the cell carrying it declares no width
        let summary = match record::final_stats(&run.dir) {
            Ok(Some(s)) => tally(&s, theme),
            Ok(None) => "incomplete".to_string(),
            Err(_) => "unreadable".to_string(),
        };
        let f =
            Fields::new().text("id", run.run_id).text("started", started).text("summary", summary);
        print(row::RUN, f, theme);
    }
    Ok(())
}

fn info(sel: &RunSelector, theme: &Theme) -> Result<(), String> {
    let ws = workspace()?;
    let dir = record::locate::resolve(&ws, sel).map_err(|e| e.to_string())?;
    let meta = record::read_meta(&dir).map_err(|e| e.to_string())?;
    let field = |label: &str, value: String| {
        print(
            row::FIELD,
            Fields::new().text("label", label.to_string()).text("value", value),
            theme,
        )
    };

    field("run id:", meta.run_id);
    field("started:", meta.started_at);
    field("captured:", meta.captured.to_string());
    field("args:", meta.args.join(" "));
    let size = record::retention::dir_size(&dir) as f64;
    print(row::SIZE, Fields::new().text("label", "size:").value("bytes", size), theme);
    field("path:", dir.display().to_string());

    let f = Fields::new().text("label", "result:");
    // `[ (… run)]` drops with the counts an unfinished run never recorded
    let f = match record::final_stats(&dir).map_err(|e| e.to_string())? {
        Some(s) => f
            .text("summary", tally(&s, theme))
            .text("ran", thousands(u64::from(s.ran())))
            .text("total", thousands(s.total as u64)),
        None => f.text("summary", "incomplete (no RunFinished recorded)"),
    };
    print(row::RESULT, f, theme);
    Ok(())
}

fn export(sel: &RunSelector, out: &std::path::Path, theme: &Theme) -> Result<(), String> {
    let ws = workspace()?;
    let dir = record::locate::resolve(&ws, sel).map_err(|e| e.to_string())?;
    record::portable::export(&dir, out).map_err(|e| e.to_string())?;
    let f =
        Fields::new().text("src", dir.display().to_string()).text("out", out.display().to_string());
    print(row::EXPORTED, f, theme);
    Ok(())
}

fn prune(theme: &Theme) -> Result<(), String> {
    let ws = workspace()?;
    let deleted = record::retention::gc(&ws, record::retention::RetentionPolicy::default());
    print(row::PRUNED, Fields::new().text("n", thousands(deleted as u64)), theme);
    Ok(())
}
