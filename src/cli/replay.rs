//! `ztest replay`: re-render a previously recorded run without re-executing it,
//! mirroring `cargo nextest replay`. The run is selected by `-R/--run-id`
//! (`latest` by default, or a run-id / unambiguous prefix / recording path), and
//! the reporter options ([`ReporterCommonOpts`]) can be overridden so the same
//! recording can be re-viewed with different output settings.
//!
//! Replay is a subcommand, not a `run` flag, for the same reason nextest made it
//! one: it bypasses preflight, build, and the cluster entirely — it only reads
//! the recording and drives the reporter.

use std::process::ExitCode;

use clap::Parser;
use nextest_metadata::NextestExitCode;

use crate::engine;
use crate::engine::output::OutputConfig;
use crate::engine::record::RunSelector;

/// `ztest replay` arguments.
#[derive(Debug, Parser)]
pub struct Args {
    /// Run ID, `latest`, or recording path to replay. Accepts `latest` (the
    /// default) for the most recent run in this workspace, a run-id or
    /// unambiguous prefix, or a path to a recording directory.
    #[arg(
        short = 'R',
        long = "run-id",
        default_value = "latest",
        value_name = "RUN_ID_OR_RECORDING"
    )]
    pub run_id: RunSelector,

    /// Exit with the same code as the original run. By default, replay exits 0
    /// if the replay itself succeeds.
    #[arg(long)]
    pub exit_code: bool,

    /// Simulate no-capture mode during replay: show both passing and failing
    /// output immediately (a convenience preset, as in nextest).
    #[arg(long, alias = "nocapture")]
    pub no_capture: bool,

    #[command(flatten)]
    pub reporter: ReporterCommonOpts,
}

/// Reporter options shared by any command that renders a run. Today only
/// `replay` flattens it; `run`'s equivalent flags still flow through its own
/// argv classifier, and folding that onto this struct is a clean follow-up.
#[derive(Debug, Default, clap::Args)]
pub struct ReporterCommonOpts {
    /// When a passing test's captured output is shown
    /// (`immediate`/`immediate-final`/`final`/`never`).
    #[arg(long, value_name = "WHEN")]
    pub success_output: Option<String>,

    /// When a failing test's captured output is shown (as `--success-output`).
    #[arg(long, value_name = "WHEN")]
    pub failure_output: Option<String>,

    /// Colouring: `auto` (default), `always`, or `never`.
    #[arg(long, value_name = "WHEN")]
    pub color: Option<String>,
}

impl ReporterCommonOpts {
    /// Resolve the captured-output display policy. `--no-capture` forces
    /// immediate display for both pass and fail (nextest's preset). An invalid
    /// `--success/failure-output` value warns and falls back to the default.
    fn output_config(&self, no_capture: bool) -> OutputConfig {
        let default = OutputConfig::default();
        let mut cfg = OutputConfig {
            success: parse_display(&self.success_output, default.success),
            failure: parse_display(&self.failure_output, default.failure),
            capture: default.capture,
        };
        if no_capture {
            cfg = cfg.with_no_capture();
        }
        cfg
    }
}

/// Parse a `--success/failure-output` value, warning and returning `default` on
/// an invalid one.
fn parse_display(
    value: &Option<String>,
    default: crate::engine::output::TestOutputDisplay,
) -> crate::engine::output::TestOutputDisplay {
    match value {
        Some(v) => v.parse().unwrap_or_else(|e| {
            eprintln!("ztest replay: {e}; using default");
            default
        }),
        None => default,
    }
}

/// Resolve the colour decision from `--color`, honouring terminal support for
/// `auto`.
fn resolve_color(opt: Option<&str>) -> bool {
    match opt {
        Some("always") => true,
        Some("never") => false,
        _ => supports_color::on(supports_color::Stream::Stdout).is_some(),
    }
}

pub fn execute(args: Args) -> ExitCode {
    let setup_error = ExitCode::from(NextestExitCode::SETUP_ERROR as u8);

    let workspace = match engine::record::locate::current_workspace() {
        Ok(w) => w,
        Err(e) => {
            eprintln!("ztest replay: {e}");
            return setup_error;
        }
    };
    let dir = match engine::record::locate::resolve(&workspace, &args.run_id) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("ztest replay: {e}");
            return setup_error;
        }
    };

    let output = args.reporter.output_config(args.no_capture);
    let color = resolve_color(args.reporter.color.as_deref());
    let unicode = supports_unicode::on(supports_unicode::Stream::Stdout);

    match engine::record::replay(&dir, output, color, unicode, args.exit_code) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("ztest replay: {e}");
            setup_error
        }
    }
}
