//! `ztest config`: user settings in `config.toml` (cluster profiles + bucket credentials =
//! `clusters.toml`, owned by `ztest cluster` / `ztest snapshot config`).
//!
//! - Precedence per setting: flag > `ZTEST_*` env > `config.toml` > built-in default

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context as _, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

use ztest::api::engine::LogTail;

const LOG_TAIL_ENV: &str = "ZTEST_LOG_TAIL";

/// `ztest config` arguments.
#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Print a setting's effective value.
    Get { key: Key },

    /// Persist a setting to `config.toml`.
    Set { key: Key, value: String },

    /// Remove a setting from `config.toml`, restoring its built-in default.
    Unset { key: Key },

    /// Print every setting's effective value and where it comes from.
    List,

    /// Print the config file's path.
    Path,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Key {
    /// Component-log lines shown per test: a line count, or `all` (default
    /// 30). Overridden by `ZTEST_LOG_TAIL` and `--log-tail`.
    LogTail,
}

/// On-disk `config.toml`. Absent field = built-in default
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct UserConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    log_tail: Option<LogTail>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Flag,
    Env,
    File,
    Default,
}

impl Source {
    fn label(self) -> &'static str {
        match self {
            Source::Flag => "--log-tail",
            Source::Env => LOG_TAIL_ENV,
            Source::File => "config.toml",
            Source::Default => "default",
        }
    }
}

fn path() -> PathBuf {
    ztest::api::paths::config_dir().join("config.toml")
}

/// Missing file → empty config
fn load() -> Result<UserConfig> {
    let path = path();
    match std::fs::read_to_string(&path) {
        Ok(body) => toml::from_str(&body).with_context(|| format!("parse {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(UserConfig::default()),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

fn save(cfg: &UserConfig) -> Result<()> {
    let path = path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    }
    let body = toml::to_string_pretty(cfg).context("serialize config.toml")?;
    std::fs::write(&path, body).with_context(|| format!("write {}", path.display()))
}

/// First source set wins. Pure (env + file passed in) → testable without process env
fn resolve_log_tail(
    flag: Option<&str>,
    env: Option<&str>,
    file: Option<LogTail>,
) -> Result<(LogTail, Source), String> {
    if let Some(v) = flag {
        return Ok((v.parse()?, Source::Flag));
    }
    if let Some(v) = env.filter(|v| !v.trim().is_empty()) {
        return Ok((v.parse().map_err(|e| format!("{LOG_TAIL_ENV}: {e}"))?, Source::Env));
    }
    Ok(file.map_or((LogTail::DEFAULT, Source::Default), |t| (t, Source::File)))
}

/// Effective log tail for `run` / `replay`. Invalid value or unreadable `config.toml` →
/// warn + default (display knob, never aborts a run)
pub(crate) fn log_tail(flag: Option<&str>, cmd: &str) -> LogTail {
    let file = load().unwrap_or_else(|e| {
        eprintln!("ztest {cmd}: {e:#}; ignoring config.toml");
        UserConfig::default()
    });
    let env = std::env::var(LOG_TAIL_ENV).ok();
    match resolve_log_tail(flag, env.as_deref(), file.log_tail) {
        Ok((tail, _)) => tail,
        Err(e) => {
            eprintln!("ztest {cmd}: {e}; using default");
            LogTail::DEFAULT
        }
    }
}

pub(crate) fn execute(args: Args) -> ExitCode {
    match dispatch(args.cmd) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ztest config: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn dispatch(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Path => println!("{}", path().display()),
        Cmd::Get { key: Key::LogTail } => println!("{}", effective_log_tail()?.0),
        Cmd::List => {
            let (tail, source) = effective_log_tail()?;
            println!("log-tail = {tail}  ({})", source.label());
        }
        Cmd::Set { key: Key::LogTail, value } => {
            let mut cfg = load()?;
            cfg.log_tail = Some(value.parse().map_err(|e: String| anyhow!(e))?);
            save(&cfg)?;
        }
        Cmd::Unset { key: Key::LogTail } => {
            let mut cfg = load()?;
            cfg.log_tail = None;
            save(&cfg)?;
        }
    }
    Ok(())
}

/// Strict (unlike [`log_tail`]): `get` / `list` exist to surface a broken value
fn effective_log_tail() -> Result<(LogTail, Source)> {
    let env = std::env::var(LOG_TAIL_ENV).ok();
    resolve_log_tail(None, env.as_deref(), load()?.log_tail).map_err(|e| anyhow!(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_tail_precedence_is_flag_then_env_then_file_then_default() {
        type Case<'a> = (Option<&'a str>, Option<&'a str>, Option<LogTail>, (LogTail, Source));
        let file = Some(LogTail::Lines(200));
        #[rustfmt::skip]
        let cases: &[Case] = &[
            (Some("all"), Some("5"), file, (LogTail::All,        Source::Flag)),
            (None,        Some("5"), file, (LogTail::Lines(5),   Source::Env)),
            (None,        Some(" "), file, (LogTail::Lines(200), Source::File)),
            (None,        None,      file, (LogTail::Lines(200), Source::File)),
            (None,        None,      None, (LogTail::Lines(30),  Source::Default)),
        ];
        for &(flag, env, file, want) in cases {
            assert_eq!(
                resolve_log_tail(flag, env, file),
                Ok(want),
                "flag={flag:?} env={env:?} file={file:?}"
            );
        }

        let bad_env = resolve_log_tail(None, Some("lots"), file).unwrap_err();
        assert!(bad_env.starts_with("ZTEST_LOG_TAIL: invalid log tail"), "{bad_env}");
        assert!(resolve_log_tail(Some("-3"), None, file).is_err());
    }

    #[test]
    fn config_toml_uses_kebab_keys_and_rejects_typos() {
        let cfg: UserConfig = toml::from_str("log-tail = \"all\"\n").unwrap();
        assert_eq!(cfg.log_tail, Some(LogTail::All));
        assert_eq!(toml::to_string_pretty(&cfg).unwrap(), "log-tail = \"all\"\n");
        assert_eq!(toml::to_string_pretty(&UserConfig::default()).unwrap(), "");
        assert!(
            toml::from_str::<UserConfig>("log_tail = 30\n").is_err(),
            "typo must not be ignored"
        );
    }
}
