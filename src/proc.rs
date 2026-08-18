//! Subprocess seam: core spawns, the presentation layer owns any PTY.

use std::io;
use std::sync::Arc;

/// Child runner. Core holds `Option<&dyn ChildHost>`; `None` = inherited stdio.
///
/// - Impl lives in `console` (PTY emulated into the live region) → no core → ui edge
#[async_trait::async_trait]
pub trait ChildHost: Send + Sync {
    async fn run_child(
        &self,
        program: &str,
        args: &[String],
        envs: &[(&str, String)],
    ) -> io::Result<i32>;
}

pub type SharedChildHost = Arc<dyn ChildHost>;

/// Run `program args` on inherited stdio → exit code (`128 + sig` when signalled)
pub fn run_inherited(program: &str, args: &[String], envs: &[(&str, String)]) -> io::Result<i32> {
    let mut cmd = std::process::Command::new(program);
    cmd.args(args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let status = cmd.status()?;
    Ok(match status.code() {
        Some(code) => code,
        None => {
            use std::os::unix::process::ExitStatusExt as _;
            status.signal().map_or(1, |sig| 128 + sig)
        }
    })
}

/// `host` present → through it (PTY); absent → inherited stdio
pub async fn run(
    host: Option<&dyn ChildHost>,
    program: &str,
    args: &[String],
    envs: &[(&str, String)],
) -> io::Result<i32> {
    match host {
        Some(h) => h.run_child(program, args, envs).await,
        None => run_inherited(program, args, envs),
    }
}
