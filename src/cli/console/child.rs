//! Work-side subprocess runner: spawn a child under a PTY and pipe its output to
//! the render thread.
//!
//! This is the producer half of the console split. The render thread
//! ([`super::render`]) owns the terminal and the `avt` grid; here we only spawn
//! the child, stream its raw PTY bytes to the render thread as `Msg::Output`,
//! and return the exit code. Running under a PTY (not a pipe) keeps the child's
//! native colour + in-place progress bars (cargo, docker BuildKit, kind) intact:
//! they only emit those when they detect a TTY.
//!
//! Off a TTY (no [`Console`]), the child inherits stdio for the plain CI log.

use std::io::{self, Read};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

use super::Console;

/// Run `program args` to completion. With a [`Console`], the child runs under a
/// PTY emulated into the session's live region; without one, it inherits stdio.
/// Returns the child's exit code (`130` when a forwarded Ctrl-C killed it).
///
/// Output ordering: a reader thread streams the child's PTY bytes to the render
/// thread until PTY EOF, then exits. We join it before returning, so by the time
/// the caller sends a `FlushLive` / phase transition every `Output` is already
/// enqueued ahead of it. That happens-before keeps native scrollback in order
/// across the two producers (see `docs/console-architecture.md`).
pub(crate) async fn run_child(
    console: Option<&Console>,
    program: &str,
    args: &[String],
    envs: &[(&str, String)],
) -> io::Result<i32> {
    let Some(console) = console else {
        return run_inherited(program, args, envs);
    };

    let size = console.size();
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: console.emu_rows(),
            cols: size.cols,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(|e| io::Error::other(format!("openpty: {e}")))?;

    let mut cmd = CommandBuilder::new(program);
    for a in args {
        cmd.arg(a);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    if std::env::var_os("TERM").is_none() {
        cmd.env("TERM", "xterm-256color");
    }
    if let Ok(cwd) = std::env::current_dir() {
        cmd.cwd(cwd);
    }

    let mut child = pair
        .slave
        .spawn_command(cmd)
        .map_err(|e| io::Error::other(format!("spawn {program}: {e}")))?;
    drop(pair.slave);

    // The render thread forwards Ctrl-C to this group.
    console.child_started(pair.master.process_group_leader());

    let mut reader = pair
        .master
        .try_clone_reader()
        .map_err(|e| io::Error::other(format!("pty reader: {e}")))?;
    let sink = console.clone();
    let reader_thread = std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if !sink.output(buf[..n].to_vec()) {
                        break; // render thread gone
                    }
                }
            }
        }
    });

    // `Child::wait` blocks; keep it off the async worker. The reader thread sees
    // PTY EOF when the child exits and the master's last fd closes.
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .map_err(io::Error::other)?;
    drop(pair.master);
    let _ = reader_thread.join();
    console.child_exited();

    Ok(exit_code_from(status, console.cancelled()))
}

/// Non-TTY fallback: inherit stdio, run synchronously, return the exit code.
fn run_inherited(program: &str, args: &[String], envs: &[(&str, String)]) -> io::Result<i32> {
    let mut cmd = std::process::Command::new(program);
    cmd.args(args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    Ok(cmd.status()?.code().unwrap_or(1))
}

/// Map a finished PTY child status to an exit code. Clean exits propagate their
/// code; a signal death reports `130` when a Ctrl-C was in flight, else `1`.
fn exit_code_from(status: io::Result<portable_pty::ExitStatus>, interrupted: bool) -> i32 {
    match status {
        Ok(s) => code_for(&s, interrupted) as i32,
        Err(err) => {
            eprintln!("ztest run: error waiting on child: {err}");
            127
        }
    }
}

/// Pure numeric exit-code decision (see [`exit_code_from`]).
fn code_for(status: &portable_pty::ExitStatus, interrupted: bool) -> u8 {
    if status.signal().is_some() {
        if interrupted { 130 } else { 1 }
    } else {
        (status.exit_code() & 0xff) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_for_maps_clean_and_signal_deaths() {
        use portable_pty::ExitStatus;
        assert_eq!(code_for(&ExitStatus::with_exit_code(0), false), 0);
        assert_eq!(code_for(&ExitStatus::with_exit_code(101), false), 101);
        assert_eq!(code_for(&ExitStatus::with_exit_code(7), true), 7);
        assert_eq!(code_for(&ExitStatus::with_signal("Killed"), true), 130);
        assert_eq!(code_for(&ExitStatus::with_signal("Hangup"), false), 1);
    }
}
