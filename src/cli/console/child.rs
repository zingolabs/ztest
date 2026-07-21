//! Work-side subprocess runner: spawn a child under a PTY and stream its raw
//! bytes to the render thread. A PTY (not a pipe) keeps the child's native colour
//! + in-place progress bars intact, since those emit only when a TTY is detected.
//! Off a TTY (no [`Console`]), the child inherits stdio for the plain CI log.

use std::io::{self, Read};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

use super::Console;

/// Run `program args` to completion. With a [`Console`], the child runs under a
/// PTY emulated into the session's live region; without one, it inherits stdio.
/// Returns the child's exit code (`130` when a forwarded Ctrl-C killed it).
///
/// The reader thread is joined before returning, so every `Output` is enqueued
/// ahead of the caller's next `FlushLive` / phase transition — that happens-before
/// keeps native scrollback ordered across the two producers.
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
            rows: console.live_rows(),
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

    // portable-pty `setsid`s the child, so its PID *is* its process-group id — use
    // it directly. `master.process_group_leader()` races the not-yet-completed
    // `setsid` and can latch a stale group, silently dropping the first Ctrl-Cs.
    console.child_started(child.process_id().map(|pid| pid as i32));

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

    // `Child::wait` blocks; keep it off the async worker.
    let wait = tokio::task::spawn_blocking(move || child.wait());
    tokio::pin!(wait);

    // Forward terminal resizes to the child's PTY (width + `live_rows`), matching
    // the render thread's `avt` grid. `master.resize` delivers SIGWINCH so tools
    // re-wrap instead of keeping their spawn-time size.
    let mut size = console.size_watch();
    let status = loop {
        tokio::select! {
            done = &mut wait => break done.map_err(io::Error::other)?,
            changed = size.changed() => {
                if changed.is_ok() {
                    let cols = size.borrow().cols;
                    let _ = pair.master.resize(PtySize {
                        rows: console.live_rows(),
                        cols,
                        pixel_width: 0,
                        pixel_height: 0,
                    });
                }
            }
        }
    };
    drop(pair.master);
    let _ = reader_thread.join();
    console.child_exited();

    Ok(exit_code_from(status, console.cancelled()))
}

/// Non-TTY fallback: inherit stdio, run synchronously, return the exit code.
/// A signal death maps to `128 + signo` (Ctrl-C → 130, as in [`code_for`]).
fn run_inherited(program: &str, args: &[String], envs: &[(&str, String)]) -> io::Result<i32> {
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

/// Map a finished PTY child status to an exit code.
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
