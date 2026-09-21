//! Terminal control shared by every live surface (run console, `ztest status`).
//!
//! - Line discipline + DEC private modes in one place (two owners = two restore paths)
//! - Restore is RAII; callers with a `process::exit` path call `restore()` explicitly too

/// Synchronized-update + cursor-visibility sequences (DEC private modes)
pub const SYNC_BEGIN: &str = "\x1b[?2026h";
pub const SYNC_END: &str = "\x1b[?2026l";
pub const CURSOR_HIDE: &str = "\x1b[?25l";
pub const CURSOR_SHOW: &str = "\x1b[?25h";

/// DECAWM. Off → an exactly-`cols`-wide line occupies one row on every terminal, so a
/// cursor-up repaint's row arithmetic can't drift (eager-wrap terminals otherwise bill a
/// full-width line as two rows); overflow clips instead of pushing the frame down
pub const WRAP_OFF: &str = "\x1b[?7l";
pub const WRAP_ON: &str = "\x1b[?7h";

/// Restores the controlling terminal's line discipline on drop.
///
/// - `ECHO` + `ICANON` off (cooked mode echoes keystrokes, `^C` worst, onto the frame)
/// - `ISIG` kept → Ctrl-C still raises `SIGINT` instead of arriving as a raw byte
/// - `Drop` = the panic/`exit` backstop behind an explicit [`TtyGuard::restore`]
pub struct TtyGuard {
    fd: std::os::fd::RawFd,
    original: Option<libc::termios>,
}

impl TtyGuard {
    /// Enter no-echo / no-canonical mode on stdin's tty, saving the prior attributes.
    /// No-op (`original: None`) off a tty
    pub fn enter() -> TtyGuard {
        let fd = libc::STDIN_FILENO;
        let mut term: libc::termios = unsafe { std::mem::zeroed() };
        let original = if unsafe { libc::tcgetattr(fd, &mut term) } == 0 {
            let saved = term;
            term.c_lflag &= !(libc::ECHO | libc::ICANON);
            unsafe { libc::tcsetattr(fd, libc::TCSANOW, &term) };
            Some(saved)
        } else {
            None
        };
        TtyGuard { fd, original }
    }

    /// Restore the saved attributes. Idempotent
    pub fn restore(&self) {
        if let Some(orig) = self.original.as_ref() {
            unsafe { libc::tcsetattr(self.fd, libc::TCSANOW, orig) };
        }
    }
}

impl Drop for TtyGuard {
    fn drop(&mut self) {
        self.restore();
    }
}
