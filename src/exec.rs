//! One `pods/exec` primitive: argv in a named container → stdout, stderr, exit status.
//!
//! - Every in-pod exec rides this (build pod, csi plugin meter, component handles)
//! - No TTY → both streams share ONE websocket and MUST be drained concurrently

use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Status;
use kube::api::{Api, AttachParams};
use tokio::io::AsyncReadExt as _;

/// Live stderr sink, one line per call. `Send + Sync` (held across an await in boxed `Send`
/// futures)
pub(crate) type LineSink<'a> = &'a (dyn Fn(&str) + Send + Sync);

/// Raw result of one exec. `status` = the websocket status channel (`None` = channel closed
/// without one, e.g. the container died under the exec)
#[derive(Debug)]
pub(crate) struct Captured {
    pub stdout: String,
    pub stderr: String,
    pub status: Option<Status>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExecError {
    #[error("exec in pod {pod}: {source}")]
    Attach { pod: String, source: kube::Error },
    #[error("read exec {stream}: {source}")]
    Read { stream: &'static str, source: std::io::Error },
}

/// Run `argv` in `container` of `pod`, each stderr line to `on_line` as it arrives
pub(crate) async fn exec(
    api: &Api<Pod>,
    pod: &str,
    container: &str,
    argv: &[&str],
    on_line: Option<LineSink<'_>>,
) -> Result<Captured, ExecError> {
    use tokio::io::AsyncBufReadExt as _;

    let ap = AttachParams::default().container(container).stdin(false).stdout(true).stderr(true);
    let mut attached = api
        .exec(pod, argv.iter().copied(), &ap)
        .await
        .map_err(|source| ExecError::Attach { pod: pod.to_string(), source })?;
    let status = attached.take_status();

    let mut stdout = attached.stdout();
    let stderr = attached.stderr();
    let mut out = String::new();
    let mut err = String::new();
    let (ro, re) = tokio::join!(
        async {
            match stdout.as_mut() {
                Some(s) => s.read_to_string(&mut out).await.map(|_| ()),
                None => Ok(()),
            }
        },
        async {
            let Some(s) = stderr else { return Ok(()) };
            let mut lines = tokio::io::BufReader::new(s).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        if let Some(cb) = on_line {
                            cb(&line);
                        }
                        err.push_str(&line);
                        err.push('\n');
                    }
                    Ok(None) => break Ok(()),
                    Err(e) => break Err(e),
                }
            }
        },
    );
    ro.map_err(|source| ExecError::Read { stream: "stdout", source })?;
    re.map_err(|source| ExecError::Read { stream: "stderr", source })?;
    // Status resolves only once the streams are done and the process joined
    let _ = attached.join().await;
    let status = match status {
        Some(fut) => fut.await,
        None => None,
    };
    Ok(Captured { stdout: out, stderr: err, status })
}

/// Exit code off an exec `Status`.
///
/// - `Success` → 0
/// - Non-zero exit → `Failure`/`NonZeroExitCode` + an `ExitCode` cause carrying the number
/// - Anything else (runtime refused the command, container gone) → `Err(message)`
pub(crate) fn exit_code(status: &Status) -> Result<i32, String> {
    if status.status.as_deref() == Some("Success") {
        return Ok(0);
    }
    let code = status
        .details
        .as_ref()
        .and_then(|d| d.causes.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.reason.as_deref() == Some("ExitCode")))
        .and_then(|c| c.message.as_deref())
        .and_then(|m| m.trim().parse::<i32>().ok());
    match (status.reason.as_deref(), code) {
        (Some("NonZeroExitCode"), Some(code)) => Ok(code),
        _ => Err(status.message.clone().unwrap_or_else(|| format!("exec status {status:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{StatusCause, StatusDetails};

    use super::*;

    fn failure(reason: Option<&str>, cause: Option<(&str, &str)>, message: &str) -> Status {
        Status {
            status: Some("Failure".into()),
            reason: reason.map(Into::into),
            message: Some(message.into()),
            details: cause.map(|(r, m)| StatusDetails {
                causes: Some(vec![StatusCause {
                    reason: Some(r.into()),
                    message: Some(m.into()),
                    ..StatusCause::default()
                }]),
                ..StatusDetails::default()
            }),
            ..Status::default()
        }
    }

    #[test]
    fn exit_code_reads_every_status_shape_the_apiserver_sends() {
        let success = Status { status: Some("Success".into()), ..Status::default() };
        let cases = [
            (success, Ok(0)),
            (failure(Some("NonZeroExitCode"), Some(("ExitCode", "137")), "exit 137"), Ok(137)),
            (
                failure(Some("NonZeroExitCode"), Some(("ExitCode", "x")), "garbled"),
                Err("garbled".to_string()),
            ),
            (
                failure(Some("InternalError"), None, "executable file not found in $PATH"),
                Err("executable file not found in $PATH".to_string()),
            ),
        ];
        for (status, want) in cases {
            assert_eq!(exit_code(&status), want, "{status:?}");
        }
    }
}
