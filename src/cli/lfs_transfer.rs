//! `ztest lfs-transfer` — the Git LFS [custom transfer agent] for the
//! chain-snapshot bucket.
//!
//! Machine-invoked. git-lfs spawns this process, speaks newline-delimited JSON
//! at its stdin, and reads replies from its stdout; a human never types it.
//! `.lfsconfig` wires it up with `lfs.standalonetransferagent`, which makes
//! git-lfs skip the batch API altogether — there is no LFS server in this
//! design, so `git lfs push` and `git lfs pull` reach R2 through exactly the
//! same [`Bucket`] that [`crate::storage::lfs`] reads seeds from at test time.
//!
//! # Why this lives in ztest
//!
//! The agent *writes* objects; the seed path *reads* them. Split across two
//! binaries, the key layout and credential contract they must agree on would be
//! an unchecked convention, and the failure mode of a drift is every fixture
//! 404ing on a cold CI run. Sharing [`Bucket::key`] makes the agreement a
//! compile-time one. It also means there is nothing extra to install: ztest is
//! already on the machine.
//!
//! # Why not the stock transfer adapter
//!
//! git-lfs's built-in `basic` adapter is one `PUT` per object, which inherits
//! R2's 4.995 GiB single-request cap — the 8.15 GiB IRONWOOD snapshot cannot be
//! pushed with it at all. [`Bucket::put`] runs a real S3 multipart upload.
//!
//! [custom transfer agent]: https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::storage::r2::Bucket;

/// Bytes transferred between `progress` messages. git-lfs renders a progress
/// bar from these; one per 4 MiB keeps a multi-GB transfer visibly moving
/// without flooding the pipe with thousands of lines.
const PROGRESS_INTERVAL: u64 = 4 * 1024 * 1024;

/// Read/write buffer for the download leg. Independent of the upload part size
/// — this one only bounds how much sits in memory between the socket and the
/// file.
const COPY_BUF: usize = 1024 * 1024;

/// The `code` on a per-object error. git-lfs treats any non-zero code as "this
/// object failed"; the value is surfaced to the user but not otherwise
/// interpreted, so one honest code beats an invented taxonomy.
const TRANSFER_FAILED: i32 = 1;

pub(crate) fn execute() -> ExitCode {
    // Current-thread is the right flavor: this is one sequential conversation
    // on stdin. git-lfs achieves concurrency by spawning several agent
    // *processes*, not by interleaving requests within one.
    super::block_on("lfs-transfer", super::Rt::Current, run())
}

// ─────────────────────────── protocol ───────────────────────────────

/// A message from git-lfs. Unknown fields are ignored by design: the spec
/// carries `operation`, `remote`, `concurrent`, and an `action` object that a
/// standalone agent has no use for, and new ones may appear.
#[derive(Debug, Deserialize)]
#[serde(tag = "event", rename_all = "lowercase")]
enum Request {
    Init {},
    Upload {
        oid: String,
        size: u64,
        path: PathBuf,
    },
    Download {
        oid: String,
        size: u64,
    },
    Terminate {},
}

#[derive(Debug, Serialize)]
struct ProtocolError {
    code: i32,
    message: String,
}

/// The reply to `init`: `{}` on success, or an `error` git-lfs reports before
/// abandoning the transfer.
#[derive(Debug, Serialize)]
struct InitResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ProtocolError>,
}

/// The reply that ends one object's transfer. `path` is set for `download`
/// only — it names the file git-lfs then *moves* into `.git/lfs/objects`, so
/// ownership of the bytes passes to git-lfs here.
#[derive(Debug, Serialize)]
struct Complete {
    event: &'static str,
    oid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ProtocolError>,
}

#[derive(Debug, Serialize)]
struct Progress<'a> {
    event: &'static str,
    oid: &'a str,
    #[serde(rename = "bytesSoFar")]
    bytes_so_far: u64,
    #[serde(rename = "bytesSinceLast")]
    bytes_since_last: u64,
}

/// Write one protocol message. Stdout carries protocol and nothing else — any
/// stray `println!` here corrupts the stream and git-lfs fails with a parse
/// error that points nowhere near the cause.
fn send<T: Serialize>(msg: &T) -> Result<(), String> {
    let mut line = serde_json::to_vec(msg).map_err(|e| format!("encode response: {e}"))?;
    line.push(b'\n');
    let mut out = std::io::stdout().lock();
    out.write_all(&line)
        .and_then(|()| out.flush())
        .map_err(|e| format!("write to git-lfs: {e}"))
}

/// Emits `progress` messages at most every [`PROGRESS_INTERVAL`] bytes.
struct ProgressReporter {
    oid: String,
    total: u64,
    since_last: u64,
}

impl ProgressReporter {
    fn new(oid: &str) -> Self {
        Self {
            oid: oid.to_string(),
            total: 0,
            since_last: 0,
        }
    }

    /// Account `n` transferred bytes, emitting a message once enough have
    /// accumulated. Reporting failures are dropped: a broken stdout will be
    /// reported by the next real message, and progress is advisory.
    fn advance(&mut self, n: usize) {
        self.total += n as u64;
        self.since_last += n as u64;
        if self.since_last >= PROGRESS_INTERVAL {
            let _ = send(&Progress {
                event: "progress",
                oid: &self.oid,
                bytes_so_far: self.total,
                bytes_since_last: self.since_last,
            });
            self.since_last = 0;
        }
    }
}

// ─────────────────────────── event loop ─────────────────────────────

async fn run() -> Result<(), String> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    // `init` is always the first message, so this is a single exchange rather
    // than a loop. Resolving the bucket here means a misconfigured environment
    // is one clear error before any transfer starts, rather than N identical
    // per-object failures.
    let Some(line) = next_line(&mut lines).await? else {
        // git-lfs closed the pipe before `init`. Nothing was asked of us.
        return Ok(());
    };
    let bucket = match parse(&line)? {
        Request::Init {} => match Bucket::resolve() {
            Ok(bucket) => {
                send(&InitResponse { error: None })?;
                bucket
            }
            Err(e) => {
                // A failed init is reported in-band and ends the session:
                // every subsequent transfer would fail the same way.
                send(&InitResponse {
                    error: Some(ProtocolError {
                        code: TRANSFER_FAILED,
                        message: e.to_string(),
                    }),
                })?;
                return Ok(());
            }
        },
        Request::Terminate {} => return Ok(()),
        other => {
            return Err(format!(
                "git-lfs sent {} before `init`, which the protocol does not allow",
                event_name(&other)
            ));
        }
    };

    let scratch = scratch_dir()?;

    while let Some(line) = next_line(&mut lines).await? {
        match parse(&line)? {
            Request::Terminate {} => break,
            Request::Init {} => {
                return Err("git-lfs sent a second `init` in one session".to_string());
            }
            Request::Upload { oid, size, path } => {
                let result = upload(&bucket, &oid, size, &path).await;
                send(&completion(oid, None, result))?;
            }
            Request::Download { oid, size } => {
                let dest = scratch.join(&oid);
                let result = download(&bucket, &oid, size, &dest).await;
                let path = result.is_ok().then(|| dest.display().to_string());
                if result.is_err() {
                    // Don't leave a partial file behind wearing an OID's name.
                    let _ = tokio::fs::remove_file(&dest).await;
                }
                send(&completion(oid, path, result))?;
            }
        }
    }

    // Whatever git-lfs already moved out is gone; this clears failures' leftovers.
    let _ = std::fs::remove_dir_all(&scratch);
    Ok(())
}

/// One object's outcome as a `complete` message. A transfer failure is reported
/// *in the message*, never by exiting — an exit would abort every other object
/// in the batch, including ones that would have succeeded.
fn completion(oid: String, path: Option<String>, result: Result<(), String>) -> Complete {
    match result {
        Ok(()) => Complete {
            event: "complete",
            oid,
            path,
            error: None,
        },
        Err(message) => Complete {
            event: "complete",
            oid,
            path: None,
            error: Some(ProtocolError {
                code: TRANSFER_FAILED,
                message,
            }),
        },
    }
}

async fn upload(
    bucket: &Bucket,
    oid: &str,
    size: u64,
    path: &std::path::Path,
) -> Result<(), String> {
    // Objects are content-addressed, so a matching oid+size already in the
    // bucket *is* this object. Re-pushing after a partial failure then costs
    // one HEAD instead of re-sending gigabytes.
    if bucket
        .has(oid, size)
        .await
        .map_err(|e| format!("checking for an existing object: {e}"))?
    {
        return Ok(());
    }

    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut progress = ProgressReporter::new(oid);
    bucket
        .put(oid, size, file, &mut |n| progress.advance(n))
        .await
        .map_err(|e| e.to_string())
}

async fn download(
    bucket: &Bucket,
    oid: &str,
    _size: u64,
    dest: &std::path::Path,
) -> Result<(), String> {
    let mut source = bucket.get(oid).await.map_err(|e| e.to_string())?;
    let mut file = tokio::fs::File::create(dest)
        .await
        .map_err(|e| format!("create {}: {e}", dest.display()))?;

    // Integrity is git-lfs's job: it hashes the file we hand back and rejects
    // it if the digest doesn't match the OID. Re-verifying here would read
    // every byte a second time to reach the same verdict.
    let mut progress = ProgressReporter::new(oid);
    let mut buf = vec![0u8; COPY_BUF];
    loop {
        let n = source
            .read(&mut buf)
            .await
            .map_err(|e| format!("read object: {e}"))?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])
            .await
            .map_err(|e| format!("write {}: {e}", dest.display()))?;
        progress.advance(n);
    }
    file.flush()
        .await
        .map_err(|e| format!("flush {}: {e}", dest.display()))
}

/// A private directory for in-flight downloads, keyed by pid so concurrent
/// agent processes (git-lfs spawns several) never collide.
fn scratch_dir() -> Result<PathBuf, String> {
    let dir = std::env::temp_dir().join(format!("ztest-lfs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    Ok(dir)
}

async fn next_line<R>(lines: &mut tokio::io::Lines<R>) -> Result<Option<String>, String>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    lines
        .next_line()
        .await
        .map_err(|e| format!("read from git-lfs: {e}"))
}

fn parse(line: &str) -> Result<Request, String> {
    serde_json::from_str(line).map_err(|e| format!("parse {line:?} from git-lfs: {e}"))
}

fn event_name(req: &Request) -> &'static str {
    match req {
        Request::Init {} => "init",
        Request::Upload { .. } => "upload",
        Request::Download { .. } => "download",
        Request::Terminate {} => "terminate",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_each_request_the_spec_defines() {
        assert!(matches!(
            parse(r#"{"event":"init","operation":"download","remote":"origin","concurrent":true,"concurrenttransfers":3}"#).unwrap(),
            Request::Init {}
        ));
        let up = parse(r#"{"event":"upload","oid":"abc","size":346232,"path":"/tmp/f.png","action":{"href":"x"}}"#).unwrap();
        assert!(matches!(up, Request::Upload { size: 346232, .. }));
        let down = parse(r#"{"event":"download","oid":"abc","size":21245,"action":null}"#).unwrap();
        assert!(matches!(down, Request::Download { size: 21245, .. }));
        assert!(matches!(
            parse(r#"{"event":"terminate"}"#).unwrap(),
            Request::Terminate {}
        ));
    }

    #[test]
    fn unknown_events_and_garbage_are_errors_not_panics() {
        assert!(parse(r#"{"event":"teleport","oid":"a"}"#).is_err());
        assert!(parse("not json").is_err());
        assert!(parse("").is_err());
    }

    /// The wire shape matters as much as the parse: git-lfs matches on exact
    /// key names, and `bytesSoFar` is camelCase where everything else is not.
    #[test]
    fn responses_serialize_to_the_documented_shapes() {
        let ok = serde_json::to_string(&InitResponse { error: None }).unwrap();
        assert_eq!(ok, "{}");

        let done = completion("abc".into(), Some("/tmp/x".into()), Ok(()));
        assert_eq!(
            serde_json::to_string(&done).unwrap(),
            r#"{"event":"complete","oid":"abc","path":"/tmp/x"}"#
        );

        let upload_done = completion("abc".into(), None, Ok(()));
        assert_eq!(
            serde_json::to_string(&upload_done).unwrap(),
            r#"{"event":"complete","oid":"abc"}"#
        );

        let failed = completion("abc".into(), None, Err("boom".into()));
        assert_eq!(
            serde_json::to_string(&failed).unwrap(),
            r#"{"event":"complete","oid":"abc","error":{"code":1,"message":"boom"}}"#
        );

        let progress = serde_json::to_string(&Progress {
            event: "progress",
            oid: "abc",
            bytes_so_far: 1234,
            bytes_since_last: 64,
        })
        .unwrap();
        assert_eq!(
            progress,
            r#"{"event":"progress","oid":"abc","bytesSoFar":1234,"bytesSinceLast":64}"#
        );
    }

    /// A failed download must not report a path — git-lfs would try to move a
    /// file that was deleted, or worse, a truncated one.
    #[test]
    fn a_failed_transfer_never_reports_a_path() {
        let failed = completion(
            "abc".into(),
            Some("/tmp/partial".into()),
            Err("boom".into()),
        );
        assert!(failed.path.is_none());
        assert!(failed.error.is_some());
    }
}
