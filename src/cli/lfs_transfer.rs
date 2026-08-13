//! `ztest lfs-transfer` — Git LFS [custom transfer agent] for the snapshot bucket.
//!
//! - Machine-invoked only: git-lfs spawns it, newline-delimited JSON over stdin/stdout
//! - `.lfsconfig`'s `lfs.standalonetransferagent` skips the batch API (no LFS server
//!   here) → push/pull reach R2 through the same [`Bucket`] [`crate::storage::lfs`] reads
//! - In ztest, not a second binary: agent writes what the seed path reads, and sharing
//!   [`Bucket::key`] makes that agreement compile-time (a drift = every fixture 404ing)
//! - Not the stock `basic` adapter: one `PUT` inherits R2's 4.995 GiB cap, and IRONWOOD
//!   is 8.15 GiB; [`Bucket::put`] runs real S3 multipart
//!
//! [custom transfer agent]: https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::storage::r2::Bucket;

/// Bytes between `progress` messages; 4 MiB keeps a multi-GB transfer visibly moving
/// without thousands of lines down the pipe
const PROGRESS_INTERVAL: u64 = 4 * 1024 * 1024;

/// Download-leg buffer, independent of upload part size (bounds only what sits in
/// memory between socket and file)
const COPY_BUF: usize = 1024 * 1024;

/// `code` on a per-object error. git-lfs reads any non-zero as failure and never
/// interprets the value → one honest code beats an invented taxonomy
const TRANSFER_FAILED: i32 = 1;

pub(crate) fn execute() -> ExitCode {
    // One sequential stdin conversation; git-lfs concurrency = several agent
    // *processes*, never interleaved requests within one
    super::block_on("lfs-transfer", super::Rt::Current, run())
}

// ─────────────────────────── protocol ───────────────────────────────

/// Message from git-lfs. Unknown fields ignored — the spec's `operation`, `remote`,
/// `concurrent` and `action` are useless to a standalone agent, and more may appear
#[derive(Debug, Deserialize)]
#[serde(tag = "event", rename_all = "lowercase")]
enum Request {
    Init {},
    Upload { oid: String, size: u64, path: PathBuf },
    Download { oid: String, size: u64 },
    Terminate {},
}

#[derive(Debug, Serialize)]
struct ProtocolError {
    code: i32,
    message: String,
}

/// Reply to `init`: `{}` on success, else an `error` git-lfs reports before abandoning
#[derive(Debug, Serialize)]
struct InitResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ProtocolError>,
}

/// Ends one object's transfer. `path` for `download` only — git-lfs *moves* that file
/// into `.git/lfs/objects`, so ownership of the bytes passes here
#[derive(Debug, Serialize)]
struct Complete {
    event: &'static str,
    oid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ProtocolError>,
}

/// git-lfs `progress` event — protocol wire shape, unrelated to the crate's own
/// [`Progress`](crate::resource::Progress) reports
#[derive(Debug, Serialize)]
struct ProgressEvent<'a> {
    event: &'static str,
    oid: &'a str,
    #[serde(rename = "bytesSoFar")]
    bytes_so_far: u64,
    #[serde(rename = "bytesSinceLast")]
    bytes_since_last: u64,
}

/// Write one protocol message. Stdout carries protocol and nothing else (a stray
/// `println!` corrupts the stream into a parse error pointing nowhere near the cause)
fn send<T: Serialize>(msg: &T) -> Result<(), String> {
    let mut line = serde_json::to_vec(msg).map_err(|e| format!("encode response: {e}"))?;
    line.push(b'\n');
    let mut out = std::io::stdout().lock();
    out.write_all(&line).and_then(|()| out.flush()).map_err(|e| format!("write to git-lfs: {e}"))
}

/// Emits `progress` at most every [`PROGRESS_INTERVAL`] bytes
struct ProgressReporter {
    oid: String,
    total: u64,
    since_last: u64,
}

impl ProgressReporter {
    fn new(oid: &str) -> Self {
        Self { oid: oid.to_string(), total: 0, since_last: 0 }
    }

    /// Account `n` bytes, emitting once enough accumulate. Send failures dropped
    /// (progress is advisory; the next real message reports a broken stdout)
    fn advance(&mut self, n: usize) {
        self.total += n as u64;
        self.since_last += n as u64;
        if self.since_last >= PROGRESS_INTERVAL {
            let _ = send(&ProgressEvent {
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

    // `init` is always first → single exchange, not a loop. Resolving the bucket here
    // turns a misconfigured env into one error, not N identical per-object failures
    let Some(line) = next_line(&mut lines).await? else {
        // Pipe closed before `init` — nothing was asked
        return Ok(());
    };
    let bucket = match parse(&line)? {
        Request::Init {} => match Bucket::resolve() {
            Ok(bucket) => {
                send(&InitResponse { error: None })?;
                bucket
            }
            Err(e) => {
                // Reported in-band, then end the session (every transfer fails alike)
                send(&InitResponse {
                    error: Some(ProtocolError { code: TRANSFER_FAILED, message: e.to_string() }),
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
                    // Never leave a partial file wearing an OID's name
                    let _ = tokio::fs::remove_file(&dest).await;
                }
                send(&completion(oid, path, result))?;
            }
        }
    }

    // What git-lfs moved out is gone; this clears the failures' leftovers
    let _ = std::fs::remove_dir_all(&scratch);
    Ok(())
}

/// One object's outcome as `complete`. Failure travels *in the message*, never as an
/// exit (which would abort every other object in the batch)
fn completion(oid: String, path: Option<String>, result: Result<(), String>) -> Complete {
    match result {
        Ok(()) => Complete { event: "complete", oid, path, error: None },
        Err(message) => Complete {
            event: "complete",
            oid,
            path: None,
            error: Some(ProtocolError { code: TRANSFER_FAILED, message }),
        },
    }
}

async fn upload(
    bucket: &Bucket,
    oid: &str,
    size: u64,
    path: &std::path::Path,
) -> Result<(), String> {
    // Content-addressed → a matching oid+size *is* this object, so a re-push after a
    // partial failure costs one HEAD instead of gigabytes
    if bucket.has(oid, size).await.map_err(|e| format!("checking for an existing object: {e}"))? {
        return Ok(());
    }

    let file =
        tokio::fs::File::open(path).await.map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut progress = ProgressReporter::new(oid);
    bucket.put(oid, size, file, &mut |n| progress.advance(n)).await.map_err(|e| e.to_string())
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

    // Integrity = git-lfs's job (it hashes what we hand back); re-verifying here reads
    // every byte twice for the same verdict
    let mut progress = ProgressReporter::new(oid);
    let mut buf = vec![0u8; COPY_BUF];
    loop {
        let n = source.read(&mut buf).await.map_err(|e| format!("read object: {e}"))?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n]).await.map_err(|e| format!("write {}: {e}", dest.display()))?;
        progress.advance(n);
    }
    file.flush().await.map_err(|e| format!("flush {}: {e}", dest.display()))
}

/// In-flight downloads, keyed by pid (git-lfs spawns several agents concurrently)
fn scratch_dir() -> Result<PathBuf, String> {
    let dir = std::env::temp_dir().join(format!("ztest-lfs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    Ok(dir)
}

async fn next_line<R>(lines: &mut tokio::io::Lines<R>) -> Result<Option<String>, String>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    lines.next_line().await.map_err(|e| format!("read from git-lfs: {e}"))
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
        assert!(matches!(parse(r#"{"event":"terminate"}"#).unwrap(), Request::Terminate {}));
    }

    #[test]
    fn unknown_events_and_garbage_are_errors_not_panics() {
        assert!(parse(r#"{"event":"teleport","oid":"a"}"#).is_err());
        assert!(parse("not json").is_err());
        assert!(parse("").is_err());
    }

    /// git-lfs matches exact key names, and `bytesSoFar` is camelCase where nothing
    /// else is → wire shape matters as much as the parse
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

        let progress = serde_json::to_string(&ProgressEvent {
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

    /// Failed download must report no path (git-lfs would move a deleted, or truncated, file)
    #[test]
    fn a_failed_transfer_never_reports_a_path() {
        let failed = completion("abc".into(), Some("/tmp/partial".into()), Err("boom".into()));
        assert!(failed.path.is_none());
        assert!(failed.error.is_some());
    }
}
