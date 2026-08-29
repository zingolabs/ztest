//! `--extra-config <uri>`: cluster facts fetched from a file or an https URL.
//!
//! - Facts only ([`ClusterSpec`](crate::cluster_config::ClusterSpec)); the credential stays
//!   in the operator's kubeconfig
//! - Remote source = whoever serves it chooses the registry ztest pulls and runs images from
//!   → transport is https-only, and the caller confirms before the profile is written
//! - Hardening decisions are pure fns here, so each is tested without a server

use std::path::PathBuf;
use std::time::Duration;

use crate::cluster_config::{ClusterSpec, ExtraConfig};

/// Whole-request budget: a config is one small document, never a slow stream
const FETCH_TIMEOUT: Duration = Duration::from_secs(20);
/// Read-side ceiling. Enforced while reading, never off `Content-Length` (attacker-chosen)
const MAX_BYTES: usize = 64 * 1024;
/// Redirects followed at most (`Attempt::previous` counts the original request, so hop *n*
/// arrives carrying *n* entries)
const MAX_REDIRECTS: usize = 5;
/// Ceiling on a parse message quoted back in an error
const MESSAGE_MAX: usize = 200;

#[derive(Debug, thiserror::Error)]
pub enum ExtraConfigError {
    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{uri}: only https:// is fetched")]
    InsecureScheme { uri: String },

    #[error("http client: {0}")]
    Client(#[source] reqwest::Error),

    #[error("GET {uri}: {source}")]
    Fetch {
        uri: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("GET {uri}: {status}")]
    Status { uri: String, status: reqwest::StatusCode },

    #[error("{uri}: over {limit} bytes")]
    TooLarge { uri: String, limit: usize },

    #[error("{uri}: not utf-8")]
    NotUtf8 { uri: String },

    /// Position + scrubbed message, never `toml::de::Error`'s own Display — that embeds the
    /// offending source line, so a one-line body reaches the terminal whole
    #[error("parse {source_name} at line {line} column {column}: {message}")]
    Parse { source_name: String, line: usize, column: usize, message: String },

    /// Values are echoed for consent and the renderer emits them verbatim, so `\r`/ESC would
    /// repaint the prompt over a registry the operator never saw
    #[error("{field}: control character in value")]
    Control { field: &'static str },
}

/// Where a `--extra-config` value points
#[derive(Debug, PartialEq, Eq)]
pub enum Source {
    Local(PathBuf),
    Remote(String),
}

impl Source {
    pub fn is_remote(&self) -> bool {
        matches!(self, Source::Remote(_))
    }

    fn name(&self) -> String {
        match self {
            Source::Local(p) => p.display().to_string(),
            Source::Remote(u) => u.clone(),
        }
    }
}

/// `https://` fetches, a bare scheme-less value is a path, any other scheme is refused.
///
/// - Refusing rather than falling through: `http://x` as a filename fails as a missing
///   file, which hides that the transport was the problem
pub fn source_of(uri: &str) -> Result<Source, ExtraConfigError> {
    if let Some(rest) = uri.split_once("://") {
        return match rest.0 {
            "https" => Ok(Source::Remote(uri.to_string())),
            _ => Err(ExtraConfigError::InsecureScheme { uri: uri.to_string() }),
        };
    }
    Ok(Source::Local(PathBuf::from(uri)))
}

/// What a redirect hop earns
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Hop {
    Follow,
    Downgraded,
    TooMany,
}

/// Scheme re-checked every hop: reqwest's default policy walks https → http silently
pub(crate) fn hop_verdict(scheme: &str, hops: usize) -> Hop {
    match (scheme, hops) {
        (_, n) if n > MAX_REDIRECTS => Hop::TooMany,
        ("https", _) => Hop::Follow,
        _ => Hop::Downgraded,
    }
}

/// Control characters dropped, length bounded.
///
/// - Parse messages quote keys back, and a served document chooses its keys
pub(crate) fn scrub(s: &str) -> String {
    let kept: String = s.chars().filter(|c| !c.is_control()).take(MESSAGE_MAX).collect();
    match kept.chars().count() == MESSAGE_MAX {
        true => format!("{kept}…"),
        false => kept,
    }
}

/// 1-based `(line, column)` of a byte offset, taken while the body is still in hand
pub(crate) fn position(body: &str, at: usize) -> (usize, usize) {
    let (mut line, mut column) = (1, 1);
    for b in body.bytes().take(at) {
        match b {
            b'\n' => (line, column) = (line + 1, 1),
            _ => column += 1,
        }
    }
    (line, column)
}

/// First value carrying a control character.
///
/// - Rejected at the boundary, not in the renderer: the echo is the only gate in front of a
///   `push`/`pull` the operator is about to run images from
/// - No registry reference or Kubernetes object name carries one
pub(crate) fn control_char_field(spec: &ClusterSpec) -> Option<&'static str> {
    spec.fields().into_iter().find(|(_, v)| v.chars().any(char::is_control)).map(|(k, _)| k)
}

/// Accumulate under [`MAX_BYTES`], so an endless body fails on what arrived
pub(crate) fn push_capped(buf: &mut Vec<u8>, chunk: &[u8], limit: usize) -> bool {
    if buf.len().saturating_add(chunk.len()) > limit {
        return false;
    }
    buf.extend_from_slice(chunk);
    true
}

fn client() -> Result<reqwest::Client, ExtraConfigError> {
    let policy = reqwest::redirect::Policy::custom(|attempt| {
        match hop_verdict(attempt.url().scheme(), attempt.previous().len()) {
            // `previous` counts the original request, so hop n arrives carrying n entries
            Hop::Follow => attempt.follow(),
            Hop::Downgraded => attempt.error("redirect left https"),
            Hop::TooMany => attempt.error("too many redirects"),
        }
    });
    reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .redirect(policy)
        .build()
        .map_err(ExtraConfigError::Client)
}

async fn fetch(uri: &str) -> Result<String, ExtraConfigError> {
    let mut resp = client()?
        .get(uri)
        .send()
        .await
        .map_err(|source| ExtraConfigError::Fetch { uri: uri.to_string(), source })?;
    if !resp.status().is_success() {
        return Err(ExtraConfigError::Status { uri: uri.to_string(), status: resp.status() });
    }
    // Content-Type ignored throughout: `toml::from_str` is the only validation worth trusting
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|source| ExtraConfigError::Fetch { uri: uri.to_string(), source })?
    {
        if !push_capped(&mut buf, &chunk, MAX_BYTES) {
            return Err(ExtraConfigError::TooLarge { uri: uri.to_string(), limit: MAX_BYTES });
        }
    }
    String::from_utf8(buf).map_err(|_| ExtraConfigError::NotUtf8 { uri: uri.to_string() })
}

/// Read `source` and take its `[ztest]` table.
///
/// - Split from [`source_of`] so a caller settles consent *before* the request: refusing
///   after fetching spends a round trip on a config that cannot be accepted
pub async fn load_from(source: &Source) -> Result<ClusterSpec, ExtraConfigError> {
    let body = match source {
        Source::Remote(url) => fetch(url).await?,
        Source::Local(path) => std::fs::read_to_string(path)
            .map_err(|e| ExtraConfigError::Read { path: path.clone(), source: e })?,
    };
    let parsed: ExtraConfig = toml::from_str(&body).map_err(|e| {
        let (line, column) = position(&body, e.span().map_or(0, |s| s.start));
        ExtraConfigError::Parse {
            source_name: source.name(),
            line,
            column,
            message: scrub(e.message()),
        }
    })?;
    match control_char_field(&parsed.ztest) {
        Some(field) => Err(ExtraConfigError::Control { field }),
        None => Ok(parsed.ztest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_config::{ClusterClass, Profile};

    const DOC: &str = "\
[ztest]
class = \"remote\"
push = \"zot.example.ts.net/ztest\"
push_secret = \"ztest-registry-creds\"
storage_class = \"topolvm-thin\"
snapshot_class = \"ztest-snapshot\"
";

    fn spec(body: &str) -> Result<ClusterSpec, toml::de::Error> {
        toml::from_str::<ExtraConfig>(body).map(|c| c.ztest)
    }

    #[test]
    fn a_ztest_table_overlays_onto_a_profile() {
        let mut profile = Profile::for_context("admin@zingo-infra");
        spec(DOC).expect("valid document").apply_to(&mut profile);

        assert_eq!(profile.class, ClusterClass::Remote);
        assert_eq!(profile.push.as_deref(), Some("zot.example.ts.net/ztest"));
        assert_eq!(profile.push_secret.as_deref(), Some("ztest-registry-creds"));
        assert_eq!(profile.storage_class.as_deref(), Some("topolvm-thin"));
        assert_eq!(profile.snapshot_class.as_deref(), Some("ztest-snapshot"));
        // Untouched by the overlay = still the operator's
        assert_eq!(profile.context.as_deref(), Some("admin@zingo-infra"));
        assert!(profile.validate().is_ok());
    }

    /// Overlay is not a replace: `add` runs it under a profile that already has a context
    #[test]
    fn an_absent_field_leaves_what_was_there() {
        let mut profile = Profile::for_context("ctx");
        profile.push = Some("kept.example/img".into());
        spec("[ztest]\npush_secret = \"s\"\n").expect("valid").apply_to(&mut profile);
        assert_eq!(profile.push.as_deref(), Some("kept.example/img"));
        assert_eq!(profile.push_secret.as_deref(), Some("s"), "the set field must still land");
    }

    /// Identity is the one thing a downloaded file must never move
    #[test]
    fn the_spec_cannot_name_a_context_or_kubeconfig() {
        for key in ["context", "kubeconfig"] {
            let body = format!("[ztest]\n{key} = \"anything\"\n");
            let err = spec(&body).expect_err("must reject");
            assert!(err.message().contains("unknown field"), "{key}: {}", err.message());
        }
    }

    #[test]
    fn an_unknown_key_is_named_rather_than_ignored() {
        let err = spec("[ztest]\npsuh = \"typo\"\n").expect_err("must reject");
        assert!(err.message().contains("unknown field"), "{}", err.message());
    }

    #[test]
    fn a_document_with_no_ztest_table_is_rejected() {
        let err = spec("[harbor]\nurl = \"https://example\"\n").expect_err("no [ztest]");
        assert!(err.message().contains("ztest"), "{}", err.message());
    }

    /// Sibling sections are the reason the file is namespaced at all
    #[test]
    fn a_section_for_another_tool_is_tolerated() {
        let body = format!("{DOC}\n[harbor]\nurl = \"https://example\"\n");
        let fields = spec(&body).expect("siblings allowed").fields();
        assert!(fields.iter().any(|(k, _)| *k == "push"), "{fields:?}");
    }

    #[test]
    fn only_https_is_fetched() {
        assert_eq!(
            source_of("https://example/c.toml").expect("https"),
            Source::Remote("https://example/c.toml".into())
        );
        for uri in ["http://example/c.toml", "ftp://example/c.toml", "file:///tmp/c.toml"] {
            let err = source_of(uri).expect_err("must not be fetched");
            assert!(matches!(err, ExtraConfigError::InsecureScheme { .. }), "{uri}: {err}");
        }
    }

    #[test]
    fn a_scheme_less_value_is_a_path() {
        assert_eq!(
            source_of("./cluster.toml").expect("path"),
            Source::Local(PathBuf::from("./cluster.toml"))
        );
    }

    #[test]
    fn a_redirect_may_not_leave_https_or_loop() {
        assert_eq!(hop_verdict("https", 1), Hop::Follow);
        assert_eq!(hop_verdict("http", 1), Hop::Downgraded);
        // `previous` counts the original request, so the last allowed hop arrives at MAX
        assert_eq!(hop_verdict("https", MAX_REDIRECTS), Hop::Follow);
        assert_eq!(hop_verdict("https", MAX_REDIRECTS + 1), Hop::TooMany);
    }

    /// One-shot loopback responder. `fetch` skips the scheme check ([`source_of`] owns it),
    /// so plain http reaches the real client and exercises its wiring
    async fn serve_once(response: Vec<u8>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
        let port = listener.local_addr().expect("bound port").port();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            if let Ok((mut sock, _)) = listener.accept().await {
                let _ = sock.read(&mut [0u8; 2048]).await;
                let _ = sock.write_all(&response).await;
                let _ = sock.shutdown().await;
            }
        });
        format!("http://127.0.0.1:{port}/c.toml")
    }

    fn ok_response(body: &str) -> Vec<u8> {
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len()).into_bytes()
    }

    /// Deleting `.redirect(policy)` puts reqwest's default back, which follows this happily
    #[tokio::test]
    async fn the_client_refuses_a_redirect_instead_of_following_it() {
        let target = serve_once(ok_response("[ztest]\npush = \"followed\"\n")).await;
        let start = serve_once(
            format!("HTTP/1.1 302 Found\r\nLocation: {target}\r\nContent-Length: 0\r\n\r\n")
                .into_bytes(),
        )
        .await;
        let err = fetch(&start).await.expect_err("redirect must not be followed");
        assert!(matches!(err, ExtraConfigError::Fetch { .. }), "{err}");
    }

    /// Dropping [`push_capped`] from the read loop leaves this body arriving intact
    #[tokio::test]
    async fn the_read_loop_stops_at_the_cap() {
        let url = serve_once(ok_response(&"#".repeat(MAX_BYTES + 1024))).await;
        let err = fetch(&url).await.expect_err("over the cap");
        assert!(matches!(err, ExtraConfigError::TooLarge { .. }), "{err}");
    }

    /// `toml::de::Error`'s own Display embeds the offending source line, so a one-line body
    /// would reach the operator's terminal whole
    #[tokio::test]
    async fn a_parse_failure_reports_position_without_the_body() {
        let secret = "SENTINEL-do-not-echo";
        let dir = std::env::temp_dir().join(format!("ztest-xc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let path = dir.join("bad.toml");
        std::fs::write(&path, format!("[ztest]\npush = \"{secret}\"\nbogus = 1\n"))
            .expect("write fixture");

        let err = load_from(&Source::Local(path.clone())).await.expect_err("must reject");
        let shown = err.to_string();
        assert!(!shown.contains(secret), "body leaked: {shown}");
        assert!(shown.contains("line 3"), "position missing: {shown}");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_scrubbed_message_keeps_no_control_bytes() {
        let scrubbed = scrub("unknown field `\u{1b}[2K\rfake`");
        assert!(!scrubbed.chars().any(char::is_control), "{scrubbed:?}");
        assert!(scrubbed.contains("unknown field"), "{scrubbed:?}");
    }

    #[test]
    fn a_position_counts_lines_and_columns_from_one() {
        assert_eq!(position("ab\ncd", 0), (1, 1));
        assert_eq!(position("ab\ncd", 3), (2, 1));
        assert_eq!(position("ab\ncd", 4), (2, 2));
        // Offset past the end must not panic on a multi-byte tail
        assert_eq!(position("é", 99).0, 1);
    }

    /// The echo is the only gate in front of the registry ztest runs images from, and the
    /// renderer emits values verbatim — ESC/`\r` would repaint it
    #[test]
    fn a_control_character_in_a_value_is_refused() {
        let hostile = spec("[ztest]\npull = \"good\\u001B[2K\\rgood\"\n").expect("parses");
        assert_eq!(control_char_field(&hostile), Some("pull"));
        assert_eq!(control_char_field(&spec(DOC).expect("clean")), None);
    }

    /// Cap rides the read loop, so a lying `Content-Length` buys nothing
    #[test]
    fn the_body_cap_counts_what_arrived() {
        let mut buf = Vec::new();
        assert!(push_capped(&mut buf, &[0u8; 8], 10));
        assert!(!push_capped(&mut buf, &[0u8; 8], 10));
        assert_eq!(buf.len(), 8, "a refused chunk must not land");
    }
}
