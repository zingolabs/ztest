//! Run / test naming. Pure functions over the environment, no I/O.
//!
//! - One namespace per `TestEnv`: `ztest-{package}-{test}-{suffix}`, slugs truncated
//!   into the 63-char DNS-1123 limit; the 8-hex suffix separates re-runs & `case_N`
//! - Name is cosmetic (`kubectl get ns` legible during a hang) — cleanup, janitor and
//!   RBAC all select on `ztest.io/role=test-env` + the `janitor/ttl` annotation
//! - Untruncated identity lives in namespace labels + a `ztest.io/test-full` annotation
//! - Components keep short stable names (`zebrad`, …) at `{name}.{ns}.svc.cluster.local`;
//!   concurrency needs no slot pattern (different namespaces)

/// Where the test process thinks it is; picked once at `TestEnv::build`.
///
/// - `run_id` = `${GITHUB_RUN_ID}` in CI / `${USER}-${PPID}` in dev, stamped on every
///   resource so one CI run or dev session groups
/// - `user` = `${USER}` else `anon` → the `ztest.io/user` namespace label
#[derive(Debug, Clone)]
pub struct RunCoords {
    pub run_id: String,
    pub user: String,
}

impl RunCoords {
    pub fn from_env() -> Result<Self, NamingError> {
        let ci_run_id =
            std::env::var("ZTEST_RUN_ID").ok().or_else(|| std::env::var("GITHUB_RUN_ID").ok());

        let user = std::env::var("USER").unwrap_or_else(|_| "anon".into());
        let run_id = match ci_run_id {
            Some(id) => id,
            None => {
                // nextest's PID separates concurrent `cargo nextest` invocations
                let ppid = ppid();
                format!("{user}-{ppid}")
            }
        };

        Ok(RunCoords { run_id, user })
    }
}

#[cfg(target_family = "unix")]
fn ppid() -> u32 {
    // libc not in deps → /proc instead (Linux-only, matches our target)
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| l.strip_prefix("PPid:").map(|v| v.trim().parse().ok())).flatten()
        })
        .unwrap_or(0)
}

#[cfg(not(target_family = "unix"))]
fn ppid() -> u32 {
    0
}

/// Invoking developer, slugged as a label *value*; sole source of `ztest.io/user`.
///
/// - Engine runs, detached syncs & `ztest cleanup --mine` must derive it identically,
///   else cleanup misses what it owns
/// - Not `ZTEST_SA`: a shared SA names the credential, not the person → two devs on one
///   SA would reap each other's namespaces
pub fn current_user() -> String {
    let raw = std::env::var("USER").unwrap_or_else(|_| "anon".into());
    slug(&raw, DNS_LABEL_MAX)
}

/// Namespace suffix: 8 hex chars (collision negligible at realistic test counts)
pub fn test_suffix() -> String {
    let v: u32 = rand::random();
    format!("{v:08x}")
}

/// Per-test namespace `ztest-{package}-{test}-{suffix}`; [`test_suffix`] makes it
/// unique per `TestEnv`. Slug caps keep it inside DNS-1123 (`6+16+1+24+1+8 = 56`)
pub fn namespace_for(package: &str, test: &str, suffix: &str) -> String {
    format!("ztest-{}-{}-{}", slug(package, 16), slug(test, 24), suffix)
}

/// DNS-1123 label ceiling — k8s object-name segment *and* label value. Slugs bound
/// for either pass this as `max`
pub const DNS_LABEL_MAX: usize = 63;

/// Per-test namespace the parent `ztest run` created for a runner pod.
///
/// - Pod path: laptop owns the lifecycle → in-pod
///   [`TestEnv::build`](crate::TestEnv::build) reads this and skips create/teardown
/// - Unset on the local path, where `TestEnv` owns the namespace itself
pub const TEST_NAMESPACE_ENV: &str = "ZTEST_TEST_NAMESPACE";

/// `s` → DNS-1123-safe fragment ≤ `max`: lowercase, non-alphanumeric runs collapsed to
/// one `-`, ends trimmed. Slugs to nothing → `"x"` (always a valid label).
///
/// `max` is a hard postcondition (`slug(s, n).len() <= n` for `n >= 1`) — callers feed
/// the API server, where one byte over is a 422, not a truncation
pub fn slug(s: &str, max: usize) -> String {
    let mut out = String::with_capacity(s.len().min(max));
    let mut pending_dash = false;
    for c in s.chars() {
        if !c.is_ascii_alphanumeric() {
            pending_dash = true;
            continue;
        }
        // Reserve separator + its character as a pair, checked *before* either is
        // written (bounding only the alphanumeric overshoots `max` by one byte when
        // truncation lands on a word boundary). Pair-only emission also makes a
        // trailing `-` unrepresentable
        let needed = usize::from(pending_dash && !out.is_empty()) + 1;
        if out.len() + needed > max {
            break;
        }
        if needed == 2 {
            out.push('-');
        }
        pending_dash = false;
        out.push(c.to_ascii_lowercase());
    }
    if out.is_empty() { "x".to_string() } else { out }
}

/// `module::test` (incl. rstest `case_N`) from the libtest thread name.
///
/// - `TestEnv::build` is awaited in the test body → driven on the test-named thread on
///   every `#[tokio::test]` flavor (only `tokio::spawn`ed tasks degrade to `tokio-rt-worker`)
/// - nextest sets no `NEXTEST_TEST_NAME` → thread name is truth, env var is a fallback
pub fn current_test_name() -> String {
    std::thread::current()
        .name()
        .map(str::to_string)
        .or_else(|| std::env::var("NEXTEST_TEST_NAME").ok())
        .unwrap_or_else(|| "unknown".into())
}

/// Test crate's package name from the *runtime* `CARGO_PKG_NAME` (the test binary's
/// crate), not `env!("CARGO_PKG_NAME")` — that resolves to `ztest`
pub fn current_package() -> String {
    std::env::var("CARGO_PKG_NAME").unwrap_or_else(|_| "unknown".into())
}

#[derive(Debug, thiserror::Error)]
pub enum NamingError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_is_dns_safe_and_bounded() {
        assert_eq!(slug("fetch_service::get_block", 24), "fetch-service-get-block");
        assert_eq!(slug("case_1_zebrad", 24), "case-1-zebrad");
        // runs collapsed, ends trimmed, lowercased
        assert_eq!(slug("::Foo__Bar::", 24), "foo-bar");
        // truncation stays in bounds & alphanumeric-terminated
        let long = slug("get_block_range_no_pools_returns_sapling_orchard", 24);
        assert!(long.len() <= 24);
        assert!(long.chars().last().unwrap().is_ascii_alphanumeric());
        // empty / all-separator input never yields an invalid empty label
        assert_eq!(slug("", 8), "x");
        assert_eq!(slug("::__::", 8), "x");
    }

    /// Truncation landing on a word boundary must not overshoot `max` — at
    /// `DNS_LABEL_MAX` a 64-byte label value is a 422 that fails the test, not the name
    #[test]
    fn slug_never_exceeds_max_when_truncating_on_a_word_boundary() {
        // Exact input that produced 64: `...scriptpubkey` fills to 62, next word
        // costs separator + character
        let s = slug(
            "testnet::tests::output_addresses_are_read_from_both_scriptpubkey_shapes",
            DNS_LABEL_MAX,
        );
        assert!(s.len() <= DNS_LABEL_MAX, "{} bytes: {s}", s.len());
    }

    /// Asserted over every cap × inputs with word boundaries at differing offsets (a
    /// hand-picked case covers only the offset it happens to hit)
    #[test]
    fn slug_postconditions_hold_at_every_cap() {
        let inputs = [
            "testnet::tests::output_addresses_are_read_from_both_scriptpubkey_shapes",
            "fetch_service::get_block_range_no_pools_returns_sapling_orchard::case_2_zcashd",
            "a_bb_ccc_dddd_eeeee_ffffff_ggggggg_hhhhhhhh_iiiiiiiii_jjjjjjjjjj",
            "x",
            "::__::",
            "",
        ];
        for input in inputs {
            for max in 1..=80 {
                let s = slug(input, max);
                assert!(s.len() <= max, "slug({input:?}, {max}) returned {} bytes: {s}", s.len());
                assert!(!s.is_empty(), "slug({input:?}, {max}) was empty");
                assert!(
                    s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                    "slug({input:?}, {max}) is not DNS-1123 safe: {s}"
                );
                assert!(
                    !s.starts_with('-') && !s.ends_with('-'),
                    "slug({input:?}, {max}) has a bare separator at an end: {s}"
                );
            }
        }
    }

    #[test]
    fn namespace_fits_dns_limit() {
        let ns = namespace_for(
            "walletless-tests",
            "fetch_service::get_block_range_no_pools_returns_sapling_orchard::case_2_zcashd",
            "3af19c2b",
        );
        assert!(ns.starts_with("ztest-"), "{ns}");
        assert!(ns.len() <= 63, "namespace too long ({}): {ns}", ns.len());
    }
}
