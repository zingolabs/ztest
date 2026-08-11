//! Run / test naming. Pure functions over the environment, no I/O.
//!
//! Each `TestEnv` gets its own Kubernetes namespace named
//! `ztest-{package}-{test}-{suffix}` (e.g. `ztest-wallet-tests-getblockrange-3af19c2b`).
//! The `ztest-` prefix marks the namespace as a ztest-created test env
//! (`kubectl get ns | grep ztest-`); the slugged package + test make
//! `kubectl get ns` self-describing during a hang; the 8-hex suffix keeps
//! re-runs and rstest `case_N` parametrizations from colliding. Slugs are
//! truncated so the whole name stays inside the 63-char DNS-1123 label limit.
//! Nothing functional keys on the name: cleanup, the janitor, and any RBAC
//! select on the `ztest.io/role=test-env` label and `janitor/ttl` annotation,
//! not the prefix. Inside the namespace, components keep short stable names
//! (`zebrad`, `zaino`, …) with a deterministic FQDN
//! `{name}.{namespace}.svc.cluster.local`. Concurrent tests never collide
//! because they live in different namespaces (no slot pattern needed).
//!
//! Full, untruncated identity (package, `module::test`, user) is also stamped
//! as namespace labels (queryable via `kubectl get ns -l`) and a
//! `ztest.io/test-full` annotation (no length limit), so name truncation
//! never loses information.

/// Where the test process thinks it is. Picked once at `TestEnv::build`.
#[derive(Debug, Clone)]
pub struct RunCoords {
    /// `${GITHUB_RUN_ID}` in CI, `${USER}-${PPID}` in dev. Stamped as a label
    /// on every resource so an operator can group all envs from one CI run or
    /// dev session.
    pub run_id: String,
    /// The invoking user (`${USER}`, or `anon`). Stamped as the
    /// `ztest.io/user` namespace label for per-developer filtering.
    pub user: String,
}

impl RunCoords {
    /// Compute coords from environment variables and the parent process.
    pub fn from_env() -> Result<Self, NamingError> {
        let ci_run_id = std::env::var("ZTEST_RUN_ID")
            .ok()
            .or_else(|| std::env::var("GITHUB_RUN_ID").ok());

        let user = std::env::var("USER").unwrap_or_else(|_| "anon".into());
        let run_id = match ci_run_id {
            Some(id) => id,
            None => {
                // nextest's PID disambiguates concurrent `cargo nextest` invocations.
                let ppid = ppid();
                format!("{user}-{ppid}")
            }
        };

        Ok(RunCoords { run_id, user })
    }
}

#[cfg(target_family = "unix")]
fn ppid() -> u32 {
    // libc not in deps; read /proc instead (Linux-only, matches our target).
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("PPid:").map(|v| v.trim().parse().ok()))
                .flatten()
        })
        .unwrap_or(0)
}

#[cfg(not(target_family = "unix"))]
fn ppid() -> u32 {
    0
}

/// The invoking developer, slugged for use as a label *value*. The single
/// source of truth for the `ztest.io/user` label: engine runs, detached syncs,
/// and `ztest cleanup`'s "mine" filter must all derive the value the same way,
/// or cleanup silently misses resources it owns.
///
/// Deliberately *not* `ZTEST_SA`: a shared remote ServiceAccount identifies the
/// credential, not the person, so two developers on one SA would reap each
/// other's namespaces.
pub fn current_user() -> String {
    let raw = std::env::var("USER").unwrap_or_else(|_| "anon".into());
    slug(&raw, DNS_LABEL_MAX)
}

/// Short random token used as the namespace suffix. 8 hex chars; collision
/// probability across realistic concurrent test counts is negligible.
pub fn test_suffix() -> String {
    let v: u32 = rand::random();
    format!("{v:08x}")
}

/// Per-test Kubernetes namespace: `ztest-{package}-{test}-{suffix}`.
/// `suffix` (from [`test_suffix`]) makes it unique per `TestEnv`. The
/// package and test slugs are truncated so the whole name fits the
/// 63-char DNS-1123 limit (`6 + 16 + 1 + 24 + 1 + 8 = 56`).
pub fn namespace_for(package: &str, test: &str, suffix: &str) -> String {
    format!("ztest-{}-{}-{}", slug(package, 16), slug(test, 24), suffix)
}

/// Maximum length of a DNS-1123 label: the ceiling for a Kubernetes object-name
/// segment and for a label *value*. Slugs bound for either pass this as `max`.
pub const DNS_LABEL_MAX: usize = 63;

/// Env var carrying the per-test namespace the parent `ztest run` created for a
/// runner pod. On the pod path the laptop owns the namespace's lifecycle (create,
/// follow, teardown), so it picks the name and injects it here; the in-pod
/// [`TestEnv::build`](crate::TestEnv::build) reads it instead of inventing its
/// own, and skips namespace creation and teardown. Unset on the local path,
/// where `TestEnv` runs in-process and owns the namespace itself.
pub const TEST_NAMESPACE_ENV: &str = "ZTEST_TEST_NAMESPACE";

/// Slugify `s` into a DNS-1123-safe fragment of at most `max` chars:
/// lowercase, every run of non-alphanumeric characters collapsed to a single
/// `-`, then trimmed of leading/trailing `-`. Empty input (or input that slugs
/// to nothing) yields `"x"` so the result is always a valid label that starts
/// and ends alphanumeric. Used for both name fragments and label values (label
/// values forbid `:`, so a raw `module::test` path must be slugged first).
///
/// `max` is a hard postcondition — `slug(s, n).len() <= n` for every `s` and
/// every `n >= 1` — because callers feed the result straight to the API server,
/// where one byte over the limit is a 422 and not a truncation.
pub fn slug(s: &str, max: usize) -> String {
    let mut out = String::with_capacity(s.len().min(max));
    let mut pending_dash = false;
    for c in s.chars() {
        if !c.is_ascii_alphanumeric() {
            pending_dash = true;
            continue;
        }
        // Reserve the separator together with the character it precedes, and
        // check *before* writing either. Bounding only the alphanumeric — or
        // testing the length after the push — lets the pair overshoot `max` by
        // the separator's byte whenever truncation lands on a word boundary,
        // which is how a 63-char cap once emitted a 64-byte label value and the
        // API server rejected the whole namespace. Emitting the separator only
        // as part of a pair is also what makes a trailing `-` unrepresentable.
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

/// The running test's name (`module::test`, including any rstest `case_N`),
/// read from the libtest thread name. `TestEnv::build` runs in the test body,
/// and on every `#[tokio::test]` flavor that future is driven on the
/// test-named thread, so the name survives (only `tokio::spawn`ed tasks
/// degrade to `tokio-rt-worker`, and `build` is awaited directly). nextest
/// does not set `NEXTEST_TEST_NAME`, so the thread name is the source of
/// truth; the env var and `"unknown"` are fallbacks only.
pub fn current_test_name() -> String {
    std::thread::current()
        .name()
        .map(str::to_string)
        .or_else(|| std::env::var("NEXTEST_TEST_NAME").ok())
        .unwrap_or_else(|| "unknown".into())
}

/// The test crate's package name (`wallet-tests`, `walletless-tests`), from the
/// `CARGO_PKG_NAME` cargo sets for the running test process. This is the
/// runtime env var (the test binary's crate), not `env!("CARGO_PKG_NAME")`
/// (which would resolve to `ztest`).
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
        assert_eq!(
            slug("fetch_service::get_block", 24),
            "fetch-service-get-block"
        );
        assert_eq!(slug("case_1_zebrad", 24), "case-1-zebrad");
        // collapses runs, trims ends, lowercases
        assert_eq!(slug("::Foo__Bar::", 24), "foo-bar");
        // truncation keeps it within bounds and alphanumeric-terminated
        let long = slug("get_block_range_no_pools_returns_sapling_orchard", 24);
        assert!(long.len() <= 24);
        assert!(long.chars().last().unwrap().is_ascii_alphanumeric());
        // empty / all-separator input never yields an empty (invalid) label
        assert_eq!(slug("", 8), "x");
        assert_eq!(slug("::__::", 8), "x");
    }

    /// The separator used to be written without a bounds check, so a slug whose
    /// truncation point landed on a word boundary came back one byte over `max`.
    /// At `DNS_LABEL_MAX` that is a 64-byte label value, which the API server
    /// rejects with a 422 that fails the test rather than the name.
    #[test]
    fn slug_never_exceeds_max_when_truncating_on_a_word_boundary() {
        // The exact input that produced a 64-byte value: `...scriptpubkey`
        // fills the slug to 62, so the next word costs a separator plus a
        // character and lands on 64.
        let s = slug(
            "testnet::tests::output_addresses_are_read_from_both_scriptpubkey_shapes",
            DNS_LABEL_MAX,
        );
        assert!(s.len() <= DNS_LABEL_MAX, "{} bytes: {s}", s.len());
    }

    /// The postcondition is the whole point of the `max` parameter, so assert it
    /// as one — over every cap a caller could pass, against inputs whose word
    /// boundaries fall at different offsets. A single hand-picked case only
    /// covers the one offset it happens to hit.
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
                assert!(
                    s.len() <= max,
                    "slug({input:?}, {max}) returned {} bytes: {s}",
                    s.len()
                );
                assert!(!s.is_empty(), "slug({input:?}, {max}) was empty");
                assert!(
                    s.chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
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
