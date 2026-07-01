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
//! select on the `zaino.io/role=test-env` label and `janitor/ttl` annotation,
//! not the prefix. Inside the namespace, components keep short stable names
//! (`zebrad`, `zaino`, …) with a deterministic FQDN
//! `{name}.{namespace}.svc.cluster.local`. Concurrent tests never collide
//! because they live in different namespaces (no slot pattern needed).
//!
//! Full, untruncated identity (package, `module::test`, user) is also stamped
//! as namespace labels (queryable via `kubectl get ns -l`) and a
//! `zaino.io/test-full` annotation (no length limit), so name truncation
//! never loses information.

/// Where the test process thinks it is. Picked once at `TestEnv::build`.
#[derive(Debug, Clone)]
pub struct RunCoords {
    /// `${GITHUB_RUN_ID}` in CI, `${USER}-${PPID}` in dev. Stamped as a label
    /// on every resource so an operator can group all envs from one CI run or
    /// dev session.
    pub run_id: String,
    /// The invoking user (`${USER}`, or `anon`). Stamped as the
    /// `zaino.io/user` namespace label for per-developer filtering.
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

/// Slugify `s` into a DNS-1123-safe fragment of at most `max` chars:
/// lowercase, every run of non-alphanumeric characters collapsed to a single
/// `-`, then trimmed of leading/trailing `-`. Empty input (or input that slugs
/// to nothing) yields `"x"` so the result is always a valid label that starts
/// and ends alphanumeric. Used for both name fragments and label values (label
/// values forbid `:`, so a raw `module::test` path must be slugged first).
pub fn slug(s: &str, max: usize) -> String {
    let mut out = String::with_capacity(s.len().min(max));
    let mut pending_dash = false;
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            if pending_dash && !out.is_empty() {
                out.push('-');
            }
            pending_dash = false;
            out.push(c.to_ascii_lowercase());
            if out.len() >= max {
                break;
            }
        } else {
            pending_dash = true;
        }
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
