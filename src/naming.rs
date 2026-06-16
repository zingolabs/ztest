//! Run / test naming. Pure functions over the environment — no I/O.
//!
//! Each `TestEnv` gets its own Kubernetes namespace named
//! `kn-{test_id}`. That gives every test isolated DNS: components have
//! short, stable names (`zebrad`, `zaino`, …) inside the namespace
//! and a deterministic FQDN `{name}.kn-{test_id}.svc.cluster.local`
//! outside. Concurrent tests never collide because they live in
//! different namespaces — no slot pattern needed.

/// Where the test process thinks it is. Picked once at `TestEnv::build`.
#[derive(Debug, Clone)]
pub struct RunCoords {
    /// `${GITHUB_RUN_ID}` in CI, `${USER}-${PPID}` in dev. Stamped as a
    /// label on every resource so an operator can group all envs from
    /// one CI run or dev session.
    pub run_id: String,
}

impl RunCoords {
    /// Compute coords from environment variables and the parent process.
    pub fn from_env() -> Result<Self, NamingError> {
        let ci_run_id = std::env::var("ZCASH_KUBE_NET_RUN_ID")
            .ok()
            .or_else(|| std::env::var("GITHUB_RUN_ID").ok());

        let run_id = match ci_run_id {
            Some(id) => id,
            None => {
                let user = std::env::var("USER").unwrap_or_else(|_| "anon".into());
                // nextest's PID disambiguates concurrent `cargo nextest` invocations.
                let ppid = ppid();
                format!("{user}-{ppid}")
            }
        };

        Ok(RunCoords { run_id })
    }
}

#[cfg(target_family = "unix")]
fn ppid() -> u32 {
    // libc not in deps; read /proc instead — Linux-only matches our target.
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
fn ppid() -> u32 { 0 }

/// Short random token used as the namespace suffix. 8 hex chars —
/// collision probability across realistic concurrent test counts is
/// negligible.
pub fn test_suffix() -> String {
    let v: u32 = rand::random();
    format!("{v:08x}")
}

/// Per-test Kubernetes namespace. `kn` stands for `kube-net`; the
/// suffix is unique per `TestEnv`.
pub fn namespace_for(test_id: &str) -> String {
    format!("kn-{test_id}")
}

#[derive(Debug, thiserror::Error)]
pub enum NamingError {}
