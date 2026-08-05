//! Quality-of-service: cluster resource allocation, job scheduling, and
//! priority. A test declares a tier at the call site; the harness lowers that
//! into pod requests/limits and a scheduling footprint, and the in-memory
//! [`scheduler::Scheduler`] admits it against probed cluster capacity. The core
//! holds no clock/randomness/I/O, so admission is a deterministic function over
//! in-memory state, unit-testable without a cluster. See `docs/qos-design.md`.
//!
//! Model: [`Resources`] (a 4-D CPU × RAM × disk-bandwidth × disk-IOPS amount in
//! integer k8s/cgroup units); [`Pool`] (general vs the dedicated NVMe pool
//! `sync` targets); [`QosClass`]/[`QosProfile`] (the tiers and their const
//! profile table). Callers resolve a class to a [`QosProfile`] and hand the
//! scheduler an explicit [`scheduler::Request`], decoupling it from the table.

pub mod governor;
pub mod ledger;
pub mod live;
pub mod schedule;
pub mod scheduler;
pub mod units;

/// The tier attributes: `#[ztest::qos::basic]` .. `#[ztest::qos::sync]`.
/// Surfaced only under `ztest::qos::*`, not the prelude.
pub use ztest_macros::{basic, integration, sync, testnet, wallet};

use std::cell::Cell;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ── Shared k8s label / annotation keys ─────────────────────────────────

/// Label marking an object's role.
pub const LABEL_ROLE: &str = "ztest.io/role";
/// `LABEL_ROLE` value stamped on a per-test namespace by
/// `cluster::ensure_namespace`. `ztest cleanup --all-users` selects on it to
/// reap every per-test namespace.
pub const ROLE_TEST_ENV: &str = "test-env";
/// Label carrying the run identity (`GITHUB_RUN_ID` in CI, `${USER}-${PPID}` in
/// dev), stamped on every resource a run owns so its envs group together and the
/// Ctrl-C reaper can find what a crash left behind. See [`crate::naming::RunCoords`].
pub const LABEL_RUN_ID: &str = "ztest.io/run-id";
/// Label carrying the (slugged) invoking user, stamped on every resource a run
/// owns — namespaces, shadow VolumeSnapshotContents — so `ztest cleanup` can
/// reclaim exactly one developer's resources. Value is `RunCoords::user`
/// slugged; see [`crate::naming::RunCoords`].
pub const LABEL_USER: &str = "ztest.io/user";
/// Label tying a cluster-scoped shadow VolumeSnapshotContent to the per-test
/// namespace it serves. A shadow VSC can't be cascaded by the namespace delete
/// (it's cluster-scoped), so the parent `ztest run` deletes it by this selector
/// at per-test teardown — prompt cleanup, no run-long accumulation.
pub const LABEL_TEST_NS: &str = "ztest.io/test-ns";

// ── Disk-I/O reservation (declared on the PVC / storage request) ────────
//
// Kubernetes has no disk-I/O field in pod `resources`, so the per-volume cap
// rides these PVC annotations, enforced via a cgroup `io.max` (CRI-O blockio).
// Migrates to a `VolumeAttributesClass` once a backend's CSI can honor one.
// See `docs/qos-io-dimension-design.md`.

/// PVC annotation carrying the volume's disk-bandwidth cap (k8s quantity,
/// bytes/sec); mirrors the cgroup `io.max` `{r,w}bps` the harness enforces.
pub const ANNOTATION_IO_BPS: &str = "qos.ztest.io/io-bps";
/// PVC annotation carrying the volume's disk-IOPS cap (plain integer, ops/sec);
/// mirrors cgroup `io.max` `{r,w}iops`.
pub const ANNOTATION_IO_IOPS: &str = "qos.ztest.io/io-iops";

// ── NVMe placement (node taint + nodeSelector) ─────────────────────────
//
// `sync` pods land on the dedicated NVMe nodes via k8s placement (tainted node
// pool + matching toleration/nodeSelector), not a capacity partition (see
// [`Pool`]). The exact label/taint key is TBD (§11); isolated here for a
// one-line production swap.

/// NodeSelector label key marking the NVMe node pool. §11 TBD.
pub const NVME_NODE_LABEL_KEY: &str = "ztest.io/pool";
/// NodeSelector label value selecting the NVMe node pool. §11 TBD.
pub const NVME_NODE_LABEL_VALUE: &str = "nvme";
/// Taint key the NVMe nodes carry; a `sync` pod tolerates it. §11 TBD.
pub const NVME_TAINT_KEY: &str = "ztest.io/pool";

/// One mebibyte, in bytes.
pub const MIB: u64 = 1024 * 1024;
/// One gibibyte, in bytes.
pub const GIB: u64 = 1024 * MIB;

/// A four-dimensional resource amount: CPU millicores, memory bytes, and disk
/// I/O bandwidth (bytes/sec) + operations (IOPS). CPU/memory match k8s
/// `requests`/`limits` units; the I/O pair matches cgroup v2 `io.max`. Integer-
/// only, so packing is exact; all dimensions independent ("fits" = fits in
/// every one).
///
/// The I/O dimensions are inert until calibrated: k8s exposes no I/O
/// `allocatable`, so an uncalibrated node's ceiling is seeded [`u64::MAX`] and
/// admission matches the CPU×memory model. See `docs/qos-io-dimension-design.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Resources {
    /// CPU in millicores (`1000` == one core).
    pub cpu_milli: u64,
    /// Memory in bytes.
    pub mem_bytes: u64,
    /// Disk bandwidth in bytes/sec (cgroup `io.max` `rbps`+`wbps`).
    pub io_bps: u64,
    /// Disk operations per second (cgroup `io.max` `riops`+`wiops`).
    pub io_iops: u64,
}

impl Resources {
    /// The empty amount: every dimension zero.
    pub const ZERO: Resources = Resources {
        cpu_milli: 0,
        mem_bytes: 0,
        io_bps: 0,
        io_iops: 0,
    };

    /// Construct an amount from all four dimensions.
    pub const fn new(cpu_milli: u64, mem_bytes: u64, io_bps: u64, io_iops: u64) -> Self {
        Resources {
            cpu_milli,
            mem_bytes,
            io_bps,
            io_iops,
        }
    }

    /// A CPU×memory amount whose I/O dimensions are unconstrained: `io_bps` and
    /// `io_iops` set to [`u64::MAX`]. For a *ceiling* on a node whose I/O has
    /// not been benchmarked, so the I/O dimensions never gate admission.
    pub const fn cpu_mem_unbounded_io(cpu_milli: u64, mem_bytes: u64) -> Self {
        Resources::new(cpu_milli, mem_bytes, u64::MAX, u64::MAX)
    }

    /// `true` iff `self` fits within `cap` in every dimension: a request is
    /// grantable only when CPU, memory, and both I/O dimensions all fit.
    pub fn fits_within(&self, cap: &Resources) -> bool {
        self.cpu_milli <= cap.cpu_milli
            && self.mem_bytes <= cap.mem_bytes
            && self.io_bps <= cap.io_bps
            && self.io_iops <= cap.io_iops
    }

    /// Dimension-wise sum, `None` on overflow of any dimension.
    pub fn checked_add(&self, other: &Resources) -> Option<Resources> {
        Some(Resources {
            cpu_milli: self.cpu_milli.checked_add(other.cpu_milli)?,
            mem_bytes: self.mem_bytes.checked_add(other.mem_bytes)?,
            io_bps: self.io_bps.checked_add(other.io_bps)?,
            io_iops: self.io_iops.checked_add(other.io_iops)?,
        })
    }

    /// Dimension-wise difference, `None` if any dimension would go negative.
    pub fn checked_sub(&self, other: &Resources) -> Option<Resources> {
        Some(Resources {
            cpu_milli: self.cpu_milli.checked_sub(other.cpu_milli)?,
            mem_bytes: self.mem_bytes.checked_sub(other.mem_bytes)?,
            io_bps: self.io_bps.checked_sub(other.io_bps)?,
            io_iops: self.io_iops.checked_sub(other.io_iops)?,
        })
    }

    /// Dimension-wise difference, clamped at zero per dimension. Used for
    /// `free = available - committed`, which stays well-defined even if a
    /// `reconcile` shrinks `available` below what is already committed (running
    /// leases are not preempted; see [`scheduler::Scheduler::reconcile`]).
    pub fn saturating_sub(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.saturating_sub(other.cpu_milli),
            mem_bytes: self.mem_bytes.saturating_sub(other.mem_bytes),
            io_bps: self.io_bps.saturating_sub(other.io_bps),
            io_iops: self.io_iops.saturating_sub(other.io_iops),
        }
    }

    /// Dimension-wise sum, saturating at `u64::MAX` per dimension.
    pub fn saturating_add(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.saturating_add(other.cpu_milli),
            mem_bytes: self.mem_bytes.saturating_add(other.mem_bytes),
            io_bps: self.io_bps.saturating_add(other.io_bps),
            io_iops: self.io_iops.saturating_add(other.io_iops),
        }
    }

    /// Dimension-wise maximum. Used by the ledger's per-unit dedup rule:
    /// a test that holds both a reservation and Jobs counts as
    /// `max(reservation_footprint, Σ job_requests)`, never their sum.
    pub fn max(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.max(other.cpu_milli),
            mem_bytes: self.mem_bytes.max(other.mem_bytes),
            io_bps: self.io_bps.max(other.io_bps),
            io_iops: self.io_iops.max(other.io_iops),
        }
    }

    /// Dimension-wise minimum. The tighter of two per-dimension bounds — e.g. a
    /// reservation capped by both an SA budget and live headroom.
    pub fn min(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.min(other.cpu_milli),
            mem_bytes: self.mem_bytes.min(other.mem_bytes),
            io_bps: self.io_bps.min(other.io_bps),
            io_iops: self.io_iops.min(other.io_iops),
        }
    }

    /// Guard for any footprint about to size a real pod: a zero-CPU or
    /// zero-memory reserve renders a container k8s treats as BestEffort (the
    /// class this harness forbids), so panic rather than emit it. `ctx` names
    /// the call site.
    pub fn assert_pod_schedulable(&self, ctx: &str) {
        assert!(
            self.cpu_milli > 0,
            "{ctx}: pod footprint has zero CPU — would not be Guaranteed",
        );
        assert!(
            self.mem_bytes > 0,
            "{ctx}: pod footprint has zero memory — would not be Guaranteed",
        );
    }

    /// The `(cpu, memory)` k8s quantity strings for a Guaranteed container at
    /// this footprint: whole-core CPU (rounded up, min 1) and exact-byte memory.
    /// The single canonical lowering every ztest pod renders through, so no site
    /// drifts into a fractional-CPU or Burstable shape.
    ///
    /// Whole cores because the kubelet CPU-manager `static` policy pins exclusive
    /// CPUs only to an integer-core Guaranteed pod; a fractional request falls to
    /// the shared pool. Panics on a degenerate footprint.
    pub fn guaranteed_cpu_mem(&self, ctx: &str) -> (String, String) {
        self.assert_pod_schedulable(ctx);
        let cores = self.cpu_milli.div_ceil(1000).max(1);
        (cores.to_string(), self.mem_bytes.to_string())
    }
}

/// A footprint's CPU / memory in exact tier units (`3c / 3 GiB`, `500m / 512
/// MiB`). The I/O dimensions are omitted: they are `0` pending calibration and
/// carry no signal in a human summary.
impl std::fmt::Display for Resources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cpu = if self.cpu_milli != 0 && self.cpu_milli.is_multiple_of(1000) {
            format!("{}c", self.cpu_milli / 1000)
        } else {
            format!("{}m", self.cpu_milli)
        };
        let mem = if self.mem_bytes != 0 && self.mem_bytes.is_multiple_of(GIB) {
            format!("{} GiB", self.mem_bytes / GIB)
        } else {
            format!("{} MiB", self.mem_bytes / MIB)
        };
        write!(f, "{cpu} / {mem}")
    }
}

/// Footprints for the ephemeral on-cluster build pods (the BuildKit build pod and
/// the seed uploader), in the QoS [`Resources`] model so they share its units,
/// whole-core rendering, and the Guaranteed invariant with the tier pods. Each is
/// created at its footprint for one job and deleted after — no idle reservation
/// and no in-place resize.
pub mod build {
    use super::{GIB, MIB, Resources};

    /// The ephemeral BuildKit build pod's Guaranteed footprint. It is created at
    /// this size for a build and deleted after (no idle/rest reservation), so the
    /// memory need only clear the heaviest layer step's peak while staying under
    /// actuatable node headroom. The compile (folded into the buildkit Dockerfile
    /// build) links the workspace here.
    pub const BUILDKIT_BUILD: Resources = Resources::new(16_000, 24 * GIB, 0, 0);
    /// The ephemeral seed-uploader pod: a trivial `sh` streaming seed bytes into
    /// a PVC. A small fixed Guaranteed reserve so it is never BestEffort.
    pub const UPLOADER: Resources = Resources::new(1_000, 256 * MIB, 0, 0);
}

/// Whole-cluster schedulable capacity: total node `allocatable` minus the
/// reservation of scheduled workloads. One global pool; general/NVMe is k8s
/// placement, not a capacity partition. Produced by the cluster probe, shown by
/// the preflight banner, and the input to admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClusterCapacity {
    /// Σ `node.status.allocatable` over schedulable nodes.
    pub allocatable: Resources,
    /// Σ over all live pods (ztest and not) of `max(effective_request,
    /// observed_usage)` per CPU/memory, plus each pod's PVC I/O. Reserving the
    /// request keeps admission scheduler-safe; observed usage catches a Burstable
    /// co-tenant running above its request. Reserving the *limit* instead would
    /// sterilize the node — a real non-ztest burst is handled by k8s eviction
    /// ordering, not by pre-reserving a ceiling.
    pub reserved: Resources,
}

impl ClusterCapacity {
    /// Schedulable headroom now: `allocatable - reserved`, floored at zero per
    /// dimension. Nets out every pod (including other concurrent runs'), so a run
    /// seeded from this at startup coexists with existing cluster load.
    pub fn free(&self) -> Resources {
        self.allocatable.saturating_sub(&self.reserved)
    }
}

/// A tier's node placement target, not a capacity partition. Capacity is one
/// global figure; the general/NVMe split is k8s placement (tainted NVMe nodes +
/// a matching toleration/nodeSelector on `sync` pods). Recorded on
/// [`QosProfile::pool`], applied at materialize time; unused by the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pool {
    /// Default placement: any general node.
    General,
    /// Dedicated NVMe nodes (tainted); `sync` only.
    Nvme,
}

impl Pool {
    /// The [`LABEL_POOL`] value for this pool.
    pub fn as_label(self) -> &'static str {
        match self {
            Pool::General => "general",
            Pool::Nvme => "nvme",
        }
    }

    /// Parse a [`LABEL_POOL`] value back into a [`Pool`]; `None` if unknown.
    pub fn from_label(s: &str) -> Option<Pool> {
        match s {
            "general" => Some(Pool::General),
            "nvme" => Some(Pool::Nvme),
            _ => None,
        }
    }
}

/// The four quality-of-service tiers a test may declare.
///
/// `Ord` follows declaration order (`Basic < Wallet < Integration < Testnet <
/// Sync`), which is also ascending priority: a stable `BTreeMap` key for
/// grouping tests by tier during deterministic config lowering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum QosClass {
    /// Sub-minute pure-logic checks. 60 s hard cap.
    Basic,
    /// Wallet-centric tests (a validator + indexer + in-process wallet doing
    /// transactions). 10 min hard cap.
    Wallet,
    /// Multi-step integration tests. 10 min hard cap.
    Integration,
    /// Longer testnet-style scenarios. 6 h hard cap.
    Testnet,
    /// Chain syncs from genesis. 48 h hard cap; NVMe pool.
    Sync,
}

/// The lowered form of a [`QosClass`]: what the harness needs to schedule and
/// size a tier.
///
/// Caps, pool, priority order, and per-tier footprint reserves are all fixed
/// (`docs/qos-design.md` §2/§11). The scheduler doesn't read the footprint
/// directly (callers pass an explicit [`scheduler::Request`]), so the engine
/// stays decoupled from the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QosProfile {
    /// The component-pod aggregate reserve *and ceiling*: divided across the
    /// validator/indexer pods the test spawns — one whole core each
    /// (`per_pod_share`/`even_share`) — as the default `requests == limits` and
    /// the size of the per-test namespace's `ResourceQuota`. Its core count is
    /// the maximum number of component pods the tier admits (each pod takes a
    /// whole core, floored), so `pods × per-pod ≤ footprint` always holds and the
    /// deploy never exceeds what the scheduler reserved. Excludes the runner pod
    /// (separate namespace); the whole reserve is [`admitted`](Self::admitted).
    pub footprint: Resources,
    /// The runner pod's own Guaranteed reserve, distinct from
    /// [`footprint`](Self::footprint). The runner runs the test binary and any
    /// in-process wallet, so `wallet` keeps real compute here while the
    /// orchestration-only tiers keep one core. Summed into
    /// [`admitted`](Self::admitted) so the scheduler accounts it.
    pub runner: Resources,
    /// Which pool the tier schedules on.
    pub pool: Pool,
    /// Scheduling priority; higher is admitted first. `sync`/`testnet` are
    /// high, `basic` low (§5.5, §6).
    pub priority: u8,
    /// The locked execution hard cap (broker exec-cap timer, §5.5).
    pub hard_cap: Duration,
}

impl QosProfile {
    /// The whole-cluster reserve the scheduler admits a test against: the
    /// component-pod aggregate ([`footprint`](Self::footprint)) plus the runner
    /// pod's own reserve ([`runner`](Self::runner)). Both are real pods the test
    /// places (in two different namespaces), so admission reserves their sum or
    /// the runner pod is unbudgeted cluster load.
    pub fn admitted(&self) -> Resources {
        self.footprint.saturating_add(&self.runner)
    }
}

impl QosClass {
    /// The [`LABEL_TIER`] value for this tier (lowercase variant name).
    pub fn as_label(self) -> &'static str {
        match self {
            QosClass::Basic => "basic",
            QosClass::Wallet => "wallet",
            QosClass::Integration => "integration",
            QosClass::Testnet => "testnet",
            QosClass::Sync => "sync",
        }
    }

    /// Parse a [`LABEL_TIER`] value back into a [`QosClass`]; `None` if unknown.
    pub fn from_label(s: &str) -> Option<QosClass> {
        match s {
            "basic" => Some(QosClass::Basic),
            "wallet" => Some(QosClass::Wallet),
            "integration" => Some(QosClass::Integration),
            "testnet" => Some(QosClass::Testnet),
            "sync" => Some(QosClass::Sync),
            _ => None,
        }
    }

    /// The const profile table: one source of truth for every tier's
    /// schedulable shape.
    ///
    /// The I/O reserves are `0` pending calibration: a test's I/O demand isn't
    /// known a priori, so it must come from measured per-tier `io.stat`, never a
    /// guess (which would re-introduce the mispricing this dimension exists to
    /// fix). Left `0`, the I/O dimensions never gate. See
    /// `docs/qos-io-dimension-design.md`.
    pub const fn profile(self) -> QosProfile {
        match self {
            QosClass::Basic => QosProfile {
                footprint: Resources::new(1_000, 512 * MIB, 0, 0),
                runner: Resources::new(1_000, 512 * MIB, 0, 0),
                pool: Pool::General,
                priority: 0,
                hard_cap: Duration::from_secs(60),
            },
            QosClass::Wallet => QosProfile {
                // 4 component cores / 2 GiB: 2 cores + 1 GiB per pod for the
                // validator + indexer topology (even-split across 2 pods). 1 GiB
                // per pod is what a regtest zebrad needs for Orchard proving and
                // the NU6 boundary work; the old 512 MiB even-share OOM-killed it.
                footprint: Resources::new(4_000, 2 * GIB, 0, 0),
                // In-process wallet lives in the runner pod, so it carries real
                // compute, not just orchestration.
                runner: Resources::new(4_000, GIB, 0, 0),
                pool: Pool::General,
                priority: 1,
                hard_cap: Duration::from_secs(10 * 60),
            },
            QosClass::Integration => QosProfile {
                // 3 component cores / 3 GiB: one whole core + 1 GiB per pod for
                // the up-to-3-pod zaino topology (zebrad + zaino-fetch +
                // zaino-state). The core count is the max component pods the tier
                // admits; each pod deploys exactly one core (`per_pod_share`).
                footprint: Resources::new(3_000, 3 * GIB, 0, 0),
                runner: Resources::new(1_000, GIB, 0, 0),
                pool: Pool::General,
                priority: 2,
                hard_cap: Duration::from_secs(10 * 60),
            },
            QosClass::Testnet => QosProfile {
                footprint: Resources::new(8_000, 10 * GIB, 0, 0),
                runner: Resources::new(1_000, GIB, 0, 0),
                pool: Pool::General,
                priority: 3,
                hard_cap: Duration::from_secs(6 * 60 * 60),
            },
            QosClass::Sync => QosProfile {
                footprint: Resources::new(16_000, 16 * GIB, 0, 0),
                runner: Resources::new(1_000, 2 * GIB, 0, 0),
                pool: Pool::Nvme,
                priority: 4,
                hard_cap: Duration::from_secs(48 * 60 * 60),
            },
        }
    }
}

// ── Runtime tier (the in-process bridge) ───────────────────────────────
//
// The `#[ztest::qos::*]` / `#[ztest::sync_test]` attributes inject
// `__enter(class)` as the body's first statement; `build()` reads `current()`
// to size pods. The thread-local is the fast path, but a `flavor =
// "multi_thread"` test (e.g. a sync profile) can migrate off the entering thread
// across an `.await` before `build()` reads the tier — so `__enter` also stamps
// a process-global backstop. That is sound because ztest runs strictly one test
// per process (`process-per-test`, mirroring `naming::current_test_name`): the
// last-entered tier is unambiguous process-wide.

thread_local! {
    static CURRENT: Cell<Option<QosClass>> = const { Cell::new(None) };
}
/// Process-global backstop for the thread-local (see the module note). `u8::MAX`
/// sentinel = "no tier entered" (→ `Basic`).
static CURRENT_GLOBAL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(u8::MAX);

/// Total, stable mapping to/from the global's `u8` (fieldless enum; kept explicit
/// rather than a `repr(u8)` cast so adding a variant is a compile error here).
fn tier_to_u8(class: QosClass) -> u8 {
    match class {
        QosClass::Basic => 0,
        QosClass::Wallet => 1,
        QosClass::Integration => 2,
        QosClass::Testnet => 3,
        QosClass::Sync => 4,
    }
}
fn tier_from_u8(v: u8) -> Option<QosClass> {
    Some(match v {
        0 => QosClass::Basic,
        1 => QosClass::Wallet,
        2 => QosClass::Integration,
        3 => QosClass::Testnet,
        4 => QosClass::Sync,
        _ => return None,
    })
}

/// Set the current test's tier. Called by the `#[ztest::qos::*]` /
/// `#[ztest::sync_test]` attributes as the test body's first statement; not
/// meant to be called directly.
#[doc(hidden)]
pub fn __enter(class: QosClass) {
    CURRENT.with(|c| c.set(Some(class)));
    CURRENT_GLOBAL.store(tier_to_u8(class), std::sync::atomic::Ordering::Relaxed);
}

/// The tier declared by the running test, or [`QosClass::Basic`] if none was
/// declared. Read by `TestEnv::build()`. Prefers the thread-local; falls back to
/// the process-global backstop for off-thread reads.
pub fn current() -> QosClass {
    if let Some(c) = CURRENT.with(|c| c.get()) {
        return c;
    }
    tier_from_u8(CURRENT_GLOBAL.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(QosClass::Basic)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Resources: the 4-D packing primitive (CPU × memory × disk-bandwidth ×
    // disk-IOPS). Every dimension gates and arithmetic is exact per dimension.

    #[test]
    fn display_renders_whole_and_fractional_units() {
        assert_eq!(
            Resources::new(3_000, 3 * GIB, 0, 0).to_string(),
            "3c / 3 GiB"
        );
        assert_eq!(
            Resources::new(500, 512 * MIB, 0, 0).to_string(),
            "500m / 512 MiB"
        );
        assert_eq!(Resources::ZERO.to_string(), "0m / 0 MiB");
    }

    #[test]
    fn fits_within_requires_every_dimension() {
        let cap = Resources::new(1_000, GIB, 100 * MIB, 5_000);
        // All four fit (equal is fine).
        assert!(cap.fits_within(&cap));
        assert!(Resources::new(500, 512 * MIB, 50 * MIB, 2_000).fits_within(&cap));
        // Exceeding any single dimension fails, the other three fitting.
        assert!(!Resources::new(2_000, GIB, 100 * MIB, 5_000).fits_within(&cap));
        assert!(!Resources::new(1_000, 2 * GIB, 100 * MIB, 5_000).fits_within(&cap));
        assert!(
            !Resources::new(1_000, GIB, 200 * MIB, 5_000).fits_within(&cap),
            "disk bandwidth gates independently"
        );
        assert!(
            !Resources::new(1_000, GIB, 100 * MIB, 9_000).fits_within(&cap),
            "disk IOPS gates independently"
        );
    }

    #[test]
    fn checked_arithmetic_is_dimension_wise_and_guards_overflow() {
        let a = Resources::new(1_000, GIB, 100 * MIB, 5_000);
        let b = Resources::new(500, 512 * MIB, 40 * MIB, 2_000);
        assert_eq!(
            a.checked_add(&b),
            Some(Resources::new(1_500, GIB + 512 * MIB, 140 * MIB, 7_000))
        );
        assert_eq!(
            a.checked_sub(&b),
            Some(Resources::new(500, 512 * MIB, 60 * MIB, 3_000))
        );
        // Under-subtraction in any dimension is None.
        assert_eq!(b.checked_sub(&a), None);
        // Overflow in any dimension is None — checked here in the IOPS
        // dimension to prove the guard is not CPU/memory-only.
        assert_eq!(
            Resources::new(0, 0, 0, u64::MAX).checked_add(&Resources::new(0, 0, 0, 1)),
            None
        );
    }

    #[test]
    fn saturating_sub_clamps_at_zero_per_dimension() {
        // Models free = available − committed after a reconcile shrinks
        // available below committed: free floors at zero, never wraps — in
        // every dimension, including the I/O pair.
        let available = Resources::new(1_000, GIB, 50 * MIB, 1_000);
        let committed = Resources::new(4_000, 8 * GIB, 200 * MIB, 9_000);
        assert_eq!(available.saturating_sub(&committed), Resources::ZERO);
    }

    // Profile table: the locked facts (not the TBD reserves).

    #[test]
    fn hard_caps_are_locked() {
        assert_eq!(QosClass::Basic.profile().hard_cap, Duration::from_secs(60));
        assert_eq!(
            QosClass::Integration.profile().hard_cap,
            Duration::from_secs(10 * 60)
        );
        assert_eq!(
            QosClass::Testnet.profile().hard_cap,
            Duration::from_secs(6 * 60 * 60)
        );
        assert_eq!(
            QosClass::Sync.profile().hard_cap,
            Duration::from_secs(48 * 60 * 60)
        );
    }

    #[test]
    fn only_sync_is_off_the_general_pool() {
        assert_eq!(QosClass::Basic.profile().pool, Pool::General);
        assert_eq!(QosClass::Integration.profile().pool, Pool::General);
        assert_eq!(QosClass::Testnet.profile().pool, Pool::General);
        assert_eq!(QosClass::Sync.profile().pool, Pool::Nvme);
    }

    #[test]
    fn wallet_tier_puts_its_compute_in_the_runner() {
        // Components: 4c / 2 GiB, even-split across the validator+indexer topology
        // → 2c / 1 GiB each (1 GiB clears the Orchard/boundary work that OOM-killed
        // zebrad at the old 512 MiB share). Runner carries the in-process wallet's
        // real compute (4c). Admitted total is their sum, 8c / 3 GiB.
        let p = QosClass::Wallet.profile();
        assert_eq!(p.footprint, Resources::new(4_000, 2 * GIB, 0, 0));
        assert_eq!(p.runner, Resources::new(4_000, GIB, 0, 0));
        assert_eq!(p.admitted(), Resources::new(8_000, 3 * GIB, 0, 0));
        assert_eq!(p.pool, Pool::General);
        assert_eq!(p.hard_cap, Duration::from_secs(10 * 60));
    }

    #[test]
    fn priority_order_matches_declaration_order() {
        let (b, w, i, t, s) = (
            QosClass::Basic.profile().priority,
            QosClass::Wallet.profile().priority,
            QosClass::Integration.profile().priority,
            QosClass::Testnet.profile().priority,
            QosClass::Sync.profile().priority,
        );
        // The general-pool ordering that drives "testnet scheduled first".
        assert!(
            b < w && w < i && i < t,
            "basic < wallet < integration < testnet"
        );
        // sync is the top tier overall (owns its own pool, ordering still
        // well-defined).
        assert!(t <= s, "sync is not below testnet");
    }

    const ALL_TIERS: [QosClass; 5] = [
        QosClass::Basic,
        QosClass::Wallet,
        QosClass::Integration,
        QosClass::Testnet,
        QosClass::Sync,
    ];

    #[test]
    fn every_tier_reserves_zero_io_pending_calibration() {
        // The I/O dimensions are structurally present but numerically dormant:
        // a per-tier I/O reserve must come from measured `io.stat`, never a
        // hand-set guess (which would re-introduce the mispricing the dimension
        // exists to fix). This guards that invariant — flip it deliberately when
        // calibration lands (docs/qos-io-dimension-design.md §6).
        for c in ALL_TIERS {
            let fp = c.profile().footprint;
            assert_eq!(fp.io_bps, 0, "{c:?} has a fabricated io_bps reserve");
            assert_eq!(fp.io_iops, 0, "{c:?} has a fabricated io_iops reserve");
        }
    }

    #[test]
    fn guaranteed_cpu_mem_rounds_cpu_up_to_whole_cores_and_keeps_bytes() {
        // 4 whole cores exact; memory verbatim in bytes.
        let (cpu, mem) = Resources::new(4_000, 2 * GIB, 0, 0).guaranteed_cpu_mem("t");
        assert_eq!(cpu, "4");
        assert_eq!(mem, (2 * GIB).to_string());
        // Fractional millicores round UP to a whole core (static-policy pinning).
        let (cpu, _) = Resources::new(2_500, GIB, 0, 0).guaranteed_cpu_mem("t");
        assert_eq!(cpu, "3");
        // Sub-core rounds up to 1.
        let (cpu, _) = Resources::new(500, 512 * MIB, 0, 0).guaranteed_cpu_mem("t");
        assert_eq!(cpu, "1");
    }

    #[test]
    #[should_panic(expected = "zero CPU")]
    fn guaranteed_cpu_mem_panics_on_zero_cpu() {
        let _ = Resources::new(0, GIB, 0, 0).guaranteed_cpu_mem("t");
    }

    #[test]
    #[should_panic(expected = "zero memory")]
    fn guaranteed_cpu_mem_panics_on_zero_memory() {
        let _ = Resources::new(1_000, 0, 0, 0).guaranteed_cpu_mem("t");
    }

    #[test]
    fn every_tier_and_build_pod_footprint_is_pod_schedulable() {
        // No ztest-spawned pod may be sized at a degenerate footprint (would be
        // BestEffort/unschedulable). Guards the runner reserves and the build
        // infra footprints as a set.
        for c in ALL_TIERS {
            c.profile()
                .footprint
                .assert_pod_schedulable("tier footprint");
            c.profile().runner.assert_pod_schedulable("tier runner");
        }
        for fp in [build::BUILDKIT_BUILD, build::UPLOADER] {
            fp.assert_pod_schedulable("build pod footprint");
        }
    }

    #[test]
    fn wallet_runner_keeps_the_in_process_wallet_compute() {
        // The wallet runs in-process in the runner pod, so its runner reserve is
        // ≥4 cores; the orchestration-only tiers keep one core.
        assert!(QosClass::Wallet.profile().runner.cpu_milli >= 4_000);
        assert_eq!(QosClass::Integration.profile().runner.cpu_milli, 1_000);
        assert_eq!(QosClass::Testnet.profile().runner.cpu_milli, 1_000);
        assert_eq!(QosClass::Sync.profile().runner.cpu_milli, 1_000);
    }

    #[test]
    fn qos_class_label_round_trips() {
        for c in ALL_TIERS {
            assert_eq!(QosClass::from_label(c.as_label()), Some(c));
        }
        assert_eq!(QosClass::from_label("nope"), None);
    }

    #[test]
    fn qos_class_serde_round_trips() {
        for c in ALL_TIERS {
            let json = serde_json::to_string(&c).unwrap();
            assert_eq!(serde_json::from_str::<QosClass>(&json).unwrap(), c);
        }
        // Variant names are the wire form.
        assert_eq!(serde_json::to_string(&QosClass::Sync).unwrap(), "\"Sync\"");
    }

    #[test]
    fn enter_sets_current_tier_on_this_thread() {
        // The thread-local fast path: entering a tier makes `current()` return
        // it on the entering thread. (The cross-thread process-global backstop is
        // a documented one-test-per-process invariant; asserting it under the
        // parallel test harness would race other tests' `__enter`, so it is left
        // to the mapping round-trip below plus the module contract.)
        __enter(QosClass::Testnet);
        assert_eq!(current(), QosClass::Testnet);
        __enter(QosClass::Sync);
        assert_eq!(current(), QosClass::Sync);
    }

    #[test]
    fn tier_u8_mapping_round_trips_and_rejects_unknown() {
        for c in ALL_TIERS {
            assert_eq!(tier_from_u8(tier_to_u8(c)), Some(c));
        }
        assert_eq!(tier_from_u8(u8::MAX), None);
    }
}
