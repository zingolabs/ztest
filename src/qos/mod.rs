//! Quality-of-service: cluster resource allocation, job scheduling, and
//! priority.
//!
//! Tests span sub-second logic checks to 48-hour chain syncs. A test author
//! declares a tier at the call site (`basic`, `integration`, `testnet`,
//! `sync`); the harness lowers that into pod requests/limits and a scheduling
//! footprint. `ztest run`'s in-memory [`scheduler::Scheduler`] is the sole
//! admission authority: it admits tests against probed cluster capacity with
//! priority ordering, backfill, and NVMe placement. See `docs/qos-design.md`.
//!
//! This module is the scheduler's pure decision core: given the live capacity,
//! the queue, and the active leases, decide what to admit. It holds no clock,
//! no randomness, and no I/O; the queue's "request_time" tiebreak is a
//! monotonic sequence counter ([`scheduler::Scheduler`]), so every operation
//! is a deterministic function over in-memory state, unit-testable without a
//! cluster.
//!
//! Model:
//! - [`Resources`]: a 2-D (CPU × RAM) amount in k8s-native integer units
//!   (millicpu, bytes). Integer-only, so arithmetic is exact.
//! - [`Pool`]: `General` vs the dedicated `Nvme` pool that `sync` targets so
//!   it never contends with the other tiers.
//! - [`QosClass`] / [`QosProfile`]: the four tiers and their const profile
//!   table (footprint, pool, priority, hard cap). The scheduler never reads
//!   these numbers directly: callers resolve a class to a [`QosProfile`] and
//!   hand the scheduler an explicit [`scheduler::Request`], keeping the engine
//!   decoupled from the still-TBD reserve table (`docs/qos-design.md` §11).

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

// ── NVMe placement (node taint + nodeSelector) ─────────────────────────
//
// `sync` pods must land on the dedicated NVMe nodes; other tiers must not.
// Realised as Kubernetes placement (a tainted node pool + a matching pod
// toleration and nodeSelector), not a capacity partition (see [`Pool`]).
// Applied to `sync` pod specs at materialize time (`manifest::PodSpec::render`).
// The exact label/taint key the NVMe nodes carry is TBD pending cluster-admin
// confirmation (§11); isolated here so the production value is a one-line swap.

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

/// A two-dimensional resource amount: CPU in millicores and memory in bytes,
/// matching the units Kubernetes uses for `requests`/`limits`.
///
/// Integer-only, so packing decisions are exact (k8s quantities are themselves
/// integer: `500m`, `512Mi`). Both dimensions are independent; "fits" means
/// fits in both (see [`Resources::fits_within`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Resources {
    /// CPU in millicores (`1000` == one core).
    pub cpu_milli: u64,
    /// Memory in bytes.
    pub mem_bytes: u64,
}

impl Resources {
    /// The empty amount: both dimensions zero.
    pub const ZERO: Resources = Resources {
        cpu_milli: 0,
        mem_bytes: 0,
    };

    /// Construct an amount from millicpu and bytes.
    pub const fn new(cpu_milli: u64, mem_bytes: u64) -> Self {
        Resources {
            cpu_milli,
            mem_bytes,
        }
    }

    /// `true` iff `self` fits within `cap` in both dimensions: a request is
    /// grantable only when its CPU and its memory both fit.
    pub fn fits_within(&self, cap: &Resources) -> bool {
        self.cpu_milli <= cap.cpu_milli && self.mem_bytes <= cap.mem_bytes
    }

    /// Dimension-wise sum, `None` on overflow of either dimension.
    pub fn checked_add(&self, other: &Resources) -> Option<Resources> {
        Some(Resources {
            cpu_milli: self.cpu_milli.checked_add(other.cpu_milli)?,
            mem_bytes: self.mem_bytes.checked_add(other.mem_bytes)?,
        })
    }

    /// Dimension-wise difference, `None` if either dimension would go
    /// negative.
    pub fn checked_sub(&self, other: &Resources) -> Option<Resources> {
        Some(Resources {
            cpu_milli: self.cpu_milli.checked_sub(other.cpu_milli)?,
            mem_bytes: self.mem_bytes.checked_sub(other.mem_bytes)?,
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
        }
    }

    /// Dimension-wise sum, saturating at `u64::MAX` per dimension.
    pub fn saturating_add(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.saturating_add(other.cpu_milli),
            mem_bytes: self.mem_bytes.saturating_add(other.mem_bytes),
        }
    }

    /// Dimension-wise maximum. Used by the ledger's per-unit dedup rule:
    /// a test that holds both a reservation and Jobs counts as
    /// `max(reservation_footprint, Σ job_requests)`, never their sum.
    pub fn max(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.max(other.cpu_milli),
            mem_bytes: self.mem_bytes.max(other.mem_bytes),
        }
    }
}

/// Whole-cluster schedulable capacity: total node `allocatable` minus the
/// requests of scheduled workloads. One global pool: NVMe vs general is a
/// Kubernetes placement concern (node taints + pod tolerations), not a capacity
/// partition (see `docs/qos-design.md`). Produced by the cluster probe
/// (`pipeline::cluster`), shown by the preflight banner, and (once wired) the
/// input to admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClusterCapacity {
    /// Σ `node.status.allocatable` over schedulable nodes.
    pub allocatable: Resources,
    /// Σ effective requests of all scheduled, live pods (ztest and non-ztest
    /// alike). Drives the preflight banner's free-headroom display.
    pub requested: Resources,
}

impl ClusterCapacity {
    /// Schedulable headroom right now: `allocatable - requested`, floored at
    /// zero per dimension. Nets out every pod — including other concurrent
    /// `ztest run`s' Guaranteed pods — so a run seeded from this at startup
    /// coexists with the load already on the cluster. Also the preflight
    /// banner's "free" figure.
    pub fn free(&self) -> Resources {
        self.allocatable.saturating_sub(&self.requested)
    }
}

/// A tier's node placement target, not a capacity partition.
///
/// Capacity is one global figure ([`ClusterCapacity`]); the broker admits
/// against it and never accounts per-pool. The general/NVMe split is Kubernetes
/// placement: NVMe nodes are tainted, and a `sync` pod carries the matching
/// toleration (+ nodeSelector) so it lands on an NVMe node while other tiers
/// don't. This enum records which placement a tier wants ([`QosProfile::pool`]),
/// applied to pod specs at materialize time; unused by
/// `scheduler`/`ledger`/`allocator`.
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
    /// Per-test namespace aggregate reserve: the amount the broker schedules
    /// against, and (split across the env's pods) the default pod
    /// requests/limits when a test doesn't call `.resources()` (§7).
    pub footprint: Resources,
    /// Which pool the tier schedules on.
    pub pool: Pool,
    /// Scheduling priority; higher is admitted first. `sync`/`testnet` are
    /// high, `basic` low (§5.5, §6).
    pub priority: u8,
    /// The locked execution hard cap (broker exec-cap timer, §5.5).
    pub hard_cap: Duration,
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

    /// The const profile table. One source of truth for every tier's
    /// schedulable shape.
    pub const fn profile(self) -> QosProfile {
        match self {
            // Caps locked at 60 s / 10 min / 10 min / 6 h / 48 h; footprints per §11.
            QosClass::Basic => QosProfile {
                footprint: Resources::new(1_000, 512 * MIB),
                pool: Pool::General,
                priority: 0,
                hard_cap: Duration::from_secs(60),
            },
            QosClass::Wallet => QosProfile {
                footprint: Resources::new(4_000, GIB),
                pool: Pool::General,
                priority: 1,
                hard_cap: Duration::from_secs(10 * 60),
            },
            QosClass::Integration => QosProfile {
                footprint: Resources::new(4_000, 2 * GIB),
                pool: Pool::General,
                priority: 2,
                hard_cap: Duration::from_secs(10 * 60),
            },
            QosClass::Testnet => QosProfile {
                footprint: Resources::new(8_000, 12 * GIB),
                pool: Pool::General,
                priority: 3,
                hard_cap: Duration::from_secs(6 * 60 * 60),
            },
            QosClass::Sync => QosProfile {
                footprint: Resources::new(16_000, 32 * GIB),
                pool: Pool::Nvme,
                priority: 4,
                hard_cap: Duration::from_secs(48 * 60 * 60),
            },
        }
    }
}

// ── Runtime tier (the in-process bridge) ───────────────────────────────
//
// The `#[ztest::qos::*]` attribute injects `__enter(class)` as a test's first
// statement; `TestEnv::build()` reads `current()` to size pods and request a
// reservation. A thread-local (not a tokio task-local): nextest runs
// process-per-test, so there is no cross-test leakage within a process, and
// `build()` reads the tier at its start, before any `.await` could migrate the
// test future to another worker thread. Mirrors the thread-name reliance in
// `naming::current_test_name`.

thread_local! {
    static CURRENT: Cell<QosClass> = const { Cell::new(QosClass::Basic) };
}

/// Set the current test's tier. Called by the `#[ztest::qos::*]` attribute as
/// the test body's first statement; not meant to be called directly.
#[doc(hidden)]
pub fn __enter(class: QosClass) {
    CURRENT.with(|c| c.set(class));
}

/// The tier declared by the running test, or [`QosClass::Basic`] if none was
/// declared. Read by `TestEnv::build()`.
pub fn current() -> QosClass {
    CURRENT.with(|c| c.get())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Resources: the 2-D packing primitive.

    #[test]
    fn fits_within_requires_both_dimensions() {
        let cap = Resources::new(1000, GIB);
        // Both fit.
        assert!(Resources::new(1000, GIB).fits_within(&cap));
        assert!(Resources::new(500, 512 * MIB).fits_within(&cap));
        // CPU fits, memory doesn't.
        assert!(!Resources::new(500, 2 * GIB).fits_within(&cap));
        // Memory fits, CPU doesn't.
        assert!(!Resources::new(2000, 512 * MIB).fits_within(&cap));
    }

    #[test]
    fn checked_arithmetic_is_dimension_wise_and_guards_overflow() {
        let a = Resources::new(1000, GIB);
        let b = Resources::new(500, 512 * MIB);
        assert_eq!(
            a.checked_add(&b),
            Some(Resources::new(1500, GIB + 512 * MIB))
        );
        assert_eq!(a.checked_sub(&b), Some(Resources::new(500, 512 * MIB)));
        // Under-subtraction in either dimension is None.
        assert_eq!(b.checked_sub(&a), None);
        // Overflow in either dimension is None.
        assert_eq!(
            Resources::new(u64::MAX, 0).checked_add(&Resources::new(1, 0)),
            None
        );
    }

    #[test]
    fn saturating_sub_clamps_at_zero_per_dimension() {
        // Models free = available − committed after a reconcile shrinks
        // available below committed: free floors at zero, never wraps.
        let available = Resources::new(1000, GIB);
        let committed = Resources::new(4000, 8 * GIB);
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
    fn wallet_tier_reserves_four_cores_one_gib() {
        let p = QosClass::Wallet.profile();
        assert_eq!(p.footprint, Resources::new(4_000, GIB));
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
    fn current_defaults_to_basic_and_enter_sets_it() {
        // Default before any `__enter` on this thread.
        assert_eq!(current(), QosClass::Basic);
        __enter(QosClass::Testnet);
        assert_eq!(current(), QosClass::Testnet);
        __enter(QosClass::Sync);
        assert_eq!(current(), QosClass::Sync);
    }
}
