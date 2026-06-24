//! Quality-of-service: the cluster resource-allocation, job-scheduling,
//! and priority system.
//!
//! Tests on this harness span sub-second logic checks to 48-hour chain
//! syncs. A test author declares a *tier* at the call site (`basic`,
//! `integration`, `testnet`, `sync`); the harness lowers that one
//! declaration into pod requests/limits, nextest backpressure, and —
//! the heart of this module — a **broker** that admits topology-booting
//! tests against shared-cluster capacity with priority ordering,
//! backfill, NVMe-pool partitioning, and per-ServiceAccount budgets.
//! See `docs/qos-design.md` for the full design.
//!
//! ## What lives here (and what doesn't)
//!
//! This module is the broker's **pure decision core**: given the live
//! capacity, the queue, and the active leases, decide what to admit. It
//! holds *no clock, no randomness, and no I/O* — the queue's
//! "request_time" tiebreak is a monotonic sequence counter
//! ([`scheduler::Scheduler`]), so every operation is a deterministic
//! function over in-memory state and is unit-testable without a cluster.
//!
//! The surrounding shell — the UDS wire protocol, the async broker
//! loop, the exec-cap timers, the kube capacity probe, and the
//! `TestEnv` lease client — wraps this core but is *not* part of this
//! increment (see `docs/qos-design.md` §5.1, §5.4, §7).
//!
//! ## Model
//!
//! - [`Resources`] — a 2-D (CPU × RAM) amount in k8s-native integer
//!   units (millicpu, bytes). No floats, so arithmetic is exact and
//!   reproducible.
//! - [`Pool`] — `General` vs the dedicated `Nvme` pool that `sync`
//!   targets so it never contends with the other tiers.
//! - [`QosClass`] / [`QosProfile`] — the four tiers and their const
//!   profile table (footprint, pool, priority, hard cap). The
//!   *scheduler* never reads these numbers directly: callers resolve a
//!   class to a [`QosProfile`] and hand the scheduler an explicit
//!   [`scheduler::Request`]. That keeps the engine decoupled from the
//!   still-TBD reserve table (`docs/qos-design.md` §11).

pub mod allocator;
pub mod kube_store;
pub mod ledger;
pub mod scheduler;
pub mod store;
pub mod units;

use std::time::Duration;

// ── Shared k8s label / annotation keys ─────────────────────────────────
//
// Reservation/Job objects encode their identity in *labels* (selectable by
// `list`, but DNS-1123-limited) and their numeric payload in *annotations*
// (no charset/length limit). This mirrors the label-vs-annotation split in
// `cluster.rs` (`zaino.io/test` slug vs `zaino.io/test-full` annotation).

/// Label marking an object's role; value [`ROLE_RESERVATION`].
pub const LABEL_ROLE: &str = "zaino.io/role";
/// `LABEL_ROLE` value for a QoS capacity reservation Lease.
pub const ROLE_RESERVATION: &str = "qos-reservation";
/// `LABEL_ROLE` value for a QoS-accounted `batch/v1` Job (its pod requests
/// count toward committed capacity).
pub const ROLE_JOB: &str = "qos-job";
/// `LABEL_ROLE` value for the singleton allocator-lock Lease.
pub const ROLE_ALLOCATOR_LOCK: &str = "qos-allocator-lock";
/// Label carrying the pool (`general`/`nvme`); see [`Pool::as_label`].
pub const LABEL_POOL: &str = "zaino.io/pool";
/// Label carrying the (slugged) ServiceAccount the reservation is charged to.
pub const LABEL_SA: &str = "zaino.io/sa";
/// Label linking a reservation and its Jobs to one accounting unit (a test's
/// namespace). Capacity is deduplicated per unit (`max` of reservation vs
/// Jobs) — see [`ledger`].
pub const LABEL_UNIT: &str = "zaino.io/unit";

/// Annotation: footprint CPU in millicores (decimal `u64`).
pub const ANN_CPU_MILLI: &str = "qos.zaino.io/cpu-milli";
/// Annotation: footprint memory in bytes (decimal `u64`).
pub const ANN_MEM_BYTES: &str = "qos.zaino.io/mem-bytes";
/// Annotation: the logical tick at which the lease was last renewed.
pub const ANN_RENEW_TICK: &str = "qos.zaino.io/renew-tick";
/// Annotation: the lease duration, in logical ticks.
pub const ANN_LEASE_TICKS: &str = "qos.zaino.io/lease-ticks";
/// Annotation: the holder identity (run-id) of the allocator lock.
pub const ANN_HOLDER: &str = "qos.zaino.io/holder";

/// One mebibyte, in bytes — for spelling memory amounts legibly.
pub const MIB: u64 = 1024 * 1024;
/// One gibibyte, in bytes.
pub const GIB: u64 = 1024 * MIB;

/// A two-dimensional resource amount: CPU in millicores and memory in
/// bytes, matching the units Kubernetes uses for `requests`/`limits`.
///
/// Integer-only by design — the broker's packing decisions must be
/// exact and reproducible, and k8s quantities are themselves integer
/// (`500m`, `512Mi`). Both dimensions are independent; "fits" means
/// fits in *both* (see [`Resources::fits_within`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Resources {
    /// CPU in millicores (`1000` == one core).
    pub cpu_milli: u64,
    /// Memory in bytes.
    pub mem_bytes: u64,
}

impl Resources {
    /// The empty amount — both dimensions zero.
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

    /// `true` iff `self` fits within `cap` in *both* dimensions. This is
    /// the 2-D packing primitive: a request is grantable only when its
    /// CPU **and** its memory both fit the available capacity.
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

    /// Dimension-wise difference, clamped at zero per dimension. Used
    /// for `free = available − committed`, which must stay well-defined
    /// even if a `reconcile` shrinks `available` below what is already
    /// committed (we do not preempt running leases — see
    /// [`scheduler::Scheduler::reconcile`]).
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
/// requests of scheduled workloads. There is **one global pool** — NVMe vs
/// general is a Kubernetes placement concern (node taints + pod tolerations),
/// not a capacity partition (see `docs/qos-design.md`). Produced by the
/// cluster probe (`pipeline::cluster`), shown by the preflight banner, and
/// (once wired) the input to admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClusterCapacity {
    /// Σ `node.status.allocatable` over schedulable nodes.
    pub allocatable: Resources,
    /// Σ effective requests of scheduled, live pods.
    pub requested: Resources,
}

impl ClusterCapacity {
    /// Schedulable headroom: `allocatable − requested`, floored at zero per
    /// dimension.
    pub fn free(&self) -> Resources {
        self.allocatable.saturating_sub(&self.requested)
    }
}

/// A tier's node **placement** target — *not* a capacity partition.
///
/// Capacity is one global figure ([`ClusterCapacity`]); the broker admits
/// against it and never accounts per-pool. The general/NVMe split is realised
/// purely as Kubernetes placement: NVMe nodes are tainted, and a `sync` pod
/// carries the matching toleration (+ nodeSelector) so it lands on an NVMe
/// node while other tiers don't — k8s keeps them from contending. This enum
/// records which placement a tier wants ([`QosProfile::pool`]); it is applied
/// to pod specs at materialize time, and is unused by `scheduler`/`ledger`/
/// `allocator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pool {
    /// Default placement — any general node.
    General,
    /// Dedicated NVMe nodes (tainted) — `sync` only.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QosClass {
    /// Sub-minute pure-logic checks. 60 s hard cap.
    Basic,
    /// Multi-step integration tests. 10 min hard cap.
    Integration,
    /// Longer testnet-style scenarios. 6 h hard cap.
    Testnet,
    /// Chain syncs from genesis. 48 h hard cap; NVMe pool.
    Sync,
}

/// The lowered form of a [`QosClass`]: what the harness needs to
/// schedule and size a tier.
///
/// **Caps, pool, and priority order are locked** (`docs/qos-design.md`
/// §2). The per-tier `footprint` reserves are still TBD (§11) — the
/// values in [`QosClass::profile`] are placeholders, and the scheduler
/// deliberately never reads them (callers pass an explicit
/// [`scheduler::Request`]), so the engine is correct regardless of the
/// final numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QosProfile {
    /// Per-test namespace aggregate reserve — the amount the broker
    /// schedules against (§2). **Placeholder pending §11.**
    pub footprint: Resources,
    /// Which pool the tier schedules on.
    pub pool: Pool,
    /// Scheduling priority; higher is admitted first. `sync`/`testnet`
    /// are high, `basic` low (§5.5, §6).
    pub priority: u8,
    /// The locked execution hard cap (broker exec-cap timer, §5.5).
    pub hard_cap: Duration,
}

impl QosClass {
    /// The const profile table. One source of truth for every tier's
    /// schedulable shape.
    pub const fn profile(self) -> QosProfile {
        match self {
            // Caps locked at 60 s / 10 min / 6 h / 48 h. Footprints are
            // placeholders pending the definitive reserve table (§11);
            // the scheduler never reads them.
            QosClass::Basic => QosProfile {
                footprint: Resources::new(500, 512 * MIB),
                pool: Pool::General,
                priority: 0,
                hard_cap: Duration::from_secs(60),
            },
            QosClass::Integration => QosProfile {
                footprint: Resources::new(2_000, 2 * GIB),
                pool: Pool::General,
                priority: 1,
                hard_cap: Duration::from_secs(10 * 60),
            },
            QosClass::Testnet => QosProfile {
                footprint: Resources::new(4_000, 8 * GIB),
                pool: Pool::General,
                priority: 2,
                hard_cap: Duration::from_secs(6 * 60 * 60),
            },
            QosClass::Sync => QosProfile {
                footprint: Resources::new(8_000, 16 * GIB),
                pool: Pool::Nvme,
                priority: 3,
                hard_cap: Duration::from_secs(48 * 60 * 60),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Resources: the 2-D packing primitive ──────────────────────────

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
        assert_eq!(a.checked_add(&b), Some(Resources::new(1500, GIB + 512 * MIB)));
        assert_eq!(a.checked_sub(&b), Some(Resources::new(500, 512 * MIB)));
        // Under-subtraction in either dimension is None.
        assert_eq!(b.checked_sub(&a), None);
        // Overflow in either dimension is None.
        assert_eq!(Resources::new(u64::MAX, 0).checked_add(&Resources::new(1, 0)), None);
    }

    #[test]
    fn saturating_sub_clamps_at_zero_per_dimension() {
        // Models free = available − committed after a reconcile shrinks
        // available below committed: free floors at zero, never wraps.
        let available = Resources::new(1000, GIB);
        let committed = Resources::new(4000, 8 * GIB);
        assert_eq!(available.saturating_sub(&committed), Resources::ZERO);
    }

    // ── Profile table: the LOCKED facts (not the TBD reserves) ─────────

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
    fn priority_order_is_basic_low_sync_testnet_high() {
        let (b, i, t, s) = (
            QosClass::Basic.profile().priority,
            QosClass::Integration.profile().priority,
            QosClass::Testnet.profile().priority,
            QosClass::Sync.profile().priority,
        );
        // The general-pool ordering that drives "testnet scheduled first":
        assert!(b < i && i < t, "basic < integration < testnet");
        // sync is the top tier overall (it owns its own pool, but the
        // ordering is still well-defined).
        assert!(t <= s, "sync is not below testnet");
    }
}
