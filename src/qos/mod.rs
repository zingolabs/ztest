//! Quality-of-service: cluster resource allocation, job scheduling, and
//! priority.
//!
//! Tests span sub-second logic checks to 48-hour chain syncs. A test author
//! declares a tier at the call site (`basic`, `integration`, `testnet`,
//! `sync`); the harness lowers that into pod requests/limits, nextest
//! backpressure, and a broker that admits topology-booting tests against
//! shared-cluster capacity with priority ordering, backfill, NVMe placement,
//! and per-ServiceAccount budgets. See `docs/qos-design.md`.
//!
//! This module is the broker's pure decision core: given the live capacity,
//! the queue, and the active leases, decide what to admit. It holds no clock,
//! no randomness, and no I/O; the queue's "request_time" tiebreak is a
//! monotonic sequence counter ([`scheduler::Scheduler`]), so every operation
//! is a deterministic function over in-memory state, unit-testable without a
//! cluster. The surrounding shell (UDS wire protocol, async broker loop,
//! exec-cap timers, kube capacity probe, `TestEnv` lease client) wraps this
//! core (see `docs/qos-design.md` §5.1, §5.4, §7).
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

pub mod allocator;
pub mod kube_store;
pub mod ledger;
pub mod live;
pub mod schedule;
pub mod scheduler;
pub mod store;
pub mod units;

/// The four tier attributes: `#[ztest::qos::basic]` .. `#[ztest::qos::sync]`.
/// Surfaced only under `ztest::qos::*`, not the prelude.
pub use ztest_macros::{basic, integration, sync, testnet};

use std::cell::Cell;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ── Shared k8s label / annotation keys ─────────────────────────────────
//
// Reservation/Job objects encode identity in labels (selectable by `list`,
// but DNS-1123-limited) and numeric payload in annotations (no charset/length
// limit). Mirrors the split in `cluster.rs` (`ztest.io/test` slug vs
// `ztest.io/test-full` annotation).

/// Label marking an object's role; value [`ROLE_RESERVATION`].
pub const LABEL_ROLE: &str = "ztest.io/role";
/// `LABEL_ROLE` value for a QoS capacity reservation Lease.
pub const ROLE_RESERVATION: &str = "qos-reservation";
/// `LABEL_ROLE` value for a QoS-accounted `batch/v1` Job (its pod requests
/// count toward committed capacity).
pub const ROLE_JOB: &str = "qos-job";
/// `LABEL_ROLE` value for the singleton allocator-lock Lease.
pub const ROLE_ALLOCATOR_LOCK: &str = "qos-allocator-lock";
/// `LABEL_ROLE` value stamped on a per-test namespace by
/// `cluster::ensure_namespace`. The capacity probe keys on it to separate
/// ztest's own pods from the non-ztest baseline (see
/// [`ClusterCapacity::admission_ceiling`]).
pub const ROLE_TEST_ENV: &str = "test-env";
/// Label carrying the run identity (`GITHUB_RUN_ID` in CI, `${USER}-${PPID}` in
/// dev), stamped on every resource a run owns so its envs group together and the
/// Ctrl-C reaper can find what a crash left behind. See [`crate::naming::RunCoords`].
pub const LABEL_RUN_ID: &str = "ztest.io/run-id";
/// Label carrying the (slugged) invoking user, stamped on every resource a run
/// owns — namespaces, shadow VolumeSnapshotContents, reservation Leases — so
/// `ztest cleanup` can reclaim exactly one developer's resources. Value is
/// `RunCoords::user` slugged; see [`crate::naming::RunCoords`].
pub const LABEL_USER: &str = "ztest.io/user";
/// Label carrying the pool (`general`/`nvme`); see [`Pool::as_label`].
pub const LABEL_POOL: &str = "ztest.io/pool";
/// Label carrying the (slugged) ServiceAccount the reservation is charged to.
pub const LABEL_SA: &str = "ztest.io/sa";
/// Label linking a reservation and its Jobs to one accounting unit (a test's
/// namespace). Capacity is deduplicated per unit (`max` of reservation vs
/// Jobs); see [`ledger`].
pub const LABEL_UNIT: &str = "ztest.io/unit";
/// Label recording the [`QosClass`] tier a reservation was granted for. Carried
/// for the live during-run panel (`qos::live`) so the parent can group active
/// reservations by tier; the scheduler/ledger ignore it (admission keys on the
/// footprint, not the tier).
pub const LABEL_TIER: &str = "qos.ztest.io/tier";

/// Annotation: footprint CPU in millicores (decimal `u64`).
pub const ANN_CPU_MILLI: &str = "qos.ztest.io/cpu-milli";
/// Annotation: footprint memory in bytes (decimal `u64`).
pub const ANN_MEM_BYTES: &str = "qos.ztest.io/mem-bytes";
/// Annotation: the logical tick at which the lease was last renewed.
pub const ANN_RENEW_TICK: &str = "qos.ztest.io/renew-tick";
/// Annotation: the lease duration, in logical ticks.
pub const ANN_LEASE_TICKS: &str = "qos.ztest.io/lease-ticks";
/// Annotation: the holder identity (run-id) of the allocator lock.
pub const ANN_HOLDER: &str = "qos.ztest.io/holder";

/// Annotation on a ServiceAccount: that SA's total CPU budget, as a Kubernetes
/// CPU quantity (`"16"`, `"16000m"`). The broker rejects/queues a run once its
/// SA's concurrent reservations would exceed this (§5.6).
pub const ANN_SA_BUDGET_CPU: &str = "qos.ztest.io/budget-cpu";
/// Annotation on a ServiceAccount: that SA's total memory budget, as a
/// Kubernetes memory quantity (`"32Gi"`). See [`ANN_SA_BUDGET_CPU`].
pub const ANN_SA_BUDGET_MEM: &str = "qos.ztest.io/budget-mem";

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

// ── Runtime admission timing (wall-clock seconds) ──────────────────────
//
// The allocator core is clock-free and counts in abstract `u64` ticks
// ([`allocator::Allocator`]); in production those ticks are unix epoch seconds
// ([`now_secs`]). These constants set the lease lifetimes and the
// admission-loop pacing the live `TestEnv::build()` uses.

/// Allocator-lock Lease TTL. Short: the critical section is a few reads plus
/// one write.
pub const LOCK_TICKS: u64 = 15;
/// Reservation Lease TTL. A live test heartbeats it every [`RENEW_INTERVAL`];
/// a crashed test's reserve is reclaimed once `renew + this + GRACE < now`.
pub const RESERVATION_TICKS: u64 = 90;
/// Reclaim grace margin (skew guard) added to the reservation TTL before a
/// dead reservation is reclaimable.
pub const GRACE: u64 = 30;
/// How often the holding test renews its reservation Lease (about a third of
/// the TTL, so two renewals can be missed before expiry).
pub const RENEW_INTERVAL: Duration = Duration::from_secs(30);
/// Poll interval while a request is [`Queued`](allocator::Outcome::Queued)
/// waiting for capacity to free up.
pub const QUEUE_POLL: Duration = Duration::from_secs(2);
/// Overall wall-clock budget for admission (queue + lock contention). Exceeding
/// it fails the build rather than blocking a test indefinitely; nextest's
/// slow-timeout is the coarser backstop (§5.2).
pub const ADMIT_BUDGET: Duration = Duration::from_secs(900);

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
    /// Σ effective requests of scheduled, live pods outside ztest-managed
    /// namespaces (those not labelled [`LABEL_ROLE`]=[`ROLE_TEST_ENV`], plus
    /// the QoS infra namespaces). The load ztest does not control and cannot
    /// shed by waiting; it sizes the admission ceiling below.
    pub baseline: Resources,
}

impl ClusterCapacity {
    /// Schedulable headroom for the banner: `allocatable - requested`, floored
    /// at zero per dimension. Nets out every pod, so it shrinks as ztest's own
    /// tests run: correct for a live "free right now" display, but not a stable
    /// admission ceiling (see [`Self::admission_ceiling`]).
    pub fn free(&self) -> Resources {
        self.allocatable.saturating_sub(&self.requested)
    }

    /// The capacity available to ztest, independent of ztest's own load:
    /// `allocatable - baseline`. The "empty-of-ztest cluster" figure the
    /// allocator admits against. Reject means a footprint exceeds this
    /// (unschedulable however many ztest tests finish); the queue/fit split is
    /// then taken against this minus the ledger's committed reservations,
    /// counting ztest load exactly once via the ledger, never the live probe.
    pub fn admission_ceiling(&self) -> Resources {
        self.allocatable.saturating_sub(&self.baseline)
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
/// `Ord` follows declaration order (`Basic < Integration < Testnet < Sync`),
/// which is also ascending priority: a stable `BTreeMap` key for grouping tests
/// by tier during deterministic config lowering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
            QosClass::Integration => "integration",
            QosClass::Testnet => "testnet",
            QosClass::Sync => "sync",
        }
    }

    /// Parse a [`LABEL_TIER`] value back into a [`QosClass`]; `None` if unknown.
    pub fn from_label(s: &str) -> Option<QosClass> {
        match s {
            "basic" => Some(QosClass::Basic),
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
            // Caps locked at 60 s / 10 min / 6 h / 48 h; footprints per §11.
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
                footprint: Resources::new(8_000, 18 * GIB),
                pool: Pool::General,
                priority: 2,
                hard_cap: Duration::from_secs(6 * 60 * 60),
            },
            QosClass::Sync => QosProfile {
                footprint: Resources::new(16_000, 32 * GIB),
                pool: Pool::Nvme,
                priority: 3,
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

// ── Admission loop (pure policy) ───────────────────────────────────────
//
// `TestEnv::build()` drives [`allocator::Allocator::try_admit`] in a retry
// loop; this function is the loop's pure decision (when to proceed / retry /
// wait / fail), factored out to be unit-testable without a cluster. The async
// glue in `env.rs` does the I/O and the sleeping.

/// Parse a ServiceAccount's QoS budget from its annotations
/// ([`ANN_SA_BUDGET_CPU`] / [`ANN_SA_BUDGET_MEM`]), reusing the cluster probe's
/// k8s quantity parsers.
///
/// - `Ok(None)`: neither annotation present, so the SA is unlimited.
/// - `Ok(Some(_))`: a present budget. A partial budget (only one dimension set)
///   leaves the missing dimension unlimited (`u64::MAX`).
/// - `Err(_)`: an annotation is present but unparseable. Reported rather than
///   silently treated as `0` (which would reject every request) or ignored, so
///   a typo'd budget fails the run with a clear message.
///
/// Pure; the kube `get` lives in the caller (`TestEnv::build`).
pub fn parse_sa_budget(
    annotations: &std::collections::BTreeMap<String, String>,
) -> Result<Option<Resources>, String> {
    let parse = |key: &str,
                 raw: Option<&String>,
                 f: fn(&str) -> Option<u64>|
     -> Result<Option<u64>, String> {
        match raw {
            None => Ok(None),
            Some(s) => f(s)
                .map(Some)
                .ok_or_else(|| format!("unparseable QoS budget annotation {key}={s:?}")),
        }
    };
    let cpu = parse(
        ANN_SA_BUDGET_CPU,
        annotations.get(ANN_SA_BUDGET_CPU),
        units::parse_cpu_milli_opt,
    )?;
    let mem = parse(
        ANN_SA_BUDGET_MEM,
        annotations.get(ANN_SA_BUDGET_MEM),
        units::parse_mem_bytes_opt,
    )?;
    if cpu.is_none() && mem.is_none() {
        return Ok(None);
    }
    Ok(Some(Resources::new(
        cpu.unwrap_or(u64::MAX),
        mem.unwrap_or(u64::MAX),
    )))
}

/// Wall-clock now as unix epoch seconds: the production value of the
/// allocator's abstract `u64` tick. Saturates at 0 before the epoch.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// What the admission loop should do next given an attempt's [`Outcome`] and
/// how long it has been trying.
///
/// [`Outcome`]: allocator::Outcome
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmitStep {
    /// Admitted; proceed to build the topology.
    Proceed,
    /// Transient (lock contended / stale read); retry immediately.
    RetryNow,
    /// Fits in principle but no room yet; wait [`QUEUE_POLL`] then retry.
    WaitThenRetry,
    /// Unschedulable even on an empty cluster / against the whole SA budget;
    /// fail fast.
    Reject(scheduler::RejectReason),
    /// The admission budget was exhausted while retrying/queuing; give up.
    Timedout,
}

/// Pure admission-loop policy. `elapsed` is the time spent admitting so far;
/// `budget` the overall ceiling ([`ADMIT_BUDGET`]). A non-terminal outcome past
/// the budget becomes [`AdmitStep::Timedout`]; [`Granted`]/[`Rejected`] are
/// terminal regardless of the clock.
///
/// [`Granted`]: allocator::Outcome::Granted
/// [`Rejected`]: allocator::Outcome::Rejected
pub fn classify(outcome: &allocator::Outcome, elapsed: Duration, budget: Duration) -> AdmitStep {
    use allocator::Outcome;
    match outcome {
        Outcome::Granted { .. } => AdmitStep::Proceed,
        Outcome::Rejected(reason) => AdmitStep::Reject(*reason),
        Outcome::Retry if elapsed < budget => AdmitStep::RetryNow,
        Outcome::Queued if elapsed < budget => AdmitStep::WaitThenRetry,
        // Retry / Queued but the budget is spent.
        _ => AdmitStep::Timedout,
    }
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
    fn priority_order_is_basic_low_sync_testnet_high() {
        let (b, i, t, s) = (
            QosClass::Basic.profile().priority,
            QosClass::Integration.profile().priority,
            QosClass::Testnet.profile().priority,
            QosClass::Sync.profile().priority,
        );
        // The general-pool ordering that drives "testnet scheduled first".
        assert!(b < i && i < t, "basic < integration < testnet");
        // sync is the top tier overall (owns its own pool, ordering still
        // well-defined).
        assert!(t <= s, "sync is not below testnet");
    }

    #[test]
    fn qos_class_label_round_trips() {
        for c in [
            QosClass::Basic,
            QosClass::Integration,
            QosClass::Testnet,
            QosClass::Sync,
        ] {
            assert_eq!(QosClass::from_label(c.as_label()), Some(c));
        }
        assert_eq!(QosClass::from_label("nope"), None);
    }

    #[test]
    fn qos_class_serde_round_trips() {
        for c in [
            QosClass::Basic,
            QosClass::Integration,
            QosClass::Testnet,
            QosClass::Sync,
        ] {
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

    // Admission-loop policy (pure).

    use crate::qos::allocator::Outcome;
    use crate::qos::scheduler::RejectReason;

    const BUDGET: Duration = Duration::from_secs(100);

    #[test]
    fn granted_proceeds_regardless_of_clock() {
        let g = Outcome::Granted {
            reservation: "qos-x".into(),
        };
        // Terminal even past the budget.
        assert_eq!(classify(&g, Duration::ZERO, BUDGET), AdmitStep::Proceed);
        assert_eq!(classify(&g, BUDGET * 2, BUDGET), AdmitStep::Proceed);
    }

    #[test]
    fn rejected_fails_fast_regardless_of_clock() {
        let r = Outcome::Rejected(RejectReason::ExceedsClusterCapacity);
        assert_eq!(
            classify(&r, Duration::ZERO, BUDGET),
            AdmitStep::Reject(RejectReason::ExceedsClusterCapacity)
        );
        // Still a reject even past budget (terminal beats Timedout).
        assert_eq!(
            classify(&r, BUDGET * 2, BUDGET),
            AdmitStep::Reject(RejectReason::ExceedsClusterCapacity)
        );
    }

    #[test]
    fn retry_and_queued_respect_the_budget() {
        // Within budget: retry immediately / wait-then-retry.
        assert_eq!(
            classify(&Outcome::Retry, Duration::ZERO, BUDGET),
            AdmitStep::RetryNow
        );
        assert_eq!(
            classify(&Outcome::Queued, Duration::ZERO, BUDGET),
            AdmitStep::WaitThenRetry
        );
        // At/over budget: both give up.
        assert_eq!(
            classify(&Outcome::Retry, BUDGET, BUDGET),
            AdmitStep::Timedout
        );
        assert_eq!(
            classify(&Outcome::Queued, BUDGET, BUDGET),
            AdmitStep::Timedout
        );
    }

    #[test]
    fn parse_sa_budget_reads_both_dims_partial_and_absent() {
        use std::collections::BTreeMap;
        // Both present.
        let full = BTreeMap::from([
            (ANN_SA_BUDGET_CPU.to_string(), "16".to_string()),
            (ANN_SA_BUDGET_MEM.to_string(), "32Gi".to_string()),
        ]);
        assert_eq!(
            parse_sa_budget(&full),
            Ok(Some(Resources::new(16_000, 32 * GIB)))
        );
        // Only CPU: mem unlimited.
        let cpu_only = BTreeMap::from([(ANN_SA_BUDGET_CPU.to_string(), "8000m".to_string())]);
        assert_eq!(
            parse_sa_budget(&cpu_only),
            Ok(Some(Resources::new(8_000, u64::MAX)))
        );
        // Neither: unbudgeted.
        assert_eq!(parse_sa_budget(&BTreeMap::new()), Ok(None));
        // Present but unparseable: a clear error, not a silent 0 (which would
        // reject every request) or a silent skip.
        let garbage = BTreeMap::from([(ANN_SA_BUDGET_CPU.to_string(), "16cores".to_string())]);
        assert!(parse_sa_budget(&garbage).is_err());
    }

    #[test]
    fn now_secs_is_a_plausible_unix_timestamp() {
        // Sanity: after 2020-01-01 and before 2100. Guards against a unit
        // (millis/nanos) regression in the tick mapping.
        let now = now_secs();
        assert!(now > 1_577_836_800, "now_secs() before 2020: {now}");
        assert!(now < 4_102_444_800, "now_secs() after 2100: {now}");
    }
}
