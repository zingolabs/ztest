//! Cluster resource allocation, scheduling, priority (`docs/design-qos.md`).
//!
//! - Test declares a tier → harness lowers it to pod requests/limits + a footprint →
//!   [`scheduler::Scheduler`] admits it against probed capacity
//! - Core holds no clock/randomness/I/O → admission is deterministic, testable clusterless
//! - [`Resources`] = 4-D CPU × RAM × disk-bandwidth × disk-IOPS, integer k8s/cgroup units
//! - [`Pool`] = general vs the dedicated NVMe pool `sync` targets
//! - Callers resolve [`QosClass`] → [`QosProfile`] → an explicit [`scheduler::Request`],
//!   keeping the scheduler decoupled from the const profile table

pub mod beacon;
pub mod ledger;
pub mod live;
pub mod schedule;
pub mod scheduler;
pub mod units;

/// Tier attributes `#[ztest::qos::basic]` .. `#[ztest::qos::sync]`, surfaced only under
/// `ztest::qos::*`, never the prelude
pub use ztest_macros::{basic, integration, sync, testnet, wallet};

use std::cell::Cell;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ── Shared k8s label / annotation keys ─────────────────────────────────

pub const LABEL_ROLE: &str = "ztest.io/role";
/// `LABEL_ROLE` value on a per-test namespace; `ztest cleanup --all-users` reaps by it
pub const ROLE_TEST_ENV: &str = "test-env";
/// Value = `RunCoords::run_id` (`GITHUB_RUN_ID` in CI, `${USER}-${PPID}` in dev); the
/// Ctrl-C reaper selects on it to find what a crash left behind
pub const LABEL_RUN_ID: &str = "ztest.io/run-id";
/// Value = `RunCoords::user`, slugged; `ztest cleanup` reclaims one developer's resources
pub const LABEL_USER: &str = "ztest.io/user";
/// Ties a cluster-scoped seed-binding VolumeSnapshotContent to the namespace it serves;
/// cluster-scoped → not cascaded by the namespace delete, so teardown deletes by selector
pub const LABEL_TEST_NS: &str = "ztest.io/test-ns";

// ── Disk-I/O reservation (declared on the PVC / storage request) ────────
//
// No disk-I/O field in pod `resources`, so the per-volume cap rides these PVC annotations,
// enforced via cgroup `io.max` (CRI-O blockio). Migrates to a `VolumeAttributesClass` once
// a backend's CSI can honor one. See `docs/design-qos.md`.

/// k8s quantity in bytes/sec; mirrors cgroup `io.max` `{r,w}bps`
pub const ANNOTATION_DISK_BPS: &str = "qos.ztest.io/disk-bps";
/// Plain integer in ops/sec; mirrors cgroup `io.max` `{r,w}iops`
pub const ANNOTATION_DISK_IOPS: &str = "qos.ztest.io/disk-iops";
/// k8s quantity in bytes; overrides the PVC's own `requests.storage` as the *reserve*
/// (provisioned size != what a CoW clone or a growing index actually charges the pool)
pub const ANNOTATION_DISK_BYTES: &str = "qos.ztest.io/disk-bytes";

// ── NVMe placement (node taint + nodeSelector) ─────────────────────────
//
// `sync` pods land on the NVMe nodes via k8s placement (tainted pool + matching
// toleration/nodeSelector), not a capacity partition (see [`Pool`]). Key TBD (§11),
// isolated here for a one-line production swap.

/// §11 TBD.
pub const NVME_NODE_LABEL_KEY: &str = "ztest.io/pool";
/// §11 TBD.
pub const NVME_NODE_LABEL_VALUE: &str = "nvme";
/// §11 TBD.
pub const NVME_TAINT_KEY: &str = "ztest.io/pool";

pub const MIB: u64 = 1024 * 1024;
pub const GIB: u64 = 1024 * MIB;

/// 5-D resource amount: k8s `requests`/`limits` units, the cgroup v2 `io.max` pair, and
/// storage capacity.
///
/// - Integer-only → packing is exact; dimensions independent ("fits" = fits every one)
/// - I/O + disk inert until calibrated (k8s surfaces neither as node `allocatable`, so an
///   uncalibrated ceiling is seeded [`u64::MAX`] and admission matches the CPU×memory model)
/// - Disk = PVC pool capacity, not node `ephemeral-storage` (different pools, never summed)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Resources {
    pub cpu_milli: u64,
    pub mem_bytes: u64,
    /// Inert dimensions omitted from the wire (`0` = the uncalibrated norm, on every
    /// beacon ever written) rather than doubling every serialized footprint
    #[serde(default, skip_serializing_if = "is_zero")]
    pub disk_bps: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub disk_iops: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub disk_bytes: u64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

impl Resources {
    pub const ZERO: Resources =
        Resources { cpu_milli: 0, mem_bytes: 0, disk_bps: 0, disk_iops: 0, disk_bytes: 0 };

    /// Disk omitted: 157 call sites predate it, and a 5th positional `u64` reads as noise
    /// → [`Resources::with_disk`] is the one way to set it
    pub const fn new(cpu_milli: u64, mem_bytes: u64, disk_bps: u64, disk_iops: u64) -> Self {
        Resources { cpu_milli, mem_bytes, disk_bps, disk_iops, disk_bytes: 0 }
    }

    pub const fn with_disk(self, disk_bytes: u64) -> Self {
        Resources { disk_bytes, ..self }
    }

    /// I/O + disk [`u64::MAX`], for a *ceiling* on an un-benchmarked node (those dimensions
    /// then never gate admission)
    pub const fn cpu_mem_unbounded_rest(cpu_milli: u64, mem_bytes: u64) -> Self {
        Resources::new(cpu_milli, mem_bytes, u64::MAX, u64::MAX).with_disk(u64::MAX)
    }

    pub fn fits_within(&self, cap: &Resources) -> bool {
        self.cpu_milli <= cap.cpu_milli
            && self.mem_bytes <= cap.mem_bytes
            && self.disk_bps <= cap.disk_bps
            && self.disk_iops <= cap.disk_iops
            && self.disk_bytes <= cap.disk_bytes
    }

    pub fn checked_add(&self, other: &Resources) -> Option<Resources> {
        Some(Resources {
            cpu_milli: self.cpu_milli.checked_add(other.cpu_milli)?,
            mem_bytes: self.mem_bytes.checked_add(other.mem_bytes)?,
            disk_bps: self.disk_bps.checked_add(other.disk_bps)?,
            disk_iops: self.disk_iops.checked_add(other.disk_iops)?,
            disk_bytes: self.disk_bytes.checked_add(other.disk_bytes)?,
        })
    }

    pub fn checked_sub(&self, other: &Resources) -> Option<Resources> {
        Some(Resources {
            cpu_milli: self.cpu_milli.checked_sub(other.cpu_milli)?,
            mem_bytes: self.mem_bytes.checked_sub(other.mem_bytes)?,
            disk_bps: self.disk_bps.checked_sub(other.disk_bps)?,
            disk_iops: self.disk_iops.checked_sub(other.disk_iops)?,
            disk_bytes: self.disk_bytes.checked_sub(other.disk_bytes)?,
        })
    }

    /// Keeps `free = available - committed` defined when a `reconcile` shrinks `available`
    /// below what is committed (running leases aren't preempted;
    /// see [`scheduler::Scheduler::reconcile`])
    pub fn saturating_sub(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.saturating_sub(other.cpu_milli),
            mem_bytes: self.mem_bytes.saturating_sub(other.mem_bytes),
            disk_bps: self.disk_bps.saturating_sub(other.disk_bps),
            disk_iops: self.disk_iops.saturating_sub(other.disk_iops),
            disk_bytes: self.disk_bytes.saturating_sub(other.disk_bytes),
        }
    }

    pub fn saturating_add(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.saturating_add(other.cpu_milli),
            mem_bytes: self.mem_bytes.saturating_add(other.mem_bytes),
            disk_bps: self.disk_bps.saturating_add(other.disk_bps),
            disk_iops: self.disk_iops.saturating_add(other.disk_iops),
            disk_bytes: self.disk_bytes.saturating_add(other.disk_bytes),
        }
    }

    /// Backs the ledger's dedup rule: a test holding both a reservation and Jobs counts as
    /// `max(reservation_footprint, Σ job_requests)`, never their sum
    pub fn max(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.max(other.cpu_milli),
            mem_bytes: self.mem_bytes.max(other.mem_bytes),
            disk_bps: self.disk_bps.max(other.disk_bps),
            disk_iops: self.disk_iops.max(other.disk_iops),
            disk_bytes: self.disk_bytes.max(other.disk_bytes),
        }
    }

    pub fn min(&self, other: &Resources) -> Resources {
        Resources {
            cpu_milli: self.cpu_milli.min(other.cpu_milli),
            mem_bytes: self.mem_bytes.min(other.mem_bytes),
            disk_bps: self.disk_bps.min(other.disk_bps),
            disk_iops: self.disk_iops.min(other.disk_iops),
            disk_bytes: self.disk_bytes.min(other.disk_bytes),
        }
    }

    /// Guard for any footprint about to size a real pod: zero CPU or memory renders a
    /// BestEffort container (the class this harness forbids), so panic rather than emit it
    pub fn assert_pod_schedulable(&self, ctx: &str) {
        assert!(self.cpu_milli > 0, "{ctx}: pod footprint has zero CPU — would not be Guaranteed",);
        assert!(
            self.mem_bytes > 0,
            "{ctx}: pod footprint has zero memory — would not be Guaranteed",
        );
    }

    /// `footprint = "15c/29Gi[/400Gi]"` → reserve. Undeclared disk dimension → `0`, the
    /// "nothing charges it" value every uncalibrated dimension already carries
    pub fn from_footprint(f: ztest_attr::Footprint) -> Resources {
        Resources::new(f.cpu_milli, f.mem_bytes, f.disk_bps.unwrap_or(0), f.disk_iops.unwrap_or(0))
            .with_disk(f.disk_bytes.unwrap_or(0))
    }

    /// `(cpu, memory)` k8s quantity strings for a Guaranteed container: whole-core CPU
    /// (rounded up, min 1) + exact-byte memory.
    ///
    /// - Sole lowering every ztest pod renders through → no site drifts fractional/Burstable
    /// - Whole cores: the kubelet `static` CPU-manager policy pins exclusive CPUs only to an
    ///   integer-core Guaranteed pod
    pub fn guaranteed_cpu_mem(&self, ctx: &str) -> (String, String) {
        self.assert_pod_schedulable(ctx);
        let cores = self.cpu_milli.div_ceil(1000).max(1);
        (cores.to_string(), self.mem_bytes.to_string())
    }
}

/// Display vocabulary. Lives on the type, not in `ui` — a formatter placed in a consumer
/// gets re-invented once per consumer
impl Resources {
    pub fn cores(&self) -> f64 {
        self.cpu_milli as f64 / 1000.0
    }

    pub fn gib(&self) -> f64 {
        self.mem_bytes as f64 / GIB as f64
    }

    /// Dense cell `16c/30Gi`, integer — a per-row footprint column that repeats a decimal
    /// buys nothing. Sub-gibibyte falls to `Mi`; sub-core keeps a decimal (`0.5c/512Mi`)
    ///
    /// - Undeclared disk drops its segment → output re-parses as `footprint = ".."`
    pub fn compact(&self) -> String {
        let cpu = match self.cpu_milli {
            m if m == 0 || m.is_multiple_of(1000) => format!("{}c", m / 1000),
            m => format!("{:.1}c", m as f64 / 1000.0),
        };
        let bytes = |b: u64| match b {
            b if b >= GIB => format!("{}Gi", b / GIB),
            b => format!("{}Mi", b / MIB),
        };
        match self.disk_bytes {
            0 => format!("{cpu}/{}", bytes(self.mem_bytes)),
            d => format!("{cpu}/{}/{}", bytes(self.mem_bytes), bytes(d)),
        }
    }

    /// `(cpu, mem)` fill against `whole`, each saturating at 100; a zero dimension in
    /// `whole` yields 0 there rather than dividing. Callers pick `min` (binding constraint)
    /// or `max` (fullness) — the two are different questions and neither is the default
    ///
    /// - Disk excluded while uncalibrated (ceiling seeds [`u64::MAX`] → fill rounds to 0)
    pub fn ratio_pct(&self, whole: &Resources) -> (u8, u8) {
        let frac = |p: u64, w: u64| match w {
            0 => 0,
            w => (p as u128 * 100 / w as u128).min(100) as u8,
        };
        (frac(self.cpu_milli, whole.cpu_milli), frac(self.mem_bytes, whole.mem_bytes))
    }
}

/// `3c / 3 GiB`, `500m / 512 MiB`. I/O + disk omitted (`0` pending calibration, no signal
/// here); [`Resources::compact`] carries disk where a footprint cell needs it
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

/// Footprints for the ephemeral on-cluster build pods, in the [`Resources`] model so they
/// share its units + Guaranteed invariant. Each is created at its footprint for one job
/// and deleted after — no idle reservation, no in-place resize.
pub mod build {
    use super::{GIB, MIB, Resources};

    /// Memory clears the heaviest layer step's peak (the workspace compile folds into this
    /// build) while staying under actuatable node headroom
    pub const BUILDKIT_BUILD: Resources = Resources::new(16_000, 24 * GIB, 0, 0);
    /// A trivial `sh` streaming seed bytes into a PVC; small fixed reserve, never BestEffort
    pub const UPLOADER: Resources = Resources::new(1_000, 256 * MIB, 0, 0);
}

/// Whole-cluster schedulable capacity from the probe = the input to admission. One global
/// pool (general/NVMe is placement, not a partition).
///
/// - `allocatable` = Σ `node.status.allocatable` over schedulable nodes
/// - `reserved` = Σ `max(effective_request, observed_usage)` over *all* live pods + PVC I/O
///   (request keeps admission scheduler-safe, usage catches a Burstable co-tenant over its
///   request; reserving the *limit* would sterilize the node)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClusterCapacity {
    pub allocatable: Resources,
    pub reserved: Resources,
}

impl ClusterCapacity {
    /// `allocatable - reserved`, floored at zero. Nets out every pod (other runs' included)
    /// → a run seeded from this at startup coexists with existing cluster load
    pub fn free(&self) -> Resources {
        self.allocatable.saturating_sub(&self.reserved)
    }
}

/// A tier's node placement target, not a capacity partition: tainted NVMe nodes + a
/// matching toleration/nodeSelector on `sync` pods, applied at materialize time and
/// unread by the scheduler
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pool {
    General,
    Nvme,
}

impl Pool {
    pub fn as_label(self) -> &'static str {
        match self {
            Pool::General => "general",
            Pool::Nvme => "nvme",
        }
    }

    pub fn from_label(s: &str) -> Option<Pool> {
        match s {
            "general" => Some(Pool::General),
            "nvme" => Some(Pool::Nvme),
            _ => None,
        }
    }
}

/// Tiers a test may declare. `Ord` = declaration order = ascending priority (a stable
/// `BTreeMap` key for grouping tests by tier during deterministic config lowering)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub enum QosClass {
    /// Un-annotated tests land here (`docs/design-qos.md`)
    #[default]
    Basic,
    Wallet,
    Integration,
    Testnet,
    Sync,
}

/// Lowered form of a [`QosClass`] (§2/§11).
///
/// - `footprint` = component-pod aggregate reserve *and* ceiling; excludes the runner pod,
///   sizes the namespace `ResourceQuota`, whole-core count = max component pods the tier
///   admits (so `pods × per-pod ≤ footprint` always holds)
/// - `footprint` + `runner` = [`admitted`](Self::admitted); higher `priority` goes first
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QosProfile {
    pub footprint: Resources,
    pub runner: Resources,
    pub pool: Pool,
    pub priority: u8,
    pub hard_cap: Duration,
}

impl QosProfile {
    /// Whole-cluster reserve a test is admitted against. Both halves are real pods, so
    /// admission must reserve their sum or the runner is unbudgeted cluster load
    pub fn admitted(&self) -> Resources {
        self.footprint.saturating_add(&self.runner)
    }

    /// Component reserve replaced by a test's `footprint = ".."`; `None` = untouched
    ///
    /// - Sole place an override lands (quota/share/budget/lease/request all read `footprint`)
    /// - `runner`/`pool`/`priority`/`hard_cap` not overridable (cross-test policy)
    pub fn with_footprint(self, footprint: Option<Resources>) -> QosProfile {
        match footprint {
            Some(footprint) => {
                footprint.assert_pod_schedulable("footprint override");
                QosProfile { footprint, ..self }
            }
            None => self,
        }
    }

    /// Per-pod Guaranteed reserve = [`footprint`](Self::footprint) split evenly
    /// across `pods` (`None` when none to size)
    ///
    /// - CPU floors to whole cores (CPU Manager `static` pins only integer cores)
    /// - Floor, not ceil → `pods x share <= footprint`, so a deploy never exceeds
    ///   what was admitted (lost sub-core stays as headroom)
    /// - cpu/mem only: this sizes a Guaranteed *container*, and io/disk are volume
    ///   properties with no per-pod split to make
    pub fn share(&self, pods: usize) -> Option<Resources> {
        if pods == 0 {
            return None;
        }
        self.footprint.assert_pod_schedulable("tier footprint");
        let n = pods as u64;
        // 1-core clamp → trips only when pods > tier cores (mispriced tier;
        // would otherwise deploy past the reservation and wedge `Pending`)
        assert!(
            self.footprint.cpu_milli / 1000 >= n,
            "QoS over-schedule: {pods} component pods need {pods} whole cores but this test \
             reserves {}m — raise the tier, or its `footprint = \"..\"` override, so the \
             core count covers the topology",
            self.footprint.cpu_milli,
        );
        Some(Resources::new(
            (self.footprint.cpu_milli / n / 1000).max(1) * 1000,
            (self.footprint.mem_bytes / n).max(1),
            0,
            0,
        ))
    }
}

impl QosClass {
    pub fn as_label(self) -> &'static str {
        match self {
            QosClass::Basic => "basic",
            QosClass::Wallet => "wallet",
            QosClass::Integration => "integration",
            QosClass::Testnet => "testnet",
            QosClass::Sync => "sync",
        }
    }

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

    /// Effective profile = tier + this test's override; read at every sizing/admission site
    ///
    /// - Bare [`profile`](Self::profile) there = latent bug (sizes default, ledger holds override)
    pub fn profile_with(self, footprint: Option<Resources>) -> QosProfile {
        self.profile().with_footprint(footprint)
    }

    /// Const profile table: one source of truth for every tier's schedulable shape. I/O
    /// reserves stay `0` pending calibration (a reserve must come from measured `io.stat`,
    /// never a guess), so those dimensions never gate. Tier *default*, not what a given
    /// test runs at — see [`profile_with`](Self::profile_with)
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
                // 2c + 1 GiB per pod across the validator + indexer pair. 1 GiB is what a
                // regtest zebrad needs for Orchard proving and the NU6 boundary work (a
                // 512 MiB share OOM-killed it)
                footprint: Resources::new(4_000, 2 * GIB, 0, 0),
                // In-process wallet lives here → real compute, not just orchestration
                runner: Resources::new(4_000, GIB, 0, 0),
                pool: Pool::General,
                priority: 1,
                hard_cap: Duration::from_secs(10 * 60),
            },
            QosClass::Integration => QosProfile {
                // 1c + 1 GiB per pod across the ≤3-pod zaino topology (zebrad +
                // zaino-fetch + zaino-state); core count = max component pods admitted
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
                // 16c / 16 GiB all in, minus the runner's slice. A ceiling the pods share,
                // not an even split: this tier's topologies are lopsided (a validator
                // serving a frozen snapshot beside an indexer building one), so tests size
                // pods with `.resources()` and this bounds their sum
                footprint: Resources::new(15_000, 15 * GIB, 0, 0),
                // Driver only marshals the test + polls an exporter; work is in the pods
                runner: Resources::new(1_000, GIB, 0, 0),
                pool: Pool::Nvme,
                priority: 4,
                hard_cap: Duration::from_secs(48 * 60 * 60),
            },
        }
    }
}

// ── Runtime tier (the in-process bridge) ───────────────────────────────
//
// `#[ztest::qos::*]` / `#[ztest::sync_test]` inject `__enter(class)` as the body's first
// statement; `build()` reads `current()` to size pods. Thread-local is the fast path, but
// a `multi_thread` test can migrate off the entering thread across an `.await` before
// `build()` reads the tier → `__enter` also stamps a process-global backstop. Sound because
// ztest is strictly process-per-test: the last-entered tier is unambiguous process-wide.

thread_local! {
    static CURRENT: Cell<Option<QosClass>> = const { Cell::new(None) };
}
/// Process-global backstop for the thread-local; `u8::MAX` = no tier entered (→ `Basic`)
static CURRENT_GLOBAL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(u8::MAX);

/// Explicit rather than a `repr(u8)` cast → adding a variant is a compile error here
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

// Override rides alongside the tier, same thread-local + global-backstop rule
// - Two atomics, no lock: `0` = unset (grammar + `assert_pod_schedulable` both reject zero)
thread_local! {
    static CURRENT_FOOTPRINT: Cell<Option<Resources>> = const { Cell::new(None) };
}
static CURRENT_FOOTPRINT_CPU: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static CURRENT_FOOTPRINT_MEM: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Set current tier + optional override. Injected by `#[ztest::qos::*]` /
/// `#[ztest::sync_test]` as the body's first statement; never called directly
#[doc(hidden)]
pub fn __enter(class: QosClass, footprint: Option<Resources>) {
    use std::sync::atomic::Ordering::Relaxed;
    CURRENT.with(|c| c.set(Some(class)));
    CURRENT_GLOBAL.store(tier_to_u8(class), Relaxed);
    CURRENT_FOOTPRINT.with(|c| c.set(footprint));
    CURRENT_FOOTPRINT_CPU.store(footprint.map_or(0, |f| f.cpu_milli), Relaxed);
    CURRENT_FOOTPRINT_MEM.store(footprint.map_or(0, |f| f.mem_bytes), Relaxed);
}

/// Tier declared by the running test, else [`QosClass::Basic`]. Read by `TestEnv::build`;
/// thread-local first, process-global backstop for off-thread reads
pub fn current() -> QosClass {
    if let Some(c) = CURRENT.with(|c| c.get()) {
        return c;
    }
    tier_from_u8(CURRENT_GLOBAL.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(QosClass::Basic)
}

/// Override declared by the running test, else `None`
fn current_footprint() -> Option<Resources> {
    if let Some(f) = CURRENT_FOOTPRINT.with(|c| c.get()) {
        return Some(f);
    }
    use std::sync::atomic::Ordering::Relaxed;
    let (cpu, mem) = (CURRENT_FOOTPRINT_CPU.load(Relaxed), CURRENT_FOOTPRINT_MEM.load(Relaxed));
    // Either dimension `0` = none entered (grammar cannot emit a half-zero footprint)
    (cpu > 0 && mem > 0).then(|| Resources::new(cpu, mem, 0, 0))
}

/// Effective profile of the running test (declared tier + declared override)
///
/// - Sole accessor for in-process sizing (shares, quota, deploy budget, sync lease)
pub fn current_profile() -> QosProfile {
    current().profile_with(current_footprint())
}

#[cfg(test)]
mod tests {
    /// Pins the tier table `docs/design-qos.md` publishes. It drifted once — the doc
    /// claimed `integration 4c/2GiB` against a `3c/3GiB` profile — and nothing caught it
    /// because no test read the table as data
    #[test]
    fn the_tier_table_matches_the_documented_footprints() {
        let expect = [
            (QosClass::Basic, 1_000, 512 * MIB, 2_000, GIB),
            (QosClass::Wallet, 4_000, 2 * GIB, 8_000, 3 * GIB),
            (QosClass::Integration, 3_000, 3 * GIB, 4_000, 4 * GIB),
            (QosClass::Testnet, 8_000, 10 * GIB, 9_000, 11 * GIB),
            (QosClass::Sync, 15_000, 15 * GIB, 16_000, 16 * GIB),
        ];
        for (class, cpu, mem, adm_cpu, adm_mem) in expect {
            let p = class.profile();
            assert_eq!(
                (p.footprint.cpu_milli, p.footprint.mem_bytes),
                (cpu, mem),
                "{} components",
                class.as_label()
            );
            assert_eq!(
                (p.admitted().cpu_milli, p.admitted().mem_bytes),
                (adm_cpu, adm_mem),
                "{} admitted",
                class.as_label()
            );
        }
    }

    /// Saturates rather than exceeding, and a zero denominator yields 0 instead of
    /// dividing — both dimensions independently, so a caller can pick min or max
    #[test]
    fn ratio_pct_is_per_dimension_and_saturates() {
        let limit = Resources::new(9_000, 24 * GIB, 0, 0);
        let usage = Resources::new(594, 10_348 * MIB, 0, 0);
        assert_eq!(usage.ratio_pct(&limit), (6, 42));
        assert_eq!(Resources::new(20_000, 0, 0, 0).ratio_pct(&limit).0, 100, "clamped at 100");
        assert_eq!(usage.ratio_pct(&Resources::ZERO), (0, 0), "no denominator, no division");
    }

    /// The dense per-row form: integer where the value is whole, and never a `0Gi` that
    /// would report a footprint smaller than the pod it describes
    #[test]
    fn compact_is_integer_but_never_rounds_a_footprint_to_nothing() {
        assert_eq!(Resources::new(15_000, 15 * GIB, 0, 0).compact(), "15c/15Gi");
        assert_eq!(Resources::new(1_000, 512 * MIB, 0, 0).compact(), "1c/512Mi");
        assert_eq!(Resources::new(500, 512 * MIB, 0, 0).compact(), "0.5c/512Mi");
        assert_eq!(Resources::ZERO.compact(), "0c/0Mi");
    }

    /// Disk rides the same footprint cell as cpu/mem, and drops out when undeclared so the
    /// rendered form is still `footprint = ".."` input
    #[test]
    fn compact_carries_disk_only_once_declared() {
        assert_eq!(
            Resources::new(15_000, 15 * GIB, 0, 0).with_disk(400 * GIB).compact(),
            "15c/15Gi/400Gi"
        );
        assert_eq!(Resources::new(15_000, 15 * GIB, 0, 0).compact(), "15c/15Gi");
    }

    /// `compact` → `footprint::parse` round trip: the cell a table renders is the string a
    /// test may paste back into its attribute
    #[test]
    fn a_rendered_footprint_reparses() {
        let r = Resources::new(15_000, 29 * GIB, 0, 0).with_disk(400 * GIB);
        let parsed = ztest_attr::footprint::parse(&r.compact()).expect("re-parses");
        assert_eq!(Resources::from_footprint(parsed), r);
    }

    /// The I/O pair is inert on every beacon ever written; carrying it would double each
    /// serialized footprint for two zeroes
    #[test]
    fn serialized_resources_omit_the_uncalibrated_io_dimensions() {
        let json = serde_json::to_string(&Resources::new(3_000, GIB, 0, 0)).expect("serialize");
        assert_eq!(json, r#"{"cpu_milli":3000,"mem_bytes":1073741824}"#);
        assert_eq!(
            serde_json::from_str::<Resources>(&json).expect("round-trips"),
            Resources::new(3_000, GIB, 0, 0)
        );
    }

    use super::*;

    // Resources: the 4-D packing primitive. Every dimension gates; arithmetic exact per one.

    #[test]
    fn display_renders_whole_and_fractional_units() {
        assert_eq!(Resources::new(3_000, 3 * GIB, 0, 0).to_string(), "3c / 3 GiB");
        assert_eq!(Resources::new(500, 512 * MIB, 0, 0).to_string(), "500m / 512 MiB");
        assert_eq!(Resources::ZERO.to_string(), "0m / 0 MiB");
    }

    /// Undeclared disk must stay off the wire: every beacon written before the dimension
    /// existed decodes, and a declared one survives the round trip
    #[test]
    fn serialized_disk_is_omitted_when_undeclared_and_carried_when_set() {
        let bare = serde_json::to_string(&Resources::new(3_000, GIB, 0, 0)).expect("serialize");
        assert!(!bare.contains("disk_bytes"), "undeclared disk must not reach the wire: {bare}");

        let sized = Resources::new(3_000, GIB, 0, 0).with_disk(400 * GIB);
        let json = serde_json::to_string(&sized).expect("serialize");
        assert_eq!(serde_json::from_str::<Resources>(&json).expect("round-trips"), sized);

        let legacy = r#"{"cpu_milli":3000,"mem_bytes":1073741824}"#;
        assert_eq!(
            serde_json::from_str::<Resources>(legacy).expect("legacy beacon decodes").disk_bytes,
            0
        );
    }

    /// Disk gates like every other dimension, and an uncalibrated ceiling never gates
    #[test]
    fn disk_gates_fits_within_unless_the_ceiling_is_unbounded() {
        let cap = Resources::new(1_000, GIB, 0, 0).with_disk(100 * GIB);
        assert!(Resources::new(1_000, GIB, 0, 0).with_disk(100 * GIB).fits_within(&cap));
        assert!(!Resources::new(1_000, GIB, 0, 0).with_disk(101 * GIB).fits_within(&cap));

        let uncalibrated = Resources::cpu_mem_unbounded_rest(1_000, GIB);
        assert!(
            Resources::new(1_000, GIB, 0, 0).with_disk(400 * GIB).fits_within(&uncalibrated),
            "an un-benchmarked node must not gate on a dimension it cannot measure"
        );
    }

    #[test]
    fn fits_within_requires_every_dimension() {
        let cap = Resources::new(1_000, GIB, 100 * MIB, 5_000);
        // All four fit (equal is fine)
        assert!(cap.fits_within(&cap));
        assert!(Resources::new(500, 512 * MIB, 50 * MIB, 2_000).fits_within(&cap));
        // Any single dimension over fails, the other three fitting
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
        assert_eq!(a.checked_sub(&b), Some(Resources::new(500, 512 * MIB, 60 * MIB, 3_000)));
        // Under-subtraction in any dimension → None
        assert_eq!(b.checked_sub(&a), None);
        // Overflow → None, checked in IOPS to prove the guard isn't CPU/memory-only
        assert_eq!(
            Resources::new(0, 0, 0, u64::MAX).checked_add(&Resources::new(0, 0, 0, 1)),
            None
        );
    }

    #[test]
    fn saturating_sub_clamps_at_zero_per_dimension() {
        // free = available − committed after a reconcile shrinks available below committed:
        // floors at zero, never wraps, in every dimension including the I/O pair
        let available = Resources::new(1_000, GIB, 50 * MIB, 1_000);
        let committed = Resources::new(4_000, 8 * GIB, 200 * MIB, 9_000);
        assert_eq!(available.saturating_sub(&committed), Resources::ZERO);
    }

    // Profile table: the locked facts, not the TBD reserves.

    #[test]
    fn hard_caps_are_locked() {
        assert_eq!(QosClass::Basic.profile().hard_cap, Duration::from_secs(60));
        assert_eq!(QosClass::Integration.profile().hard_cap, Duration::from_secs(10 * 60));
        assert_eq!(QosClass::Testnet.profile().hard_cap, Duration::from_secs(6 * 60 * 60));
        assert_eq!(QosClass::Sync.profile().hard_cap, Duration::from_secs(48 * 60 * 60));
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
        // 4c / 2 GiB even-split across validator+indexer → 2c / 1 GiB each (1 GiB clears
        // the Orchard/boundary work a 512 MiB share OOM-killed); runner carries the
        // in-process wallet's 4c; admitted = their sum, 8c / 3 GiB
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
        // The general-pool ordering behind "testnet scheduled first"
        assert!(b < w && w < i && i < t, "basic < wallet < integration < testnet");
        // sync tops the order overall (own pool, ordering still well-defined)
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
        // I/O dimensions present but dormant: a reserve must come from measured `io.stat`,
        // never a guess (which re-introduces the mispricing the dimension exists to fix).
        // Flip this when calibration lands (§6)
        for c in ALL_TIERS {
            let fp = c.profile().footprint;
            assert_eq!(fp.disk_bps, 0, "{c:?} has a fabricated disk_bps reserve");
            assert_eq!(fp.disk_iops, 0, "{c:?} has a fabricated disk_iops reserve");
        }
    }

    #[test]
    fn guaranteed_cpu_mem_rounds_cpu_up_to_whole_cores_and_keeps_bytes() {
        // 4 whole cores exact; memory verbatim in bytes
        let (cpu, mem) = Resources::new(4_000, 2 * GIB, 0, 0).guaranteed_cpu_mem("t");
        assert_eq!(cpu, "4");
        assert_eq!(mem, (2 * GIB).to_string());
        // Fractional millicores round UP to a whole core (static-policy pinning)
        let (cpu, _) = Resources::new(2_500, GIB, 0, 0).guaranteed_cpu_mem("t");
        assert_eq!(cpu, "3");
        // Sub-core rounds up to 1
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
        // No ztest-spawned pod may be sized degenerate (BestEffort/unschedulable); covers
        // the runner reserves + build-infra footprints as a set
        for c in ALL_TIERS {
            c.profile().footprint.assert_pod_schedulable("tier footprint");
            c.profile().runner.assert_pod_schedulable("tier runner");
        }
        for fp in [build::BUILDKIT_BUILD, build::UPLOADER] {
            fp.assert_pod_schedulable("build pod footprint");
        }
    }

    #[test]
    fn wallet_runner_keeps_the_in_process_wallet_compute() {
        // Wallet runs in-process in the runner → ≥4 cores; orchestration-only tiers keep 1
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
        // Variant names are the wire form
        assert_eq!(serde_json::to_string(&QosClass::Sync).unwrap(), "\"Sync\"");
    }

    #[test]
    fn enter_sets_current_tier_on_this_thread() {
        // Thread-local fast path only: the process-global backstop rests on a
        // one-test-per-process invariant, so asserting it here would race other tests'
        // `__enter` under the parallel harness
        __enter(QosClass::Testnet, None);
        assert_eq!(current(), QosClass::Testnet);
        __enter(QosClass::Sync, None);
        assert_eq!(current(), QosClass::Sync);
    }

    // ── footprint override ──────────────────────────────────────────────
    //
    // Moves the component reserve + what derives from it, nothing else

    #[test]
    fn an_override_replaces_only_the_component_reserve() {
        let base = QosClass::Sync.profile();
        let over = Resources::new(15_000, 29 * GIB, 0, 0);
        let eff = base.with_footprint(Some(over));

        assert_eq!(eff.footprint, over);
        assert_eq!(eff.runner, base.runner, "runner is not overridable");
        assert_eq!(eff.priority, base.priority, "priority is not overridable");
        assert_eq!(eff.hard_cap, base.hard_cap, "hard cap is not overridable");
        assert_eq!(eff.pool, base.pool, "placement is not overridable");
    }

    #[test]
    fn no_override_leaves_the_profile_identical() {
        let base = QosClass::Sync.profile();
        assert_eq!(base.with_footprint(None), base);
    }

    #[test]
    fn admitted_tracks_the_override_so_the_reserve_covers_the_pods() {
        // Keeps a run inside its lease (pods' max collective request + runner = reserved)
        let base = QosClass::Sync.profile();
        let over = Resources::new(15_000, 29 * GIB, 0, 0);
        let eff = base.with_footprint(Some(over));
        assert_eq!(eff.admitted(), over.saturating_add(&base.runner));
        assert!(eff.footprint.fits_within(&eff.admitted()));
    }

    #[test]
    fn share_divides_the_override_rather_than_the_tier_default() {
        let eff =
            QosClass::Sync.profile().with_footprint(Some(Resources::new(16_000, 32 * GIB, 0, 0)));
        let share = eff.share(2).expect("two pods");
        assert_eq!(share, Resources::new(8_000, 16 * GIB, 0, 0));
        // Floor invariant holds against the effective footprint
        assert!(
            Resources::new(share.cpu_milli * 2, share.mem_bytes * 2, 0, 0)
                .fits_within(&eff.footprint)
        );
    }

    #[test]
    #[should_panic(expected = "footprint override")]
    fn a_zero_override_is_refused_rather_than_rendering_besteffort_pods() {
        // Grammar rejects first; guards a programmatically-built override
        let _ = QosClass::Sync.profile().with_footprint(Some(Resources::new(0, GIB, 0, 0)));
    }

    #[test]
    fn profile_with_is_the_same_lowering_as_with_footprint() {
        let over = Resources::new(4_000, 8 * GIB, 0, 0);
        assert_eq!(
            QosClass::Basic.profile_with(Some(over)),
            QosClass::Basic.profile().with_footprint(Some(over))
        );
    }

    #[test]
    fn enter_carries_the_override_into_the_effective_profile() {
        let over = Resources::new(15_000, 29 * GIB, 0, 0);
        __enter(QosClass::Sync, Some(over));
        assert_eq!(current(), QosClass::Sync);
        assert_eq!(current_profile().footprint, over);
        assert_eq!(current_profile().runner, QosClass::Sync.profile().runner);

        // Re-enter without one → tier default (stale override must not leak process-wide)
        __enter(QosClass::Sync, None);
        assert_eq!(current_profile(), QosClass::Sync.profile());
    }

    #[test]
    fn tier_u8_mapping_round_trips_and_rejects_unknown() {
        for c in ALL_TIERS {
            assert_eq!(tier_from_u8(tier_to_u8(c)), Some(c));
        }
        assert_eq!(tier_from_u8(u8::MAX), None);
    }

    /// Synthetic tier → sizing tested independently of the reserve table
    fn tier(cpu_milli: u64, mem_bytes: u64) -> QosProfile {
        QosProfile {
            footprint: Resources::new(cpu_milli, mem_bytes, 0, 0),
            runner: Resources::new(1_000, GIB, 0, 0),
            pool: Pool::General,
            priority: 0,
            hard_cap: Duration::from_secs(60),
        }
    }

    #[test]
    fn share_floors_cpu_to_whole_cores_and_splits_memory() {
        // 16c/32Gi across 2 pods: an exact split
        let s = tier(16_000, 32 * GIB).share(2).unwrap();
        assert_eq!((s.cpu_milli, s.mem_bytes), (8_000, 16 * GIB));

        // 8c/18Gi across 3 → 2667m each, floored to 2c; the spare core stays inside the
        // reservation as headroom rather than over-deploying
        assert_eq!(tier(8_000, 18 * GIB).share(3).unwrap().cpu_milli, 2_000);

        // Smallest viable tier: one core, one pod
        let s = tier(1_000, 512 * MIB).share(1).unwrap();
        assert_eq!((s.cpu_milli, s.mem_bytes), (1_000, 512 * MIB));

        assert!(tier(8_000, 8 * GIB).share(0).is_none(), "no pods to size");
    }

    #[test]
    fn the_default_split_never_exceeds_the_footprint_it_splits() {
        // Flooring invariant across every topology a tier admits: the pods' total request
        // never tops what admission reserved
        let t = tier(8_000, 18 * GIB);
        for pods in 1..=8 {
            let s = t.share(pods).expect("pods > 0");
            let total = s.cpu_milli * pods as u64;
            assert!(total <= t.footprint.cpu_milli, "{pods} pods want {total}m");
        }
        assert_eq!(t.share(3).unwrap().cpu_milli, 2_000, "floor(2667m) = 2c");
    }

    #[test]
    #[should_panic(expected = "over-schedule")]
    fn share_panics_when_a_topology_needs_more_pods_than_the_tier_has_cores() {
        // 4 pods on a 3-core tier: each clamps to 1c → a 4c deploy against a 3c budget
        tier(3_000, 3 * GIB).share(4);
    }
}
