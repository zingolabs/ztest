//! Run status published on its ledger Lease (`docs/design-status.md`).
//!
//! - [`ledger::drive`](super::ledger) rewrites the lease every tick → status rides free
//! - Reserved = upper bound `assert_invariant` enforces → no committed figure to disagree
//! - Sole feed for `ztest status` (one namespace, one watch, no pod list)

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::coordination::v1::Lease;
use serde::{Deserialize, Serialize};

use super::{LABEL_USER, Resources};

/// Denormalized index keys. The JSON blob is the record; these three exist so the ledger's
/// hot path ([`reservation_of`](super::ledger), [`kind_of`]) can classify and sum a lease
/// without parsing it. Written from the same [`Beacon`], never edited independently
pub(crate) const ANN_RESERVE_CPU: &str = "ztest.io/reserve-cpu-milli";
pub(crate) const ANN_RESERVE_MEM: &str = "ztest.io/reserve-mem-bytes";
const ANN_KIND: &str = "ztest.io/kind";

/// The record: one [`Beacon`] as JSON
const ANN_BEACON: &str = "ztest.io/beacon";

/// Display shows 3; the slack absorbs a churn between write and read. Count + aggregate
/// stay exact past it, so the overflow row is exact from a truncated list
const MAX_RUNNING: usize = 8;

/// Bar right edges step instead of shimmering (a raw ETA re-projects every tick)
fn quantize(d: Duration) -> Duration {
    let step = if d < Duration::from_secs(3600) { 60 } else { 900 };
    match d.as_secs() {
        0 => Duration::ZERO,
        secs => Duration::from_secs(((secs + step / 2) / step).max(1) * step),
    }
}

/// What a lease holds capacity for. `Claim` reserves zero — a waiter parked in
/// [`acquire`](super::ledger::acquire), visible to peers and to `ztest status`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LeaseKind {
    #[default]
    Run,
    Build,
    Sync,
    Claim,
}

impl LeaseKind {
    pub fn as_str(self) -> &'static str {
        match self {
            LeaseKind::Run => "run",
            LeaseKind::Build => "build",
            LeaseKind::Sync => "sync",
            LeaseKind::Claim => "claim",
        }
    }

    /// Unknown → `Run` (a newer writer's kind must not drop the row)
    fn parse(s: &str) -> LeaseKind {
        match s {
            "build" => LeaseKind::Build,
            "sync" => LeaseKind::Sync,
            "claim" => LeaseKind::Claim,
            _ => LeaseKind::Run,
        }
    }
}

/// One in-flight test. Footprint is the *effective* one
/// ([`profile_with`](super::QosClass::profile_with) honours `.resources()` overrides), so
/// it ships rather than being re-derived from a tier the viewer would look up wrong
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningTest {
    pub name: String,
    pub footprint: Resources,
    pub started_at: DateTime<Utc>,
    /// Beside the footprint, not instead of it — an override makes the two independent,
    /// and the left panel's tally groups by tier
    #[serde(default)]
    pub tier: super::QosClass,
}

/// One run, as `ztest status` sees it. Serialized whole into the lease's record
/// annotation; `run_id`/`user` ride along but the object always overrules them on decode
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Beacon {
    pub run_id: String,
    pub user: String,
    pub kind: LeaseKind,
    pub reserve: Resources,
    pub started_at: DateTime<Utc>,
    pub total: u32,
    pub queued: u32,
    pub failed: u32,
    /// Newest-first, `MAX_RUNNING` cap; `running_count` stays exact past it
    pub running: Vec<RunningTest>,
    pub running_count: u32,
    pub running_footprint: Resources,
    /// `Claim` only — what the waiter is blocked on
    pub needs: Option<Resources>,
    pub eta_override: Option<Duration>,
}

/// Engine → ledger status message. Counts and aggregate are derived from `running`, so a
/// caller cannot publish a tally that disagrees with the list it came from
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    pub total: u32,
    pub queued: u32,
    pub failed: u32,
    /// Newest-first; [`Beacon::annotations`] truncates the wire copy, never this
    pub running: Vec<RunningTest>,
    pub eta_override: Option<Duration>,
}

impl Default for Beacon {
    fn default() -> Beacon {
        Beacon::new("", "?", LeaseKind::Run, Resources::ZERO)
    }
}

impl Beacon {
    /// Freshly acquired, before any test has been admitted
    pub fn new(run_id: &str, user: &str, kind: LeaseKind, reserve: Resources) -> Beacon {
        Beacon {
            run_id: run_id.to_string(),
            user: user.to_string(),
            kind,
            reserve,
            started_at: Utc::now(),
            total: 0,
            queued: 0,
            failed: 0,
            running: Vec::new(),
            running_count: 0,
            running_footprint: Resources::ZERO,
            needs: None,
            eta_override: None,
        }
    }

    /// Merge a [`Progress`], leaving `reserve` (the reconcile loop's) untouched
    pub fn apply(&mut self, p: Progress) {
        self.total = p.total;
        self.queued = p.queued;
        self.failed = p.failed;
        self.eta_override = p.eta_override;
        self.running_count = p.running.len() as u32;
        self.running_footprint =
            p.running.iter().fold(Resources::ZERO, |a, t| a.saturating_add(&t.footprint));
        self.running = p.running;
    }

    /// Derived, never stored — no counter that can drift out of step with the others
    pub fn completed(&self) -> u32 {
        self.total.saturating_sub(self.queued).saturating_sub(self.running_count)
    }

    /// Running tests grouped by tier. Truncated past [`MAX_RUNNING`] — the tally covers
    /// what the list carries, and [`elided`](Self::elided) accounts for the rest
    pub fn by_tier(&self) -> std::collections::BTreeMap<super::QosClass, super::live::TierLive> {
        super::live::tier_tally(self.running.iter().map(|t| (t.tier, t.footprint)))
    }

    /// Tests running beyond the [`MAX_RUNNING`] list, and what they hold together
    pub fn elided(&self) -> Option<(u32, Resources)> {
        let n = self.running_count.saturating_sub(self.running.len() as u32);
        (n > 0).then(|| {
            let shown = self
                .running
                .iter()
                .fold(Resources::ZERO, |acc, t| acc.saturating_add(&t.footprint));
            (n, self.running_footprint.saturating_sub(&shown))
        })
    }

    /// Projected time to this run's last test.
    ///
    /// - Mean wall-clock throughput since [`started_at`](Self::started_at) — already embeds
    ///   the run's live parallelism, so `running_count` is not a second factor
    /// - Nothing completed = no rate → `None`, rendered `?` (never a fabricated countdown)
    /// - Assumes uniform per-test cost (no per-tier queue histogram on the wire), so a
    ///   queue mixing `basic` and `sync` projects badly
    /// - [`eta_override`](Self::eta_override) wins: a sync driver knows its own remaining
    ///   time from chain pace, which no test-count model can reach
    pub fn eta(&self, now: DateTime<Utc>) -> Option<Duration> {
        if let Some(d) = self.eta_override {
            return Some(quantize(d));
        }
        // No inventory = nothing to project from (a build, or a lease written before its
        // engine reported); `Some(ZERO)` there would claim an imminent finish
        if self.total == 0 {
            return None;
        }
        let remaining = self.queued + self.running_count;
        if remaining == 0 {
            return Some(Duration::ZERO);
        }
        let completed = self.completed();
        if completed == 0 {
            return None;
        }
        let elapsed = (now - self.started_at).to_std().ok()?;
        let per_test = elapsed.as_secs_f64() / f64::from(completed);
        Some(quantize(Duration::from_secs_f64(per_test * f64::from(remaining))))
    }

    /// This run's annotations, replacing the whole set (server-side apply, so a key dropped
    /// here is dropped on the object)
    pub fn annotations(&self) -> BTreeMap<String, String> {
        let mut a = BTreeMap::from([
            (ANN_RESERVE_CPU.to_string(), self.reserve.cpu_milli.to_string()),
            (ANN_RESERVE_MEM.to_string(), self.reserve.mem_bytes.to_string()),
            (ANN_KIND.to_string(), self.kind.as_str().to_string()),
        ]);
        // Truncated for the wire only; `running_count`/`running_footprint` stay exact, so
        // the overflow row is exact from a capped list
        let wire = Beacon {
            running: self.running[..self.running.len().min(MAX_RUNNING)].to_vec(),
            ..self.clone()
        };
        if let Ok(json) = serde_json::to_string(&wire) {
            a.insert(ANN_BEACON.to_string(), json);
        }
        a
    }

    /// Cheap kind probe, for callers classifying a lease without parsing the record
    pub fn kind_of(lease: &Lease) -> LeaseKind {
        lease
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(ANN_KIND))
            .map(|s| LeaseKind::parse(s))
            .unwrap_or_default()
    }

    /// `None` = not a ztest lease (a foreign holder in the namespace).
    ///
    /// - Record absent → rebuilt from the index keys + `creationTimestamp`, so a pre-beacon
    ///   writer's reserve-only lease renders as a row with zeroed progress, never a
    ///   dropped run
    /// - `run_id`/`user` always taken from the object: the label reap and the ledger key on
    ///   those, so a stale blob must not be able to disagree about identity
    pub fn decode(lease: &Lease) -> Option<Beacon> {
        let meta = &lease.metadata;
        let run_id = meta.name.clone()?;
        let ann = meta.annotations.as_ref()?;
        let num = |k: &str| ann.get(k).and_then(|s| s.trim().parse::<u64>().ok()).unwrap_or(0);
        let mut beacon = match ann.get(ANN_BEACON).and_then(|j| serde_json::from_str(j).ok()) {
            Some(b) => b,
            None => Beacon {
                kind: ann.get(ANN_KIND).map(|s| LeaseKind::parse(s)).unwrap_or_default(),
                reserve: Resources::new(num(ANN_RESERVE_CPU), num(ANN_RESERVE_MEM), 0, 0),
                started_at: meta.creation_timestamp.as_ref().map(|t| t.0)?,
                ..Beacon::default()
            },
        };
        beacon.run_id = run_id;
        beacon.user = meta
            .labels
            .as_ref()
            .and_then(|l| l.get(LABEL_USER))
            .cloned()
            .unwrap_or_else(|| "?".into());
        beacon.reserve = Resources::new(num(ANN_RESERVE_CPU), num(ANN_RESERVE_MEM), 0, 0);
        Some(beacon)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::{GIB, MIB};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};

    fn ts(s: &str) -> DateTime<Utc> {
        s.parse().expect("static timestamp")
    }

    fn test(name: &str, cpu: u64, mem: u64) -> RunningTest {
        RunningTest {
            name: name.into(),
            footprint: Resources::new(cpu, mem, 0, 0),
            started_at: ts("2026-08-17T14:33:03Z"),
            tier: crate::qos::QosClass::Sync,
        }
    }

    fn beacon() -> Beacon {
        Beacon {
            run_id: "elicb-47192".into(),
            user: "elicb".into(),
            kind: LeaseKind::Run,
            reserve: Resources::new(30_000, 30 * GIB, 0, 0),
            started_at: ts("2026-08-17T14:20:00Z"),
            total: 17,
            queued: 12,
            failed: 0,
            running: vec![
                test("sync::feat_nu6_3_topology", 15_000, 15 * GIB),
                test("wallet::send_shielded", 15_000, 15 * GIB),
            ],
            running_count: 2,
            running_footprint: Resources::new(30_000, 30 * GIB, 0, 0),
            needs: None,
            eta_override: None,
        }
    }

    fn lease_of(b: &Beacon) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(b.run_id.clone()),
                labels: Some(BTreeMap::from([(LABEL_USER.to_string(), b.user.clone())])),
                annotations: Some(b.annotations()),
                ..Default::default()
            },
            spec: None,
        }
    }

    #[test]
    fn annotations_round_trip_through_a_lease() {
        let b = beacon();
        assert_eq!(Beacon::decode(&lease_of(&b)).as_ref(), Some(&b));
    }

    #[test]
    fn completed_is_derived_never_stored() {
        assert_eq!(beacon().completed(), 3);
    }

    /// The overflow row (`+ 12 more · 12c/6Gi`) must be exact from a truncated list, so the
    /// count and aggregate are carried separately and survive the cap
    #[test]
    fn a_truncated_list_still_yields_an_exact_overflow() {
        let mut b = beacon();
        b.running = (0..15).map(|i| test(&format!("basic::t{i}"), 1_000, 512 * MIB)).collect();
        b.running_count = 15;
        b.running_footprint = Resources::new(15_000, 15 * 512 * MIB, 0, 0);

        let decoded = Beacon::decode(&lease_of(&b)).expect("beacon decodes");
        assert_eq!(decoded.running.len(), MAX_RUNNING, "list capped on the wire");
        assert_eq!(decoded.running_count, 15, "count survives the cap");

        let (n, held) = decoded.elided().expect("7 tests beyond the list");
        assert_eq!(n, 7);
        assert_eq!(held.cpu_milli, 7_000);
        assert_eq!(held.mem_bytes, 7 * 512 * MIB);
    }

    #[test]
    fn a_claim_carries_what_it_waits_for_and_reserves_nothing() {
        let b = Beacon {
            kind: LeaseKind::Claim,
            reserve: Resources::ZERO,
            needs: Some(Resources::new(8_000, 10 * GIB, 0, 0)),
            running: vec![],
            running_count: 0,
            running_footprint: Resources::ZERO,
            total: 9,
            queued: 9,
            ..beacon()
        };
        let decoded = Beacon::decode(&lease_of(&b)).expect("claim decodes");
        assert_eq!(decoded.kind, LeaseKind::Claim);
        assert_eq!(decoded.reserve, Resources::ZERO, "a claim adds zero to sum_reservations");
        assert_eq!(decoded.needs.expect("needs").cpu_milli, 8_000);
    }

    /// A pre-beacon writer's lease still yields a row: reserve + age, zeroed progress
    #[test]
    fn a_reserve_only_lease_decodes_with_zero_progress() {
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some("run-a".into()),
                creation_timestamp: Some(Time(ts("2026-08-17T14:00:00Z"))),
                annotations: Some(BTreeMap::from([(
                    ANN_RESERVE_CPU.to_string(),
                    "8000".to_string(),
                )])),
                ..Default::default()
            },
            spec: None,
        };
        let b = Beacon::decode(&lease).expect("reserve-only lease still decodes");
        assert_eq!(b.reserve.cpu_milli, 8_000);
        assert_eq!(b.kind, LeaseKind::Run, "unknown/absent kind falls back, never drops the row");
        assert_eq!(b.total, 0);
        assert_eq!(b.user, "?");
    }

    /// 3 done in 13 min, 14 left → ~1h. Throughput is wall-clock, so the run's parallelism
    /// is already in the figure and must not be applied a second time
    #[test]
    fn eta_projects_from_mean_wall_clock_throughput() {
        let b = beacon();
        let eta = b.eta(ts("2026-08-17T14:33:00Z")).expect("3 completed = a rate");
        assert_eq!(eta, Duration::from_secs(3600), "13min/3 × 14 = 60.7min → nearest 15min");
    }

    #[test]
    fn a_run_with_nothing_completed_is_unprojectable() {
        let mut b = beacon();
        b.queued = 15;
        assert_eq!(b.completed(), 0);
        assert_eq!(b.eta(ts("2026-08-17T14:33:00Z")), None, "no rate → `?`, never a guess");
    }

    /// A sync's progress is chain height, not test counts — the count model would return
    /// `None` forever, so the driver publishes its own remaining time
    /// A stale or pre-engine lease reserves capacity but has no inventory; projecting
    /// `0s` from it would advertise capacity about to free that never will
    #[test]
    fn a_run_with_no_inventory_is_unprojectable() {
        let mut b = beacon();
        b.total = 0;
        b.queued = 0;
        b.running = vec![];
        b.running_count = 0;
        assert_eq!(b.eta(ts("2026-08-17T14:33:00Z")), None);
    }

    #[test]
    fn an_eta_override_wins_over_the_count_model() {
        let mut b = beacon();
        b.kind = LeaseKind::Sync;
        b.total = 0;
        b.queued = 0;
        b.eta_override = Some(Duration::from_secs(31 * 3600));
        let decoded = Beacon::decode(&lease_of(&b)).expect("sync beacon decodes");
        assert_eq!(decoded.eta(ts("2026-08-17T14:33:00Z")), Some(Duration::from_secs(31 * 3600)));
    }

    #[test]
    fn quantization_steps_by_minute_then_quarter_hour() {
        assert_eq!(quantize(Duration::from_secs(0)), Duration::ZERO);
        assert_eq!(quantize(Duration::from_secs(20)), Duration::from_secs(60), "floored, not to 0");
        assert_eq!(quantize(Duration::from_secs(100)), Duration::from_secs(120));
        assert_eq!(quantize(Duration::from_secs(7_000)), Duration::from_secs(7_200));
    }

    #[test]
    fn a_finished_queue_projects_to_now() {
        let mut b = beacon();
        b.queued = 0;
        b.running = vec![];
        b.running_count = 0;
        assert_eq!(b.eta(ts("2026-08-17T14:33:00Z")), Some(Duration::ZERO));
    }

    #[test]
    fn a_foreign_lease_is_not_a_beacon() {
        let lease = Lease {
            metadata: ObjectMeta { name: Some("kube-scheduler".into()), ..Default::default() },
            spec: None,
        };
        assert_eq!(Beacon::decode(&lease), None);
    }
}
