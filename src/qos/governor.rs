//! Live cross-run capacity governor.
//!
//! [`ledger::acquire`](super::ledger::acquire) reserves a run's opening slice once,
//! at startup. But the cluster is not static: other runs come and go, the build
//! pod is created and deleted, non-ztest load shifts. Without a live authority a
//! run is frozen at whatever it grabbed at `t0` — so a run that started on a busy
//! cluster stays throttled long after it empties, and a run that grabbed a lot
//! never cedes to a peer.
//!
//! The governor closes that loop. Each reconcile it re-reads the ledger and the
//! cluster's pods, folds in this run's live demand (from the scheduler, over
//! [`RunDemand`]), and recomputes the fair-share elastic reservation
//! ([`ledger::reserve_from_state`](super::ledger::reserve_from_state)). It then
//! updates *both* sinks that must stay consistent — the run's Lease (what other
//! runs subtract) and the scheduler's ceiling (what this run may admit into) — so
//! the cross-run invariant `committed ≤ Σ reservations` holds continuously.
//!
//! ## Keeping the invariant across a change
//!
//! The scheduler guarantees `committed ≤ available` at all times (its no-overload
//! property). So the invariant reduces to keeping `available ≤ reservation` — the
//! Lease must always cover what the scheduler is *allowed* to admit. The two are
//! written from different tasks at different instants, so the rule is:
//!
//! - **Grow:** write the Lease to `target`, *then* raise the ceiling — the Lease
//!   covers the larger ceiling before the scheduler can admit into it.
//! - **Shrink:** lower the ceiling immediately, but hold the Lease at the
//!   *previous* ceiling for one reconcile. Until the scheduler has processed the
//!   lower ceiling, it may still admit up to the previous one, so the Lease must
//!   keep covering that. The reconcile cadence (seconds) dwarfs the scheduler's
//!   sub-millisecond reaction, so the Lease catches down on the next tick.
//!
//! Both collapse to one rule: renew the Lease at `max(target, previous_ceiling)`
//! *before* publishing `target` as the new ceiling.

use std::time::Duration;

use kube::Client;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::Resources;
use super::ledger::{self, Renewer};

/// How often the governor recomputes this run's reservation. Well under the lease
/// TTL (so a reconcile doubles as the renewal heartbeat) and short enough that
/// freed cluster capacity reaches admission within a couple of seconds.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(2);

/// This run's live appetite, published by the run loop on every admit/release and
/// read by the governor to size the reservation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunDemand {
    /// Footprint of this run's active leases — what it is already running. The
    /// reservation never drops below this (no preemption).
    pub committed: Resources,
    /// `committed` plus every queued request's footprint — the most this run
    /// could use if capacity allowed. Caps the reservation so a run holds only
    /// what it can use, leaving the rest for concurrent runs.
    pub demand: Resources,
}

/// Drives a run's cross-run reservation live for the run's duration. Dropping it
/// aborts the reconcile loop (and thus the lease renewal); release the Lease
/// itself via the [`Renewer`] the caller retains.
#[derive(Debug)]
pub struct CapacityGovernor {
    task: JoinHandle<()>,
}

impl Drop for CapacityGovernor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Configuration handed to the governor once at startup. `allocatable` and
/// `budget` are fixed for the run; the live inputs arrive over `demand_rx`.
pub struct GovernorConfig {
    pub client: Client,
    pub run_id: String,
    /// Whole-cluster node capacity (fixed from the probe).
    pub allocatable: Resources,
    /// This SA's total budget.
    pub budget: Resources,
    /// The acquire-time slice; seeds the ceiling channel so the scheduler starts
    /// correct before the first reconcile lands.
    pub initial: Resources,
    /// Renews/rewrites this run's Lease.
    pub renewer: Renewer,
}

// `kube::Client` is not `Debug`; the identifying detail is the run id.
impl std::fmt::Debug for GovernorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GovernorConfig")
            .field("run_id", &self.run_id)
            .field("allocatable", &self.allocatable)
            .field("budget", &self.budget)
            .field("initial", &self.initial)
            .finish_non_exhaustive()
    }
}

/// Start the governor. Returns the handle (keep it alive for the run) and the
/// ceiling receiver the run loop reconciles the scheduler from.
pub fn spawn(
    cfg: GovernorConfig,
    demand_rx: watch::Receiver<RunDemand>,
) -> (CapacityGovernor, watch::Receiver<Resources>) {
    let (ceiling_tx, ceiling_rx) = watch::channel(cfg.initial);
    let task = tokio::spawn(reconcile_loop(cfg, demand_rx, ceiling_tx));
    (CapacityGovernor { task }, ceiling_rx)
}

async fn reconcile_loop(
    cfg: GovernorConfig,
    demand_rx: watch::Receiver<RunDemand>,
    ceiling_tx: watch::Sender<Resources>,
) {
    let leases = ledger::lease_api(&cfg.client);
    let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The last ceiling published to the scheduler; the scheduler may currently be
    // admitting up to it, so the Lease must never drop below it (see module docs).
    let mut prev = cfg.initial;
    loop {
        ticker.tick().await;

        // A crashed concurrent run's stale reservation would otherwise pin our
        // fair share down for its whole TTL; sweep it as acquire does.
        let _ = ledger::sweep_expired(&leases).await;

        let d = *demand_rx.borrow();
        // Hold the acquire-time slice until the engine publishes real demand.
        // `interval` fires its first tick immediately, and the seed demand is
        // zero; acting on it would drop the reservation (and the Lease) to zero
        // while this run's pods are already scheduling — briefly breaking the
        // cross-run invariant `committed ≤ reservation`. A genuine zero only
        // recurs when the run has drained, at which point the governor is about
        // to be dropped anyway, so holding is harmless there too.
        if d == RunDemand::default() {
            // Still renew (at the acquire slice) so the TTL heartbeat is kept
            // through a slow engine start.
            let _ = cfg.renewer.renew_at(prev).await;
            continue;
        }

        let (lease_list, pods) = match (
            ledger::list_leases(&leases).await,
            ledger::all_pods(&cfg.client).await,
        ) {
            (Ok(l), Ok(p)) => (l, p),
            // Transient list failure: hold the current ceiling and lease, retry
            // next tick. The lease TTL tolerates several missed reconciles.
            _ => continue,
        };

        let target = ledger::reserve_from_state(
            &lease_list,
            &pods,
            &cfg.run_id,
            cfg.allocatable,
            cfg.budget,
            d.committed,
            d.demand,
        );

        prev = apply(&ceiling_tx, &cfg.renewer, prev, target).await;
    }
}

/// Move this run's reservation toward `target`, holding the invariant
/// `available ≤ reservation` throughout (see the module docs). Renews the Lease
/// at `max(target, prev)` — covering whatever the scheduler may still admit until
/// it processes a shrink — *before* publishing `target` as the new ceiling.
/// Returns the ceiling now in effect (next reconcile's `prev`): `target` on a
/// successful renew, else `prev` unchanged (a missed write can only under-admit,
/// never overcommit).
async fn apply(
    ceiling_tx: &watch::Sender<Resources>,
    renewer: &Renewer,
    prev: Resources,
    target: Resources,
) -> Resources {
    let lease = target.max(&prev);
    if renewer.renew_at(lease).await.is_err() {
        return prev;
    }
    if target != prev {
        let _ = ceiling_tx.send(target);
    }
    target
}
