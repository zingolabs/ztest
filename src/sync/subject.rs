//! Sync-subject abstraction = the whole of what the harness knows about what it watches.
//!
//! - Subject launches its own sync or merely observes one; harness never names an engine
//! - Yields a raw per-tick [`ProgressView`], folded into a [`Snapshot`](crate::sync::Snapshot)
//! - Object-safe on purpose: a profile binds `Box<dyn SyncSubject>`, so a new subject is a
//!   new impl (any crate's) and never a new arm in ztest

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::RpcError;
use crate::handles::wallet::PoolBalances;
use crate::metrics::Exposition;

use super::tree::TreeRoots;
use super::work::{Op, Work};

/// Lifecycle position, the only phase vocabulary the harness owns.
///
/// - Deliberately engine-neutral: a subject's own stage names ride
///   [`ProgressView::detail`], so no engine's scan taxonomy lands in this enum
/// - `Syncing` = launched and working, whatever the subject calls that internally
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Phase {
    Starting,
    Syncing,
    Done,
}

/// Unknown word → `Syncing`, never an error: a 48 h detached sync outlives the CLI build
/// watching it, and a driver from another build may publish a stage word this one retired
impl<'de> Deserialize<'de> for Phase {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match String::deserialize(d)?.as_str() {
            "Starting" => Phase::Starting,
            "Done" => Phase::Done,
            _ => Phase::Syncing,
        })
    }
}

/// Variant name = the wire tag = the rendered word, one definition (serde derives the
/// same names) — a rename changes what a running driver publishes
impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

/// Progress columns every subject exposes, enough for the subject-agnostic probes
/// (monotonic height, no-stall, reached-target).
///
/// Balances + tree roots are wallet extras; observers report neither, said explicitly by
/// the defaults rather than by a zero a probe would read as passing
pub trait ProgressView: Send + std::fmt::Debug {
    fn height(&self) -> u32;
    fn target(&self) -> Option<u32>;
    /// Fraction complete `0.0..=100.0`, default `height / target`.
    ///
    /// - Override where progress is non-linear in height (a wallet scans tip-first over tree
    ///   shards, so height understates it badly mid-scan)
    /// - Denominator is a per-tick reading → can move **backwards** on a growing chain
    fn pct(&self) -> f32 {
        match self.target() {
            Some(target) if target > 0 => {
                (100.0 * f64::from(self.height()) / f64::from(target)) as f32
            }
            _ => 0.0,
        }
    }
    fn phase(&self) -> Phase;
    /// Subject's own word for its current stage, rendered beside [`phase`](Self::phase)
    /// (`"historic scan"`, `"downloading headers"`). `None` = the lifecycle word alone
    fn detail(&self) -> Option<&'static str> {
        None
    }
    /// Subject's own cumulative protocol work. `None` (default, every observer) → derived
    /// from [`height`](Self::height) via [`ChainWork`](crate::sync::ChainWork), needing
    /// nothing from the component. Override only where height misreports progress
    fn work(&self) -> Option<Work> {
        None
    }
    /// Confirmed per-pool balances. `None` (default) = not a wallet, *not* zero — a probe
    /// reading [`Snapshot::balances`](crate::sync::Snapshot::balances) panics instead of
    /// comparing zeroes that can never fail
    fn balances(&self) -> Option<PoolBalances> {
        None
    }
    /// Note-commitment-tree roots = wallet side of the independent-authority check against
    /// an indexer's `GetTreeState`. [`TreeRoots::UNREPORTED`] (no trees) != per-pool `None`
    fn tree_roots(&self) -> TreeRoots {
        TreeRoots::UNREPORTED
    }
}

/// One sync subject, lifecycle driven by the runner: [`launch`](Self::launch) once,
/// [`progress`](Self::progress) each tick, [`is_complete`](Self::is_complete), then
/// [`stop`](Self::stop) on fatal violation or cancellation.
///
/// - Driving subject: `launch` starts the engine it owns, `stop` checkpoints it
/// - Observing subject (component syncs itself): both no-op, runner is a pure prober
// `Sync` not just `Send`: `progress(&self)` borrows across an await, so `async_trait`
// needs the subject shareable for that future to be `Send`
#[async_trait]
pub trait SyncSubject: Send + Sync {
    /// Start the sync; the runner calls it exactly once
    async fn launch(&mut self) -> Result<(), RpcError>;

    /// Boxed, not an associated type: the profile binds one `dyn SyncSubject`, so ztest
    /// carries no enum of known subjects (allocation is per tick, seconds apart)
    async fn progress(&self) -> Result<Box<dyn ProgressView>, RpcError>;

    async fn is_complete(&self) -> bool;

    /// Series [`progress`](Self::progress) reads `op` from, named by the preflight
    /// diagnostic. `None` = no named source (derived work, or an op never counted here)
    /// Default = engine ticks, the reading every subject provably has. Only the subject may
    /// name an exporter; scraping a neighbour renders its progress under this one's name
    fn observes(&self) -> super::Observed {
        super::Observed::ticks("subject")
    }

    /// Declared rows + one scrape to check them against; `None` = no exporter to check.
    ///
    /// Preflight compares the two, so a family renamed upstream fails by name in seconds
    /// rather than as an em-dash for the length of the run
    async fn declared(&self) -> Option<(&'static [crate::metrics::Row], Exposition)> {
        None
    }

    fn work_source(&self, _op: Op) -> Option<crate::metrics::Family> {
        None
    }

    /// Graceful stop — checkpoint, never a kill; observers have nothing to stop
    async fn stop(&mut self) -> Result<(), RpcError> {
        Ok(())
    }
}
