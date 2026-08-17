//! Sync-subject abstraction = what varies between wallet, indexer, validator (design
//! §"One harness, three subjects").
//!
//! - `SyncRunner<S>` generic over it; subject launches or merely observes its own sync
//! - Yields a raw per-tick [`ProgressView`], folded into a [`Snapshot`](crate::sync::Snapshot)

use async_trait::async_trait;

use crate::RpcError;
use crate::handles::wallet::PoolBalances;

use super::tree::TreeRoots;
use super::work::{Op, Work};

/// Live sync phase, surfaced in `watch` and readable by probes.
///
/// - Wallet: from pepper-sync `ScanPriority` (`Historic` ← `{Historic, OpenAdjacent,
///   Scanning}`, `Finalizing` ← `{RefetchingNullifiers, ScannedWithoutMapping}`, rest by name)
/// - Validator: from the headers-vs-blocks download split
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Starting,
    Verifying,
    ChainTip,
    FoundNote,
    Historic,
    Finalizing,
    Downloading,
    Done,
}

impl Phase {
    /// Display/wire tag. Explicit, not `Debug` (crosses the driver→controller stream, where
    /// a rename would silently change what a running sync publishes)
    pub fn as_str(&self) -> &'static str {
        match self {
            Phase::Starting => "Starting",
            Phase::Verifying => "Verifying",
            Phase::ChainTip => "ChainTip",
            Phase::FoundNote => "FoundNote",
            Phase::Historic => "Historic",
            Phase::Finalizing => "Finalizing",
            Phase::Downloading => "Downloading",
            Phase::Done => "Done",
        }
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
/// Wallet: ztest owns the engine (launch spawns `pepper_sync::sync`). Self-syncing
/// indexer/validator: `launch`/`stop` no-op, runner is a pure observer
// `Sync` not just `Send`: `progress(&self)` borrows across an await, so `async_trait`
// needs the subject shareable for that future to be `Send`
#[async_trait]
pub trait SyncSubject: Send + Sync {
    type Progress: ProgressView;

    /// Start the sync; the runner calls it exactly once
    async fn launch(&mut self) -> Result<(), RpcError>;

    async fn progress(&self) -> Result<Self::Progress, RpcError>;

    async fn is_complete(&self) -> bool;

    /// Series [`progress`](Self::progress) reads `op` from, named by the preflight
    /// diagnostic. `None` = no named source (derived work, or an op never counted here)
    fn work_source(&self, _op: Op) -> Option<&'static str> {
        None
    }

    /// Graceful stop (wallet: `sync_mode = Shutdown`); observers have nothing to stop
    async fn stop(&mut self) -> Result<(), RpcError> {
        Ok(())
    }
}
