//! The sync subject abstraction: what varies between a wallet, indexer, or
//! validator sync (design §"One harness, three subjects"). `SyncRunner<S>` is
//! generic over this; a subject launches or merely observes its own sync and
//! yields a raw per-tick [`ProgressView`], which the runner folds into an
//! immutable [`Snapshot`](crate::sync::Snapshot).

use async_trait::async_trait;

use crate::RpcError;
use crate::handles::wallet::PoolBalances;

use super::tree::TreeRoots;
use super::work::Work;

/// The live phase of a sync, surfaced in `watch` and observable by probes. For
/// a wallet it derives from pepper-sync's `ScanPriority`; for a validator from
/// the headers-vs-blocks download split.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Sync has not produced a first reading yet.
    Starting,
    /// Re-verifying a previously scanned range still on the main chain
    /// (`ScanPriority::Verify`) — the reorg-verification phase.
    Verifying,
    /// Completing the note-commitment tree shard at the chain tip
    /// (`ScanPriority::ChainTip`).
    ChainTip,
    /// Completing tree shards adjacent to a found note (`ScanPriority::FoundNote`).
    FoundNote,
    /// Bulk historic scan (`ScanPriority::{Historic, OpenAdjacent, Scanning}`).
    Historic,
    /// Re-fetching nullifiers for final spend detection
    /// (`ScanPriority::{RefetchingNullifiers, ScannedWithoutMapping}`).
    Finalizing,
    /// A self-syncing subject downloading headers/blocks from peers.
    Downloading,
    /// Fully scanned / at tip.
    Done,
}

impl Phase {
    /// The display/wire tag. Explicit rather than `Debug`, because it crosses the
    /// driver→controller event stream, where a rename would silently change what
    /// a running sync publishes.
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

/// The common progress columns every subject exposes — enough for the
/// subject-agnostic probes (monotonic height, no-stall, reached-target).
/// Balances and note-commitment-tree roots are wallet extras: an observer
/// subject (an indexer or validator being watched) has no wallet and reports
/// neither, which the defaults below say explicitly rather than by returning a
/// zero that a probe would read as a passing observation.
pub trait ProgressView: Send + std::fmt::Debug {
    /// Highest height this subject has synced/scanned through.
    fn height(&self) -> u32;
    /// The chain tip this sync targets, if known.
    fn target(&self) -> Option<u32>;
    /// Fraction complete in `0.0..=100.0`.
    ///
    /// Defaults to `height / target`, which is the honest answer for any
    /// subject that advances linearly through the chain — an observer has
    /// nothing to add to that arithmetic and should not restate it.
    ///
    /// Override where progress is *not* linear in height: a wallet scans
    /// chain-tip-first over tree shards, so its true fraction is
    /// `percentage_total_outputs_scanned` and its height understates it badly
    /// mid-scan.
    ///
    /// Note the denominator is a per-tick reading, not a constant. On a chain
    /// still producing blocks this can move **backwards** — the subject gained
    /// ground while the tip gained more. That is a true reading, and nothing
    /// downstream may assume it rises monotonically.
    fn pct(&self) -> f32 {
        match self.target() {
            Some(target) if target > 0 => {
                (100.0 * f64::from(self.height()) / f64::from(target)) as f32
            }
            _ => 0.0,
        }
    }
    /// The live phase.
    fn phase(&self) -> Phase;
    /// The subject's own count of cumulative protocol work completed, when it
    /// has a truer one than the chain-derived index.
    ///
    /// `None` — the default, and what every observer subject uses — means the
    /// harness derives work from [`height`](Self::height) via
    /// [`ChainWork`](crate::sync::ChainWork). That path needs nothing from the
    /// component beyond a height, which is what keeps the metric available for
    /// a Go lightwalletd or a C++ zcashd on the same terms as a Rust zaino.
    ///
    /// Override only where height genuinely misreports progress: a wallet
    /// scans non-linearly (chain-tip-first, shard-based), so its height is not
    /// its progress and its own counters are the honest source.
    fn work(&self) -> Option<Work> {
        None
    }
    /// The subject's confirmed per-pool balances, for subjects that hold funds.
    ///
    /// `None` — the default — means this subject is not a wallet and has no
    /// balance to report. It is *not* "zero": a probe reading it through
    /// [`Snapshot::balances`](crate::sync::Snapshot::balances) panics rather
    /// than comparing zeroes that can never fail.
    fn balances(&self) -> Option<PoolBalances> {
        None
    }
    /// The subject's note-commitment-tree roots at this tick — the wallet side
    /// of the independent-authority check against an indexer's `GetTreeState`.
    ///
    /// Defaults to [`TreeRoots::UNREPORTED`] for every subject that maintains
    /// no trees. A wallet returns [`TreeRoots::reported`], whose per-pool
    /// `None` then carries real information (the pool is empty at this height,
    /// or its shard tree is still incomplete mid-scan).
    fn tree_roots(&self) -> TreeRoots {
        TreeRoots::UNREPORTED
    }
}

/// One sync subject. The runner drives its lifecycle: [`launch`](Self::launch)
/// once, [`progress`](Self::progress) each tick, [`is_complete`](Self::is_complete)
/// as the completion predicate, [`stop`](Self::stop) on a fatal violation or
/// cancellation. For a wallet ztest owns the engine (launch spawns
/// `pepper_sync::sync`); for a self-syncing indexer/validator `launch`/`stop`
/// are no-ops and the runner is a pure observer.
// `Sync` (not just `Send`): `progress(&self)` borrows `&self` across an await,
// so `async_trait` requires the subject be shareable for that future to be
// `Send`. Every subject already is (the wallet is driven through a shared
// `Arc<RwLock<LightWallet>>`).
#[async_trait]
pub trait SyncSubject: Send + Sync {
    /// The per-tick reading this subject produces.
    type Progress: ProgressView;

    /// Start the sync. Idempotent-safe: the runner calls it exactly once.
    async fn launch(&mut self) -> Result<(), RpcError>;

    /// Capture the current raw progress.
    async fn progress(&self) -> Result<Self::Progress, RpcError>;

    /// Whether the sync has finished (reached tip / the engine task returned).
    async fn is_complete(&self) -> bool;

    /// Stop the sync gracefully (wallet: `sync_mode = Shutdown`). Observers
    /// have nothing to stop.
    async fn stop(&mut self) -> Result<(), RpcError> {
        Ok(())
    }
}
