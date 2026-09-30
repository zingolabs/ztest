//! Authority tier: the exact bytes a simulated wallet received, decoded and held to zebra.
//!
//! - Off the load path: its own task, fed through a bounded queue ([`Ledger::queue`])
//! - Tip-dependent answers wait until `BURIED` deep, then are judged only if zebra still holds
//!   the block the answer names (a reorged answer = skipped, never failed)
//! - Address UTXOs: judged on arrival, only while zebra's tip = the served tip (no history to bury)
//! - Mempool: a served tx must be one zebra's mempool held (bytes identical), checked against a
//!   rolling snapshot refreshed on a miss

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message;
use tokio::sync::mpsc;

use crate::loadtest::ledger::{Audit, Ledger};
use crate::loadtest::reference::{
    self, Fees, ReferenceError, Zebra, block_diff, shielded, tree_state_diff,
};
use crate::loadtest::wire::digest;
use crate::proto::{
    BlockId, CompactBlock, GetAddressUtxosReplyList, LightdInfo, RawTransaction, SubtreeRoot,
    TreeState,
};

/// Blocks above an answer's height before it is judged (a reorg deeper = beyond the test)
const BURIED: u32 = 10;

/// 1 in N audited blocks recomputes every fee from prevouts (the rest take the served fee)
const FEE_CHECK_EVERY: u64 = 8;

/// Zebra tip re-read at most this often
const TIP_REFRESH: Duration = Duration::from_secs(5);

/// What the chain must say, fixed for the run
#[derive(Debug, Clone)]
pub struct Expect {
    /// `LightdInfo.chain_name` (declared by config, never read off zebra)
    pub chain_name: String,
    pub sapling_activation: u32,
}

pub struct Auditor {
    zebra: Arc<Zebra>,
    ledger: Arc<Ledger>,
    expect: Expect,
    tip: (u32, Instant),
    deferred: VecDeque<(u32, Audit)>,
    roots: HashMap<&'static str, Vec<SubtreeRoot>>,
    /// Raw-tx digests zebra's mempool held at any read this run
    mempool: HashSet<u128>,
    audited_blocks: u64,
}

impl std::fmt::Debug for Auditor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Auditor")
            .field("deferred", &self.deferred.len())
            .field("audited_blocks", &self.audited_blocks)
            .finish_non_exhaustive()
    }
}

impl Auditor {
    pub fn new(zebra: Arc<Zebra>, ledger: Arc<Ledger>, expect: Expect) -> Self {
        Self {
            zebra,
            ledger,
            expect,
            tip: (0, Instant::now() - TIP_REFRESH),
            deferred: VecDeque::new(),
            roots: HashMap::new(),
            mempool: HashSet::new(),
            audited_blocks: 0,
        }
    }

    /// Drains `queue` until its senders drop, then judges what was deferred (waiting for burial
    /// up to `settle`)
    pub async fn run(mut self, mut queue: mpsc::Receiver<Audit>, settle: Duration) {
        while let Some(audit) = queue.recv().await {
            self.judge(audit).await;
            self.release_buried().await;
        }
        let deadline = Instant::now() + settle;
        while !self.deferred.is_empty() && Instant::now() < deadline {
            tokio::time::sleep(TIP_REFRESH).await;
            self.release_buried().await;
        }
        for _ in self.deferred.drain(..) {
            self.ledger.skipped();
        }
    }

    async fn zebra_tip(&mut self) -> Option<u32> {
        if self.tip.1.elapsed() >= TIP_REFRESH {
            self.tip = (self.zebra.tip().await.ok()?, Instant::now());
        }
        Some(self.tip.0)
    }

    async fn release_buried(&mut self) {
        let Some(tip) = self.zebra_tip().await else {
            return;
        };
        let mut waiting = VecDeque::new();
        while let Some((height, audit)) = self.deferred.pop_front() {
            if height + BURIED <= tip {
                self.judge_buried(audit).await;
            } else {
                waiting.push_back((height, audit));
            }
        }
        self.deferred = waiting;
    }

    async fn judge(&mut self, audit: Audit) {
        match audit {
            Audit::Full { height, .. } | Audit::TreeState { height, .. } => {
                self.deferred.push_back((height, audit))
            }
            Audit::Tip { ref message } => match BlockId::decode(message.as_ref()) {
                Ok(id) => self.deferred.push_back((id.height as u32, audit)),
                Err(e) => self.failed("latest_block", 0, format!("undecodable BlockID: {e}")),
            },
            other => self.judge_buried(other).await,
        }
    }

    async fn judge_buried(&mut self, audit: Audit) {
        let check = audit.check();
        let verdict = match audit {
            Audit::Shielded { height, message } => self.shielded(height, &message).await,
            Audit::Full { height, message, .. } => self.full(height, &message).await,
            Audit::TreeState { height, message, .. } => self.tree_state(height, &message).await,
            Audit::Subtrees { pool, start, max, messages } => {
                self.subtrees(pool, start, max, &messages).await
            }
            Audit::Tip { message } => self.latest(&message).await,
            Audit::Mempool { message } => self.mempool_tx(&message).await,
            Audit::LightdInfo { message } => self.lightd_info(&message).await,
            Audit::AddressUtxos { address, message, tip } => {
                self.utxos(&address, &message, tip).await
            }
        };
        match verdict {
            Verdict::Holds => self.ledger.audited(check),
            Verdict::Reorged | Verdict::Unjudged => self.ledger.skipped(),
            Verdict::Differs(height, detail) => self.failed(check, height, detail),
        }
    }

    fn failed(&self, check: &'static str, height: u64, detail: String) {
        self.ledger.violated(check, height, detail);
    }

    async fn shielded(&mut self, height: u32, message: &[u8]) -> Verdict {
        let served = match CompactBlock::decode(message) {
            Ok(b) => b,
            Err(e) => return Verdict::Differs(u64::from(height), format!("undecodable: {e}")),
        };
        self.audited_blocks += 1;
        let fees = match self.audited_blocks % FEE_CHECK_EVERY {
            0 => Fees::Checked,
            _ => Fees::AsServed(&served),
        };
        match self.zebra.compact_block(height, fees).await {
            Ok(expected) => differs(u64::from(height), block_diff(&shielded(expected), &served)),
            Err(e) => unjudged(e),
        }
    }

    async fn full(&mut self, height: u32, message: &[u8]) -> Verdict {
        let served = match CompactBlock::decode(message) {
            Ok(b) => b,
            Err(e) => return Verdict::Differs(u64::from(height), format!("undecodable: {e}")),
        };
        match self.zebra.block_hash(height).await {
            Ok(hash) if hash.as_slice() != served.hash => return Verdict::Reorged,
            Ok(_) => {}
            Err(e) => return unjudged(e),
        }
        match self.zebra.compact_block(height, Fees::AsServed(&served)).await {
            Ok(expected) => differs(u64::from(height), block_diff(&expected, &served)),
            Err(e) => unjudged(e),
        }
    }

    async fn tree_state(&mut self, height: u32, message: &[u8]) -> Verdict {
        let served = match TreeState::decode(message) {
            Ok(s) => s,
            Err(e) => return Verdict::Differs(u64::from(height), format!("undecodable: {e}")),
        };
        match self.zebra.tree_state(height).await {
            Ok(expected) if expected.hash != served.hash => Verdict::Reorged,
            Ok(expected) => differs(u64::from(height), tree_state_diff(&expected, &served)),
            Err(e) => unjudged(e),
        }
    }

    async fn subtrees(
        &mut self,
        pool: &'static str,
        start: u32,
        max: u32,
        messages: &[Bytes],
    ) -> Verdict {
        let mut served = Vec::with_capacity(messages.len());
        for message in messages {
            match SubtreeRoot::decode(message.as_ref()) {
                Ok(root) => served.push(root),
                Err(e) => return Verdict::Differs(0, format!("{pool} undecodable root: {e}")),
            }
        }
        // listing re-read whenever the answer reaches past it (a subtree completed since)
        let listed = self.roots.get(pool).map(Vec::len);
        if listed.is_none_or(|listed| listed < start as usize + served.len()) {
            match self.zebra.subtree_roots(pool).await {
                Ok(roots) => {
                    self.roots.insert(pool, roots);
                }
                Err(e) => return unjudged(e),
            }
        }
        let every = &self.roots[pool];
        let end = match max {
            0 => every.len(),
            max => every.len().min(start as usize + max as usize),
        };
        let expected = every.get(start as usize..end).unwrap_or(&[]);
        match served.iter().zip(expected).position(|(s, e)| s != e) {
            Some(at) => Verdict::Differs(
                expected[at].completing_block_height,
                format!(
                    "{pool} root {}: served {:?}, zebra {:?}",
                    start as usize + at,
                    served[at],
                    expected[at]
                ),
            ),
            None if served.len() == expected.len() => Verdict::Holds,
            // zaino a block behind the one completing zebra's newest subtree
            None if served.len() < expected.len() => Verdict::Unjudged,
            None => Verdict::Differs(
                0,
                format!(
                    "{pool} from {start} max {max}: served {} roots, zebra {}",
                    served.len(),
                    expected.len()
                ),
            ),
        }
    }

    async fn latest(&mut self, message: &[u8]) -> Verdict {
        let served = match BlockId::decode(message) {
            Ok(id) => id,
            Err(e) => return Verdict::Differs(0, format!("undecodable: {e}")),
        };
        match self.zebra.block_hash(served.height as u32).await {
            Ok(hash) if hash.as_slice() == served.hash => Verdict::Holds,
            // zaino's tip then ≠ zebra's block there now = a reorg between (judged as one)
            Ok(_) => Verdict::Reorged,
            Err(e) => unjudged(e),
        }
    }

    /// A served mempool tx = bytes zebra's mempool held at some point this run
    async fn mempool_tx(&mut self, message: &[u8]) -> Verdict {
        let served = match RawTransaction::decode(message) {
            Ok(tx) => tx,
            Err(e) => return Verdict::Differs(0, format!("undecodable: {e}")),
        };
        if served.height != 0 {
            return Verdict::Differs(0, format!("mempool tx served at height {}", served.height));
        }
        let wanted = digest(&served.data);
        if self.mempool.contains(&wanted) {
            return Verdict::Holds;
        }
        let listed = match self.zebra.mempool().await {
            Ok(listed) => listed,
            Err(e) => return unjudged(e),
        };
        for txid in listed {
            if let Ok(raw) = self.zebra.raw_transaction(&txid).await {
                self.mempool.insert(digest(&raw));
            }
        }
        match self.mempool.contains(&wanted) {
            true => Verdict::Holds,
            // mined between the stream and this read = zebra's bytes by txid are unreachable here
            false => Verdict::Unjudged,
        }
    }

    async fn lightd_info(&mut self, message: &[u8]) -> Verdict {
        let served = match LightdInfo::decode(message) {
            Ok(info) => info,
            Err(e) => return Verdict::Differs(0, format!("undecodable: {e}")),
        };
        let tip = match self.zebra_tip().await {
            Some(tip) => tip,
            None => return Verdict::Unjudged,
        };
        let pairs = [
            ("chain_name", served.chain_name.clone(), self.expect.chain_name.clone()),
            (
                "sapling_activation_height",
                served.sapling_activation_height.to_string(),
                self.expect.sapling_activation.to_string(),
            ),
        ];
        if let Some((field, s, e)) = pairs.into_iter().find(|(_, s, e)| s != e) {
            return Verdict::Differs(0, format!("{field}: served {s}, expected {e}"));
        }
        // served tip trails zebra's by the blocks mined since (≤ BURIED)
        let served_tip = served.block_height as u32;
        match served_tip <= tip + BURIED && served_tip + BURIED >= tip {
            true => Verdict::Holds,
            false => Verdict::Differs(
                u64::from(served_tip),
                format!("block_height {served_tip}, zebra {tip}"),
            ),
        }
    }

    /// UTXOs = the address at the current tip → comparable only while zebra sits at `served_tip`
    /// (zaino's tip when it answered) across the whole read
    async fn utxos(&mut self, address: &str, message: &[u8], served_tip: u32) -> Verdict {
        let served = match GetAddressUtxosReplyList::decode(message) {
            Ok(list) => list.address_utxos,
            Err(e) => return Verdict::Differs(0, format!("undecodable: {e}")),
        };
        let before = self.zebra.tip().await;
        let expected = match self.zebra.address_utxos(address).await {
            Ok(utxos) => utxos,
            Err(e) => return unjudged(e),
        };
        if served == expected {
            return Verdict::Holds;
        }
        match (before, self.zebra.tip().await) {
            (Ok(a), Ok(b)) if a == served_tip && b == served_tip => Verdict::Differs(
                u64::from(served_tip),
                format!(
                    "{address}: {} served vs {} at zebra; first differing utxo {:?}",
                    served.len(),
                    expected.len(),
                    served.iter().zip(&expected).find(|(s, e)| s != e)
                ),
            ),
            _ => Verdict::Unjudged,
        }
    }
}

enum Verdict {
    Holds,
    /// Zebra no longer holds the block the answer names
    Reorged,
    /// Zebra could not answer, or the chain moved between the two reads
    Unjudged,
    Differs(u64, String),
}

fn differs(height: u64, diff: Option<String>) -> Verdict {
    match diff {
        None => Verdict::Holds,
        Some(detail) => Verdict::Differs(height, detail),
    }
}

fn unjudged(error: ReferenceError) -> Verdict {
    tracing::debug!(%error, "audit unjudged");
    Verdict::Unjudged
}

/// Calibration's own checks (the ledger's totals also take the auditor's, running beside it)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Calibration {
    pub audited: u64,
    pub violations: u64,
}

/// Calibration: `heights` served one at a time on a quiet server, every fee recomputed,
/// both shapes (`GetBlock` + `GetBlockRange`) held to zebra
pub async fn calibrate(
    zebra: &Zebra,
    client: &crate::loadtest::wire::RawClient,
    heights: &[u32],
    ledger: &Ledger,
) -> Calibration {
    use crate::loadtest::wire::{encoded, path};
    use crate::proto::BlockRange;

    let mut tally = Calibration::default();
    for &height in heights {
        let id = BlockId { height: u64::from(height), hash: Vec::new() };
        let range =
            BlockRange { start: Some(id.clone()), end: Some(id.clone()), pool_types: Vec::new() };
        let full = client.unary(path::GET_BLOCK, encoded(&id)).await;
        let shielded_answer = async {
            let mut stream = client.stream(path::GET_BLOCK_RANGE, encoded(&range)).await?;
            stream.message().await
        }
        .await;
        let (full, shielded_answer) = match (full, shielded_answer) {
            (Ok(full), Ok(Some(one))) => (full, one),
            (full, one) => {
                tally.violations += 1;
                ledger.violated(
                    "calibration",
                    u64::from(height),
                    format!(
                        "zaino refused: GetBlock {:?}, GetBlockRange {:?}",
                        full.err(),
                        one.map(|o| o.is_some())
                    ),
                );
                continue;
            }
        };
        let (Ok(full_block), Ok(shielded_block)) =
            (CompactBlock::decode(full.as_ref()), CompactBlock::decode(shielded_answer.as_ref()))
        else {
            tally.violations += 1;
            ledger.violated("calibration", u64::from(height), "undecodable CompactBlock".into());
            continue;
        };
        let expected = match zebra.compact_block(height, Fees::Checked).await {
            Ok(expected) => expected,
            Err(error) => {
                tally.violations += 1;
                ledger.violated("calibration", u64::from(height), format!("zebra: {error}"));
                continue;
            }
        };
        let diffs = [
            ("block_full", block_diff(&expected, &full_block)),
            ("block_shielded", block_diff(&reference::shielded(expected), &shielded_block)),
        ];
        for (check, diff) in diffs {
            match diff {
                None => {
                    tally.audited += 1;
                    ledger.audited(check);
                }
                Some(detail) => {
                    tally.violations += 1;
                    ledger.violated(check, u64::from(height), detail);
                }
            }
        }
        ledger.shielded_block(height, &shielded_answer);
    }
    tally
}
