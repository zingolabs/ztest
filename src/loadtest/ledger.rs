//! Correctness under load, in two tiers.
//!
//! - Consistency (every response, inline): one block's bytes identical wherever and however
//!   often served (heights below `stable_below`; above = reorg window, audited instead)
//! - Authority (sampled, off the load path): the exact bytes a wallet received, decoded and held
//!   to zebra by [`Auditor`](crate::loadtest::audit::Auditor)
//! - Sampling = a hash of the key (reproducible across runs, no RNG state)

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::loadtest::oracle::Violation;
use crate::loadtest::wire::digest;

/// One served answer the auditor holds to zebra
#[derive(Debug)]
pub enum Audit {
    /// `GetBlockRange` default pools, one message
    Shielded {
        height: u32,
        message: Bytes,
    },
    /// `GetBlock` (every pool); `tip` = served tip then (reorg window → judged once buried)
    Full {
        height: u32,
        message: Bytes,
        tip: u32,
    },
    TreeState {
        height: u32,
        message: Bytes,
        tip: u32,
    },
    /// Whole answer to `GetSubtreeRoots(pool, start, max)`, one message per root
    Subtrees {
        pool: &'static str,
        start: u32,
        max: u32,
        messages: Vec<Bytes>,
    },
    /// `GetLatestBlock` answer
    Tip {
        message: Bytes,
    },
    /// One `GetMempoolStream` message
    Mempool {
        message: Bytes,
    },
    LightdInfo {
        message: Bytes,
    },
    AddressUtxos {
        address: String,
        message: Bytes,
        tip: u32,
    },
}

impl Audit {
    pub fn check(&self) -> &'static str {
        match self {
            Audit::Shielded { .. } => "block_shielded",
            Audit::Full { .. } => "block_full",
            Audit::TreeState { .. } => "tree_state",
            Audit::Subtrees { .. } => "subtree_roots",
            Audit::Tip { .. } => "latest_block",
            Audit::Mempool { .. } => "mempool_tx",
            Audit::LightdInfo { .. } => "lightd_info",
            Audit::AddressUtxos { .. } => "address_utxos",
        }
    }
}

/// Totals a stage report carries
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tallies {
    /// Distinct heights whose bytes the consistency tier holds
    pub blocks_held: u64,
    /// Responses matched against an earlier identical one
    pub consistent: u64,
    pub queued: u64,
    /// Audit queue full → answer not audited (coverage, not correctness)
    pub dropped: u64,
    pub audited: u64,
    /// Judged unauditable (a reorged tip answer, zebra refusing): neither pass nor fail
    pub skipped: u64,
    pub violations: u64,
}

/// Shared by every simulated wallet + the auditor
pub struct Ledger {
    stable_below: u32,
    /// `stable_below` entries, digest by height (`0` = unseen)
    blocks: Vec<AtomicU64>,
    /// First-seen digest per keyed answer (tree state by height, roots by request, ...)
    answers: Mutex<HashMap<String, u64>>,
    /// Keys already queued (each distinct answer audited at most once)
    queued_keys: Mutex<HashSet<String>>,
    audit_every: u64,
    /// Taken by [`close`](Self::close): the auditor holds this ledger, so a dropped ledger
    /// could never end its queue
    audits: Mutex<Option<mpsc::Sender<Audit>>>,
    violations: Mutex<Vec<Violation>>,
    consistent: AtomicU64,
    held: AtomicU64,
    queued: AtomicU64,
    dropped: AtomicU64,
    audited: AtomicU64,
    skipped: AtomicU64,
    violated: AtomicU64,
}

impl std::fmt::Debug for Ledger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ledger")
            .field("stable_below", &self.stable_below)
            .field("tallies", &self.tallies())
            .finish_non_exhaustive()
    }
}

/// Violations kept verbatim (the count keeps going; the first few name the defect)
const KEPT: usize = 64;

impl Ledger {
    /// `audit_every` = 1 in N first-seen blocks audited (every other answer kind: all)
    pub fn new(stable_below: u32, audit_every: u64, audits: mpsc::Sender<Audit>) -> Self {
        Self {
            stable_below,
            blocks: (0..stable_below).map(|_| AtomicU64::new(0)).collect(),
            answers: Mutex::new(HashMap::new()),
            queued_keys: Mutex::new(HashSet::new()),
            audit_every: audit_every.max(1),
            audits: Mutex::new(Some(audits)),
            violations: Mutex::new(Vec::new()),
            consistent: AtomicU64::new(0),
            held: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            audited: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            violated: AtomicU64::new(0),
        }
    }

    pub fn stable_below(&self) -> u32 {
        self.stable_below
    }

    /// One default-pools block off a stream: held for consistency, sampled for authority
    pub fn shielded_block(&self, height: u32, message: &Bytes) {
        let Some(slot) = self.blocks.get(height as usize) else {
            return;
        };
        let seen =
            u64::from_le_bytes(digest(message).to_le_bytes()[..8].try_into().expect("8")).max(1);
        match slot.compare_exchange(0, seen, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => {
                self.held.fetch_add(1, Ordering::Relaxed);
                if sampled(u64::from(height), self.audit_every) {
                    self.queue(Audit::Shielded { height, message: message.clone() });
                }
            }
            Err(held) if held == seen => {
                self.consistent.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => self.violated(
                "block_consistent",
                u64::from(height),
                "default-pools bytes differ from this height's first answer".to_owned(),
            ),
        }
    }

    /// A keyed answer: first sight queued for audit, later sights must match it byte-for-byte
    /// while `stable` (an answer the tip can move = queued once per distinct answer instead)
    pub fn answer(
        &self,
        key: String,
        message_digest: u128,
        stable: bool,
        audit: impl FnOnce() -> Audit,
    ) {
        let seen = u64::from_le_bytes(message_digest.to_le_bytes()[..8].try_into().expect("8"));
        let first = {
            let mut answers = self.answers.lock().expect("answers poisoned");
            match answers.get(&key) {
                Some(held) if *held == seen => {
                    self.consistent.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Some(_) if stable => {
                    drop(answers);
                    return self.violated(
                        "answer_consistent",
                        0,
                        format!("{key}: bytes differ from its first answer"),
                    );
                }
                Some(_) => {
                    answers.insert(key.clone(), seen);
                    format!("{key}#{seen:x}")
                }
                None => {
                    answers.insert(key.clone(), seen);
                    key
                }
            }
        };
        if self.queued_keys.lock().expect("queued poisoned").insert(first) {
            self.queue(audit());
        }
    }

    /// Queue without waiting (a full queue = coverage lost, never load slowed)
    pub fn queue(&self, audit: Audit) {
        let audits = self.audits.lock().expect("audit sender poisoned");
        match audits.as_ref().map(|audits| audits.try_send(audit)) {
            Some(Ok(())) => {
                self.queued.fetch_add(1, Ordering::Relaxed);
            }
            Some(Err(_)) | None => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// No more audits: the auditor drains what is queued, then ends
    pub fn close(&self) {
        self.audits.lock().expect("audit sender poisoned").take();
    }

    pub fn violated(&self, check: &'static str, height: u64, detail: String) {
        self.violated.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("ztest_load_violations_total", "check" => check).increment(1);
        let mut kept = self.violations.lock().expect("violations poisoned");
        if kept.len() < KEPT {
            kept.push(Violation { height, field: check.to_owned(), detail });
        }
    }

    pub fn audited(&self, check: &'static str) {
        self.audited.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("ztest_load_verified_total", "check" => check).increment(1);
    }

    pub fn skipped(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn tallies(&self) -> Tallies {
        Tallies {
            blocks_held: self.held.load(Ordering::Relaxed),
            consistent: self.consistent.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            audited: self.audited.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            violations: self.violated.load(Ordering::Relaxed),
        }
    }

    pub fn violations(&self) -> Vec<Violation> {
        self.violations.lock().expect("violations poisoned").clone()
    }
}

/// 1 in `every`, by a hash of `key`
pub fn sampled(key: u64, every: u64) -> bool {
    splitmix64(key).is_multiple_of(every)
}

pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stable height's bytes: first sight held (and sampled for audit), a repeat = consistent,
    /// a differing repeat = a violation; above `stable_below` nothing is held
    #[tokio::test]
    async fn a_block_is_held_on_first_sight_and_any_differing_repeat_is_a_violation() {
        let (audits, mut queued) = mpsc::channel(8);
        let ledger = Ledger::new(100, 1, audits);
        let (block, other) = (Bytes::from_static(b"block-7"), Bytes::from_static(b"block-7'"));

        ledger.shielded_block(7, &block);
        ledger.shielded_block(7, &block);
        ledger.shielded_block(7, &other);
        ledger.shielded_block(150, &other);
        let audit = queued.try_recv().expect("first sight audited (every = 1)");
        assert!(matches!(audit, Audit::Shielded { height: 7, .. }), "{audit:?}");
        assert!(queued.try_recv().is_err(), "a repeat is never re-audited");

        let tallies = ledger.tallies();
        let want = Tallies {
            blocks_held: 1,
            consistent: 1,
            queued: 1,
            violations: 1,
            ..Tallies::default()
        };
        assert_eq!(tallies, want);
        assert_eq!(ledger.violations()[0].field, "block_consistent");

        // tip-movable answer: a change re-queues the new answer, never a violation
        let key = || "tree_state/tip".to_owned();
        let tip = |b: &'static [u8]| Audit::Tip { message: Bytes::from_static(b) };
        ledger.answer(key(), 1, false, || tip(b"a"));
        ledger.answer(key(), 1, false, || tip(b"a"));
        ledger.answer(key(), 2, false, || tip(b"b"));
        assert_eq!((queued.try_recv().is_ok(), queued.try_recv().is_ok()), (true, true));
        ledger.answer("roots/sapling/0/0".to_owned(), 3, true, || tip(b"r"));
        ledger.answer("roots/sapling/0/0".to_owned(), 4, true, || tip(b"r'"));
        assert_eq!(ledger.tallies().violations, 2, "a stable answer may never change");
    }
}
