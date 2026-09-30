//! Simulated light wallets, shaped on the two real sync engines (zaino
//! `docs/notes/lightwallet-serving-audit.md` §1.2, read at source).
//!
//! - [`steady`] = pepper-sync at the tip: permanent mempool stream, `GetLatestBlock` ≤ 10 s,
//!   and per block the burst (tip, tree state, 3 × 2 subtree-root passes, a 10-block verify
//!   range, a `GetBlock` reorg check); a mobile fraction polls `GetLightdInfo` every 5 s
//! - [`restore`] = librustzcash's loop from a birthday: tree state + roots + utxos, then
//!   1000-block batches, a `GetTreeState(start − 1)` before each, optionally paced
//! - One H2 connection per wallet; every response checked inline ([`Ledger`]) and every block's
//!   height / link / completeness asserted as it streams

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message;
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};

use crate::loadtest::ledger::{Audit, Ledger, sampled, splitmix64};
use crate::loadtest::measure::{Measure, Method};
use crate::loadtest::wire::{RawClient, block_head, digest, encoded, path};
use crate::proto::{
    BlockId, BlockRange, ChainSpec, Empty, GetAddressUtxosArg, GetSubtreeRootsArg, ShieldedProtocol,
};

/// Blocks a synced wallet re-verifies below the tip each block (pepper-sync's verify range)
const VERIFY_BLOCKS: u64 = 10;

/// `GetLatestBlock` floor between blocks (pepper-sync `CHECK_NEW_BLOCKS_INTERVAL`)
const TIP_POLL: Duration = Duration::from_secs(10);

/// zingo-mobile's `GetLightdInfo` cadence while the app is open
const LIGHTD_POLL: Duration = Duration::from_secs(5);

/// 1 in N `GetLightdInfo` answers audited (all identical within a block)
const LIGHTD_AUDIT_EVERY: u64 = 64;

/// Retry hint zaino sends with an admission refusal (`grpc-retry-pushback-ms`)
const PUSHBACK: Duration = Duration::from_millis(250);

const POOLS: [(&str, ShieldedProtocol); 3] = [
    ("sapling", ShieldedProtocol::Sapling),
    ("orchard", ShieldedProtocol::Orchard),
    ("ironwood", ShieldedProtocol::Ironwood),
];

/// What every session shares
#[derive(Debug, Clone)]
pub struct Cx {
    pub uri: Arc<str>,
    pub measure: Arc<Measure>,
    pub ledger: Arc<Ledger>,
    pub stop: CancellationToken,
    /// Concurrent dials (a thundering connect storm measures the accept path, not the load)
    pub dialing: Arc<Semaphore>,
    /// Sessions holding a connection now
    pub connected: Arc<AtomicUsize>,
}

impl Cx {
    async fn dial(&self) -> Option<RawClient> {
        let _permit = self.dialing.acquire().await.ok()?;
        let started = Instant::now();
        match RawClient::connect(&self.uri).await {
            Ok(client) => {
                self.measure.request(Method::Connect, started.elapsed(), 0, Code::Ok);
                Some(client)
            }
            Err(error) => {
                tracing::debug!(%error, "wallet dial failed");
                self.measure.request(Method::Connect, started.elapsed(), 0, Code::Unavailable);
                None
            }
        }
    }

    /// One unary request, timed and tallied
    async fn unary(
        &self,
        client: &RawClient,
        method: Method,
        path: &'static str,
        request: Bytes,
    ) -> Option<Bytes> {
        let started = Instant::now();
        let answer = client.unary(path, request).await;
        self.tally(method, started, answer.as_ref().map_or(0, Bytes::len), answer.as_ref().err());
        answer.ok()
    }

    /// One server stream drained into its messages (roots, utxos: small, whole-answer audits)
    async fn collected(
        &self,
        client: &RawClient,
        method: Method,
        path: &'static str,
        request: Bytes,
    ) -> Option<Vec<Bytes>> {
        let started = Instant::now();
        let drained = async {
            let mut stream = client.stream(path, request).await?;
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok::<_, Status>(messages)
        }
        .await;
        let bytes = drained.as_ref().map_or(0, |m| m.iter().map(Bytes::len).sum());
        self.tally(method, started, bytes, drained.as_ref().err());
        drained.ok()
    }

    fn tally(&self, method: Method, started: Instant, bytes: usize, failure: Option<&Status>) {
        let code = failure.map_or(Code::Ok, Status::code);
        self.measure.request(method, started.elapsed(), bytes as u64, code);
    }

    /// Backs off on an admission refusal as a real client would (its pushback)
    async fn back_off(&self) {
        tokio::select! {
            _ = self.stop.cancelled() => {}
            _ = tokio::time::sleep(PUSHBACK) => {}
        }
    }
}

/// One block announcement every steady wallet reacts to at once
#[derive(Debug)]
pub struct Burst {
    started: Instant,
    pending: AtomicUsize,
}

impl Burst {
    pub fn new(wallets: usize) -> Self {
        Self { started: Instant::now(), pending: AtomicUsize::new(wallets) }
    }

    pub fn drained(&self) -> bool {
        self.pending.load(Ordering::Relaxed) == 0
    }

    fn done(&self, measure: &Measure) {
        if self.pending.fetch_sub(1, Ordering::Relaxed) == 1 {
            measure.burst(self.started.elapsed());
        }
    }
}

/// Acks the burst a wallet took, however the wallet leaves it
struct Taken<'a> {
    burst: Option<Arc<Burst>>,
    measure: &'a Measure,
}

impl Drop for Taken<'_> {
    fn drop(&mut self) {
        if let Some(burst) = self.burst.take() {
            burst.done(self.measure);
        }
    }
}

/// A synced wallet's tip state
#[derive(Debug, Default)]
struct Synced {
    tip: u64,
    /// Completed subtrees seen per pool (`POOLS` order)
    roots: [u32; 3],
}

/// pepper-sync, synced, app open (`mobile` = zingo-mobile's `GetLightdInfo` poll too)
pub async fn steady(
    cx: Cx,
    id: u64,
    mobile: bool,
    mut bursts: watch::Receiver<Option<Arc<Burst>>>,
) {
    let Some(client) = cx.dial().await else {
        return;
    };
    cx.connected.fetch_add(1, Ordering::Relaxed);
    let mempool = tokio::spawn(mempool(cx.clone(), client.clone()));
    let lightd = mobile.then(|| tokio::spawn(lightd(cx.clone(), client.clone(), id)));
    bursts.borrow_and_update();

    // polls spread across the interval (N wallets = N/10 req/s, not N every 10 s)
    let offset = Duration::from_millis(splitmix64(id) % TIP_POLL.as_millis() as u64);
    let mut poll = tokio::time::interval_at(tokio::time::Instant::now() + offset, TIP_POLL);
    let mut synced = Synced::default();
    loop {
        tokio::select! {
            _ = cx.stop.cancelled() => break,
            changed = bursts.changed() => {
                if changed.is_err() {
                    break;
                }
                let taken = Taken { burst: bursts.borrow_and_update().clone(), measure: &cx.measure };
                if taken.burst.is_some() {
                    burst(&cx, &client, &mut synced).await;
                }
            }
            _ = poll.tick() => {
                if cx.unary(&client, Method::GetLatestBlock, path::GET_LATEST_BLOCK, encoded(&ChainSpec {})).await.is_none() {
                    cx.back_off().await;
                }
            }
        }
    }
    mempool.abort();
    if let Some(lightd) = lightd {
        lightd.abort();
    }
    cx.connected.fetch_sub(1, Ordering::Relaxed);
}

/// One block's worth of a synced wallet's requests, in pepper-sync's order
async fn burst(cx: &Cx, client: &RawClient, synced: &mut Synced) {
    let Some(latest) = cx
        .unary(client, Method::GetLatestBlock, path::GET_LATEST_BLOCK, encoded(&ChainSpec {}))
        .await
    else {
        return;
    };
    let Ok(tip) = BlockId::decode(latest.as_ref()) else {
        return cx.ledger.violated("latest_block", 0, "undecodable BlockID".into());
    };
    cx.ledger.answer(format!("tip/{}", tip.height), digest(&latest), false, || Audit::Tip {
        message: latest.clone(),
    });
    let height = tip.height as u32;

    let id = BlockId { height: tip.height, hash: Vec::new() };
    if let Some(state) =
        cx.unary(client, Method::GetTreeState, path::GET_TREE_STATE, encoded(&id)).await
    {
        let stable = height < cx.ledger.stable_below();
        cx.ledger.answer(format!("tree_state/{height}"), digest(&state), stable, || {
            Audit::TreeState { height, message: state.clone(), tip: height }
        });
    }

    for (at, (pool, protocol)) in POOLS.into_iter().enumerate() {
        // an unbounded ask from what it holds, then the confirming empty pass
        for _ in 0..2 {
            let start = synced.roots[at];
            let ask = GetSubtreeRootsArg {
                start_index: start,
                shielded_protocol: protocol as i32,
                max_entries: 0,
            };
            let Some(roots) = cx
                .collected(client, Method::GetSubtreeRoots, path::GET_SUBTREE_ROOTS, encoded(&ask))
                .await
            else {
                break;
            };
            let whole: Vec<u8> = roots.iter().flat_map(|r| r.iter().copied()).collect();
            cx.ledger.answer(format!("roots/{pool}/{start}"), digest(&whole), false, || {
                Audit::Subtrees { pool, start, max: 0, messages: roots.clone() }
            });
            synced.roots[at] += roots.len() as u32;
        }
    }

    let from = tip.height.saturating_sub(VERIFY_BLOCKS - 1).max(synced.tip + 1).min(tip.height);
    stream_blocks(cx, client, from, tip.height, None, None).await;
    synced.tip = tip.height;

    if let Some(block) = cx.unary(client, Method::GetBlock, path::GET_BLOCK, encoded(&id)).await {
        match block_head(&block) {
            Some(head) if head.height == tip.height => cx.ledger.answer(
                format!("block/{height}/{}", hex::encode(head.hash)),
                digest(&block),
                false,
                || Audit::Full { height, message: block.clone(), tip: height },
            ),
            head => cx.ledger.violated(
                "block_full",
                tip.height,
                format!("GetBlock({height}) answered {head:?}"),
            ),
        }
    }
}

/// `GetMempoolStream`, held and re-subscribed as soon as it ends (a block ends it)
async fn mempool(cx: Cx, client: RawClient) {
    while !cx.stop.is_cancelled() {
        let started = Instant::now();
        let mut stream = match client.stream(path::GET_MEMPOOL_STREAM, encoded(&Empty {})).await {
            Ok(stream) => stream,
            Err(status) => {
                cx.tally(Method::GetMempoolStream, started, 0, Some(&status));
                cx.back_off().await;
                continue;
            }
        };
        cx.tally(Method::GetMempoolStream, started, 0, None);
        while let Ok(Some(tx)) = stream.message().await {
            cx.measure.streamed(Method::GetMempoolStream, tx.len() as u64);
            let key = digest(&tx);
            cx.ledger.answer(format!("mempool/{key:x}"), key, true, || Audit::Mempool {
                message: tx.clone(),
            });
        }
    }
}

/// zingo-mobile's `GetLightdInfo` poll
async fn lightd(cx: Cx, client: RawClient, id: u64) {
    let offset = Duration::from_millis(splitmix64(id ^ 0x11) % LIGHTD_POLL.as_millis() as u64);
    let mut poll = tokio::time::interval_at(tokio::time::Instant::now() + offset, LIGHTD_POLL);
    let mut asked = id;
    loop {
        poll.tick().await;
        let Some(info) = cx
            .unary(&client, Method::GetLightdInfo, path::GET_LIGHTD_INFO, encoded(&Empty {}))
            .await
        else {
            continue;
        };
        asked += 1;
        if sampled(asked, LIGHTD_AUDIT_EVERY) {
            cx.ledger.queue(Audit::LightdInfo { message: info });
        }
    }
}

/// A restoring wallet's shape
#[derive(Debug, Clone)]
pub struct RestoreShape {
    /// Birthdays drawn from `[lowest, highest − span]`
    pub lowest: u32,
    pub highest: u32,
    pub span: u32,
    pub batch: u32,
    /// Client scan rate, bytes/s (`None` = as fast as the server delivers)
    pub pace: Option<u64>,
    /// Real t-addresses; each restore asks `GetAddressUtxos` for two of them
    pub addresses: Arc<Vec<String>>,
}

/// librustzcash's sync loop, from one birthday after another until stopped
pub async fn restore(cx: Cx, id: u64, shape: RestoreShape) {
    let Some(client) = cx.dial().await else {
        return;
    };
    cx.connected.fetch_add(1, Ordering::Relaxed);
    let mut pace = shape.pace.map(Pace::new);
    let room = shape.highest.saturating_sub(shape.lowest).saturating_sub(shape.span).max(1);
    for round in 0u64.. {
        if cx.stop.is_cancelled() {
            break;
        }
        let birthday = shape.lowest + (splitmix64((id << 32) | round) % u64::from(room)) as u32;
        let end = (birthday + shape.span).min(shape.highest);

        let latest = cx
            .unary(&client, Method::GetLatestBlock, path::GET_LATEST_BLOCK, encoded(&ChainSpec {}))
            .await;
        let tip =
            latest.and_then(|l| BlockId::decode(l.as_ref()).ok()).map_or(0, |id| id.height as u32);
        for (pool, protocol) in POOLS {
            let ask = GetSubtreeRootsArg {
                start_index: 0,
                shielded_protocol: protocol as i32,
                max_entries: 0,
            };
            if let Some(roots) = cx
                .collected(&client, Method::GetSubtreeRoots, path::GET_SUBTREE_ROOTS, encoded(&ask))
                .await
            {
                let whole: Vec<u8> = roots.iter().flat_map(|r| r.iter().copied()).collect();
                cx.ledger.answer(format!("roots/{pool}/0"), digest(&whole), false, || {
                    Audit::Subtrees { pool, start: 0, max: 0, messages: roots.clone() }
                });
            }
        }
        utxos(&cx, &client, &shape.addresses, id, round, tip).await;

        let mut link = None;
        let mut start = birthday;
        while start < end && !cx.stop.is_cancelled() {
            let last = (start + shape.batch - 1).min(end - 1);
            tree_state(&cx, &client, start.saturating_sub(1)).await;
            match stream_blocks(
                &cx,
                &client,
                u64::from(start),
                u64::from(last),
                link,
                pace.as_mut(),
            )
            .await
            {
                Some(hash) => link = Some(hash),
                None => break,
            }
            start = last + 1;
        }
    }
    cx.connected.fetch_sub(1, Ordering::Relaxed);
}

async fn tree_state(cx: &Cx, client: &RawClient, height: u32) {
    let id = BlockId { height: u64::from(height), hash: Vec::new() };
    if let Some(state) =
        cx.unary(client, Method::GetTreeState, path::GET_TREE_STATE, encoded(&id)).await
    {
        let stable = height < cx.ledger.stable_below();
        cx.ledger.answer(format!("tree_state/{height}"), digest(&state), stable, || {
            Audit::TreeState { height, message: state.clone(), tip: height }
        });
    }
}

/// `GetAddressUtxos` for two of the known addresses (librustzcash, once per iteration)
async fn utxos(cx: &Cx, client: &RawClient, addresses: &[String], id: u64, round: u64, tip: u32) {
    if addresses.is_empty() {
        return;
    }
    for pick in 0..2u64 {
        let address =
            &addresses[(splitmix64(id ^ (round << 8) ^ pick) % addresses.len() as u64) as usize];
        let ask = GetAddressUtxosArg {
            addresses: vec![address.clone()],
            start_height: 0,
            max_entries: 0,
        };
        if let Some(list) =
            cx.unary(client, Method::GetAddressUtxos, path::GET_ADDRESS_UTXOS, encoded(&ask)).await
        {
            cx.ledger.answer(format!("utxos/{address}/{tip}"), digest(&list), false, || {
                Audit::AddressUtxos { address: address.clone(), message: list.clone(), tip }
            });
        }
    }
}

/// Client scan rate: after each message, wait until its bytes' share of the second has passed
#[derive(Debug)]
struct Pace {
    rate: f64,
    next: tokio::time::Instant,
}

impl Pace {
    fn new(bytes_per_second: u64) -> Self {
        Self { rate: bytes_per_second as f64, next: tokio::time::Instant::now() }
    }

    async fn consumed(&mut self, bytes: usize) {
        let now = tokio::time::Instant::now();
        self.next = self.next.max(now) + Duration::from_secs_f64(bytes as f64 / self.rate);
        if self.next > now {
            tokio::time::sleep_until(self.next).await;
        }
    }
}

/// `GetBlockRange(from ..= to)`, default pools, every block checked as it arrives
///
/// - heights contiguous from `from`, each `prev_hash` = the previous block's hash (`link` = the
///   block before `from`, when the caller streamed it), exactly `to − from + 1` blocks
/// - returns the last block's hash (`None` = refused, cut short, or violated)
async fn stream_blocks(
    cx: &Cx,
    client: &RawClient,
    from: u64,
    to: u64,
    mut link: Option<[u8; 32]>,
    mut pace: Option<&mut Pace>,
) -> Option<[u8; 32]> {
    let range = BlockRange {
        start: Some(BlockId { height: from, hash: Vec::new() }),
        end: Some(BlockId { height: to, hash: Vec::new() }),
        pool_types: Vec::new(),
    };
    let started = Instant::now();
    let mut stream = match client.stream(path::GET_BLOCK_RANGE, encoded(&range)).await {
        Ok(stream) => stream,
        Err(status) => {
            cx.tally(Method::GetBlockRange, started, 0, Some(&status));
            if status.code() == Code::Unavailable {
                cx.back_off().await;
            }
            return None;
        }
    };
    let mut next = from;
    loop {
        let message = match stream.message().await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(status) => {
                cx.tally(Method::GetBlockRange, started, 0, Some(&status));
                return None;
            }
        };
        cx.measure.streamed(Method::GetBlockRange, message.len() as u64);
        let Some(head) = block_head(&message) else {
            cx.ledger.violated("block_walk", next, "CompactBlock head will not walk".into());
            return None;
        };
        if head.height != next {
            cx.ledger.violated(
                "range_contiguous",
                next,
                format!("expected {next}, served {}", head.height),
            );
            return None;
        }
        if link.is_some_and(|prev| prev != head.prev_hash) {
            cx.ledger.violated("chain_link", next, "prev_hash ≠ the previous block's hash".into());
            return None;
        }
        cx.ledger.shielded_block(next as u32, &message);
        link = Some(head.hash);
        next += 1;
        if let Some(pace) = pace.as_deref_mut() {
            pace.consumed(message.len()).await;
        }
        if cx.stop.is_cancelled() {
            return None;
        }
    }
    cx.tally(Method::GetBlockRange, started, 0, None);
    if next != to + 1 {
        cx.ledger.violated("range_complete", next, format!("[{from}, {to}] ended at {next}"));
        return None;
    }
    link
}
