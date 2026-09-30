//! Simulated light wallets, shaped on the two real sync engines (zaino
//! `docs/notes/lightwallet-serving-audit.md` §1.2, read at source).
//!
//! - [`steady`] = pepper-sync at the tip: permanent mempool stream, `GetLatestBlock` ≤ 10 s, and
//!   per block the burst (tip, tree state, each pool's roots, a 10-block verify range, a
//!   `GetBlock` reorg check); a mobile fraction polls `GetLightdInfo` every 5 s
//! - [`pepper_sync`] = a never-used wallet's whole sync, birthday → tip: tree states, roots, 21
//!   gap-limit `GetTaddressTxids`, then one `GetBlockRange` at a time (verify, chain-tip shard,
//!   historic shards ascending), under the mempool stream + tip poll
//! - [`librustzcash`] = its loop, birthday → tip: roots + utxos, then 1000-block batches, a
//!   `GetTreeState(start − 1)` before each
//! - One H2 connection per wallet; every response checked inline ([`Ledger`]) and every block's
//!   height / link / completeness asserted as it streams

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message;
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};
use zcash_protocol::consensus::{NetworkConstants, NetworkType};

use crate::loadtest::ledger::{Audit, Ledger, sampled, splitmix64};
use crate::loadtest::measure::{EngineCpu, Measure, Method};
use crate::loadtest::wire::{RawClient, block_head, digest, encoded, path};
use crate::proto::{
    BlockId, BlockRange, ChainSpec, Empty, GetAddressUtxosArg, GetSubtreeRootsArg,
    ShieldedProtocol, SubtreeRoot, TransparentAddressBlockFilter,
};

/// Blocks a wallet re-verifies from where it last stood (pepper-sync `VERIFY_BLOCK_RANGE_SIZE`)
const VERIFY_BLOCKS: u32 = 10;

/// `GetLatestBlock` floor between blocks (pepper-sync `CHECK_NEW_BLOCKS_INTERVAL`)
const TIP_POLL: Duration = Duration::from_secs(10);

/// zingo-mobile's `GetLightdInfo` cadence while the app is open
const LIGHTD_POLL: Duration = Duration::from_secs(5);

/// 1 in N `GetLightdInfo` answers audited (all identical within a block)
const LIGHTD_AUDIT_EVERY: u64 = 64;

/// Retry hint zaino sends with an admission refusal (`grpc-retry-pushback-ms`)
const PUSHBACK: Duration = Duration::from_millis(250);

/// Addresses a never-used zingolib wallet asks after: external 0, then 10 empties per scope
/// (gap limit; external from 1, refund from 0)
const UNUSED_WALLET_ADDRESSES: u64 = 21;

/// Blocks below its birthday pepper-sync's transparent discovery starts from
const TRANSPARENT_LOOKBACK: u32 = 100;

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
    pub cpu: Arc<EngineCpu>,
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
    engine_cpu: Option<Duration>,
    pending: AtomicUsize,
}

impl Burst {
    pub fn new(wallets: usize, cpu: &EngineCpu) -> Self {
        Self { started: Instant::now(), engine_cpu: cpu.used(), pending: AtomicUsize::new(wallets) }
    }

    pub fn drained(&self) -> bool {
        self.pending.load(Ordering::Relaxed) == 0
    }

    fn done(&self, cx: &Cx) {
        // saturating: a wallet joining mid-announcement takes a burst that never counted it
        let left =
            self.pending.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |p| p.checked_sub(1));
        if left == Ok(1) {
            let drain = self.started.elapsed();
            let busy = self
                .engine_cpu
                .zip(cx.cpu.used())
                .map(|(before, after)| cx.cpu.busy(after.saturating_sub(before), drain));
            cx.measure.burst(drain, busy);
        }
    }
}

/// Acks the burst a wallet took, however the wallet leaves it
struct Taken<'a> {
    burst: Option<Arc<Burst>>,
    cx: &'a Cx,
}

impl Drop for Taken<'_> {
    fn drop(&mut self) {
        if let Some(burst) = self.burst.take() {
            burst.done(self.cx);
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
///
/// - Opens with one catch-up burst of its own (roots from 0), then takes the announced ones
pub async fn steady(
    cx: Cx,
    id: u64,
    mobile: bool,
    mut bursts: watch::Receiver<Option<Arc<Burst>>>,
) {
    let Some(client) = cx.dial().await else {
        return;
    };
    let mempool = tokio::spawn(mempool(cx.clone(), client.clone()));
    let lightd = mobile.then(|| tokio::spawn(lightd(cx.clone(), client.clone(), id)));
    let mut synced = Synced::default();
    burst(&cx, &client, &mut synced).await;
    // seen before counted: every burst counting this wallet = one it takes
    bursts.borrow_and_update();
    cx.connected.fetch_add(1, Ordering::Relaxed);

    let mut poll = tip_ticks(id);
    loop {
        tokio::select! {
            _ = cx.stop.cancelled() => break,
            changed = bursts.changed() => {
                if changed.is_err() {
                    break;
                }
                let taken = Taken { burst: bursts.borrow_and_update().clone(), cx: &cx };
                if taken.burst.is_some() {
                    burst(&cx, &client, &mut synced).await;
                }
            }
            _ = poll.tick() => {
                if latest(&cx, &client).await.is_none() {
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

/// `TIP_POLL` ticks, offset per wallet (N wallets = N/10 req/s, not N every 10 s)
fn tip_ticks(id: u64) -> tokio::time::Interval {
    let offset = Duration::from_millis(splitmix64(id) % TIP_POLL.as_millis() as u64);
    tokio::time::interval_at(tokio::time::Instant::now() + offset, TIP_POLL)
}

async fn latest(cx: &Cx, client: &RawClient) -> Option<Bytes> {
    cx.unary(client, Method::GetLatestBlock, path::GET_LATEST_BLOCK, encoded(&ChainSpec {})).await
}

async fn latest_height(cx: &Cx, client: &RawClient) -> Option<u32> {
    let latest = latest(cx, client).await?;
    BlockId::decode(latest.as_ref()).ok().map(|id| id.height as u32)
}

/// One block's worth of a synced wallet's requests, in pepper-sync's order
async fn burst(cx: &Cx, client: &RawClient, synced: &mut Synced) {
    let Some(latest) = latest(cx, client).await else {
        return;
    };
    let Ok(tip) = BlockId::decode(latest.as_ref()) else {
        return cx.ledger.violated("latest_block", 0, "undecodable BlockID".into());
    };
    cx.ledger.answer(format!("tip/{}", tip.height), digest(&latest), false, || Audit::Tip {
        message: latest.clone(),
    });
    let height = tip.height as u32;
    tree_state(cx, client, height).await;

    for (pool, held) in synced.roots.iter_mut().enumerate() {
        if let Some(gained) = subtree_roots(cx, client, pool, *held).await {
            *held += gained.len() as u32;
        }
    }

    let verify_from = tip.height.saturating_sub(u64::from(VERIFY_BLOCKS) - 1);
    let from = verify_from.max(synced.tip + 1).min(tip.height);
    stream_blocks(cx, client, from, tip.height, None, None).await;
    synced.tip = tip.height;

    let id = BlockId { height: tip.height, hash: Vec::new() };
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

/// Where a syncing wallet's birthday falls (clamped to the chain the run can serve)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Birthdays {
    /// Created within the last `blocks` (a new install)
    Recent { blocks: u32 },
    /// `[from, to)`, e.g. an era of heavy blocks
    Between { from: u32, to: u32 },
    /// Anywhere since Sapling activation
    Anywhere,
}

impl Birthdays {
    /// `[from, to)` within `[lowest, highest)`, never empty
    fn span(self, lowest: u32, highest: u32) -> (u32, u32) {
        let (from, to) = match self {
            Birthdays::Recent { blocks } => (highest.saturating_sub(blocks), highest),
            Birthdays::Between { from, to } => (from, to),
            Birthdays::Anywhere => (lowest, highest),
        };
        let from = from.clamp(lowest, highest.saturating_sub(1).max(lowest));
        (from, to.min(highest).max(from + 1))
    }
}

/// A syncing wallet's shape
///
/// - Birthdays = one of `birthdays`, drawn per sync, within `[lowest, highest)`
/// - `activations` = each pool's, `POOLS` order; `batch` = librustzcash's blocks per range
/// - `pace` = scan rate, bytes/s (`None` = as fast as served); `addresses` for `GetAddressUtxos`
#[derive(Debug, Clone)]
pub struct SyncShape {
    pub birthdays: Arc<[Birthdays]>,
    pub lowest: u32,
    pub highest: u32,
    pub activations: [u32; 3],
    pub network: NetworkType,
    pub batch: u32,
    pub pace: Option<u64>,
    pub addresses: Arc<Vec<String>>,
}

/// Height wallet `id` starts its `round`th sync from: a profile drawn from `shape.birthdays`, then
/// a height uniform within it
fn birthday(shape: &SyncShape, id: u64, round: u64) -> u32 {
    let draw = splitmix64((id << 32) | round);
    let profiles = shape.birthdays.len().max(1) as u64;
    let profile = shape.birthdays.get((draw % profiles) as usize).copied();
    let (from, to) = profile.unwrap_or(Birthdays::Anywhere).span(shape.lowest, shape.highest);
    from + (splitmix64(draw) % u64::from(to - from)) as u32
}

/// pepper-sync syncing a never-used wallet, one birthday after another until stopped
pub async fn pepper_sync(cx: Cx, id: u64, shape: SyncShape) {
    let Some(client) = cx.dial().await else {
        return;
    };
    cx.connected.fetch_add(1, Ordering::Relaxed);
    let mempool = tokio::spawn(mempool(cx.clone(), client.clone()));
    let tip_poll = tokio::spawn(tip_poll(cx.clone(), client.clone(), id));
    let mut pace = shape.pace.map(Pace::new);
    for round in 0u64.. {
        if cx.stop.is_cancelled() {
            break;
        }
        if unused_wallet_sync(&cx, &client, &shape, (id, round), pace.as_mut()).await.is_none() {
            cx.back_off().await;
        }
    }
    mempool.abort();
    tip_poll.abort();
    cx.connected.fetch_sub(1, Ordering::Relaxed);
}

async fn tip_poll(cx: Cx, client: RawClient, id: u64) {
    let mut poll = tip_ticks(id);
    loop {
        poll.tick().await;
        if latest(&cx, &client).await.is_none() {
            cx.back_off().await;
        }
    }
}

/// One whole sync, in pepper-sync's order (`None` = refused, cut short, or stopped)
///
/// - One fetcher per wallet → every call sequential, one block stream open at a time
async fn unused_wallet_sync(
    cx: &Cx,
    client: &RawClient,
    shape: &SyncShape,
    (id, round): (u64, u64),
    mut pace: Option<&mut Pace>,
) -> Option<()> {
    let birthday = birthday(shape, id, round);
    let sapling = shape.activations[0];
    let tip = latest_height(cx, client).await?;
    if birthday > sapling {
        tree_state(cx, client, birthday - 1).await;
    }
    tree_state(cx, client, tip).await;

    let mut completing: [Vec<u32>; 3] = Default::default();
    for (pool, heights) in completing.iter_mut().enumerate() {
        let roots = subtree_roots(cx, client, pool, 0).await?;
        heights.extend(
            roots
                .iter()
                .filter_map(|root| SubtreeRoot::decode(root.as_ref()).ok())
                .map(|root| root.completing_block_height as u32),
        );
    }

    let from = birthday.saturating_sub(TRANSPARENT_LOOKBACK).max(sapling);
    for index in 0..UNUSED_WALLET_ADDRESSES {
        let address = unused_address(shape.network, (id, round, index));
        taddress_txids(cx, client, &address, from, tip).await?;
    }
    if birthday > sapling {
        tree_state(cx, client, birthday).await;
    }

    let shards = Shards { activations: shape.activations, completing };
    let mut streamed: Option<(u32, [u8; 32])> = None;
    for range in shards.scan_order(birthday, tip) {
        let link = streamed.filter(|(next, _)| *next == range.start).map(|(_, hash)| hash);
        let (from, to) = (u64::from(range.start), u64::from(range.end - 1));
        let hash = stream_blocks(cx, client, from, to, link, pace.as_deref_mut()).await?;
        streamed = Some((range.end, hash));
    }
    Some(())
}

/// Block ranges holding each pool's shards (`POOLS` order): a pool's activation, then every
/// subtree root's completing height, bound them
#[derive(Debug)]
struct Shards {
    activations: [u32; 3],
    completing: [Vec<u32>; 3],
}

impl Shards {
    /// pepper-sync `determine_block_range`: the shards holding `height`, of the newest pool up to
    /// `pool` active there (past that pool's last root = its open shard, to `tip`)
    fn range(&self, pool: usize, height: u32, birthday: u32, tip: u32) -> Range<u32> {
        let pool = (0..=pool).rev().find(|&p| height >= self.activations[p]).unwrap_or(0);
        let (activation, completing) = (self.activations[pool], &self.completing[pool]);
        let starts = std::iter::once(activation).chain(completing.iter().copied());
        let mut holding = starts
            .zip(completing)
            .map(|(start, end)| start..end + 1)
            .filter(|r| r.contains(&height));
        match holding.next() {
            Some(first) => first.start..holding.last().map_or(first.end, |last| last.end),
            None => completing.last().copied().unwrap_or(activation.max(birthday))..tip + 1,
        }
    }

    /// Ranges an unused wallet streams, in pepper-sync's priority order: verify, the chain-tip
    /// shard, then historic shards ascending
    fn scan_order(&self, birthday: u32, tip: u32) -> Vec<Range<u32>> {
        if birthday > tip {
            return Vec::new();
        }
        let verify = birthday..(birthday + VERIFY_BLOCKS).min(tip + 1);
        let chain_tip = (0..POOLS.len())
            .map(|pool| self.range(pool, tip, birthday, tip).start)
            .min()
            .unwrap_or(birthday)
            .clamp(verify.end, tip + 1);
        let mut order = vec![verify.clone(), chain_tip..tip + 1];
        let mut next = verify.end;
        while next < chain_tip {
            let shard = self.range(POOLS.len() - 1, next, birthday, tip);
            let end = shard.end.clamp(next + 1, chain_tip);
            order.push(next..end);
            next = end;
        }
        order.retain(|range| !range.is_empty());
        order
    }
}

/// p2pkh of a hash no key produced = an address with no history, as an unused wallet's are
fn unused_address(network: NetworkType, (id, round, index): (u64, u64, u64)) -> String {
    let seed: Vec<u8> = [id, round, index].iter().flat_map(|part| part.to_le_bytes()).collect();
    let mut payload = network.b58_pubkey_address_prefix().to_vec();
    payload.extend_from_slice(&blake3::hash(&seed).as_bytes()[..20]);
    bs58::encode(payload).with_check().into_string()
}

/// `GetTaddressTxids(address, from ..= to)` for an [`unused_address`]: any transaction served =
/// a violation
async fn taddress_txids(
    cx: &Cx,
    client: &RawClient,
    address: &str,
    from: u32,
    to: u32,
) -> Option<()> {
    let at = |height: u32| Some(BlockId { height: u64::from(height), hash: Vec::new() });
    let ask = TransparentAddressBlockFilter {
        address: address.to_owned(),
        range: Some(BlockRange { start: at(from), end: at(to), pool_types: Vec::new() }),
    };
    let served = cx
        .collected(client, Method::GetTaddressTxids, path::GET_TADDRESS_TXIDS, encoded(&ask))
        .await?;
    if !served.is_empty() {
        cx.ledger.violated(
            "taddress_txids",
            u64::from(from),
            format!("{address} (never used): {} transactions served", served.len()),
        );
    }
    Some(())
}

/// librustzcash's sync loop, birthday → tip, one birthday after another until stopped
pub async fn librustzcash(cx: Cx, id: u64, shape: SyncShape) {
    let Some(client) = cx.dial().await else {
        return;
    };
    cx.connected.fetch_add(1, Ordering::Relaxed);
    let mut pace = shape.pace.map(Pace::new);
    for round in 0u64.. {
        if cx.stop.is_cancelled() {
            break;
        }
        if batched_sync(&cx, &client, &shape, (id, round), pace.as_mut()).await.is_none() {
            cx.back_off().await;
        }
    }
    cx.connected.fetch_sub(1, Ordering::Relaxed);
}

/// One whole sync (`None` = refused, cut short, or stopped)
async fn batched_sync(
    cx: &Cx,
    client: &RawClient,
    shape: &SyncShape,
    (id, round): (u64, u64),
    mut pace: Option<&mut Pace>,
) -> Option<()> {
    let tip = latest_height(cx, client).await?;
    for pool in 0..POOLS.len() {
        roots_from(cx, client, pool, 0).await;
    }
    utxos(cx, client, &shape.addresses, id, round, tip).await;

    let mut link = None;
    let mut start = birthday(shape, id, round);
    while start <= tip {
        let last = (start + shape.batch - 1).min(tip);
        tree_state(cx, client, start.saturating_sub(1)).await;
        let (from, to) = (u64::from(start), u64::from(last));
        link = Some(stream_blocks(cx, client, from, to, link, pace.as_deref_mut()).await?);
        start = last + 1;
    }
    Some(())
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

/// One unbounded `GetSubtreeRoots` pass over `POOLS[pool]`, from root `start`
async fn roots_from(cx: &Cx, client: &RawClient, pool: usize, start: u32) -> Option<Vec<Bytes>> {
    let (name, protocol) = POOLS[pool];
    let ask = GetSubtreeRootsArg {
        start_index: start,
        shielded_protocol: protocol as i32,
        max_entries: 0,
    };
    let roots = cx
        .collected(client, Method::GetSubtreeRoots, path::GET_SUBTREE_ROOTS, encoded(&ask))
        .await?;
    let whole: Vec<u8> = roots.iter().flat_map(|r| r.iter().copied()).collect();
    cx.ledger.answer(format!("roots/{name}/{start}"), digest(&whole), false, || Audit::Subtrees {
        pool: name,
        start,
        max: 0,
        messages: roots.clone(),
    });
    Some(roots)
}

/// pepper-sync's root fetch past the `held` it has: re-asked from where each pass ended until
/// one comes back empty (a cut stream ends cleanly too)
async fn subtree_roots(cx: &Cx, client: &RawClient, pool: usize, held: u32) -> Option<Vec<Bytes>> {
    let mut gained = Vec::new();
    loop {
        let pass = roots_from(cx, client, pool, held + gained.len() as u32).await?;
        if pass.is_empty() {
            return Some(gained);
        }
        gained.extend(pass);
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

    /// Token bucket: sleeps only once `PACE_QUANTUM` ahead, oversleep kept as credit
    ///
    /// - Per-message sleeps of < 1 ms round up to tokio's 1 ms timer → small blocks capped the
    ///   rate at ~1 block/ms
    /// - Credit capped at `PACE_QUANTUM` of lag (a slow server banks no burst for later)
    async fn consumed(&mut self, bytes: usize) {
        let now = tokio::time::Instant::now();
        let earliest = now.checked_sub(PACE_QUANTUM).unwrap_or(now);
        self.next = self.next.max(earliest) + Duration::from_secs_f64(bytes as f64 / self.rate);
        if self.next > now + PACE_QUANTUM {
            tokio::time::sleep_until(self.next).await;
        }
    }
}

const PACE_QUANTUM: Duration = Duration::from_millis(20);

/// `GetBlockRange(from ..= to)`, default pools, every block checked as it arrives
///
/// - latency = first message (a whole stream's time = its length ÷ the client's pace)
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
        if next == from {
            cx.tally(Method::GetBlockRange, started, 0, None);
        }
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
    if next == from {
        cx.tally(Method::GetBlockRange, started, 0, None);
    }
    if next != to + 1 {
        cx.ledger.violated("range_complete", next, format!("[{from}, {to}] ended at {next}"));
        return None;
    }
    link
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 3 MB in 3 KB blocks at 5 MB/s = 0.6 s: small blocks paced to the rate, not to the timer
    #[tokio::test(start_paused = true)]
    async fn a_paced_scan_holds_its_rate_on_small_blocks() {
        let mut pace = Pace::new(5_000_000);
        let started = tokio::time::Instant::now();
        for _ in 0..1_000 {
            pace.consumed(3_000).await;
        }
        let elapsed = started.elapsed();
        let want = Duration::from_millis(600);
        assert!(elapsed.abs_diff(want) <= PACE_QUANTUM, "{elapsed:?}, want ≈ {want:?}");
    }

    /// Sapling from 10 (roots complete at 50, 120), Orchard from 100 (150, 180), Ironwood from
    /// 200 (no root yet); tip 300
    #[test]
    fn an_unused_wallet_streams_verify_then_the_chain_tip_shard_then_history_ascending() {
        let shards = Shards {
            activations: [10, 100, 200],
            completing: [vec![50, 120], vec![150, 180], vec![]],
        };

        // height → the shards of the newest pool active there, asked of each pool
        let cases = [
            ((0, 30), 10..51, "inside sapling's first shard"),
            ((0, 50), 10..121, "a completing block holds two shards' commitments"),
            ((0, 300), 120..301, "past sapling's last root = its open shard"),
            ((1, 300), 180..301, "orchard's open shard"),
            ((2, 300), 200..301, "no ironwood root = from its activation"),
            ((2, 130), 100..151, "ironwood not active at 130 → orchard's shard"),
            ((2, 60), 50..121, "neither newer pool active at 60 → sapling's shard"),
        ];
        for ((pool, height), want, why) in cases {
            assert_eq!(shards.range(pool, height, 20, 300), want, "{why}");
        }

        let order = shards.scan_order(20, 300);
        // 51..120 = one sapling shard (orchard, active from 100, not yet at 51)
        let want = [20..30, 120..301, 30..51, 51..120];
        assert_eq!(order, want, "verify, chain tip (sapling's open shard), history by shard");
        assert_eq!(
            shards.scan_order(295, 300),
            [Range { start: 295, end: 301 }],
            "birthday in verify"
        );
        assert_eq!(shards.scan_order(150, 300), [150..160, 160..301], "birthday past every root");
        assert!(shards.scan_order(301, 300).is_empty(), "birthday above the tip");

        let covered: u32 = order.iter().map(|range| range.end - range.start).sum();
        assert_eq!(covered, 281, "every block of [20, 300] exactly once");
    }

    #[test]
    fn birthdays_come_from_every_profile_and_unused_addresses_are_valid_and_distinct() {
        let profiles = [
            Birthdays::Recent { blocks: 10_000 },
            Birthdays::Between { from: 1_700_000, to: 2_200_000 },
            Birthdays::Anywhere,
        ];
        let shape = SyncShape {
            birthdays: profiles.as_slice().into(),
            lowest: 419_200,
            highest: 3_400_000,
            activations: [419_200, 1_687_104, 3_300_000],
            network: NetworkType::Main,
            batch: 1_000,
            pace: None,
            addresses: Arc::default(),
        };
        let drawn: Vec<u32> = (0..300).map(|id| birthday(&shape, id, 0)).collect();
        assert!(drawn.iter().all(|at| (419_200..3_400_000).contains(at)), "{drawn:?}");
        let recent = drawn.iter().filter(|at| **at >= 3_390_000).count();
        let sandblast = drawn.iter().filter(|at| (1_700_000..2_200_000).contains(*at)).count();
        assert!(recent > 50 && sandblast > 50, "each profile drawn: {recent} recent, {sandblast}");
        assert_ne!(birthday(&shape, 1, 0), birthday(&shape, 1, 1), "a new round draws anew");

        // spans clamp to the chain the run serves, never empty
        assert_eq!(Birthdays::Between { from: 0, to: 10 }.span(50, 100), (50, 51));
        assert_eq!(Birthdays::Recent { blocks: 1_000 }.span(50, 100), (50, 100));
        assert_eq!(Birthdays::Anywhere.span(50, 50), (50, 51));

        let address = unused_address(NetworkType::Main, (1, 2, 3));
        let decoded = bs58::decode(&address).with_check(None).into_vec().expect("base58check");
        assert_eq!((&address[..2], decoded.len(), &decoded[..2]), ("t1", 22, &[0x1c, 0xb8][..]));
        assert_ne!(address, unused_address(NetworkType::Main, (1, 2, 4)));
        assert!(unused_address(NetworkType::Regtest, (1, 2, 3)).starts_with("tm"));
    }
}
