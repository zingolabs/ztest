//! Light-wallet load over a live indexer: calibrate against zebra, then ramp stages.
//!
//! - Engine on its own OS thread + current-thread runtime (1 core; its CPU read apart from the
//!   auditor's and the harness's)
//! - Each stage: sessions dial (ramped), warm up, then one measured window; server CPU from
//!   the indexer pod's cgroup ([`ServerMeter`]), the driver's from its own
//! - A stage is **client-bound** when the driver's cgroup throttled it: its throughput then
//!   bounds the client, not the server, and the report says so

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prost::Message;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::loadtest::audit::{self, Auditor, Expect};
use crate::loadtest::ledger::{Ledger, Tallies, splitmix64};
use crate::loadtest::measure::{CgroupCpu, Measure, Method, MethodWindow, thread_cpu};
use crate::loadtest::oracle::Violation;
use crate::loadtest::reference::Zebra;
use crate::loadtest::report::LatencyStats;
use crate::loadtest::wallet::{self, Burst, Cx, RestoreShape};
use crate::loadtest::wire::{RawClient, encoded, path};
use crate::proto::{BlockId, ChainSpec};
use crate::protocol::client::JsonRpcClient;
use crate::sync::{ProgressView, SyncSubject};
use crate::{EnvError, RpcError};

/// Server-side cost, read off the indexer pod (cgroup v2)
#[async_trait]
pub trait ServerMeter: Send + Sync + fmt::Debug {
    async fn cpu(&self) -> Result<Duration, String>;
    /// Resident bytes page-cache pressure cannot reclaim (heap + stacks)
    async fn memory(&self) -> Result<u64, String>;
}

/// [`ServerMeter`] over a component pod's own cgroup (v2), read with `cat` in its container
#[derive(Debug, Clone)]
pub struct PodCgroup {
    pod: crate::handles::ComponentPod,
}

impl PodCgroup {
    pub fn new(pod: crate::handles::ComponentPod) -> Self {
        Self { pod }
    }

    async fn read(&self, file: &str) -> Result<String, String> {
        let path = format!("/sys/fs/cgroup/{file}");
        let output = self
            .pod
            .exec(&["cat", &path], Duration::from_secs(20))
            .await
            .map_err(|e| e.to_string())?;
        match output.success() {
            true => Ok(output.stdout),
            false => Err(format!("cat {path}: exit {}: {}", output.status, output.stderr)),
        }
    }
}

#[async_trait]
impl ServerMeter for PodCgroup {
    async fn cpu(&self) -> Result<Duration, String> {
        let stat = self.read("cpu.stat").await?;
        CgroupCpu::parse(&stat)
            .map(|cpu| cpu.usage)
            .ok_or_else(|| format!("cpu.stat without usage_usec: {stat}"))
    }

    /// `anon` of `memory.stat` = heap + stacks, the part page-cache pressure cannot reclaim
    async fn memory(&self) -> Result<u64, String> {
        let stat = self.read("memory.stat").await?;
        stat.lines()
            .find_map(|line| line.strip_prefix("anon ")?.trim().parse().ok())
            .ok_or_else(|| format!("memory.stat without anon: {stat}"))
    }
}

/// What one stage offers
#[derive(Debug, Clone, Copy)]
pub enum Load {
    /// Synced wallets at the tip; `tick` = a block announcement every so often (real blocks
    /// trigger one too, but every 75 s is too few samples)
    Steady { wallets: usize, tick: Duration },
    /// Wallets restoring from old birthdays; `pace` = client scan rate, bytes/s
    Restore { sessions: usize, pace: Option<u64> },
    /// Both at once (does a restore wave hurt the tip?)
    Mixed { wallets: usize, tick: Duration, sessions: usize, pace: Option<u64> },
}

impl Load {
    fn parts(self) -> (usize, Duration, usize, Option<u64>) {
        match self {
            Load::Steady { wallets, tick } => (wallets, tick, 0, None),
            Load::Restore { sessions, pace } => (0, Duration::MAX, sessions, pace),
            Load::Mixed { wallets, tick, sessions, pace } => (wallets, tick, sessions, pace),
        }
    }
}

impl fmt::Display for Load {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pace = |pace: Option<u64>| match pace {
            Some(rate) => format!("at {} MB/s each", rate / 1_000_000),
            None => "unpaced".to_owned(),
        };
        match self {
            Load::Steady { wallets, tick } => {
                write!(f, "steady {wallets} wallets, burst every {}s", tick.as_secs())
            }
            Load::Restore { sessions, pace: p } => {
                write!(f, "restore {sessions} sessions {}", pace(*p))
            }
            Load::Mixed { wallets, sessions, pace: p, .. } => {
                write!(f, "mixed {wallets} wallets + {sessions} restores {}", pace(*p))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Stage {
    pub load: Load,
    pub warmup: Duration,
    pub hold: Duration,
}

/// Everything a run does, fixed up front (a run reproduces from its plan: no RNG state)
#[derive(Debug, Clone)]
pub struct Plan {
    pub stages: Vec<Stage>,
    /// Heights held to zebra before any load (both shapes, every fee)
    pub calibration: usize,
    /// 1 in N first-seen blocks under load held to zebra
    pub audit_every: u64,
    pub audit_queue: usize,
    /// Blocks below the served tip treated as settled (above = reorg window)
    pub reorg_margin: u32,
    pub restore_span: u32,
    pub restore_batch: u32,
    /// 1 in N steady wallets also polls `GetLightdInfo` (mobile)
    pub mobile_every: usize,
    /// Concurrent dials while a stage ramps
    pub dials: usize,
    /// How long after the load the auditor may wait for tip answers to bury
    pub settle: Duration,
}

impl Plan {
    /// Mainnet after a full index build: steady ramp to 5k wallets, restore ramp to 64
    /// unpaced / 128 paced, then a mixed stage
    ///
    /// - 5k connections + mempool streams > 1 GiB driver (sized by `QosClass::Sync` runner)
    pub fn mainnet() -> Self {
        let steady = |wallets| Stage {
            load: Load::Steady { wallets, tick: Duration::from_secs(20) },
            warmup: Duration::from_secs(40),
            hold: Duration::from_secs(180),
        };
        let restore = |sessions, pace| Stage {
            load: Load::Restore { sessions, pace },
            warmup: Duration::from_secs(20),
            hold: Duration::from_secs(120),
        };
        Self {
            stages: vec![
                steady(250),
                steady(1_000),
                steady(2_500),
                steady(5_000),
                restore(1, None),
                restore(4, None),
                restore(16, None),
                restore(64, None),
                restore(32, Some(5_000_000)),
                restore(128, Some(5_000_000)),
                Stage {
                    load: Load::Mixed {
                        wallets: 1_000,
                        tick: Duration::from_secs(20),
                        sessions: 16,
                        pace: None,
                    },
                    warmup: Duration::from_secs(40),
                    hold: Duration::from_secs(180),
                },
            ],
            calibration: 400,
            audit_every: 64,
            audit_queue: 4_096,
            reorg_margin: 100,
            restore_span: 20_000,
            restore_batch: 1_000,
            mobile_every: 3,
            dials: 256,
            settle: Duration::from_secs(20 * 60),
        }
    }

    /// Minutes on a regtest chain: every stage kind once, every answer audited
    pub fn smoke() -> Self {
        let short =
            |load| Stage { load, warmup: Duration::from_secs(5), hold: Duration::from_secs(20) };
        Self {
            stages: vec![
                short(Load::Steady { wallets: 50, tick: Duration::from_secs(5) }),
                short(Load::Restore { sessions: 4, pace: None }),
                short(Load::Mixed {
                    wallets: 20,
                    tick: Duration::from_secs(5),
                    sessions: 2,
                    pace: Some(1_000_000),
                }),
            ],
            calibration: 64,
            audit_every: 1,
            audit_queue: 16_384,
            reorg_margin: 2,
            restore_span: 40,
            restore_batch: 10,
            mobile_every: 2,
            dials: 32,
            settle: Duration::from_secs(60),
        }
    }
}

/// What the load is aimed at
#[derive(Debug, Clone)]
pub struct Target {
    /// Indexer gRPC, `http://host:port`
    pub uri: String,
    pub zebra: JsonRpcClient,
    pub server: Arc<dyn ServerMeter>,
    /// Expected `LightdInfo.chain_name` (`main` / `test` / `regtest`)
    pub chain_name: String,
}

/// One stage's window
#[derive(Debug, Clone)]
pub struct StageReport {
    pub load: Load,
    pub window: Duration,
    /// Sessions holding a connection when the window closed
    pub connected: usize,
    pub methods: Vec<MethodWindow>,
    pub burst_drain: Option<LatencyStats>,
    pub server_cores: Option<f64>,
    pub server_memory: Option<u64>,
    /// Engine thread (the load itself)
    pub client_cores: Option<f64>,
    /// Share of the window the driver pod sat throttled at its CPU limit
    pub throttled: Option<f64>,
    pub ledger: Tallies,
}

/// Throttled beyond this = the driver, not the server, set the pace
const CLIENT_BOUND: f64 = 0.05;

impl StageReport {
    pub fn client_bound(&self) -> bool {
        self.throttled.is_some_and(|t| t > CLIENT_BOUND)
            || self.client_cores.is_some_and(|c| c > 0.9)
    }

    pub fn requests_per_second(&self) -> f64 {
        let ok: u64 =
            self.methods.iter().filter(|m| m.method != Method::Connect).map(|m| m.ok).sum();
        ok as f64 / self.window.as_secs_f64()
    }

    pub fn bytes_per_second(&self) -> f64 {
        self.methods.iter().map(|m| m.bytes).sum::<u64>() as f64 / self.window.as_secs_f64()
    }

    pub fn failures(&self) -> u64 {
        self.methods.iter().flat_map(|m| m.failed.values()).sum()
    }

    /// Server CPU per MB received (`None` = no server meter, or nothing moved)
    pub fn cpu_ms_per_mb(&self) -> Option<f64> {
        let mb = self.bytes_per_second() / 1e6;
        self.server_cores.filter(|_| mb > 0.0).map(|cores| cores * 1e3 / mb)
    }
}

impl fmt::Display for StageReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let opt =
            |v: Option<f64>, unit: &str| v.map_or("—".to_owned(), |v| format!("{v:.2}{unit}"));
        writeln!(
            f,
            "{}{} — {:.0} s window, {} connected",
            self.load,
            if self.client_bound() { "  [CLIENT-BOUND]" } else { "" },
            self.window.as_secs_f64(),
            self.connected,
        )?;
        writeln!(
            f,
            "  {:.0} req/s, {:.1} MB/s, {} failed | server {} cores, {} | driver {} cores, throttled {}",
            self.requests_per_second(),
            self.bytes_per_second() / 1e6,
            self.failures(),
            opt(self.server_cores, ""),
            self.server_memory.map_or("—".to_owned(), |b| format!(
                "{:.2} GiB",
                b as f64 / f64::from(1u32 << 30)
            )),
            opt(self.client_cores, ""),
            opt(self.throttled.map(|t| t * 100.0), "%"),
        )?;
        if let Some(drain) = &self.burst_drain {
            writeln!(
                f,
                "  burst drain: {} bursts, p50 {:?}, p99 {:?}, max {:?}",
                drain.count, drain.p50, drain.p99, drain.max
            )?;
        }
        if let Some(cost) = self.cpu_ms_per_mb() {
            writeln!(f, "  server {cost:.2} CPU-ms per MB")?;
        }
        for m in &self.methods {
            writeln!(
                f,
                "  {:<18} ok {:>8}  p50 {:>9.2?}  p99 {:>9.2?}  p99.9 {:>9.2?}  {:>9.1} MB{}",
                m.method.name(),
                m.ok,
                m.latency.p50,
                m.latency.p99,
                m.latency.p999,
                m.bytes as f64 / 1e6,
                if m.failed.is_empty() {
                    String::new()
                } else {
                    format!("  failed {:?}", m.failed)
                },
            )?;
        }
        let l = &self.ledger;
        writeln!(
            f,
            "  verified: {} audited vs zebra, {} consistent repeats, {} unjudged, {} audit drops, {} violations",
            l.audited, l.consistent, l.skipped, l.dropped, l.violations
        )
    }
}

/// The whole run
#[derive(Debug, Clone)]
pub struct LoadReport {
    pub calibrated: Vec<u32>,
    pub calibration: Tallies,
    pub stages: Vec<StageReport>,
    pub ledger: Tallies,
    pub violations: Vec<Violation>,
}

impl fmt::Display for LoadReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "calibration: {} heights held to zebra ({} checks passed, {} violations)",
            self.calibrated.len(),
            self.calibration.audited,
            self.calibration.violations
        )?;
        for stage in &self.stages {
            write!(f, "{stage}")?;
        }
        let steady = self
            .stages
            .iter()
            .filter(|s| {
                matches!(s.load, Load::Steady { .. }) && s.failures() == 0 && !s.client_bound()
            })
            .filter_map(|s| Some((s.connected, s.burst_drain?.p99)))
            .max_by_key(|(wallets, _)| *wallets);
        let restore = self
            .stages
            .iter()
            .filter(|s| matches!(s.load, Load::Restore { .. }))
            .max_by(|a, b| a.bytes_per_second().total_cmp(&b.bytes_per_second()));
        if let Some((wallets, p99)) = steady {
            writeln!(
                f,
                "headline: {wallets} synced wallets, burst drain p99 {p99:?}, no failure, server-bound"
            )?;
        }
        if let Some(best) = restore {
            writeln!(
                f,
                "headline: restore peak {:.1} MB/s ({}){}",
                best.bytes_per_second() / 1e6,
                best.load,
                if best.client_bound() {
                    ", CLIENT-BOUND: a floor on the server's capacity"
                } else {
                    ""
                }
            )?;
        }
        writeln!(
            f,
            "overall: {} audited vs zebra, {} consistent repeats, {} unjudged, {} audit drops, {} violations",
            self.ledger.audited,
            self.ledger.consistent,
            self.ledger.skipped,
            self.ledger.dropped,
            self.ledger.violations
        )?;
        for v in &self.violations {
            writeln!(f, "  VIOLATION {v}")?;
        }
        Ok(())
    }
}

/// A run's live state, shared with the probes watching it
#[derive(Debug, Default)]
pub struct LoadRun {
    stages_done: AtomicU32,
    stages: AtomicU32,
    ledger: OnceLock<Arc<Ledger>>,
    outcome: Mutex<Option<Result<LoadReport, String>>>,
    finished: AtomicBool,
}

impl LoadRun {
    /// Violations so far (live; `0` before calibration starts)
    pub fn violations(&self) -> Vec<Violation> {
        self.ledger.get().map(|l| l.violations()).unwrap_or_default()
    }

    pub fn violation_count(&self) -> u64 {
        self.ledger.get().map_or(0, |l| l.tallies().violations)
    }

    pub fn report(&self) -> Option<Result<LoadReport, String>> {
        self.outcome.lock().expect("outcome poisoned").clone()
    }

    fn finish(&self, outcome: Result<LoadReport, String>) {
        *self.outcome.lock().expect("outcome poisoned") = Some(outcome);
        self.finished.store(true, Ordering::Relaxed);
    }
}

/// Calibrate, then every stage in order
pub async fn run(target: Target, plan: Plan, state: Arc<LoadRun>) -> Result<LoadReport, LoadError> {
    let raise = rlimit::increase_nofile_limit(u64::MAX);
    tracing::info!(nofile = ?raise, "load driver open-file limit");
    let zebra = Arc::new(Zebra::new(target.zebra.clone()));
    let client = RawClient::connect(&target.uri).await?;
    let latest = client.unary(path::GET_LATEST_BLOCK, encoded(&ChainSpec {})).await?;
    let served = BlockId::decode(latest.as_ref())
        .map_err(|e| tonic::Status::internal(format!("undecodable BlockID: {e}")))?
        .height as u32;
    let tip = served.min(zebra.tip().await?);
    let stable_below = tip.saturating_sub(plan.reorg_margin);
    let chain = Chain::read(&target.zebra).await?;

    let (audits, queue) = mpsc::channel(plan.audit_queue);
    let ledger = Arc::new(Ledger::new(stable_below, plan.audit_every, audits));
    let _ = state.ledger.set(Arc::clone(&ledger));
    state.stages.store(plan.stages.len() as u32, Ordering::Relaxed);
    let expect =
        Expect { chain_name: target.chain_name.clone(), sapling_activation: chain.sapling };
    let auditor = tokio::spawn(
        Auditor::new(Arc::clone(&zebra), Arc::clone(&ledger), expect).run(queue, plan.settle),
    );

    let calibrated = chain.calibration_heights(stable_below, plan.calibration);
    tracing::info!(
        heights = calibrated.len(),
        stable_below,
        "load calibration: served blocks vs zebra"
    );
    audit::calibrate(&zebra, &client, &calibrated, &ledger).await;
    let calibration = ledger.tallies();
    let addresses = Arc::new(chain.addresses(&target.zebra, &calibrated).await);

    let shape = RestoreShape {
        lowest: chain.sapling.max(1),
        highest: stable_below,
        span: plan.restore_span,
        batch: plan.restore_batch,
        pace: None,
        addresses,
    };
    let engine = Engine {
        plan: plan.clone(),
        uri: target.uri.clone().into(),
        ledger: Arc::clone(&ledger),
        server: Arc::clone(&target.server),
        shape,
        state: Arc::clone(&state),
    };
    let (done, stages) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("ztest-load".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("load runtime");
            let _ = done.send(runtime.block_on(engine.run()));
        })
        .map_err(|e| LoadError::Thread(format!("spawning: {e}")))?;
    let stages = stages.await.map_err(|_| LoadError::Thread("died before its report".into()))?;

    ledger.close();
    let _ = auditor.await;
    Ok(LoadReport {
        calibrated,
        calibration,
        stages,
        ledger: ledger.tallies(),
        violations: ledger.violations(),
    })
}

/// Why a run could not start or finish (a wrong answer is a violation, never this)
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("indexer: {0}")]
    Indexer(#[from] tonic::Status),
    #[error("dialing the indexer: {0}")]
    Dial(#[from] EnvError),
    #[error("zebra: {0}")]
    Zebra(#[from] crate::loadtest::reference::ReferenceError),
    #[error("zebra: {0}")]
    ZebraRpc(#[from] RpcError),
    #[error("load thread: {0}")]
    Thread(String),
}

/// Chain facts the plan draws from (zebra's own schedule, never compiled in)
#[derive(Debug)]
struct Chain {
    sapling: u32,
    activations: Vec<u32>,
}

#[derive(Debug, Deserialize)]
struct Upgrade {
    name: String,
    activationheight: u32,
}

#[derive(Debug, Deserialize)]
struct Info {
    upgrades: std::collections::BTreeMap<String, Upgrade>,
}

#[derive(Debug, Deserialize)]
struct Outputs {
    tx: Vec<TxOutputs>,
}

#[derive(Debug, Deserialize)]
struct TxOutputs {
    vout: Vec<OutputAddress>,
}

#[derive(Debug, Deserialize)]
struct OutputAddress {
    #[serde(rename = "scriptPubKey")]
    script: ScriptAddresses,
}

#[derive(Debug, Deserialize)]
struct ScriptAddresses {
    #[serde(default)]
    addresses: Vec<String>,
}

/// Addresses the restores query (enough to spread, few enough to stay auditable)
const ADDRESSES: usize = 24;

impl Chain {
    async fn read(zebra: &JsonRpcClient) -> Result<Self, LoadError> {
        let info: Info = zebra.call("getblockchaininfo", json!([])).await?;
        let sapling =
            info.upgrades.values().find(|u| u.name == "Sapling").map_or(1, |u| u.activationheight);
        let activations = info.upgrades.values().map(|u| u.activationheight).collect();
        Ok(Self { sapling, activations })
    }

    /// Stratified over `[1, stable_below)`: each activation ± 1, the last blocks before the
    /// settled tip, and an even spread (jittered by a hash of the slot) over the rest
    fn calibration_heights(&self, stable_below: u32, spread: usize) -> Vec<u32> {
        let top = stable_below.saturating_sub(1);
        let mut heights: Vec<u32> =
            self.activations.iter().flat_map(|&a| [a.saturating_sub(1), a, a + 1]).collect();
        heights.extend(top.saturating_sub(20)..=top);
        let step = (u64::from(top) / spread.max(1) as u64).max(1);
        for slot in 0..spread as u64 {
            heights.push((slot * step + splitmix64(slot) % step) as u32);
        }
        heights.retain(|&h| h >= 1 && h <= top);
        heights.sort_unstable();
        heights.dedup();
        heights
    }

    async fn addresses(&self, zebra: &JsonRpcClient, heights: &[u32]) -> Vec<String> {
        let mut found = Vec::new();
        for &height in heights.iter().rev() {
            if found.len() >= ADDRESSES {
                break;
            }
            let Ok(block) = zebra.call::<Outputs>("getblock", json!([height.to_string(), 2])).await
            else {
                continue;
            };
            // coinbase skipped: pool payouts (millions of receives) are no light wallet's query
            found.extend(
                block
                    .tx
                    .into_iter()
                    .skip(1)
                    .flat_map(|tx| tx.vout)
                    .flat_map(|o| o.script.addresses),
            );
            found.sort_unstable();
            found.dedup();
        }
        found.truncate(ADDRESSES);
        found
    }
}

/// What the engine thread owns
struct Engine {
    plan: Plan,
    uri: Arc<str>,
    ledger: Arc<Ledger>,
    server: Arc<dyn ServerMeter>,
    shape: RestoreShape,
    state: Arc<LoadRun>,
}

/// One edge of a stage's window
struct Reading {
    at: Instant,
    server_cpu: Option<Duration>,
    driver: Option<CgroupCpu>,
    thread: Option<Duration>,
}

impl Engine {
    async fn run(self) -> Vec<StageReport> {
        let mut reports = Vec::with_capacity(self.plan.stages.len());
        for stage in &self.plan.stages {
            tracing::info!(load = %stage.load, "load stage");
            let report = self.stage(stage).await;
            tracing::info!("load stage done\n{report}");
            reports.push(report);
            self.state.stages_done.fetch_add(1, Ordering::Relaxed);
        }
        reports
    }

    async fn reading(&self) -> Reading {
        Reading {
            at: Instant::now(),
            server_cpu: self
                .server
                .cpu()
                .await
                .inspect_err(|e| tracing::warn!(%e, "server cpu"))
                .ok(),
            driver: CgroupCpu::own(),
            thread: thread_cpu(),
        }
    }

    fn cx(&self, measure: &Arc<Measure>, stop: &CancellationToken, dialing: &Arc<Semaphore>) -> Cx {
        Cx {
            uri: Arc::clone(&self.uri),
            measure: Arc::clone(measure),
            ledger: Arc::clone(&self.ledger),
            stop: stop.clone(),
            dialing: Arc::clone(dialing),
            connected: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn stage(&self, stage: &Stage) -> StageReport {
        let measure = Arc::new(Measure::default());
        let stop = CancellationToken::new();
        let dialing = Arc::new(Semaphore::new(self.plan.dials));
        let (wallets, tick, restores, pace) = stage.load.parts();
        let steady_cx = self.cx(&measure, &stop, &dialing);
        let restore_cx = self.cx(&measure, &stop, &dialing);
        let mut sessions = JoinSet::new();

        if wallets > 0 {
            let (bursts, announced) = watch::channel(None);
            for id in 0..wallets {
                let mobile = id % self.plan.mobile_every.max(1) == 0;
                sessions.spawn(wallet::steady(
                    steady_cx.clone(),
                    id as u64,
                    mobile,
                    announced.clone(),
                ));
            }
            sessions.spawn(coordinator(steady_cx.clone(), bursts, tick));
        }
        for id in 0..restores {
            let shape = RestoreShape { pace, ..self.shape.clone() };
            sessions.spawn(wallet::restore(restore_cx.clone(), id as u64, shape));
        }

        tokio::time::sleep(stage.warmup).await;
        let (tallies_before, before) = (self.ledger.tallies(), self.reading().await);
        measure.open();
        tokio::time::sleep(stage.hold).await;
        measure.close();
        let after = self.reading().await;
        let connected = steady_cx.connected.load(Ordering::Relaxed)
            + restore_cx.connected.load(Ordering::Relaxed);
        let server_memory = self.server.memory().await.ok();

        stop.cancel();
        if tokio::time::timeout(Duration::from_secs(30), async {
            while sessions.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            sessions.abort_all();
        }

        let window = after.at - before.at;
        let per_second = |d: Duration| d.as_secs_f64() / window.as_secs_f64();
        let tallies = self.ledger.tallies();
        StageReport {
            load: stage.load,
            window,
            connected,
            methods: measure.methods(),
            burst_drain: measure.burst_drain(),
            server_cores: before
                .server_cpu
                .zip(after.server_cpu)
                .map(|(b, a)| per_second(a.saturating_sub(b))),
            server_memory,
            client_cores: before
                .thread
                .zip(after.thread)
                .map(|(b, a)| per_second(a.saturating_sub(b))),
            throttled: before
                .driver
                .zip(after.driver)
                .map(|(b, a)| per_second(a.since(b).throttled)),
            ledger: Tallies {
                blocks_held: tallies.blocks_held - tallies_before.blocks_held,
                consistent: tallies.consistent - tallies_before.consistent,
                queued: tallies.queued - tallies_before.queued,
                dropped: tallies.dropped - tallies_before.dropped,
                audited: tallies.audited - tallies_before.audited,
                skipped: tallies.skipped - tallies_before.skipped,
                violations: tallies.violations - tallies_before.violations,
            },
        }
    }
}

/// Announces bursts: on each real new tip, and every `tick` between (never over an undrained
/// burst, unless it has run past twice the tick)
async fn coordinator(cx: Cx, bursts: watch::Sender<Option<Arc<Burst>>>, tick: Duration) {
    let Ok(client) = RawClient::connect(&cx.uri).await else {
        return;
    };
    let mut seen = None;
    let mut next = Instant::now() + tick;
    let mut announced_at = Instant::now();
    while !cx.stop.is_cancelled() {
        tokio::select! {
            _ = cx.stop.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
        let tip = match client.unary(path::GET_LATEST_BLOCK, encoded(&ChainSpec {})).await {
            Ok(answer) => BlockId::decode(answer.as_ref()).ok().map(|id| id.height),
            Err(_) => None,
        };
        let mined = tip.is_some() && seen.is_some() && tip != seen;
        seen = tip.or(seen);
        if !mined && Instant::now() < next {
            continue;
        }
        let busy = bursts.borrow().as_ref().is_some_and(|b| !b.drained());
        if busy && announced_at.elapsed() < tick * 2 {
            continue;
        }
        bursts.send_replace(Some(Arc::new(Burst::new(cx.connected.load(Ordering::Relaxed)))));
        announced_at = Instant::now();
        next = announced_at + tick;
    }
}

/// A load run as a sync phase: progress = stages done of the plan's
#[derive(Debug)]
pub struct LoadSubject {
    run: Option<(Target, Plan)>,
    state: Arc<LoadRun>,
}

impl LoadSubject {
    pub fn new(target: Target, plan: Plan) -> Self {
        Self { run: Some((target, plan)), state: Arc::new(LoadRun::default()) }
    }

    /// Live handle for probes (violations) and the report once done
    pub fn state(&self) -> Arc<LoadRun> {
        Arc::clone(&self.state)
    }
}

#[derive(Debug)]
struct LoadProgress {
    done: u32,
    total: u32,
}

impl ProgressView for LoadProgress {
    fn height(&self) -> u32 {
        self.done
    }
    fn target(&self) -> Option<u32> {
        Some(self.total)
    }
}

#[async_trait]
impl SyncSubject for LoadSubject {
    async fn launch(&mut self) -> Result<(), RpcError> {
        let Some((target, plan)) = self.run.take() else {
            return Ok(());
        };
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let outcome = run(target, plan, Arc::clone(&state)).await.map_err(|e| e.to_string());
            match &outcome {
                Ok(report) => tracing::info!("load report\n{report}"),
                Err(error) => tracing::error!(%error, "load run failed"),
            }
            state.finish(outcome);
        });
        Ok(())
    }

    async fn progress(&self) -> Result<Box<dyn ProgressView>, RpcError> {
        Ok(Box::new(LoadProgress {
            done: self.state.stages_done.load(Ordering::Relaxed),
            total: self.state.stages.load(Ordering::Relaxed),
        }))
    }

    async fn is_complete(&self) -> bool {
        self.state.finished.load(Ordering::Relaxed)
    }

    async fn failure(&self) -> Option<String> {
        match self.state.report() {
            Some(Err(error)) => Some(error),
            _ => None,
        }
    }
}
