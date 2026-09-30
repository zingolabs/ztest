//! What a load stage measured: client-observed latency, bytes, codes; burst drain; CPU on
//! both sides.
//!
//! - In-process `hdrhistogram` = the stage report's exact percentiles; the same samples feed
//!   the driver's Prometheus exporter (`ztest_load_*`) for Grafana
//! - Series handles resolved once per method (a per-request registry lookup = client CPU the
//!   driver cannot spare)
//! - Recording gated by [`Measure::open`]: warm-up and ramp traffic never lands in a window

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use hdrhistogram::Histogram;
use tonic::Code;

use crate::loadtest::report::LatencyStats;

/// Every RPC a simulated wallet issues, and its dial
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Method {
    Connect,
    GetLatestBlock,
    GetBlock,
    GetBlockRange,
    GetTreeState,
    GetSubtreeRoots,
    GetLightdInfo,
    GetMempoolStream,
    GetAddressUtxos,
    GetTaddressTxids,
}

impl Method {
    pub const ALL: [Method; 10] = [
        Method::Connect,
        Method::GetLatestBlock,
        Method::GetBlock,
        Method::GetBlockRange,
        Method::GetTreeState,
        Method::GetSubtreeRoots,
        Method::GetLightdInfo,
        Method::GetMempoolStream,
        Method::GetAddressUtxos,
        Method::GetTaddressTxids,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Method::Connect => "Connect",
            Method::GetLatestBlock => "GetLatestBlock",
            Method::GetBlock => "GetBlock",
            Method::GetBlockRange => "GetBlockRange",
            Method::GetTreeState => "GetTreeState",
            Method::GetSubtreeRoots => "GetSubtreeRoots",
            Method::GetLightdInfo => "GetLightdInfo",
            Method::GetMempoolStream => "GetMempoolStream",
            Method::GetAddressUtxos => "GetAddressUtxos",
            Method::GetTaddressTxids => "GetTaddressTxids",
        }
    }
}

mod family {
    pub const REQUESTS: &str = "ztest_load_requests_total";
    pub const LATENCY: &str = "ztest_load_latency_seconds";
    pub const RECEIVED: &str = "ztest_load_received_bytes_total";
    pub const BURST_DRAIN: &str = "ztest_load_burst_drain_seconds";
}

fn histogram() -> Histogram<u64> {
    Histogram::new(3).expect("3 significant figures is valid")
}

/// One method's window; latency = request sent → whole unary answer / a stream's first message
struct MethodTally {
    latency: Mutex<Histogram<u64>>,
    codes: Mutex<BTreeMap<i32, u64>>,
    bytes: AtomicU64,
    prom_latency: metrics::Histogram,
    prom_bytes: metrics::Counter,
    prom_ok: metrics::Counter,
}

/// `busiest` = highest engine utilisation over any one burst's drain (1.0 = every worker on-CPU)
struct Bursts {
    drain: Histogram<u64>,
    busiest: Option<f64>,
}

/// One fleet's measurements, windowed by [`open`](Measure::open) / [`close`](Measure::close)
pub struct Measure {
    open: AtomicBool,
    methods: [MethodTally; Method::ALL.len()],
    bursts: Mutex<Bursts>,
    prom_drain: metrics::Histogram,
}

impl std::fmt::Debug for Measure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Measure").field("open", &self.is_open()).finish_non_exhaustive()
    }
}

impl Default for Measure {
    fn default() -> Self {
        Self {
            open: AtomicBool::new(false),
            methods: Method::ALL.map(|method| MethodTally {
                latency: Mutex::new(histogram()),
                codes: Mutex::new(BTreeMap::new()),
                bytes: AtomicU64::new(0),
                prom_latency: metrics::histogram!(family::LATENCY, "method" => method.name()),
                prom_bytes: metrics::counter!(family::RECEIVED, "method" => method.name()),
                prom_ok: metrics::counter!(family::REQUESTS, "method" => method.name(), "code" => "Ok"),
            }),
            bursts: Mutex::new(Bursts { drain: histogram(), busiest: None }),
            prom_drain: metrics::histogram!(family::BURST_DRAIN),
        }
    }
}

impl Measure {
    /// New window: samples from here on count, earlier windows' dropped
    pub fn open(&self) {
        self.close();
        for tally in &self.methods {
            tally.latency.lock().expect("latency poisoned").reset();
            tally.codes.lock().expect("codes poisoned").clear();
            tally.bytes.store(0, Ordering::Relaxed);
        }
        *self.bursts.lock().expect("bursts poisoned") =
            Bursts { drain: histogram(), busiest: None };
        self.open.store(true, Ordering::Relaxed);
    }

    pub fn close(&self) {
        self.open.store(false, Ordering::Relaxed);
    }

    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Relaxed)
    }

    /// One finished request (`bytes` = message bytes received, framing excluded)
    pub fn request(&self, method: Method, elapsed: Duration, bytes: u64, code: Code) {
        let tally = &self.methods[method as usize];
        if code == Code::Ok {
            tally.prom_ok.increment(1);
            tally.prom_latency.record(elapsed.as_secs_f64());
        } else {
            metrics::counter!(family::REQUESTS, "method" => method.name(), "code" => code_name(code))
                .increment(1);
        }
        tally.prom_bytes.increment(bytes);
        if !self.is_open() {
            return;
        }
        if code == Code::Ok {
            let micros = elapsed.as_micros().max(1) as u64;
            let _ = tally.latency.lock().expect("latency poisoned").record(micros);
        }
        *tally.codes.lock().expect("codes poisoned").entry(code as i32).or_insert(0) += 1;
        tally.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Bytes a stream delivered before its end (counted as they arrive: a restore's bytes are
    /// its throughput, whenever its stream ends)
    pub fn streamed(&self, method: Method, bytes: u64) {
        let tally = &self.methods[method as usize];
        tally.prom_bytes.increment(bytes);
        if self.is_open() {
            tally.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// One burst: trigger → the last wallet done; `busy` = engine utilisation across that drain
    pub fn burst(&self, drain: Duration, busy: Option<f64>) {
        self.prom_drain.record(drain.as_secs_f64());
        if self.is_open() {
            let micros = drain.as_micros().max(1) as u64;
            let mut bursts = self.bursts.lock().expect("bursts poisoned");
            let _ = bursts.drain.record(micros);
            bursts.busiest = bursts.busiest.into_iter().chain(busy).reduce(f64::max);
        }
    }

    pub fn methods(&self) -> Vec<MethodWindow> {
        Method::ALL
            .iter()
            .zip(&self.methods)
            .filter_map(|(method, tally)| {
                let codes = tally.codes.lock().expect("codes poisoned").clone();
                let bytes = tally.bytes.load(Ordering::Relaxed);
                (!codes.is_empty() || bytes > 0).then(|| MethodWindow {
                    method: *method,
                    latency: LatencyStats::from_hist(
                        &tally.latency.lock().expect("latency poisoned"),
                    ),
                    ok: codes.get(&(Code::Ok as i32)).copied().unwrap_or(0),
                    failed: codes
                        .into_iter()
                        .filter(|(code, _)| *code != Code::Ok as i32)
                        .map(|(code, n)| (code_name(Code::from_i32(code)), n))
                        .collect(),
                    bytes,
                })
            })
            .collect()
    }

    /// p99 over every answered request of the window (dials excluded: a ramp artefact)
    pub fn p99(&self) -> Option<Duration> {
        let mut all = histogram();
        for (method, tally) in Method::ALL.iter().zip(&self.methods) {
            if *method != Method::Connect {
                all.add(&*tally.latency.lock().expect("latency poisoned"))
                    .expect("same-precision histograms merge");
            }
        }
        (!all.is_empty()).then(|| Duration::from_micros(all.value_at_quantile(0.99)))
    }

    pub fn burst_drain(&self) -> Option<LatencyStats> {
        let bursts = self.bursts.lock().expect("bursts poisoned");
        (!bursts.drain.is_empty()).then(|| LatencyStats::from_hist(&bursts.drain))
    }

    pub fn busiest_burst(&self) -> Option<f64> {
        self.bursts.lock().expect("bursts poisoned").busiest
    }
}

/// One method over one stage's window
#[derive(Debug, Clone)]
pub struct MethodWindow {
    pub method: Method,
    pub latency: LatencyStats,
    pub ok: u64,
    pub failed: BTreeMap<&'static str, u64>,
    pub bytes: u64,
}

pub fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "Ok",
        Code::Cancelled => "Cancelled",
        Code::Unknown => "Unknown",
        Code::InvalidArgument => "InvalidArgument",
        Code::DeadlineExceeded => "DeadlineExceeded",
        Code::NotFound => "NotFound",
        Code::AlreadyExists => "AlreadyExists",
        Code::PermissionDenied => "PermissionDenied",
        Code::ResourceExhausted => "ResourceExhausted",
        Code::FailedPrecondition => "FailedPrecondition",
        Code::Aborted => "Aborted",
        Code::OutOfRange => "OutOfRange",
        Code::Unimplemented => "Unimplemented",
        Code::Internal => "Internal",
        Code::Unavailable => "Unavailable",
        Code::DataLoss => "DataLoss",
        Code::Unauthenticated => "Unauthenticated",
    }
}

/// CPU of the pod this code runs in: cgroup v2 `cpu.stat`
///
/// - `throttled` = time the kernel held the pod off-CPU at its limit (> 0 = the driver, not the
///   server, set the pace)
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CgroupCpu {
    pub usage: Duration,
    pub throttled: Duration,
}

impl CgroupCpu {
    /// `None` off a cgroup-v2 host (a laptop run), never a guessed zero
    pub fn own() -> Option<Self> {
        Self::parse(&std::fs::read_to_string("/sys/fs/cgroup/cpu.stat").ok()?)
    }

    pub fn parse(stat: &str) -> Option<Self> {
        let field = |name: &str| {
            stat.lines().find_map(|line| {
                let (key, value) = line.split_once(' ')?;
                (key == name).then(|| value.trim().parse::<u64>().ok()).flatten()
            })
        };
        Some(Self {
            usage: Duration::from_micros(field("usage_usec")?),
            throttled: Duration::from_micros(field("throttled_usec").unwrap_or(0)),
        })
    }

    pub fn since(self, earlier: Self) -> Self {
        Self {
            usage: self.usage.saturating_sub(earlier.usage),
            throttled: self.throttled.saturating_sub(earlier.throttled),
        }
    }
}

/// CPU the load engine's own threads have run (apart from the auditor's + the harness's, which
/// share the driver's cgroup)
///
/// - Threads enlist / retire themselves (`runtime::Builder::on_thread_start` / `on_thread_stop`)
#[derive(Debug)]
pub struct EngineCpu {
    workers: usize,
    threads: Mutex<EngineThreads>,
}

#[derive(Debug, Default)]
struct EngineThreads {
    live: Vec<PathBuf>,
    retired: Duration,
}

impl EngineCpu {
    pub fn new(workers: usize) -> Self {
        Self { workers, threads: Mutex::default() }
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    pub fn enlist(&self) {
        if let Some(stat) = own_schedstat() {
            self.threads.lock().expect("engine threads poisoned").live.push(stat);
        }
    }

    /// Keeps an exiting thread's CPU in the total (blocking-pool threads come and go per dial)
    pub fn retire(&self) {
        let Some(stat) = own_schedstat() else {
            return;
        };
        let mut threads = self.threads.lock().expect("engine threads poisoned");
        threads.live.retain(|live| *live != stat);
        threads.retired += on_cpu(&stat).unwrap_or_default();
    }

    /// `None` off Linux procfs (a laptop run), never a guessed zero
    pub fn used(&self) -> Option<Duration> {
        let threads = self.threads.lock().expect("engine threads poisoned");
        if threads.live.is_empty() {
            return None;
        }
        Some(
            threads.retired + threads.live.iter().filter_map(|stat| on_cpu(stat)).sum::<Duration>(),
        )
    }

    /// Share of the workers' time spent on-CPU between two [`used`](Self::used) readings
    pub fn busy(&self, used: Duration, over: Duration) -> f64 {
        used.as_secs_f64() / (over.as_secs_f64() * self.workers as f64)
    }
}

/// `/proc/<pid>/task/<tid>/schedstat` of the calling thread (readable from any thread)
fn own_schedstat() -> Option<PathBuf> {
    let task = std::fs::read_link("/proc/thread-self").ok()?;
    Some(Path::new("/proc").join(task).join("schedstat"))
}

/// First `schedstat` field = ns on-CPU
fn on_cpu(schedstat: &Path) -> Option<Duration> {
    let stat = std::fs::read_to_string(schedstat).ok()?;
    Some(Duration::from_nanos(stat.split_whitespace().next()?.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Samples before `open` feed Prometheus only; after, the window too, by code and bytes
    #[test]
    fn only_the_open_window_lands_in_the_level_report() {
        let measure = Measure::default();
        measure.request(Method::GetBlock, Duration::from_millis(9), 100, Code::Ok);
        assert!(measure.methods().is_empty(), "warm-up = no window sample");

        measure.open();
        measure.request(Method::GetBlock, Duration::from_millis(2), 100, Code::Ok);
        measure.request(Method::GetBlock, Duration::from_millis(3), 0, Code::Unavailable);
        measure.streamed(Method::GetBlockRange, 5_000);
        measure.burst(Duration::from_millis(250), Some(0.4));
        measure.burst(Duration::from_millis(300), Some(0.9));
        measure.burst(Duration::from_millis(200), None);
        let windows = measure.methods();
        let block = windows.iter().find(|w| w.method == Method::GetBlock).expect("GetBlock");
        assert_eq!((block.ok, block.bytes, block.latency.count), (1, 100, 1));
        assert_eq!(block.failed, BTreeMap::from([("Unavailable", 1)]));
        let range = windows.iter().find(|w| w.method == Method::GetBlockRange).expect("range");
        assert_eq!((range.ok, range.bytes), (0, 5_000));
        assert_eq!(measure.burst_drain().map(|d| d.count), Some(3));
        assert_eq!(measure.busiest_burst(), Some(0.9));
        measure.request(Method::Connect, Duration::from_secs(5), 0, Code::Ok);
        measure.request(Method::GetTreeState, Duration::from_millis(40), 10, Code::Ok);
        assert_eq!(measure.p99().map(|p| p.as_millis()), Some(40), "dials excluded");

        measure.open();
        assert!(measure.methods().is_empty(), "a new window starts empty");
        assert_eq!((measure.p99(), measure.burst_drain().is_none()), (None, true));

        let stat = "usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\nnr_periods 10\n\
                    nr_throttled 2\nthrottled_usec 300000\n";
        let cpu = CgroupCpu::parse(stat).expect("parses");
        let want =
            CgroupCpu { usage: Duration::from_millis(2500), throttled: Duration::from_millis(300) };
        assert_eq!(cpu, want);
    }
}
