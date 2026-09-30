//! What a load stage measured: client-observed latency, bytes, codes; burst drain; CPU on
//! both sides.
//!
//! - In-process `hdrhistogram` = the stage report's exact percentiles; the same samples feed
//!   the driver's Prometheus exporter (`ztest_load_*`) for Grafana
//! - Series handles resolved once per method (a per-request registry lookup = client CPU the
//!   1-core driver cannot spare)
//! - Recording gated by [`Measure::open`]: warm-up and ramp traffic never lands in a window

use std::collections::BTreeMap;
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
}

impl Method {
    pub const ALL: [Method; 9] = [
        Method::Connect,
        Method::GetLatestBlock,
        Method::GetBlock,
        Method::GetBlockRange,
        Method::GetTreeState,
        Method::GetSubtreeRoots,
        Method::GetLightdInfo,
        Method::GetMempoolStream,
        Method::GetAddressUtxos,
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

/// One method's window; latency = request sent → last byte of the answer
struct MethodTally {
    latency: Mutex<Histogram<u64>>,
    codes: Mutex<BTreeMap<i32, u64>>,
    bytes: AtomicU64,
    prom_latency: metrics::Histogram,
    prom_bytes: metrics::Counter,
    prom_ok: metrics::Counter,
}

/// One stage's measurements (a new one per stage)
pub struct Measure {
    open: AtomicBool,
    methods: [MethodTally; Method::ALL.len()],
    bursts: Mutex<Histogram<u64>>,
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
            bursts: Mutex::new(histogram()),
            prom_drain: metrics::histogram!(family::BURST_DRAIN),
        }
    }
}

impl Measure {
    /// Samples from here on count (warm-up over)
    pub fn open(&self) {
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

    /// One burst: trigger → the last wallet done
    pub fn burst(&self, drain: Duration) {
        self.prom_drain.record(drain.as_secs_f64());
        if self.is_open() {
            let micros = drain.as_micros().max(1) as u64;
            let _ = self.bursts.lock().expect("bursts poisoned").record(micros);
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

    pub fn burst_drain(&self) -> Option<LatencyStats> {
        let bursts = self.bursts.lock().expect("bursts poisoned");
        (!bursts.is_empty()).then(|| LatencyStats::from_hist(&bursts))
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

/// CPU time the calling thread has run (`/proc/thread-self/schedstat`, ns): the engine thread's
/// own share of the driver, apart from the auditor's
pub fn thread_cpu() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/thread-self/schedstat").ok()?;
    Some(Duration::from_nanos(stat.split_whitespace().next()?.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Samples before `open` feed Prometheus only; after, the window too, by code and bytes
    #[test]
    fn only_the_open_window_lands_in_the_stage_report() {
        let measure = Measure::default();
        measure.request(Method::GetBlock, Duration::from_millis(9), 100, Code::Ok);
        assert!(measure.methods().is_empty(), "warm-up = no window sample");

        measure.open();
        measure.request(Method::GetBlock, Duration::from_millis(2), 100, Code::Ok);
        measure.request(Method::GetBlock, Duration::from_millis(3), 0, Code::Unavailable);
        measure.streamed(Method::GetBlockRange, 5_000);
        measure.burst(Duration::from_millis(250));
        let windows = measure.methods();
        let block = windows.iter().find(|w| w.method == Method::GetBlock).expect("GetBlock");
        assert_eq!((block.ok, block.bytes, block.latency.count), (1, 100, 1));
        assert_eq!(block.failed, BTreeMap::from([("Unavailable", 1)]));
        let range = windows.iter().find(|w| w.method == Method::GetBlockRange).expect("range");
        assert_eq!((range.ok, range.bytes), (0, 5_000));
        assert_eq!(measure.burst_drain().map(|d| d.count), Some(1));

        let stat = "usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\nnr_periods 10\n\
                    nr_throttled 2\nthrottled_usec 300000\n";
        let cpu = CgroupCpu::parse(stat).expect("parses");
        let want =
            CgroupCpu { usage: Duration::from_millis(2500), throttled: Duration::from_millis(300) };
        assert_eq!(cpu, want);
    }
}
