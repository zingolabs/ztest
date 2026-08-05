//! The test-author `SyncRunner` facade (design §"Test-author API").
//!
//! `#[ztest::sync_test]` hands the body a `SyncRunner`. The body is a
//! registration program: build the topology, bind the subject, register named
//! invariants at cadences, schedule the nemesis, then `run.run()`. This facade
//! collects that registration and, on `run()`, builds the low-level
//! [`SyncEngine`](crate::sync::SyncEngine) over the bound subject and executes.
//!
//! Runtime note: `topology()` and `run()` provision a real cluster (via
//! [`TestEnv`]) — verified on a cluster, not in unit tests. The registration +
//! [`manifest`](SyncRunner::manifest) path is cluster-free (powers `describe`).

use std::time::Duration;

use crate::backends::librustzcash::LrzWallet;
use crate::env::TestEnv;
use crate::error::EnvError;
use crate::handles::wallet::{Account, AccountId};

use super::nemesis::{Nemesis, NemesisBuilder};
use super::probe::{Cadence, Class, ProbeBuilder, ProbeSpec, Severity, SyncCtx};
use super::reporter::EventReporter;
use super::runner::{SyncEngine, SyncOutcome, SyncVerdict};

/// How aggressively the wallet sync downloads and scans — the block batch size
/// [`sync_subject`](LrzWallet::sync_subject) drives `zcash_client_backend::sync`
/// with. A ztest-owned knob (not the wallet library's own type) so a
/// `#[ztest::sync_test]` body sets it without depending on the backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PerformanceLevel {
    /// Small batches — least memory, most round-trips.
    Low,
    /// Balanced default.
    Medium,
    /// Large batches — fastest drive-to-tip on a short regtest chain.
    High,
}

impl PerformanceLevel {
    /// Compact-block batch size for `zcash_client_backend::sync::run`.
    pub(crate) fn batch_size(self) -> u32 {
        match self {
            PerformanceLevel::Low => 25,
            PerformanceLevel::Medium => 100,
            PerformanceLevel::High => 1_000,
        }
    }
}

/// The sync subject a profile binds via `run.sync(Subject::wallet(account))`.
#[derive(Debug)]
pub struct Subject {
    kind: SubjectKind,
}

#[derive(Debug)]
enum SubjectKind {
    Wallet {
        wallet: LrzWallet,
        account: AccountId,
        performance: Option<PerformanceLevel>,
    },
}

impl Subject {
    /// Drive an in-process wallet account (ztest owns the sync engine).
    pub fn wallet(account: &Account<LrzWallet>) -> Self {
        Subject {
            kind: SubjectKind::Wallet {
                wallet: account.wallet().clone(),
                account: account.id(),
                performance: None,
            },
        }
    }

    /// Override the sync performance level (compact-block batch size).
    pub fn performance(mut self, level: PerformanceLevel) -> Self {
        match &mut self.kind {
            SubjectKind::Wallet { performance, .. } => *performance = Some(level),
        }
        self
    }
}

/// A static, cluster-free summary of what a profile registered — the body's
/// invariant + nemesis manifest for `ztest sync describe`.
#[derive(Debug, Clone)]
pub struct SyncManifest {
    /// Registered probe names with their class.
    pub probes: Vec<(String, Class)>,
    /// Named scheduled faults.
    pub scheduled_faults: Vec<String>,
    /// Count of probabilistic channel rules.
    pub buggify_rules: usize,
    /// The nemesis seed.
    pub seed: u64,
}

/// The test-author-facing runner.
pub struct SyncRunner {
    env: TestEnv,
    probes: Vec<ProbeSpec>,
    nemesis: Nemesis,
    subject: Option<Subject>,
    tick: Duration,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for SyncRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncRunner")
            .field("probes", &self.probes.len())
            .field("bound_subject", &self.subject.is_some())
            .field("scheduled_faults", &self.nemesis.scheduled.len())
            .finish_non_exhaustive()
    }
}

impl Default for SyncRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncRunner {
    /// A fresh runner. The macro constructs this and passes it to the body.
    pub fn new() -> Self {
        Self {
            env: TestEnv::builder(),
            probes: Vec::new(),
            nemesis: Nemesis::default(),
            subject: None,
            tick: Duration::from_secs(5),
            timeout: None,
        }
    }

    /// Build the topology: the closure adds validators/indexers/wallets to the
    /// `TestEnv` and returns their handles; this provisions the cluster and
    /// hands the handles back. (Cluster-bound.)
    pub async fn topology<F, R>(&mut self, f: F) -> Result<R, EnvError>
    where
        F: FnOnce(&mut TestEnv) -> R,
    {
        let handles = f(&mut self.env);
        self.env.build().await?;
        Ok(handles)
    }

    /// Bind the subject to sync/observe.
    pub fn sync(&mut self, subject: Subject) {
        self.subject = Some(subject);
    }

    /// Base sampling interval (default 5 s).
    pub fn tick(&mut self, tick: Duration) -> &mut Self {
        self.tick = tick;
        self
    }
    /// Overall run cap.
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    /// Register a safety invariant (true at every tick).
    pub fn always(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Always, severity)
    }
    /// Register a liveness invariant (must (re)satisfy within its `window`).
    pub fn eventually(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Eventually, severity)
    }
    /// Register a coverage invariant (true on ≥1 tick over the run).
    pub fn sometimes(&mut self) -> ProbeBuilder<'_> {
        self.builder(Class::Sometimes, Severity::Fatal)
    }
    /// Register a terminal post-condition (evaluated once at tip).
    pub fn at_completion(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::AtCompletion, severity)
    }

    fn builder(&mut self, class: Class, severity: Severity) -> ProbeBuilder<'_> {
        let cadence = match class {
            Class::Eventually => Cadence::Window(Duration::MAX),
            _ => Cadence::EachTick,
        };
        ProbeBuilder {
            sink: &mut self.probes,
            class,
            severity,
            cadence,
            after: None,
            name: None,
            hold_for: None,
        }
    }

    /// Configure the chaos schedule (`run.nemesis().at(..).partition(..)...`).
    pub fn nemesis(&mut self) -> NemesisBuilder<'_> {
        self.nemesis.builder()
    }

    /// The cluster-free registration manifest (for `describe`).
    pub fn manifest(&self) -> SyncManifest {
        SyncManifest {
            probes: self
                .probes
                .iter()
                .map(|p| (p.name.clone(), p.class))
                .collect(),
            scheduled_faults: self
                .nemesis
                .scheduled
                .iter()
                .filter_map(|f| f.name.clone())
                .collect(),
            buggify_rules: self.nemesis.buggify.len(),
            seed: self.nemesis.seed,
        }
    }

    /// Provision (if not already), bind the engine over the subject, and run to
    /// completion. (Cluster-bound.)
    pub async fn run(self) -> SyncOutcome {
        let Some(subject) = self.subject else {
            return errored("run.sync(..) must be called before run.run()");
        };
        match subject.kind {
            SubjectKind::Wallet {
                wallet,
                account,
                performance,
            } => {
                let ws = match wallet.sync_subject(account, performance).await {
                    Ok(ws) => ws,
                    Err(e) => return errored(format!("bind wallet subject: {e}")),
                };
                // The topology's indexer is the independent authority RPC-backed
                // probes (`tree_root_matches_indexer`, `chain_continuity`) check
                // the wallet against; without it `cx.indexer()` is `None` and
                // those probes cannot run.
                let oracle = match self.env.single_indexer().await {
                    Ok(ix) => ix,
                    Err(e) => return errored(format!("bind sync oracle: {e}")),
                };
                let detached = super::active_sync_id();
                let profile = std::env::var(super::SYNC_PROFILE_ENV).unwrap_or_default();
                let probe_count = self.probes.len();
                let mut engine = SyncEngine::new(ws)
                    .with_probes(self.probes)
                    .with_tick(self.tick)
                    .with_ctx(SyncCtx::new(Some(oracle)));
                if let Some(t) = self.timeout {
                    engine = engine.with_timeout(t);
                }
                // Detached: the driver's log is the only channel to a terminal
                // watching this run, so publish the engine's live state there as
                // an event stream. A local run keeps the default silent reporter —
                // `cargo test` has no watcher and its stdout is the test's own.
                if let Some(sync_id) = &detached {
                    engine = engine.with_reporter(Box::new(EventReporter::new(
                        sync_id,
                        &profile,
                        self.tick,
                        probe_count,
                    )));
                }
                // Detached (`ztest sync start`) mode: the pod runs the same body,
                // but `ztest sync stop` (or a SIGTERM on node loss / `rm`) must
                // checkpoint gracefully, not kill. Arm the in-pod stop-watch and
                // route it into the engine's cancellation.
                if let (Some(sync_id), Some(kube), Some(ns)) =
                    (&detached, self.env.kube_client(), self.env.namespace())
                {
                    let cancel = super::detached::watch_stop(&kube, &ns).await;
                    engine = engine.with_cancel(cancel);
                    tracing::info!(sync_id = %sync_id, "detached sync: stop-watch armed");
                }
                // Nemesis application — wrapping the client in a `ChaosIndexer`
                // for buggify rules, and applying the scheduled k8s
                // `NetworkChaos` on a timer — is the cluster-side wiring; the
                // recorded schedule (`self.nemesis`) is what `describe` prints.
                let mut outcome = engine.run().await;
                // Attach the run's server-side metrics (best-effort). The engine
                // has no cluster handle; the facade owns the env, so it queries
                // thanos here, scoped to this run's namespace.
                if let (Some(kube), Some(ns)) = (self.env.kube_client(), self.env.namespace()) {
                    outcome.metrics = crate::metrics::query_run_metrics(&kube, &ns).await;
                    if !outcome.metrics.is_empty() {
                        for s in &outcome.metrics.samples {
                            if let Some(v) = s.value {
                                tracing::info!(metric = %s.name, value = v, "server-side metric");
                            }
                        }
                    }
                }
                // Collect any profiled component's artifacts (flamegraph.svg +
                // profile.pb, best-effort). After the metrics read, since
                // draining the pods here would otherwise race the last scrape.
                // Written under the artifact dir; paths logged for the parent's
                // runner→laptop hop.
                let dest = std::path::PathBuf::from(
                    std::env::var("ZTEST_ARTIFACT_DIR")
                        .unwrap_or_else(|_| "ztest-artifacts".to_string()),
                );
                for artifact in self.env.collect_profiles(&dest).await {
                    tracing::info!(artifact = %artifact.display(), "collected profile artifact");
                }
                // Mirror the final report to a ConfigMap so `ztest sync report`
                // / `status` work after the pod is gone (detached mode only).
                if let (Some(sync_id), Some(kube), Some(ns)) =
                    (&detached, self.env.kube_client(), self.env.namespace())
                {
                    let report = super::SyncReportMirror::from_outcome(sync_id, &profile, &outcome);
                    super::detached::write_report(&kube, &ns, &report).await;
                    tracing::info!(sync_id = %sync_id, "detached sync: report mirrored");
                }
                outcome
            }
        }
    }
}

/// Build an errored outcome for a facade-level failure (bad wiring / bind).
fn errored(msg: impl Into<String>) -> SyncOutcome {
    SyncOutcome {
        verdict: SyncVerdict::Errored,
        violations: Vec::new(),
        coverage_gaps: Vec::new(),
        error: Some(msg.into()),
        ticks: 0,
        dropped_snapshots: 0,
        metrics: crate::metrics::MetricsSummary::default(),
    }
}
