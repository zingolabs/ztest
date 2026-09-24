//! Test-author `SyncRunner` facade (design §"Test-author API").
//!
//! - Body = registration program: topology → bind subject → named invariants at
//!   cadences → later phases (`then`) → nemesis schedule → `run.run()`
//! - Derefs to the first [`Phase`] → `run.always(..)`/`run.tick(..)` register there
//! - `topology()`/`run()` are cluster-bound ([`TestEnv`]); registration +
//!   [`manifest`](SyncRunner::manifest) is cluster-free (powers `describe`)

use std::future::Future;
use std::sync::Arc;

use crate::env::TestEnv;
use crate::error::EnvError;
use crate::handles::pod::Watched;

use super::nemesis::{Nemesis, NemesisBuilder};
use super::phase::{Phase, PhaseError, Source};
use super::probe::{Class, SyncCtx};
use super::runner::{SyncEngine, SyncOutcome};
use super::subject::SyncSubject;

/// Cluster-free summary of a profile's registrations: phases + their invariants + nemesis,
/// for `ztest sync describe`
#[derive(Debug, Clone)]
pub struct SyncManifest {
    pub phases: Vec<PhaseManifest>,
    pub scheduled_faults: Vec<String>,
    pub buggify_rules: usize,
    pub seed: u64,
}

#[derive(Debug, Clone)]
pub struct PhaseManifest {
    pub name: String,
    pub probes: Vec<(String, Class)>,
}

pub struct SyncRunner {
    env: TestEnv,
    first: Phase,
    rest: Vec<Phase>,
    nemesis: Nemesis,
}

impl std::fmt::Debug for SyncRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncRunner")
            .field("first", &self.first)
            .field("rest", &self.rest)
            .field("scheduled_faults", &self.nemesis.scheduled.len())
            .finish_non_exhaustive()
    }
}

impl Default for SyncRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for SyncRunner {
    type Target = Phase;
    fn deref(&self) -> &Phase {
        &self.first
    }
}
impl std::ops::DerefMut for SyncRunner {
    fn deref_mut(&mut self) -> &mut Phase {
        &mut self.first
    }
}

impl SyncRunner {
    pub fn new() -> Self {
        Self {
            env: TestEnv::builder(),
            first: Phase::new("sync", Source::Unbound),
            rest: Vec::new(),
            nemesis: Nemesis::default(),
        }
    }

    /// Closure adds validators/indexers/wallets and returns handles; this
    /// provisions the cluster and hands them back. (Cluster-bound)
    pub async fn topology<F, R>(&mut self, f: F) -> Result<R, EnvError>
    where
        F: FnOnce(&mut TestEnv) -> R,
    {
        let handles = f(&mut self.env);
        self.env.build().await?;
        Ok(handles)
    }

    /// Bind what the first phase watches. Any [`SyncSubject`] — ztest's backends implement
    /// it, and so can a consuming crate's own component; the harness never names an engine
    pub fn sync(&mut self, subject: impl SyncSubject + 'static) {
        self.first.source = Source::Bound(Box::new(subject));
    }

    /// Later phase; its probes/knobs registered on the returned [`Phase`]
    ///
    /// - `build(ctx)` → subject, run at phase start (e.g. wallet needing a serving indexer)
    /// - Starts only after the previous phase completed with no fatal violation
    /// - Own tick/timeout/`requires_work`/probes; nothing inherited from phase 1
    pub fn then<F, Fut, S>(&mut self, name: impl Into<String>, build: F) -> &mut Phase
    where
        F: FnOnce(SyncCtx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, PhaseError>> + Send + 'static,
        S: SyncSubject + 'static,
    {
        let index = self.rest.len();
        self.rest.push(Phase::deferred(name, build));
        &mut self.rest[index]
    }

    /// Chain this run is pinned to: read from the artifact manifest at compile
    /// time, verified against the validator in [`topology`](Self::topology) (so a
    /// probe asserts a fact neither subject nor validator produced).
    ///
    /// # Panics
    ///
    /// Before [`topology`](Self::topology), or with no restored chain archive.
    /// See [`TestEnv::chain`]
    pub fn chain(&self) -> crate::ChainSnapshot {
        self.env.chain()
    }

    /// Configure the chaos schedule (`run.nemesis().at(..).partition(..)...`)
    pub fn nemesis(&mut self) -> NemesisBuilder<'_> {
        self.nemesis.builder()
    }

    /// Cluster-free registration manifest (for `describe`)
    pub fn manifest(&self) -> SyncManifest {
        SyncManifest {
            phases: std::iter::once(&self.first)
                .chain(&self.rest)
                .map(|phase| PhaseManifest {
                    name: phase.name.clone(),
                    probes: phase.probes.iter().map(|p| (p.name.clone(), p.class)).collect(),
                })
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

    /// Provision if needed, bind the engine over the phases, run to completion.
    /// (Cluster-bound.)
    pub async fn run(self) -> SyncOutcome {
        let SyncRunner { env, first, rest, nemesis } = self;
        if matches!(first.source, Source::Unbound) {
            return SyncOutcome::error_outcome(
                "run.sync(..) must be called before run.run()".into(),
            );
        }
        // `ChainWork` reads `chainMetadata` through this to turn a height into a
        // work vector, and it names the segment's network. No reader = no denominator
        let reader = match env.single_indexer().await {
            Ok(ix) => ix,
            Err(e) => return SyncOutcome::error_outcome(format!("bind chain reader: {e}")),
        };
        let pods = match env.component_pods().await {
            Ok(pods) => pods,
            Err(e) => return SyncOutcome::error_outcome(format!("list component pods: {e}")),
        };
        let watched: Vec<Arc<dyn Watched>> =
            pods.iter().cloned().map(|p| Arc::new(p) as Arc<dyn Watched>).collect();
        let engine = SyncEngine::phased(first, rest)
            .with_ctx(SyncCtx::new(Some(reader)).with_pods(pods))
            .with_nemesis(&nemesis)
            .with_watched(watched);
        drive(env, engine).await
    }
}

/// Wire what only a detached run has onto `engine`, run it, attach what the engine cannot
/// reach: flushed profiles + the mirrored durable report
async fn drive(env: TestEnv, mut engine: SyncEngine) -> SyncOutcome {
    let detached = super::active_sync_id();
    let profile = std::env::var(super::SYNC_PROFILE_ENV).unwrap_or_default();
    // Detached: a Prometheus target, read like any component. Local runs keep the silent
    // reporter (nothing scrapes a `cargo test`)
    if detached.is_some() {
        if let Err(e) = super::export::install() {
            return SyncOutcome::error_outcome(format!("driver metrics exporter: {e}"));
        }
        engine = engine.with_reporter(Box::new(super::export::MetricsReporter));
    }
    // `ztest sync stop` (and SIGTERM on node loss) must checkpoint, not kill →
    // route the in-pod stop-watch into engine cancellation. No namespace arg: it
    // polls the driver's *own* pod via the downward API, which sits in the run
    // namespace while deploying into the sync namespace
    if let (Some(sync_id), Some(kube)) = (&detached, env.kube_client()) {
        let cancel = super::detached::watch_stop(&kube).await;
        engine = engine.with_cancel(cancel);
        tracing::info!(sync_id = %sync_id, "detached sync: stop-watch armed");
    }
    let outcome = engine.run().await;
    if let Some(sync_id) = &detached {
        tracing::info!("profiles available with `ztest sync perf {sync_id}`");
    }
    // Mirror to a ConfigMap so `ztest sync status` works after the pod is gone
    if let (Some(sync_id), Some(kube)) = (&detached, env.kube_client()) {
        let report = super::SyncReportMirror::from_outcome(sync_id, &profile, &outcome);
        super::detached::write_report(&kube, &report).await;
        tracing::info!(sync_id = %sync_id, "detached sync: report mirrored");
    }
    // Strict order: verdict durable → teardown → namespace offered to the reaper. Client
    // cloned out first because `Drop` is what tears down (seed bindings), and a reaper
    // acting on a shortened TTL would otherwise be free to delete this pod mid-teardown
    let kube = env.kube_client();
    drop(env);
    if let (Some(sync_id), Some(kube)) = (&detached, kube) {
        super::detached::mark_finished(&kube, sync_id).await;
    }
    outcome
}
