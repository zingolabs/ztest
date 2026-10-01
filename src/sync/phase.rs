//! [`Phase`] = one subject + its own probes, tick, timeout, completion, preflight.
//!
//! - Run = phases in declaration order; next starts only once the previous one completed with no
//!   fatal violation (at_completion included)
//! - Subject built at phase start by an async factory over the topology (may need earlier
//!   phases' work: a wallet needs a serving indexer)

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::metrics::Family;

use super::probe::{
    Always, AtCompletion, Eventually, ProbeBuilder, ProbeSpec, Severity, Sometimes,
};
use super::runner::{DEFAULT_TICK, SyncVerdict};
use super::subject::SyncSubject;
use super::work::OpSet;

type Building<'a> = Pin<Box<dyn Future<Output = anyhow::Result<Box<dyn SyncSubject>>> + 'a>>;

/// Object-safe `AsyncFnOnce(&T) -> Result<S>`
pub(super) trait Factory<T> {
    fn build<'a>(self: Box<Self>, topology: &'a T) -> Building<'a>;
}

struct Typed<F, S>(F, PhantomData<fn() -> S>);

impl<T, S, F> Factory<T> for Typed<F, S>
where
    S: SyncSubject + 'static,
    F: AsyncFnOnce(&T) -> anyhow::Result<S> + 'static,
{
    fn build<'a>(self: Box<Self>, topology: &'a T) -> Building<'a> {
        Box::pin(async move {
            let subject = (self.0)(topology).await?;
            Ok(Box::new(subject) as Box<dyn SyncSubject>)
        })
    }
}

pub(super) enum Source<T> {
    Bound(Box<dyn SyncSubject>),
    Deferred(Box<dyn Factory<T>>),
}

impl<T> Source<T> {
    /// Subject for this phase, built now if deferred
    pub(super) async fn subject(self, topology: &T) -> anyhow::Result<Box<dyn SyncSubject>> {
        match self {
            Source::Bound(subject) => Ok(subject),
            Source::Deferred(factory) => factory.build(topology).await,
        }
    }
}

/// Registration surface of one phase ([`Run::phase`](crate::sync::Run::phase))
pub struct Phase<T> {
    pub(super) name: String,
    pub(super) source: Option<Source<T>>,
    pub(super) probes: Vec<ProbeSpec<T>>,
    pub(super) tick: Duration,
    pub(super) timeout: Option<Duration>,
    pub(super) stop_height: Option<u32>,
    /// Resolved into `stop_height` at the first reading
    pub(super) stop_after: Option<u32>,
    pub(super) required_work: OpSet,
    pub(super) ready: Vec<Family>,
}

impl<T> std::fmt::Debug for Phase<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Phase")
            .field("name", &self.name)
            .field("probes", &self.probes.len())
            .field("tick", &self.tick)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl<T> Phase<T> {
    fn with_source(name: impl Into<String>, source: Source<T>) -> Self {
        Phase {
            name: name.into(),
            source: Some(source),
            probes: Vec::new(),
            tick: DEFAULT_TICK,
            timeout: None,
            stop_height: None,
            stop_after: None,
            required_work: OpSet::NONE,
            ready: Vec::new(),
        }
    }

    /// Phase over an already-built subject
    pub fn bound(name: impl Into<String>, subject: impl SyncSubject + 'static) -> Self {
        Self::with_source(name, Source::Bound(Box::new(subject)))
    }

    /// Phase whose subject `build` constructs at phase start, from the run's topology
    pub fn new<F, S>(name: impl Into<String>, build: F) -> Self
    where
        F: AsyncFnOnce(&T) -> anyhow::Result<S> + 'static,
        S: SyncSubject + 'static,
    {
        Self::with_source(name, Source::Deferred(Box::new(Typed(build, PhantomData))))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Base sampling interval (default 5 s)
    pub fn tick(&mut self, tick: Duration) -> &mut Self {
        self.tick = tick;
        self
    }
    /// Cap on this phase alone, subject construction included
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    /// Finish at `height`, not at tip — what makes throughput a measurement of the
    /// *software* (two runs to tip cover different work; `perf --base` refuses them)
    pub fn until_height(&mut self, height: u32) -> &mut Self {
        self.stop_height = Some(height);
        self.stop_after = None;
        self
    }

    /// Finish `blocks` past the phase's first reading, not at the subject's own completion
    ///
    /// - For a phase opening at a live tip (start height unknown at registration)
    pub fn for_blocks(&mut self, blocks: u32) -> &mut Self {
        self.stop_height = None;
        self.stop_after = Some(blocks);
        self
    }

    /// Ops this phase's probes will [`Work::require`](crate::sync::Work::require).
    ///
    /// - Checked against one live reading before the phase → a subject not publishing them
    ///   fails by series name, not as a `require` panic hours in
    /// - Subject ↔ component agree on those series by string only, across repos
    pub fn requires_work(&mut self, ops: OpSet) -> &mut Self {
        self.required_work = ops;
        self
    }

    /// Widen (or narrow) one family's ready window for this phase — e.g. a cold mainnet
    /// validator delaying zaino's first block past
    /// `ztest::backends::zainod::family::FETCHED_HEIGHT`'s 60 s default
    pub fn ready_within(&mut self, family: impl Into<Family>, ready: Duration) -> &mut Self {
        let family = Family { ready, ..family.into() };
        self.ready.retain(|f| !f.is(family));
        self.ready.push(family);
        self
    }

    pub(super) fn ready_for(&self, family: Family) -> Duration {
        self.ready.iter().find(|f| f.is(family)).map_or(family.ready, |f| f.ready)
    }

    /// Safety invariant (true at every evaluation)
    pub fn always(
        &mut self,
        name: impl Into<String>,
        severity: Severity,
    ) -> ProbeBuilder<'_, T, Always> {
        ProbeBuilder::new(&mut self.probes, name.into(), severity)
    }
    /// Liveness invariant (must (re)satisfy within its `window`)
    pub fn eventually(
        &mut self,
        name: impl Into<String>,
        severity: Severity,
    ) -> ProbeBuilder<'_, T, Eventually> {
        ProbeBuilder::new(&mut self.probes, name.into(), severity)
    }
    /// Coverage invariant, true on ≥1 tick. A miss fails the phase (green without coverage = weak)
    pub fn sometimes(&mut self, name: impl Into<String>) -> ProbeBuilder<'_, T, Sometimes> {
        ProbeBuilder::new(&mut self.probes, name.into(), Severity::Fatal)
    }
    /// Terminal post-condition (evaluated once at completion)
    pub fn at_completion(
        &mut self,
        name: impl Into<String>,
        severity: Severity,
    ) -> ProbeBuilder<'_, T, AtCompletion> {
        ProbeBuilder::new(&mut self.probes, name.into(), severity)
    }

    pub fn probe_count(&self) -> usize {
        self.probes.len()
    }
}

/// Probes judged in every phase ([`Run::throughout`](crate::sync::Run::throughout))
///
/// - One body across phases (captured state carries over), scheduler state fresh per phase
/// - No `sometimes` (coverage per phase vs per run = ambiguous)
#[derive(Debug)]
pub struct Throughout<'r, T> {
    pub(super) probes: &'r mut Vec<ProbeSpec<T>>,
}

impl<T> Throughout<'_, T> {
    pub fn always(
        &mut self,
        name: impl Into<String>,
        severity: Severity,
    ) -> ProbeBuilder<'_, T, Always> {
        ProbeBuilder::new(self.probes, name.into(), severity)
    }
    pub fn eventually(
        &mut self,
        name: impl Into<String>,
        severity: Severity,
    ) -> ProbeBuilder<'_, T, Eventually> {
        ProbeBuilder::new(self.probes, name.into(), severity)
    }
    pub fn at_completion(
        &mut self,
        name: impl Into<String>,
        severity: Severity,
    ) -> ProbeBuilder<'_, T, AtCompletion> {
        ProbeBuilder::new(self.probes, name.into(), severity)
    }
}

/// One attempted phase, as report/mirror/watch render it
///
/// - Phases after a failed one never start → absent (not listed as failed)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseOutcome {
    pub name: String,
    pub verdict: SyncVerdict,
    pub started_ms: u64,
    pub elapsed_ms: u64,
    pub ticks: u64,
    pub violations: usize,
    pub coverage_gaps: Vec<String>,
    pub error: Option<String>,
    pub restarts: u32,
}

impl PhaseOutcome {
    /// `wallet: Passed in 1h02m (1200 ticks, 0 violations, 1 restart)`
    pub fn describe(&self) -> String {
        let span = crate::fmt::format_span(Duration::from_millis(self.elapsed_ms));
        let mut line = format!(
            "{}: {} in {span} ({} ticks, {} violations",
            self.name, self.verdict, self.ticks, self.violations
        );
        if self.restarts > 0 {
            line.push_str(&format!(", {} restarts", self.restarts));
        }
        if !self.coverage_gaps.is_empty() {
            line.push_str(&format!(", gaps: {}", self.coverage_gaps.join(", ")));
        }
        line.push(')');
        if let Some(e) = &self.error {
            line.push_str(&format!(" — {e}"));
        }
        line
    }
}
