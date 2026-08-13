//! [`Graph`]: dependency-ordered executor driving providers.
//!
//! - Forward: run a node once its deps are `Ready` (siblings concurrent to
//!   `max_concurrent`; a failed dep leaves it `Blocked`)
//! - Reverse: reap only once dependents are gone, skipping [`Lifetime::Cached`]
//! - No Kubernetes here — all K8s goes through [`Provider`], so ordering /
//!   concurrency / failure-isolation stay unit-testable against fakes

use std::collections::{HashMap, HashSet, VecDeque};

use futures::stream::{FuturesUnordered, StreamExt};
use thiserror::Error;

use crate::resource::context::Cx;
use crate::resource::provider::{NodeId, Provider};
use crate::resource::state::{NodeState, Readiness, ResourceError};

/// Dependency graph of [`Provider`] nodes. [`add`](Self::add)/[`add_dedup`](Self::add_dedup),
/// then [`validate`](Self::validate) before [`provision`](Self::provision) — execution
/// assumes a checked DAG
#[derive(Default)]
pub struct Graph {
    nodes: HashMap<NodeId, Box<dyn Provider>>,
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph").field("nodes", &self.nodes.keys().collect::<Vec<_>>()).finish()
    }
}

impl Graph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Same id twice → [`GraphError::Duplicate`]; content-addressed fan-out wants
    /// [`add_dedup`](Self::add_dedup)
    pub fn add(&mut self, provider: Box<dyn Provider>) -> Result<(), GraphError> {
        let id = provider.id();
        if self.nodes.contains_key(&id) {
            return Err(GraphError::Duplicate(id));
        }
        self.nodes.insert(id, provider);
        Ok(())
    }

    /// Insert, first-writer-wins. [`plan_runtime`](super::plan_runtime) needs it: two
    /// tests declaring one seed source compute one id and must collapse to one node
    pub fn add_dedup(&mut self, provider: Box<dyn Provider>) {
        self.nodes.entry(provider.id()).or_insert(provider);
    }

    /// Every declared dep exists, no cycles (Kahn). MUST precede
    /// [`provision`](Self::provision) — a cycle leaves nodes `Pending` forever
    pub fn validate(&self) -> Result<(), GraphError> {
        for (id, node) in &self.nodes {
            for dep in node.deps() {
                if !self.nodes.contains_key(&dep) {
                    return Err(GraphError::MissingDep { node: id.clone(), dep });
                }
            }
        }
        // `indegree[n]` = nodes n depends ON: peel roots, decrement dependents;
        // non-zero remainder = cycle
        let mut indegree: HashMap<NodeId, usize> =
            self.nodes.iter().map(|(k, node)| (k.clone(), node.deps().len())).collect();
        let mut queue: VecDeque<NodeId> =
            indegree.iter().filter(|&(_, &d)| d == 0).map(|(k, _)| k.clone()).collect();
        let mut peeled = 0usize;
        while let Some(id) = queue.pop_front() {
            peeled += 1;
            for (other_id, other) in &self.nodes {
                if other.deps().contains(&id) {
                    let d = indegree.get_mut(other_id).expect("indegree seeded");
                    *d -= 1;
                    if *d == 0 {
                        queue.push_back(other_id.clone());
                    }
                }
            }
        }
        if peeled != self.nodes.len() {
            return Err(GraphError::Cycle { count: self.nodes.len() - peeled });
        }
        Ok(())
    }

    /// Provision in dependency order, ≤ `max_concurrent` (clamped to ≥1) at a time.
    ///
    /// - Cap at 1 when providers share a serial resource (the console PTY cannot render
    ///   two concurrent builds coherently)
    /// - [`Readiness::Ready`] from [`probe`](Provider::probe) short-circuits provision
    ///   but still walks `Pending → Acquiring → Ready` through `on_change`
    pub async fn provision<F>(
        &self,
        cx: &Cx,
        max_concurrent: usize,
        mut on_change: F,
    ) -> HashMap<NodeId, NodeState>
    where
        F: FnMut(&NodeId, &NodeState),
    {
        let cap = max_concurrent.max(1);
        let mut state: HashMap<NodeId, NodeState> =
            self.nodes.keys().map(|k| (k.clone(), NodeState::Pending)).collect();
        let mut inflight = FuturesUnordered::new();

        loop {
            // Still-Pending: any dep unavailable → Blocked, all deps Ready → runnable,
            // else reclassify next round
            let mut to_block: Vec<NodeId> = Vec::new();
            let mut to_run: Vec<NodeId> = Vec::new();
            for (id, node) in &self.nodes {
                if !matches!(state[id], NodeState::Pending) {
                    continue;
                }
                let deps = node.deps();
                if deps.iter().any(|d| state.get(d).is_some_and(|s| s.is_unavailable())) {
                    to_block.push(id.clone());
                } else if deps.iter().all(|d| state.get(d).is_some_and(|s| s.is_ready())) {
                    to_run.push(id.clone());
                }
            }

            let blocked_any = !to_block.is_empty();
            for id in to_block {
                state.insert(id.clone(), NodeState::Blocked);
                on_change(&id, &NodeState::Blocked);
            }

            // Over the cap stays Pending, reclassifies once a slot frees
            let free = cap.saturating_sub(inflight.len());
            for id in to_run.into_iter().take(free) {
                state.insert(id.clone(), NodeState::Acquiring);
                on_change(&id, &NodeState::Acquiring);
                let node = self.nodes.get(&id).expect("id from nodes");
                inflight.push(async move { (id, run_one(node.as_ref(), cx).await) });
            }

            if inflight.is_empty() {
                // Loop again so `Blocked` propagates transitively before we finish
                if blocked_any {
                    continue;
                }
                break;
            }

            if let Some((id, result)) = inflight.next().await {
                state.insert(id.clone(), result.clone());
                on_change(&id, &result);
            }
        }

        state
    }

    /// Reverse-order teardown of every provisioned non-[`Cached`](super::Lifetime::Cached)
    /// node: reaped only after every dependent is gone.
    ///
    /// - Independent subtrees concurrent; idempotent and failure-isolated
    /// - Only `Ready` nodes are candidates (`Failed`/`Blocked` never materialized)
    pub async fn teardown<F>(
        &self,
        cx: &Cx,
        states: &HashMap<NodeId, NodeState>,
        mut on_change: F,
    ) -> Vec<(NodeId, Result<(), ResourceError>)>
    where
        F: FnMut(&NodeId, &Result<(), ResourceError>),
    {
        // dependents[x] = every node listing x as a dep
        let mut dependents: HashMap<NodeId, Vec<NodeId>> =
            self.nodes.keys().map(|k| (k.clone(), Vec::new())).collect();
        for (id, node) in &self.nodes {
            for dep in node.deps() {
                if let Some(v) = dependents.get_mut(&dep) {
                    v.push(id.clone());
                }
            }
        }

        // Ready + reaped-lifetime only; everything else is already "gone" for ordering
        // (Cached stays put, never-provisioned has nothing to remove)
        let mut remaining: HashSet<NodeId> = self
            .nodes
            .iter()
            .filter(|(id, node)| {
                states.get(*id).is_some_and(NodeState::is_ready) && node.lifetime().is_reaped()
            })
            .map(|(id, _)| id.clone())
            .collect();
        let mut gone: HashSet<NodeId> =
            self.nodes.keys().filter(|id| !remaining.contains(*id)).cloned().collect();

        let mut inflight = FuturesUnordered::new();
        let mut launched: HashSet<NodeId> = HashSet::new();
        let mut results: Vec<(NodeId, Result<(), ResourceError>)> = Vec::new();

        loop {
            // Tear down once every dependent is gone
            let ready: Vec<NodeId> = remaining
                .iter()
                .filter(|id| {
                    !launched.contains(*id) && dependents[*id].iter().all(|d| gone.contains(d))
                })
                .cloned()
                .collect();
            for id in ready {
                launched.insert(id.clone());
                let node = self.nodes.get(&id).expect("id from nodes");
                inflight.push(async move { (id, node.teardown(cx).await) });
            }

            if inflight.is_empty() {
                break;
            }

            if let Some((id, result)) = inflight.next().await {
                remaining.remove(&id);
                gone.insert(id.clone());
                on_change(&id, &result);
                results.push((id, result));
            }
        }

        results
    }
}

/// Probe, then provision if absent
async fn run_one(node: &dyn Provider, cx: &Cx) -> NodeState {
    match node.probe(cx).await {
        Readiness::Ready => NodeState::Ready,
        Readiness::Absent => match node.provision(cx).await {
            Ok(()) => NodeState::Ready,
            Err(e) => NodeState::Failed(e.to_string()),
        },
    }
}

/// Shape errors from [`Graph::validate`]. Runtime failures are [`ResourceError`] →
/// [`NodeState::Failed`](super::NodeState::Failed) instead (one bad node never aborts a run)
#[derive(Debug, Error)]
pub enum GraphError {
    #[error("node {0:?} added twice")]
    Duplicate(NodeId),

    #[error("node {node:?} depends on unknown node {dep:?}")]
    MissingDep { node: NodeId, dep: NodeId },

    #[error("dependency cycle involving {count} node(s)")]
    Cycle { count: usize },
}

// Object-safe trait → a `Fake` provider with a shared event log covers ordering,
// concurrency capping, blocking, short-circuit-on-Ready and teardown, no cluster.
// `Fake`'s `NodeId::Image(String)` is arbitrary, chosen for readable assertions

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::provider::NodeId;
    use crate::resource::state::Lifetime;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    // `Fake` keeps its state in a `TestCx` and never touches `cx.client` (the boundary
    // `Cx` is `test_cx()`'s offline stub) → `Cx::client` need not go optional for tests
    fn mk_cx(log: SharedLog) -> TestCx {
        TestCx { log }
    }

    type SharedLog = Arc<Mutex<TestState>>;

    #[derive(Default, Debug)]
    struct TestState {
        events: Vec<String>,
        cur_inflight: usize,
        peak_inflight: usize,
    }

    #[derive(Clone, Debug)]
    struct TestCx {
        log: SharedLog,
    }

    impl TestCx {
        fn record(&self, s: String) {
            self.log.lock().unwrap().events.push(s);
        }
        fn events(&self) -> Vec<String> {
            self.log.lock().unwrap().events.clone()
        }
        fn index_of(&self, s: &str) -> Option<usize> {
            self.events().iter().position(|e| e == s)
        }
        fn enter(&self) {
            let mut s = self.log.lock().unwrap();
            s.cur_inflight += 1;
            if s.cur_inflight > s.peak_inflight {
                s.peak_inflight = s.cur_inflight;
            }
        }
        fn leave(&self) {
            self.log.lock().unwrap().cur_inflight -= 1;
        }
        fn peak(&self) -> usize {
            self.log.lock().unwrap().peak_inflight
        }
    }

    fn img(s: &str) -> NodeId {
        NodeId::Image(s.to_string())
    }

    /// Appends provision/teardown events to the shared log, honoring fail/ready flags
    #[derive(Debug)]
    struct Fake {
        id: NodeId,
        label: String,
        deps: Vec<NodeId>,
        life: Lifetime,
        already_ready: bool,
        fail_provision: bool,
        fail_teardown: bool,
        cx: TestCx,
    }

    impl Fake {
        fn new(label: &str, cx: TestCx) -> Self {
            Self {
                id: img(label),
                label: label.to_string(),
                deps: Vec::new(),
                life: Lifetime::RunScoped,
                already_ready: false,
                fail_provision: false,
                fail_teardown: false,
                cx,
            }
        }
        fn deps(mut self, d: &[&str]) -> Self {
            self.deps = d.iter().map(|s| img(s)).collect();
            self
        }
        fn cached(mut self) -> Self {
            self.life = Lifetime::Cached;
            self
        }
        fn ready(mut self) -> Self {
            self.already_ready = true;
            self
        }
        fn fails(mut self) -> Self {
            self.fail_provision = true;
            self
        }
        fn fails_teardown(mut self) -> Self {
            self.fail_teardown = true;
            self
        }
        fn boxed(self) -> Box<dyn Provider> {
            Box::new(self)
        }
    }

    #[async_trait]
    impl Provider for Fake {
        fn id(&self) -> NodeId {
            self.id.clone()
        }
        fn deps(&self) -> Vec<NodeId> {
            self.deps.clone()
        }
        fn lifetime(&self) -> Lifetime {
            self.life
        }
        async fn probe(&self, _cx: &Cx) -> Readiness {
            if self.already_ready { Readiness::Ready } else { Readiness::Absent }
        }
        async fn provision(&self, _cx: &Cx) -> Result<(), ResourceError> {
            self.cx.record(format!("provision:{}", self.label));
            // Straddle a yield so provisions overlap and peak-in-flight witnesses the cap
            self.cx.enter();
            tokio::task::yield_now().await;
            self.cx.leave();
            if self.fail_provision {
                Err(ResourceError::Provision(format!("{} boom", self.label)))
            } else {
                Ok(())
            }
        }
        async fn teardown(&self, _cx: &Cx) -> Result<(), ResourceError> {
            self.cx.record(format!("teardown:{}", self.label));
            if self.fail_teardown {
                Err(ResourceError::Teardown(format!("{} stuck", self.label)))
            } else {
                Ok(())
            }
        }
    }

    fn graph(fakes: Vec<Fake>) -> Graph {
        let mut g = Graph::new();
        for f in fakes {
            g.add(f.boxed()).unwrap();
        }
        g
    }

    /// Real `Cx`, client pointed at nothing; `Fake` never calls through it
    async fn test_cx() -> Cx {
        let cfg = kube::Config::new("http://127.0.0.1:1".parse().unwrap());
        let client = kube::Client::try_from(cfg).expect("build offline client");
        Cx::headless(client)
    }

    #[tokio::test]
    async fn linear_chain_provisions_in_dependency_order() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()).deps(&["b"]),
        ]);
        g.validate().unwrap();
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert!(state.values().all(NodeState::is_ready));
        assert!(tcx.index_of("provision:a") < tcx.index_of("provision:b"));
        assert!(tcx.index_of("provision:b") < tcx.index_of("provision:c"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrency_cap_bounds_in_flight() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        // Five independent nodes, cap 2 → never more than 2 in flight
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()),
            Fake::new("c", tcx.clone()),
            Fake::new("d", tcx.clone()),
            Fake::new("e", tcx.clone()),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, 2, |_, _| {}).await;
        assert!(state.values().all(NodeState::is_ready));
        assert!(tcx.peak() <= 2, "peak exceeded cap");
        assert!(tcx.peak() >= 2, "should reach the cap");
    }

    #[tokio::test]
    async fn diamond_provisions_all_and_respects_edges() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        // a → {b, c} → d
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()).deps(&["a"]),
            Fake::new("d", tcx.clone()).deps(&["b", "c"]),
        ]);
        g.validate().unwrap();
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert!(state.values().all(NodeState::is_ready));
        assert!(tcx.index_of("provision:a") < tcx.index_of("provision:b"));
        assert!(tcx.index_of("provision:a") < tcx.index_of("provision:c"));
        assert!(tcx.index_of("provision:b") < tcx.index_of("provision:d"));
        assert!(tcx.index_of("provision:c") < tcx.index_of("provision:d"));
    }

    #[tokio::test]
    async fn failed_dep_blocks_dependents_but_not_siblings() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        // a fails → b Blocked, c (independent) still Ready
        let g = graph(vec![
            Fake::new("a", tcx.clone()).fails(),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert!(matches!(state[&img("a")], NodeState::Failed(_)));
        assert_eq!(state[&img("b")], NodeState::Blocked);
        assert_eq!(state[&img("c")], NodeState::Ready);
        assert!(tcx.index_of("provision:b").is_none());
    }

    #[tokio::test]
    async fn blocked_propagates_transitively() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()).fails(),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()).deps(&["b"]),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert_eq!(state[&img("b")], NodeState::Blocked);
        assert_eq!(state[&img("c")], NodeState::Blocked);
    }

    #[tokio::test]
    async fn probe_ready_short_circuits_provision() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![Fake::new("a", tcx.clone()).ready()]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert_eq!(state[&img("a")], NodeState::Ready);
        assert!(tcx.index_of("provision:a").is_none(), "should not provision");
    }

    #[test]
    fn validate_rejects_missing_dep() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![Fake::new("a", tcx.clone()).deps(&["ghost"])]);
        assert!(matches!(g.validate(), Err(GraphError::MissingDep { .. })));
    }

    #[test]
    fn validate_rejects_cycle() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()).deps(&["b"]),
            Fake::new("b", tcx.clone()).deps(&["a"]),
        ]);
        assert!(matches!(g.validate(), Err(GraphError::Cycle { .. })));
    }

    #[tokio::test]
    async fn teardown_is_reverse_topological() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()).deps(&["b"]),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        let results = g.teardown(&cx, &state, |_, _| {}).await;
        assert!(results.iter().all(|(_, r)| r.is_ok()));
        assert!(tcx.index_of("teardown:c") < tcx.index_of("teardown:b"));
        assert!(tcx.index_of("teardown:b") < tcx.index_of("teardown:a"));
    }

    #[tokio::test]
    async fn teardown_skips_cached_nodes() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("seed", tcx.clone()).cached(),
            Fake::new("ns", tcx.clone()).deps(&["seed"]),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        g.teardown(&cx, &state, |_, _| {}).await;
        assert!(tcx.index_of("teardown:ns").is_some());
        assert!(tcx.index_of("teardown:seed").is_none(), "cache must survive");
    }

    #[tokio::test]
    async fn teardown_only_touches_provisioned_nodes() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()).fails(),
            Fake::new("b", tcx.clone()).deps(&["a"]),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        g.teardown(&cx, &state, |_, _| {}).await;
        assert!(tcx.index_of("teardown:a").is_none());
        assert!(tcx.index_of("teardown:b").is_none());
    }

    #[tokio::test]
    async fn teardown_failure_is_isolated() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        // b's teardown fails → a and c still reaped, failure in the report not the sweep
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()).fails_teardown(),
            Fake::new("c", tcx.clone()),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        let results = g.teardown(&cx, &state, |_, _| {}).await;
        assert_eq!(results.len(), 3);
        assert_eq!(results.iter().filter(|(_, r)| r.is_err()).count(), 1);
        for id in ["a", "b", "c"] {
            assert!(tcx.index_of(&format!("teardown:{id}")).is_some());
        }
    }

    // Helpers used only behind cfg(test)
    #[allow(dead_code)]
    fn _touch_atomic() {
        let _ = AtomicUsize::new(0).fetch_add(1, Ordering::SeqCst);
    }
}
