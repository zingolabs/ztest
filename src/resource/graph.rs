//! The resource graph executor: forward provisioning and reverse teardown over a
//! set of [`Provider`]s.
//!
//! The same graph serves both directions:
//! - Provision walks forward in dependency order. A node runs the moment all its
//!   deps are `Ready`; independent nodes run concurrently; a node whose dep
//!   failed is `Blocked` (never attempted) while its siblings proceed. Running
//!   each node as soon as its inputs are ready lets a slow image or archive skip
//!   only its own dependents.
//! - Teardown walks reverse: a node is reaped only once every node that depends
//!   on it is gone. `Cached` nodes are skipped (the cross-run cache); failures
//!   are isolated so one stuck delete can't strand the rest.
//!
//! The executor is generic over the identity and context types and contains no
//! Kubernetes code, so its ordering/concurrency/failure logic is unit-tested
//! against fake providers below.

use std::collections::{HashMap, HashSet, VecDeque};

use futures::stream::{FuturesUnordered, StreamExt};

use super::provider::{NodeId, NodeState, Provider, Readiness, ResourceError};

/// A static error in the graph's shape, caught by [`Graph::validate`] before any
/// work runs.
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("node {0} was added twice")]
    Duplicate(String),
    #[error("node {node} depends on unknown node {dep}")]
    MissingDep { node: String, dep: String },
    #[error("dependency cycle involving {0} node(s)")]
    Cycle(usize),
}

/// A dependency graph of resources to provision (forward) and reap (reverse).
pub struct Graph<Id: NodeId, Cx: Send + Sync> {
    nodes: HashMap<Id, Box<dyn Provider<Id, Cx>>>,
}

impl<Id: NodeId, Cx: Send + Sync> std::fmt::Debug for Graph<Id, Cx> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph")
            .field("nodes", &self.nodes.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl<Id: NodeId, Cx: Send + Sync> Default for Graph<Id, Cx> {
    fn default() -> Self {
        Graph {
            nodes: HashMap::new(),
        }
    }
}

impl<Id: NodeId, Cx: Send + Sync> Graph<Id, Cx> {
    pub fn new() -> Self {
        Graph::default()
    }

    /// Add a provider. Returns [`GraphError::Duplicate`] if a node with the same
    /// content-addressed id was already added: callers dedup by not adding the
    /// same resource twice; equal ids are the same node.
    pub fn add(&mut self, provider: Box<dyn Provider<Id, Cx>>) -> Result<(), GraphError> {
        let id = provider.id();
        if self.nodes.contains_key(&id) {
            return Err(GraphError::Duplicate(format!("{id:?}")));
        }
        self.nodes.insert(id, provider);
        Ok(())
    }

    /// Insert a provider, silently ignoring a duplicate id (the resource is
    /// already in the graph). The natural call for content-addressed fan-out:
    /// many consumers ask for the same image/seed; the first wins.
    pub fn add_dedup(&mut self, provider: Box<dyn Provider<Id, Cx>>) {
        let id = provider.id();
        self.nodes.entry(id).or_insert(provider);
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Reject a malformed graph up front: unknown dependencies or cycles. Always
    /// call before [`provision`](Self::provision); the executor assumes a valid,
    /// acyclic graph (a cycle would otherwise leave nodes `Pending` forever).
    pub fn validate(&self) -> Result<(), GraphError> {
        for (id, node) in &self.nodes {
            for dep in node.deps() {
                if !self.nodes.contains_key(&dep) {
                    return Err(GraphError::MissingDep {
                        node: format!("{id:?}"),
                        dep: format!("{dep:?}"),
                    });
                }
            }
        }
        // Kahn's algorithm: if we can't peel every node, there's a cycle.
        let mut indegree: HashMap<Id, usize> = self.nodes.keys().map(|k| (k.clone(), 0)).collect();
        for node in self.nodes.values() {
            for _dep in node.deps() {
                *indegree.get_mut(&node.id()).unwrap() += 1;
            }
        }
        let mut queue: VecDeque<Id> = indegree
            .iter()
            .filter(|&(_, &d)| d == 0)
            .map(|(k, _)| k.clone())
            .collect();
        let mut peeled = 0;
        while let Some(id) = queue.pop_front() {
            peeled += 1;
            // Peel edges *out of* this node: every node that depends on `id`.
            for node in self.nodes.values() {
                if node.deps().contains(&id) {
                    let d = indegree.get_mut(&node.id()).unwrap();
                    *d -= 1;
                    if *d == 0 {
                        queue.push_back(node.id());
                    }
                }
            }
        }
        if peeled != self.nodes.len() {
            return Err(GraphError::Cycle(self.nodes.len() - peeled));
        }
        Ok(())
    }

    /// Provision every node, forward in dependency order, running up to
    /// `max_concurrent` nodes at once. Returns the terminal state of each node.
    ///
    /// `max_concurrent` bounds parallelism: pass `1` when providers share a serial
    /// resource (e.g. the console's single PTY live region, where two concurrent
    /// `docker build`s would fight over the grid), or a larger cap for independent
    /// network work. Clamped to at least 1.
    ///
    /// `on_change` is called on every state transition (for the live panel). A
    /// node runs when all deps are `Ready`; if any dep is unavailable the node is
    /// `Blocked` and its dependents transitively `Blocked`; everything else keeps
    /// going. A `probe` reporting `Ready` short-circuits `provision`.
    pub async fn provision(
        &self,
        cx: &Cx,
        max_concurrent: usize,
        mut on_change: impl FnMut(&Id, &NodeState),
    ) -> HashMap<Id, NodeState> {
        let cap = max_concurrent.max(1);
        let mut state: HashMap<Id, NodeState> = self
            .nodes
            .keys()
            .map(|k| (k.clone(), NodeState::Pending))
            .collect();
        let mut inflight = FuturesUnordered::new();

        loop {
            // Classify every still-Pending node against its deps' current state.
            let mut to_block: Vec<Id> = Vec::new();
            let mut to_run: Vec<Id> = Vec::new();
            for (id, node) in &self.nodes {
                if !matches!(state[id], NodeState::Pending) {
                    continue;
                }
                let deps = node.deps();
                if deps
                    .iter()
                    .any(|d| state.get(d).is_some_and(|s| s.is_unavailable()))
                {
                    to_block.push(id.clone());
                } else if deps
                    .iter()
                    .all(|d| state.get(d).is_some_and(|s| s.is_ready()))
                {
                    to_run.push(id.clone());
                }
            }

            let blocked_any = !to_block.is_empty();
            for id in to_block {
                state.insert(id.clone(), NodeState::Blocked);
                on_change(&id, &NodeState::Blocked);
            }
            // Launch only up to the free concurrency slots; the rest stay
            // `Pending` and are relaunched next iteration as slots free.
            let free = cap.saturating_sub(inflight.len());
            for id in to_run.into_iter().take(free) {
                state.insert(id.clone(), NodeState::Acquiring);
                on_change(&id, &NodeState::Acquiring);
                let node = self.nodes.get(&id).expect("id from nodes");
                inflight.push(async move { (id, run_one(node.as_ref(), cx).await) });
            }

            if inflight.is_empty() {
                // Nothing running. If we blocked a node this pass, loop again to
                // propagate `Blocked` to *its* dependents before giving up;
                // otherwise the remaining Pending nodes are all blocked and we're
                // done.
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

    /// Reap every provisioned, non-`Cached` node, reverse in dependency order: a
    /// node is torn down only after all nodes that depend on it are gone.
    /// Independent subtrees are reaped concurrently. Idempotent and failure
    /// isolated: a teardown error is recorded and its siblings still run.
    ///
    /// `states` is the report from [`provision`](Self::provision): only nodes that
    /// reached `Ready` (i.e. actually exist) are candidates.
    pub async fn teardown(
        &self,
        cx: &Cx,
        states: &HashMap<Id, NodeState>,
        mut on_change: impl FnMut(&Id, &Result<(), ResourceError>),
    ) -> Vec<(Id, Result<(), ResourceError>)> {
        // Reverse edges: dependents[x] = nodes that list x in their deps.
        let mut dependents: HashMap<Id, Vec<Id>> =
            self.nodes.keys().map(|k| (k.clone(), Vec::new())).collect();
        for (id, node) in &self.nodes {
            for dep in node.deps() {
                if let Some(v) = dependents.get_mut(&dep) {
                    v.push(id.clone());
                }
            }
        }

        // Candidates: provisioned + reaped-lifetime. Everything else is already
        // "gone" for ordering purposes (never blocks a dep from being reaped).
        let mut remaining: HashSet<Id> = self
            .nodes
            .iter()
            .filter(|(id, node)| {
                states.get(*id).is_some_and(NodeState::is_ready) && node.lifetime().is_reaped()
            })
            .map(|(id, _)| id.clone())
            .collect();
        let mut gone: HashSet<Id> = self
            .nodes
            .keys()
            .filter(|id| !remaining.contains(*id))
            .cloned()
            .collect();

        let mut inflight = FuturesUnordered::new();
        let mut launched: HashSet<Id> = HashSet::new();
        let mut results: Vec<(Id, Result<(), ResourceError>)> = Vec::new();

        loop {
            let ready: Vec<Id> = remaining
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

/// Probe, then provision if absent. The per-node body of [`Graph::provision`].
async fn run_one<Id: NodeId, Cx: Send + Sync>(node: &dyn Provider<Id, Cx>, cx: &Cx) -> NodeState {
    match node.probe(cx).await {
        Readiness::Ready => NodeState::Ready,
        Readiness::Absent => match node.provision(cx).await {
            Ok(()) => NodeState::Ready,
            Err(e) => NodeState::Failed(e.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::provider::Lifetime;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    type Id = &'static str;

    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Shared context: an event log the fakes append to (for order assertions),
    /// plus live/peak in-flight counters (for the concurrency-cap assertion).
    #[derive(Clone, Debug, Default)]
    struct Cx {
        log: Arc<Mutex<Vec<String>>>,
        cur: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }
    impl Cx {
        fn record(&self, s: String) {
            self.log.lock().unwrap().push(s);
        }
        fn events(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
        fn index_of(&self, s: &str) -> Option<usize> {
            self.events().iter().position(|e| e == s)
        }
        fn enter(&self) {
            let n = self.cur.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(n, Ordering::SeqCst);
        }
        fn leave(&self) {
            self.cur.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct Fake {
        id: Id,
        deps: Vec<Id>,
        life: Lifetime,
        already_ready: bool,
        fail_provision: bool,
        fail_teardown: bool,
    }
    impl Fake {
        fn new(id: Id) -> Self {
            Fake {
                id,
                deps: Vec::new(),
                life: Lifetime::RunScoped,
                already_ready: false,
                fail_provision: false,
                fail_teardown: false,
            }
        }
        fn deps(mut self, d: &[Id]) -> Self {
            self.deps = d.to_vec();
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
        fn boxed(self) -> Box<dyn Provider<Id, Cx>> {
            Box::new(self)
        }
    }

    #[async_trait]
    impl Provider<Id, Cx> for Fake {
        fn id(&self) -> Id {
            self.id
        }
        fn deps(&self) -> Vec<Id> {
            self.deps.clone()
        }
        fn lifetime(&self) -> Lifetime {
            self.life
        }
        async fn probe(&self, _cx: &Cx) -> Readiness {
            if self.already_ready {
                Readiness::Ready
            } else {
                Readiness::Absent
            }
        }
        async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
            cx.record(format!("provision:{}", self.id));
            // Straddle a yield so concurrent provisions actually overlap, letting
            // the peak-in-flight counter witness the concurrency cap.
            cx.enter();
            tokio::task::yield_now().await;
            cx.leave();
            if self.fail_provision {
                Err(ResourceError::Provision(format!("{} boom", self.id)))
            } else {
                Ok(())
            }
        }
        async fn teardown(&self, cx: &Cx) -> Result<(), ResourceError> {
            cx.record(format!("teardown:{}", self.id));
            if self.fail_teardown {
                Err(ResourceError::Teardown(format!("{} stuck", self.id)))
            } else {
                Ok(())
            }
        }
    }

    fn graph(fakes: Vec<Fake>) -> Graph<Id, Cx> {
        let mut g = Graph::new();
        for f in fakes {
            g.add(f.boxed()).unwrap();
        }
        g
    }

    #[tokio::test]
    async fn linear_chain_provisions_in_dependency_order() {
        let g = graph(vec![
            Fake::new("a"),
            Fake::new("b").deps(&["a"]),
            Fake::new("c").deps(&["b"]),
        ]);
        g.validate().unwrap();
        let cx = Cx::default();
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert!(state.values().all(NodeState::is_ready));
        assert!(cx.index_of("provision:a") < cx.index_of("provision:b"));
        assert!(cx.index_of("provision:b") < cx.index_of("provision:c"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrency_cap_bounds_in_flight() {
        // Five independent nodes, cap 2 → never more than 2 provisioning at once.
        let g = graph(vec![
            Fake::new("a"),
            Fake::new("b"),
            Fake::new("c"),
            Fake::new("d"),
            Fake::new("e"),
        ]);
        let cx = Cx::default();
        let state = g.provision(&cx, 2, |_, _| {}).await;
        assert!(state.values().all(NodeState::is_ready));
        assert!(cx.peak.load(Ordering::SeqCst) <= 2, "peak exceeded cap");
        assert!(cx.peak.load(Ordering::SeqCst) >= 2, "should reach the cap");
    }

    #[tokio::test]
    async fn diamond_provisions_all_and_respects_edges() {
        // a → {b, c} → d
        let g = graph(vec![
            Fake::new("a"),
            Fake::new("b").deps(&["a"]),
            Fake::new("c").deps(&["a"]),
            Fake::new("d").deps(&["b", "c"]),
        ]);
        g.validate().unwrap();
        let cx = Cx::default();
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert!(state.values().all(NodeState::is_ready));
        assert!(cx.index_of("provision:a") < cx.index_of("provision:b"));
        assert!(cx.index_of("provision:a") < cx.index_of("provision:c"));
        assert!(cx.index_of("provision:b") < cx.index_of("provision:d"));
        assert!(cx.index_of("provision:c") < cx.index_of("provision:d"));
    }

    #[tokio::test]
    async fn failed_dep_blocks_dependents_but_not_siblings() {
        // a fails → b (needs a) is Blocked; c (independent) still Ready.
        let g = graph(vec![
            Fake::new("a").fails(),
            Fake::new("b").deps(&["a"]),
            Fake::new("c"),
        ]);
        let cx = Cx::default();
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert!(matches!(state["a"], NodeState::Failed(_)));
        assert_eq!(state["b"], NodeState::Blocked);
        assert_eq!(state["c"], NodeState::Ready);
        // b's provision was never even attempted.
        assert!(cx.index_of("provision:b").is_none());
    }

    #[tokio::test]
    async fn blocked_propagates_transitively() {
        // a fails → b blocked → c (needs b) blocked.
        let g = graph(vec![
            Fake::new("a").fails(),
            Fake::new("b").deps(&["a"]),
            Fake::new("c").deps(&["b"]),
        ]);
        let cx = Cx::default();
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert_eq!(state["b"], NodeState::Blocked);
        assert_eq!(state["c"], NodeState::Blocked);
    }

    #[tokio::test]
    async fn probe_ready_short_circuits_provision() {
        let g = graph(vec![Fake::new("a").ready()]);
        let cx = Cx::default();
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert_eq!(state["a"], NodeState::Ready);
        assert!(cx.index_of("provision:a").is_none(), "should not provision");
    }

    #[test]
    fn validate_rejects_missing_dep() {
        let g = graph(vec![Fake::new("a").deps(&["ghost"])]);
        assert!(matches!(g.validate(), Err(GraphError::MissingDep { .. })));
    }

    #[test]
    fn validate_rejects_cycle() {
        let g = graph(vec![
            Fake::new("a").deps(&["b"]),
            Fake::new("b").deps(&["a"]),
        ]);
        assert!(matches!(g.validate(), Err(GraphError::Cycle(_))));
    }

    #[tokio::test]
    async fn teardown_is_reverse_topological() {
        let g = graph(vec![
            Fake::new("a"),
            Fake::new("b").deps(&["a"]),
            Fake::new("c").deps(&["b"]),
        ]);
        let cx = Cx::default();
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        let results = g.teardown(&cx, &state, |_, _| {}).await;
        assert!(results.iter().all(|(_, r)| r.is_ok()));
        // Reverse of provision order: c before b before a.
        assert!(cx.index_of("teardown:c") < cx.index_of("teardown:b"));
        assert!(cx.index_of("teardown:b") < cx.index_of("teardown:a"));
    }

    #[tokio::test]
    async fn teardown_skips_cached_nodes() {
        // seed (Cached) ← test-ns (RunScoped). Only the run-scoped node is reaped.
        let g = graph(vec![
            Fake::new("seed").cached(),
            Fake::new("ns").deps(&["seed"]),
        ]);
        let cx = Cx::default();
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        g.teardown(&cx, &state, |_, _| {}).await;
        assert!(cx.index_of("teardown:ns").is_some());
        assert!(cx.index_of("teardown:seed").is_none(), "cache must survive");
    }

    #[tokio::test]
    async fn teardown_only_touches_provisioned_nodes() {
        // a fails → b blocked. Neither exists, so neither is torn down.
        let g = graph(vec![Fake::new("a").fails(), Fake::new("b").deps(&["a"])]);
        let cx = Cx::default();
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        g.teardown(&cx, &state, |_, _| {}).await;
        assert!(cx.index_of("teardown:a").is_none());
        assert!(cx.index_of("teardown:b").is_none());
    }

    #[tokio::test]
    async fn teardown_failure_is_isolated() {
        // b's teardown fails; a and c must still be reaped, and the failure
        // surfaces in the report rather than aborting the sweep.
        let g = graph(vec![
            Fake::new("a"),
            Fake::new("b").fails_teardown(),
            Fake::new("c"),
        ]);
        let cx = Cx::default();
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        let results = g.teardown(&cx, &state, |_, _| {}).await;
        assert_eq!(results.len(), 3);
        assert_eq!(results.iter().filter(|(_, r)| r.is_err()).count(), 1);
        for id in ["a", "b", "c"] {
            assert!(cx.index_of(&format!("teardown:{id}")).is_some());
        }
    }
}
