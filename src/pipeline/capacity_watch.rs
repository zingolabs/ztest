//! Live cluster-capacity watcher: the [`super::cluster`] probe is one-shot, so it
//! misses everything scheduled after it (the build pod, test pods). This keeps
//! [`ClusterCapacity`] current from Pod/Node/PVC watches, folded in memory by
//! [`super::cluster::capacity_from`].

use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{Node, PersistentVolumeClaim, Pod};
use kube::runtime::{WatchStreamExt, reflector, watcher};
use kube::{Api, Client};
use tokio::sync::watch;

use super::cluster::capacity_from;
use crate::qos::ClusterCapacity;

/// Recompute is a pure in-memory fold (no API traffic), so this only bounds how
/// fast a pod change reaches the banner; well under the render cadence.
const RECOMPUTE_INTERVAL: Duration = Duration::from_millis(500);

/// Dropping this aborts the watch tasks, so teardown needs no explicit lifetime.
#[derive(Debug)]
pub struct CapacityWatch {
    rx: watch::Receiver<ClusterCapacity>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for CapacityWatch {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl CapacityWatch {
    pub fn receiver(&self) -> watch::Receiver<ClusterCapacity> {
        self.rx.clone()
    }
}

/// `initial` seeds the channel until the first watch sync. Must run on a
/// multi-thread runtime so the streams progress while the caller blocks on the
/// build.
pub fn spawn(client: Client, initial: ClusterCapacity) -> CapacityWatch {
    let (tx, rx) = watch::channel(initial);

    let (pods, pods_w) = reflector::store::<Pod>();
    let (nodes, nodes_w) = reflector::store::<Node>();
    let (pvcs, pvcs_w) = reflector::store::<PersistentVolumeClaim>();

    let mut tasks = Vec::with_capacity(4);
    tasks.push(tokio::spawn(drive(
        "pods",
        watcher(Api::all(client.clone()), watcher::Config::default()).reflect(pods_w),
    )));
    tasks.push(tokio::spawn(drive(
        "nodes",
        watcher(Api::all(client.clone()), watcher::Config::default()).reflect(nodes_w),
    )));
    tasks.push(tokio::spawn(drive(
        "pvcs",
        watcher(Api::all(client), watcher::Config::default()).reflect(pvcs_w),
    )));

    tasks.push(tokio::spawn(async move {
        // Until the initial lists land, a fold would be over a half-empty cache
        // and flash a near-zero capacity; hold the seed instead.
        nodes.wait_until_ready().await.ok();
        pods.wait_until_ready().await.ok();
        pvcs.wait_until_ready().await.ok();

        let mut ticker = tokio::time::interval(RECOMPUTE_INTERVAL);
        loop {
            ticker.tick().await;
            let (n, p, v) = (nodes.state(), pods.state(), pvcs.state());
            let cap = capacity_from(
                n.iter().map(|a| &**a),
                p.iter().map(|a| &**a),
                v.iter().map(|a| &**a),
            );
            tx.send_if_modified(|cur| {
                let changed = *cur != cap;
                if changed {
                    *cur = cap;
                }
                changed
            });
        }
    }));

    CapacityWatch { rx, tasks }
}

/// Drive a reflected stream to keep its store fresh. A persistent error (e.g. a
/// missing `watch` grant) leaves that store un-synced — capacity falls back to the
/// seed rather than failing the run; transient errors are retried by the watcher.
async fn drive<K>(
    what: &'static str,
    stream: impl futures::Stream<Item = Result<watcher::Event<K>, watcher::Error>>,
) where
    K: kube::Resource + Clone + std::fmt::Debug + Send + 'static,
    K::DynamicType: Default,
{
    futures::pin_mut!(stream);
    while let Some(ev) = stream.next().await {
        if let Err(e) = ev {
            tracing::debug!(error = %e, resource = what, "capacity watch stream error");
        }
    }
}
