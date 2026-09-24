//! Background drive-to-tip task a wallet sync subject owns → outcome as the runner reads it.
//!
//! - Outcome recorded by the task itself (a `JoinHandle` only says "finished", not how)
//! - Finished with no recorded outcome = panicked / aborted → a failure, never a silent pass

use std::future::Future;
use std::sync::{Arc, OnceLock};

use tokio::task::JoinHandle;

pub(crate) struct SyncTask<T> {
    handle: JoinHandle<()>,
    outcome: Arc<OnceLock<Result<T, String>>>,
}

impl<T: Send + Sync + 'static> SyncTask<T> {
    pub(crate) fn spawn<F>(sync: F) -> Self
    where
        F: Future<Output = Result<T, String>> + Send + 'static,
    {
        let outcome = Arc::new(OnceLock::new());
        let slot = outcome.clone();
        let handle = tokio::spawn(async move {
            let _ = slot.set(sync.await);
        });
        Self { handle, outcome }
    }

    pub(crate) fn succeeded(&self) -> Option<&T> {
        self.outcome.get().and_then(|o| o.as_ref().ok())
    }

    pub(crate) fn failure(&self) -> Option<String> {
        match self.outcome.get() {
            Some(Err(e)) => Some(e.clone()),
            Some(Ok(_)) => None,
            None => self.handle.is_finished().then(|| "sync task ended without an outcome".into()),
        }
    }

    pub(crate) fn abort(&self) {
        let _ = self.outcome.set(Err("sync stopped before completion".into()));
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn outcome_is_success_failure_or_a_task_that_never_reported() {
        let ok = SyncTask::spawn(async { Ok::<u32, String>(7) });
        let err = SyncTask::spawn(async { Err::<u32, String>("indexer went away".into()) });
        let panicked = SyncTask::<u32>::spawn(async { panic!("scan worker died") });
        for task in [&ok, &err, &panicked] {
            while !task.handle.is_finished() {
                tokio::task::yield_now().await;
            }
        }
        assert_eq!((ok.succeeded(), ok.failure()), (Some(&7), None));
        assert_eq!((err.succeeded(), err.failure()), (None, Some("indexer went away".into())));
        assert_eq!(
            (panicked.succeeded(), panicked.failure()),
            (None, Some("sync task ended without an outcome".into()))
        );

        let stopped = SyncTask::spawn(std::future::pending::<Result<u32, String>>());
        assert_eq!(stopped.failure(), None, "still running = neither outcome");
        stopped.abort();
        assert_eq!(stopped.failure(), Some("sync stopped before completion".into()));
    }
}
