//! Tokio adapter for the [`Spawner`] trait.

use futures::future::BoxFuture;
use tokio::runtime::Handle;

use super::Spawner;

/// A [`Spawner`] backed by a Tokio runtime [`Handle`].
///
/// Construct one from the runtime that is currently running with
/// [`TokioSpawner::current`], or from an explicit handle with
/// [`TokioSpawner::from_handle`].
#[derive(Clone)]
pub struct TokioSpawner {
    handle: Handle,
}

impl TokioSpawner {
    /// Captures the handle of the currently running Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called outside the context of a Tokio runtime.
    pub fn current() -> Self {
        Self {
            handle: Handle::current(),
        }
    }

    /// Builds a spawner from an explicit Tokio runtime handle.
    pub fn from_handle(handle: Handle) -> Self {
        Self { handle }
    }
}

impl Spawner for TokioSpawner {
    fn spawn(&self, future: BoxFuture<'static, ()>) {
        self.handle.spawn(future);
    }

    fn spawn_unboxed<F>(&self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        // Tokio's task cell stores the future inline, so this is a single
        // allocation for the whole task.
        self.handle.spawn(future);
    }

    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        self.handle.spawn_blocking(f);
    }

    fn sleep(&self, dur: std::time::Duration) -> BoxFuture<'static, ()> {
        // Lazy: the `Sleep` is constructed on first poll, which happens inside
        // the runtime context (the future is spawned via `handle.spawn`), so no
        // explicit `Handle::enter` is needed.
        Box::pin(async move {
            tokio::time::sleep(dur).await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn spawn_runs_future() {
        let spawner = TokioSpawner::current();
        let (tx, rx) = oneshot::channel();
        spawner.spawn(Box::pin(async move {
            let _ = tx.send(42);
        }));
        assert_eq!(rx.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn spawn_blocking_runs_closure() {
        let spawner = TokioSpawner::current();
        let flag = Arc::new(AtomicBool::new(false));
        let (tx, rx) = oneshot::channel();
        let f = flag.clone();
        spawner.spawn_blocking(Box::new(move || {
            f.store(true, Ordering::SeqCst);
            let _ = tx.send(());
        }));
        rx.await.unwrap();
        assert!(flag.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn clone_is_independent_handle() {
        let spawner = TokioSpawner::current();
        let cloned = spawner.clone();
        let (tx, rx) = oneshot::channel();
        cloned.spawn(Box::pin(async move {
            let _ = tx.send(7);
        }));
        assert_eq!(rx.await.unwrap(), 7);
    }
}
