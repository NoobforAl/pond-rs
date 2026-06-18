//! smol adapter for the [`Spawner`] trait.
//!
//! Backed by [`async-global-executor`], the multi-threaded global executor that
//! powers the smol runtime. Worker threads are started on demand, so a
//! [`SmolSpawner`] is ready to use without any explicit runtime setup — build a
//! pool with [`Pool::builder_with`](crate::Pool::builder_with).

use futures::future::BoxFuture;

use super::Spawner;

/// A [`Spawner`] backed by the smol global executor (`async-global-executor`).
///
/// The spawner is a zero-sized handle to the process-wide executor; cloning it
/// is free.
#[derive(Clone, Copy, Default, Debug)]
pub struct SmolSpawner;

impl SmolSpawner {
    /// Creates a spawner for the smol global executor.
    ///
    /// Executor worker threads start lazily on first use.
    pub fn new() -> Self {
        Self
    }
}

impl Spawner for SmolSpawner {
    fn spawn(&self, future: BoxFuture<'static, ()>) {
        async_global_executor::spawn(future).detach();
    }

    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        async_global_executor::spawn_blocking(f).detach();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn spawn_runs_future() {
        let spawner = SmolSpawner::new();
        let (tx, rx) = async_channel::bounded::<i32>(1);
        spawner.spawn(Box::pin(async move {
            let _ = tx.send(42).await;
        }));
        let got = futures::executor::block_on(async move { rx.recv().await.unwrap() });
        assert_eq!(got, 42);
    }

    #[test]
    fn spawn_blocking_runs_closure() {
        let spawner = SmolSpawner::new();
        let flag = Arc::new(AtomicBool::new(false));
        let (tx, rx) = async_channel::bounded::<()>(1);
        let f = flag.clone();
        spawner.spawn_blocking(Box::new(move || {
            f.store(true, Ordering::SeqCst);
            let _ = tx.try_send(());
        }));
        futures::executor::block_on(async move { rx.recv().await.unwrap() });
        assert!(flag.load(Ordering::SeqCst));
    }
}
