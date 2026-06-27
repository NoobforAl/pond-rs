//! `async-std` adapter for the [`Spawner`] trait.
//!
//! `async-std` runs its own multi-threaded executor, started on demand, so an
//! [`AsyncStdSpawner`] needs no explicit runtime setup — build a pool with
//! [`Pool::builder_with`](crate::Pool::builder_with).

use futures::future::BoxFuture;

use super::Spawner;

/// A [`Spawner`] backed by the `async-std` runtime.
///
/// The spawner is a zero-sized handle to the process-wide `async-std` executor;
/// cloning it is free.
#[derive(Clone, Copy, Default, Debug)]
pub struct AsyncStdSpawner;

impl AsyncStdSpawner {
    /// Creates a spawner for the `async-std` runtime.
    pub fn new() -> Self {
        Self
    }
}

impl Spawner for AsyncStdSpawner {
    fn spawn(&self, future: BoxFuture<'static, ()>) {
        // The `JoinHandle` detaches when dropped, leaving the task running.
        async_std::task::spawn(future);
    }

    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        async_std::task::spawn_blocking(f);
    }

    fn sleep(&self, dur: std::time::Duration) -> BoxFuture<'static, ()> {
        // Native async timer instead of the default thread-per-sleep, so
        // scheduled/deadline work scales without spawning OS threads.
        Box::pin(async move {
            async_std::task::sleep(dur).await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn spawn_runs_future() {
        let spawner = AsyncStdSpawner::new();
        let (tx, rx) = async_channel::bounded::<i32>(1);
        spawner.spawn(Box::pin(async move {
            let _ = tx.send(7).await;
        }));
        let got = async_std::task::block_on(async move { rx.recv().await.unwrap() });
        assert_eq!(got, 7);
    }

    #[test]
    fn spawn_blocking_runs_closure() {
        let spawner = AsyncStdSpawner::new();
        let flag = Arc::new(AtomicBool::new(false));
        let (tx, rx) = async_channel::bounded::<()>(1);
        let f = flag.clone();
        spawner.spawn_blocking(Box::new(move || {
            f.store(true, Ordering::SeqCst);
            let _ = tx.try_send(());
        }));
        async_std::task::block_on(async move { rx.recv().await.unwrap() });
        assert!(flag.load(Ordering::SeqCst));
    }
}
