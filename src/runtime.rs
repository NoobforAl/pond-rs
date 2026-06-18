//! Runtime abstraction.
//!
//! The async [`Pool`](crate::Pool) is generic over a [`Spawner`], which is the
//! only point where a concrete async runtime is required. Ship a [`Spawner`]
//! implementation for a runtime and the pool works on it unchanged.
//!
//! Adapters ship for several runtimes, each behind its own feature:
//!
//! * [`TokioSpawner`] — `runtime-tokio` (default)
//! * `SmolSpawner` — `runtime-smol` (backed by `async-global-executor`)
//! * `AsyncStdSpawner` — `runtime-async-std`
//!
//! Any other runtime works by implementing [`Spawner`] yourself. (Embassy is not
//! supported: its statically-allocated, fixed-size task model has no dynamic
//! boxed-future spawning and no `spawn_blocking`, so it can't satisfy this
//! trait.)

use std::time::Duration;

use futures::future::BoxFuture;

#[cfg(feature = "runtime-tokio")]
mod tokio;
#[cfg(feature = "runtime-tokio")]
pub use self::tokio::TokioSpawner;

#[cfg(feature = "runtime-smol")]
mod smol;
#[cfg(feature = "runtime-smol")]
pub use self::smol::SmolSpawner;

#[cfg(feature = "runtime-async-std")]
mod async_std;
#[cfg(feature = "runtime-async-std")]
pub use self::async_std::AsyncStdSpawner;

/// Abstraction over an async runtime's task-spawning capability.
///
/// Implementations launch *detached* work. The pool drives task results through
/// its own channels, so a spawner never needs to return a join handle — it only
/// needs to run the future (or blocking closure) to completion somewhere.
///
/// A `Spawner` must be cheap to clone (typically an `Arc` or runtime handle) and
/// shareable across threads.
pub trait Spawner: Clone + Send + Sync + 'static {
    /// Spawns a detached async task that runs to completion on the runtime.
    fn spawn(&self, future: BoxFuture<'static, ()>);

    /// Offloads a blocking closure onto a thread where blocking is acceptable.
    ///
    /// Used by code paths that bridge blocking work into the async pool. Runtimes
    /// without a dedicated blocking pool may spawn a thread.
    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>);

    /// Returns a future that completes after `dur` elapses.
    ///
    /// Used for deadline-based cancellation (see
    /// [`PoolBuilder::deadline`](crate::PoolBuilder::deadline)). The default
    /// implementation offloads a blocking sleep via
    /// [`spawn_blocking`](Spawner::spawn_blocking), so it works on any runtime
    /// without a native timer. Runtimes that have one (e.g. Tokio) override this
    /// for efficiency.
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let (tx, rx) = async_channel::bounded::<()>(1);
        self.spawn_blocking(Box::new(move || {
            std::thread::sleep(dur);
            let _ = tx.try_send(());
        }));
        Box::pin(async move {
            let _ = rx.recv().await;
        })
    }
}
