//! Runtime abstraction.
//!
//! The async [`Pool`](crate::Pool) is generic over a [`Spawner`], which is the
//! only point where a concrete async runtime is required. Ship a [`Spawner`]
//! implementation for a runtime and the pool works on it unchanged.
//!
//! Tokio is supported out of the box via [`TokioSpawner`] (enabled by the
//! default `runtime-tokio` feature).

use futures::future::BoxFuture;

#[cfg(feature = "runtime-tokio")]
mod tokio;
#[cfg(feature = "runtime-tokio")]
pub use self::tokio::TokioSpawner;

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
}
