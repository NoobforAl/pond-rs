//! A process-wide default pool and package-level convenience functions.
//!
//! Mirrors pond's default pool: a lazily-initialized, unbounded pool reachable
//! through free functions. Requires the `runtime-tokio` feature and must first
//! be used from within a Tokio runtime.

use std::future::Future;
use std::sync::OnceLock;

use crate::group::{ResultTaskGroup, TryTaskGroup};
use crate::pool::{Pool, TaskHandle};
use crate::runtime::TokioSpawner;
use crate::subpool::Subpool;

static DEFAULT: OnceLock<Pool<TokioSpawner>> = OnceLock::new();

/// Returns the process-wide default pool, creating it on first use.
///
/// # Panics
///
/// Panics on first use if called outside a Tokio runtime.
pub fn default_pool() -> &'static Pool<TokioSpawner> {
    DEFAULT.get_or_init(|| Pool::builder().build())
}

/// Submits a task to the default pool.
pub fn submit<F, Fut, T>(task: F) -> TaskHandle<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    default_pool().submit(task)
}

/// Creates a [`ResultTaskGroup`] on the default pool.
pub fn group<T: Send + 'static>() -> ResultTaskGroup<TokioSpawner, T> {
    default_pool().group()
}

/// Creates a [`TryTaskGroup`] on the default pool.
pub fn try_group<T: Send + 'static, E: Send + 'static>() -> TryTaskGroup<TokioSpawner, T, E> {
    default_pool().try_group()
}

/// Creates a [`Subpool`] of the default pool.
pub fn subpool(max_concurrency: usize) -> Subpool<TokioSpawner> {
    default_pool().subpool(max_concurrency)
}
