//! A process-wide default pool and package-level convenience functions.
//!
//! A lazily-initialized, unbounded pool reachable through free functions.
//! Requires the `runtime-tokio` feature and must first be used from within a
//! Tokio runtime.

use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::group::{ResultTaskGroup, TryTaskGroup};
use crate::pool::{Pool, TaskHandle};
use crate::runtime::TokioSpawner;
use crate::schedule::{RetryPolicy, ScheduledTask};
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
pub fn group<T: Send + 'static>() -> ResultTaskGroup<Pool<TokioSpawner>, T> {
    default_pool().group()
}

/// Creates a [`TryTaskGroup`] on the default pool.
pub fn try_group<T: Send + 'static, E: Send + 'static>() -> TryTaskGroup<Pool<TokioSpawner>, T, E> {
    default_pool().try_group()
}

/// Creates a [`Subpool`] of the default pool.
pub fn subpool(max_concurrency: usize) -> Subpool<TokioSpawner> {
    default_pool().subpool(max_concurrency)
}

/// Submits a task to the default pool after `delay` elapses.
pub fn submit_after<F, Fut, T>(delay: Duration, task: F) -> TaskHandle<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    default_pool().submit_after(delay, task)
}

/// Submits a task to the default pool at the absolute instant `when`.
pub fn submit_at<F, Fut, T>(when: Instant, task: F) -> TaskHandle<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    default_pool().submit_at(when, task)
}

/// Schedules a recurring task on the default pool (see [`Pool::schedule_every`]).
pub fn schedule_every<F, Fut>(interval: Duration, factory: F) -> ScheduledTask
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    default_pool().schedule_every(interval, factory)
}

/// Submits a retrying task to the default pool (see [`Pool::submit_with_retry`]).
pub fn submit_with_retry<F, Fut, T, E>(
    policy: RetryPolicy,
    factory: F,
) -> TaskHandle<std::result::Result<T, E>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
    default_pool().submit_with_retry(policy, factory)
}

/// Fans out `count` copies of `factory` on the default pool.
pub fn submit_n<F, Fut, T>(count: usize, factory: F) -> Vec<TaskHandle<T>>
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    default_pool().submit_n(count, factory)
}
