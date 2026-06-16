//! Internal abstraction over things that can run tasks.
//!
//! [`TaskSink`] lets [`ResultTaskGroup`](crate::ResultTaskGroup) /
//! [`TryTaskGroup`](crate::TryTaskGroup) operate over either a [`Pool`](crate::Pool)
//! or a [`Subpool`](crate::Subpool) without duplicating their logic.

use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use crate::cancel::CancelToken;
use crate::pool::TaskHandle;

/// Something that accepts async tasks and exposes a cancel token + shared data.
///
/// Implemented by [`Pool`](crate::Pool) and [`Subpool`](crate::Subpool).
pub trait TaskSink: Clone + Send + Sync + 'static {
    /// Submits a task, returning a handle that resolves with its result.
    fn submit<F, Fut, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static;

    /// The sink's cancel token.
    fn cancel_token(&self) -> CancelToken;

    /// Shared task data attached to the owning pool, if any.
    fn task_data(&self) -> Option<Arc<dyn Any + Send + Sync>>;
}
