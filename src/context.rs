//! Per-task context handed to context-aware tasks.
//!
//! A [`TaskContext`] is passed to tasks submitted via the `*_ctx` methods
//! (e.g. [`Pool::submit_ctx`](crate::Pool::submit_ctx)). It lets a task:
//!
//! * observe cancellation — break out early ([`is_cancelled`](TaskContext::is_cancelled))
//!   or `await` it to race work against a shutdown ([`cancelled`](TaskContext::cancelled)),
//! * reach the underlying [`CancelToken`], and
//! * read shared data attached to the pool/group ([`data`](TaskContext::data)) — the
//!   idiomatic way to "send data out" of tasks is to stash an
//!   `async_channel::Sender<T>` (wrapped in `Arc`) as that shared value and have
//!   each task pull it and send through it.

use std::any::Any;
use std::sync::Arc;

use crate::cancel::CancelToken;

/// Context handed to a task submitted via a `*_ctx` method.
///
/// Cheap to clone (it holds an [`Arc`]-backed token and an optional `Arc` to
/// shared data).
#[derive(Clone)]
pub struct TaskContext {
    cancel: CancelToken,
    shared: Option<Arc<dyn Any + Send + Sync>>,
}

impl TaskContext {
    pub(crate) fn new(cancel: CancelToken, shared: Option<Arc<dyn Any + Send + Sync>>) -> Self {
        Self { cancel, shared }
    }

    /// Returns `true` if the owning pool/group has been cancelled.
    ///
    /// Tasks can poll this to abandon work early.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Resolves when the owning pool/group is cancelled.
    ///
    /// Useful with `tokio::select!` to race a unit of work against cancellation.
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }

    /// Returns the owning [`CancelToken`].
    pub fn cancel_token(&self) -> CancelToken {
        self.cancel.clone()
    }

    /// Returns the shared data attached to the pool/group, if it was set and is
    /// of type `T`.
    ///
    /// Attach it via [`PoolBuilder::task_data`](crate::PoolBuilder::task_data).
    pub fn data<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        self.shared.clone()?.downcast::<T>().ok()
    }
}
