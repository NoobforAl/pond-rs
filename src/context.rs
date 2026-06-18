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
    /// Returns `None` if no data was attached or the stored type is not `T`.
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    /// use pond::Pool;
    ///
    /// let pool = Pool::builder().max_concurrency(2).task_data(Arc::new(42u64)).build();
    /// let handle = pool.submit_ctx(|ctx| async move {
    ///     // Reads the shared `u64`; a wrong type would yield `None`.
    ///     *ctx.data::<u64>().unwrap()
    /// });
    /// assert_eq!(handle.await.unwrap(), 42);
    /// # }
    /// ```
    pub fn data<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        self.shared.clone()?.downcast::<T>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_none_when_unset() {
        let ctx = TaskContext::new(CancelToken::new(), None);
        assert!(ctx.data::<u64>().is_none());
    }

    #[test]
    fn data_returns_value_of_matching_type_only() {
        let shared: Arc<dyn Any + Send + Sync> = Arc::new(42u64);
        let ctx = TaskContext::new(CancelToken::new(), Some(shared));
        assert_eq!(ctx.data::<u64>().as_deref(), Some(&42));
        // Wrong type downcasts to None rather than panicking.
        assert!(ctx.data::<String>().is_none());
    }

    #[test]
    fn is_cancelled_delegates_to_token() {
        let token = CancelToken::new();
        let ctx = TaskContext::new(token.clone(), None);
        assert!(!ctx.is_cancelled());
        token.cancel();
        assert!(ctx.is_cancelled());
        // The token handed back is the same shared signal.
        assert!(ctx.cancel_token().is_cancelled());
    }
}
