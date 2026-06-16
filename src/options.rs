//! Pool configuration and the [`PoolBuilder`].

use crate::cancel::CancelToken;
use crate::pool::Pool;
use crate::runtime::Spawner;

/// The capacity of a pool's task queue (tasks accepted but not yet running).
///
/// Mirrors pond's `WithQueueSize` / `Unbounded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueueSize {
    /// No limit on the number of queued tasks (the default).
    #[default]
    Unbounded,
    /// At most `n` tasks may be queued waiting for a worker.
    Bounded(usize),
}

impl QueueSize {
    /// The effective capacity as a `usize` (`usize::MAX` when unbounded).
    pub(crate) fn capacity(self) -> usize {
        match self {
            QueueSize::Unbounded => usize::MAX,
            QueueSize::Bounded(n) => n,
        }
    }
}

impl From<usize> for QueueSize {
    fn from(n: usize) -> Self {
        QueueSize::Bounded(n)
    }
}

/// Builder for an async [`Pool`].
///
/// Create one with [`Pool::builder`] (Tokio) or [`Pool::builder_with`] (any
/// [`Spawner`]).
pub struct PoolBuilder<S: Spawner> {
    pub(crate) spawner: S,
    pub(crate) max_concurrency: usize,
    pub(crate) queue_size: QueueSize,
    pub(crate) non_blocking: bool,
    pub(crate) panic_recovery: bool,
    pub(crate) cancel_token: CancelToken,
}

impl<S: Spawner> PoolBuilder<S> {
    pub(crate) fn new(spawner: S) -> Self {
        Self {
            spawner,
            max_concurrency: 0, // 0 == unbounded concurrency
            queue_size: QueueSize::Unbounded,
            non_blocking: false,
            panic_recovery: true,
            cancel_token: CancelToken::new(),
        }
    }

    /// Sets the maximum number of tasks that may run concurrently.
    ///
    /// `0` means unbounded concurrency (the default), matching pond.
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max;
        self
    }

    /// Sets the queue capacity. Accepts a [`QueueSize`] or a `usize`
    /// (interpreted as [`QueueSize::Bounded`]).
    pub fn queue_size(mut self, size: impl Into<QueueSize>) -> Self {
        self.queue_size = size.into();
        self
    }

    /// Makes the pool non-blocking: when the queue is full, `submit` drops the
    /// task and resolves its handle with [`Error::QueueFull`](crate::Error::QueueFull).
    pub fn non_blocking(mut self, non_blocking: bool) -> Self {
        self.non_blocking = non_blocking;
        self
    }

    /// Enables or disables panic recovery (enabled by default).
    ///
    /// When enabled, a task panic is captured and surfaced as
    /// [`Error::Panicked`](crate::Error::Panicked). When disabled, the panic is
    /// allowed to propagate on the worker.
    pub fn panic_recovery(mut self, enabled: bool) -> Self {
        self.panic_recovery = enabled;
        self
    }

    /// Supplies an external [`CancelToken`], allowing shared cancellation
    /// (the analogue of pond's `WithContext`).
    pub fn cancel_token(mut self, token: CancelToken) -> Self {
        self.cancel_token = token;
        self
    }

    /// Builds the pool.
    pub fn build(self) -> Pool<S> {
        Pool::from_builder(self)
    }
}
