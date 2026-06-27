//! Pool configuration and the [`PoolBuilder`].

use std::any::Any;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use crate::cancel::CancelToken;
use crate::metrics::MetricsSnapshot;
use crate::pool::Pool;
use crate::runtime::Spawner;
use crate::sink::ResultSink;

/// Periodic metrics-export configuration (see [`PoolBuilder::metrics_channel`]).
///
/// Every `interval`, the pool pushes a [`MetricsSnapshot`] to the sink until it
/// stops. Render Prometheus text from the snapshot with
/// [`to_prometheus_text`](MetricsSnapshot::to_prometheus_text) (the
/// `metrics-export` feature).
#[derive(Clone)]
pub struct MetricsConfig {
    pub(crate) interval: Duration,
    pub(crate) sink: Arc<dyn ResultSink<MetricsSnapshot>>,
}

impl MetricsConfig {
    /// Pushes a snapshot to `sink` every `interval`.
    pub fn new<K: ResultSink<MetricsSnapshot>>(interval: Duration, sink: K) -> Self {
        Self {
            interval,
            sink: Arc::new(sink),
        }
    }
}

impl std::fmt::Debug for MetricsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsConfig")
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

/// Bulk pool configuration applied via [`PoolBuilder::config`].
///
/// Equivalent to setting the corresponding individual builder methods; fields
/// left `None` are ignored.
#[derive(Clone, Default)]
pub struct PoolConfig {
    /// Name used as the Prometheus `pool` label, also via [`Pool::name`](crate::Pool::name).
    pub pool_name: Option<String>,
    /// Periodic metrics export to a channel.
    pub metrics: Option<MetricsConfig>,
}

/// The capacity of a pool's task queue (tasks accepted but not yet running).
///
/// A `usize` converts to [`QueueSize::Bounded`], so builder methods accept either:
///
/// ```
/// use pond::QueueSize;
///
/// assert_eq!(QueueSize::from(8), QueueSize::Bounded(8));
/// assert_eq!(QueueSize::default(), QueueSize::Unbounded);
/// ```
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
    pub(crate) min_concurrency: usize,
    pub(crate) queue_size: QueueSize,
    pub(crate) non_blocking: bool,
    pub(crate) panic_recovery: bool,
    pub(crate) cancel_token: CancelToken,
    pub(crate) cancel_when: Option<BoxFuture<'static, ()>>,
    pub(crate) deadline: Option<Duration>,
    pub(crate) task_data: Option<Arc<dyn Any + Send + Sync>>,
    pub(crate) pool_name: Option<String>,
    pub(crate) metrics: Option<MetricsConfig>,
}

impl<S: Spawner> PoolBuilder<S> {
    pub(crate) fn new(spawner: S) -> Self {
        Self {
            spawner,
            max_concurrency: 0, // 0 == unbounded concurrency
            min_concurrency: 0,
            queue_size: QueueSize::Unbounded,
            non_blocking: false,
            panic_recovery: true,
            cancel_token: CancelToken::new(),
            cancel_when: None,
            deadline: None,
            task_data: None,
            pool_name: None,
            metrics: None,
        }
    }

    /// Applies a [`PoolConfig`] in bulk (a name and/or periodic metrics export).
    ///
    /// Equivalent to calling [`pool_name`](Self::pool_name) /
    /// [`metrics_channel`](Self::metrics_channel) for each set field.
    pub fn config(mut self, cfg: PoolConfig) -> Self {
        if let Some(name) = cfg.pool_name {
            self.pool_name = Some(name);
        }
        if let Some(metrics) = cfg.metrics {
            self.metrics = Some(metrics);
        }
        self
    }

    /// Sets the pool's name (used as the Prometheus `pool` label and returned by
    /// [`Pool::name`](crate::Pool::name)).
    pub fn pool_name(mut self, name: impl Into<String>) -> Self {
        self.pool_name = Some(name.into());
        self
    }

    /// Pushes a [`MetricsSnapshot`] to `sink` every `interval` until the pool
    /// stops. `sink` is any [`ResultSink`] (e.g. an `async_channel::Sender`).
    pub fn metrics_channel<K>(mut self, interval: Duration, sink: K) -> Self
    where
        K: ResultSink<MetricsSnapshot>,
    {
        self.metrics = Some(MetricsConfig::new(interval, sink));
        self
    }

    /// Sets the maximum number of tasks that may run concurrently.
    ///
    /// `0` means unbounded concurrency (the default).
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max;
        self
    }

    /// Sets a floor for concurrency: [`resize`](crate::Pool::resize) will not
    /// drop the bounded concurrency below this value.
    ///
    /// For the async pool, workers are ephemeral, so this is a *floor* rather
    /// than a count of always-running threads. The blocking pool honors it as a
    /// genuine core-thread count.
    pub fn min_concurrency(mut self, min: usize) -> Self {
        self.min_concurrency = min;
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

    /// Supplies an external [`CancelToken`], allowing shared cancellation.
    pub fn cancel_token(mut self, token: CancelToken) -> Self {
        self.cancel_token = token;
        self
    }

    /// Shuts the pool down when `fut` completes.
    ///
    /// Bridges any external cancellation source into the pool — e.g. a
    /// `tokio_util::sync::CancellationToken`'s `cancelled()` future, a
    /// request-scoped future, or a `oneshot` receiver. The future's output is
    /// ignored. This is the analogue of passing a cancellable `context.Context`.
    pub fn cancel_when<F>(mut self, fut: F) -> Self
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.cancel_when = Some(Box::pin(async move {
            let _ = fut.await;
        }));
        self
    }

    /// Shuts the pool down after `dur` elapses (a deadline).
    ///
    /// Uses the runtime's timer via [`Spawner::sleep`](crate::Spawner::sleep), so
    /// it works on any runtime.
    pub fn deadline(mut self, dur: Duration) -> Self {
        self.deadline = Some(dur);
        self
    }

    /// Attaches shared data made available to tasks via
    /// [`TaskContext::data`](crate::TaskContext::data).
    ///
    /// A common use is to stash an `Arc<async_channel::Sender<T>>` so tasks
    /// submitted with `submit_ctx` can stream results out.
    pub fn task_data(mut self, data: Arc<dyn Any + Send + Sync>) -> Self {
        self.task_data = Some(data);
        self
    }

    /// Builds the pool.
    pub fn build(self) -> Pool<S> {
        Pool::from_builder(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_size_capacity() {
        assert_eq!(QueueSize::Unbounded.capacity(), usize::MAX);
        assert_eq!(QueueSize::Bounded(16).capacity(), 16);
        assert_eq!(QueueSize::Bounded(0).capacity(), 0);
    }

    #[test]
    fn queue_size_from_usize_is_bounded() {
        assert_eq!(QueueSize::from(5), QueueSize::Bounded(5));
        let qs: QueueSize = 12usize.into();
        assert_eq!(qs, QueueSize::Bounded(12));
    }

    #[test]
    fn queue_size_default_is_unbounded() {
        assert_eq!(QueueSize::default(), QueueSize::Unbounded);
    }
}
