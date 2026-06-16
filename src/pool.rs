//! The async worker [`Pool`].

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use event_listener::Event;
use futures::channel::oneshot;
use futures::FutureExt;

use crate::cancel::CancelToken;
use crate::error::{panic_message, Error, Result};
use crate::group::{ResultTaskGroup, TryTaskGroup};
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::options::PoolBuilder;
use crate::runtime::Spawner;
use crate::subpool::Subpool;

#[cfg(feature = "runtime-tokio")]
use crate::runtime::TokioSpawner;

/// An async worker pool that runs futures with bounded concurrency on a
/// pluggable runtime.
///
/// Clone a `Pool` freely: clones share the same underlying workers, queue and
/// metrics.
pub struct Pool<S: Spawner> {
    inner: Arc<Inner<S>>,
}

impl<S: Spawner> Clone for Pool<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<S: Spawner> {
    spawner: S,
    // Permit channel acts as a counting semaphore. A worker must hold a permit
    // (a `()` token) to run when concurrency is limited.
    permit_tx: async_channel::Sender<()>,
    permit_rx: async_channel::Receiver<()>,
    // When false, concurrency is unbounded and permits are skipped.
    limited: AtomicBool,
    max_concurrency: AtomicUsize, // 0 == unbounded
    capacity: usize,              // queue capacity (usize::MAX == unbounded)
    panic_recovery: bool,
    non_blocking: bool,
    closed: AtomicBool,
    cancel: CancelToken,
    metrics: Metrics,
    idle: Event, // notified when the pool becomes fully idle
    slot: Event, // notified when a queue slot frees up
}

#[cfg(feature = "runtime-tokio")]
impl Pool<TokioSpawner> {
    /// Starts building a pool backed by the current Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime context.
    pub fn builder() -> PoolBuilder<TokioSpawner> {
        PoolBuilder::new(TokioSpawner::current())
    }
}

impl<S: Spawner> Pool<S> {
    /// Starts building a pool backed by a custom [`Spawner`].
    pub fn builder_with(spawner: S) -> PoolBuilder<S> {
        PoolBuilder::new(spawner)
    }

    pub(crate) fn from_builder(b: PoolBuilder<S>) -> Self {
        let limited = b.max_concurrency != 0;
        // An unbounded token channel: `send` never blocks, so releasing a permit
        // is always immediate.
        let (permit_tx, permit_rx) = async_channel::unbounded::<()>();
        if limited {
            for _ in 0..b.max_concurrency {
                let _ = permit_tx.try_send(());
            }
        }

        let inner = Arc::new(Inner {
            spawner: b.spawner,
            permit_tx,
            permit_rx,
            limited: AtomicBool::new(limited),
            max_concurrency: AtomicUsize::new(b.max_concurrency),
            capacity: b.queue_size.capacity(),
            panic_recovery: b.panic_recovery,
            non_blocking: b.non_blocking,
            closed: AtomicBool::new(false),
            cancel: b.cancel_token,
            metrics: Metrics::new(),
            idle: Event::new(),
            slot: Event::new(),
        });

        // Watch the cancel token: when it fires, stop accepting tasks and wake
        // any tasks blocked on a permit so they can observe the cancellation.
        let watch = inner.clone();
        inner.spawner.spawn(Box::pin(async move {
            watch.cancel.cancelled().await;
            // ordering: `Relaxed` — `closed` is an independent stop flag; the
            // channel `close()` calls below provide the real synchronization.
            watch.closed.store(true, Ordering::Relaxed);
            watch.permit_rx.close();
            watch.permit_tx.close();
            watch.slot.notify(usize::MAX);
            watch.idle.notify(usize::MAX);
        }));

        Pool { inner }
    }

    /// Submits a task, returning a [`TaskHandle`] that resolves with its result.
    ///
    /// This call never blocks. If the pool is stopped, or it is non-blocking and
    /// the queue is full, the returned handle resolves with the corresponding
    /// [`Error`] (the task is not run).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(target = "pond", name = "pond.submit", level = "debug", skip_all)
    )]
    pub fn submit<F, Fut, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();

        if self.is_stopped() {
            return TaskHandle::resolved(Err(self.stop_error()));
        }

        if self.inner.non_blocking && self.inner.metrics.waiting() as usize >= self.inner.capacity {
            self.inner.metrics.inc_dropped();
            return TaskHandle::resolved(Err(Error::QueueFull));
        }

        let (tx, rx) = oneshot::channel();
        self.spawn_task(task, tx);
        TaskHandle::new(rx)
    }

    /// Submits a task fire-and-forget, discarding its result handle.
    ///
    /// The analogue of pond's `Go`. The task still runs; only the handle (and
    /// therefore the result) is dropped.
    pub fn spawn<F, Fut, T>(&self, task: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = self.submit(task);
        drop(handle);
    }

    /// Submits a task, rejecting immediately if the pool is stopped or its
    /// bounded queue is full.
    ///
    /// Unlike [`submit`](Self::submit), the error is returned directly rather
    /// than surfaced through the handle.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(target = "pond", name = "pond.try_submit", level = "debug", skip_all)
    )]
    pub fn try_submit<F, Fut, T>(&self, task: F) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();

        if self.is_stopped() {
            return Err(self.stop_error());
        }
        if self.inner.metrics.waiting() as usize >= self.inner.capacity {
            self.inner.metrics.inc_dropped();
            return Err(Error::QueueFull);
        }

        let (tx, rx) = oneshot::channel();
        self.spawn_task(task, tx);
        Ok(TaskHandle::new(rx))
    }

    /// Submits a task, awaiting until a queue slot is available (true async
    /// backpressure for bounded queues).
    ///
    /// Rejects only if the pool is stopped.
    // `submit_async` intentionally returns the awaitable `TaskHandle`; under
    // `tracing::instrument` clippy otherwise flags it as yielding an awaitable.
    #[allow(clippy::async_yields_async)]
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            target = "pond",
            name = "pond.submit_async",
            level = "debug",
            skip_all
        )
    )]
    pub async fn submit_async<F, Fut, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();

        loop {
            if self.is_stopped() {
                return TaskHandle::resolved(Err(self.stop_error()));
            }
            if (self.inner.metrics.waiting() as usize) < self.inner.capacity {
                break;
            }
            let listener = self.inner.slot.listen();
            if (self.inner.metrics.waiting() as usize) < self.inner.capacity || self.is_stopped() {
                continue;
            }
            listener.await;
        }

        let (tx, rx) = oneshot::channel();
        self.spawn_task(task, tx);
        TaskHandle::new(rx)
    }

    fn spawn_task<F, Fut, T>(&self, task: F, tx: oneshot::Sender<Result<T>>)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_waiting();

        // A single `Arc` clone moved into the task future (the worker body). The
        // submit path itself uses `&self.inner`, saving one inc/dec per submit.
        let inner = self.inner.clone();
        let task_fut = async move {
            // Acquire a permit when concurrency is limited.
            // ordering: `Relaxed` — this flag only gates the fast path; the permit
            // handoff itself is synchronized by the channel.
            let _permit = if inner.limited.load(Ordering::Relaxed) {
                match inner.permit_rx.recv().await {
                    Ok(()) => Some(PermitGuard {
                        tx: inner.permit_tx.clone(),
                    }),
                    Err(_) => None, // channel closed (cancelled): proceed to dispose
                }
            } else {
                None
            };

            inner.metrics.dec_waiting();
            inner.slot.notify(1);

            // Cancelled tasks are disposed without running.
            if inner.cancel.is_cancelled() {
                inner.metrics.inc_canceled();
                trace_event!("pond.task canceled before execution");
                let _ = tx.send(Err(inner.cancel.cause().unwrap_or(Error::Canceled)));
                inner.maybe_notify_idle();
                return;
            }

            inner.metrics.inc_running();

            if inner.panic_recovery {
                match AssertUnwindSafe(task()).catch_unwind().await {
                    Ok(value) => {
                        inner.metrics.inc_successful();
                        let _ = tx.send(Ok(value));
                    }
                    Err(payload) => {
                        inner.metrics.inc_failed();
                        trace_event!("pond.task panicked");
                        let _ = tx.send(Err(Error::Panicked(panic_message(payload))));
                    }
                }
            } else {
                // No recovery: a panic here propagates on the worker task. The
                // handle's sender is dropped, so an awaiter observes `Canceled`.
                let value = task().await;
                inner.metrics.inc_successful();
                let _ = tx.send(Ok(value));
            }

            inner.metrics.dec_running();
            inner.maybe_notify_idle();
        };

        // The `pond.task` span's duration is the task's wall-clock time, including
        // any wait for a concurrency permit. Compiled out without `tracing`.
        #[cfg(feature = "tracing")]
        let task_fut = tracing::Instrument::instrument(
            task_fut,
            tracing::debug_span!(target: "pond", "pond.task"),
        );

        self.inner.spawner.spawn(Box::pin(task_fut));
    }

    /// Signals the pool to stop accepting new tasks and returns a future that
    /// completes once all queued and running tasks have finished.
    ///
    /// Already-queued tasks are still run to completion (graceful drain).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(target = "pond", name = "pond.stop", level = "debug", skip_all)
    )]
    pub fn stop(&self) -> StopHandle {
        // ordering: `Relaxed` — a plain stop flag; draining waits on the `idle`
        // event, which carries its own synchronization.
        self.inner.closed.store(true, Ordering::Relaxed);
        self.inner.slot.notify(usize::MAX);
        let inner = self.inner.clone();
        StopHandle {
            fut: Box::pin(async move { inner.wait_idle().await }),
        }
    }

    /// Stops the pool and waits for all tasks to finish.
    pub async fn stop_and_wait(&self) {
        self.stop().await;
    }

    /// Cancels the pool: queued tasks are abandoned (resolving with
    /// [`Error::Canceled`]) while running tasks are allowed to finish.
    pub fn cancel(&self) {
        self.inner.cancel.cancel();
    }

    /// Returns the [`CancelToken`] driving this pool's cancellation.
    pub fn cancel_token(&self) -> CancelToken {
        self.inner.cancel.clone()
    }

    /// Returns `true` if the pool has been stopped or cancelled.
    #[inline]
    pub fn is_stopped(&self) -> bool {
        // ordering: `Relaxed` for the stop flag; `is_cancelled` does its own
        // `Acquire` load to pick up the cancellation cause.
        self.inner.closed.load(Ordering::Relaxed) || self.inner.cancel.is_cancelled()
    }

    fn stop_error(&self) -> Error {
        if self.inner.cancel.is_cancelled() {
            self.inner.cancel.cause().unwrap_or(Error::Canceled)
        } else {
            Error::PoolStopped
        }
    }

    /// Dynamically changes the maximum concurrency.
    ///
    /// `0` makes concurrency unbounded. Growing adds permits immediately;
    /// shrinking retires permits as running tasks release them (in-flight tasks
    /// are never interrupted).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(target = "pond", name = "pond.resize", level = "debug", skip(self))
    )]
    pub fn resize(&self, max_concurrency: usize) {
        let inner = &self.inner;
        // ordering: `Relaxed` — `max_concurrency`/`limited` are config flags; the
        // permit channel below carries the actual concurrency synchronization.
        let old = inner
            .max_concurrency
            .swap(max_concurrency, Ordering::Relaxed);
        if max_concurrency == 0 {
            // Become unbounded: stop requiring permits and wake any waiters.
            inner.limited.store(false, Ordering::Relaxed);
            // Top up tokens so any task currently blocked on `recv` proceeds.
            for _ in 0..inner.permit_rx.receiver_count().max(1) {
                let _ = inner.permit_tx.try_send(());
            }
            return;
        }

        let was_unbounded = old == 0;
        inner.limited.store(true, Ordering::Relaxed);

        if was_unbounded {
            // Seed a fresh budget of permits.
            for _ in 0..max_concurrency {
                let _ = inner.permit_tx.try_send(());
            }
        } else if max_concurrency > old {
            for _ in 0..(max_concurrency - old) {
                let _ = inner.permit_tx.try_send(());
            }
        } else if max_concurrency < old {
            // Drain surplus permits as they become available.
            let surplus = old - max_concurrency;
            let rx = inner.permit_rx.clone();
            inner.spawner.spawn(Box::pin(async move {
                for _ in 0..surplus {
                    if rx.recv().await.is_err() {
                        break;
                    }
                }
            }));
        }
    }

    // --- Metrics accessors (mirror pond's BasePool) -------------------------

    /// Returns a point-in-time snapshot of all metrics.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    /// Number of tasks currently running.
    pub fn running_workers(&self) -> u64 {
        self.inner.metrics.running()
    }

    /// Total number of tasks ever submitted (including dropped).
    pub fn submitted_tasks(&self) -> u64 {
        self.inner.metrics.submitted()
    }

    /// Number of tasks currently queued (not yet running).
    pub fn waiting_tasks(&self) -> u64 {
        self.inner.metrics.waiting()
    }

    /// Number of tasks that completed successfully.
    pub fn successful_tasks(&self) -> u64 {
        self.inner.metrics.successful()
    }

    /// Number of tasks that completed with an error or panic.
    pub fn failed_tasks(&self) -> u64 {
        self.inner.metrics.failed()
    }

    /// Number of completed tasks (successful + failed).
    pub fn completed_tasks(&self) -> u64 {
        self.inner.metrics.completed()
    }

    /// Number of tasks dropped because the queue was full.
    pub fn dropped_tasks(&self) -> u64 {
        self.inner.metrics.dropped()
    }

    /// Number of tasks canceled before execution.
    pub fn canceled_tasks(&self) -> u64 {
        self.inner.metrics.canceled()
    }

    /// The configured maximum concurrency (`0` == unbounded).
    pub fn max_concurrency(&self) -> usize {
        // ordering: `Relaxed` — reading a config value, not synchronizing on it.
        self.inner.max_concurrency.load(Ordering::Relaxed)
    }

    // --- Groups & subpools --------------------------------------------------

    /// Creates a [`ResultTaskGroup`] whose tasks return `T`.
    pub fn group<T: Send + 'static>(&self) -> ResultTaskGroup<S, T> {
        ResultTaskGroup::new(self.clone())
    }

    /// Creates a [`TryTaskGroup`] of fallible tasks returning `Result<T, E>`.
    ///
    /// The first failing task cancels the remaining queued tasks.
    pub fn try_group<T: Send + 'static, E: Send + 'static>(&self) -> TryTaskGroup<S, T, E> {
        TryTaskGroup::new(self.clone())
    }

    /// Creates a subpool that runs on this pool but limits its own concurrency
    /// to `max_concurrency` tasks.
    ///
    /// # Panics
    ///
    /// Panics if `max_concurrency` exceeds this pool's bound (when bounded), or
    /// if it is zero. This matches pond's validation.
    pub fn subpool(&self, max_concurrency: usize) -> Subpool<S> {
        Subpool::new(self.clone(), max_concurrency)
    }
}

impl<S: Spawner> Inner<S> {
    fn maybe_notify_idle(&self) {
        if self.metrics.running() == 0 && self.metrics.waiting() == 0 {
            self.idle.notify(usize::MAX);
        }
    }

    fn is_idle(&self) -> bool {
        self.metrics.running() == 0 && self.metrics.waiting() == 0
    }

    async fn wait_idle(&self) {
        loop {
            if self.is_idle() {
                return;
            }
            // Keep the listener alive across the await so a notification that
            // fires while we're parked is not lost.
            let listener = self.idle.listen();
            if self.is_idle() {
                return;
            }
            listener.await;
        }
    }
}

/// Guard returned to a running task; releases its permit on drop.
struct PermitGuard {
    tx: async_channel::Sender<()>,
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        let _ = self.tx.try_send(());
    }
}

/// A handle to a submitted task. Awaiting it yields the task's result.
///
/// Dropping the handle does **not** cancel the task; it simply discards the result.
pub struct TaskHandle<T> {
    rx: oneshot::Receiver<Result<T>>,
}

impl<T> TaskHandle<T> {
    pub(crate) fn new(rx: oneshot::Receiver<Result<T>>) -> Self {
        Self { rx }
    }

    pub(crate) fn resolved(result: Result<T>) -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(result);
        Self { rx }
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            // Sender dropped without sending: the task panicked without recovery
            // (or the pool was torn down) — treat as cancellation.
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Canceled)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Future returned by [`Pool::stop`]; resolves when the pool is fully drained.
pub struct StopHandle {
    fut: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl Future for StopHandle {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.fut.as_mut().poll(cx)
    }
}
