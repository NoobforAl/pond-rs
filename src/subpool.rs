//! Subpools: a child pool that runs on a parent pool but caps its own concurrency.
//!
//! A subpool shares the parent's runtime and worker budget, while additionally
//! limiting how many of *its* tasks run at once. Stopping or cancelling a subpool
//! does not affect the parent. A `Subpool` mirrors the [`Pool`] surface: it can
//! submit (blocking / try / async / fire-and-forget / context-aware), resize,
//! cancel, run task groups, and report the same metrics.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use event_listener::Event;
use futures::channel::oneshot;
use futures::future::{select, Either};
use futures::FutureExt;

use crate::cancel::CancelToken;
use crate::context::TaskContext;
use crate::error::{panic_message, Error, Result};
use crate::group::{ResultTaskGroup, TryTaskGroup};
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::options::QueueSize;
use crate::pool::{Pool, StopHandle, TaskHandle};
use crate::runtime::Spawner;
use crate::submit::TaskSink;

/// A concurrency-limited child of a [`Pool`].
///
/// A subpool runs on its parent's workers but caps how many it may use at once,
/// so one workload can't starve the rest. Create one with
/// [`Pool::subpool`](crate::Pool::subpool).
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use pond::Pool;
///
/// let pool = Pool::builder().max_concurrency(100).build();
/// let subpool = pool.subpool(10); // at most 10 of the 100 workers
/// let handle = subpool.submit(|| async { 6 * 7 });
/// assert_eq!(handle.await.unwrap(), 42);
/// # }
/// ```
pub struct Subpool<S: Spawner> {
    inner: Arc<SubInner<S>>,
}

impl<S: Spawner> Clone for Subpool<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct SubInner<S: Spawner> {
    parent: Pool<S>,
    permit_tx: async_channel::Sender<()>,
    permit_rx: async_channel::Receiver<()>,
    limited: AtomicBool,
    max_concurrency: AtomicUsize,
    // Serializes `resize` so concurrent calls don't interleave permit changes.
    resize_lock: std::sync::Mutex<()>,
    capacity: usize, // queued-task cap (usize::MAX == unbounded)
    non_blocking: bool,
    metrics: Metrics,
    // Tasks admitted but not yet terminal; idle detection keys off this single
    // counter to avoid the dequeue/start race (see `Pool`'s `outstanding`).
    outstanding: AtomicUsize,
    closed: AtomicBool,
    cancel: CancelToken,
    idle: Event,
    slot: Event,
    // Notified when the last `Subpool` handle drops, so the cancellation watcher
    // (which holds a `Weak<SubInner>`) wakes and terminates. See `Drop`.
    shutdown: Arc<Event>,
}

impl<S: Spawner> Subpool<S> {
    pub(crate) fn new(parent: Pool<S>, max_concurrency: usize) -> Self {
        Self::from_parts(
            parent,
            max_concurrency,
            usize::MAX,
            false,
            CancelToken::new(),
        )
    }

    pub(crate) fn from_parts(
        parent: Pool<S>,
        max_concurrency: usize,
        capacity: usize,
        non_blocking: bool,
        cancel: CancelToken,
    ) -> Self {
        let parent_max = parent.max_concurrency();
        // Validate against the parent's bound.
        if parent_max != 0 && max_concurrency > parent_max {
            panic!(
                "subpool max_concurrency ({max_concurrency}) cannot exceed parent's ({parent_max})"
            );
        }
        // 0 means "inherit the parent's limit" (unbounded if the parent is too).
        let effective = if max_concurrency == 0 {
            parent_max
        } else {
            max_concurrency
        };
        let limited = effective != 0;

        let (permit_tx, permit_rx) = async_channel::unbounded::<()>();
        if limited {
            for _ in 0..effective {
                let _ = permit_tx.try_send(());
            }
        }

        let inner = Arc::new(SubInner {
            parent,
            permit_tx,
            permit_rx,
            limited: AtomicBool::new(limited),
            max_concurrency: AtomicUsize::new(effective),
            resize_lock: std::sync::Mutex::new(()),
            capacity,
            non_blocking,
            metrics: Metrics::new(),
            outstanding: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            cancel,
            idle: Event::new(),
            slot: Event::new(),
            shutdown: Arc::new(Event::new()),
        });

        // Watch cancellation: close the permit channel and wake waiters so
        // cancelled tasks observe it (works for `cancel()` and external tokens).
        //
        // Holds a `Weak<SubInner>` and races a `shutdown` signal (fired by
        // `Drop`) so it never keeps the subpool alive and exits promptly when the
        // last handle drops instead of parking on `cancelled()` forever.
        let weak = Arc::downgrade(&inner);
        let cancel = inner.cancel.clone();
        let shutdown = inner.shutdown.clone();
        inner.parent.spawn_detached(Box::pin(async move {
            let cancelled = Box::pin(cancel.cancelled());
            if let Either::Right(_) = select(cancelled, shutdown.listen()).await {
                return; // subpool dropped: `SubInner` is gone, nothing to do
            }
            if let Some(inner) = weak.upgrade() {
                inner.closed.store(true, Ordering::Relaxed);
                inner.permit_rx.close();
                inner.permit_tx.close();
                inner.slot.notify(usize::MAX);
                inner.idle.notify(usize::MAX);
            }
        }));

        Subpool { inner }
    }

    /// Submits a task to the subpool. Never blocks; a stopped subpool or a full
    /// bounded queue (when non-blocking) resolves the handle with an [`Error`].
    ///
    /// # Panics
    ///
    /// A panicking task is **always** caught and surfaced as
    /// [`Error::Panicked`] on the returned handle, independent of the parent
    /// pool's [`panic_recovery`](crate::PoolBuilder::panic_recovery) setting. A
    /// subpool is an isolation boundary, so one task's panic never aborts the
    /// shared parent workers.
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
        if self.inner.non_blocking {
            if !self.inner.try_admit() {
                self.inner.metrics.inc_dropped();
                warn_event!(
                    capacity = self.inner.capacity,
                    "pond.subpool.submit dropped task: queue full"
                );
                return TaskHandle::resolved(Err(Error::QueueFull));
            }
        } else {
            self.inner.admit();
        }
        self.spawn_task(task)
    }

    /// Submits a task, rejecting immediately if the subpool is stopped or its
    /// bounded queue is full.
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
        if !self.inner.try_admit() {
            self.inner.metrics.inc_dropped();
            return Err(Error::QueueFull);
        }
        Ok(self.spawn_task(task))
    }

    /// Submits a task, awaiting a free queue slot for bounded subpools.
    /// Rejects only if the subpool is stopped.
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
            if self.inner.try_admit() {
                break;
            }
            let listener = self.inner.slot.listen();
            if self.inner.try_admit() {
                break;
            }
            if self.is_stopped() {
                return TaskHandle::resolved(Err(self.stop_error()));
            }
            listener.await;
        }
        self.spawn_task(task)
    }

    /// Fire-and-forget submission (discards the result handle).
    pub fn spawn<F, Fut, T>(&self, task: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        drop(self.submit(task));
    }

    /// Submits a task that receives a [`TaskContext`] bound to the subpool's
    /// cancel token and the parent pool's shared data.
    pub fn submit_ctx<F, Fut, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let ctx = TaskContext::new(self.inner.cancel.clone(), self.inner.parent.task_data());
        self.submit(move || task(ctx))
    }

    fn spawn_task<F, Fut, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // Queued + outstanding counters were already bumped by the caller's
        // admission step (`admit`/`try_admit`).
        let (tx, rx) = oneshot::channel();
        let run = self.inner.clone();

        // Run on the parent (consuming a parent permit), gated by a subpool
        // permit. The task's result flows through our own oneshot so we control
        // cancellation, panic capture and metrics.
        self.inner.parent.spawn(move || async move {
            let _permit = if run.limited.load(Ordering::Relaxed) {
                // Child span: time spent queued behind the subpool's concurrency
                // cap. Compiled out without `tracing`.
                let recv = run.permit_rx.recv();
                #[cfg(feature = "tracing")]
                let recv = tracing::Instrument::instrument(
                    recv,
                    tracing::debug_span!(target: "pond", "pond.task.wait_permit"),
                );
                match recv.await {
                    Ok(()) => Some(SubPermit { inner: run.clone() }),
                    Err(_) => None, // channel closed (cancelled): proceed to dispose
                }
            } else {
                None
            };

            run.metrics.dec_waiting();
            run.slot.notify(1);

            if run.cancel.is_cancelled() {
                run.metrics.inc_canceled();
                let _ = tx.send(Err(run.cancel.cause().unwrap_or(Error::Canceled)));
                run.dispatch_done();
                return;
            }

            run.metrics.inc_running();
            match AssertUnwindSafe(task()).catch_unwind().await {
                Ok(value) => {
                    run.metrics.inc_successful();
                    let _ = tx.send(Ok(value));
                }
                Err(payload) => {
                    run.metrics.inc_failed();
                    let _ = tx.send(Err(Error::Panicked(panic_message(payload))));
                }
            }
            run.metrics.dec_running();
            run.dispatch_done();
        });

        TaskHandle::new(rx)
    }

    /// Returns `true` if the subpool has been stopped or cancelled.
    pub fn is_stopped(&self) -> bool {
        self.inner.closed.load(Ordering::Relaxed) || self.inner.cancel.is_cancelled()
    }

    fn stop_error(&self) -> Error {
        if self.inner.cancel.is_cancelled() {
            self.inner.cancel.cause().unwrap_or(Error::Canceled)
        } else {
            Error::PoolStopped
        }
    }

    /// Signals the subpool to stop and returns a future that completes once its
    /// queued and running tasks finish. The parent pool is unaffected.
    pub fn stop(&self) -> StopHandle {
        self.inner.closed.store(true, Ordering::Relaxed);
        self.inner.slot.notify(usize::MAX);
        let inner = self.inner.clone();
        StopHandle::new(Box::pin(async move { inner.wait_idle().await }))
    }

    /// Stops the subpool and waits for its in-flight and queued tasks to finish.
    pub async fn stop_and_wait(&self) {
        self.stop().await;
    }

    /// Cancels the subpool: queued tasks are abandoned (resolving with
    /// [`Error::Canceled`]); running tasks finish. The parent is unaffected.
    pub fn cancel(&self) {
        self.inner.cancel.cancel();
    }

    /// Returns the [`CancelToken`] driving this subpool's cancellation.
    pub fn cancel_token(&self) -> CancelToken {
        self.inner.cancel.clone()
    }

    /// Dynamically changes the subpool's maximum concurrency.
    ///
    /// `0` makes it unbounded. Cannot exceed the parent's bound (when bounded).
    ///
    /// # Panics
    ///
    /// Panics if `max_concurrency` exceeds the parent's bound.
    pub fn resize(&self, max_concurrency: usize) {
        let parent_max = self.inner.parent.max_concurrency();
        if parent_max != 0 && max_concurrency > parent_max {
            panic!(
                "subpool max_concurrency ({max_concurrency}) cannot exceed parent's ({parent_max})"
            );
        }
        let inner = &self.inner;
        // Serialize against other resizes so permit reconciliation is atomic.
        let _resize = inner.resize_lock.lock().unwrap();
        let old = inner
            .max_concurrency
            .swap(max_concurrency, Ordering::Relaxed);
        if max_concurrency == 0 {
            inner.limited.store(false, Ordering::Relaxed);
            for _ in 0..inner.permit_rx.receiver_count().max(1) {
                let _ = inner.permit_tx.try_send(());
            }
            return;
        }
        let was_unbounded = old == 0;
        inner.limited.store(true, Ordering::Relaxed);
        if was_unbounded {
            for _ in 0..max_concurrency {
                let _ = inner.permit_tx.try_send(());
            }
        } else if max_concurrency > old {
            for _ in 0..(max_concurrency - old) {
                let _ = inner.permit_tx.try_send(());
            }
        } else if max_concurrency < old {
            let surplus = old - max_concurrency;
            let rx = inner.permit_rx.clone();
            inner.parent.spawn_detached(Box::pin(async move {
                for _ in 0..surplus {
                    if rx.recv().await.is_err() {
                        break;
                    }
                }
            }));
        }
    }

    /// The subpool's effective maximum concurrency (`0` == unbounded).
    pub fn max_concurrency(&self) -> usize {
        self.inner.max_concurrency.load(Ordering::Relaxed)
    }

    /// Creates a [`ResultTaskGroup`] scoped to this subpool.
    pub fn group<T: Send + 'static>(&self) -> ResultTaskGroup<Subpool<S>, T> {
        ResultTaskGroup::new(self.clone())
    }

    /// Creates a [`TryTaskGroup`] scoped to this subpool.
    pub fn try_group<T: Send + 'static, E: Send + 'static>(
        &self,
    ) -> TryTaskGroup<Subpool<S>, T, E> {
        TryTaskGroup::new(self.clone())
    }

    // --- Metrics accessors --------------------------------------------------

    /// A snapshot of the subpool's own metrics.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    /// Number of subpool tasks currently running.
    pub fn running_workers(&self) -> u64 {
        self.inner.metrics.running()
    }

    /// Number of subpool tasks submitted.
    pub fn submitted_tasks(&self) -> u64 {
        self.inner.metrics.submitted()
    }

    /// Number of subpool tasks currently queued (not yet running).
    pub fn waiting_tasks(&self) -> u64 {
        self.inner.metrics.waiting()
    }

    /// Number of subpool tasks that completed successfully.
    pub fn successful_tasks(&self) -> u64 {
        self.inner.metrics.successful()
    }

    /// Number of subpool tasks that completed with an error or panic.
    pub fn failed_tasks(&self) -> u64 {
        self.inner.metrics.failed()
    }

    /// Number of completed subpool tasks (successful + failed).
    pub fn completed_tasks(&self) -> u64 {
        self.inner.metrics.completed()
    }

    /// Number of subpool tasks dropped because the queue was full.
    pub fn dropped_tasks(&self) -> u64 {
        self.inner.metrics.dropped()
    }

    /// Number of subpool tasks canceled before execution.
    pub fn canceled_tasks(&self) -> u64 {
        self.inner.metrics.canceled()
    }
}

impl<S: Spawner> SubInner<S> {
    fn is_idle(&self) -> bool {
        self.outstanding.load(Ordering::Relaxed) == 0
    }

    /// Unconditionally admits a task (bumps queued + outstanding counters).
    #[inline]
    fn admit(&self) {
        self.metrics.inc_waiting();
        self.outstanding.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically admits a task only if the bounded queue has room.
    #[inline]
    fn try_admit(&self) -> bool {
        if self.metrics.try_reserve_waiting(self.capacity) {
            self.outstanding.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Records a task reaching a terminal state; signals idle if it was the last.
    #[inline]
    fn dispatch_done(&self) {
        if self.outstanding.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.idle.notify(usize::MAX);
        }
    }

    async fn wait_idle(&self) {
        loop {
            if self.is_idle() {
                return;
            }
            let listener = self.idle.listen();
            if self.is_idle() {
                return;
            }
            listener.await;
        }
    }
}

impl<S: Spawner> Drop for SubInner<S> {
    fn drop(&mut self) {
        // Wake the cancellation watcher (it holds only a `Weak<SubInner>`) so it
        // terminates rather than leaking as a parked task on the parent runtime.
        self.shutdown.notify(usize::MAX);
    }
}

impl<S: Spawner> TaskSink for Subpool<S> {
    fn submit<F, Fut, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.submit(task)
    }

    fn cancel_token(&self) -> CancelToken {
        self.cancel_token()
    }

    fn task_data(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.inner.parent.task_data()
    }
}

/// Builder for a [`Subpool`] with a concurrency cap, a bounded queue, and/or an
/// external cancel token (the analogue of Go pond's `NewSubpoolContext`).
pub struct SubpoolBuilder<S: Spawner> {
    parent: Pool<S>,
    max_concurrency: usize,
    queue_size: QueueSize,
    non_blocking: bool,
    cancel_token: CancelToken,
}

impl<S: Spawner> SubpoolBuilder<S> {
    pub(crate) fn new(parent: Pool<S>) -> Self {
        Self {
            parent,
            max_concurrency: 0,
            queue_size: QueueSize::Unbounded,
            non_blocking: false,
            cancel_token: CancelToken::new(),
        }
    }

    /// Caps the subpool's concurrency (`0` = inherit the parent's limit).
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max;
        self
    }

    /// Sets the subpool's queue capacity (tasks accepted but not yet running).
    pub fn queue_size(mut self, size: impl Into<QueueSize>) -> Self {
        self.queue_size = size.into();
        self
    }

    /// Makes the subpool non-blocking: a full bounded queue rejects new tasks.
    pub fn non_blocking(mut self, non_blocking: bool) -> Self {
        self.non_blocking = non_blocking;
        self
    }

    /// Uses an external [`CancelToken`] for the subpool.
    pub fn cancel_token(mut self, token: CancelToken) -> Self {
        self.cancel_token = token;
        self
    }

    /// Builds the subpool.
    ///
    /// # Panics
    ///
    /// Panics if `max_concurrency` exceeds the parent's bound (when bounded).
    pub fn build(self) -> Subpool<S> {
        Subpool::from_parts(
            self.parent,
            self.max_concurrency,
            self.queue_size.capacity(),
            self.non_blocking,
            self.cancel_token,
        )
    }
}

/// Releases a subpool permit on drop.
struct SubPermit<S: Spawner> {
    inner: Arc<SubInner<S>>,
}

impl<S: Spawner> Drop for SubPermit<S> {
    fn drop(&mut self) {
        let _ = self.inner.permit_tx.try_send(());
    }
}
