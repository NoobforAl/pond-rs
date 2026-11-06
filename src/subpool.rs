//! Subpools: a child pool that runs on a parent pool but caps its own concurrency.
//!
//! A subpool shares the parent's runtime and worker budget, while additionally
//! limiting how many of *its* tasks run at once. Stopping or cancelling a subpool
//! does not affect the parent; stopping the parent drains queued subpool tasks,
//! and cancelling it abandons them. A `Subpool` mirrors the [`Pool`] surface: it can
//! submit (blocking / try / async / fire-and-forget / context-aware), resize,
//! cancel, and run task groups.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use event_listener::{Event, IntoNotification};
use futures::channel::oneshot;
use futures::FutureExt;

use crate::cancel::CancelToken;
use crate::context::TaskContext;
use crate::error::{panic_message, Error, Result};
use crate::group::{ResultTaskGroup, TryTaskGroup};
use crate::limiter::Limiter;
use crate::metrics::Metrics;
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
    limiter: Limiter,
    capacity: usize, // queued-task cap (usize::MAX == unbounded)
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
        Self::from_parts(parent, max_concurrency, usize::MAX, CancelToken::new())
    }

    pub(crate) fn from_parts(
        parent: Pool<S>,
        max_concurrency: usize,
        capacity: usize,
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

        let inner = Arc::new(SubInner {
            parent,
            limiter: Limiter::new(effective),
            capacity,
            metrics: Metrics::new(),
            outstanding: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            cancel,
            idle: Event::new(),
            slot: Event::new(),
            shutdown: Arc::new(Event::new()),
        });

        // Watch cancellation: close the limiter and wake waiters so cancelled
        // tasks observe it (works for `cancel()` and external tokens).
        //
        // Holds a `Weak<SubInner>` and races a `shutdown` signal (fired by
        // `Drop`) so it never keeps the subpool alive and exits promptly when the
        // last handle drops instead of parking on `cancelled()` forever.
        let weak = Arc::downgrade(&inner);
        let cancel = inner.cancel.clone();
        let shutdown = inner.shutdown.clone();
        inner.parent.spawn_detached(Box::pin(async move {
            if !cancel.cancelled_unless_dropped(&shutdown, &weak).await {
                return; // subpool dropped: `SubInner` is gone, nothing to do
            }
            if let Some(inner) = weak.upgrade() {
                inner.closed.store(true, Ordering::Relaxed);
                inner.limiter.close();
                inner.slot.notify(usize::MAX);
                inner.idle.notify(usize::MAX);
            }
        }));

        Subpool { inner }
    }

    /// Submits a task to the subpool. Never blocks; a stopped subpool or a full
    /// bounded queue resolves the handle with an [`Error`]. Use
    /// [`submit_async`](Self::submit_async) to wait for a queue slot instead of
    /// rejecting.
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
        if !self.inner.try_admit() {
            self.inner.metrics.inc_dropped();
            return TaskHandle::resolved(Err(Error::QueueFull));
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
        // admission step (`try_admit`), so from here on the guard settles them,
        // whatever happens to the task.
        let (tx, rx) = oneshot::channel();
        let mut guard = SubTask {
            inner: self.inner.clone(),
            tx: Some(tx),
            queued: true,
            running: false,
            reserved: false,
        };

        // Have the parent count this task from now on, so stopping the parent
        // drains it even while it is still queued here.
        guard.reserved = self.inner.parent.try_reserve().is_ok();
        if !guard.reserved {
            drop(guard); // the parent stopped since `is_stopped`: resolves `rx`
            return TaskHandle::new(rx);
        }

        // Take the subpool permit *before* asking the parent for anything: tasks
        // queued here must hold none of the parent's permits or queue slots, or a
        // busy subpool would starve the parent's other work. The result flows
        // through our own oneshot so we control cancellation, panic capture and
        // metrics.
        let run = self.inner.clone();
        self.inner.parent.spawn_detached_unboxed(async move {
            let mut guard = guard;
            // `None` means the subpool was cancelled; the check below disposes.
            let _permit = run.limiter.acquire().await;

            guard.queued = false;
            run.metrics.dec_waiting();
            run.slot.notify(1usize.additional());

            if run.cancel.is_cancelled() {
                return;
            }

            let guard = &mut guard;
            let run = &run;
            run.parent
                .run_reserved(move || async move {
                    // The subpool may have been cancelled while this task waited
                    // for the parent.
                    if run.cancel.is_cancelled() {
                        return;
                    }
                    guard.running = true;
                    run.metrics.inc_running();
                    // Called inside this block so the catch also covers a panic
                    // in the closure's synchronous part.
                    let body = async move { task().await };
                    match AssertUnwindSafe(body).catch_unwind().await {
                        Ok(value) => {
                            run.metrics.inc_successful();
                            guard.resolve(Ok(value));
                        }
                        Err(payload) => {
                            run.metrics.inc_failed();
                            guard.resolve(Err(Error::Panicked(panic_message(payload))));
                        }
                    }
                })
                .await;
        });

        TaskHandle::new(rx)
    }

    /// Returns `true` if the subpool has been stopped or cancelled, or its parent
    /// has (a subpool can't run anything once its parent is stopped).
    pub fn is_stopped(&self) -> bool {
        self.inner.closed.load(Ordering::Relaxed)
            || self.inner.cancel.is_cancelled()
            || self.inner.parent.is_stopped()
    }

    fn stop_error(&self) -> Error {
        self.inner.stop_error()
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
    /// `0` makes it unbounded, releasing every queued task. Cannot exceed the
    /// parent's bound (when bounded). As with [`Pool::resize`], lowering the limit
    /// never interrupts running tasks.
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
        self.inner.limiter.set_limit(max_concurrency);
        // A higher limit means more free workers, which also frees queue room.
        self.inner.slot.notify(usize::MAX);
    }

    /// The subpool's effective maximum concurrency (`0` == unbounded).
    pub fn max_concurrency(&self) -> usize {
        self.inner.limiter.limit()
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
}

impl<S: Spawner> SubInner<S> {
    fn is_idle(&self) -> bool {
        self.outstanding.load(Ordering::Relaxed) == 0
    }

    /// Why this subpool isn't accepting or running work. The parent comes before
    /// this subpool's own graceful stop, which rejects new work but never abandons
    /// queued tasks: if one was abandoned, a cancellation is the real reason.
    fn stop_error(&self) -> Error {
        if self.cancel.is_cancelled() {
            self.cancel.cause().unwrap_or(Error::Canceled)
        } else if self.parent.is_stopped() {
            self.parent.stop_error()
        } else if self.closed.load(Ordering::Relaxed) {
            Error::PoolStopped
        } else {
            Error::Canceled
        }
    }

    /// Atomically admits a task only if the bounded queue or a free worker has
    /// room.
    #[inline]
    fn try_admit(&self) -> bool {
        if self
            .metrics
            .try_reserve_waiting(self.capacity, || self.limiter.available())
        {
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
    cancel_token: CancelToken,
}

impl<S: Spawner> SubpoolBuilder<S> {
    pub(crate) fn new(parent: Pool<S>) -> Self {
        Self {
            parent,
            max_concurrency: 0,
            queue_size: QueueSize::Unbounded,
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

    /// Accepted for builder parity; no effect on a subpool (whose submits never
    /// block — a full bounded queue always rejects with
    /// [`Error::QueueFull`]). See [`Subpool::submit_async`] for the
    /// wait-for-a-slot path.
    pub fn non_blocking(self, _non_blocking: bool) -> Self {
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
            self.cancel_token,
        )
    }
}

/// One admitted subpool task. Settles the counters and signals idle on drop,
/// however the task ended: finished, panicked, disposed of by this subpool or its
/// parent, or dropped by a runtime shutting down (even before its first poll). A
/// task that never produced a result resolves its handle with the stop error.
struct SubTask<S: Spawner, T> {
    inner: Arc<SubInner<S>>,
    tx: Option<oneshot::Sender<Result<T>>>,
    queued: bool,
    running: bool,
    reserved: bool,
}

impl<S: Spawner, T> SubTask<S, T> {
    fn resolve(&mut self, result: Result<T>) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(result);
        }
    }
}

impl<S: Spawner, T> Drop for SubTask<S, T> {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        if self.queued {
            inner.metrics.dec_waiting();
        }
        if self.running {
            inner.metrics.dec_running();
        } else {
            inner.metrics.inc_canceled();
        }
        self.resolve(Err(inner.stop_error()));
        // The permit this task held makes a worker free, which is queue room.
        // (Nobody ever waits for room in an unbounded queue.)
        if inner.capacity != usize::MAX {
            inner.slot.notify(1usize.additional());
        }
        if self.reserved {
            inner.parent.release_reservation();
        }
        inner.dispatch_done();
    }
}
