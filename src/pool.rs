//! The async worker [`Pool`].

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use event_listener::{Event, IntoNotification};
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::FutureExt;

use crate::cancel::CancelToken;
use crate::context::TaskContext;
use crate::error::{panic_message, Error, Result};
use crate::group::{GroupBuilder, ResultTaskGroup, TryGroupBuilder, TryTaskGroup};
use crate::limiter::Limiter;
use crate::metrics::Metrics;
use crate::options::PoolBuilder;
use crate::runtime::Spawner;
use crate::submit::TaskSink;
use crate::subpool::{Subpool, SubpoolBuilder};

#[cfg(all(feature = "runtime-async-std", not(feature = "runtime-tokio")))]
use crate::runtime::AsyncStdSpawner;
#[cfg(all(feature = "runtime-smol", not(feature = "runtime-tokio")))]
use crate::runtime::SmolSpawner;
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
    // A task must hold a permit from this to run.
    limiter: Limiter,
    min_concurrency: usize, // resize floor (0 == none)
    capacity: usize,        // queue capacity (usize::MAX == unbounded)
    panic_recovery: bool,
    closed: AtomicBool,
    cancel: CancelToken,
    metrics: Metrics,
    // Tasks admitted but not yet at a terminal state (queued *or* running),
    // incremented exactly once at admission and decremented exactly once when the
    // task finishes (completed, failed, or disposed). Idle detection keys off this
    // single counter rather than `waiting == 0 && running == 0`, which has a race
    // window between a task being dequeued (waiting--) and started (running++).
    outstanding: AtomicUsize,
    idle: Event,    // notified when the pool becomes fully idle
    slot: Event,    // notified when a queue slot frees up
    stopped: Event, // notified when the pool is stopped or cancelled
    // Notified when the last `Pool` handle drops, so the background watcher
    // (which holds a `Weak<Inner>`) wakes and terminates instead of parking
    // forever. See `Drop for Inner`.
    shutdown: Arc<Event>,
    task_data: Option<Arc<dyn Any + Send + Sync>>,
    name: Option<String>,
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

// On a non-Tokio build, `Pool::builder()` binds to the enabled runtime's global
// executor instead, so code written against `Pool::builder()` (examples,
// doctests) compiles under any single-runtime feature set. When `runtime-tokio`
// is enabled, the impl above wins regardless of the other features.
#[cfg(all(feature = "runtime-smol", not(feature = "runtime-tokio")))]
impl Pool<SmolSpawner> {
    /// Starts building a pool backed by the smol global executor
    /// (`async-global-executor`). Worker threads start lazily on first use.
    pub fn builder() -> PoolBuilder<SmolSpawner> {
        PoolBuilder::new(SmolSpawner::new())
    }
}

#[cfg(all(feature = "runtime-async-std", not(feature = "runtime-tokio")))]
impl Pool<AsyncStdSpawner> {
    /// Starts building a pool backed by the `async-std` global executor, which
    /// starts on demand.
    pub fn builder() -> PoolBuilder<AsyncStdSpawner> {
        PoolBuilder::new(AsyncStdSpawner::new())
    }
}

impl<S: Spawner> Pool<S> {
    /// Starts building a pool backed by a custom [`Spawner`].
    pub fn builder_with(spawner: S) -> PoolBuilder<S> {
        PoolBuilder::new(spawner)
    }

    pub(crate) fn from_builder(mut b: PoolBuilder<S>) -> Self {
        let cancel_when = b.cancel_when.take();
        let deadline = b.deadline.take();
        let task_data = b.task_data.take();
        let name = b.pool_name.take();

        let inner = Arc::new(Inner {
            spawner: b.spawner,
            limiter: Limiter::new(b.max_concurrency),
            min_concurrency: b.min_concurrency,
            capacity: b.queue_size.capacity(),
            panic_recovery: b.panic_recovery,
            closed: AtomicBool::new(false),
            cancel: b.cancel_token,
            metrics: Metrics::new(),
            outstanding: AtomicUsize::new(0),
            idle: Event::new(),
            slot: Event::new(),
            stopped: Event::new(),
            shutdown: Arc::new(Event::new()),
            task_data,
            name,
        });

        // Watch the cancel token: when it fires, stop accepting tasks and wake
        // any tasks blocked on a permit so they can observe the cancellation.
        //
        // The watcher holds a `Weak<Inner>` (not a strong `Arc`), so it never
        // keeps the pool alive on its own. It also races a `shutdown` signal that
        // `Drop for Inner` fires, so when the last `Pool` handle drops the watcher
        // wakes and exits instead of parking on `cancelled()` forever.
        let weak = Arc::downgrade(&inner);
        let cancel = inner.cancel.clone();
        let shutdown = inner.shutdown.clone();
        inner.spawner.spawn(Box::pin(async move {
            if !cancel.cancelled_unless_dropped(&shutdown, &weak).await {
                return; // pool dropped: nothing to clean up, `Inner` is gone
            }
            if let Some(inner) = weak.upgrade() {
                // ordering: `Relaxed` — `closed` is an independent stop flag; the
                // limiter and event notifications below do the synchronizing.
                inner.closed.store(true, Ordering::Relaxed);
                inner.limiter.close();
                inner.slot.notify(usize::MAX);
                inner.stopped.notify(usize::MAX);
                inner.idle.notify(usize::MAX);
            }
        }));

        // Bridge external cancellation sources into the pool's cancel token.
        if let Some(fut) = cancel_when {
            inner.cancel.cancel_on(&inner.spawner, fut);
        }
        if let Some(dur) = deadline {
            inner.cancel.cancel_after(&inner.spawner, dur);
        }

        Pool { inner }
    }

    /// The pool's configured name, if any (set via
    /// [`PoolBuilder::pool_name`](crate::PoolBuilder::pool_name)).
    pub fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    /// Submits a task, returning a [`TaskHandle`] that resolves with its result.
    ///
    /// This call never blocks. If the pool is stopped, or its bounded queue is
    /// full, the returned handle resolves with the corresponding [`Error`] (the
    /// task is not run). Use [`submit_async`](Self::submit_async) to wait for a
    /// queue slot instead of rejecting.
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

        // Bounded queues reject when full (submit never blocks, so a full queue
        // can only reject — never grow past its capacity). Unbounded queues
        // (`capacity == usize::MAX`) always admit.
        if !self.inner.try_admit() {
            self.inner.metrics.inc_dropped();
            return TaskHandle::resolved(Err(Error::QueueFull));
        }

        let (tx, rx) = oneshot::channel();
        self.spawn_task(task, Some(tx));
        TaskHandle::new(rx)
    }

    /// Submits a task fire-and-forget, without creating a result channel.
    ///
    /// The task still runs; because no [`TaskHandle`] is wanted, no oneshot
    /// channel is allocated (a fire-and-forget storm therefore costs less
    /// memory than the equivalent `submit` + dropped-handle loop). On a
    /// stopped pool the task is not run; on a full bounded queue it is dropped.
    pub fn spawn<F, Fut, T>(&self, task: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();

        if self.is_stopped() {
            return;
        }
        if !self.inner.try_admit() {
            self.inner.metrics.inc_dropped();
            return;
        }
        self.spawn_task(task, None);
    }

    /// Submits a task that receives a [`TaskContext`], returning a handle.
    ///
    /// The context lets the task observe cancellation (to break out early) and
    /// read shared data attached via
    /// [`PoolBuilder::task_data`](crate::PoolBuilder::task_data).
    pub fn submit_ctx<F, Fut, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let ctx = self.task_context();
        self.submit(move || task(ctx))
    }

    /// Fire-and-forget variant of [`submit_ctx`](Self::submit_ctx).
    pub fn spawn_ctx<F, Fut, T>(&self, task: F)
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let ctx = self.task_context();
        self.spawn(move || task(ctx));
    }

    /// Builds a [`TaskContext`] bound to this pool's cancel token and shared data.
    pub(crate) fn task_context(&self) -> TaskContext {
        TaskContext::new(self.inner.cancel.clone(), self.inner.task_data.clone())
    }

    /// The shared task data attached to this pool, if any.
    pub(crate) fn task_data(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.inner.task_data.clone()
    }

    /// Spawns a detached future on the pool's runtime, bypassing the queue,
    /// permits and metrics. Used internally for watchers (e.g. subpools).
    pub(crate) fn spawn_detached(&self, fut: BoxFuture<'static, ()>) {
        self.inner.spawner.spawn(fut);
    }

    /// The runtime's sleep future for `dur`. Used by the scheduling helpers in
    /// `schedule.rs` (a sibling module that can't reach the private spawner).
    pub(crate) fn sleep(&self, dur: std::time::Duration) -> BoxFuture<'static, ()> {
        self.inner.spawner.sleep(dur)
    }

    /// Like [`spawn_detached`](Self::spawn_detached), without boxing the future.
    pub(crate) fn spawn_detached_unboxed<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.spawner.spawn_unboxed(fut);
    }

    /// Resolves once the pool is stopped or cancelled.
    pub(crate) async fn stopped(&self) {
        loop {
            if self.is_stopped() {
                return;
            }
            let listener = self.inner.stopped.listen();
            // ordering: pairs with the full fence in `Event::notify`, so either
            // this re-check sees the stop or the notify sees the listener.
            fence(Ordering::SeqCst);
            if self.is_stopped() {
                return;
            }
            listener.await;
        }
    }

    /// Counts work a subpool has accepted but not yet handed over, so stopping
    /// this pool drains it too. Fails if the pool is already stopped. Each `Ok`
    /// must be paired with [`release_reservation`](Self::release_reservation).
    pub(crate) fn try_reserve(&self) -> Result<()> {
        if self.is_stopped() {
            return Err(self.stop_error());
        }
        self.inner.inc_outstanding();
        Ok(())
    }

    pub(crate) fn release_reservation(&self) {
        self.inner.dispatch_done();
    }

    /// Runs `task` inline on the caller's runtime task, under this pool's queue
    /// and permit rules, on behalf of a subpool holding a
    /// [`try_reserve`](Self::try_reserve). The reservation is why a graceful stop
    /// doesn't reject here; if the pool is cancelled, `task` is dropped unrun.
    pub(crate) async fn run_reserved<F, Fut, T>(&self, task: F)
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = T> + Send,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();
        loop {
            if self.inner.cancel.is_cancelled() {
                return;
            }
            if self.inner.try_admit() {
                break;
            }
            let listener = self.inner.slot.listen();
            if self.inner.try_admit() {
                break;
            }
            if self.inner.cancel.is_cancelled() {
                return;
            }
            listener.await;
        }
        run_task(TaskGuard::new(self.inner.clone()), task, None).await;
    }

    /// Submits a task, rejecting immediately if the pool is stopped or its
    /// bounded queue is full.
    ///
    /// Unlike [`submit`](Self::submit), the error is returned directly rather
    /// than surfaced through the handle.
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

        let (tx, rx) = oneshot::channel();
        self.spawn_task(task, Some(tx));
        Ok(TaskHandle::new(rx))
    }

    /// Submits a task, awaiting until a queue slot is available (true async
    /// backpressure for bounded queues).
    ///
    /// Rejects only if the pool is stopped.
    pub async fn submit_async<F, Fut, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();

        // Atomically reserve a queue slot, parking on `slot` until one frees. The
        // reservation (not a bare capacity read) is what makes this real
        // backpressure even under many concurrent producers.
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

        let (tx, rx) = oneshot::channel();
        self.spawn_task(task, Some(tx));
        TaskHandle::new(rx)
    }

    fn spawn_task<F, Fut, T>(&self, task: F, tx: Option<oneshot::Sender<Result<T>>>)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // The queued metric and the outstanding (idle) counter were already
        // bumped by the caller's admission step (`try_admit`). `tx` is `None`
        // for fire-and-forget tasks, which skip the result channel entirely.
        //
        // The guard exists before the future does, so even a future the runtime
        // drops without ever polling (it is shutting down) still settles them.
        let guard = TaskGuard::new(self.inner.clone());

        // Hand the concrete future to the runtime unboxed: the runtime's own
        // task allocation stores it inline, so the pool no longer pays a
        // separate `Box` allocation (and an indirection per poll) per task.
        self.inner.spawner.spawn_unboxed(run_task(guard, task, tx));
    }

    /// Signals the pool to stop accepting new tasks and returns a future that
    /// completes once all queued and running tasks have finished.
    ///
    /// Already-queued tasks are still run to completion (graceful drain).
    pub fn stop(&self) -> StopHandle {
        // ordering: `Relaxed` — a plain stop flag; draining waits on the `idle`
        // event, which carries its own synchronization.
        self.inner.closed.store(true, Ordering::Relaxed);
        self.inner.slot.notify(usize::MAX);
        self.inner.stopped.notify(usize::MAX);
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

    pub(crate) fn stop_error(&self) -> Error {
        if self.inner.cancel.is_cancelled() {
            self.inner.cancel.cause().unwrap_or(Error::Canceled)
        } else {
            Error::PoolStopped
        }
    }

    /// Dynamically changes the maximum concurrency.
    ///
    /// `0` makes concurrency unbounded, releasing every queued task. Raising the
    /// limit lets more queued tasks start immediately; lowering it holds new
    /// starts until enough running tasks have finished (in-flight tasks are never
    /// interrupted). A bounded limit is raised to the configured
    /// [`min_concurrency`](crate::PoolBuilder::min_concurrency) floor if needed.
    pub fn resize(&self, max_concurrency: usize) {
        let inner = &self.inner;
        // Honor the configured floor: never shrink bounded concurrency below it.
        let max_concurrency = if max_concurrency == 0 {
            0
        } else {
            max_concurrency.max(inner.min_concurrency)
        };
        inner.limiter.set_limit(max_concurrency);
        // A higher limit means more free workers, which also frees queue room.
        inner.slot.notify(usize::MAX);
    }

    // --- Concurrency config ---------------------------------------------------

    /// The configured maximum concurrency (`0` == unbounded).
    pub fn max_concurrency(&self) -> usize {
        self.inner.limiter.limit()
    }

    // --- Groups & subpools --------------------------------------------------

    /// Creates a [`ResultTaskGroup`] whose tasks return `T`.
    pub fn group<T: Send + 'static>(&self) -> ResultTaskGroup<Pool<S>, T> {
        ResultTaskGroup::new(self.clone())
    }

    /// Creates a [`TryTaskGroup`] of fallible tasks returning `Result<T, E>`.
    ///
    /// The first failing task cancels the remaining queued tasks.
    pub fn try_group<T: Send + 'static, E: Send + 'static>(&self) -> TryTaskGroup<Pool<S>, T, E> {
        TryTaskGroup::new(self.clone())
    }

    /// Starts building a [`ResultTaskGroup`] with a concurrency cap and/or an
    /// external cancel token.
    ///
    /// E.g. on a pool of 100, `pool.group_builder::<T>().max_concurrency(10)`
    /// limits the group to 10 workers, leaving 90 for other work.
    pub fn group_builder<T: Send + 'static>(&self) -> GroupBuilder<S, T> {
        GroupBuilder::new(self.clone())
    }

    /// Starts building a [`TryTaskGroup`] with a concurrency cap and/or an
    /// external cancel token.
    pub fn try_group_builder<T: Send + 'static, E: Send + 'static>(
        &self,
    ) -> TryGroupBuilder<S, T, E> {
        TryGroupBuilder::new(self.clone())
    }

    /// Starts building a [`Subpool`] with a concurrency cap, a bounded queue,
    /// and/or an external cancel token.
    pub fn subpool_builder(&self) -> SubpoolBuilder<S> {
        SubpoolBuilder::new(self.clone())
    }

    /// Creates a subpool that runs on this pool but limits its own concurrency
    /// to `max_concurrency` tasks (`0` inherits this pool's limit).
    ///
    /// # Panics
    ///
    /// Panics if `max_concurrency` exceeds this pool's bound (when bounded).
    pub fn subpool(&self, max_concurrency: usize) -> Subpool<S> {
        Subpool::new(self.clone(), max_concurrency)
    }
}

impl<S: Spawner> Inner<S> {
    /// Records an admitted task (queued, not yet terminal).
    #[inline]
    fn inc_outstanding(&self) {
        self.outstanding.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically admits a task only if the bounded queue or a free worker has
    /// room, closing the check-then-increment race. Returns `false` (admitting
    /// nothing) when full. An unbounded queue (`capacity == usize::MAX`) always
    /// admits.
    #[inline]
    fn try_admit(&self) -> bool {
        if self
            .metrics
            .try_reserve_waiting(self.capacity, || self.limiter.available())
        {
            self.inc_outstanding();
            true
        } else {
            false
        }
    }

    /// Records a task reaching a terminal state and signals idle if it was the
    /// last one. Called exactly once per admitted task.
    #[inline]
    fn dispatch_done(&self) {
        if self.outstanding.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.idle.notify(usize::MAX);
        }
    }

    fn is_idle(&self) -> bool {
        self.outstanding.load(Ordering::Relaxed) == 0
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

impl<S: Spawner> Drop for Inner<S> {
    fn drop(&mut self) {
        // The last `Pool` handle is gone and no task futures reference us (they
        // each hold a strong `Arc<Inner>` while live), so wake the background
        // watcher — which holds only a `Weak<Inner>` — so it observes the
        // shutdown and terminates instead of leaking as a parked task.
        self.shutdown.notify(usize::MAX);
    }
}

/// The worker body of one admitted task.
async fn run_task<S, F, Fut, T>(
    mut guard: TaskGuard<S>,
    task: F,
    mut tx: Option<oneshot::Sender<Result<T>>>,
) where
    S: Spawner,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = T> + Send,
    T: Send + 'static,
{
    // `None` means the pool was cancelled; the check below disposes of the task.
    // Declared after `guard`, so the permit is back before the guard reports the
    // task finished.
    let _permit = guard.inner.limiter.acquire().await;

    guard.queued = false;
    guard.inner.metrics.dec_waiting();
    guard.inner.slot.notify(1usize.additional());

    // Cancelled tasks are disposed without running.
    if guard.inner.cancel.is_cancelled() {
        if let Some(tx) = tx.take() {
            let _ = tx.send(Err(guard.inner.cancel.cause().unwrap_or(Error::Canceled)));
        }
        return;
    }

    guard.running = true;
    guard.inner.metrics.inc_running();

    // Called inside this async block so the catch below also covers a panic in
    // the closure's synchronous part, before it returns its future.
    let run = async move { task().await };

    if guard.inner.panic_recovery {
        match AssertUnwindSafe(run).catch_unwind().await {
            Ok(value) => {
                guard.inner.metrics.inc_successful();
                if let Some(tx) = tx.take() {
                    let _ = tx.send(Ok(value));
                }
            }
            Err(payload) => {
                guard.inner.metrics.inc_failed();
                if let Some(tx) = tx.take() {
                    let _ = tx.send(Err(Error::Panicked(panic_message(payload))));
                }
            }
        }
    } else {
        // No recovery: a panic here propagates on the worker task and the
        // handle's sender drops, so an awaiter observes `Canceled`. The guard
        // still settles the counters during unwinding.
        let value = run.await;
        guard.inner.metrics.inc_successful();
        if let Some(tx) = tx.take() {
            let _ = tx.send(Ok(value));
        }
    }
}

/// Settles an admitted task's counters and signals idle on drop, however the task
/// ended: finished, disposed of, panicked, or dropped by a runtime shutting down
/// (even before its first poll). Without this, a leaked count would wedge
/// [`Pool::stop_and_wait`].
struct TaskGuard<S: Spawner> {
    inner: Arc<Inner<S>>,
    queued: bool,
    running: bool,
}

impl<S: Spawner> TaskGuard<S> {
    fn new(inner: Arc<Inner<S>>) -> Self {
        Self {
            inner,
            queued: true,
            running: false,
        }
    }
}

impl<S: Spawner> Drop for TaskGuard<S> {
    fn drop(&mut self) {
        if self.queued {
            self.inner.metrics.dec_waiting();
        }
        if self.running {
            self.inner.metrics.dec_running();
        } else {
            self.inner.metrics.inc_canceled();
        }
        // The permit this task held makes a worker free, which is queue room.
        // (Nobody ever waits for room in an unbounded queue.)
        if self.inner.capacity != usize::MAX {
            self.inner.slot.notify(1usize.additional());
        }
        self.inner.dispatch_done();
    }
}

/// A handle to a submitted task. Awaiting it yields the task's result.
///
/// Dropping the handle does **not** cancel the task; it simply discards the result.
pub struct TaskHandle<T> {
    state: HandleState<T>,
}

// Sound: nothing inside is structurally pinned — the oneshot receiver is `Unpin`
// and the resolved slot is plain data — so the handle can always be moved out of
// a `Pin` regardless of `T`.
impl<T> Unpin for TaskHandle<T> {}

enum HandleState<T> {
    /// Waiting on the worker's oneshot.
    Chan(oneshot::Receiver<Result<T>>),
    /// Already resolved at creation (e.g. a rejected submission). Stored
    /// inline, so a rejected `submit` allocates nothing.
    Done(Option<Result<T>>),
}

impl<T> TaskHandle<T> {
    pub(crate) fn new(rx: oneshot::Receiver<Result<T>>) -> Self {
        Self {
            state: HandleState::Chan(rx),
        }
    }

    pub(crate) fn resolved(result: Result<T>) -> Self {
        Self {
            state: HandleState::Done(Some(result)),
        }
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            HandleState::Done(slot) => Poll::Ready(slot.take().unwrap_or(Err(Error::Canceled))),
            HandleState::Chan(rx) => match Pin::new(rx).poll(cx) {
                Poll::Ready(Ok(result)) => Poll::Ready(result),
                // Sender dropped without sending: the task panicked without recovery
                // (or the pool was torn down) — treat as cancellation.
                Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Canceled)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// Future returned by [`Pool::stop`]; resolves when the pool is fully drained.
pub struct StopHandle {
    fut: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl StopHandle {
    pub(crate) fn new(fut: Pin<Box<dyn Future<Output = ()> + Send>>) -> Self {
        Self { fut }
    }
}

impl Future for StopHandle {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.fut.as_mut().poll(cx)
    }
}

impl<S: Spawner> TaskSink for Pool<S> {
    fn submit<F, Fut, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // Inherent `Pool::submit` (takes priority over this trait method).
        self.submit(task)
    }

    fn cancel_token(&self) -> CancelToken {
        self.cancel_token()
    }

    fn task_data(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.task_data()
    }
}

// Folded in from the former `tests/proptest_metrics.rs`: once the pool has
// drained, every submitted task must be accounted for in exactly one terminal
// bucket and nothing may be left running or queued. Uses the internal counters
// directly (they are no longer part of the public API).
#[cfg(all(test, feature = "runtime-tokio"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn counters_balance_after_drain() {
        let pool = Pool::builder().max_concurrency(4).build();
        let mut handles = Vec::new();
        for i in 0..24 {
            handles.push(pool.submit(move || async move {
                if i % 5 == 0 {
                    panic!("boom");
                }
                i
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        pool.stop_and_wait().await;

        let m = &pool.inner.metrics;
        assert_eq!(m.submitted(), 24);
        assert_eq!(m.successful() + m.failed(), 24, "every task is terminal");
        assert_eq!(m.waiting(), 0, "no tasks should remain queued");
        assert_eq!(m.running(), 0, "no workers should remain running");
        // Unbounded queue + no external cancel => nothing dropped or canceled.
        assert_eq!(m.dropped(), 0);
        assert_eq!(m.canceled(), 0);
    }
}
