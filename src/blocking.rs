//! A synchronous, thread-based worker pool.
//!
//! Unlike the async [`Pool`](crate::Pool), this pool runs **blocking** closures
//! on OS threads and needs no async runtime. It mirrors the same concepts:
//! bounded concurrency, task handles, cancellation, task groups and
//! graceful shutdown — plus a genuine **core/max thread model** (a minimum number
//! of always-alive threads and a maximum it grows to on demand, with idle extra
//! threads retiring after a keep-alive timeout).
//!
//! Dropping the last handle shuts the pool down in the background: queued tasks
//! still run and their handles still resolve, then the threads exit. Call
//! [`stop_and_wait`](Pool::stop_and_wait) to wait for that to finish.
//!
//! ```
//! use pond::blocking::Pool;
//!
//! let pool = Pool::builder().max_concurrency(4).build();
//! let handle = pool.submit(|| 1 + 2);
//! assert_eq!(handle.wait().unwrap(), 3);
//! pool.stop_and_wait();
//! ```

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::thread::{JoinHandle, ThreadId};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

use crate::cancel::CancelToken;
use crate::error::{panic_message, Error, Result};
use crate::metrics::Metrics;
use crate::options::QueueSize;

mod group;
pub use group::{BlockingResultGroup, BlockingTryGroup};

type Job = Box<dyn FnOnce() + Send + 'static>;

const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(60);

enum Msg {
    Run(Job),
    Shutdown,
}

/// Builder for a blocking [`Pool`].
pub struct PoolBuilder {
    max_concurrency: usize,
    min_concurrency: usize,
    queue_size: QueueSize,
    non_blocking: bool,
    panic_recovery: bool,
    keep_alive: Duration,
    cancel_token: CancelToken,
}

impl Default for PoolBuilder {
    fn default() -> Self {
        Self {
            max_concurrency: 0,
            min_concurrency: 0,
            queue_size: QueueSize::Unbounded,
            non_blocking: false,
            panic_recovery: true,
            keep_alive: DEFAULT_KEEP_ALIVE,
            cancel_token: CancelToken::new(),
        }
    }
}

impl PoolBuilder {
    /// Maximum number of worker threads. `0` uses the available parallelism.
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max;
        self
    }

    /// Minimum number of *core* worker threads kept alive even while idle.
    ///
    /// `0` (the default) makes every thread a core thread (a fixed-size pool of
    /// `max_concurrency`). A non-zero value enables a dynamic pool: `min` core
    /// threads always run and the pool grows to `max_concurrency` on demand.
    pub fn min_concurrency(mut self, min: usize) -> Self {
        self.min_concurrency = min;
        self
    }

    /// Queue capacity (default unbounded).
    pub fn queue_size(mut self, size: impl Into<QueueSize>) -> Self {
        self.queue_size = size.into();
        self
    }

    /// When non-blocking, a full queue rejects new tasks instead of blocking
    /// the caller.
    pub fn non_blocking(mut self, non_blocking: bool) -> Self {
        self.non_blocking = non_blocking;
        self
    }

    /// Enables or disables panic recovery (enabled by default).
    ///
    /// When disabled, a panic unwinds and ends its worker thread (the panic hook
    /// runs as usual, and the task's handle resolves with
    /// [`Error::Canceled`]). The pool starts a replacement thread, so it keeps its
    /// capacity.
    pub fn panic_recovery(mut self, enabled: bool) -> Self {
        self.panic_recovery = enabled;
        self
    }

    /// How long a non-core (dynamically spawned) idle thread waits for work
    /// before retiring. Only relevant when `min_concurrency < max_concurrency`.
    ///
    /// ```
    /// use std::time::Duration;
    /// use pond::blocking::Pool;
    ///
    /// // 2 always-alive threads, growing to 8 under load; extra threads retire
    /// // after 5s idle.
    /// let pool = Pool::builder()
    ///     .min_concurrency(2)
    ///     .max_concurrency(8)
    ///     .keep_alive(Duration::from_secs(5))
    ///     .build();
    /// assert_eq!(pool.min_concurrency(), 2);
    /// pool.stop_and_wait();
    /// ```
    pub fn keep_alive(mut self, dur: Duration) -> Self {
        self.keep_alive = dur;
        self
    }

    /// Supplies an external [`CancelToken`] for shared cancellation.
    pub fn cancel_token(mut self, token: CancelToken) -> Self {
        self.cancel_token = token;
        self
    }

    /// Builds the pool, spawning its core worker threads.
    pub fn build(self) -> Pool {
        Pool::new_inner(self)
    }
}

/// A synchronous worker pool backed by OS threads.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<Inner>,
    // Worker threads hold `inner` too, so its refcount can't tell when the user
    // has dropped the pool. Only `Pool` handles hold this.
    _owner: Arc<Owner>,
}

/// Shuts the workers down when the last [`Pool`] handle drops.
struct Owner {
    inner: Arc<Inner>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.inner.begin_shutdown();
    }
}

struct Inner {
    sender: Sender<Msg>,
    receiver: Receiver<Msg>,
    // Live worker threads keyed by `ThreadId`. A worker removes its own entry as
    // it exits (keep-alive retire *or* shutdown), so retired non-core threads no
    // longer accumulate dead `JoinHandle`s for the pool's lifetime.
    workers: Mutex<HashMap<ThreadId, JoinHandle<()>>>,
    core_size: usize,
    max_size: usize,
    // Slot semaphore for a *bounded* pool: pre-filled with one token per worker
    // plus one per queue place. A submitter takes a token (blocking, or `try` when
    // non-blocking) before enqueuing; it goes back when the job ends. So at most
    // `capacity` jobs ever wait behind busy workers — true backpressure — while
    // keeping the `Msg` channel unbounded so shutdown signalling can never block.
    // `None` when unbounded.
    queue_slots: Option<(Sender<()>, Receiver<()>)>,
    non_blocking: bool,
    panic_recovery: bool,
    keep_alive: Duration,
    live: AtomicUsize, // threads currently alive
    idle: AtomicUsize, // threads currently waiting for work
    // Submitters hold this for read across their stopped-check and enqueue, and
    // shutdown takes it for write, so no job can land behind the `Shutdown`
    // messages where no worker would ever see it.
    admission: RwLock<()>,
    closed: AtomicBool,
    cancel: CancelToken,
    metrics: Metrics,
}

impl Pool {
    /// Creates a pool with the given number of worker threads.
    pub fn new(max_concurrency: usize) -> Self {
        PoolBuilder::default()
            .max_concurrency(max_concurrency)
            .build()
    }

    /// Starts building a pool.
    pub fn builder() -> PoolBuilder {
        PoolBuilder::default()
    }

    fn new_inner(b: PoolBuilder) -> Self {
        let max_size = if b.max_concurrency == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        } else {
            b.max_concurrency
        };
        // `0` min means a fixed pool: every thread is core.
        let core_size = if b.min_concurrency == 0 {
            max_size
        } else {
            b.min_concurrency.min(max_size)
        };

        let (sender, receiver) = crossbeam_channel::unbounded::<Msg>();

        let capacity = b.queue_size.capacity();
        let queue_slots = if capacity == usize::MAX {
            None
        } else {
            let tokens = capacity.saturating_add(max_size);
            let (tx, rx) = crossbeam_channel::bounded::<()>(tokens);
            for _ in 0..tokens {
                let _ = tx.try_send(());
            }
            Some((tx, rx))
        };

        let inner = Arc::new(Inner {
            sender,
            receiver,
            workers: Mutex::new(HashMap::with_capacity(max_size)),
            core_size,
            max_size,
            queue_slots,
            non_blocking: b.non_blocking,
            panic_recovery: b.panic_recovery,
            keep_alive: b.keep_alive,
            live: AtomicUsize::new(0),
            idle: AtomicUsize::new(0),
            admission: RwLock::new(()),
            closed: AtomicBool::new(false),
            cancel: b.cancel_token,
            metrics: Metrics::new(),
        });

        // Spawn the core threads up front.
        for _ in 0..core_size {
            inner
                .spawn_worker(true)
                .expect("failed to spawn a worker thread");
        }

        Pool {
            _owner: Arc::new(Owner {
                inner: inner.clone(),
            }),
            inner,
        }
    }

    /// Grows the pool by one non-core thread if more jobs are queued than there
    /// are idle threads to take them.
    ///
    /// Checked after enqueuing and against the queue depth, because a thread just
    /// handed a job still counts as idle until it wakes: "is anyone idle?" alone
    /// would leave a second job stuck behind it, with thread slots to spare.
    fn maybe_grow(&self) {
        let waiting = self.inner.metrics.waiting();
        if waiting > self.inner.idle.load(Ordering::Relaxed) as u64 && !self.is_stopped() {
            // Under load, staying at the current size is fine if this fails.
            let _ = self.inner.spawn_worker(false);
        }
    }

    /// Acquires a slot for a bounded pool. When `block` is set, waits for a free
    /// slot (true backpressure); otherwise returns `Err` immediately if there is
    /// none. Always succeeds for an unbounded pool.
    fn acquire_slot(&self, block: bool) -> std::result::Result<Option<SlotToken>, ()> {
        match &self.inner.queue_slots {
            Some((tx, rx)) => {
                let got = if block {
                    rx.recv().is_ok()
                } else {
                    rx.try_recv().is_ok()
                };
                if got {
                    Ok(Some(SlotToken(tx.clone())))
                } else {
                    Err(())
                }
            }
            None => Ok(None),
        }
    }

    /// Queues a task holding `slot`, unless the pool stopped in the meantime
    /// (possibly while the submitter was blocked waiting for that slot).
    fn enqueue<F, T>(&self, task: F, slot: Option<SlotToken>) -> Result<BlockingTaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let rx = {
            let _admission = self
                .inner
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if self.is_stopped() {
                return Err(self.stop_error());
            }
            let (rx, job) = self.make_job(task, slot);
            self.inner.metrics.inc_waiting();
            let _ = self.inner.sender.send(Msg::Run(job));
            rx
        };
        self.maybe_grow();
        Ok(BlockingTaskHandle::chan(rx))
    }

    /// Submits a task. Returns a handle to await its result.
    ///
    /// For a **bounded** pool (a `queue_size`), the default (blocking) submit
    /// applies backpressure: it waits for a free queue slot when the queue is
    /// full, rather than growing it without limit. A `non_blocking` pool instead
    /// drops the task with [`Error::QueueFull`]. An unbounded pool never blocks.
    pub fn submit<F, T>(&self, task: F) -> BlockingTaskHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();

        if self.is_stopped() {
            return BlockingTaskHandle::resolved(Err(self.stop_error()));
        }
        let Ok(slot) = self.acquire_slot(!self.inner.non_blocking) else {
            self.inner.metrics.inc_dropped();
            return BlockingTaskHandle::resolved(Err(Error::QueueFull));
        };
        self.enqueue(task, slot)
            .unwrap_or_else(|e| BlockingTaskHandle::resolved(Err(e)))
    }

    /// Submits a task, returning an error immediately if the pool is stopped or
    /// its bounded queue is full (never blocks, regardless of `non_blocking`).
    pub fn try_submit<F, T>(&self, task: F) -> Result<BlockingTaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();

        if self.is_stopped() {
            return Err(self.stop_error());
        }
        let Ok(slot) = self.acquire_slot(false) else {
            self.inner.metrics.inc_dropped();
            return Err(Error::QueueFull);
        };
        self.enqueue(task, slot)
    }

    fn make_job<F, T>(&self, task: F, slot: Option<SlotToken>) -> (Receiver<Result<T>>, Job)
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = crossbeam_channel::bounded::<Result<T>>(1);
        let metrics = self.inner.metrics.clone();
        let recovery = self.inner.panic_recovery;
        let cancel = self.inner.cancel.clone();

        let job: Job = Box::new(move || {
            metrics.dec_waiting();
            // A task cancelled before it runs is disposed without executing.
            if cancel.is_cancelled() {
                metrics.inc_canceled();
                drop(slot);
                let _ = tx.send(Err(cancel.cause().unwrap_or(Error::Canceled)));
                return;
            }
            metrics.inc_running();
            // Guards, so a panic that isn't recovered still settles these.
            let running = RunningGuard(&metrics);
            let result = if recovery {
                match catch_unwind(AssertUnwindSafe(task)) {
                    Ok(value) => {
                        metrics.inc_successful();
                        Ok(value)
                    }
                    Err(payload) => {
                        metrics.inc_failed();
                        Err(Error::Panicked(panic_message(payload)))
                    }
                }
            } else {
                let value = task();
                metrics.inc_successful();
                Ok(value)
            };
            drop(running);
            // Free the slot before resolving, so whoever is waiting on this
            // handle can't then see a stale `QueueFull`.
            drop(slot);
            let _ = tx.send(result);
        });

        (rx, job)
    }

    /// Cancels the pool: queued tasks are abandoned (resolving with
    /// [`Error::Canceled`]) as workers reach them; running tasks finish.
    pub fn cancel(&self) {
        self.inner.cancel.cancel();
    }

    /// Returns the [`CancelToken`] driving this pool's cancellation.
    pub fn cancel_token(&self) -> CancelToken {
        self.inner.cancel.clone()
    }

    /// Creates a [`BlockingResultGroup`] whose tasks return `T`.
    pub fn group<T: Send + 'static>(&self) -> BlockingResultGroup<T> {
        BlockingResultGroup::new(self.clone())
    }

    /// Creates a [`BlockingTryGroup`] of fallible tasks returning `Result<T, E>`.
    pub fn try_group<T: Send + 'static, E: Send + 'static>(&self) -> BlockingTryGroup<T, E> {
        BlockingTryGroup::new(self.clone())
    }

    /// Stops the pool, waiting for all queued and running tasks to finish.
    pub fn stop_and_wait(&self) {
        self.inner.begin_shutdown();
        self.join_workers();
    }

    fn join_workers(&self) {
        // Loop: a worker that panics while we join it starts a replacement to
        // finish the queue, so new handles can appear until the map stays empty.
        loop {
            // Drain the handles and release the lock *before* joining: a worker
            // removes its own entry as it exits, which needs this same lock, so
            // holding it across `join` would deadlock.
            let handles: Vec<JoinHandle<()>> = {
                let mut workers = self.inner.workers.lock().unwrap();
                workers.drain().map(|(_, h)| h).collect()
            };
            if handles.is_empty() {
                return;
            }
            for handle in handles {
                let _ = handle.join();
            }
        }
    }

    /// Returns `true` if the pool has been stopped or cancelled.
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

    /// The maximum number of worker threads.
    pub fn max_concurrency(&self) -> usize {
        self.inner.max_size
    }

    /// The number of core (always-alive) worker threads.
    pub fn min_concurrency(&self) -> usize {
        self.inner.core_size
    }

    /// The number of worker threads currently alive.
    pub fn live_workers(&self) -> usize {
        self.inner.live.load(Ordering::Relaxed)
    }
}

impl Inner {
    /// Stops accepting work and queues a shutdown signal for every possible live
    /// thread, behind any jobs already queued. Idempotent; doesn't wait.
    fn begin_shutdown(&self) {
        let _admission = self
            .admission
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        for _ in 0..self.max_size {
            let _ = self.sender.send(Msg::Shutdown);
        }
    }

    /// Starts a worker unless the pool is already at `max_size`. Returns `Err`
    /// only if the OS refuses to create the thread.
    fn spawn_worker(self: &Arc<Self>, core: bool) -> std::io::Result<()> {
        // Reserve the place atomically: concurrent submitters must not all see
        // room and overshoot `max_size`.
        let mut live = self.live.load(Ordering::Acquire);
        loop {
            if live >= self.max_size {
                return Ok(());
            }
            match self.live.compare_exchange_weak(
                live,
                live + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => live = actual,
            }
        }
        let inner = self.clone();
        // Register the handle under the `workers` lock *before* releasing it, so a
        // worker that exits immediately can't race ahead of its own registration
        // (its self-removal also takes this lock, so it waits for the insert).
        let mut workers = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
        match std::thread::Builder::new().spawn(move || worker_loop(inner, core)) {
            Ok(handle) => {
                workers.insert(handle.thread().id(), handle);
                Ok(())
            }
            Err(e) => {
                self.live.fetch_sub(1, Ordering::AcqRel);
                Err(e)
            }
        }
    }
}

/// Returns a bounded pool's slot when its job ends, however it ends: finished,
/// panicked, or dropped without ever running.
struct SlotToken(Sender<()>);

impl Drop for SlotToken {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

struct RunningGuard<'a>(&'a Metrics);

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.0.dec_running();
    }
}

/// Runs a worker's exit bookkeeping on every way out of [`worker_loop`]. A worker
/// unwinding from a task panic also starts its own replacement; otherwise the
/// pool would permanently lose capacity, and with its last worker gone queued
/// tasks would never run.
struct WorkerExit {
    inner: Arc<Inner>,
    core: bool,
}

impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.inner.live.fetch_sub(1, Ordering::AcqRel);
        // Before removing our own entry, so `join_workers` can't see an empty map
        // while a replacement is still to come. Unconditional even when stopping:
        // the replacement has to finish the queue and take a `Shutdown` itself.
        if std::thread::panicking() {
            let _ = self.inner.spawn_worker(self.core);
        }
        // Reap our own handle so retired threads don't accumulate. A no-op if
        // `join_workers` already drained the map during shutdown.
        self.inner
            .workers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&std::thread::current().id());
    }
}

fn worker_loop(inner: Arc<Inner>, core: bool) {
    let _exit = WorkerExit {
        inner: inner.clone(),
        core,
    };
    loop {
        inner.idle.fetch_add(1, Ordering::Relaxed);
        let msg = if core {
            inner
                .receiver
                .recv()
                .map_err(|_| RecvTimeoutError::Disconnected)
        } else {
            inner.receiver.recv_timeout(inner.keep_alive)
        };
        inner.idle.fetch_sub(1, Ordering::Relaxed);

        match msg {
            Ok(Msg::Run(job)) => job(),
            // A job arrived just as the keep-alive ran out; don't retire past it.
            Err(RecvTimeoutError::Timeout) if inner.metrics.waiting() > 0 => {}
            // A non-core thread idle past keep-alive retires, same as any other
            // exit: drop out of the loop and let `WorkerExit` clean up.
            Ok(Msg::Shutdown)
            | Err(RecvTimeoutError::Timeout)
            | Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

/// A handle to a task submitted to a blocking [`Pool`].
pub struct BlockingTaskHandle<T> {
    state: HandleState<T>,
}

enum HandleState<T> {
    /// Waiting on the worker's result channel.
    Chan(Receiver<Result<T>>),
    /// Already resolved at creation (e.g. a rejected submission). Stored
    /// inline, so a rejected `submit` allocates nothing.
    Done(Option<Result<T>>),
}

impl<T> BlockingTaskHandle<T> {
    fn chan(rx: Receiver<Result<T>>) -> Self {
        Self {
            state: HandleState::Chan(rx),
        }
    }

    fn resolved(result: Result<T>) -> Self {
        Self {
            state: HandleState::Done(Some(result)),
        }
    }

    /// Blocks until the task completes and returns its result.
    pub fn wait(self) -> Result<T> {
        match self.state {
            HandleState::Done(slot) => slot.unwrap_or(Err(Error::Canceled)),
            HandleState::Chan(rx) => rx.recv().unwrap_or(Err(Error::Canceled)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_and_wait_returns_value() {
        let pool = Pool::builder().max_concurrency(2).build();
        assert_eq!(pool.submit(|| 1 + 2).wait().unwrap(), 3);
        // The pool survives and keeps serving tasks.
        assert_eq!(pool.submit(|| 7).wait().unwrap(), 7);
        pool.stop_and_wait();
    }

    #[test]
    fn builder_reflects_core_and_max() {
        let pool = Pool::builder()
            .min_concurrency(2)
            .max_concurrency(8)
            .build();
        assert_eq!(pool.min_concurrency(), 2);
        assert_eq!(pool.max_concurrency(), 8);
        // Core threads are spawned eagerly.
        assert_eq!(pool.live_workers(), 2);
        pool.stop_and_wait();
    }

    #[test]
    fn panicking_task_is_recovered_as_error() {
        let pool = Pool::builder().max_concurrency(1).build();
        let err = pool.submit(|| panic!("boom")).wait().unwrap_err();
        assert!(matches!(err, Error::Panicked(_)));
        // The pool survives the panic and keeps serving tasks.
        assert_eq!(pool.submit(|| 7).wait().unwrap(), 7);
        pool.stop_and_wait();
    }

    #[test]
    fn stop_and_wait_is_idempotent() {
        let pool = Pool::builder().max_concurrency(2).build();
        pool.submit(|| ()).wait().unwrap();
        pool.stop_and_wait();
        pool.stop_and_wait(); // second call must not hang or panic
        assert!(pool.is_stopped());
    }

    #[test]
    fn retired_threads_are_reaped() {
        let pool = Pool::builder()
            .min_concurrency(1)
            .max_concurrency(4)
            .keep_alive(Duration::from_millis(50))
            .build();

        // Drive several bursts; each grows the pool with non-core threads that
        // must retire and be reaped (not leak a JoinHandle) afterwards.
        for _ in 0..3 {
            let handles: Vec<_> = (0..4)
                .map(|_| pool.submit(|| std::thread::sleep(Duration::from_millis(40))))
                .collect();
            for h in handles {
                h.wait().unwrap();
            }
            // Wait past keep_alive so the extra (non-core) threads retire.
            std::thread::sleep(Duration::from_millis(250));

            let tracked = pool.inner.workers.lock().unwrap().len();
            // The tracked-handle count must collapse back toward the core size and
            // never exceed the live thread count — proving handles are reaped.
            assert!(
                tracked <= pool.live_workers() + 1,
                "leaked JoinHandles: tracked={tracked}, live={}",
                pool.live_workers()
            );
            assert!(tracked <= 4, "tracked handles grew past max: {tracked}");
        }

        pool.stop_and_wait();
        assert_eq!(pool.inner.workers.lock().unwrap().len(), 0);
    }

    #[test]
    fn bounded_blocking_submit_applies_backpressure() {
        use std::sync::mpsc;

        let pool = Pool::builder().max_concurrency(1).queue_size(1).build();
        let (release_tx, release_rx) = crossbeam_channel::bounded::<()>(1);
        let started = Arc::new(AtomicBool::new(false));
        let s = started.clone();
        // Occupy the only worker until released.
        let _gate = pool.submit(move || {
            s.store(true, Ordering::SeqCst);
            let _ = release_rx.recv();
        });
        while !started.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        // Fill the single queue slot.
        let _queued = pool.submit(|| {});

        // The next submit must block: worker busy, queue full.
        let pool2 = pool.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let h = std::thread::spawn(move || {
            let _ = pool2.submit(|| {});
            let _ = done_tx.send(());
        });
        assert!(
            done_rx.recv_timeout(Duration::from_millis(150)).is_err(),
            "submit should block on a full bounded queue"
        );

        // Releasing the worker clears backpressure and the blocked submit proceeds.
        let _ = release_tx.send(());
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "blocked submit should proceed once a slot frees"
        );
        h.join().unwrap();
        pool.stop_and_wait();
    }

    #[test]
    fn bounded_non_blocking_drops_when_full() {
        let pool = Pool::builder()
            .max_concurrency(1)
            .queue_size(1)
            .non_blocking(true)
            .build();
        let (release_tx, release_rx) = crossbeam_channel::bounded::<()>(1);
        let started = Arc::new(AtomicBool::new(false));
        let s = started.clone();
        let _gate = pool.submit(move || {
            s.store(true, Ordering::SeqCst);
            let _ = release_rx.recv();
        });
        while !started.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        // One fits in the queue; the next is dropped rather than blocking.
        pool.try_submit(|| {}).expect("first queued task fits");
        assert!(matches!(pool.try_submit(|| {}), Err(Error::QueueFull)));
        assert!(matches!(pool.submit(|| {}).wait(), Err(Error::QueueFull)));
        let _ = release_tx.send(());
        pool.stop_and_wait();
    }

    #[test]
    fn submit_after_cancel_is_rejected() {
        let pool = Pool::builder().max_concurrency(2).build();
        pool.cancel();
        assert!(pool.is_stopped());
        // try_submit reports the rejection explicitly.
        assert!(pool.try_submit(|| 1).is_err());
        // submit resolves to an error rather than running the task.
        assert!(pool.submit(|| 1).wait().is_err());
    }
}
