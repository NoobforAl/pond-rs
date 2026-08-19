//! A synchronous, thread-based worker pool.
//!
//! Unlike the async [`Pool`](crate::Pool), this pool runs **blocking** closures
//! on OS threads and needs no async runtime. It mirrors the same concepts:
//! bounded concurrency, task handles, cancellation, task groups and
//! graceful shutdown — plus a genuine **core/max thread model** (a minimum number
//! of always-alive threads and a maximum it grows to on demand, with idle extra
//! threads retiring after a keep-alive timeout).
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
use std::sync::{Arc, Mutex};
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
    capacity: usize,
    // Queue-slot semaphore for a *bounded* pool: pre-filled with `capacity`
    // tokens. A submitter takes a token (blocking, or `try` when non-blocking)
    // before enqueuing; a worker returns it when it dequeues the job. This bounds
    // the queue depth — true backpressure — while keeping the `Msg` channel
    // unbounded so shutdown signalling can never block. `None` when unbounded.
    queue_slots: Option<(Sender<()>, Receiver<()>)>,
    non_blocking: bool,
    panic_recovery: bool,
    keep_alive: Duration,
    live: AtomicUsize, // threads currently alive
    idle: AtomicUsize, // threads currently waiting for work
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
        // Build the queue-slot semaphore only for a genuinely bounded queue
        // (1..usize::MAX). A capacity of 0 is handled inline (rendezvous / always
        // reject) and unbounded needs no slots.
        let queue_slots = if capacity == 0 || capacity == usize::MAX {
            None
        } else {
            let (tx, rx) = crossbeam_channel::bounded::<()>(capacity);
            for _ in 0..capacity {
                let _ = tx.try_send(());
            }
            Some((tx, rx))
        };

        let pool = Pool {
            inner: Arc::new(Inner {
                sender,
                receiver,
                workers: Mutex::new(HashMap::with_capacity(max_size)),
                core_size,
                max_size,
                capacity,
                queue_slots,
                non_blocking: b.non_blocking,
                panic_recovery: b.panic_recovery,
                keep_alive: b.keep_alive,
                live: AtomicUsize::new(0),
                idle: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                cancel: b.cancel_token,
                metrics: Metrics::new(),
            }),
        };

        // Spawn the core threads up front.
        for _ in 0..core_size {
            pool.spawn_worker(true);
        }
        pool
    }

    fn spawn_worker(&self, core: bool) {
        self.inner.live.fetch_add(1, Ordering::Relaxed);
        let inner = self.inner.clone();
        // Register the handle under the `workers` lock *before* releasing it, so a
        // worker that exits immediately can't race ahead of its own registration
        // (its self-removal also takes this lock, so it waits for the insert).
        let mut workers = self.inner.workers.lock().unwrap();
        let handle = std::thread::spawn(move || worker_loop(inner, core));
        workers.insert(handle.thread().id(), handle);
    }

    /// Grows the pool by one non-core thread if all threads are busy and we are
    /// below `max_size`.
    fn maybe_grow(&self) {
        if self.inner.idle.load(Ordering::Relaxed) == 0
            && self.inner.live.load(Ordering::Relaxed) < self.inner.max_size
            && !self.is_stopped()
        {
            self.spawn_worker(false);
        }
    }

    /// Acquires a queue slot for a bounded pool. When `block` is set, waits for a
    /// free slot (true backpressure); otherwise returns `false` immediately if the
    /// queue is full. Always succeeds for an unbounded pool.
    fn acquire_slot(&self, block: bool) -> bool {
        match &self.inner.queue_slots {
            Some((_, rx)) => {
                if block {
                    rx.recv().is_ok()
                } else {
                    rx.try_recv().is_ok()
                }
            }
            // No semaphore: unbounded admits always; a zero-capacity queue admits
            // only in blocking mode (a rendezvous-style direct handoff).
            None => self.inner.capacity != 0 || block,
        }
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
        // Grow first so a fresh worker can relieve a full bounded queue before a
        // blocking submit parks on `acquire_slot`.
        self.maybe_grow();
        if !self.acquire_slot(!self.inner.non_blocking) {
            self.inner.metrics.inc_dropped();
            return BlockingTaskHandle::resolved(Err(Error::QueueFull));
        }

        let (rx, job) = self.make_job(task);
        self.inner.metrics.inc_waiting();
        let _ = self.inner.sender.send(Msg::Run(job));
        BlockingTaskHandle::chan(rx)
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
        self.maybe_grow();
        if !self.acquire_slot(false) {
            self.inner.metrics.inc_dropped();
            return Err(Error::QueueFull);
        }

        let (rx, job) = self.make_job(task);
        self.inner.metrics.inc_waiting();
        let _ = self.inner.sender.send(Msg::Run(job));
        Ok(BlockingTaskHandle::chan(rx))
    }

    fn make_job<F, T>(&self, task: F) -> (Receiver<Result<T>>, Job)
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = crossbeam_channel::bounded::<Result<T>>(1);
        let metrics = self.inner.metrics.clone();
        let recovery = self.inner.panic_recovery;
        let cancel = self.inner.cancel.clone();
        // Returned to the queue-slot semaphore once the job is dequeued, so a
        // blocked submitter can proceed (bounded pools only).
        let slot_tx = self.inner.queue_slots.as_ref().map(|(tx, _)| tx.clone());

        let job: Job = Box::new(move || {
            metrics.dec_waiting();
            if let Some(slot_tx) = &slot_tx {
                let _ = slot_tx.try_send(());
            }
            // A task cancelled before it runs is disposed without executing.
            if cancel.is_cancelled() {
                metrics.inc_canceled();
                let _ = tx.send(Err(cancel.cause().unwrap_or(Error::Canceled)));
                return;
            }
            metrics.inc_running();
            if recovery {
                match catch_unwind(AssertUnwindSafe(task)) {
                    Ok(value) => {
                        metrics.inc_successful();
                        let _ = tx.send(Ok(value));
                    }
                    Err(payload) => {
                        metrics.inc_failed();
                        let _ = tx.send(Err(Error::Panicked(panic_message(payload))));
                    }
                }
            } else {
                let value = task();
                metrics.inc_successful();
                let _ = tx.send(Ok(value));
            }
            metrics.dec_running();
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
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            self.join_workers();
            return;
        }
        // Send enough shutdown signals to retire every possible live thread.
        for _ in 0..self.inner.max_size {
            let _ = self.inner.sender.send(Msg::Shutdown);
        }
        self.join_workers();
    }

    fn join_workers(&self) {
        // Drain the handles and release the lock *before* joining: a worker
        // removes its own entry as it exits, which needs this same lock, so
        // holding it across `join` would deadlock.
        let handles: Vec<JoinHandle<()>> = {
            let mut workers = self.inner.workers.lock().unwrap();
            workers.drain().map(|(_, h)| h).collect()
        };
        for handle in handles {
            let _ = handle.join();
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

fn worker_loop(inner: Arc<Inner>, core: bool) {
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
            // A non-core thread idle past keep-alive retires, same as any other
            // exit: drop out of the loop and reap self below.
            Ok(Msg::Shutdown)
            | Err(RecvTimeoutError::Timeout)
            | Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    inner.live.fetch_sub(1, Ordering::Relaxed);
    // Reap our own handle so retired threads don't accumulate. A no-op if
    // `join_workers` already drained the map during shutdown.
    inner
        .workers
        .lock()
        .unwrap()
        .remove(&std::thread::current().id());
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
