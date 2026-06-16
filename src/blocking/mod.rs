//! A synchronous, thread-based worker pool.
//!
//! Unlike the async [`Pool`](crate::Pool), this pool runs **blocking** closures
//! on OS threads and needs no async runtime. It mirrors the same concepts:
//! bounded concurrency, task handles, metrics, cancellation, task groups and
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

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

use crate::cancel::CancelToken;
use crate::error::{panic_message, Error, Result};
use crate::metrics::{Metrics, MetricsSnapshot};
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
    workers: Mutex<Vec<JoinHandle<()>>>,
    core_size: usize,
    max_size: usize,
    capacity: usize,
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

        let pool = Pool {
            inner: Arc::new(Inner {
                sender,
                receiver,
                workers: Mutex::new(Vec::with_capacity(max_size)),
                core_size,
                max_size,
                capacity: b.queue_size.capacity(),
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
        let handle = std::thread::spawn(move || worker_loop(inner, core));
        self.inner.workers.lock().unwrap().push(handle);
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

    /// Submits a task. Returns a handle to await its result.
    pub fn submit<F, T>(&self, task: F) -> BlockingTaskHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();

        if self.is_stopped() {
            return BlockingTaskHandle::resolved(Err(self.stop_error()));
        }
        if self.inner.non_blocking && self.inner.metrics.waiting() as usize >= self.inner.capacity {
            self.inner.metrics.inc_dropped();
            return BlockingTaskHandle::resolved(Err(Error::QueueFull));
        }

        let (rx, job) = self.make_job(task);
        let _ = self.inner.sender.send(Msg::Run(job));
        self.maybe_grow();
        BlockingTaskHandle { rx }
    }

    /// Submits a task, returning an error immediately if the pool is stopped or
    /// its bounded queue is full.
    pub fn try_submit<F, T>(&self, task: F) -> Result<BlockingTaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
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

        let (rx, job) = self.make_job(task);
        let _ = self.inner.sender.send(Msg::Run(job));
        self.maybe_grow();
        Ok(BlockingTaskHandle { rx })
    }

    fn make_job<F, T>(&self, task: F) -> (Receiver<Result<T>>, Job)
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = crossbeam_channel::bounded::<Result<T>>(1);
        let metrics = self.inner.metrics.clone();
        metrics.inc_waiting();
        let recovery = self.inner.panic_recovery;
        let cancel = self.inner.cancel.clone();

        let job: Job = Box::new(move || {
            metrics.dec_waiting();
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
        let mut workers = self.inner.workers.lock().unwrap();
        for handle in workers.drain(..) {
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

    /// A snapshot of all metrics.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    /// Number of tasks currently running.
    pub fn running_workers(&self) -> u64 {
        self.inner.metrics.running()
    }

    /// Total number of tasks ever submitted.
    pub fn submitted_tasks(&self) -> u64 {
        self.inner.metrics.submitted()
    }

    /// Number of tasks currently queued.
    pub fn waiting_tasks(&self) -> u64 {
        self.inner.metrics.waiting()
    }

    /// Number of tasks canceled before execution.
    pub fn canceled_tasks(&self) -> u64 {
        self.inner.metrics.canceled()
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
            Ok(Msg::Shutdown) => break,
            Err(RecvTimeoutError::Timeout) => {
                // A non-core thread that has been idle too long retires.
                inner.live.fetch_sub(1, Ordering::Relaxed);
                return;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    inner.live.fetch_sub(1, Ordering::Relaxed);
}

/// A handle to a task submitted to a blocking [`Pool`].
pub struct BlockingTaskHandle<T> {
    rx: Receiver<Result<T>>,
}

impl<T> BlockingTaskHandle<T> {
    fn resolved(result: Result<T>) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let _ = tx.send(result);
        Self { rx }
    }

    /// Blocks until the task completes and returns its result.
    pub fn wait(self) -> Result<T> {
        self.rx.recv().unwrap_or(Err(Error::Canceled))
    }
}
