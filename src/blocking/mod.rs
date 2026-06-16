//! A synchronous, thread-based worker pool.
//!
//! Unlike the async [`Pool`](crate::Pool), this pool runs **blocking** closures
//! on a fixed set of OS threads and needs no async runtime. It mirrors the same
//! concepts: bounded concurrency, task handles, metrics, graceful shutdown.
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender};

use crate::error::{panic_message, Error, Result};
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::options::QueueSize;

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Msg {
    Run(Job),
    Shutdown,
}

/// Builder for a blocking [`Pool`].
pub struct PoolBuilder {
    max_concurrency: usize,
    queue_size: QueueSize,
    non_blocking: bool,
    panic_recovery: bool,
}

impl Default for PoolBuilder {
    fn default() -> Self {
        Self {
            max_concurrency: 0,
            queue_size: QueueSize::Unbounded,
            non_blocking: false,
            panic_recovery: true,
        }
    }
}

impl PoolBuilder {
    /// Maximum number of worker threads. `0` uses the available parallelism.
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max;
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

    /// Builds the pool, spawning its worker threads.
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
    workers: Mutex<Vec<JoinHandle<()>>>,
    max_concurrency: usize,
    capacity: usize,
    non_blocking: bool,
    panic_recovery: bool,
    closed: AtomicBool,
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
        let workers_n = if b.max_concurrency == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        } else {
            b.max_concurrency
        };

        let (sender, receiver) = crossbeam_channel::unbounded::<Msg>();
        let metrics = Metrics::new();

        let mut workers = Vec::with_capacity(workers_n);
        for _ in 0..workers_n {
            let rx: Receiver<Msg> = receiver.clone();
            workers.push(std::thread::spawn(move || worker_loop(rx)));
        }

        Pool {
            inner: Arc::new(Inner {
                sender,
                workers: Mutex::new(workers),
                max_concurrency: workers_n,
                capacity: b.queue_size.capacity(),
                non_blocking: b.non_blocking,
                panic_recovery: b.panic_recovery,
                closed: AtomicBool::new(false),
                metrics,
            }),
        }
    }

    /// Submits a task. Returns a handle to await its result.
    ///
    /// If the pool is stopped, or it is non-blocking and the queue is full, the
    /// handle resolves with the corresponding [`Error`].
    pub fn submit<F, T>(&self, task: F) -> BlockingTaskHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.inner.metrics.inc_submitted();

        // ordering: `Relaxed` — gating read; the channel send synchronizes the job.
        if self.inner.closed.load(Ordering::Relaxed) {
            return BlockingTaskHandle::resolved(Err(Error::PoolStopped));
        }
        if self.inner.non_blocking && self.inner.metrics.waiting() as usize >= self.inner.capacity {
            self.inner.metrics.inc_dropped();
            return BlockingTaskHandle::resolved(Err(Error::QueueFull));
        }

        let (rx, job) = self.make_job(task);
        let _ = self.inner.sender.send(Msg::Run(job));
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

        // ordering: `Relaxed` — gating read; the channel send synchronizes the job.
        if self.inner.closed.load(Ordering::Relaxed) {
            return Err(Error::PoolStopped);
        }
        if self.inner.metrics.waiting() as usize >= self.inner.capacity {
            self.inner.metrics.inc_dropped();
            return Err(Error::QueueFull);
        }

        let (rx, job) = self.make_job(task);
        let _ = self.inner.sender.send(Msg::Run(job));
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

        let job: Job = Box::new(move || {
            metrics.dec_waiting();
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

    /// Stops the pool, waiting for all queued and running tasks to finish.
    pub fn stop_and_wait(&self) {
        // ordering: `AcqRel` — only the thread that wins this swap sends the
        // shutdown messages, so it must publish/observe prior pool state.
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            // Already stopping/stopped; just wait for the workers.
            self.join_workers();
            return;
        }
        let workers = self.inner.workers.lock().unwrap();
        for _ in 0..workers.len() {
            let _ = self.inner.sender.send(Msg::Shutdown);
        }
        drop(workers);
        self.join_workers();
    }

    fn join_workers(&self) {
        let mut workers = self.inner.workers.lock().unwrap();
        for handle in workers.drain(..) {
            let _ = handle.join();
        }
    }

    /// Returns `true` if the pool has been stopped.
    pub fn is_stopped(&self) -> bool {
        // ordering: `Relaxed` — observing the stop flag; no data depends on it.
        self.inner.closed.load(Ordering::Relaxed)
    }

    /// The number of worker threads.
    pub fn max_concurrency(&self) -> usize {
        self.inner.max_concurrency
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
}

fn worker_loop(rx: Receiver<Msg>) {
    while let Ok(msg) = rx.recv() {
        match msg {
            Msg::Run(job) => job(),
            Msg::Shutdown => break,
        }
    }
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
