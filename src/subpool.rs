//! Subpools: a child pool that runs on a parent pool but caps its own concurrency.
//!
//! Mirrors pond's subpools. A subpool shares the parent's runtime and worker
//! budget, while additionally limiting how many of *its* tasks run at once.
//! Stopping a subpool does not affect the parent.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use event_listener::Event;

use crate::cancel::CancelToken;
use crate::error::Error;
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::pool::{Pool, TaskHandle};
use crate::runtime::Spawner;

/// A concurrency-limited child of a [`Pool`].
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
    limited: bool,
    max_concurrency: usize,
    metrics: Metrics,
    closed: AtomicBool,
    cancel: CancelToken,
    idle: Event,
}

impl<S: Spawner> Subpool<S> {
    pub(crate) fn new(parent: Pool<S>, max_concurrency: usize) -> Self {
        let parent_max = parent.max_concurrency();
        // Validate against the parent's bound, matching pond.
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

        Subpool {
            inner: Arc::new(SubInner {
                parent,
                permit_tx,
                permit_rx,
                limited,
                max_concurrency: effective,
                metrics: Metrics::new(),
                closed: AtomicBool::new(false),
                cancel: CancelToken::new(),
                idle: Event::new(),
            }),
        }
    }

    /// Submits a task to the subpool.
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

        self.inner.metrics.inc_waiting();
        // A single `Arc` clone moved into the task closure (saves one inc/dec).
        let run = self.inner.clone();

        self.inner.parent.submit(move || async move {
            // Acquire a subpool permit (limits subpool concurrency).
            let _permit = if run.limited {
                match run.permit_rx.recv().await {
                    Ok(()) => Some(SubPermit { inner: run.clone() }),
                    Err(_) => None,
                }
            } else {
                None
            };
            run.metrics.dec_waiting();
            run.metrics.inc_running();
            // The guard accounts for completion (success/failure) and idleness
            // even if the task panics.
            let _guard = RunGuard { inner: run.clone() };
            task().await
        })
    }

    /// Returns `true` if the subpool has been stopped.
    pub fn is_stopped(&self) -> bool {
        // ordering: `Relaxed` stop flag; `is_cancelled` does its own `Acquire`.
        self.inner.closed.load(Ordering::Relaxed) || self.inner.cancel.is_cancelled()
    }

    fn stop_error(&self) -> Error {
        if self.inner.cancel.is_cancelled() {
            self.inner.cancel.cause().unwrap_or(Error::Canceled)
        } else {
            Error::PoolStopped
        }
    }

    /// Stops the subpool and waits for its in-flight and queued tasks to finish.
    /// The parent pool is unaffected.
    pub async fn stop_and_wait(&self) {
        // ordering: `Relaxed` — draining waits on the `idle` event for sync.
        self.inner.closed.store(true, Ordering::Relaxed);
        loop {
            if self.inner.is_idle() {
                return;
            }
            let listener = self.inner.idle.listen();
            if self.inner.is_idle() {
                return;
            }
            listener.await;
        }
    }

    /// The subpool's effective maximum concurrency (`0` == unbounded).
    pub fn max_concurrency(&self) -> usize {
        self.inner.max_concurrency
    }

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
}

impl<S: Spawner> SubInner<S> {
    fn is_idle(&self) -> bool {
        self.metrics.running() == 0 && self.metrics.waiting() == 0
    }

    fn maybe_notify_idle(&self) {
        if self.is_idle() {
            self.idle.notify(usize::MAX);
        }
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

/// Accounts for a subpool task's completion, even across a panic.
struct RunGuard<S: Spawner> {
    inner: Arc<SubInner<S>>,
}

impl<S: Spawner> Drop for RunGuard<S> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.inner.metrics.inc_failed();
        } else {
            self.inner.metrics.inc_successful();
        }
        self.inner.metrics.dec_running();
        self.inner.maybe_notify_idle();
    }
}
