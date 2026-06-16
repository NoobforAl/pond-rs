//! Pool metrics.
//!
//! Mirrors pond's metric surface: running workers, submitted, waiting,
//! successful, failed, completed, dropped and canceled task counts.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Shared, atomically-updated counters backing a pool's metrics.
///
/// Cloning shares the same underlying counters (it is `Arc`-backed), so a clone
/// observes the same live values as the original.
#[derive(Clone, Default)]
pub(crate) struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Default)]
struct MetricsInner {
    running_workers: AtomicU64,
    submitted: AtomicU64,
    waiting: AtomicU64,
    successful: AtomicU64,
    failed: AtomicU64,
    dropped: AtomicU64,
    canceled: AtomicU64,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    // Counters are independent tallies, so `Relaxed` is the correct (cheapest)
    // ordering throughout — no other memory is published *through* them.

    #[inline]
    pub(crate) fn inc_running(&self) {
        self.inner.running_workers.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn dec_running(&self) {
        self.inner.running_workers.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn inc_submitted(&self) {
        self.inner.submitted.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn inc_waiting(&self) {
        self.inner.waiting.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn dec_waiting(&self) {
        self.inner.waiting.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn inc_successful(&self) {
        self.inner.successful.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn inc_failed(&self) {
        self.inner.failed.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn inc_dropped(&self) {
        self.inner.dropped.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn inc_canceled(&self) {
        self.inner.canceled.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn running(&self) -> u64 {
        self.inner.running_workers.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn submitted(&self) -> u64 {
        self.inner.submitted.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn waiting(&self) -> u64 {
        self.inner.waiting.load(Ordering::Relaxed)
    }

    // Single-counter accessors: load just the one atomic instead of taking a
    // full `snapshot()` (which loads all of them) when only one is needed.

    #[inline]
    pub(crate) fn successful(&self) -> u64 {
        self.inner.successful.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn failed(&self) -> u64 {
        self.inner.failed.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn completed(&self) -> u64 {
        self.successful() + self.failed()
    }

    #[inline]
    pub(crate) fn dropped(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn canceled(&self) -> u64 {
        self.inner.canceled.load(Ordering::Relaxed)
    }

    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        let i = &self.inner;
        let successful = i.successful.load(Ordering::Relaxed);
        let failed = i.failed.load(Ordering::Relaxed);
        MetricsSnapshot {
            running_workers: i.running_workers.load(Ordering::Relaxed),
            submitted: i.submitted.load(Ordering::Relaxed),
            waiting: i.waiting.load(Ordering::Relaxed),
            successful,
            failed,
            completed: successful + failed,
            dropped: i.dropped.load(Ordering::Relaxed),
            canceled: i.canceled.load(Ordering::Relaxed),
        }
    }
}

/// An immutable, point-in-time view of a pool's metrics.
///
/// Obtain one via [`Pool::metrics`](crate::Pool::metrics). Individual counters
/// are also exposed directly on the pool (e.g. `running_workers()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetricsSnapshot {
    /// Number of workers currently executing or waiting for a task.
    pub running_workers: u64,
    /// Total number of tasks ever submitted (including dropped ones).
    pub submitted: u64,
    /// Number of tasks currently queued but not yet running.
    pub waiting: u64,
    /// Number of tasks that completed successfully.
    pub successful: u64,
    /// Number of tasks that completed with an error or a panic.
    pub failed: u64,
    /// Number of completed tasks (`successful + failed`).
    pub completed: u64,
    /// Number of tasks dropped because the queue was full (non-blocking pools).
    pub dropped: u64,
    /// Number of tasks canceled before execution (e.g. via the cancel token).
    pub canceled: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_and_snapshot() {
        let m = Metrics::new();
        m.inc_submitted();
        m.inc_submitted();
        m.inc_running();
        m.inc_waiting();
        m.inc_successful();
        m.inc_failed();
        m.inc_dropped();
        m.inc_canceled();

        let snap = m.snapshot();
        assert_eq!(snap.submitted, 2);
        assert_eq!(snap.running_workers, 1);
        assert_eq!(snap.waiting, 1);
        assert_eq!(snap.successful, 1);
        assert_eq!(snap.failed, 1);
        assert_eq!(snap.completed, 2);
        assert_eq!(snap.dropped, 1);
        assert_eq!(snap.canceled, 1);
    }

    #[test]
    fn clone_shares_counters() {
        let m = Metrics::new();
        let m2 = m.clone();
        m.inc_submitted();
        assert_eq!(m2.snapshot().submitted, 1);
    }

    #[test]
    fn dec_counters() {
        let m = Metrics::new();
        m.inc_running();
        m.inc_running();
        m.dec_running();
        m.inc_waiting();
        m.dec_waiting();
        let snap = m.snapshot();
        assert_eq!(snap.running_workers, 1);
        assert_eq!(snap.waiting, 0);
    }
}
