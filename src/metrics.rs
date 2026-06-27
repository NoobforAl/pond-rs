//! Pool metrics.
//!
//! The pool's metric surface: running workers, submitted, waiting,
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

/// A cache-line-aligned `AtomicU64`, so independently-contended counters don't
/// share a line and cause false sharing under high concurrency (many submitters
/// bumping submit-side counters while workers bump completion-side ones). `Deref`
/// keeps every call site reading/writing it exactly like a plain atomic.
#[repr(align(64))]
#[derive(Default)]
struct Padded(AtomicU64);

impl std::ops::Deref for Padded {
    type Target = AtomicU64;
    #[inline]
    fn deref(&self) -> &AtomicU64 {
        &self.0
    }
}

#[derive(Default)]
struct MetricsInner {
    running_workers: Padded,
    submitted: Padded,
    waiting: Padded,
    successful: Padded,
    failed: Padded,
    dropped: Padded,
    canceled: Padded,
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

    /// Atomically reserves a queue slot iff fewer than `capacity` tasks are
    /// currently waiting, incrementing `waiting` on success.
    ///
    /// This closes the check-then-increment race in the submit paths: concurrent
    /// callers can no longer all observe `waiting < capacity` and then each push,
    /// overshooting a bounded queue. Returns `false` (without incrementing) when
    /// the queue is already full.
    #[inline]
    pub(crate) fn try_reserve_waiting(&self, capacity: usize) -> bool {
        let cap = capacity as u64;
        let mut cur = self.inner.waiting.load(Ordering::Relaxed);
        loop {
            if cur >= cap {
                return false;
            }
            match self.inner.waiting.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
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
    /// Number of tasks currently executing (have acquired a concurrency permit
    /// and are running). Queued-but-not-started tasks are counted by `waiting`.
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

#[cfg(feature = "metrics-export")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics-export")))]
impl MetricsSnapshot {
    /// Renders this snapshot in [Prometheus text exposition format].
    ///
    /// `pool_name` is attached as a `pool` label on every metric. Cumulative
    /// tallies are emitted as `counter`s (with the conventional `_total` suffix)
    /// and live values as `gauge`s. The result is a ready-to-serve scrape body,
    /// produced with `std::fmt` only — no extra dependency.
    ///
    /// [Prometheus text exposition format]: https://prometheus.io/docs/instrumenting/exposition_formats/
    ///
    /// ```
    /// use pond::MetricsSnapshot;
    ///
    /// let text = MetricsSnapshot::default().to_prometheus_text("default");
    /// assert!(text.contains("pond_tasks_submitted_total{pool=\"default\"} 0"));
    /// assert!(text.contains("# TYPE pond_running_workers gauge"));
    /// ```
    pub fn to_prometheus_text(&self, pool_name: &str) -> String {
        use std::fmt::Write as _;

        // Escape the label value per the exposition format (backslash, quote, LF).
        let label = pool_name
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");

        let mut out = String::new();
        let mut metric = |name: &str, kind: &str, help: &str, value: u64| {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {kind}");
            let _ = writeln!(out, "{name}{{pool=\"{label}\"}} {value}");
        };

        metric(
            "pond_tasks_submitted_total",
            "counter",
            "Total tasks ever submitted (including dropped ones).",
            self.submitted,
        );
        metric(
            "pond_tasks_successful_total",
            "counter",
            "Tasks that completed successfully.",
            self.successful,
        );
        metric(
            "pond_tasks_failed_total",
            "counter",
            "Tasks that completed with an error or panic.",
            self.failed,
        );
        metric(
            "pond_tasks_completed_total",
            "counter",
            "Completed tasks (successful + failed).",
            self.completed,
        );
        metric(
            "pond_tasks_dropped_total",
            "counter",
            "Tasks dropped because the queue was full.",
            self.dropped,
        );
        metric(
            "pond_tasks_canceled_total",
            "counter",
            "Tasks canceled before execution.",
            self.canceled,
        );
        metric(
            "pond_running_workers",
            "gauge",
            "Tasks currently executing (holding a concurrency permit).",
            self.running_workers,
        );
        metric(
            "pond_waiting_tasks",
            "gauge",
            "Tasks currently queued but not yet running.",
            self.waiting,
        );

        out
    }
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

    #[cfg(feature = "metrics-export")]
    #[test]
    fn prometheus_text_renders_counters_and_gauges() {
        let m = Metrics::new();
        m.inc_submitted();
        m.inc_successful();
        m.inc_running();
        let text = m.snapshot().to_prometheus_text("api");

        assert!(text.contains("# TYPE pond_tasks_submitted_total counter"));
        assert!(text.contains("pond_tasks_submitted_total{pool=\"api\"} 1"));
        assert!(text.contains("pond_tasks_successful_total{pool=\"api\"} 1"));
        assert!(text.contains("# TYPE pond_running_workers gauge"));
        assert!(text.contains("pond_running_workers{pool=\"api\"} 1"));
    }

    #[cfg(feature = "metrics-export")]
    #[test]
    fn prometheus_label_is_escaped() {
        let text = MetricsSnapshot::default().to_prometheus_text("a\"b\\c");
        assert!(text.contains("pool=\"a\\\"b\\\\c\""));
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

    use proptest::prelude::*;

    proptest! {
        /// Each counter must reflect exactly its own `inc`/`dec` calls — no
        /// cross-contamination between fields — and `completed` must always equal
        /// `successful + failed`.
        #[test]
        fn snapshot_reflects_operations(
            submitted in 0u64..50,
            successful in 0u64..50,
            failed in 0u64..50,
            dropped in 0u64..50,
            canceled in 0u64..50,
            running_inc in 0u64..50,
            running_dec in 0u64..50,
            waiting_inc in 0u64..50,
            waiting_dec in 0u64..50,
        ) {
            // Never decrement below zero (the counters are unsigned).
            let running_dec = running_dec.min(running_inc);
            let waiting_dec = waiting_dec.min(waiting_inc);

            let m = Metrics::new();
            for _ in 0..submitted { m.inc_submitted(); }
            for _ in 0..successful { m.inc_successful(); }
            for _ in 0..failed { m.inc_failed(); }
            for _ in 0..dropped { m.inc_dropped(); }
            for _ in 0..canceled { m.inc_canceled(); }
            for _ in 0..running_inc { m.inc_running(); }
            for _ in 0..running_dec { m.dec_running(); }
            for _ in 0..waiting_inc { m.inc_waiting(); }
            for _ in 0..waiting_dec { m.dec_waiting(); }

            let snap = m.snapshot();
            prop_assert_eq!(snap.submitted, submitted);
            prop_assert_eq!(snap.successful, successful);
            prop_assert_eq!(snap.failed, failed);
            prop_assert_eq!(snap.dropped, dropped);
            prop_assert_eq!(snap.canceled, canceled);
            prop_assert_eq!(snap.running_workers, running_inc - running_dec);
            prop_assert_eq!(snap.waiting, waiting_inc - waiting_dec);
            prop_assert_eq!(snap.completed, snap.successful + snap.failed);
        }
    }
}
