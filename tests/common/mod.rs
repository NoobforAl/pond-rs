//! Shared helpers for integration tests.
//!
//! Lives in `tests/common/` (a module directory, not its own test binary) so
//! every integration test file can `mod common; use common::*;`. Each test file
//! compiles this module into its own crate, so items unused by a given file are
//! expected — hence the blanket allows below (`dead_code` for unused helpers,
//! `unused_imports` for unused re-exports, `unreachable_pub` because a test
//! binary exports nothing).
#![allow(dead_code, unused_imports, unreachable_pub)]

pub mod mock_spawner;
pub use mock_spawner::MockSpawner;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::{Pool, QueueSize, TokioSpawner};
use tokio::time::sleep;

/// Polls until `cond` returns true, or panics after ~2s.
///
/// Prefer this over fixed sleeps: it reacts as soon as the condition holds and
/// only times out if something is genuinely wrong.
pub async fn wait_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        sleep(Duration::from_millis(2)).await;
    }
    panic!("condition not met within timeout");
}

/// Builds a Tokio-backed pool with a bounded queue. Must be called from within a
/// Tokio runtime (e.g. inside a `#[tokio::test]`).
pub fn build_bounded_pool(max_concurrency: usize, queue_capacity: usize) -> Pool<TokioSpawner> {
    Pool::builder()
        .max_concurrency(max_concurrency)
        .queue_size(QueueSize::Bounded(queue_capacity))
        .build()
}

/// Tracks live and peak concurrency. The guard returned by [`enter`](Self::enter)
/// decrements the live count on drop, so the count stays correct even if a task
/// panics.
#[derive(Clone)]
pub struct ConcurrencyTracker {
    current: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl ConcurrencyTracker {
    pub fn new() -> Self {
        Self {
            current: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn enter(&self) -> ConcurrencyGuard {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        ConcurrencyGuard {
            current: self.current.clone(),
        }
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

impl Default for ConcurrencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ConcurrencyGuard {
    current: Arc<AtomicUsize>,
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }
}
