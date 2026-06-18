//! A runtime-free [`Spawner`](pond::Spawner) test double.
//!
//! `MockSpawner` lets unit/integration tests drive `Pool<S>` without Tokio: each
//! spawned future runs to completion on its own dedicated thread via
//! `futures::executor::block_on` (a dev-dependency). This proves the pool is
//! genuinely runtime-agnostic and gives tests a `spawn_count` to assert how many
//! tasks were launched.
//!
//! Determinism note: tasks run on background threads, so tests should gate task
//! progress with a channel/`Notify` they control and observe outcomes with
//! [`wait_until`](super::wait_until) rather than relying on wall-clock timing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future::BoxFuture;
use pond::Spawner;

/// A [`Spawner`](pond::Spawner) that runs each task on a dedicated thread, with no
/// async runtime required. Clones share the same spawn counter.
#[derive(Clone, Default)]
pub struct MockSpawner {
    spawn_count: Arc<AtomicUsize>,
    blocking_count: Arc<AtomicUsize>,
}

impl MockSpawner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of `spawn` calls observed so far.
    pub fn spawn_count(&self) -> usize {
        self.spawn_count.load(Ordering::SeqCst)
    }

    /// Number of `spawn_blocking` calls observed so far.
    pub fn blocking_count(&self) -> usize {
        self.blocking_count.load(Ordering::SeqCst)
    }
}

impl Spawner for MockSpawner {
    fn spawn(&self, future: BoxFuture<'static, ()>) {
        self.spawn_count.fetch_add(1, Ordering::SeqCst);
        // Each task gets its own thread; `block_on` parks (no busy spin) on
        // futures that are not yet ready, including ones that wait forever.
        std::thread::spawn(move || futures::executor::block_on(future));
    }

    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        self.blocking_count.fetch_add(1, Ordering::SeqCst);
        std::thread::spawn(f);
    }
}
