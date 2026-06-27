#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Lifecycle / leak regression tests.
//!
//! These guard the fix for the background-watcher Arc cycle: before it, the
//! cancel-watcher and metrics-export tasks held a strong `Arc<Inner>` and parked
//! forever, so dropping a `Pool` without `stop()`/`cancel()` leaked `Inner` (and
//! its permit channels, metrics and shared data) for the process lifetime.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};
use std::time::Duration;

use pond::Pool;

/// A probe whose `Weak` can only be upgraded while the pool's `Inner` is alive,
/// because the sole strong reference lives in the pool's `task_data`.
fn probe() -> (Arc<dyn std::any::Any + Send + Sync>, Weak<AtomicBool>) {
    let strong = Arc::new(AtomicBool::new(true));
    let weak = Arc::downgrade(&strong);
    (strong as Arc<dyn std::any::Any + Send + Sync>, weak)
}

#[tokio::test]
async fn dropping_pool_without_stop_frees_inner() {
    let (data, weak) = probe();
    let pool = Pool::builder().max_concurrency(4).task_data(data).build();
    // Run and finish some work so no task future retains `Inner`.
    pool.submit(|| async { 1 + 1 }).await.unwrap();
    assert!(weak.upgrade().is_some(), "alive while the pool is held");

    drop(pool);
    // `Inner` drops synchronously with the last `Pool` handle; give the runtime a
    // tick for the woken watcher task to unwind too.
    tokio::task::yield_now().await;
    assert!(
        weak.upgrade().is_none(),
        "Inner (and its task_data) leaked after dropping the pool"
    );
}

#[tokio::test]
async fn dropping_pool_with_metrics_export_frees_inner() {
    let (data, weak) = probe();
    let (tx, _rx) = async_channel::unbounded();
    let pool = Pool::builder()
        .max_concurrency(2)
        .task_data(data)
        .metrics_channel(Duration::from_millis(5), tx)
        .build();
    pool.submit(|| async { 7 }).await.unwrap();
    drop(pool);

    // The metrics task holds only a `Weak<Inner>`, so `Inner` frees immediately.
    tokio::task::yield_now().await;
    assert!(
        weak.upgrade().is_none(),
        "metrics-export task leaked Inner after drop"
    );
}

#[tokio::test]
async fn dropping_subpool_does_not_wedge_parent() {
    let pool = Pool::builder().max_concurrency(8).build();
    // Previously each subpool leaked a parked watcher task (holding SubInner).
    for _ in 0..256 {
        let sub = pool.subpool(2);
        sub.submit(|| async { 1 }).await.unwrap();
        drop(sub);
    }
    // The parent is unaffected and still serves work.
    assert_eq!(pool.submit(|| async { 42 }).await.unwrap(), 42);
    pool.stop_and_wait().await;
}
