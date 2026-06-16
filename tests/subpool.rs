//! Integration tests for subpools.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::Pool;
use tokio::time::sleep;

#[tokio::test]
async fn subpool_limits_its_own_concurrency() {
    let parent = Pool::builder().max_concurrency(10).build();
    let sub = parent.subpool(2);

    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..20 {
        let current = current.clone();
        let peak = peak.clone();
        handles.push(sub.submit(move || async move {
            let now = current.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            sleep(Duration::from_millis(5)).await;
            current.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "subpool peak {} exceeded cap 2",
        peak.load(Ordering::SeqCst)
    );
}

#[tokio::test]
#[should_panic(expected = "cannot exceed parent")]
async fn subpool_cap_above_parent_panics() {
    let parent = Pool::builder().max_concurrency(2).build();
    let _ = parent.subpool(5);
}

#[tokio::test]
async fn subpool_stop_leaves_parent_running() {
    let parent = Pool::builder().max_concurrency(4).build();
    let sub = parent.subpool(2);

    for _ in 0..5 {
        let _h = sub.submit(|| async {
            sleep(Duration::from_millis(2)).await;
        });
    }
    sub.stop_and_wait().await;
    assert!(sub.is_stopped());
    // Submitting to a stopped subpool is rejected.
    assert!(sub.submit(|| async { 1 }).await.is_err());

    // Parent still accepts and runs work.
    assert_eq!(parent.submit(|| async { 7 }).await.unwrap(), 7);
}

#[tokio::test]
async fn subpool_tracks_metrics() {
    let parent = Pool::builder().max_concurrency(4).build();
    let sub = parent.subpool(2);
    let mut handles = Vec::new();
    for i in 0..6 {
        handles.push(sub.submit(move || async move { i }));
    }
    for h in handles {
        h.await.unwrap();
    }
    sub.stop_and_wait().await;
    let m = sub.metrics();
    assert_eq!(m.submitted, 6);
    assert_eq!(m.successful, 6);
    assert_eq!(m.running_workers, 0);
}

#[tokio::test]
async fn subpool_zero_inherits_parent_limit() {
    let parent = Pool::builder().max_concurrency(3).build();
    let sub = parent.subpool(0);
    assert_eq!(sub.max_concurrency(), 3);
}
