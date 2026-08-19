#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Leak regressions for pool lifecycle churn (no profiler — pure `Weak` probes
//! and completion counting, so it complements the dhat-based `heap.rs`).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

/// Many build -> mixed workload -> drain -> drop rounds. Every round's `Inner`
/// (permit channels, counters, shared data) must be freed after the pool is
/// stopped and dropped — otherwise long-running services churn-leak.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_churn_frees_every_inner() {
    for round in 0..200 {
        let (data, weak) = probe();
        let pool = Pool::builder().max_concurrency(4).task_data(data).build();

        // Mixed work: successful completions, recovered panics, fire-and-forget.
        let mut handles = Vec::new();
        for i in 0..50 {
            handles.push(pool.submit(move || async move {
                if i % 25 == 24 {
                    panic!("boom");
                }
                i
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        for _ in 0..20 {
            pool.spawn(|| async { tokio::task::yield_now().await });
        }
        pool.stop_and_wait().await;
        drop(pool);

        // `stop_and_wait` returns once every task has reached a terminal
        // state, and that signal is sent from *inside* the task future (see
        // `RunningGuard::drop`). The future — and the `Arc<Inner>` it owns — is
        // dropped by the runtime a moment later, on whichever worker thread ran
        // it. So `Inner` is expected to outlive `stop_and_wait` briefly; this
        // waits for that, it does not assume it has already happened.
        //
        // The wait is against a wall clock rather than a yield count. A count of
        // `yield_now()` calls is not a wait at all: it reschedules *this* task
        // and never blocks on another thread reaping its task cell, so under CPU
        // contention (CI runners especially) the loop could run out while the
        // drop was still pending. A real leak still fails here — nothing ever
        // frees `Inner` and the deadline expires.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut freed = false;
        loop {
            if weak.upgrade().is_none() {
                freed = true;
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            freed,
            "round {round}: pool Inner still alive 5s after stop_and_wait + drop"
        );
    }
}

/// A fire-and-forget storm (handles dropped immediately) must still execute
/// every task and reach idle — guards against a leaked `outstanding` admission
/// that would wedge `stop_and_wait` forever.
#[tokio::test]
async fn fire_and_forget_storm_still_drains() {
    let pool = Pool::builder().max_concurrency(4).build();
    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..10_000 {
        let d = done.clone();
        pool.spawn(move || async move {
            tokio::task::yield_now().await;
            d.fetch_add(1, Ordering::SeqCst);
        });
    }
    tokio::time::timeout(Duration::from_secs(30), pool.stop_and_wait())
        .await
        .expect("fire-and-forget storm never drained");
    assert_eq!(done.load(Ordering::SeqCst), 10_000);
}

/// Subpool churn: creating and dropping subpools on a long-lived parent must
/// never wedge the parent, and every subpool's work completes. (The subpool
/// builder has no `task_data` to hang a `Weak` probe on; allocation balance is
/// covered by the dhat test in `heap.rs`.)
#[tokio::test]
async fn subpool_churn_keeps_parent_healthy() {
    let parent = Pool::builder().max_concurrency(8).build();
    for round in 0..500 {
        let sub = parent.subpool_builder().max_concurrency(2).build();

        let h = sub.submit(move || async move { 7 + round });
        assert_eq!(h.await.unwrap(), 7 + round);
        sub.stop_and_wait().await;
        drop(sub);
    }
    // The parent is unaffected by all that churn.
    assert_eq!(parent.submit(|| async { 42 }).await.unwrap(), 42);
    parent.stop_and_wait().await;
}
