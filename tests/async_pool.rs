#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Integration tests for the async [`Pool`].

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::{Error, Pool, QueueSize};
use tokio::time::sleep;

mod common;
use common::wait_until;

#[tokio::test]
async fn stop_and_wait_does_not_hang_after_unrecovered_panic() {
    // Regression: a non-recovered task panic must still resolve its handle (as
    // `Canceled`) and must not leak the running tally — otherwise `stop_and_wait`
    // (which waits for the pool to go idle) would hang forever.
    let pool = Pool::builder()
        .max_concurrency(2)
        .panic_recovery(false)
        .build();
    let handle = pool.submit(|| async { panic!("boom") });
    assert!(matches!(handle.await, Err(Error::Canceled)));

    tokio::time::timeout(Duration::from_secs(5), pool.stop_and_wait())
        .await
        .expect("stop_and_wait hung after a non-recovered panic");
}

#[tokio::test]
async fn submit_and_await_result() {
    let pool = Pool::builder().max_concurrency(4).build();
    let handle = pool.submit(|| async { 1 + 2 });
    assert_eq!(handle.await.unwrap(), 3);
}

#[tokio::test]
async fn submit_many_tasks_all_run() {
    let pool = Pool::builder().max_concurrency(8).build();
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..1000 {
        let c = counter.clone();
        handles.push(pool.submit(move || async move {
            c.fetch_add(1, Ordering::SeqCst);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 1000);
}

#[tokio::test]
async fn respects_max_concurrency() {
    let max = 3;
    let pool = Pool::builder().max_concurrency(max).build();

    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..50 {
        let current = current.clone();
        let peak = peak.clone();
        handles.push(pool.submit(move || async move {
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
        peak.load(Ordering::SeqCst) <= max,
        "peak concurrency {} exceeded max {}",
        peak.load(Ordering::SeqCst),
        max
    );
}

#[tokio::test]
async fn try_submit_rejects_when_queue_full() {
    // Concurrency 1, queue capacity 1: one running + one queued is the max.
    let pool = Pool::builder()
        .max_concurrency(1)
        .queue_size(QueueSize::Bounded(1))
        .build();

    // Block the single worker; `started` flips once the task body is running
    // (i.e. it has left the queue and holds the only concurrency permit).
    let gate = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(AtomicBool::new(false));
    let g = gate.clone();
    let s = started.clone();
    let _busy = pool.submit(move || async move {
        s.store(true, Ordering::SeqCst);
        g.notified().await;
    });
    wait_until(|| started.load(Ordering::SeqCst)).await;

    // Fill the queue (capacity 1); admission is synchronous, so once this
    // returns Ok the queue is genuinely full.
    let _queued = pool.try_submit(|| async {}).expect("first queued task ok");

    // Now the queue is full -> reject.
    let rejected = pool.try_submit(|| async {});
    assert_eq!(rejected.err(), Some(Error::QueueFull));

    gate.notify_waiters();
    pool.stop_and_wait().await;
}

#[tokio::test]
async fn non_blocking_submit_drops_and_resolves_queue_full() {
    let pool = Pool::builder()
        .max_concurrency(1)
        .queue_size(QueueSize::Bounded(1))
        .non_blocking(true)
        .build();

    let gate = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(AtomicBool::new(false));
    let g = gate.clone();
    let s = started.clone();
    let _busy = pool.submit(move || async move {
        s.store(true, Ordering::SeqCst);
        g.notified().await;
    });
    wait_until(|| started.load(Ordering::SeqCst)).await;

    let _queued = pool.submit(|| async {});

    // Dropped task's handle resolves with QueueFull.
    let dropped = pool.submit(|| async { 99 });
    assert_eq!(dropped.await, Err(Error::QueueFull));

    gate.notify_waiters();
    pool.stop_and_wait().await;
}

#[tokio::test]
async fn unbounded_queue_never_drops() {
    let pool = Pool::builder().max_concurrency(2).build();
    for _ in 0..500 {
        let _h = pool.try_submit(|| async {}).expect("never rejected");
    }
    pool.stop_and_wait().await;
}

#[tokio::test]
async fn panic_is_recovered_by_default() {
    let pool = Pool::builder().max_concurrency(1).build();
    let handle = pool.submit(|| async { panic!("boom") });
    match handle.await {
        Err(Error::Panicked(msg)) => assert!(msg.contains("boom")),
        other => panic!("expected Panicked, got {other:?}"),
    }

    // Pool stays healthy after a panic.
    assert_eq!(pool.submit(|| async { 7 }).await.unwrap(), 7);
}

#[tokio::test]
async fn submit_after_stop_is_rejected() {
    let pool = Pool::builder().max_concurrency(2).build();
    pool.stop_and_wait().await;
    assert!(pool.is_stopped());
    let handle = pool.submit(|| async { 1 });
    assert_eq!(handle.await, Err(Error::PoolStopped));
}

#[tokio::test]
async fn stop_and_wait_drains_queued_tasks() {
    let pool = Pool::builder().max_concurrency(2).build();
    let counter = Arc::new(AtomicUsize::new(0));
    for _ in 0..100 {
        let c = counter.clone();
        pool.spawn(move || async move {
            sleep(Duration::from_millis(1)).await;
            c.fetch_add(1, Ordering::SeqCst);
        });
    }
    pool.stop_and_wait().await;
    assert_eq!(counter.load(Ordering::SeqCst), 100);
}

#[tokio::test]
async fn cancel_cancels_queued_tasks() {
    let pool = Pool::builder().max_concurrency(1).build();

    let gate = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(AtomicBool::new(false));
    let g = gate.clone();
    let s = started.clone();
    let running = pool.submit(move || async move {
        s.store(true, Ordering::SeqCst);
        g.notified().await;
        "done"
    });
    wait_until(|| started.load(Ordering::SeqCst)).await;

    // Queue several tasks behind the busy worker.
    let queued: Vec<_> = (0..5)
        .map(|i| pool.submit(move || async move { i }))
        .collect();

    pool.cancel();
    gate.notify_waiters();

    // The running task completes; queued tasks are canceled.
    assert_eq!(running.await.unwrap(), "done");
    for h in queued {
        assert_eq!(h.await, Err(Error::Canceled));
    }
}

#[tokio::test]
async fn resize_grow_increases_concurrency() {
    let pool = Pool::builder().max_concurrency(1).build();
    pool.resize(4);
    assert_eq!(pool.max_concurrency(), 4);

    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..20 {
        let current = current.clone();
        let peak = peak.clone();
        handles.push(pool.submit(move || async move {
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
        peak.load(Ordering::SeqCst) > 1,
        "resize did not raise concurrency"
    );
    assert!(peak.load(Ordering::SeqCst) <= 4);
}

#[tokio::test]
async fn resize_to_zero_is_unbounded() {
    let pool = Pool::builder().max_concurrency(1).build();
    pool.resize(0);
    assert_eq!(pool.max_concurrency(), 0);

    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..10 {
        let current = current.clone();
        let peak = peak.clone();
        handles.push(pool.submit(move || async move {
            let now = current.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            sleep(Duration::from_millis(5)).await;
            current.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert!(peak.load(Ordering::SeqCst) > 1);
}

#[tokio::test]
async fn submit_async_applies_backpressure() {
    let pool = Pool::builder()
        .max_concurrency(1)
        .queue_size(QueueSize::Bounded(2))
        .build();

    // This should complete without rejecting, awaiting slots as needed.
    let mut handles = Vec::new();
    for i in 0..10 {
        handles.push(pool.submit_async(move || async move { i }).await);
    }
    let mut sum = 0;
    for h in handles {
        sum += h.await.unwrap();
    }
    assert_eq!(sum, (0..10).sum());
}

#[tokio::test]
async fn pool_is_cloneable_and_shares_state() {
    let pool = Pool::builder().max_concurrency(2).build();
    let clone = pool.clone();
    // Work submitted through the clone runs on the same pool, so the original
    // observes its result and keeps serving work.
    assert_eq!(clone.submit(|| async { 21 }).await.unwrap() * 2, 42);
    assert_eq!(pool.submit(|| async { 7 }).await.unwrap(), 7);
}

#[tokio::test]
async fn cancel_when_future_shuts_down_pool() {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let pool = Pool::builder()
        .max_concurrency(1)
        .cancel_when(async move {
            let _ = rx.await;
        })
        .build();
    assert!(!pool.is_stopped());

    // Fire the external signal; the pool should observe cancellation.
    let _ = tx.send(());
    wait_until({
        let pool = pool.clone();
        move || pool.is_stopped()
    })
    .await;

    let h = pool.submit(|| async { 1 });
    assert_eq!(h.await, Err(Error::Canceled));
}

#[tokio::test(start_paused = true)]
async fn long_running_tasks_complete_under_virtual_time() {
    // Each task "sleeps" for 10 minutes. With Tokio's paused clock, virtual time
    // auto-advances once every task is parked on its timer, so the whole test
    // finishes in milliseconds while still exercising long-duration waits.
    let pool = Pool::builder().max_concurrency(4).build();

    let handles: Vec<_> = (0..4)
        .map(|i| {
            pool.submit(move || async move {
                sleep(Duration::from_secs(10 * 60)).await; // 10 minutes
                i
            })
        })
        .collect();

    let mut got = Vec::new();
    for h in handles {
        got.push(h.await.unwrap());
    }
    got.sort_unstable();
    assert_eq!(got, vec![0, 1, 2, 3]);
    pool.stop_and_wait().await;
}

#[tokio::test]
async fn deadline_cancels_pending_work() {
    let pool = Pool::builder()
        .max_concurrency(1)
        .deadline(Duration::from_millis(20))
        .build();

    // Occupy the single worker so the rest stay queued past the deadline.
    let gate = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(AtomicBool::new(false));
    let g = gate.clone();
    let s = started.clone();
    let busy = pool.submit(move || async move {
        s.store(true, Ordering::SeqCst);
        g.notified().await;
    });
    wait_until(|| started.load(Ordering::SeqCst)).await;

    let queued: Vec<_> = (0..3)
        .map(|i| pool.submit(move || async move { i }))
        .collect();

    // Let the deadline elapse, then release the busy worker.
    sleep(Duration::from_millis(50)).await;
    gate.notify_waiters();
    let _ = busy.await;

    assert!(pool.is_stopped());
    for h in queued {
        assert_eq!(h.await, Err(Error::Canceled));
    }
}
