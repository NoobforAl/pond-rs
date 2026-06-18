//! Integration tests for subpools.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::{Error, Pool};
use tokio::time::sleep;

#[tokio::test]
async fn subpool_always_recovers_panics_even_when_parent_disables_recovery() {
    // The parent disables panic recovery, but a subpool is an isolation boundary
    // and recovers regardless — the panic surfaces as `Error::Panicked` and the
    // subpool keeps serving tasks.
    let parent = Pool::builder()
        .max_concurrency(4)
        .panic_recovery(false)
        .build();
    let sub = parent.subpool(2);

    let err = sub.submit(|| async { panic!("boom") }).await.unwrap_err();
    assert!(matches!(err, Error::Panicked(_)), "got {err:?}");

    // Subpool still works after the panic.
    assert_eq!(sub.submit(|| async { 7 }).await.unwrap(), 7);
}

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

#[tokio::test]
async fn subpool_panic_is_recovered_and_counted() {
    let parent = Pool::builder().max_concurrency(2).build();
    let sub = parent.subpool(1);
    let h = sub.submit(|| async { panic!("subpool boom") });
    match h.await {
        Err(pond::Error::Panicked(msg)) => assert!(msg.contains("subpool boom")),
        other => panic!("expected Panicked, got {other:?}"),
    }
    assert_eq!(sub.failed_tasks(), 1);
    // Subpool stays healthy.
    assert_eq!(sub.submit(|| async { 7 }).await.unwrap(), 7);
}

#[tokio::test]
async fn subpool_cancel_abandons_queued_tasks() {
    let parent = Pool::builder().max_concurrency(4).build();
    let sub = parent.subpool(1);

    let gate = Arc::new(tokio::sync::Notify::new());
    let g = gate.clone();
    let running = sub.submit(move || async move {
        g.notified().await;
        "done"
    });
    // Wait until the gate task occupies the single subpool slot.
    for _ in 0..500 {
        if sub.running_workers() == 1 {
            break;
        }
        sleep(Duration::from_millis(2)).await;
    }

    let queued: Vec<_> = (0..5)
        .map(|i| sub.submit(move || async move { i }))
        .collect();
    sub.cancel();
    gate.notify_waiters();

    assert_eq!(running.await.unwrap(), "done");
    for h in queued {
        assert_eq!(h.await, Err(pond::Error::Canceled));
    }
    assert!(sub.canceled_tasks() >= 1);
    // Parent is unaffected.
    assert_eq!(parent.submit(|| async { 3 }).await.unwrap(), 3);
}

#[tokio::test]
async fn subpool_try_submit_rejects_when_full() {
    use pond::QueueSize;
    // A bounded subpool: build via builder once Phase 4 lands; here we exercise
    // try_submit's stopped-rejection path which is always available.
    let parent = Pool::builder()
        .max_concurrency(1)
        .queue_size(QueueSize::Bounded(1))
        .build();
    let sub = parent.subpool(1);
    sub.stop_and_wait().await;
    assert!(sub.try_submit(|| async { 1 }).is_err());
}

#[tokio::test]
async fn subpool_group_collects_ordered_results() {
    let parent = Pool::builder().max_concurrency(8).build();
    let sub = parent.subpool(2);
    let mut group = sub.group::<usize>();
    for i in 0..10usize {
        group.submit(move || async move {
            sleep(Duration::from_millis((10 - i) as u64)).await;
            i * 3
        });
    }
    let results = group.wait().await.unwrap();
    assert_eq!(results, (0..10).map(|i| i * 3).collect::<Vec<_>>());
}

#[tokio::test]
async fn group_builder_caps_concurrency_for_partitioning() {
    // Pool of 100; a group limited to 10 — leaving 90 for other work.
    let pool = Pool::builder().max_concurrency(100).build();
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut group = pool.group_builder::<()>().max_concurrency(10).build();
    for _ in 0..60 {
        let current = current.clone();
        let peak = peak.clone();
        group.submit(move || async move {
            let now = current.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            sleep(Duration::from_millis(5)).await;
            current.fetch_sub(1, Ordering::SeqCst);
        });
    }
    group.wait().await.unwrap();
    assert!(
        peak.load(Ordering::SeqCst) <= 10,
        "group peak {} exceeded cap 10",
        peak.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn subpool_builder_bounded_queue_rejects_when_full() {
    use pond::QueueSize;
    let parent = Pool::builder().max_concurrency(1).build();
    let sub = parent
        .subpool_builder()
        .max_concurrency(1)
        .queue_size(QueueSize::Bounded(1))
        .non_blocking(true)
        .build();

    let gate = Arc::new(tokio::sync::Notify::new());
    let g = gate.clone();
    let _busy = sub.submit(move || async move {
        g.notified().await;
    });
    for _ in 0..500 {
        if sub.running_workers() == 1 {
            break;
        }
        sleep(Duration::from_millis(2)).await;
    }

    let _queued = sub.submit(|| async {}); // fills the 1-slot queue
    let rejected = sub.submit(|| async { 9 });
    assert_eq!(rejected.await, Err(pond::Error::QueueFull));
    assert!(sub.dropped_tasks() >= 1);

    gate.notify_waiters();
}

#[tokio::test]
async fn subpool_builder_external_cancel_token() {
    use pond::CancelToken;
    let parent = Pool::builder().max_concurrency(4).build();
    let token = CancelToken::new();
    let sub = parent
        .subpool_builder()
        .max_concurrency(2)
        .cancel_token(token.clone())
        .build();

    // Cancelling the external token stops the subpool.
    token.cancel();
    assert!(sub.is_stopped());
    assert_eq!(sub.submit(|| async { 1 }).await, Err(pond::Error::Canceled));
    // Parent unaffected.
    assert_eq!(parent.submit(|| async { 5 }).await.unwrap(), 5);
}

#[tokio::test]
async fn min_concurrency_is_a_resize_floor() {
    let pool = Pool::builder()
        .max_concurrency(8)
        .min_concurrency(4)
        .build();
    pool.resize(1);
    assert_eq!(pool.max_concurrency(), 4); // clamped up to the floor
    pool.resize(6);
    assert_eq!(pool.max_concurrency(), 6);
    pool.resize(0);
    assert_eq!(pool.max_concurrency(), 0); // explicit unbounded still allowed
}

#[tokio::test]
async fn multi_tenant_isolation_caps_each_tenant() {
    // Shared pool of 12 workers, two tenants each capped at 4. Tenant A bursts
    // hard; tenant B submits a modest set. Neither may exceed its own cap, and
    // A's burst must not starve B out of its slice.
    let pool = Pool::builder().max_concurrency(12).build();
    let tenant_a = pool.subpool_builder().max_concurrency(4).build();
    let tenant_b = pool.subpool_builder().max_concurrency(4).build();

    // Per-tenant peak trackers (same pattern as the partitioning test above).
    let a_cur = Arc::new(AtomicUsize::new(0));
    let a_peak = Arc::new(AtomicUsize::new(0));
    let b_cur = Arc::new(AtomicUsize::new(0));
    let b_peak = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..40 {
        let cur = a_cur.clone();
        let peak = a_peak.clone();
        handles.push(tenant_a.submit(move || async move {
            let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            sleep(Duration::from_millis(5)).await;
            cur.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    for _ in 0..10 {
        let cur = b_cur.clone();
        let peak = b_peak.clone();
        handles.push(tenant_b.submit(move || async move {
            let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            sleep(Duration::from_millis(5)).await;
            cur.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let a = a_peak.load(Ordering::SeqCst);
    let b = b_peak.load(Ordering::SeqCst);
    assert!(a <= 4, "tenant A peak {a} exceeded its cap 4");
    assert!(b <= 4, "tenant B peak {b} exceeded its cap 4");
    // Tenant B was not starved by A's burst: all its jobs ran and completed.
    assert!(b >= 1, "tenant B never ran — starved by tenant A");
    assert_eq!(tenant_b.completed_tasks(), 10);
    // Combined demand can't exceed the shared pool's total capacity.
    assert!(
        a + b <= 12,
        "combined peak {} exceeded shared cap 12",
        a + b
    );
}

#[tokio::test]
async fn subpool_resize_grows_concurrency() {
    let parent = Pool::builder().max_concurrency(8).build();
    let sub = parent.subpool(1);
    sub.resize(4);
    assert_eq!(sub.max_concurrency(), 4);

    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..16 {
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
    assert!(peak.load(Ordering::SeqCst) > 1);
    assert!(peak.load(Ordering::SeqCst) <= 4);
}
