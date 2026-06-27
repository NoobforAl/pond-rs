#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Integration tests for the task-scheduling helpers.
//!
//! Timing tests use Tokio's virtual clock (`start_paused`), so they're
//! deterministic and instant — the runtime auto-advances time when idle.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pond::{Pool, RetryPolicy};

#[tokio::test(start_paused = true)]
async fn submit_after_runs_after_delay() {
    let pool = Pool::builder().max_concurrency(2).build();
    // `tokio::time::Instant` tracks the virtual clock; `std::time::Instant` would not.
    let start = tokio::time::Instant::now();
    let handle = pool.submit_after(Duration::from_secs(10), || async { 42 });
    assert_eq!(handle.await.unwrap(), 42);
    assert!(
        start.elapsed() >= Duration::from_secs(10),
        "delay was not honored"
    );
}

#[tokio::test(start_paused = true)]
async fn submit_at_past_instant_runs_promptly() {
    let pool = Pool::builder().max_concurrency(2).build();
    let past = Instant::now() - Duration::from_secs(5);
    let handle = pool.submit_at(past, || async { 7 });
    assert_eq!(handle.await.unwrap(), 7);
}

#[tokio::test(start_paused = true)]
async fn schedule_every_fires_until_stopped() {
    let pool = Pool::builder().max_concurrency(4).build();
    let count = Arc::new(AtomicUsize::new(0));

    let c = count.clone();
    let task = pool.schedule_every(Duration::from_millis(100), move || {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
        }
    });

    // ~3 intervals elapse (ticks at 100/200/300ms).
    tokio::time::sleep(Duration::from_millis(350)).await;
    task.stop();
    assert!(task.is_stopped());
    let after_stop = count.load(Ordering::SeqCst);
    assert!(after_stop >= 3, "expected >= 3 ticks, got {after_stop}");

    // No further ticks after stopping.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(count.load(Ordering::SeqCst), after_stop);
}

#[tokio::test(start_paused = true)]
async fn submit_with_retry_succeeds_after_failures() {
    let pool = Pool::builder().max_concurrency(2).build();
    let attempts = Arc::new(AtomicUsize::new(0));

    let a = attempts.clone();
    let policy = RetryPolicy::new(5)
        .backoff(Duration::from_millis(10))
        .exponential(true);
    let handle = pool.submit_with_retry(policy, move || {
        let a = a.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err("transient")
            } else {
                Ok::<usize, &str>(n)
            }
        }
    });

    assert_eq!(handle.await.unwrap(), Ok(2));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn submit_with_retry_exhausts_and_returns_last_error() {
    let pool = Pool::builder().max_concurrency(2).build();
    let attempts = Arc::new(AtomicUsize::new(0));

    let a = attempts.clone();
    let policy = RetryPolicy::new(3);
    let handle = pool.submit_with_retry(policy, move || {
        let a = a.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            Err::<u32, &str>("always")
        }
    });

    assert_eq!(handle.await.unwrap(), Err("always"));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "should try exactly max_attempts"
    );
}

#[tokio::test(start_paused = true)]
async fn submit_with_retry_if_stops_on_non_retryable() {
    let pool = Pool::builder().max_concurrency(2).build();
    let attempts = Arc::new(AtomicUsize::new(0));

    let a = attempts.clone();
    let policy = RetryPolicy::new(5).backoff(Duration::from_millis(10));
    // Only retry "transient" errors; a "permanent" error returns immediately.
    let handle = pool.submit_with_retry_if(
        policy,
        move || {
            let a = a.clone();
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                Err::<u32, &str>("permanent")
            }
        },
        |e| *e == "transient",
    );

    assert_eq!(handle.await.unwrap(), Err("permanent"));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "non-retryable error must not be retried"
    );
}

#[tokio::test(start_paused = true)]
async fn submit_with_retry_caps_backoff() {
    // With an aggressive exponential and a tight cap, the total wait stays
    // bounded by max_attempts * max_backoff (plus jitter headroom).
    let pool = Pool::builder().max_concurrency(2).build();
    let policy = RetryPolicy::new(4)
        .backoff(Duration::from_secs(1))
        .exponential(true)
        .max_backoff(Duration::from_secs(2))
        .jitter(true);

    let start = tokio::time::Instant::now();
    let handle = pool.submit_with_retry(policy, || async { Err::<u32, &str>("nope") });
    assert_eq!(handle.await.unwrap(), Err("nope"));
    // Uncapped this would be 1 + 2 + 4 = 7s of backoff; capped at 2s it is at
    // most 2 + 2 + 2 = 6s, and with equal jitter strictly less.
    assert!(
        start.elapsed() < Duration::from_secs(7),
        "max_backoff did not cap the delay: {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn submit_n_runs_count_copies() {
    let pool = Pool::builder().max_concurrency(4).build();
    let handles = pool.submit_n(5, |i| async move { i * 2 });
    assert_eq!(handles.len(), 5);

    let mut got = Vec::new();
    for h in handles {
        got.push(h.await.unwrap());
    }
    got.sort_unstable();
    assert_eq!(got, vec![0, 2, 4, 6, 8]);
}

#[tokio::test]
async fn job_is_reusable_and_cloneable() {
    let pool = Pool::builder().max_concurrency(2).build();
    let counter = Arc::new(AtomicUsize::new(0));

    let c = counter.clone();
    let job = pool.job(move || {
        let c = c.clone();
        async move { c.fetch_add(1, Ordering::SeqCst) }
    });

    let r1 = job.submit().await.unwrap();
    let r2 = job.clone().submit().await.unwrap(); // clone shares the factory
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_ne!(r1, r2, "each submission ran the task once");

    // Fan-out from the same template.
    let handles = job.submit_n(3);
    for h in handles {
        let _ = h.await.unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 5);
}

#[cfg(feature = "runtime-smol")]
#[test]
fn submit_after_is_runtime_agnostic() {
    use pond::SmolSpawner;

    let pool = Pool::builder_with(SmolSpawner::new())
        .max_concurrency(2)
        .build();
    let got = futures::executor::block_on(async {
        pool.submit_after(Duration::from_millis(20), || async { 5 })
            .await
            .unwrap()
    });
    assert_eq!(got, 5);
}
