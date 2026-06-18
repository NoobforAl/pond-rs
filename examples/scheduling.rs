//! Task scheduling & management: delayed, recurring, retry, and replication.
//!
//! Run with: `cargo run --example scheduling`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::{Pool, RetryPolicy};

#[tokio::main]
async fn main() {
    let pool = Pool::builder().max_concurrency(4).build();

    // 1. Delayed: run a task after a delay.
    let delayed = pool.submit_after(Duration::from_millis(50), || async { "ran late" });
    println!("delayed result: {}", delayed.await.unwrap());

    // 2. Recurring: run every interval until stopped.
    let ticks = Arc::new(AtomicUsize::new(0));
    let t = ticks.clone();
    let schedule = pool.schedule_every(Duration::from_millis(20), move || {
        let t = t.clone();
        async move {
            t.fetch_add(1, Ordering::SeqCst);
        }
    });
    tokio::time::sleep(Duration::from_millis(110)).await;
    schedule.stop();
    println!("recurring fired {} times", ticks.load(Ordering::SeqCst));

    // 3. Retry: resubmit on failure with exponential backoff.
    let attempts = Arc::new(AtomicUsize::new(0));
    let a = attempts.clone();
    let policy = RetryPolicy::new(5)
        .backoff(Duration::from_millis(5))
        .exponential(true);
    let retried = pool.submit_with_retry(policy, move || {
        let a = a.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err("flaky upstream")
            } else {
                Ok::<usize, &str>(n)
            }
        }
    });
    println!(
        "retry succeeded on attempt {:?} after {} tries",
        retried.await.unwrap(),
        attempts.load(Ordering::SeqCst)
    );

    // 4a. Replication — fan-out: N copies at once.
    let handles = pool.submit_n(4, |i| async move { i * i });
    let mut squares = Vec::new();
    for h in handles {
        squares.push(h.await.unwrap());
    }
    squares.sort_unstable();
    println!("fan-out squares: {squares:?}");

    // 4b. Replication — reusable template: define once, fire repeatedly.
    let job = pool.job(|| async { "from template" });
    let first = job.submit().await.unwrap();
    let second = job.submit().await.unwrap();
    println!("template fired twice: {first:?}, {second:?}");

    pool.stop_and_wait().await;
}
