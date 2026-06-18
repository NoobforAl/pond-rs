//! Backpressure: the three ways to submit when the queue can fill up.
//!
//! * `try_submit` — never waits; returns `Err(QueueFull)` immediately.
//! * `submit_async` — waits for a free queue slot (applies backpressure).
//! * `submit` — never waits; on a non-blocking pool it drops at capacity.
//!
//! Run with: `cargo run --example backpressure`

use std::time::Duration;

use pond::{Error, Pool, QueueSize};

#[tokio::main]
async fn main() {
    // One worker, a one-slot queue, non-blocking: easy to saturate.
    let pool = Pool::builder()
        .max_concurrency(1)
        .queue_size(QueueSize::Bounded(1))
        .non_blocking(true)
        .build();

    // Occupy the worker.
    let _busy = pool.submit(|| async { tokio::time::sleep(Duration::from_millis(50)).await });
    // Fill the single queue slot.
    let _queued = pool.submit(|| async {});

    // try_submit reports the rejection without waiting.
    match pool.try_submit(|| async {}) {
        Err(Error::QueueFull) => println!("try_submit: rejected (queue full), as expected"),
        Err(e) => println!("try_submit: rejected with {e}"),
        Ok(_) => println!("try_submit: unexpectedly accepted"),
    }

    // submit_async applies backpressure: it awaits a free slot instead of failing.
    let handle = pool.submit_async(|| async { 99 }).await;
    println!(
        "submit_async eventually accepted: {}",
        handle.await.unwrap()
    );

    pool.stop_and_wait().await;
}
