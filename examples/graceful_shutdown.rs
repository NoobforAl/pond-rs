//! Graceful shutdown: stop accepting new work, then drain everything in flight.
//!
//! Run with: `cargo run --example graceful_shutdown`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::Pool;

#[tokio::main]
async fn main() {
    let pool = Pool::builder().max_concurrency(4).build();
    let done = Arc::new(AtomicUsize::new(0));

    // Queue more work than can run at once.
    for _ in 0..20 {
        let done = done.clone();
        pool.spawn(move || async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            done.fetch_add(1, Ordering::SeqCst);
        });
    }

    // `stop()` stops accepting new tasks; awaiting the handle drains the queued
    // and running ones (a graceful drain — nothing already accepted is dropped).
    pool.stop().await;

    // Submissions after stop are rejected rather than silently lost.
    assert!(pool.submit(|| async { 1 }).await.is_err());

    println!(
        "all {} queued tasks completed before exit",
        done.load(Ordering::SeqCst)
    );
}
