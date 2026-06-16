//! Cancel a pool's queued work with a shared `CancelToken` (pond's `WithContext`).
//!
//! Run with: `cargo run --example pool_context`

use std::time::Duration;

use pond::{CancelToken, Pool};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    // A shared cancel token lets external code cancel the pool, like a context.
    let token = CancelToken::new();
    let pool = Pool::builder()
        .max_concurrency(2)
        .cancel_token(token.clone())
        .build();

    let handles: Vec<_> = (0..20)
        .map(|i| {
            pool.submit(move || async move {
                sleep(Duration::from_millis(50)).await;
                i
            })
        })
        .collect();

    // Let a couple of tasks start, then cancel the rest.
    sleep(Duration::from_millis(20)).await;
    token.cancel();

    let mut completed = 0;
    let mut canceled = 0;
    for h in handles {
        match h.await {
            Ok(_) => completed += 1,
            Err(_) => canceled += 1,
        }
    }
    println!("completed = {completed}, canceled = {canceled}");
    println!(
        "metrics: successful={}, canceled={}",
        pool.successful_tasks(),
        pool.canceled_tasks()
    );
}
