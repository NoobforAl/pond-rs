//! Resize a running pool's concurrency limit up and down — e.g. to track load.
//!
//! Run with: `cargo run --example dynamic_resize`

use pond::Pool;

#[tokio::main]
async fn main() {
    let pool = Pool::builder().max_concurrency(2).build();
    println!("start:  max_concurrency = {}", pool.max_concurrency());

    // Scale up for a burst.
    pool.resize(16);
    println!("scaled up: max_concurrency = {}", pool.max_concurrency());

    let mut handles = Vec::new();
    for i in 0..16 {
        handles.push(pool.submit(move || async move { i * i }));
    }
    let sum: u64 = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .sum();
    println!("sum of squares = {sum}");

    // Scale back down once the burst is over.
    pool.resize(2);
    println!("scaled down: max_concurrency = {}", pool.max_concurrency());

    pool.stop_and_wait().await;
}
