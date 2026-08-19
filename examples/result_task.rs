//! Submit tasks that return values and await their results.
//!
//! Run with: `cargo run --example result_task`

use pond::Pool;

#[tokio::main]
async fn main() {
    let pool = Pool::builder().max_concurrency(4).build();

    // Submit ten tasks, each returning a computed value.
    let handles: Vec<_> = (0..10)
        .map(|i| pool.submit(move || async move { i * i }))
        .collect();

    let mut squares = Vec::new();
    for handle in handles {
        squares.push(handle.await.unwrap());
    }
    println!("squares = {squares:?}");

    pool.stop_and_wait().await;
    println!("completed {} tasks", squares.len());
}
