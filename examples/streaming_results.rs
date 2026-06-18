//! Stream results out of tasks as they finish, instead of collecting handles.
//!
//! The idiomatic pattern: stash a channel `Sender` as the pool's shared task
//! data, and have each context-aware task pull it and send through it. Here we
//! use a `tokio::sync::mpsc` sender (any `Send + Sync + 'static` channel works).
//!
//! Run with: `cargo run --example streaming_results`

use std::sync::Arc;

use pond::Pool;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::unbounded_channel::<usize>();

    // Attach the sender as shared task data (wrapped in `Arc`).
    let pool = Pool::builder()
        .max_concurrency(4)
        .task_data(Arc::new(tx))
        .build();

    let task_count = 10;
    for i in 0..task_count {
        pool.spawn_ctx(move |ctx| async move {
            let result = i * i;
            // Pull the shared sender and stream this task's result out.
            if let Some(sender) = ctx.data::<mpsc::UnboundedSender<usize>>() {
                let _ = sender.send(result);
            }
        });
    }

    // Collect results as they stream in. (We know how many to expect; relying on
    // channel close would require dropping the pool's own hold on the sender.)
    let mut received = Vec::new();
    for _ in 0..task_count {
        received.push(rx.recv().await.unwrap());
    }
    received.sort_unstable();
    println!("streamed results: {received:?}");

    pool.stop_and_wait().await;
}
