//! Builder config: stream task results to a channel, and plug in any channel
//! you like via the `ResultSink` interface.
//!
//! Run with: `cargo run --example config_channels`

use pond::{Pool, ResultSink};
use tokio::sync::mpsc;

/// A custom sink: wrap any channel in a newtype and implement `ResultSink`.
/// (The orphan rule means you implement it for *your* type, not the foreign
/// sender directly.)
struct TokioSink(mpsc::UnboundedSender<u64>);

impl ResultSink<u64> for TokioSink {
    fn send(&self, value: u64) -> bool {
        self.0.send(value).is_ok()
    }
}

#[tokio::main]
async fn main() {
    // --- 1. Typed result pool: results stream to a channel, no handles. ---
    let pool = Pool::builder().max_concurrency(4).build();
    let (tx, rx) = async_channel::unbounded::<u64>();
    let typed = pool.results_to(tx);
    for i in 0..5u64 {
        typed.submit(move || async move { i * i });
    }
    let mut squares = Vec::new();
    for _ in 0..5 {
        squares.push(rx.recv().await.unwrap());
    }
    squares.sort_unstable();
    println!("streamed squares: {squares:?}");

    // --- 2. Streaming group with a concurrency cap. ---
    let (gtx, grx) = async_channel::unbounded::<u64>();
    let group = pool
        .group_builder::<u64>()
        .max_concurrency(2)
        .result_channel(gtx);
    for i in 0..4u64 {
        group.submit(move || async move { i + 100 });
    }
    group.wait().await.unwrap();
    let mut group_results = Vec::new();
    while let Ok(v) = grx.try_recv() {
        group_results.push(v);
    }
    group_results.sort_unstable();
    println!("streamed group results: {group_results:?}");

    // --- 3. Custom channel via the ResultSink interface. ---
    let (ctx, mut crx) = mpsc::unbounded_channel::<u64>();
    let custom = pool.results_to(TokioSink(ctx));
    custom.submit(|| async { 7 });
    println!("custom-sink result: {}", crx.recv().await.unwrap());

    pool.stop_and_wait().await;
}
