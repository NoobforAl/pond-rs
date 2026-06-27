//! Heap memory profile of a pool workload, viewable in the browser.
//!
//! Run with: `cargo run --release --example mem_profile --features dhat-heap`
//! Then open the generated `target/dhat-heap.json` in the dh_view web viewer:
//! https://nnethercote.github.io/dh_view/dh_view.html
//!
//! `dhat` replaces the global allocator and records every allocation; the JSON
//! it writes on exit shows total/peak bytes and the call sites responsible.

use pond::Pool;

// dhat hooks the global allocator to track heap usage.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[tokio::main]
async fn main() {
    // Profiling runs for the lifetime of this guard; dropping it writes the JSON.
    let _profiler = dhat::Profiler::builder()
        .file_name("target/dhat-heap.json")
        .build();

    // A representative workload: many short tasks through a bounded pool.
    let pool = Pool::builder().max_concurrency(8).build();
    let mut handles = Vec::new();
    for i in 0..10_000u64 {
        handles.push(pool.submit(move || async move { i * 2 }));
    }
    let mut sum = 0u64;
    for h in handles {
        sum += h.await.unwrap();
    }
    pool.stop_and_wait().await;
    println!("workload done (sum = {sum})");

    // `_profiler` drops here, writing target/dhat-heap.json.
    println!("wrote target/dhat-heap.json");
    println!("view it at: https://nnethercote.github.io/dh_view/dh_view.html");
}
