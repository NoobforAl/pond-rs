#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Heap *profiling* harness (not an assertion test).
//!
//! Unlike `heap.rs` — which asserts no growth — this writes a full dhat
//! allocation profile to `target/dhat-heap.json`, viewable in
//! <https://nnethercote.github.io/dh_view/dh_view.html>. It is `#[ignore]`d so
//! it never runs in CI; invoke it explicitly:
//!
//! ```text
//! cargo test --test heap_profile --features runtime-tokio -- --ignored --nocapture
//! ```
//!
//! dhat's profiler is process-global, so this file holds exactly one test.

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use pond::Pool;

const TASKS: usize = 20_000;

#[test]
#[ignore = "profiling harness — run explicitly with --ignored"]
fn profile_submit_await_workload() {
    let _profiler = dhat::Profiler::builder()
        .file_name("target/dhat-heap.json")
        .build();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let pool = Pool::builder().max_concurrency(8).build();
        let handles: Vec<_> = (0..TASKS)
            .map(|i| pool.submit(move || async move { i.wrapping_mul(2654435761) }))
            .collect();
        for h in handles {
            let _ = h.await;
        }
        pool.stop_and_wait().await;
    });

    let s = dhat::HeapStats::get();
    println!(
        "tasks={TASKS} total_blocks={} total_bytes={} peak_bytes={} \
         bytes_per_task={:.1} blocks_per_task={:.2}",
        s.total_blocks,
        s.total_bytes,
        s.max_bytes,
        s.total_bytes as f64 / TASKS as f64,
        s.total_blocks as f64 / TASKS as f64,
    );
    println!("profile written to target/dhat-heap.json");
}
