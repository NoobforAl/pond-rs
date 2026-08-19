#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Heap-leak regression test using dhat (a dev-dependency — test-only, never
//! part of the shipped dependency graph).
//!
//! Runs waves of submit/complete work through fresh pools and asserts the live
//! heap returns to (near) baseline afterwards. A per-task leak — a leaked `Arc`,
//! oneshot channel or queued future that outlives its task — shows up as linear
//! growth across waves, far exceeding the slack allowed here.
//!
//! dhat's `Profiler` is process-global, so this file holds exactly one `#[test]`
//! and must stay alone in its own test binary.

// dhat only sees allocations made through its allocator; it forwards to the
// system allocator whenever no profiler is active.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use pond::Pool;

#[test]
fn sustained_workload_does_not_grow_live_heap() {
    let _profiler = dhat::Profiler::builder().testing().build();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let wave = |n: usize| {
        rt.block_on(async {
            let pool = Pool::builder().max_concurrency(8).build();
            let handles: Vec<_> = (0..n)
                .map(|i| pool.submit(move || async move { i.wrapping_mul(2654435761) }))
                .collect();
            for h in handles {
                let _ = h.await;
            }
            pool.stop_and_wait().await;
        });
    };

    // Warm-up wave: populate the runtime's and allocator's caches so they don't
    // pollute the baseline.
    wave(20_000);
    let baseline = dhat::HeapStats::get().curr_bytes;

    // Five more full waves: 100k tasks through five fresh pools.
    for _ in 0..5 {
        wave(20_000);
    }

    let stats = dhat::HeapStats::get();
    let after = stats.curr_bytes;
    println!(
        "dhat heap: baseline={baseline}B after={after}B peak={}B total_alloc={}B",
        stats.max_bytes, stats.total_bytes
    );

    // Any leaked per-task allocation (each at least ~a hundred bytes) would grow
    // the live heap by tens of MB across 100k tasks. Allow 4 MiB of slack for
    // allocator / runtime bookkeeping noise.
    assert!(
        after <= baseline + 4 * 1024 * 1024,
        "live heap grew by {} bytes across 5x20k tasks (baseline {baseline}B, after {after}B) — \
         suspect a per-task leak",
        after.saturating_sub(baseline),
    );
}
