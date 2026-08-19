//! Lifecycle benchmarks: the cost of building a pool, driving a workload
//! through it, draining it and dropping it — the churn pattern of per-request
//! or per-job pools. This exercises setup/teardown paths (permit channel,
//! watcher task, `Inner` teardown) that the throughput benches don't isolate.
//!
//! Run with: `cargo bench --bench lifecycle_bench`
#![allow(missing_docs)] // criterion's generated harness items are not documented

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pond::Pool;

const WORKLOADS: &[u64] = &[0, 64, 1_024];

/// Build -> drain -> drop one short-lived pool per iteration. The `0` case is
/// the pure lifecycle cost (build + watcher spawn + idle wait + teardown).
fn build_drain_drop(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("pool_lifecycle");
    for &n in WORKLOADS {
        group.throughput(Throughput::Elements(n.max(1)));
        group.bench_with_input(BenchmarkId::new("build_drain_drop", n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let pool = Pool::builder().max_concurrency(8).build();
                    for _ in 0..n {
                        pool.spawn(|| async { black_box(1) });
                    }
                    pool.stop_and_wait().await;
                });
            });
        });
    }
    group.finish();
}

/// Same lifecycle, but through subpools: parent built once, each iteration
/// builds/drains/drops a capped subpool. Isolates the second permit gate and
/// subpool watcher costs.
fn subpool_build_drain_drop(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("subpool_lifecycle");
    for &n in WORKLOADS {
        group.throughput(Throughput::Elements(n.max(1)));
        group.bench_with_input(BenchmarkId::new("build_drain_drop", n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let parent = Pool::builder().max_concurrency(8).build();
                    let sub = parent.subpool_builder().max_concurrency(4).build();
                    for _ in 0..n {
                        sub.spawn(|| async { black_box(1) });
                    }
                    sub.stop_and_wait().await;
                    parent.stop_and_wait().await;
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, build_drain_drop, subpool_build_drain_drop);
criterion_main!(benches);
