//! Throughput benchmarks for the async and blocking pools.
//!
//! Run with: `cargo bench`
#![allow(missing_docs)] // criterion's generated harness items are not documented

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pond::Pool;

const TASK_COUNTS: &[u64] = &[1_000, 10_000];

/// Submit N trivial tasks to the async pool and await all of them.
fn async_submit(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("async_submit");
    for &n in TASK_COUNTS {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let pool = Pool::builder().max_concurrency(8).build();
                    let handles: Vec<_> = (0..n)
                        .map(|i| pool.submit(move || async move { black_box(i) }))
                        .collect();
                    for h in handles {
                        h.await.unwrap();
                    }
                    pool.stop_and_wait().await;
                });
            });
        });
    }
    group.finish();
}

/// Baseline: the same N trivial tasks via raw `tokio::spawn`, no pool. The gap
/// between this and `async_submit` is pond's per-task overhead.
fn tokio_spawn_baseline(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("tokio_spawn_baseline");
    for &n in TASK_COUNTS {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let handles: Vec<_> = (0..n)
                        .map(|i| tokio::spawn(async move { black_box(i) }))
                        .collect();
                    for h in handles {
                        h.await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

/// Per-task submit -> await latency on an already-running pool. Isolates the
/// fixed cost of a single round-trip from the bulk-throughput benches.
fn async_submit_one_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let pool = rt.block_on(async { Pool::builder().max_concurrency(8).build() });

    c.bench_function("async_submit_one_latency", |b| {
        b.iter(|| {
            rt.block_on(async {
                let h = pool.submit(|| async { black_box(1u64) });
                black_box(h.await.unwrap());
            });
        });
    });
}

/// Collect N results through a task group, in submission order.
fn async_group(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("async_group");
    for &n in TASK_COUNTS {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let pool = Pool::builder().max_concurrency(8).build();
                    let mut g = pool.group::<u64>();
                    for i in 0..n {
                        g.submit(move || async move { black_box(i) });
                    }
                    let out = g.wait().await.unwrap();
                    black_box(out.len());
                    pool.stop_and_wait().await;
                });
            });
        });
    }
    group.finish();
}

/// Many producers hammering one shared pool concurrently. This is the scenario
/// where metric-counter contention (submit-side vs completion-side atomics) would
/// show up, so it doubles as the regression bench for the counter layout.
fn async_many_producers(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    const PRODUCERS: u64 = 8;
    const PER_PRODUCER: u64 = 2_000;

    let mut group = c.benchmark_group("async_many_producers");
    group.throughput(Throughput::Elements(PRODUCERS * PER_PRODUCER));
    group.bench_function("8x2000", |b| {
        b.iter(|| {
            rt.block_on(async {
                let pool = Pool::builder().max_concurrency(8).build();
                let producers: Vec<_> = (0..PRODUCERS)
                    .map(|_| {
                        let pool = pool.clone();
                        tokio::spawn(async move {
                            let handles: Vec<_> = (0..PER_PRODUCER)
                                .map(|i| pool.submit(move || async move { black_box(i) }))
                                .collect();
                            for h in handles {
                                h.await.unwrap();
                            }
                        })
                    })
                    .collect();
                for p in producers {
                    p.await.unwrap();
                }
                pool.stop_and_wait().await;
            });
        });
    });
    group.finish();
}

/// Submit N trivial tasks to the blocking pool and wait for all of them.
#[cfg(feature = "blocking")]
fn blocking_submit(c: &mut Criterion) {
    use pond::blocking::Pool as BlockingPool;

    let mut group = c.benchmark_group("blocking_submit");
    for &n in TASK_COUNTS {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let pool = BlockingPool::new(8);
                let handles: Vec<_> = (0..n).map(|i| pool.submit(move || black_box(i))).collect();
                for h in handles {
                    h.wait().unwrap();
                }
                pool.stop_and_wait();
            });
        });
    }
    group.finish();
}

#[cfg(feature = "blocking")]
criterion_group!(
    benches,
    async_submit,
    tokio_spawn_baseline,
    async_submit_one_latency,
    async_group,
    async_many_producers,
    blocking_submit
);
#[cfg(not(feature = "blocking"))]
criterion_group!(
    benches,
    async_submit,
    tokio_spawn_baseline,
    async_submit_one_latency,
    async_group,
    async_many_producers
);
criterion_main!(benches);
