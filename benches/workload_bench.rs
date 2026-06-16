//! Workload-shaped benchmarks.
//!
//! `pool_bench.rs` measures raw submit/await overhead with trivial tasks. This
//! suite measures the pool under *representative* work:
//!
//! * `cpu_bound`        — each task does real CPU work (a small hash mix),
//! * `concurrency_scaling` — fixed work, varying `max_concurrency`,
//! * `async_yielding`   — tasks with several `.await` points (I/O-like), and
//! * `multi_producer`   — many concurrent submitters contending on one pool.
//!
//! Run with: `cargo bench --bench workload_bench`
#![allow(missing_docs)] // criterion's generated harness items are not documented

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pond::Pool;
use tokio::runtime::Runtime;

/// A small, deterministic CPU workload. Representative of real per-task cost and
/// opaque enough (via `black_box`) that the optimizer can't fold it away.
#[inline]
fn cpu_work(seed: u64) -> u64 {
    let mut h = seed ^ 0xcbf2_9ce4_8422_2325;
    for i in 0..256u64 {
        h = (h ^ black_box(i)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// N tasks, each doing a fixed chunk of CPU work.
fn cpu_bound(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("cpu_bound");
    for &n in &[1_000u64, 10_000] {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let pool = Pool::builder().max_concurrency(8).build();
                    let handles: Vec<_> = (0..n)
                        .map(|i| pool.submit(move || async move { cpu_work(i) }))
                        .collect();
                    let mut acc = 0u64;
                    for h in handles {
                        acc ^= h.await.unwrap();
                    }
                    black_box(acc);
                    pool.stop_and_wait().await;
                });
            });
        });
    }
    group.finish();
}

/// Fixed total work (5,000 CPU tasks) run at different concurrency caps. Shows
/// how throughput scales — and where pool overhead vs. parallelism balances.
fn concurrency_scaling(c: &mut Criterion) {
    let rt = rt();
    const N: u64 = 5_000;
    let mut group = c.benchmark_group("concurrency_scaling");
    group.throughput(Throughput::Elements(N));
    for &conc in &[1usize, 4, 16, 64] {
        group.bench_with_input(BenchmarkId::from_parameter(conc), &conc, |b, &conc| {
            b.iter(|| {
                rt.block_on(async {
                    let pool = Pool::builder().max_concurrency(conc).build();
                    let handles: Vec<_> = (0..N)
                        .map(|i| pool.submit(move || async move { cpu_work(i) }))
                        .collect();
                    let mut acc = 0u64;
                    for h in handles {
                        acc ^= h.await.unwrap();
                    }
                    black_box(acc);
                    pool.stop_and_wait().await;
                });
            });
        });
    }
    group.finish();
}

/// N tasks that each yield to the runtime several times — emulating an async task
/// awaiting I/O (without real wall-clock sleeps). Stresses the executor's poll
/// path through the pool.
fn async_yielding(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("async_yielding");
    for &n in &[1_000u64, 10_000] {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let pool = Pool::builder().max_concurrency(16).build();
                    let handles: Vec<_> = (0..n)
                        .map(|i| {
                            pool.submit(move || async move {
                                for _ in 0..4 {
                                    tokio::task::yield_now().await;
                                }
                                i
                            })
                        })
                        .collect();
                    for h in handles {
                        black_box(h.await.unwrap());
                    }
                    pool.stop_and_wait().await;
                });
            });
        });
    }
    group.finish();
}

/// `P` concurrent producer tasks each submit a share of the total, contending on
/// the pool's shared queue and permit channel.
fn multi_producer(c: &mut Criterion) {
    let rt = rt();
    const TOTAL: u64 = 8_000;
    let mut group = c.benchmark_group("multi_producer");
    group.throughput(Throughput::Elements(TOTAL));
    for &producers in &[2usize, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(producers),
            &producers,
            |b, &producers| {
                let per = TOTAL / producers as u64;
                b.iter(|| {
                    rt.block_on(async {
                        let pool = Pool::builder().max_concurrency(8).build();
                        let done = Arc::new(AtomicUsize::new(0));
                        let tasks: Vec<_> = (0..producers)
                            .map(|_| {
                                let pool = pool.clone();
                                let done = done.clone();
                                tokio::spawn(async move {
                                    let handles: Vec<_> = (0..per)
                                        .map(|i| {
                                            let done = done.clone();
                                            pool.submit(move || async move {
                                                done.fetch_add(1, Ordering::Relaxed);
                                                black_box(i)
                                            })
                                        })
                                        .collect();
                                    for h in handles {
                                        let _ = h.await;
                                    }
                                })
                            })
                            .collect();
                        for t in tasks {
                            t.await.unwrap();
                        }
                        black_box(done.load(Ordering::Relaxed));
                        pool.stop_and_wait().await;
                    });
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    cpu_bound,
    concurrency_scaling,
    async_yielding,
    multi_producer
);
criterion_main!(benches);
