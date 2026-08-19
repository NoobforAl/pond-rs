#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Randomized concurrency stress tests.
//!
//! Deterministic (fixed seeds, no `rand` dependency): each round drives many
//! producers through interleaved submit / resize / cancel / stop and then asserts
//! the pool quiesces with no hang, no panic escaping, and every submitted task
//! accounted for (tracked with shared atomics).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::Pool;
use tokio::time::timeout;

/// splitmix64 — a tiny deterministic PRNG so rounds are reproducible.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Submit + resize churn, then graceful stop. Every submitted task must have
/// started by the time `stop_and_wait` returns (no leaked `outstanding`, which
/// would hang the drain instead).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn submit_and_resize_then_stop_quiesces() {
    for seed in 0..16u64 {
        let pool = Pool::builder()
            .max_concurrency(4)
            .min_concurrency(1)
            .build();

        // `submitted` bumps on submission; `started` bumps first thing in each
        // task body (before any panic), so started == submitted after the drain
        // proves every task reached a terminal state.
        let submitted = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));

        let mut producers = Vec::new();
        for p in 0..6u64 {
            let pool = pool.clone();
            let submitted = submitted.clone();
            let started = started.clone();
            producers.push(tokio::spawn(async move {
                let mut s = mix(seed ^ p.wrapping_mul(0x9E37_79B9_7F4A_7C15));
                for _ in 0..200 {
                    s = mix(s);
                    match s % 8 {
                        0 => pool.resize((s as usize % 8) + 1),
                        1 => {
                            submitted.fetch_add(1, Ordering::SeqCst);
                            let started = started.clone();
                            pool.spawn(move || async move {
                                started.fetch_add(1, Ordering::SeqCst);
                                panic!("boom")
                            });
                        }
                        _ => {
                            submitted.fetch_add(1, Ordering::SeqCst);
                            let started = started.clone();
                            pool.spawn(move || async move {
                                started.fetch_add(1, Ordering::SeqCst);
                                tokio::task::yield_now().await;
                            });
                        }
                    }
                }
            }));
        }
        for p in producers {
            p.await.unwrap();
        }

        timeout(Duration::from_secs(10), pool.stop_and_wait())
            .await
            .expect("stop_and_wait hung");

        assert_eq!(
            started.load(Ordering::SeqCst),
            submitted.load(Ordering::SeqCst),
            "task accounting mismatch (seed {seed})"
        );
    }
}

/// Cancellation racing a submit storm must not hang, and no task may run that
/// was never submitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_load_quiesces() {
    for seed in 0..16u64 {
        let pool = Pool::builder().max_concurrency(2).build();

        let submitted = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));

        let mut producers = Vec::new();
        for _ in 0..4 {
            let pool = pool.clone();
            let submitted = submitted.clone();
            let started = started.clone();
            producers.push(tokio::spawn(async move {
                for _ in 0..150 {
                    submitted.fetch_add(1, Ordering::SeqCst);
                    let started = started.clone();
                    pool.spawn(move || async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                    });
                }
            }));
        }

        let cpool = pool.clone();
        let canceller = tokio::spawn(async move {
            for _ in 0..(seed % 5) {
                tokio::task::yield_now().await;
            }
            cpool.cancel();
        });

        for p in producers {
            p.await.unwrap();
        }
        canceller.await.unwrap();

        timeout(Duration::from_secs(10), pool.stop_and_wait())
            .await
            .expect("stop_and_wait hung after cancel");

        assert!(
            started.load(Ordering::SeqCst) <= submitted.load(Ordering::SeqCst),
            "started tasks exceeded submissions (seed {seed})"
        );
    }
}
