#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Randomized concurrency stress tests.
//!
//! Deterministic (fixed seeds, no `rand` dependency): each round drives many
//! producers through interleaved submit / resize / cancel / stop and then asserts
//! the pool quiesces with no hang, no panic escaping, and consistent metrics.

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

/// Submit + resize churn, then graceful stop. Every admitted task must reach a
/// terminal state (no leaked `outstanding`), and `stop_and_wait` must not hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn submit_and_resize_then_stop_quiesces() {
    for seed in 0..16u64 {
        let pool = Pool::builder()
            .max_concurrency(4)
            .min_concurrency(1)
            .build();

        let mut producers = Vec::new();
        for p in 0..6u64 {
            let pool = pool.clone();
            producers.push(tokio::spawn(async move {
                let mut s = mix(seed ^ p.wrapping_mul(0x9E37_79B9_7F4A_7C15));
                for _ in 0..200 {
                    s = mix(s);
                    match s % 8 {
                        0 => pool.resize((s as usize % 8) + 1),
                        1 => pool.spawn(|| async { panic!("boom") }),
                        _ => pool.spawn(|| async { tokio::task::yield_now().await }),
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

        let m = pool.metrics();
        // No producer submitted after stop and nothing was dropped/cancelled, so
        // every submission ran to success or panic.
        assert_eq!(m.waiting, 0, "queued tasks leaked (seed {seed})");
        assert_eq!(m.running_workers, 0, "running tasks leaked (seed {seed})");
        assert_eq!(
            m.submitted,
            m.successful + m.failed,
            "task accounting mismatch (seed {seed})"
        );
        assert_eq!(m.completed, m.successful + m.failed);
    }
}

/// Cancellation racing a submit storm must not hang or leak outstanding tasks,
/// and terminal counters must never exceed submissions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_load_quiesces() {
    for seed in 0..16u64 {
        let pool = Pool::builder().max_concurrency(2).build();

        let mut producers = Vec::new();
        for _ in 0..4 {
            let pool = pool.clone();
            producers.push(tokio::spawn(async move {
                for _ in 0..150 {
                    pool.spawn(|| async { tokio::task::yield_now().await });
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

        let m = pool.metrics();
        assert_eq!(m.waiting, 0, "queued tasks leaked (seed {seed})");
        assert_eq!(m.running_workers, 0, "running tasks leaked (seed {seed})");
        assert!(
            m.successful + m.failed + m.canceled + m.dropped <= m.submitted,
            "terminal counts exceeded submissions (seed {seed})"
        );
    }
}
