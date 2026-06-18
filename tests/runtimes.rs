//! End-to-end checks that the runtime-agnostic [`Pool`] runs on the non-Tokio
//! runtime adapters. Each runtime is driven by its own executor threads, so the
//! test thread can await results with `futures::executor::block_on`.
//!
//! Gated on the optional runtime features; with neither enabled this file is
//! empty and compiles away.
#![cfg(any(feature = "runtime-smol", feature = "runtime-async-std"))]

use std::time::Duration;

#[cfg(feature = "runtime-smol")]
#[test]
fn smol_pool_runs_tasks() {
    use pond::{Pool, SmolSpawner};

    let pool = Pool::builder_with(SmolSpawner::new())
        .max_concurrency(4)
        .build();
    let got = futures::executor::block_on(async { pool.submit(|| async { 2 + 3 }).await.unwrap() });
    assert_eq!(got, 5);
}

#[cfg(feature = "runtime-smol")]
#[test]
fn smol_pool_deadline_stops_via_sleep_default() {
    use pond::{Pool, SmolSpawner};

    // Exercises the generic `Spawner::sleep` default (blocking-thread fallback):
    // the deadline must shut the pool down even with no native timer.
    let pool = Pool::builder_with(SmolSpawner::new())
        .deadline(Duration::from_millis(50))
        .build();
    std::thread::sleep(Duration::from_millis(300));
    assert!(pool.is_stopped(), "deadline should have stopped the pool");
}

#[cfg(feature = "runtime-async-std")]
#[test]
fn async_std_pool_runs_tasks() {
    use pond::{AsyncStdSpawner, Pool};

    let pool = Pool::builder_with(AsyncStdSpawner::new())
        .max_concurrency(4)
        .build();
    let got =
        futures::executor::block_on(async { pool.submit(|| async { 10 + 1 }).await.unwrap() });
    assert_eq!(got, 11);
}

#[cfg(feature = "runtime-async-std")]
#[test]
fn async_std_pool_deadline_stops_via_sleep_default() {
    use pond::{AsyncStdSpawner, Pool};

    let pool = Pool::builder_with(AsyncStdSpawner::new())
        .deadline(Duration::from_millis(50))
        .build();
    std::thread::sleep(Duration::from_millis(300));
    assert!(pool.is_stopped(), "deadline should have stopped the pool");
}
