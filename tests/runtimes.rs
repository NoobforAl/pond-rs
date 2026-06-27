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
fn smol_pool_deadline_stops_via_native_timer() {
    use pond::{Pool, SmolSpawner};

    // The deadline is driven by the smol adapter's `async_io::Timer`-backed
    // `Spawner::sleep` (no blocking-thread fallback).
    let pool = Pool::builder_with(SmolSpawner::new())
        .deadline(Duration::from_millis(50))
        .build();
    std::thread::sleep(Duration::from_millis(300));
    assert!(pool.is_stopped(), "deadline should have stopped the pool");
}

#[cfg(feature = "runtime-smol")]
#[test]
fn smol_schedule_every_fires_repeatedly() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use pond::{Pool, SmolSpawner};

    // Each tick uses the native timer; previously every tick spawned a blocking
    // OS thread for the sleep. Just assert the schedule actually fires several
    // times and stops cleanly.
    let pool = Pool::builder_with(SmolSpawner::new()).build();
    let ticks = Arc::new(AtomicU32::new(0));
    let t = ticks.clone();
    let sched = pool.schedule_every(Duration::from_millis(20), move || {
        let t = t.clone();
        async move {
            t.fetch_add(1, Ordering::Relaxed);
        }
    });
    std::thread::sleep(Duration::from_millis(250));
    sched.stop();
    assert!(
        ticks.load(Ordering::Relaxed) >= 3,
        "recurring schedule should fire several times"
    );
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
fn async_std_pool_deadline_stops_via_native_timer() {
    use pond::{AsyncStdSpawner, Pool};

    // Driven by `async_std::task::sleep` via the adapter's `Spawner::sleep`.
    let pool = Pool::builder_with(AsyncStdSpawner::new())
        .deadline(Duration::from_millis(50))
        .build();
    std::thread::sleep(Duration::from_millis(300));
    assert!(pool.is_stopped(), "deadline should have stopped the pool");
}
