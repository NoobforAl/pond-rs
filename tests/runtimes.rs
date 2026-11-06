//! End-to-end checks that the runtime-agnostic [`Pool`] runs on the non-Tokio
//! runtime adapters. Each runtime is driven by its own executor threads, so the
//! test thread can await results with `futures::executor::block_on`.
//!
//! Gated on the optional runtime features; with neither enabled this file is
//! empty and compiles away.
#![cfg(any(feature = "runtime-smol", feature = "runtime-async-std"))]

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use pond::{Error, Pool, Spawner};

/// `block_on` on a separate thread, so a hang fails the test instead of wedging it.
fn finishes<T: Send + 'static>(fut: impl Future<Output = T> + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(futures::executor::block_on(fut));
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("timed out: this should have finished")
}

/// Polls `f` until it holds or patience runs out.
fn eventually(f: impl Fn() -> bool) -> bool {
    for _ in 0..1000 {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    f()
}

/// The concurrency and shutdown behavior that isn't Tokio-specific, run against
/// whichever runtime adapter is passed in.
fn lifecycle_checks<S: Spawner>(spawner: S) {
    resize_releases_queued_tasks(spawner.clone());
    resize_round_trip_keeps_the_limit(spawner.clone());
    panic_before_the_future_is_returned(spawner.clone());
    subpool_settles_when_the_parent_is_stopped_or_cancelled(spawner.clone());
    queued_subpool_tasks_do_not_starve_the_parent(spawner.clone());
    delayed_work_resolves_when_the_pool_stops(spawner.clone());
    try_group_failure_cancels_queued_tasks(spawner);
}

fn resize_releases_queued_tasks<S: Spawner>(spawner: S) {
    let pool = Pool::builder_with(spawner).max_concurrency(1).build();
    let (open, gate) = async_channel::unbounded::<()>();
    let started = Arc::new(AtomicUsize::new(0));
    for _ in 0..4 {
        let (gate, s) = (gate.clone(), started.clone());
        pool.spawn(move || async move {
            s.fetch_add(1, Ordering::SeqCst);
            let _ = gate.recv().await;
        });
    }
    std::thread::sleep(Duration::from_millis(100));
    pool.resize(0);
    assert!(eventually(|| started.load(Ordering::SeqCst) == 4));
    drop(open);
    finishes(async move { pool.stop_and_wait().await });
}

fn resize_round_trip_keeps_the_limit<S: Spawner>(spawner: S) {
    let pool = Pool::builder_with(spawner.clone())
        .max_concurrency(2)
        .build();
    pool.resize(0);
    pool.resize(2);
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    for _ in 0..20 {
        let (c, p, spawner) = (current.clone(), peak.clone(), spawner.clone());
        pool.spawn(move || async move {
            p.fetch_max(c.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
            spawner.sleep(Duration::from_millis(5)).await;
            c.fetch_sub(1, Ordering::SeqCst);
        });
    }
    finishes(async move { pool.stop_and_wait().await });
    assert!(peak.load(Ordering::SeqCst) <= 2);
}

fn panic_before_the_future_is_returned<S: Spawner>(spawner: S) {
    let pool = Pool::builder_with(spawner).build();
    let sub = pool.subpool(1);
    let on_pool = pool.submit(|| -> std::future::Ready<i32> { panic!("sync part") });
    let on_sub = sub.submit(|| -> std::future::Ready<i32> { panic!("sync part") });
    finishes(async move {
        assert!(matches!(on_pool.await, Err(Error::Panicked(_))));
        assert!(matches!(on_sub.await, Err(Error::Panicked(_))));
        sub.stop_and_wait().await;
    });
}

fn subpool_settles_when_the_parent_is_stopped_or_cancelled<S: Spawner>(spawner: S) {
    let parent = Pool::builder_with(spawner.clone()).build();
    let sub = parent.subpool(2);
    finishes(async move {
        parent.stop_and_wait().await;
        let _ = sub.submit(|| async { 1 }).await;
        sub.stop_and_wait().await;
    });

    let parent = Pool::builder_with(spawner).max_concurrency(1).build();
    let sub = parent.subpool(1);
    let (open, gate) = async_channel::unbounded::<()>();
    for _ in 0..4 {
        let gate = gate.clone();
        sub.spawn(move || async move {
            let _ = gate.recv().await;
        });
    }
    std::thread::sleep(Duration::from_millis(50));
    parent.cancel();
    drop(open);
    finishes(async move { sub.stop_and_wait().await });
}

fn queued_subpool_tasks_do_not_starve_the_parent<S: Spawner>(spawner: S) {
    let parent = Pool::builder_with(spawner).max_concurrency(2).build();
    let sub = parent.subpool(1);
    let (open, gate) = async_channel::unbounded::<()>();
    for _ in 0..4 {
        let gate = gate.clone();
        sub.spawn(move || async move {
            let _ = gate.recv().await;
        });
    }
    std::thread::sleep(Duration::from_millis(100));
    let handle = parent.submit(|| async { 42 });
    assert_eq!(finishes(handle), Ok(42));
    drop(open);
}

fn delayed_work_resolves_when_the_pool_stops<S: Spawner>(spawner: S) {
    let pool = Pool::builder_with(spawner).build();
    let handle = pool.submit_after(Duration::from_secs(600), || async { 1 });
    pool.cancel();
    assert_eq!(finishes(handle), Err(Error::Canceled));
}

fn try_group_failure_cancels_queued_tasks<S: Spawner>(spawner: S) {
    let pool = Pool::builder_with(spawner).max_concurrency(2).build();
    let started = Arc::new(AtomicUsize::new(0));
    let ran = Arc::new(AtomicUsize::new(0));
    let (open_slow, slow_gate) = async_channel::unbounded::<()>();
    let (open_fail, fail_gate) = async_channel::unbounded::<()>();
    let mut group = pool.try_group::<i32, String>();

    // Both workers are taken: an earlier task, and the one that will fail.
    let s = started.clone();
    group.submit(move || async move {
        s.fetch_add(1, Ordering::SeqCst);
        let _ = slow_gate.recv().await;
        Ok(0)
    });
    let s = started.clone();
    group.submit(move || async move {
        s.fetch_add(1, Ordering::SeqCst);
        let _ = fail_gate.recv().await;
        Err("boom".to_string())
    });
    assert!(eventually(|| started.load(Ordering::SeqCst) == 2));
    for _ in 0..10 {
        let r = ran.clone();
        group.submit(move || async move {
            r.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        });
    }

    // The failure must cancel the group by itself, with `wait` not yet running.
    let token = group.cancel_token();
    drop(open_fail);
    assert!(eventually(|| token.is_cancelled()));
    drop(open_slow);

    assert!(finishes(group.wait()).is_err());
    assert_eq!(ran.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "runtime-smol")]
#[test]
fn smol_lifecycle() {
    lifecycle_checks(pond::SmolSpawner::new());
}

#[cfg(feature = "runtime-async-std")]
#[test]
fn async_std_lifecycle() {
    lifecycle_checks(pond::AsyncStdSpawner::new());
}

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
