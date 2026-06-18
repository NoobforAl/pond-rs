//! Runtime-free unit tests for `Pool` logic, driven by the [`MockSpawner`] test
//! double instead of Tokio. These assert the synchronous bookkeeping that runs
//! on the submit/stop/resize paths — no task scheduling races involved.

mod common;
use common::MockSpawner;

use pond::Pool;

#[test]
fn builder_with_uses_provided_spawner() {
    let spawner = MockSpawner::new();
    let _pool = Pool::builder_with(spawner.clone())
        .max_concurrency(2)
        .build();
    // Building spawns the internal cancel-watch task on our spawner.
    assert!(spawner.spawn_count() >= 1);
}

#[test]
fn submit_counts_are_synchronous_and_runtime_free() {
    let spawner = MockSpawner::new();
    let pool = Pool::builder_with(spawner.clone())
        .max_concurrency(4)
        .build();
    let spawns_after_build = spawner.spawn_count();

    // A task that blocks forever so it can't complete and perturb counts.
    for _ in 0..5 {
        pool.spawn(|| async {
            std::future::pending::<()>().await;
        });
    }

    // `submitted` and `spawn` both happen synchronously inside submit/spawn.
    assert_eq!(pool.metrics().submitted, 5);
    assert_eq!(spawner.spawn_count(), spawns_after_build + 5);
}

#[test]
fn cancel_marks_pool_stopped() {
    let pool = Pool::builder_with(MockSpawner::new())
        .max_concurrency(2)
        .build();
    assert!(!pool.is_stopped());
    pool.cancel();
    assert!(pool.is_stopped());
}

#[test]
fn resize_updates_max_concurrency() {
    let pool = Pool::builder_with(MockSpawner::new())
        .max_concurrency(2)
        .build();
    assert_eq!(pool.max_concurrency(), 2);
    pool.resize(10);
    assert_eq!(pool.max_concurrency(), 10);
}
