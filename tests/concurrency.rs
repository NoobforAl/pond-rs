#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Concurrency regression tests for the admission / idle-tracking fixes.

use std::sync::Arc;
use std::time::Duration;

use pond::{Error, Pool};

/// `stop_and_wait` must never return before every queued task has actually run.
///
/// Guards the idle false-positive race: idle was previously derived from
/// `waiting == 0 && running == 0`, which has a window between a task being
/// dequeued (waiting--) and started (running++). The `outstanding` counter closes
/// it. Run many rounds to make any residual window surface.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_and_wait_drains_every_task() {
    const ROUNDS: usize = 300;
    const TASKS: u64 = 64;

    for _ in 0..ROUNDS {
        let pool = Pool::builder().max_concurrency(4).build();
        for _ in 0..TASKS {
            pool.spawn(|| async {
                tokio::task::yield_now().await;
            });
        }
        pool.stop_and_wait().await;
        assert_eq!(
            pool.completed_tasks(),
            TASKS,
            "stop_and_wait returned before all tasks finished"
        );
    }
}

/// A non-blocking bounded queue must never admit more than `capacity` tasks, even
/// under a storm of concurrent producers. Guards the check-then-increment race.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bounded_queue_never_overshoots_under_contention() {
    const CAP: usize = 16;
    const PRODUCERS: usize = 64;

    // One worker, occupied by a gate task, so nothing in the burst can run and
    // free a slot — the queue fills to exactly CAP and the rest are dropped.
    let pool = Pool::builder()
        .max_concurrency(1)
        .queue_size(CAP)
        .non_blocking(true)
        .build();

    let (started_tx, started_rx) = async_channel::bounded::<()>(1);
    let (release_tx, release_rx) = async_channel::bounded::<()>(1);
    pool.spawn(move || async move {
        let _ = started_tx.send(()).await;
        let _ = release_rx.recv().await; // hold the only worker
    });
    started_rx.recv().await.unwrap(); // gate is running; queue is empty

    // Hammer the bounded queue from many tasks at once.
    let pool = Arc::new(pool);
    let mut producers = Vec::new();
    for _ in 0..PRODUCERS {
        let pool = pool.clone();
        producers.push(tokio::spawn(async move {
            let mut accepted = 0u64;
            for _ in 0..32 {
                match pool.try_submit(|| async {}) {
                    Ok(_) => accepted += 1,
                    Err(Error::QueueFull) => {}
                    Err(e) => panic!("unexpected error: {e}"),
                }
            }
            accepted
        }));
    }

    let mut total_accepted = 0u64;
    for p in producers {
        total_accepted += p.await.unwrap();
    }

    assert_eq!(
        total_accepted, CAP as u64,
        "bounded queue admitted more than its capacity"
    );
    assert!(
        pool.waiting_tasks() <= CAP as u64,
        "waiting exceeded capacity"
    );

    let _ = release_tx.send(()).await;
    tokio::time::timeout(Duration::from_secs(5), pool.stop_and_wait())
        .await
        .expect("pool should drain");
}
