//! Integration tests for the synchronous blocking pool.
#![cfg(feature = "blocking")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use pond::blocking::Pool;
use pond::{Error, QueueSize};

#[test]
fn submit_and_wait_result() {
    let pool = Pool::builder().max_concurrency(4).build();
    let handle = pool.submit(|| 1 + 2);
    assert_eq!(handle.wait().unwrap(), 3);
    pool.stop_and_wait();
}

#[test]
fn runs_many_tasks() {
    let pool = Pool::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..500 {
        let c = counter.clone();
        handles.push(pool.submit(move || c.fetch_add(1, Ordering::SeqCst)));
    }
    for h in handles {
        h.wait().unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 500);
    pool.stop_and_wait();
}

#[test]
fn respects_concurrency_limit() {
    let max = 3;
    let pool = Pool::new(max);
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..30 {
        let current = current.clone();
        let peak = peak.clone();
        handles.push(pool.submit(move || {
            let now = current.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(5));
            current.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    for h in handles {
        h.wait().unwrap();
    }
    assert!(peak.load(Ordering::SeqCst) <= max);
    pool.stop_and_wait();
}

#[test]
fn panic_is_recovered() {
    let pool = Pool::new(1);
    let handle = pool.submit(|| -> i32 { panic!("boom") });
    match handle.wait() {
        Err(Error::Panicked(msg)) => assert!(msg.contains("boom")),
        other => panic!("expected Panicked, got {other:?}"),
    }
    // Pool stays healthy.
    assert_eq!(pool.submit(|| 5).wait().unwrap(), 5);
    pool.stop_and_wait();
}

#[test]
fn submit_after_stop_rejected() {
    let pool = Pool::new(2);
    pool.stop_and_wait();
    assert!(pool.is_stopped());
    assert_eq!(pool.submit(|| 1).wait(), Err(Error::PoolStopped));
}

#[test]
fn try_submit_rejects_when_queue_full() {
    let pool = Pool::builder()
        .max_concurrency(1)
        .queue_size(QueueSize::Bounded(1))
        .build();

    // Occupy the single worker with a barrier it can't pass yet.
    let barrier = Arc::new(Barrier::new(2));
    let b = barrier.clone();
    let _busy = pool.submit(move || {
        b.wait();
    });

    // Give the worker a moment to pick up the busy task.
    std::thread::sleep(Duration::from_millis(20));
    let _queued = pool.try_submit(|| {}).expect("queued ok");

    std::thread::sleep(Duration::from_millis(20));
    let rejected = pool.try_submit(|| {});
    assert_eq!(rejected.err(), Some(Error::QueueFull));

    barrier.wait();
    pool.stop_and_wait();
}

#[test]
fn metrics_track_completion() {
    let pool = Pool::new(2);
    let mut handles = Vec::new();
    for i in 0..10 {
        handles.push(pool.submit(move || i));
    }
    for h in handles {
        h.wait().unwrap();
    }
    pool.stop_and_wait();
    let m = pool.metrics();
    assert_eq!(m.submitted, 10);
    assert_eq!(m.successful, 10);
    assert_eq!(m.completed, 10);
    assert_eq!(m.running_workers, 0);
}

#[test]
fn stop_and_wait_drains_queue() {
    let pool = Pool::new(2);
    let counter = Arc::new(AtomicUsize::new(0));
    for _ in 0..50 {
        let c = counter.clone();
        let _h = pool.submit(move || {
            std::thread::sleep(Duration::from_millis(1));
            c.fetch_add(1, Ordering::SeqCst);
        });
    }
    pool.stop_and_wait();
    assert_eq!(counter.load(Ordering::SeqCst), 50);
}

#[test]
fn pool_clone_shares_state() {
    let pool = Pool::new(2);
    let clone = pool.clone();
    clone.submit(|| {}).wait().unwrap();
    assert_eq!(pool.submitted_tasks(), 1);
    pool.stop_and_wait();
}

#[test]
fn core_max_threads_grow_under_load_then_retire() {
    let pool = Pool::builder()
        .min_concurrency(1)
        .max_concurrency(4)
        .keep_alive(Duration::from_millis(50))
        .build();
    assert_eq!(pool.min_concurrency(), 1);

    let mut handles = Vec::new();
    for _ in 0..4 {
        handles.push(pool.submit(|| std::thread::sleep(Duration::from_millis(100))));
    }
    // While the 4 tasks run, the pool should have grown beyond its core size.
    std::thread::sleep(Duration::from_millis(40));
    let peak = pool.live_workers();
    assert!(peak > 1, "expected growth beyond core, live={peak}");
    assert!(peak <= 4);

    for h in handles {
        h.wait().unwrap();
    }
    // Idle non-core threads retire after the keep-alive window.
    std::thread::sleep(Duration::from_millis(250));
    assert!(
        pool.live_workers() <= 2,
        "expected retirement toward core, live={}",
        pool.live_workers()
    );
    pool.stop_and_wait();
}

#[test]
fn cancel_abandons_queued_tasks() {
    let pool = Pool::new(1);
    let barrier = Arc::new(Barrier::new(2));
    let b = barrier.clone();
    let busy = pool.submit(move || {
        b.wait();
    });
    std::thread::sleep(Duration::from_millis(20));

    let queued: Vec<_> = (0..5).map(|i| pool.submit(move || i)).collect();
    pool.cancel();
    barrier.wait();
    busy.wait().unwrap();

    for h in queued {
        assert_eq!(h.wait(), Err(Error::Canceled));
    }
    assert!(pool.canceled_tasks() >= 1);
}

#[test]
fn group_collects_ordered_results() {
    let pool = Pool::new(4);
    let mut g = pool.group::<usize>();
    for i in 0..10 {
        g.submit(move || i * 2);
    }
    let results = g.wait().unwrap();
    assert_eq!(results, (0..10).map(|i| i * 2).collect::<Vec<_>>());
    pool.stop_and_wait();
}

#[test]
fn try_group_aborts_on_first_error() {
    use pond::GroupError;
    let pool = Pool::new(2);
    let mut g = pool.try_group::<usize, String>();
    for i in 0..10 {
        g.submit(move || {
            if i == 3 {
                Err(format!("bad {i}"))
            } else {
                Ok(i)
            }
        });
    }
    match g.wait() {
        Err(GroupError::Task(msg)) => assert!(msg.contains("bad 3")),
        other => panic!("expected task error, got {other:?}"),
    }
    pool.stop_and_wait();
}
