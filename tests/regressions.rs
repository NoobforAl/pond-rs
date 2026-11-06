#![allow(missing_docs)]
#![cfg(all(feature = "runtime-tokio", feature = "blocking"))]
//! Regressions for concurrency bugs: each test pins behavior the docs promise and
//! the pool once got wrong, mostly by hanging. Timeouts here are generous because
//! they only ever elapse when a test is about to fail.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use pond::{Error, GroupError, Pool, RetryPolicy};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};

type BlockingPool = pond::blocking::Pool;

const PATIENCE: Duration = Duration::from_secs(10);
const SETTLE: Duration = Duration::from_millis(100);

/// A gate tasks wait on until the test calls `close()`.
fn gate() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(0))
}

async fn wait_at_gate(g: &Semaphore) {
    let _ = g.acquire().await;
}

/// Polls `f` until it holds or patience runs out.
fn eventually(f: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    f()
}

/// Async [`eventually`], yielding to the runtime between polls.
async fn eventually_async(f: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        sleep(Duration::from_millis(5)).await;
    }
    f()
}

/// Runs `f` on its own thread, returning `None` if it doesn't finish in time.
fn with_watchdog<T: Send + 'static>(
    patience: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(patience).ok()
}

/// A task that tracks how many instances run at once.
fn tracked(
    current: &Arc<AtomicUsize>,
    peak: &Arc<AtomicUsize>,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    let (current, peak) = (current.clone(), peak.clone());
    async move {
        peak.fetch_max(current.fetch_add(1, SeqCst) + 1, SeqCst);
        sleep(Duration::from_millis(10)).await;
        current.fetch_sub(1, SeqCst);
    }
}

// --- resize ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn resize_to_zero_releases_every_queued_task() {
    let pool = Pool::builder().max_concurrency(1).build();
    let g = gate();
    let started = Arc::new(AtomicUsize::new(0));
    for _ in 0..6 {
        let (g, s) = (g.clone(), started.clone());
        pool.spawn(move || async move {
            s.fetch_add(1, SeqCst);
            wait_at_gate(&g).await;
        });
    }
    sleep(SETTLE).await;
    assert_eq!(started.load(SeqCst), 1);

    pool.resize(0);
    let all_started = eventually_async(|| started.load(SeqCst) == 6).await;
    g.close();
    assert!(all_started, "only {} of 6 started", started.load(SeqCst));
}

#[tokio::test(flavor = "multi_thread")]
async fn subpool_resize_to_zero_releases_every_queued_task() {
    let pool = Pool::builder().build();
    let sub = pool.subpool(1);
    let g = gate();
    let started = Arc::new(AtomicUsize::new(0));
    for _ in 0..6 {
        let (g, s) = (g.clone(), started.clone());
        sub.spawn(move || async move {
            s.fetch_add(1, SeqCst);
            wait_at_gate(&g).await;
        });
    }
    sleep(SETTLE).await;
    sub.resize(0);
    let all_started = eventually_async(|| started.load(SeqCst) == 6).await;
    g.close();
    assert!(all_started, "only {} of 6 started", started.load(SeqCst));
}

#[tokio::test(flavor = "multi_thread")]
async fn resize_round_trip_keeps_the_limit() {
    let pool = Pool::builder().max_concurrency(2).build();
    pool.resize(0);
    pool.resize(2);

    let (current, peak) = Default::default();
    let handles: Vec<_> = (0..30)
        .map(|_| {
            let task = tracked(&current, &peak);
            pool.submit(move || task)
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    assert!(peak.load(SeqCst) <= 2, "peak {}", peak.load(SeqCst));
}

#[tokio::test(flavor = "multi_thread")]
async fn lowering_the_limit_counts_tasks_started_while_unbounded() {
    let pool = Pool::builder().build();
    let g = gate();
    for _ in 0..4 {
        let g = g.clone();
        pool.spawn(move || async move { wait_at_gate(&g).await });
    }
    sleep(SETTLE).await;
    pool.resize(1);

    let ran = Arc::new(AtomicBool::new(false));
    let r = ran.clone();
    let late = pool.submit(move || async move { r.store(true, SeqCst) });
    sleep(SETTLE).await;
    assert!(
        !ran.load(SeqCst),
        "4 are still running against a limit of 1"
    );

    g.close();
    timeout(PATIENCE, late).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_resizes_settle_on_the_last_limit() {
    let pool = Pool::builder().max_concurrency(4).build();
    let resizers: Vec<_> = (0..8)
        .map(|i| {
            let pool = pool.clone();
            tokio::spawn(async move {
                for n in 0..50 {
                    pool.resize(1 + (i + n) % 8);
                    tokio::task::yield_now().await;
                }
            })
        })
        .collect();
    for r in resizers {
        r.await.unwrap();
    }
    pool.resize(3);

    let (current, peak) = Default::default();
    let handles: Vec<_> = (0..30)
        .map(|_| {
            let task = tracked(&current, &peak);
            pool.submit(move || task)
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    assert!(peak.load(SeqCst) <= 3, "peak {}", peak.load(SeqCst));
}

// --- panics ------------------------------------------------------------------

#[tokio::test]
async fn panic_before_the_future_is_returned_counts_as_a_panic() {
    let pool = Pool::builder().build();
    let result = pool
        .submit(|| -> std::future::Ready<i32> { panic!("sync part") })
        .await;
    assert!(matches!(result, Err(Error::Panicked(_))), "got {result:?}");
}

#[tokio::test]
async fn subpool_survives_a_panic_before_the_future_is_returned() {
    let pool = Pool::builder().build();
    let sub = pool.subpool(2);
    let result = sub
        .submit(|| -> std::future::Ready<i32> { panic!("sync part") })
        .await;
    assert!(matches!(result, Err(Error::Panicked(_))), "got {result:?}");
    timeout(PATIENCE, sub.stop_and_wait()).await.unwrap();
}

// --- background watcher ------------------------------------------------------

#[tokio::test]
async fn pool_dropped_before_its_watcher_runs_leaves_no_task() {
    let rt = tokio::runtime::Handle::current();
    let before = rt.metrics().num_alive_tasks();
    for _ in 0..100 {
        drop(Pool::builder().build());
    }
    sleep(SETTLE).await;
    assert_eq!(rt.metrics().num_alive_tasks(), before);
}

#[tokio::test]
async fn subpool_dropped_before_its_watcher_runs_leaves_no_task() {
    let pool = Pool::builder().build();
    tokio::task::yield_now().await;
    let rt = tokio::runtime::Handle::current();
    let before = rt.metrics().num_alive_tasks();
    for _ in 0..100 {
        drop(pool.subpool(1));
    }
    sleep(SETTLE).await;
    assert_eq!(rt.metrics().num_alive_tasks(), before);
}

// --- subpool / parent --------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn queued_subpool_tasks_do_not_starve_the_parent() {
    let parent = Pool::builder().max_concurrency(2).build();
    let sub = parent.subpool(1);
    let g = gate();
    for _ in 0..10 {
        let g = g.clone();
        sub.spawn(move || async move { wait_at_gate(&g).await });
    }
    sleep(SETTLE).await;
    let result = timeout(PATIENCE, parent.submit(|| async { 42 })).await;
    g.close();
    assert_eq!(result.expect("parent task never ran").unwrap(), 42);
}

#[tokio::test]
async fn subpool_on_a_stopped_parent_rejects_and_can_still_stop() {
    let parent = Pool::builder().build();
    let sub = parent.subpool(2);
    parent.stop_and_wait().await;
    assert!(sub.is_stopped());
    assert!(matches!(
        sub.submit(|| async { 1 }).await,
        Err(Error::PoolStopped)
    ));
    assert!(matches!(
        sub.try_submit(|| async { 1 }),
        Err(Error::PoolStopped)
    ));
    timeout(PATIENCE, sub.stop_and_wait()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_parent_settles_queued_subpool_tasks() {
    let parent = Pool::builder().max_concurrency(1).build();
    let sub = parent.subpool(1);
    let g = gate();
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let g = g.clone();
            sub.submit(move || async move { wait_at_gate(&g).await })
        })
        .collect();
    sleep(SETTLE).await;
    parent.cancel();
    g.close();

    timeout(PATIENCE, sub.stop_and_wait()).await.unwrap();
    let mut results = Vec::new();
    for h in handles {
        results.push(timeout(PATIENCE, h).await.unwrap());
    }
    // One was already running and finishes; the rest are abandoned with the cause.
    let finished = results.iter().filter(|r| r.is_ok()).count();
    let abandoned = results
        .iter()
        .filter(|r| **r == Err(Error::Canceled))
        .count();
    assert_eq!((finished, abandoned), (1, 4), "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_the_parent_drains_queued_subpool_tasks() {
    let parent = Pool::builder().max_concurrency(1).build();
    let sub = parent.subpool(1);
    let done = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let d = done.clone();
            sub.submit(move || async move {
                sleep(Duration::from_millis(10)).await;
                d.fetch_add(1, SeqCst);
            })
        })
        .collect();

    timeout(PATIENCE, parent.stop_and_wait()).await.unwrap();
    assert_eq!(done.load(SeqCst), 5);
    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn subpool_task_waits_for_room_in_a_full_parent_queue() {
    let parent = Pool::builder().max_concurrency(1).queue_size(1).build();
    let g = gate();
    let busy = g.clone();
    let _running = parent.submit(move || async move { wait_at_gate(&busy).await });
    let _queued = parent.submit(|| async {});
    sleep(SETTLE).await;

    let sub = parent.subpool(1);
    let handle = sub.submit(|| async { 7 });
    sleep(SETTLE).await;
    g.close();

    assert_eq!(timeout(PATIENCE, handle).await.unwrap().unwrap(), 7);
    timeout(PATIENCE, sub.stop_and_wait()).await.unwrap();
}

#[test]
fn subpool_tasks_dropped_with_their_runtime_are_settled() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (parent, sub, handle) = rt.block_on(async {
        let parent = Pool::builder().build();
        let sub = parent.subpool(1);
        sub.spawn(std::future::pending::<()>);
        sleep(SETTLE).await; // it has the permit, so the next task must queue
        let handle = sub.submit(|| async { 1 });
        sleep(SETTLE).await;
        (parent, sub, handle)
    });
    rt.shutdown_timeout(Duration::from_secs(1));

    let other = tokio::runtime::Runtime::new().unwrap();
    other.block_on(async {
        timeout(PATIENCE, sub.stop_and_wait()).await.unwrap();
        timeout(PATIENCE, parent.stop_and_wait()).await.unwrap();
        assert!(timeout(PATIENCE, handle).await.unwrap().is_err());
    });
}

// --- groups ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn try_group_failure_cancels_queued_tasks_right_away() {
    let pool = Pool::builder().max_concurrency(2).build();
    let (started, ran) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (slow_gate, fail_gate) = (gate(), gate());
    let mut group = pool.try_group::<i32, String>();

    // Both workers are taken: an earlier task, and the one that will fail.
    let (s, g) = (started.clone(), slow_gate.clone());
    group.submit(move || async move {
        s.fetch_add(1, SeqCst);
        wait_at_gate(&g).await;
        Ok(0)
    });
    let (s, g) = (started.clone(), fail_gate.clone());
    group.submit(move || async move {
        s.fetch_add(1, SeqCst);
        wait_at_gate(&g).await;
        Err("boom".to_string())
    });
    assert!(eventually_async(|| started.load(SeqCst) == 2).await);
    for _ in 0..20 {
        let r = ran.clone();
        group.submit(move || async move {
            r.fetch_add(1, SeqCst);
            Ok(1)
        });
    }

    // The failure must cancel the group by itself: `wait` isn't running, and
    // would be stuck on the earlier task if it were.
    let token = group.cancel_token();
    fail_gate.close();
    assert!(eventually_async(|| token.is_cancelled()).await);
    slow_gate.close();

    assert!(matches!(group.wait().await, Err(GroupError::Task(e)) if e == "boom"));
    assert_eq!(ran.load(SeqCst), 0, "queued tasks ran after the failure");
}

#[tokio::test(flavor = "multi_thread")]
async fn group_panic_cancels_queued_tasks_right_away() {
    let pool = Pool::builder().max_concurrency(2).build();
    let (started, ran) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (slow_gate, panic_gate) = (gate(), gate());
    let mut group = pool.group::<i32>();

    let (s, g) = (started.clone(), slow_gate.clone());
    group.submit(move || async move {
        s.fetch_add(1, SeqCst);
        wait_at_gate(&g).await;
        0
    });
    let (s, g) = (started.clone(), panic_gate.clone());
    group.submit(move || async move {
        s.fetch_add(1, SeqCst);
        wait_at_gate(&g).await;
        panic!("boom");
    });
    assert!(eventually_async(|| started.load(SeqCst) == 2).await);
    for _ in 0..20 {
        let r = ran.clone();
        group.submit(move || async move {
            r.fetch_add(1, SeqCst);
            1
        });
    }

    let token = group.cancel_token();
    panic_gate.close();
    assert!(eventually_async(|| token.is_cancelled()).await);
    slow_gate.close();

    assert!(matches!(group.wait().await, Err(Error::Panicked(_))));
    assert_eq!(ran.load(SeqCst), 0, "queued tasks ran after the panic");
}

#[tokio::test]
async fn cancelled_groups_report_it_instead_of_a_partial_result() {
    let pool = Pool::builder().build();

    let mut group = pool.group::<i32>();
    group.submit(|| async { 1 });
    group.cancel();
    assert_eq!(group.wait().await, Err(Error::Canceled));

    let mut group = pool.try_group::<i32, String>();
    group.submit(|| async { Ok(1) });
    group.cancel();
    assert_eq!(group.wait().await, Err(GroupError::Pool(Error::Canceled)),);
}

#[test]
fn blocking_try_group_failure_cancels_queued_tasks_right_away() {
    let pool = BlockingPool::new(2);
    let (started, ran) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (slow_gate, fail_gate) = (BlockingGate::default(), BlockingGate::default());
    let mut group = pool.try_group::<i32, String>();

    let (s, g) = (started.clone(), slow_gate.clone());
    group.submit(move || {
        s.fetch_add(1, SeqCst);
        g.wait();
        Ok(0)
    });
    let (s, g) = (started.clone(), fail_gate.clone());
    group.submit(move || {
        s.fetch_add(1, SeqCst);
        g.wait();
        Err("boom".to_string())
    });
    assert!(eventually(|| started.load(SeqCst) == 2));
    for _ in 0..20 {
        let r = ran.clone();
        group.submit(move || {
            r.fetch_add(1, SeqCst);
            Ok(1)
        });
    }

    let token = group.cancel_token();
    fail_gate.open();
    assert!(eventually(|| token.is_cancelled()));
    slow_gate.open();

    assert!(matches!(group.wait(), Err(GroupError::Task(e)) if e == "boom"));
    assert_eq!(ran.load(SeqCst), 0, "queued tasks ran after the failure");
    pool.stop_and_wait();
}

#[test]
fn cancelled_blocking_groups_report_it_instead_of_a_partial_result() {
    let pool = BlockingPool::new(1);

    let mut group = pool.group::<i32>();
    group.submit(|| 1);
    group.cancel();
    // The worker may have started the task already, which is a full result.
    let result = group.wait();
    assert!(result == Err(Error::Canceled) || result == Ok(vec![1]));

    // Hold the only worker so the task is definitely skipped.
    let (release, held) = mpsc::channel::<()>();
    let busy = pool.submit(move || {
        let _ = held.recv();
    });
    let mut group = pool.try_group::<i32, String>();
    group.submit(|| Ok(1));
    group.cancel();
    drop(release);
    busy.wait().unwrap();
    assert_eq!(group.wait(), Err(GroupError::Pool(Error::Canceled)));
    pool.stop_and_wait();
}

// --- queue bounds ------------------------------------------------------------

#[tokio::test]
async fn zero_queue_with_unbounded_workers_never_rejects_or_hangs() {
    let pool = Pool::builder().queue_size(0).build();
    let handle = timeout(PATIENCE, pool.submit_async(|| async { 1 }))
        .await
        .expect("submit_async hung");
    assert_eq!(handle.await.unwrap(), 1);
    assert_eq!(pool.submit(|| async { 2 }).await.unwrap(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_queue_rejects_when_busy_and_submit_async_waits_for_a_worker() {
    let pool = Pool::builder().max_concurrency(1).queue_size(0).build();
    let g = gate();
    let busy = g.clone();
    let running = pool.try_submit(move || async move { wait_at_gate(&busy).await });
    assert!(running.is_ok());
    sleep(SETTLE).await;

    assert!(matches!(
        pool.try_submit(|| async {}),
        Err(Error::QueueFull)
    ));

    let p = pool.clone();
    let waiter = tokio::spawn(async move { p.submit_async(|| async { 3 }).await.await });
    sleep(SETTLE).await;
    assert!(!waiter.is_finished(), "worker is busy, queue holds nothing");

    g.close();
    assert_eq!(timeout(PATIENCE, waiter).await.unwrap().unwrap(), Ok(3));
}

#[tokio::test]
async fn idle_workers_absorb_a_burst_beyond_the_queue_size() {
    let pool = Pool::builder().max_concurrency(10).queue_size(1).build();
    // No yield, so none of these has started: 10 for the workers, 1 for the queue.
    let accepted: Vec<_> = (0..11)
        .map(|i| pool.try_submit(move || async move { i }).expect("has room"))
        .collect();
    assert!(matches!(
        pool.try_submit(|| async { 0 }),
        Err(Error::QueueFull)
    ));
    for (i, h) in accepted.into_iter().enumerate() {
        assert_eq!(h.await.unwrap(), i);
    }
}

// --- scheduling --------------------------------------------------------------

#[tokio::test]
async fn delayed_task_resolves_as_soon_as_the_pool_stops() {
    let pool = Pool::builder().build();
    let cancelled = pool.submit_after(Duration::from_secs(600), || async { 1 });
    pool.cancel();
    assert_eq!(
        timeout(PATIENCE, cancelled).await.unwrap(),
        Err(Error::Canceled)
    );

    let pool = Pool::builder().build();
    let stopped = pool.submit_after(Duration::from_secs(600), || async { 1 });
    tokio::task::yield_now().await;
    pool.stop_and_wait().await;
    assert_eq!(
        timeout(PATIENCE, stopped).await.unwrap(),
        Err(Error::PoolStopped)
    );
}

#[tokio::test]
async fn retry_backoff_ends_as_soon_as_the_pool_stops() {
    let pool = Pool::builder().build();
    let handle = pool.submit_with_retry(
        RetryPolicy::new(5).backoff(Duration::from_secs(600)),
        || async { Err::<i32, _>("nope") },
    );
    sleep(SETTLE).await; // the first attempt fails; it is now in backoff
    pool.cancel();
    assert_eq!(
        timeout(PATIENCE, handle).await.unwrap(),
        Err(Error::Canceled)
    );
}

#[tokio::test]
async fn recurring_task_ends_as_soon_as_the_pool_stops() {
    let rt = tokio::runtime::Handle::current();
    let pool = Pool::builder().build();
    tokio::task::yield_now().await;
    let before = rt.metrics().num_alive_tasks();
    let _schedule = pool.schedule_every(Duration::from_secs(600), || async {});
    tokio::task::yield_now().await;
    assert_eq!(rt.metrics().num_alive_tasks(), before + 1);

    pool.stop_and_wait().await;
    sleep(SETTLE).await;
    assert_eq!(rt.metrics().num_alive_tasks(), before);
}

// --- runtime teardown --------------------------------------------------------

#[test]
fn stop_after_the_runtime_dropped_queued_tasks_does_not_hang() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = rt.block_on(async {
        let pool = Pool::builder().max_concurrency(1).build();
        pool.spawn(std::future::pending::<()>);
        sleep(SETTLE).await; // it has the permit, so the next task must queue
        pool.spawn(|| async {});
        sleep(SETTLE).await;
        pool
    });
    rt.shutdown_timeout(Duration::from_secs(1));

    let other = tokio::runtime::Runtime::new().unwrap();
    other.block_on(async {
        timeout(PATIENCE, pool.stop_and_wait()).await.unwrap();
    });
}

// --- blocking pool -----------------------------------------------------------

#[test]
fn blocking_pool_replaces_a_worker_lost_to_an_unrecovered_panic() {
    let pool = BlockingPool::builder()
        .max_concurrency(1)
        .panic_recovery(false)
        .build();
    let result = pool.submit(|| panic!("boom")).wait();
    assert_eq!(result, Err::<(), _>(Error::Canceled));

    let p = pool.clone();
    let next = with_watchdog(PATIENCE, move || p.submit(|| 7).wait());
    assert_eq!(next, Some(Ok(7)), "the next task never ran");
    assert!(eventually(|| pool.live_workers() == 1));
    pool.stop_and_wait();
    assert_eq!(pool.live_workers(), 0);
}

#[test]
fn blocking_pool_finishes_the_queue_when_a_panic_hits_during_stop() {
    let pool = BlockingPool::builder()
        .max_concurrency(1)
        .panic_recovery(false)
        .build();
    let (release, held) = mpsc::channel::<()>();
    let _busy = pool.submit(move || {
        let _ = held.recv();
        panic!("boom");
    });
    let after = pool.submit(|| 7);

    let p = pool.clone();
    let stop = std::thread::spawn(move || p.stop_and_wait());
    std::thread::sleep(SETTLE);
    drop(release);

    let result = with_watchdog(PATIENCE, move || after.wait());
    assert_eq!(result, Some(Ok(7)), "the queued task never ran");
    stop.join().unwrap();
    assert_eq!(pool.live_workers(), 0);
}

#[test]
fn blocking_submit_released_by_stop_is_rejected_not_lost() {
    let pool = BlockingPool::builder()
        .max_concurrency(1)
        .queue_size(1)
        .build();
    let (release, held) = mpsc::channel::<()>();
    let (started, is_started) = mpsc::channel::<()>();
    let _busy = pool.submit(move || {
        started.send(()).unwrap();
        let _ = held.recv();
    });
    is_started.recv().unwrap();
    let _queued = pool.submit(|| {});

    // Parks on the full queue; only gets a slot after the pool has stopped.
    let p = pool.clone();
    let late = std::thread::spawn(move || p.submit(|| 1).wait());
    std::thread::sleep(SETTLE);
    let p = pool.clone();
    let stop = std::thread::spawn(move || p.stop_and_wait());
    std::thread::sleep(SETTLE);
    drop(release);

    let result = with_watchdog(PATIENCE, move || late.join().unwrap());
    assert_eq!(result, Some(Err(Error::PoolStopped)));
    stop.join().unwrap();
}

#[test]
fn blocking_pool_grows_when_its_only_idle_worker_is_already_taken() {
    for _ in 0..5 {
        let pool = BlockingPool::builder()
            .min_concurrency(1)
            .max_concurrency(4)
            .build();
        std::thread::sleep(Duration::from_millis(20)); // let the core worker park
        let (tx, rx) = mpsc::channel::<()>();
        // `a` only finishes if `b` runs alongside it.
        let a = pool.submit(move || rx.recv_timeout(PATIENCE).is_ok());
        let _b = pool.submit(move || {
            let _ = tx.send(());
        });
        assert!(a.wait().unwrap(), "b waited behind a despite spare threads");
        pool.stop_and_wait();
    }
}

#[test]
fn blocking_pool_never_grows_past_its_maximum() {
    let pool = BlockingPool::builder()
        .min_concurrency(1)
        .max_concurrency(3)
        .keep_alive(Duration::from_secs(60))
        .build();
    let (current, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let submitters: Vec<_> = (0..8)
        .map(|_| {
            let (pool, current, peak) = (pool.clone(), current.clone(), peak.clone());
            std::thread::spawn(move || {
                for _ in 0..50 {
                    let (c, p) = (current.clone(), peak.clone());
                    pool.submit(move || {
                        p.fetch_max(c.fetch_add(1, SeqCst) + 1, SeqCst);
                        std::thread::yield_now();
                        c.fetch_sub(1, SeqCst);
                    });
                    assert!(pool.live_workers() <= 3, "{}", pool.live_workers());
                }
            })
        })
        .collect();
    for s in submitters {
        s.join().unwrap();
    }
    pool.stop_and_wait();
    assert!(peak.load(SeqCst) <= 3, "peak {}", peak.load(SeqCst));
}

#[test]
fn blocking_zero_queue_makes_submit_wait_for_a_free_worker() {
    let pool = BlockingPool::builder()
        .max_concurrency(1)
        .queue_size(0)
        .build();
    let (release, held) = mpsc::channel::<()>();
    let (started, is_started) = mpsc::channel::<()>();
    let _busy = pool.submit(move || {
        started.send(()).unwrap();
        let _ = held.recv();
    });
    is_started.recv().unwrap();

    assert!(matches!(pool.try_submit(|| {}), Err(Error::QueueFull)));

    let p = pool.clone();
    let (done, is_done) = mpsc::channel();
    let submitter = std::thread::spawn(move || {
        let handle = p.submit(|| 5);
        done.send(()).unwrap();
        handle.wait()
    });
    assert!(
        is_done.recv_timeout(SETTLE).is_err(),
        "submit should block while the only worker is busy"
    );
    drop(release);
    assert_eq!(submitter.join().unwrap(), Ok(5));
    pool.stop_and_wait();
}

#[test]
fn blocking_non_blocking_pool_absorbs_a_burst_with_idle_workers() {
    let pool = BlockingPool::builder()
        .max_concurrency(4)
        .queue_size(1)
        .non_blocking(true)
        .build();
    let gate = BlockingGate::default();
    // 4 for the workers and 1 for the queue, wherever they have got to.
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let gate = gate.clone();
            pool.try_submit(move || gate.wait()).expect("has room")
        })
        .collect();
    assert!(matches!(pool.try_submit(|| ()), Err(Error::QueueFull)));
    gate.open();
    for h in handles {
        h.wait().unwrap();
    }
    pool.stop_and_wait();
}

/// `wait` parks the calling thread until `open`.
#[derive(Clone, Default)]
struct BlockingGate(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);

impl BlockingGate {
    fn wait(&self) {
        let (open, changed) = &*self.0;
        let mut open = open.lock().unwrap();
        while !*open {
            open = changed.wait(open).unwrap();
        }
    }

    fn open(&self) {
        let (open, changed) = &*self.0;
        *open.lock().unwrap() = true;
        changed.notify_all();
    }
}

thread_local!(static ON_THREAD_EXIT: RefCell<Option<SetOnDrop>> = const { RefCell::new(None) });

struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
    }
}

#[test]
fn dropping_the_blocking_pool_ends_its_threads_after_the_queue() {
    let exited = Arc::new(AtomicBool::new(false));
    let pool = BlockingPool::new(1);
    let e = exited.clone();
    pool.submit(move || ON_THREAD_EXIT.with(|slot| *slot.borrow_mut() = Some(SetOnDrop(e))))
        .wait()
        .unwrap();

    let queued = pool.submit(|| {
        std::thread::sleep(Duration::from_millis(50));
        9
    });
    drop(pool);

    assert_eq!(
        queued.wait(),
        Ok(9),
        "queued work still runs after the drop"
    );
    assert!(
        eventually(|| exited.load(SeqCst)),
        "thread outlived the pool"
    );
}

#[test]
fn clones_keep_the_blocking_pool_alive() {
    let pool = BlockingPool::new(1);
    let clone = pool.clone();
    drop(pool);
    assert_eq!(clone.submit(|| 1).wait(), Ok(1));
    clone.stop_and_wait();
}

// --- documented edge ---------------------------------------------------------

#[tokio::test]
async fn subpool_zero_inherits_the_parent_limit() {
    let pool = Pool::builder().max_concurrency(4).build();
    assert_eq!(pool.subpool(0).max_concurrency(), 4);
}
