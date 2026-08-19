#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Heavy-workload tests: real filesystem I/O and high task counts.
//!
//! Where `workloads.rs` models application patterns with simulated work (a
//! `sleep` standing in for a query), these drive the pool with work that
//! actually touches the filesystem, and with task counts an order of magnitude
//! larger. That combination is what shakes out the failures which only appear
//! under sustained load:
//!
//! * data lost or corrupted when many tasks write concurrently,
//! * a concurrency cap that slips once tasks park on real I/O rather than on a
//!   timer (real I/O yields at different points than `sleep` does),
//! * large task results that don't survive the result channel,
//! * a pool that completes its work but never quiesces, and
//! * a panic in the middle of an I/O task taking the pool down with it.
//!
//! Every test cleans up its own temp directory and none of them touch the
//! network, so they are safe to run in CI.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::Pool;

/// A unique temp directory for one test.
///
/// Deliberately under the OS temp dir rather than the repo: the working tree
/// lives on OneDrive, which is slow and can hold locks on files under test.
fn temp_dir(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("pond_io_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p); // start from a clean slate
    std::fs::create_dir_all(&p).expect("create temp dir");
    p
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// The payload written to file `i` — varying length so a truncated or
/// interleaved write can't accidentally still match.
fn payload(i: usize) -> String {
    format!("file-{i}:{}", "x".repeat(i % 997))
}

/// Many tasks each write a file and read it back; every byte must survive.
///
/// The round-trip is inside the task, so a task that raced with another (shared
/// buffer, reused path, a handle closed too early) fails on its own read rather
/// than needing a separate verification pass to notice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_file_writes_round_trip_intact() {
    const FILES: usize = 500;
    let dir = temp_dir("roundtrip");

    let pool = Pool::builder().max_concurrency(16).build();
    let handles: Vec<_> = (0..FILES)
        .map(|i| {
            let path = dir.join(format!("f{i}.txt"));
            pool.submit(move || async move {
                let want = payload(i);
                tokio::fs::write(&path, want.as_bytes()).await.unwrap();
                let got = tokio::fs::read_to_string(&path).await.unwrap();
                assert_eq!(got, want, "file {i} came back different than written");
                got.len()
            })
        })
        .collect();

    let mut total = 0usize;
    for h in handles {
        total += h.await.unwrap();
    }
    pool.stop_and_wait().await;

    // Every file exists on disk afterwards, with the right content.
    let expected: usize = (0..FILES).map(|i| payload(i).len()).sum();
    assert_eq!(total, expected, "total bytes written differ from expected");
    assert_eq!(
        std::fs::read_dir(&dir).unwrap().count(),
        FILES,
        "wrong number of files left on disk"
    );

    cleanup(&dir);
}

/// The concurrency cap holds when tasks park on real file I/O.
///
/// A `sleep`-based task yields at a predictable point; a real I/O task yields
/// wherever the OS decides to. This checks the permit accounting doesn't depend
/// on that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_cap_holds_under_real_io() {
    const TASKS: usize = 200;
    const CAP: usize = 4;
    let dir = temp_dir("cap");

    let pool = Pool::builder().max_concurrency(CAP).build();
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..TASKS)
        .map(|i| {
            let path = dir.join(format!("c{i}.bin"));
            let current = current.clone();
            let peak = peak.clone();
            pool.submit(move || async move {
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);

                // Several real I/O operations, so the task is parked in the
                // kernel for most of its life.
                tokio::fs::write(&path, vec![b'a'; 4096]).await.unwrap();
                let _ = tokio::fs::read(&path).await.unwrap();
                tokio::fs::remove_file(&path).await.unwrap();

                current.fetch_sub(1, Ordering::SeqCst);
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }
    pool.stop_and_wait().await;

    let peak = peak.load(Ordering::SeqCst);
    assert!(peak <= CAP, "{peak} tasks ran concurrently, cap was {CAP}");
    // ...and the cap must be the reason, not a pool that serialized everything:
    // without this the test would pass on a pool with no concurrency at all.
    assert!(
        peak > 1,
        "no tasks overlapped (peak {peak}) — the cap wasn't what limited this"
    );
    cleanup(&dir);
}

/// 20k tasks through a bounded pool: all complete, and the pool then quiesces.
///
/// The `timeout` is the real assertion — a pool that drops a permit, or misses
/// an idle notification, hangs here instead of failing fast.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn high_load_all_tasks_complete_and_pool_quiesces() {
    const TASKS: usize = 20_000;

    let pool = Pool::builder().max_concurrency(8).build();
    let done = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..TASKS)
        .map(|i| {
            let done = done.clone();
            pool.submit(move || async move {
                done.fetch_add(1, Ordering::Relaxed);
                i.wrapping_mul(2_654_435_761)
            })
        })
        .collect();

    let mut checksum = 0usize;
    for h in handles {
        checksum = checksum.wrapping_add(h.await.unwrap());
    }

    assert_eq!(done.load(Ordering::Relaxed), TASKS, "not every task ran");
    let expected = (0..TASKS).fold(0usize, |acc, i| {
        acc.wrapping_add(i.wrapping_mul(2_654_435_761))
    });
    assert_eq!(
        checksum, expected,
        "results don't match the tasks submitted"
    );

    tokio::time::timeout(Duration::from_secs(30), pool.stop_and_wait())
        .await
        .expect("pool did not quiesce after 20k tasks");
}

/// Repeated waves through one long-lived pool.
///
/// A pool reused across waves has to return every permit each time. If it
/// leaks even one per wave, a later wave runs at reduced concurrency and
/// eventually deadlocks — which surfaces here as the timeout firing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_waves_keep_full_throughput() {
    const WAVES: usize = 10;
    const PER_WAVE: usize = 1_000;

    let pool = Pool::builder().max_concurrency(8).build();

    for wave in 0..WAVES {
        let handles: Vec<_> = (0..PER_WAVE)
            .map(|i| pool.submit(move || async move { i }))
            .collect();

        let fut = async {
            let mut sum = 0usize;
            for h in handles {
                sum += h.await.unwrap();
            }
            sum
        };
        let sum = tokio::time::timeout(Duration::from_secs(30), fut)
            .await
            .unwrap_or_else(|_| panic!("wave {wave} stalled — suspect a leaked permit"));

        assert_eq!(
            sum,
            (0..PER_WAVE).sum::<usize>(),
            "wave {wave} lost results"
        );
    }

    pool.stop_and_wait().await;
}

/// Large task results survive the result channel intact.
///
/// Trivial `u64` results dominate the rest of the suite; this checks nothing
/// truncates or shares a buffer when a task returns megabytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_payloads_survive_the_result_channel() {
    const TASKS: usize = 64;
    const SIZE: usize = 1 << 20; // 1 MiB each

    let pool = Pool::builder().max_concurrency(8).build();
    let handles: Vec<_> = (0..TASKS)
        .map(|i| {
            pool.submit(move || async move {
                // Byte value derived from the task index, so a swapped result
                // between two tasks is detectable.
                vec![(i % 251) as u8; SIZE]
            })
        })
        .collect();

    for (i, h) in handles.into_iter().enumerate() {
        let buf = h.await.unwrap();
        assert_eq!(buf.len(), SIZE, "task {i} returned a short buffer");
        assert!(
            buf.iter().all(|&b| b == (i % 251) as u8),
            "task {i} returned another task's data"
        );
    }
    pool.stop_and_wait().await;
}

/// A few slow I/O tasks must not stop many fast ones from finishing.
///
/// With `max_concurrency` above the number of slow tasks, the fast ones should
/// flow past them rather than queueing behind them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_tasks_do_not_block_fast_ones() {
    const SLOW: usize = 4;
    const FAST: usize = 400;
    let dir = temp_dir("mixed");

    let pool = Pool::builder().max_concurrency(8).build();

    // Counts slow tasks that have finished. The assertion below is that the
    // fast tasks all drain while this is still zero.
    let slow_done = Arc::new(AtomicUsize::new(0));

    let slow: Vec<_> = (0..SLOW)
        .map(|i| {
            let path = dir.join(format!("slow{i}.bin"));
            let slow_done = slow_done.clone();
            pool.submit(move || async move {
                // A big write plus a deliberate pause: the "slow query" case.
                tokio::fs::write(&path, vec![0u8; 1 << 20]).await.unwrap();
                tokio::time::sleep(Duration::from_secs(1)).await;
                let len = tokio::fs::metadata(&path).await.unwrap().len() as usize;
                slow_done.fetch_add(1, Ordering::SeqCst);
                len
            })
        })
        .collect();

    let fast: Vec<_> = (0..FAST)
        .map(|i| pool.submit(move || async move { i }))
        .collect();

    // The fast tasks are awaited first and must not need the slow ones to
    // finish. Checking `slow_done` is what makes this meaningful: a wall-clock
    // timeout alone would still pass if the pool serialized everything, since
    // 4 x 1s is well inside any bound generous enough not to be flaky.
    let fast_fut = async {
        let mut sum = 0usize;
        for h in fast {
            sum += h.await.unwrap();
        }
        sum
    };
    let sum = tokio::time::timeout(Duration::from_secs(30), fast_fut)
        .await
        .expect("fast tasks never completed");
    assert_eq!(sum, (0..FAST).sum::<usize>());
    assert_eq!(
        slow_done.load(Ordering::SeqCst),
        0,
        "fast tasks only finished after slow ones did — they were queued behind them"
    );

    for h in slow {
        assert_eq!(h.await.unwrap(), 1 << 20);
    }
    pool.stop_and_wait().await;

    cleanup(&dir);
}

/// A panic inside an I/O task is contained: that task's handle reports the
/// panic, every other task still completes, and the pool stays usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_during_io_does_not_poison_the_pool() {
    const TASKS: usize = 200;
    let dir = temp_dir("panic");

    let pool = Pool::builder()
        .max_concurrency(8)
        .panic_recovery(true)
        .build();

    // Every seventh task panics after doing some real I/O.
    let handles: Vec<_> = (0..TASKS)
        .map(|i| {
            let path = dir.join(format!("p{i}.txt"));
            pool.submit(move || async move {
                tokio::fs::write(&path, format!("{i}")).await.unwrap();
                if i % 7 == 0 {
                    panic!("task {i} failed mid-write");
                }
                i
            })
        })
        .collect();

    let mut ok = 0usize;
    let mut panicked = 0usize;
    for (i, h) in handles.into_iter().enumerate() {
        match h.await {
            Ok(v) => {
                assert_eq!(v, i);
                ok += 1;
            }
            Err(_) => panicked += 1,
        }
    }

    let expected_panics = (0..TASKS).filter(|i| i % 7 == 0).count();
    assert_eq!(panicked, expected_panics, "wrong number of panics reported");
    assert_eq!(ok, TASKS - expected_panics, "healthy tasks were lost");

    // The pool still works afterwards.
    assert_eq!(pool.submit(|| async { 42u32 }).await.unwrap(), 42);
    pool.stop_and_wait().await;

    cleanup(&dir);
}
