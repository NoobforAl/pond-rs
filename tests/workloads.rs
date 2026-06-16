//! Realistic workload scenario tests.
//!
//! Where `async_pool.rs` checks individual behaviors, this suite exercises the
//! pool the way applications actually use it: as a **database connection pool**
//! running SQL-style queries. Each scenario maps to a real pattern —
//!
//! * a connection pool capping concurrent queries,
//! * scatter/gather fan-out across shards (ordered results),
//! * a transaction batch that aborts on the first constraint violation,
//! * request-burst backpressure,
//! * per-tenant connection isolation via subpools,
//! * many concurrent producers hammering one pool, and
//! * a two-stage fetch → transform pipeline.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::{GroupError, Pool, QueueSize};
use tokio::time::sleep;

/// A tiny stand-in for a SQL query: simulates round-trip latency and returns the
/// number of "rows" it produced.
async fn run_query(rows: usize) -> usize {
    sleep(Duration::from_millis(2)).await; // simulate query round-trip
    rows
}

/// Tracks live and peak concurrency. Returned guard decrements on drop so the
/// count is correct even if a task panics.
#[derive(Clone)]
struct ConcurrencyTracker {
    current: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl ConcurrencyTracker {
    fn new() -> Self {
        Self {
            current: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn enter(&self) -> ConcurrencyGuard {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        ConcurrencyGuard {
            current: self.current.clone(),
        }
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

struct ConcurrencyGuard {
    current: Arc<AtomicUsize>,
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn connection_pool_caps_concurrent_queries() {
    let max_connections = 4;
    let pool = Pool::builder().max_concurrency(max_connections).build();
    let tracker = ConcurrencyTracker::new();

    let mut handles = Vec::new();
    for q in 0..40 {
        let tracker = tracker.clone();
        handles.push(pool.submit(move || async move {
            let _guard = tracker.enter();
            run_query(q % 5).await
        }));
    }

    let mut total_rows = 0;
    for h in handles {
        total_rows += h.await.unwrap();
    }

    let expected: usize = (0..40).map(|q| q % 5).sum();
    assert_eq!(
        total_rows, expected,
        "every query result must be accounted for"
    );
    assert!(
        tracker.peak() <= max_connections,
        "peak {} exceeded connection limit {}",
        tracker.peak(),
        max_connections
    );
}

#[tokio::test]
async fn scatter_gather_returns_results_in_submission_order() {
    let pool = Pool::builder().max_concurrency(8).build();
    let mut group = pool.group::<usize>();

    // Earlier shards finish *last* (longer sleep), so completion order is the
    // reverse of submission order — results must still come back in order.
    for shard in 0..16usize {
        group.submit(move || async move {
            sleep(Duration::from_millis((16 - shard) as u64)).await;
            shard * 10
        });
    }

    let results = group.wait().await.unwrap();
    let expected: Vec<usize> = (0..16).map(|s| s * 10).collect();
    assert_eq!(results, expected);
}

#[tokio::test]
async fn transaction_batch_aborts_on_first_constraint_violation() {
    let pool = Pool::builder().max_concurrency(2).build();
    let mut tx = pool.try_group::<usize, String>();

    let ran = Arc::new(AtomicUsize::new(0));
    for stmt in 0..10usize {
        let ran = ran.clone();
        tx.submit(move || async move {
            ran.fetch_add(1, Ordering::SeqCst);
            sleep(Duration::from_millis(1)).await;
            if stmt == 3 {
                Err(format!("constraint violation at statement {stmt}"))
            } else {
                Ok(stmt)
            }
        });
    }

    match tx.wait().await {
        Err(GroupError::Task(msg)) => assert!(msg.contains("statement 3")),
        other => panic!("expected the constraint violation, got {other:?}"),
    }

    // The first error cancels the remaining *queued* statements, so not all ten
    // should have started (with concurrency 2 there is plenty still queued).
    assert!(
        ran.load(Ordering::SeqCst) < 10,
        "abort should have skipped some queued statements"
    );
}

#[tokio::test]
async fn request_burst_applies_backpressure_without_dropping() {
    // Small pool + bounded queue: a 100-request burst far exceeds capacity.
    let pool = Pool::builder()
        .max_concurrency(4)
        .queue_size(QueueSize::Bounded(8))
        .build();

    let processed = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..100 {
        let processed = processed.clone();
        // `submit_async` awaits a free slot instead of rejecting.
        let h = pool
            .submit_async(move || async move {
                run_query(1).await;
                processed.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        handles.push(h);
    }
    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(processed.load(Ordering::SeqCst), 100);
    assert_eq!(
        pool.dropped_tasks(),
        0,
        "backpressure must not drop requests"
    );
}

#[tokio::test]
async fn tenant_subpool_caps_its_own_concurrency() {
    // Shared DB pool with 8 connections; one tenant is limited to 2.
    let db = Pool::builder().max_concurrency(8).build();
    let tenant = db.subpool(2);
    let tracker = ConcurrencyTracker::new();

    let mut handles = Vec::new();
    for _ in 0..20 {
        let tracker = tracker.clone();
        handles.push(tenant.submit(move || async move {
            let _guard = tracker.enter();
            run_query(1).await
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    assert!(
        tracker.peak() <= 2,
        "tenant used {} connections, limit was 2",
        tracker.peak()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_producers_submit_concurrently() {
    let pool = Pool::builder().max_concurrency(8).build();
    let completed = Arc::new(AtomicUsize::new(0));

    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 125;

    let producers: Vec<_> = (0..PRODUCERS)
        .map(|_| {
            let pool = pool.clone();
            let completed = completed.clone();
            tokio::spawn(async move {
                let mut handles = Vec::new();
                for _ in 0..PER_PRODUCER {
                    let completed = completed.clone();
                    handles.push(pool.submit(move || async move {
                        completed.fetch_add(1, Ordering::SeqCst);
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            })
        })
        .collect();

    for p in producers {
        p.await.unwrap();
    }

    let total = PRODUCERS * PER_PRODUCER;
    assert_eq!(completed.load(Ordering::SeqCst), total);
    assert_eq!(pool.submitted_tasks() as usize, total);
}

#[tokio::test]
async fn staged_fetch_transform_pipeline() {
    // Stage 1 "fetches" rows; stage 2 "transforms" them. Two pools chained, the
    // output of each stage-1 task feeding a stage-2 task.
    let fetch = Pool::builder().max_concurrency(4).build();
    let transform = Pool::builder().max_concurrency(4).build();

    let stage1: Vec<_> = (0..20usize)
        .map(|id| fetch.submit(move || async move { run_query(id).await }))
        .collect();

    let mut stage2 = Vec::new();
    for h in stage1 {
        let rows = h.await.unwrap();
        stage2.push(transform.submit(move || async move { rows * 2 }));
    }

    let mut total = 0;
    for h in stage2 {
        total += h.await.unwrap();
    }

    let expected: usize = (0..20).map(|id| id * 2).sum();
    assert_eq!(total, expected);

    fetch.stop_and_wait().await;
    transform.stop_and_wait().await;
}

#[cfg(feature = "blocking")]
#[test]
fn blocking_pool_simulates_synchronous_db_driver() {
    use pond::blocking::Pool as BlockingPool;
    use std::thread;

    // A blocking driver (no async) capped at 4 connections.
    let pool = BlockingPool::builder().max_concurrency(4).build();
    let tracker = ConcurrencyTracker::new();

    let mut handles = Vec::new();
    for q in 0..40usize {
        let tracker = tracker.clone();
        handles.push(pool.submit(move || {
            let _guard = tracker.enter();
            thread::sleep(Duration::from_millis(2)); // blocking driver call
            q % 5
        }));
    }

    let mut total = 0;
    for h in handles {
        total += h.wait().unwrap();
    }

    let expected: usize = (0..40).map(|q| q % 5).sum();
    assert_eq!(total, expected);
    assert!(tracker.peak() <= 4, "peak {} exceeded 4", tracker.peak());
    pool.stop_and_wait();
}
