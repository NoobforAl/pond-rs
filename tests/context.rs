#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Tests for per-task `TaskContext` (cancellation awareness + shared data).

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::Pool;
use tokio::time::sleep;

async fn wait_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        sleep(Duration::from_millis(2)).await;
    }
    panic!("condition not met within timeout");
}

#[tokio::test]
async fn task_ctx_reads_shared_data_and_streams_results() {
    // Attach an mpsc sender as shared data; tasks pull it and stream results out.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
    let data: Arc<dyn Any + Send + Sync> = Arc::new(tx);
    let pool = Pool::builder().max_concurrency(4).task_data(data).build();

    for i in 0..10u64 {
        pool.spawn_ctx(move |ctx| async move {
            let sender = ctx
                .data::<tokio::sync::mpsc::UnboundedSender<u64>>()
                .expect("shared sender present");
            let _ = sender.send(i * 2);
        });
    }
    pool.stop_and_wait().await;

    let mut got = Vec::new();
    while let Ok(v) = rx.try_recv() {
        got.push(v);
    }
    got.sort_unstable();
    assert_eq!(got, (0..10).map(|i| i * 2).collect::<Vec<_>>());
}

#[tokio::test]
async fn task_ctx_breaks_early_on_cancel_midrun() {
    let pool = Pool::builder().max_concurrency(1).build();
    let started = Arc::new(AtomicBool::new(false));
    let s = started.clone();

    let h = pool.submit_ctx(move |ctx| async move {
        s.store(true, Ordering::SeqCst);
        tokio::select! {
            _ = ctx.cancelled() => "cancelled",
            _ = sleep(Duration::from_secs(10)) => "completed",
        }
    });

    // Make sure the task is actually running before we cancel.
    wait_until(|| started.load(Ordering::SeqCst)).await;

    pool.cancel();
    assert_eq!(h.await.unwrap(), "cancelled");
}

#[tokio::test]
async fn task_ctx_data_absent_returns_none() {
    let pool = Pool::builder().max_concurrency(1).build();
    let h = pool.submit_ctx(|ctx| async move { ctx.data::<u64>().is_none() });
    assert!(h.await.unwrap());
}
