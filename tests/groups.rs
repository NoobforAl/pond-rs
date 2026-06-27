#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Integration tests for task groups.

use std::time::Duration;

use pond::{GroupError, Pool};
use tokio::time::sleep;

#[tokio::test]
async fn result_group_collects_in_submission_order() {
    let pool = Pool::builder().max_concurrency(4).build();
    let mut group = pool.group::<usize>();
    for i in 0..10 {
        // Later tasks finish sooner, but results must stay in submission order.
        group.submit(move || async move {
            sleep(Duration::from_millis((10 - i) as u64)).await;
            i
        });
    }
    let results = group.wait().await.unwrap();
    assert_eq!(results, (0..10).collect::<Vec<_>>());
}

#[tokio::test]
async fn result_group_empty_is_ok() {
    let pool = Pool::builder().max_concurrency(2).build();
    let group = pool.group::<i32>();
    assert_eq!(group.wait().await.unwrap(), Vec::<i32>::new());
}

#[tokio::test]
async fn try_group_returns_first_task_error_and_cancels_rest() {
    let pool = Pool::builder().max_concurrency(1).build();
    let mut group = pool.try_group::<i32, String>();
    for i in 0..6 {
        group.submit(move || async move {
            if i == 2 {
                Err(format!("failed at {i}"))
            } else {
                sleep(Duration::from_millis(1)).await;
                Ok(i)
            }
        });
    }
    match group.wait().await {
        Err(GroupError::Task(msg)) => assert_eq!(msg, "failed at 2"),
        other => panic!("expected Task error, got {other:?}"),
    }
}

#[tokio::test]
async fn try_group_all_ok_collects_values() {
    let pool = Pool::builder().max_concurrency(4).build();
    let mut group = pool.try_group::<i32, String>();
    for i in 0..5 {
        group.submit(move || async move { Ok(i * 2) });
    }
    let out = group.wait().await.unwrap();
    assert_eq!(out, vec![0, 2, 4, 6, 8]);
}

#[tokio::test]
async fn group_panic_surfaces_as_pool_error() {
    let pool = Pool::builder().max_concurrency(2).build();
    let mut group = pool.group::<i32>();
    group.submit(|| async { 1 });
    group.submit(|| async { panic!("kaboom") });
    let result = group.wait().await;
    assert!(result.is_err(), "panicking task should fail the group");
}

#[tokio::test]
async fn group_does_not_cancel_parent_pool() {
    let pool = Pool::builder().max_concurrency(2).build();
    let mut group = pool.try_group::<i32, String>();
    group.submit(|| async { Err("boom".to_string()) });
    let _ = group.wait().await;
    // Parent pool still works.
    assert_eq!(pool.submit(|| async { 42 }).await.unwrap(), 42);
}
