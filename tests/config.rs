#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Tests for builder config: result-channel streaming and pool naming.

use pond::{Pool, PoolConfig};

#[tokio::test]
async fn result_pool_streams_each_result() {
    let pool = Pool::builder().max_concurrency(4).build();
    let (tx, rx) = async_channel::unbounded::<u64>();
    let typed = pool.results_to(tx);

    for i in 0..5u64 {
        typed.submit(move || async move { i * i });
    }

    let mut got = Vec::new();
    for _ in 0..5 {
        got.push(rx.recv().await.unwrap());
    }
    got.sort_unstable();
    assert_eq!(got, vec![0, 1, 4, 9, 16]);
}

#[tokio::test]
async fn group_result_channel_streams_then_wait_completes() {
    let pool = Pool::builder().max_concurrency(4).build();
    let (tx, rx) = async_channel::unbounded::<u64>();
    let group = pool
        .group_builder::<u64>()
        .max_concurrency(2)
        .result_channel(tx);

    for i in 0..6u64 {
        group.submit(move || async move { i });
    }
    group.wait().await.unwrap();

    let mut got = Vec::new();
    while let Ok(v) = rx.try_recv() {
        got.push(v);
    }
    got.sort_unstable();
    assert_eq!(got, vec![0, 1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn try_group_result_channel_streams_oks_and_errs() {
    let pool = Pool::builder().max_concurrency(4).build();
    let (tx, rx) = async_channel::unbounded::<Result<u64, String>>();
    let group = pool.try_group_builder::<u64, String>().result_channel(tx);

    for i in 0..4u64 {
        group.submit(move || async move {
            if i == 2 {
                Err("two".to_string())
            } else {
                Ok(i)
            }
        });
    }
    group.wait().await.unwrap();

    let (mut oks, mut errs) = (0, 0);
    while let Ok(r) = rx.try_recv() {
        match r {
            Ok(_) => oks += 1,
            Err(_) => errs += 1,
        }
    }
    assert_eq!(oks, 3);
    assert_eq!(errs, 1);
}

#[tokio::test]
async fn config_struct_matches_individual_methods() {
    let by_method = Pool::builder().pool_name("api").build();
    let by_config = Pool::builder()
        .config(PoolConfig {
            pool_name: Some("api".into()),
        })
        .build();
    assert_eq!(by_method.name(), Some("api"));
    assert_eq!(by_config.name(), Some("api"));
}

#[cfg(feature = "runtime-smol")]
#[test]
fn results_to_is_runtime_agnostic() {
    use pond::SmolSpawner;

    let pool = Pool::builder_with(SmolSpawner::new())
        .max_concurrency(2)
        .build();
    let (tx, rx) = async_channel::unbounded::<u64>();
    let typed = pool.results_to(tx);
    typed.submit(|| async { 99 });

    let got = futures::executor::block_on(async { rx.recv().await.unwrap() });
    assert_eq!(got, 99);
}
