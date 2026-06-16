//! Integration tests for the process-wide default pool.
//!
//! The default pool binds to the Tokio runtime that first uses it, so these
//! checks share a single runtime (one `#[tokio::test]`) — mirroring how a real
//! application has exactly one long-lived runtime.
#![cfg(feature = "runtime-tokio")]

#[tokio::test]
async fn default_pool_submit_group_and_subpool() {
    // submit
    let handle = pond::submit(|| async { 21 * 2 });
    assert_eq!(handle.await.unwrap(), 42);

    // group collects in submission order
    let mut group = pond::group::<i32>();
    for i in 0..5 {
        group.submit(move || async move { i });
    }
    assert_eq!(group.wait().await.unwrap(), vec![0, 1, 2, 3, 4]);

    // subpool
    let sub = pond::subpool(2);
    assert_eq!(sub.max_concurrency(), 2);
    assert_eq!(sub.submit(|| async { "ok" }).await.unwrap(), "ok");
}
