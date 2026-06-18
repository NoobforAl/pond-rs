//! Shut a pool down automatically — on a deadline, or when an external signal
//! fires (`cancel_when`).
//!
//! Run with: `cargo run --example cancel_deadline`

use std::time::Duration;

use pond::Pool;

#[tokio::main]
async fn main() {
    // 1. Deadline: the pool stops itself once the duration elapses.
    let pool = Pool::builder()
        .max_concurrency(4)
        .deadline(Duration::from_millis(50))
        .build();
    tokio::time::sleep(Duration::from_millis(120)).await;
    println!("after deadline, stopped = {}", pool.is_stopped());

    // 2. cancel_when: stop when an arbitrary future completes (e.g. a shutdown
    //    signal). Here a oneshot receiver stands in for that signal.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let pool = Pool::builder()
        .max_concurrency(4)
        .cancel_when(async move {
            let _ = rx.await;
        })
        .build();

    assert!(!pool.is_stopped());
    let _ = tx.send(()); // fire the signal
    pool.cancel_token().cancelled().await; // wait for the pool to observe it
    println!("after signal, stopped = {}", pool.is_stopped());
}
