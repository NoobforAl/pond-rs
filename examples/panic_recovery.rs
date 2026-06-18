//! Panic handling: with recovery on (the default) a panicking task is caught and
//! surfaced as `Error::Panicked`; with recovery off the handle resolves to
//! `Error::Canceled` (the panic propagates on the worker instead).
//!
//! Run with: `cargo run --example panic_recovery`

use pond::{Error, Pool};

#[tokio::main]
async fn main() {
    // Recovery ON (default): the pool survives and reports the panic.
    let pool = Pool::builder().max_concurrency(2).build();
    match pool.submit(|| async { panic!("kaboom") }).await {
        Err(Error::Panicked(msg)) => println!("recovered panic: {msg}"),
        other => println!("unexpected: {other:?}"),
    }
    // The pool keeps working after a recovered panic.
    println!(
        "still alive: {}",
        pool.submit(|| async { 1 + 1 }).await.unwrap()
    );
    pool.stop_and_wait().await;

    // Recovery OFF: the worker task unwinds; the awaiter sees `Canceled`.
    let strict = Pool::builder()
        .max_concurrency(2)
        .panic_recovery(false)
        .build();
    match strict.submit(|| async { panic!("kaboom") }).await {
        Err(Error::Canceled) => println!("no recovery: handle resolved Canceled"),
        other => println!("unexpected: {other:?}"),
    }
    strict.stop_and_wait().await;
}
