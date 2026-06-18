//! Run a pool on a custom runtime by implementing the `Spawner` trait.
//!
//! This toy spawner runs each task on its own thread (via `futures` block_on),
//! proving the pool is genuinely runtime-agnostic — no Tokio involved.
//!
//! Run with: `cargo run --example custom_spawner`

use futures::future::BoxFuture;
use pond::{Pool, Spawner};

/// A minimal `Spawner`: one OS thread per task. Real adapters delegate to a
/// runtime's executor (see `TokioSpawner`, `SmolSpawner`).
#[derive(Clone, Default)]
struct ThreadSpawner;

impl Spawner for ThreadSpawner {
    fn spawn(&self, future: BoxFuture<'static, ()>) {
        std::thread::spawn(move || futures::executor::block_on(future));
    }

    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(f);
    }
    // `sleep` uses the trait's default (a blocking sleep offloaded to a thread).
}

fn main() {
    let pool = Pool::builder_with(ThreadSpawner).max_concurrency(4).build();

    // The pool runs on `ThreadSpawner`; drive the result handle from this thread.
    let result = futures::executor::block_on(async {
        let handle = pool.submit(|| async { 6 * 7 });
        handle.await.unwrap()
    });

    println!("custom-runtime result: {result}");
}
