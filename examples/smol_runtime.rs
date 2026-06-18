//! Run a pool on the smol runtime instead of Tokio.
//!
//! Run with: `cargo run --example smol_runtime --features runtime-smol`

use pond::{Pool, SmolSpawner};

fn main() {
    // `SmolSpawner` runs tasks on `async-global-executor`'s worker threads, so no
    // explicit runtime setup is needed.
    let pool = Pool::builder_with(SmolSpawner::new())
        .max_concurrency(4)
        .build();

    // The pool's tasks run on smol's threads; drive the handles from here.
    let total: u64 = futures::executor::block_on(async {
        let mut sum = 0;
        let mut handles = Vec::new();
        for i in 0..8u64 {
            handles.push(pool.submit(move || async move { i * 2 }));
        }
        for h in handles {
            sum += h.await.unwrap();
        }
        sum
    });

    println!("smol-runtime total: {total}");
}
