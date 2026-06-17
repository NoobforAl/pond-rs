//! Real async I/O through the pool: write a batch of files concurrently, then
//! read them all back — every task `.await`s actual disk I/O via `tokio::fs`.
//!
//! This is the bread-and-butter use of the async pool: bound the number of
//! in-flight I/O operations (here, 8) while a much larger batch of awaitable
//! work flows through it.
//!
//! Run with: `cargo run --example async_io`

use std::time::Duration;

use pond::Pool;

#[tokio::main]
async fn main() {
    // A scratch directory in the OS temp dir (not the project tree).
    let dir = std::env::temp_dir().join(format!("pond_async_io_{}", std::process::id()));
    tokio::fs::create_dir_all(&dir).await.unwrap();

    // At most 8 file operations run at once, however many we submit.
    let pool = Pool::builder().max_concurrency(8).build();

    // Phase 1 — write 32 files concurrently. Each task awaits a (simulated)
    // latency and then a real `tokio::fs::write`.
    let writes: Vec<_> = (0..32u32)
        .map(|i| {
            let path = dir.join(format!("chunk_{i:02}.txt"));
            pool.submit(move || async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let body = format!("chunk {i} payload\n").repeat((i + 1) as usize);
                tokio::fs::write(&path, body.as_bytes()).await.unwrap();
                path
            })
        })
        .collect();

    let mut paths = Vec::new();
    for w in writes {
        paths.push(w.await.unwrap());
    }

    // Phase 2 — read every file back concurrently and total the bytes.
    let reads: Vec<_> = paths
        .into_iter()
        .map(|path| {
            pool.submit(move || async move {
                let bytes = tokio::fs::read(&path).await.unwrap();
                bytes.len()
            })
        })
        .collect();

    let mut total = 0usize;
    for r in reads {
        total += r.await.unwrap();
    }

    pool.stop_and_wait().await;
    println!("wrote and read back {total} bytes across 32 files");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}
