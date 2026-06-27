//! Live memory + span profiling in the Tracy GUI.
//!
//! Setup (one time): download the Tracy profiler GUI (`Tracy.exe`) from
//! https://github.com/wolfpld/tracy/releases — use the release that matches the
//! `tracy-client` version (0.17.x ⇒ Tracy 0.11.x).
//!
//! Then:
//!   1. Launch Tracy and click "Connect".
//!   2. Run: `cargo run --release --example tracy_profile --features tracy`
//!
//! Tracy shows a live memory timeline (every allocation, with call sites) plus
//! pond's task spans (`pond.task`, `pond.task.wait_permit`, …) on the same
//! timeline, so you can correlate memory with pool activity.

use std::time::Duration;

use pond::Pool;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// Route every allocation through Tracy (the second arg is the call-stack depth
// captured per allocation). Use the client re-exported by `tracing-tracy` so
// there's a single `tracy-client` version.
#[global_allocator]
static GLOBAL: tracing_tracy::client::ProfiledAllocator<std::alloc::System> =
    tracing_tracy::client::ProfiledAllocator::new(std::alloc::System, 100);

#[tokio::main]
async fn main() {
    // Bridge pond's `tracing` spans into Tracy (also starts the Tracy client).
    tracing_subscriber::registry()
        .with(tracing_tracy::TracyLayer::default())
        .init();

    let pool = Pool::builder().max_concurrency(8).build();

    println!("Profiling... connect the Tracy GUI now. Running 1000 rounds.");
    // A sustained workload so you have time to inspect it live in Tracy.
    for round in 0..1000u64 {
        let mut handles = Vec::new();
        for i in 0..1_000u64 {
            handles.push(pool.submit(move || async move {
                // Allocate a little so it shows up in the memory view.
                let v: Vec<u64> = (0..(i % 64)).collect();
                v.iter().sum::<u64>()
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        if round % 10 == 0 {
            println!("round {round}");
        }
    }

    pool.stop_and_wait().await;
    println!("done — disconnect Tracy or save the trace from its GUI.");
}
