//! Monitor task timing and pool behavior with `tracing`.
//!
//! Run with: `cargo run --example tracing_debug --features tracing`
//!
//! Each `pond.task` span's duration is the task's wall-clock time. With the
//! `tracing` feature off, the pool emits nothing (the instrumentation is
//! compiled out), so this is purely a debug/test aid.

#[cfg(feature = "tracing")]
#[tokio::main]
async fn main() {
    use std::time::Duration;

    use pond::Pool;
    use tokio::time::sleep;

    // Any tracing subscriber works; here we print spans/events with timing.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_target(true)
        .init();

    let pool = Pool::builder().max_concurrency(2).build();

    let handles: Vec<_> = (0..5)
        .map(|i| {
            pool.submit(move || async move {
                sleep(Duration::from_millis(10 * (i + 1))).await;
                i
            })
        })
        .collect();

    for h in handles {
        let _ = h.await;
    }

    pool.stop_and_wait().await;
}

#[cfg(not(feature = "tracing"))]
fn main() {
    eprintln!("run with `--features tracing` to see task instrumentation");
}
