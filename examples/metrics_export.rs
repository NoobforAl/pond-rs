//! Expose pool metrics in Prometheus text format.
//!
//! Run with: `cargo run --example metrics_export --features metrics-export`
//!
//! In a real service you'd serve the string returned by `to_prometheus_text`
//! from your `/metrics` HTTP handler so Prometheus can scrape it.

use pond::Pool;

#[tokio::main]
async fn main() {
    let pool = Pool::builder().max_concurrency(4).build();

    // Run a mix of succeeding and failing tasks so the counters are non-trivial.
    let mut handles = Vec::new();
    for i in 0..10 {
        handles.push(pool.submit(move || async move {
            if i % 4 == 0 {
                panic!("task {i} failed");
            }
            i
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    pool.stop_and_wait().await;

    // This is exactly what a `/metrics` endpoint would return.
    let scrape_body = pool.metrics().to_prometheus_text("example_pool");
    println!("{scrape_body}");
}
