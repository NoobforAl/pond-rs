//! Read pool metrics — the integration point for exporters like Prometheus.
//!
//! A Prometheus exporter would register gauges/counters backed by these
//! accessors (e.g. `pool.running_workers()`, `pool.submitted_tasks()`). Here we
//! simply print a snapshot.
//!
//! Run with: `cargo run --example metrics`

use std::time::Duration;

use pond::Pool;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let pool = Pool::builder().max_concurrency(4).build();

    for i in 0..50 {
        pool.spawn(move || async move {
            sleep(Duration::from_millis(5)).await;
            let _ = i;
        });
    }

    // Sample metrics while work is in flight.
    sleep(Duration::from_millis(10)).await;
    let snap = pool.metrics();
    println!(
        "in flight: running={}, waiting={}, submitted={}",
        snap.running_workers, snap.waiting, snap.submitted
    );

    pool.stop_and_wait().await;
    let snap = pool.metrics();
    println!(
        "final: successful={}, failed={}, completed={}, dropped={}, canceled={}",
        snap.successful, snap.failed, snap.completed, snap.dropped, snap.canceled
    );
}
