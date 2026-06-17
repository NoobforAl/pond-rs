//! A multi-tenant job runner: one shared pool, one capped subpool per tenant.
//!
//! A common pattern for shared infrastructure (a job service, a query gateway)
//! is to admit work from many tenants through a single pool of workers, while
//! guaranteeing that no single tenant can monopolize it. Here a shared pool of
//! 12 workers is partitioned into three tenant subpools capped at 4 each: even
//! when one tenant submits a large burst, it can never run more than its slice,
//! leaving capacity for everyone else.
//!
//! Run with: `cargo run --example multi_tenant`

use std::time::Duration;

use pond::Pool;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    // Shared infrastructure: 12 workers for all tenants combined.
    let pool = Pool::builder().max_concurrency(12).build();

    // Each tenant gets a capped slice of the shared pool (4 of the 12 workers).
    let tenants = [("acme", 20), ("globex", 8), ("initech", 8)];

    let mut tenant_pools = Vec::new();
    for (name, job_count) in tenants {
        let sub = pool.subpool_builder().max_concurrency(4).build();

        // Fan out this tenant's jobs; "acme" deliberately bursts harder.
        let handles: Vec<_> = (0..job_count)
            .map(|i| {
                sub.submit(move || async move {
                    sleep(Duration::from_millis(10)).await;
                    i
                })
            })
            .collect();

        tenant_pools.push((name, sub, handles));
    }

    // Drain each tenant's work and report what completed.
    for (name, sub, handles) in tenant_pools {
        for h in handles {
            let _ = h.await;
        }
        println!(
            "tenant {name:>8}: completed={} (cap {} of {} shared workers)",
            sub.completed_tasks(),
            sub.max_concurrency(),
            pool.max_concurrency(),
        );
    }

    pool.stop_and_wait().await;
    println!("shared pool: successful={} total", pool.successful_tasks());
}
