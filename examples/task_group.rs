//! Group several tasks and wait for them together, collecting results in order.
//!
//! Run with: `cargo run --example task_group`

use pond::{GroupError, Pool};

#[tokio::main]
async fn main() {
    let pool = Pool::builder().max_concurrency(4).build();

    // A group of value-returning tasks; results come back in submission order.
    let mut group = pool.group::<String>();
    for i in 0..5 {
        group.submit(move || async move { format!("task #{i}") });
    }
    let results = group.wait().await.unwrap();
    println!("group results: {results:?}");

    // A fallible group: the first error cancels the remaining queued tasks.
    let mut fallible = pool.try_group::<i32, String>();
    for i in 0..5 {
        fallible.submit(move || async move {
            if i == 3 {
                Err(format!("task {i} failed"))
            } else {
                Ok(i)
            }
        });
    }
    match fallible.wait().await {
        Ok(values) => println!("all ok: {values:?}"),
        Err(GroupError::Task(e)) => println!("first task error: {e}"),
        Err(GroupError::Pool(e)) => println!("pool error: {e}"),
    }

    pool.stop_and_wait().await;
}
