//! The synchronous, thread-based pool — no async runtime required.
//!
//! Run with: `cargo run --example blocking`

#[cfg(feature = "blocking")]
fn main() {
    use pond::blocking::Pool;

    let pool = Pool::builder().max_concurrency(4).build();

    let handles: Vec<_> = (0..8)
        .map(|i| {
            pool.submit(move || {
                // Pretend to do some blocking work.
                std::thread::sleep(std::time::Duration::from_millis(10));
                i * 10
            })
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.wait().unwrap()).collect();
    println!("results = {results:?}");

    pool.stop_and_wait();
    println!("completed {} tasks", results.len());
}

#[cfg(not(feature = "blocking"))]
fn main() {
    eprintln!("this example requires the `blocking` feature");
}
