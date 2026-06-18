# pond-rs

An idiomatic, runtime-agnostic worker pool for Rust.

> Inspired by Pond, a Go worker-pool library — this is an independent Rust project, not a
> binding or a port.
>
> The package is published as `pond-rs`; the library is imported as `pond` (`use pond::…`).

## Features

- **Async pool** that runs futures with bounded concurrency on a pluggable runtime.
- **Blocking pool** that runs synchronous closures on OS threads (no async runtime needed).
- **Pluggable runtime** via the `Spawner` trait — choose your async runtime with a Cargo
  feature: `runtime-tokio` (default), `runtime-smol`, or `runtime-async-std`. Any other runtime
  works by implementing `Spawner` yourself.
- Task **handles** (awaitable futures), **task groups**, **subpools**, dynamic **resize**,
  graceful **shutdown**, **cancellation**, and rich **metrics**.
- **Context integration**: bridge any external cancellation source or deadline into the pool, and
  hand each task a `TaskContext` to break early or carry/stream data.
- **Concurrency partitioning**: cap a group or subpool to a slice of the pool (e.g. on a pool of
  100, give a group 10 and leave 90 for everything else).
- **Min/max workers**: the blocking pool runs a genuine core/max thread model with idle keep-alive.

## Quick start

```rust
use pond::Pool;

#[tokio::main]
async fn main() {
    let pool = Pool::builder().max_concurrency(4).build();

    let handle = pool.submit(|| async { 1 + 2 });
    assert_eq!(handle.await.unwrap(), 3);

    pool.stop_and_wait().await;
}
```

## Context, partitioning & worker limits

```rust
use std::time::Duration;
use pond::Pool;

#[tokio::main]
async fn main() {
    // Shut the pool down on an external signal or a deadline.
    let pool = Pool::builder()
        .max_concurrency(100)
        .deadline(Duration::from_secs(30))   // or .cancel_when(some_future)
        .build();

    // A group that may use at most 10 of the 100 workers (90 stay free).
    let mut group = pool.group_builder::<u64>().max_concurrency(10).build();
    for i in 0..50 { group.submit(move || async move { i * 2 }); }
    let _results = group.wait().await.unwrap();

    // A task that receives a context — break early when cancelled.
    let h = pool.submit_ctx(|ctx| async move {
        if ctx.is_cancelled() { return 0; }
        42
    });
    let _ = h.await;
}
```

```rust
use std::time::Duration;
use pond::blocking::Pool;

// Blocking pool with a real core/max thread model: 2 always-alive threads,
// growing to 16 under load, extra threads retiring after 30s idle.
let pool = Pool::builder()
    .min_concurrency(2)
    .max_concurrency(16)
    .keep_alive(Duration::from_secs(30))
    .build();
let handle = pool.submit(|| expensive_blocking_work());
let _ = handle.wait();
# fn expensive_blocking_work() -> u32 { 1 }
```

## Cargo features

| Feature             | Default | Description                                                        |
| ------------------- | ------- | ----------------------------------------------------------------- |
| `runtime-tokio`     | yes     | Tokio adapter (`TokioSpawner`).                                   |
| `runtime-smol`      | no      | smol adapter (`SmolSpawner`), backed by `async-global-executor`. |
| `runtime-async-std` | no      | `async-std` adapter (`AsyncStdSpawner`).                         |
| `blocking`          | yes     | Synchronous, thread-based `blocking::Pool`.                       |
| `tracing`           | no      | Debug/monitoring: emit `tracing` spans + events (zero cost when off). |
| `tokio-console`     | no      | Live async task monitoring via tokio-console (needs `tokio_unstable`). |
| `metrics-export`    | no      | `MetricsSnapshot::to_prometheus_text` (Prometheus exposition; no extra deps). |

Pick a non-default runtime with `--no-default-features` plus the runtime you want, then build the
pool with `Pool::builder_with`:

```rust
use pond::{Pool, SmolSpawner};

let pool = Pool::builder_with(SmolSpawner::new()).max_concurrency(4).build();
let result = futures::executor::block_on(async { pool.submit(|| async { 1 + 2 }).await.unwrap() });
assert_eq!(result, 3);
```

> **Embassy is intentionally not supported.** Its executor uses statically allocated, compile-time
> fixed task pools (no dynamic/boxed-future spawning) and no `spawn_blocking`, which is incompatible
> with this pool's runtime-agnostic `Spawner` model. It targets `no_std` embedded; pond is `std`.

## Monitoring & debugging

Enable the **`tracing`** feature to observe task timing and pool behavior in tests
and while debugging. The pools then emit `tracing` spans (`target = "pond"`):
`pond.task` (one per task — its duration is the task's wall-clock time),
`pond.submit` / `pond.try_submit` / `pond.submit_async`, `pond.group.wait`,
`pond.stop`, and `pond.resize`. Attach any subscriber:

```bash
cargo run --example tracing_debug --features tracing
```

```rust
tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();
```

For a **live** view of tasks (poll counts, busy/idle time), use tokio-console:

```bash
RUSTFLAGS="--cfg tokio_unstable" cargo run --features tokio-console
# in your code, once: pond::monitor::init_console();
```

> Leave both features **off in production** — with the feature disabled every
> instrumentation site is compiled out, so there is no runtime cost. (This is the
> Rust compiler removing the debug code, exactly as intended.)

## Development

```bash
cargo test --all-features          # run the test suite
cargo clippy --all-targets --all-features -- -D warnings   # lint
cargo fmt --all                    # format
cargo bench                        # run throughput benchmarks (Criterion)
```

Lint configuration lives in `[lints]` in `Cargo.toml` (forbids `unsafe`, warns on missing docs /
unreachable `pub`, enables `clippy::all`) plus `clippy.toml` (MSRV).

Benchmarks (Criterion):

- `benches/pool_bench.rs` — raw submit/await throughput for the async and blocking pools.
- `benches/workload_bench.rs` — representative workloads: CPU-bound tasks, concurrency scaling,
  I/O-like yielding tasks, and multi-producer contention.

Realistic, scenario-based tests modeled on a database connection pool (capped concurrent queries,
scatter/gather, transaction abort-on-error, backpressure, per-tenant subpool isolation) live in
`tests/workloads.rs`.

### Release builds

For best runtime performance in your own application, enable cross-crate optimization in your
**binary's** `Cargo.toml` (a library's profiles are ignored when it is used as a dependency):

```toml
[profile.release]
lto = "thin"        # or "fat" for maximum inlining at the cost of compile time
codegen-units = 1
```

> **Do not set `panic = "abort"`** if you rely on `pond`'s panic recovery (the default): recovery
> uses stack unwinding (`catch_unwind`), which `panic = "abort"` disables — panicking tasks would
> then abort the whole process instead of surfacing `Error::Panicked`.

## License

MIT
