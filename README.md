# pond (Rust)

An idiomatic, runtime-agnostic worker pool for Rust — a from-scratch rewrite of the Go
[`pond`](https://github.com/alitto/pond) library.

> **Status:** under active development (Rust port). The original Go sources remain in this
> repository for reference until the Rust implementation reaches parity.

## Features

- **Async pool** that runs futures with bounded concurrency on a pluggable runtime.
- **Blocking pool** that runs synchronous closures on OS threads (no async runtime needed).
- **Pluggable runtime** via the `Spawner` trait — Tokio supported out of the box behind the
  default `runtime-tokio` feature; other runtimes (async-std, smol, …) can be added later.
- Task **handles** (awaitable futures), **task groups**, **subpools**, dynamic **resize**,
  graceful **shutdown**, **cancellation**, and rich **metrics**.

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

## Cargo features

| Feature         | Default | Description                                                        |
| --------------- | ------- | ----------------------------------------------------------------- |
| `runtime-tokio` | yes     | Tokio adapter (`TokioSpawner`).                                   |
| `blocking`      | yes     | Synchronous, thread-based `blocking::Pool`.                       |
| `tracing`       | no      | Debug/monitoring: emit `tracing` spans + events (zero cost when off). |
| `tokio-console` | no      | Live async task monitoring via tokio-console (needs `tokio_unstable`). |

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
