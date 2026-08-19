# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Performance
- **One fewer heap allocation per submitted task.** `Spawner` gained a
  `spawn_unboxed` method (defaulted, so existing implementations are unaffected)
  that hands the runtime the concrete task future instead of a `Box`ed one. The
  runtime's own task cell now stores the future inline, cutting per-task
  allocations from 3.05 to 2.04 and removing a pointer indirection from every
  poll. Measured against raw `tokio::spawn` in the same run, pond's per-task
  overhead drops from ~317 ns to ~200 ns; `async_group` throughput improves
  ~26%.
- **Lock-free admission for unbounded queues.** `try_reserve_waiting` now takes
  a plain `fetch_add` when the queue is unbounded (the default) instead of the
  compare-exchange loop needed to enforce a bound, so concurrent submitters no
  longer retry against each other on a check that can never fail. Worth ~5% on
  the 8-producer `async_many_producers` bench; neutral on single-producer paths,
  as expected.
- **No `Arc` traffic for the per-task guards.** `PermitGuard`, `RunningGuard`
  and the subpool's `SubPermit` now borrow the pool's `Inner` (which the task
  future already owns for its whole life) instead of cloning an `Arc`, removing
  two atomic refcount round-trips per task.

### Added
- `tests/heavy_io.rs`: heavy-workload tests driving the async pool with real
  filesystem I/O and high task counts — concurrent file round-trips, the
  concurrency cap under real I/O (rather than a `sleep` stand-in), 20k tasks to
  completion and quiescence, ten reused waves (catches a per-wave permit leak),
  multi-megabyte task results, slow tasks not blocking fast ones, and a panic
  mid-I/O leaving the pool usable.
- `tests/sqlite_load.rs`: SQLite-under-load tests for the blocking pool,
  asserting data correctness rather than just throughput — concurrent
  read-modify-write loses no updates, bulk transactional inserts land exactly
  once, readers never observe a half-applied transaction, and a task that fails
  mid-transaction rolls back without leaking rows.
- `tests/heap_profile.rs`: an `#[ignore]`d dhat harness that writes a full
  allocation profile to `target/dhat-heap.json` (`make memprofile`), for
  attributing per-task heap cost. Complements the assertion-only `heap.rs`.

### Fixed
- Doctests now compile and pass under the non-Tokio runtime features
  (`runtime-smol`, `runtime-async-std`): `Pool::builder()` binds to the enabled
  runtime's global executor instead of existing only under `runtime-tokio`.
- **Lifecycle leak:** dropping an async `Pool`/`Subpool` without calling
  `stop()`/`cancel()` no longer leaks its `Inner` (permit channels, internal
  counters, shared data) and its background watcher task. The watcher now holds a
  `Weak` reference and is woken to terminate when the last handle drops.
- **Blocking pool thread leak:** non-core worker threads that retire on keep-alive
  no longer leave their `JoinHandle` behind; handles are reaped as threads exit.
- **Idle race:** `stop_and_wait()` could return before a just-dequeued task
  started running. Idle is now tracked by a single `outstanding` counter, closing
  the window.
- **Bounded-queue overshoot:** concurrent `submit`/`try_submit`/`submit_async`
  could admit past a bounded queue's capacity; admission is now an atomic
  reservation. `resize` is serialized so concurrent calls can't drift the permit
  budget.

### Changed
- **Bounded blocking pools now apply real backpressure:** a `queue_size`-bounded
  blocking `Pool` (not `non_blocking`) blocks `submit` until a queue slot frees,
  instead of silently ignoring the bound. `non_blocking` still drops with
  `QueueFull`, and unbounded pools are unchanged.
- Non-Tokio runtimes use native timers: the `runtime-smol` adapter sleeps via
  `async-io` and `runtime-async-std` via `async_std::task::sleep`, so scheduled
  work no longer spawns a blocking OS thread per sleep.
- Cache-line-pad the metric counters to avoid false sharing under many concurrent
  producers (measured ~3-4% throughput gain on the contention benchmark).

### Added
- `join_all(handles)` to await a `Vec<TaskHandle<T>>` and collect results in order.
- `RetryPolicy::max_backoff` (cap exponential growth) and `RetryPolicy::jitter`
  (equal jitter, no `rand` dependency); `Pool::submit_with_retry_if` to retry only
  errors matching a predicate.
- A many-producers contention benchmark and randomized concurrency stress tests
  (`tests/stress.rs`), plus lifecycle/leak regression tests (`tests/lifecycle.rs`).
- Builder config for streaming: a `ResultSink<T>` channel interface
  (impl'd for `async_channel::Sender`, extensible to any channel via a newtype);
  `Pool::results_to` → a typed `ResultPool` that streams each result to a sink;
  `GroupBuilder`/`TryGroupBuilder::result_channel` → a `StreamGroup` that streams
  results as they finish; and `PoolBuilder::pool_name` (plus a bulk `PoolConfig`
  via `.config(..)`). All runtime-agnostic; a `config_channels` example
  demonstrates them.
- Task scheduling & management on `Pool` (and the default-pool free functions):
  delayed (`submit_after`, `submit_at`), recurring (`schedule_every` → cancellable
  `ScheduledTask`), retry-on-failure (`submit_with_retry` + `RetryPolicy`), and
  replication — fan-out (`submit_n`) or a reusable `Job` template. All
  runtime-agnostic (built on `Spawner::sleep`), with a `scheduling` example.
- Additional async runtime adapters selectable by feature: `runtime-smol`
  (`SmolSpawner`, backed by `async-global-executor`) and `runtime-async-std`
  (`AsyncStdSpawner`), alongside the default `runtime-tokio`.
- `Spawner::sleep` so deadline-based cancellation (`PoolBuilder::deadline`) works
  on every runtime, not just Tokio.
- New examples: graceful shutdown, custom spawner, backpressure, panic recovery,
  cancel/deadline, dynamic resize, streaming results, smol runtime, and bounded
  concurrent HTTP requests (self-contained, no network).
- Shared integration-test utilities and a `MockSpawner` test double; unit tests
  for the pool, blocking pool, groups, and subpools; property tests for counter
  invariants.
- Leak/heap regression tests: `tests/leak.rs` (pool/subpool churn with `Weak`
  probes, fire-and-forget storm drain) and `tests/heap.rs` (dhat-based live-heap
  growth check across 100k tasks; dhat is a dev-dependency only). A
  `lifecycle_bench` Criterion benchmark for short-lived pool/subpool churn.
- Project hygiene: `CHANGELOG.md`, `CONTRIBUTING.md`, `deny.toml`, crates.io
  metadata, MSRV CI check, and `cargo test --doc` in CI.

### Security
- Pinned `event-listener >= 5.4.2` (fixes RUSTSEC-2026-0221, `!Send` tags
  crossing thread boundaries via `StackSlot`); dev-graph `crossbeam-epoch`
  bumped to 0.9.20 (RUSTSEC-2026-0204). `cargo audit` is now clean of
  vulnerabilities; the remaining advisory is the informational
  "async-std discontinued" warning on the opt-in `runtime-async-std` feature.

### Removed
- **Observability features and crates:** the `tracing`, `tokio-console`, `tracy`
  and `dhat-heap` features (and their optional dependencies) are gone, along
  with the `monitor` module, instrumentation sites and profiling examples. The
  crate now ships no monitoring/observability surface; instrument it from the
  caller if needed.
- **Public metrics API:** `MetricsSnapshot`, `Pool::metrics()`,
  `blocking::Pool::metrics()`, the per-counter accessors on pools/subpools, and
  `PoolBuilder::metrics_channel` (periodic snapshot export). The internal
  counters still drive bounded-queue admission and backpressure, but are no
  longer exposed.

## [0.1.0]

### Added
- Initial release: an idiomatic, runtime-agnostic worker pool for Rust.
- Async `Pool` with bounded concurrency over a pluggable `Spawner` (Tokio out of
  the box), awaitable task handles, task groups, subpools, dynamic resize,
  graceful shutdown, cancellation, deadlines, and metrics.
- Blocking `blocking::Pool` with a core/max thread model and idle keep-alive.
- Optional `tracing` instrumentation and `tokio-console` integration.

[Unreleased]: https://github.com/NoobforAl/pond-rs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/NoobforAl/pond-rs/releases/tag/v0.1.0
