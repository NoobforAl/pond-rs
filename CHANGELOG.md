# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Lifecycle leak:** dropping an async `Pool`/`Subpool` without calling
  `stop()`/`cancel()` no longer leaks its `Inner` (permit channels, metrics,
  shared data) and its background watcher/metrics tasks. The watchers now hold a
  `Weak` reference and are woken to terminate when the last handle drops.
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
- Builder config for streaming & monitoring: a `ResultSink<T>` channel interface
  (impl'd for `async_channel::Sender`, extensible to any channel via a newtype);
  `Pool::results_to` → a typed `ResultPool` that streams each result to a sink;
  `GroupBuilder`/`TryGroupBuilder::result_channel` → a `StreamGroup` that streams
  results as they finish; and `PoolBuilder::metrics_channel` / `pool_name` (plus a
  bulk `PoolConfig` via `.config(..)`) to push periodic `MetricsSnapshot`s to a
  channel. All runtime-agnostic; a `config_channels` example demonstrates them.
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
- `metrics-export` feature with `MetricsSnapshot::to_prometheus_text`.
- New examples: graceful shutdown, custom spawner, backpressure, panic recovery,
  cancel/deadline, dynamic resize, streaming results, smol runtime, metrics
  export, and bounded concurrent HTTP requests (self-contained, no network).
- Shared integration-test utilities and a `MockSpawner` test double; unit tests
  for the pool, blocking pool, groups, and subpools; property tests for metrics
  invariants.
- Tracing spans for permit-wait latency and pool cancellation; a warning event on
  queue-full drops.
- Project hygiene: `CHANGELOG.md`, `CONTRIBUTING.md`, `deny.toml`, crates.io
  metadata, MSRV CI check, and `cargo test --doc` in CI.

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
