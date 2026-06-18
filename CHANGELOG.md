# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
