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
- **Subpools no longer starve their parent.** A queued subpool task used to hold a
  parent permit while it waited for a subpool permit, so a small subpool with a
  backlog could occupy every parent worker. A task now takes its subpool permit
  first and holds nothing of the parent's until then.
- **Subpool `stop_and_wait` no longer hangs** when the parent silently discarded a
  task (parent stopped, cancelled, or its queue full), or after a panic in the
  synchronous part of a task's closure. Every admitted task now has one accounting
  guard, also on the async `Pool`, so a task dropped by a runtime shutting down
  can't wedge `stop_and_wait` either.
- **`resize` keeps its limits.** `resize(0)` now releases every queued task, not
  one, and `resize(n)` after `resize(0)` no longer leaves extra permits behind (a
  limit of 2 could run 5 tasks at once). Permits are now counted by a shared
  semaphore instead of stored as channel tokens, replacing the duplicated logic in
  `Pool` and `Subpool`.
- **Task groups short-circuit when a task fails, not when `wait` reaches it.**
  Previously a failure behind a slow task let every queued task run anyway. Applies
  to async and blocking groups, for returned errors and panics alike.
- **Panics in the synchronous part of a task's closure** (before it returns its
  future) are now reported as `Error::Panicked` instead of `Error::Canceled`.
- **Background watcher leak:** a `Pool` or `Subpool` dropped before its watcher
  first ran left that task parked forever.
- **Delayed work observes shutdown promptly:** `submit_after`, `submit_at`,
  `schedule_every` and retry backoff now end as soon as the pool stops, instead of
  sitting out the rest of the delay.
- **Blocking pool:**
  - With `panic_recovery(false)`, a worker lost to a panic is now replaced. Before,
    the pool lost that capacity for good, and a one-thread pool hung forever.
  - A `submit` blocked on a full queue while the pool stopped could enqueue its job
    after the workers had gone, hanging `wait()`. It now gets `PoolStopped`.
  - The pool now grows when a second task arrives before the only idle worker has
    woken for the first, and can no longer overshoot `max_concurrency` under
    concurrent submitters.
  - Dropping the last handle now shuts the pool down in the background (queued
    tasks still run) rather than leaking its threads.
- Doc corrections: `Pool::subpool(0)` inherits the parent's limit (it never
  panicked), and `ResultPool::pool` no longer suggests metrics, which aren't public.
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
- Tokio 1.54 raised its minimum to Rust 1.85. pond's MSRV stays 1.75, but on Rust
  older than 1.85 Tokio must be pinned: `cargo update -p tokio --precise 1.53.1`.
- Bumped `rusqlite` (dev-only) to 0.41, and CI to `actions/checkout` v7 and
  `codecov/codecov-action` v7.
- **`queue_size` now counts only tasks waiting behind busy workers.** A burst that
  free workers can absorb is no longer rejected with `QueueFull` (10 idle workers
  and `queue_size(1)` used to reject the second task of a burst), and
  `submit_async` now wakes when a worker frees up. As a result `queue_size(0)` works
  as "no queue": async pools run a task if a worker is free and otherwise reject,
  or wait in `submit_async`, which used to hang. Blocking `submit` waits for a free
  worker; on blocking pools `queue_size(0)` used to be silently unbounded.
- **Cancelled groups report it.** `wait()` now returns the cancellation cause if
  the group was cancelled and any task was skipped, as documented, instead of
  `Ok` with the skipped results silently missing.
- **`Subpool` follows its parent's shutdown.** `is_stopped()` is now also `true`
  once the parent is stopped, and new submissions are rejected right away with the
  parent's stop error. Queued subpool tasks count towards the parent's graceful
  stop, and a parent cancel abandons them with its cause.
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
