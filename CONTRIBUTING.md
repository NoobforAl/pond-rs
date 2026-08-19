# Contributing to pond-rs

Thanks for your interest in improving pond! This guide covers the local workflow,
project conventions, and a few decisions worth knowing before you open a PR.

## Development setup

A `Makefile` wraps the common cargo commands:

```bash
make test        # tests on the default feature set
make test-all    # tests + doctests on ALL features (every runtime)
make clippy      # clippy with warnings denied
make fmt-check   # formatting check
make doc         # build the API docs
make all         # fmt-check + clippy + test-all (run this before pushing)
```

Reports & profiling (saved to files, opened in your browser):

```bash
make bench         # run Criterion benchmarks
make report        # open the Criterion HTML report (target/criterion/report/)
make coverage      # HTML coverage report (needs: cargo install cargo-llvm-cov)
make flamegraph    # CPU flamegraph of the benches (needs: cargo install flamegraph)
```

## Feature flags

The crate is split into opt-in features (see `Cargo.toml` and the README table):

- Runtime adapters — exactly the async runtime the `Pool` spawns on:
  `runtime-tokio` (default), `runtime-smol`, `runtime-async-std`.
- `blocking` — the synchronous `blocking::Pool` (default).

The default feature set used throughout CI is `runtime-tokio,blocking`.

## Adding a new runtime adapter

The async pool is generic over the public `Spawner` trait (`src/runtime.rs`):
`spawn`, `spawn_blocking`, and `sleep` (which has a runtime-agnostic default). To
support a new runtime:

1. Add `src/runtime/<name>.rs` implementing `Spawner` for a `<Name>Spawner`.
2. Gate the module and its re-export behind a `runtime-<name>` feature in
   `src/runtime.rs`, and re-export the spawner from `src/lib.rs`.
3. Add the feature and optional dependency to `Cargo.toml`, and a CI step.

> **Embassy is intentionally unsupported.** Its statically-allocated, fixed-size
> task model has no dynamic boxed-future spawning and no `spawn_blocking`, so it
> cannot satisfy `Spawner`. It targets `no_std` embedded; pond is `std`.

## Testing

- Unit tests live in `#[cfg(test)]` modules inside `src/`. Use the runtime-free
  `MockSpawner` (`tests/common/mock_spawner.rs`) and `Pool::builder_with` to test
  pool logic without Tokio.
- Integration tests live in `tests/`; shared helpers (e.g. `wait_until`,
  `ConcurrencyTracker`) are in `tests/common/`.
- Prefer deterministic synchronization (channels, `Notify`, `tokio::test`
  `start_paused`, and `wait_until` polling) over fixed sleeps.
- Run doctests with `cargo test --doc --features runtime-tokio,blocking`.
- Property tests (counter invariants) use `proptest`; never `await` inside a
  `proptest!` closure — drive any async via `Runtime::block_on`.

### Why no `loom`?

We deliberately do not use the `loom` concurrency model checker. The crate's own
atomics are few and simple (independent counters / flags with documented
orderings); the real concurrency flows through `async-channel` and
`event-listener`, which are already loom-tested upstream. Revisit only if a
concurrency bug escapes the unit, integration, and property tests.

## Benchmarks

Criterion benches live in `benches/`. Run them with `make bench` (or
`cargo bench`). `benches/pool_bench.rs` includes a raw `tokio::spawn` baseline so
pool overhead is measurable.

## MSRV

The minimum supported Rust version is **1.75** (`rust-version` in `Cargo.toml`,
also pinned in `clippy.toml`). CI verifies a build on 1.75. Bumping the MSRV is a
notable change — call it out in the PR and `CHANGELOG.md`.

## Pull requests

1. `make all` passes (fmt, clippy with `-D warnings`, tests).
2. `cargo doc --no-deps` is warning-free (CI builds docs with `-D warnings`).
3. New public items have doc comments (`missing_docs` is a warning, denied in CI).
4. Update `CHANGELOG.md` under `[Unreleased]`.
5. No `unsafe` — the crate sets `unsafe_code = "forbid"`.
