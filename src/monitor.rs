//! Debug/monitoring helpers (enabled by the `tracing` feature).
//!
//! When the `tracing` feature is on, the pools emit [`tracing`] spans and events
//! describing task lifecycle and timing. Attach any `tracing` subscriber to view
//! them — for example, in a test:
//!
//! ```ignore
//! tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();
//! ```
//!
//! Spans emitted (all at `debug`/`trace` level, `target = "pond"`):
//! - `pond.submit`, `pond.try_submit`, `pond.submit_async` — submission paths
//! - `pond.task` — wraps a single task's execution (its duration is the task's
//!   wall-clock time, including time spent awaiting a concurrency permit)
//! - `pond.group.wait`, `pond.stop`, `pond.resize` — lifecycle operations
//!
//! With the feature **off**, none of this is compiled in: every instrumentation
//! site disappears and there is no runtime cost.

/// Initializes the [tokio-console](https://docs.rs/console-subscriber) subscriber
/// so you can watch tasks live with the `tokio-console` CLI.
///
/// Requires the `tokio-console` feature and building with
/// `RUSTFLAGS="--cfg tokio_unstable"`.
#[cfg(feature = "tokio-console")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-console")))]
pub fn init_console() {
    console_subscriber::init();
}
