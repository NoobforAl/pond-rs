//! Internal instrumentation macros.
//!
//! These expand to `tracing` calls when the `tracing` feature is enabled, and to
//! nothing otherwise — so instrumentation has zero cost in production builds.

/// Emit a `trace`-level event under the `pond` target (no-op without `tracing`).
macro_rules! trace_event {
    ($($arg:tt)*) => {{
        #[cfg(feature = "tracing")]
        {
            tracing::trace!(target: "pond", $($arg)*);
        }
    }};
}

/// Emit a `warn`-level event under the `pond` target (no-op without `tracing`).
///
/// For conditions an operator should see in production logs, such as a task
/// being dropped because the queue is full.
macro_rules! warn_event {
    ($($arg:tt)*) => {{
        #[cfg(feature = "tracing")]
        {
            tracing::warn!(target: "pond", $($arg)*);
        }
    }};
}
