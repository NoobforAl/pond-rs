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
