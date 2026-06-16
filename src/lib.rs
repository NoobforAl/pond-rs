//! # pond
//!
//! An idiomatic, runtime-agnostic worker pool for Rust.
//!
//! Inspired by Pond, a Go worker-pool library — this is an independent Rust
//! project, not a binding or a port.
//!
//! pond provides two flavours of pool that share the same concepts (bounded
//! concurrency, task handles, groups, subpools, metrics, graceful shutdown):
//!
//! * an **async** [`Pool`] that runs futures with bounded concurrency on a
//!   pluggable async runtime (Tokio out of the box), and
//! * a **blocking** [`blocking::Pool`] that runs synchronous closures on a pool
//!   of OS threads (no async runtime required).
//!
//! ## Runtime abstraction
//!
//! The async [`Pool`] is generic over a [`Spawner`]. A [`TokioSpawner`] ships
//! behind the default `runtime-tokio` feature; other runtimes can be supported
//! by implementing [`Spawner`].
//!
//! ## Example
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use pond::Pool;
//!
//! let pool = Pool::builder().max_concurrency(4).build();
//! let handle = pool.submit(|| async { 1 + 2 });
//! assert_eq!(handle.await.unwrap(), 3);
//! pool.stop_and_wait().await;
//! # }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]

// The tokio-console integration relies on Tokio's unstable task instrumentation,
// which is only available when the whole build sets `--cfg tokio_unstable`.
#[cfg(all(feature = "tokio-console", not(tokio_unstable)))]
compile_error!(
    "the `tokio-console` feature requires building with RUSTFLAGS=\"--cfg tokio_unstable\""
);

#[macro_use]
mod macros;

mod cancel;
mod context;
mod error;
mod metrics;
mod runtime;

pub use crate::cancel::CancelToken;
pub use crate::context::TaskContext;
pub use crate::error::{Error, Result};
pub use crate::metrics::MetricsSnapshot;
pub use crate::runtime::Spawner;

#[cfg(feature = "runtime-tokio")]
pub use crate::runtime::TokioSpawner;

// Async pool surface.
mod group;
mod options;
mod pool;
mod submit;
mod subpool;
pub use crate::group::{GroupBuilder, GroupError, ResultTaskGroup, TryGroupBuilder, TryTaskGroup};
pub use crate::options::{PoolBuilder, QueueSize};
pub use crate::pool::{Pool, StopHandle, TaskHandle};
pub use crate::submit::TaskSink;
pub use crate::subpool::{Subpool, SubpoolBuilder};

// Process-wide default pool (Tokio only).
#[cfg(feature = "runtime-tokio")]
mod default;
#[cfg(feature = "runtime-tokio")]
pub use crate::default::{default_pool, group, submit, subpool, try_group};

// Synchronous, thread-based pool.
#[cfg(feature = "blocking")]
#[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
pub mod blocking;

// Debug/monitoring helpers (tracing spans + tokio-console).
#[cfg(feature = "tracing")]
#[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
pub mod monitor;
