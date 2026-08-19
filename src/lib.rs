//! # pond
//!
//! An idiomatic, runtime-agnostic worker pool for Rust.
//!
//! Inspired by Pond, a Go worker-pool library — this is an independent Rust
//! project, not a binding or a port.
//!
//! pond provides two flavours of pool that share the same concepts (bounded
//! concurrency, task handles, groups, subpools, graceful shutdown):
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
//!
//! ## Architecture
//!
//! A few internal design notes for contributors and the curious:
//!
//! * **Concurrency is a permit channel, not a counter.** A bounded pool fills an
//!   unbounded channel with `max_concurrency` `()` permits; each task must
//!   receive one before running and returns it (via an RAII guard) when done.
//!   This makes "wait for a free slot" a plain channel `recv().await` — no
//!   condition variables — and makes [`resize`](Pool::resize) just add or drain
//!   permits.
//! * **Subpools layer a second gate.** A [`Subpool`] runs on its parent's workers
//!   but holds its own permit channel, so a task must acquire *both* a subpool
//!   permit and a parent permit. That is how a subpool caps its share of the
//!   parent without a separate thread pool.
//! * **One submission path, abstracted by `TaskSink`.** [`Pool`] and [`Subpool`]
//!   both implement [`TaskSink`], so task groups ([`ResultTaskGroup`],
//!   [`TryTaskGroup`]) are generic over where they submit.
//! * **Cancellation is an `event-listener` broadcast.** [`CancelToken`] flips an
//!   atomic and wakes all waiters; the pool spawns one watcher task that reacts
//!   by closing the permit channels and notifying idle/slot listeners. That
//!   watcher holds only a `Weak` reference to the pool's shared state, so
//!   **dropping the last [`Pool`] handle frees it and stops the watcher** even
//!   if you never call `stop`/`cancel`.
//! * **Internal counters are independent atomics.** Because no counter publishes
//!   other memory, `Relaxed` ordering is correct and cheapest.
//!
//! The async [`Pool`] never assumes Tokio: every runtime touchpoint goes through
//! the [`Spawner`] trait, so the same logic runs on Tokio, smol, or async-std.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod cancel;
mod context;
mod error;
mod metrics;
mod runtime;

pub use crate::cancel::CancelToken;
pub use crate::context::TaskContext;
pub use crate::error::{Error, Result};
pub use crate::runtime::Spawner;

#[cfg(feature = "runtime-tokio")]
pub use crate::runtime::TokioSpawner;

#[cfg(feature = "runtime-smol")]
pub use crate::runtime::SmolSpawner;

#[cfg(feature = "runtime-async-std")]
pub use crate::runtime::AsyncStdSpawner;

// Async pool surface.
mod group;
mod join;
mod options;
mod pool;
mod result_pool;
mod schedule;
mod sink;
mod submit;
mod subpool;
pub use crate::group::{
    GroupBuilder, GroupError, ResultTaskGroup, StreamGroup, TryGroupBuilder, TryTaskGroup,
};
pub use crate::join::join_all;
pub use crate::options::{PoolBuilder, PoolConfig, QueueSize};
pub use crate::pool::{Pool, StopHandle, TaskHandle};
pub use crate::result_pool::ResultPool;
pub use crate::schedule::{Job, RetryPolicy, ScheduledTask};
pub use crate::sink::ResultSink;
pub use crate::submit::TaskSink;
pub use crate::subpool::{Subpool, SubpoolBuilder};

// Process-wide default pool (Tokio only).
#[cfg(feature = "runtime-tokio")]
mod default;
#[cfg(feature = "runtime-tokio")]
pub use crate::default::{
    default_pool, group, schedule_every, submit, submit_after, submit_at, submit_n,
    submit_with_retry, subpool, try_group,
};

// Synchronous, thread-based pool.
#[cfg(feature = "blocking")]
#[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
pub mod blocking;
