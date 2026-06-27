//! A typed pool wrapper that streams task results to a [`ResultSink`].
//!
//! `Pool`/`PoolBuilder` are untyped over task result types (each `submit` has its
//! own `T`), so a typed *result channel* can't live on the builder directly.
//! [`ResultPool`] is the typed view: every task it runs produces the same `T`,
//! and each result is pushed to your channel as it completes — no handles to
//! collect, no `wait()`.

use std::future::Future;
use std::sync::Arc;

use crate::context::TaskContext;
use crate::pool::Pool;
use crate::runtime::Spawner;
use crate::sink::ResultSink;

impl<S: Spawner> Pool<S> {
    /// Returns a typed [`ResultPool`] that streams each task's result to `sink`.
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use pond::Pool;
    ///
    /// let pool = Pool::builder().max_concurrency(4).build();
    /// let (tx, rx) = async_channel::unbounded::<u64>();
    /// let typed = pool.results_to(tx);
    ///
    /// for i in 0..3 {
    ///     typed.submit(move || async move { i * i });
    /// }
    /// let mut got = vec![rx.recv().await.unwrap(), rx.recv().await.unwrap(), rx.recv().await.unwrap()];
    /// got.sort_unstable();
    /// assert_eq!(got, vec![0, 1, 4]);
    /// # }
    /// ```
    pub fn results_to<T, K>(&self, sink: K) -> ResultPool<S, T>
    where
        T: Send + 'static,
        K: ResultSink<T>,
    {
        ResultPool {
            pool: self.clone(),
            sink: Arc::new(sink),
        }
    }
}

/// A typed pool whose task results stream to a [`ResultSink`]. Create one with
/// [`Pool::results_to`]. Cloning is cheap (shares the pool and the sink).
pub struct ResultPool<S: Spawner, T> {
    pool: Pool<S>,
    sink: Arc<dyn ResultSink<T>>,
}

impl<S: Spawner, T> Clone for ResultPool<S, T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            sink: self.sink.clone(),
        }
    }
}

impl<S: Spawner, T: Send + 'static> ResultPool<S, T> {
    /// Submits a task; its result is sent to the sink on completion
    /// (fire-and-forget — there is no handle to await).
    pub fn submit<F, Fut>(&self, task: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let sink = self.sink.clone();
        self.pool.spawn(move || async move {
            let value = task().await;
            sink.send(value);
        });
    }

    /// Like [`submit`](Self::submit), but the task receives a [`TaskContext`].
    pub fn submit_ctx<F, Fut>(&self, task: F)
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let sink = self.sink.clone();
        self.pool.spawn_ctx(move |ctx| async move {
            let value = task(ctx).await;
            sink.send(value);
        });
    }

    /// The underlying pool, e.g. for metrics or shutdown.
    pub fn pool(&self) -> &Pool<S> {
        &self.pool
    }
}
