//! Task groups: submit many related tasks and wait for them together.
//!
//! * [`ResultTaskGroup`] collects task outputs **in submission order**.
//! * [`TryTaskGroup`] additionally short-circuits: the first failing task (a
//!   returned `Err` or a panic) cancels the remaining *queued* tasks (in-flight
//!   tasks are allowed to finish).
//!
//! A group owns its own [`CancelToken`], so cancelling a group never affects the
//! parent pool.

use std::future::Future;
use std::marker::PhantomData;

use crate::cancel::CancelToken;
use crate::context::TaskContext;
use crate::error::Error;
use crate::pool::{Pool, TaskHandle};
use crate::runtime::Spawner;
use crate::submit::TaskSink;
use crate::subpool::Subpool;

/// What happened to a single group task once a worker reached it.
enum Outcome<V> {
    /// The task ran and produced a value.
    Done(V),
    /// The group was cancelled before this task started, so it was skipped.
    Skipped,
}

/// Error returned by [`TryTaskGroup::wait`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupError<E> {
    /// A task returned this error.
    Task(E),
    /// A task failed at the pool level (e.g. panicked or was cancelled).
    Pool(Error),
}

impl<E: std::fmt::Display> std::fmt::Display for GroupError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupError::Task(e) => write!(f, "task error: {e}"),
            GroupError::Pool(e) => write!(f, "{e}"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for GroupError<E> {}

/// A group of tasks whose outputs are collected in submission order.
///
/// Create one with [`Pool::group`].
pub struct ResultTaskGroup<P: TaskSink, T> {
    pool: P,
    token: CancelToken,
    handles: Vec<TaskHandle<Outcome<T>>>,
}

impl<P: TaskSink, T: Send + 'static> ResultTaskGroup<P, T> {
    pub(crate) fn new(pool: P) -> Self {
        Self::new_with_token(pool, CancelToken::new())
    }

    pub(crate) fn new_with_token(pool: P, token: CancelToken) -> Self {
        Self {
            pool,
            token,
            handles: Vec::new(),
        }
    }

    /// Submits a task to the group. Chainable.
    pub fn submit<F, Fut>(&mut self, task: F) -> &mut Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let token = self.token.clone();
        let handle = self.pool.submit(move || async move {
            if token.is_cancelled() {
                Outcome::Skipped
            } else {
                Outcome::Done(task().await)
            }
        });
        self.handles.push(handle);
        self
    }

    /// Submits a task that receives a [`TaskContext`] bound to the group's
    /// cancel token (so it can observe group cancellation). Chainable.
    pub fn submit_ctx<F, Fut>(&mut self, task: F) -> &mut Self
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let ctx = TaskContext::new(self.token.clone(), self.pool.task_data());
        self.submit(move || task(ctx))
    }

    /// Returns the group's [`CancelToken`].
    pub fn cancel_token(&self) -> CancelToken {
        self.token.clone()
    }

    /// Cancels the group: queued tasks are skipped; running tasks finish.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Waits for all tasks, returning their outputs in submission order.
    ///
    /// If any task panics or is cancelled, the group is cancelled and the first
    /// such [`Error`] is returned.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(target = "pond", name = "pond.group.wait", level = "debug", skip_all)
    )]
    pub async fn wait(self) -> Result<Vec<T>, Error> {
        let mut out = Vec::with_capacity(self.handles.len());
        let mut first_err: Option<Error> = None;
        for handle in self.handles {
            match handle.await {
                Ok(Outcome::Done(v)) => {
                    if first_err.is_none() {
                        out.push(v);
                    }
                }
                Ok(Outcome::Skipped) => {}
                Err(e) => {
                    if first_err.is_none() {
                        self.token.cancel();
                        first_err = Some(e);
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(out),
        }
    }
}

/// A group of fallible tasks that short-circuits on the first error.
///
/// Create one with [`Pool::try_group`].
pub struct TryTaskGroup<P: TaskSink, T, E> {
    pool: P,
    token: CancelToken,
    #[allow(clippy::type_complexity)]
    handles: Vec<TaskHandle<Outcome<Result<T, E>>>>,
}

impl<P: TaskSink, T: Send + 'static, E: Send + 'static> TryTaskGroup<P, T, E> {
    pub(crate) fn new(pool: P) -> Self {
        Self::new_with_token(pool, CancelToken::new())
    }

    pub(crate) fn new_with_token(pool: P, token: CancelToken) -> Self {
        Self {
            pool,
            token,
            handles: Vec::new(),
        }
    }

    /// Submits a fallible task to the group. Chainable.
    pub fn submit<F, Fut>(&mut self, task: F) -> &mut Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let token = self.token.clone();
        let handle = self.pool.submit(move || async move {
            if token.is_cancelled() {
                Outcome::Skipped
            } else {
                Outcome::Done(task().await)
            }
        });
        self.handles.push(handle);
        self
    }

    /// Submits a fallible task that receives a [`TaskContext`] bound to the
    /// group's cancel token. Chainable.
    pub fn submit_ctx<F, Fut>(&mut self, task: F) -> &mut Self
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let ctx = TaskContext::new(self.token.clone(), self.pool.task_data());
        self.submit(move || task(ctx))
    }

    /// Returns the group's [`CancelToken`].
    pub fn cancel_token(&self) -> CancelToken {
        self.token.clone()
    }

    /// Cancels the group: queued tasks are skipped; running tasks finish.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Waits for all tasks. Returns their outputs in submission order, or the
    /// first error encountered (after cancelling the remaining queued tasks).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            target = "pond",
            name = "pond.try_group.wait",
            level = "debug",
            skip_all
        )
    )]
    pub async fn wait(self) -> Result<Vec<T>, GroupError<E>> {
        let mut out = Vec::with_capacity(self.handles.len());
        let mut first_err: Option<GroupError<E>> = None;
        for handle in self.handles {
            match handle.await {
                Ok(Outcome::Done(Ok(v))) => {
                    if first_err.is_none() {
                        out.push(v);
                    }
                }
                Ok(Outcome::Done(Err(e))) => {
                    if first_err.is_none() {
                        self.token.cancel();
                        first_err = Some(GroupError::Task(e));
                    }
                }
                Ok(Outcome::Skipped) => {}
                Err(pe) => {
                    if first_err.is_none() {
                        self.token.cancel();
                        first_err = Some(GroupError::Pool(pe));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(out),
        }
    }
}

/// Builder for a [`ResultTaskGroup`] with a concurrency cap and/or an external
/// cancel token.
///
/// A capped group runs on a dedicated [`Subpool`] of the parent pool, so it can
/// only use up to `max_concurrency` of the parent's workers — e.g. on a pool of
/// 100, `max_concurrency(10)` leaves 90 workers for everything else.
pub struct GroupBuilder<S: Spawner, T> {
    parent: Pool<S>,
    max_concurrency: usize,
    cancel_token: Option<CancelToken>,
    _marker: PhantomData<fn() -> T>,
}

impl<S: Spawner, T: Send + 'static> GroupBuilder<S, T> {
    pub(crate) fn new(parent: Pool<S>) -> Self {
        Self {
            parent,
            max_concurrency: 0,
            cancel_token: None,
            _marker: PhantomData,
        }
    }

    /// Caps how many of the group's tasks may run at once (`0` = inherit the
    /// parent's limit).
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max;
        self
    }

    /// Uses an external [`CancelToken`] for the group (e.g. to tie it to a
    /// request context). The analogue of Go pond's `NewGroupContext`.
    pub fn cancel_token(mut self, token: CancelToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// Builds the group.
    pub fn build(self) -> ResultTaskGroup<Subpool<S>, T> {
        let sub = Subpool::from_parts(
            self.parent,
            self.max_concurrency,
            usize::MAX,
            false,
            CancelToken::new(),
        );
        let token = self.cancel_token.unwrap_or_default();
        ResultTaskGroup::new_with_token(sub, token)
    }
}

/// Builder for a [`TryTaskGroup`] with a concurrency cap and/or an external
/// cancel token. See [`GroupBuilder`].
pub struct TryGroupBuilder<S: Spawner, T, E> {
    parent: Pool<S>,
    max_concurrency: usize,
    cancel_token: Option<CancelToken>,
    _marker: PhantomData<fn() -> (T, E)>,
}

impl<S: Spawner, T: Send + 'static, E: Send + 'static> TryGroupBuilder<S, T, E> {
    pub(crate) fn new(parent: Pool<S>) -> Self {
        Self {
            parent,
            max_concurrency: 0,
            cancel_token: None,
            _marker: PhantomData,
        }
    }

    /// Caps how many of the group's tasks may run at once (`0` = inherit).
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max;
        self
    }

    /// Uses an external [`CancelToken`] for the group.
    pub fn cancel_token(mut self, token: CancelToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// Builds the group.
    pub fn build(self) -> TryTaskGroup<Subpool<S>, T, E> {
        let sub = Subpool::from_parts(
            self.parent,
            self.max_concurrency,
            usize::MAX,
            false,
            CancelToken::new(),
        );
        let token = self.cancel_token.unwrap_or_default();
        TryTaskGroup::new_with_token(sub, token)
    }
}
