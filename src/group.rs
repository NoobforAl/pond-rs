//! Task groups: submit many related tasks and wait for them together.
//!
//! Mirrors pond's `TaskGroup` / `ResultTaskGroup`:
//!
//! * [`ResultTaskGroup`] collects task outputs **in submission order**.
//! * [`TryTaskGroup`] additionally short-circuits: the first failing task (a
//!   returned `Err` or a panic) cancels the remaining *queued* tasks (in-flight
//!   tasks are allowed to finish, just like pond).
//!
//! A group owns its own [`CancelToken`], so cancelling a group never affects the
//! parent pool.

use std::future::Future;

use crate::cancel::CancelToken;
use crate::error::Error;
use crate::pool::{Pool, TaskHandle};
use crate::runtime::Spawner;

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
pub struct ResultTaskGroup<S: Spawner, T> {
    pool: Pool<S>,
    token: CancelToken,
    handles: Vec<TaskHandle<Outcome<T>>>,
}

impl<S: Spawner, T: Send + 'static> ResultTaskGroup<S, T> {
    pub(crate) fn new(pool: Pool<S>) -> Self {
        Self {
            pool,
            token: CancelToken::new(),
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
pub struct TryTaskGroup<S: Spawner, T, E> {
    pool: Pool<S>,
    token: CancelToken,
    #[allow(clippy::type_complexity)]
    handles: Vec<TaskHandle<Outcome<Result<T, E>>>>,
}

impl<S: Spawner, T: Send + 'static, E: Send + 'static> TryTaskGroup<S, T, E> {
    pub(crate) fn new(pool: Pool<S>) -> Self {
        Self {
            pool,
            token: CancelToken::new(),
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
