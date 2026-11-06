//! Task groups for the blocking [`Pool`](super::Pool).
//!
//! The synchronous counterparts of [`ResultTaskGroup`](crate::ResultTaskGroup) /
//! [`TryTaskGroup`](crate::TryTaskGroup): submit many related blocking tasks and
//! collect their results in submission order, with first-error short-circuiting.

use crate::cancel::{CancelOnDrop, CancelToken};
use crate::error::Error;
use crate::group::GroupError;

use super::{BlockingTaskHandle, Pool};

/// What happened to a single group task once a worker reached it.
enum Outcome<V> {
    Done(V),
    Skipped,
}

/// A group of blocking tasks whose outputs are collected in submission order.
///
/// Create one with [`Pool::group`](super::Pool::group).
pub struct BlockingResultGroup<T> {
    pool: Pool,
    token: CancelToken,
    handles: Vec<BlockingTaskHandle<Outcome<T>>>,
}

impl<T: Send + 'static> BlockingResultGroup<T> {
    pub(crate) fn new(pool: Pool) -> Self {
        Self {
            pool,
            token: CancelToken::new(),
            handles: Vec::new(),
        }
    }

    /// Submits a task to the group. Chainable.
    pub fn submit<F>(&mut self, task: F) -> &mut Self
    where
        F: FnOnce() -> T + Send + 'static,
    {
        let token = self.token.clone();
        let handle = self.pool.submit(move || {
            if token.is_cancelled() {
                return Outcome::Skipped;
            }
            // A panic unwinds through this and cancels the group immediately.
            let on_panic = CancelOnDrop(&token);
            let value = task();
            std::mem::forget(on_panic);
            Outcome::Done(value)
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
    /// A panicking task cancels the group as soon as it happens, whether or not
    /// `wait` is running yet. If any task panicked, the error of the earliest one
    /// in submission order is returned. Otherwise, if the group was cancelled and
    /// any task was skipped as a result, the cancellation cause is returned.
    pub fn wait(self) -> Result<Vec<T>, Error> {
        let mut out = Vec::with_capacity(self.handles.len());
        let mut first_err: Option<Error> = None;
        let mut skipped = false;
        for handle in self.handles {
            match handle.wait() {
                Ok(Outcome::Done(v)) => {
                    if first_err.is_none() {
                        out.push(v);
                    }
                }
                Ok(Outcome::Skipped) => skipped = true,
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
            None if skipped => Err(self.token.cause().unwrap_or(Error::Canceled)),
            None => Ok(out),
        }
    }
}

/// A group of fallible blocking tasks that short-circuits on the first error.
///
/// Create one with [`Pool::try_group`](super::Pool::try_group).
pub struct BlockingTryGroup<T, E> {
    pool: Pool,
    token: CancelToken,
    #[allow(clippy::type_complexity)]
    handles: Vec<BlockingTaskHandle<Outcome<Result<T, E>>>>,
}

impl<T: Send + 'static, E: Send + 'static> BlockingTryGroup<T, E> {
    pub(crate) fn new(pool: Pool) -> Self {
        Self {
            pool,
            token: CancelToken::new(),
            handles: Vec::new(),
        }
    }

    /// Submits a fallible task to the group. Chainable.
    pub fn submit<F>(&mut self, task: F) -> &mut Self
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        let token = self.token.clone();
        let handle = self.pool.submit(move || {
            if token.is_cancelled() {
                return Outcome::Skipped;
            }
            // Cancels the group immediately on a returned `Err` or a panic.
            let on_failure = CancelOnDrop(&token);
            let result = task();
            if result.is_ok() {
                std::mem::forget(on_failure);
            }
            Outcome::Done(result)
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

    /// Waits for all tasks to finish or be skipped, and returns their outputs in
    /// submission order.
    ///
    /// A failing task cancels the remaining queued tasks as soon as it fails,
    /// whether or not `wait` is running yet. If any task failed, the error of the
    /// earliest one in submission order is returned. Otherwise, if the group was
    /// cancelled and any task was skipped as a result, the cancellation cause is
    /// returned as [`GroupError::Pool`].
    pub fn wait(self) -> Result<Vec<T>, GroupError<E>> {
        let mut out = Vec::with_capacity(self.handles.len());
        let mut first_err: Option<GroupError<E>> = None;
        let mut skipped = false;
        for handle in self.handles {
            match handle.wait() {
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
                Ok(Outcome::Skipped) => skipped = true,
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
            None if skipped => Err(GroupError::Pool(
                self.token.cause().unwrap_or(Error::Canceled),
            )),
            None => Ok(out),
        }
    }
}
