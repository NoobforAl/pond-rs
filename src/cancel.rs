//! A small, runtime-agnostic cancellation token.
//!
//! This replaces Go pond's use of `context.Context` / `context.WithCancelCause`.
//! A [`CancelToken`] can be cancelled once; all clones observe the cancellation
//! and any number of tasks can `await` it. An optional [`Error`] cause is stored
//! so waiters can learn *why* cancellation happened (e.g. the pool was stopped
//! versus an explicit cancel).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use event_listener::Event;

use crate::error::Error;

/// A cloneable handle to a one-shot cancellation signal.
///
/// Cloning shares the same signal: cancelling any clone cancels them all.
#[derive(Clone)]
pub struct CancelToken {
    inner: Arc<Inner>,
}

struct Inner {
    cancelled: AtomicBool,
    cause: Mutex<Option<Error>>,
    event: Event,
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelToken {
    /// Creates a new, un-cancelled token.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                cancelled: AtomicBool::new(false),
                cause: Mutex::new(None),
                event: Event::new(),
            }),
        }
    }

    /// Cancels the token with the default [`Error::Canceled`] cause.
    ///
    /// Subsequent calls are no-ops; the first cause to be set wins.
    pub fn cancel(&self) {
        self.cancel_with(Error::Canceled);
    }

    /// Cancels the token, recording `cause` as the reason (if not already cancelled).
    pub fn cancel_with(&self, cause: Error) {
        // Set the cause before flipping the flag so a waiter that observes
        // cancellation always sees a cause.
        {
            let mut guard = self.inner.cause.lock().unwrap();
            if guard.is_none() {
                *guard = Some(cause);
            }
        }
        // ordering: `Release` publishes the cause (set above) to any thread that
        // later observes cancellation via an `Acquire` load.
        if !self.inner.cancelled.swap(true, Ordering::Release) {
            // First cancellation: wake everyone.
            self.inner.event.notify(usize::MAX);
        }
    }

    /// Returns `true` if the token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        // ordering: `Acquire` pairs with the `Release` swap in `cancel_with` so a
        // reader that sees `true` also sees the recorded cause.
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Returns the recorded cancellation cause, if cancelled.
    pub fn cause(&self) -> Option<Error> {
        if self.is_cancelled() {
            self.inner.cause.lock().unwrap().clone()
        } else {
            None
        }
    }

    /// Resolves once the token is cancelled. Returns immediately if already cancelled.
    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            // Register interest *before* re-checking to avoid missing a wake.
            let listener = self.inner.event.listen();
            if self.is_cancelled() {
                return;
            }
            listener.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn starts_uncancelled() {
        let t = CancelToken::new();
        assert!(!t.is_cancelled());
        assert_eq!(t.cause(), None);
    }

    #[test]
    fn cancel_sets_flag_and_default_cause() {
        let t = CancelToken::new();
        t.cancel();
        assert!(t.is_cancelled());
        assert_eq!(t.cause(), Some(Error::Canceled));
    }

    #[test]
    fn first_cause_wins() {
        let t = CancelToken::new();
        t.cancel_with(Error::PoolStopped);
        t.cancel_with(Error::Canceled);
        assert_eq!(t.cause(), Some(Error::PoolStopped));
    }

    #[test]
    fn clone_shares_state() {
        let t = CancelToken::new();
        let t2 = t.clone();
        t.cancel_with(Error::GroupStopped);
        assert!(t2.is_cancelled());
        assert_eq!(t2.cause(), Some(Error::GroupStopped));
    }

    #[test]
    fn cancelled_returns_immediately_when_already_cancelled() {
        let t = CancelToken::new();
        t.cancel();
        block_on(t.cancelled());
    }

    #[test]
    fn cancelled_resolves_after_cancel() {
        let t = CancelToken::new();
        let t2 = t.clone();
        // Cancel, then await: the await must observe it.
        t.cancel();
        block_on(async move {
            t2.cancelled().await;
        });
    }
}
