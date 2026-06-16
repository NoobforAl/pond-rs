//! Error types returned by pool operations.

use std::fmt;

/// Errors produced when submitting or awaiting tasks.
///
/// This is the Rust counterpart of pond's sentinel errors (`ErrQueueFull`,
/// `ErrPoolStopped`, `ErrContextCanceled`, `ErrPanic`, `ErrGroupStopped`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The task queue is full and the pool is non-blocking, so the task was rejected.
    QueueFull,
    /// The pool has been stopped and no longer accepts tasks.
    PoolStopped,
    /// The task was canceled before it finished, usually because the pool's
    /// [`CancelToken`](crate::CancelToken) was triggered.
    Canceled,
    /// The task panicked. The captured panic message is included when available.
    Panicked(String),
    /// The owning task group was stopped, so the task will not run.
    GroupStopped,
}

impl Error {
    /// Returns `true` if this error represents a panic in user code.
    pub fn is_panic(&self) -> bool {
        matches!(self, Error::Panicked(_))
    }

    /// Returns `true` if this error represents a cancellation.
    pub fn is_canceled(&self) -> bool {
        matches!(self, Error::Canceled)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::QueueFull => write!(f, "queue is full"),
            Error::PoolStopped => write!(f, "pool stopped"),
            Error::Canceled => write!(f, "task canceled"),
            Error::Panicked(msg) => write!(f, "task panicked: {msg}"),
            Error::GroupStopped => write!(f, "task group stopped"),
        }
    }
}

impl std::error::Error for Error {}

/// Convenience alias for results returned by pool operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Extracts a human-readable message from a captured panic payload.
///
/// Shared by the async and blocking pools' panic-recovery paths.
pub(crate) fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "task panicked".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_match_concepts() {
        assert_eq!(Error::QueueFull.to_string(), "queue is full");
        assert_eq!(Error::PoolStopped.to_string(), "pool stopped");
        assert_eq!(Error::Canceled.to_string(), "task canceled");
        assert_eq!(Error::GroupStopped.to_string(), "task group stopped");
        assert_eq!(
            Error::Panicked("boom".into()).to_string(),
            "task panicked: boom"
        );
    }

    #[test]
    fn classifiers() {
        assert!(Error::Panicked("x".into()).is_panic());
        assert!(!Error::QueueFull.is_panic());
        assert!(Error::Canceled.is_canceled());
        assert!(!Error::Canceled.is_panic());
    }

    #[test]
    fn is_std_error() {
        fn assert_error<E: std::error::Error>(_: &E) {}
        assert_error(&Error::PoolStopped);
    }
}
