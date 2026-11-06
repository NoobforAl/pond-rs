//! The concurrency gate shared by [`Pool`](crate::Pool) and
//! [`Subpool`](crate::Subpool): a counting semaphore whose limit can change while
//! tasks hold and wait for permits.
//!
//! Permits are counted rather than stored as tokens, so a resize has nothing to
//! reconcile. Raising the limit (or removing it) wakes every waiter to re-check;
//! lowering it simply stops new acquisitions until enough holders have finished,
//! which never interrupts a running task. Tasks are counted even while the limit
//! is `0` (unbounded), so a later `resize(n)` accounts for the ones still running.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use event_listener::{Event, IntoNotification};

pub(crate) struct Limiter {
    limit: AtomicUsize, // 0 == unbounded
    held: AtomicUsize,
    closed: AtomicBool,
    freed: Event,
}

/// Releases its permit on drop, including during unwinding.
pub(crate) struct LimitPermit<'a> {
    limiter: &'a Limiter,
}

impl Limiter {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit: AtomicUsize::new(limit),
            held: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            freed: Event::new(),
        }
    }

    /// The configured limit (`0` == unbounded).
    pub(crate) fn limit(&self) -> usize {
        // ordering: `Relaxed` — reporting a config value, not synchronizing on it.
        self.limit.load(Ordering::Relaxed)
    }

    /// Changes the limit and wakes every waiter to re-check it.
    pub(crate) fn set_limit(&self, limit: usize) {
        // ordering: `SeqCst` — waiters register a listener and then re-read the
        // limit, so this store and that load must not both miss each other.
        self.limit.store(limit, Ordering::SeqCst);
        self.freed.notify(usize::MAX);
    }

    /// How many more tasks could start right now (`usize::MAX` when unbounded).
    pub(crate) fn available(&self) -> usize {
        match self.limit.load(Ordering::SeqCst) {
            0 => usize::MAX,
            limit => limit.saturating_sub(self.held.load(Ordering::SeqCst)),
        }
    }

    /// Fails every current and future [`acquire`](Self::acquire) with `None`.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.freed.notify(usize::MAX);
    }

    fn try_acquire(&self) -> Option<LimitPermit<'_>> {
        let limit = self.limit.load(Ordering::SeqCst);
        if limit == 0 {
            self.held.fetch_add(1, Ordering::SeqCst);
            return Some(LimitPermit { limiter: self });
        }
        let mut held = self.held.load(Ordering::SeqCst);
        while held < limit {
            match self.held.compare_exchange_weak(
                held,
                held + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Some(LimitPermit { limiter: self }),
                Err(actual) => held = actual,
            }
        }
        None
    }

    /// Waits for a permit. Returns `None` once the limiter is closed.
    pub(crate) async fn acquire(&self) -> Option<LimitPermit<'_>> {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(permit) = self.try_acquire() {
                return Some(permit);
            }
            // Register interest, then re-check, so a release between the check
            // above and this listener isn't missed.
            let listener = self.freed.listen();
            if self.closed.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(permit) = self.try_acquire() {
                return Some(permit);
            }
            listener.await;
        }
    }
}

impl Drop for LimitPermit<'_> {
    fn drop(&mut self) {
        self.limiter.held.fetch_sub(1, Ordering::SeqCst);
        // `additional`: a plain `notify(1)` is a no-op while an earlier waiter is
        // notified but hasn't woken yet, so two quick releases would wake only
        // one waiter and strand a free permit.
        self.limiter.freed.notify(1usize.additional());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::FutureExt;

    #[test]
    fn enforces_limit_and_release() {
        let l = Limiter::new(1);
        let p = block_on(l.acquire()).unwrap();
        assert_eq!(l.available(), 0);
        assert!(l.acquire().now_or_never().is_none());
        drop(p);
        assert_eq!(l.available(), 1);
        assert!(l.acquire().now_or_never().is_some());
    }

    #[test]
    fn lowering_the_limit_waits_for_holders_to_finish() {
        let l = Limiter::new(2);
        let a = block_on(l.acquire()).unwrap();
        let b = block_on(l.acquire()).unwrap();
        l.set_limit(1);
        drop(a);
        assert!(l.acquire().now_or_never().is_none(), "one holder remains");
        drop(b);
        assert!(l.acquire().now_or_never().is_some());
    }

    #[test]
    fn unbounded_holders_still_count_against_a_later_limit() {
        let l = Limiter::new(0);
        assert_eq!(l.available(), usize::MAX);
        let _a = block_on(l.acquire()).unwrap();
        l.set_limit(1);
        assert_eq!(l.available(), 0);
        assert!(l.acquire().now_or_never().is_none());
    }

    #[test]
    fn close_fails_acquire() {
        let l = Limiter::new(1);
        l.close();
        assert!(block_on(l.acquire()).is_none());
    }
}
