//! Task scheduling and management helpers layered on [`Pool`].
//!
//! These build entirely on the existing runtime-agnostic seams (`Spawner::sleep`,
//! detached watcher tasks, and ordinary `submit`), so they work on every runtime
//! and add no dependencies:
//!
//! * **Delayed** — [`Pool::submit_after`], [`Pool::submit_at`].
//! * **Recurring** — [`Pool::schedule_every`] (returns a cancellable [`ScheduledTask`]).
//! * **Retry** — [`Pool::submit_with_retry`] with a [`RetryPolicy`].
//! * **Replication** — [`Pool::submit_n`] (fan-out) or a reusable [`Job`] template.
//!
//! The actual work always runs through the pool's normal concurrency permits when
//! it fires; only the *waiting* happens off-pool. A delayed/recurring/retry task
//! is therefore counted in [metrics](crate::MetricsSnapshot) only once it fires.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use futures::future::{select, Either};

use crate::cancel::CancelToken;
use crate::error::Result;
use crate::pool::{Pool, TaskHandle};
use crate::runtime::Spawner;

impl<S: Spawner> Pool<S> {
    /// Submits `task` to run after `delay` elapses, returning a handle that
    /// resolves with its result.
    ///
    /// The wait does not occupy a worker; when the delay elapses the task enters
    /// the pool's normal permit-gated execution. If the pool is stopped before
    /// then, the handle resolves with the stop error.
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::time::Duration;
    /// use pond::Pool;
    ///
    /// let pool = Pool::builder().max_concurrency(2).build();
    /// let handle = pool.submit_after(Duration::from_millis(10), || async { 42 });
    /// assert_eq!(handle.await.unwrap(), 42);
    /// # }
    /// ```
    pub fn submit_after<F, Fut, T>(&self, delay: Duration, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<Result<T>>();
        let pool = self.clone();
        let sleep = self.sleep(delay);
        self.spawn_detached(Box::pin(async move {
            sleep.await;
            let result = pool.submit(task).await;
            let _ = tx.send(result);
        }));
        TaskHandle::new(rx)
    }

    /// Submits `task` to run at the absolute instant `when` (immediately if
    /// `when` is already in the past).
    pub fn submit_at<F, Fut, T>(&self, when: Instant, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let delay = when.saturating_duration_since(Instant::now());
        self.submit_after(delay, task)
    }

    /// Runs `factory` repeatedly, once per `interval`, until the returned
    /// [`ScheduledTask`] is stopped or the pool is stopped.
    ///
    /// `factory` is called to produce a fresh future for each tick, so it must be
    /// reusable (`Fn`, not `FnOnce`). The first run happens after the first
    /// interval elapses. Each tick is a fire-and-forget submission to the pool.
    pub fn schedule_every<F, Fut>(&self, interval: Duration, factory: F) -> ScheduledTask
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let cancel = CancelToken::new();
        let pool = self.clone();
        let factory = Arc::new(factory);
        let watch = cancel.clone();

        self.spawn_detached(Box::pin(async move {
            loop {
                if watch.is_cancelled() || pool.is_stopped() {
                    break;
                }
                // Race the interval timer against cancellation so `stop()` is
                // observed promptly even mid-wait.
                let tick = pool.sleep(interval);
                let cancelled = Box::pin(watch.cancelled());
                if let Either::Right(_) = select(tick, cancelled).await {
                    break; // cancelled during the wait
                }
                if watch.is_cancelled() || pool.is_stopped() {
                    break;
                }
                let f = factory.clone();
                pool.spawn(move || async move { (f.as_ref())().await });
            }
        }));

        ScheduledTask { cancel }
    }

    /// Submits a fallible `factory`, retrying on `Err` per `policy`.
    ///
    /// The returned handle resolves with the first `Ok(v)`, or — once attempts
    /// are exhausted — the last `Err(e)`. `factory` must be reusable (`Fn`). If
    /// the pool itself rejects a submission (e.g. it was stopped), retrying stops
    /// and that pool error surfaces on the handle.
    pub fn submit_with_retry<F, Fut, T, E>(
        &self,
        policy: RetryPolicy,
        factory: F,
    ) -> TaskHandle<std::result::Result<T, E>>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_with_retry_if(policy, factory, |_| true)
    }

    /// Like [`submit_with_retry`](Self::submit_with_retry), but only retries an
    /// error for which `should_retry` returns `true`.
    ///
    /// A non-retryable error (predicate returns `false`) is returned immediately
    /// without further attempts — useful when some failures are permanent (e.g. a
    /// `404`) and retrying only wastes work.
    pub fn submit_with_retry_if<F, Fut, T, E, R>(
        &self,
        policy: RetryPolicy,
        factory: F,
        should_retry: R,
    ) -> TaskHandle<std::result::Result<T, E>>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        R: Fn(&E) -> bool + Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<Result<std::result::Result<T, E>>>();
        let pool = self.clone();
        let factory = Arc::new(factory);
        self.spawn_detached(Box::pin(async move {
            let mut backoff = policy.backoff;
            for attempt in 0..policy.max_attempts {
                if attempt > 0 && !backoff.is_zero() {
                    pool.sleep(policy.delay_for(backoff)).await;
                    if policy.exponential {
                        backoff = backoff.saturating_mul(2);
                    }
                }
                let f = factory.clone();
                match pool.submit(move || (f.as_ref())()).await {
                    Ok(Ok(value)) => {
                        let _ = tx.send(Ok(Ok(value)));
                        return;
                    }
                    Ok(Err(e)) => {
                        // Stop early on the last attempt or a non-retryable error.
                        if attempt + 1 >= policy.max_attempts || !should_retry(&e) {
                            let _ = tx.send(Ok(Err(e)));
                            return;
                        }
                    }
                    Err(pool_err) => {
                        let _ = tx.send(Err(pool_err));
                        return;
                    }
                }
            }
        }));
        TaskHandle::new(rx)
    }

    /// Submits `count` copies of `factory` concurrently (a fan-out), returning a
    /// handle per copy. `factory` receives the copy index.
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use pond::Pool;
    ///
    /// let pool = Pool::builder().max_concurrency(4).build();
    /// let handles = pool.submit_n(3, |i| async move { i * 10 });
    /// let mut got = Vec::new();
    /// for h in handles {
    ///     got.push(h.await.unwrap());
    /// }
    /// assert_eq!(got, vec![0, 10, 20]);
    /// # }
    /// ```
    pub fn submit_n<F, Fut, T>(&self, count: usize, factory: F) -> Vec<TaskHandle<T>>
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        (0..count)
            .map(|i| {
                // Futures are lazy, so creating it here and submitting a closure
                // that yields it is fine — it isn't polled until the task runs.
                let fut = factory(i);
                self.submit(move || fut)
            })
            .collect()
    }

    /// Creates a reusable [`Job`] from `factory` that can be submitted repeatedly.
    ///
    /// Use this when you want to define a unit of work once and fire it on demand
    /// (the alternative to the one-shot [`submit_n`](Pool::submit_n) fan-out).
    pub fn job<F>(&self, factory: F) -> Job<S, F> {
        Job {
            pool: self.clone(),
            factory: Arc::new(factory),
        }
    }
}

/// Handle to a recurring task created by [`Pool::schedule_every`].
///
/// Dropping it does **not** stop the schedule; call [`stop`](ScheduledTask::stop).
#[derive(Clone)]
pub struct ScheduledTask {
    cancel: CancelToken,
}

impl ScheduledTask {
    /// Stops the schedule; no further ticks fire. Idempotent.
    pub fn stop(&self) {
        self.cancel.cancel();
    }

    /// Returns `true` if the schedule has been stopped.
    pub fn is_stopped(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// The underlying [`CancelToken`], e.g. to stop the schedule from elsewhere.
    pub fn cancel_token(&self) -> CancelToken {
        self.cancel.clone()
    }
}

/// Retry behavior for [`Pool::submit_with_retry`].
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    max_attempts: u32,
    backoff: Duration,
    exponential: bool,
    max_backoff: Option<Duration>,
    jitter: bool,
}

impl RetryPolicy {
    /// At most `max_attempts` total tries (clamped to at least 1), no backoff.
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            backoff: Duration::ZERO,
            exponential: false,
            max_backoff: None,
            jitter: false,
        }
    }

    /// Sets the delay between attempts.
    pub fn backoff(mut self, backoff: Duration) -> Self {
        self.backoff = backoff;
        self
    }

    /// When `true`, the backoff doubles after each attempt.
    pub fn exponential(mut self, exponential: bool) -> Self {
        self.exponential = exponential;
        self
    }

    /// Caps the backoff so exponential growth never delays longer than `cap`.
    pub fn max_backoff(mut self, cap: Duration) -> Self {
        self.max_backoff = Some(cap);
        self
    }

    /// Adds randomized jitter to each backoff (equal jitter: the effective delay
    /// is drawn from `[backoff/2, backoff]`), spreading retries so many failing
    /// callers don't all wake at once. Off by default.
    pub fn jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// The effective delay for a computed `backoff`: capped by `max_backoff`,
    /// then optionally jittered.
    fn delay_for(&self, backoff: Duration) -> Duration {
        let capped = match self.max_backoff {
            Some(cap) => backoff.min(cap),
            None => backoff,
        };
        if self.jitter {
            equal_jitter(capped)
        } else {
            capped
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(3)
    }
}

/// Equal jitter: returns a delay in `[base/2, base]`, varied per call.
///
/// No `rand` dependency — a process-wide counter seeds a `splitmix64` mix, so
/// concurrent retry sequences get different delays (real load spreading) while
/// staying std-only.
fn equal_jitter(base: Duration) -> Duration {
    static SEED: AtomicU64 = AtomicU64::new(0);
    let seed = SEED.fetch_add(1, Ordering::Relaxed);
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // Fraction in [0, 1).
    let frac = (z >> 11) as f64 / (1u64 << 53) as f64;
    let half = base / 2;
    half + half.mul_f64(frac)
}

/// A reusable task template bound to a [`Pool`], created by [`Pool::job`].
///
/// Cloning is cheap (it shares the pool and an `Arc` of the factory). Submit it
/// as many times as you like with [`submit`](Job::submit) / [`submit_n`](Job::submit_n).
pub struct Job<S: Spawner, F> {
    pool: Pool<S>,
    factory: Arc<F>,
}

impl<S: Spawner, F> Clone for Job<S, F> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            factory: self.factory.clone(),
        }
    }
}

impl<S: Spawner, F> Job<S, F> {
    /// Submits one copy of the job, returning its result handle.
    pub fn submit<Fut, T>(&self) -> TaskHandle<T>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let factory = self.factory.clone();
        self.pool.submit(move || (factory.as_ref())())
    }

    /// Submits `count` copies of the job concurrently.
    pub fn submit_n<Fut, T>(&self, count: usize) -> Vec<TaskHandle<T>>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        (0..count).map(|_| self.submit()).collect()
    }
}
