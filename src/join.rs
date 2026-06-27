//! Awaiting many task handles at once.

use std::future::Future;

use crate::error::Result;
use crate::pool::TaskHandle;

/// Awaits every [`TaskHandle`] and collects their results in the **same order**
/// as the input.
///
/// A convenience over hand-rolling a loop after a fan-out such as
/// [`Pool::submit_n`](crate::Pool::submit_n) or [`Pool::group`](crate::Pool::group).
/// Each element is the individual task's [`Result`] — one task failing (panic or
/// cancellation) does not affect the others.
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use pond::{join_all, Pool};
///
/// let pool = Pool::builder().max_concurrency(4).build();
/// let handles = pool.submit_n(4, |i| async move { i * i });
/// let results = join_all(handles).await;
/// let values: Vec<_> = results.into_iter().map(|r| r.unwrap()).collect();
/// assert_eq!(values, vec![0, 1, 4, 9]);
/// # }
/// ```
pub async fn join_all<I, T>(handles: I) -> Vec<Result<T>>
where
    I: IntoIterator<Item = TaskHandle<T>>,
    TaskHandle<T>: Future<Output = Result<T>>,
{
    futures::future::join_all(handles).await
}
