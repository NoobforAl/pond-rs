//! A channel-agnostic sink interface for streaming values out of the pool.
//!
//! [`ResultSink`] is the "bring your own channel" interface: implement it for any
//! sender you like and the pool will push task results (or metric snapshots) to
//! it. An implementation for [`async_channel::Sender`] ships out of the box; for
//! other channels (`tokio::sync::mpsc`, `crossbeam`, `std::sync::mpsc::SyncSender`,
//! a closure, …) implement the one method yourself.

/// A destination the pool can push values to (task results or metric snapshots).
///
/// Implementors must be `Send + Sync + 'static` because the sink is shared across
/// worker tasks. [`send`](ResultSink::send) is non-blocking and best-effort:
/// prefer an **unbounded** channel for lossless streaming, since a bounded one
/// may drop when full.
pub trait ResultSink<T>: Send + Sync + 'static {
    /// Delivers `value`. Returns `false` if it could not be delivered (the
    /// channel was full or closed).
    fn send(&self, value: T) -> bool;
}

impl<T: Send + 'static> ResultSink<T> for async_channel::Sender<T> {
    fn send(&self, value: T) -> bool {
        self.try_send(value).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn async_channel_sender_is_a_sink() {
        let (tx, rx) = async_channel::unbounded::<u32>();
        // Use it through the trait object, as the pool will.
        let sink: &dyn ResultSink<u32> = &tx;
        assert!(sink.send(7));
        assert_eq!(rx.try_recv().unwrap(), 7);

        drop(rx);
        // Delivery now fails (channel closed) rather than panicking.
        assert!(!sink.send(8));
    }
}
