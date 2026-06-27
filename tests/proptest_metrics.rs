#![allow(missing_docs)]
#![cfg(feature = "runtime-tokio")]
//! Property test for the pool's metric accounting at quiescence.
//!
//! For any task count and failure pattern, once the pool has drained every
//! submitted task must be accounted for in exactly one terminal bucket, and no
//! tasks may be left running or waiting.

use proptest::prelude::*;

use pond::Pool;

proptest! {
    // A real pool per case is comparatively expensive; keep the case count modest.
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn metrics_balance_after_drain(total in 0usize..200, fail_every in 1usize..7) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let snap = rt.block_on(async {
            let pool = Pool::builder().max_concurrency(8).build();
            let mut handles = Vec::with_capacity(total);
            for i in 0..total {
                handles.push(pool.submit(move || async move {
                    if i % fail_every == 0 {
                        panic!("boom");
                    }
                    i
                }));
            }
            for h in handles {
                let _ = h.await;
            }
            pool.stop_and_wait().await;
            pool.metrics()
        });

        let total = total as u64;
        prop_assert_eq!(snap.submitted, total);
        prop_assert_eq!(snap.running_workers, 0, "no workers should remain running");
        prop_assert_eq!(snap.waiting, 0, "no tasks should remain queued");
        // Unbounded queue + no external cancel => every task either succeeded or
        // failed (panicked). Nothing dropped or canceled.
        prop_assert_eq!(snap.dropped, 0);
        prop_assert_eq!(snap.canceled, 0);
        prop_assert_eq!(
            snap.successful + snap.failed + snap.dropped + snap.canceled,
            total,
            "every submitted task must land in exactly one terminal bucket"
        );
    }
}
