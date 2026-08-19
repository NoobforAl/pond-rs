#![allow(missing_docs)]
#![cfg(feature = "blocking")]
//! SQLite under load: the blocking pool as a database worker pool.
//!
//! `sqlite.rs` covers the basics — rows go in, the count comes out, a bounded
//! pool caps connections. This file asks the harder question: when many pooled
//! workers hit one database at once, does the *data* stay correct?
//!
//! Each test pins a specific invariant that a scheduling bug would break:
//!
//! * concurrent read-modify-write loses no updates,
//! * bulk transactional inserts land exactly once, with no duplicates,
//! * a reader never observes a half-applied transaction, and
//! * a task that fails mid-transaction rolls back cleanly and leaves the pool
//!   usable.
//!
//! SQLite is compiled in via `rusqlite`'s `bundled` feature and is a
//! **dev-dependency only** — it never enters the shipped dependency graph.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::blocking::Pool;
use rusqlite::{params, Connection, TransactionBehavior};

/// A unique temp-file DB path for one test, in the OS temp dir (off OneDrive,
/// which is slow and can hold locks on files under test).
fn db_path(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("pond_sqlite_load_{}_{tag}.db", std::process::id()));
    cleanup(&p); // start from a clean slate
    p
}

/// Open a connection tuned for concurrent access.
///
/// WAL lets readers run while a writer holds the write lock; `busy_timeout`
/// makes a blocked writer wait for the lock instead of failing with "database
/// is locked". `synchronous=NORMAL` is the usual companion to WAL and keeps
/// these tests from being dominated by fsync time.
fn open(path: &Path) -> Connection {
    let conn = Connection::open(path).expect("open sqlite db");
    conn.busy_timeout(Duration::from_secs(10)).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        .unwrap();
    conn
}

/// Best-effort removal of the DB file and its WAL sidecars.
fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// Concurrent read-modify-write must not lose updates.
///
/// This is the invariant that a naive `SELECT` then `UPDATE` gets wrong: each
/// task increments the same counter row inside an IMMEDIATE transaction, so
/// SQLite serializes them. If the pool ran a task twice, dropped one, or let
/// two share a transaction, the final total wouldn't match.
#[test]
fn concurrent_increments_lose_no_updates() {
    const TASKS: i64 = 200;
    let path = db_path("increments");
    {
        let conn = open(&path);
        conn.execute_batch(
            "CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER NOT NULL);
             INSERT INTO counter (id, value) VALUES (1, 0);",
        )
        .unwrap();
    }

    let pool = Pool::builder().max_concurrency(8).build();
    let handles: Vec<_> = (0..TASKS)
        .map(|_| {
            let path = path.clone();
            pool.submit(move || {
                let mut conn = open(&path);
                // IMMEDIATE takes the write lock up front, so two tasks can't
                // both read the old value and then race to write it back.
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                let cur: i64 = tx
                    .query_row("SELECT value FROM counter WHERE id = 1", [], |r| r.get(0))
                    .unwrap();
                tx.execute(
                    "UPDATE counter SET value = ?1 WHERE id = 1",
                    params![cur + 1],
                )
                .unwrap();
                tx.commit().unwrap();
            })
        })
        .collect();
    for h in handles {
        h.wait().unwrap();
    }
    pool.stop_and_wait();

    let conn = open(&path);
    let value: i64 = conn
        .query_row("SELECT value FROM counter WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        value, TASKS,
        "lost updates: counter is {value} after {TASKS} increments"
    );
    drop(conn);
    cleanup(&path);
}

/// Bulk transactional inserts land exactly once each.
///
/// Every task owns a disjoint key range and writes it in one transaction. A
/// duplicate would violate the primary key (so the task itself fails), and a
/// missing row shows up in the count and the checksum.
#[test]
fn bulk_transactional_inserts_land_exactly_once() {
    const TASKS: i64 = 20;
    const PER_TASK: i64 = 250;
    let path = db_path("bulk");
    {
        let conn = open(&path);
        conn.execute_batch("CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
            .unwrap();
    }

    let pool = Pool::builder().max_concurrency(8).build();
    let handles: Vec<_> = (0..TASKS)
        .map(|t| {
            let path = path.clone();
            pool.submit(move || {
                let mut conn = open(&path);
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                for i in 0..PER_TASK {
                    let id = t * PER_TASK + i;
                    tx.execute(
                        "INSERT INTO events (id, payload) VALUES (?1, ?2)",
                        params![id, format!("payload-{id}")],
                    )
                    .unwrap();
                }
                tx.commit().unwrap();
                PER_TASK
            })
        })
        .collect();

    let mut inserted = 0i64;
    for h in handles {
        inserted += h.wait().unwrap();
    }
    pool.stop_and_wait();
    assert_eq!(inserted, TASKS * PER_TASK);

    let conn = open(&path);
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
        .unwrap();
    let distinct: i64 = conn
        .query_row("SELECT COUNT(DISTINCT id) FROM events", [], |r| r.get(0))
        .unwrap();
    let sum: i64 = conn
        .query_row("SELECT COALESCE(SUM(id), 0) FROM events", [], |r| r.get(0))
        .unwrap();

    let total = TASKS * PER_TASK;
    assert_eq!(count, total, "row count wrong");
    assert_eq!(distinct, total, "duplicate ids present");
    assert_eq!(sum, (0..total).sum::<i64>(), "wrong rows landed");
    drop(conn);
    cleanup(&path);
}

/// A reader must never observe a half-applied transaction.
///
/// Writers insert in fixed-size batches inside one transaction; readers count
/// rows concurrently. Under WAL a reader sees a consistent snapshot, so every
/// count it observes must be a whole number of batches. A torn read (a count
/// that isn't a multiple of the batch size) would mean a transaction leaked
/// partial state — the kind of thing that happens if pooled work shares a
/// connection it shouldn't.
#[test]
fn readers_never_see_a_partial_transaction() {
    const WRITERS: i64 = 12;
    const BATCH: i64 = 100;
    const READERS: usize = 6;
    const READS_EACH: usize = 40;

    let path = db_path("snapshot");
    {
        let conn = open(&path);
        conn.execute_batch("CREATE TABLE rows_t (id INTEGER PRIMARY KEY);")
            .unwrap();
    }

    let pool = Pool::builder().max_concurrency(8).build();
    let torn = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));

    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let path = path.clone();
            pool.submit(move || {
                let mut conn = open(&path);
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                for i in 0..BATCH {
                    tx.execute(
                        "INSERT INTO rows_t (id) VALUES (?1)",
                        params![w * BATCH + i],
                    )
                    .unwrap();
                }
                tx.commit().unwrap();
            })
        })
        .collect();

    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let path = path.clone();
            let torn = torn.clone();
            let reads = reads.clone();
            pool.submit(move || {
                let conn = open(&path);
                for _ in 0..READS_EACH {
                    let count: i64 = conn
                        .query_row("SELECT COUNT(*) FROM rows_t", [], |r| r.get(0))
                        .unwrap();
                    if count % BATCH != 0 {
                        torn.fetch_add(1, Ordering::SeqCst);
                    }
                    reads.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
        })
        .collect();

    for h in writers {
        h.wait().unwrap();
    }
    for h in readers {
        h.wait().unwrap();
    }
    pool.stop_and_wait();

    assert_eq!(
        torn.load(Ordering::SeqCst),
        0,
        "a reader saw a partially-applied batch out of {} reads",
        reads.load(Ordering::SeqCst)
    );

    let conn = open(&path);
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM rows_t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, WRITERS * BATCH);
    drop(conn);
    cleanup(&path);
}

/// A task that fails mid-transaction rolls back, and the pool keeps working.
///
/// Half the tasks panic after inserting several rows but before committing.
/// Those rows must not be visible afterwards — and the surviving tasks, running
/// on the same pooled workers, must still commit normally.
#[test]
fn failed_transaction_rolls_back_and_pool_continues() {
    const TASKS: i64 = 40;
    const PER_TASK: i64 = 25;
    let path = db_path("rollback");
    {
        let conn = open(&path);
        conn.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, tag INTEGER NOT NULL);")
            .unwrap();
    }

    let pool = Pool::builder()
        .max_concurrency(8)
        .panic_recovery(true)
        .build();

    let handles: Vec<_> = (0..TASKS)
        .map(|t| {
            let path = path.clone();
            pool.submit(move || {
                let mut conn = open(&path);
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                for i in 0..PER_TASK {
                    tx.execute(
                        "INSERT INTO items (id, tag) VALUES (?1, ?2)",
                        params![t * PER_TASK + i, t],
                    )
                    .unwrap();
                }
                if t % 2 == 0 {
                    // Panic with the transaction still open: `Transaction`'s
                    // `Drop` rolls back, so none of these rows should survive.
                    panic!("task {t} failed before commit");
                }
                tx.commit().unwrap();
                PER_TASK
            })
        })
        .collect();

    let mut committed = 0i64;
    let mut failed = 0i64;
    for h in handles {
        match h.wait() {
            Ok(n) => committed += n,
            Err(_) => failed += 1,
        }
    }
    pool.stop_and_wait();

    assert_eq!(
        failed,
        TASKS / 2,
        "wrong number of tasks reported as failed"
    );
    assert_eq!(committed, (TASKS / 2) * PER_TASK);

    let conn = open(&path);
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count, committed,
        "rolled-back rows are visible: expected {committed}, found {count}"
    );
    // Nothing from an even-numbered (panicking) task may have survived.
    let leaked: i64 = conn
        .query_row("SELECT COUNT(*) FROM items WHERE tag % 2 = 0", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        leaked, 0,
        "{leaked} rows leaked from rolled-back transactions"
    );
    drop(conn);

    // The pool is still usable after all those panics.
    let pool2 = Pool::builder().max_concurrency(2).build();
    assert_eq!(pool2.submit(|| 7u32).wait().unwrap(), 7);
    pool2.stop_and_wait();

    cleanup(&path);
}
