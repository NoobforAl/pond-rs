//! SQLite-backed integration tests: the pool as a database worker pool.
//!
//! These model a realistic workload — a `users` table populated and queried
//! through the **blocking** pool, which is the natural fit for synchronous DB
//! drivers like rusqlite (each task opens its own connection and does blocking
//! SQL). They require the default `blocking` feature.

#![cfg(feature = "blocking")]

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::blocking::Pool;
use rusqlite::{params, Connection};

/// A unique temp-file DB path for one test, in the OS temp dir (off OneDrive).
fn db_path(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("pond_sqlite_{}_{tag}.db", std::process::id()));
    cleanup(&p); // start from a clean slate
    p
}

/// Open a connection tuned for concurrent access: WAL mode plus a busy timeout
/// so a writer waits for the lock instead of failing with "database is locked".
fn open(path: &PathBuf) -> Connection {
    let conn = Connection::open(path).expect("open sqlite db");
    conn.busy_timeout(Duration::from_secs(5)).unwrap();
    // execute_batch discards the row that `PRAGMA journal_mode` returns.
    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    conn
}

/// Best-effort removal of the DB file and its WAL sidecars.
fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// Many tasks concurrently INSERT user rows; the final row count must match.
#[test]
fn concurrent_user_inserts_via_blocking_pool() {
    let path = db_path("inserts");
    {
        let conn = open(&path);
        conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            .unwrap();
    }

    let pool = Pool::builder().max_concurrency(8).build();
    let handles: Vec<_> = (0..50i64)
        .map(|id| {
            let path = path.clone();
            pool.submit(move || {
                let conn = open(&path);
                conn.execute(
                    "INSERT INTO users (id, name) VALUES (?1, ?2)",
                    params![id, format!("user{id}")],
                )
                .unwrap();
            })
        })
        .collect();
    for h in handles {
        h.wait().unwrap();
    }
    pool.stop_and_wait();

    let conn = open(&path);
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 50);
    drop(conn);
    cleanup(&path);
}

/// A bounded pool acts as a connection pool: it caps how many DB workers run
/// concurrently, even under a much larger query load.
#[test]
fn bounded_query_pool_caps_concurrent_connections() {
    let path = db_path("reads");
    {
        let mut conn = open(&path);
        conn.execute_batch("CREATE TABLE nums (v INTEGER NOT NULL);")
            .unwrap();
        let tx = conn.transaction().unwrap();
        for v in 0..100i64 {
            tx.execute("INSERT INTO nums (v) VALUES (?1)", params![v])
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let cap = 4;
    let pool = Pool::builder().max_concurrency(cap).build();
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..40)
        .map(|_| {
            let path = path.clone();
            let current = current.clone();
            let peak = peak.clone();
            pool.submit(move || {
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                let conn = open(&path);
                let sum: i64 = conn
                    .query_row("SELECT COALESCE(SUM(v), 0) FROM nums", [], |row| row.get(0))
                    .unwrap();
                // Hold the "connection" a moment so concurrency can build up.
                std::thread::sleep(Duration::from_millis(5));
                current.fetch_sub(1, Ordering::SeqCst);
                sum
            })
        })
        .collect();

    for h in handles {
        assert_eq!(h.wait().unwrap(), 4950); // sum of 0..100
    }
    pool.stop_and_wait();

    assert!(
        peak.load(Ordering::SeqCst) <= cap,
        "concurrent DB workers {} exceeded cap {cap}",
        peak.load(Ordering::SeqCst)
    );
    cleanup(&path);
}
