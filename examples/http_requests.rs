//! Bound concurrent outbound HTTP requests with a pool — a common client-side
//! rate-limit / upstream-connection-cap pattern.
//!
//! Self-contained: it starts a tiny in-process HTTP/1.1 server (raw Tokio TCP,
//! no external crates) and fetches from it, so it needs no network access and is
//! safe to run in CI.
//!
//! Run with: `cargo run --example http_requests`

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::Pool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const REQUESTS: usize = 20;
const MAX_CONCURRENCY: usize = 4;

#[tokio::main]
async fn main() {
    let addr = start_server().await;

    // Cap outbound concurrency: at most `MAX_CONCURRENCY` requests are ever in
    // flight, the rest queue — exactly what you want when calling a rate-limited
    // or connection-limited upstream.
    let pool = Pool::builder().max_concurrency(MAX_CONCURRENCY).build();

    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..REQUESTS {
        let current = current.clone();
        let peak = peak.clone();
        handles.push(pool.submit(move || async move {
            let now = current.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            let result = fetch(addr, format!("/item/{i}")).await;
            current.fetch_sub(1, Ordering::SeqCst);
            result
        }));
    }

    let mut ok = 0usize;
    for h in handles {
        match h.await.unwrap() {
            Ok(bytes) => {
                ok += 1;
                let _ = bytes;
            }
            Err(e) => eprintln!("request failed: {e}"),
        }
    }
    pool.stop_and_wait().await;

    let observed_peak = peak.load(Ordering::SeqCst);
    println!("completed {ok}/{REQUESTS} HTTP requests");
    println!("peak concurrent requests: {observed_peak} (cap was {MAX_CONCURRENCY})");
    assert_eq!(ok, REQUESTS, "every request should succeed");
    assert!(
        observed_peak <= MAX_CONCURRENCY,
        "pool must cap concurrency at {MAX_CONCURRENCY}"
    );
}

/// Performs one HTTP/1.1 GET over a fresh connection and returns the response
/// size in bytes.
async fn fetch(addr: SocketAddr, path: String) -> std::io::Result<usize> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?; // server closes => EOF
    Ok(response.len())
}

/// Starts a minimal HTTP/1.1 server on an ephemeral port and returns its address.
/// Each connection gets a fixed 200 response after a small simulated latency.
async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                // Drain the request bytes (we don't need to parse them here).
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                // Simulate upstream latency so requests genuinely overlap.
                tokio::time::sleep(Duration::from_millis(10)).await;

                let body = b"hello from pond";
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(body).await;
                let _ = stream.flush().await;
                // Dropping `stream` closes the connection, signalling EOF.
            });
        }
    });

    addr
}
