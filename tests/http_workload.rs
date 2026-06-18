//! HTTP-workload integration test: the pool caps concurrent outbound requests.
//!
//! Self-contained — a tiny in-process HTTP/1.1 server (raw Tokio TCP) stands in
//! for an upstream service, so the test needs no network access.
#![cfg(feature = "runtime-tokio")]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pond::Pool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_caps_concurrent_http_requests() {
    let addr = start_server().await;

    const REQUESTS: usize = 15;
    const CAP: usize = 3;
    let pool = Pool::builder().max_concurrency(CAP).build();

    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..REQUESTS {
        let current = current.clone();
        let peak = peak.clone();
        handles.push(pool.submit(move || async move {
            let now = current.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            let result = fetch(addr, format!("/r/{i}")).await;
            current.fetch_sub(1, Ordering::SeqCst);
            result
        }));
    }

    let mut ok = 0usize;
    for h in handles {
        if h.await.unwrap().is_ok() {
            ok += 1;
        }
    }
    pool.stop_and_wait().await;

    assert_eq!(ok, REQUESTS, "every request should succeed");
    let observed = peak.load(Ordering::SeqCst);
    assert!(observed <= CAP, "peak {observed} exceeded the cap {CAP}");
    assert!(observed >= 2, "expected the requests to actually overlap");

    let m = pool.metrics();
    assert_eq!(m.submitted, REQUESTS as u64);
    assert_eq!(m.successful, REQUESTS as u64);
}

async fn fetch(addr: SocketAddr, path: String) -> std::io::Result<usize> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(response.len())
}

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
                let body = b"ok";
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(body).await;
                let _ = stream.flush().await;
            });
        }
    });
    addr
}
