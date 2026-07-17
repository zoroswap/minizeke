//! Opt-in operational harnesses. These are ignored because they require running services,
//! credentials, and (for order submission) a fresh signed v2 payload.

use std::{env, sync::Arc, time::Instant};

use anyhow::{Context, Result, bail};
use tokio::{net::TcpStream, sync::Semaphore, task::JoinSet};

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} must be set for this ignored harness"))
}

fn number(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

#[tokio::test]
#[ignore = "requires a running API and a fresh signed v2 order payload"]
async fn measure_v2_order_admissions_per_second() -> Result<()> {
    let base_url = env::var("LOAD_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:7799".into());
    let payloads: Vec<serde_json::Value> = serde_json::from_str(&required("LOAD_ORDERS_JSON")?)
        .context("parse LOAD_ORDERS_JSON as an array of signed v2 requests")?;
    if payloads.is_empty() {
        bail!("LOAD_ORDERS_JSON must contain at least one signed request");
    }
    let requests = number("LOAD_REQUESTS", payloads.len());
    let concurrency = number("LOAD_CONCURRENCY", 8);
    let permits = Arc::new(Semaphore::new(concurrency));
    let client = reqwest::Client::new();
    let started = Instant::now();
    let mut tasks = JoinSet::new();

    for index in 0..requests {
        let permit = permits.clone().acquire_owned().await?;
        let client = client.clone();
        let payload = payloads[index % payloads.len()].clone();
        let endpoint = format!("{base_url}/orders/new");
        tasks.spawn(async move {
            let _permit = permit;
            client.post(endpoint).json(&payload).send().await
        });
    }

    let mut accepted = 0_usize;
    let mut rate_limited = 0_usize;
    let mut failed = 0_usize;
    while let Some(result) = tasks.join_next().await {
        match result?? {
            response if response.status().is_success() => accepted += 1,
            response if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                rate_limited += 1
            }
            _ => failed += 1,
        }
    }
    let elapsed = started.elapsed();
    let orders_per_second = accepted as f64 / elapsed.as_secs_f64();
    eprintln!(
        "orders/sec={orders_per_second:.2} accepted={accepted} rate_limited={rate_limited} \
         failed={failed} attempted={requests} elapsed={elapsed:?}"
    );
    if accepted == 0 {
        bail!("no order admission succeeded");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a running API listener"]
async fn measure_concurrent_socket_capacity() -> Result<()> {
    let target = env::var("LOAD_SOCKET_TARGET").unwrap_or_else(|_| "127.0.0.1:7799".into());
    let requested = number("LOAD_SOCKETS", 100);
    let mut sockets = Vec::with_capacity(requested);
    for _ in 0..requested {
        match TcpStream::connect(&target).await {
            Ok(socket) => sockets.push(socket),
            Err(error) => {
                eprintln!("socket connection stopped at {}: {error}", sockets.len());
                break;
            }
        }
    }
    eprintln!(
        "sockets_open={} sockets_requested={} target={target}",
        sockets.len(),
        requested
    );
    if sockets.is_empty() {
        bail!("no socket connected");
    }
    drop(sockets);
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicitly configured remote testnet endpoints"]
async fn remote_testnet_health_smoke() -> Result<()> {
    let api_health = required("REMOTE_API_HEALTH_URL")?;
    let network_health = required("REMOTE_TESTNET_HEALTH_URL")?;
    let client = reqwest::Client::new();
    for endpoint in [api_health, network_health] {
        let response = client
            .get(&endpoint)
            .send()
            .await
            .with_context(|| format!("GET {endpoint}"))?;
        if !response.status().is_success() {
            bail!("{endpoint} returned {}", response.status());
        }
    }
    Ok(())
}
