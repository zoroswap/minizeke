//! Dedicated token faucet process. The main server proxies `POST /mint` to this process,
//! keeping faucet signing keys and its Miden client isolated from trading/custody.
//!
//! ```sh
//! cargo run --bin faucet_server
//! FAUCET_SERVER_URL=127.0.0.1:7801 cargo run --bin faucet_server
//! ```

use std::env;

use anyhow::Result;
use dotenv::dotenv;
use minizeke::faucet::{DEFAULT_FAUCET_SERVER_URL, initialize, router};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,miden_core=off,log=warn")
            }),
        )
        .with_target(false)
        .init();

    let bind_address =
        env::var("FAUCET_SERVER_URL").unwrap_or_else(|_| DEFAULT_FAUCET_SERVER_URL.to_string());
    let socket_address: std::net::SocketAddr = bind_address.parse()?;
    if !socket_address.ip().is_loopback() {
        anyhow::bail!("FAUCET_SERVER_URL must bind to a loopback address");
    }
    let state = initialize().await?;
    let listener = tokio::net::TcpListener::bind(&bind_address).await?;
    info!(address = %bind_address, "Faucet server listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
