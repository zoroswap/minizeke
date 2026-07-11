//! One-time deployment script: deploys the vault, the two pool faucets and the pool
//! account, wires the pool id into the vault, and writes `deployment.{network}.json`
//! for the server to attach to. Run once per environment:
//!
//! ```sh
//! cargo run --bin spawn            # fails if a deployment file already exists
//! SPAWN_FORCE=1 cargo run --bin spawn   # redeploy and overwrite the file
//! ```

use std::env;

use anyhow::{Result, anyhow};
use dotenv::dotenv;
use minizeke::{
    deployment::Deployment,
    miden_env::MidenNetwork,
    pool::deploy_pool,
    test_utils::{get_client, get_faucet, get_pool_client},
    vault::{deploy_vault, set_pool_account_id_on_vault},
};

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

    let force = env::var("SPAWN_FORCE").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    if Deployment::exists() && !force {
        return Err(anyhow!(
            "deployment file {} already exists; set SPAWN_FORCE=1 to redeploy and overwrite",
            Deployment::path().display()
        ));
    }

    let network = MidenNetwork::from_env();
    println!("[SPAWN] network: {}", network.as_str());

    let mut client = get_client().await?;
    client.ensure_genesis_in_place().await?;
    client.sync_state().await?;

    let mut pool_client = get_pool_client().await?;
    pool_client.ensure_genesis_in_place().await?;
    pool_client.sync_state().await?;

    println!("[SPAWN] deploying vault");
    let vault = deploy_vault(&mut client).await?;
    let vault_id = vault.id();
    println!("[SPAWN] vault: {}", vault_id.to_hex());

    let symbol0 = env::var("ASSET0_SYMBOL").unwrap_or_else(|_| "ASTA".to_string());
    let symbol1 = env::var("ASSET1_SYMBOL").unwrap_or_else(|_| "ASTB".to_string());
    println!("[SPAWN] deploying faucets {symbol0} / {symbol1}");
    let asset0 = get_faucet(&mut client, &symbol0).await?;
    let asset1 = get_faucet(&mut client, &symbol1).await?;
    println!(
        "[SPAWN] asset0: {} asset1: {}",
        asset0.to_hex(),
        asset1.to_hex()
    );

    // deployed from the pool client so the vault stays untracked in the store that
    // submits the FPI swap txs (see get_pool_client docs)
    println!("[SPAWN] deploying pool");
    let pool = deploy_pool(&mut pool_client, vault_id, asset0, asset1).await?;
    let pool_id = pool.id();
    println!(
        "[SPAWN] pool: {} ({})",
        pool_id.to_hex(),
        pool_id.to_bech32(network.endpoint().to_network_id())
    );

    println!("[SPAWN] wiring pool id into the vault");
    set_pool_account_id_on_vault(&mut client, vault_id, pool_id).await?;

    let deployment = Deployment {
        network: network.as_str().to_string(),
        vault_id,
        pool_id,
        asset0,
        asset1,
        lp_account_id: None,
        deposits: Vec::new(),
    };
    deployment.save()?;
    println!(
        "[SPAWN] deployment written to {}",
        Deployment::path().display()
    );
    println!("[SPAWN] next: seed liquidity with `cargo run --bin deposit_pools`");

    Ok(())
}
