//! One-time deployment script: deploys asset faucets, the vault and its first pool shard,
//! authorizes the shard, and writes `deployment.{network}.json`
//! for the server to attach to. Run once per environment:
//!
//! ```sh
//! cargo run --bin spawn            # fails if a deployment file already exists
//! SPAWN_FORCE=1 cargo run --bin spawn   # redeploy and overwrite the file
//! ```

use std::{collections::HashSet, env};

use anyhow::{Context, Result, anyhow, bail};
use dotenv::dotenv;
use minizeke::{
    asset_config::load_asset_configs,
    deployment::{AssetInfo, DEPLOYMENT_SCHEMA_VERSION, Deployment, validate_capacity_budget},
    miden_env::MidenNetwork,
    oracle_sse::{fetch_price_feeds, oracle_base_url, resolve_feed_id},
    pool::deploy_pool,
    test_utils::{get_client, get_faucet, get_operator, get_pool_client},
    vault::{DEFAULT_ASSET_CAPACITY, DEFAULT_POOL_USER_CAPACITY, add_pool_to_vault, deploy_vault},
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
    let asset_configs = load_asset_configs()?;
    let oracle_url = oracle_base_url()?;
    let price_feeds = fetch_price_feeds(&oracle_url).await?;
    let mut feed_ids = HashSet::with_capacity(asset_configs.len());
    let mut resolved_assets = Vec::with_capacity(asset_configs.len());
    for config in asset_configs {
        let oracle_feed_id = resolve_feed_id(&price_feeds, &config.symbol)?;
        if !feed_ids.insert(oracle_feed_id.clone()) {
            bail!("multiple configured assets resolve to oracle feed {oracle_feed_id}");
        }
        resolved_assets.push((config, oracle_feed_id));
    }
    println!("[SPAWN] network: {}", network.as_str());

    let mut client = get_client().await?;
    client.ensure_genesis_in_place().await?;
    client.sync_state().await?;

    let mut pool_client = get_pool_client().await?;
    pool_client.ensure_genesis_in_place().await?;
    pool_client.sync_state().await?;

    println!("[SPAWN] deploying operator");
    let operator_id = get_operator(&mut client).await?;
    println!("[SPAWN] operator: {}", operator_id.to_hex());

    let mut assets = Vec::with_capacity(resolved_assets.len());
    for (config, oracle_feed_id) in resolved_assets {
        println!("[SPAWN] deploying faucet {}", config.symbol);
        let faucet_id = get_faucet(
            &mut client,
            &config.symbol,
            config.decimals,
            config.max_supply,
        )
        .await?;
        println!("[SPAWN] faucet {}: {}", config.symbol, faucet_id.to_hex());
        assets.push(AssetInfo {
            faucet_id,
            symbol: config.symbol,
            decimals: config.decimals,
            oracle_feed_id,
        });
    }

    let pool_user_capacity = env::var("POOL_USER_CAPACITY")
        .ok()
        .map(|value| value.parse())
        .transpose()
        .context("POOL_USER_CAPACITY must be an unsigned integer")?
        .unwrap_or(DEFAULT_POOL_USER_CAPACITY);
    let asset_capacity = env::var("ASSET_CAPACITY")
        .ok()
        .map(|value| value.parse())
        .transpose()
        .context("ASSET_CAPACITY must be an unsigned integer")?
        .unwrap_or(DEFAULT_ASSET_CAPACITY);
    validate_capacity_budget(pool_user_capacity, asset_capacity)?;
    if assets.len() > asset_capacity as usize {
        bail!(
            "configured assets ({}) exceed ASSET_CAPACITY ({asset_capacity})",
            assets.len()
        );
    }

    println!("[SPAWN] deploying network vault");
    let vault = deploy_vault(&mut client, operator_id, pool_user_capacity).await?;
    let vault_id = vault.id();
    println!(
        "[SPAWN] vault: {} (pool_user_capacity={pool_user_capacity}, asset_capacity={asset_capacity})",
        vault_id.to_hex()
    );

    // deployed from the pool client so the vault stays untracked in the store that
    // submits the FPI swap txs (see get_pool_client docs)
    println!("[SPAWN] deploying pool shard");
    let pool = deploy_pool(&mut pool_client, vault_id).await?;
    let pool_id = pool.id();
    println!(
        "[SPAWN] pool: {} ({})",
        pool_id.to_hex(),
        pool_id.to_bech32(network.endpoint().to_network_id())
    );

    println!("[SPAWN] authorizing pool shard");
    add_pool_to_vault(&mut client, operator_id, vault_id, pool_id).await?;

    let deployment = Deployment {
        schema_version: DEPLOYMENT_SCHEMA_VERSION,
        network: network.as_str().to_string(),
        operator_account_id: operator_id,
        vault_id,
        assets,
        pools: vec![pool_id],
        pool_user_capacity,
        asset_capacity,
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
