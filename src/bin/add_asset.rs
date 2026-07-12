//! Adds one asset faucet to an existing deployment.
//!
//! ```sh
//! ASSET_SYMBOL=USDC cargo run --bin add_asset
//! ```

use std::{env, fs};

use anyhow::{Context, Result, bail};
use dotenv::dotenv;
use miden_client::asset::FungibleAsset;
use minizeke::{
    asset_config::{initial_liquidity_base_units, load_asset_configs},
    deployment::{AssetInfo, Deployment, DepositRecord},
    oracle_sse::{fetch_price_feeds, oracle_base_url, resolve_feed_id},
    test_utils::{deposit_liquidity_on_vault, get_client, get_faucet, mint_asset_to_user},
};

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    let value = value.trim();
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value.to_string())
}

fn save_atomically(deployment: &Deployment) -> Result<()> {
    let path = Deployment::path();
    let temp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("json")
    ));
    let contents = serde_json::to_string_pretty(deployment)?;
    fs::write(&temp_path, contents)
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    fs::rename(&temp_path, &path)
        .with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

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

    let symbol = required_env("ASSET_SYMBOL")?;
    let config = load_asset_configs()?
        .into_iter()
        .find(|asset| asset.symbol.eq_ignore_ascii_case(&symbol))
        .with_context(|| format!("{symbol} is not defined in the asset config"))?;
    let oracle_url = oracle_base_url()?;
    let price_feeds = fetch_price_feeds(&oracle_url).await?;
    let oracle_feed_id = resolve_feed_id(&price_feeds, &config.symbol)?;
    let deposit_amount = env::var("DEPOSIT_AMOUNT")
        .ok()
        .map(|value| value.parse())
        .transpose()
        .context("DEPOSIT_AMOUNT must be an unsigned integer")?
        .unwrap_or(initial_liquidity_base_units(&config)?);

    let mut deployment = Deployment::load()?;
    if deployment
        .assets
        .iter()
        .any(|asset| asset.symbol.eq_ignore_ascii_case(&symbol))
    {
        bail!("asset symbol {symbol} already exists");
    }
    if deployment
        .assets
        .iter()
        .any(|asset| asset.oracle_feed_id.eq_ignore_ascii_case(&oracle_feed_id))
    {
        bail!("oracle feed ID {oracle_feed_id} already exists");
    }

    let mut client = get_client().await?;
    client.ensure_genesis_in_place().await?;
    client.sync_state().await?;

    println!("[ADD_ASSET] deploying faucet {symbol}");
    let faucet_id = get_faucet(
        &mut client,
        &config.symbol,
        config.decimals,
        config.max_supply,
    )
    .await?;

    let mut deposit = None;
    if let Some(lp_id) = deployment.lp_account_id {
        println!("[ADD_ASSET] seeding {deposit_amount} to existing LP");
        mint_asset_to_user(&mut client, faucet_id, lp_id, deposit_amount).await?;
        deposit_liquidity_on_vault(
            &mut client,
            deployment.vault_id,
            lp_id,
            FungibleAsset::new(faucet_id, deposit_amount)?,
        )
        .await?;
        deposit = Some(DepositRecord {
            faucet_id,
            amount: deposit_amount,
        });
    }

    deployment.assets.push(AssetInfo {
        faucet_id,
        symbol: config.symbol,
        decimals: config.decimals,
        oracle_feed_id,
    });
    if let Some(deposit) = deposit {
        deployment.deposits.push(deposit);
    }
    save_atomically(&deployment)?;

    println!(
        "[ADD_ASSET] added {} to {}",
        faucet_id.to_hex(),
        Deployment::path().display()
    );
    Ok(())
}
