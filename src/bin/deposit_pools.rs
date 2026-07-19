//! Seeds initial pool liquidity: creates (or reuses) a server-owned LP account, mints
//! every configured asset and sends DEPOSIT notes to the vault. Each deposit is recorded in
//! the deployment file so the server can rebuild pool states on startup.
//!
//! ```sh
//! cargo run --bin deposit_pools                       # assets.toml amount per pool
//! DEPOSIT_AMOUNT=500000000 cargo run --bin deposit_pools
//! ```

use std::{collections::HashMap, env};

use anyhow::{Context, Result};
use dotenv::dotenv;
use miden_client::asset::FungibleAsset;
use minizeke::{
    asset_config::{initial_liquidity_base_units, load_asset_configs},
    deployment::{Deployment, DepositRecord},
    test_utils::{deposit_liquidity_on_vault, get_client, get_user, mint_asset_to_user},
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

    let amount_override = env::var("DEPOSIT_AMOUNT")
        .ok()
        .map(|value| value.parse())
        .transpose()
        .context("DEPOSIT_AMOUNT must be an unsigned integer")?;
    let asset_configs = load_asset_configs()?
        .into_iter()
        .map(|asset| (asset.symbol.to_ascii_uppercase(), asset))
        .collect::<HashMap<_, _>>();

    let mut deployment = Deployment::load()?;
    println!(
        "[DEPOSIT] network: {} vault: {}",
        deployment.network,
        deployment.vault_id.to_hex()
    );

    let mut client = get_client().await?;
    client.ensure_genesis_in_place().await?;
    client.sync_state().await?;

    let lp_id = match deployment.lp_account_id {
        Some(id) => {
            println!("[DEPOSIT] reusing LP account {}", id.to_hex());
            id
        }
        None => {
            let id = get_user(&mut client).await?;
            println!("[DEPOSIT] created LP account {}", id.to_hex());
            deployment.lp_account_id = Some(id);
            deployment.save()?;
            id
        }
    };

    for asset in deployment.assets.clone() {
        let config = asset_configs
            .get(&asset.symbol.to_ascii_uppercase())
            .with_context(|| format!("{} is not defined in assets.toml", asset.symbol))?;
        let amount = amount_override.unwrap_or(initial_liquidity_base_units(config)?);
        let faucet_id = asset.faucet_id;
        if deployment
            .deposits
            .iter()
            .any(|deposit| deposit.faucet_id == faucet_id)
        {
            println!("[DEPOSIT] {} already seeded; skipping", asset.symbol);
            continue;
        }
        println!(
            "[DEPOSIT] minting {amount} {} ({}) to the LP",
            asset.symbol,
            faucet_id.to_hex()
        );
        mint_asset_to_user(&mut client, faucet_id, lp_id, amount).await?;

        println!(
            "[DEPOSIT] depositing {amount} {} into the vault",
            asset.symbol
        );
        deposit_liquidity_on_vault(
            &mut client,
            deployment.vault_id,
            lp_id,
            FungibleAsset::new(faucet_id, amount)?,
        )
        .await?;

        // record after the on-chain leg succeeded so the file never over-reports
        deployment
            .deposits
            .push(DepositRecord { faucet_id, amount });
        deployment.save()?;
        println!(
            "[DEPOSIT] recorded deposit in {}",
            Deployment::path().display()
        );
    }

    println!("[DEPOSIT] done; the server can now be started with `cargo run`");
    Ok(())
}
