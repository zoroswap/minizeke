//! Deploys and authorizes one additional pool shard for an existing deployment.

use anyhow::Result;
use dotenv::dotenv;
use minizeke::{
    deployment::Deployment,
    pool::deploy_pool,
    test_utils::{get_client, get_pool_client},
    vault::add_pool_to_vault,
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

    let mut deployment = Deployment::load()?;

    let mut pool_client = get_pool_client().await?;
    pool_client.ensure_genesis_in_place().await?;
    pool_client.sync_state().await?;

    let mut client = get_client().await?;
    client.ensure_genesis_in_place().await?;
    client.sync_state().await?;

    println!("[SPAWN_POOL] deploying pool shard");
    let pool_id = deploy_pool(&mut pool_client, deployment.vault_id)
        .await?
        .id();

    println!("[SPAWN_POOL] authorizing {}", pool_id.to_hex());
    add_pool_to_vault(
        &mut client,
        deployment.operator_account_id,
        deployment.vault_id,
        pool_id,
    )
    .await?;

    deployment.pools.push(pool_id);
    deployment.save()?;
    println!(
        "[SPAWN_POOL] added {} to {}",
        pool_id.to_hex(),
        Deployment::path().display()
    );
    Ok(())
}
