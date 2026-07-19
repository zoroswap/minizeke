//! Deploys and activates one additional pool shard for an existing deployment.
//! Prefer the server-owned pool provisioner for automatic growth; this binary is the
//! explicit admin path for the same deploy → publish → activate sequence.

use anyhow::Result;
use dotenv::dotenv;
use minizeke::{
    deployment::Deployment,
    pool_manager::{activate_pool_shard, deploy_pool_shard, publish_pool_to_deployment},
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

    let deployment = Deployment::load()?;

    println!("[SPAWN_POOL] deploying pool shard");
    let pool_id = deploy_pool_shard(deployment.vault_id).await?;

    println!("[SPAWN_POOL] publishing {}", pool_id.to_hex());
    publish_pool_to_deployment(pool_id)?;

    println!("[SPAWN_POOL] authorizing {}", pool_id.to_hex());
    activate_pool_shard(deployment.operator_account_id, deployment.vault_id, pool_id).await?;

    println!(
        "[SPAWN_POOL] added {} to {}",
        pool_id.to_hex(),
        Deployment::path().display()
    );
    Ok(())
}
