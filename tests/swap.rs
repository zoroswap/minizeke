use std::time::Duration;

use anyhow::Result;
use miden_client::asset::FungibleAsset;
use minizeke::{
    deployment::{AssetInfo, DEPLOYMENT_SCHEMA_VERSION, Deployment},
    intent::Intent,
    miden_env::MidenNetwork,
    order::{Order, OrderDetails, OrderExecutionResult},
    pool::{USER_INITIAL_ON_CHAIN_BALANCE, deploy_pool, get_user_balance_from_pool},
    test_utils::{
        fund_user_on_vault, get_client, get_faucet, get_miden_execution, get_pool_client,
        get_vault, mint_asset_to_user, register_user_on_vault,
    },
    user::get_users,
    vault::add_pool_to_vault,
};
use uuid::Uuid;

const NUM_USERS: u32 = 1;

#[tokio::test]
async fn test_swap() -> Result<()> {
    tracing_subscriber::fmt().init();
    dotenv::dotenv().ok();

    let test_dir = std::env::temp_dir().join(format!("minizeke-swap-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&test_dir)?;
    std::env::set_current_dir(&test_dir)?;

    // The server no longer deploys anything: it attaches to the accounts recorded in a
    // deployment file. The test deploys its own vault/faucets/pool and points
    // DEPLOYMENT_FILE at a scratch config for MidenExecution to load.
    let deployment_file = std::env::temp_dir().join("minizeke_test_swap_deployment.json");
    let _ = std::fs::remove_file(&deployment_file);
    unsafe { std::env::set_var("DEPLOYMENT_FILE", &deployment_file) };

    let mut client = get_client().await?;
    let mut pool_client = get_pool_client().await?;

    let (vault_id, operator_id) = get_vault(&mut client).await?;
    let asset0 = get_faucet(&mut client, "BTC", 8, 10_000_000_000).await?;
    let asset1 = get_faucet(&mut client, "ETH", 8, 10_000_000_000).await?;
    let pool = deploy_pool(&mut pool_client, vault_id).await?;
    let pool_id = pool.id();
    add_pool_to_vault(&mut client, operator_id, vault_id, pool_id).await?;

    Deployment {
        schema_version: DEPLOYMENT_SCHEMA_VERSION,
        network: MidenNetwork::from_env().as_str().to_string(),
        operator_account_id: operator_id,
        vault_id,
        assets: [
            (
                asset0,
                "BTC",
                "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
            ),
            (
                asset1,
                "ETH",
                "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
            ),
        ]
        .into_iter()
        .map(|(faucet_id, symbol, oracle_feed_id)| AssetInfo {
            faucet_id,
            symbol: symbol.to_string(),
            decimals: 8,
            oracle_feed_id: oracle_feed_id.to_string(),
        })
        .collect(),
        pools: vec![pool_id],
        lp_account_id: None,
        deposits: Vec::new(),
    }
    .save()?;

    // The test owns its users: create, register and fund them like production traders
    // would (the server never holds user keys anymore).
    let users = get_users(NUM_USERS, &mut client).await?;
    for user in &users {
        let user_id = user.id();
        register_user_on_vault(
            &mut client,
            vault_id,
            user_id,
            user.pubkey().to_commitment().into(),
        )
        .await?;
        for asset in [asset0, asset1] {
            mint_asset_to_user(&mut client, asset, user_id, USER_INITIAL_ON_CHAIN_BALANCE).await?;
            fund_user_on_vault(
                &mut client,
                vault_id,
                user_id,
                FungibleAsset::new(asset, USER_INITIAL_ON_CHAIN_BALANCE)?,
            )
            .await?;
        }
        println!("User {} registered and funded.", user_id.to_hex());
    }

    let mut miden_execution = get_miden_execution().await?;

    let mut orders = Vec::with_capacity(users.len());
    for user in &users {
        let user_id = user.id();
        // Miden block headers and the production API both use Unix seconds. Keep this as a
        // wall-clock deadline: deriving expiry from block height would test a different contract.
        let expires_at = u64::try_from(chrono::Utc::now().timestamp())? + 3_600;

        let intent = Intent::new_swap(user_id, asset0, 10, asset1, 20, Uuid::new_v4(), expires_at);

        let msg_word = intent.message_word();
        let signature = user.sign(msg_word);

        let order = Order::new(
            signature,
            user_id,
            OrderDetails {
                asset_in: asset0,
                amount_in: 10,
                asset_out: asset1,
                min_amount_out: 20,
            },
            user.pubkey(),
            intent,
        );

        let order = order.start_processing();
        let order = order.processed(OrderExecutionResult { amount_out: 20 });
        orders.push(order);
    }

    miden_execution.handle_batch(orders).await;

    for user in &users {
        let user_id = user.id();
        let expected = (
            USER_INITIAL_ON_CHAIN_BALANCE - 10,
            USER_INITIAL_ON_CHAIN_BALANCE + 20,
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let balances = (
                get_user_balance_from_pool(pool_id, vault_id, asset0, user_id).await?,
                get_user_balance_from_pool(pool_id, vault_id, asset1, user_id).await?,
            );
            if balances == expected {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "swap balances did not finalize: got {balances:?}, expected {expected:?}"
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    Ok(())
}
