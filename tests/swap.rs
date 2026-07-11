use std::time::Duration;

use anyhow::Result;
use miden_client::asset::FungibleAsset;
use minizeke::{
    deployment::Deployment,
    intent::Intent,
    miden_env::MidenNetwork,
    order::{Order, OrderDetails, OrderExecutionResult},
    pool::{
        USER_INITIAL_ON_CHAIN_BALANCE, deploy_pool, get_user_balance_from_pool,
        get_user_trades_slot_name,
    },
    test_utils::{
        fund_user_on_vault, get_client, get_faucet, get_miden_execution, get_pool_client,
        get_vault, mint_asset_to_user, register_user_on_vault,
    },
    user::get_users,
    vault::set_pool_account_id_on_vault,
};

const NUM_USERS: u32 = 10;

#[tokio::test]
async fn test_swap() -> Result<()> {
    tracing_subscriber::fmt().init();

    // The server no longer deploys anything: it attaches to the accounts recorded in a
    // deployment file. The test deploys its own vault/faucets/pool and points
    // DEPLOYMENT_FILE at a scratch config for MidenExecution to load.
    let deployment_file = std::env::temp_dir().join("minizeke_test_swap_deployment.json");
    let _ = std::fs::remove_file(&deployment_file);
    unsafe { std::env::set_var("DEPLOYMENT_FILE", &deployment_file) };

    let mut client = get_client().await?;
    let mut pool_client = get_pool_client().await?;

    let vault_id = get_vault(&mut client).await?;
    let asset0 = get_faucet(&mut client, "ASTA").await?;
    let asset1 = get_faucet(&mut client, "ASTB").await?;
    let pool = deploy_pool(&mut pool_client, vault_id, asset0, asset1).await?;
    let pool_id = pool.id();
    set_pool_account_id_on_vault(&mut client, vault_id, pool_id).await?;

    Deployment {
        network: MidenNetwork::from_env().as_str().to_string(),
        vault_id,
        pool_id,
        asset0,
        asset1,
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
            mint_asset_to_user(&mut client, asset, user_id, USER_INITIAL_ON_CHAIN_BALANCE)
                .await?;
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
        let user_key_slot = get_user_trades_slot_name(user.index());

        let intent = Intent {
            user_suffix: user_id.suffix().as_canonical_u64(),
            user_prefix: user_id.prefix().as_u64(),
            user_key_prefix: user_key_slot.id().prefix().as_canonical_u64(),
            user_key_suffix: user_key_slot.id().suffix().as_canonical_u64(),
            sell_idx: 0,
            buy_idx: 1,
            sell_amount: 10,
            buy_amount: 10,
        };

        let msg_word = intent.message_word();
        let signature = user.sign(msg_word);

        let order = Order::new(
            signature,
            user_id,
            OrderDetails {
                asset_in: asset0,
                amount_in: 10,
                asset_out: asset1,
                min_amount_out: 10,
            },
            user.pubkey(),
        );

        let order = order.start_processing();
        let order = order.processed(OrderExecutionResult { amount_out: 10 });
        orders.push(order);
    }

    miden_execution.handle_batch(orders).await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    for user in &users {
        let user_id = user.id();
        let asset0_bal = get_user_balance_from_pool(pool_id, vault_id, asset0, 0, user_id).await?;
        let asset1_bal = get_user_balance_from_pool(pool_id, vault_id, asset1, 1, user_id).await?;
        assert_eq!(asset0_bal, USER_INITIAL_ON_CHAIN_BALANCE - 10);
        assert_eq!(asset1_bal, USER_INITIAL_ON_CHAIN_BALANCE + 10);
    }

    Ok(())
}
