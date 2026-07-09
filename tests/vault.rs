use std::time::Duration;

use anyhow::Result;
use miden_client::{
    asset::FungibleAsset, testing::common::wait_for_blocks, transaction::TransactionRequestBuilder,
};
use minizeke::{
    note::{
        FundInstructions, InitRedeemInstructions, RedeemInstructions, ZekeNote,
        ZekeNoteInstructions,
    },
    test_utils::{get_client, get_funded_user, get_vault},
    vault::get_vault_user_asset_info,
};
use tracing::info;

#[tokio::test]
async fn test_fund_redeem() -> Result<()> {
    tracing_subscriber::fmt().init();
    let mut client = get_client().await?;

    info!("[TEST] creating vault");
    let vault_id = get_vault(&mut client).await?;

    info!("[TEST] creating a funded user");
    let (user_id, faucet_id) = get_funded_user(&mut client).await?;

    let user_balance_before_fund = client.account_reader(user_id).get_balance(faucet_id).await?;

    info!("[TEST] creating a FUND note");
    let fund_note = ZekeNote::new(
        ZekeNoteInstructions::Fund(FundInstructions {
            user_id,
            vault_id,
            note_assets: vec![FungibleAsset::new(faucet_id, 199)?],
        }),
        client.code_builder(),
    )?;

    info!("[TEST] building tx for sending FUND note");
    let tx_req = TransactionRequestBuilder::new()
        .own_output_notes(vec![fund_note.note().clone()])
        .build()?;

    info!("[TEST] sending a FUND note");
    client.submit_new_transaction(user_id, tx_req).await?;
    client.sync_state().await?;

    wait_for_blocks(&mut client, 1).await;

    info!("[TEST] building tx for consuming FUND note");
    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(fund_note.note().clone(), None)])
        .build()?;

    info!("[TEST] consuming a FUND note");
    client.submit_new_transaction(vault_id, tx_req).await?;
    client.sync_state().await?;

    wait_for_blocks(&mut client, 1).await;

    let vault_info = get_vault_user_asset_info(&client, vault_id, faucet_id, user_id).await?;
    assert_eq!(vault_info.total_funding, 199);
    assert_eq!(vault_info.total_initiated_redeems, 0);
    assert_eq!(vault_info.total_redeems, 0);

    // ------------------------------------------------------------------------------------------
    // STEP 1: INITIATE_REDEEM
    // ------------------------------------------------------------------------------------------

    info!("[TEST] creating an INIT_REDEEM note");
    let init_redeem_note = ZekeNote::new(
        ZekeNoteInstructions::InitRedeem(InitRedeemInstructions {
            user_id,
            vault_id,
            min_expected_asset: FungibleAsset::new(faucet_id, 199)?,
        }),
        client.code_builder(),
    )?;

    info!("[TEST] sending an INIT_REDEEM note");
    let tx_req = TransactionRequestBuilder::new()
        .own_output_notes(vec![init_redeem_note.note().clone()])
        .build()?;
    client.submit_new_transaction(user_id, tx_req).await?;
    client.sync_state().await?;

    wait_for_blocks(&mut client, 1).await;

    info!("[TEST] consuming an INIT_REDEEM note");
    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(init_redeem_note.note().clone(), None)])
        .build()?;
    client.submit_new_transaction(vault_id, tx_req).await?;
    client.sync_state().await?;

    wait_for_blocks(&mut client, 1).await;

    let vault_info = get_vault_user_asset_info(&client, vault_id, faucet_id, user_id).await?;
    assert_eq!(vault_info.total_funding, 199);
    assert_eq!(vault_info.total_initiated_redeems, 199);
    assert_eq!(vault_info.total_redeems, 0);
    assert_eq!(vault_info.pending_redeem(), 199);

    // ------------------------------------------------------------------------------------------
    // STEP 2: REDEEM
    // ------------------------------------------------------------------------------------------

    info!("[TEST] creating a REDEEM note");
    let redeem_note = ZekeNote::new(
        ZekeNoteInstructions::Redeem(RedeemInstructions {
            user_id,
            vault_id,
            min_expected_asset: FungibleAsset::new(faucet_id, 199)?,
        }),
        client.code_builder(),
    )?;

    info!("[TEST] sending a REDEEM note");
    let tx_req = TransactionRequestBuilder::new()
        .own_output_notes(vec![redeem_note.note().clone()])
        .build()?;
    client.submit_new_transaction(user_id, tx_req).await?;
    client.sync_state().await?;

    wait_for_blocks(&mut client, 1).await;

    info!("[TEST] consuming a REDEEM note");
    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(redeem_note.note().clone(), None)])
        .build()?;
    client.submit_new_transaction(vault_id, tx_req).await?;
    client.sync_state().await?;

    wait_for_blocks(&mut client, 1).await;

    let vault_info = get_vault_user_asset_info(&client, vault_id, faucet_id, user_id).await?;
    assert_eq!(vault_info.total_funding, 199);
    assert_eq!(vault_info.total_initiated_redeems, 199);
    assert_eq!(vault_info.total_redeems, 199);
    assert_eq!(vault_info.pending_redeem(), 0);

    // ------------------------------------------------------------------------------------------
    // FINAL: the user consumes the P2ID payout note and ends up with his original balance
    // ------------------------------------------------------------------------------------------

    info!("[TEST] consuming the P2ID payout note as the user");
    loop {
        client.sync_state().await?;

        let consumable_notes = client.get_consumable_notes(Some(user_id)).await?;
        let notes = consumable_notes
            .iter()
            .map(|(note, _)| note.clone().try_into())
            .collect::<Result<Vec<_>, _>>()?;

        if !notes.is_empty() {
            let tx_req = TransactionRequestBuilder::new().build_consume_notes(notes)?;
            client.submit_new_transaction(user_id, tx_req).await?;
            break;
        }
        info!("[TEST] waiting for the P2ID payout note...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    client.sync_state().await?;

    wait_for_blocks(&mut client, 1).await;

    let user_balance = client.account_reader(user_id).get_balance(faucet_id).await?;
    assert_eq!(user_balance, user_balance_before_fund);

    Ok(())
}
