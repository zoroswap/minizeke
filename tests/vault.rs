use anyhow::Result;
use miden_client::{
    asset::FungibleAsset, testing::common::wait_for_blocks, transaction::TransactionRequestBuilder,
};
use minizeke::{
    note::{FundInstructions, ZekeNote, ZekeNoteInstructions},
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

    Ok(())
}
