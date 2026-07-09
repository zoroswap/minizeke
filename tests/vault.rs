use anyhow::Result;
use miden_client::{asset::FungibleAsset, transaction::TransactionRequestBuilder};
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
    )?
    .note()
    .clone();

    info!("[TEST] building a FUND transaction");
    let fund_script = fund_note.recipient().script().clone();
    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(fund_note, None)])
        .expected_ntx_scripts(vec![fund_script])
        .build()?;

    info!("[TEST] submitting a FUND transaction");
    client.submit_new_transaction(vault_id, tx_req).await?;

    info!("[TEST] syncing state and checking on-chain vault storage");
    client.sync_state().await?;

    let vault_info = get_vault_user_asset_info(&client, vault_id, faucet_id, user_id).await?;
    assert_eq!(vault_info.total_funding, 199);
    assert_eq!(vault_info.total_initiated_redeems, 0);
    assert_eq!(vault_info.total_redeems, 0);

    Ok(())
}
