use anyhow::Result;
use miden_client::{asset::FungibleAsset, transaction::TransactionRequestBuilder};
use minizeke::{
    note::{FundInstructions, ZekeNote, ZekeNoteInstructions},
    test_utils::{get_client, get_funded_user, get_vault},
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
    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(fund_note, None)])
        .build()?;

    info!("[TEST] submitting a FUND transaction");
    client.submit_new_transaction(vault_id, tx_req).await?;

    Ok(())
}
