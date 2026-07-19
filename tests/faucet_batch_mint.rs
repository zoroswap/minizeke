//! Verifies multi-recipient faucet minting via `own_output_notes` (one tx, N P2IDs).

use anyhow::Result;
use miden_client::{
    asset::{Asset, FungibleAsset},
    note::NoteType,
};
use miden_protocol::note::NoteAttachments;
use miden_standards::note::P2idNote;
use minizeke::{
    faucet::build_batch_mint_request,
    test_utils::{consume_all_notes_for, get_client, get_faucet, get_user, submit_tx_resilient},
};
use uuid::Uuid;

#[tokio::test]
async fn batch_mints_two_recipients_in_one_transaction() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .init();
    dotenv::dotenv().ok();

    let test_dir = std::env::temp_dir().join(format!("minizeke-faucet-batch-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&test_dir)?;
    // Faucet/user keys go into ./keystore relative to cwd.
    std::env::set_current_dir(&test_dir)?;

    let mut client = get_client().await?;
    let faucet_id = get_faucet(&mut client, "BAT", 8, 10_000_000_000).await?;
    let user_a = get_user(&mut client).await?;
    let user_b = get_user(&mut client).await?;
    let amount = 1_000_000u64;

    let note_a = P2idNote::create(
        faucet_id,
        user_a,
        vec![Asset::from(FungibleAsset::new(faucet_id, amount)?)],
        NoteType::Public,
        NoteAttachments::empty(),
        client.rng(),
    )?;
    let note_b = P2idNote::create(
        faucet_id,
        user_b,
        vec![Asset::from(FungibleAsset::new(faucet_id, amount)?)],
        NoteType::Public,
        NoteAttachments::empty(),
        client.rng(),
    )?;

    let request =
        build_batch_mint_request(vec![note_a, note_b]).map_err(|error| anyhow::anyhow!(error))?;
    let tx_id = submit_tx_resilient(&mut client, faucet_id, request).await?;
    tracing::info!(%tx_id, "batch mint submitted");

    for user_id in [user_a, user_b] {
        loop {
            consume_all_notes_for(&mut client, user_id).await?;
            client.sync_state().await?;
            let balance = client
                .account_reader(user_id)
                .get_balance(faucet_id)
                .await?;
            if balance >= amount {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    let balance_a = client.account_reader(user_a).get_balance(faucet_id).await?;
    let balance_b = client.account_reader(user_b).get_balance(faucet_id).await?;
    assert_eq!(balance_a, amount);
    assert_eq!(balance_b, amount);
    Ok(())
}
