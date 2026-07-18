use anyhow::{Context, Result, anyhow};
use miden_client::{
    Client,
    account::AccountId,
    asset::{Asset, FungibleAsset},
    keystore::FilesystemKeyStore,
    note::NoteType,
    transaction::TransactionRequestBuilder,
};
use miden_protocol::note::NoteAttachments;
use miden_standards::note::P2idNote;
use tracing::info;

use crate::test_utils::submit_tx_resilient;

/// Max P2ID notes emitted in a single bank transaction.
const P2ID_BATCH_SIZE: usize = 8;

/// Send `amount` of `faucet_id` from `bank_id` to each recipient as a public P2ID note.
/// Recipients are chunked into batches of [`P2ID_BATCH_SIZE`] notes per transaction.
pub async fn send_p2id_batch(
    client: &mut Client<FilesystemKeyStore>,
    bank_id: AccountId,
    recipients: &[AccountId],
    faucet_id: AccountId,
    amount: u64,
) -> Result<()> {
    if recipients.is_empty() {
        return Ok(());
    }
    if amount == 0 {
        return Err(anyhow!("P2ID transfer amount must be non-zero"));
    }

    for (batch_index, chunk) in recipients.chunks(P2ID_BATCH_SIZE).enumerate() {
        let mut notes = Vec::with_capacity(chunk.len());
        for &target in chunk {
            let asset = FungibleAsset::new(faucet_id, amount)
                .map_err(|error| anyhow!("invalid P2ID asset: {error:?}"))?;
            let note = P2idNote::create(
                bank_id,
                target,
                vec![Asset::from(asset)],
                NoteType::Public,
                NoteAttachments::empty(),
                client.rng(),
            )
            .map_err(|error| anyhow!("create P2ID note: {error:?}"))?;
            notes.push(note);
        }
        let tx_req = TransactionRequestBuilder::new()
            .own_output_notes(notes)
            .build()
            .context("build batched P2ID transaction")?;
        let tx_id = submit_tx_resilient(client, bank_id, tx_req).await?;
        info!(
            batch = batch_index,
            recipients = chunk.len(),
            faucet = %faucet_id.to_hex(),
            amount,
            transaction = %tx_id.to_hex(),
            "bank distributed P2ID batch"
        );
        client.sync_state().await?;
    }
    Ok(())
}
