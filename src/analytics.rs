use std::{collections::HashSet, env, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use miden_client::{
    Client, account::AccountId, keystore::FilesystemKeyStore, note::NoteScriptRoot,
    store::NoteFilter,
};
use miden_core::Word;
use tracing::{error, info, warn};

use crate::{
    analytics_store::{AnalyticsStore, CashFlow, CashFlowKind, NoteCursor},
    asset_utils::word_to_asset,
    deployment::Deployment,
    message_broker::message_broker::{MessageBroker, VaultCashFlowEvent, VaultCashFlowKind},
    note::{NoteKind, ZekeNote},
    test_utils::get_analytics_client,
};

pub async fn initialize(
    store: Arc<AnalyticsStore>,
    message_broker: Arc<MessageBroker>,
) -> Result<()> {
    let deployment = Deployment::load()?;
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = started_tx.send(Err(error.to_string()));
                return;
            }
        };
        match runtime.block_on(AnalyticsWorker::new(deployment, store, message_broker)) {
            Ok(worker) => {
                let _ = started_tx.send(Ok(()));
                runtime.block_on(worker.run());
            }
            Err(error) => {
                let _ = started_tx.send(Err(error.to_string()));
            }
        }
    });
    started_rx
        .recv()
        .map_err(|error| anyhow!("analytics worker exited during startup: {error}"))?
        .map_err(anyhow::Error::msg)
}

struct AnalyticsWorker {
    client: Client<FilesystemKeyStore>,
    store: Arc<AnalyticsStore>,
    message_broker: Arc<MessageBroker>,
    vault_id: AccountId,
    supported_assets: HashSet<AccountId>,
    fund_root: NoteScriptRoot,
    init_redeem_root: NoteScriptRoot,
    redeem_root: NoteScriptRoot,
}

impl AnalyticsWorker {
    async fn new(
        deployment: Deployment,
        store: Arc<AnalyticsStore>,
        message_broker: Arc<MessageBroker>,
    ) -> Result<Self> {
        let mut client = get_analytics_client().await?;
        client.ensure_genesis_in_place().await?;
        client.import_account_by_id(deployment.vault_id).await?;
        client.sync_state().await?;
        let builder = client.code_builder();
        let fund_root =
            ZekeNote::get_note_script(builder.clone(), NoteKind::Fund.masm_name())?.root();
        let init_redeem_root =
            ZekeNote::get_note_script(builder.clone(), NoteKind::InitRedeem.masm_name())?.root();
        let redeem_root = ZekeNote::get_note_script(builder, NoteKind::Redeem.masm_name())?.root();
        Ok(Self {
            client,
            store,
            message_broker,
            vault_id: deployment.vault_id,
            supported_assets: deployment
                .assets
                .into_iter()
                .map(|asset| asset.faucet_id)
                .collect(),
            fund_root,
            init_redeem_root,
            redeem_root,
        })
    }

    async fn run(mut self) {
        let seconds = env::var("ANALYTICS_SYNC_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5_u64)
            .max(1);
        let mut interval = tokio::time::interval(Duration::from_secs(seconds));
        info!("analytics cash-flow worker started");
        loop {
            interval.tick().await;
            if let Err(error) = self.sync_notes().await {
                // Surface the full chain — bare "RPC error" Display is useless for ops.
                error!(error = %format!("{error:#}"), "analytics note sync failed");
            }
        }
    }

    async fn sync_notes(&mut self) -> Result<()> {
        const SOURCE: &str = "vault_cash_flows";
        self.client.sync_state().await?;
        let cursor = self.store.note_cursor(SOURCE)?;
        let mut notes = self.client.get_input_notes(NoteFilter::Consumed).await?;
        notes.sort_by_key(|note| {
            (
                note.state()
                    .consumed_block_height()
                    .map(|block| block.as_u32())
                    .unwrap_or_default(),
                stable_note_id(note),
            )
        });
        for note in notes {
            let Some(block_num) = note
                .state()
                .consumed_block_height()
                .map(|block| block.as_u32())
            else {
                continue;
            };
            let note_cursor = NoteCursor {
                block_num,
                note_id: stable_note_id(&note),
            };
            if note_cursor <= cursor {
                continue;
            }
            if note
                .consumer_account()
                .is_some_and(|account| account != self.vault_id)
            {
                self.store.set_note_cursor(SOURCE, &note_cursor)?;
                continue;
            }
            let root = note.details().script().root();
            if root != self.fund_root && root != self.init_redeem_root && root != self.redeem_root {
                self.store.set_note_cursor(SOURCE, &note_cursor)?;
                continue;
            }
            let storage = note.details().storage().items();
            if storage.len() < 12 {
                warn!("ignoring malformed user cash-flow note");
                self.store.set_note_cursor(SOURCE, &note_cursor)?;
                continue;
            }
            let (kind, user_id, asset) = if root == self.fund_root {
                let user_id = AccountId::try_from_elements(storage[8], storage[9])?;
                let asset = note
                    .assets()
                    .iter_fungible()
                    .next()
                    .ok_or_else(|| anyhow!("FUND note has no fungible asset"))?;
                (CashFlowKind::Fund, user_id, asset)
            } else {
                let expected = Word::new([storage[0], storage[1], storage[2], storage[3]]);
                let asset = word_to_asset(expected)?;
                let user_id = if root == self.redeem_root {
                    AccountId::try_from_elements(storage[8], storage[9])?
                } else {
                    note.metadata()
                        .map(|metadata| metadata.sender())
                        .ok_or_else(|| anyhow!("INIT_REDEEM note is missing sender metadata"))?
                };
                let kind = if root == self.redeem_root {
                    CashFlowKind::Redeem
                } else {
                    CashFlowKind::InitRedeem
                };
                (kind, user_id, asset)
            };
            if !self.supported_assets.contains(&asset.faucet_id()) {
                self.store.set_note_cursor(SOURCE, &note_cursor)?;
                continue;
            }
            if !self.store.has_mark(&asset.faucet_id().to_hex())? {
                // Do not advance beyond an unpriced cash flow. Tuple ordering makes this
                // retry bounded to the unprocessed suffix instead of replaying all history.
                return Ok(());
            }
            let event_time = note
                .created_at()
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64);
            let amount = asset.amount().as_u64();
            let faucet_id = asset.faucet_id();
            let inserted = self.store.record_cash_flow(&CashFlow {
                event_id: format!("vault:{}", note_cursor.note_id),
                kind,
                user_id: user_id.to_hex(),
                asset_id: faucet_id.to_hex(),
                amount,
                event_time,
            })?;
            if inserted {
                let kind = match kind {
                    CashFlowKind::Fund => VaultCashFlowKind::Fund,
                    CashFlowKind::InitRedeem => VaultCashFlowKind::InitRedeem,
                    CashFlowKind::Redeem => VaultCashFlowKind::Redeem,
                };
                let _ = self.message_broker.broadcast_vault_cashflow(VaultCashFlowEvent {
                    user_id,
                    faucet_id,
                    amount,
                    kind,
                });
            }
            self.store.set_note_cursor(SOURCE, &note_cursor)?;
        }
        Ok(())
    }
}

fn stable_note_id(note: &miden_client::store::InputNoteRecord) -> String {
    note.id()
        .map(|id| id.to_hex())
        .unwrap_or_else(|| note.details_commitment().to_hex())
}
