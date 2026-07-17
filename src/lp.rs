use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
    time::Duration,
};

use alloy_primitives::U256;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use miden_client::{
    Client, account::AccountId, assembly::CodeBuilder, asset::FungibleAsset,
    keystore::FilesystemKeyStore, note::NoteScriptRoot, store::NoteFilter,
};
use miden_core::Word;
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info, warn};

use crate::{
    asset_utils::word_to_asset,
    deployment::{AssetInfo, Deployment},
    lp_store::{LpOperationKind as StoredOperationKind, LpPosition, LpStore},
    message_broker::message_broker::{
        LpChainEvent, LpOperationKind, MessageBroker, PoolStateEvent,
    },
    note::{DepositInstructions, NoteKind, ZekeNote, ZekeNoteInstructions},
    pool::{PoolBalances, PoolMetadata, PoolSettings, PoolState},
    test_utils::get_lp_client,
    vault::{checkpoint_lp_entitlement_on_vault, get_vault_lp_info},
};

pub const DEFAULT_LP_CHECKPOINT_INTERVAL_SECS: u64 = 600;
pub const DEFAULT_LP_SYNC_INTERVAL_SECS: u64 = 2;
pub const DEFAULT_LP_MIN_DEPOSIT_AMOUNT: u64 = 1;

#[derive(Clone)]
pub struct LpService {
    store: Arc<LpStore>,
    vault_id: AccountId,
    supported_assets: Arc<HashSet<AccountId>>,
    minimum_deposit: u64,
}

impl LpService {
    pub fn store(&self) -> Arc<LpStore> {
        self.store.clone()
    }

    pub fn minimum_deposit(&self) -> u64 {
        self.minimum_deposit
    }

    pub fn build_deposit_note(&self, lp_id: AccountId, asset: FungibleAsset) -> Result<ZekeNote> {
        if !self.supported_assets.contains(&asset.faucet_id()) {
            return Err(anyhow!(
                "unsupported LP asset {}",
                asset.faucet_id().to_hex()
            ));
        }
        if asset.amount().as_u64() < self.minimum_deposit {
            return Err(anyhow!(
                "deposit amount must be at least {}",
                self.minimum_deposit
            ));
        }
        ZekeNote::new(
            ZekeNoteInstructions::Deposit(DepositInstructions {
                lp_id,
                vault_id: self.vault_id,
                asset,
            }),
            CodeBuilder::new(),
        )
    }

    pub fn position(&self, lp_id: AccountId, faucet_id: AccountId) -> Result<Option<LpPosition>> {
        self.store.position(lp_id, faucet_id)
    }
}

struct LpWorker {
    client: Client<FilesystemKeyStore>,
    store: Arc<LpStore>,
    broker: Arc<MessageBroker>,
    deployment: Deployment,
    pool_states: HashMap<AccountId, PoolState>,
    deposit_root: NoteScriptRoot,
    withdraw_root: NoteScriptRoot,
    minimum_deposit: u64,
}

pub async fn initialize(
    broker: Arc<MessageBroker>,
    initial_pool_states: HashMap<AccountId, PoolState>,
) -> Result<LpService> {
    let deployment = Deployment::load()?;
    let minimum_deposit = env_u64("LP_MIN_DEPOSIT_AMOUNT", DEFAULT_LP_MIN_DEPOSIT_AMOUNT)?;
    let store = Arc::new(LpStore::open_from_env()?);
    seed_deployment_positions(&store, &deployment)?;
    let supported_assets = Arc::new(
        deployment
            .assets
            .iter()
            .map(|asset| asset.faucet_id)
            .collect::<HashSet<_>>(),
    );
    let service = LpService {
        store: store.clone(),
        vault_id: deployment.vault_id,
        supported_assets,
        minimum_deposit,
    };

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
        match runtime.block_on(LpWorker::new(
            deployment,
            store,
            broker,
            initial_pool_states,
            minimum_deposit,
        )) {
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
        .map_err(|error| anyhow!("LP worker exited during startup: {error}"))?
        .map_err(anyhow::Error::msg)?;
    Ok(service)
}

impl LpWorker {
    async fn new(
        deployment: Deployment,
        store: Arc<LpStore>,
        broker: Arc<MessageBroker>,
        pool_states: HashMap<AccountId, PoolState>,
        minimum_deposit: u64,
    ) -> Result<Self> {
        let mut client = get_lp_client().await?;
        client.ensure_genesis_in_place().await?;
        client
            .import_account_by_id(deployment.operator_account_id)
            .await?;
        client.import_account_by_id(deployment.vault_id).await?;
        client.sync_state().await?;

        let code_builder = client.code_builder();
        let deposit_root =
            ZekeNote::get_note_script(code_builder.clone(), NoteKind::Deposit.masm_name())?.root();
        let withdraw_root =
            ZekeNote::get_note_script(code_builder, NoteKind::Withdraw.masm_name())?.root();

        let worker = Self {
            client,
            store,
            broker,
            deployment,
            pool_states,
            deposit_root,
            withdraw_root,
            minimum_deposit,
        };
        worker.initialize_sync_cursor().await?;
        info!(
            minimum_deposit,
            "LP worker initialized with an independent Miden client"
        );
        Ok(worker)
    }

    async fn initialize_sync_cursor(&self) -> Result<()> {
        if self.store.sync_cursor()? != 0 {
            return Ok(());
        }
        let notes = self.client.get_input_notes(NoteFilter::Consumed).await?;
        let baseline = notes
            .iter()
            .filter_map(|note| note.state().consumed_block_height())
            .map(|block| block.as_u32())
            .max()
            .unwrap_or(0);
        self.store.set_sync_cursor(baseline)?;
        Ok(())
    }

    async fn run(mut self) {
        let sync_secs =
            env_u64("LP_SYNC_INTERVAL_SECS", DEFAULT_LP_SYNC_INTERVAL_SECS).unwrap_or(2);
        let checkpoint_secs = env_u64(
            "LP_CHECKPOINT_INTERVAL_SECS",
            DEFAULT_LP_CHECKPOINT_INTERVAL_SECS,
        )
        .unwrap_or(DEFAULT_LP_CHECKPOINT_INTERVAL_SECS);
        let mut sync_interval = tokio::time::interval(Duration::from_secs(sync_secs.max(1)));
        let mut checkpoint_interval =
            tokio::time::interval(Duration::from_secs(checkpoint_secs.max(1)));
        checkpoint_interval.reset();
        let mut pool_rx = self.broker.subscribe_pool_state();
        let mut applied_rx = self.broker.subscribe_lp_applied();

        loop {
            tokio::select! {
                _ = sync_interval.tick() => {
                    if let Err(error) = self.sync_lp_notes().await {
                        error!(%error, "LP note sync failed");
                    }
                }
                _ = checkpoint_interval.tick() => {
                    if let Err(error) = self.checkpoint_entitlements().await {
                        error!(%error, "LP entitlement checkpoint failed");
                    }
                }
                event = pool_rx.recv() => match event {
                    Ok(PoolStateEvent { pool_states, .. }) => {
                        for (faucet_id, state) in pool_states {
                            self.pool_states.insert(faucet_id, state);
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        warn!(skipped, "LP pool-state subscriber lagged");
                    }
                    Err(RecvError::Closed) => break,
                },
                event = applied_rx.recv() => match event {
                    Ok(event) => {
                        let result = if let Some(reason) = event.error {
                            self.store.mark_failed(&event.note_id, &reason).map(|_| false)
                        } else {
                            self.store.apply_operation(
                                &event.note_id,
                                event.lp_shares,
                                now_millis(),
                            )
                        };
                        if let Err(error) = result {
                            error!(note_id = %event.note_id, %error, "failed to persist applied LP event");
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        warn!(skipped, "LP applied-event subscriber lagged");
                    }
                    Err(RecvError::Closed) => break,
                },
            }
        }
    }

    async fn sync_lp_notes(&mut self) -> Result<()> {
        self.client.sync_state().await?;
        let cursor = self.store.sync_cursor()?;
        let notes = self.client.get_input_notes(NoteFilter::Consumed).await?;
        let mut max_block = cursor;
        for note in notes {
            let Some(block_num) = note
                .state()
                .consumed_block_height()
                .map(|block| block.as_u32())
            else {
                continue;
            };
            if block_num < cursor {
                continue;
            }
            max_block = max_block.max(block_num);
            if note.details().script().root() != self.deposit_root
                && note.details().script().root() != self.withdraw_root
            {
                continue;
            }
            if note
                .consumer_account()
                .is_some_and(|id| id != self.deployment.vault_id)
            {
                continue;
            }
            let storage = note.details().storage().items();
            if storage.len() < 12 {
                warn!("ignoring malformed LP note storage");
                continue;
            }
            let lp_id = AccountId::try_from_elements(storage[8], storage[9])?;
            let note_id = note
                .id()
                .map(|id| id.to_hex())
                .unwrap_or_else(|| note.details_commitment().to_hex());
            let nullifier = note.nullifier().map(|value| value.to_hex());
            let created_at = note.created_at().unwrap_or_else(now_millis);

            let (kind, asset) = if note.details().script().root() == self.deposit_root {
                let asset = note
                    .assets()
                    .iter_fungible()
                    .next()
                    .ok_or_else(|| anyhow!("DEPOSIT note has no fungible asset"))?;
                (LpOperationKind::Deposit, asset)
            } else {
                let expected = Word::new([storage[0], storage[1], storage[2], storage[3]]);
                (LpOperationKind::Withdraw, word_to_asset(expected)?)
            };
            if !self
                .deployment
                .assets
                .iter()
                .any(|configured| configured.faucet_id == asset.faucet_id())
            {
                continue;
            }
            let stored_kind = match kind {
                LpOperationKind::Deposit => StoredOperationKind::Deposit,
                LpOperationKind::Withdraw => StoredOperationKind::Withdraw,
            };
            let inserted = self.store.record_confirmed(
                &note_id,
                nullifier.as_deref(),
                stored_kind,
                lp_id,
                asset.faucet_id(),
                asset.amount().as_u64(),
                block_num,
                created_at,
            )?;
            if inserted
                && kind == LpOperationKind::Deposit
                && asset.amount().as_u64() < self.minimum_deposit
            {
                self.store.mark_failed(
                    &note_id,
                    &format!("deposit below minimum {}", self.minimum_deposit),
                )?;
            }
        }
        self.store.set_sync_cursor(max_block)?;
        self.replay_confirmed()
    }

    fn replay_confirmed(&self) -> Result<()> {
        for operation in self.store.confirmed_operations()? {
            let lp_id = AccountId::from_hex(&operation.lp_id)?;
            let faucet_id = AccountId::from_hex(&operation.faucet_id)?;
            let kind = if operation.kind == "deposit" {
                LpOperationKind::Deposit
            } else {
                LpOperationKind::Withdraw
            };
            let shares_hint = if kind == LpOperationKind::Withdraw {
                self.withdrawal_shares_hint(lp_id, faucet_id, operation.asset_amount)?
            } else {
                None
            };
            self.broker.broadcast_lp_chain(LpChainEvent {
                note_id: operation.note_id,
                kind,
                lp_id,
                faucet_id,
                asset_amount: operation.asset_amount,
                shares_hint,
            })?;
        }
        Ok(())
    }

    fn withdrawal_shares_hint(
        &self,
        lp_id: AccountId,
        faucet_id: AccountId,
        amount: u64,
    ) -> Result<Option<u64>> {
        let Some(position) = self.store.position(lp_id, faucet_id)? else {
            return Ok(None);
        };
        if position.checkpoint_value == 0 || position.checkpoint_shares == 0 {
            return Ok(None);
        }
        Ok(Some(checkpoint_share_burn(&position, amount)?))
    }

    async fn checkpoint_entitlements(&mut self) -> Result<()> {
        for position in self.store.positions()? {
            if position.shares == 0 {
                continue;
            }
            let lp_id = AccountId::from_hex(&position.lp_id)?;
            let faucet_id = AccountId::from_hex(&position.faucet_id)?;
            let pool = self
                .pool_states
                .get(&faucet_id)
                .ok_or_else(|| anyhow!("missing pool state for checkpoint"))?;
            let (value, _, _) = pool.get_withdraw_asset_amount_out(U256::from(position.shares))?;
            let value = value.saturating_to::<u64>();
            let info =
                get_vault_lp_info(&self.client, self.deployment.vault_id, faucet_id, lp_id).await?;
            let target = info.withdrawn.saturating_add(value);
            if target > info.entitlement {
                checkpoint_lp_entitlement_on_vault(
                    &mut self.client,
                    self.deployment.operator_account_id,
                    self.deployment.vault_id,
                    faucet_id,
                    lp_id,
                    target,
                )
                .await?;
            }
            self.store.record_checkpoint(
                lp_id,
                faucet_id,
                position.shares,
                value,
                info.withdrawn,
                now_millis(),
            )?;
        }
        Ok(())
    }
}

fn seed_deployment_positions(store: &LpStore, deployment: &Deployment) -> Result<()> {
    let Some(lp_id) = deployment.lp_account_id else {
        return Ok(());
    };
    let assets = deployment
        .assets
        .iter()
        .map(|asset| (asset.faucet_id, asset))
        .collect::<HashMap<AccountId, &AssetInfo>>();
    let mut pools = HashMap::new();
    for asset in &deployment.assets {
        pools.insert(
            asset.faucet_id,
            PoolState::new(
                PoolSettings::default(),
                PoolBalances::default(),
                0,
                PoolMetadata {
                    name: "Deployment asset",
                    asset_decimals: asset.decimals,
                },
            ),
        );
    }
    let mut shares_by_asset = HashMap::<AccountId, u64>::new();
    for record in &deployment.deposits {
        if !assets.contains_key(&record.faucet_id) {
            continue;
        }
        let pool = pools.get_mut(&record.faucet_id).unwrap();
        let (shares, supply, balances) =
            pool.get_deposit_lp_amount_out(U256::from(record.amount))?;
        pool.update_state(balances, supply);
        *shares_by_asset.entry(record.faucet_id).or_default() = shares_by_asset
            .get(&record.faucet_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(shares.saturating_to::<u64>());
    }
    for (faucet_id, shares) in shares_by_asset {
        store.seed_position(lp_id, faucet_id, shares, now_millis())?;
    }
    Ok(())
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    env::var(name)
        .ok()
        .map(|value| value.parse().with_context(|| format!("parse {name}")))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn now_millis() -> u64 {
    Utc::now().timestamp_millis() as u64
}

fn checkpoint_share_burn(position: &LpPosition, amount: u64) -> Result<u64> {
    if position.checkpoint_value == 0 || position.checkpoint_shares == 0 {
        return Err(anyhow!("position has no checkpoint valuation"));
    }
    let numerator = u128::from(amount) * u128::from(position.checkpoint_shares);
    let shares = numerator
        .div_ceil(u128::from(position.checkpoint_value))
        .min(u128::from(position.shares));
    Ok(u64::try_from(shares)?)
}

#[cfg(test)]
mod tests {
    use miden_client::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_UPDATABLE_CODE,
    };

    use super::*;

    #[test]
    fn partial_withdrawal_burns_checkpointed_share_fraction() {
        let position = LpPosition {
            lp_id: "lp".to_string(),
            faucet_id: "asset".to_string(),
            shares: 100,
            checkpoint_shares: 100,
            checkpoint_value: 250,
            checkpoint_withdrawn: 0,
            updated_at: 0,
        };
        assert_eq!(checkpoint_share_burn(&position, 25).unwrap(), 10);
        assert_eq!(checkpoint_share_burn(&position, 26).unwrap(), 11);
        assert_eq!(checkpoint_share_burn(&position, 1_000).unwrap(), 100);
    }

    #[test]
    fn deposit_note_builder_is_permissionless_but_enforces_dust_limit() {
        let lp_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE).unwrap();
        let vault_id =
            AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_UPDATABLE_CODE).unwrap();
        let faucet_id = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap();
        let service = LpService {
            store: Arc::new(LpStore::open(":memory:").unwrap()),
            vault_id,
            supported_assets: Arc::new(HashSet::from([faucet_id])),
            minimum_deposit: 10,
        };

        assert!(
            service
                .build_deposit_note(lp_id, FungibleAsset::new(faucet_id, 9).unwrap())
                .is_err()
        );
        let note = service
            .build_deposit_note(lp_id, FungibleAsset::new(faucet_id, 10).unwrap())
            .unwrap();
        assert_eq!(
            note.note()
                .assets()
                .iter_fungible()
                .next()
                .unwrap()
                .amount()
                .as_u64(),
            10
        );
    }
}
